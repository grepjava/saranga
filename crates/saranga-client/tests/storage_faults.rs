//! An in-process Saranga broker on a failing disk. A write or sync failing
//! under the journals, a page file or a large message file is a critical
//! I/O error, as Artemis's `IOCriticalErrorListener` makes it: the client
//! is answered with an error, not left waiting, the broker asks to stop,
//! and a broker started again on the same data has every message whose
//! send (or commit) was answered, once, and none whose acknowledgement was.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use saranga_client::message;
use saranga_client::*;
use saranga_fault::{Error, Fault, Op};
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};

const WAIT: Duration = Duration::from_secs(10);

fn config(dir: &Path) -> BrokerConfig {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>65536</journal-file-size>
             <journal-min-files>2</journal-min-files>
             <address-settings>
               <address-setting match="paged">
                 <max-size-bytes>10240</max-size-bytes>
                 <page-size-bytes>20480</page-size-bytes>
                 <address-full-policy>PAGE</address-full-policy>
               </address-setting>
             </address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap()
}

async fn start(dir: &Path) -> (Arc<Broker>, String) {
    let broker = Broker::start(config(dir)).await.unwrap();
    let address = format!("127.0.0.1:{}", free_port());
    broker
        .add_acceptor("artemis", &address, Acceptor::default())
        .await
        .unwrap();
    (broker, address)
}

/// What `saranga run` does to stop a broker.
async fn stop(broker: Arc<Broker>) {
    broker.pause_acceptors().await;
    broker.close_unestablished();
    for c in broker.registry().connections() {
        c.fail();
    }
    while !broker.registry().all_closed() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    broker.stop().unwrap();
    // Nothing keeps a stopped broker, or the files it had open.
    let weak = Arc::downgrade(&broker);
    let storage = Arc::downgrade(broker.storage().unwrap());
    drop(broker);
    for _ in 0..500 {
        if weak.strong_count() + storage.strong_count() == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "a stopped broker is still referenced: broker {}, storage {}",
        weak.strong_count(),
        storage.strong_count()
    );
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn session(address: &str, options: SessionOptions) -> (Connection, Session) {
    let conn = Connection::connect(address, ConnectionOptions::default())
        .await
        .unwrap();
    let session = conn.create_session(options).await.unwrap();
    (conn, session)
}

async fn queue(session: &Session, name: &str) {
    session
        .create_queue(
            QueueConfiguration::new(name)
                .routing_type(RoutingType::Anycast)
                .durable(true),
        )
        .await
        .unwrap();
}

fn numbered(n: i32, body: &[u8]) -> CoreMessage {
    let mut m = message::bytes_message(body);
    m.put_property("n", PropertyValue::Int(n));
    m
}

fn number(d: &Delivery) -> i32 {
    match d.message.property("n") {
        Some(PropertyValue::Int(n)) => *n,
        other => panic!("no number: {other:?}"),
    }
}

/// Waits for the broker to ask to stop after a critical I/O error.
async fn assert_critical(broker: &Broker) {
    tokio::time::timeout(WAIT, broker.shutdown_requested())
        .await
        .expect("the broker did not ask to stop");
    assert!(broker.is_critical_io_failed());
}

/// Every message left on `queue`, by number, acknowledged.
async fn drain(address: &str, queue: &str) -> Vec<i32> {
    let (_conn, session) = session(
        address,
        SessionOptions {
            ack_batch_size: 0,
            ..SessionOptions::default()
        },
    )
    .await;
    let consumer = session.create_consumer(queue, None, false).await.unwrap();
    session.start().await.unwrap();
    let mut got = Vec::new();
    while let Some(d) = consumer.receive(Duration::from_millis(500)).await.unwrap() {
        got.push(number(&d));
        consumer.acknowledge(&d).await.unwrap();
    }
    got
}

/// Sends numbered durable messages until one fails; the numbers answered.
async fn send_until_failure(producer: &Producer, body: &[u8], from: i32) -> (Vec<i32>, i32) {
    let mut sent = Vec::new();
    for n in from.. {
        let send = producer.send(numbered(n, body));
        match tokio::time::timeout(WAIT, send).await {
            Ok(Ok(())) => sent.push(n),
            Ok(Err(_)) => return (sent, n),
            Err(_) => panic!("send {n} was never answered"),
        }
        assert!(n < from + 2_000, "no send failed");
    }
    unreachable!()
}

/// Checks what a restarted broker has: every send answered, once, in
/// order, and at most the one that failed besides.
fn assert_kept(got: &[i32], sent: &[i32], failed: i32) {
    let unique: BTreeSet<i32> = got.iter().copied().collect();
    assert_eq!(unique.len(), got.len(), "duplicates: {got:?}");
    for n in sent {
        assert!(unique.contains(n), "message {n} was lost");
    }
    for n in got {
        assert!(
            sent.contains(n) || *n == failed,
            "message {n} was never sent"
        );
    }
    let mut sorted = got.to_vec();
    sorted.sort();
    assert_eq!(got, sorted, "out of order");
}

/// Sends `before` messages to `address`, arms `fault`, sends until one
/// fails, then restarts the broker on the same data and checks the queue.
async fn scenario(fault: impl FnOnce(&Path) -> Fault, address_name: &str, body: &[u8]) {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address) = start(dir.path()).await;
    let (conn, s) = session(&address, SessionOptions::default()).await;
    queue(&s, address_name).await;
    let producer = s.create_producer(Some(address_name)).await.unwrap();
    let mut sent = Vec::new();
    for n in 0..20 {
        producer.send(numbered(n, body)).await.unwrap();
        sent.push(n);
    }
    let armed = fault(dir.path()).arm();
    let (more, failed) = send_until_failure(&producer, body, 20).await;
    sent.extend(more);
    assert!(armed.hits() >= 1);
    assert_critical(&broker).await;
    drop(conn);
    // The disk is still failing as the broker stops.
    stop(broker).await;
    drop(armed);

    let (broker, address) = start(dir.path()).await;
    let got = drain(&address, address_name).await;
    assert_kept(&got, &sent, failed);
    stop(broker).await;
    std::fs::remove_dir_all(dir.path()).expect("a file of the data was left open");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_journal_sync() {
    scenario(
        |d| Fault::new(Op::Sync, d).named(".amq"),
        "orders",
        &[7; 100],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_disk_under_the_journal() {
    scenario(
        |d| {
            Fault::new(Op::Write, d)
                .named(".amq")
                .error(Error::DiskFull)
        },
        "orders",
        &[7; 100],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_torn_journal_write() {
    scenario(
        |d| Fault::new(Op::Write, d).named(".amq").torn(23),
        "orders",
        &[7; 100],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_journal_file_that_cannot_be_made() {
    // 20 KiB a message fills a 64 KiB file every few sends.
    scenario(
        |d| Fault::new(Op::Open, d).named(".amq"),
        "orders",
        &[7; 20 * 1024],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_page_write() {
    scenario(
        |d| {
            Fault::new(Op::Write, d)
                .named(".page")
                .error(Error::DiskFull)
        },
        "paged",
        &[7; 1024],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_page_sync() {
    scenario(
        |d| Fault::new(Op::Sync, d).named(".page"),
        "paged",
        &[7; 1024],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_large_message_write() {
    scenario(
        |d| Fault::new(Op::Write, d).named(".msg").torn(4096),
        "orders",
        &[7; 200 * 1024],
    )
    .await;
}

/// A commit whose sync fails is not answered as done, and a restarted
/// broker has all of its transaction or none of it.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_commit_is_all_or_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address) = start(dir.path()).await;
    let (conn, s) = session(&address, SessionOptions::transacted()).await;
    queue(&s, "orders").await;
    let producer = s.create_producer(Some("orders")).await.unwrap();
    for n in 0..10 {
        producer.send(numbered(n, &[1; 100])).await.unwrap();
    }
    s.commit().await.unwrap();
    for n in 10..20 {
        producer.send(numbered(n, &[1; 100])).await.unwrap();
    }
    let armed = Fault::new(Op::Sync, dir.path()).named(".amq").arm();
    let commit = tokio::time::timeout(WAIT, s.commit())
        .await
        .expect("the commit was never answered");
    assert!(commit.is_err(), "a commit that was not synced succeeded");
    assert_critical(&broker).await;
    drop(conn);
    stop(broker).await;
    drop(armed);

    let (broker, address) = start(dir.path()).await;
    let got = drain(&address, "orders").await;
    assert!(
        got == (0..10).collect::<Vec<_>>() || got == (0..20).collect::<Vec<_>>(),
        "{got:?}"
    );
    stop(broker).await;
}

/// Acknowledgements committed before the failure stay done; those of the
/// commit that failed are undone with it.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_acknowledgement_commit() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address) = start(dir.path()).await;
    {
        let (_conn, s) = session(&address, SessionOptions::default()).await;
        queue(&s, "orders").await;
        let producer = s.create_producer(Some("orders")).await.unwrap();
        for n in 0..20 {
            producer.send(numbered(n, &[1; 100])).await.unwrap();
        }
    }
    let (conn, s) = session(&address, SessionOptions::transacted()).await;
    let consumer = s.create_consumer("orders", None, false).await.unwrap();
    s.start().await.unwrap();
    let take = async |count: usize| {
        for _ in 0..count {
            let d = consumer.receive(WAIT).await.unwrap().expect("a message");
            consumer.acknowledge(&d).await.unwrap();
        }
    };
    take(10).await;
    s.commit().await.unwrap();
    take(5).await;
    let armed = Fault::new(Op::Sync, dir.path()).named(".amq").arm();
    let commit = tokio::time::timeout(WAIT, s.commit())
        .await
        .expect("the commit was never answered");
    assert!(commit.is_err(), "a commit that was not synced succeeded");
    assert_critical(&broker).await;
    drop(conn);
    stop(broker).await;
    drop(armed);

    let (broker, address) = start(dir.path()).await;
    let got = drain(&address, "orders").await;
    assert!(
        got == (10..20).collect::<Vec<_>>() || got == (15..20).collect::<Vec<_>>(),
        "{got:?}"
    );
    stop(broker).await;
}

/// A broker that cannot write its `server.lock` does not start, and starts
/// once the disk is back.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_lock_that_cannot_be_written() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Fault::new(Op::Write, dir.path()).named("server.lock").arm();
    let started = tokio::time::timeout(WAIT, Broker::start(config(dir.path())))
        .await
        .expect("the start was never over");
    assert!(started.is_err(), "a broker started without its server.lock");
    assert!(armed.hits() >= 1);
    drop(armed);
    let (broker, address) = start(dir.path()).await;
    let (_conn, s) = session(&address, SessionOptions::default()).await;
    queue(&s, "orders").await;
    stop(broker).await;
}

/// With no fault, a broker stopped with messages in memory, in pages and
/// in large message files, and freed, leaves them all to the next one.
#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_broker_leaves_its_messages() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, address) = start(dir.path()).await;
    let (conn, s) = session(&address, SessionOptions::default()).await;
    let mut sent = Vec::new();
    for (queue_name, size) in [("orders", 100), ("paged", 1024), ("orders", 200 * 1024)] {
        queue_if_missing(&s, queue_name).await;
        let producer = s.create_producer(Some(queue_name)).await.unwrap();
        for n in 0..30 {
            let n = sent.len() as i32 * 1000 + n;
            producer.send(numbered(n, &vec![1; size])).await.unwrap();
        }
        sent.push(queue_name);
    }
    drop(conn);
    stop(broker).await;

    let (broker, address) = start(dir.path()).await;
    assert_eq!(
        drain(&address, "orders").await,
        (0..30).chain(2000..2030).collect::<Vec<_>>()
    );
    assert_eq!(
        drain(&address, "paged").await,
        (1000..1030).collect::<Vec<_>>()
    );
    stop(broker).await;
}

async fn queue_if_missing(session: &Session, name: &str) {
    let r = session
        .create_queue(
            QueueConfiguration::new(name)
                .routing_type(RoutingType::Anycast)
                .durable(true),
        )
        .await;
    if let Err(e) = r {
        assert!(e.to_string().contains("AMQ229019"), "{e}");
    }
}
