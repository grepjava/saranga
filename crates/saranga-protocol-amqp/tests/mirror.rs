//! Mirroring between in-process Saranga brokers: a broker connection's
//! `<mirror>` sends its broker's queues, messages and acknowledgements to
//! the peer, and with a mirror each way neither sends back what the other
//! sent.

use std::sync::Arc;
use std::time::Duration;

use saranga_client::message;
use saranga_client::*;
use saranga_protocol_amqp::{AmqpConfig, AmqpProtocol};
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const WAIT: Duration = Duration::from_secs(10);

struct Node {
    broker: Arc<Broker>,
    port: u16,
    acceptor: JoinHandle<std::io::Result<()>>,
}

async fn start(dir: &std::path::Path, listener: TcpListener, core: &str) -> Node {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let file = saranga_config::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <security-enabled>false</security-enabled>
             {core}
           </core></configuration>"#
    ))
    .unwrap();
    let mut config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    config
        .protocols
        .push(Arc::new(AmqpProtocol::new(AmqpConfig {
            broker_connections: file.broker_connections.clone(),
            ..AmqpConfig::default()
        })));
    let broker = Broker::start(config).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let acceptor = tokio::spawn(broker.clone().serve_acceptor(listener, Acceptor::default()));
    Node {
        broker,
        port,
        acceptor,
    }
}

async fn stop(n: Node) {
    n.acceptor.abort();
    let _ = n.acceptor.await;
    for c in n.broker.registry().connections() {
        c.disconnect();
    }
    n.broker.stop().unwrap();
}

async fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn session(port: u16) -> (Connection, Session) {
    let conn = Connection::connect(("127.0.0.1", port), ConnectionOptions::default())
        .await
        .unwrap();
    let session = conn
        .create_session(SessionOptions {
            ack_batch_size: 0,
            ..SessionOptions::default()
        })
        .await
        .unwrap();
    session.start().await.unwrap();
    (conn, session)
}

async fn send(session: &Session, address: &str, from: u32, n: u32) {
    let producer = session.create_producer(Some(address)).await.unwrap();
    for i in from..from + n {
        producer
            .send(message::text_message(&format!("m{i}")))
            .await
            .unwrap();
    }
    producer.close();
}

async fn take(consumer: &Consumer, n: usize) -> Vec<String> {
    let mut out = Vec::new();
    while out.len() < n {
        let d = consumer
            .receive(WAIT)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("only {} of {n} messages", out.len()));
        consumer.acknowledge(&d).await.unwrap();
        out.push(d.message.text().unwrap().unwrap());
    }
    out
}

fn count(n: &Node, queue: &str) -> Option<i64> {
    n.broker
        .post_office()
        .queue(queue)
        .map(|q| q.message_count())
}

fn connected(n: &Node, name: &str) -> bool {
    n.broker
        .broker_connection_registry()
        .get(name)
        .is_some_and(|c| c.is_connected())
}

fn mirror_to(port: u16, name: &str, sync: bool) -> String {
    format!(
        r#"<broker-connections>
             <amqp-connection uri="tcp://127.0.0.1:{port}" name="{name}" retry-interval="100">
               <mirror sync="{sync}"/>
             </amqp-connection>
           </broker-connections>"#
    )
}

fn dirs() -> (tempfile::TempDir, tempfile::TempDir) {
    (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap())
}

/// The queue made, its messages sent and acknowledged and the queue
/// deleted on one broker happen on its mirror too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mirrors_queues_messages_and_acks() {
    let d = dirs();
    let lb = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let la = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let pb = lb.local_addr().unwrap().port();
    let b = start(d.1.path(), lb, "").await;
    let a = start(d.0.path(), la, &mirror_to(pb, "to-b", false)).await;
    wait_until("connected", || connected(&a, "to-b")).await;
    let (_ca, sa) = session(a.port).await;
    sa.create_queue(
        QueueConfiguration::new("orders")
            .address("orders")
            .routing_type(RoutingType::Anycast),
    )
    .await
    .unwrap();
    wait_until("the queue mirrored", || count(&b, "orders").is_some()).await;
    send(&sa, "orders", 0, 10).await;
    wait_until("the messages mirrored", || count(&b, "orders") == Some(10)).await;
    let consumer = sa.create_consumer("orders", None, false).await.unwrap();
    assert_eq!(take(&consumer, 4).await, ["m0", "m1", "m2", "m3"]);
    consumer.close().await;
    wait_until("the acks mirrored", || count(&b, "orders") == Some(6)).await;
    // What is left on the mirror is what is left here.
    let (_cb, sb) = session(b.port).await;
    let consumer = sb.create_consumer("orders", None, false).await.unwrap();
    assert_eq!(
        take(&consumer, 6).await,
        ["m4", "m5", "m6", "m7", "m8", "m9"]
    );
    consumer.close().await;
    sa.delete_queue("orders").await.unwrap();
    wait_until("the queue removed", || count(&b, "orders").is_none()).await;
    stop(a).await;
    stop(b).await;
}

/// A large message is mirrored whole: the receiving broker streams its body to
/// a file as it arrives and reads it back for the consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mirrors_a_large_message() {
    let d = dirs();
    let lb = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let la = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let pb = lb.local_addr().unwrap().port();
    let b = start(d.1.path(), lb, "").await;
    let a = start(d.0.path(), la, &mirror_to(pb, "to-b", false)).await;
    wait_until("connected", || connected(&a, "to-b")).await;
    let (_ca, sa) = session(a.port).await;
    sa.create_queue(
        QueueConfiguration::new("big")
            .address("big")
            .routing_type(RoutingType::Anycast),
    )
    .await
    .unwrap();
    wait_until("the queue mirrored", || count(&b, "big").is_some()).await;

    // Well past the 100 KiB large-message threshold.
    let body: Vec<u8> = (0..300_000).map(|i| (i * 31 % 251) as u8).collect();
    let producer = sa.create_producer(Some("big")).await.unwrap();
    producer.send(message::bytes_message(&body)).await.unwrap();
    wait_until("the message mirrored", || count(&b, "big") == Some(1)).await;

    // B spilled the body to a file as it arrived; reading it back gives the
    // whole message.
    let (_cb, sb) = session(b.port).await;
    let consumer = sb.create_consumer("big", None, false).await.unwrap();
    let got = consumer
        .receive(WAIT)
        .await
        .unwrap()
        .expect("a message within the timeout");
    assert_eq!(got.message.body.len(), body.len(), "the whole body arrived");
    assert!(
        got.message.body == body,
        "the body round-trips through the mirror unchanged"
    );
    consumer.acknowledge(&got).await.unwrap();
    consumer.close().await;
    stop(a).await;
    stop(b).await;
}

/// With a mirror each way, what each sends reaches the other once, and an
/// acknowledgement on either removes the message from both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dual_mirrors_do_not_loop() {
    let d = dirs();
    let la = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let lb = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let (pa, pb) = (
        la.local_addr().unwrap().port(),
        lb.local_addr().unwrap().port(),
    );
    let queue = r#"<addresses>
        <address name="jobs"><anycast><queue name="jobs"/></anycast></address>
      </addresses>"#;
    let a = start(
        d.0.path(),
        la,
        &format!("{queue}{}", mirror_to(pb, "to-b", false)),
    )
    .await;
    let b = start(
        d.1.path(),
        lb,
        &format!("{queue}{}", mirror_to(pa, "to-a", false)),
    )
    .await;
    wait_until("connected", || {
        connected(&a, "to-b") && connected(&b, "to-a")
    })
    .await;
    let (_ca, sa) = session(a.port).await;
    let (_cb, sb) = session(b.port).await;
    send(&sa, "jobs", 0, 5).await;
    send(&sb, "jobs", 5, 5).await;
    wait_until("both have all", || {
        count(&a, "jobs") == Some(10) && count(&b, "jobs") == Some(10)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(count(&a, "jobs"), Some(10), "no loop back to a");
    assert_eq!(count(&b, "jobs"), Some(10), "no loop back to b");
    let consumer = sb.create_consumer("jobs", None, false).await.unwrap();
    let mut got = take(&consumer, 10).await;
    got.sort();
    assert_eq!(got.len(), 10);
    consumer.close().await;
    wait_until("acks reach a", || count(&a, "jobs") == Some(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(count(&b, "jobs"), Some(0));
    stop(a).await;
    stop(b).await;
}

/// With `sync`, a send is answered once the peer has the message.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_waits_for_the_peer() {
    let d = dirs();
    let lb = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let la = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let pb = lb.local_addr().unwrap().port();
    let queue = r#"<addresses>
        <address name="jobs"><anycast><queue name="jobs"/></anycast></address>
      </addresses>"#;
    let b = start(d.1.path(), lb, queue).await;
    let a = start(
        d.0.path(),
        la,
        &format!("{queue}{}", mirror_to(pb, "to-b", true)),
    )
    .await;
    wait_until("connected", || connected(&a, "to-b")).await;
    let (_ca, sa) = session(a.port).await;
    let producer = sa.create_producer(Some("jobs")).await.unwrap();
    for i in 0..5 {
        producer
            .send(message::text_message(&format!("m{i}")))
            .await
            .unwrap();
        assert_eq!(count(&b, "jobs"), Some(i + 1), "the peer has it already");
    }
    stop(a).await;
    stop(b).await;
}

/// With `sync`, committing a prepared XA branch through management answers
/// once the peer has what the commit sends — as Artemis's
/// `commitPreparedTransaction` waits on the operation context (where a sync
/// mirror's acknowledgement is pending) before reporting success.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committing_a_prepared_branch_waits_for_a_sync_mirror() {
    use saranga_buffer::SimpleString;
    use saranga_message::CoreMessage;
    use saranga_server::management::Json;
    use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
    use saranga_server::transaction::{Xid, xid_base64};

    const N: i64 = 200;
    let d = dirs();
    let lb = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let la = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let pb = lb.local_addr().unwrap().port();
    let queue = r#"<addresses>
        <address name="jobs"><anycast><queue name="jobs"/></anycast></address>
      </addresses>"#;
    let b = start(d.1.path(), lb, queue).await;
    let a = start(
        d.0.path(),
        la,
        &format!("{queue}{}", mirror_to(pb, "to-b", true)),
    )
    .await;
    wait_until("connected", || connected(&a, "to-b")).await;

    // An XA branch on A that sends N messages, prepared but not committed.
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(saranga_security::ConnectionInfo {
            remote_address: "test".into(),
            certificates: None,
            security_domain: None,
            connection_id: None,
            sasl_principals: None,
            sni_host: None,
        }),
        Vec::new(),
    ));
    let mut session = ServerSession::new(
        a.broker.clone(),
        conn,
        SessionOptions {
            name: "xa".into(),
            username: None,
            password: None,
            xa: true,
            auto_commit_sends: false,
            auto_commit_acks: false,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    );
    let xid = Xid {
        format_id: 7,
        branch_qualifier: b"bq".to_vec(),
        global_transaction_id: b"gtx".to_vec(),
    };
    session.xa_start(xid.clone()).unwrap();
    for _ in 0..N {
        let message = CoreMessage {
            address: Some(SimpleString::from("jobs")),
            durable: true,
            ..Default::default()
        };
        session.send(message, None).await.unwrap();
    }
    session.xa_end(xid.clone()).unwrap();
    session.xa_prepare(xid.clone()).unwrap();
    session.take_completion().wait().await.unwrap();
    assert_eq!(count(&b, "jobs"), Some(0), "nothing is committed yet");

    let committed = a
        .broker
        .management()
        .invoke(
            &a.broker,
            "broker",
            "commitPreparedTransaction",
            &[Json::from(xid_base64(&xid))],
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(committed, Json::from(true));
    assert_eq!(
        count(&b, "jobs"),
        Some(N),
        "a sync mirror's peer has every committed message when the call returns"
    );
    drop(session);
    stop(a).await;
    stop(b).await;
}

/// A management move answers once a `sync` mirror's peer has the moved
/// copies and the originals' acknowledgements, as a client's send and
/// acknowledgement do, not while they are still on their way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_management_move_waits_for_a_sync_mirror() {
    use saranga_server::management::Json;

    const N: u32 = 200;
    let d = dirs();
    let lb = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let la = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let pb = lb.local_addr().unwrap().port();
    let queues = r#"<addresses>
        <address name="jobs"><anycast><queue name="jobs"/></anycast></address>
        <address name="done"><anycast><queue name="done"/></anycast></address>
      </addresses>"#;
    let b = start(d.1.path(), lb, queues).await;
    let a = start(
        d.0.path(),
        la,
        &format!("{queues}{}", mirror_to(pb, "to-b", true)),
    )
    .await;
    wait_until("connected", || connected(&a, "to-b")).await;
    let (_ca, sa) = session(a.port).await;
    send(&sa, "jobs", 0, N).await;
    wait_until("the messages mirrored", || {
        count(&b, "jobs") == Some(i64::from(N))
    })
    .await;

    let moved = a
        .broker
        .management()
        .invoke(
            &a.broker,
            "queue.jobs",
            "moveMessages",
            &[Json::Null, Json::from("done")],
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(moved.as_i64(), Some(i64::from(N)), "{moved:?}");
    assert_eq!(
        (count(&b, "done"), count(&b, "jobs")),
        (Some(i64::from(N)), Some(0)),
        "a move answered as done must have reached the sync mirror's peer"
    );
    drop(sa);
    stop(a).await;
    stop(b).await;
}

/// A management copy reaches a mirror's peer, as a copy routed through the
/// post office does in Artemis, and with a `sync` mirror the operation
/// answers once the peer has it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_management_copy_is_mirrored() {
    use saranga_server::management::Json;

    let d = dirs();
    let lb = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let la = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let pb = lb.local_addr().unwrap().port();
    let queues = r#"<addresses>
        <address name="jobs"><anycast><queue name="jobs"/></anycast></address>
        <address name="done"><anycast><queue name="done"/></anycast></address>
      </addresses>"#;
    let b = start(d.1.path(), lb, queues).await;
    let a = start(
        d.0.path(),
        la,
        &format!("{queues}{}", mirror_to(pb, "to-b", true)),
    )
    .await;
    wait_until("connected", || connected(&a, "to-b")).await;
    let (_ca, sa) = session(a.port).await;
    send(&sa, "jobs", 0, 1).await;
    wait_until("the message mirrored", || count(&b, "jobs") == Some(1)).await;
    let id = a.broker.post_office().queue("jobs").unwrap().browse().await[0].message_id();

    let copied = a
        .broker
        .management()
        .invoke(
            &a.broker,
            "queue.jobs",
            "copyMessage",
            &[Json::Long(id), Json::from("done")],
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(copied, Json::from(true));
    assert_eq!(count(&a, "done"), Some(1));
    assert_eq!(
        (count(&b, "done"), count(&b, "jobs")),
        (Some(1), Some(1)),
        "the copy must reach the sync mirror's peer by the time the call returns"
    );
    drop(sa);
    stop(a).await;
    stop(b).await;
}
