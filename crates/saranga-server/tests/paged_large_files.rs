//! The files of large messages that were paged: a non-durable one is read
//! back from its `.tmp` file, and a page deleted does not take the file of a
//! message another queue still holds.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_config as cfg;
use saranga_message::CoreMessage;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &Path, settings: &str) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
             <address-settings>{settings}</address-settings>
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "s".into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

fn files(root: &Path, ext: &str) -> Vec<PathBuf> {
    fn walk(p: &Path, ext: &str, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(p).into_iter().flatten().flatten() {
            let path = e.path();
            if path.is_dir() {
                walk(&path, ext, out);
            } else if path.extension().is_some_and(|x| x == ext) {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, ext, &mut out);
    out
}

fn queue(broker: &Arc<Broker>, name: &str, address: &str) {
    let mut spec = QueueSpec::new(name, address);
    spec.auto_create_address = Some(true);
    spec.durable = true;
    spec.routing_type = Some(saranga_server::RoutingType::Multicast);
    broker.post_office().create_queue(spec, false).unwrap();
}

#[derive(Default)]
struct Sink {
    ids: Mutex<Vec<i64>>,
    failed: AtomicBool,
}

impl DeliverySink for Sink {
    fn deliver(&self, _: i64, message: &Arc<ServerMessage>, _: i32) -> i64 {
        self.ids.lock().unwrap().push(message.message_id());
        1
    }
    fn deliver_large(&self, _: i64, message: &ServerMessage, _: i64, _: i32) -> i64 {
        self.ids.lock().unwrap().push(message.message_id());
        1
    }
    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        0
    }
    fn chunk_size(&self) -> usize {
        64 * 1024
    }
    fn disconnect(&self, _: i64, _: &str) {
        self.failed.store(true, Ordering::SeqCst);
    }
}

/// Non-durable large messages paged past the page cache are read back from
/// their `.tmp` files (`LargeBody` picks the file by `isDurable`), and their
/// files go with the pages once delivered.
#[tokio::test]
async fn a_non_durable_paged_large_message_is_read_from_its_tmp_file() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(
        dir.path(),
        r#"<address-setting match="q">
             <max-size-bytes>1</max-size-bytes>
             <page-size-bytes>50</page-size-bytes>
             <max-read-page-messages>1</max-read-page-messages>
             <address-full-policy>PAGE</address-full-policy>
           </address-setting>"#,
    )
    .await;
    queue(&broker, "q", "q");
    let mut s = session(&broker);
    for i in 0..9u8 {
        s.send(
            CoreMessage {
                address: Some("q".into()),
                durable: false,
                body: vec![i; 70_000].into(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    }
    assert_eq!(files(dir.path(), "tmp").len(), 9);
    let sink = Arc::new(Sink::default());
    let consumer = sink.clone();
    s.create_consumer(1, "q", None, 0, false, move |_| consumer.clone())
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;
    let mut acked = 0;
    for _ in 0..200 {
        let next = sink.ids.lock().unwrap().get(acked).copied();
        match next {
            Some(id) => {
                s.acknowledge(1, id, true).await.unwrap();
                acked += 1;
            }
            None if acked >= 8 => break,
            None => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    assert!(
        !sink.failed.load(Ordering::SeqCst),
        "a depaged body was looked for as a .msg file"
    );
    assert!(acked >= 8, "delivered {acked}");
    broker.post_office().paging_store("q").cleanup();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let delivered = sink.ids.lock().unwrap().clone();
    let left: Vec<_> = files(dir.path(), "tmp")
        .into_iter()
        .filter(|p| {
            let id: i64 = p.file_stem().unwrap().to_str().unwrap().parse().unwrap();
            delivered.contains(&id)
        })
        .collect();
    assert!(left.is_empty(), "delivered bodies left: {left:?}");
    broker.stop().unwrap();
}

/// A large message routed both to a paging address's queue and to a queue
/// that does not page: the page deleted once the first queue consumed it
/// leaves the file the second still holds, which a restart reads in full.
#[tokio::test]
async fn a_deleted_page_keeps_the_file_another_queue_holds() {
    let dir = tempfile::tempdir().unwrap();
    let settings = r##"<address-setting match="orders.eu">
                          <max-size-bytes>1</max-size-bytes>
                          <page-size-bytes>256</page-size-bytes>
                          <address-full-policy>PAGE</address-full-policy>
                        </address-setting>
                        <address-setting match="orders.#">
                          <max-size-bytes>-1</max-size-bytes>
                        </address-setting>"##;
    let broker = start(dir.path(), settings).await;
    queue(&broker, "q_paged", "orders.eu");
    queue(&broker, "q_wild", "orders.#");
    let mut s = session(&broker);
    for i in 0..3u8 {
        s.send(
            CoreMessage {
                address: Some("orders.eu".into()),
                durable: true,
                body: vec![i + 1; 70_000].into(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        s.take_completion().wait().await.unwrap();
    }
    let paged = broker.post_office().queue("q_paged").unwrap();
    for _ in 0..5 {
        paged.delete_matching(None).await;
        broker.post_office().paging_store("orders.eu").cleanup();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        files(dir.path(), "msg").len(),
        3,
        "q_wild still holds all three"
    );
    broker.stop().unwrap();
    drop((paged, s, broker));

    let broker = start(dir.path(), settings).await;
    let wild = broker.post_office().queue("q_wild").unwrap();
    let sizes: Vec<u64> = wild
        .browse()
        .await
        .iter()
        .filter_map(|m| m.message.large().map(|l| l.size()))
        .collect();
    assert_eq!(sizes, vec![70_000; 3]);
    broker.stop().unwrap();
}

fn tx_session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "t".into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: false,
            auto_commit_acks: false,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

/// A transaction pages a non-durable and a durable message to a durable
/// queue. The page transaction counts both (`getNumberOfDurableQueues`), so
/// acknowledging the non-durable one does not complete it: the durable
/// message is still there after a restart.
#[tokio::test]
async fn a_non_durable_message_s_ack_leaves_its_page_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let settings = r#"<address-setting match="q">
                        <max-size-bytes>1</max-size-bytes>
                        <page-size-bytes>4096</page-size-bytes>
                        <address-full-policy>PAGE</address-full-policy>
                      </address-setting>"#;
    let broker = start(dir.path(), settings).await;
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    broker.post_office().create_queue(spec, false).unwrap();
    let send = |durable: bool, byte: u8| CoreMessage {
        address: Some("q".into()),
        durable,
        body: vec![byte; 512].into(),
        ..Default::default()
    };
    let mut s = session(&broker);
    s.send(send(true, 1), None).await.unwrap();
    s.take_completion().wait().await.unwrap();
    let mut t = tx_session(&broker);
    t.send(send(false, 2), None).await.unwrap();
    t.send(send(true, 3), None).await.unwrap();
    t.commit().unwrap();
    t.take_completion().wait().await.unwrap();

    // The first message and the non-durable one acknowledged.
    let sink = Arc::new(Sink::default());
    let consumer = sink.clone();
    s.create_consumer(1, "q", None, 0, false, move |_| consumer.clone())
        .await
        .unwrap();
    s.consumer_credits(1, 2);
    s.set_started(true).await;
    for _ in 0..300 {
        if sink.ids.lock().unwrap().len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let got = sink.ids.lock().unwrap().clone();
    assert_eq!(got.len(), 2);
    for id in got {
        s.acknowledge(1, id, true).await.unwrap();
        s.take_completion().wait().await.unwrap();
    }
    assert_eq!(broker.post_office().queue("q").unwrap().message_count(), 1);
    s.close(false).await;
    t.close(false).await;
    broker.stop().unwrap();
    drop((s, t, broker));

    let broker = start(dir.path(), settings).await;
    assert_eq!(
        broker.post_office().queue("q").unwrap().message_count(),
        1,
        "the committed durable message survives the restart"
    );
    broker.stop().unwrap();
}
