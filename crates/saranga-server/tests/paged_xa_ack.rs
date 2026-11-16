//! A paged message acknowledged in a prepared XA transaction, across a
//! restart: its page is kept until the transaction ends, a rollback gives
//! the message back, and a commit acknowledges it for good.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_config as cfg;
use saranga_message::CoreMessage;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::transaction::Xid;
use saranga_server::{Broker, BrokerConfig};

const SETTINGS: &str = r#"<address-setting match="q">
    <max-size-bytes>1</max-size-bytes>
    <page-size-bytes>4096</page-size-bytes>
    <address-full-policy>PAGE</address-full-policy>
  </address-setting>"#;

async fn start(dir: &Path) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
             <address-settings>{SETTINGS}</address-settings>
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

fn session(broker: &Arc<Broker>, xa: bool) -> ServerSession {
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
            xa,
            auto_commit_sends: !xa,
            auto_commit_acks: !xa,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

fn pages(root: &Path) -> Vec<PathBuf> {
    fn walk(p: &Path, out: &mut Vec<PathBuf>) {
        for e in std::fs::read_dir(p).into_iter().flatten().flatten() {
            let path = e.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|x| x == "page") {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, &mut out);
    out
}

#[derive(Default)]
struct Sink(Mutex<Vec<i64>>);

impl DeliverySink for Sink {
    fn deliver(&self, _: i64, message: &Arc<ServerMessage>, _: i32) -> i64 {
        self.0.lock().unwrap().push(message.message_id());
        1
    }
    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        1
    }
    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        0
    }
    fn chunk_size(&self) -> usize {
        64 * 1024
    }
}

async fn received(sink: &Sink, n: usize) -> Vec<i64> {
    for _ in 0..500 {
        if sink.0.lock().unwrap().len() >= n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    sink.0.lock().unwrap().clone()
}

fn xid() -> Xid {
    Xid {
        format_id: 1,
        branch_qualifier: b"b1".to_vec(),
        global_transaction_id: b"g1".to_vec(),
    }
}

/// Two messages, the second paged; the first acknowledged, the second
/// acknowledged in an XA transaction that is prepared, then the broker
/// stops.
async fn prepare_a_paged_ack(dir: &Path) {
    let broker = start(dir).await;
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    broker.post_office().create_queue(spec, false).unwrap();
    let mut s = session(&broker, false);
    for i in 0..2u8 {
        s.send(
            CoreMessage {
                address: Some("q".into()),
                durable: true,
                body: vec![i; 512].into(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        s.take_completion().wait().await.unwrap();
    }
    let sink = Arc::new(Sink::default());
    let c = sink.clone();
    s.create_consumer(1, "q", None, 0, false, move |_| c.clone())
        .await
        .unwrap();
    s.consumer_credits(1, 1);
    s.set_started(true).await;
    let first = received(&sink, 1).await[0];
    s.acknowledge(1, first, true).await.unwrap();
    s.take_completion().wait().await.unwrap();
    s.close(false).await;

    let mut x = session(&broker, true);
    x.xa_start(xid()).unwrap();
    let sink = Arc::new(Sink::default());
    let c = sink.clone();
    x.create_consumer(1, "q", None, 0, false, move |_| c.clone())
        .await
        .unwrap();
    x.consumer_credits(1, -1);
    x.set_started(true).await;
    let second = received(&sink, 1).await[0];
    x.acknowledge(1, second, true).await.unwrap();
    x.xa_end(xid()).unwrap();
    x.xa_prepare(xid()).unwrap();
    x.take_completion().wait().await.unwrap();
    x.close(false).await;
    broker.stop().unwrap();
}

#[tokio::test]
async fn a_rolled_back_prepared_paged_ack_gives_the_message_back() {
    let dir = tempfile::tempdir().unwrap();
    prepare_a_paged_ack(dir.path()).await;
    assert!(!pages(dir.path()).is_empty());

    let broker = start(dir.path()).await;
    assert!(
        !pages(dir.path()).is_empty(),
        "the page went while the transaction was prepared"
    );
    let mut x = session(&broker, true);
    x.xa_rollback(xid()).await.unwrap();
    x.take_completion().wait().await.unwrap();
    assert_eq!(broker.post_office().queue("q").unwrap().message_count(), 1);
    broker.stop().unwrap();
    drop((x, broker));

    let broker = start(dir.path()).await;
    assert_eq!(
        broker.post_office().queue("q").unwrap().message_count(),
        1,
        "the rolled back message is still there after a restart"
    );
    broker.stop().unwrap();
}

#[tokio::test]
async fn a_committed_prepared_paged_ack_acknowledges_for_good() {
    let dir = tempfile::tempdir().unwrap();
    prepare_a_paged_ack(dir.path()).await;

    let broker = start(dir.path()).await;
    let mut x = session(&broker, true);
    x.xa_commit(xid(), false).unwrap();
    x.take_completion().wait().await.unwrap();
    assert_eq!(broker.post_office().queue("q").unwrap().message_count(), 0);
    broker.stop().unwrap();
    drop((x, broker));

    let broker = start(dir.path()).await;
    assert_eq!(broker.post_office().queue("q").unwrap().message_count(), 0);
    broker.stop().unwrap();
}
