//! A delivery's count across a restart: journaled as it is delivered with
//! `persist-delivery-count-before-delivery`, and counted for a message
//! acknowledged in a prepared XA branch that is rolled back after the
//! restart.

use std::path::Path;
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

async fn start(dir: &Path, strict: bool) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <persist-delivery-count-before-delivery>{strict}</persist-delivery-count-before-delivery>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    let b = Broker::start(config).await.unwrap();
    if b.post_office().queue("q").is_none() {
        let mut spec = QueueSpec::new("q", "q");
        spec.auto_create_address = Some(true);
        spec.durable = true;
        b.post_office().create_queue(spec, false).unwrap();
    }
    b
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

/// The delivery counts it is given.
#[derive(Default)]
struct Sink(Mutex<Vec<(i64, i32)>>);

impl DeliverySink for Sink {
    fn deliver(&self, _: i64, message: &Arc<ServerMessage>, count: i32) -> i64 {
        self.0.lock().unwrap().push((message.message_id(), count));
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

async fn send(broker: &Arc<Broker>) {
    let mut s = session(broker, false);
    s.send(
        CoreMessage {
            address: Some("q".into()),
            durable: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    s.take_completion().wait().await.unwrap();
    s.close(false).await;
}

async fn consume(s: &mut ServerSession) -> (Arc<Sink>, (i64, i32)) {
    let sink = Arc::new(Sink::default());
    let c = sink.clone();
    s.create_consumer(1, "q", None, 0, false, move |_| c.clone())
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;
    for _ in 0..500 {
        if let Some(d) = sink.0.lock().unwrap().first().copied() {
            return (sink.clone(), d);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("nothing delivered");
}

/// The count a delivered message comes back with after a restart that
/// left it unacknowledged.
async fn count_after_restart(strict: bool) -> i32 {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), strict).await;
    send(&broker).await;
    let mut s = session(&broker, false);
    let (_sink, (_, count)) = consume(&mut s).await;
    assert_eq!(count, 1);
    broker.stop().unwrap();
    drop((s, broker));

    let broker = start(dir.path(), strict).await;
    let mut s = session(&broker, false);
    let (_sink, (_, count)) = consume(&mut s).await;
    s.close(false).await;
    broker.stop().unwrap();
    count
}

#[tokio::test]
async fn a_delivery_is_journaled_before_it_goes_when_asked() {
    assert_eq!(count_after_restart(true).await, 2);
}

#[tokio::test]
async fn a_delivery_is_not_journaled_before_it_goes_by_default() {
    assert_eq!(count_after_restart(false).await, 1);
}

fn xid() -> Xid {
    Xid {
        format_id: 1,
        branch_qualifier: b"b".to_vec(),
        global_transaction_id: b"g".to_vec(),
    }
}

/// A message acknowledged in a prepared branch has had that delivery
/// when the branch is rolled back after a restart, whether or not its
/// count was journaled as it went.
async fn rolled_back_count(strict: bool) -> i32 {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), strict).await;
    send(&broker).await;
    let mut x = session(&broker, true);
    x.xa_start(xid()).unwrap();
    let (_sink, (id, count)) = consume(&mut x).await;
    assert_eq!(count, 1);
    x.acknowledge(1, id, true).await.unwrap();
    x.xa_end(xid()).unwrap();
    x.xa_prepare(xid()).unwrap();
    x.take_completion().wait().await.unwrap();
    broker.stop().unwrap();
    drop((x, broker));

    let broker = start(dir.path(), strict).await;
    let mut x = session(&broker, true);
    x.xa_rollback(xid()).await.unwrap();
    x.take_completion().wait().await.unwrap();
    let mut s = session(&broker, false);
    let (_sink, (_, count)) = consume(&mut s).await;
    s.close(false).await;
    broker.stop().unwrap();
    count
}

#[tokio::test]
async fn a_rolled_back_prepared_acknowledgement_counts_its_delivery() {
    assert_eq!(rolled_back_count(false).await, 2);
}

#[tokio::test]
async fn a_rolled_back_prepared_acknowledgement_counts_its_delivery_once() {
    assert_eq!(rolled_back_count(true).await, 2);
}
