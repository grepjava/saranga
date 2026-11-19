//! Work given a session's XA branch after another session committed it.
//! A send goes on its own, as Artemis's `addOperation` on a committed
//! transaction runs `afterCommit` at once: delivered, not counted forever
//! in a branch that never commits again. An acknowledgement gives the
//! message back to its queue rather than leaving it neither acknowledged
//! nor delivered again (where Artemis leaves it).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::transaction::Xid;
use saranga_server::{Broker, BrokerConfig, RoutingType};

fn session(broker: &Arc<Broker>, name: &str, xa: bool) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal(name),
        Arc::new(Default::default()),
        vec![],
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: name.into(),
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

fn xid() -> Xid {
    Xid {
        format_id: 1,
        branch_qualifier: b"t".to_vec(),
        global_transaction_id: b"t".to_vec(),
    }
}

fn message() -> saranga_message::CoreMessage {
    saranga_message::CoreMessage {
        address: Some("q".into()),
        ..Default::default()
    }
}

async fn broker() -> Arc<Broker> {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.routing_type = Some(RoutingType::Anycast);
    broker.post_office().create_queue(spec, false).unwrap();
    broker
}

struct Record(Arc<Mutex<Vec<i64>>>);

impl DeliverySink for Record {
    fn deliver(&self, _: i64, m: &Arc<ServerMessage>, _: i32) -> i64 {
        self.0.lock().unwrap().push(m.message_id());
        1
    }
    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        1
    }
    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        1
    }
    fn chunk_size(&self) -> usize {
        1024
    }
}

#[tokio::test]
async fn a_send_after_another_session_committed_the_branch_is_delivered() {
    let broker = broker().await;
    let mut a = session(&broker, "a", true);
    let mut c = session(&broker, "c", true);
    a.xa_start(xid()).unwrap();
    c.xa_commit(xid(), true).unwrap();
    a.send(message(), None).await.unwrap();
    let _ = a.xa_end(xid());
    a.close(false).await;
    c.close(false).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let po = broker.post_office();
    assert_eq!(
        po.queue("q").unwrap().message_count(),
        1,
        "the send was lost"
    );
    po.delete_queue("q", false, false).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        po.paging_store("q").address_size(),
        0,
        "the send stayed counted"
    );
    broker.stop().unwrap();
}

#[tokio::test]
async fn an_ack_after_another_session_committed_the_branch_gives_the_message_back() {
    let broker = broker().await;
    let mut p = session(&broker, "p", false);
    p.send(message(), None).await.unwrap();
    let mut a = session(&broker, "a", true);
    let mut c = session(&broker, "c", true);
    a.xa_start(xid()).unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let record = seen.clone();
    a.create_consumer(1, "q", None, 0, false, move |_| {
        Arc::new(Record(record.clone()))
    })
    .await
    .unwrap();
    a.consumer_credits(1, -1);
    a.set_started(true).await;
    let id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(id) = seen.lock().unwrap().first() {
                return *id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("delivered");
    c.xa_commit(xid(), true).unwrap();
    a.acknowledge(1, id, true).await.unwrap();
    let _ = a.xa_end(xid());
    // Back in the queue, delivered again.
    let again = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if seen.lock().unwrap().len() > 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        again.is_ok(),
        "the message is neither acknowledged nor delivered again"
    );
    assert_eq!(broker.post_office().queue("q").unwrap().message_count(), 1);
    a.close(false).await;
    c.close(false).await;
    broker.stop().unwrap();
}
