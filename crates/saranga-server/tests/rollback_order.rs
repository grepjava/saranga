//! References coming back from a rollback go back in queue order (the order
//! they were added to the queue in), not in message ID order: IDs are given
//! at send, and transactions can commit in another order.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

/// Records the message IDs delivered, in order.
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

fn session(broker: &Arc<Broker>, name: &str, transacted: bool) -> ServerSession {
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
            xa: false,
            auto_commit_sends: !transacted,
            auto_commit_acks: !transacted,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

async fn delivered(seen: &Mutex<Vec<i64>>, n: usize) -> Vec<i64> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let v = seen.lock().unwrap().clone();
            if v.len() >= n {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the deliveries")
}

#[tokio::test]
async fn a_rollback_keeps_the_queue_order() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    broker.post_office().create_queue(spec, false).unwrap();
    // Sent in one order, committed in the other.
    let mut first = session(&broker, "first", true);
    let mut second = session(&broker, "second", true);
    for s in [&mut first, &mut second] {
        s.send(
            saranga_message::CoreMessage {
                address: Some("q".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    }
    second.commit().unwrap();
    first.commit().unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut c = session(&broker, "consumer", true);
    let record = seen.clone();
    c.create_consumer(1, "q", None, 0, false, move |_| {
        Arc::new(Record(record.clone()))
    })
    .await
    .unwrap();
    c.consumer_credits(1, -1);
    c.set_started(true).await;
    let order = delivered(&seen, 2).await;
    assert!(
        order[0] > order[1],
        "queue order is commit order: {order:?}"
    );
    seen.lock().unwrap().clear();
    c.rollback(false, false).await;
    let again = delivered(&seen, 2).await;
    assert_eq!(again, order, "redelivered in queue order");
}
