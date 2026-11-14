//! An internal queue (a mirror's or a cluster's store-and-forward queue)
//! holds what must reach a peer, in the order it came: as in Artemis it has
//! no dead-letter or redelivery-delay check, no priorities and no groups.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::{DeliverySink, Queue};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

async fn broker() -> Arc<Broker> {
    Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap()
}

fn queue(b: &Broker, name: &str, internal: bool) -> Queue {
    let mut spec = QueueSpec::new(name, name);
    spec.auto_create_address = Some(true);
    spec.internal = internal;
    spec.group_buckets = Some(-1);
    b.post_office().create_queue(spec, false).unwrap()
}

fn message(id: i64, address: &str) -> CoreMessage {
    CoreMessage {
        message_id: id,
        address: Some(SimpleString::from(address)),
        ..Default::default()
    }
}

/// A reference coming back that has had its delivery attempts stays on an
/// internal queue; on another it goes (to the dead-letter address, which
/// here is none).
#[tokio::test]
async fn an_internal_queue_has_no_delivery_limit() {
    let b = broker().await;
    for (name, internal, left) in [("sf", true, 1), ("plain", false, 0)] {
        let q = queue(&b, name, internal);
        let mut r = MessageRef::new(ServerMessage::new(message(1, name)));
        r.delivery_count = 10;
        q.reinsert(vec![r]);
        assert_eq!(q.browse().await.len(), left, "{name}");
    }
}

/// An internal queue delivers in the order messages came, whatever their
/// priority.
#[tokio::test]
async fn an_internal_queue_keeps_its_order_whatever_the_priority() {
    let b = broker().await;
    for (name, internal, order) in [("sf", true, [1, 2]), ("plain", false, [2, 1])] {
        let q = queue(&b, name, internal);
        for (id, priority) in [(1, 0), (2, 9)] {
            q.add(MessageRef::new(ServerMessage::new(CoreMessage {
                priority,
                ..message(id, name)
            })));
        }
        let ids: Vec<i64> = q.browse().await.iter().map(|r| r.message_id()).collect();
        assert_eq!(ids, order, "{name}");
    }
}

/// Records which consumer got each message.
struct Record(i64, Arc<Mutex<Vec<i64>>>);

impl DeliverySink for Record {
    fn deliver(&self, _: i64, _: &Arc<ServerMessage>, _: i32) -> i64 {
        self.1.lock().unwrap().push(self.0);
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

/// An internal queue has no message groups: messages of one group go round
/// the consumers as any others, where another queue pins the group to one.
#[tokio::test]
async fn an_internal_queue_has_no_groups() {
    let b = broker().await;
    for (name, internal) in [("sf", true), ("plain", false)] {
        let q = queue(&b, name, internal);
        let conn = Arc::new(ClientConnection::new(
            Transport::internal("test"),
            Arc::default(),
            vec![],
        ));
        let mut s = ServerSession::new(
            b.clone(),
            conn,
            SessionOptions {
                name: name.into(),
                username: None,
                password: None,
                xa: false,
                auto_commit_sends: true,
                auto_commit_acks: true,
                pre_acknowledge: false,
                default_address: None,
            },
            Default::default(),
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        for c in 1..=2 {
            let seen = seen.clone();
            s.create_consumer(c, name, None, 0, false, move |_| {
                Arc::new(Record(c, seen.clone()))
            })
            .await
            .unwrap();
            s.consumer_credits(c, -1);
        }
        s.set_started(true).await;
        for id in 1..=4 {
            let mut m = message(id, name);
            m.properties.insert(
                SimpleString::from("_AMQ_GROUP_ID"),
                PropertyValue::String(SimpleString::from("g")),
            );
            q.add(MessageRef::new(ServerMessage::new(m)));
        }
        let got = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let v = seen.lock().unwrap().clone();
                if v.len() == 4 {
                    return v;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("four deliveries");
        let consumers = {
            let mut c = got.clone();
            c.sort();
            c.dedup();
            c.len()
        };
        assert_eq!(consumers, if internal { 2 } else { 1 }, "{name}: {got:?}");
        s.close(false).await;
    }
}
