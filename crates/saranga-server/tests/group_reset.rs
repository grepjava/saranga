//! `resetGroup` forgets a group's consumer whether the queue keys its groups
//! by ID or, with `group-buckets`, by the bucket the ID hashes to, as
//! Artemis's `BucketMessageGroups.remove` hashes the ID it is given.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

struct Sink(Mutex<Vec<i64>>);

impl DeliverySink for Sink {
    fn deliver(&self, _: i64, message: &Arc<ServerMessage>, _: i32) -> i64 {
        self.0.lock().unwrap().push(message.message_id());
        1
    }
    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        unreachable!()
    }
    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        unreachable!()
    }
    fn chunk_size(&self) -> usize {
        100 * 1024
    }
}

fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("groups"),
        Arc::new(Default::default()),
        vec![],
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

async fn wait_for(what: impl Fn() -> bool) {
    for _ in 0..200 {
        if what() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out");
}

async fn reset_forgets_the_group(group_buckets: i32) {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "a");
    spec.auto_create_address = Some(true);
    spec.group_buckets = Some(group_buckets);
    let q = broker.post_office().create_queue(spec, false).unwrap();

    let sink = Arc::new(Sink(Mutex::new(Vec::new())));
    let consumer_sink = sink.clone();
    let mut s = session(&broker);
    s.create_consumer(1, "q", None, 0, false, move |_| consumer_sink.clone())
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;

    let mut m = CoreMessage {
        message_id: broker.post_office().ids().next(),
        address: Some(SimpleString::from("a")),
        ..Default::default()
    };
    m.properties.insert(
        SimpleString::from("_AMQ_GROUP_ID"),
        PropertyValue::String(SimpleString::from("g")),
    );
    let id = m.message_id;
    s.send_with_id(m).await.unwrap();
    wait_for(|| sink.0.lock().unwrap().contains(&id)).await;
    assert_eq!(q.groups().await.len(), 1, "the group is pinned");

    q.reset_groups(Some("g".into())).await;
    assert!(
        q.groups().await.is_empty(),
        "group-buckets {group_buckets}: resetGroup must forget the group's consumer"
    );
    s.close(false).await;
    broker.stop().unwrap();
}

#[tokio::test]
async fn reset_group_forgets_a_bucketed_group() {
    reset_forgets_the_group(8).await;
}

#[tokio::test]
async fn reset_group_forgets_a_group_kept_by_id() {
    reset_forgets_the_group(-1).await;
}
