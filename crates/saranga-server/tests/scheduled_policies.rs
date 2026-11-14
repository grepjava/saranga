//! A scheduled message that falls due is put on its queue under the queue's
//! policies, as Artemis's `addHead(refs, true)` does: on a last-value queue
//! it replaces the queued message with its key, and a full ring queue drops
//! it.

use std::sync::Arc;
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::Queue;
use saranga_server::{Broker, BrokerConfig, RoutingType};

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Message `id`, with last-value key `k`, due at `scheduled` (0 for now).
fn message(id: i64, scheduled: i64) -> MessageRef {
    let mut core = CoreMessage {
        message_id: id,
        address: Some("q".into()),
        ..Default::default()
    };
    core.properties.insert(
        SimpleString::from("_AMQ_LVQ_NAME"),
        PropertyValue::String(SimpleString::from("k")),
    );
    let mut r = MessageRef::new(ServerMessage::new(core));
    r.scheduled_time = scheduled;
    r
}

async fn queue(f: impl FnOnce(&mut QueueSpec)) -> (Arc<Broker>, Queue) {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.routing_type = Some(RoutingType::Anycast);
    f(&mut spec);
    let q = broker.post_office().create_queue(spec, false).unwrap();
    (broker, q)
}

/// The message count once the scheduled message, due in 200 ms, is due
/// and handled.
async fn settled_count(q: &Queue) -> i64 {
    tokio::time::sleep(Duration::from_millis(800)).await;
    q.message_count()
}

#[tokio::test]
async fn a_due_message_replaces_the_last_value_with_its_key() {
    let (broker, q) = queue(|s| s.last_value = Some(true)).await;
    q.add(message(1, 0));
    q.add(message(2, now_millis() + 200));
    assert_eq!(settled_count(&q).await, 1, "one value for the key");
    broker.stop().unwrap();
}

#[tokio::test]
async fn a_due_message_is_dropped_by_a_full_ring() {
    let (broker, q) = queue(|s| s.ring_size = Some(1)).await;
    q.add(message(1, 0));
    q.add(message(2, now_millis() + 200));
    assert_eq!(settled_count(&q).await, 1, "the ring holds one");
    broker.stop().unwrap();
}
