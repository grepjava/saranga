//! Changing a queued message's priority must not leak its address-memory
//! charge. Artemis changes the message in place; Saranga rebuilds the
//! (immutable) message, so the reference's `addSize` charge has to move to the
//! new message or it is never taken back.

use std::sync::Arc;

use saranga_buffer::SimpleString;
use saranga_message::CoreMessage;
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::{AckReason, Selection, SelectionAction};
use saranga_server::{Broker, BrokerConfig};

fn message(id: i64) -> MessageRef {
    MessageRef::new(ServerMessage::new(CoreMessage {
        message_id: id,
        address: Some(SimpleString::from("a")),
        priority: 4,
        ..Default::default()
    }))
}

#[tokio::test]
async fn changing_priority_then_deleting_frees_the_address_memory() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "a");
    spec.auto_create_address = Some(true);
    spec.durable = false;
    let q = broker.post_office().create_queue(spec, false).unwrap();

    let base = broker.post_office().paging().global_size();
    q.add(message(1));
    let charged = broker.post_office().paging().global_size();
    assert!(
        charged > base,
        "a queued message should charge its address memory"
    );

    // Raise the priority (the buggy path rebuilt the message and dropped the
    // reference's charge).
    let hits = q
        .act(Selection::filter(None), SelectionAction::ChangePriority(9))
        .await;
    assert_eq!(hits, 1, "the one message is reprioritised");

    // Delete it: its charge must be released.
    q.act(
        Selection::filter(None),
        SelectionAction::Remove(AckReason::Normal),
    )
    .await;

    assert_eq!(
        broker.post_office().paging().global_size(),
        base,
        "deleting a reprioritised message must free its address memory"
    );
    broker.stop().unwrap();
}
