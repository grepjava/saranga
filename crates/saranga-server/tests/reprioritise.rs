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

/// Changing a durable message's priority must keep its durable journal
/// reference. Artemis mutates the message in place and keeps its
/// `RefCountMessage` state; Saranga rebuilds the message, so the reference's
/// durable count has to move to the rebuilt message. Otherwise the rebuilt
/// message starts at zero and its final `durableDown` returns -1 instead of 0,
/// so the journal record of the message is never deleted.
#[tokio::test]
async fn changing_priority_keeps_the_durable_journal_reference() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "a");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    let q = broker.post_office().create_queue(spec, false).unwrap();

    let msg = ServerMessage::new(CoreMessage {
        message_id: 1,
        address: Some(SimpleString::from("a")),
        durable: true,
        priority: 4,
        ..Default::default()
    });
    // Routing a durable message to a durable queue takes a durable journal
    // reference (`processRoute` -> `durableUp`).
    assert_eq!(msg.durable_up(), 1, "routed durable reference");
    q.add(MessageRef::new(msg));

    q.act(Selection::filter(None), SelectionAction::ChangePriority(9))
        .await;

    // The rebuilt message must carry the durable reference: its first
    // `durableDown` returns 0 (the journal record is then deleted), not -1.
    let refs = q.browse().await;
    assert_eq!(refs.len(), 1, "the message is still queued");
    assert_eq!(
        refs[0].message.durable_down(),
        0,
        "the reprioritised durable message keeps its journal reference"
    );
    broker.stop().unwrap();
}
