//! The reaper deletes a queue as `destroyQueue` with `checkMessageCount`
//! does: a queue holding more than its auto-delete message count when it
//! is deleted (messages came since the reaper looked) stays.

use std::sync::Arc;

use saranga_message::CoreMessage;
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::{Broker, BrokerConfig};

#[tokio::test]
async fn an_auto_delete_keeps_a_queue_with_too_many_messages() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let po = broker.post_office();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_delete = Some(true);
    spec.auto_delete_message_count = Some(0);
    let q = po.create_queue(spec, false).unwrap();
    po.route_to_queue(
        MessageRef::new(ServerMessage::new(CoreMessage::default())),
        &q,
    );
    assert_eq!(q.message_count(), 1);
    let e = po.auto_delete_queue("q").unwrap_err();
    assert!(e.to_string().contains("AMQ229215"), "{e}");
    assert!(po.queue("q").is_some());
    // An operator's delete is not checked.
    po.delete_queue("q", true, false).unwrap();
    assert!(po.queue("q").is_none());
}
