//! `isExpiryDisabled`: neither the expiry scan nor management's expiry
//! touches a queue on its own expiry address or a mirror's
//! store-and-forward queue.

use std::sync::Arc;

use saranga_message::CoreMessage;
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::{Queue, Selection, SelectionAction};
use saranga_server::settings::{AddressSettings, UniformSettings};
use saranga_server::{Broker, BrokerConfig};

/// A queue "q" holding one expired message: on its own expiry address, or
/// a mirror's store-and-forward queue.
async fn queue(own_expiry_address: bool) -> (Arc<Broker>, Queue) {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    if own_expiry_address {
        config.settings = Arc::new(UniformSettings(Arc::new(AddressSettings {
            expiry_address: Some("q".into()),
            ..Default::default()
        })));
    }
    let broker = Broker::start(config).await.unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    let q = broker.post_office().create_queue(spec, false).unwrap();
    if !own_expiry_address {
        q.set_mirror_controller(true);
    }
    q.add(MessageRef::new(ServerMessage::new(CoreMessage {
        message_id: 100,
        address: Some("q".into()),
        expiration: 1,
        ..Default::default()
    })));
    assert_eq!(q.browse().await.len(), 1);
    (broker, q)
}

#[tokio::test]
async fn the_expiry_scan_leaves_a_queue_on_its_own_expiry_address() {
    let (broker, q) = queue(true).await;
    q.expire_scan();
    let left = q.browse().await;
    assert_eq!(q.messages_expired(), 0);
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].message_id(), 100, "the message keeps its identity");
    broker.stop().unwrap();
}

#[tokio::test]
async fn the_expiry_scan_leaves_a_mirror_store_and_forward_queue() {
    let (broker, q) = queue(false).await;
    q.expire_scan();
    assert_eq!(q.browse().await.len(), 1);
    assert_eq!(q.messages_expired(), 0);
    broker.stop().unwrap();
}

#[tokio::test]
async fn management_expires_nothing_on_its_own_expiry_address() {
    let (broker, q) = queue(true).await;
    assert_eq!(
        q.act(Selection::default(), SelectionAction::Expire).await,
        0
    );
    assert_eq!(q.browse().await.len(), 1);
    broker.stop().unwrap();
}

#[tokio::test]
async fn management_expires_nothing_on_a_mirror_store_and_forward_queue() {
    let (broker, q) = queue(false).await;
    assert_eq!(
        q.act(Selection::default(), SelectionAction::Expire).await,
        0
    );
    assert_eq!(q.browse().await.len(), 1);
    assert_eq!(q.messages_expired(), 0);
    broker.stop().unwrap();
}
