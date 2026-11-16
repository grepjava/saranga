//! A transaction's send to a queue deleted before the commit leaves
//! nothing behind: the reference is acknowledged as Artemis's destroyed
//! queue acknowledges what reaches it (`removeMessagesWhileDelivering`),
//! so it no longer counts against its address.

use std::sync::Arc;

use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig, RoutingType};

#[tokio::test]
async fn a_send_committed_to_a_deleted_queue_counts_for_nothing() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let po = broker.post_office();
    let mut spec = QueueSpec::new("q", "a");
    spec.routing_type = Some(RoutingType::Anycast);
    spec.auto_create_address = Some(true);
    po.create_queue(spec, false).unwrap();
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    let mut s = ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "s".into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: false,
            auto_commit_acks: false,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    );
    s.send(
        CoreMessage {
            address: Some("a".into()),
            body: vec![0u8; 1000].into(),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let store = po.paging_store("a");
    assert!(store.address_size() > 0, "counted once routed");
    po.delete_queue("q", false, false).unwrap();
    s.commit().unwrap();
    s.take_completion().wait().await.unwrap();
    for _ in 0..100 {
        if store.address_size() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(store.address_size(), 0, "nothing left counted");
    broker.stop().unwrap();
}
