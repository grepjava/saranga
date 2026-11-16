//! A notification refused by its address's limits (`FAIL`) is not sent:
//! Artemis's `route` throws, and the notification is neither paged nor
//! queued past the limit.

use std::sync::Arc;
use std::time::Duration;

use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::{Broker, BrokerConfig, RoutingType};

#[tokio::test]
async fn a_full_fail_notification_address_takes_no_more() {
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <address-settings><address-setting match="activemq.notifications">
               <max-size-bytes>1</max-size-bytes>
               <address-full-policy>FAIL</address-full-policy>
             </address-setting></address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let b = Broker::start(
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let mut spec = QueueSpec::new("notes", "activemq.notifications");
    spec.auto_create_address = Some(true);
    spec.routing_type = Some(RoutingType::Multicast);
    let notes = b.post_office().create_queue(spec, false).unwrap();
    // Each queue made is a BINDING_ADDED notification.
    for i in 0..5 {
        let mut spec = QueueSpec::new(format!("q{i}").as_str(), "a");
        spec.auto_create_address = Some(true);
        b.post_office().create_queue(spec, false).unwrap();
    }
    let mut seen = 0;
    for _ in 0..100 {
        seen = notes.message_count();
        if seen >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(seen >= 1, "the first notification fits");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(notes.message_count(), 1, "the rest are refused");
    b.stop().unwrap();
}

/// `sendQueueInfoToQueue` to a queue whose address pages: the reset and
/// the topology go behind what is paged (`processRoute`), not in memory
/// ahead of older notifications still in pages.
#[tokio::test]
async fn queue_info_to_a_paging_queue_is_paged() {
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <address-settings><address-setting match="sf">
               <max-size-bytes>1</max-size-bytes>
               <page-size-bytes>10240</page-size-bytes>
               <address-full-policy>PAGE</address-full-policy>
             </address-setting></address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let b = Broker::start(
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let mut spec = QueueSpec::new("sf", "sf");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    let sf = b.post_office().create_queue(spec, false).unwrap();
    // Put the address over its size so it pages.
    let conn = Arc::new(saranga_server::session::ClientConnection::new(
        saranga_server::session::Transport::internal("test"),
        Arc::new(Default::default()),
        vec![],
    ));
    let mut s = saranga_server::session::ServerSession::new(
        b.clone(),
        conn,
        saranga_server::session::SessionOptions {
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
    );
    for _ in 0..2 {
        s.send(
            saranga_message::CoreMessage {
                address: Some("sf".into()),
                durable: true,
                body: vec![1; 100].into(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        s.take_completion().wait().await.unwrap();
    }
    assert!(b.post_office().paging_store("sf").is_paging());
    let paged = || sf.page_subscription().unwrap().message_count();
    let before = paged();
    b.post_office()
        .send_queue_info_to_queue("sf", "sf")
        .await
        .unwrap();
    assert!(paged() > before, "the reset message is paged");
    b.stop().unwrap();
}
