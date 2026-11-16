//! A temporary queue's address goes with it when its connection does, as
//! Artemis's `TempQueueCleanerUpper` forces it, whatever
//! `auto-delete-addresses` says.

use std::sync::Arc;

use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig, RoutingType};

#[tokio::test]
async fn a_temporary_queue_takes_its_address_with_it() {
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r##"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>false</persistence-enabled>
             <address-settings><address-setting match="#">
               <auto-delete-addresses>false</auto-delete-addresses>
             </address-setting></address-settings>
           </core></configuration>"##,
    )
    .unwrap();
    let broker = Broker::start(
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    let mut s = ServerSession::new(
        broker.clone(),
        conn.clone(),
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
    );
    let mut spec = QueueSpec::new("tq", "tq");
    spec.routing_type = Some(RoutingType::Anycast);
    spec.temporary = true;
    spec.durable = false;
    s.create_queue(spec).unwrap();
    let po = broker.post_office();
    assert!(po.address_info("tq").is_some());
    conn.remove_temp_queues(&broker);
    assert!(po.queue("tq").is_none());
    assert!(po.address_info("tq").is_none(), "the address went too");
    broker.stop().unwrap();
}
