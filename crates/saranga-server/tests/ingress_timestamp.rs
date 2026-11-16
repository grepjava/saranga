//! `<enable-ingress-timestamp>`: a message sent to a matching address is
//! stamped with the time the broker took it (`setIngressTimestamp`), and
//! one sent elsewhere is not.

use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig, RoutingType};

#[tokio::test]
async fn a_message_is_stamped_where_its_address_says() {
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>false</persistence-enabled>
             <address-settings><address-setting match="stamped">
               <enable-ingress-timestamp>true</enable-ingress-timestamp>
             </address-setting></address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let broker = Broker::start(
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let po = broker.post_office();
    for name in ["stamped", "plain"] {
        let mut spec = QueueSpec::new(name, name);
        spec.routing_type = Some(RoutingType::Anycast);
        spec.auto_create_address = Some(true);
        po.create_queue(spec, false).unwrap();
    }
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
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    );
    let before = saranga_server::message::now_millis();
    for name in ["stamped", "plain"] {
        s.send(
            CoreMessage {
                address: Some(SimpleString::from(name)),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        s.take_completion().wait().await.unwrap();
    }
    let after = saranga_server::message::now_millis();
    let stamp = |name: &str| {
        let q = po.queue(name).unwrap();
        async move {
            q.browse().await[0]
                .message
                .core()
                .properties
                .get(&SimpleString::from("_AMQ_INGRESS_TIMESTAMP"))
                .cloned()
        }
    };
    match stamp("stamped").await {
        Some(PropertyValue::Long(t)) => assert!((before..=after).contains(&t), "{t}"),
        other => panic!("not stamped: {other:?}"),
    }
    assert!(stamp("plain").await.is_none());
    broker.stop().unwrap();
}
