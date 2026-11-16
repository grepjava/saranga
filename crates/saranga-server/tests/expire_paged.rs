//! The expiry scan reaches paged messages as Artemis's `ExpiryScanner`
//! does: a queue that expired something, or holds nothing in memory, reads
//! in more of its pages and scans them, paused or not.

use std::sync::Arc;
use std::time::Duration;

use saranga_buffer::SimpleString;
use saranga_config as cfg;
use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig, RoutingType};

fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
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

#[tokio::test]
async fn a_paused_queue_expires_its_paged_messages() {
    let dir = tempfile::tempdir().unwrap();
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>65536</journal-file-size>
             <address-settings>
               <address-setting match="p">
                 <max-size-bytes>10000</max-size-bytes>
                 <page-size-bytes>20000</page-size-bytes>
                 <address-full-policy>PAGE</address-full-policy>
                 <max-read-page-messages>2</max-read-page-messages>
                 <expiry-address>exp</expiry-address>
               </address-setting>
             </address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let broker = Broker::start(
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap(),
    )
    .await
    .unwrap();
    for name in ["p", "exp"] {
        let mut spec = QueueSpec::new(name, name);
        spec.auto_create_address = Some(true);
        spec.routing_type = Some(RoutingType::Anycast);
        broker.post_office().create_queue(spec, false).unwrap();
    }
    let p = broker.post_office().queue("p").unwrap();
    let exp = broker.post_office().queue("exp").unwrap();
    p.pause(false);
    let mut s = session(&broker);
    let expiration = saranga_server::message::now_millis() + 300;
    for _ in 0..12 {
        let m = CoreMessage {
            address: Some(SimpleString::from("p")),
            durable: true,
            expiration,
            body: vec![7u8; 4000].into(),
            ..Default::default()
        };
        s.send(m, None).await.unwrap();
        s.take_completion().wait().await.unwrap();
    }
    assert!(
        broker
            .post_office()
            .paging()
            .existing_store("p")
            .is_some_and(|st| st.is_paging())
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    for _ in 0..100 {
        p.expire_scan();
        if exp.message_count() == 12 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(exp.message_count(), 12, "every paged message expired");
    assert_eq!(p.message_count(), 0);
    broker.stop().unwrap();
}
