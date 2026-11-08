//! A send refused does not keep its duplicate ID: sending it again once the
//! address has room delivers it, as in Artemis, where the ID joins the cache
//! only when the send commits.

use std::path::Path;
use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_config as cfg;
use saranga_message::CoreMessage;
use saranga_server::management::Json;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &Path, persistence: bool) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>{persistence}</persistence-enabled>
             <address-settings>
               <address-setting match="full">
                 <max-size-bytes>10000</max-size-bytes>
                 <address-full-policy>FAIL</address-full-policy>
               </address-setting>
             </address-settings>
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

async fn op(broker: &Arc<Broker>, resource: &str, operation: &str, params: Vec<Json>) -> Json {
    broker
        .management()
        .invoke(broker, resource, operation, &params, Default::default())
        .await
        .unwrap()
}

/// A session whose connection has flow control, as a Core client's has.
fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(saranga_security::ConnectionInfo {
            remote_address: "test".into(),
            certificates: None,
            security_domain: None,
            connection_id: None,
            sasl_principals: None,
            sni_host: None,
        }),
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

/// Sends 4 KB, with a duplicate ID if given; the error's message if refused.
async fn send(session: &mut ServerSession, duplicate: Option<&str>) -> Result<(), String> {
    let mut message = CoreMessage {
        address: Some(SimpleString::from("full")),
        durable: true,
        body: vec![7u8; 4000].into(),
        ..Default::default()
    };
    if let Some(id) = duplicate {
        message.properties.insert(
            SimpleString::from("_AMQ_DUPL_ID"),
            PropertyValue::String(SimpleString::from(id)),
        );
    }
    let sent = session.send(message, None).await;
    session
        .take_completion()
        .wait()
        .await
        .map_err(|e| e.to_string())?;
    sent.map_err(|e| e.message)
}

async fn retry_after_full(persistence: bool) {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), persistence).await;
    op(
        &broker,
        "broker",
        "createQueue",
        vec![
            Json::from("full"),
            Json::from("ANYCAST"),
            Json::from("full"),
            Json::Null,
            Json::Bool(true),
            Json::Long(-1),
            Json::Bool(false),
            Json::Bool(true),
        ],
    )
    .await;
    let mut s = session(&broker);
    // Fill the address until it refuses.
    let mut filled = 0;
    while send(&mut s, None).await.is_ok() {
        filled += 1;
        assert!(filled < 10, "the address never filled");
    }
    let e = send(&mut s, Some("d1")).await.unwrap_err();
    assert!(e.contains("AMQ229102"), "{e}");
    op(&broker, "queue.full", "removeAllMessages", vec![]).await;
    send(&mut s, Some("d1")).await.unwrap();
    assert_eq!(
        op(&broker, "queue.full", "countMessages", vec![]).await,
        Json::Long(1),
        "the retry was delivered"
    );
    // Once delivered, the ID is a duplicate.
    send(&mut s, Some("d1")).await.unwrap();
    assert_eq!(
        op(&broker, "queue.full", "countMessages", vec![]).await,
        Json::Long(1),
        "a second copy was dropped"
    );
    s.close(false).await;
    broker.stop().unwrap();
}

#[tokio::test]
async fn a_send_refused_as_full_can_be_sent_again() {
    retry_after_full(true).await;
}

#[tokio::test]
async fn a_send_refused_as_full_can_be_sent_again_without_persistence() {
    retry_after_full(false).await;
}
