//! A move whose source queue is deleted while its journal commit is on its
//! way still reaches its target: the queue's actor finishes the move before
//! it stops (Artemis's afterCommit runs whatever became of the source).

use std::sync::Arc;
use std::time::Duration;

use saranga_buffer::SimpleString;
use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::{Broker, BrokerConfig, RoutingType};

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn anycast(name: &str) -> QueueSpec {
    let mut spec = QueueSpec::new(name, name);
    spec.auto_create_address = Some(true);
    spec.routing_type = Some(RoutingType::Anycast);
    spec
}

async fn persistent_broker(dir: &std::path::Path) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <address-settings>
               <address-setting match="q">
                 <expiry-address>EXP</expiry-address>
               </address-setting>
             </address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

fn plain_session(broker: &Arc<Broker>) -> saranga_server::session::ServerSession {
    use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
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

/// A queue deleted while an expiry move's journal commit is in flight: the

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expiry_move_reaches_its_target_when_the_source_queue_goes() {
    let dir = tempfile::tempdir().unwrap();
    let b = persistent_broker(dir.path()).await;
    let po = b.post_office();
    let q = po.create_queue(anycast("q"), false).unwrap();
    let exp = po.create_queue(anycast("EXP"), false).unwrap();
    let mut s = plain_session(&b);
    for _ in 0..20 {
        let m = CoreMessage {
            address: Some(SimpleString::from("q")),
            durable: true,
            expiration: now_millis() + 200,
            ..Default::default()
        };
        s.send(m, None).await.unwrap();
        let _ = s.take_completion().wait().await;
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(q.message_count(), 20);
    q.expire_scan();
    po.delete_queue("q", false, false).unwrap();
    for _ in 0..100 {
        if exp.message_count() == 20 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(exp.message_count(), 20, "every expired message reaches EXP");
    b.stop().unwrap();
}
