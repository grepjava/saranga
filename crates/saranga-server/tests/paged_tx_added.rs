//! A paged send in a transaction counts in `messagesAdded` when the
//! transaction commits, and not at all when it rolls back, as Artemis's
//! page counter counts it (`PageSubscriptionCounterImpl.increment(tx, ...)`).

use std::sync::Arc;

use saranga_config as cfg;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::Queue;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig, RoutingType};

async fn start(dir: &std::path::Path) -> (Arc<Broker>, Queue) {
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <address-settings>
               <address-setting match="q">
                 <max-size-bytes>1</max-size-bytes>
                 <address-full-policy>PAGE</address-full-policy>
               </address-setting>
             </address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    let broker = Broker::start(config).await.unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.routing_type = Some(RoutingType::Anycast);
    spec.durable = true;
    spec.auto_create_address = Some(true);
    let q = broker.post_office().create_queue(spec, false).unwrap();
    (broker, q)
}

fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("t"),
        Arc::new(Default::default()),
        vec![],
    ));
    ServerSession::new(
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
    )
}

/// Sends five durable messages in the session's transaction; the address
/// pages after the first.
async fn send_five(broker: &Arc<Broker>, s: &mut ServerSession) {
    for _ in 0..5 {
        s.send(
            saranga_message::CoreMessage {
                address: Some("q".into()),
                durable: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        s.take_completion().wait().await.unwrap();
    }
    assert!(broker.post_office().paging_store("q").is_paging());
}

#[tokio::test(flavor = "multi_thread")]
async fn rolled_back_paged_sends_are_not_added() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, q) = start(dir.path()).await;
    let mut s = session(&broker);
    send_five(&broker, &mut s).await;
    assert_eq!(q.messages_added(), 0, "counted before the commit");
    s.rollback(false, false).await;
    s.take_completion().wait().await.unwrap();
    assert_eq!(q.message_count(), 0);
    assert_eq!(q.messages_added(), 0, "rolled-back sends counted as added");
    broker.stop().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn committed_paged_sends_are_added_once() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, q) = start(dir.path()).await;
    let mut s = session(&broker);
    send_five(&broker, &mut s).await;
    s.commit().unwrap();
    s.take_completion().wait().await.unwrap();
    assert_eq!(q.message_count(), 5);
    assert_eq!(q.messages_added(), 5);
    broker.stop().unwrap();
}
