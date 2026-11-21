//! `Queue::holding`: which messages a queue may still deliver. A client's
//! packet-ID correlation for a message no queue of the client holds is
//! freed (MQTT), so the answer must not leave out a message that waits in a
//! page.

use std::collections::HashSet;
use std::sync::Arc;

use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig, RoutingType};

async fn broker(dir: &std::path::Path) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <address-settings>
               <address-setting match="paged">
                 <max-size-bytes>1</max-size-bytes>
                 <page-size-bytes>5000</page-size-bytes>
                 <address-full-policy>PAGE</address-full-policy>
               </address-setting>
             </address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

async fn send(broker: &Arc<Broker>, address: &str, n: usize) {
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
    for _ in 0..n {
        let m = CoreMessage {
            address: Some(address.into()),
            durable: true,
            body: vec![1u8; 1000].into(),
            ..Default::default()
        };
        s.send(m, None).await.unwrap();
        s.take_completion().wait().await.unwrap();
    }
    s.close(false).await;
}

fn queue(broker: &Arc<Broker>, name: &str) -> saranga_server::queue::Queue {
    let mut spec = QueueSpec::new(name, name);
    spec.auto_create_address = Some(true);
    spec.routing_type = Some(RoutingType::Anycast);
    spec.durable = true;
    broker.post_office().create_queue(spec, false).unwrap();
    broker.post_office().queue(name).unwrap()
}

/// A queue answers for what it holds: its messages, not others.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_holds_its_own_messages() {
    let dir = tempfile::tempdir().unwrap();
    let broker = broker(dir.path()).await;
    let q = queue(&broker, "plain");
    send(&broker, "plain", 2).await;
    let ids: HashSet<i64> = q.browse().await.iter().map(|r| r.message_id()).collect();
    assert_eq!(ids.len(), 2);
    let mut asked = ids.clone();
    asked.insert(i64::MAX);
    assert_eq!(q.holding(asked).await, ids);
    broker.stop().unwrap();
}

/// While a queue has paged messages, any message asked about may wait in a
/// page not read yet: it is held.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paging_queue_may_hold_any_message() {
    let dir = tempfile::tempdir().unwrap();
    let broker = broker(dir.path()).await;
    let q = queue(&broker, "paged");
    send(&broker, "paged", 40).await;
    assert!(q.page_subscription().is_some_and(|p| p.message_count() > 0));
    let asked: HashSet<i64> = [i64::MAX].into();
    assert_eq!(q.holding(asked.clone()).await, asked);
    broker.stop().unwrap();
}
