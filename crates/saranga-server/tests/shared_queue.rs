//! `createSharedQueue` as `ActiveMQServerImpl` does it: a non-durable shared
//! queue is temporary and transient (it goes with its last consumer), its
//! address is created whatever `auto-create-addresses` says, and a queue
//! already there with another address or filter is refused.

use std::path::Path;
use std::sync::Arc;

use saranga_config as cfg;
use saranga_core_protocol::ErrorType;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig, RoutingType};

struct Nothing;

impl DeliverySink for Nothing {
    fn deliver(&self, _: i64, _: &Arc<ServerMessage>, _: i32) -> i64 {
        1
    }
    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        1
    }
    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        1
    }
    fn chunk_size(&self) -> usize {
        1024
    }
}

/// A broker that creates no address on its own.
async fn start(dir: &Path) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(
        r##"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>false</persistence-enabled>
             <address-settings>
               <address-setting match="#">
                 <auto-create-addresses>false</auto-create-addresses>
                 <auto-create-queues>false</auto-create-queues>
               </address-setting>
             </address-settings>
           </core></configuration>"##,
    )
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("shared"),
        Arc::new(Default::default()),
        vec![],
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "shared".into(),
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

fn shared(name: &str, address: &str, filter: Option<&str>, durable: bool) -> QueueSpec {
    let mut spec = QueueSpec::new(name, address);
    spec.routing_type = Some(RoutingType::Multicast);
    spec.filter_string = filter.map(str::to_owned);
    spec.durable = durable;
    spec
}

#[tokio::test]
async fn a_shared_queue_is_made_without_auto_created_addresses() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    let mut s = session(&broker);
    s.create_shared_queue(shared("sub", "topic", None, true))
        .expect("the address is created for a shared queue");
    assert!(broker.post_office().queue("sub").is_some());
    assert!(broker.post_office().address_info("topic").is_some());
}

#[tokio::test]
async fn a_non_durable_shared_queue_goes_with_its_last_consumer() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    let mut s = session(&broker);
    s.create_shared_queue(shared("volatile", "topic", None, false))
        .unwrap();
    let q = broker.post_office().queue("volatile").unwrap();
    assert!(q.config().temporary, "temporary, as Artemis makes it");
    for id in [1, 2] {
        s.create_consumer(id, "volatile", None, 0, false, |_| Arc::new(Nothing))
            .await
            .unwrap();
    }
    s.close_consumer(1, false).await;
    assert!(
        broker.post_office().queue("volatile").is_some(),
        "a consumer is left"
    );
    s.close_consumer(2, false).await;
    assert!(
        broker.post_office().queue("volatile").is_none(),
        "gone with its last consumer"
    );
}

#[tokio::test]
async fn a_durable_shared_queue_stays_without_consumers() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    let mut s = session(&broker);
    s.create_shared_queue(shared("kept", "topic", None, true))
        .unwrap();
    s.create_consumer(1, "kept", None, 0, false, |_| Arc::new(Nothing))
        .await
        .unwrap();
    s.close_consumer(1, false).await;
    assert!(broker.post_office().queue("kept").is_some());
}

#[tokio::test]
async fn a_shared_queue_with_another_address_or_filter_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    let mut s = session(&broker);
    s.create_shared_queue(shared("sub", "topic", Some("a = 1"), true))
        .unwrap();
    // The same again is taken as it is.
    s.create_shared_queue(shared("sub", "topic", Some("a = 1"), true))
        .unwrap();
    let e = s
        .create_shared_queue(shared("sub", "other", Some("a = 1"), true))
        .unwrap_err();
    assert_eq!(e.kind, ErrorType::InvalidTransientQueueUse);
    assert!(e.message.starts_with("AMQ229082"), "{}", e.message);
    let e = s
        .create_shared_queue(shared("sub", "topic", Some("a = 2"), true))
        .unwrap_err();
    assert_eq!(e.kind, ErrorType::InvalidTransientQueueUse);
    assert!(e.message.starts_with("AMQ229083"), "{}", e.message);
}

/// A queue deleted between a consumer's lookup of it and its admission
/// (here by the sink's factory, which runs in between, as the last consumer
/// of a transient queue leaving would) refuses the consumer rather than
/// taking it on a queue that is gone.
#[tokio::test]
async fn a_consumer_on_a_queue_deleted_meanwhile_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    let mut s = session(&broker);
    s.create_shared_queue(shared("gone", "topic", None, false))
        .unwrap();
    let po = broker.post_office().clone();
    let created = s
        .create_consumer(1, "gone", None, 0, false, move |_| {
            po.delete_queue("gone", true, false).unwrap();
            Arc::new(Nothing)
        })
        .await;
    let e = created.expect_err("a consumer on a deleted queue");
    assert_eq!(e.kind, ErrorType::QueueDoesNotExist, "{}", e.message);
    assert!(broker.post_office().queue("gone").is_none());
}
