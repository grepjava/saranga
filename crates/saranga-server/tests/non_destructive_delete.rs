//! Deleting a non-destructive queue acknowledges each of its messages once.
//! Its consumers' deliveries are copies of references it still holds, and
//! acknowledging those too took a durable message's count to nothing while
//! another queue still held it: its record went, and the message with it
//! at the next start.

use std::sync::Arc;
use std::time::Duration;

use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

struct Sink;

impl DeliverySink for Sink {
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

fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("consumers"),
        Arc::new(Default::default()),
        vec![],
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "consumers".into(),
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
async fn deleting_a_non_destructive_queue_keeps_another_queues_message() {
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>65536</journal-file-size>
           </core></configuration>"#,
    )
    .unwrap();
    let config =
        || BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
    let broker = Broker::start(config()).await.unwrap();
    // One durable message on two queues of an address, "a" non-destructive.
    for name in ["a", "b"] {
        let mut spec = QueueSpec::new(name, "addr");
        spec.auto_create_address = Some(true);
        spec.durable = true;
        spec.non_destructive = Some(name == "a");
        broker.post_office().create_queue(spec, false).unwrap();
    }
    let mut s = session(&broker);
    s.create_consumer(1, "a", None, 0, false, |_| Arc::new(Sink))
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;
    s.send(
        saranga_message::CoreMessage {
            address: Some("addr".into()),
            durable: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    s.take_completion().wait().await.unwrap();
    // Delivered on "a", not acknowledged, when "a" goes.
    let a = broker.post_office().queue("a").unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while a.stats().await.delivering_count != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    broker
        .post_office()
        .delete_queue_awaited("a", false, false)
        .await
        .unwrap();
    assert_eq!(broker.post_office().queue("b").unwrap().message_count(), 1);
    s.close(true).await;
    broker.stop().unwrap();
    drop(s);
    drop(broker);

    let broker = Broker::start(config()).await.unwrap();
    assert_eq!(
        broker.post_office().queue("b").unwrap().message_count(),
        1,
        "b's message is still there after a restart"
    );
    broker.stop().unwrap();
}

/// Rejecting a delivery on a non-destructive queue takes the message off
/// that queue and acknowledges it there once: deleting the queue afterwards
/// leaves another queue's copy of a durable message in place.
#[tokio::test]
async fn rejecting_on_a_non_destructive_queue_keeps_another_queues_message() {
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>65536</journal-file-size>
           </core></configuration>"#,
    )
    .unwrap();
    let config =
        || BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
    let broker = Broker::start(config()).await.unwrap();
    for name in ["a", "b"] {
        let mut spec = QueueSpec::new(name, "addr");
        spec.auto_create_address = Some(true);
        spec.durable = true;
        spec.non_destructive = Some(name == "a");
        broker.post_office().create_queue(spec, false).unwrap();
    }
    let mut s = session(&broker);
    s.create_consumer(1, "a", None, 0, false, |_| Arc::new(Sink))
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;
    s.send(
        saranga_message::CoreMessage {
            address: Some("addr".into()),
            durable: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    s.take_completion().wait().await.unwrap();
    let a = broker.post_office().queue("a").unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while a.stats().await.delivering_count != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let id = broker.post_office().queue("b").unwrap().browse().await[0].message_id();
    s.reject(1, id);
    tokio::time::timeout(Duration::from_secs(3), async {
        while a.message_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the rejected message leaves the non-destructive queue");
    broker
        .post_office()
        .delete_queue_awaited("a", false, false)
        .await
        .unwrap();
    assert_eq!(broker.post_office().queue("b").unwrap().message_count(), 1);
    s.close(true).await;
    broker.stop().unwrap();
    drop(s);
    drop(broker);

    let broker = Broker::start(config()).await.unwrap();
    assert_eq!(
        broker.post_office().queue("b").unwrap().message_count(),
        1,
        "b's message is still there after a restart"
    );
    broker.stop().unwrap();
}
