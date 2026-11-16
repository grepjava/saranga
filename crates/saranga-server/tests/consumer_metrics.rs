//! A consumer's metrics (`ServerConsumerMetrics`) follow each delivery the
//! way Artemis keeps them: a pre-acknowledged delivery counts as
//! acknowledged, a browser counts nothing, and a cancelled, rejected or
//! closed consumer's deliveries leave its in-transit size. The
//! slow-consumer check measures a queue's rate from the queue's creation,
//! so one turned on later does not take all the queue has had for one
//! period's traffic.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_message::CoreMessage;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::{ConsumerMetrics, DeliverySink};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

#[derive(Default)]
struct Sink {
    delivered: Mutex<Vec<i64>>,
    slow: Mutex<u32>,
}

impl DeliverySink for Sink {
    fn deliver(&self, _: i64, message: &Arc<ServerMessage>, _: i32) -> i64 {
        self.delivered.lock().unwrap().push(message.message_id());
        1
    }
    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        1
    }
    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        0
    }
    fn chunk_size(&self) -> usize {
        64 * 1024
    }
    fn slow_consumer(&self, _: i64) {
        *self.slow.lock().unwrap() += 1;
    }
}

async fn broker() -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>false</persistence-enabled>
           </core></configuration>"#,
    )
    .unwrap();
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()))
        .path()
        .to_owned();
    let mut config =
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), &dir).unwrap();
    config.address_queue_scan_period = -1;
    let broker = Broker::start(config).await.unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    broker.post_office().create_queue(spec, false).unwrap();
    broker
}

fn session(broker: &Arc<Broker>, name: &str, pre_acknowledge: bool) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    // Listed, as an accepted connection is, for the slow-consumer check.
    std::mem::forget(broker.registry().add(&conn));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: name.into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge,
            default_address: None,
        },
        Default::default(),
    )
}

async fn send(s: &mut ServerSession, n: u8) {
    for i in 0..n {
        s.send(
            CoreMessage {
                address: Some("q".into()),
                body: vec![i].into(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    }
}

async fn consume(s: &mut ServerSession, id: i64, browse_only: bool, n: usize) -> Arc<Sink> {
    let sink = Arc::new(Sink::default());
    let c = sink.clone();
    s.create_consumer(id, "q", None, 0, browse_only, move |_| c.clone())
        .await
        .unwrap();
    s.consumer_credits(id, -1);
    s.set_started(true).await;
    for _ in 0..300 {
        if sink.delivered.lock().unwrap().len() >= n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(sink.delivered.lock().unwrap().len(), n);
    sink
}

fn metrics(broker: &Arc<Broker>, session: &str, id: i64) -> Arc<ConsumerMetrics> {
    broker
        .registry()
        .sessions()
        .iter()
        .find(|s| s.name == session)
        .and_then(|s| s.consumers().into_iter().find(|c| c.id == id))
        .map(|c| c.metrics.clone())
        .expect("the consumer is registered")
}

/// Each pre-acknowledged delivery is acknowledged as it is sent.
#[tokio::test]
async fn a_pre_acknowledged_delivery_counts_as_acknowledged() {
    let broker = broker().await;
    let mut s = session(&broker, "s", true);
    send(&mut s, 3).await;
    consume(&mut s, 1, false, 3).await;
    let m = metrics(&broker, "s", 1);
    assert_eq!(m.delivered(), 3);
    assert_eq!(m.acknowledged(), 3);
    assert_eq!(m.in_transit_size(), 0);
    broker.stop().unwrap();
}

/// A browser's deliveries are not the consumer's (`!browseOnly`).
#[tokio::test]
async fn a_browser_counts_no_delivery() {
    let broker = broker().await;
    let mut s = session(&broker, "s", false);
    send(&mut s, 2).await;
    consume(&mut s, 1, true, 2).await;
    let m = metrics(&broker, "s", 1);
    assert_eq!(m.delivered(), 0);
    assert_eq!(m.in_transit_size(), 0);
    broker.stop().unwrap();
}

/// A cancelled and a rejected delivery leave the in-transit size, as does
/// what a consumer still held when its deliveries were cancelled.
#[tokio::test]
async fn cancelled_and_rejected_deliveries_leave_transit() {
    let broker = broker().await;
    let mut s = session(&broker, "s", false);
    send(&mut s, 3).await;
    let sink = consume(&mut s, 1, false, 3).await;
    let ids = sink.delivered.lock().unwrap().clone();
    let m = metrics(&broker, "s", 1);
    let all = m.in_transit_size();
    assert!(all > 0);
    // Stopped, so the cancelled message is not delivered again.
    s.set_started(false).await;
    s.cancel_one(1, ids[0], false);
    s.reject(1, ids[1]);
    for _ in 0..300 {
        if m.acknowledged() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(m.acknowledged(), 2, "the cancel and the reject");
    assert_eq!(m.in_transit_size(), all / 3, "the third alone");
    s.close_consumer(1, false).await;
    assert_eq!(m.in_transit_size(), 0, "closed, it holds nothing");
    broker.stop().unwrap();
}

/// The queue had its traffic long before the check was turned on: that is
/// not one period's rate, so a consumer that took everything is not slow.
#[tokio::test]
async fn a_late_slow_consumer_check_measures_from_the_queue_creation() {
    let broker = broker().await;
    let mut s = session(&broker, "s", true);
    send(&mut s, 10).await;
    let sink = consume(&mut s, 1, false, 10).await;
    // The queue has averaged under 5 a second when the check starts.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let settings = saranga_config::AddressSettings {
        slow_consumer_threshold: Some(5),
        slow_consumer_check_period: Some(1),
        slow_consumer_policy: Some(saranga_config::SlowConsumerPolicy::Notify),
        ..Default::default()
    };
    broker
        .post_office()
        .add_address_settings("q", settings, "{}")
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1800)).await;
    assert_eq!(
        *sink.slow.lock().unwrap(),
        0,
        "10 messages over 3.5s is under 5 a second"
    );
    broker.stop().unwrap();
}

/// A consumer closed with deliveries it never acknowledged gives them back
/// at once (`cancel(tx, ref, true)`): the redelivery delay is for messages
/// a client failed to process, and most of these never reached it.
#[tokio::test]
async fn a_closed_consumer_returns_its_messages_without_the_redelivery_delay() {
    let broker = broker().await;
    let settings = saranga_config::AddressSettings {
        redelivery_delay: Some(60_000),
        ..Default::default()
    };
    broker
        .post_office()
        .add_address_settings("q", settings, "{}")
        .unwrap();
    let mut s = session(&broker, "s", false);
    send(&mut s, 2).await;
    consume(&mut s, 1, false, 2).await;
    s.close_consumer(1, false).await;
    // Delivered again at once, not in a minute.
    consume(&mut s, 2, false, 2).await;
    broker.stop().unwrap();
}
