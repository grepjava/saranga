//! What a non-destructive queue's consumer rolls back comes back to it,
//! with its delivery count, and goes to the dead-letter address once it
//! has used its delivery attempts; a reference taken off such a queue and
//! put back is not lost.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

/// Records each delivery: message ID and delivery count.
#[derive(Default)]
struct Sink(Mutex<Vec<(i64, i32)>>);

impl DeliverySink for Sink {
    fn deliver(&self, _: i64, m: &Arc<ServerMessage>, count: i32) -> i64 {
        self.0.lock().unwrap().push((m.message_id(), count));
        1
    }
    fn deliver_large(&self, _: i64, m: &ServerMessage, _: i64, count: i32) -> i64 {
        self.0.lock().unwrap().push((m.message_id(), count));
        1
    }
    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        1
    }
    fn chunk_size(&self) -> usize {
        1024
    }
}

impl Sink {
    /// Waits for delivery `n` (from 1) and returns it.
    async fn nth(&self, n: usize) -> Option<(i64, i32)> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(d) = self.0.lock().unwrap().get(n - 1) {
                    return *d;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .ok()
    }
}

fn session(broker: &Arc<Broker>, transacted: bool) -> ServerSession {
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
            auto_commit_acks: !transacted,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

async fn broker(settings: &str) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(&format!(
        r##"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>false</persistence-enabled>
             <address-settings><address-setting match="#">{settings}</address-setting></address-settings>
           </core></configuration>"##
    ))
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config =
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
    let broker = Broker::start(config).await.unwrap();
    for (name, non_destructive) in [("nd", true), ("DLQ", false)] {
        let mut spec = QueueSpec::new(name, name);
        spec.auto_create_address = Some(true);
        spec.non_destructive = Some(non_destructive);
        broker.post_office().create_queue(spec, false).unwrap();
    }
    broker
}

async fn send(s: &mut ServerSession) {
    s.send(
        saranga_message::CoreMessage {
            address: Some("nd".into()),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
}

async fn consume(s: &mut ServerSession, sink: &Arc<Sink>) {
    let sink = sink.clone();
    s.create_consumer(1, "nd", None, 0, false, move |_| sink)
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;
}

/// A transacted consumer acknowledges a message and rolls back: it gets
/// the message again, counted as a redelivery.
#[tokio::test]
async fn a_rolled_back_message_comes_back_to_its_consumer() {
    let broker = broker("").await;
    let mut s = session(&broker, true);
    send(&mut s).await;
    let sink = Arc::new(Sink::default());
    consume(&mut s, &sink).await;
    let (id, count) = sink.nth(1).await.expect("delivered");
    assert_eq!(count, 1);
    s.acknowledge(1, id, true).await.unwrap();
    s.rollback(false, false).await;
    assert_eq!(
        sink.nth(2).await,
        Some((id, 2)),
        "the rolled-back message comes back, its second delivery"
    );
    assert_eq!(broker.post_office().queue("nd").unwrap().message_count(), 1);
    s.close(false).await;
    broker.stop().unwrap();
}

/// A message rolled back as often as max-delivery-attempts allows goes to
/// the dead-letter address and leaves the non-destructive queue.
#[tokio::test]
async fn a_message_rolled_back_too_often_goes_to_the_dead_letter_address() {
    let broker = broker(
        "<dead-letter-address>DLQ</dead-letter-address><max-delivery-attempts>2</max-delivery-attempts>",
    )
    .await;
    let mut s = session(&broker, true);
    send(&mut s).await;
    let sink = Arc::new(Sink::default());
    consume(&mut s, &sink).await;
    for n in 1..=2 {
        let (id, count) = sink.nth(n).await.expect("delivered");
        assert_eq!(count, n as i32);
        s.acknowledge(1, id, true).await.unwrap();
        s.rollback(false, false).await;
    }
    let po = broker.post_office();
    tokio::time::timeout(Duration::from_secs(2), async {
        while po.queue("DLQ").unwrap().message_count() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the message reaches the dead-letter queue");
    assert_eq!(po.queue("nd").unwrap().message_count(), 0);
    assert_eq!(sink.nth(3).await, None, "not delivered a third time");
    s.close(false).await;
    broker.stop().unwrap();
}

/// A reference taken off a non-destructive queue and put back (as when
/// whoever took it cannot use it) is on the queue again.
#[tokio::test]
async fn a_message_taken_off_and_put_back_is_kept() {
    let broker = broker("").await;
    let mut s = session(&broker, false);
    send(&mut s).await;
    let q = broker.post_office().queue("nd").unwrap();
    let taken = q.take_matching(None).await;
    assert_eq!(taken.len(), 1);
    let id = taken[0].message_id();
    q.reinsert(taken);
    let sink = Arc::new(Sink::default());
    consume(&mut s, &sink).await;
    assert_eq!(
        sink.nth(1).await.map(|(m, _)| m),
        Some(id),
        "the message is back on the queue"
    );
    s.close(false).await;
    broker.stop().unwrap();
}
