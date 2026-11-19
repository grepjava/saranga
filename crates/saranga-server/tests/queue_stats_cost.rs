//! A queue's statistics (the Prometheus gauges, the console's samples and
//! the management attributes read from them) come from counts the queue
//! keeps, as Artemis's `QueuePendingMessageMetrics` does: reading them
//! costs the same whatever the queue's depth, and does not hold up the
//! queue's dispatch with a walk of its messages.

use std::sync::Arc;
use std::time::{Duration, Instant};

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::Queue;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

fn session(b: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        vec![],
    ));
    ServerSession::new(
        b.clone(),
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

/// Sends until the queue holds `depth` messages, one in four scheduled an
/// hour ahead.
async fn fill(s: &mut ServerSession, q: &Queue, depth: usize, sent: &mut usize) {
    let later = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 3_600_000;
    while *sent < depth {
        let mut m = CoreMessage {
            address: Some("q".into()),
            body: vec![1; 100].into(),
            ..Default::default()
        };
        if sent.is_multiple_of(4) {
            m.properties.insert(
                SimpleString::from("_AMQ_SCHED_DELIVERY"),
                PropertyValue::Long(later),
            );
        }
        s.send(m, None).await.unwrap();
        s.take_completion().wait().await.unwrap();
        *sent += 1;
    }
    while (q.message_count() as usize) < depth {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The fastest of a batch of `stats()` reads, per read.
async fn read_cost(q: &Queue) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..5 {
        let t = Instant::now();
        for _ in 0..50 {
            std::hint::black_box(q.stats().await);
        }
        best = best.min(t.elapsed() / 50);
    }
    best
}

#[tokio::test(flavor = "multi_thread")]
async fn reading_a_queues_statistics_does_not_grow_with_its_depth() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    let q = broker.post_office().create_queue(spec, false).unwrap();
    let mut s = session(&broker);
    let mut sent = 0;

    fill(&mut s, &q, 8_192, &mut sent).await;
    let shallow = read_cost(&q).await;
    let before = q.stats().await;
    fill(&mut s, &q, 65_536, &mut sent).await;
    let deep = read_cost(&q).await;
    let after = q.stats().await;

    // The counts follow the messages added.
    assert_eq!(before.scheduled_count, 2_048);
    assert_eq!(after.scheduled_count, 16_384);
    assert_eq!(after.pending_size, before.pending_size * 8);
    // Eight times the depth: a walk would take about eight times as long.
    assert!(
        deep < shallow * 3 + Duration::from_micros(200),
        "a read took {deep:?} at depth 65536 against {shallow:?} at 8192"
    );
}
