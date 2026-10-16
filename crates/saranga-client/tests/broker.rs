//! Integration tests against a real broker. They run only when
//! `SARANGA_TEST_BROKER=host:port` is set, e.g. against the reference image:
//!
//! ```text
//! docker run -d --name artemis -e ANONYMOUS_LOGIN=true \
//!   -p 127.0.0.1:61618:61616 saranga/artemis-ref:2.57.0
//! SARANGA_TEST_BROKER=127.0.0.1:61618 cargo test -p saranga-client --test broker
//! ```

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use saranga_client::message::{self, JMS_CORRELATION_ID};
use saranga_client::*;

const WAIT: Duration = Duration::from_secs(5);

fn broker() -> Option<String> {
    let addr = std::env::var("SARANGA_TEST_BROKER").ok();
    if addr.is_none() {
        eprintln!("SARANGA_TEST_BROKER not set; skipping");
    }
    addr
}

/// A queue/address name unique to this run.
fn unique(prefix: &str) -> String {
    static N: AtomicU32 = AtomicU32::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "saranga.it.{prefix}.{nanos}.{}",
        N.fetch_add(1, Ordering::SeqCst)
    )
}

async fn connect(addr: &str) -> Connection {
    Connection::connect(addr, ConnectionOptions::default())
        .await
        .expect("connect")
}

/// Auto-ack options with acks sent right away (JMS AUTO_ACKNOWLEDGE uses
/// an ack batch size of 0).
fn auto_ack() -> SessionOptions {
    SessionOptions {
        ack_batch_size: 0,
        ..SessionOptions::default()
    }
}

async fn anycast_queue(session: &Session, name: &str) {
    session
        .create_queue(
            QueueConfiguration::new(name)
                .routing_type(RoutingType::Anycast)
                .durable(false),
        )
        .await
        .expect("create queue");
}

async fn wait_for_count(session: &Session, queue: &str, expected: i64) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let q = session.queue_query(queue).await.unwrap();
        if q.message_count == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "queue {queue} has {} messages, expected {expected}",
            q.message_count
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn receive_text(consumer: &Consumer) -> (Delivery, String) {
    let d = consumer
        .receive(WAIT)
        .await
        .unwrap()
        .expect("a message within the timeout");
    let text = d.message.text().unwrap().unwrap();
    (d, text)
}

#[tokio::test]
async fn anycast_send_receive() {
    let Some(addr) = broker() else { return };
    let conn = connect(&addr).await;
    assert_eq!(conn.version(), 136);
    let session = conn.create_session(auto_ack()).await.unwrap();
    session
        .add_metadata("saranga-test", "anycast")
        .await
        .unwrap();
    let queue = unique("anycast");
    anycast_queue(&session, &queue).await;

    let q = session.queue_query(&queue).await.unwrap();
    assert!(q.exists);
    assert_eq!(q.routing_type, Some(RoutingType::Anycast));
    let a = session.address_query(&queue).await.unwrap();
    assert!(a.exists && a.supports_anycast);
    assert_eq!(a.queue_names, vec![queue.clone()]);

    let producer = session.create_producer(Some(&queue)).await.unwrap();
    let mut m = message::text_message("hello saranga");
    m.put_string_property(JMS_CORRELATION_ID, "corr-1")
        .put_property("pInt", PropertyValue::Int(-7))
        .put_property("pLong", PropertyValue::Long(i64::MAX))
        .set_new_user_id();
    let user_id = m.user_id;
    producer.send(m).await.unwrap();
    producer
        .send(message::bytes_message(&[0, 1, 2, 255]))
        .await
        .unwrap();
    // Non-durable, so not blocking; a few hundred KiB to go through
    // several rounds of producer credits.
    for i in 0..200 {
        let mut m = message::text_message(&format!("{i}:{}", "x".repeat(1000)));
        m.set_durable(false);
        producer.send(m).await.unwrap();
    }
    wait_for_count(&session, &queue, 202).await;

    let consumer = session.create_consumer(&queue, None, false).await.unwrap();
    assert_eq!(consumer.initial_window_size(), 1024 * 1024);
    session.start().await.unwrap();

    let (d, text) = receive_text(&consumer).await;
    assert_eq!(text, "hello saranga");
    assert_eq!(d.delivery_count, 1);
    assert!(d.message.durable);
    assert_eq!(d.message.priority, 4);
    assert_eq!(d.message.user_id, user_id);
    assert_eq!(d.message.address.as_ref().unwrap().to_string(), queue);
    assert_eq!(
        d.message.string_property(JMS_CORRELATION_ID).as_deref(),
        Some("corr-1")
    );
    assert_eq!(d.message.property("pInt"), Some(&PropertyValue::Int(-7)));
    assert_eq!(
        d.message.property("pLong"),
        Some(&PropertyValue::Long(i64::MAX))
    );
    consumer.acknowledge(&d).await.unwrap();

    let d = consumer.receive(WAIT).await.unwrap().unwrap();
    assert_eq!(d.message.message_type, message_type::BYTES);
    assert_eq!(d.message.body[..], [0, 1, 2, 255]);
    consumer.acknowledge(&d).await.unwrap();

    for i in 0..200 {
        let (d, text) = receive_text(&consumer).await;
        assert!(text.starts_with(&format!("{i}:")));
        assert!(!d.message.durable);
        consumer.acknowledge(&d).await.unwrap();
    }
    assert!(consumer.receive_immediate().await.unwrap().is_none());
    wait_for_count(&session, &queue, 0).await;

    consumer.close().await;
    producer.close();
    session.delete_queue(&queue).await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn broker_errors_keep_their_code() {
    let Some(addr) = broker() else { return };
    let conn = connect(&addr).await;
    let session = conn.create_session(auto_ack()).await.unwrap();
    let err = session
        .create_consumer(&unique("missing"), None, false)
        .await
        .err()
        .expect("no such queue");
    assert_eq!(err.kind(), Some(ErrorType::QueueDoesNotExist), "{err}");

    let key = unique("unique-meta");
    session.add_unique_metadata(&key, "a").await.unwrap();
    let other = conn.create_session(auto_ack()).await.unwrap();
    let err = other.add_unique_metadata(&key, "a").await.unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::DuplicateMetadata), "{err}");

    // The session is still usable after an error response.
    let queue = unique("after-error");
    anycast_queue(&session, &queue).await;
    let err = session
        .create_queue(QueueConfiguration::new(&queue).routing_type(RoutingType::Anycast))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::QueueExists), "{err}");
    session.delete_queue(&queue).await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn multicast_two_subscriptions() {
    let Some(addr) = broker() else { return };
    let conn = connect(&addr).await;
    let session = conn.create_session(auto_ack()).await.unwrap();
    let address = unique("topic");
    session
        .create_address(&address, &[RoutingType::Multicast], false)
        .await
        .unwrap();
    let (sub1, sub2) = (format!("{address}.sub1"), format!("{address}.sub2"));
    for sub in [&sub1, &sub2] {
        session
            .create_queue(
                QueueConfiguration::new(sub.as_str())
                    .address(&address)
                    .routing_type(RoutingType::Multicast)
                    .durable(false),
            )
            .await
            .unwrap();
    }
    let a = session.address_query(&address).await.unwrap();
    assert!(a.supports_multicast);
    assert_eq!(a.queue_names.len(), 2);

    let c1 = session.create_consumer(&sub1, None, false).await.unwrap();
    let c2 = session.create_consumer(&sub2, None, false).await.unwrap();
    session.start().await.unwrap();
    let producer = session.create_producer(None).await.unwrap();
    for i in 0..3 {
        producer
            .send_to(&address, message::text_message(&format!("m{i}")))
            .await
            .unwrap();
    }
    for c in [&c1, &c2] {
        for i in 0..3 {
            let (d, text) = receive_text(c).await;
            assert_eq!(text, format!("m{i}"));
            c.acknowledge(&d).await.unwrap();
        }
        assert!(c.receive_immediate().await.unwrap().is_none());
    }
    c1.close().await;
    c2.close().await;
    session.delete_queue(&sub1).await.unwrap();
    session.delete_queue(&sub2).await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn transacted_commit_and_rollback() {
    let Some(addr) = broker() else { return };
    let conn = connect(&addr).await;
    let admin = conn.create_session(auto_ack()).await.unwrap();
    let queue = unique("tx");
    anycast_queue(&admin, &queue).await;

    let tx = conn
        .create_session(SessionOptions::transacted())
        .await
        .unwrap();
    let producer = tx.create_producer(Some(&queue)).await.unwrap();
    assert!(!producer.is_block_on_durable_send());
    producer.send(message::text_message("one")).await.unwrap();
    producer.send(message::text_message("two")).await.unwrap();
    // Uncommitted sends are not on the queue.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(admin.queue_query(&queue).await.unwrap().message_count, 0);
    tx.commit().await.unwrap();
    wait_for_count(&admin, &queue, 2).await;

    let consumer = tx.create_consumer(&queue, None, false).await.unwrap();
    tx.start().await.unwrap();
    for expected in ["one", "two"] {
        let (d, text) = receive_text(&consumer).await;
        assert_eq!(text, expected);
        assert_eq!(d.delivery_count, 1);
        consumer.acknowledge(&d).await.unwrap();
    }
    tx.rollback(false).await.unwrap();

    // Redelivered with the delivery count bumped.
    for expected in ["one", "two"] {
        let (d, text) = receive_text(&consumer).await;
        assert_eq!(text, expected);
        assert_eq!(d.delivery_count, 2);
        consumer.acknowledge(&d).await.unwrap();
    }
    tx.commit().await.unwrap();
    wait_for_count(&admin, &queue, 0).await;
    assert!(consumer.receive_immediate().await.unwrap().is_none());

    tx.close().await;
    admin.delete_queue(&queue).await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn cumulative_and_individual_acks() {
    let Some(addr) = broker() else { return };
    let conn = connect(&addr).await;
    let session = conn.create_session(auto_ack()).await.unwrap();
    let queue = unique("acks");
    anycast_queue(&session, &queue).await;
    let producer = session.create_producer(Some(&queue)).await.unwrap();
    for i in 1..=5 {
        producer
            .send(message::text_message(&i.to_string()))
            .await
            .unwrap();
    }
    session.start().await.unwrap();

    // A cumulative ack of the third message acks 1..=3.
    let consumer = session.create_consumer(&queue, None, false).await.unwrap();
    let mut got = Vec::new();
    for _ in 0..5 {
        got.push(receive_text(&consumer).await);
    }
    consumer.acknowledge(&got[2].0).await.unwrap();
    consumer.close().await;
    wait_for_count(&session, &queue, 2).await;

    // Individually ack only the last one; the other comes back.
    let consumer = session.create_consumer(&queue, None, false).await.unwrap();
    let (d4, t4) = receive_text(&consumer).await;
    let (d5, t5) = receive_text(&consumer).await;
    assert_eq!((t4.as_str(), t5.as_str()), ("4", "5"));
    // A consumer closing normally does not count its unacked messages as
    // delivered, so the delivery count is not checked here.
    assert!(d4.delivery_count >= 1);
    consumer.individual_acknowledge(&d5).await.unwrap();
    consumer.close().await;
    wait_for_count(&session, &queue, 1).await;

    let consumer = session.create_consumer(&queue, None, false).await.unwrap();
    let (d, text) = receive_text(&consumer).await;
    assert_eq!(text, "4");
    consumer.acknowledge(&d).await.unwrap();
    consumer.close().await;
    wait_for_count(&session, &queue, 0).await;

    session.delete_queue(&queue).await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn browse_only() {
    let Some(addr) = broker() else { return };
    let conn = connect(&addr).await;
    let session = conn.create_session(auto_ack()).await.unwrap();
    let queue = unique("browse");
    anycast_queue(&session, &queue).await;
    let producer = session.create_producer(Some(&queue)).await.unwrap();
    for i in 0..3 {
        producer
            .send(message::text_message(&format!("b{i}")))
            .await
            .unwrap();
    }
    session.start().await.unwrap();

    let browser = session.create_consumer(&queue, None, true).await.unwrap();
    assert!(browser.is_browse_only());
    for i in 0..3 {
        let (_, text) = receive_text(&browser).await;
        assert_eq!(text, format!("b{i}"));
    }
    assert!(browser.receive_immediate().await.unwrap().is_none());
    browser.close().await;
    assert_eq!(session.queue_query(&queue).await.unwrap().message_count, 3);

    let consumer = session.create_consumer(&queue, None, false).await.unwrap();
    for i in 0..3 {
        let (d, text) = receive_text(&consumer).await;
        assert_eq!(text, format!("b{i}"));
        assert_eq!(d.delivery_count, 1);
        consumer.acknowledge(&d).await.unwrap();
    }
    consumer.close().await;
    wait_for_count(&session, &queue, 0).await;
    session.delete_queue(&queue).await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn consumer_with_filter() {
    let Some(addr) = broker() else { return };
    let conn = connect(&addr).await;
    let session = conn.create_session(auto_ack()).await.unwrap();
    let queue = unique("filter");
    anycast_queue(&session, &queue).await;
    let producer = session.create_producer(Some(&queue)).await.unwrap();
    for (i, colour) in ["red", "blue", "red", "green"].iter().enumerate() {
        let mut m = message::text_message(&format!("{colour}{i}"));
        m.put_string_property("colour", colour);
        producer.send(m).await.unwrap();
    }
    session.start().await.unwrap();

    let reds = session
        .create_consumer(&queue, Some("colour = 'red'"), false)
        .await
        .unwrap();
    assert_eq!(reds.filter().as_deref(), Some("colour = 'red'"));
    for expected in ["red0", "red2"] {
        let (d, text) = receive_text(&reds).await;
        assert_eq!(text, expected);
        reds.acknowledge(&d).await.unwrap();
    }
    assert!(reds.receive_immediate().await.unwrap().is_none());
    reds.close().await;
    wait_for_count(&session, &queue, 2).await;

    let rest = session.create_consumer(&queue, None, false).await.unwrap();
    for expected in ["blue1", "green3"] {
        let (d, text) = receive_text(&rest).await;
        assert_eq!(text, expected);
        rest.acknowledge(&d).await.unwrap();
    }
    rest.close().await;
    session.delete_queue(&queue).await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn window_sizes() {
    let Some(addr) = broker() else { return };
    let conn = connect(&addr).await;
    let session = conn.create_session(auto_ack()).await.unwrap();
    session.start().await.unwrap();

    for (window, initial, client) in [
        (None, 1024 * 1024, 512 * 1024),
        (Some(-1), -1, -1),
        (Some(0), 0, 0),
        (Some(1), 1, 1),
        (Some(4096), 4096, 2048),
    ] {
        let queue = unique("window");
        anycast_queue(&session, &queue).await;
        let producer = session.create_producer(Some(&queue)).await.unwrap();
        for i in 0..10 {
            producer
                .send(message::text_message(&format!("w{i}")))
                .await
                .unwrap();
        }
        let consumer = session
            .create_consumer_with(
                &queue,
                ConsumerOptions {
                    window_size: window,
                    ..ConsumerOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(consumer.initial_window_size(), initial);
        assert_eq!(consumer.client_window_size(), client);

        tokio::time::sleep(Duration::from_millis(300)).await;
        let buffered = consumer.buffered();
        match window {
            // A slow consumer gets nothing until it asks.
            Some(0) => assert_eq!(buffered, 0),
            Some(1) => assert!(buffered <= 1, "window 1 buffered {buffered}"),
            _ => assert_eq!(buffered, 10, "window {window:?}"),
        }
        for i in 0..10 {
            let (d, text) = receive_text(&consumer).await;
            assert_eq!(text, format!("w{i}"));
            consumer.acknowledge(&d).await.unwrap();
            if window == Some(0) {
                // At most the one message its returned credits allow.
                assert!(consumer.buffered() <= 1);
            }
        }
        assert!(consumer.receive_immediate().await.unwrap().is_none());
        consumer.close().await;
        wait_for_count(&session, &queue, 0).await;
        producer.close();
        session.delete_queue(&queue).await.unwrap();
    }
    conn.close().await;
}

#[tokio::test]
async fn closed_connection_fails_calls() {
    let Some(addr) = broker() else { return };
    let conn = connect(&addr).await;
    let session = conn.create_session(auto_ack()).await.unwrap();
    conn.close().await;
    let err = session.queue_query("anything").await.unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::NotConnected), "{err}");
}
