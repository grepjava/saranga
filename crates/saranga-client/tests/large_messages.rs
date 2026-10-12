//! Large messages through an in-process Saranga broker: sent in chunks by
//! the producer, stored by the broker, delivered in chunks and received
//! whole.

use std::sync::Arc;
use std::time::Duration;

use saranga_client::message::{self, HDR_LARGE_COMPRESSED};
use saranga_client::*;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::net::TcpListener;

const WAIT: Duration = Duration::from_secs(10);

async fn start_broker() -> String {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(broker.serve_acceptor(listener, Acceptor::default()));
    address.to_string()
}

/// A connection and a session with acks sent right away, and a queue.
async fn setup(options: SessionOptions, queue: &str) -> (Connection, Session) {
    let address = start_broker().await;
    let conn = Connection::connect(&address, ConnectionOptions::default())
        .await
        .unwrap();
    let session = conn.create_session(options).await.unwrap();
    session
        .create_queue(
            QueueConfiguration::new(queue)
                .routing_type(RoutingType::Anycast)
                .durable(false),
        )
        .await
        .unwrap();
    (conn, session)
}

fn auto_ack() -> SessionOptions {
    SessionOptions {
        ack_batch_size: 0,
        ..SessionOptions::default()
    }
}

fn patterned(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 % 251) as u8).collect()
}

/// Bytes that do not compress.
fn noise(len: usize) -> Vec<u8> {
    let mut x = 0x2545_f491u32;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect()
}

async fn receive(consumer: &Consumer) -> Delivery {
    consumer
        .receive(WAIT)
        .await
        .unwrap()
        .expect("a message within the timeout")
}

#[tokio::test]
async fn one_mib_round_trip() {
    let (conn, session) = setup(auto_ack(), "large").await;
    let producer = session.create_producer(Some("large")).await.unwrap();
    let body = patterned(1024 * 1024);
    let mut m = message::bytes_message(&body);
    m.put_string_property("color", "blue")
        .put_property("n", PropertyValue::Int(7))
        .set_new_user_id();
    let user_id = m.user_id;
    producer.send(m).await.unwrap();

    let consumer = session.create_consumer("large", None, false).await.unwrap();
    session.start().await.unwrap();
    let d = receive(&consumer).await;
    assert_eq!(d.message.body.len(), body.len());
    assert!(d.message.body == body, "body differs");
    assert_eq!(d.message.string_property("color").as_deref(), Some("blue"));
    assert_eq!(d.message.property("n"), Some(&PropertyValue::Int(7)));
    assert_eq!(d.message.user_id, user_id);
    assert!(d.message.durable);
    assert_eq!(d.message.message_type, message_type::BYTES);
    assert_eq!(d.delivery_count, 1);
    // The broker records the body size.
    assert_eq!(
        d.message.property(message::HDR_LARGE_BODY_SIZE),
        Some(&PropertyValue::Long(body.len() as i64))
    );
    consumer.acknowledge(&d).await.unwrap();
    assert!(
        consumer
            .receive(Duration::from_millis(100))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(session.queue_query("large").await.unwrap().message_count, 0);
    conn.close().await;
}

#[tokio::test]
async fn text_bigger_than_the_minimum() {
    let (conn, session) = setup(auto_ack(), "text").await;
    let producer = session.create_producer(Some("text")).await.unwrap();
    // UTF-16 on the wire: 300 KiB of body for 150 Ki characters.
    let text: String = (0..150 * 1024)
        .map(|i| char::from(b'a' + (i % 26) as u8))
        .collect();
    producer.send(message::text_message(&text)).await.unwrap();
    // A non-durable one too: the last chunk is not blocking.
    let mut m = message::text_message(&text[..120 * 1024]);
    m.set_durable(false);
    producer.send(m).await.unwrap();

    let consumer = session.create_consumer("text", None, false).await.unwrap();
    session.start().await.unwrap();
    let d = receive(&consumer).await;
    assert_eq!(d.message.text().unwrap().as_deref(), Some(text.as_str()));
    consumer.acknowledge(&d).await.unwrap();
    let d = receive(&consumer).await;
    assert!(!d.message.durable);
    assert_eq!(
        d.message.text().unwrap().as_deref(),
        Some(&text[..120 * 1024])
    );
    consumer.acknowledge(&d).await.unwrap();
    conn.close().await;
}

#[tokio::test]
async fn transacted_send_rollback_and_commit() {
    let (conn, session) = setup(SessionOptions::transacted(), "tx").await;
    let producer = session.create_producer(Some("tx")).await.unwrap();
    let body = patterned(400 * 1024);
    producer.send(message::bytes_message(&body)).await.unwrap();
    // Not visible until committed.
    assert_eq!(session.queue_query("tx").await.unwrap().message_count, 0);
    session.commit().await.unwrap();
    assert_eq!(session.queue_query("tx").await.unwrap().message_count, 1);

    let consumer = session.create_consumer("tx", None, false).await.unwrap();
    session.start().await.unwrap();
    let d = receive(&consumer).await;
    assert!(d.message.body == body);
    consumer.acknowledge(&d).await.unwrap();
    // Rolled back: delivered again, whole.
    session.rollback(false).await.unwrap();
    let d = receive(&consumer).await;
    assert!(d.message.body == body);
    assert_eq!(d.delivery_count, 2);
    consumer.acknowledge(&d).await.unwrap();
    session.commit().await.unwrap();
    assert_eq!(session.queue_query("tx").await.unwrap().message_count, 0);
    conn.close().await;
}

#[tokio::test]
async fn many_small_chunks_and_order() {
    // 1 KiB chunks both ways: hundreds of continuation packets per
    // message, through producer credits and a small consumer window.
    let options = SessionOptions {
        min_large_message_size: 1024,
        consumer_window_size: 8 * 1024,
        ..auto_ack()
    };
    let (conn, session) = setup(options, "mix").await;
    let producer = session.create_producer(Some("mix")).await.unwrap();
    let sizes = [10, 300 * 1024, 1020, 1021, 2048, 50, 100 * 1024 + 7, 5];
    for (i, &len) in sizes.iter().enumerate() {
        let mut m = message::bytes_message(&patterned(len));
        m.put_property("i", PropertyValue::Int(i as i32));
        if i % 2 == 1 {
            m.set_durable(false);
        }
        producer.send(m).await.unwrap();
    }

    let consumer = session.create_consumer("mix", None, false).await.unwrap();
    session.start().await.unwrap();
    for (i, &len) in sizes.iter().enumerate() {
        let d = receive(&consumer).await;
        assert_eq!(d.message.property("i"), Some(&PropertyValue::Int(i as i32)));
        assert_eq!(d.message.body.len(), len, "message {i}");
        assert!(d.message.body == patterned(len), "message {i}");
        consumer.acknowledge(&d).await.unwrap();
    }
    assert!(
        consumer
            .receive(Duration::from_millis(100))
            .await
            .unwrap()
            .is_none()
    );
    conn.close().await;
}

#[tokio::test]
async fn slow_consumer_receives_large_messages() {
    let options = SessionOptions {
        min_large_message_size: 4096,
        consumer_window_size: 0,
        ..auto_ack()
    };
    let (conn, session) = setup(options, "slow").await;
    let producer = session.create_producer(Some("slow")).await.unwrap();
    for len in [100_000, 10, 20_000] {
        producer
            .send(message::bytes_message(&patterned(len)))
            .await
            .unwrap();
    }
    let consumer = session.create_consumer("slow", None, false).await.unwrap();
    session.start().await.unwrap();
    for len in [100_000, 10, 20_000] {
        let d = receive(&consumer).await;
        assert!(d.message.body == patterned(len));
        consumer.acknowledge(&d).await.unwrap();
    }
    conn.close().await;
}

#[tokio::test]
async fn compressed_round_trip() {
    let options = SessionOptions {
        compress_large_messages: true,
        compression_level: 9,
        ..auto_ack()
    };
    let (conn, session) = setup(options, "zip").await;
    let producer = session.create_producer(Some("zip")).await.unwrap();
    // Compresses below the minimum: sent as a regular message.
    let small = patterned(1024 * 1024);
    producer.send(message::bytes_message(&small)).await.unwrap();
    // Does not compress: still large, chunks of compressed bytes.
    let big = noise(300 * 1024);
    let mut m = message::bytes_message(&big);
    m.put_string_property("kind", "noise");
    producer.send(m).await.unwrap();

    // A consumer on a session that does not compress inflates both.
    let other = conn.create_session(auto_ack()).await.unwrap();
    let consumer = other.create_consumer("zip", None, false).await.unwrap();
    other.start().await.unwrap();
    for (body, kind) in [(&small, None), (&big, Some("noise"))] {
        let d = receive(&consumer).await;
        assert_eq!(d.message.body.len(), body.len());
        assert!(&d.message.body == body);
        assert_eq!(d.message.string_property("kind").as_deref(), kind);
        assert_eq!(
            d.message.property(HDR_LARGE_COMPRESSED),
            Some(&PropertyValue::Bool(false))
        );
        consumer.acknowledge(&d).await.unwrap();
    }
    conn.close().await;
}
