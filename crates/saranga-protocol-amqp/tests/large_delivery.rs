//! A large AMQP message queued with only its headers in memory is delivered
//! whole: the queue reads its body back from the file and rebuilds the message
//! before handing it to a consumer that cannot take it in chunks.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_buffer::SimpleString;
use saranga_protocol_amqp::message::AmqpMessage;
use saranga_protocol_amqp::native::AmqpNative;
use saranga_server::large::LargeMessageStore;
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

const NOW: i64 = 1_750_000_000_000;

fn data_message(body: &[u8]) -> Vec<u8> {
    let mut v = vec![0x00, 0x53, 0x75, 0xB0];
    v.extend_from_slice(&(body.len() as u32).to_be_bytes());
    v.extend_from_slice(body);
    v
}

fn has_run(haystack: &[u8], byte: u8, len: usize) -> bool {
    haystack.windows(len).any(|w| w.iter().all(|&b| b == byte))
}

/// A sink that keeps what it is handed and takes large messages whole, as a
/// non-Core protocol does.
struct Capture(Arc<Mutex<Vec<Arc<ServerMessage>>>>);
impl DeliverySink for Capture {
    fn deliver(&self, _: i64, message: &Arc<ServerMessage>, _: i32) -> i64 {
        self.0.lock().unwrap().push(message.clone());
        1
    }
    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        0
    }
    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        0
    }
    fn chunk_size(&self) -> usize {
        1024
    }
    fn supports_large(&self) -> bool {
        false
    }
}

fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("large"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "large".into(),
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
async fn a_slim_large_message_is_delivered_whole() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "a");
    spec.auto_create_address = Some(true);
    spec.durable = false;
    let q = broker.post_office().create_queue(spec, false).unwrap();

    // A large native message whose body lives in a file, keeping only its
    // headers in memory, as the receive path now queues it.
    let body = vec![0xCDu8; 200_000];
    let mut full = AmqpMessage::new(0, data_message(&body), NOW).unwrap();
    full.set_message_id(1);
    full.set_address(SimpleString::from("a"));
    full.make_large();
    let store = LargeMessageStore::in_memory();
    let mut w = store.create(full.message_id(), true).unwrap();
    w.append(full.data()).unwrap();
    let large_body = w.finish().unwrap();
    let slim = AmqpMessage::from_large_record(&full.large_record(), Vec::new(), NOW).unwrap();
    assert!(
        slim.data().len() < 4096,
        "the queued message holds only headers"
    );
    let msg = ServerMessage::new_native_large(AmqpNative::arc(slim), large_body);

    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut session = session(&broker);
    let c = captured.clone();
    session
        .create_consumer(1, "q", None, 0, false, move |_| {
            Arc::new(Capture(c.clone()))
        })
        .await
        .unwrap();
    session.consumer_credits(1, -1);
    session.set_started(true).await;
    q.add(MessageRef::new(msg));
    session.prompt_delivery(1);

    for _ in 0..200 {
        if !captured.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !captured.lock().unwrap().is_empty(),
        "no delivery: consumers={} queued={}",
        q.consumer_count(),
        q.message_count(),
    );
    let delivered = captured.lock().unwrap().pop().expect("a delivery");
    let native = delivered
        .native()
        .and_then(|n| n.as_any().downcast_ref::<AmqpNative>())
        .expect("a native AMQP message");
    assert!(
        has_run(&native.message.send_buffer(1), 0xCD, body.len()),
        "the delivered message carries its whole body"
    );
    broker.stop().unwrap();
}
