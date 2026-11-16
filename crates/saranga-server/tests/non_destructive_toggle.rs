//! A queue made destructive or non-destructive (`updateQueue`) while it has
//! deliveries out: each delivery is acknowledged for what it was when it
//! was sent. A copy a non-destructive queue sent leaves the message, a
//! reference a destructive queue sent takes it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_message::CoreMessage;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

#[derive(Default)]
struct Sink(Mutex<Vec<i64>>);

impl DeliverySink for Sink {
    fn deliver(&self, _: i64, message: &Arc<ServerMessage>, _: i32) -> i64 {
        self.0.lock().unwrap().push(message.message_id());
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
}

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

fn spec(non_destructive: bool) -> QueueSpec {
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.non_destructive = Some(non_destructive);
    spec
}

async fn delivered(sink: &Sink, n: usize) -> Vec<i64> {
    for _ in 0..300 {
        if sink.0.lock().unwrap().len() >= n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let ids = sink.0.lock().unwrap().clone();
    assert_eq!(ids.len(), n);
    ids
}

#[tokio::test]
async fn deliveries_are_acknowledged_for_what_they_were_when_sent() {
    for starts_non_destructive in [true, false] {
        let b = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
            .await
            .unwrap();
        b.post_office()
            .create_queue(spec(starts_non_destructive), false)
            .unwrap();
        let mut s = session(&b);
        s.send(
            CoreMessage {
                address: Some("q".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        s.take_completion().wait().await.unwrap();
        let sink = Arc::new(Sink::default());
        let c = sink.clone();
        s.create_consumer(1, "q", None, 0, false, move |_| c.clone())
            .await
            .unwrap();
        s.consumer_credits(1, -1);
        s.set_started(true).await;
        let ids = delivered(&sink, 1).await;
        b.post_office()
            .update_queue(spec(!starts_non_destructive))
            .unwrap();
        s.acknowledge(1, ids[0], true).await.unwrap();
        s.take_completion().wait().await.unwrap();
        let q = b.post_office().queue("q").unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let expected = if starts_non_destructive { 1 } else { 0 };
        assert_eq!(
            q.message_count(),
            expected,
            "started non-destructive: {starts_non_destructive}"
        );
        b.stop().unwrap();
    }
}

/// A queue made non-exclusive forgets its exclusive consumer
/// (`setExclusive(false)`): made exclusive again, it picks one anew, and
/// that consumer's first message is flagged (`group-first-key`).
#[tokio::test]
async fn a_queue_made_non_exclusive_forgets_its_exclusive_consumer() {
    let b = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let spec = |exclusive: bool| {
        let mut spec = QueueSpec::new("q", "q");
        spec.auto_create_address = Some(true);
        spec.exclusive = Some(exclusive);
        spec.group_first_key = Some("first".into());
        spec
    };
    b.post_office().create_queue(spec(true), false).unwrap();
    let mut s = session(&b);
    let firsts = Arc::new(FirstSink::default());
    let c = firsts.clone();
    s.create_consumer(1, "q", None, 0, false, move |_| c.clone())
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;
    let send = async |s: &mut ServerSession| {
        s.send(
            CoreMessage {
                address: Some("q".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        s.take_completion().wait().await.unwrap();
    };
    send(&mut s).await;
    for _ in 0..300 {
        if firsts.0.lock().unwrap().len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    b.post_office().update_queue(spec(false)).unwrap();
    b.post_office().update_queue(spec(true)).unwrap();
    send(&mut s).await;
    for _ in 0..300 {
        if firsts.0.lock().unwrap().len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(*firsts.0.lock().unwrap(), [true, true]);
    b.stop().unwrap();
}

/// Whether each delivery carried the `first` flag.
#[derive(Default)]
struct FirstSink(Mutex<Vec<bool>>);

impl DeliverySink for FirstSink {
    fn deliver(&self, _: i64, message: &Arc<ServerMessage>, _: i32) -> i64 {
        let first = message
            .core()
            .properties
            .get(&saranga_buffer::SimpleString::from("first"))
            .is_some();
        self.0.lock().unwrap().push(first);
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
}
