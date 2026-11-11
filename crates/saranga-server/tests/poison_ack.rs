//! A poison acknowledgement (OpenWire names the failure's cause) adds
//! properties to the message before dead-lettering it. The message is
//! immutable, so it is rebuilt; the rebuilt message must keep the reference's
//! accounting, or acknowledging it decrements empty counters and leaks the
//! address memory (and journal record) the reference charged.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

struct Sink(Mutex<Vec<i64>>);

impl DeliverySink for Sink {
    fn deliver(&self, _: i64, message: &Arc<ServerMessage>, _: i32) -> i64 {
        self.0.lock().unwrap().push(message.message_id());
        1
    }
    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        unreachable!()
    }
    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        unreachable!()
    }
    fn chunk_size(&self) -> usize {
        100 * 1024
    }
}

fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("poison"),
        Arc::new(Default::default()),
        vec![],
    ));
    ServerSession::new(
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
    )
}

async fn wait_for(what: impl Fn() -> bool) {
    for _ in 0..200 {
        if what() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out");
}

/// A message rejected with a failure cause and dropped (no dead-letter address)
/// frees the address memory it charged, rather than leaving it charged because
/// the rebuilt message that carried the cause had empty counters.
#[tokio::test]
async fn a_poison_acknowledged_message_frees_its_memory() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "a");
    spec.auto_create_address = Some(true);
    // No dead-letter address: the poison-acked message is dropped, so the
    // queue's address memory must return to where it started.
    let q = broker.post_office().create_queue(spec, false).unwrap();

    let sink = Arc::new(Sink(Mutex::new(Vec::new())));
    let consumer_sink = sink.clone();
    let mut s = session(&broker);
    s.create_consumer(1, "q", None, 0, false, move |_| consumer_sink.clone())
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;

    let base = broker.post_office().paging().global_size();
    let m = CoreMessage {
        message_id: broker.post_office().ids().next(),
        address: Some(SimpleString::from("a")),
        ..Default::default()
    };
    let id = m.message_id;
    s.send_with_id(m).await.unwrap();
    wait_for(|| sink.0.lock().unwrap().contains(&id)).await;
    assert!(
        broker.post_office().paging().global_size() > base,
        "a delivered message charges its address memory"
    );

    // OpenWire's poison acknowledgement, naming the failure's cause.
    q.reject_with(
        1,
        id,
        vec![(
            "_AMQ_POISON_CAUSE".to_owned(),
            PropertyValue::String(SimpleString::from("boom")),
        )],
    );
    wait_for(|| q.message_count() == 0).await;

    assert_eq!(
        broker.post_office().paging().global_size(),
        base,
        "a poison-acknowledged, dropped message must free its address memory"
    );
    s.close(false).await;
    broker.stop().unwrap();
}

/// A broker whose addresses dead-letter to `DLA` and auto-create their
/// dead-letter resource queues.
async fn start_with_dla(dir: &std::path::Path) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r##"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>false</persistence-enabled>
             <security-enabled>false</security-enabled>
             <address-settings>
               <address-setting match="#">
                 <dead-letter-address>DLA</dead-letter-address>
                 <auto-create-dead-letter-resources>true</auto-create-dead-letter-resources>
                 <dead-letter-queue-prefix>DLQ.</dead-letter-queue-prefix>
                 <dead-letter-queue-suffix></dead-letter-queue-suffix>
               </address-setting>
             </address-settings>
           </core></configuration>"##,
    )
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

/// Dead-lettering a message whose address contains an apostrophe creates the
/// dead-letter resource queue and routes the message to it. The address goes
/// into the resource queue's selector as a string literal, so an apostrophe in
/// it must be doubled or the filter does not parse, the resource queue is never
/// created, and the message is dropped for want of a binding.
#[tokio::test]
async fn an_apostrophe_address_dead_letters_through_its_resource_queue() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_with_dla(dir.path()).await;

    let mut spec = QueueSpec::new("q", "orders'west");
    spec.auto_create_address = Some(true);
    let q = broker.post_office().create_queue(spec, false).unwrap();

    let sink = Arc::new(Sink(Mutex::new(Vec::new())));
    let consumer_sink = sink.clone();
    let mut s = session(&broker);
    s.create_consumer(1, "q", None, 0, false, move |_| consumer_sink.clone())
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;

    let m = CoreMessage {
        message_id: broker.post_office().ids().next(),
        address: Some(SimpleString::from("orders'west")),
        ..Default::default()
    };
    let id = m.message_id;
    s.send_with_id(m).await.unwrap();
    wait_for(|| sink.0.lock().unwrap().contains(&id)).await;

    q.reject_with(
        1,
        id,
        vec![(
            "_AMQ_POISON_CAUSE".to_owned(),
            PropertyValue::String(SimpleString::from("boom")),
        )],
    );

    // The message reaches the dead-letter resource queue, which only exists if
    // its apostrophe-bearing selector parsed.
    wait_for(|| {
        broker
            .post_office()
            .queue("DLQ.orders'west")
            .is_some_and(|dlq| dlq.message_count() == 1)
    })
    .await;

    s.close(false).await;
    broker.stop().unwrap();
}
