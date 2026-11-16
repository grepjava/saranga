//! A forced delivery (`forceDelivery`) waits behind a large message being
//! sent in chunks. A rollback that takes the large message back gives up
//! its transfer: the forced delivery goes then, rather than wait for the
//! next large message to finish, or for ever.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_message::CoreMessage;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::{DeliverySink, FORCED_DELIVERY_MESSAGE};
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

#[derive(Default)]
struct Sink {
    forced: Mutex<Vec<i64>>,
    large: Mutex<usize>,
}

impl DeliverySink for Sink {
    fn deliver(&self, _: i64, message: &Arc<ServerMessage>, _: i32) -> i64 {
        if let Some(saranga_buffer::PropertyValue::Long(seq)) = message
            .core()
            .properties
            .get(&FORCED_DELIVERY_MESSAGE.into())
        {
            self.forced.lock().unwrap().push(*seq);
        }
        1
    }
    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        *self.large.lock().unwrap() += 1;
        1000
    }
    fn deliver_continuation(&self, _: i64, chunk: Vec<u8>, _: bool) -> i64 {
        chunk.len() as i64
    }
    fn chunk_size(&self) -> usize {
        1000
    }
}

#[tokio::test]
async fn a_rollback_mid_transfer_sends_the_forced_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
           </core></configuration>"#,
    )
    .unwrap();
    let b = Broker::start(
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap(),
    )
    .await
    .unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    b.post_office().create_queue(spec, false).unwrap();
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        vec![],
    ));
    let mut s = ServerSession::new(
        b.clone(),
        conn,
        SessionOptions {
            name: "s".into(),
            username: None,
            password: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: false,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    );
    s.send(
        CoreMessage {
            address: Some("q".into()),
            durable: true,
            body: vec![9u8; 300_000].into(),
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
    // Enough for the start of the transfer only.
    s.consumer_credits(1, 3000);
    s.set_started(true).await;
    for _ in 0..300 {
        if *sink.large.lock().unwrap() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(*sink.large.lock().unwrap(), 1, "the transfer started");
    s.force_delivery(1, 7);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(sink.forced.lock().unwrap().is_empty(), "waits behind it");
    s.rollback(false, false).await;
    for _ in 0..300 {
        if !sink.forced.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(*sink.forced.lock().unwrap(), [7]);
    b.stop().unwrap();
}
