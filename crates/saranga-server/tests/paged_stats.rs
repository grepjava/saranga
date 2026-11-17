//! A queue's counts and sizes take a depaged message once: the page
//! subscription counts it until it is acknowledged, so the delivering and
//! scheduled messages added to it are only those never paged, as Artemis's
//! `getNonPaged*` metrics are.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::management::Json;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

#[derive(Default)]
struct Sink(Mutex<usize>);

impl DeliverySink for Sink {
    fn deliver(&self, _: i64, _: &Arc<ServerMessage>, _: i32) -> i64 {
        *self.0.lock().unwrap() += 1;
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

async fn start(dir: &std::path::Path) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <address-settings><address-setting match="q">
               <max-size-bytes>1</max-size-bytes>
               <page-size-bytes>10240</page-size-bytes>
               <address-full-policy>PAGE</address-full-policy>
             </address-setting></address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let b = Broker::start(
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap(),
    )
    .await
    .unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.durable = true;
    b.post_office().create_queue(spec, false).unwrap();
    b
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

async fn send(s: &mut ServerSession, scheduled: bool) {
    let mut m = CoreMessage {
        address: Some("q".into()),
        durable: true,
        body: vec![1; 100].into(),
        ..Default::default()
    };
    if scheduled {
        let later = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 3_600_000;
        m.properties.insert(
            SimpleString::from("_AMQ_SCHED_DELIVERY"),
            PropertyValue::Long(later),
        );
    }
    s.send(m, None).await.unwrap();
    s.take_completion().wait().await.unwrap();
}

async fn attr(b: &Arc<Broker>, name: &str) -> i64 {
    let v = b
        .management()
        .attribute(b, "queue.q", name, Default::default())
        .await
        .unwrap_or_else(|e| panic!("{name}: {e}"));
    match v {
        Json::Long(n) => n,
        other => panic!("{name}: {other:?}"),
    }
}

#[tokio::test]
async fn depaged_delivering_and_scheduled_messages_are_counted_once() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path()).await;
    let mut s = session(&b);
    // The first stays in memory; the next two are paged, the last of them
    // scheduled for later.
    send(&mut s, false).await;
    send(&mut s, false).await;
    send(&mut s, true).await;
    assert_eq!(attr(&b, "MessageCount").await, 3);
    assert_eq!(attr(&b, "DurableMessageCount").await, 3);
    let size = attr(&b, "PersistentSize").await;
    assert!(size > 0);
    assert_eq!(attr(&b, "DurablePersistentSize").await, size);

    let sink = Arc::new(Sink::default());
    let c = sink.clone();
    s.create_consumer(1, "q", None, 0, false, move |_| c.clone())
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;
    for _ in 0..500 {
        if *sink.0.lock().unwrap() == 2 && attr(&b, "ScheduledCount").await == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(*sink.0.lock().unwrap(), 2, "both unscheduled delivered");
    assert_eq!(
        attr(&b, "ScheduledCount").await,
        1,
        "the scheduled one depaged"
    );
    assert_eq!(attr(&b, "DeliveringCount").await, 2);

    assert_eq!(attr(&b, "MessageCount").await, 3);
    assert_eq!(attr(&b, "DurableMessageCount").await, 3);
    assert_eq!(attr(&b, "PersistentSize").await, size);
    assert_eq!(attr(&b, "DurablePersistentSize").await, size);
    s.close(false).await;
    b.stop().unwrap();
}
