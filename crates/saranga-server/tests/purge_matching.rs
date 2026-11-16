//! Purging a queue (`deleteMatchingReferences`) takes every message it
//! holds: the queued ones, those scheduled for later and those still in
//! pages. That is what `purge-on-no-consumers` does when the last consumer
//! leaves, and what MQTT does to a retained message or a session's queue.

use std::sync::Arc;
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::{Broker, BrokerConfig};

struct Sink;

impl DeliverySink for Sink {
    fn deliver(&self, _: i64, _: &Arc<ServerMessage>, _: i32) -> i64 {
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

async fn start(dir: &std::path::Path, purge_on_no_consumers: bool) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>102400</journal-file-size>
             <address-settings><address-setting match="paged">
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
    for name in ["q", "paged"] {
        let mut spec = QueueSpec::new(name, name);
        spec.auto_create_address = Some(true);
        spec.durable = true;
        spec.purge_on_no_consumers = Some(purge_on_no_consumers);
        b.post_office().create_queue(spec, false).unwrap();
    }
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

async fn send(s: &mut ServerSession, address: &str, scheduled: bool) {
    let mut m = CoreMessage {
        address: Some(address.into()),
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

async fn settled(q: &saranga_server::queue::Queue, n: i64) {
    for _ in 0..300 {
        if q.message_count() == n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(q.message_count(), n, "{}", q.name());
}

/// The last consumer leaving a `purge-on-no-consumers` queue takes the
/// scheduled message with the queued one, and the paged ones.
#[tokio::test]
async fn purge_on_no_consumers_takes_scheduled_and_paged_messages() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), true).await;
    let mut s = session(&b);
    for name in ["q", "paged"] {
        s.create_consumer(1, name, None, 0, false, |_| Arc::new(Sink))
            .await
            .unwrap();
        // Stopped: what is sent stays queued.
        s.consumer_credits(1, 0);
        send(&mut s, name, false).await;
        send(&mut s, name, false).await;
        send(&mut s, name, true).await;
        let q = b.post_office().queue(name).unwrap();
        settled(&q, 3).await;
        s.close_consumer(1, false).await;
        settled(&q, 0).await;
    }
    b.stop().unwrap();
}

/// `delete_matching` (an MQTT session's or retained message's cleanup)
/// and `take_matching` (a retained message replaced in a transaction)
/// reach scheduled and paged messages too.
#[tokio::test]
async fn delete_and_take_matching_reach_scheduled_and_paged_messages() {
    let dir = tempfile::tempdir().unwrap();
    let b = start(dir.path(), false).await;
    let mut s = session(&b);
    for name in ["q", "paged"] {
        send(&mut s, name, false).await;
        send(&mut s, name, true).await;
        let q = b.post_office().queue(name).unwrap();
        settled(&q, 2).await;
        assert_eq!(q.delete_matching(None).await, 2, "{name}");
        settled(&q, 0).await;

        send(&mut s, name, false).await;
        send(&mut s, name, true).await;
        settled(&q, 2).await;
        let taken = q.take_matching(None).await;
        assert_eq!(taken.len(), 2, "{name}");
        q.reinsert(taken);
        settled(&q, 2).await;
    }
    b.stop().unwrap();
}
