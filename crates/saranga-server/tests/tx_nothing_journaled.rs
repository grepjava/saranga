//! A transaction that journals nothing has nothing to commit or roll back:
//! a duplicate ID under `id-cache-size` 0, or the acknowledgement of a paged
//! message on a non-durable queue, does not make it look persistent.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_buffer::{PropertyValue, SimpleString, TypedProperties};
use saranga_config as cfg;
use saranga_message::CoreMessage;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::transaction::Xid;
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &Path, core: &str) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
             {core}
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

fn session(broker: &Arc<Broker>, xa: bool) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "s".into(),
            username: None,
            password: None,
            xa,
            auto_commit_sends: false,
            auto_commit_acks: false,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

fn non_durable_queue(broker: &Arc<Broker>, name: &str) -> saranga_server::queue::Queue {
    let mut spec = QueueSpec::new(name, name);
    spec.auto_create_address = Some(true);
    spec.durable = false;
    broker.post_office().create_queue(spec, false).unwrap()
}

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

/// `id-cache-size` 0 journals no duplicate ID: a transaction sending a
/// non-durable message with one commits.
#[tokio::test]
async fn a_duplicate_id_with_no_cache_writes_nothing_to_commit() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(
        dir.path(),
        "<id-cache-size>0</id-cache-size><persist-id-cache>true</persist-id-cache>",
    )
    .await;
    let queue = non_durable_queue(&broker, "q");
    let mut s = session(&broker, false);
    let mut properties = TypedProperties::default();
    properties.insert(
        SimpleString::from("_AMQ_DUPL_ID"),
        PropertyValue::String(SimpleString::from("d0")),
    );
    s.send(
        CoreMessage {
            address: Some("q".into()),
            durable: false,
            properties,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    s.commit().expect("nothing journaled, nothing to fail");
    s.take_completion().wait().await.unwrap();
    assert_eq!(queue.message_count(), 1);
    broker.stop().unwrap();
}

/// A non-durable queue's paged message acknowledged in an XA transaction
/// that rolls back: nothing was journaled for it, the rollback succeeds and
/// the message is the queue's again.
#[tokio::test]
async fn a_rolled_back_paged_ack_on_a_non_durable_queue_keeps_the_message() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(
        dir.path(),
        r#"<address-settings><address-setting match="pg">
             <max-size-bytes>1</max-size-bytes>
             <page-size-bytes>4096</page-size-bytes>
             <address-full-policy>PAGE</address-full-policy>
           </address-setting></address-settings>"#,
    )
    .await;
    let queue = non_durable_queue(&broker, "pg");
    let mut producer = session(&broker, false);
    for i in 0..2u8 {
        producer
            .send(
                CoreMessage {
                    address: Some("pg".into()),
                    durable: false,
                    body: vec![i; 300].into(),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
    }
    producer.commit().unwrap();
    producer.take_completion().wait().await.unwrap();

    let mut c = session(&broker, true);
    let xid = Xid {
        format_id: 1,
        branch_qualifier: b"nd".to_vec(),
        global_transaction_id: b"nd".to_vec(),
    };
    c.xa_start(xid.clone()).unwrap();
    let sink = Arc::new(Sink::default());
    let s2 = sink.clone();
    c.create_consumer(1, "pg", None, 0, false, move |_| s2.clone())
        .await
        .unwrap();
    c.consumer_credits(1, 2);
    c.set_started(true).await;
    for _ in 0..300 {
        if sink.0.lock().unwrap().len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let paged = sink.0.lock().unwrap()[1];
    c.acknowledge(1, paged, true).await.unwrap();
    c.xa_end(xid.clone()).unwrap();
    c.xa_rollback(xid)
        .await
        .expect("nothing journaled, nothing to fail");
    c.close(false).await;
    assert_eq!(queue.message_count(), 2);
    broker.stop().unwrap();
}
