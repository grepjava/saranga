//! A management move that fails on a message still in its page leaves it
//! there (`move` before `iter.remove()`): the queue reads it in its turn,
//! once, after the messages before it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_config as cfg;
use saranga_message::CoreMessage;
use saranga_server::management::Json;
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

#[tokio::test]
async fn a_failed_move_of_a_paged_message_leaves_it_in_its_page() {
    let dir = tempfile::tempdir().unwrap();
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
             <address-settings>
               <address-setting match="src">
                 <max-size-bytes>1</max-size-bytes>
                 <page-size-bytes>256</page-size-bytes>
                 <max-read-page-messages>1</max-read-page-messages>
                 <address-full-policy>PAGE</address-full-policy>
               </address-setting>
               <address-setting match="dst">
                 <max-size-bytes>10</max-size-bytes>
                 <address-full-policy>FAIL</address-full-policy>
               </address-setting>
             </address-settings>
           </core></configuration>"#,
    )
    .unwrap();
    let config =
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
    let broker = Broker::start(config).await.unwrap();
    for name in ["src", "dst"] {
        let mut spec = QueueSpec::new(name, name);
        spec.auto_create_address = Some(true);
        spec.durable = true;
        broker.post_office().create_queue(spec, false).unwrap();
    }
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("test"),
        Arc::new(Default::default()),
        Vec::new(),
    ));
    let mut s = ServerSession::new(
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
    );
    let send = |address: &str, size: usize| CoreMessage {
        address: Some(address.into()),
        durable: true,
        body: vec![1; size].into(),
        ..Default::default()
    };
    // dst full: a move to it fails.
    s.send(send("dst", 100), None).await.unwrap();
    s.take_completion().wait().await.unwrap();
    for _ in 0..3 {
        s.send(send("src", 300), None).await.unwrap();
        s.take_completion().wait().await.unwrap();
    }
    let _ = broker
        .management()
        .invoke(
            &broker,
            "queue.src",
            "moveMessages",
            &[Json::from(""), Json::from("dst")],
            Default::default(),
        )
        .await;

    let sink = Arc::new(Sink::default());
    let c = sink.clone();
    s.create_consumer(1, "src", None, 0, false, move |_| c.clone())
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;
    let mut acked = 0;
    for _ in 0..300 {
        let next = sink.0.lock().unwrap().get(acked).copied();
        match next {
            Some(id) => {
                s.acknowledge(1, id, true).await.unwrap();
                acked += 1;
            }
            None => tokio::time::sleep(Duration::from_millis(10)).await,
        }
        if acked >= 3 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if sink.0.lock().unwrap().len() == acked {
                break;
            }
        }
    }
    let got = sink.0.lock().unwrap().clone();
    let mut sorted = got.clone();
    sorted.sort();
    assert_eq!(got, sorted, "delivered in the order sent");
    assert_eq!(got.len(), 3, "each once: {got:?}");
    assert_eq!(
        broker.post_office().queue("src").unwrap().message_count(),
        0
    );
    broker.stop().unwrap();
}
