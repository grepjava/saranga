//! Acknowledgements a rolled back transaction kept (OpenWire, AMQP) go back
//! to their consumer ahead of what it was delivered since, in their order,
//! as `ServerConsumerImpl.backToDelivering` puts them (`addFirst`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_message::CoreMessage;
use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::transaction::Transaction;
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

/// Three delivered; the first two acknowledged in a transaction that rolls
/// back keeping them. A cumulative acknowledgement up to the second then
/// takes the first two, not the third.
#[tokio::test]
async fn a_cumulative_ack_after_kept_acks_leaves_the_later_delivery() {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.address_queue_scan_period = -1;
    let broker = Broker::start(config).await.unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    broker.post_office().create_queue(spec, false).unwrap();
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
            auto_commit_acks: false,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    );
    for i in 0..3u8 {
        s.send(
            CoreMessage {
                address: Some("q".into()),
                body: vec![i].into(),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    }
    let sink = Arc::new(Sink::default());
    let c = sink.clone();
    s.create_consumer(1, "q", None, 0, false, move |_| c.clone())
        .await
        .unwrap();
    s.consumer_credits(1, -1);
    s.set_started(true).await;
    for _ in 0..300 {
        if sink.0.lock().unwrap().len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let ids = sink.0.lock().unwrap().clone();
    assert_eq!(ids.len(), 3);
    let (m1, m2) = (ids[0], ids[1]);

    let tx = Transaction::new_ref(broker.post_office().ids().next(), None);
    s.acknowledge_in(1, m1, true, Some(tx.clone()))
        .await
        .unwrap();
    s.acknowledge_in(1, m2, true, Some(tx.clone()))
        .await
        .unwrap();
    s.rollback_tx_keeping(&tx, &[(1, m1), (1, m2)]).unwrap();

    let tx = Transaction::new_ref(broker.post_office().ids().next(), None);
    s.acknowledge_in(1, m2, false, Some(tx.clone()))
        .await
        .unwrap();
    s.commit_tx(&tx).unwrap();
    s.take_completion().wait().await.unwrap();
    s.close(false).await;
    assert_eq!(
        broker.post_office().queue("q").unwrap().message_count(),
        1,
        "the third message was never acknowledged"
    );
    broker.stop().unwrap();
}
