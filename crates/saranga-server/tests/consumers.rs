//! A session-local consumer ID identifies one consumer. Reusing it must not
//! leave the first consumer registered on its queue with nothing to reach it.

use std::sync::Arc;

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
        1
    }
    fn chunk_size(&self) -> usize {
        1024
    }
}

fn session(broker: &Arc<Broker>) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("consumers"),
        Arc::new(Default::default()),
        vec![],
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: "consumers".into(),
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
async fn a_reused_consumer_id_is_refused_and_leaks_nothing() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "a");
    spec.auto_create_address = Some(true);
    let q = broker.post_office().create_queue(spec, false).unwrap();
    let mut session = session(&broker);

    session
        .create_consumer(1, "q", None, 0, false, |_| Arc::new(Sink))
        .await
        .unwrap();
    // The same session-local ID a second time is refused.
    let again = session
        .create_consumer(1, "q", None, 0, false, |_| Arc::new(Sink))
        .await;
    assert!(again.is_err(), "a reused consumer ID should be refused");

    session.set_started(true).await;
    assert_eq!(
        q.consumer_count(),
        1,
        "only the first consumer is registered"
    );

    session.close(true).await;
    assert_eq!(q.consumer_count(), 0, "no consumer is left after close");
    broker.stop().unwrap();
}
