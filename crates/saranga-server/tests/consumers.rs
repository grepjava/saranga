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

fn named_session(broker: &Arc<Broker>, name: &str) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal("consumers"),
        Arc::new(Default::default()),
        vec![],
    ));
    ServerSession::new(
        broker.clone(),
        conn,
        SessionOptions {
            name: name.into(),
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

/// Two sessions racing to the last slot of a `max-consumers` queue: the limit
/// is checked and the slot reserved atomically, so only one is admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_consumers_cannot_exceed_max_consumers() {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("limited", "limited");
    spec.auto_create_address = Some(true);
    spec.max_consumers = Some(1);
    let queue = broker.post_office().create_queue(spec, false).unwrap();

    // The barrier, waited on inside each sink factory, releases both tasks
    // together between the (former) check and registration — the window the
    // non-atomic version let both through.
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut tasks = vec![];
    for i in 0..2 {
        let b = broker.clone();
        let gate = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let mut s = named_session(&b, &format!("race-{i}"));
            let admitted = s
                .create_consumer(i, "limited", None, 0, false, move |_| {
                    gate.wait();
                    Arc::new(Sink) as Arc<dyn DeliverySink>
                })
                .await
                .is_ok();
            (s, admitted)
        }));
    }
    let (s0, ok0) = tasks.remove(0).await.unwrap();
    let (s1, ok1) = tasks.remove(0).await.unwrap();
    assert_eq!(
        queue.consumer_count(),
        1,
        "max-consumers=1 must admit only one of the two racing consumers"
    );
    assert!(
        ok0 ^ ok1,
        "exactly one consumer is admitted and the other refused (ok0={ok0}, ok1={ok1})"
    );

    drop((s0, s1));
    broker.stop().unwrap();
}
