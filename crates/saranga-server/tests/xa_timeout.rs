//! Work a session gives its transaction after the resource manager rolled
//! it back (its XA timeout) goes in a new transaction rolled back at once,
//! as Artemis's `addOperation` on a rolled-back transaction runs the
//! operation's `afterRollback` at once: a send is dropped with nothing left
//! counted against its address, an acknowledgement returns the message.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_server::message::ServerMessage;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::DeliverySink;
use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
use saranga_server::transaction::{self, Xid};
use saranga_server::{Broker, BrokerConfig};

struct Record(Arc<Mutex<Vec<i64>>>);

impl DeliverySink for Record {
    fn deliver(&self, _: i64, m: &Arc<ServerMessage>, _: i32) -> i64 {
        self.0.lock().unwrap().push(m.message_id());
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

fn session(broker: &Arc<Broker>, name: &str, xa: bool) -> ServerSession {
    let conn = Arc::new(ClientConnection::new(
        Transport::internal(name),
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
            xa,
            auto_commit_sends: !xa,
            auto_commit_acks: !xa,
            pre_acknowledge: false,
            default_address: None,
        },
        Default::default(),
    )
}

fn xid(name: &str) -> Xid {
    Xid {
        format_id: 1,
        branch_qualifier: name.as_bytes().to_vec(),
        global_transaction_id: name.as_bytes().to_vec(),
    }
}

/// The transaction reaper's pass, with every branch timed out.
fn time_out(broker: &Arc<Broker>) {
    for tx in broker.resource_manager().take_timed_out(i64::MAX / 2) {
        let mut outcome = transaction::lock(&tx).rollback().unwrap();
        outcome.write(None).unwrap();
        outcome.apply();
    }
}

async fn broker() -> Arc<Broker> {
    let broker = Broker::start(BrokerConfig::new(Arc::new(SelectorFilters)))
        .await
        .unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    broker.post_office().create_queue(spec, false).unwrap();
    broker
}

fn message() -> saranga_message::CoreMessage {
    saranga_message::CoreMessage {
        address: Some("q".into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_send_after_the_timeout_leaves_nothing_counted() {
    let broker = broker().await;
    let mut s = session(&broker, "xa", true);
    s.xa_start(xid("t")).unwrap();
    time_out(&broker);
    s.send(message(), None).await.unwrap();
    let _ = s.xa_end(xid("t"));
    let _ = s.xa_rollback(xid("t")).await;
    s.close(false).await;
    let po = broker.post_office();
    assert_eq!(po.queue("q").unwrap().message_count(), 0);
    assert_eq!(
        po.paging_store("q").address_size(),
        0,
        "the dropped send stayed counted"
    );
}

#[tokio::test]
async fn an_acknowledgement_after_the_timeout_returns_the_message() {
    let broker = broker().await;
    let mut producer = session(&broker, "p", false);
    producer.send(message(), None).await.unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut s = session(&broker, "xa", true);
    let record = seen.clone();
    s.create_consumer(1, "q", None, 0, false, move |_| {
        Arc::new(Record(record.clone()))
    })
    .await
    .unwrap();
    s.consumer_credits(1, -1);
    s.xa_start(xid("t")).unwrap();
    s.set_started(true).await;
    let deliveries = |n: usize| {
        let seen = seen.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let v = seen.lock().unwrap().clone();
                    if v.len() >= n {
                        return v;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
        }
    };
    let id = deliveries(1).await.expect("the delivery")[0];
    time_out(&broker);
    s.acknowledge(1, id, false).await.unwrap();
    // Acknowledged in a transaction that never ends, it would be in limbo
    // until a restart; it comes back to be delivered again.
    let again = deliveries(2).await.expect("the message delivered again");
    assert_eq!(again[1], id);
}
