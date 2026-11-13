//! AMQP transactions are the connection's, not their session's
//! (`AMQPConnectionCallback`): one declared on a session can be discharged
//! through another, and outlives its session until the connection closes.

use std::sync::Arc;
use std::time::Duration;

use saranga_protocol_amqp::composite::Composite;
use saranga_protocol_amqp::transaction::{Coordinator, Declare, Discharge, TransactionalState};
use saranga_protocol_amqp::{AmqpConfig, AmqpProtocol, codec, frame, messaging::*, transport::*};
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::queue::Queue;
use saranga_server::{Acceptor, Broker, BrokerConfig, RoutingType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct Client {
    s: TcpStream,
    /// The next delivery ID on each channel.
    next: [u32; 2],
}

impl Client {
    async fn connect(broker: &Arc<Broker>) -> (Self, tokio::task::JoinHandle<()>) {
        Self::connect_to(broker, Acceptor::default()).await
    }

    /// Connects through an acceptor with `acceptor`'s parameters.
    async fn connect_to(
        broker: &Arc<Broker>,
        acceptor: Acceptor,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let task = tokio::spawn({
            let broker = broker.clone();
            async move {
                let _ = broker.serve_acceptor(l, acceptor).await;
            }
        });
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(&frame::AMQP_HEADER).await.unwrap();
        let mut h = [0; 8];
        s.read_exact(&mut h).await.unwrap();
        let mut c = Client { s, next: [0; 2] };
        c.send(
            0,
            Performative::Open(Open {
                container_id: "tx".into(),
                ..Default::default()
            }),
            &[],
        )
        .await;
        c.until(|_, p| matches!(p, Performative::Open(_))).await;
        (c, task)
    }

    async fn send(&mut self, channel: u16, p: Performative, payload: &[u8]) {
        let mut b = vec![];
        frame::encode_amqp(&mut b, channel, &p, payload);
        self.s.write_all(&b).await.unwrap();
    }

    async fn recv(&mut self) -> (u16, Performative) {
        let mut h = [0; 8];
        tokio::time::timeout(Duration::from_secs(5), self.s.read_exact(&mut h))
            .await
            .expect("a frame in time")
            .unwrap();
        let n = u32::from_be_bytes(h[..4].try_into().unwrap()) as usize;
        let mut b = h.to_vec();
        b.resize(n, 0);
        self.s.read_exact(&mut b[8..]).await.unwrap();
        let f = frame::decode(&b).unwrap();
        match f.body {
            frame::FrameBody::Amqp(p, _) => (f.channel, p),
            x => panic!("{x:?}"),
        }
    }

    /// Reads frames until one `f` accepts, which it returns.
    async fn until(&mut self, f: impl Fn(u16, &Performative) -> bool) -> Performative {
        loop {
            let (ch, p) = self.recv().await;
            if f(ch, &p) {
                return p;
            }
        }
    }

    async fn begin(&mut self, channel: u16) {
        self.send(
            channel,
            Performative::Begin(Begin {
                next_outgoing_id: 0,
                incoming_window: 100,
                outgoing_window: 100,
                ..Default::default()
            }),
            &[],
        )
        .await;
        self.until(|ch, p| ch == channel && matches!(p, Performative::Begin(_)))
            .await;
    }

    async fn end(&mut self, channel: u16) {
        self.send(channel, Performative::End(End::default()), &[])
            .await;
        self.until(|ch, p| ch == channel && matches!(p, Performative::End(_)))
            .await;
    }

    /// Attaches a sending link, `handle`, to the coordinator or `address`,
    /// and waits for its credit.
    async fn attach(&mut self, channel: u16, handle: u32, address: Option<&str>) {
        let target = match address {
            None => TargetKind::Coordinator(Coordinator::default()),
            Some(a) => TargetKind::Target(Target {
                address: Some(a.into()),
                capabilities: Some(vec!["queue".into()]),
                ..Default::default()
            }),
        };
        self.send(
            channel,
            Performative::Attach(Attach {
                name: format!("{channel}-{handle}"),
                handle,
                role: Role::Sender,
                initial_delivery_count: Some(0),
                target: Some(target),
                ..Default::default()
            }),
            &[],
        )
        .await;
        self.until(|ch, p| {
            ch == channel && matches!(p, Performative::Flow(f) if f.handle == Some(handle))
        })
        .await;
    }

    /// Sends `payload` on a link and returns the outcome the broker settled it
    /// with.
    async fn transfer(
        &mut self,
        channel: u16,
        handle: u32,
        state: Option<DeliveryState>,
        payload: &[u8],
    ) -> DeliveryState {
        let id = self.next[channel as usize];
        self.next[channel as usize] += 1;
        self.send(
            channel,
            Performative::Transfer(Transfer {
                handle,
                delivery_id: Some(id),
                delivery_tag: Some(id.to_be_bytes().to_vec()),
                message_format: Some(0),
                settled: Some(false),
                state,
                ..Default::default()
            }),
            payload,
        )
        .await;
        match self
            .until(|ch, p| {
                ch == channel && matches!(p, Performative::Disposition(d) if d.first == id)
            })
            .await
        {
            Performative::Disposition(d) => d.state.expect("an outcome"),
            _ => unreachable!(),
        }
    }

    /// A coordinator's `declare` or `discharge`, as an `amqp-value` body.
    async fn coordinate(
        &mut self,
        channel: u16,
        handle: u32,
        body: &impl Composite,
    ) -> DeliveryState {
        let mut payload = vec![0x00, 0x53, 0x77];
        codec::encode(&body.to_value(), &mut payload);
        self.transfer(channel, handle, None, &payload).await
    }

    async fn declare(&mut self, channel: u16, handle: u32) -> Vec<u8> {
        match self.coordinate(channel, handle, &Declare::default()).await {
            DeliveryState::Declared(d) => d.txn_id,
            x => panic!("{x:?}"),
        }
    }

    /// Sends a message to the link `handle` in transaction `txn_id`.
    async fn send_in(&mut self, channel: u16, handle: u32, txn_id: &[u8]) {
        let state = DeliveryState::Transactional(TransactionalState {
            txn_id: txn_id.to_vec(),
            ..Default::default()
        });
        // A durable header, then a data section of eight bytes.
        let payload = [
            0x00, 0x53, 0x70, 0xc0, 2, 1, 0x41, 0x00, 0x53, 0x75, 0xa0, 8, 1, 2, 3, 4, 5, 6, 7, 8,
        ];
        let outcome = self.transfer(channel, handle, Some(state), &payload).await;
        assert!(
            matches!(outcome, DeliveryState::Transactional(_)),
            "{outcome:?}"
        );
    }

    /// Attaches a receiving link, `handle`, to `address` with `credit`.
    async fn receiver(&mut self, channel: u16, handle: u32, address: &str, credit: u32) {
        self.send(
            channel,
            Performative::Attach(Attach {
                name: format!("{channel}-{handle}"),
                handle,
                role: Role::Receiver,
                source: Some(Source {
                    address: Some(address.into()),
                    capabilities: Some(vec!["queue".into()]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            &[],
        )
        .await;
        self.until(|ch, p| ch == channel && matches!(p, Performative::Attach(_)))
            .await;
        self.send(
            channel,
            Performative::Flow(Flow {
                incoming_window: 100,
                next_outgoing_id: 0,
                outgoing_window: 100,
                handle: Some(handle),
                delivery_count: Some(0),
                link_credit: Some(credit),
                ..Default::default()
            }),
            &[],
        )
        .await;
    }

    /// The next delivery on `channel`, within `wait`.
    async fn delivery(&mut self, channel: u16, wait: Duration) -> Option<u32> {
        tokio::time::timeout(
            wait,
            self.until(|ch, p| ch == channel && matches!(p, Performative::Transfer(_))),
        )
        .await
        .ok()
        .map(|p| match p {
            Performative::Transfer(t) => t.delivery_id.expect("a delivery ID"),
            _ => unreachable!(),
        })
    }

    /// The receiver's settled disposition of `id`, in transaction `txn_id`.
    async fn dispose_in(&mut self, channel: u16, id: u32, txn_id: &[u8], outcome: DeliveryState) {
        self.send(
            channel,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled: true,
                state: Some(DeliveryState::Transactional(TransactionalState {
                    txn_id: txn_id.to_vec(),
                    outcome: Some(Box::new(outcome)),
                })),
                batchable: false,
            }),
            &[],
        )
        .await;
    }
}

impl Client {
    /// An unsettled Accepted disposition of `id` in transaction `txn_id`,
    /// or outside any with `txn_id` `None`.
    async fn accept(&mut self, channel: u16, id: u32, txn_id: Option<&[u8]>, settled: bool) {
        let state = match txn_id {
            Some(txn_id) => DeliveryState::Transactional(TransactionalState {
                txn_id: txn_id.to_vec(),
                outcome: Some(Box::new(DeliveryState::Accepted)),
            }),
            None => DeliveryState::Accepted,
        };
        self.send(
            channel,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled,
                state: Some(state),
                batchable: false,
            }),
            &[],
        )
        .await;
    }

    /// Whether the broker settles delivery `id` on `channel` within `wait`.
    async fn settles(&mut self, channel: u16, id: u32, wait: Duration) -> bool {
        tokio::time::timeout(
            wait,
            self.until(|ch, p| {
                ch == channel
                    && matches!(p, Performative::Disposition(d)
                        if d.role == Role::Sender && d.first == id && d.settled)
            }),
        )
        .await
        .is_ok()
    }
}

/// A message `m` with the duplicate ID `x`.
const DUPLICATE: &[u8] = &[
    0x00, 0x53, 0x74, 0xc1, 18, 2, 0xa1, 12, b'_', b'A', b'M', b'Q', b'_', b'D', b'U', b'P', b'L',
    b'_', b'I', b'D', 0xa1, 1, b'x', 0x00, 0x53, 0x77, 0xa1, 1, b'm',
];

/// A persistent broker, so that a commit's queue updates wait for its
/// journal write, with a durable queue `q`.
async fn broker(dir: &tempfile::TempDir) -> (Arc<Broker>, Queue) {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <security-enabled>false</security-enabled>
           </core></configuration>"#,
    )
    .unwrap();
    let mut cfg =
        BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
    cfg.protocols
        .push(Arc::new(AmqpProtocol::new(AmqpConfig::default())));
    let broker = Broker::start(cfg).await.unwrap();
    let mut spec = QueueSpec::new("q", "q");
    spec.auto_create_address = Some(true);
    spec.routing_type = Some(RoutingType::Anycast);
    spec.durable = true;
    let q = broker.post_office().create_queue(spec, false).unwrap();
    (broker, q)
}

fn discharge(txn_id: Vec<u8>, fail: bool) -> Discharge {
    Discharge {
        txn_id,
        fail: Some(fail),
    }
}

/// The commit's queue updates are made before the discharge through
/// another session is answered, not left waiting on the declaring session.
#[tokio::test]
async fn a_commit_through_another_session_is_made_before_it_is_answered() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, q) = broker(&dir).await;
    let (mut c, task) = Client::connect(&broker).await;
    c.begin(0).await;
    c.begin(1).await;
    c.attach(0, 0, None).await;
    c.attach(0, 1, Some("q")).await;
    c.attach(1, 0, None).await;
    let txn = c.declare(0, 0).await;
    c.send_in(0, 1, &txn).await;
    assert_eq!(q.message_count(), 0, "not yet committed");

    let outcome = c.coordinate(1, 0, &discharge(txn, false)).await;
    assert!(matches!(outcome, DeliveryState::Accepted), "{outcome:?}");
    assert_eq!(
        q.message_count(),
        1,
        "a commit answered as done must have reached the queue"
    );

    task.abort();
    broker.stop().unwrap();
}

/// A transaction left open when its session ends rolls back: at once when
/// no other session is left, with the connection otherwise, either way
/// giving back the memory its sends took.
#[tokio::test]
async fn an_ended_sessions_open_transaction_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, q) = broker(&dir).await;
    let paging = broker.post_office().paging().clone();
    let base = paging.global_size();

    // The only session ends.
    let (mut c, task) = Client::connect(&broker).await;
    c.begin(0).await;
    c.attach(0, 0, None).await;
    c.attach(0, 1, Some("q")).await;
    let txn = c.declare(0, 0).await;
    c.send_in(0, 1, &txn).await;
    assert!(paging.global_size() > base, "the send is charged");
    c.end(0).await;
    assert_eq!(
        paging.global_size(),
        base,
        "rolled back as the session ended"
    );
    drop(c);
    task.abort();

    // Another session outlives it, and the connection then closes.
    let (mut c, task) = Client::connect(&broker).await;
    c.begin(0).await;
    c.begin(1).await;
    c.attach(0, 0, None).await;
    c.attach(0, 1, Some("q")).await;
    let txn = c.declare(0, 0).await;
    c.send_in(0, 1, &txn).await;
    c.end(0).await;
    assert!(
        paging.global_size() > base,
        "still open: another session can discharge it"
    );
    drop(c);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while paging.global_size() != base && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        paging.global_size(),
        base,
        "rolled back with the connection"
    );
    assert_eq!(q.message_count(), 0);
    task.abort();
    broker.stop().unwrap();
}

/// As in Artemis, a transaction whose session has ended can still be
/// committed through another session of the connection.
#[tokio::test]
async fn an_ended_sessions_transaction_commits_through_another() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, q) = broker(&dir).await;
    let (mut c, task) = Client::connect(&broker).await;
    c.begin(0).await;
    c.begin(1).await;
    c.attach(0, 0, None).await;
    c.attach(0, 1, Some("q")).await;
    c.attach(1, 0, None).await;
    let txn = c.declare(0, 0).await;
    c.send_in(0, 1, &txn).await;
    c.end(0).await;
    let outcome = c.coordinate(1, 0, &discharge(txn, false)).await;
    assert!(matches!(outcome, DeliveryState::Accepted), "{outcome:?}");
    assert_eq!(q.message_count(), 1);
    task.abort();
    broker.stop().unwrap();
}

/// A Rejected outcome in a transaction is applied as one outside it (see
/// COMPAT), so under `amqpTreatRejectAsUnmodifiedDeliveryFailed` it returns
/// the message to the queue, failed, instead of rejecting it.
#[tokio::test]
async fn a_transactional_reject_can_be_treated_as_a_failed_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, q) = broker(&dir).await;
    let mut acceptor = Acceptor::default();
    acceptor.params.insert(
        "amqpTreatRejectAsUnmodifiedDeliveryFailed".to_owned(),
        "true".to_owned(),
    );
    let (mut c, task) = Client::connect_to(&broker, acceptor).await;
    c.begin(0).await;
    c.attach(0, 0, None).await;
    c.attach(0, 1, Some("q")).await;
    let outcome = c
        .transfer(0, 1, None, &[0x00, 0x53, 0x77, 0xa1, 1, b'm'])
        .await;
    assert!(matches!(outcome, DeliveryState::Accepted), "{outcome:?}");
    c.receiver(0, 2, "q", 1).await;
    let id = c
        .delivery(0, Duration::from_secs(5))
        .await
        .expect("delivered");
    let txn = c.declare(0, 0).await;
    c.dispose_in(0, id, &txn, DeliveryState::Rejected(Rejected::default()))
        .await;
    let outcome = c.coordinate(0, 0, &discharge(txn, false)).await;
    assert!(matches!(outcome, DeliveryState::Accepted), "{outcome:?}");
    assert_eq!(q.message_count(), 1, "returned to the queue, not rejected");
    task.abort();
    broker.stop().unwrap();
}

/// A Modified outcome in a transaction keeps an undeliverable-here message
/// from the consumer that gave it, as one outside a transaction does.
#[tokio::test]
async fn a_transactional_modified_can_be_undeliverable_here() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, q) = broker(&dir).await;
    let (mut c, task) = Client::connect(&broker).await;
    c.begin(0).await;
    c.attach(0, 0, None).await;
    c.attach(0, 1, Some("q")).await;
    let outcome = c
        .transfer(0, 1, None, &[0x00, 0x53, 0x77, 0xa1, 1, b'm'])
        .await;
    assert!(matches!(outcome, DeliveryState::Accepted), "{outcome:?}");
    c.receiver(0, 2, "q", 2).await;
    let id = c
        .delivery(0, Duration::from_secs(5))
        .await
        .expect("delivered");
    let txn = c.declare(0, 0).await;
    let modified = Modified {
        delivery_failed: Some(true),
        undeliverable_here: Some(true),
        ..Default::default()
    };
    c.dispose_in(0, id, &txn, DeliveryState::Modified(modified))
        .await;
    assert_eq!(
        c.delivery(0, Duration::from_millis(500)).await,
        None,
        "not given again to the consumer that found it undeliverable"
    );
    assert_eq!(q.message_count(), 1);
    task.abort();
    broker.stop().unwrap();
}

/// A commit that rolls the transaction back instead (it was made
/// rollback-only by a refused send) leaves the deliveries acknowledged in
/// it unsettled, with their consumer, as a rollback does: they are settled
/// as accepted only once committed.
#[tokio::test]
async fn a_failed_commit_does_not_settle_its_acknowledgements() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, q) = broker(&dir).await;
    let (mut c, task) = Client::connect(&broker).await;
    c.begin(0).await;
    c.attach(0, 0, None).await;
    c.attach(0, 1, Some("q")).await;
    let outcome = c.transfer(0, 1, None, DUPLICATE).await;
    assert!(matches!(outcome, DeliveryState::Accepted), "{outcome:?}");
    c.receiver(0, 2, "q", 2).await;
    let id = c
        .delivery(0, Duration::from_secs(5))
        .await
        .expect("delivered");
    let txn = c.declare(0, 0).await;
    c.accept(0, id, Some(&txn), false).await;
    // The same duplicate ID again, in the transaction: refused, and the
    // transaction can only roll back.
    let state = DeliveryState::Transactional(TransactionalState {
        txn_id: txn.clone(),
        ..Default::default()
    });
    c.transfer(0, 1, Some(state), DUPLICATE).await;
    let outcome = c.coordinate(0, 0, &discharge(txn, false)).await;
    assert!(matches!(outcome, DeliveryState::Rejected(_)), "{outcome:?}");
    assert!(
        !c.settles(0, id, Duration::from_millis(500)).await,
        "not settled as accepted by a commit that failed"
    );
    assert_eq!(
        c.delivery(0, Duration::from_millis(300)).await,
        None,
        "kept with its consumer, not given again"
    );
    // Still the client's to settle.
    c.accept(0, id, None, true).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while q.message_count() != 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(q.message_count(), 0, "acknowledged by its settlement");
    task.abort();
    broker.stop().unwrap();
}

/// A rollback keeps a delivery acknowledged in the transaction but not
/// settled with its consumer even when it was acknowledged on another
/// session than the one that declared the transaction.
#[tokio::test]
async fn a_rollback_keeps_another_sessions_unsettled_acknowledgement() {
    let dir = tempfile::tempdir().unwrap();
    let (broker, q) = broker(&dir).await;
    let (mut c, task) = Client::connect(&broker).await;
    c.begin(0).await;
    c.attach(0, 0, None).await;
    c.attach(0, 1, Some("q")).await;
    let outcome = c
        .transfer(0, 1, None, &[0x00, 0x53, 0x77, 0xa1, 1, b'm'])
        .await;
    assert!(matches!(outcome, DeliveryState::Accepted), "{outcome:?}");
    c.begin(1).await;
    c.receiver(1, 0, "q", 2).await;
    let id = c
        .delivery(1, Duration::from_secs(5))
        .await
        .expect("delivered");
    let txn = c.declare(0, 0).await;
    c.accept(1, id, Some(&txn), false).await;
    let outcome = c.coordinate(0, 0, &discharge(txn, true)).await;
    assert!(matches!(outcome, DeliveryState::Accepted), "{outcome:?}");
    assert_eq!(
        c.delivery(1, Duration::from_millis(500)).await,
        None,
        "kept with its consumer, not given again under another delivery ID"
    );
    c.accept(1, id, None, true).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while q.message_count() != 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(q.message_count(), 0, "acknowledged by its settlement");
    task.abort();
    broker.stop().unwrap();
}
