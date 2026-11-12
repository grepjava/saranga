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
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let task = tokio::spawn({
            let broker = broker.clone();
            async move {
                let _ = broker.serve_acceptor(l, Acceptor::default()).await;
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
}

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
