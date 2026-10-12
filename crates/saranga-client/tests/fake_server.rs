//! Protocol tests against an in-process fake broker. The fake decodes what
//! the client sends with `saranga-core-protocol` and answers the way the
//! Java broker does (see the `core-basic` golden capture), so these tests
//! pin down the exact packets: correlation IDs, blocking rules, producer
//! credits and consumer window arithmetic.

use std::time::Duration;

use saranga_client::message::{self, MessageExt};
use saranga_client::*;
use saranga_core_protocol::packets::cluster::*;
use saranga_core_protocol::packets::connection::*;
use saranga_core_protocol::packets::large::*;
use saranga_core_protocol::packets::message::*;
use saranga_core_protocol::packets::session::*;
use saranga_core_protocol::{Context, Direction, HANDSHAKE, Packet, read_frame};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

const WAIT: Duration = Duration::from_secs(5);

type Replier = Box<dyn FnMut(i64, &Packet) -> Vec<(i64, Packet)> + Send>;

/// The client side of a fake broker: what the client sent, and a way to
/// push packets to it.
struct Fake {
    received: mpsc::UnboundedReceiver<(i64, Packet)>,
    inject: mpsc::UnboundedSender<Inject>,
}

enum Inject {
    Packet(i64, Box<Packet>),
    Close,
}

fn server_ctx() -> Context {
    Context::new(Direction::ToClient, 136)
}

/// Encodes a server-to-client frame exactly as the fake sends it.
fn frame(channel: i64, packet: &Packet) -> Vec<u8> {
    packet.encode_frame(channel, &server_ctx())
}

impl Fake {
    /// The next packet the client sent, skipping pings.
    async fn next(&mut self) -> (i64, Packet) {
        loop {
            let p = self.next_raw().await;
            if !matches!(p.1, Packet::Ping(_)) {
                return p;
            }
        }
    }

    async fn next_raw(&mut self) -> (i64, Packet) {
        tokio::time::timeout(WAIT, self.received.recv())
            .await
            .expect("timed out waiting for a packet")
            .expect("fake broker gone")
    }

    /// Asserts the client sends nothing (but pings) for a short while.
    async fn assert_quiet(&mut self) {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(150);
        loop {
            match tokio::time::timeout_at(deadline, self.received.recv()).await {
                Err(_) => return,
                Ok(Some((_, Packet::Ping(_)))) => {}
                Ok(other) => panic!("unexpected packet {other:?}"),
            }
        }
    }

    fn send(&self, channel: i64, packet: impl Into<Packet>) {
        self.inject
            .send(Inject::Packet(channel, Box::new(packet.into())))
            .unwrap();
    }

    fn close(&self) {
        self.inject.send(Inject::Close).unwrap();
    }
}

async fn read_frame_bytes<R: AsyncRead + Unpin>(r: &mut R) -> Option<Vec<u8>> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await.ok()?;
    let mut buf = vec![0; 4 + i32::from_be_bytes(len) as usize];
    buf[..4].copy_from_slice(&len);
    r.read_exact(&mut buf[4..]).await.ok()?;
    Some(buf)
}

async fn serve<S: AsyncRead + AsyncWrite + Send + 'static>(
    stream: S,
    mut reply: Replier,
    log: mpsc::UnboundedSender<(i64, Packet)>,
    mut inject: mpsc::UnboundedReceiver<Inject>,
) {
    let (mut r, mut w) = tokio::io::split(stream);
    let mut handshake = [0u8; 7];
    r.read_exact(&mut handshake).await.unwrap();
    assert_eq!(&handshake, HANDSHAKE);
    let client_ctx = Context::new(Direction::ToServer, 136);
    loop {
        tokio::select! {
            buf = read_frame_bytes(&mut r) => {
                let Some(buf) = buf else { return };
                let (f, _) = read_frame(&buf).unwrap().unwrap();
                let packet = Packet::decode(&f, &client_ctx).unwrap();
                for (ch, p) in reply(f.channel_id, &packet) {
                    if w.write_all(&frame(ch, &p)).await.is_err() {
                        return;
                    }
                }
                if log.send((f.channel_id, packet)).is_err() {
                    return;
                }
            }
            cmd = inject.recv() => match cmd {
                Some(Inject::Packet(ch, p)) => {
                    if w.write_all(&frame(ch, &p)).await.is_err() {
                        return;
                    }
                }
                Some(Inject::Close) | None => {
                    let _ = w.shutdown().await;
                    return;
                }
            }
        }
    }
}

fn null_response(correlation_id: i64) -> Packet {
    NullResponseMessageV2 {
        correlation_id: Some(correlation_id),
    }
    .into()
}

fn topology() -> Packet {
    ClusterTopologyChangeMessageV4 {
        node_id: "00000000-0000-0000-0000-000000000000".into(),
        unique_event_id: 1,
        update: Some(TopologyUpdate {
            last: true,
            ..Default::default()
        }),
        backup_group_name: None,
        scale_down_group_name: None,
        server_version: 136,
    }
    .into()
}

fn queue_query_response(queue: &SimpleString, default_window: Option<i32>) -> Packet {
    SessionQueueQueryResponseMessageV3 {
        exists: true,
        durable: true,
        address: Some(queue.clone()),
        name: Some(queue.clone()),
        auto_create_queues: true,
        routing_type: ROUTING_TYPE_ANYCAST,
        max_consumers: -1,
        default_consumer_window_size: saranga_core_protocol::Flagged(default_window),
        sections: SessionQueueQueryResponseMessageV3::SECTIONS,
        ..Default::default()
    }
    .into()
}

/// What the Java broker answers, packet by packet.
fn broker_reply(ch: i64, p: &Packet) -> Vec<(i64, Packet)> {
    match p {
        Packet::Ping(x) => vec![(0, Ping { ..x.clone() }.into())],
        Packet::SubscribeClusterTopologyUpdatesMessageV2(_) => vec![(0, topology())],
        Packet::CreateSessionMessageV2(_) | Packet::CreateSessionMessage(_) => vec![(
            1,
            CreateSessionResponseMessage {
                server_version: 136,
            }
            .into(),
        )],
        Packet::SessionQueueQueryMessage(q) => {
            vec![(ch, queue_query_response(&q.queue_name, Some(1024 * 1024)))]
        }
        Packet::SessionCreateConsumerMessage(c) if c.requires_response => {
            vec![(ch, queue_query_response(&c.queue_name, Some(1024 * 1024)))]
        }
        Packet::SessionBindingQueryMessage(_) => vec![(
            ch,
            SessionBindingQueryResponseMessageV5 {
                exists: true,
                auto_create_queues: true,
                auto_create_addresses: true,
                supports_anycast: true,
                sections: SessionBindingQueryResponseMessageV5::SECTIONS,
                ..Default::default()
            }
            .into(),
        )],
        Packet::SessionAddMetaDataMessageV2(_)
        | Packet::SessionUniqueAddMetaDataMessage(_)
        | Packet::CreateAddressMessage(_)
        | Packet::CreateQueueMessageV2(_)
        | Packet::CreateSharedQueueMessageV2(_)
        | Packet::SessionDeleteQueueMessage(_)
        | Packet::SessionConsumerCloseMessage(_)
        | Packet::SessionCloseMessage(_)
        | Packet::SessionStop(_)
        | Packet::RollbackMessage(_) => vec![(ch, null_response(-1))],
        Packet::SessionCommitMessageV2(c) => {
            vec![(ch, null_response(c.correlation_id.unwrap_or(-1)))]
        }
        Packet::SessionSendMessageV3(s) if s.requires_response => {
            vec![(ch, null_response(s.correlation_id))]
        }
        Packet::SessionSendContinuationMessageV3(s) if s.requires_response => {
            vec![(ch, null_response(s.correlation_id.unwrap_or(-1)))]
        }
        Packet::SessionAcknowledgeMessage(a) if a.requires_response => {
            vec![(ch, null_response(-1))]
        }
        Packet::SessionIndividualAcknowledgeMessage(a) if a.requires_response => {
            vec![(ch, null_response(-1))]
        }
        Packet::SessionRequestProducerCreditsMessage(r) => vec![(
            ch,
            SessionProducerCreditsMessage {
                credits: r.credits,
                address: r.address.clone(),
            }
            .into(),
        )],
        _ => vec![],
    }
}

fn start_fake(reply: Replier) -> (tokio::io::DuplexStream, Fake) {
    let (client, server) = tokio::io::duplex(1 << 20);
    let (log_tx, log_rx) = mpsc::unbounded_channel();
    let (inject_tx, inject_rx) = mpsc::unbounded_channel();
    tokio::spawn(serve(server, reply, log_tx, inject_rx));
    (
        client,
        Fake {
            received: log_rx,
            inject: inject_tx,
        },
    )
}

async fn setup_with(reply: Replier, options: ConnectionOptions) -> (Connection, Fake) {
    let (stream, mut fake) = start_fake(reply);
    let conn = Connection::connect_with(stream, options).await.unwrap();
    // Two pings, then the topology subscription.
    for _ in 0..2 {
        assert!(matches!(fake.next_raw().await.1, Packet::Ping(_)));
    }
    assert!(matches!(
        fake.next_raw().await.1,
        Packet::SubscribeClusterTopologyUpdatesMessageV2(_)
    ));
    (conn, fake)
}

async fn setup() -> (Connection, Fake) {
    setup_with(Box::new(broker_reply), ConnectionOptions::default()).await
}

async fn open_session(conn: &Connection, fake: &mut Fake, options: SessionOptions) -> Session {
    let s = conn.create_session(options).await.unwrap();
    assert!(matches!(
        fake.next().await,
        (1, Packet::CreateSessionMessageV2(_))
    ));
    s
}

/// A delivery as the broker encodes it.
fn delivery(consumer_id: i64, message_id: i64, text: &str, priority: u8) -> Packet {
    let mut m = message::text_message(text);
    m.message_id = message_id;
    m.priority = priority;
    SessionReceiveMessage {
        message: m,
        consumer_id,
        delivery_count: 1,
    }
    .into()
}

fn flow_credits(p: &Packet) -> i32 {
    match p {
        Packet::SessionConsumerFlowCreditMessage(f) => f.credits,
        other => panic!("expected flow credits, got {other:?}"),
    }
}

#[tokio::test]
async fn connect_and_create_session_like_java() {
    let (stream, mut fake) = start_fake(Box::new(broker_reply));
    let conn = Connection::connect_with(stream, ConnectionOptions::default())
        .await
        .unwrap();
    // ClientSessionFactoryImpl.schedulePing: send() plus the scheduled
    // pinger's first run, both with the default TTL.
    for _ in 0..2 {
        match fake.next_raw().await {
            (0, Packet::Ping(p)) => assert_eq!(p.connection_ttl, 60_000),
            other => panic!("{other:?}"),
        }
    }
    match fake.next_raw().await {
        (0, Packet::SubscribeClusterTopologyUpdatesMessageV2(s)) => {
            assert!(!s.cluster_connection);
            assert_eq!(s.client_version, 136);
        }
        other => panic!("{other:?}"),
    }
    // The topology V4 message set the channel version.
    assert_eq!(conn.version(), 136);

    let s1 = conn
        .create_session(SessionOptions {
            username: Some("guest".into()),
            password: Some("secret".into()),
            client_id: Some("cid".into()),
            ..SessionOptions::transacted()
        })
        .await
        .unwrap();
    match fake.next().await {
        (1, Packet::CreateSessionMessageV2(c)) => {
            assert_eq!(c.name.to_string(), s1.name());
            assert_eq!(c.name.len(), 36);
            assert_eq!(c.session_channel_id, 10);
            assert_eq!(c.version, 136);
            assert_eq!(c.username.unwrap().to_string(), "guest");
            assert_eq!(c.password.unwrap().to_string(), "secret");
            assert_eq!(c.min_large_message_size, 102_400);
            assert!(!c.xa && !c.auto_commit_sends && !c.auto_commit_acks);
            assert_eq!(c.window_size, -1);
            assert!(!c.pre_acknowledge);
            assert_eq!(c.default_address, None);
            assert_eq!(c.client_id.unwrap().to_string(), "cid");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(s1.server_version(), 136);
    conn.create_session(SessionOptions::default())
        .await
        .unwrap();
    match fake.next().await {
        (1, Packet::CreateSessionMessageV2(c)) => {
            assert_eq!(c.session_channel_id, 11);
            assert!(c.auto_commit_sends && c.auto_commit_acks);
            assert_eq!(c.username, None);
            assert_eq!(c.client_id, None);
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn connect_over_tcp() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (log_tx, mut log_rx) = mpsc::unbounded_channel();
    let (_inject_tx, inject_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        serve(sock, Box::new(broker_reply), log_tx, inject_rx).await;
    });
    let conn = Connection::connect(addr, ConnectionOptions::default())
        .await
        .unwrap();
    let session = conn
        .create_session(SessionOptions::default())
        .await
        .unwrap();
    let q = session.queue_query("q").await.unwrap();
    assert!(q.exists);
    assert_eq!(q.default_consumer_window_size, Some(1024 * 1024));
    let mut seen = Vec::new();
    while let Ok(Some((_, p))) =
        tokio::time::timeout(Duration::from_millis(100), log_rx.recv()).await
    {
        seen.push(p.name());
    }
    assert!(seen.contains(&"SessionQueueQueryMessage"), "{seen:?}");
    conn.close().await;
}

#[tokio::test]
async fn connect_waits_for_topology() {
    // Without a topology answer, connect times out after the call timeout.
    let reply: Replier = Box::new(|ch, p| match p {
        Packet::SubscribeClusterTopologyUpdatesMessageV2(_) => vec![],
        _ => broker_reply(ch, p),
    });
    let (stream, _fake) = start_fake(reply);
    let err = Connection::connect_with(
        stream,
        ConnectionOptions {
            call_timeout: Duration::from_millis(200),
            ..ConnectionOptions::default()
        },
    )
    .await
    .err()
    .unwrap();
    assert_eq!(err.kind(), Some(ErrorType::ConnectionTimedOut), "{err}");

    // A node exit, or an update without `last`, does not complete it.
    let reply: Replier = Box::new(|ch, p| match p {
        Packet::SubscribeClusterTopologyUpdatesMessageV2(_) => vec![
            (
                0,
                ClusterTopologyChangeMessageV4 {
                    node_id: "n".into(),
                    update: None,
                    server_version: 136,
                    ..Default::default()
                }
                .into(),
            ),
            (
                0,
                ClusterTopologyChangeMessageV2 {
                    node_id: "n".into(),
                    update: Some(TopologyUpdate::default()),
                    ..Default::default()
                }
                .into(),
            ),
        ],
        _ => broker_reply(ch, p),
    });
    let (stream, fake) = start_fake(reply);
    let connecting = tokio::spawn(Connection::connect_with(
        stream,
        ConnectionOptions::default(),
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!connecting.is_finished());
    fake.send(0, topology());
    let conn = connecting.await.unwrap().unwrap();
    assert_eq!(conn.version(), 136);
}

#[tokio::test]
async fn blocking_calls_use_decreasing_correlation_ids() {
    let (conn, mut fake) = setup().await;
    let session = open_session(&conn, &mut fake, SessionOptions::default()).await;

    // -2: carried by nothing on the wire, answered with -1 like the broker.
    session.add_metadata("k", "v").await.unwrap();
    match fake.next().await {
        (10, Packet::SessionAddMetaDataMessageV2(m)) => {
            assert_eq!(m.key.to_string(), "k");
            assert!(m.requires_confirmation);
        }
        other => panic!("{other:?}"),
    }
    // Producer creation: credits request, then CreateProducerMessage (id 1).
    let producer = session.create_producer(Some("q")).await.unwrap();
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionRequestProducerCreditsMessage(_))
    ));
    match fake.next().await {
        (10, Packet::CreateProducerMessage(p)) => {
            assert_eq!(p.id, 1);
            assert_eq!(p.address.unwrap().to_string(), "q");
        }
        other => panic!("{other:?}"),
    }
    // A durable send blocks: -3 is the next ID.
    producer.send(message::text_message("a")).await.unwrap();
    let _credits = fake.next().await;
    match fake.next().await {
        (10, Packet::SessionSendMessageV3(s)) => {
            assert!(s.requires_response);
            assert_eq!(s.correlation_id, -3);
            assert_eq!(s.sender_id, 1);
            assert_eq!(s.message.address.unwrap().to_string(), "q");
        }
        other => panic!("{other:?}"),
    }
    // A non-durable send does not block and keeps correlation 0.
    let mut m = message::text_message("b");
    m.set_durable(false);
    producer.send(m).await.unwrap();
    match fake.next().await {
        (10, Packet::SessionSendMessageV3(s)) => {
            assert!(!s.requires_response);
            assert_eq!(s.correlation_id, 0);
        }
        other => panic!("{other:?}"),
    }
    session.commit().await.unwrap();
    match fake.next().await {
        (10, Packet::SessionCommitMessageV2(c)) => assert_eq!(c.correlation_id, Some(-4)),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn responses_match_on_type_and_correlation() {
    // The fake answers sends late, after a stale response and one of the
    // wrong type.
    let reply: Replier = Box::new(|ch, p| match p {
        Packet::SessionSendMessageV3(_) => vec![],
        _ => broker_reply(ch, p),
    });
    let options = SessionOptions {
        producer_window_size: -1,
        ..SessionOptions::default()
    };
    let (conn, mut fake) = setup_with(reply, ConnectionOptions::default()).await;
    let session = open_session(&conn, &mut fake, options).await;
    let producer = session.create_producer(Some("q")).await.unwrap();
    let _create = fake.next().await;
    let send = tokio::spawn(async move { producer.send(message::text_message("x")).await });
    let corr = match fake.next().await {
        (10, Packet::SessionSendMessageV3(s)) => s.correlation_id,
        other => panic!("{other:?}"),
    };
    assert_eq!(corr, -2);
    fake.send(10, null_response(-1));
    fake.send(10, queue_query_response(&"q".into(), None));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!send.is_finished());
    fake.send(10, null_response(corr));
    send.await.unwrap().unwrap();
}

#[tokio::test]
async fn exceptions_carry_the_artemis_code() {
    let reply: Replier = Box::new(|ch, p| match p {
        Packet::SessionCreateConsumerMessage(_) => vec![(
            ch,
            ActiveMQExceptionMessageV2 {
                code: 100,
                message: Some("AMQ229017: Queue nope does not exist".into()),
                correlation_id: Some(-1),
            }
            .into(),
        )],
        Packet::CreateSessionMessageV2(c) if c.username.is_some() => vec![(
            1,
            ActiveMQExceptionMessageV2 {
                code: 105,
                message: Some("AMQ229031: Unable to validate user".into()),
                correlation_id: None,
            }
            .into(),
        )],
        _ => broker_reply(ch, p),
    });
    let (conn, mut fake) = setup_with(reply, ConnectionOptions::default()).await;
    let err = conn
        .create_session(SessionOptions {
            username: Some("bad".into()),
            ..SessionOptions::default()
        })
        .await
        .err()
        .unwrap();
    assert_eq!(err.kind(), Some(ErrorType::SecurityException));
    assert_eq!(err.code(), Some(105));
    assert!(err.to_string().contains("AMQ229031"));
    let _ = fake.next().await;

    let session = open_session(&conn, &mut fake, SessionOptions::default()).await;
    let err = session
        .create_consumer("nope", None, false)
        .await
        .err()
        .unwrap();
    assert_eq!(err.kind(), Some(ErrorType::QueueDoesNotExist));
    // The session is still usable.
    session.queue_query("q").await.unwrap();
}

#[tokio::test]
async fn a_call_timeout_fails_the_connection() {
    let reply: Replier = Box::new(|ch, p| match p {
        Packet::SessionQueueQueryMessage(_) => vec![],
        _ => broker_reply(ch, p),
    });
    let (conn, mut fake) = setup_with(
        reply,
        ConnectionOptions {
            call_timeout: Duration::from_millis(200),
            ..ConnectionOptions::default()
        },
    )
    .await;
    let session = open_session(&conn, &mut fake, SessionOptions::default()).await;
    let err = session.queue_query("q").await.unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::ConnectionTimedOut), "{err}");
    assert!(err.to_string().contains("AMQ219014"));
    assert_eq!(
        conn.failure().unwrap().kind(),
        Some(ErrorType::ConnectionTimedOut)
    );
}

#[tokio::test]
async fn server_disconnect_unblocks_and_fails() {
    let reply: Replier = Box::new(|ch, p| match p {
        Packet::SessionQueueQueryMessage(_) => vec![],
        _ => broker_reply(ch, p),
    });
    let (conn, mut fake) = setup_with(reply, ConnectionOptions::default()).await;
    let session = open_session(&conn, &mut fake, SessionOptions::default()).await;
    let consumer = session.create_consumer("q", None, false).await.unwrap();
    let s2 = session.clone();
    let call = tokio::spawn(async move { s2.queue_query("q").await });
    let receive = tokio::spawn(async move { consumer.receive(Duration::ZERO).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    fake.send(
        0,
        DisconnectMessageV3 {
            node_id: Some("node".into()),
            reason: 0,
            ..Default::default()
        },
    );
    let err = call.await.unwrap().unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::Unblocked), "{err}");
    let err = receive.await.unwrap().unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::Disconnected), "{err}");
    assert_eq!(
        conn.failure().unwrap().kind(),
        Some(ErrorType::Disconnected)
    );
    let err = session.start().await.unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::Disconnected));
}

#[tokio::test]
async fn transport_close_fails_the_connection() {
    let (conn, mut fake) = setup().await;
    let session = open_session(&conn, &mut fake, SessionOptions::default()).await;
    fake.close();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let err = session.queue_query("q").await.unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::NotConnected), "{err}");
}

#[tokio::test]
async fn pings_and_failure_check() {
    // The broker answers nothing after the topology: no data within the
    // TTL fails the connection (PingRunnable).
    let reply: Replier = Box::new(|ch, p| match p {
        Packet::Ping(_) => vec![],
        _ => broker_reply(ch, p),
    });
    let (conn, mut fake) = setup_with(
        reply,
        ConnectionOptions {
            client_failure_check_period: Some(Duration::from_millis(40)),
            connection_ttl: Some(Duration::from_millis(100)),
            ..ConnectionOptions::default()
        },
    )
    .await;
    match fake.next_raw().await {
        (0, Packet::Ping(p)) => assert_eq!(p.connection_ttl, 100),
        other => panic!("{other:?}"),
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        conn.failure().unwrap().kind(),
        Some(ErrorType::ConnectionTimedOut)
    );

    // With answered pings it stays up.
    let (conn, mut fake) = setup_with(
        Box::new(broker_reply),
        ConnectionOptions {
            client_failure_check_period: Some(Duration::from_millis(40)),
            connection_ttl: Some(Duration::from_millis(100)),
            ..ConnectionOptions::default()
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(conn.failure().is_none());
    let _ = fake.next_raw().await;

    // A check period of -1 sends one ping only.
    let (stream, mut fake) = start_fake(Box::new(broker_reply));
    let _conn = Connection::connect_with(
        stream,
        ConnectionOptions {
            client_failure_check_period: None,
            connection_ttl: None,
            ..ConnectionOptions::default()
        },
    )
    .await
    .unwrap();
    match fake.next_raw().await {
        (0, Packet::Ping(p)) => assert_eq!(p.connection_ttl, -1),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        fake.next_raw().await.1,
        Packet::SubscribeClusterTopologyUpdatesMessageV2(_)
    ));
}

#[tokio::test]
async fn producer_credits_follow_the_java_request_pattern() {
    // Credits are granted only when the test says so.
    let reply: Replier = Box::new(|ch, p| match p {
        Packet::SessionRequestProducerCreditsMessage(_) => vec![],
        _ => broker_reply(ch, p),
    });
    let (conn, mut fake) = setup_with(reply, ConnectionOptions::default()).await;
    let options = SessionOptions {
        block_on_durable_send: false,
        ..SessionOptions::default()
    };
    let session = open_session(&conn, &mut fake, options).await;
    let producer = session.create_producer(Some("q")).await.unwrap();
    // init: half the 64 KiB window.
    match fake.next().await {
        (10, Packet::SessionRequestProducerCreditsMessage(r)) => {
            assert_eq!(r.credits, 32 * 1024);
            assert_eq!(r.address.to_string(), "q");
        }
        other => panic!("{other:?}"),
    }
    let _create = fake.next().await;

    // No credits yet: balance + arriving (32 KiB) <= needed (32 KiB), but
    // needed - arriving is 0, so nothing more is requested and the send waits.
    let send = tokio::spawn(async move {
        producer.send(message::text_message("hello")).await.unwrap();
        producer
    });
    fake.assert_quiet().await;
    assert!(!send.is_finished());

    fake.send(
        10,
        SessionProducerCreditsMessage {
            credits: 32 * 1024,
            address: "q".into(),
        },
    );
    let producer = send.await.unwrap();
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionSendMessageV3(_))
    ));

    // Balance is now 32 KiB - size with nothing arriving: the next send asks
    // for another 32 KiB, as in the golden capture, and goes out at once.
    producer.send(message::text_message("hello")).await.unwrap();
    match fake.next().await {
        (10, Packet::SessionRequestProducerCreditsMessage(r)) => assert_eq!(r.credits, 32 * 1024),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionSendMessageV3(_))
    ));
    // 32 KiB arriving: balance + arriving > 32 KiB, so no new request.
    producer.send(message::text_message("hello")).await.unwrap();
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionSendMessageV3(_))
    ));

    // A message bigger than balance + arriving asks for its size minus
    // what is arriving.
    let mut big = message::bytes_message(&vec![7u8; 70_000]);
    // The credit size is the encode size with the address set.
    big.address = Some("q".into());
    let mut w = saranga_buffer::Writer::new();
    big.encode(&mut w);
    let big_size = w.len() as i32;
    let send = tokio::spawn(async move { producer.send(big).await });
    match fake.next().await {
        (10, Packet::SessionRequestProducerCreditsMessage(r)) => {
            assert_eq!(r.credits, big_size - 32 * 1024)
        }
        other => panic!("{other:?}"),
    }
    fake.send(
        10,
        SessionProducerCreditsMessage {
            credits: 32 * 1024,
            address: "q".into(),
        },
    );
    fake.send(
        10,
        SessionProducerCreditsMessage {
            credits: big_size - 32 * 1024,
            address: "q".into(),
        },
    );
    send.await.unwrap().unwrap();
}

#[tokio::test]
async fn producer_flow_control_off_and_address_full() {
    let (conn, mut fake) = setup().await;
    let session = open_session(
        &conn,
        &mut fake,
        SessionOptions {
            producer_window_size: -1,
            ..SessionOptions::default()
        },
    )
    .await;
    let producer = session.create_producer(Some("q")).await.unwrap();
    assert!(matches!(
        fake.next().await,
        (10, Packet::CreateProducerMessage(_))
    ));
    producer.send(message::text_message("x")).await.unwrap();
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionSendMessageV3(_))
    ));

    // FAIL address-full policy: the broker grants with a fail message.
    let reply: Replier = Box::new(|ch, p| match p {
        Packet::SessionRequestProducerCreditsMessage(r) => vec![(
            ch,
            SessionProducerCreditsFailMessage {
                credits: r.credits,
                address: r.address.clone(),
            }
            .into(),
        )],
        _ => broker_reply(ch, p),
    });
    let (conn, mut fake) = setup_with(reply, ConnectionOptions::default()).await;
    let session = open_session(&conn, &mut fake, SessionOptions::default()).await;
    let producer = session.create_producer(Some("full")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let err = producer.send(message::text_message("x")).await.unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::AddressFull), "{err}");
}

/// The next continuation chunk: (body, continues, message body size,
/// requires response).
async fn next_chunk(fake: &mut Fake) -> (Vec<u8>, bool, i64, bool) {
    match fake.next().await {
        (10, Packet::SessionSendContinuationMessageV3(c)) => {
            assert_eq!(c.sender_id, 1);
            (
                c.body,
                c.continues,
                c.message_body_size,
                c.requires_response,
            )
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn large_messages_are_sent_in_chunks() {
    let (conn, mut fake) = setup().await;
    let options = SessionOptions {
        producer_window_size: -1,
        min_large_message_size: 1024,
        ..SessionOptions::default()
    };
    let session = open_session(&conn, &mut fake, options).await;
    let producer = session.create_producer(Some("q")).await.unwrap();
    let _create = fake.next().await;

    // 4 + body <= min-large-message-size: a regular send.
    producer
        .send(message::bytes_message(&[7; 1020]))
        .await
        .unwrap();
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionSendMessageV3(_))
    ));

    // The headers, then chunks of 1024 bytes; only the last one (durable,
    // so blocking) asks for a response and carries the body size.
    let body: Vec<u8> = (0..2500u32).map(|i| i as u8).collect();
    let mut m = message::bytes_message(&body);
    m.put_string_property("p", "v");
    producer.send(m).await.unwrap();
    match fake.next().await {
        (10, Packet::SessionSendLargeMessage(l)) => {
            assert!(l.message.body.is_empty());
            assert_eq!(l.message.address.as_ref().unwrap().to_string(), "q");
            assert_eq!(l.message.string_property("p").as_deref(), Some("v"));
            assert_eq!(l.message.property(message::HDR_LARGE_COMPRESSED), None);
        }
        other => panic!("{other:?}"),
    }
    let mut received = Vec::new();
    for _ in 0..2 {
        let (chunk, continues, _, requires_response) = next_chunk(&mut fake).await;
        assert_eq!(chunk.len(), 1024);
        assert!(continues && !requires_response);
        received.extend(chunk);
    }
    let (chunk, continues, size, requires_response) = next_chunk(&mut fake).await;
    assert_eq!((chunk.len(), continues, size), (452, false, 2500));
    assert!(requires_response);
    received.extend(chunk);
    assert_eq!(received, body);

    // A multiple of the chunk size ends with an empty last chunk, as Java
    // reads the body until end of stream. Non-durable: nothing blocks.
    let mut m = message::bytes_message(&[1; 2048]);
    m.set_durable(false);
    producer.send(m).await.unwrap();
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionSendLargeMessage(_))
    ));
    for _ in 0..2 {
        let (chunk, continues, _, _) = next_chunk(&mut fake).await;
        assert_eq!((chunk.len(), continues), (1024, true));
    }
    let (chunk, continues, size, requires_response) = next_chunk(&mut fake).await;
    assert_eq!((chunk.len(), continues, size), (0, false, 2048));
    assert!(!requires_response);

    // Headers of min-large-message-size or more are refused, nothing sent.
    let mut m = message::bytes_message(&[0; 2000]);
    m.put_string_property("big", &"x".repeat(600));
    let err = producer.send(m).await.unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::IllegalState), "{err}");
    assert!(err.to_string().contains("AMQ219022"), "{err}");
    fake.assert_quiet().await;
}

#[tokio::test]
async fn compressed_large_messages() {
    let (conn, mut fake) = setup().await;
    let options = SessionOptions {
        producer_window_size: -1,
        min_large_message_size: 1024,
        compress_large_messages: true,
        ..SessionOptions::default()
    };
    let session = open_session(&conn, &mut fake, options).await;
    let producer = session.create_producer(Some("q")).await.unwrap();
    let _create = fake.next().await;

    // Compressed below the minimum: a regular message with its real size.
    producer
        .send(message::bytes_message(&[b'a'; 10_000]))
        .await
        .unwrap();
    match fake.next().await {
        (10, Packet::SessionSendMessageV3(s)) => {
            let m = s.message;
            assert!(m.body.len() < 1024);
            assert_eq!(
                m.property(message::HDR_LARGE_COMPRESSED),
                Some(&PropertyValue::Bool(true))
            );
            assert_eq!(
                m.property(message::HDR_LARGE_BODY_SIZE),
                Some(&PropertyValue::Long(10_000))
            );
        }
        other => panic!("{other:?}"),
    }

    // Still large once compressed: chunks of compressed bytes, and the
    // uncompressed size on the last one.
    let mut x = 0x2545_f491u32;
    let noise: Vec<u8> = (0..5000)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect();
    producer.send(message::bytes_message(&noise)).await.unwrap();
    match fake.next().await {
        (10, Packet::SessionSendLargeMessage(l)) => assert_eq!(
            l.message.property(message::HDR_LARGE_COMPRESSED),
            Some(&PropertyValue::Bool(true))
        ),
        other => panic!("{other:?}"),
    }
    let mut compressed = 0;
    loop {
        let (chunk, continues, size, _) = next_chunk(&mut fake).await;
        compressed += chunk.len();
        if !continues {
            assert_eq!(size, 5000);
            break;
        }
        assert_eq!(chunk.len(), 1024);
    }
    assert!(compressed > 1024);
}

#[tokio::test]
async fn large_messages_are_received_whole() {
    let (conn, mut fake) = setup().await;
    let session = open_session(&conn, &mut fake, SessionOptions::default()).await;
    session.start().await.unwrap();
    let _start = fake.next().await;
    let consumer = session
        .create_consumer_with(
            "q",
            ConsumerOptions {
                window_size: Some(4096),
                ..ConsumerOptions::default()
            },
        )
        .await
        .unwrap();
    let _create = fake.next().await;
    assert_eq!(flow_credits(&fake.next().await.1), 4096);

    let body: Vec<u8> = (0..2500u32).map(|i| (i % 251) as u8).collect();
    let mut m = message::bytes_message(&[]);
    m.message_id = 42;
    m.put_string_property("p", "v");
    let header: Packet = SessionReceiveLargeMessage {
        consumer_id: 0,
        delivery_count: 1,
        large_message_size: body.len() as i64,
        message: m,
    }
    .into();
    let header_size = frame(10, &header).len() as i32;
    let chunk = |range: std::ops::Range<usize>, continues| -> Packet {
        SessionReceiveContinuationMessage {
            body: body[range].to_vec(),
            continues,
            consumer_id: 0,
        }
        .into()
    };
    let chunk_size = frame(10, &chunk(0..1000, true)).len() as i32;
    fake.send(10, header);
    fake.send(10, chunk(0..1000, true));
    fake.send(10, chunk(1000..2000, true));
    // Not read yet: the chunks' credits are held.
    fake.assert_quiet().await;
    assert_eq!(consumer.buffered(), 1);

    let receive = tokio::spawn(async move {
        let d = consumer.receive(WAIT).await.unwrap().unwrap();
        (consumer, d)
    });
    // Taken: the header, then the chunks held (past the 2048 client window).
    assert_eq!(
        flow_credits(&fake.next().await.1),
        header_size + 2 * chunk_size
    );
    fake.send(10, chunk(2000..2500, false));
    let (consumer, d) = receive.await.unwrap();
    assert_eq!(d.message_id(), 42);
    assert_eq!(d.message.body, body);
    assert_eq!(d.message.string_property("p").as_deref(), Some("v"));
    assert_eq!(d.message.address.as_ref().unwrap().to_string(), "q");
    assert_eq!(d.flow_control_size(), header_size);
    assert!(d.encode_size() > 2500);

    // A regular message after it arrives in order.
    fake.send(10, delivery(0, 43, "after", 4));
    let d = consumer.receive(WAIT).await.unwrap().unwrap();
    assert_eq!(d.message.text().unwrap().as_deref(), Some("after"));

    // A continuation with no large message in progress: credits go back.
    fake.send(10, chunk(0..1000, false));
    fake.assert_quiet().await;
    fake.send(10, chunk(0..1000, false));
    let credits = flow_credits(&fake.next().await.1);
    assert!(credits >= 2048, "{credits}");
}

#[tokio::test]
async fn consumer_credits_follow_the_window() {
    let (conn, mut fake) = setup().await;
    let session = open_session(&conn, &mut fake, SessionOptions::default()).await;
    session.start().await.unwrap();
    assert!(matches!(fake.next().await, (10, Packet::SessionStart(_))));

    // Default window: the queue's address-settings window (1 MiB) is sent.
    let c0 = session.create_consumer("q", None, false).await.unwrap();
    match fake.next().await {
        (10, Packet::SessionCreateConsumerMessage(c)) => {
            assert_eq!(c.id, 0);
            assert_eq!(c.queue_name.to_string(), "q");
            assert!(c.requires_response);
            assert!(!c.browse_only);
            assert_eq!(c.priority, 0);
        }
        other => panic!("{other:?}"),
    }
    match fake.next().await {
        (10, Packet::SessionConsumerFlowCreditMessage(f)) => {
            assert_eq!((f.consumer_id, f.credits), (0, 1024 * 1024));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(c0.client_window_size(), 512 * 1024);

    // Window 4096: credits go back once 2048 bytes of frames are consumed.
    let c1 = session
        .create_consumer_with(
            "q",
            ConsumerOptions {
                window_size: Some(4096),
                filter: Some("a = 1".into()),
                ..ConsumerOptions::default()
            },
        )
        .await
        .unwrap();
    match fake.next().await {
        (10, Packet::SessionCreateConsumerMessage(c)) => {
            assert_eq!(c.id, 1);
            assert_eq!(c.filter_string.unwrap().to_string(), "a = 1");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(flow_credits(&fake.next().await.1), 4096);

    let body = "x".repeat(300);
    let packet = delivery(1, 1, &body, 4);
    let size = frame(10, &packet).len() as i32;
    let per_window = (2048 + size - 1) / size;
    for id in 0..per_window as i64 * 2 {
        fake.send(10, delivery(1, id, &body, 4));
    }
    for i in 1..=per_window * 2 {
        let d = c1.receive(WAIT).await.unwrap().unwrap();
        assert_eq!(d.flow_control_size(), size);
        assert_eq!(d.encode_size(), size - 13 - 12);
        if i % per_window == 0 {
            // The whole accumulated amount, whole frames included.
            assert_eq!(flow_credits(&fake.next().await.1), size * per_window);
        } else {
            fake.assert_quiet().await;
        }
    }
}

#[tokio::test]
async fn unlimited_and_slow_consumers() {
    let reply: Replier = Box::new(|ch, p| match p {
        // Forced delivery: the broker answers with the marker message.
        Packet::SessionForceConsumerDelivery(f) => {
            let mut m = message::new_message(0, false);
            m.put_property(FORCED_DELIVERY_MESSAGE, PropertyValue::Long(f.sequence));
            vec![(
                ch,
                SessionReceiveMessage {
                    message: m,
                    consumer_id: f.consumer_id,
                    delivery_count: 0,
                }
                .into(),
            )]
        }
        _ => broker_reply(ch, p),
    });
    let (conn, mut fake) = setup_with(reply, ConnectionOptions::default()).await;
    let session = open_session(&conn, &mut fake, SessionOptions::default()).await;
    session.start().await.unwrap();
    let _start = fake.next().await;

    // Window -1: -1 credits to start it, and never any more.
    let unlimited = session
        .create_consumer_with(
            "q",
            ConsumerOptions {
                window_size: Some(-1),
                ..ConsumerOptions::default()
            },
        )
        .await
        .unwrap();
    let _create = fake.next().await;
    assert_eq!(flow_credits(&fake.next().await.1), -1);
    for id in 0..3 {
        fake.send(10, delivery(0, id, "m", 4));
    }
    for _ in 0..3 {
        unlimited.receive(WAIT).await.unwrap().unwrap();
    }
    fake.assert_quiet().await;

    // Window 0: no initial credits; each receive sends 1 credit first.
    let slow = session
        .create_consumer_with(
            "q",
            ConsumerOptions {
                window_size: Some(0),
                ..ConsumerOptions::default()
            },
        )
        .await
        .unwrap();
    let _create = fake.next().await;
    fake.assert_quiet().await;
    let packet = delivery(1, 10, "slow", 4);
    let size = frame(10, &packet).len() as i32;
    let receive = tokio::spawn(async move {
        let d = slow.receive(WAIT).await.unwrap();
        (slow, d)
    });
    assert_eq!(flow_credits(&fake.next().await.1), 1);
    fake.send(10, packet);
    let (slow, d) = receive.await.unwrap();
    assert_eq!(d.unwrap().message.text().unwrap().as_deref(), Some("slow"));
    // The message's credits minus the one sent up front.
    assert_eq!(flow_credits(&fake.next().await.1), size - 1);

    // Nothing there: 1 credit, wait, reset to 0; then the forced delivery
    // round (1 credit, force with sequence 0, marker, reset to 0).
    let none = slow.receive(Duration::from_millis(100)).await.unwrap();
    assert!(none.is_none());
    assert_eq!(flow_credits(&fake.next().await.1), 1);
    assert_eq!(flow_credits(&fake.next().await.1), 0);
    assert_eq!(flow_credits(&fake.next().await.1), 1);
    match fake.next().await {
        (10, Packet::SessionForceConsumerDelivery(f)) => {
            assert_eq!((f.consumer_id, f.sequence), (1, 0));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(flow_credits(&fake.next().await.1), 0);

    // receive_immediate forces again with the next sequence.
    assert!(slow.receive_immediate().await.unwrap().is_none());
    assert_eq!(flow_credits(&fake.next().await.1), 1);
    match fake.next().await {
        (10, Packet::SessionForceConsumerDelivery(f)) => assert_eq!(f.sequence, 1),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn acks_are_batched_then_flushed() {
    let (conn, mut fake) = setup().await;
    // Default ack batch size (1 MiB): acks wait for commit.
    let session = open_session(&conn, &mut fake, SessionOptions::transacted()).await;
    session.start().await.unwrap();
    let _start = fake.next().await;
    let consumer = session.create_consumer("q", None, false).await.unwrap();
    let _create = fake.next().await;
    let _credits = fake.next().await;
    for id in [5, 6] {
        fake.send(10, delivery(0, id, "m", 4));
    }
    for _ in 0..2 {
        let d = consumer.receive(WAIT).await.unwrap().unwrap();
        consumer.acknowledge(&d).await.unwrap();
    }
    fake.assert_quiet().await;
    session.commit().await.unwrap();
    match fake.next().await {
        (10, Packet::SessionAcknowledgeMessage(a)) => {
            assert_eq!((a.consumer_id, a.message_id), (0, 6));
            assert!(!a.requires_response);
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionCommitMessageV2(_))
    ));

    // Batch size 0 acks at once; block_on_acknowledge makes it blocking.
    let s2 = conn
        .create_session(SessionOptions {
            ack_batch_size: 0,
            block_on_acknowledge: true,
            ..SessionOptions::default()
        })
        .await
        .unwrap();
    let _create_session = fake.next().await;
    s2.start().await.unwrap();
    let _start = fake.next().await;
    let consumer = s2.create_consumer("q", None, false).await.unwrap();
    let _create = fake.next().await;
    let _credits = fake.next().await;
    fake.send(11, delivery(0, 7, "m", 4));
    let d = consumer.receive(WAIT).await.unwrap().unwrap();
    consumer.acknowledge(&d).await.unwrap();
    match fake.next().await {
        (11, Packet::SessionAcknowledgeMessage(a)) => {
            assert_eq!(a.message_id, 7);
            assert!(a.requires_response);
        }
        other => panic!("{other:?}"),
    }

    // A message with a non-default priority switches to individual acks.
    fake.send(11, delivery(0, 8, "urgent", 9));
    let d = consumer.receive(WAIT).await.unwrap().unwrap();
    consumer.acknowledge(&d).await.unwrap();
    match fake.next().await {
        (11, Packet::SessionIndividualAcknowledgeMessage(a)) => assert_eq!(a.message_id, 8),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn buffered_messages_are_received_by_priority() {
    let (conn, mut fake) = setup().await;
    let session = open_session(&conn, &mut fake, SessionOptions::default()).await;
    let consumer = session.create_consumer("q", None, false).await.unwrap();
    let _create = fake.next().await;
    let _credits = fake.next().await;
    for (id, prio) in [(1, 4), (2, 9), (3, 0), (4, 9)] {
        fake.send(10, delivery(0, id, "m", prio));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(consumer.buffered(), 4);
    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(consumer.receive(WAIT).await.unwrap().unwrap().message_id());
    }
    assert_eq!(ids, [2, 4, 1, 3]);
}

#[tokio::test]
async fn rollback_stops_returns_credits_and_restarts() {
    let (conn, mut fake) = setup().await;
    let session = open_session(&conn, &mut fake, SessionOptions::transacted()).await;
    session.start().await.unwrap();
    let _start = fake.next().await;
    let consumer = session
        .create_consumer_with(
            "q",
            ConsumerOptions {
                window_size: Some(2),
                ..ConsumerOptions::default()
            },
        )
        .await
        .unwrap();
    let _create = fake.next().await;
    assert_eq!(flow_credits(&fake.next().await.1), 2);
    let packet = delivery(0, 1, "a", 4);
    let size = frame(10, &packet).len() as i32;
    fake.send(10, packet);
    fake.send(10, delivery(0, 2, "b", 4));
    let d = consumer.receive(WAIT).await.unwrap().unwrap();
    consumer.acknowledge(&d).await.unwrap();
    // Client window 1: every consumed message returns its credits.
    assert_eq!(flow_credits(&fake.next().await.1), size);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(consumer.buffered(), 1);

    session.rollback(false).await.unwrap();
    assert!(matches!(fake.next().await, (10, Packet::SessionStop(_))));
    // The buffered message's credits.
    assert_eq!(flow_credits(&fake.next().await.1), size);
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionAcknowledgeMessage(_))
    ));
    match fake.next().await {
        (10, Packet::RollbackMessage(r)) => assert!(!r.consider_last_message_as_delivered),
        other => panic!("{other:?}"),
    }
    assert!(matches!(fake.next().await, (10, Packet::SessionStart(_))));
    assert_eq!(consumer.buffered(), 0);
}

#[tokio::test]
async fn session_operations_send_java_defaults() {
    let (conn, mut fake) = setup().await;
    let session = open_session(&conn, &mut fake, SessionOptions::default()).await;

    session
        .create_address(
            "addr",
            &[RoutingType::Anycast, RoutingType::Multicast],
            true,
        )
        .await
        .unwrap();
    match fake.next().await {
        (10, Packet::CreateAddressMessage(a)) => {
            assert_eq!(a.routing_types, [0, 1]);
            assert!(a.requires_response && a.auto_created);
        }
        other => panic!("{other:?}"),
    }

    session
        .create_queue(QueueConfiguration::new("q"))
        .await
        .unwrap();
    match fake.next().await {
        (10, Packet::CreateQueueMessageV2(q)) => {
            let expected = CreateQueueMessageV2 {
                address: "q".into(),
                queue_name: "q".into(),
                durable: true,
                requires_response: true,
                routing_type: -1,
                max_consumers: -1,
                sections: CreateQueueMessageV2::SECTIONS,
                ..Default::default()
            };
            assert_eq!(q, expected);
        }
        other => panic!("{other:?}"),
    }

    let err = session
        .create_queue(QueueConfiguration::new("t").temporary(true))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::InternalError));

    session
        .create_shared_queue(
            QueueConfiguration::new("shared")
                .address("addr")
                .routing_type(RoutingType::Multicast)
                .filter("x > 1"),
        )
        .await
        .unwrap();
    match fake.next().await {
        (10, Packet::CreateSharedQueueMessageV2(q)) => {
            assert_eq!(q.address.to_string(), "addr");
            assert_eq!(q.routing_type, 0);
            assert_eq!(q.filter_string.unwrap().to_string(), "x > 1");
            assert_eq!(q.max_consumers.0, None);
            assert!(q.durable && q.requires_response);
        }
        other => panic!("{other:?}"),
    }

    let a = session.address_query("addr").await.unwrap();
    assert!(a.exists && a.supports_anycast && !a.supports_multicast);
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionBindingQueryMessage(_))
    ));

    session.delete_queue("q").await.unwrap();
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionDeleteQueueMessage(_))
    ));
    session.add_unique_metadata("u", "1").await.unwrap();
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionUniqueAddMetaDataMessage(_))
    ));

    // Close: consumers (blocking close), producers, then the session.
    let consumer = session.create_consumer("q", None, true).await.unwrap();
    match fake.next().await {
        (10, Packet::SessionCreateConsumerMessage(c)) => assert!(c.browse_only),
        other => panic!("{other:?}"),
    }
    let _credits = fake.next().await;
    let producer = session.create_producer(None).await.unwrap();
    match fake.next().await {
        (10, Packet::CreateProducerMessage(p)) => assert_eq!(p.address, None),
        other => panic!("{other:?}"),
    }
    session.close().await;
    match fake.next().await {
        (10, Packet::SessionConsumerCloseMessage(c)) => assert_eq!(c.consumer_id, 0),
        other => panic!("{other:?}"),
    }
    match fake.next().await {
        (10, Packet::RemoveProducerMessage(p)) => assert_eq!(p.id, 1),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionCloseMessage(_))
    ));
    assert!(session.is_closed());
    assert!(consumer.is_closed());
    let err = producer.send(message::text_message("x")).await.unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::ObjectClosed));
    let err = session.queue_query("q").await.unwrap_err();
    assert_eq!(err.kind(), Some(ErrorType::ObjectClosed));
}

#[tokio::test]
async fn expired_messages_are_expired_not_returned() {
    let (conn, mut fake) = setup().await;
    let session = open_session(&conn, &mut fake, SessionOptions::default()).await;
    let consumer = session.create_consumer("q", None, false).await.unwrap();
    let _create = fake.next().await;
    let _credits = fake.next().await;
    let mut m = message::text_message("old");
    m.message_id = 3;
    m.expiration = 1;
    fake.send(
        10,
        SessionReceiveMessage {
            message: m,
            consumer_id: 0,
            delivery_count: 1,
        },
    );
    fake.send(10, delivery(0, 4, "fresh", 4));
    let d = consumer.receive(WAIT).await.unwrap().unwrap();
    assert_eq!(d.message_id(), 4);
    match fake.next().await {
        (10, Packet::SessionExpireMessage(e)) => {
            assert_eq!((e.consumer_id, e.message_id), (0, 3));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn connection_close_closes_sessions_without_disconnect() {
    let (conn, mut fake) = setup().await;
    let _session = open_session(&conn, &mut fake, SessionOptions::default()).await;
    conn.close().await;
    assert!(matches!(
        fake.next().await,
        (10, Packet::SessionCloseMessage(_))
    ));
    // Then the transport closes with nothing else sent.
    assert!(
        tokio::time::timeout(WAIT, fake.received.recv())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        conn.failure().unwrap().kind(),
        Some(ErrorType::NotConnected)
    );
}
