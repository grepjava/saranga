//! An AMQP connection: `AMQPConnectionContext`, `ProtonHandler`,
//! `AMQPSessionContext`, the link contexts, `ProtonTransactionHandler` and
//! `AMQPSessionCallback`, with the parts of Proton-J's transport they
//! rely on.
//!
//! One task reads and handles frames in order. Everything written goes
//! through one ordered queue whose entries wait for the journal writes
//! before them, so a disposition goes out once its message is stored. The
//! queues delivering to the client's links hand their messages to the
//! task, which assigns delivery IDs and writes the transfers.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use saranga_core_protocol::ErrorType;
use saranga_security::Authenticated;
use saranga_server::message::{ServerMessage, now_millis};
use saranga_server::protocol::Connection as Accepted;
use saranga_server::queue::{DeliverySink, FORCED_DELIVERY_MESSAGE, Queue};
use saranga_server::session::{
    ClientConnection, Completion, ServerSession, SessionOptions, Transport,
};
use saranga_server::{Broker, BrokerError, RoutingType};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::composite::{Composite, Fields};
use crate::frame::{self, AMQP_HEADER, FrameBody, SASL_HEADER};
use crate::link::{self, LinkError, ReceiverTarget, SenderSource, capability};
use crate::message::AmqpMessage;
use crate::messaging::{
    DeliveryState, Modified, Rejected, Source, TargetKind, TerminusDurability,
    TerminusExpiryPolicy, section,
};
use crate::native::AmqpNative;
use crate::sasl::{self, SaslFrame, SaslInit, SaslMechanisms, SaslOutcome};
use crate::transaction::{
    Coordinator, Declare, Declared, Discharge, TransactionalState, capability as txn_cap,
};
use crate::transport::{
    Attach, Begin, Close, Detach, Disposition, End, Error, Flow, Open, Performative,
    ReceiverSettleMode, Role, SenderSettleMode, Transfer, condition,
};
use crate::types::{Symbol, Value};
use crate::{AmqpConfig, Settings};

/// The SASL mechanisms offered by default (`MechanismFinder`).
const DEFAULT_MECHANISMS: [&str; 2] = ["PLAIN", "ANONYMOUS"];
/// `ActiveMQClient.DEFAULT_CONNECTION_TTL`.
const DEFAULT_CONNECTION_TTL: i64 = 60_000;
/// Session windows (Proton-J's `DEFAULT_WINDOW_SIZE`).
const WINDOW: u32 = i32::MAX as u32;
const CHANNEL_MAX: u16 = 65535;
const HANDLE_MAX: u32 = 65535;
/// `amqp:connection-establishment-failed`.
const CONNECTION_OPEN_FAILED: &str = "amqp:connection-establishment-failed";

/// Container IDs of the connections open now (`connectedClientIds`).
#[derive(Default)]
pub struct ContainerIds(Mutex<HashMap<String, usize>>);

impl ContainerIds {
    /// `addClientConnection`: false if `sole` and another connection has
    /// the ID.
    fn add(&self, id: &str, sole: bool) -> bool {
        let mut m = self.0.lock().unwrap_or_else(|e| e.into_inner());
        match m.get_mut(id) {
            Some(n) if sole && *n != 0 => false,
            Some(n) => {
                *n += 1;
                true
            }
            None => {
                m.insert(id.to_owned(), 1);
                true
            }
        }
    }

    fn remove(&self, id: &str) {
        let mut m = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(n) = m.get_mut(id) {
            *n -= 1;
            if *n == 0 {
                m.remove(id);
            }
        }
    }
}

// ------------------------------------------------------------ output

enum Out {
    /// Bytes to write once these journal writes are done.
    Send { wait: Completion, bytes: Vec<u8> },
    /// Everything before is written: close.
    Close,
}

#[derive(Clone)]
struct Outbound(mpsc::UnboundedSender<Out>);

impl Outbound {
    fn send(&self, wait: Completion, bytes: Vec<u8>) {
        let _ = self.0.send(Out::Send { wait, bytes });
    }

    fn now(&self, bytes: Vec<u8>) {
        self.send(Completion::default(), bytes);
    }

    fn close(&self) {
        let _ = self.0.send(Out::Close);
    }

    fn frame(&self, channel: u16, p: Performative, payload: &[u8], wait: Completion) {
        let mut out = Vec::new();
        frame::encode_amqp(&mut out, channel, &p, payload);
        self.send(wait, out);
    }
}

async fn write_loop<W: AsyncWrite + Unpin>(mut wr: W, mut rx: mpsc::UnboundedReceiver<Out>) {
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut broken = false;
    while let Some(out) = rx.recv().await {
        let close = match out {
            Out::Send { wait, bytes } => {
                if let Err(e) = wait.wait().await {
                    tracing::error!("AMQ119031: storage operation failed: {e}");
                }
                buf.extend_from_slice(&bytes);
                false
            }
            Out::Close => true,
        };
        if !broken && !buf.is_empty() && (close || rx.is_empty() || buf.len() > 256 * 1024) {
            broken = wr.write_all(&buf).await.is_err() || wr.flush().await.is_err();
            buf.clear();
        }
        if close {
            break;
        }
    }
    let _ = wr.shutdown().await;
}

// ------------------------------------------------------------ input

struct Reader<R> {
    rd: R,
    buf: Vec<u8>,
}

#[derive(Debug)]
enum ReadError {
    Closed,
    TooLarge(u32),
    Malformed(String),
}

impl<R: AsyncRead + Unpin> Reader<R> {
    async fn fill(&mut self, n: usize) -> Result<(), ReadError> {
        while self.buf.len() < n {
            let mut chunk = [0u8; 16 * 1024];
            let got = self
                .rd
                .read(&mut chunk)
                .await
                .map_err(|_| ReadError::Closed)?;
            if got == 0 {
                return Err(ReadError::Closed);
            }
            self.buf.extend_from_slice(&chunk[..got]);
        }
        Ok(())
    }

    async fn header(&mut self) -> Result<[u8; 8], ReadError> {
        self.fill(8).await?;
        let h: [u8; 8] = self.buf[..8].try_into().expect("8 bytes");
        self.buf.drain(..8);
        Ok(h)
    }

    /// The next frame, no larger than `max`.
    async fn frame(&mut self, max: u32) -> Result<Vec<u8>, ReadError> {
        self.fill(8).await?;
        let size = frame::frame_size(&self.buf).expect("4 bytes");
        if size < 8 {
            return Err(ReadError::Malformed(format!("frame size {size}")));
        }
        if size > max {
            return Err(ReadError::TooLarge(size));
        }
        self.fill(size as usize).await?;
        Ok(self.buf.drain(..size as usize).collect())
    }
}

// ------------------------------------------------------------ deliveries

/// What the queues tell the connection's task.
enum Event {
    /// A message for a link the client receives on.
    Deliver {
        channel: u16,
        handle: u32,
        message: Arc<ServerMessage>,
        delivery_count: i32,
    },
    /// A drain the client asked for is done: no more messages now.
    Drained { channel: u16, handle: u32 },
    /// The broker closes the link (`AMQPSessionCallback.disconnect`).
    Disconnect {
        channel: u16,
        handle: u32,
        reason: String,
    },
    /// A broker connection attaches a link on its session.
    Open(Box<LocalLink>),
    /// A broker connection is done with the connection: it closes.
    Fail(String),
}

/// How a broker connection reaches its connection's task.
#[derive(Clone)]
pub struct ConnectionHandle(mpsc::UnboundedSender<Event>);

impl ConnectionHandle {
    /// Attaches a link on the connection's session.
    pub fn open(&self, link: LocalLink) {
        let _ = self.0.send(Event::Open(Box::new(link)));
    }

    /// Closes the connection (`fail`).
    pub fn fail(&self, reason: impl Into<String>) {
        let _ = self.0.send(Event::Fail(reason.into()));
    }

    /// Whether the connection's task is gone.
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

/// What a connection the broker opens says and does
/// (`createOutgoingConnectionEntry`).
pub struct Outgoing {
    pub user: Option<String>,
    pub password: Option<String>,
    /// The URI's `saslMechanisms`, when given: the only ones chosen.
    pub sasl_mechanisms: Option<Vec<String>>,
    /// Added to the Open's properties.
    pub properties: Fields,
    pub desired_capabilities: Option<Vec<Symbol>>,
    /// Told when the peer's Open comes.
    pub on_open: Option<Box<dyn FnOnce() + Send + Sync>>,
}

/// How a connection the broker opened ended.
#[derive(Debug, Clone, PartialEq)]
pub enum Ended {
    /// The peer closed it, or the socket went.
    Closed,
    /// It never opened: the handshake failed.
    Refused(String),
    /// A link of the broker connection failed (`error(e, lastRetryCounter)`).
    LinkFailed(String),
    /// The broker connection closed it.
    Stopped,
}

/// A sender link's credit as the queue sees it: what the client gave less
/// what is on its way.
#[derive(Default)]
struct Credit {
    available: i64,
    /// Deliveries handed to the task and not written yet.
    pending: i64,
    closed: bool,
}

/// `ProtonServerSenderContext` as the queue's consumer sink.
struct Sink {
    events: mpsc::UnboundedSender<Event>,
    channel: u16,
    handle: u32,
    credit: Arc<Mutex<Credit>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl DeliverySink for Sink {
    fn deliver(&self, _consumer_id: i64, message: &ServerMessage, delivery_count: i32) -> i64 {
        if message
            .core()
            .properties
            .get(&saranga_buffer::SimpleString::from(FORCED_DELIVERY_MESSAGE))
            .is_some()
        {
            let _ = self.events.send(Event::Drained {
                channel: self.channel,
                handle: self.handle,
            });
            return 0;
        }
        {
            let mut c = lock(&self.credit);
            c.available -= 1;
            c.pending += 1;
        }
        let message = delivery_message(message);
        let _ = self.events.send(Event::Deliver {
            channel: self.channel,
            handle: self.handle,
            message,
            delivery_count,
        });
        0
    }

    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        0
    }

    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        0
    }

    fn chunk_size(&self) -> usize {
        100 * 1024
    }

    fn supports_large(&self) -> bool {
        false
    }

    fn disconnect(&self, _consumer_id: i64, reason: &str) {
        let _ = self.events.send(Event::Disconnect {
            channel: self.channel,
            handle: self.handle,
            reason: reason.to_owned(),
        });
    }

    /// `hasCredits`: credit left and the link open.
    fn has_credits(&self, _message: &ServerMessage) -> bool {
        let c = lock(&self.credit);
        !c.closed && c.available > 0
    }
}

/// The delivery's own handle on the message the queue keeps: the native
/// form if it has one, else its Core form.
fn delivery_message(m: &ServerMessage) -> Arc<ServerMessage> {
    match m.native() {
        Some(n) => ServerMessage::new_native(n.clone()),
        None => ServerMessage::new(m.delivery_core().clone()),
    }
}

/// `AmqpTransferTagGenerator`: pooled tags, else the next counter value in
/// its fewest big-endian bytes.
#[derive(Default)]
struct Tags {
    next: u64,
    pool: Vec<Vec<u8>>,
}

impl Tags {
    fn take(&mut self) -> Vec<u8> {
        if let Some(t) = self.pool.pop() {
            return t;
        }
        let n = self.next;
        self.next += 1;
        let bytes = n.to_be_bytes();
        let skip = bytes.iter().take_while(|&&b| b == 0).count().min(7);
        bytes[skip..].to_vec()
    }

    fn give_back(&mut self, tag: Vec<u8>) {
        if self.pool.len() < 1024 {
            self.pool.push(tag);
        }
    }
}

// ------------------------------------------------------------ links

/// A delivery the broker sent and the client has not settled.
struct OutDelivery {
    handle: u32,
    consumer: i64,
    message_id: i64,
    tag: Vec<u8>,
    /// Acknowledged in this transaction, settled when it commits.
    txn: Option<Vec<u8>>,
    /// The client settled it (with a transactional acknowledgement).
    remote_settled: bool,
}

/// A message arriving in parts.
struct Incoming {
    delivery_id: Option<u32>,
    format: u32,
    settled: bool,
    state: Option<DeliveryState>,
    payload: Vec<u8>,
}

/// A link the client sends on (`ProtonServerReceiverContext`).
struct ReceiverLink {
    name: String,
    target: ReceiverTarget,
    source: Option<Source>,
    /// What the broker granted and has not been used.
    credit: u32,
    delivery_count: u32,
    incoming: Option<Incoming>,
    dynamic_expiry: TerminusExpiryPolicy,
}

/// A link the client receives on (`ProtonServerSenderContext`).
struct SenderLink {
    name: String,
    consumer: i64,
    resolved: SenderSource,
    source: Option<Source>,
    credit: Arc<Mutex<Credit>>,
    /// The link's credit and delivery count, as the sender keeps them.
    link_credit: u32,
    delivery_count: u32,
    presettled: bool,
    draining: bool,
}

/// The transaction coordinator link (`ProtonTransactionHandler`).
struct CoordinatorLink {
    credit: u32,
    delivery_count: u32,
    incoming: Option<Incoming>,
}

enum Link {
    Receiver(ReceiverLink),
    Sender(SenderLink),
    Coordinator(CoordinatorLink),
}

/// A session, on this end's channel. Its links are kept by this end's
/// handles: the peer's are mapped to them as its frames come
/// (`TransportSession`).
struct Session {
    server: ServerSession,
    next_outgoing_id: u32,
    next_incoming_id: u32,
    remote_incoming_window: u32,
    next_delivery_id: u32,
    next_consumer: i64,
    links: HashMap<u32, Link>,
    out: BTreeMap<u32, OutDelivery>,
    tags: Tags,
    /// Transfer frames waiting for the client's incoming window.
    blocked: std::collections::VecDeque<Vec<u8>>,
    /// The peer's handles, and this end's for them.
    handles: HashMap<u32, u32>,
    /// Links this end detached, waiting for the peer's detach.
    detaching: std::collections::HashSet<u32>,
    /// Links this end attached, waiting for the peer's attach.
    pending: HashMap<u32, LocalLink>,
}

impl Session {
    /// `allocateLocalHandle`: the lowest handle not in use.
    fn allocate_handle(&self) -> u32 {
        (0..=HANDLE_MAX)
            .find(|h| {
                !self.links.contains_key(h)
                    && !self.detaching.contains(h)
                    && !self.pending.contains_key(h)
            })
            .unwrap_or(HANDLE_MAX)
    }

    /// This end's handle for the peer's.
    fn local_handle(&self, remote: u32) -> u32 {
        self.handles.get(&remote).copied().unwrap_or(u32::MAX)
    }
}

/// A link this end attaches (a broker connection's), waiting for the
/// peer's attach.
pub struct LocalLink {
    /// The attach sent; its handle is given when it is.
    pub attach: Attach,
    pub kind: LocalLinkKind,
}

/// What a link this end attaches is for.
#[derive(Debug, Clone)]
pub enum LocalLinkKind {
    /// Sends a queue's messages (`AMQPOutgoingController`).
    Sender { queue: String },
    /// Takes the peer's messages to a queue's address, with its routing
    /// type (`connectReceiver`).
    Receiver {
        queue: String,
        address: String,
        routing: RoutingType,
    },
}

/// A declared transaction (`ProtonTransactionImpl`).
struct Txn {
    tx: saranga_server::transaction::TxRef,
    /// The session that declared it.
    channel: u16,
}

/// A failure that ends the connection.
struct Fatal(Error);

impl Fatal {
    fn new(condition: &str, description: impl Into<String>) -> Self {
        Self(Error::new(condition, Some(description.into())))
    }
}

type Handled = Result<(), Fatal>;

struct Conn {
    broker: Arc<Broker>,
    client: Arc<ClientConnection>,
    settings: Settings,
    config: AmqpConfig,
    registry: Arc<ContainerIds>,
    out: Outbound,
    events: mpsc::UnboundedSender<Event>,
    /// From the SASL exchange.
    user: Option<String>,
    password: Option<String>,
    auth: Authenticated,
    remote_container: Option<String>,
    registered: bool,
    opened: bool,
    remote_max_frame: u32,
    /// The client's idle-time-out: an empty frame goes out in half of it.
    remote_idle: Option<Duration>,
    /// `connection-ttl`: the connection closes when nothing comes for it.
    ttl: Option<Duration>,
    last_read: Instant,
    last_write: Instant,
    /// Sessions by this end's channel.
    sessions: HashMap<u16, Session>,
    /// The peer's channels, and this end's for them.
    channels: HashMap<u16, u16>,
    txns: HashMap<Vec<u8>, Txn>,
    /// Temporary addresses of dynamic targets, removed with their links.
    temp_addresses: Vec<String>,
    closed: bool,
    /// Set when the broker opened the connection.
    outgoing: Option<Outgoing>,
    /// A broker connection's session.
    main_channel: Option<u16>,
    /// Why a connection the broker opened ended, when not by the peer.
    ended: Option<Ended>,
}

/// Serves one connection until it closes.
pub async fn serve(
    broker: Arc<Broker>,
    c: Accepted,
    settings: Settings,
    config: AmqpConfig,
    registry: Arc<ContainerIds>,
) {
    let client = Arc::new(
        ClientConnection::new(
            Transport::socket("AMQP", "ActiveMQProtonRemotingConnection", c.peer, c.local),
            Arc::new(c.info),
            c.acceptor.prefixes.clone(),
        )
        .with_acceptor(&c.acceptor.name),
    );
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    drive(
        broker, client, c.stream, settings, config, registry, None, events_tx, events_rx,
    )
    .await;
}

/// Opens a connection to another broker over `stream`, as the client
/// (`createOutgoingConnectionEntry`): the handle the broker connection
/// reaches it by, and its task, which ends with the connection.
#[allow(clippy::too_many_arguments)] // the parts of a connection entry
pub fn connect_out<S>(
    broker: Arc<Broker>,
    stream: S,
    peer: Option<std::net::SocketAddr>,
    local: Option<std::net::SocketAddr>,
    settings: Settings,
    config: AmqpConfig,
    registry: Arc<ContainerIds>,
    outgoing: Outgoing,
) -> (
    ConnectionHandle,
    impl std::future::Future<Output = Ended> + Send + 'static,
)
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let info = saranga_security::ConnectionInfo {
        remote_address: peer.map(|p| p.to_string()).unwrap_or_default(),
        certificates: None,
        security_domain: None,
        connection_id: None,
    };
    let client = Arc::new(ClientConnection::new(
        Transport::socket("AMQP", "ActiveMQProtonRemotingConnection", peer, local),
        Arc::new(info),
        Vec::new(),
    ));
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let handle = ConnectionHandle(events_tx.clone());
    let task = drive(
        broker,
        client,
        stream,
        settings,
        config,
        registry,
        Some(outgoing),
        events_tx,
        events_rx,
    );
    (handle, task)
}

/// Runs a connection, either way, until it ends.
#[allow(clippy::too_many_arguments)]
async fn drive<S>(
    broker: Arc<Broker>,
    client: Arc<ClientConnection>,
    stream: S,
    settings: Settings,
    config: AmqpConfig,
    registry: Arc<ContainerIds>,
    outgoing: Option<Outgoing>,
    events_tx: mpsc::UnboundedSender<Event>,
    mut events_rx: mpsc::UnboundedReceiver<Event>,
) -> Ended
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let _registered = broker.registry().add(&client);
    let (rd, wr) = tokio::io::split(stream);
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let writer = tokio::spawn(write_loop(wr, out_rx));
    let ttl = match settings.idle_timeout {
        Some(t) => t,
        None if broker.connection_ttl_override() != -1 => broker.connection_ttl_override(),
        None => DEFAULT_CONNECTION_TTL,
    };
    let mut conn = Conn {
        broker,
        client,
        settings,
        config,
        registry,
        out: Outbound(out_tx),
        events: events_tx,
        user: None,
        password: None,
        auth: Authenticated::default(),
        remote_container: None,
        registered: false,
        opened: false,
        remote_max_frame: u32::MAX,
        remote_idle: None,
        ttl: (ttl > 0).then(|| Duration::from_millis(ttl as u64)),
        last_read: Instant::now(),
        last_write: Instant::now(),
        sessions: HashMap::new(),
        channels: HashMap::new(),
        txns: HashMap::new(),
        temp_addresses: Vec::new(),
        closed: false,
        outgoing,
        main_channel: None,
        ended: None,
    };
    let mut reader = Reader {
        rd,
        buf: Vec::new(),
    };
    let shaken = if conn.outgoing.is_some() {
        conn.client_handshake(&mut reader).await
    } else {
        conn.handshake(&mut reader).await
    };
    if shaken {
        if conn.outgoing.is_some() {
            conn.open_out().await;
        }
        conn.run(&mut reader, &mut events_rx).await;
    }
    conn.shutdown().await;
    conn.out.close();
    let _ = writer.await;
    conn.ended.take().unwrap_or(Ended::Closed)
}

impl Conn {
    // -------------------------------------------------------- SASL

    fn mechanisms(&self) -> Vec<String> {
        if self.settings.sasl_mechanisms.is_empty() {
            DEFAULT_MECHANISMS.iter().map(|m| (*m).to_owned()).collect()
        } else {
            self.settings.sasl_mechanisms.clone()
        }
    }

    /// Whether a client may skip authentication (`isSupportsAnonymous`).
    async fn accepts_anonymous(&mut self) -> bool {
        match self.broker.security().cloned() {
            None => true,
            Some(store) => {
                let info = self.client.security_info();
                tokio::task::spawn_blocking(move || store.authenticate(None, None, &info).is_ok())
                    .await
                    .unwrap_or(false)
            }
        }
    }

    /// The protocol headers and SASL (`ProtonHandler`). False when the
    /// connection ends there.
    async fn handshake<R: AsyncRead + Unpin>(&mut self, r: &mut Reader<R>) -> bool {
        let Ok(header) = r.header().await else {
            return false;
        };
        if header == AMQP_HEADER {
            if !self.accepts_anonymous().await {
                // SASL is required.
                self.out.now(SASL_HEADER.to_vec());
                return false;
            }
            self.out.now(AMQP_HEADER.to_vec());
            return true;
        }
        if header != SASL_HEADER {
            self.out.now(AMQP_HEADER.to_vec());
            return false;
        }
        let offered = self.mechanisms();
        let mut out = SASL_HEADER.to_vec();
        frame::encode_sasl(
            &mut out,
            &SaslFrame::Mechanisms(SaslMechanisms {
                sasl_server_mechanisms: offered.iter().map(|m| Symbol::from(m.as_str())).collect(),
            }),
        );
        self.out.now(out);
        let init = match r
            .frame(self.settings.initial_remote_max_frame_size.max(512))
            .await
        {
            Ok(bytes) => match frame::decode(&bytes) {
                Ok(frame::Frame {
                    body: FrameBody::Sasl(SaslFrame::Init(init)),
                    ..
                }) => init,
                _ => return false,
            },
            Err(_) => return false,
        };
        let code = self.sasl_init(&init, &offered).await;
        let mut out = Vec::new();
        frame::encode_sasl(
            &mut out,
            &SaslFrame::Outcome(SaslOutcome {
                code,
                additional_data: None,
            }),
        );
        self.out.now(out);
        if code != sasl::code::OK {
            // Artemis leaves the socket for the client to close; it is
            // closed here once the outcome is written.
            return false;
        }
        match r.header().await {
            Ok(h) if h == AMQP_HEADER => {
                self.out.now(AMQP_HEADER.to_vec());
                true
            }
            _ => {
                self.out.now(AMQP_HEADER.to_vec());
                false
            }
        }
    }

    /// The client's side of the protocol headers and SASL, for a
    /// connection the broker opens. False, with why, when it fails.
    async fn client_handshake<R: AsyncRead + Unpin>(&mut self, r: &mut Reader<R>) -> bool {
        let refused = |conn: &mut Self, why: String| {
            conn.ended = Some(Ended::Refused(why));
            false
        };
        self.out.now(SASL_HEADER.to_vec());
        match r.header().await {
            Ok(h) if h == SASL_HEADER => {}
            Ok(_) => return refused(self, "the peer does not offer SASL".into()),
            Err(_) => return refused(self, "the connection closed".into()),
        }
        let max = self.settings.initial_remote_max_frame_size.max(512);
        let offered = match r.frame(max).await.map(|b| frame::decode(&b)) {
            Ok(Ok(frame::Frame {
                body: FrameBody::Sasl(SaslFrame::Mechanisms(m)),
                ..
            })) => m
                .sasl_server_mechanisms
                .into_iter()
                .map(|s| s.0)
                .collect::<Vec<_>>(),
            _ => return refused(self, "no SASL mechanisms came".into()),
        };
        let Some((mechanism, response)) = self.choose_mechanism(&offered) else {
            return refused(
                self,
                format!("no SASL mechanism to use among {}", offered.join(", ")),
            );
        };
        let mut out = Vec::new();
        frame::encode_sasl(
            &mut out,
            &SaslFrame::Init(SaslInit {
                mechanism: Symbol::from(mechanism),
                initial_response: Some(response),
                hostname: None,
            }),
        );
        self.out.now(out);
        let outcome = loop {
            match r.frame(max).await.map(|b| frame::decode(&b)) {
                Ok(Ok(frame::Frame {
                    body: FrameBody::Sasl(SaslFrame::Outcome(o)),
                    ..
                })) => break o,
                // The mechanisms chosen answer a challenge with nothing.
                Ok(Ok(frame::Frame {
                    body: FrameBody::Sasl(SaslFrame::Challenge(_)),
                    ..
                })) => {
                    let mut out = Vec::new();
                    frame::encode_sasl(
                        &mut out,
                        &SaslFrame::Response(sasl::SaslResponse {
                            response: Vec::new(),
                        }),
                    );
                    self.out.now(out);
                }
                _ => return refused(self, "no SASL outcome came".into()),
            }
        };
        if outcome.code != sasl::code::OK {
            return refused(
                self,
                format!("SASL {mechanism} failed with code {}", outcome.code),
            );
        }
        self.out.now(AMQP_HEADER.to_vec());
        match r.header().await {
            Ok(h) if h == AMQP_HEADER => true,
            _ => refused(self, "the peer did not answer the AMQP header".into()),
        }
    }

    /// `SaslFactory.chooseMechanism`: among those offered (and allowed by
    /// the URI's `saslMechanisms`), PLAIN with a user and password, then
    /// XOAUTH2, then ANONYMOUS. EXTERNAL needs a client certificate and
    /// SCRAM is not supported, so neither is chosen.
    fn choose_mechanism(&self, offered: &[String]) -> Option<(&'static str, Vec<u8>)> {
        let o = self.outgoing.as_ref()?;
        let allowed = |m: &str| {
            offered.iter().any(|x| x == m)
                && o.sasl_mechanisms
                    .as_ref()
                    .filter(|e| !e.is_empty())
                    .is_none_or(|e| e.iter().any(|x| x == m))
        };
        let creds = match (&o.user, &o.password) {
            (Some(u), Some(p)) if !u.is_empty() && !p.is_empty() => Some((u, p)),
            _ => None,
        };
        if let Some((u, p)) = creds {
            if allowed("PLAIN") {
                let mut r = vec![0u8];
                r.extend_from_slice(u.as_bytes());
                r.push(0);
                r.extend_from_slice(p.as_bytes());
                return Some(("PLAIN", r));
            }
            if allowed("XOAUTH2") {
                let r = format!("user={u}\u{1}auth=Bearer {p}\u{1}\u{1}");
                return Some(("XOAUTH2", r.into_bytes()));
            }
        }
        allowed("ANONYMOUS").then(|| ("ANONYMOUS", Vec::new()))
    }

    /// A broker connection's Open and its session's Begin
    /// (`AMQPConnectionContext.open`, `session.open`).
    async fn open_out(&mut self) {
        let Some(o) = &self.outgoing else { return };
        let mut open = self.local_open();
        open.offered_capabilities = None;
        open.desired_capabilities = o.desired_capabilities.clone();
        let mut properties = open.properties.take().unwrap_or_default();
        for (k, v) in &o.properties.0 {
            properties.insert(k.clone(), v.clone());
        }
        open.properties = Some(properties);
        self.send(0, Performative::Open(open));
        let channel = self.begin_local().await;
        self.main_channel = Some(channel);
    }

    /// Begins a session from this end, on the lowest free channel.
    async fn begin_local(&mut self) -> u16 {
        let channel = (0..=CHANNEL_MAX)
            .find(|c| !self.sessions.contains_key(c))
            .unwrap_or(CHANNEL_MAX);
        let mut session = self.new_session(0, WINDOW);
        session.server.set_started(true).await;
        self.sessions.insert(channel, session);
        self.send(
            channel,
            Performative::Begin(Begin {
                remote_channel: None,
                next_outgoing_id: 1,
                incoming_window: WINDOW,
                outgoing_window: WINDOW,
                handle_max: HANDLE_MAX,
                ..Default::default()
            }),
        );
        channel
    }

    /// One `sasl-init`: the outcome code.
    async fn sasl_init(&mut self, init: &SaslInit, offered: &[String]) -> u8 {
        let mechanism = init.mechanism.as_str();
        if !offered.iter().any(|m| m == mechanism) {
            return sasl::code::SYS;
        }
        match mechanism {
            "ANONYMOUS" => sasl::code::OK,
            "PLAIN" => {
                let Some(data) = &init.initial_response else {
                    return sasl::code::AUTH;
                };
                // String.split("\0"): trailing empty parts go.
                let text = String::from_utf8_lossy(data).into_owned();
                let mut parts: Vec<&str> = text.split('\0').collect();
                while parts.last() == Some(&"") {
                    parts.pop();
                }
                let (user, pass) = match parts.as_slice() {
                    [u, p] => (Some((*u).to_owned()), Some((*p).to_owned())),
                    [_, u, p] => (Some((*u).to_owned()), Some((*p).to_owned())),
                    _ => (None, None),
                };
                self.user = user.clone();
                self.password = pass.clone();
                match self.broker.security().cloned() {
                    None => sasl::code::OK,
                    Some(store) if !store.is_enabled() => sasl::code::OK,
                    Some(store) => {
                        let info = self.client.security_info();
                        let ok = tokio::task::spawn_blocking(move || {
                            store
                                .authenticate(user.as_deref(), pass.as_deref(), &info)
                                .is_ok()
                        })
                        .await
                        .unwrap_or(false);
                        if ok { sasl::code::OK } else { sasl::code::AUTH }
                    }
                }
            }
            "EXTERNAL" => {
                let has_cert = self.client.security_info().certificates.is_some();
                let empty = init.initial_response.as_ref().is_none_or(|r| r.is_empty());
                match (has_cert, empty) {
                    (false, _) => sasl::code::SYS,
                    (true, true) => sasl::code::OK,
                    (true, false) => sasl::code::AUTH,
                }
            }
            _ => sasl::code::SYS,
        }
    }

    // -------------------------------------------------------- the loop

    async fn run<R: AsyncRead + Unpin>(
        &mut self,
        r: &mut Reader<R>,
        events: &mut mpsc::UnboundedReceiver<Event>,
    ) {
        let max = self.settings.max_frame_size.max(frame::MIN_MAX_FRAME_SIZE);
        let client = self.client.clone();
        while !self.closed {
            let tick = self.next_tick();
            tokio::select! {
                // Management failed the connection: it goes as if the
                // client had.
                () = client.failed() => break,
                read = r.frame(max) => match read {
                    Ok(bytes) => {
                        self.last_read = Instant::now();
                        let result = self.handle_frame(&bytes).await;
                        if let Err(Fatal(e)) = result {
                            self.close_with(e);
                        }
                    }
                    Err(ReadError::Closed) => break,
                    Err(ReadError::TooLarge(size)) => self.close_with(Error::new(
                        condition::FRAMING_ERROR,
                        Some(format!("frame size {size} larger than the maximum {max}")),
                    )),
                    Err(ReadError::Malformed(m)) => {
                        self.close_with(Error::new(condition::FRAMING_ERROR, Some(m)))
                    }
                },
                Some(ev) = events.recv() => {
                    let result = self.handle_event(ev).await;
                    if let Err(Fatal(e)) = result {
                        self.close_with(e);
                    }
                }
                _ = tokio::time::sleep(tick) => self.tick(),
            }
        }
    }

    fn next_tick(&self) -> Duration {
        let now = Instant::now();
        let mut next = Duration::from_secs(1);
        if let Some(t) = self.ttl {
            next = next.min(t.saturating_sub(now - self.last_read));
        }
        if let Some(remote) = self.remote_idle {
            next = next.min((remote / 2).saturating_sub(now - self.last_write));
        }
        next.max(Duration::from_millis(10))
    }

    /// `Transport.tick`: the idle timeout, and heartbeats.
    fn tick(&mut self) {
        let now = Instant::now();
        if let Some(t) = self.ttl
            && now - self.last_read >= t
        {
            self.close_with(Error::new(
                condition::RESOURCE_LIMIT_EXCEEDED,
                Some("local-idle-timeout expired".into()),
            ));
            return;
        }
        if let Some(remote) = self.remote_idle
            && now - self.last_write >= remote / 2
        {
            let mut out = Vec::new();
            frame::encode_empty(&mut out);
            self.write(out);
        }
    }

    fn write(&mut self, bytes: Vec<u8>) {
        self.last_write = Instant::now();
        self.out.now(bytes);
    }

    fn send(&mut self, channel: u16, p: Performative) {
        self.last_write = Instant::now();
        self.out.frame(channel, p, &[], Completion::default());
    }

    /// Closes the connection with an error: Open first if it was never
    /// sent, then Close.
    fn close_with(&mut self, error: Error) {
        if self.closed {
            return;
        }
        if !self.opened {
            self.send(0, Performative::Open(self.local_open()));
        }
        self.send(0, Performative::Close(Close { error: Some(error) }));
        self.closed = true;
    }

    fn local_open(&self) -> Open {
        let mut properties = Fields::default();
        properties.insert("product", Value::string("saranga"));
        properties.insert("version", Value::string(env!("CARGO_PKG_VERSION")));
        Open {
            container_id: self.broker.node_id().to_owned(),
            hostname: None,
            max_frame_size: self.settings.max_frame_size,
            channel_max: CHANNEL_MAX,
            idle_time_out: self.ttl.map(|t| (t.as_millis() / 2) as u32),
            offered_capabilities: Some(
                [
                    capability::SOLE_CONNECTION,
                    capability::DELAYED_DELIVERY,
                    capability::SHARED_SUBS,
                    capability::ANONYMOUS_RELAY,
                ]
                .iter()
                .map(|c| Symbol::from(*c))
                .collect(),
            ),
            properties: Some(properties),
            ..Default::default()
        }
    }

    async fn handle_frame(&mut self, bytes: &[u8]) -> Handled {
        let f =
            frame::decode(bytes).map_err(|e| Fatal::new(condition::DECODE_ERROR, e.to_string()))?;
        let (perf, payload) = match f.body {
            FrameBody::Empty => return Ok(()),
            FrameBody::Sasl(_) => {
                return Err(Fatal::new(
                    condition::FRAMING_ERROR,
                    "SASL frame after SASL",
                ));
            }
            FrameBody::Amqp(p, payload) => (p, payload),
        };
        if !self.opened && !matches!(perf, Performative::Open(_)) {
            return Err(Fatal::new(
                condition::ILLEGAL_STATE,
                format!("{} before open", perf.name()),
            ));
        }
        // The peer's channel, and this end's for it.
        let remote = f.channel;
        let local = self.channels.get(&remote).copied();
        let unknown = |what: &str| {
            Fatal::new(
                condition::ILLEGAL_STATE,
                format!("{what} on an unknown channel {remote}"),
            )
        };
        match perf {
            Performative::Open(o) => self.on_open(o).await,
            Performative::Begin(b) => self.on_begin(remote, b).await,
            Performative::Attach(a) => match local {
                Some(ch) => self.on_attach(ch, a).await,
                None => Err(unknown("attach")),
            },
            Performative::Flow(mut fl) => match local {
                Some(ch) => {
                    if let (Some(h), Some(s)) = (fl.handle, self.sessions.get(&ch)) {
                        fl.handle = Some(s.local_handle(h));
                    }
                    self.on_flow(ch, fl)
                }
                None => Ok(()),
            },
            Performative::Transfer(mut t) => match local {
                Some(ch) => {
                    if let Some(s) = self.sessions.get(&ch) {
                        t.handle = s.local_handle(t.handle);
                    }
                    self.on_transfer(ch, t, payload).await
                }
                None => Err(unknown("transfer")),
            },
            Performative::Disposition(d) => match local {
                Some(ch) => self.on_disposition(ch, d).await,
                None => Ok(()),
            },
            Performative::Detach(d) => match local {
                Some(ch) => self.on_detach(ch, d).await,
                None => Ok(()),
            },
            Performative::End(e) => match local {
                Some(ch) => {
                    self.channels.remove(&remote);
                    self.on_end(ch, e).await
                }
                None => Ok(()),
            },
            Performative::Close(c) => {
                if self.outgoing.is_some()
                    && let Some(e) = &c.error
                {
                    tracing::warn!(
                        "the peer closed the broker connection: {} {}",
                        e.condition.as_str(),
                        e.description.as_deref().unwrap_or_default()
                    );
                }
                self.send(0, Performative::Close(Close::default()));
                self.closed = true;
                Ok(())
            }
        }
    }

    // -------------------------------------------------------- connection

    /// The client's Open: the user is validated, the container ID checked.
    async fn on_open(&mut self, open: Open) -> Handled {
        if self.opened {
            return Err(Fatal::new(condition::ILLEGAL_STATE, "a second open"));
        }
        self.remote_container = Some(open.container_id.clone());
        self.remote_max_frame = open.max_frame_size.max(frame::MIN_MAX_FRAME_SIZE);
        self.remote_idle = open
            .idle_time_out
            .filter(|&t| t > 0)
            .map(|t| Duration::from_millis(u64::from(t)));
        if let Some(o) = &mut self.outgoing {
            // This end opened already, and its user is the broker.
            self.opened = true;
            self.client.set_client_id(Some(open.container_id));
            if let Some(f) = o.on_open.take() {
                f();
            }
            return Ok(());
        }
        // validateUser
        if let Some(store) = self.broker.security().cloned() {
            let info = self.client.security_info();
            let (user, pass) = (self.user.clone(), self.password.clone());
            let result = tokio::task::spawn_blocking(move || {
                store.authenticate(user.as_deref(), pass.as_deref(), &info)
            })
            .await
            .unwrap_or_else(|e| Err(saranga_security::SecurityError(e.to_string())));
            match result {
                Ok(a) => self.auth = a,
                Err(e) => {
                    let mut props = Fields::default();
                    props.insert(CONNECTION_OPEN_FAILED, Value::Boolean(true));
                    return Err(
                        self.refuse(Error::new(condition::UNAUTHORIZED_ACCESS, Some(e.0)), props)
                    );
                }
            }
        }
        let sole = link::has(
            open.desired_capabilities.as_ref(),
            capability::SOLE_CONNECTION,
        );
        if !self.registry.add(&open.container_id, sole) {
            let mut props = Fields::default();
            props.insert(CONNECTION_OPEN_FAILED, Value::string("true"));
            let mut info = Fields::default();
            info.insert("invalid-field", Value::symbol("container-id"));
            return Err(self.refuse(
                Error {
                    condition: Symbol::from(condition::INVALID_FIELD),
                    description: None,
                    info: Some(info),
                },
                props,
            ));
        }
        self.registered = true;
        self.client.set_client_id(Some(open.container_id));
        self.opened = true;
        let o = self.local_open();
        self.send(0, Performative::Open(o));
        Ok(())
    }

    /// An Open that refuses the connection, then the Close carrying why.
    fn refuse(&mut self, error: Error, properties: Fields) -> Fatal {
        let open = Open {
            container_id: self.broker.node_id().to_owned(),
            max_frame_size: self.settings.max_frame_size,
            channel_max: CHANNEL_MAX,
            properties: Some(properties),
            ..Default::default()
        };
        self.send(0, Performative::Open(open));
        self.opened = true;
        Fatal(error)
    }

    /// A session's state, its server session not started yet. A broker
    /// connection's sessions are internal: nothing they do is checked
    /// (`createInternalSession`).
    fn new_session(&self, next_incoming_id: u32, remote_incoming_window: u32) -> Session {
        let mut server = ServerSession::new(
            self.broker.clone(),
            self.client.clone(),
            SessionOptions {
                name: uuid::Uuid::new_v4().to_string(),
                username: self.user.clone(),
                password: self.password.clone(),
                xa: false,
                auto_commit_sends: true,
                auto_commit_acks: true,
                pre_acknowledge: false,
                default_address: None,
            },
            self.auth.clone(),
        );
        if self.outgoing.is_some() {
            server.set_internal();
        }
        Session {
            server,
            next_outgoing_id: 1,
            next_incoming_id,
            remote_incoming_window,
            next_delivery_id: 0,
            next_consumer: 0,
            links: HashMap::new(),
            out: BTreeMap::new(),
            tags: Tags::default(),
            blocked: std::collections::VecDeque::new(),
            handles: HashMap::new(),
            detaching: std::collections::HashSet::new(),
            pending: HashMap::new(),
        }
    }

    /// The peer's Begin: the answer to one this end sent, or a session it
    /// begins, given the lowest free channel here.
    async fn on_begin(&mut self, remote: u16, begin: Begin) -> Handled {
        if self.channels.contains_key(&remote) {
            return Err(Fatal::new(
                condition::ILLEGAL_STATE,
                format!("channel {remote} in use"),
            ));
        }
        if let Some(local) = begin.remote_channel {
            let Some(s) = self.sessions.get_mut(&local) else {
                return Err(Fatal::new(
                    condition::ILLEGAL_STATE,
                    format!("a begin answering channel {local}, which has no session"),
                ));
            };
            s.next_incoming_id = begin.next_outgoing_id;
            s.remote_incoming_window = begin.incoming_window;
            self.channels.insert(remote, local);
            return Ok(());
        }
        let channel = (0..=CHANNEL_MAX)
            .find(|c| !self.sessions.contains_key(c))
            .unwrap_or(CHANNEL_MAX);
        let mut session = self.new_session(begin.next_outgoing_id, begin.incoming_window);
        // AMQPSessionCallback.init starts the session.
        session.server.set_started(true).await;
        self.sessions.insert(channel, session);
        self.channels.insert(remote, channel);
        self.send(
            channel,
            Performative::Begin(Begin {
                remote_channel: Some(remote),
                next_outgoing_id: 1,
                incoming_window: WINDOW,
                outgoing_window: WINDOW,
                handle_max: HANDLE_MAX,
                ..Default::default()
            }),
        );
        Ok(())
    }

    async fn on_end(&mut self, channel: u16, _end: End) -> Handled {
        if let Some(mut s) = self.sessions.remove(&channel) {
            let handles: Vec<u32> = s.links.keys().copied().collect();
            for h in handles {
                if let Some(l) = s.links.remove(&h) {
                    self.close_link(&mut s, l, false).await;
                }
            }
            s.server.close(false).await;
        }
        self.send(channel, Performative::End(End::default()));
        Ok(())
    }

    /// The connection is gone: transactions roll back, sessions close,
    /// temporary destinations go.
    async fn shutdown(&mut self) {
        let txns: Vec<Txn> = self.txns.drain().map(|(_, t)| t).collect();
        for t in txns {
            if let Some(s) = self.sessions.get_mut(&t.channel) {
                let _ = s.server.rollback_tx(&t.tx);
            }
        }
        let channels: Vec<u16> = self.sessions.keys().copied().collect();
        for ch in channels {
            if let Some(mut s) = self.sessions.remove(&ch) {
                let handles: Vec<u32> = s.links.keys().copied().collect();
                for h in handles {
                    if let Some(l) = s.links.remove(&h) {
                        self.close_link(&mut s, l, false).await;
                    }
                }
                s.server.close(true).await;
            }
        }
        self.client.remove_temp_queues(&self.broker);
        for a in std::mem::take(&mut self.temp_addresses) {
            self.broker.post_office().remove_address(&a);
        }
        if self.registered
            && let Some(c) = &self.remote_container
        {
            self.registry.remove(c);
        }
    }

    // -------------------------------------------------------- links

    async fn on_attach(&mut self, channel: u16, mut attach: Attach) -> Handled {
        let Some(s) = self.sessions.get_mut(&channel) else {
            return Err(Fatal::new(
                condition::ILLEGAL_STATE,
                "attach on an unknown channel",
            ));
        };
        if s.handles.contains_key(&attach.handle) {
            return Err(Fatal::new(
                condition::HANDLE_IN_USE,
                format!("handle {} is in use", attach.handle),
            ));
        }
        // The answer to a link this end attached: the same name, the other
        // role.
        let answering = s
            .pending
            .iter()
            .find(|(_, p)| p.attach.name == attach.name && p.attach.role != attach.role)
            .map(|(h, _)| *h);
        if let Some(local) = answering {
            let link = s.pending.remove(&local).expect("found");
            s.handles.insert(attach.handle, local);
            return self.local_link_attached(channel, local, link, attach).await;
        }
        let local = s.allocate_handle();
        s.handles.insert(attach.handle, local);
        attach.handle = local;
        match attach.role {
            // The client sends: the broker receives.
            Role::Sender => match &attach.target {
                Some(TargetKind::Coordinator(_)) => self.attach_coordinator(channel, attach),
                _ => self.attach_receiver(channel, attach).await,
            },
            Role::Receiver => self.attach_sender(channel, attach).await,
        }
    }

    /// Refuses a link: an Attach without the terminus, then a closing
    /// Detach with the error.
    fn refuse_link(&mut self, channel: u16, mut reply: Attach, e: LinkError) {
        match reply.role {
            Role::Receiver => reply.target = None,
            Role::Sender => reply.source = None,
        }
        let handle = reply.handle;
        self.send(channel, Performative::Attach(reply));
        self.send(
            channel,
            Performative::Detach(Detach {
                handle,
                closed: true,
                error: Some(Error::new(e.condition, Some(e.description))),
            }),
        );
        if let Some(s) = self.sessions.get_mut(&channel) {
            s.detaching.insert(handle);
        }
    }

    fn link_flow(
        &self,
        s: &Session,
        handle: u32,
        delivery_count: u32,
        credit: u32,
        drain: bool,
    ) -> Flow {
        Flow {
            next_incoming_id: Some(s.next_incoming_id),
            incoming_window: WINDOW,
            next_outgoing_id: s.next_outgoing_id,
            outgoing_window: WINDOW,
            handle: Some(handle),
            delivery_count: Some(delivery_count),
            link_credit: Some(credit),
            available: None,
            drain,
            echo: false,
            properties: None,
        }
    }

    fn attach_coordinator(&mut self, channel: u16, attach: Attach) -> Handled {
        let credits = self.settings.credits.max(0) as u32;
        let reply = Attach {
            name: attach.name.clone(),
            handle: attach.handle,
            role: Role::Receiver,
            snd_settle_mode: attach.snd_settle_mode,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: attach.source.clone(),
            target: Some(TargetKind::Coordinator(Coordinator {
                capabilities: Some(vec![
                    Symbol::from(txn_cap::LOCAL),
                    Symbol::from(txn_cap::MULTI_TXNS_PER_SSN),
                    Symbol::from(txn_cap::MULTI_SSNS_PER_TXN),
                ]),
            })),
            ..Default::default()
        };
        self.send(channel, Performative::Attach(reply));
        let s = self.sessions.get_mut(&channel).expect("checked");
        s.links.insert(
            attach.handle,
            Link::Coordinator(CoordinatorLink {
                credit: credits,
                delivery_count: attach.initial_delivery_count.unwrap_or(0),
                incoming: None,
            }),
        );
        let s = &self.sessions[&channel];
        let flow = self.link_flow(
            s,
            attach.handle,
            attach.initial_delivery_count.unwrap_or(0),
            credits,
            false,
        );
        self.send(channel, Performative::Flow(flow));
        Ok(())
    }

    async fn attach_receiver(&mut self, channel: u16, attach: Attach) -> Handled {
        let mut reply = Attach {
            name: attach.name.clone(),
            handle: attach.handle,
            role: Role::Receiver,
            snd_settle_mode: attach.snd_settle_mode,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: attach.source.clone(),
            target: attach.target.clone(),
            ..Default::default()
        };
        let Some(TargetKind::Target(mut target)) = attach.target.clone() else {
            // Artemis fails the whole connection here (a null target).
            self.refuse_link(
                channel,
                reply,
                LinkError::new(condition::INVALID_FIELD, "no target"),
            );
            return Ok(());
        };
        let s = self.sessions.get_mut(&channel).expect("checked");
        let resolved = match link::resolve_target(&mut s.server, &mut target) {
            Ok(r) => r,
            Err(e) => {
                self.refuse_link(channel, reply, e);
                return Ok(());
            }
        };
        if resolved.dynamic
            && resolved.explicit.or(resolved.implicit) != Some(RoutingType::Anycast)
            && let Some(a) = &resolved.address
        {
            self.temp_addresses.push(a.clone());
        }
        if link::has(
            attach.desired_capabilities.as_ref(),
            capability::DELAYED_DELIVERY,
        ) {
            reply.offered_capabilities = Some(vec![Symbol::from(capability::DELAYED_DELIVERY)]);
        }
        let expiry = target.expiry_policy;
        reply.target = Some(TargetKind::Target(target));
        self.send(channel, Performative::Attach(reply));
        let credits = self.settings.credits.max(0) as u32;
        let s = self.sessions.get_mut(&channel).expect("checked");
        s.server
            .add_producer(&attach.name, "AMQP", resolved.address.as_deref());
        let delivery_count = attach.initial_delivery_count.unwrap_or(0);
        s.links.insert(
            attach.handle,
            Link::Receiver(ReceiverLink {
                name: attach.name.clone(),
                target: resolved,
                source: attach.source,
                credit: credits,
                delivery_count,
                incoming: None,
                dynamic_expiry: expiry,
            }),
        );
        let s = &self.sessions[&channel];
        let flow = self.link_flow(s, attach.handle, delivery_count, credits, false);
        self.send(channel, Performative::Flow(flow));
        Ok(())
    }

    async fn attach_sender(&mut self, channel: u16, attach: Attach) -> Handled {
        let mut source = attach.source.clone();
        let reply = Attach {
            name: attach.name.clone(),
            handle: attach.handle,
            role: Role::Sender,
            snd_settle_mode: attach.snd_settle_mode,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: None,
            target: attach.target.clone(),
            initial_delivery_count: Some(0),
            ..Default::default()
        };
        let container = self.remote_container.clone();
        let core_naming = self.config.use_core_subscription_naming;
        let s = self.sessions.get_mut(&channel).expect("checked");
        let resolved = match link::resolve_source(
            &mut s.server,
            &mut source,
            &attach.name,
            container.as_deref(),
            attach.desired_capabilities.as_ref(),
            attach.properties.as_ref(),
            core_naming,
        ) {
            Ok(r) => r,
            Err(e) => {
                self.refuse_link(channel, reply, e);
                return Ok(());
            }
        };
        let consumer = s.next_consumer;
        s.next_consumer += 1;
        let credit = Arc::new(Mutex::new(Credit::default()));
        let sink = Arc::new(Sink {
            events: self.events.clone(),
            channel,
            handle: attach.handle,
            credit: credit.clone(),
        });
        // SelectorTranslator: JMS header names become the broker's.
        let filter = if resolved.multicast {
            None
        } else {
            resolved
                .selector
                .as_deref()
                .map(saranga_filter::translate::convert_to_activemq_filter_string)
        };
        let created = s
            .server
            .create_consumer(
                consumer,
                &resolved.queue,
                filter,
                resolved.priority,
                resolved.browse,
                move |_: &Queue| sink as Arc<dyn DeliverySink>,
            )
            .await;
        if let Err(e) = created {
            let le = if e.kind == ErrorType::SecurityException {
                LinkError::new(
                    condition::UNAUTHORIZED_ACCESS,
                    format!(
                        "AMQ119015: not authorized to create consumer, {}",
                        e.message
                    ),
                )
            } else {
                LinkError::new(
                    condition::INTERNAL_ERROR,
                    format!("AMQ119005: error creating consumer, {}", e.message),
                )
            };
            // A temporary subscription queue made for this link goes.
            if let Some(t) = &resolved.temp_queue {
                let _ = s.server.delete_queue_with(t, false);
            }
            self.refuse_link(channel, reply, le);
            return Ok(());
        }
        s.server.consumer_credits(consumer, -1);
        let presettled = attach.snd_settle_mode == SenderSettleMode::Settled;
        s.links.insert(
            attach.handle,
            Link::Sender(SenderLink {
                name: attach.name.clone(),
                consumer,
                resolved,
                source: source.clone(),
                credit,
                link_credit: 0,
                delivery_count: 0,
                presettled,
                draining: false,
            }),
        );
        let mut reply = reply;
        reply.source = source;
        self.send(channel, Performative::Attach(reply));
        Ok(())
    }

    async fn on_detach(&mut self, channel: u16, detach: Detach) -> Handled {
        let Some(s) = self.sessions.get_mut(&channel) else {
            return Ok(());
        };
        let Some(handle) = s.handles.remove(&detach.handle) else {
            return Ok(());
        };
        // The answer to a detach this end sent.
        if s.detaching.remove(&handle) {
            return Ok(());
        }
        // A link this end attached that the peer refused.
        if let Some(p) = s.pending.remove(&handle) {
            self.send(
                channel,
                Performative::Detach(Detach {
                    handle,
                    closed: detach.closed,
                    error: None,
                }),
            );
            self.link_failed(refused_message(&p));
            return Ok(());
        }
        let link = s.links.remove(&handle);
        let mut s = self.sessions.remove(&channel).expect("checked");
        let mut closed = detach.closed;
        let was_open = link.is_some();
        if let Some(l) = link {
            let link_detach_expiry = match &l {
                Link::Sender(sl) => sl.source.as_ref().map(|x| x.expiry_policy),
                Link::Receiver(rl) => rl.source.as_ref().map(|x| x.expiry_policy),
                Link::Coordinator(_) => None,
            } == Some(TerminusExpiryPolicy::LinkDetach);
            closed |= link_detach_expiry;
            self.close_link(&mut s, l, closed).await;
        }
        self.sessions.insert(channel, s);
        self.send(
            channel,
            Performative::Detach(Detach {
                handle,
                closed: detach.closed,
                error: None,
            }),
        );
        // AMQPBrokerConnection.linkClosed: a broker connection whose link
        // the peer closes is made again.
        if self.outgoing.is_some() && closed && was_open {
            self.link_failed(
                "AMQ119021: The broker connection had a remote link closed unexpectedly".into(),
            );
        }
        Ok(())
    }

    /// Attaches a broker connection's link on its session, with the lowest
    /// free handle.
    fn open_local(&mut self, mut link: LocalLink) {
        let Some(channel) = self.main_channel else {
            return;
        };
        let Some(s) = self.sessions.get_mut(&channel) else {
            return;
        };
        let handle = s.allocate_handle();
        link.attach.handle = handle;
        let attach = link.attach.clone();
        s.pending.insert(handle, link);
        self.send(channel, Performative::Attach(attach));
    }

    /// The peer's attach answering a link this end attached (the
    /// `AMQP_LINK_INITIALIZER_KEY` runnable of `connectSender` and
    /// `connectReceiver`).
    async fn local_link_attached(
        &mut self,
        channel: u16,
        handle: u32,
        link: LocalLink,
        remote: Attach,
    ) -> Handled {
        let refused = match &link.kind {
            LocalLinkKind::Sender { .. } => remote.target.is_none(),
            LocalLinkKind::Receiver { .. } => remote.source.is_none(),
        };
        if refused {
            self.link_failed(refused_message(&link));
            return Ok(());
        }
        let name = link.attach.name.clone();
        match link.kind {
            LocalLinkKind::Sender { queue } => {
                let consumer = {
                    let s = self.sessions.get_mut(&channel).expect("checked");
                    let c = s.next_consumer;
                    s.next_consumer += 1;
                    c
                };
                let credit = Arc::new(Mutex::new(Credit::default()));
                let sink = Arc::new(Sink {
                    events: self.events.clone(),
                    channel,
                    handle,
                    credit: credit.clone(),
                });
                let s = self.sessions.get_mut(&channel).expect("checked");
                // AMQPOutgoingController.init: createSender(queue).
                let created = s
                    .server
                    .create_consumer(consumer, &queue, None, 0, false, move |_: &Queue| {
                        sink as Arc<dyn DeliverySink>
                    })
                    .await;
                if let Err(e) = created {
                    let error = Error::new(
                        condition::INTERNAL_ERROR,
                        Some(format!("AMQ119005: error creating consumer, {}", e.message)),
                    );
                    self.send(
                        channel,
                        Performative::Detach(Detach {
                            handle,
                            closed: true,
                            error: Some(error),
                        }),
                    );
                    if let Some(s) = self.sessions.get_mut(&channel) {
                        s.detaching.insert(handle);
                    }
                    return Ok(());
                }
                s.server.consumer_credits(consumer, -1);
                let routing_type = s.server.queue_query(&queue).routing_type;
                s.links.insert(
                    handle,
                    Link::Sender(SenderLink {
                        name,
                        consumer,
                        resolved: SenderSource {
                            queue,
                            multicast: false,
                            selector: None,
                            browse: false,
                            priority: 0,
                            temp_queue: None,
                            shared: false,
                            global: false,
                            is_volatile: false,
                            routing_type: Some(routing_type),
                        },
                        source: link.attach.source.clone(),
                        credit,
                        link_credit: 0,
                        delivery_count: 0,
                        presettled: false,
                        draining: false,
                    }),
                );
            }
            LocalLinkKind::Receiver {
                queue,
                address,
                routing,
            } => {
                let s = self.sessions.get_mut(&channel).expect("checked");
                // The queue that asked for the link may have gone meanwhile.
                if !s.server.queue_query(&queue).exists {
                    let error = Error::new(
                        condition::NOT_FOUND,
                        Some(format!(
                            "AMQ119002: target address {address} does not exist"
                        )),
                    );
                    s.detaching.insert(handle);
                    self.send(
                        channel,
                        Performative::Detach(Detach {
                            handle,
                            closed: true,
                            error: Some(error),
                        }),
                    );
                    return Ok(());
                }
                let credits = self.settings.credits.max(0) as u32;
                let delivery_count = remote.initial_delivery_count.unwrap_or(0);
                s.server.add_producer(&name, "AMQP", Some(&address));
                s.links.insert(
                    handle,
                    Link::Receiver(ReceiverLink {
                        name,
                        target: ReceiverTarget {
                            address: Some(address),
                            explicit: Some(routing),
                            implicit: Some(routing),
                            dynamic: false,
                        },
                        source: link.attach.source.clone(),
                        credit: credits,
                        delivery_count,
                        incoming: None,
                        dynamic_expiry: TerminusExpiryPolicy::SessionEnd,
                    }),
                );
                let s = &self.sessions[&channel];
                let flow = self.link_flow(s, handle, delivery_count, credits, false);
                self.send(channel, Performative::Flow(flow));
            }
        }
        Ok(())
    }

    /// A broker connection's link failed: the connection closes, to be
    /// made again (`error(e, lastRetryCounter)`).
    fn link_failed(&mut self, message: String) {
        if self.ended.is_none() {
            tracing::warn!("{message}");
            self.ended = Some(Ended::LinkFailed(message.clone()));
        }
        self.close_with(Error::new(condition::INTERNAL_ERROR, Some(message)));
    }

    /// `close(remoteClose)` of a link: its consumer closes; a closed
    /// subscription or temporary destination goes.
    async fn close_link(&mut self, s: &mut Session, link: Link, remote_close: bool) {
        match link {
            Link::Sender(sl) => {
                lock(&sl.credit).closed = true;
                s.server.close_consumer(sl.consumer, false).await;
                // Deliveries of this link not yet settled are gone with it.
                s.out.retain(|_, d| d.consumer != sl.consumer);
                if remote_close {
                    self.close_subscription(s, &sl);
                }
            }
            Link::Receiver(rl) => {
                if rl.target.dynamic
                    && matches!(
                        rl.dynamic_expiry,
                        TerminusExpiryPolicy::LinkDetach | TerminusExpiryPolicy::SessionEnd
                    )
                    && let Some(a) = &rl.target.address
                {
                    let _ = s.server.delete_queue_with(a, false);
                    if self.temp_addresses.iter().any(|t| t == a) {
                        self.broker.post_office().remove_address(a);
                        self.temp_addresses.retain(|t| t != a);
                    }
                }
                s.server.remove_producer(&rl.name);
            }
            Link::Coordinator(_) => {}
        }
    }

    /// `DefaultSenderController.close(true)`: unsubscribes a durable
    /// subscription whose last consumer left, removes temporary queues.
    fn close_subscription(&mut self, s: &mut Session, sl: &SenderLink) {
        let Some(source) = &sl.source else { return };
        let r = &sl.resolved;
        let expiry_ends = matches!(
            source.expiry_policy,
            TerminusExpiryPolicy::LinkDetach | TerminusExpiryPolicy::SessionEnd
        );
        if r.multicast {
            if source.durable == TerminusDurability::None && expiry_ends {
                if let Some(t) = &r.temp_queue {
                    let _ = s.server.delete_queue_with(t, false);
                }
                return;
            }
            let pub_id = sl.name.split('|').next().unwrap_or(&sl.name);
            let queue = link::create_queue_name(
                self.config.use_core_subscription_naming,
                self.remote_container.as_deref(),
                pub_id,
                r.shared,
                r.global,
                r.is_volatile,
            );
            let q = s.server.queue_query(&queue);
            if q.exists
                && !r.is_volatile
                && q.consumer_count == 0
                && let Err(e) = s.server.delete_queue(&queue)
            {
                tracing::warn!("Error on link close: {}", e.message);
            }
        } else if source.dynamic && expiry_ends {
            let _ = s.server.delete_queue_with(&r.queue, false);
        }
    }

    // -------------------------------------------------------- flow

    fn on_flow(&mut self, channel: u16, flow: Flow) -> Handled {
        let Some(s) = self.sessions.get_mut(&channel) else {
            return Ok(());
        };
        s.remote_incoming_window = match flow.next_incoming_id {
            Some(next) => next
                .wrapping_add(flow.incoming_window)
                .wrapping_sub(s.next_outgoing_id),
            None => flow.incoming_window,
        };
        if !s.blocked.is_empty() {
            self.last_write = Instant::now();
            send_transfers(s, Vec::new(), &self.out);
        }
        let mut reply = None;
        let session_flow = |s: &Session| Flow {
            next_incoming_id: Some(s.next_incoming_id),
            incoming_window: WINDOW,
            next_outgoing_id: s.next_outgoing_id,
            outgoing_window: WINDOW,
            ..Default::default()
        };
        match flow.handle {
            None => {
                if flow.echo {
                    reply = Some(session_flow(s));
                }
            }
            Some(handle) => {
                let base = session_flow(s);
                if let Some(Link::Sender(sl)) = s.links.get_mut(&handle) {
                    // The credit the client gives from where it counted.
                    let remote_count = flow.delivery_count.unwrap_or(0);
                    let credit = flow.link_credit.unwrap_or(0);
                    sl.link_credit = remote_count
                        .wrapping_add(credit)
                        .wrapping_sub(sl.delivery_count);
                    {
                        let mut c = lock(&sl.credit);
                        c.available = i64::from(sl.link_credit) - c.pending;
                    }
                    let consumer = sl.consumer;
                    let drain = flow.drain && !sl.draining;
                    if drain {
                        sl.draining = true;
                    }
                    if flow.echo {
                        reply = Some(Flow {
                            handle: Some(handle),
                            delivery_count: Some(sl.delivery_count),
                            link_credit: Some(sl.link_credit),
                            ..base
                        });
                    }
                    s.server.prompt_delivery(consumer);
                    if drain {
                        // Delivers what it can, then says it is done.
                        s.server.force_delivery(consumer, 0);
                    }
                }
            }
        }
        if let Some(f) = reply {
            self.send(channel, Performative::Flow(f));
        }
        Ok(())
    }

    // -------------------------------------------------------- incoming

    async fn on_transfer(&mut self, channel: u16, t: Transfer, payload: Vec<u8>) -> Handled {
        let Some(s) = self.sessions.get_mut(&channel) else {
            return Err(Fatal::new(
                condition::ILLEGAL_STATE,
                "transfer on an unknown channel",
            ));
        };
        s.next_incoming_id = s.next_incoming_id.wrapping_add(1);
        let complete = match s.links.get_mut(&t.handle) {
            Some(Link::Receiver(rl)) => accumulate(
                &mut rl.incoming,
                &t,
                payload,
                &mut rl.credit,
                &mut rl.delivery_count,
            ),
            Some(Link::Coordinator(cl)) => accumulate(
                &mut cl.incoming,
                &t,
                payload,
                &mut cl.credit,
                &mut cl.delivery_count,
            ),
            Some(Link::Sender(_)) => {
                return Err(Fatal::new(
                    condition::ILLEGAL_STATE,
                    "transfer on a sending link",
                ));
            }
            None => {
                return Err(Fatal::new(
                    condition::UNATTACHED_HANDLE,
                    format!("handle {} is not attached", t.handle),
                ));
            }
        };
        let Some(incoming) = complete else {
            return Ok(());
        };
        let is_coordinator = matches!(
            self.sessions[&channel].links.get(&t.handle),
            Some(Link::Coordinator(_))
        );
        if is_coordinator {
            self.coordinate(channel, t.handle, incoming).await
        } else {
            self.receive(channel, t.handle, incoming).await
        }
    }

    /// A message from the client (`ProtonServerReceiverContext.onMessage`
    /// and `AMQPSessionCallback.serverSend`).
    async fn receive(&mut self, channel: u16, handle: u32, incoming: Incoming) -> Handled {
        let now = now_millis();
        let txn_id = match &incoming.state {
            Some(DeliveryState::Transactional(ts)) => Some(ts.txn_id.clone()),
            _ => None,
        };
        let tx = match &txn_id {
            Some(id) => match self.txns.get(id) {
                Some(t) => Some(t.tx.clone()),
                None => {
                    return Err(Fatal::new(
                        condition::INTERNAL_ERROR,
                        format!("AMQ119014: Transaction not found: xid={}", binary_text(id)),
                    ));
                }
            },
            None => None,
        };
        let container = self.remote_container.clone();
        let s = self.sessions.get_mut(&channel).expect("checked");
        let Some(Link::Receiver(rl)) = s.links.get_mut(&handle) else {
            return Ok(());
        };
        let target = rl.target.clone();
        let source = rl.source.clone();
        let large_at = self.settings.min_large_message_size;
        s.server.set_sender(Some(rl.name.clone()));
        let result = server_send(
            &mut s.server,
            &target,
            incoming.format,
            incoming.payload,
            container,
            tx.clone(),
            now,
            large_at,
        )
        .await;
        s.server.set_sender(None);
        let wait = s.server.take_completion();
        rl_topup(
            s,
            handle,
            self.settings.credits,
            self.settings.low_credits,
            &self.out,
            channel,
        );
        if incoming.settled {
            return Ok(());
        }
        let Some(id) = incoming.delivery_id else {
            return Ok(());
        };
        let state = match result {
            Ok(()) => match txn_id {
                Some(txn_id) => DeliveryState::Transactional(TransactionalState {
                    txn_id,
                    outcome: Some(Box::new(DeliveryState::Accepted)),
                }),
                None => DeliveryState::Accepted,
            },
            Err(e) => {
                if let Some(tx) = &tx {
                    saranga_server::transaction::lock(tx).mark_rollback_only(&e);
                }
                failure_state(
                    source.as_ref(),
                    &e,
                    self.settings.use_modified_for_transient_delivery_errors,
                )
            }
        };
        self.last_write = Instant::now();
        self.out.frame(
            channel,
            Performative::Disposition(Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled: true,
                state: Some(state),
                batchable: false,
            }),
            &[],
            wait,
        );
        Ok(())
    }

    // -------------------------------------------------------- transactions

    async fn coordinate(&mut self, channel: u16, handle: u32, incoming: Incoming) -> Handled {
        let now = now_millis();
        let body = AmqpMessage::new(i64::from(incoming.format), incoming.payload, now)
            .ok()
            .and_then(|m| m.body())
            .and_then(|(code, v)| (code == section::AMQP_VALUE).then_some(v));
        let state = match body {
            Some(v @ Value::Described(..)) if matches!(&v, Value::Described(d, _) if Declare::is(d)) =>
            {
                let _ = <Declare as Composite>::from_value(v);
                let id = uuid::Uuid::new_v4().to_string().into_bytes();
                let s = self.sessions.get_mut(&channel).expect("checked");
                let tx = s.server.new_transaction();
                self.txns.insert(id.clone(), Txn { tx, channel });
                DeliveryState::Declared(Declared { txn_id: id })
            }
            Some(v @ Value::Described(..)) if matches!(&v, Value::Described(d, _) if Discharge::is(d)) => {
                match <Discharge as Composite>::from_value(v) {
                    Ok(d) => self.discharge(channel, d).await,
                    Err(e) => DeliveryState::Rejected(Rejected {
                        error: Some(Error::new("failed", Some(e.to_string()))),
                    }),
                }
            }
            _ => DeliveryState::Rejected(Rejected {
                error: Some(Error::new(
                    "failed",
                    Some("not a declare or discharge".into()),
                )),
            }),
        };
        let s = self.sessions.get_mut(&channel).expect("checked");
        let wait = s.server.take_completion();
        if let Some(Link::Coordinator(cl)) = s.links.get_mut(&handle)
            && cl.credit < self.settings.low_credits.max(0) as u32
        {
            cl.credit += self.settings.credits.max(0) as u32;
            let (count, credit) = (cl.delivery_count, cl.credit);
            let s = &self.sessions[&channel];
            let flow = self.link_flow(s, handle, count, credit, false);
            self.send(channel, Performative::Flow(flow));
        }
        if let Some(id) = incoming.delivery_id {
            self.last_write = Instant::now();
            self.out.frame(
                channel,
                Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: id,
                    last: None,
                    settled: true,
                    state: Some(state),
                    batchable: false,
                }),
                &[],
                wait,
            );
        }
        Ok(())
    }

    /// Commits or rolls back (`ProtonTransactionHandler`, `discharge`).
    async fn discharge(&mut self, coordinator_channel: u16, d: Discharge) -> DeliveryState {
        let Some(txn) = self.txns.remove(&d.txn_id) else {
            return DeliveryState::Rejected(Rejected {
                error: Some(Error::new(
                    condition::ILLEGAL_STATE,
                    Some(format!(
                        "AMQ119014: Transaction not found: xid={}",
                        binary_text(&d.txn_id)
                    )),
                )),
            });
        };
        let fail = d.fail.unwrap_or(false);
        // The deliveries the client acknowledged in the transaction.
        let mut acked: Vec<(u16, u32)> = Vec::new();
        for (ch, s) in &self.sessions {
            for (id, od) in &s.out {
                if od.txn.as_deref() == Some(&d.txn_id[..]) {
                    acked.push((*ch, *id));
                }
            }
        }
        let channel = if self.sessions.contains_key(&txn.channel) {
            txn.channel
        } else {
            coordinator_channel
        };
        let result = {
            let s = self.sessions.get_mut(&channel).expect("checked");
            if fail {
                // Acknowledged but unsettled deliveries stay with their
                // consumers as delivered (`ProtonTransactionRefsOperation`).
                let keep: Vec<(i64, i64)> = acked
                    .iter()
                    .filter(|(ch, _)| *ch == channel)
                    .filter_map(|(_, id)| s.out.get(id))
                    .filter(|o| !o.remote_settled)
                    .map(|o| (o.consumer, o.message_id))
                    .collect();
                s.server.rollback_tx_keeping(&txn.tx, &keep)
            } else {
                s.server.commit_tx(&txn.tx)
            }
        };
        for (ch, id) in &acked {
            let Some(s) = self.sessions.get_mut(ch) else {
                continue;
            };
            let remote_settled = s.out.get(id).is_some_and(|o| o.remote_settled);
            if remote_settled {
                // Settled by the client: gone either way.
                if let Some(o) = s.out.remove(id) {
                    s.tags.give_back(o.tag);
                }
            } else if fail {
                if let Some(o) = s.out.get_mut(id) {
                    o.txn = None;
                }
            } else if let Some(o) = s.out.remove(id) {
                s.tags.give_back(o.tag);
                self.last_write = Instant::now();
                self.out.frame(
                    *ch,
                    Performative::Disposition(Disposition {
                        role: Role::Sender,
                        first: *id,
                        last: None,
                        settled: true,
                        state: Some(DeliveryState::Transactional(TransactionalState {
                            txn_id: d.txn_id.clone(),
                            outcome: Some(Box::new(DeliveryState::Accepted)),
                        })),
                        batchable: false,
                    }),
                    &[],
                    Completion::default(),
                );
            }
        }
        match result {
            Ok(()) => DeliveryState::AcceptedNew,
            Err(e) => DeliveryState::Rejected(Rejected {
                error: Some(Error::new(broker_condition(&e), Some(e.message))),
            }),
        }
    }

    // -------------------------------------------------------- outgoing

    async fn handle_event(&mut self, ev: Event) -> Handled {
        match ev {
            Event::Deliver {
                channel,
                handle,
                message,
                delivery_count,
            } => self.deliver(channel, handle, message, delivery_count).await,
            Event::Drained { channel, handle } => {
                let Some(s) = self.sessions.get_mut(&channel) else {
                    return Ok(());
                };
                let Some(Link::Sender(sl)) = s.links.get_mut(&handle) else {
                    return Ok(());
                };
                if !sl.draining {
                    return Ok(());
                }
                sl.draining = false;
                // drained(): the rest of the credit is used up.
                sl.delivery_count = sl.delivery_count.wrapping_add(sl.link_credit);
                sl.link_credit = 0;
                lock(&sl.credit).available = 0;
                let (count, _) = (sl.delivery_count, 0);
                let s = &self.sessions[&channel];
                let flow = self.link_flow(s, handle, count, 0, true);
                self.send(channel, Performative::Flow(flow));
                Ok(())
            }
            Event::Disconnect {
                channel,
                handle,
                reason,
            } => {
                // ProtonServerSenderContext.close(RESOURCE_DELETED).
                let Some(mut s) = self.sessions.remove(&channel) else {
                    return Ok(());
                };
                if let Some(l) = s.links.remove(&handle) {
                    self.close_link(&mut s, l, false).await;
                    self.send(
                        channel,
                        Performative::Detach(Detach {
                            handle,
                            closed: true,
                            error: Some(Error::new(condition::RESOURCE_DELETED, Some(reason))),
                        }),
                    );
                    s.detaching.insert(handle);
                }
                self.sessions.insert(channel, s);
                Ok(())
            }
            Event::Open(link) => {
                self.open_local(*link);
                Ok(())
            }
            Event::Fail(reason) => {
                if self.ended.is_none() {
                    self.ended = Some(Ended::Stopped);
                }
                self.close_with(Error::new(condition::INTERNAL_ERROR, Some(reason)));
                Ok(())
            }
        }
    }

    /// Writes one delivery (`ProtonServerSenderContext.deliverMessage`).
    async fn deliver(
        &mut self,
        channel: u16,
        handle: u32,
        message: Arc<ServerMessage>,
        delivery_count: i32,
    ) -> Handled {
        let max_frame = self.remote_max_frame.min(self.settings.max_frame_size) as usize;
        let Some(s) = self.sessions.get_mut(&channel) else {
            return Ok(());
        };
        let Some(Link::Sender(sl)) = s.links.get_mut(&handle) else {
            return Ok(());
        };
        let (format, payload) = match message
            .native()
            .and_then(|n| n.as_any().downcast_ref::<AmqpNative>())
        {
            Some(n) => (
                n.message.message_format as u32,
                n.message.send_buffer(delivery_count),
            ),
            None => (
                0,
                crate::convert::from_core(message.delivery_core(), now_millis())
                    .send_buffer(delivery_count),
            ),
        };
        sl.link_credit = sl.link_credit.saturating_sub(1);
        sl.delivery_count = sl.delivery_count.wrapping_add(1);
        lock(&sl.credit).pending -= 1;
        let presettled = sl.presettled;
        let consumer = sl.consumer;
        let id = s.next_delivery_id;
        s.next_delivery_id = s.next_delivery_id.wrapping_add(1);
        let tag = if presettled {
            Vec::new()
        } else {
            s.tags.take()
        };
        let first = Transfer {
            handle,
            delivery_id: Some(id),
            delivery_tag: Some(tag.clone()),
            message_format: Some(format),
            settled: Some(presettled),
            more: false,
            ..Default::default()
        };
        let frames = transfer_frames(channel, first, &payload, max_frame);
        self.last_write = Instant::now();
        send_transfers(s, frames, &self.out);
        let s = self.sessions.get_mut(&channel).expect("checked");
        if presettled {
            // Settled when written: acknowledged now.
            let _ = s
                .server
                .acknowledge_in(consumer, message.message_id(), true, None)
                .await;
        } else {
            s.out.insert(
                id,
                OutDelivery {
                    handle,
                    consumer,
                    message_id: message.message_id(),
                    tag,
                    txn: None,
                    remote_settled: false,
                },
            );
        }
        Ok(())
    }

    /// The client's disposition of deliveries the broker sent
    /// (`ProtonServerSenderContext.onMessage`).
    async fn on_disposition(&mut self, channel: u16, d: Disposition) -> Handled {
        if d.role != Role::Receiver {
            // About messages the client sent: the broker settled them.
            return Ok(());
        }
        let treat_reject = self.settings.treat_reject_as_unmodified_delivery_failed;
        let last = d.last.unwrap_or(d.first);
        let ids: Vec<u32> = {
            let Some(s) = self.sessions.get(&channel) else {
                return Ok(());
            };
            s.out.range(d.first..=last).map(|(k, _)| *k).collect()
        };
        for id in ids {
            let txn = match &d.state {
                Some(DeliveryState::Transactional(ts)) => match self.txns.get(&ts.txn_id) {
                    Some(t) => Some((ts.txn_id.clone(), t.tx.clone(), ts.outcome.clone())),
                    None => {
                        return Err(Fatal::new(
                            condition::INTERNAL_ERROR,
                            format!(
                                "AMQ119014: Transaction not found: xid={}",
                                binary_text(&ts.txn_id)
                            ),
                        ));
                    }
                },
                _ => None,
            };
            let s = self.sessions.get_mut(&channel).expect("checked");
            let Some(od) = s.out.get(&id) else { continue };
            let (consumer, message_id, handle) = (od.consumer, od.message_id, od.handle);
            let browse =
                matches!(s.links.get(&handle), Some(Link::Sender(sl)) if sl.resolved.browse);
            let mut settle = true;
            match &d.state {
                Some(DeliveryState::Accepted) | Some(DeliveryState::AcceptedNew) => {
                    if !browse
                        && let Err(e) = s
                            .server
                            .acknowledge_in(consumer, message_id, true, None)
                            .await
                    {
                        tracing::debug!(
                            "AMQ119007: error acknowledging message {message_id}, {}",
                            e.message
                        );
                    }
                }
                Some(DeliveryState::Released) => {
                    if !browse {
                        s.server.cancel_one(consumer, message_id, false);
                    }
                }
                Some(DeliveryState::Rejected(_)) => {
                    if !browse {
                        if treat_reject {
                            s.server.cancel_one(consumer, message_id, true);
                        } else {
                            s.server.reject(consumer, message_id);
                        }
                    }
                }
                Some(DeliveryState::Modified(m)) => {
                    if !browse {
                        let failed = m.delivery_failed.unwrap_or(false);
                        if m.undeliverable_here.unwrap_or(false) {
                            s.server.cancel_undeliverable(consumer, message_id, failed);
                        } else {
                            s.server.cancel_one(consumer, message_id, failed);
                        }
                    }
                }
                Some(DeliveryState::Transactional(_)) => {
                    let (txn_id, tx, outcome) = txn.expect("looked up");
                    match outcome.as_deref() {
                        Some(DeliveryState::Accepted) | Some(DeliveryState::AcceptedNew) => {
                            if !browse
                                && let Err(e) = s
                                    .server
                                    .acknowledge_in(consumer, message_id, true, Some(tx))
                                    .await
                            {
                                tracing::debug!(
                                    "acknowledging {message_id} in a transaction: {}",
                                    e.message
                                );
                            }
                            if let Some(od) = s.out.get_mut(&id) {
                                od.txn = Some(txn_id.clone());
                                od.remote_settled = d.settled;
                            }
                            settle = false;
                            if !d.settled {
                                self.last_write = Instant::now();
                                self.out.frame(
                                    channel,
                                    Performative::Disposition(Disposition {
                                        role: Role::Sender,
                                        first: id,
                                        last: None,
                                        settled: false,
                                        state: Some(DeliveryState::Transactional(
                                            TransactionalState {
                                                txn_id,
                                                outcome: Some(Box::new(DeliveryState::Accepted)),
                                            },
                                        )),
                                        batchable: false,
                                    }),
                                    &[],
                                    Completion::default(),
                                );
                            }
                        }
                        // Other outcomes in a transaction: as without one.
                        Some(DeliveryState::Released) => {
                            s.server.cancel_one(consumer, message_id, false)
                        }
                        Some(DeliveryState::Rejected(_)) => s.server.reject(consumer, message_id),
                        Some(DeliveryState::Modified(m)) => s.server.cancel_one(
                            consumer,
                            message_id,
                            m.delivery_failed.unwrap_or(false),
                        ),
                        _ => settle = false,
                    }
                }
                // Settled with no outcome, or received: nothing to do yet.
                None | Some(DeliveryState::Received(_)) | Some(DeliveryState::Declared(_)) => {
                    settle = false;
                }
            }
            let s = self.sessions.get_mut(&channel).expect("checked");
            let wait = s.server.take_completion();
            if settle && let Some(od) = s.out.remove(&id) {
                s.tags.give_back(od.tag);
                if !d.settled {
                    self.last_write = Instant::now();
                    self.out.frame(
                        channel,
                        Performative::Disposition(Disposition {
                            role: Role::Sender,
                            first: id,
                            last: None,
                            settled: true,
                            state: d.state.clone(),
                            batchable: false,
                        }),
                        &[],
                        wait,
                    );
                } else if !wait.is_empty() {
                    self.out.send(wait, Vec::new());
                }
            }
        }
        Ok(())
    }
}

/// `AMQPSessionCallback.serverSend`: the address and routing type, then
/// the send.
#[allow(clippy::too_many_arguments)] // AMQPSessionCallback.serverSend's
async fn server_send(
    server: &mut ServerSession,
    target: &ReceiverTarget,
    format: u32,
    payload: Vec<u8>,
    container: Option<String>,
    tx: Option<saranga_server::transaction::TxRef>,
    now: i64,
    large_at: usize,
) -> Result<(), BrokerError> {
    // At amqpMinLargeMessageSize the message goes to a file of its own.
    let large = large_at > 0 && payload.len() >= large_at;
    let file = large.then(|| payload.clone());
    let mut msg = AmqpMessage::new(i64::from(format), payload, now)
        .map_err(|e| BrokerError::internal(e.to_string()))?;
    let address = match &target.address {
        Some(a) => a.clone(),
        None => match msg.address() {
            Some(a) => a.to_string_lossy(),
            None => {
                return Err(BrokerError::new(
                    ErrorType::InternalError,
                    "Missing 'to' field for message sent to an anonymous producer",
                ));
            }
        },
    };
    // deduceRoutingType
    let routing = match target.explicit {
        Some(t) => Some(t),
        None if target.address.is_none() => {
            msg.routing_type().map(routing_from_byte).or_else(|| {
                server
                    .strip_prefix(&address)
                    .1
                    .or_else(|| link::implicit_routing_type(server, &address))
            })
        }
        None => match msg
            .annotation(crate::message::annotation::ROUTING_TYPE)
            .and_then(Value::as_i64)
        {
            Some(b) => Some(routing_from_byte(b as i8)),
            None => target.implicit,
        },
    };
    if server.check_auto_create(&address, routing)?
        == saranga_server::postoffice::AutoCreate::NotFound
    {
        let e = BrokerError::new(
            ErrorType::AddressDoesNotExist,
            format!("AMQ119002: target address {address} does not exist"),
        );
        if let Some(tx) = &tx {
            saranga_server::transaction::lock(tx).mark_rollback_only(&e);
        }
        return Err(e);
    }
    msg.set_connection_id(container);
    let body = match file {
        Some(bytes) => {
            let id = server.broker().post_office().ids().next();
            msg.set_message_id(id);
            msg.make_large();
            let io = |e: std::io::Error| BrokerError::new(ErrorType::IoError, e.to_string());
            let mut w = server
                .broker()
                .large_messages()
                .create(id, msg.is_durable())
                .map_err(io)?;
            w.append(&bytes).map_err(io)?;
            Some(w.finish().map_err(io)?)
        }
        None => None,
    };
    let native = AmqpNative::arc(msg);
    server
        .send_native_large_to(native, body, tx, Some(&address), routing)
        .await
}

/// Adds a transfer to the delivery it continues, or starts one; the
/// delivery when it is complete.
fn accumulate(
    incoming: &mut Option<Incoming>,
    t: &Transfer,
    payload: Vec<u8>,
    credit: &mut u32,
    delivery_count: &mut u32,
) -> Option<Incoming> {
    match incoming {
        Some(inc) => {
            inc.payload.extend_from_slice(&payload);
            if let Some(st) = &t.state {
                inc.state = Some(st.clone());
            }
            if t.settled == Some(true) {
                inc.settled = true;
            }
        }
        None => {
            *credit = credit.saturating_sub(1);
            *delivery_count = delivery_count.wrapping_add(1);
            *incoming = Some(Incoming {
                delivery_id: t.delivery_id,
                format: t.message_format.unwrap_or(0),
                settled: t.settled.unwrap_or(false),
                state: t.state.clone(),
                payload,
            });
        }
    }
    if t.aborted {
        *incoming = None;
        return None;
    }
    if t.more {
        return None;
    }
    incoming.take()
}

/// `topUpCreditIfNeeded`: back to `credits` once at or below `low`.
fn rl_topup(s: &mut Session, handle: u32, credits: i32, low: i32, out: &Outbound, channel: u16) {
    let Some(Link::Receiver(rl)) = s.links.get_mut(&handle) else {
        return;
    };
    let (credits, low) = (credits.max(0) as u32, low.max(0) as u32);
    if rl.credit <= low && credits > rl.credit {
        rl.credit = credits;
        let flow = Flow {
            next_incoming_id: Some(s.next_incoming_id),
            incoming_window: WINDOW,
            next_outgoing_id: s.next_outgoing_id,
            outgoing_window: WINDOW,
            handle: Some(handle),
            delivery_count: Some(rl.delivery_count),
            link_credit: Some(rl.credit),
            ..Default::default()
        };
        out.frame(
            channel,
            Performative::Flow(flow),
            &[],
            Completion::default(),
        );
    }
}

/// The frames of one delivery, split to the frame size, and how many.
fn transfer_frames(
    channel: u16,
    first: Transfer,
    payload: &[u8],
    max_frame: usize,
) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    let mut out = Vec::with_capacity(payload.len().min(max_frame) + 64);
    let header_len = |t: &Transfer| {
        let mut v = Vec::new();
        Composite::encode(t, &mut v);
        8 + v.len()
    };
    let mut probe = first.clone();
    probe.more = true;
    let room = max_frame.saturating_sub(header_len(&probe)).max(1);
    if payload.len() <= room {
        frame::encode_amqp(&mut out, channel, &Performative::Transfer(first), payload);
        return vec![out];
    }
    let mut rest = payload;
    let mut t = probe;
    loop {
        let r = max_frame.saturating_sub(header_len(&t)).max(1);
        let n = rest.len().min(r);
        t.more = n < rest.len();
        frame::encode_amqp(
            &mut out,
            channel,
            &Performative::Transfer(t.clone()),
            &rest[..n],
        );
        frames.push(std::mem::take(&mut out));
        rest = &rest[n..];
        if rest.is_empty() {
            break;
        }
        t = Transfer {
            handle: t.handle,
            more: true,
            ..Default::default()
        };
    }
    frames
}

/// Writes transfer frames while the client's incoming window allows,
/// keeping the rest for its next flow.
fn send_transfers(s: &mut Session, frames: Vec<Vec<u8>>, out: &Outbound) {
    s.blocked.extend(frames);
    let mut bytes = Vec::new();
    while s.remote_incoming_window > 0 {
        let Some(f) = s.blocked.pop_front() else {
            break;
        };
        bytes.extend(f);
        s.next_outgoing_id = s.next_outgoing_id.wrapping_add(1);
        s.remote_incoming_window -= 1;
    }
    if !bytes.is_empty() {
        out.now(bytes);
    }
}

/// `RoutingType.getType`: 0 multicast, 1 anycast.
fn routing_from_byte(b: i8) -> RoutingType {
    if b == 0 {
        RoutingType::Multicast
    } else {
        RoutingType::Anycast
    }
}

/// Proton-J's `Binary.toString`.
fn binary_text(b: &[u8]) -> String {
    crate::describe::to_string(&Value::Binary(b.to_vec()))
}

/// The AMQP condition of a broker failure (`createRejected`).
fn broker_condition(e: &BrokerError) -> &'static str {
    match e.kind {
        ErrorType::SecurityException => condition::UNAUTHORIZED_ACCESS,
        ErrorType::AddressFull => condition::RESOURCE_LIMIT_EXCEEDED,
        _ => "failed",
    }
}

/// `determineDeliveryState` for a message the broker could not take.
fn failure_state(source: Option<&Source>, e: &BrokerError, use_modified: bool) -> DeliveryState {
    let outcomes = source
        .and_then(|s| s.outcomes.as_ref())
        .filter(|o| !o.is_empty());
    let default = source.and_then(|s| s.default_outcome.clone());
    let effective_default = if outcomes.is_none() {
        default.clone()
    } else {
        None
    };
    let offers = |name: &str| outcomes.is_some_and(|o| o.iter().any(|s| s.as_str() == name));
    let rejected = DeliveryState::Rejected(Rejected {
        error: Some(Error::new(broker_condition(e), Some(e.message.clone()))),
    });
    if e.kind == ErrorType::AddressFull
        && use_modified
        && (offers(Modified::NAME) || matches!(effective_default, Some(DeliveryState::Modified(_))))
    {
        return DeliveryState::Modified(Modified {
            delivery_failed: Some(true),
            ..Default::default()
        });
    }
    if offers(Rejected::NAME) || matches!(effective_default, Some(DeliveryState::Rejected(_))) {
        return rejected;
    }
    match default {
        Some(d) => d,
        None => rejected,
    }
}

/// Why the peer refused a broker connection's link (`senderLinkRefused`,
/// `receiverLinkRefused`).
fn refused_message(link: &LocalLink) -> String {
    match &link.kind {
        LocalLinkKind::Sender { .. } => {
            let address = match &link.attach.target {
                Some(TargetKind::Target(t)) => t.address.clone().unwrap_or_default(),
                _ => String::new(),
            };
            format!("AMQ119023: Sender link refused for address {address}")
        }
        LocalLinkKind::Receiver { address, .. } => {
            format!("AMQ119029: Receiver link refused for address {address}")
        }
    }
}
