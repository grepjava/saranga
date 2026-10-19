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
use saranga_server::protocol::{Connection as Accepted, DirectOut, Written};
use saranga_server::queue::{DeliverySink, FORCED_DELIVERY_MESSAGE, Queue};
use saranga_server::session::{
    ClientConnection, Completion, ServerSession, SessionOptions, Transport,
};
use saranga_server::{Broker, BrokerError, RoutingType};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::composite::{Composite, Fields};
use crate::federation::constants as fed;
use crate::federation::manager::ConsumerKind;
use crate::federation::{FedLink, Federation};
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
use saranga_buffer::SimpleString;

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

/// Past this, corked bytes go to the writer without waiting for the task
/// to run out of work.
const CORK_LIMIT: usize = 64 * 1024;

struct Outbound {
    tx: mpsc::UnboundedSender<Out>,
    direct: DirectOut,
    /// What the task wrote since it last flushed, with nothing to wait for:
    /// it goes to the writer as one once the task has no frame or event
    /// left to handle, as Netty flushes at the end of a read. Handed over
    /// one frame at a time, a burst of deliveries or dispositions went out
    /// in a segment each, and the client woke and read for each.
    corked: Mutex<Cork>,
}

#[derive(Default)]
struct Cork {
    bytes: Vec<u8>,
    /// Only the task's loop flushes: before it, as the handshake waits
    /// for the peer, everything goes at once.
    on: bool,
}

impl Outbound {
    fn new(tx: mpsc::UnboundedSender<Out>, direct: DirectOut) -> Self {
        Self {
            tx,
            direct,
            corked: Mutex::new(Cork::default()),
        }
    }

    /// Hands `out` to the writer.
    fn queue(&self, out: Out) {
        self.direct.queue(|| self.tx.send(out).is_ok());
    }

    /// Writes `bytes`, which wait for nothing: to the socket while the
    /// writer has nothing left to write, rather than waking it on another
    /// thread for each delivery and disposition.
    fn ready(&self, bytes: Vec<u8>) {
        self.direct.write(bytes, |bytes| {
            self.tx
                .send(Out::Send {
                    wait: Completion::default(),
                    bytes,
                })
                .is_ok()
        });
    }

    /// Corks what is written from now on, or stops and flushes.
    fn cork(&self, on: bool) {
        lock(&self.corked).on = on;
        if !on {
            self.flush();
        }
    }

    fn send(&self, wait: Completion, bytes: Vec<u8>) {
        if wait.is_empty() {
            let mut corked = lock(&self.corked);
            if !corked.on && corked.bytes.is_empty() {
                drop(corked);
                self.ready(bytes);
                return;
            }
            if corked.bytes.is_empty() {
                corked.bytes = bytes;
            } else {
                corked.bytes.extend_from_slice(&bytes);
            }
            if corked.on && corked.bytes.len() < CORK_LIMIT {
                return;
            }
            let bytes = std::mem::take(&mut corked.bytes);
            drop(corked);
            self.ready(bytes);
        } else {
            // What came before goes first, without waiting for these
            // writes.
            self.flush();
            self.queue(Out::Send { wait, bytes });
        }
    }

    /// Writes what is corked.
    fn flush(&self) {
        let bytes = std::mem::take(&mut lock(&self.corked).bytes);
        if !bytes.is_empty() {
            self.ready(bytes);
        }
    }

    fn now(&self, bytes: Vec<u8>) {
        self.send(Completion::default(), bytes);
    }

    fn close(&self) {
        self.flush();
        self.queue(Out::Close);
    }

    fn frame(&self, channel: u16, p: Performative, payload: &[u8], wait: Completion) {
        // The header and a performative fit in 128 bytes but for long names.
        let mut out = Vec::with_capacity(128 + payload.len());
        frame::encode_amqp(&mut out, channel, &p, payload);
        self.send(wait, out);
    }
}

async fn write_loop<W: AsyncWrite + Unpin>(
    mut wr: W,
    mut rx: mpsc::UnboundedReceiver<Out>,
    written: Written,
) {
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    // How many of the items taken are in `buf`, to count off once written.
    let mut held = 0;
    let mut broken = false;
    while let Some(out) = rx.recv().await {
        let close = match out {
            Out::Send { wait, bytes } => {
                // What is ready goes out before waiting for the journal, so
                // frames already written are not held behind a later sync.
                let mut done = std::pin::pin!(wait.wait());
                let result = match saranga_server::session::poll_once(done.as_mut()).await {
                    Some(r) => r,
                    None => {
                        if !broken && !buf.is_empty() {
                            broken = wr.write_all(&buf).await.is_err() || wr.flush().await.is_err();
                        }
                        buf.clear();
                        written.done(std::mem::take(&mut held));
                        done.await
                    }
                };
                if let Err(e) = result {
                    tracing::error!("AMQ119031: storage operation failed: {e}");
                }
                buf.extend_from_slice(&bytes);
                held += 1;
                false
            }
            Out::Close => true,
        };
        if close || rx.is_empty() || buf.len() > 256 * 1024 {
            if !broken && !buf.is_empty() {
                broken = wr.write_all(&buf).await.is_err() || wr.flush().await.is_err();
            }
            buf.clear();
            written.done(std::mem::take(&mut held));
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
    /// What was read and not yet taken: frames are split off it, not copied.
    buf: ::bytes::BytesMut,
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
            self.buf.reserve((n - self.buf.len()).max(16 * 1024));
            let got = self
                .rd
                .read_buf(&mut self.buf)
                .await
                .map_err(|_| ReadError::Closed)?;
            if got == 0 {
                return Err(ReadError::Closed);
            }
        }
        Ok(())
    }

    async fn header(&mut self) -> Result<[u8; 8], ReadError> {
        self.fill(8).await?;
        let h: [u8; 8] = self.buf[..8].try_into().expect("8 bytes");
        let _ = self.buf.split_to(8);
        Ok(h)
    }

    /// Whether a whole frame (or a header) is read and not yet taken.
    fn ready(&self) -> bool {
        self.buf.len() >= 8
            && (self.buf.starts_with(b"AMQP")
                || frame::frame_size(&self.buf).is_some_and(|n| self.buf.len() >= n as usize))
    }

    /// The next frame, no larger than `max`.
    async fn frame(&mut self, max: u32) -> Result<::bytes::Bytes, ReadError> {
        self.fill(8).await?;
        let size = frame::frame_size(&self.buf).expect("4 bytes");
        if size < 8 {
            return Err(ReadError::Malformed(format!("frame size {size}")));
        }
        if size > max {
            return Err(ReadError::TooLarge(size));
        }
        self.fill(size as usize).await?;
        Ok(self.buf.split_to(size as usize).freeze())
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
    /// The queue's answer to an acknowledgement the client's
    /// disposition made (`acknowledge_then`).
    Acked {
        channel: u16,
        pending: saranga_server::session::AckPending,
        result: saranga_server::queue::AckResult,
    },
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
    /// A federation routes a message to an address here (a control or
    /// events link's queue).
    Route { address: String, payload: Vec<u8> },
    /// A federation acts on one of its links, by name.
    LinkOp { name: String, op: LinkOp },
    /// A federation failed: a broker connection's is made again, a
    /// target's connection closes (`signalError`).
    LinkFailed(String),
    /// An address a receiver sends to has room again: its credit is topped
    /// up (`creditRunnable`, run once memory is freed).
    TopUp { channel: u16, handle: u32, id: u64 },
    /// A receiver's drain or stop took longer than its quiesce timeout.
    Quiesce { channel: u16, handle: u32, id: u64 },
}

/// What a federation asks of one of its receivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkOp {
    /// Drain its credit and grant no more (`ProtonAbstractReceiver.stop`).
    Stop,
    /// Grant credit again (`start`).
    Start,
    /// Detach it (`close`).
    Close,
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

    /// Routes a message (its encoded sections) to an address.
    pub fn route(&self, address: String, payload: Vec<u8>) {
        let _ = self.0.send(Event::Route { address, payload });
    }

    /// Acts on a link this end attached, by name.
    pub fn link_op(&self, name: String, op: LinkOp) {
        let _ = self.0.send(Event::LinkOp { name, op });
    }

    /// Fails the connection for a federation's error.
    pub fn link_failed(&self, message: String) {
        let _ = self.0.send(Event::LinkFailed(message));
    }

    /// Whether the connection's task is gone.
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

/// Told when the peer's Open comes, with the capabilities it offers.
pub type OnOpen = Box<dyn FnOnce(&[Symbol]) + Send + Sync>;

/// What a connection the broker opens says and does
/// (`createOutgoingConnectionEntry`).
pub struct Outgoing {
    pub user: Option<String>,
    pub password: Option<String>,
    /// The URI's `saslMechanisms`, when given: the only ones chosen.
    pub sasl_mechanisms: Option<Vec<String>>,
    /// Whether the TLS handshake sent this end's certificate: SASL
    /// EXTERNAL can then be chosen.
    pub local_certificate: bool,
    /// Added to the Open's properties.
    pub properties: Fields,
    pub desired_capabilities: Option<Vec<Symbol>>,
    /// Told when the peer's Open comes, with its offered capabilities.
    pub on_open: Option<OnOpen>,
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
    fn deliver(&self, _consumer_id: i64, message: &Arc<ServerMessage>, delivery_count: i32) -> i64 {
        if message
            .core()
            .properties
            .get_str(FORCED_DELIVERY_MESSAGE)
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

/// The message to deliver: the one the queue keeps if it is native, else
/// its Core form.
fn delivery_message(m: &Arc<ServerMessage>) -> Arc<ServerMessage> {
    match m.native() {
        Some(_) => m.clone(),
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
    // Shared, as each transfer takes them while the session is lent out.
    name: Arc<str>,
    target: Arc<ReceiverTarget>,
    source: Option<Arc<Source>>,
    /// What the broker granted and has not been used.
    credit: u32,
    delivery_count: u32,
    incoming: Option<Incoming>,
    dynamic_expiry: TerminusExpiryPolicy,
    /// What its messages are for.
    mode: ReceiveMode,
    /// Its credit window and low mark, when not the acceptor's.
    window: Option<(i32, i32)>,
    state: ReceiverState,
    /// The address its last message went to, kept for the next, which
    /// shares it rather than holding its own.
    last_address: Option<SimpleString>,
    /// Told apart from a link attached later on the same handle.
    id: u64,
    /// A top-up waits for its address to have room.
    flow_waiting: bool,
    /// The drain's or the stop's timeout, while one runs.
    quiesce: Option<tokio::task::AbortHandle>,
    /// An anonymous relay's last address's policy, and whether a clash
    /// with another's was logged (`lastAddressPolicy`,
    /// `addressAlreadyClashed`).
    last_policy: Option<saranga_server::settings::AddressFullPolicy>,
    clashed: bool,
}

/// A receiving link's ID, unique in the broker.
fn next_link_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// What a receiving link does with a message.
#[derive(Clone)]
enum ReceiveMode {
    /// Sends it on (`ProtonServerReceiverContext`).
    Route,
    /// A federation control link's command (`AMQPFederationCommandProcessor`).
    Commands(Arc<Federation>),
    /// A federation events link's event (`AMQPFederationEventProcessor`).
    Events(Arc<Federation>),
    /// A federation or bridge receiver's message
    /// (`AMQPFederatedAddressDeliveryHandler`,
    /// `AMQPFederatedQueueDeliveryReceiver`,
    /// `AMQPBridgeAddressDeliveryReceiver`,
    /// `AMQPBridgeQueueDeliveryReceiver`).
    Managed(Arc<dyn ManagedReceiver>),
    /// A peer's mirror link (`AMQPMirrorControllerTarget`).
    Mirror(Arc<crate::mirror::target::MirrorTarget>),
}

/// A receiver a broker connection's policy keeps attached to the peer: a
/// federation's or a bridge's. Its manager stops, starts and closes it by
/// name ([`LinkOp`]).
pub trait ManagedReceiver: Send + Sync {
    fn link_name(&self) -> &str;
    /// Where its messages go: an address, or a queue's FQQN.
    fn destination(&self) -> &str;
    /// Whether each message gets one more hop (address federation).
    fn adds_hop(&self) -> bool {
        false
    }
    /// Its credit window and low mark.
    fn credits(&self) -> i32;
    fn credits_low(&self) -> i32;
    fn use_modified(&self) -> bool;
    /// Whether an address-full failure drains its credit, and how long
    /// the peer is given to drain it.
    fn drain_on_transient_delivery_errors(&self) -> bool;
    fn link_quiesce_timeout(&self) -> i32;
    /// The timeout its stops are given (`receiverQuiesceTimeout`).
    fn stop_timeout(&self) -> i32;
    fn large_threshold(&self) -> i32;
    fn is_closed(&self) -> bool;
    /// The peer answered the attach, taking the link or not.
    fn attached(&self, opened: bool);
    /// The peer closed the link: whether for a reason the connection
    /// survives (the link closed interceptor's answer).
    fn remote_closed(&self, error: Option<&Error>) -> bool;
    /// A stop asked for completed (its credit drained), or not.
    fn stopped(&self, ok: bool);
}

/// `ProtonAbstractReceiver`'s states as a stop sees them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReceiverState {
    Started,
    /// Draining its credit after an address-full failure, to start again
    /// once drained.
    Draining,
    Stopping,
    Stopped,
}

/// A federation's sending link (`AMQPFederationSenderController` and the
/// event dispatcher).
#[derive(Clone)]
struct FedSender {
    federation: Arc<Federation>,
    kind: FedSenderKind,
}

#[derive(Clone)]
enum FedSenderKind {
    /// Its queue is removed with it.
    Control,
    Events,
    /// The federation binding it consumes, removed when the peer closes the
    /// link.
    Address {
        address: String,
        queue: String,
    },
    Queue {
        queue: String,
    },
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
    fed: Option<FedSender>,
    /// A bridge's sender (`AMQPBridgeToSenderController`).
    bridge: Option<Arc<crate::bridge::Sender>>,
    /// A mirror's link: its store-and-forward address, whose other
    /// messages carry delivery annotations (`setProtocolData`).
    mirror: Option<String>,
}

/// The transaction coordinator link (`ProtonTransactionHandler`).
struct CoordinatorLink {
    credit: u32,
    delivery_count: u32,
    incoming: Option<Incoming>,
}

enum Link {
    Receiver(ReceiverLink),
    // Boxed: a sender holds much more than the other links.
    Sender(Box<SenderLink>),
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
    /// A federation's link.
    Federation(FedLink),
    /// A bridge's link.
    Bridge(crate::bridge::BridgeLink),
    /// A mirror's link, sending its store-and-forward queue
    /// (`connectSender` for a `<mirror>`).
    Mirror(Arc<crate::mirror::source::MirrorSource>),
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
    /// The same, shared by the messages it sends (`connectionID`).
    remote_container_id: Option<Arc<str>>,
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
    /// The federation on this connection, either end
    /// (`FEDERATION_INSTANCE_RECORD`).
    federation: Option<Arc<Federation>>,
    /// The internal session federation messages are routed through.
    fed_session: Option<ServerSession>,
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
    let (rd, wr) = saranga_server::protocol::split(stream);
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let (direct, written) = DirectOut::new(&wr);
    let writer = tokio::spawn(write_loop(wr, out_rx, written));
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
        out: Outbound::new(out_tx, direct),
        events: events_tx,
        user: None,
        password: None,
        auth: Authenticated::default(),
        remote_container: None,
        remote_container_id: None,
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
        federation: None,
        fed_session: None,
    };
    let mut reader = Reader {
        rd,
        buf: ::bytes::BytesMut::new(),
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
    // Acknowledgements on their way finish before the sessions close; one
    // answered from now on finishes alone.
    events_rx.close();
    while let Ok(ev) = events_rx.try_recv() {
        if let Event::Acked {
            channel,
            pending,
            result,
        } = ev
        {
            conn.acked(channel, pending, result);
        }
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

    /// `ClientSASLFactoryImpl.chooseMechanism`: among those offered (and
    /// allowed by the URI's `saslMechanisms`), EXTERNAL when the TLS
    /// handshake sent this end's certificate, then PLAIN with a user and
    /// password, then XOAUTH2, then ANONYMOUS. SCRAM is not supported, so
    /// it is not chosen.
    fn choose_mechanism(&self, offered: &[String]) -> Option<(&'static str, Vec<u8>)> {
        let o = self.outgoing.as_ref()?;
        let allowed = |m: &str| {
            offered.iter().any(|x| x == m)
                && o.sasl_mechanisms
                    .as_ref()
                    .filter(|e| !e.is_empty())
                    .is_none_or(|e| e.iter().any(|x| x == m))
        };
        if o.local_certificate && allowed("EXTERNAL") {
            return Some(("EXTERNAL", Vec::new()));
        }
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
        // One timer, set again when it fires, rather than one made and
        // dropped for each frame: tick looks at the idle times itself.
        let tick = tokio::time::sleep(self.next_tick());
        tokio::pin!(tick);
        self.out.cork(true);
        while !self.closed {
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
                () = &mut tick => {
                    self.tick();
                    tick.as_mut().reset(tokio::time::Instant::now() + self.next_tick());
                }
            }
            // Frames already read take no cooperative budget: without this,
            // a client sending fast keeps the task running, and the tasks it
            // woke (the queues) wait behind it.
            if !r.ready() && events.is_empty() {
                self.out.flush();
            }
            tokio::task::consume_budget().await;
        }
        self.out.cork(false);
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
        self.remote_container_id = Some(open.container_id.as_str().into());
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
                f(open.offered_capabilities.as_deref().unwrap_or_default());
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
        // A target goes with its connection (handleConnectionShutdown); a
        // source is told by its broker connection.
        if let Some(f) = self.federation.take()
            && !f.is_source()
        {
            f.shutdown();
        }
        if let Some(mut s) = self.fed_session.take() {
            s.close(false).await;
        }
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
                // isReplicaTarget
                Some(TargetKind::Target(t))
                    if t.address
                        .as_deref()
                        .is_some_and(|a| a.starts_with(saranga_server::mirror::MIRROR_ADDRESS)) =>
                {
                    self.attach_mirror_target(channel, attach)
                }
                _ if link::has(
                    attach.desired_capabilities.as_ref(),
                    fed::FEDERATION_CONTROL_LINK,
                ) =>
                {
                    self.attach_control_processor(channel, attach).await
                }
                _ if link::has(
                    attach.desired_capabilities.as_ref(),
                    fed::FEDERATION_EVENT_LINK,
                ) =>
                {
                    self.attach_events_processor(channel, attach)
                }
                _ => self.attach_receiver(channel, attach).await,
            },
            Role::Receiver => {
                let desired = attach.desired_capabilities.as_ref();
                if link::has(desired, fed::FEDERATION_ADDRESS_RECEIVER) {
                    self.attach_federation_sender(channel, attach, FedSenderRole::Address)
                        .await
                } else if link::has(desired, fed::FEDERATION_QUEUE_RECEIVER) {
                    self.attach_federation_sender(channel, attach, FedSenderRole::Queue)
                        .await
                } else if link::has(desired, fed::FEDERATION_EVENT_LINK) {
                    self.attach_federation_sender(channel, attach, FedSenderRole::Events)
                        .await
                } else {
                    self.attach_sender(channel, attach).await
                }
            }
        }
    }

    // -------------------------------------------------------- mirror links

    /// `handleReplicaTargetLinkOpened` and `addReplicaTarget`: the peer's
    /// mirror link, which must be allowed to send to its address and desire
    /// `amq.mirror`, makes this end its target. The answer offers
    /// `amq.mirror` and names this broker (`x-opt-amq-bkr-id`).
    fn attach_mirror_target(&mut self, channel: u16, attach: Attach) -> Handled {
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
        let address = match &attach.target {
            Some(TargetKind::Target(t)) => t.address.clone().unwrap_or_default(),
            _ => String::new(),
        };
        let s = self.sessions.get_mut(&channel).expect("checked");
        if let Err(e) = s
            .server
            .security_check(&address, None, saranga_config::CheckType::Send)
        {
            let e = LinkError::new(
                condition::UNAUTHORIZED_ACCESS,
                format!(
                    "AMQ119017: not authorized to create producer, {}",
                    e.message
                ),
            );
            tracing::warn!("{}", e.description);
            self.refuse_link(channel, reply, e);
            return Ok(());
        }
        if !link::has(
            attach.desired_capabilities.as_ref(),
            crate::mirror::MIRROR_CAPABILITY,
        ) {
            let e = LinkError::new(
                condition::ILLEGAL_STATE,
                format!(
                    "AMQ119024: link is missing a desired capability declaration {}",
                    crate::mirror::MIRROR_CAPABILITY
                ),
            );
            tracing::warn!("{}", e.description);
            self.refuse_link(channel, reply, e);
            return Ok(());
        }
        let remote_id = attach
            .properties
            .as_ref()
            .and_then(|p| p.get(crate::mirror::BROKER_ID))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let credits = self.settings.credits;
        let target = Arc::new(crate::mirror::target::MirrorTarget::new(
            self.broker.clone(),
            remote_id,
            credits,
        ));
        let mut properties = Fields::default();
        properties.insert(
            crate::mirror::BROKER_ID,
            Value::string(self.broker.node_id()),
        );
        reply.properties = Some(properties);
        reply.offered_capabilities = Some(vec![Symbol::from(crate::mirror::MIRROR_CAPABILITY)]);
        self.send(channel, Performative::Attach(reply));
        let credits = credits.max(0) as u32;
        let delivery_count = attach.initial_delivery_count.unwrap_or(0);
        let s = self.sessions.get_mut(&channel).expect("checked");
        s.links.insert(
            attach.handle,
            Link::Receiver(ReceiverLink {
                last_address: None,
                id: next_link_id(),
                flow_waiting: false,
                quiesce: None,
                last_policy: None,
                clashed: false,
                name: attach.name.clone().into(),
                target: Arc::new(ReceiverTarget {
                    address: Some(address),
                    explicit: None,
                    implicit: None,
                    dynamic: false,
                }),
                source: attach.source.clone().map(Arc::new),
                credit: credits,
                delivery_count,
                incoming: None,
                dynamic_expiry: TerminusExpiryPolicy::SessionEnd,
                mode: ReceiveMode::Mirror(target),
                window: None,
                state: ReceiverState::Started,
            }),
        );
        let s = &self.sessions[&channel];
        let flow = self.link_flow(s, attach.handle, delivery_count, credits, false);
        self.send(channel, Performative::Flow(flow));
        Ok(())
    }

    // -------------------------------------------------------- federation links

    /// `handleFederationControlLinkOpened` and
    /// `addFederationCommandProcessor`: the peer's federation control
    /// link makes this end its target.
    async fn attach_control_processor(&mut self, channel: u16, attach: Attach) -> Handled {
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
        let s = self.sessions.get_mut(&channel).expect("checked");
        if s.server
            .security_check(
                fed::FEDERATION_BASE_VALIDATION_ADDRESS,
                None,
                saranga_config::CheckType::Send,
            )
            .is_err()
        {
            let e = LinkError::new(
                condition::UNAUTHORIZED_ACCESS,
                "User does not have permission to attach to the federation control address",
            );
            tracing::warn!("{}", e.description);
            self.refuse_link(channel, reply, e);
            return Ok(());
        }
        if self.federation.is_some() {
            self.refuse_link(
                channel,
                reply,
                LinkError::new(
                    condition::ILLEGAL_STATE,
                    "Unexpected federation instance found on connection when creating control link processor",
                ),
            );
            return Ok(());
        }
        let mut target = match &attach.target {
            Some(TargetKind::Target(t)) if t.dynamic => t.clone(),
            other => {
                self.refuse_link(
                    channel,
                    reply,
                    LinkError::new(
                        condition::INTERNAL_ERROR,
                        format!("Remote Target did not arrive as dynamic node: {other:?}"),
                    ),
                );
                return Ok(());
            }
        };
        let props = attach.properties.clone().unwrap_or_default();
        let configuration: Vec<(String, Value)> = match props.get(fed::FEDERATION_CONFIGURATION) {
            Some(Value::Map(m)) => m
                .iter()
                .filter_map(|(k, v)| k.as_str().map(|k| (k.to_owned(), v.clone())))
                .collect(),
            _ => Vec::new(),
        };
        let name = props
            .get(fed::FEDERATION_NAME)
            .and_then(Value::as_str)
            .map_or_else(|| attach.name.clone(), str::to_owned);
        let version = props
            .get(fed::FEDERATION_VERSION)
            .and_then(Value::as_i64)
            .map_or(fed::FEDERATION_V1, |v| v as i32);
        let federation = Federation::target(
            &self.broker,
            name,
            crate::federation::Connection {
                handle: ConnectionHandle(self.events.clone()),
                remote_container: self.remote_container.clone().unwrap_or_default(),
                configuration: crate::federation::config::Configuration::new(
                    configuration,
                    self.settings.clone(),
                ),
                remote_version: version,
            },
        );
        // initialize: the session is tagged; the target's receivers are
        // attached on it.
        self.tag_federation_session(channel, &federation.name);
        self.main_channel.get_or_insert(channel);
        self.federation = Some(federation.clone());
        target.address = Some(attach.name.clone());
        reply.target = Some(TargetKind::Target(target));
        let mut properties = Fields::default();
        properties.insert(fed::FEDERATION_VERSION, Value::Int(fed::FEDERATION_V2));
        reply.properties = Some(properties);
        reply.offered_capabilities = Some(vec![Symbol::from(fed::FEDERATION_CONTROL_LINK)]);
        self.send(channel, Performative::Attach(reply));
        self.insert_processor(channel, &attach, ReceiveMode::Commands(federation));
        Ok(())
    }

    /// A receiving link for the federation's control or events messages,
    /// with ten credits topped up at three.
    fn insert_processor(&mut self, channel: u16, attach: &Attach, mode: ReceiveMode) {
        let delivery_count = attach.initial_delivery_count.unwrap_or(0);
        let s = self.sessions.get_mut(&channel).expect("checked");
        s.links.insert(
            attach.handle,
            Link::Receiver(ReceiverLink {
                last_address: None,
                id: next_link_id(),
                flow_waiting: false,
                quiesce: None,
                last_policy: None,
                clashed: false,
                name: attach.name.clone().into(),
                target: Arc::new(ReceiverTarget {
                    address: None,
                    explicit: None,
                    implicit: None,
                    dynamic: false,
                }),
                source: attach.source.clone().map(Arc::new),
                credit: 10,
                delivery_count,
                incoming: None,
                dynamic_expiry: TerminusExpiryPolicy::SessionEnd,
                mode,
                window: Some((10, 3)),
                state: ReceiverState::Started,
            }),
        );
        let s = &self.sessions[&channel];
        let flow = self.link_flow(s, attach.handle, delivery_count, 10, false);
        self.send(channel, Performative::Flow(flow));
    }

    /// `addFederationEventProcessor`: the peer's events link.
    fn attach_events_processor(&mut self, channel: u16, attach: Attach) -> Handled {
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
        let Some(federation) = self.federation.clone() else {
            self.refuse_link(
                channel,
                reply,
                LinkError::new(
                    condition::ILLEGAL_STATE,
                    "Unexpected federation event processor opened on connection without a federation instance active",
                ),
            );
            return Ok(());
        };
        let mut target = match &attach.target {
            Some(TargetKind::Target(t)) if t.dynamic => t.clone(),
            other => {
                self.refuse_link(
                    channel,
                    reply,
                    LinkError::new(
                        condition::INTERNAL_ERROR,
                        format!("Remote Terminus did not arrive as dynamic node: {other:?}"),
                    ),
                );
                return Ok(());
            }
        };
        target.address = Some(attach.name.clone());
        reply.target = Some(TargetKind::Target(target));
        reply.offered_capabilities = Some(vec![Symbol::from(fed::FEDERATION_EVENT_LINK)]);
        self.send(channel, Performative::Attach(reply));
        self.insert_processor(channel, &attach, ReceiveMode::Events(federation));
        Ok(())
    }

    /// The peer's federation receiver or events receiver: a sender
    /// (`AMQPFederationAddressSenderController`,
    /// `AMQPFederationQueueSenderController`,
    /// `AMQPFederationEventDispatcher`).
    async fn attach_federation_sender(
        &mut self,
        channel: u16,
        attach: Attach,
        role: FedSenderRole,
    ) -> Handled {
        let mut reply = Attach {
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
        let what = match role {
            FedSenderRole::Address => "federation Address sender",
            FedSenderRole::Queue => "federation Queue sender",
            FedSenderRole::Events => "federation event processor",
        };
        let Some(federation) = self.federation.clone() else {
            self.refuse_link(
                channel,
                reply,
                LinkError::new(
                    condition::ILLEGAL_STATE,
                    format!(
                        "Unexpected {what} opened on connection without a federation instance active"
                    ),
                ),
            );
            return Ok(());
        };
        let Some(mut source) = attach.source.clone() else {
            self.refuse_link(
                channel,
                reply,
                LinkError::new(
                    condition::NOT_IMPLEMENTED,
                    "Null source lookup not supported on federation links.",
                ),
            );
            return Ok(());
        };
        let priority = attach
            .properties
            .as_ref()
            .and_then(|p| p.get(fed::FEDERATION_RECEIVER_PRIORITY))
            .and_then(Value::as_i64)
            .map_or(0, |p| p as i32);
        let resolved = match role {
            FedSenderRole::Events => {
                if !source.dynamic {
                    Err(LinkError::new(
                        condition::INTERNAL_ERROR,
                        format!("Remote Terminus did not arrive as dynamic node: {source:?}"),
                    ))
                } else {
                    let queue = federation.events_queue_name(&attach.name);
                    source.address = Some(queue.clone());
                    self.federation_queue(channel, &queue)
                        .map(|()| (queue, None, FedSenderKind::Events))
                        .map_err(|e| LinkError::new(condition::INTERNAL_ERROR, e))
                }
            }
            FedSenderRole::Address => {
                self.resolve_address_sender(channel, &attach, &source, &federation)
            }
            FedSenderRole::Queue => self.resolve_queue_sender(channel, &source, &federation),
        };
        let (queue, filter, kind) = match resolved {
            Ok(r) => r,
            Err(e) => {
                self.refuse_link(channel, reply, e);
                return Ok(());
            }
        };
        let offered = match role {
            FedSenderRole::Address => fed::FEDERATION_ADDRESS_RECEIVER,
            FedSenderRole::Queue => fed::FEDERATION_QUEUE_RECEIVER,
            FedSenderRole::Events => fed::FEDERATION_EVENT_LINK,
        };
        let is_events = matches!(kind, FedSenderKind::Events);
        let fs = FedSender {
            federation: federation.clone(),
            kind,
        };
        let presettled = attach.snd_settle_mode == SenderSettleMode::Settled;
        if let Err(e) = self
            .queue_sender(
                channel,
                attach.handle,
                attach.name.clone(),
                &queue,
                filter,
                priority,
                Some(source.clone()),
                Some(fs),
            )
            .await
        {
            self.refuse_link(channel, reply, LinkError::new(condition::INTERNAL_ERROR, e));
            return Ok(());
        }
        if let Some(Link::Sender(sl)) = self
            .sessions
            .get_mut(&channel)
            .and_then(|s| s.links.get_mut(&attach.handle))
        {
            sl.presettled = presettled;
        }
        if is_events {
            federation.register_events_sender(queue);
        }
        reply.source = Some(source);
        reply.offered_capabilities = Some(vec![Symbol::from(offered)]);
        self.send(channel, Performative::Attach(reply));
        Ok(())
    }

    /// `AMQPFederationAddressSenderController.createServerConsumer`: the
    /// federation binding of the address, made if missing.
    fn resolve_address_sender(
        &mut self,
        channel: u16,
        attach: &Attach,
        source: &Source,
        federation: &Federation,
    ) -> Result<(String, Option<String>, FedSenderKind), LinkError> {
        let source_address = source.address.clone().unwrap_or_default();
        let props = attach
            .properties
            .as_ref()
            .and_then(|p| p.get(fed::FEDERATED_ADDRESS_SOURCE_PROPERTIES))
            .cloned();
        let prop = |key: &str| props.as_ref().and_then(|m| m.map_get(&Value::string(key)));
        let auto_delete = prop(fed::ADDRESS_AUTO_DELETE)
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let auto_delete_delay = prop(fed::ADDRESS_AUTO_DELETE_DELAY)
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let auto_delete_count = prop(fed::ADDRESS_AUTO_DELETE_MSG_COUNT)
            .and_then(Value::as_i64)
            .unwrap_or(0);
        // The receiver's filters: its selector, and no-local.
        let (selector_entry, no_local) = link::find_filters(source.filter.as_ref());
        let jms = selector_entry
            .as_ref()
            .and_then(|(_, v)| link::selector_text(v));
        if let Some(sel) = &jms
            && saranga_filter::Filter::parse(sel).is_err()
        {
            return Err(LinkError::new(condition::INVALID_FIELD, "Invalid filter"));
        }
        let selector = match no_local {
            Some(_) => {
                let no_local = format!(
                    "{}<>'{}'",
                    link::CONNECTION_ID_PROPERTY,
                    self.remote_container.as_deref().unwrap_or_default()
                );
                Some(match jms {
                    Some(j) => format!("{j} AND {no_local}"),
                    None => no_local,
                })
            }
            None => jms,
        };
        let (address, queue) = match source_address.split_once("::") {
            Some((a, q)) => (a.to_owned(), q.to_owned()),
            None => (source_address.clone(), attach.name.clone()),
        };
        let s = self.sessions.get_mut(&channel).expect("checked");
        let created = s
            .server
            .check_auto_create(&address, Some(RoutingType::Multicast))
            .map_err(|e| {
                if e.kind == ErrorType::SecurityException {
                    LinkError::new(
                        condition::UNAUTHORIZED_ACCESS,
                        format!(
                            "AMQ119015: not authorized to create consumer, {}",
                            e.message
                        ),
                    )
                } else {
                    LinkError::new(condition::INTERNAL_ERROR, e.message)
                }
            })?;
        let info = self.broker.post_office().address_info(&address);
        if created == saranga_server::postoffice::AutoCreate::NotFound || info.is_none() {
            federation.register_missing_address(&address);
            return Err(LinkError::new(
                condition::NOT_FOUND,
                "AMQ119010: source address does not exist",
            ));
        }
        if !info.is_some_and(|i| i.routing_types.multicast) {
            return Err(LinkError::new(
                condition::ILLEGAL_STATE,
                format!("Address {address} is not configured for MULTICAST support"),
            ));
        }
        let routing = source_routing_type(source);
        let mut q = s.server.queue_query(&queue);
        if !q.exists {
            let mut spec =
                saranga_server::postoffice::QueueSpec::new(queue.as_str(), address.as_str());
            spec.routing_type = Some(routing);
            spec.auto_create_address = Some(false);
            spec.max_consumers = Some(-1);
            spec.purge_on_no_consumers = Some(false);
            spec.filter_string = selector.clone();
            spec.durable = true;
            spec.auto_created = false;
            spec.auto_delete = Some(auto_delete);
            spec.auto_delete_delay = Some(auto_delete_delay);
            spec.auto_delete_message_count = Some(auto_delete_count);
            match s.server.create_queue(spec) {
                Ok(_) => {}
                Err(e) if e.kind == ErrorType::QueueExists => {}
                Err(e) => return Err(LinkError::new(condition::INTERNAL_ERROR, e.message)),
            }
            q = s.server.queue_query(&queue);
        }
        if *q.address != *address {
            return Err(LinkError::new(
                condition::ILLEGAL_STATE,
                format!(
                    "Requested queue: {queue} for federation of address: {address}, but it is already mapped to a different address: {}",
                    q.address
                ),
            ));
        }
        Ok((
            queue.clone(),
            None,
            FedSenderKind::Address { address, queue },
        ))
    }

    /// `AMQPFederationQueueSenderController.createServerConsumer`: the
    /// queue, which must be there.
    fn resolve_queue_sender(
        &mut self,
        channel: u16,
        source: &Source,
        federation: &Federation,
    ) -> Result<(String, Option<String>, FedSenderKind), LinkError> {
        let source_address = source.address.clone().unwrap_or_default();
        let (address, queue) = match source_address.split_once("::") {
            Some((a, q)) => (Some(a.to_owned()), q.to_owned()),
            None => (None, source_address.clone()),
        };
        let s = self.sessions.get_mut(&channel).expect("checked");
        let q = s.server.queue_query(&queue);
        if !q.exists {
            federation.register_missing_queue(&queue);
            return Err(LinkError::new(
                condition::NOT_FOUND,
                format!("Queue: '{queue}' does not exist"),
            ));
        }
        if let Some(a) = &address
            && *q.address != **a
        {
            federation.register_missing_queue(&queue);
            return Err(LinkError::new(
                condition::NOT_FOUND,
                format!("Queue: '{queue}' is not mapped to specified address: {a}"),
            ));
        }
        let (selector_entry, _) = link::find_filters(source.filter.as_ref());
        let filter = selector_entry
            .as_ref()
            .and_then(|(_, v)| link::selector_text(v));
        if let Some(f) = &filter
            && saranga_filter::Filter::parse(f).is_err()
        {
            return Err(LinkError::new(condition::INVALID_FIELD, "Invalid filter"));
        }
        // No need for the queue's own filter again.
        let filter = filter.filter(|f| q.filter_string.as_deref() != Some(f.as_str()));
        Ok((queue.clone(), filter, FedSenderKind::Queue { queue }))
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
        let s = self.sessions.get_mut(&channel).expect("checked");
        s.server
            .add_producer(&attach.name, "AMQP", resolved.address.as_deref());
        let delivery_count = attach.initial_delivery_count.unwrap_or(0);
        s.links.insert(
            attach.handle,
            Link::Receiver(ReceiverLink {
                last_address: None,
                id: next_link_id(),
                flow_waiting: false,
                quiesce: None,
                last_policy: None,
                clashed: false,
                name: attach.name.as_str().into(),
                target: Arc::new(resolved),
                source: attach.source.map(Arc::new),
                credit: 0,
                delivery_count,
                incoming: None,
                dynamic_expiry: expiry,
                mode: ReceiveMode::Route,
                window: None,
                state: ReceiverState::Started,
            }),
        );
        // start: the credit once the address has room.
        self.top_up(channel, attach.handle);
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
            Link::Sender(Box::new(SenderLink {
                name: attach.name.clone(),
                consumer,
                resolved,
                source: source.clone(),
                credit,
                link_credit: 0,
                delivery_count: 0,
                presettled,
                draining: false,
                fed: None,
                bridge: None,
                mirror: None,
            })),
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
        // The federation's interceptors: a remote close it expects is not
        // the connection's failure.
        let mut intercepted = false;
        let mut link_name = String::new();
        if let Some(l) = &link {
            match l {
                Link::Receiver(rl) => {
                    link_name = rl.name.to_string();
                    if let ReceiveMode::Managed(c) = &rl.mode {
                        intercepted = c.remote_closed(detach.error.as_ref());
                    }
                }
                Link::Sender(sl) => {
                    link_name = sl.name.clone();
                    if let Some(FedSender {
                        kind: FedSenderKind::Address { .. } | FedSenderKind::Queue { .. },
                        ..
                    }) = &sl.fed
                    {
                        intercepted = detach.error.is_none();
                    }
                    if let Some(b) = &sl.bridge {
                        intercepted = b.remote_closed(detach.error.as_ref());
                    }
                }
                Link::Coordinator(_) => {}
            }
        }
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
        if intercepted || !was_open {
            return Ok(());
        }
        // AMQPBrokerConnection.linkClosed: a broker connection whose link
        // the peer closes is made again.
        if self.outgoing.is_some() && closed {
            self.link_failed(
                "AMQ119021: The broker connection had a remote link closed unexpectedly".into(),
            );
        } else if self.outgoing.is_none() && self.federation.is_some() {
            // AMQPFederationTarget.handleLinkRemoteClose
            self.federation_failed(format!("Federation link closed unexpectedly: {link_name}"));
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
            LocalLinkKind::Sender { .. } | LocalLinkKind::Mirror(_) => remote.target.is_none(),
            LocalLinkKind::Receiver { .. } => remote.source.is_none(),
            LocalLinkKind::Federation(f) => {
                let f = f.clone();
                return self
                    .federation_link_attached(channel, handle, link, f, remote)
                    .await;
            }
            LocalLinkKind::Bridge(b) => {
                let b = b.clone();
                return self
                    .bridge_link_attached(channel, handle, link, b, remote)
                    .await;
            }
        };
        if refused {
            self.link_failed(refused_message(&link));
            return Ok(());
        }
        let name = link.attach.name.clone();
        match link.kind {
            LocalLinkKind::Mirror(source) => {
                // The peer must offer amq.mirror and name itself, not as
                // this broker.
                if !link::has(
                    remote.offered_capabilities.as_ref(),
                    crate::mirror::MIRROR_CAPABILITY,
                ) {
                    self.link_failed(format!(
                        "AMQ119018: link is missing an offered capability declaration [{}]",
                        crate::mirror::MIRROR_CAPABILITY
                    ));
                    return Ok(());
                }
                let remote_id = remote
                    .properties
                    .as_ref()
                    .and_then(|p| p.get(crate::mirror::BROKER_ID))
                    .map(|v| v.as_str().map(str::to_owned).unwrap_or_default());
                let Some(remote_id) = remote_id else {
                    self.link_failed(
                        "AMQ119019: There is no brokerID defined on the target connection. Connection will be closed."
                            .into(),
                    );
                    return Ok(());
                };
                if remote_id == self.broker.node_id() {
                    self.link_failed(
                        "AMQ119022: The broker connection is trying to connect to itself. Check your configuration."
                            .into(),
                    );
                    return Ok(());
                }
                source.set_remote_id(Some(remote_id));
                let queue = source.snf().name().to_string();
                let attach_source = link.attach.source.clone();
                match self
                    .queue_sender(channel, handle, name, &queue, None, 0, attach_source, None)
                    .await
                {
                    Ok(()) => {
                        let s = self.sessions.get_mut(&channel).expect("checked");
                        if let Some(Link::Sender(sl)) = s.links.get_mut(&handle) {
                            sl.mirror = Some(source.snf_address().to_owned());
                        }
                    }
                    Err(e) => self.refuse_local(channel, handle, condition::INTERNAL_ERROR, e),
                }
            }
            LocalLinkKind::Sender { queue } => {
                // AMQPOutgoingController.init: createSender(queue).
                let source = link.attach.source.clone();
                if let Err(e) = self
                    .queue_sender(channel, handle, name, &queue, None, 0, source, None)
                    .await
                {
                    self.refuse_local(channel, handle, condition::INTERNAL_ERROR, e);
                }
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
                let delivery_count = remote.initial_delivery_count.unwrap_or(0);
                s.server.add_producer(&name, "AMQP", Some(&address));
                s.links.insert(
                    handle,
                    Link::Receiver(ReceiverLink {
                        last_address: None,
                        id: next_link_id(),
                        flow_waiting: false,
                        quiesce: None,
                        last_policy: None,
                        clashed: false,
                        name: name.into(),
                        target: Arc::new(ReceiverTarget {
                            address: Some(address),
                            explicit: Some(routing),
                            implicit: Some(routing),
                            dynamic: false,
                        }),
                        source: link.attach.source.clone().map(Arc::new),
                        credit: 0,
                        delivery_count,
                        incoming: None,
                        dynamic_expiry: TerminusExpiryPolicy::SessionEnd,
                        mode: ReceiveMode::Route,
                        window: None,
                        state: ReceiverState::Started,
                    }),
                );
                self.top_up(channel, handle);
            }
            LocalLinkKind::Federation(_) | LocalLinkKind::Bridge(_) => {
                unreachable!("handled above")
            }
        }
        Ok(())
    }

    /// Detaches a link this end attached, with an error.
    fn refuse_local(&mut self, channel: u16, handle: u32, condition: &str, message: String) {
        self.send(
            channel,
            Performative::Detach(Detach {
                handle,
                closed: true,
                error: Some(Error::new(condition, Some(message))),
            }),
        );
        if let Some(s) = self.sessions.get_mut(&channel) {
            s.detaching.insert(handle);
        }
    }

    /// Detaches a link this end attached, with no error (`link.close()`).
    fn close_local(&mut self, channel: u16, handle: u32) {
        self.send(
            channel,
            Performative::Detach(Detach {
                handle,
                closed: true,
                error: None,
            }),
        );
        if let Some(s) = self.sessions.get_mut(&channel) {
            s.detaching.insert(handle);
        }
    }

    /// A sending link consuming a queue (`createSender`): the consumer
    /// and the link. The error is `AMQ119005`'s text.
    #[allow(clippy::too_many_arguments)] // the parts of a sender
    async fn queue_sender(
        &mut self,
        channel: u16,
        handle: u32,
        name: String,
        queue: &str,
        filter: Option<String>,
        priority: i32,
        source: Option<Source>,
        fed: Option<FedSender>,
    ) -> Result<(), String> {
        let credit = Arc::new(Mutex::new(Credit::default()));
        let sink = Arc::new(Sink {
            events: self.events.clone(),
            channel,
            handle,
            credit: credit.clone(),
        });
        let s = self.sessions.get_mut(&channel).expect("checked");
        let consumer = s.next_consumer;
        s.next_consumer += 1;
        let filter = filter
            .as_deref()
            .map(saranga_filter::translate::convert_to_activemq_filter_string);
        let created = s
            .server
            .create_consumer(
                consumer,
                queue,
                filter,
                priority,
                false,
                move |_: &Queue| sink as Arc<dyn DeliverySink>,
            )
            .await;
        if let Err(e) = created {
            return Err(format!("AMQ119005: error creating consumer, {}", e.message));
        }
        s.server.consumer_credits(consumer, -1);
        let routing_type = s.server.queue_query(queue).routing_type;
        s.links.insert(
            handle,
            Link::Sender(Box::new(SenderLink {
                name,
                consumer,
                resolved: SenderSource {
                    queue: queue.to_owned(),
                    multicast: false,
                    selector: None,
                    browse: false,
                    priority,
                    temp_queue: None,
                    shared: false,
                    global: false,
                    is_volatile: false,
                    routing_type: Some(routing_type),
                },
                source,
                credit,
                link_credit: 0,
                delivery_count: 0,
                presettled: false,
                draining: false,
                fed,
                bridge: None,
                mirror: None,
            })),
        );
        Ok(())
    }

    /// `createTemporaryQueue(name, ANYCAST, 1, true)` for a federation
    /// link's messages.
    fn federation_queue(&mut self, channel: u16, queue: &str) -> Result<(), String> {
        let s = self.sessions.get_mut(&channel).expect("checked");
        let mut spec = saranga_server::postoffice::QueueSpec::new(queue, queue);
        spec.routing_type = Some(RoutingType::Anycast);
        spec.durable = false;
        spec.temporary = true;
        spec.max_consumers = Some(1);
        spec.auto_delete = Some(true);
        match s.server.create_queue(spec) {
            Ok(_) => Ok(()),
            Err(e) if e.kind == ErrorType::QueueExists => Ok(()),
            Err(e) => Err(format!(
                "AMQ119001: error creating temporary queue, {}",
                e.message
            )),
        }
    }

    /// Marks this end's session on the connection as a federation's
    /// (`addMetaData(FEDERATION_NAME, name)`): its consumers are
    /// federation consumers.
    fn tag_federation_session(&self, channel: u16, name: &str) {
        if let Some(s) = self.sessions.get(&channel) {
            self.broker
                .add_session_metadata(s.server.name(), fed::FEDERATION_NAME_METADATA, name);
        }
    }

    /// The peer's answer to a federation's link (the
    /// `AMQP_LINK_INITIALIZER_KEY` runnables of `AMQPFederationSource` and
    /// `AMQPFederationConsumer`).
    async fn federation_link_attached(
        &mut self,
        channel: u16,
        handle: u32,
        link: LocalLink,
        kind: FedLink,
        remote: Attach,
    ) -> Handled {
        let name = link.attach.name.clone();
        let offered = |cap: &str| link::has(remote.offered_capabilities.as_ref(), cap);
        match kind {
            FedLink::Control(f) => {
                let Some(TargetKind::Target(t)) = &remote.target else {
                    self.link_failed(format!(
                        "AMQ119025: Federation control link refused: address = {name}"
                    ));
                    return Ok(());
                };
                if !offered(fed::FEDERATION_CONTROL_LINK) {
                    self.link_failed(format!(
                        "AMQ119018: link is missing an offered capability declaration {}",
                        fed::FEDERATION_CONTROL_LINK
                    ));
                    return Ok(());
                }
                self.tag_federation_session(channel, &f.name);
                // AMQPFederationCommandDispatcher.init
                let queue = f.control_queue_name(t.address.as_deref().unwrap_or_default());
                let made = match self.federation_queue(channel, &queue) {
                    Ok(()) => {
                        let fs = FedSender {
                            federation: f.clone(),
                            kind: FedSenderKind::Control,
                        };
                        self.queue_sender(
                            channel,
                            handle,
                            name,
                            &queue,
                            None,
                            0,
                            link.attach.source.clone(),
                            Some(fs),
                        )
                        .await
                    }
                    Err(e) => Err(e),
                };
                if let Err(e) = made {
                    self.link_failed(e);
                    return Ok(());
                }
                self.federation = Some(f.clone());
                let version = remote
                    .properties
                    .as_ref()
                    .and_then(|p| p.get(fed::FEDERATION_VERSION))
                    .and_then(Value::as_i64)
                    .map_or(fed::FEDERATION_V1, |v| v as i32);
                let container = self.remote_container.clone().unwrap_or_default();
                f.control_attached(container, version, queue);
            }
            FedLink::EventsSender(f) => {
                let ok = remote.target.is_some() && offered(fed::FEDERATION_EVENT_LINK);
                let mut queue = None;
                if ok {
                    // AMQPFederationEventDispatcher.init
                    let q = f.events_queue_name(&name);
                    let made = match self.federation_queue(channel, &q) {
                        Ok(()) => {
                            let fs = FedSender {
                                federation: f.clone(),
                                kind: FedSenderKind::Events,
                            };
                            self.queue_sender(
                                channel,
                                handle,
                                name,
                                &q,
                                None,
                                0,
                                link.attach.source.clone(),
                                Some(fs),
                            )
                            .await
                        }
                        Err(e) => Err(e),
                    };
                    match made {
                        Ok(()) => queue = Some(q),
                        Err(e) => {
                            self.link_failed(e);
                            return Ok(());
                        }
                    }
                } else {
                    // Events are not sent to this peer.
                    self.close_local(channel, handle);
                }
                f.events_sender_attached(queue);
            }
            FedLink::EventsReceiver(f) => {
                let ok = remote.source.is_some() && offered(fed::FEDERATION_EVENT_LINK);
                if ok {
                    let delivery_count = remote.initial_delivery_count.unwrap_or(0);
                    let s = self.sessions.get_mut(&channel).expect("checked");
                    s.links.insert(
                        handle,
                        Link::Receiver(ReceiverLink {
                            last_address: None,
                            id: next_link_id(),
                            flow_waiting: false,
                            quiesce: None,
                            last_policy: None,
                            clashed: false,
                            name: name.into(),
                            target: Arc::new(ReceiverTarget {
                                address: None,
                                explicit: None,
                                implicit: None,
                                dynamic: false,
                            }),
                            source: remote.source.clone().map(Arc::new),
                            credit: 10,
                            delivery_count,
                            incoming: None,
                            dynamic_expiry: TerminusExpiryPolicy::SessionEnd,
                            mode: ReceiveMode::Events(f.clone()),
                            window: Some((10, 3)),
                            state: ReceiverState::Started,
                        }),
                    );
                    let s = &self.sessions[&channel];
                    let flow = self.link_flow(s, handle, delivery_count, 10, false);
                    self.send(channel, Performative::Flow(flow));
                } else {
                    self.close_local(channel, handle);
                }
                f.events_receiver_attached();
            }
            FedLink::Consumer(c) => {
                if c.is_closed() {
                    // Closed before the peer answered.
                    self.close_local(channel, handle);
                    return Ok(());
                }
                let cap = match &c.kind {
                    ConsumerKind::Address { .. } => fed::FEDERATION_ADDRESS_RECEIVER,
                    ConsumerKind::Queue { .. } => fed::FEDERATION_QUEUE_RECEIVER,
                };
                if remote.source.is_some() && !offered(cap) {
                    self.federation_failed(format!(
                        "AMQ119018: link is missing an offered capability declaration {cap}"
                    ));
                    return Ok(());
                }
                self.attach_managed_receiver(channel, handle, &link, c, &remote);
            }
        }
        Ok(())
    }

    /// A federation or bridge receiver the peer answered: kept with its
    /// own credit window, granted once the peer took it. One the peer
    /// refused waits for its detach.
    fn attach_managed_receiver(
        &mut self,
        channel: u16,
        handle: u32,
        link: &LocalLink,
        c: Arc<dyn ManagedReceiver>,
        remote: &Attach,
    ) {
        let opened = remote.source.is_some();
        let address = c.destination().to_owned();
        let delivery_count = remote.initial_delivery_count.unwrap_or(0);
        let s = self.sessions.get_mut(&channel).expect("checked");
        let implicit = link::implicit_routing_type(&s.server, &address);
        s.server
            .add_producer(&link.attach.name, "AMQP", Some(&address));
        s.links.insert(
            handle,
            Link::Receiver(ReceiverLink {
                last_address: None,
                id: next_link_id(),
                flow_waiting: false,
                quiesce: None,
                last_policy: None,
                clashed: false,
                name: link.attach.name.clone().into(),
                target: Arc::new(ReceiverTarget {
                    address: Some(address),
                    explicit: None,
                    implicit,
                    dynamic: false,
                }),
                source: link.attach.source.clone().map(Arc::new),
                credit: 0,
                delivery_count,
                incoming: None,
                dynamic_expiry: TerminusExpiryPolicy::SessionEnd,
                mode: ReceiveMode::Managed(c.clone()),
                window: Some((c.credits(), c.credits_low())),
                state: ReceiverState::Started,
            }),
        );
        if opened {
            self.top_up(channel, handle);
        }
        c.attached(opened);
    }

    /// The peer's answer to a bridge's link (the `AMQP_LINK_INITIALIZER_KEY`
    /// runnables of the bridge receivers and senders). A link the peer
    /// refused is kept until its detach, which recovery answers.
    async fn bridge_link_attached(
        &mut self,
        channel: u16,
        handle: u32,
        link: LocalLink,
        kind: crate::bridge::BridgeLink,
        remote: Attach,
    ) -> Handled {
        use crate::bridge::{BridgeLink, SenderKind};
        let name = link.attach.name.clone();
        let not_found = |what: &str| format!("AMQ119002: target address {what} does not exist");
        match kind {
            BridgeLink::Receiver(r) => {
                if r.is_closed() {
                    // Closed before the peer answered.
                    self.close_local(channel, handle);
                    return Ok(());
                }
                // initialize: the local address or queue must still be there.
                let missing = match &r.queue {
                    Some(q) => (!self.broker.post_office().queue_query(q).exists).then_some(q),
                    None => self
                        .broker
                        .post_office()
                        .address_info(&r.destination)
                        .is_none()
                        .then_some(&r.destination),
                };
                if let Some(what) = missing {
                    let message = not_found(what);
                    self.refuse_local(channel, handle, condition::NOT_FOUND, message);
                    return Ok(());
                }
                self.attach_managed_receiver(channel, handle, &link, r, &remote);
            }
            BridgeLink::Sender(b) => {
                if b.is_closed() {
                    self.close_local(channel, handle);
                    return Ok(());
                }
                // createServerConsumer
                let made = match &b.kind {
                    SenderKind::Address {
                        address,
                        queue,
                        routing,
                        durable,
                        filter,
                        auto_delete,
                        auto_delete_delay,
                        auto_delete_message_count,
                    } => {
                        let po = self.broker.post_office();
                        if po.address_info(address).is_none() {
                            Err((condition::NOT_FOUND, not_found(address)))
                        } else {
                            let s = self.sessions.get_mut(&channel).expect("checked");
                            let mut spec =
                                saranga_server::postoffice::QueueSpec::new(queue.as_str(), address.as_str());
                            spec.routing_type = Some(*routing);
                            spec.filter_string = filter.clone().filter(|f| !f.is_empty());
                            spec.max_consumers = Some(1);
                            if *durable {
                                // The subscription is recovered, else made.
                                let q = s.server.queue_query(queue);
                                if q.exists {
                                    if q.address.to_string() != *address {
                                        Err((
                                            condition::ILLEGAL_STATE,
                                            format!(
                                                "Requested queue: {queue} for bridge to address: {address}, but it is already mapped to a different address: {}",
                                                q.address
                                            ),
                                        ))
                                    } else {
                                        Ok(())
                                    }
                                } else {
                                    spec.durable = true;
                                    spec.auto_create_address = Some(false);
                                    spec.purge_on_no_consumers = Some(false);
                                    spec.auto_delete = Some(*auto_delete);
                                    spec.auto_delete_delay = Some(*auto_delete_delay);
                                    spec.auto_delete_message_count =
                                        Some(*auto_delete_message_count);
                                    s.server.create_queue(spec).map(|_| ()).map_err(|e| {
                                        (condition::INTERNAL_ERROR, e.message)
                                    })
                                }
                            } else {
                                // createTemporaryQueue(address, queue, routing,
                                // filter, 1, true)
                                spec.durable = false;
                                spec.temporary = true;
                                spec.auto_delete = Some(true);
                                s.server.create_queue(spec).map(|_| ()).map_err(|e| {
                                    (
                                        condition::INTERNAL_ERROR,
                                        format!(
                                            "AMQ119001: error creating temporary queue, {}",
                                            e.message
                                        ),
                                    )
                                })
                            }
                        }
                        .map(|()| (queue.clone(), None))
                    }
                    SenderKind::Queue {
                        address,
                        queue,
                        filter,
                    } => {
                        let po = self.broker.post_office();
                        if po.address_info(address).is_none() {
                            Err((condition::NOT_FOUND, not_found(address)))
                        } else if !po.queue_query(queue).exists {
                            Err((condition::NOT_FOUND, not_found(queue)))
                        } else {
                            Ok((queue.clone(), filter.clone().filter(|f| !f.is_empty())))
                        }
                    }
                };
                let (queue, filter) = match made {
                    Ok(x) => x,
                    Err((cond, message)) => {
                        self.refuse_local(channel, handle, cond, message);
                        return Ok(());
                    }
                };
                let source = link.attach.source.clone();
                if let Err(e) = self
                    .queue_sender(
                        channel, handle, name, &queue, filter, b.priority, source, None,
                    )
                    .await
                {
                    self.refuse_local(channel, handle, condition::INTERNAL_ERROR, e);
                    return Ok(());
                }
                if let Some(Link::Sender(sl)) = self
                    .sessions
                    .get_mut(&channel)
                    .and_then(|s| s.links.get_mut(&handle))
                {
                    sl.presettled = b.presettled;
                    sl.bridge = Some(b.clone());
                }
                b.attached(remote.target.is_some());
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
                match sl.fed.as_ref().map(|f| &f.kind) {
                    // The command dispatcher's and event dispatcher's
                    // temporary queues go with their links.
                    Some(FedSenderKind::Control | FedSenderKind::Events) => {
                        let _ = s.server.delete_queue_with(&sl.resolved.queue, false);
                    }
                    // handleLinkRemotelyClosed: no more demand, so the
                    // federation binding goes.
                    Some(FedSenderKind::Address { queue, .. }) => {
                        if remote_close && s.server.queue_query(queue).exists {
                            let _ = s.server.delete_queue_with(queue, false);
                        }
                    }
                    Some(FedSenderKind::Queue { .. }) => {}
                    // tryDeleteTemporarySubscriptionQueue: a bridge's
                    // temporary subscription goes with its link.
                    None if sl.bridge.is_some() => {
                        if let Some(crate::bridge::SenderKind::Address {
                            queue,
                            durable: false,
                            ..
                        }) = sl.bridge.as_ref().map(|b| &b.kind)
                            && s.server.queue_query(queue).exists
                        {
                            let _ = s.server.delete_queue_with(queue, false);
                        }
                    }
                    None => {
                        if remote_close {
                            self.close_subscription(s, &sl);
                        }
                    }
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
        let mut stopped = None;
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
                } else if let Some(Link::Receiver(rl)) = s.links.get_mut(&handle) {
                    // The sender's count: credit it used up without sending
                    // (a drain) is gone.
                    if let Some(count) = flow.delivery_count {
                        let advanced = count.wrapping_sub(rl.delivery_count);
                        if (advanced as i32) > 0 {
                            rl.credit = rl.credit.saturating_sub(advanced);
                            rl.delivery_count = count;
                        }
                    }
                    stopped = Some(handle);
                }
            }
        }
        if let Some(h) = stopped {
            self.check_stopped(channel, h);
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
        let container = self.remote_container_id.clone();
        let s = self.sessions.get_mut(&channel).expect("checked");
        let Some(Link::Receiver(rl)) = s.links.get_mut(&handle) else {
            return Ok(());
        };
        let mode = rl.mode.clone();
        let mut target = rl.target.clone();
        let source = rl.source.clone();
        let mut large_at = self.settings.min_large_message_size;
        let mut payload = incoming.payload;
        match &mode {
            ReceiveMode::Route => {}
            ReceiveMode::Commands(f) | ReceiveMode::Events(f) => {
                let f = f.clone();
                let decoded = AmqpMessage::new(i64::from(incoming.format), payload, now)
                    .map_err(|e| e.to_string())
                    .and_then(|m| {
                        if matches!(mode, ReceiveMode::Commands(_)) {
                            crate::federation::policy::decode_command(&m, &f.wildcards)
                        } else {
                            crate::federation::policy::decode_event(&m)
                        }
                    });
                match decoded {
                    Ok(r) => f.received(r),
                    Err(e) => {
                        let what = if matches!(mode, ReceiveMode::Commands(_)) {
                            "control"
                        } else {
                            "event"
                        };
                        self.federation_failed(if e.starts_with("Remote sent") {
                            e
                        } else {
                            format!("Error while processing incoming {what} message: {e}")
                        });
                        return Ok(());
                    }
                }
                let s = self.sessions.get_mut(&channel).expect("checked");
                rl_topup(s, handle, 10, 3, &self.out, channel);
                if !incoming.settled
                    && let Some(id) = incoming.delivery_id
                {
                    self.send(
                        channel,
                        Performative::Disposition(Disposition {
                            role: Role::Receiver,
                            first: id,
                            last: None,
                            settled: true,
                            state: Some(DeliveryState::Accepted),
                            batchable: false,
                        }),
                    );
                }
                return Ok(());
            }
            ReceiveMode::Managed(c) => {
                large_at = c.large_threshold().max(0) as usize;
                // incrementAMQPMessageHops
                if c.adds_hop()
                    && let Ok(mut m) =
                        AmqpMessage::new(i64::from(incoming.format), payload.clone(), now)
                {
                    let hops = m
                        .annotation(fed::MESSAGE_HOPS_ANNOTATION)
                        .as_ref()
                        .and_then(Value::as_i64)
                        .map_or(1, |h| h as i32 + 1);
                    m.set_annotation(fed::MESSAGE_HOPS_ANNOTATION, Value::Int(hops));
                    m.reencode(now);
                    payload = m.data().to_vec();
                }
                Arc::make_mut(&mut target).address = Some(c.destination().to_owned());
            }
            ReceiveMode::Mirror(_) => {}
        }
        let result = match &mode {
            // A mirrored message that fails is logged and accepted all the
            // same (`actualDelivery`).
            ReceiveMode::Mirror(t) => {
                let _ = t
                    .receive(&mut s.server, incoming.format, payload, now, large_at)
                    .await;
                Ok(())
            }
            _ => {
                s.server.set_sender(Some(rl.name.clone()));
                let r = server_send(
                    &mut s.server,
                    &target,
                    incoming.format,
                    payload,
                    container,
                    tx.clone(),
                    now,
                    large_at,
                    incoming.settled,
                    &mut Anonymous {
                        address: &mut rl.last_address,
                        policy: &mut rl.last_policy,
                        clashed: &mut rl.clashed,
                    },
                )
                .await;
                s.server.set_sender(None);
                r
            }
        };
        let wait = s.server.take_completion();
        if let Err(e) = &result
            && !matches!(mode, ReceiveMode::Mirror(_))
        {
            // actualDelivery's and inSessionSend's logger.warn
            if !e.message.starts_with(MISSING_TO) {
                tracing::warn!("{}", e.message);
            }
            if e.kind == ErrorType::AddressFull {
                self.drain_on_full(channel, handle);
            }
        }
        if matches!(mode, ReceiveMode::Mirror(_)) {
            // AMQPMirrorControllerTarget.doCreditTopUpRun: no address to
            // wait for.
            let s = self.sessions.get_mut(&channel).expect("checked");
            rl_topup(
                s,
                handle,
                self.settings.credits,
                self.settings.low_credits,
                &self.out,
                channel,
            );
        } else {
            self.top_up(channel, handle);
        }
        self.check_stopped(channel, handle);
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
                    source.as_deref(),
                    &e,
                    match &mode {
                        ReceiveMode::Managed(c) => c.use_modified(),
                        _ => self.settings.use_modified_for_transient_delivery_errors,
                    },
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
            Event::Acked {
                channel,
                pending,
                result,
            } => {
                self.acked(channel, pending, result);
                Ok(())
            }
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
                    // resourceDeletedAction: the peer hears when it is back.
                    if let Link::Sender(sl) = &l
                        && let Some(fs) = &sl.fed
                    {
                        match &fs.kind {
                            FedSenderKind::Address { address, .. } => {
                                fs.federation.register_missing_address(address);
                            }
                            FedSenderKind::Queue { queue } => {
                                fs.federation.register_missing_queue(queue);
                            }
                            _ => {}
                        }
                    }
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
            Event::Route { address, payload } => {
                self.route_internal(&address, payload).await;
                Ok(())
            }
            Event::LinkOp { name, op } => {
                self.link_op(&name, op).await;
                Ok(())
            }
            Event::LinkFailed(message) => {
                self.federation_failed(message);
                Ok(())
            }
            Event::TopUp {
                channel,
                handle,
                id,
            } => {
                let (credits, low) = (self.settings.credits, self.settings.low_credits);
                if let Some(s) = self.sessions.get_mut(&channel)
                    && let Some(Link::Receiver(rl)) = s.links.get_mut(&handle)
                    && rl.id == id
                {
                    rl.flow_waiting = false;
                    rl_topup(s, handle, credits, low, &self.out, channel);
                }
                Ok(())
            }
            Event::Quiesce {
                channel,
                handle,
                id,
            } => {
                self.quiesce_timed_out(channel, handle, id).await;
                Ok(())
            }
        }
    }

    /// `topUpCreditIfNeeded` and `doCreditTopUpRun`: a started receiver
    /// low on credit is topped up once its address has room
    /// (`AMQPSessionCallback.flow`): an anonymous relay's last address, or
    /// before it sent, the broker's memory (its disk when `#` pages).
    fn top_up(&mut self, channel: u16, handle: u32) {
        let (credits, low) = (self.settings.credits, self.settings.low_credits);
        let Some(s) = self.sessions.get_mut(&channel) else {
            return;
        };
        let Some(Link::Receiver(rl)) = s.links.get_mut(&handle) else {
            return;
        };
        if rl.state != ReceiverState::Started || rl.flow_waiting {
            return;
        }
        // Nothing the credit runnable would grant: no need to ask.
        let (window, window_low) = rl.window.unwrap_or((credits, low));
        let (window, window_low) = (window.max(0) as u32, window_low.max(0) as u32);
        if rl.credit > window_low || window <= rl.credit {
            return;
        }
        let address: Option<String> = match &rl.target.address {
            Some(a) => Some(a.clone()),
            None => rl.last_address.as_ref().map(|a| a.to_string_lossy()),
        };
        let gate = Gate::default();
        let grant = gate.grant(
            self.events.clone(),
            Event::TopUp {
                channel,
                handle,
                id: rl.id,
            },
        );
        let broker = &self.broker;
        match address {
            None => {
                let paging = broker.post_office().paging();
                if broker.post_office().settings("#").address_full_policy
                    == saranga_server::settings::AddressFullPolicy::Page
                {
                    paging.check_storage(grant);
                } else {
                    paging.check_memory(grant);
                }
            }
            Some(a) => match broker.page_store(&a) {
                Some(store) => {
                    store.check_memory(true, grant, None, Some(s.server.blocked_runnables()));
                }
                None => grant(),
            },
        }
        if gate.now() {
            rl_topup(s, handle, credits, low, &self.out, channel);
        } else if let Some(Link::Receiver(rl)) = s.links.get_mut(&handle) {
            rl.flow_waiting = true;
        }
    }

    /// `deliveryFailed` on an address-full failure: a started receiver
    /// with credit drains it, and starts again once drained.
    fn drain_on_full(&mut self, channel: u16, handle: u32) {
        let fallback = (
            self.settings.drain_on_transient_delivery_errors,
            self.settings.link_quiesce_timeout,
        );
        let Some(s) = self.sessions.get_mut(&channel) else {
            return;
        };
        let Some(Link::Receiver(rl)) = s.links.get_mut(&handle) else {
            return;
        };
        let (drain, timeout) = match &rl.mode {
            ReceiveMode::Managed(c) => (
                c.drain_on_transient_delivery_errors(),
                c.link_quiesce_timeout(),
            ),
            _ => fallback,
        };
        if !drain || rl.state != ReceiverState::Started || rl.credit == 0 {
            return;
        }
        rl.state = ReceiverState::Draining;
        let (count, credit, id) = (rl.delivery_count, rl.credit, rl.id);
        if timeout > 0 {
            rl.quiesce = Some(quiesce_timer(&self.events, channel, handle, id, timeout));
        }
        let s = &self.sessions[&channel];
        let flow = self.link_flow(s, handle, count, credit, true);
        self.send(channel, Performative::Flow(flow));
    }

    /// The drain's timeout: a stopping receiver reports its stop failed;
    /// any other is closed with `detach-forced`.
    async fn quiesce_timed_out(&mut self, channel: u16, handle: u32, id: u64) {
        let Some(s) = self.sessions.get_mut(&channel) else {
            return;
        };
        let Some(Link::Receiver(rl)) = s.links.get_mut(&handle) else {
            return;
        };
        if rl.id != id || rl.quiesce.take().is_none() {
            return;
        }
        match rl.state {
            ReceiverState::Stopping => {
                if let ReceiveMode::Managed(c) = &rl.mode {
                    c.stopped(false);
                }
            }
            ReceiverState::Draining => {
                let error = Error::new(
                    condition::DETACH_FORCED,
                    Some("Timed out waiting for remote sender to drain".into()),
                );
                let mut s = self.sessions.remove(&channel).expect("found");
                if let Some(l) = s.links.remove(&handle) {
                    if let Link::Receiver(rl) = &l
                        && let ReceiveMode::Managed(c) = &rl.mode
                    {
                        c.remote_closed(Some(&error));
                    }
                    self.close_link(&mut s, l, false).await;
                    s.detaching.insert(handle);
                    self.send(
                        channel,
                        Performative::Detach(Detach {
                            handle,
                            closed: true,
                            error: Some(error),
                        }),
                    );
                }
                self.sessions.insert(channel, s);
            }
            _ => {}
        }
    }

    // -------------------------------------------------------- federation

    /// A federation's error: a broker connection is made again
    /// (`brokerConnection.error`), a target's connection closes
    /// (`AMQPFederationTarget.signalError`).
    fn federation_failed(&mut self, message: String) {
        if self.outgoing.is_some() {
            self.link_failed(message);
        } else {
            tracing::warn!("{message}");
            self.close_with(Error::new(condition::INTERNAL_ERROR, Some(message)));
        }
    }

    /// `postOffice.route(message, true)` of a federation's command or
    /// event, through an internal session.
    async fn route_internal(&mut self, address: &str, payload: Vec<u8>) {
        if self.fed_session.is_none() {
            let mut s = self.new_session(0, 0).server;
            s.set_internal();
            s.set_started(true).await;
            self.fed_session = Some(s);
        }
        let s = self.fed_session.as_mut().expect("made above");
        let msg = match AmqpMessage::new(0, payload, now_millis()) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("federation message to {address}: {e}");
                return;
            }
        };
        let r = s
            .send_native_large_to(
                AmqpNative::arc(msg),
                None,
                None,
                Some(address),
                Some(RoutingType::Anycast),
            )
            .await;
        // The queue takes it once the session's writes are done.
        let wait = s.take_completion();
        self.out.send(wait, Vec::new());
        if let Err(e) = r {
            tracing::warn!("federation message to {address}: {}", e.message);
        }
    }

    /// Where a link of this end is, by name.
    fn find_link(&self, name: &str) -> Option<(u16, u32)> {
        self.sessions.iter().find_map(|(ch, s)| {
            s.links.iter().find_map(|(h, l)| {
                let n = match l {
                    Link::Receiver(rl) => &*rl.name,
                    Link::Sender(sl) => sl.name.as_str(),
                    Link::Coordinator(_) => return None,
                };
                (n == name).then_some((*ch, *h))
            })
        })
    }

    /// Stops, starts or closes a federation receiver.
    async fn link_op(&mut self, name: &str, op: LinkOp) {
        let Some((channel, handle)) = self.find_link(name) else {
            return;
        };
        match op {
            LinkOp::Stop => {
                let s = self.sessions.get_mut(&channel).expect("found");
                let Some(Link::Receiver(rl)) = s.links.get_mut(&handle) else {
                    return;
                };
                let draining = match rl.state {
                    ReceiverState::Started => false,
                    ReceiverState::Draining => true,
                    _ => return,
                };
                rl.state = ReceiverState::Stopping;
                // A stop with its own timeout takes over a drain's.
                if draining
                    && let ReceiveMode::Managed(c) = &rl.mode
                    && c.stop_timeout() > 0
                    && let Some(t) = rl.quiesce.take()
                {
                    t.abort();
                }
                // receiver.drain(0): what credit there is, drained, unless
                // it is draining already.
                if rl.credit > 0 && !draining {
                    let (count, credit) = (rl.delivery_count, rl.credit);
                    let s = &self.sessions[&channel];
                    let flow = self.link_flow(s, handle, count, credit, true);
                    self.send(channel, Performative::Flow(flow));
                }
                self.check_stopped(channel, handle);
            }
            LinkOp::Start => {
                let s = self.sessions.get_mut(&channel).expect("found");
                let Some(Link::Receiver(rl)) = s.links.get_mut(&handle) else {
                    return;
                };
                if rl.state != ReceiverState::Stopped {
                    return;
                }
                rl.state = ReceiverState::Started;
                self.top_up(channel, handle);
            }
            LinkOp::Close => {
                let mut s = self.sessions.remove(&channel).expect("found");
                if let Some(l) = s.links.remove(&handle) {
                    self.close_link(&mut s, l, false).await;
                    s.detaching.insert(handle);
                    self.send(
                        channel,
                        Performative::Detach(Detach {
                            handle,
                            closed: true,
                            error: None,
                        }),
                    );
                }
                self.sessions.insert(channel, s);
            }
        }
    }

    /// `tryCompletePendingQuiesce`: a stopping receiver whose credit is
    /// all used or drained has stopped.
    fn check_stopped(&mut self, channel: u16, handle: u32) {
        let Some(s) = self.sessions.get_mut(&channel) else {
            return;
        };
        let Some(Link::Receiver(rl)) = s.links.get_mut(&handle) else {
            return;
        };
        if rl.credit != 0 || rl.incoming.is_some() {
            return;
        }
        match rl.state {
            ReceiverState::Stopping => {
                rl.state = ReceiverState::Stopped;
                if let Some(t) = rl.quiesce.take() {
                    t.abort();
                }
                if let ReceiveMode::Managed(c) = &rl.mode {
                    c.stopped(true);
                }
            }
            ReceiverState::Draining => {
                rl.state = ReceiverState::Started;
                if let Some(t) = rl.quiesce.take() {
                    t.abort();
                }
                self.top_up(channel, handle);
            }
            _ => {}
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
        let da = sl
            .mirror
            .as_deref()
            .and_then(|snf| crate::mirror::delivery_annotations(&message, snf));
        let (format, payload) = match message
            .native()
            .and_then(|n| n.as_any().downcast_ref::<AmqpNative>())
        {
            Some(n) => (
                n.message.message_format as u32,
                n.message.send_buffer_with(delivery_count, da.as_ref()),
            ),
            None => (
                0,
                crate::convert::from_core(message.delivery_core(), now_millis())
                    .send_buffer_with(delivery_count, da.as_ref()),
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

    /// The rest of an acknowledgement once its queue answered: in its
    /// session, or alone if the session has ended.
    fn acked(
        &mut self,
        channel: u16,
        pending: saranga_server::session::AckPending,
        result: saranga_server::queue::AckResult,
    ) {
        match self.sessions.get_mut(&channel) {
            Some(s) => {
                if let Err(e) = s.server.acknowledged(pending, result) {
                    tracing::debug!("AMQ119007: error acknowledging message, {}", e.message);
                }
            }
            None => pending.finish_alone(result),
        }
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
                    let acked = if browse {
                        Ok(())
                    } else if d.settled {
                        // Settled by the client, so nothing goes back: the
                        // queue's answer comes as an event, and the next
                        // disposition need not wait for it.
                        let events = self.events.clone();
                        s.server.acknowledge_then(
                            consumer,
                            message_id,
                            true,
                            move |pending, result| {
                                let acked = Event::Acked {
                                    channel,
                                    pending,
                                    result,
                                };
                                if let Err(mpsc::error::SendError(Event::Acked {
                                    pending,
                                    result,
                                    ..
                                })) = events.send(acked)
                                {
                                    pending.finish_alone(result);
                                }
                            },
                        )
                    } else {
                        // The broker settles it once the acknowledgement is
                        // stored, so it waits for the queue.
                        s.server
                            .acknowledge_in(consumer, message_id, true, None)
                            .await
                            .map(drop)
                    };
                    if let Err(e) = acked {
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
    container: Option<Arc<str>>,
    tx: Option<saranga_server::transaction::TxRef>,
    now: i64,
    large_at: usize,
    settled: bool,
    anonymous: &mut Anonymous<'_>,
) -> Result<(), BrokerError> {
    // At amqpMinLargeMessageSize the message goes to a file of its own.
    let large = large_at > 0 && payload.len() >= large_at;
    let file = large.then(|| payload.clone());
    let mut msg = AmqpMessage::new(i64::from(format), payload, now)
        .map_err(|e| BrokerError::internal(e.to_string()))?;
    let address: std::borrow::Cow<'_, str> = match &target.address {
        Some(a) => a.as_str().into(),
        None => match msg.address() {
            Some(a) => a.to_string_lossy().into(),
            None => {
                return Err(BrokerError::new(ErrorType::InternalError, MISSING_TO));
            }
        },
    };
    if target.address.is_none() {
        validate_anonymous_address(server, &address, anonymous);
    }
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
            .as_ref()
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
    // isRejectingMessages: past the address's reject threshold, a settled
    // message is dropped and its transaction rolled back, any other
    // refused.
    if server
        .broker()
        .page_store(&address)
        .is_some_and(|s| s.is_rejecting_messages())
    {
        let e = BrokerError::new(
            ErrorType::AddressFull,
            format!(
                "Address is full: {}",
                target.address.as_deref().unwrap_or("null")
            ),
        );
        if !settled {
            return Err(e);
        }
        if let Some(tx) = &tx {
            saranga_server::transaction::lock(tx).mark_rollback_only(&e);
        }
        return Ok(());
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
    // The ID and the address the session would give it (setMessageID,
    // setAddress), given here so that the session takes the message as it
    // is rather than copying it to change them.
    if msg.message_id() == 0 {
        msg.set_message_id(server.broker().post_office().ids().next());
    }
    let to = match anonymous.address.as_ref() {
        Some(a) if a.eq_str(&address) => a.clone(),
        _ => anonymous
            .address
            .insert(SimpleString::from(&*address))
            .clone(),
    };
    msg.route_to(to);
    let native = AmqpNative::arc(msg);
    server
        .send_native_large_to(native, body, tx, Some(&address), routing)
        .await
}

/// What `serverSend` asks of an anonymous relay's last address.
const MISSING_TO: &str = "Missing 'to' field for message sent to an anonymous producer";

/// A receiver's last address, and for an anonymous relay that address's
/// policy and whether a clash was logged.
struct Anonymous<'a> {
    address: &'a mut Option<SimpleString>,
    policy: &'a mut Option<saranga_server::settings::AddressFullPolicy>,
    clashed: &'a mut bool,
}

/// `validateAddressOnAnonymousLink`: an anonymous relay sending to an
/// address of another full policy than the last's is warned about once.
fn validate_anonymous_address(server: &ServerSession, address: &str, a: &mut Anonymous<'_>) {
    if a.address.as_ref().is_some_and(|l| l.eq_str(address)) {
        return;
    }
    let current = server
        .broker()
        .page_store(address)
        .map(|s| s.address_full_policy());
    if let (Some(last), Some(current)) = (*a.policy, current)
        && last != current
        && !*a.clashed
    {
        *a.clashed = true;
        tracing::warn!(
            "AMQ111004: AddressFullPolicy clash on an anonymous producer between destinations {}(addressFullPolicy={}) and {}(addressFullPolicy={}). This could lead to semantic inconsistencies on your clients. Notice you could have other instances of this scenario however this message will only be logged once. log.debug output would show all instances of this event.",
            a.address
                .as_ref()
                .map_or_else(|| "null".to_owned(), |l| l.to_string_lossy()),
            policy_name(last),
            address,
            policy_name(current)
        );
    }
    *a.address = Some(SimpleString::from(address));
    *a.policy = current;
}

/// Tells the connection's task when a drain has taken `timeout`
/// milliseconds.
fn quiesce_timer(
    events: &mpsc::UnboundedSender<Event>,
    channel: u16,
    handle: u32,
    id: u64,
    timeout: i32,
) -> tokio::task::AbortHandle {
    let events = events.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(timeout.max(0) as u64)).await;
        let _ = events.send(Event::Quiesce {
            channel,
            handle,
            id,
        });
    })
    .abort_handle()
}

/// `AddressFullMessagePolicy.name()`.
fn policy_name(p: saranga_server::settings::AddressFullPolicy) -> &'static str {
    use saranga_server::settings::AddressFullPolicy as P;
    match p {
        P::Page => "PAGE",
        P::Block => "BLOCK",
        P::Fail => "FAIL",
        P::Drop => "DROP",
    }
}

/// `AtomicRunnable` for a credit top-up: run where the check grants it at
/// once, else sent to the connection's task once there is room.
#[derive(Clone, Default)]
struct Gate(Arc<Mutex<GateState>>);

#[derive(Default)]
enum GateState {
    #[default]
    Checking,
    Now,
    Later,
}

impl Gate {
    fn grant(
        &self,
        events: mpsc::UnboundedSender<Event>,
        ev: Event,
    ) -> saranga_server::paging::Grant {
        let gate = self.clone();
        Box::new(move || {
            let mut st = lock(&gate.0);
            if matches!(*st, GateState::Checking) {
                *st = GateState::Now;
            } else {
                drop(st);
                let _ = events.send(ev);
            }
        })
    }

    /// After the check: whether it granted at once.
    fn now(&self) -> bool {
        let mut st = lock(&self.0);
        if matches!(*st, GateState::Now) {
            return true;
        }
        *st = GateState::Later;
        false
    }
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
    // A stopping or stopped receiver grants no credit.
    if rl.state != ReceiverState::Started {
        return;
    }
    let (credits, low) = rl.window.unwrap_or((credits, low));
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
        LocalLinkKind::Sender { .. } | LocalLinkKind::Mirror(_) => {
            let address = match &link.attach.target {
                Some(TargetKind::Target(t)) => t.address.clone().unwrap_or_default(),
                _ => String::new(),
            };
            format!("AMQ119023: Sender link refused for address {address}")
        }
        LocalLinkKind::Receiver { address, .. } => {
            format!("AMQ119029: Receiver link refused for address {address}")
        }
        LocalLinkKind::Federation(_) | LocalLinkKind::Bridge(_) => format!(
            "AMQ119021: The broker connection had a remote link closed unexpectedly: {}",
            link.attach.name
        ),
    }
}

/// Which of the federation's senders the peer's receiver asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FedSenderRole {
    Address,
    Queue,
    Events,
}

/// `AMQPFederationSenderController.getRoutingType`: the source's first
/// topic or queue capability, else the default (multicast).
fn source_routing_type(source: &Source) -> RoutingType {
    for c in source.capabilities.iter().flatten() {
        match c.as_str() {
            capability::TOPIC => return RoutingType::Multicast,
            capability::QUEUE => return RoutingType::Anycast,
            _ => {}
        }
    }
    RoutingType::Multicast
}
