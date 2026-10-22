//! An OpenWire connection: `OpenWireConnection`, with `AMQSession` and the
//! command handling of `OpenWireProtocolManager`.
//!
//! One task reads and handles commands in order. Replies wait for the
//! journal writes their command lined up, in order, on a second task;
//! dispatches go straight to the writer from the queues delivering them.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use saranga_buffer::PropertyValue;
use saranga_config::CheckType;
use saranga_core_protocol::ErrorType;
use saranga_core_protocol::packets::xa::Xid;
use saranga_message::CoreMessage;
use saranga_security::{Authenticated, SecurityError};
use saranga_server::postoffice::{AutoCreate, AutoCreateOptions, QueueSpec, split_fqqn};
use saranga_server::protocol::{Connection as Accepted, DirectOut, Written};
use saranga_server::queue::AckResult;
use saranga_server::session::{
    AckPending, ClientConnection, Completion, ServerSession, SessionOptions, Transport,
};
use saranga_server::settings::AddressFullPolicy;
use saranga_server::transaction::{self, TxError, TxRef, TxState, xa_code};
use saranga_server::{Broker, BrokerError, RoutingType};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::codec::{Input, Throwable};
use crate::commands::*;
use crate::consumer::{AmqConsumer, CONNECTION_ID_PROPERTY, Delivered, NewConsumer, message_size};
use crate::convert;
use crate::destination::{TEMP_QUEUE, TEMP_TOPIC, TOPIC};
use crate::primitive::{self, Primitive, PrimitiveMap};
use crate::{Manager, TempList, TransactionId, WireFormat, lock};

const ADVISORY_TOPIC_PREFIX: &str = "ActiveMQ.Advisory.";
const CONNECTION_ADVISORY: &str = "ActiveMQ.Advisory.Connection";
const TEMP_QUEUE_ADVISORY: &str = "ActiveMQ.Advisory.TempQueue";
const TEMP_TOPIC_ADVISORY: &str = "ActiveMQ.Advisory.TempTopic";
/// `Message.HDR_DUPLICATE_DETECTION_ID`.
const DUPLICATE_ID_PROPERTY: &str = "_AMQ_DUPL_ID";

// ---------------------------------------------------------------- errors

/// `serviceException` of an `IOException`: the `ConnectionError` that tells
/// the client its connection failed.
pub(crate) fn io_connection_error(message: &str) -> DataStructure {
    DataStructure::ConnectionError(ConnectionError {
        exception: Some(Box::new(OwError::io(message).throwable())),
        ..Default::default()
    })
}

/// A Java exception for an `ExceptionResponse` or `ConnectionError`.
#[derive(Debug, Clone)]
pub(crate) struct OwError {
    class: String,
    message: Option<String>,
}

impl OwError {
    fn new(class: &str, message: impl Into<String>) -> Self {
        Self {
            class: class.into(),
            message: Some(message.into()),
        }
    }

    /// `javax.jms.IllegalStateException`.
    fn jms_ise(message: impl Into<String>) -> Self {
        Self::new("javax.jms.IllegalStateException", message)
    }

    fn java_ise(message: impl Into<String>) -> Self {
        Self::new("java.lang.IllegalStateException", message)
    }

    fn invalid_destination(message: impl Into<String>) -> Self {
        Self::new("javax.jms.InvalidDestinationException", message)
    }

    fn npe() -> Self {
        Self {
            class: "java.lang.NullPointerException".into(),
            message: None,
        }
    }

    fn io(message: impl Into<String>) -> Self {
        Self::new("java.io.IOException", message)
    }

    fn resource_allocation(message: impl Into<String>) -> Self {
        Self::new("javax.jms.ResourceAllocationException", message)
    }

    fn is_invalid_destination(&self) -> bool {
        self.class == "javax.jms.InvalidDestinationException"
    }

    fn throwable(&self) -> Throwable {
        Throwable::new(&self.class, self.message.clone())
    }
}

/// The class `convertException` sends for an Artemis exception.
fn broker_class(kind: ErrorType) -> &'static str {
    match kind {
        ErrorType::SecurityException => "javax.jms.JMSSecurityException",
        ErrorType::QueueDoesNotExist => "javax.jms.InvalidDestinationException",
        ErrorType::AddressDoesNotExist => {
            "org.apache.activemq.artemis.api.core.ActiveMQAddressDoesNotExistException"
        }
        ErrorType::IllegalState => {
            "org.apache.activemq.artemis.api.core.ActiveMQIllegalStateException"
        }
        ErrorType::QueueExists => {
            "org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException"
        }
        ErrorType::AddressExists => {
            "org.apache.activemq.artemis.api.core.ActiveMQAddressExistsException"
        }
        ErrorType::AddressFull => {
            "org.apache.activemq.artemis.api.core.ActiveMQAddressFullException"
        }
        ErrorType::DuplicateIdRejected => {
            "org.apache.activemq.artemis.api.core.ActiveMQDuplicateIdException"
        }
        ErrorType::InvalidFilterExpression => {
            "org.apache.activemq.artemis.api.core.ActiveMQInvalidFilterExpressionException"
        }
        ErrorType::IoError => "org.apache.activemq.artemis.api.core.ActiveMQIOErrorException",
        ErrorType::MaxConsumerLimitExceeded => {
            "org.apache.activemq.artemis.api.core.ActiveMQQueueMaxConsumerLimitReached"
        }
        ErrorType::TransactionRolledBack => {
            "org.apache.activemq.artemis.api.core.ActiveMQTransactionRolledBackException"
        }
        ErrorType::InternalError => {
            "org.apache.activemq.artemis.api.core.ActiveMQInternalErrorException"
        }
        _ => "org.apache.activemq.artemis.api.core.ActiveMQException",
    }
}

impl From<BrokerError> for OwError {
    fn from(e: BrokerError) -> Self {
        Self::new(broker_class(e.kind), e.message)
    }
}

impl From<TxError> for OwError {
    fn from(e: TxError) -> Self {
        match e {
            TxError::Broker(b) => b.into(),
            TxError::Xa(x) => Self {
                class: "org.apache.activemq.artemis.core.exception.ActiveMQXAException".into(),
                message: x.message,
            },
        }
    }
}

impl From<convert::ConvertError> for OwError {
    fn from(e: convert::ConvertError) -> Self {
        Self::new("java.lang.RuntimeException", e.0)
    }
}

type OwResult<T> = Result<T, OwError>;

// ---------------------------------------------------------------- output

enum Out {
    Bytes(Vec<u8>),
    Close,
}

/// The write side of a connection, shared with the consumers delivering
/// to it: the negotiated wire format and the writer task's queue.
pub(crate) struct Outbound {
    tx: mpsc::UnboundedSender<Out>,
    direct: DirectOut,
    wf: Mutex<WireFormat>,
    destroyed: AtomicBool,
    /// When anything was last written (`lastSent`).
    last_sent: AtomicI64,
}

impl Outbound {
    /// `physicalSend`.
    pub fn send(&self, command: &DataStructure) -> bool {
        let bytes = match lock(&self.wf).marshal(Some(command)) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("error sending {}: {e}", command.class_name());
                return false;
            }
        };
        self.last_sent
            .store(saranga_server::message::now_millis(), Ordering::SeqCst);
        let tx = &self.tx;
        self.direct.write(bytes, |b| tx.send(Out::Bytes(b)).is_ok())
    }

    /// `sendCommand`: nothing once the connection is destroyed.
    pub fn send_command(&self, command: &DataStructure) -> bool {
        if self.destroyed.load(Ordering::SeqCst) {
            return false;
        }
        self.send(command)
    }
}

async fn write_loop<W: AsyncWrite + Unpin>(
    mut wr: W,
    mut rx: mpsc::UnboundedReceiver<Out>,
    written: Written,
) {
    let mut buf = Vec::with_capacity(64 * 1024);
    while let Some(first) = rx.recv().await {
        let mut close = matches!(first, Out::Close);
        // The items taken, to count off once written.
        let mut held = 1;
        if let Out::Bytes(b) = first {
            buf.extend_from_slice(&b);
        }
        while !close && buf.len() < 256 * 1024 {
            match rx.try_recv() {
                Ok(Out::Bytes(b)) => buf.extend_from_slice(&b),
                Ok(Out::Close) => close = true,
                Err(_) => break,
            }
            held += 1;
        }
        if !buf.is_empty() && wr.write_all(&buf).await.is_err() {
            break;
        }
        buf.clear();
        written.done(held);
        if close {
            break;
        }
    }
    let _ = wr.shutdown().await;
}

/// A reply, sent once the journal writes before it are done
/// (`sendAsyncResponse` through the connection's operation context).
struct Reply {
    completion: Completion,
    command: Option<DataStructure>,
    /// Run once the writes are done (credits returned on commit).
    after: Option<Box<dyn FnOnce() + Send>>,
}

async fn completer(out: Arc<Outbound>, mut rx: mpsc::UnboundedReceiver<Reply>) {
    while let Some(reply) = rx.recv().await {
        match reply.completion.wait().await {
            Err(e) => {
                tracing::warn!("AMQ222020: error processing IO callback: {e}");
                let correlation = match &reply.command {
                    Some(DataStructure::Response(r)) => r.correlation_id,
                    _ => 0,
                };
                out.send(&exception_response(
                    correlation,
                    &OwError::io(e.to_string()),
                ));
            }
            Ok(()) => completed(&out, reply.command, reply.after),
        }
    }
}

/// A reply whose journal writes are done: the command goes out, then what
/// follows it.
fn completed(
    out: &Outbound,
    command: Option<DataStructure>,
    after: Option<Box<dyn FnOnce() + Send>>,
) {
    if let Some(c) = &command {
        out.send(c);
    }
    if let Some(after) = after {
        after();
    }
}

fn exception_response(correlation_id: i32, e: &OwError) -> DataStructure {
    DataStructure::ExceptionResponse(ExceptionResponse {
        correlation_id,
        exception: Some(Box::new(e.throwable())),
        ..Default::default()
    })
}

fn response(correlation_id: i32) -> DataStructure {
    DataStructure::Response(Response {
        correlation_id,
        ..Default::default()
    })
}

/// What the connection's task hears besides its client.
enum Event {
    /// `delayedStop`: a refused login closes the connection.
    Stop,
    /// The queue answered an acknowledgement made outside a transaction
    /// ([`Connection::acknowledge`]).
    Acked {
        session: SessionId,
        pending: AckPending,
        result: AckResult,
    },
    /// An address that held producers' messages back has room: those
    /// released are sent (`sendShouldBlockProducer`'s task).
    Released,
    /// An address that stopped the connection's reads has room
    /// (`restoreAutoRead`).
    RestoreAutoRead,
    /// Management closed a session (`closeSessionWithID`).
    CloseSession { name: String },
}

/// A message a full address held back, sent once it has room.
struct BlockedSend {
    session: SessionId,
    message: CoreMessage,
    tx: Option<TxRef>,
    sender: Arc<str>,
    /// Its send in [`Connection::producer_sends`].
    send: u64,
}

/// A producer's send waiting for its destinations to take it: the last
/// to answers the producer (`count`).
struct ProducerSend {
    remaining: usize,
    command_id: i32,
    /// A `ProducerAck` of `size` answers it, else a `Response`.
    ack: Option<(ProducerId, i32)>,
}

/// Runs a grant where the check grants it at once, else sends `event` to
/// the connection's task once there is room.
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

// ---------------------------------------------------------------- naming

fn connection_id_string(id: Option<&ConnectionId>) -> String {
    id.and_then(|c| c.value.clone())
        .unwrap_or_else(|| "null".into())
}

fn session_id_string(id: &SessionId) -> String {
    format!(
        "{}:{}",
        id.connection_id.as_deref().unwrap_or("null"),
        id.value
    )
}

fn consumer_id_string(id: &ConsumerId) -> String {
    format!(
        "{}:{}:{}",
        id.connection_id.as_deref().unwrap_or("null"),
        id.session_id,
        id.value
    )
}

fn session_of_consumer(id: &ConsumerId) -> SessionId {
    SessionId {
        connection_id: id.connection_id.clone(),
        value: id.session_id,
    }
}

fn session_of_producer(id: &ProducerId) -> SessionId {
    SessionId {
        connection_id: id.connection_id.clone(),
        value: id.session_id,
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:x}")).collect()
}

/// `TransactionId.toString()`.
fn transaction_id_string(id: &TransactionId) -> String {
    match id {
        TransactionId::Local(l) => format!(
            "TX:{}:{}",
            connection_id_string(l.connection_id.as_ref()),
            l.value
        ),
        TransactionId::Xa(x) => format!(
            "XID:[{},globalId={},branchId={}]",
            x.format_id,
            hex(x.global_transaction_id.as_deref().unwrap_or_default()),
            hex(x.branch_qualifier.as_deref().unwrap_or_default())
        ),
    }
}

/// `OpenWireUtil.toXID`.
fn to_xid(x: &XaTransactionId) -> Xid {
    Xid {
        format_id: x.format_id,
        branch_qualifier: x.branch_qualifier.clone().unwrap_or_default(),
        global_transaction_id: x.global_transaction_id.clone().unwrap_or_default(),
    }
}

fn type_string(d: &Destination) -> &'static str {
    match d.data_type {
        TOPIC => "Topic",
        TEMP_QUEUE => "TempQueue",
        TEMP_TOPIC => "TempTopic",
        _ => "Queue",
    }
}

/// `AdvisorySupport.isAdvisoryTopic`.
fn is_advisory(d: &Destination) -> bool {
    if d.is_composite() {
        d.composite_destinations().iter().any(is_advisory)
    } else {
        d.is_topic() && d.physical_name().starts_with(ADVISORY_TOPIC_PREFIX)
    }
}

/// `isTempDestinationAdvisoryTopic`.
fn is_temp_advisory(d: &Destination) -> bool {
    let one = |d: &Destination| {
        d.is_topic()
            && !d.is_composite()
            && matches!(
                d.physical_name().as_str(),
                TEMP_QUEUE_ADVISORY | TEMP_TOPIC_ADVISORY
            )
    };
    if d.is_composite() {
        let members = d.composite_destinations();
        !members.is_empty() && members.iter().all(one)
    } else {
        one(d)
    }
}

/// `getDestinationAdvisoryTopic`.
fn destination_advisory(d: &Destination) -> Destination {
    Destination::topic(format!("{ADVISORY_TOPIC_PREFIX}{}", type_string(d)))
}

/// Class and physical name (`ActiveMQDestination.equals`).
fn same_destination(a: &Destination, b: &Destination) -> bool {
    a.data_type == b.data_type && a.physical_name() == b.physical_name()
}

/// `createQueueNameForSubscription`.
fn subscription_queue_name(client_id: Option<&str>, subscription: Option<&str>) -> String {
    let escape = |s: Option<&str>| {
        s.unwrap_or_default()
            .replace('\\', "\\\\")
            .replace('.', "\\.")
    };
    match client_id {
        Some(c) => format!("{}.{}", escape(Some(c)), escape(subscription)),
        None => escape(subscription),
    }
}

/// `SelectorTranslator.convertToActiveMQFilterString`, with JMSMessageID
/// read from the OpenWire header property.
fn translate_selector(s: &str) -> String {
    let s = saranga_filter::translate::convert_to_activemq_filter_string(s);
    saranga_filter::translate::parse(&s, "AMQUserID", convert::AMQ_MSG_MESSAGE_ID)
}

/// AMQ5 `min`: a non-positive value loses.
fn amq_min(a: i64, b: i64) -> i64 {
    if (a >= b || a <= 0) && b > 0 { b } else { a }
}

fn java_hash(s: &str) -> i32 {
    saranga_storage::java::string_hash(&s.encode_utf16().collect::<Vec<_>>())
}

// ---------------------------------------------------------------- state

/// `AMQSession`.
struct AmqSession {
    session: ServerSession,
    /// The session's unnegotiated wire format, for conversion.
    wf: WireFormat,
    /// Queues checked on send (`existingQueuesCache`).
    queue_cache: Vec<Option<String>>,
    next_consumer: i64,
    /// The connection's own session, `-1`.
    internal: bool,
}

/// A consumer as the client sees it: one member per destination of a
/// composite (`AMQConsumerBrokerExchange`).
struct Exchange {
    session: SessionId,
    members: Vec<Arc<AmqConsumer>>,
}

impl Exchange {
    /// The member an acknowledgement or pull is for: the only one, or the
    /// one its destination names.
    fn member(&self, destination: Option<&Destination>) -> Option<&Arc<AmqConsumer>> {
        if self.members.len() == 1 {
            return self.members.first();
        }
        let d = destination?;
        self.members
            .iter()
            .find(|m| same_destination(&m.destination, d))
    }
}

/// An acknowledgement made in a client transaction.
struct TxAck {
    session: SessionId,
    consumer: Arc<AmqConsumer>,
    delivered: Delivered,
}

/// A client transaction: Artemis's `txMap` entry for local ones, and for
/// both what the protocol keeps besides the transaction.
struct TxEntry {
    tx: TxRef,
    /// `protocolData`: the session of the last send or acknowledgement.
    session: Option<SessionId>,
    acks: Vec<TxAck>,
    /// Credits the consumers get back on commit.
    credits: Vec<(Arc<AmqConsumer>, i32)>,
}

impl TxEntry {
    fn new(tx: TxRef) -> Self {
        Self {
            tx,
            session: None,
            acks: Vec::new(),
            credits: Vec::new(),
        }
    }
}

/// A temporary address or queue the connection made, deleted with it.
struct TempResource {
    name: String,
    queue: bool,
}

struct Connection {
    broker: Arc<Broker>,
    manager: Arc<Manager>,
    client: Arc<ClientConnection>,
    out: Arc<Outbound>,
    replies: mpsc::UnboundedSender<Reply>,
    events: mpsc::UnboundedSender<Event>,
    key: u64,
    remote: String,
    local: String,
    in_wf: WireFormat,
    /// The `ConnectionInfo` that logged in (`state`).
    info: Option<ConnectionInfo>,
    authenticated: Authenticated,
    internal: Option<ServerSession>,
    sessions: HashMap<SessionId, AmqSession>,
    producers: HashMap<ProducerId, ProducerInfo>,
    exchanges: HashMap<ConsumerId, Exchange>,
    local_txs: HashMap<LocalTransactionId, TxEntry>,
    xa_txs: HashMap<XaTransactionId, TxEntry>,
    temp_destinations: TempList,
    temp_resources: Vec<TempResource>,
    no_local: Arc<AtomicBool>,
    dont_send_response: bool,
    destroyed: bool,
    /// The socket is read (`autoRead`): a full address under BLOCK stops
    /// the reads of a producer that does not wait for answers.
    auto_read: bool,
    /// The command being handled (`lastCommand`).
    handling: Option<i32>,
    /// The TTL is not checked while reads are stopped (`disableTtl`).
    ttl_disabled: bool,
    /// Sends held back by full addresses, by key.
    producer_sends: HashMap<u64, ProducerSend>,
    /// Those released, to be sent before anything the client sends after
    /// (Artemis sends them as they are released).
    released: Arc<Mutex<std::collections::VecDeque<BlockedSend>>>,
    next_producer_send: u64,
    // keep-alive
    use_keep_alive: bool,
    max_inactivity: i64,
    ttl: Option<i64>,
    ttl_from: Option<Instant>,
    data_received: bool,
    last_check: Instant,
}

/// Serves an OpenWire connection until it closes.
pub async fn serve(broker: Arc<Broker>, manager: Arc<Manager>, c: Accepted) {
    let remote = c.info.remote_address.trim_start_matches('/').to_owned();
    let local = local_address(&c);
    let client = Arc::new(
        ClientConnection::new(
            Transport::socket("OPENWIRE", "OpenWireConnection", c.peer, c.local),
            Arc::new(c.info),
            c.acceptor.prefixes.clone(),
        )
        .with_acceptor(&c.acceptor.name),
    );
    let _registered = broker.registry().add(&client);
    let failed = client.clone();
    let (mut rd, wr) = saranga_server::protocol::split(c.stream);
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let (direct, written) = DirectOut::new(&wr);
    let writer = tokio::spawn(write_loop(wr, out_rx, written));
    let out = Arc::new(Outbound {
        tx: out_tx,
        direct,
        wf: Mutex::new(WireFormat::new(crate::codec::DEFAULT_WIRE_VERSION)),
        destroyed: AtomicBool::new(false),
        last_sent: AtomicI64::new(-1),
    });
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (replies_tx, replies_rx) = mpsc::unbounded_channel();
    let completer = tokio::spawn(completer(out.clone(), replies_rx));
    let settings = manager.settings.clone();
    let key = manager.next_connection();
    let mut conn = Connection {
        broker,
        manager,
        client,
        out,
        replies: replies_tx,
        events: events_tx,
        key,
        remote,
        local,
        in_wf: WireFormat::new(crate::codec::DEFAULT_WIRE_VERSION),
        info: None,
        authenticated: Authenticated::default(),
        internal: None,
        sessions: HashMap::new(),
        producers: HashMap::new(),
        exchanges: HashMap::new(),
        local_txs: HashMap::new(),
        xa_txs: HashMap::new(),
        temp_destinations: TempList::default(),
        temp_resources: Vec::new(),
        no_local: Arc::new(AtomicBool::new(false)),
        dont_send_response: false,
        destroyed: false,
        auto_read: true,
        handling: None,
        ttl_disabled: false,
        producer_sends: HashMap::new(),
        released: Arc::default(),
        next_producer_send: 0,
        use_keep_alive: settings.use_keep_alive,
        max_inactivity: settings.max_inactivity_duration,
        ttl: None,
        ttl_from: None,
        data_received: false,
        last_check: Instant::now(),
    };
    // sendHandshake
    conn.out
        .send(&DataStructure::WireFormatInfo(server_wire_format_info()));
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    // `connection-ttl-check-interval`: pings and the TTL.
    let mut tick = tokio::time::interval(Duration::from_millis(2000));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = failed.failed() => break,
            event = events_rx.recv() => match event {
                Some(Event::Stop) | None => break,
                Some(Event::Acked { session, pending, result }) => conn.acked(&session, pending, result),
                Some(Event::Released) => conn.send_released().await,
                Some(Event::RestoreAutoRead) => conn.restore_auto_read(),
                Some(Event::CloseSession { name }) => conn.close_server_session(&name).await,
            },
            _ = tick.tick() => {
                if !conn.check_ttl() {
                    break;
                }
                conn.check_inactivity();
            },
            read = rd.read_buf(&mut buf), if conn.auto_read => match read {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    conn.data_received = true;
                    if !conn.process(&mut buf).await {
                        break;
                    }
                }
            },
        }
    }
    // Acknowledgements the queues answered are finished; any answering
    // later find the channel closed and finish on their own.
    events_rx.close();
    while let Ok(event) = events_rx.try_recv() {
        if let Event::Acked {
            session,
            pending,
            result,
        } = event
        {
            conn.acked(&session, pending, result);
        }
    }
    conn.fail().await;
    drop(conn);
    let _ = completer.await;
    let _ = writer.await;
}

/// `getLocalAddress`: `ip:port`, the address the client connected to.
fn local_address(c: &Accepted) -> String {
    let host = &c.acceptor.host;
    let unspecified = host.is_empty() || host == "0.0.0.0" || host == "::" || host == "[::]";
    let ip = match (unspecified, c.peer) {
        (true, Some(p)) => p.ip().to_string(),
        _ => host.clone(),
    };
    if ip.contains(':') && !ip.starts_with('[') {
        format!("[{ip}]:{}", c.acceptor.port)
    } else {
        format!("{ip}:{}", c.acceptor.port)
    }
}

/// `getPreferedWireFormatInfo` of `OpenWireFormatFactory` with the cache
/// off.
fn server_wire_format_info() -> WireFormatInfo {
    let mut props = PrimitiveMap::new();
    props.insert("StackTraceEnabled".into(), Primitive::Bool(true));
    props.insert("CacheEnabled".into(), Primitive::Bool(false));
    props.insert("TcpNoDelayEnabled".into(), Primitive::Bool(true));
    props.insert("TightEncodingEnabled".into(), Primitive::Bool(true));
    props.insert("SizePrefixDisabled".into(), Primitive::Bool(false));
    props.insert("MaxInactivityDuration".into(), Primitive::Long(30_000));
    props.insert(
        "MaxInactivityDurationInitalDelay".into(),
        Primitive::Long(10_000),
    );
    props.insert("CacheSize".into(), Primitive::Int(1024));
    props.insert("MaxFrameSize".into(), Primitive::Long(i64::MAX));
    props.insert("MaxFrameSizeEnabled".into(), Primitive::Bool(true));
    props.insert("ProviderName".into(), Primitive::String("ActiveMQ".into()));
    props.insert(
        "ProviderVersion".into(),
        Primitive::String("5.19.10".into()),
    );
    props.insert("PlatformDetails".into(), Primitive::String("Java".into()));
    let mut marshalled = Vec::new();
    let _ = primitive::write_map(&mut marshalled, Some(&props));
    WireFormatInfo {
        magic: b"ActiveMQ".to_vec(),
        version: crate::codec::DEFAULT_WIRE_VERSION as i32,
        marshalled_properties: Some(marshalled),
    }
}

/// `commandId` and `responseRequired` of a command; `None` for a data
/// structure that is not one.
fn command_header(ds: &DataStructure) -> Option<(i32, bool)> {
    macro_rules! header {
        ($($v:ident),*) => {
            match ds {
                $(DataStructure::$v(c) => Some((c.command_id, c.response_required)),)*
                DataStructure::WireFormatInfo(_) => Some((0, false)),
                DataStructure::PartialCommand(c) => Some((c.command_id, false)),
                DataStructure::LastPartialCommand(c) => Some((c.command_id, false)),
                _ => None,
            }
        };
    }
    header!(
        BrokerInfo,
        BrokerSubscriptionInfo,
        ConnectionControl,
        ConnectionError,
        ConnectionInfo,
        ConsumerControl,
        ConsumerInfo,
        ControlCommand,
        DataArrayResponse,
        DataResponse,
        DestinationInfo,
        ExceptionResponse,
        FlushCommand,
        IntegerResponse,
        KeepAliveInfo,
        Message,
        MessageAck,
        MessageDispatch,
        MessageDispatchNotification,
        MessagePull,
        ProducerAck,
        ProducerInfo,
        RemoveInfo,
        RemoveSubscriptionInfo,
        ReplayCommand,
        Response,
        SessionInfo,
        ShutdownInfo,
        TransactionInfo
    )
}

/// What handling a command asks of `act` besides its reply.
enum Handled {
    /// A reply (or none, then a `Response` if one was asked for).
    Reply(Option<Box<DataStructure>>),
    /// An `ExceptionResponse` the handler returned rather than threw.
    Returned(OwError),
    /// The reply is already on its way.
    Sent,
    /// Close the connection now (`shutdown(true)`).
    Close,
}

impl Connection {
    // ------------------------------------------------------------ input

    /// Handles every complete frame in `buf`. False when the connection
    /// must close.
    async fn process(&mut self, buf: &mut Vec<u8>) -> bool {
        let mut at = 0;
        let mut keep_going = true;
        while buf.len() - at >= 4 {
            let size = i32::from_be_bytes(buf[at..at + 4].try_into().expect("4 bytes"));
            if size < 0 {
                keep_going = false;
                break;
            }
            let end = at + 4 + size as usize;
            if buf.len() < end {
                break;
            }
            let frame = buf[at..end].to_vec();
            at = end;
            match self.in_wf.unmarshal(&frame) {
                Ok(Some(command)) => {
                    if !self.act(command).await {
                        keep_going = false;
                        break;
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    self.send_exception(0, &OwError::io(e.to_string()));
                }
            }
        }
        buf.drain(..at);
        keep_going
    }

    /// `act`. False when the connection must close.
    async fn act(&mut self, command: DataStructure) -> bool {
        let Some((command_id, response_required)) = command_header(&command) else {
            self.send_exception(
                0,
                &OwError::new(
                    "java.lang.ClassCastException",
                    format!(
                        "class org.apache.activemq.command.{} cannot be cast to class org.apache.activemq.command.Command",
                        command.class_name()
                    ),
                ),
            );
            return true;
        };
        // The pre-authentication gate.
        if self.info.is_none()
            && !matches!(
                command,
                DataStructure::WireFormatInfo(_)
                    | DataStructure::ConnectionControl(_)
                    | DataStructure::ConnectionInfo(_)
            )
        {
            return false;
        }
        if matches!(command, DataStructure::KeepAliveInfo(_)) {
            return true;
        }
        // What was released before this command came is sent before it.
        self.send_released().await;
        let is_connection_info = matches!(command, DataStructure::ConnectionInfo(_));
        let class = command.class_name();
        // setLastCommand
        self.handling = Some(command_id);
        let visited = self.visit(command, command_id, response_required).await;
        self.handling = None;
        let handled = match visited {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!("Errors occurred during the buffering operation of {class}: {e:?}");
                if response_required {
                    Handled::Returned(e)
                } else {
                    Handled::Reply(None)
                }
            }
        };
        let mut reply = match handled {
            Handled::Close => return false,
            Handled::Sent => {
                self.reply(None, None);
                return true;
            }
            Handled::Reply(r) => r.map(|b| *b),
            Handled::Returned(e) => {
                if (is_connection_info || self.info.is_none()) && !self.destroyed {
                    self.delayed_stop();
                }
                if response_required {
                    Some(exception_response(command_id, &e))
                } else {
                    // serviceException
                    self.out
                        .send(&DataStructure::ConnectionError(ConnectionError {
                            exception: Some(Box::new(e.throwable())),
                            ..Default::default()
                        }));
                    None
                }
            }
        };
        if response_required && reply.is_none() {
            reply = Some(response(command_id));
        }
        if self.dont_send_response {
            self.dont_send_response = false;
            reply = None;
        }
        if let Some(mut r) = reply {
            // sendAsyncResponse: the response answers this command.
            match &mut r {
                DataStructure::Response(x) => x.correlation_id = command_id,
                DataStructure::IntegerResponse(x) => x.correlation_id = command_id,
                DataStructure::DataArrayResponse(x) => x.correlation_id = command_id,
                DataStructure::DataResponse(x) => x.correlation_id = command_id,
                DataStructure::ExceptionResponse(x) => x.correlation_id = command_id,
                _ => {}
            }
            self.reply(Some(r), None);
        } else {
            // Routed messages join their queues once journaled, reply or
            // not.
            self.reply(None, None);
        }
        true
    }

    /// The journal writes every session lined up so far.
    fn completion(&mut self) -> Completion {
        let mut completion = Completion::default();
        if let Some(s) = self.internal.as_mut() {
            completion.merge(s.take_completion());
        }
        for s in self.sessions.values_mut() {
            completion.merge(s.session.take_completion());
        }
        completion
    }

    /// Sends a reply once the writes before it are done.
    fn reply(&mut self, command: Option<DataStructure>, after: Option<Box<dyn FnOnce() + Send>>) {
        let completion = self.completion();
        if command.is_none() && after.is_none() && completion.is_empty() {
            return;
        }
        self.queue_reply(Reply {
            completion,
            command,
            after,
        });
    }

    fn queue_reply(&mut self, reply: Reply) {
        let _ = self.replies.send(reply);
    }

    /// `sendException`: an `ExceptionResponse` now.
    fn send_exception(&self, correlation_id: i32, e: &OwError) {
        self.out.send(&exception_response(correlation_id, e));
    }

    /// `delayedStop`: the connection fails two seconds later.
    fn delayed_stop(&self) {
        let events = self.events.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(2000)).await;
            let _ = events.send(Event::Stop);
        });
    }

    async fn visit(&mut self, command: DataStructure, id: i32, rr: bool) -> OwResult<Handled> {
        let not_implemented = || OwError::jms_ise("not implemented! ");
        Ok(match command {
            DataStructure::WireFormatInfo(info) => {
                self.process_wire_format(&info);
                Handled::Reply(None)
            }
            DataStructure::ConnectionInfo(info) => self.process_add_connection(info).await,
            DataStructure::SessionInfo(info) => {
                self.process_add_session(&info).await;
                Handled::Reply(None)
            }
            DataStructure::ConsumerInfo(info) => {
                self.process_add_consumer(info).await?;
                Handled::Reply(None)
            }
            DataStructure::ProducerInfo(info) => {
                self.process_add_producer(info).await?;
                Handled::Reply(None)
            }
            DataStructure::TransactionInfo(info) => self.process_transaction(info).await?,
            DataStructure::DestinationInfo(info) => match info.operation_type {
                0 => match self.add_destination(&info).await {
                    Ok(()) => Handled::Reply(None),
                    Err(e) => Handled::Returned(e),
                },
                1 => {
                    self.remove_destination(&info).await?;
                    Handled::Reply(None)
                }
                t => return Err(OwError::io(format!("Unknown operation type: {t}"))),
            },
            DataStructure::RemoveSubscriptionInfo(info) => {
                self.process_remove_subscription(&info).await?;
                Handled::Reply(None)
            }
            DataStructure::Message(msg) => {
                self.process_message(msg, id, rr).await?;
                Handled::Reply(None)
            }
            DataStructure::MessageAck(ack) => {
                self.process_message_ack(&ack).await?;
                Handled::Reply(None)
            }
            DataStructure::RemoveInfo(info) => {
                let last = info.last_delivered_sequence_id;
                match info.object_id.map(|b| *b) {
                    Some(DataStructure::ConnectionId(_)) => {
                        self.process_remove_connection(last).await
                    }
                    Some(DataStructure::SessionId(s)) => {
                        self.process_remove_session(&s, last).await
                    }
                    Some(DataStructure::ConsumerId(c)) => {
                        self.process_remove_consumer(&c, last).await?
                    }
                    Some(DataStructure::ProducerId(p)) => self.process_remove_producer(&p)?,
                    Some(other) => {
                        return Err(OwError::io(format!(
                            "Unknown remove command type: {}",
                            other.data_type()
                        )));
                    }
                    None => return Err(OwError::npe()),
                }
                Handled::Reply(None)
            }
            DataStructure::MessagePull(pull) => {
                self.process_message_pull(&pull)?;
                Handled::Reply(None)
            }
            DataStructure::ShutdownInfo(_) => Handled::Close,
            DataStructure::ConsumerControl(cc) => {
                if let Some(e) = cc.consumer_id.as_ref().and_then(|c| self.exchanges.get(c)) {
                    for m in &e.members {
                        m.set_prefetch(cc.prefetch);
                    }
                }
                Handled::Reply(None)
            }
            DataStructure::BrokerInfo(_)
            | DataStructure::FlushCommand(_)
            | DataStructure::ConnectionError(_)
            | DataStructure::ControlCommand(_) => return Err(not_implemented()),
            DataStructure::PartialCommand(_) | DataStructure::LastPartialCommand(_) => {
                return Err(OwError::java_ise(
                    "The transport layer should filter out PartialCommand instances but received: PartialCommand",
                ));
            }
            _ => Handled::Reply(None),
        })
    }

    // ------------------------------------------------------------ wire format and keep-alive

    /// `processWireFormat`: both wire formats take what the client
    /// supports, then the broker introduces itself.
    fn process_wire_format(&mut self, info: &WireFormatInfo) {
        let props = info
            .marshalled_properties
            .as_deref()
            .and_then(|b| primitive::read_map(&mut Input::new(b)).ok().flatten())
            .unwrap_or_default();
        let flag = |k: &str| matches!(props.get(k), Some(Primitive::Bool(true)));
        let long = |k: &str| match props.get(k) {
            Some(Primitive::Long(v)) => *v,
            Some(Primitive::Int(v)) => i64::from(*v),
            _ => 0,
        };
        let version = amq_min(12, i64::from(info.version)) as u32;
        let apply = |wf: &mut WireFormat| {
            wf.version = version;
            wf.stack_trace_enabled = flag("StackTraceEnabled");
            wf.tcp_no_delay_enabled = flag("TcpNoDelayEnabled");
            wf.set_cache_enabled(false);
            wf.tight_encoding_enabled = flag("TightEncodingEnabled");
            wf.size_prefix_disabled = false;
            wf.max_frame_size = amq_min(i64::MAX, long("MaxFrameSize"));
        };
        apply(&mut self.in_wf);
        apply(&mut lock(&self.out.wf));
        // sendBrokerInfo
        self.out.send(&DataStructure::BrokerInfo(BrokerInfo {
            broker_id: Some(BrokerId {
                value: Some(self.broker.node_id().to_owned()),
            }),
            broker_name: Some(self.manager.broker_name.clone()),
            broker_url: Some(self.local.clone()),
            ..Default::default()
        }));
        // configureInactivityParams
        let s = &self.manager.settings;
        let duration = long("MaxInactivityDuration").min(s.max_inactivity_duration);
        let initial =
            long("MaxInactivityDurationInitalDelay").min(s.max_inactivity_duration_initial_delay);
        self.use_keep_alive = duration != 0 && s.use_keep_alive;
        self.max_inactivity = duration;
        if self.use_keep_alive {
            if duration >= 0 {
                self.ttl = Some(duration);
                self.ttl_from = Some(Instant::now() + Duration::from_millis(initial.max(0) as u64));
            }
            self.check_inactivity();
        }
    }

    /// `checkInactivity`: a ping when nothing went out for half the
    /// inactivity duration.
    fn check_inactivity(&self) {
        if !self.use_keep_alive || self.destroyed {
            return;
        }
        let now = saranga_server::message::now_millis();
        if now - self.out.last_sent.load(Ordering::SeqCst) >= self.max_inactivity / 2 {
            self.out
                .send_command(&DataStructure::KeepAliveInfo(KeepAliveInfo::default()));
        }
    }

    /// The failure check of the connection TTL. False when it passed with
    /// nothing from the client.
    fn check_ttl(&mut self) -> bool {
        let (Some(ttl), Some(from)) = (self.ttl, self.ttl_from) else {
            return true;
        };
        if self.ttl_disabled {
            return true;
        }
        let now = Instant::now();
        if now < from {
            self.data_received = false;
            self.last_check = now;
            return true;
        }
        if self.data_received {
            self.data_received = false;
            self.last_check = now;
            return true;
        }
        if now >= self.last_check + Duration::from_millis(ttl.max(0) as u64) {
            tracing::warn!(
                "AMQ222067: Connection failure has been detected: AMQ229014: Did not receive data from {} within the {ttl}ms connection TTL. The connection will now be closed. [code=CONNECTION_TIMEDOUT]",
                self.remote
            );
            return false;
        }
        true
    }

    // ------------------------------------------------------------ connections

    /// `processAddConnection`.
    async fn process_add_connection(&mut self, info: ConnectionInfo) -> Handled {
        match self.add_connection(info).await {
            Ok(()) => Handled::Reply(None),
            Err(e) => Handled::Returned(e),
        }
    }

    async fn add_connection(&mut self, mut info: ConnectionInfo) -> OwResult<()> {
        // validateUser
        let authenticated = match self.broker.security().cloned() {
            None => Authenticated::default(),
            Some(store) => {
                let security_info = self.client.security_info();
                let (user, password) = (info.user_name.clone(), info.password.clone());
                let result = tokio::task::spawn_blocking(move || {
                    store.authenticate(user.as_deref(), password.as_deref(), &security_info)
                })
                .await
                .unwrap_or_else(|e| Err(SecurityError(format!("login failed: {e}"))));
                match result {
                    Ok(a) => a,
                    Err(_) => {
                        return Err(OwError::new(
                            "java.lang.SecurityException",
                            format!(
                                "User name [{}] or password is invalid.",
                                info.user_name.as_deref().unwrap_or("null")
                            ),
                        ));
                    }
                }
            }
        };
        self.destroyed = false;
        self.out.destroyed.store(false, Ordering::SeqCst);
        let Some(client_id) = info.client_id.clone() else {
            return Err(OwError::new(
                "javax.jms.InvalidClientIDException",
                "No clientID specified for connection request",
            ));
        };
        if let Err(from) = self.manager.claim_client_id(
            &client_id,
            self.key,
            &self.remote,
            info.failover_reconnect,
        ) {
            return Err(OwError::new(
                "javax.jms.InvalidClientIDException",
                format!(
                    "Broker: {} - Client: {client_id} already connected from {from}",
                    self.manager.broker_name
                ),
            ));
        }
        if info.client_ip.is_none() {
            info.client_ip = Some(self.remote.clone());
        }
        self.client.set_client_id(Some(client_id));
        self.authenticated = authenticated;
        self.internal = Some(self.new_session(uuid::Uuid::new_v4().to_string(), &info));
        self.info = Some(info.clone());
        self.manager
            .add_connection(self.key, self.temp_destinations.clone());
        // The connection advisory.
        let mut copy = info.clone();
        copy.password = Some(String::new());
        copy.failover_reconnect = false;
        self.fire_advisory(
            &Destination::topic(CONNECTION_ADVISORY),
            DataStructure::ConnectionInfo(copy),
            None,
        )
        .await;
        // The connection's own session.
        let own = SessionId {
            connection_id: info.connection_id.as_ref().and_then(|c| c.value.clone()),
            value: -1,
        };
        self.add_session(own).await;
        Ok(())
    }

    fn new_session(&self, name: String, info: &ConnectionInfo) -> ServerSession {
        let events = self.events.clone();
        let closed = name.clone();
        let session = ServerSession::new(
            self.broker.clone(),
            self.client.clone(),
            SessionOptions {
                name,
                username: info.user_name.clone(),
                password: info.password.clone(),
                xa: false,
                auto_commit_sends: true,
                auto_commit_acks: false,
                pre_acknowledge: false,
                default_address: None,
            },
            self.authenticated.clone(),
        );
        session.entry().set_closer(Box::new(move |_force| {
            let _ = events.send(Event::CloseSession {
                name: closed.clone(),
            });
        }));
        session
    }

    /// `ServerSessionImpl.close(failed = true)` for management: its
    /// transaction rolls back and its consumers close (their disconnect
    /// already failed the connection). `AMQSession.close` adds nothing, and
    /// the connection keeps the closed session, so a later consumer on it is
    /// refused (AMQ229232).
    async fn close_server_session(&mut self, name: &str) {
        for s in self.sessions.values_mut() {
            if s.session.name() == name {
                s.session.close(true).await;
            }
        }
    }

    fn connection_id(&self) -> Option<ConnectionId> {
        self.info.as_ref().and_then(|i| i.connection_id.clone())
    }

    fn client_id(&self) -> Option<String> {
        self.info.as_ref().and_then(|i| i.client_id.clone())
    }

    /// `processRemoveConnection`.
    async fn process_remove_connection(&mut self, last_delivered: i64) {
        for e in self.exchanges.values() {
            for m in &e.members {
                m.set_last_delivered_sequence(last_delivered);
            }
        }
        let client_id = self.client_id();
        self.manager
            .remove_connection(client_id.as_deref(), self.key);
        self.disconnect().await;
    }

    /// `disconnect`: the sessions close, then the temporary destinations
    /// go. Nothing is sent.
    async fn disconnect(&mut self) {
        if self.info.is_none() || self.destroyed {
            return;
        }
        let ids: Vec<SessionId> = self.sessions.keys().cloned().collect();
        for id in ids {
            self.close_session(&id).await;
        }
        self.remove_temp_resources().await;
        self.destroyed = true;
        self.out.destroyed.store(true, Ordering::SeqCst);
    }

    /// The transport closed or failed (`fail`, `doFail`).
    async fn fail(&mut self) {
        self.send_released().await;
        if self.info.is_some() {
            let client_id = self.client_id();
            self.manager
                .remove_connection(client_id.as_deref(), self.key);
        }
        // rollbackInProgressLocalTransactions
        let txs: Vec<TxEntry> = self.local_txs.drain().map(|(_, e)| e).collect();
        if let Some(s) = self.internal.as_mut() {
            for e in txs {
                let state = transaction::lock(&e.tx).state;
                if !matches!(
                    state,
                    TxState::Prepared | TxState::RolledBack | TxState::Committed
                ) {
                    let _ = s.rollback_tx(&e.tx);
                }
            }
        }
        self.disconnect().await;
        if let Some(mut s) = self.internal.take() {
            s.close(false).await;
            let completion = s.take_completion();
            let _ = completion.wait().await;
        }
        self.out.destroyed.store(true, Ordering::SeqCst);
        let tx = &self.out.tx;
        self.out.direct.queue(|| tx.send(Out::Close).is_ok());
    }

    /// The temporary destinations made through the connection's sessions
    /// (`TempResourceCleanerUpper`), each announced gone.
    async fn remove_temp_resources(&mut self) {
        // Subscription queues first, so that their addresses can go.
        self.client.remove_temp_queues(&self.broker);
        let resources = std::mem::take(&mut self.temp_resources);
        let post_office = self.broker.post_office().clone();
        for r in resources {
            if r.queue {
                let Some(q) = post_office.queue(&r.name) else {
                    continue;
                };
                if !q.config().temporary {
                    continue;
                }
                let address = q.config().address.clone();
                let auto_delete = post_office
                    .address_info(&address)
                    .is_none_or(|a| a.temporary || a.auto_created);
                if post_office
                    .delete_queue_awaited(&r.name, false, auto_delete)
                    .await
                    .is_ok()
                {
                    self.temp_destination_deleted(Destination::new(TEMP_QUEUE, r.name.clone()))
                        .await;
                }
            } else {
                if !post_office
                    .address_info(&r.name)
                    .is_some_and(|a| a.temporary)
                {
                    continue;
                }
                if post_office.remove_address(&r.name) && !r.name.starts_with(ADVISORY_TOPIC_PREFIX)
                {
                    self.temp_destination_deleted(Destination::new(TEMP_TOPIC, r.name.clone()))
                        .await;
                }
            }
        }
    }

    /// `tempDestinationDeleted`.
    async fn temp_destination_deleted(&mut self, d: Destination) {
        lock(&self.temp_destinations).retain(|i| {
            !i.destination
                .as_ref()
                .is_some_and(|x| same_destination(x, &d))
        });
        let info = DestinationInfo {
            connection_id: self.connection_id(),
            operation_type: 1,
            destination: Some(d.clone()),
            ..Default::default()
        };
        self.fire_advisory(
            &destination_advisory(&d),
            DataStructure::DestinationInfo(info),
            None,
        )
        .await;
    }

    // ------------------------------------------------------------ advisories

    /// `fireAdvisory`: routed only to an address that exists, with no
    /// security check.
    async fn fire_advisory(
        &mut self,
        topic: &Destination,
        data: DataStructure,
        original_connection_id: Option<String>,
    ) {
        if !self.manager.settings.support_advisory {
            return;
        }
        let name = topic.physical_name();
        if self.broker.post_office().address_info(&name).is_none() {
            return;
        }
        let origin = original_connection_id
            .unwrap_or_else(|| connection_id_string(self.connection_id().as_ref()));
        let mut props = PrimitiveMap::new();
        props.insert(CONNECTION_ID_PROPERTY.into(), Primitive::String(origin));
        props.insert(
            "originBrokerName".into(),
            Primitive::String(self.manager.broker_name.clone()),
        );
        props.insert(
            "originBrokerId".into(),
            Primitive::String(self.manager.advisory_broker_id.clone()),
        );
        props.insert(
            "originBrokerURL".into(),
            Primitive::String(self.local.clone()),
        );
        let mut marshalled = Vec::new();
        if primitive::write_map(&mut marshalled, Some(&props)).is_err() {
            return;
        }
        let producer = self.manager.advisory_producer.clone();
        let now = saranga_server::message::now_millis();
        let msg = Message {
            data_type: 23,
            producer_id: Some(producer.clone()),
            destination: Some(topic.clone()),
            message_id: Some(MessageId {
                producer_id: Some(producer),
                producer_sequence_id: self.manager.next_advisory_sequence(),
                ..Default::default()
            }),
            persistent: false,
            type_: Some("Advisory".into()),
            timestamp: now,
            marshalled_properties: Some(marshalled),
            data_structure: Some(Box::new(data)),
            ..Default::default()
        };
        let mut wf = WireFormat::new(crate::codec::DEFAULT_WIRE_VERSION);
        let mut core = match convert::inbound(&msg, &mut wf, now) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("error firing advisory on {name}: {e}");
                return;
            }
        };
        core.address = Some(name.as_str().into());
        set_routing_type(&mut core, RoutingType::Multicast);
        if let Some(s) = self.internal.as_mut()
            && let Err(e) = s.send_unchecked(core).await
        {
            tracing::warn!("error firing advisory on {name}: {}", e.message);
        }
    }

    // ------------------------------------------------------------ sessions

    /// `processAddSession`.
    async fn process_add_session(&mut self, info: &SessionInfo) {
        if let Some(id) = &info.session_id
            && !self.sessions.contains_key(id)
        {
            self.add_session(id.clone()).await;
        }
    }

    async fn add_session(&mut self, id: SessionId) {
        let Some(info) = self.info.clone() else {
            return;
        };
        let mut session = self.new_session(session_id_string(&id), &info);
        session.set_started(true).await;
        let internal = id.value == -1;
        self.sessions.insert(
            id,
            AmqSession {
                session,
                wf: WireFormat::new(crate::codec::DEFAULT_WIRE_VERSION),
                queue_cache: vec![None; self.manager.settings.destination_cache_size],
                next_consumer: 0,
                internal,
            },
        );
    }

    /// `processRemoveSession`.
    async fn process_remove_session(&mut self, id: &SessionId, last_delivered: i64) {
        for e in self.exchanges.values().filter(|e| &e.session == id) {
            for m in &e.members {
                m.set_last_delivered_sequence(last_delivered);
            }
        }
        self.close_session(id).await;
    }

    /// `AMQSession.close`: what its consumers were delivering goes back,
    /// counted as the client said.
    async fn close_session(&mut self, id: &SessionId) {
        let consumers: Vec<ConsumerId> = self
            .exchanges
            .iter()
            .filter(|(_, e)| &e.session == id)
            .map(|(c, _)| c.clone())
            .collect();
        for c in consumers {
            if let Some(e) = self.exchanges.remove(&c) {
                self.close_exchange(e).await;
            }
        }
        self.producers.retain(|p, _| &session_of_producer(p) != id);
        if let Some(mut s) = self.sessions.remove(id) {
            s.session.close(false).await;
            let c = s.session.take_completion();
            self.queue_reply(Reply {
                completion: c,
                command: None,
                after: None,
            });
        }
    }

    // ------------------------------------------------------------ producers and sends

    /// `processAddProducer`.
    async fn process_add_producer(&mut self, info: ProducerInfo) -> OwResult<()> {
        let Some(pid) = info.producer_id.clone() else {
            return Err(OwError::npe());
        };
        let sid = session_of_producer(&pid);
        if !self.sessions.contains_key(&sid) {
            return Err(OwError::jms_ise(format!(
                "Cannot add a producer to a session that had not been registered: {}",
                session_id_string(&sid)
            )));
        }
        if self.producers.contains_key(&pid) {
            return Ok(());
        }
        if let Some(d) = &info.destination {
            self.check_send_permission(&sid, d)?;
            if !is_advisory(d) {
                let add = DestinationInfo {
                    connection_id: self.connection_id(),
                    operation_type: 0,
                    destination: Some(d.clone()),
                    ..Default::default()
                };
                self.add_destination(&add).await?;
            }
        }
        let name = convert::producer_id_string(&pid);
        let address = info.destination.as_ref().map(|d| d.physical_name());
        if let Some(s) = self.sessions.get(&sid) {
            s.session
                .add_producer(&name, "OPENWIRE", address.as_deref());
        }
        self.producers.insert(pid, info);
        Ok(())
    }

    /// `checkDestinationForSendPermission`.
    fn check_send_permission(&self, sid: &SessionId, d: &Destination) -> OwResult<()> {
        if self.broker.security().is_none() {
            return Ok(());
        }
        let Some(s) = self.sessions.get(sid) else {
            return Err(OwError::npe());
        };
        let members = if d.is_composite() {
            d.composite_destinations()
        } else {
            vec![d.clone()]
        };
        for m in members {
            let name = m.physical_name();
            let (address, queue) = split_fqqn(&name);
            s.session.security_check(address, queue, CheckType::Send)?;
        }
        Ok(())
    }

    /// `processRemoveProducer`.
    fn process_remove_producer(&mut self, id: &ProducerId) -> OwResult<()> {
        self.producers.remove(id);
        if let Some(s) = self.sessions.get(&session_of_producer(id)) {
            s.session.remove_producer(&convert::producer_id_string(id));
        }
        if !self.sessions.contains_key(&session_of_producer(id)) {
            return Err(OwError::npe());
        }
        Ok(())
    }

    /// `lookupTX`: the client's transaction, its entry noting `session`.
    fn lookup_tx(
        &mut self,
        id: Option<&TransactionId>,
        session: Option<&SessionId>,
    ) -> Option<TxRef> {
        let entry = match id? {
            TransactionId::Local(l) => self.local_txs.get_mut(l)?,
            TransactionId::Xa(x) => {
                let tx = self.broker.resource_manager().get(&to_xid(x))?;
                self.xa_txs
                    .entry(x.clone())
                    .or_insert_with(|| TxEntry::new(tx.clone()))
            }
        };
        if let Some(s) = session {
            entry.session = Some(s.clone());
        }
        Some(entry.tx.clone())
    }

    fn tx_entry(&mut self, id: &TransactionId) -> Option<&mut TxEntry> {
        match id {
            TransactionId::Local(l) => self.local_txs.get_mut(l),
            TransactionId::Xa(x) => self.xa_txs.get_mut(x),
        }
    }

    /// `processMessage` and `AMQSession.send`.
    async fn process_message(&mut self, msg: Message, command_id: i32, rr: bool) -> OwResult<()> {
        let Some(pid) = msg.producer_id.clone() else {
            return Err(OwError::npe());
        };
        let Some(producer) = self.producers.get(&pid).cloned() else {
            return Err(OwError::npe());
        };
        let send_producer_ack = !rr && producer.window_size > 0;
        let sid = session_of_producer(&pid);
        let tx = self.lookup_tx(msg.transaction_id.as_ref(), Some(&sid));
        if let Some(t) = &msg.transaction_id
            && tx.is_none()
        {
            return Err(OwError::jms_ise(format!(
                "Transaction not started, {}",
                transaction_id_string(t)
            )));
        }
        let anonymous = producer.destination.is_none();
        match self
            .send(
                &producer,
                &msg,
                tx.clone(),
                command_id,
                rr,
                send_producer_ack,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                if let Some(tx) = &tx {
                    transaction::lock(tx).mark_rollback_only(&BrokerError::new(
                        ErrorType::GenericException,
                        e.message.clone().unwrap_or_default(),
                    ));
                } else if e.is_invalid_destination() && anonymous {
                    self.send_exception(command_id, &e);
                }
                Err(e)
            }
        }
    }

    async fn send(
        &mut self,
        producer: &ProducerInfo,
        msg: &Message,
        tx: Option<TxRef>,
        command_id: i32,
        rr: bool,
        send_producer_ack: bool,
    ) -> OwResult<()> {
        let pid = producer.producer_id.clone().expect("checked");
        let sid = session_of_producer(&pid);
        let Some(dest) = msg.destination.clone() else {
            return Err(OwError::npe());
        };
        if producer.destination.is_none() {
            self.check_send_permission(&sid, &dest)?;
        }
        let targets = if dest.is_composite() {
            dest.composite_destinations()
        } else {
            vec![dest.clone()]
        };
        let now = saranga_server::message::now_millis();
        let client_id = self.client_id();
        let fault_tolerant = self.info.as_ref().is_some_and(|i| i.fault_tolerant);
        let dup_detection = self.manager.settings.use_duplicate_detection_on_failover;
        let Some(session) = self.sessions.get_mut(&sid) else {
            return Err(OwError::npe());
        };
        let mut msg = msg.clone();
        msg.broker_in_time = now;
        if dest.is_composite() {
            msg.original_destination = Some(dest.clone());
        }
        let mut core = convert::inbound(&msg, &mut session.wf, now)?;
        core.properties.insert(
            CONNECTION_ID_PROPERTY.into(),
            match &client_id {
                Some(c) => PropertyValue::String(c.as_str().into()),
                None => PropertyValue::Null,
            },
        );
        if fault_tolerant
            && dup_detection
            && core.properties.get_str(DUPLICATE_ID_PROPERTY).is_none()
            && !producer
                .destination
                .as_ref()
                .is_some_and(Destination::is_temporary)
            && let Some(id) = &msg.message_id
        {
            core.properties.insert(
                DUPLICATE_ID_PROPERTY.into(),
                PropertyValue::String(convert::message_id_string(id).as_str().into()),
            );
        }
        let should_block = producer.window_size > 0 || rr;
        if should_block {
            self.dont_send_response = true;
        }
        let sender: Arc<str> = convert::producer_id_string(&pid).into();
        // The destinations still to take it (`count`), the last of which
        // answers the producer.
        let key = self.next_producer_send;
        self.next_producer_send += 1;
        if should_block {
            self.producer_sends.insert(
                key,
                ProducerSend {
                    remaining: targets.len(),
                    command_id,
                    ack: send_producer_ack.then(|| (pid.clone(), message_size(&msg) as i32)),
                },
            );
        }
        let last = targets.len() - 1;
        for (i, target) in targets.iter().enumerate() {
            let address = target.physical_name();
            let mut m = if i == last {
                std::mem::take(&mut core)
            } else {
                core.clone()
            };
            m.address = Some(address.as_str().into());
            if target.is_queue() {
                if let Err(e) =
                    self.check_cached_existing_queue(&sid, &address, target.is_temporary())
                {
                    self.producer_sends.remove(&key);
                    return Err(e);
                }
                set_routing_type(&mut m, RoutingType::Anycast);
            } else {
                set_routing_type(&mut m, RoutingType::Multicast);
            }
            let store = self.broker.page_store(&address);
            if should_block {
                // sendShouldBlockProducer
                let task = BlockedSend {
                    session: sid.clone(),
                    message: m,
                    tx: tx.clone(),
                    sender: sender.clone(),
                    send: key,
                };
                match store {
                    None => self.blocked_send(task).await,
                    Some(store) => {
                        let gate = Gate::default();
                        let slot = Arc::new(Mutex::new(Some(task)));
                        let grant = {
                            let slot = slot.clone();
                            let events = self.events.clone();
                            let released = self.released.clone();
                            let gate = gate.clone();
                            Box::new(move || {
                                let mut st = lock(&gate.0);
                                if matches!(*st, GateState::Checking) {
                                    *st = GateState::Now;
                                } else {
                                    drop(st);
                                    if let Some(t) = lock(&slot).take() {
                                        lock(&released).push_back(t);
                                        let _ = events.send(Event::Released);
                                    }
                                }
                            }) as saranga_server::paging::Grant
                        };
                        let list = self
                            .sessions
                            .get(&sid)
                            .map(|s| s.session.blocked_runnables().clone());
                        let granted = store.check_memory(false, grant, None, list.as_deref());
                        let now = gate.now();
                        if !granted {
                            self.dont_send_response = false;
                            self.ttl_disabled = false;
                            // Blocked through management, the message is
                            // still sent once unblocked, and answered.
                            if !store.is_blocked_via_management() {
                                self.producer_sends.remove(&key);
                            }
                            return Err(OwError::resource_allocation(format!(
                                "Queue is full {address}"
                            )));
                        }
                        let task = if now { lock(&slot).take() } else { None };
                        if let Some(t) = task {
                            self.blocked_send(t).await;
                        }
                    }
                }
            } else {
                if let Some(store) = store {
                    let gate = Gate::default();
                    let grant = gate.grant(self.events.clone(), Event::RestoreAutoRead);
                    let blocked = std::cell::Cell::new(false);
                    let list = self
                        .sessions
                        .get(&sid)
                        .map(|s| s.session.blocked_runnables().clone());
                    let granted = store.check_memory(
                        true,
                        grant,
                        Some(&|| blocked.set(true)),
                        list.as_deref(),
                    );
                    // blockConnection, then restoreAutoRead if granted at
                    // once.
                    if blocked.get() {
                        self.auto_read = false;
                        self.ttl_disabled = true;
                    }
                    if gate.now() || !granted {
                        self.restore_auto_read();
                    }
                    if !granted {
                        return Err(OwError::resource_allocation(format!(
                            "Queue is full {address}"
                        )));
                    }
                } else {
                    self.restore_auto_read();
                }
                let session = self.sessions.get_mut(&sid).expect("checked");
                session.session.set_sender(Some(sender.clone()));
                let result = session.session.send_in(m, None, tx.clone()).await;
                session.session.set_sender(None);
                result?;
            }
        }
        Ok(())
    }

    /// Sends what full addresses released, in the order released.
    async fn send_released(&mut self) {
        loop {
            let next = lock(&self.released).pop_front();
            let Some(t) = next else { break };
            self.blocked_send(t).await;
        }
    }

    /// `restoreAutoRead`: the socket is read again, and the TTL checked.
    fn restore_auto_read(&mut self) {
        if !self.auto_read {
            self.auto_read = true;
            self.ttl_disabled = false;
        }
    }

    /// `sendShouldBlockProducer`'s task: the message is sent, and the last
    /// of its destinations answers the producer, with this send's error if
    /// it failed, else a `ProducerAck` or `Response` once it is written.
    async fn blocked_send(&mut self, t: BlockedSend) {
        let result = match self.sessions.get_mut(&t.session) {
            Some(session) => {
                session.session.set_sender(Some(t.sender));
                let r = session.session.send_in(t.message, None, t.tx).await;
                session.session.set_sender(None);
                r.map_err(OwError::from)
            }
            None => Err(OwError::npe()),
        };
        self.ttl_disabled = false;
        let Some(ps) = self.producer_sends.get_mut(&t.send) else {
            return;
        };
        ps.remaining -= 1;
        if ps.remaining > 0 {
            return;
        }
        let ps = self.producer_sends.remove(&t.send).expect("present");
        match result {
            Err(e) => {
                self.dont_send_response = false;
                // sendException: the command being handled, if any.
                let correlation = self.handling.unwrap_or(0);
                self.send_exception(correlation, &e);
            }
            Ok(()) => {
                let reply = match ps.ack {
                    Some((producer_id, size)) => DataStructure::ProducerAck(ProducerAck {
                        producer_id: Some(producer_id),
                        size,
                        ..Default::default()
                    }),
                    None => response(ps.command_id),
                };
                self.reply(Some(reply), None);
            }
        }
    }

    /// `checkCachedExistingQueues`.
    fn check_cached_existing_queue(
        &mut self,
        sid: &SessionId,
        address: &str,
        temporary: bool,
    ) -> OwResult<()> {
        let Some(s) = self.sessions.get(sid) else {
            return Err(OwError::npe());
        };
        let size = s.queue_cache.len();
        let index = (java_hash(address) as u32 as usize) & (size.next_power_of_two() - 1);
        let index = index.min(size - 1);
        if s.queue_cache[index].as_deref() == Some(address) {
            return Ok(());
        }
        self.check_auto_create(sid, address, temporary, None, true)?;
        if let Some(s) = self.sessions.get_mut(sid) {
            s.queue_cache[index] = Some(address.to_owned());
        }
        Ok(())
    }

    /// `AMQSession.checkAutoCreate`.
    fn check_auto_create(
        &mut self,
        sid: &SessionId,
        name: &str,
        temporary: bool,
        filter: Option<String>,
        is_queue: bool,
    ) -> OwResult<()> {
        let post_office = self.broker.post_office();
        let (address, fqqn_queue) = split_fqqn(name);
        let routing_type = if fqqn_queue.is_some() {
            match post_office.address_info(address) {
                Some(a) if a.routing_types.multicast => RoutingType::Multicast,
                Some(_) => RoutingType::Anycast,
                None => post_office.settings(address).default_address_routing_type,
            }
        } else if is_queue {
            RoutingType::Anycast
        } else {
            RoutingType::Multicast
        };
        let Some(s) = self.sessions.get(sid) else {
            return Err(OwError::npe());
        };
        let result = s.session.check_auto_create_with(
            name,
            Some(routing_type),
            &AutoCreateOptions {
                temporary,
                durable: None,
                filter,
            },
        )?;
        match result {
            AutoCreate::NotFound => Err(OwError::invalid_destination(format!(
                "Destination '{name}' doesn't exist and auto-creation is disabled."
            ))),
            AutoCreate::Created if temporary => {
                let queue = routing_type == RoutingType::Anycast || fqqn_queue.is_some();
                self.temp_resources.push(TempResource {
                    name: if queue {
                        fqqn_queue.unwrap_or(address).to_owned()
                    } else {
                        address.to_owned()
                    },
                    queue,
                });
                Ok(())
            }
            _ => Ok(()),
        }
    }

    // ------------------------------------------------------------ destinations

    /// `addDestination`.
    async fn add_destination(&mut self, info: &DestinationInfo) -> OwResult<()> {
        let Some(dest) = info.destination.clone() else {
            return Err(OwError::npe());
        };
        let advisory = is_advisory(&dest);
        if !self.manager.settings.support_advisory && advisory {
            return Ok(());
        }
        let name = dest.physical_name();
        let temporary = dest.is_temporary();
        let routing_type = if dest.is_queue() {
            RoutingType::Anycast
        } else {
            RoutingType::Multicast
        };
        let Some(internal) = self.internal.as_ref() else {
            return Err(OwError::npe());
        };
        let result = internal.check_auto_create_with(
            &name,
            Some(routing_type),
            &AutoCreateOptions {
                temporary,
                durable: Some(!temporary),
                filter: None,
            },
        )?;
        let created = match result {
            AutoCreate::Created => true,
            AutoCreate::NotFound => {
                return Err(OwError::invalid_destination(format!(
                    "{} {name} does not exist.",
                    type_string(&dest)
                )));
            }
            _ => false,
        };
        if created && advisory && self.manager.settings.suppress_internal_management_objects {
            self.broker.post_office().set_address_internal(&name);
        }
        if created && temporary {
            self.temp_resources.push(TempResource {
                name: name.clone(),
                queue: dest.is_queue(),
            });
        }
        if temporary {
            let mut list = lock(&self.temp_destinations);
            if !list.iter().any(|i| {
                i.destination
                    .as_ref()
                    .is_some_and(|d| d.physical_name() == name)
            }) {
                list.push(info.clone());
            }
        }
        if created && !advisory {
            let added = DestinationInfo {
                connection_id: self.connection_id(),
                operation_type: 0,
                destination: Some(dest.clone()),
                ..Default::default()
            };
            self.fire_advisory(
                &destination_advisory(&dest),
                DataStructure::DestinationInfo(added),
                None,
            )
            .await;
        }
        Ok(())
    }

    /// `removeDestination`.
    async fn remove_destination(&mut self, info: &DestinationInfo) -> OwResult<()> {
        let Some(dest) = info.destination.clone() else {
            return Err(OwError::npe());
        };
        let name = dest.physical_name();
        if !dest.is_temporary() {
            tracing::warn!("OpenWire client sending a queue remove towards {name}");
        }
        let post_office = self.broker.post_office().clone();
        let Some(internal) = self.internal.as_ref() else {
            return Err(OwError::npe());
        };
        if dest.is_queue() {
            if let Some(q) = post_office.queue(&name) {
                let check = if q.is_durable() {
                    CheckType::DeleteDurableQueue
                } else {
                    CheckType::DeleteNonDurableQueue
                };
                internal.security_check(&q.config().address, Some(&name), check)?;
                match post_office.delete_queue(&name, true, false) {
                    Ok(()) => self.client.remove_temp_queue(&name),
                    Err(e) if e.kind == ErrorType::QueueDoesNotExist => {}
                    Err(e) => return Err(e.into()),
                }
            }
        } else if post_office.address_info(&name).is_some() {
            internal.security_check(&name, None, CheckType::DeleteAddress)?;
            for q in post_office.queues() {
                if *q.config().address == *name {
                    let _ = post_office.delete_queue(q.name(), false, false);
                    self.client.remove_temp_queue(q.name());
                }
            }
            post_office.remove_address(&name);
        }
        if !is_advisory(&dest) {
            let removed = DestinationInfo {
                connection_id: self.connection_id(),
                operation_type: 1,
                destination: Some(dest.clone()),
                ..Default::default()
            };
            self.fire_advisory(
                &destination_advisory(&dest),
                DataStructure::DestinationInfo(removed),
                None,
            )
            .await;
        }
        Ok(())
    }

    // ------------------------------------------------------------ consumers

    /// `convertWildcard`.
    fn convert_wildcard(&self, d: &Destination) -> String {
        let name = d.physical_name();
        if d.is_temporary() || is_advisory(d) {
            return name;
        }
        let openwire = saranga_config::WildcardConfiguration {
            routing_enabled: true,
            delimiter: '.',
            any_words: '>',
            single_word: '*',
        };
        let w = self.broker.post_office().wildcard();
        let target = saranga_config::WildcardConfiguration {
            routing_enabled: w.routing_enabled,
            delimiter: w.delimiter,
            any_words: w.any_words,
            single_word: w.single_word,
        };
        openwire.convert(&name, &target)
    }

    /// `processAddConsumer` and `AMQSession.createConsumer`.
    async fn process_add_consumer(&mut self, info: ConsumerInfo) -> OwResult<()> {
        let Some(cid) = info.consumer_id.clone() else {
            return Err(OwError::npe());
        };
        let sid = session_of_consumer(&cid);
        if !self.sessions.contains_key(&sid) {
            return Err(OwError::jms_ise(format!(
                "ActiveMQServerImpl::name=localhost Cannot add a consumer to a session that had not been registered: {}",
                session_id_string(&sid)
            )));
        }
        if self.exchanges.contains_key(&cid) {
            return Ok(());
        }
        let Some(dest) = info.destination.clone() else {
            return Err(OwError::npe());
        };
        let members = if dest.is_composite() {
            dest.composite_destinations()
        } else {
            vec![dest.clone()]
        };
        let advisory = is_advisory(&dest);
        let mut created = Vec::new();
        let mut browsers = Vec::new();
        for member in members {
            let mut internal_address = false;
            if advisory {
                if !self.manager.settings.support_advisory {
                    continue;
                }
                internal_address = self.manager.settings.suppress_internal_management_objects;
            }
            let mut member = member;
            let mut selector_aware = false;
            if member.is_queue()
                && let Some((fqqn, aware)) =
                    self.manager.virtual_topic_fqqn(&member.physical_name())
            {
                member = Destination::queue(fqqn);
                selector_aware = aware;
            }
            let queue_name = self.convert_wildcard(&member);
            let temporary = member.is_temporary() || is_advisory(&member);
            // extractFilterStringOrNull
            let filter = if member.is_topic() || selector_aware {
                info.selector.clone()
            } else {
                None
            };
            self.check_auto_create(&sid, &queue_name, temporary, filter, member.is_queue())?;
            let (consumer, filter) = self
                .create_member(&info, &sid, member, advisory, internal_address)
                .await?;
            if consumer.browser {
                browsers.push((consumer.clone(), filter));
            }
            created.push(consumer);
        }
        self.exchanges.insert(
            cid.clone(),
            Exchange {
                session: sid.clone(),
                members: created.clone(),
            },
        );
        if created.is_empty() {
            return Ok(());
        }
        for (c, filter) in browsers {
            self.browse(&c, filter.as_deref()).await?;
        }
        if advisory && is_temp_advisory(&dest) {
            for di in self.manager.temporary_destinations() {
                let Some(d) = di.destination.clone() else {
                    continue;
                };
                let origin = connection_id_string(di.connection_id.as_ref());
                self.fire_advisory(
                    &destination_advisory(&d),
                    DataStructure::DestinationInfo(di),
                    Some(origin),
                )
                .await;
            }
        }
        Ok(())
    }

    /// `AMQConsumer` construction and `init`.
    async fn create_member(
        &mut self,
        info: &ConsumerInfo,
        sid: &SessionId,
        member: Destination,
        advisory: bool,
        internal_address: bool,
    ) -> OwResult<(Arc<AmqConsumer>, Option<String>)> {
        let cid = info.consumer_id.clone().expect("checked");
        let mut selector = info.selector.as_deref().map(translate_selector);
        let mut pre_ack = false;
        if info.no_local {
            if !advisory {
                self.no_local.store(true, Ordering::SeqCst);
            } else {
                pre_ack = true;
            }
            let id = info
                .client_id
                .clone()
                .or_else(|| cid.connection_id.clone())
                .unwrap_or_else(|| "null".into());
            let no_local = format!("{CONNECTION_ID_PROPERTY}<>'{id}'");
            selector = Some(match &info.selector {
                None => no_local,
                Some(s) => format!("{s} AND {no_local}"),
            });
        }
        let destination_name = self.convert_wildcard(&member);
        let (queue_name, filter) = if member.is_topic() {
            let q = self
                .create_topic_subscription(
                    sid,
                    info,
                    &destination_name,
                    selector.clone(),
                    dest_is_composite(info),
                    internal_address,
                )
                .await?;
            let fqqn = split_fqqn(&destination_name).1.is_some();
            (q, if fqqn { selector } else { None })
        } else {
            (destination_name, selector)
        };
        let session = self.sessions.get_mut(sid).expect("checked");
        let core_id = session.next_consumer;
        session.next_consumer += 1;
        let consumer = AmqConsumer::new(NewConsumer {
            id: cid,
            destination: member.clone(),
            core_id,
            prefetch: info.prefetch_size,
            browser: info.browser,
            pre_ack,
            strip_connection_id: session.internal && advisory,
            out: self.out.clone(),
            no_local: self.no_local.clone(),
            node_id: self.broker.node_id().to_owned(),
            client: self.client.clone(),
        });
        if info.browser {
            // A browser reads a snapshot of the queue.
            let post_office = self.broker.post_office();
            let name = saranga_server::postoffice::queue_name_of(&queue_name);
            let queue = post_office
                .queue(name)
                .ok_or_else(|| BrokerError::no_such_queue(name))?;
            session.session.security_check(
                &queue.config().address,
                Some(name),
                CheckType::Browse,
            )?;
            consumer.set_queue(queue);
            return Ok((consumer, filter));
        }
        let sink = consumer.sink();
        session.session.set_pre_acknowledge(pre_ack);
        let result = session
            .session
            .create_consumer(
                core_id,
                &queue_name,
                filter,
                i32::from(info.priority),
                false,
                move |_| sink,
            )
            .await;
        session.session.set_pre_acknowledge(false);
        result?;
        // credits = -1: only the prefetch window paces it.
        session.session.consumer_credits(core_id, -1);
        if let Some(q) = session.session.consumer_queue(core_id) {
            consumer.set_queue(q.clone());
        }
        Ok((consumer, None))
    }

    /// A browser's messages, then the end of the browse.
    async fn browse(&mut self, c: &Arc<AmqConsumer>, selector: Option<&str>) -> OwResult<()> {
        let Some(queue) = c.queue().cloned() else {
            return Ok(());
        };
        let filter = self.broker.post_office().filters().create(selector)?;
        for r in queue.browse().await {
            if filter.as_ref().is_none_or(|f| f.matches(&*r.message)) {
                c.deliver_browsed(&r.message, r.delivery_count);
            }
        }
        c.browse_finished();
        Ok(())
    }

    /// `createTopicSubscription`.
    async fn create_topic_subscription(
        &mut self,
        sid: &SessionId,
        info: &ConsumerInfo,
        address: &str,
        selector: Option<String>,
        composite: bool,
        internal: bool,
    ) -> OwResult<String> {
        let session = self.sessions.get_mut(sid).expect("checked");
        if info.subscription_name.is_some() {
            let mut name = subscription_queue_name(
                info.client_id.as_deref(),
                info.subscription_name.as_deref(),
            );
            if composite {
                name.push_str(address);
            }
            let mut spec = QueueSpec::new(name.as_str(), split_fqqn(address).0);
            spec.routing_type = Some(RoutingType::Multicast);
            spec.filter_string = selector.clone();
            spec.internal = internal;
            let existing = session.session.queue_query(&name);
            if existing.exists {
                if existing.consumer_count > 0 {
                    return Err(OwError::java_ise(
                        "Cannot create a subscriber on the durable subscription since it already has subscriber(s)",
                    ));
                }
                let configuration_managed = self
                    .broker
                    .post_office()
                    .queue(&name)
                    .is_some_and(|q| q.config().configuration_managed);
                let changed =
                    existing.filter_string != selector || *existing.address != *spec.address;
                if changed && !configuration_managed {
                    session.session.delete_queue(&name).await?;
                    session.session.create_queue(spec)?;
                }
            } else {
                session.session.create_queue(spec)?;
            }
            return Ok(name);
        }
        let (bare, fqqn_queue) = split_fqqn(address);
        if let Some(q) = fqqn_queue
            && self.broker.post_office().queue(q).is_some()
        {
            return Ok(q.to_owned());
        }
        let name = match fqqn_queue {
            Some(q) => q.to_owned(),
            None => uuid::Uuid::new_v4().to_string(),
        };
        let mut spec = QueueSpec::new(name.as_str(), bare);
        spec.routing_type = Some(RoutingType::Multicast);
        spec.filter_string = selector;
        spec.durable = false;
        spec.temporary = true;
        spec.internal = internal;
        session.session.create_queue(spec)?;
        // Advisory queues cannot be paged: `AMQConsumer` enforces DROP on
        // their store.
        if bare.starts_with(ADVISORY_TOPIC_PREFIX) {
            self.broker
                .post_office()
                .paging_store(bare)
                .enforce_policy(AddressFullPolicy::Drop);
        }
        Ok(name)
    }

    /// `processRemoveConsumer`.
    async fn process_remove_consumer(
        &mut self,
        id: &ConsumerId,
        last_delivered: i64,
    ) -> OwResult<()> {
        if self.destroyed {
            return Ok(());
        }
        let sid = session_of_consumer(id);
        if !self.sessions.contains_key(&sid) {
            return Err(OwError::jms_ise(format!(
                "Cannot remove a consumer from a session that had not been registered: {}",
                session_id_string(&sid)
            )));
        }
        let Some(e) = self.exchanges.remove(id) else {
            return Err(OwError::jms_ise(format!(
                "Cannot remove a consumer that had not been registered: {}",
                consumer_id_string(id)
            )));
        };
        for m in &e.members {
            m.set_last_delivered_sequence(last_delivered);
        }
        self.close_exchange(e).await;
        Ok(())
    }

    /// `removeConsumer`: each member's deliveries go back to its queue,
    /// counted or not as `lastDeliveredSequenceId` says.
    async fn close_exchange(&mut self, e: Exchange) {
        let Some(session) = self.sessions.get_mut(&e.session) else {
            return;
        };
        for m in &e.members {
            for (message, counts) in m.close() {
                session.session.cancel_one(m.core_id, message, counts);
            }
            if !m.browser {
                session.session.close_consumer(m.core_id, false).await;
            }
        }
    }

    /// `processMessagePull`.
    fn process_message_pull(&mut self, pull: &MessagePull) -> OwResult<()> {
        let Some(e) = pull
            .consumer_id
            .as_ref()
            .and_then(|c| self.exchanges.get(c))
        else {
            return Err(OwError::jms_ise("Consumer does not exist"));
        };
        let Some(m) = e.member(pull.destination.as_ref()).cloned() else {
            return Ok(());
        };
        let session = e.session.clone();
        if let Some(sequence) = m.pull(pull.timeout)
            && let Some(s) = self.sessions.get(&session)
        {
            s.session.force_delivery(m.core_id, sequence);
        }
        Ok(())
    }

    /// `processMessageAck`: its failures are never reported, but make a
    /// transaction roll back.
    async fn process_message_ack(&mut self, ack: &MessageAck) -> OwResult<()> {
        let Some(cid) = ack.consumer_id.clone() else {
            return Err(OwError::npe());
        };
        let sid = session_of_consumer(&cid);
        let tx = self.lookup_tx(ack.transaction_id.as_ref(), Some(&sid));
        if let Some(t) = &ack.transaction_id
            && tx.is_none()
        {
            return Err(OwError::jms_ise(format!(
                "Transaction not started, {}",
                transaction_id_string(t)
            )));
        }
        if !self.sessions.contains_key(&sid) {
            return Err(OwError::npe());
        }
        if let Err(e) = self.acknowledge(ack, &sid, tx.clone()).await
            && let Some(tx) = &tx
        {
            transaction::lock(tx).mark_rollback_only(&BrokerError::new(
                ErrorType::GenericException,
                e.message.unwrap_or_default(),
            ));
        }
        Ok(())
    }

    /// The rest of an acknowledgement made outside a transaction, once the
    /// queue answered: its journal writes go with the next reply.
    fn acked(&mut self, sid: &SessionId, pending: AckPending, result: AckResult) {
        match self.sessions.get_mut(sid) {
            Some(s) => {
                if let Err(e) = s.session.acknowledged(pending, result) {
                    tracing::debug!("{}", e.message);
                }
            }
            None => pending.finish_alone(result),
        }
        self.reply(None, None);
    }

    /// `AMQConsumer.acknowledge`.
    async fn acknowledge(
        &mut self,
        ack: &MessageAck,
        sid: &SessionId,
        tx: Option<TxRef>,
    ) -> OwResult<()> {
        let cid = ack.consumer_id.clone().expect("checked");
        let Some(m) = self
            .exchanges
            .get(&cid)
            .ok_or_else(OwError::npe)?
            .member(ack.destination.as_ref())
            .cloned()
        else {
            return Ok(());
        };
        let count = ack.message_count;
        match ack.ack_type {
            3 => return Ok(()),
            0 => {
                m.replenish(count, true);
                return Ok(());
            }
            _ => {}
        }
        let Some(last) = ack.last_message_id.as_ref() else {
            return Err(OwError::npe());
        };
        let first = ack.first_message_id.as_ref().unwrap_or(last);
        let remove = !m.browser && !m.non_destructive();
        let acked = m.take_range(first, last, remove);
        if acked.is_empty() && remove && !m.temporary_queue() {
            return Ok(());
        }
        if !remove {
            m.replenish(count, false);
            return Ok(());
        }
        let session = self.sessions.get_mut(sid).ok_or_else(OwError::npe)?;
        match ack.ack_type {
            6 => {
                for d in &acked {
                    let _ = session.session.expire(m.core_id, d.core);
                    m.replenish(1, false);
                }
                return Ok(());
            }
            2 | 4 if tx.is_none() => {
                // Outside a transaction nothing waits for the queue's
                // answer (its failure only marks a transaction): each
                // acknowledgement is on its way at once, and the next
                // delivery's credit need not wait for it.
                for d in &acked {
                    let events = self.events.clone();
                    let session_id = sid.clone();
                    let sent = session.session.acknowledge_then(
                        m.core_id,
                        d.core,
                        true,
                        move |pending, result| {
                            let acked = Event::Acked {
                                session: session_id,
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
                    );
                    if let Err(e) = sent {
                        tracing::debug!("{}", e.message);
                    }
                }
            }
            2 | 4 => {
                let mut failure = None;
                for d in &acked {
                    if let Err(e) = session
                        .session
                        .acknowledge_in(m.core_id, d.core, true, tx.clone())
                        .await
                    {
                        failure.get_or_insert(e);
                    }
                }
                if let Some(e) = failure {
                    return Err(e.into());
                }
            }
            1 => {
                let cause = ack.poison_cause.as_deref().map(|t| {
                    let class = t.class.as_deref().unwrap_or("java.lang.Throwable");
                    match &t.message {
                        Some(msg) => format!("{class}: {msg}"),
                        None => class.to_owned(),
                    }
                });
                for d in &acked {
                    let props = cause
                        .iter()
                        .map(|c| {
                            (
                                convert::AMQ_MSG_DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY.to_owned(),
                                PropertyValue::String(c.as_str().into()),
                            )
                        })
                        .collect();
                    session.session.reject_with(m.core_id, d.core, props);
                }
            }
            _ => {}
        }
        match (&ack.transaction_id, tx) {
            (Some(t), Some(_)) => {
                let t = t.clone();
                if let Some(entry) = self.tx_entry(&t) {
                    entry.credits.push((m.clone(), count));
                    if matches!(ack.ack_type, 2 | 4) {
                        entry.acks.extend(acked.into_iter().map(|d| TxAck {
                            session: sid.clone(),
                            consumer: m.clone(),
                            delivered: d,
                        }));
                    }
                }
            }
            _ => m.replenish(count, false),
        }
        Ok(())
    }

    /// `processRemoveSubscription`.
    async fn process_remove_subscription(&mut self, info: &RemoveSubscriptionInfo) -> OwResult<()> {
        let invalid =
            || OwError::jms_ise("Rejecting remove subscription command with invalid data");
        let Some(state) = &self.info else {
            return Err(OwError::jms_ise(
                "Cannot remove a subscription on a connection that had not been registered",
            ));
        };
        if state.connection_id != info.connection_id || state.client_id != info.client_id {
            return Err(invalid());
        }
        let name =
            subscription_queue_name(info.client_id.as_deref(), info.subscription_name.as_deref());
        let internal = self.internal.as_mut().ok_or_else(OwError::npe)?;
        match internal.delete_queue(&name).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind == ErrorType::QueueDoesNotExist => Err(OwError {
                class: "javax.jms.InvalidDestinationException".into(),
                message: None,
            }),
            Err(e) => Err(e.into()),
        }
    }

    // ------------------------------------------------------------ transactions

    async fn process_transaction(&mut self, info: TransactionInfo) -> OwResult<Handled> {
        if info.type_ == 5 {
            return Ok(self.recover(info.command_id));
        }
        let Some(id) = info.transaction_id.clone() else {
            return Err(OwError::npe());
        };
        match info.type_ {
            0 => self.begin(&id)?,
            1 => return self.prepare(&id).map(|r| Handled::Reply(r.map(Box::new))),
            2 => return self.commit(&id, true, info.command_id).await,
            3 => return self.commit(&id, false, info.command_id).await,
            4 => self.rollback(&id).await?,
            6 => match &id {
                TransactionId::Local(l) => {
                    self.local_txs.remove(l);
                }
                TransactionId::Xa(_) => {
                    return Err(TxError::Xa(transaction::XaError {
                        code: xa_code::XAER_NOTA,
                        message: None,
                    })
                    .into());
                }
            },
            7 => {
                if let TransactionId::Xa(x) = &id
                    && self.broker.resource_manager().get(&to_xid(x)).is_none()
                {
                    return Err(TxError::Xa(transaction::XaError::new(
                        xa_code::XAER_NOTA,
                        format!(
                            "Cannot find suspended transaction to end {}",
                            transaction::xid_string(&to_xid(x))
                        ),
                    ))
                    .into());
                }
            }
            t => return Err(OwError::io(format!("Transaction info type unknown: {t}"))),
        }
        Ok(Handled::Reply(None))
    }

    /// `processRecoverTransactions`: every in-doubt branch the broker
    /// has, whoever made it.
    fn recover(&self, command_id: i32) -> Handled {
        let data = self
            .broker
            .resource_manager()
            .in_doubt()
            .into_iter()
            .map(|x| {
                DataStructure::XaTransactionId(XaTransactionId {
                    format_id: x.format_id,
                    global_transaction_id: Some(x.global_transaction_id),
                    branch_qualifier: Some(x.branch_qualifier),
                })
            })
            .collect();
        Handled::Reply(Some(Box::new(DataStructure::DataArrayResponse(
            DataArrayResponse {
                correlation_id: command_id,
                data: Some(data),
                ..Default::default()
            },
        ))))
    }

    /// `processBeginTransaction`.
    fn begin(&mut self, id: &TransactionId) -> OwResult<()> {
        let internal = self.internal.as_ref().ok_or_else(OwError::npe)?;
        match id {
            TransactionId::Xa(x) => {
                let tx = internal.begin_xa(to_xid(x))?;
                self.xa_txs.insert(x.clone(), TxEntry::new(tx));
            }
            TransactionId::Local(l) => {
                let tx = internal.new_transaction();
                self.local_txs.insert(l.clone(), TxEntry::new(tx));
            }
        }
        Ok(())
    }

    fn not_started(id: &TransactionId) -> OwError {
        OwError::jms_ise(format!(
            "Transaction not started, {}",
            transaction_id_string(id)
        ))
    }

    /// `processPrepareTransaction`.
    fn prepare(&mut self, id: &TransactionId) -> OwResult<Option<DataStructure>> {
        match id {
            TransactionId::Xa(x) => {
                let internal = self.internal.as_mut().ok_or_else(OwError::npe)?;
                internal.xa_prepare(to_xid(x))?;
            }
            TransactionId::Local(l) => {
                if !self.local_txs.contains_key(l) {
                    return Err(OwError::npe());
                }
                return Err(OwError::java_ise("Cannot prepare non XA transaction"));
            }
        }
        Ok(Some(DataStructure::IntegerResponse(IntegerResponse {
            result: xa_code::XA_OK,
            ..Default::default()
        })))
    }

    /// `processCommit`: the consumers get their credits back once it is
    /// written.
    async fn commit(
        &mut self,
        id: &TransactionId,
        one_phase: bool,
        command_id: i32,
    ) -> OwResult<Handled> {
        let entry = match id {
            TransactionId::Local(l) => self.local_txs.remove(l),
            TransactionId::Xa(x) => {
                let known = self.broker.resource_manager().get(&to_xid(x));
                match known {
                    None => None,
                    Some(tx) => Some(self.xa_txs.remove(x).unwrap_or_else(|| TxEntry::new(tx))),
                }
            }
        };
        let Some(entry) = entry else {
            return Err(Self::not_started(id));
        };
        let result: OwResult<()> = match id {
            TransactionId::Xa(x) => {
                let internal = self.internal.as_mut().ok_or_else(OwError::npe)?;
                internal.xa_commit(to_xid(x), one_phase).map_err(Into::into)
            }
            TransactionId::Local(_) => {
                let session = match &entry.session {
                    Some(s) if self.sessions.contains_key(s) => {
                        &mut self.sessions.get_mut(s).expect("checked").session
                    }
                    _ => self.internal.as_mut().ok_or_else(OwError::npe)?,
                };
                session.commit_tx(&entry.tx).map_err(Into::into)
            }
        };
        if let Err(e) = result {
            if let TransactionId::Xa(x) = id
                && transaction::lock(&entry.tx).state == TxState::Suspended
            {
                self.xa_txs.insert(x.clone(), entry);
                return Err(OwError::new(
                    "javax.transaction.xa.XAException",
                    e.message.unwrap_or_default(),
                ));
            }
            return Err(e);
        }
        let credits = entry.credits;
        let after: Box<dyn FnOnce() + Send> = Box::new(move || {
            for (c, n) in credits {
                c.replenish(n, false);
            }
        });
        self.reply(Some(response(command_id)), Some(after));
        Ok(Handled::Sent)
    }

    /// `processRollbackTransaction`: what the transaction acknowledged
    /// stays with the consumers that still have it, delivered once more.
    async fn rollback(&mut self, id: &TransactionId) -> OwResult<()> {
        let entry = match id {
            TransactionId::Local(l) => self.local_txs.remove(l),
            TransactionId::Xa(x) => {
                let known = self.broker.resource_manager().get(&to_xid(x));
                match known {
                    None => None,
                    Some(tx) => Some(self.xa_txs.remove(x).unwrap_or_else(|| TxEntry::new(tx))),
                }
            }
        };
        let Some(entry) = entry else {
            return Err(Self::not_started(id));
        };
        if let TransactionId::Xa(x) = id {
            let xid = to_xid(x);
            if transaction::lock(&entry.tx).state == TxState::Suspended {
                return Err(OwError::new(
                    "javax.transaction.xa.XAException",
                    format!(
                        "Cannot commit transaction, it is suspended {}",
                        transaction::xid_string(&xid)
                    ),
                ));
            }
            self.broker.resource_manager().remove(&xid);
        }
        let state = transaction::lock(&entry.tx).state;
        if state == TxState::RolledBack {
            return Ok(());
        }
        let session_id = entry
            .session
            .clone()
            .filter(|s| self.sessions.contains_key(s));
        let kept: Vec<TxAck> = entry
            .acks
            .into_iter()
            .filter(|a| Some(&a.session) == session_id.as_ref())
            .collect();
        let keep: Vec<(i64, i64)> = kept
            .iter()
            .map(|a| (a.consumer.core_id, a.delivered.core))
            .collect();
        let session = match &session_id {
            Some(s) => &mut self.sessions.get_mut(s).expect("checked").session,
            None => self.internal.as_mut().ok_or_else(OwError::npe)?,
        };
        session.rollback_tx_keeping(&entry.tx, &keep)?;
        // Back to their consumers, in the order they were delivered.
        let mut by_consumer: Vec<(Arc<AmqConsumer>, Vec<Delivered>)> = Vec::new();
        for a in kept {
            match by_consumer
                .iter_mut()
                .find(|(c, _)| Arc::ptr_eq(c, &a.consumer))
            {
                Some((_, v)) => v.push(a.delivered),
                None => by_consumer.push((a.consumer, vec![a.delivered])),
            }
        }
        for (c, refs) in by_consumer {
            c.back_to_delivering(refs);
        }
        Ok(())
    }
}

fn dest_is_composite(info: &ConsumerInfo) -> bool {
    info.destination
        .as_ref()
        .is_some_and(Destination::is_composite)
}

fn set_routing_type(m: &mut CoreMessage, t: RoutingType) {
    m.properties.insert(
        saranga_server::message::HDR_ROUTING_TYPE.into(),
        PropertyValue::Byte(t.to_byte()),
    );
}
