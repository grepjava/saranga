//! A STOMP connection: `StompConnection`, the session handling of
//! `StompProtocolManager` and `StompSession`, and the versioned frame
//! handlers (`StompFrameHandlerV10`, `V11`, `V12`).
//!
//! One task reads and handles frames in order. Replies wait for the
//! journal writes their frame lined up, in order, on a second task;
//! messages go straight to the writer from the queues delivering them.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_message::CoreMessage;
use saranga_security::{Authenticated, SecurityError};
use saranga_server::postoffice::{AutoCreate, QueueSpec, split_fqqn};
use saranga_server::protocol::Connection as Accepted;
use saranga_server::queue::{AckResult, DeliverySink, Queue};
use saranga_server::session::{
    AckPending, ClientConnection, Completion, ServerSession, SessionOptions, Transport,
};
use saranga_server::transaction::TxRef;
use saranga_server::{Broker, BrokerError, RoutingType};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::convert;
use crate::decoder::{self, DecodeError, Rules};
use crate::frame::{Frame, Kind, names};

/// The protocol versions (`StompVersions`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    V10,
    V11,
    V12,
}

impl Version {
    fn as_str(self) -> &'static str {
        match self {
            Self::V10 => "1.0",
            Self::V11 => "1.1",
            Self::V12 => "1.2",
        }
    }

    fn rules(self) -> Rules {
        match self {
            Self::V10 => Rules::V10,
            Self::V11 => Rules::V11,
            Self::V12 => Rules::V12,
        }
    }

    fn kind(self) -> Kind {
        match self {
            Self::V10 => Kind::V10,
            Self::V11 => Kind::V11,
            Self::V12 => Kind::V12,
        }
    }
}

/// The acceptor parameters STOMP reads.
#[derive(Debug, Clone)]
struct Settings {
    /// `stompEnableMessageId`: sent messages carry `amqMessageId`.
    enable_message_id: bool,
    /// `stompMinLargeMessageSize`; -1 for never.
    min_large_message_size: i64,
    /// `stompConsumerWindowSize` (or `stompConsumerCredits`).
    consumer_window_size: i32,
    /// `disableStompServerHeader`.
    disable_server_header: bool,
    /// The connection TTL in milliseconds.
    ttl: i64,
    /// `connectionTtlMin` and `connectionTtlMax`.
    ttl_min: i64,
    ttl_max: i64,
    /// `heartBeatToConnectionTtlModifier`.
    heart_beat_modifier: f64,
}

impl Settings {
    fn from(acceptor: &saranga_server::Acceptor, broker: &Broker) -> Self {
        let p = |k: &str| acceptor.params.get(k).map(String::as_str);
        let flag = |k: &str| p(k).is_some_and(|v| v.eq_ignore_ascii_case("true"));
        let int = |keys: &[&str], default: i64| {
            keys.iter()
                .find_map(|k| p(k).and_then(|v| v.trim().parse::<i64>().ok()))
                .unwrap_or(default)
        };
        // createConnectionEntry: the acceptor's `connectionTtl`, else the
        // broker's override, else a minute.
        let ttl = match p("connectionTtl").and_then(|v| v.parse::<i64>().ok()) {
            Some(t) if t > 0 => t,
            _ if broker.connection_ttl_override() != -1 => broker.connection_ttl_override(),
            _ => 60_000,
        };
        Self {
            enable_message_id: flag("stompEnableMessageId") || flag("stomp-enable-message-id"),
            min_large_message_size: int(
                &["stompMinLargeMessageSize", "stomp-min-large-message-size"],
                100 * 1024,
            ),
            consumer_window_size: int(
                &[
                    "stompConsumerWindowSize",
                    "stompConsumerCredits",
                    "stomp-consumer-credits",
                ],
                10 * 1024,
            ) as i32,
            disable_server_header: flag("disableStompServerHeader"),
            ttl,
            ttl_min: int(&["connectionTtlMin"], 1000),
            ttl_max: int(&["connectionTtlMax"], i64::MAX),
            heart_beat_modifier: p("heartBeatToConnectionTtlModifier")
                .and_then(|v| v.parse().ok())
                .unwrap_or(2.0),
        }
    }
}

/// `StompConnection.SERVER_NAME`, with Saranga's name.
const SERVER_NAME: &str = concat!("Saranga/", env!("CARGO_PKG_VERSION"));

enum Out {
    Bytes(Vec<u8>),
    Close,
}

/// The write side of a connection, shared with the queues delivering to
/// its subscriptions.
struct Outbound {
    tx: mpsc::UnboundedSender<Out>,
    destroyed: AtomicBool,
    remote: String,
}

impl Outbound {
    /// `StompProtocolManager.send` with `physicalSend`.
    fn send(&self, frame: &Frame) -> bool {
        if self.destroyed.load(Ordering::SeqCst) {
            tracing::warn!("AMQ332068: connection closed {}", self.remote);
            return false;
        }
        if self.tx.send(Out::Bytes(frame.encode())).is_err() {
            return false;
        }
        if frame.command == names::ERROR && !frame.ping {
            tracing::warn!(
                "AMQ332069: Sent ERROR frame to STOMP client {}: {}",
                self.remote,
                frame
                    .header(names::MESSAGE_HEADER)
                    .unwrap_or("no message header")
            );
        }
        true
    }
}

async fn write_loop<W: AsyncWrite + Unpin>(mut wr: W, mut rx: mpsc::UnboundedReceiver<Out>) {
    let mut buf = Vec::with_capacity(64 * 1024);
    while let Some(first) = rx.recv().await {
        let mut close = matches!(first, Out::Close);
        if let Out::Bytes(b) = first {
            buf.extend_from_slice(&b);
        }
        while !close && buf.len() < 256 * 1024 {
            match rx.try_recv() {
                Ok(Out::Bytes(b)) => buf.extend_from_slice(&b),
                Ok(Out::Close) => close = true,
                Err(_) => break,
            }
        }
        if !buf.is_empty() && wr.write_all(&buf).await.is_err() {
            break;
        }
        buf.clear();
        if close {
            break;
        }
    }
    let _ = wr.shutdown().await;
}

/// What the connection's task hears besides its client.
enum Event {
    /// An auto-acknowledged delivery went out (`afterDeliveryTasks`).
    AutoAck { consumer: i64, message: i64 },
    /// The queue answered an auto-acknowledgement made in `tx`.
    AutoAcked {
        tx: TxRef,
        pending: AckPending,
        result: AckResult,
    },
    /// A reply that closes the connection was written.
    Close,
    /// The broker closed a subscription's consumer
    /// (`StompSession.disconnect`).
    Disconnected { consumer: i64 },
}

/// A reply, sent once the journal writes before it are done
/// (`StompProtocolManager.sendReply`).
struct Reply {
    completion: Completion,
    frame: Option<Frame>,
    /// `StompPostReceiptFunction`, or what follows CONNECTED.
    after: Option<Box<dyn FnOnce() + Send>>,
    /// Close the connection after it.
    close: bool,
    /// For the error frame if the writes fail.
    kind: Kind,
}

async fn completer(
    out: Arc<Outbound>,
    events: mpsc::UnboundedSender<Event>,
    mut rx: mpsc::UnboundedReceiver<Reply>,
) {
    while let Some(reply) = rx.recv().await {
        match reply.completion.wait().await {
            Err(e) => {
                tracing::warn!("AMQ222020: error processing IO callback: {e}");
                let mut err = StompError::bundle("Error sending reply");
                err.body = Some(e.to_string());
                out.send(&err.frame(reply.kind));
                if reply.close {
                    let _ = events.send(Event::Close);
                }
            }
            Ok(()) => completed(&out, &events, reply.frame, reply.after, reply.close),
        }
    }
}

/// A reply whose journal writes are done: the frame goes out, then what
/// follows it.
fn completed(
    out: &Outbound,
    events: &mpsc::UnboundedSender<Event>,
    frame: Option<Frame>,
    after: Option<Box<dyn FnOnce() + Send>>,
    mut close: bool,
) {
    if let Some(frame) = &frame {
        out.send(frame);
        close |= frame.needs_disconnect;
    }
    if let Some(after) = after {
        after();
    }
    if close {
        let _ = events.send(Event::Close);
    }
}

/// An ERROR to send (`ActiveMQStompException`).
#[derive(Debug, Clone)]
struct StompError {
    message: String,
    headers: Vec<(String, String)>,
    body: Option<String>,
    disconnect: Option<bool>,
    /// An `ActiveMQSecurityException`: a refused login.
    security: bool,
}

impl StompError {
    /// A message from Artemis's bundle or a plain exception: the
    /// `ActiveMQStompException(String)` constructor drops every `:`.
    fn bundle(message: impl Into<String>) -> Self {
        Self {
            message: message.into().replace(':', ""),
            headers: Vec::new(),
            body: None,
            disconnect: None,
            security: false,
        }
    }

    /// `new ActiveMQStompException(connection, msg)`, which keeps the text.
    fn plain(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ..Self::bundle("")
        }
    }

    /// `new ActiveMQStompException(msg, cause)`: the cause's message is the
    /// body.
    fn caused(message: impl Into<String>, cause: impl Into<String>) -> Self {
        Self {
            body: Some(cause.into()),
            ..Self::bundle(message)
        }
    }

    /// `getFrame`.
    fn frame(&self, kind: Kind) -> Frame {
        let mut frame = Frame::new(names::ERROR, kind);
        frame.add_header(names::MESSAGE_HEADER, &self.message);
        for (k, v) in &self.headers {
            frame.add_header(k, v);
        }
        if let Some(body) = &self.body {
            frame.add_header(names::CONTENT_TYPE, "text/plain");
            frame.set_body(body.as_bytes().to_vec());
        }
        if let Some(d) = self.disconnect {
            frame.needs_disconnect = d;
        }
        frame
    }
}

type StompResult<T> = Result<T, StompError>;

/// An acknowledgement mode (`AckModeValues`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum AckMode {
    Auto,
    Client,
    ClientIndividual,
    /// Anything else, which acknowledges like `client`.
    Other(String),
}

impl AckMode {
    fn parse(s: Option<&str>) -> Self {
        match s {
            None | Some("auto") => Self::Auto,
            Some("client") => Self::Client,
            Some("client-individual") => Self::ClientIndividual,
            Some(o) => Self::Other(o.to_owned()),
        }
    }
}

/// `StompSubscription`, shared with the subscription's delivery sink.
struct Subscription {
    id: String,
    ack: AckMode,
    queue: String,
    multicast: bool,
    no_local: bool,
    /// `consumerWindowSize`; -1 for no flow control.
    window: i32,
    /// The consumer was closed: deliveries in flight are dropped.
    closed: AtomicBool,
}

/// Messages delivered to client-acknowledged subscriptions and not yet
/// acknowledged: (consumer, message) to the frame's size
/// (`messagesToAck`).
type ToAck = Arc<Mutex<HashMap<(i64, i64), i64>>>;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// `StompSession.sendMessage`: a subscription's deliveries.
struct StompSink {
    out: Arc<Outbound>,
    events: mpsc::UnboundedSender<Event>,
    subscription: Arc<Subscription>,
    kind: Kind,
    to_ack: ToAck,
}

impl StompSession {
    /// `sendInternal`: the producer is made at the first send, and the
    /// send is counted against it.
    fn start_sending(&mut self) {
        let name = self.sender.get_or_insert_with(|| {
            let name = uuid::Uuid::new_v4().to_string();
            self.session.add_producer(&name, "STOMP", None);
            name
        });
        self.session.set_sender(Some(name.as_str().into()));
    }
}

impl DeliverySink for StompSink {
    fn deliver(
        &self,
        consumer_id: i64,
        message: &std::sync::Arc<saranga_server::message::ServerMessage>,
        delivery_count: i32,
    ) -> i64 {
        let sub = &self.subscription;
        if sub.closed.load(Ordering::SeqCst) {
            return 0;
        }
        // A message of another protocol as Core (`toCore`).
        let core = message.delivery_core();
        let frame = match convert::message_frame(
            self.kind,
            &sub.id,
            self.kind == Kind::V12 && sub.ack != AckMode::Auto,
            consumer_id,
            core,
            delivery_count,
        ) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(
                    "AMQ332071: Unable to send message to client: {}: {e}",
                    core.message_id
                );
                return 0;
            }
        };
        let length = frame.encode().len() as i64;
        if sub.ack == AckMode::Auto {
            if self.out.send(&frame) {
                let _ = self.events.send(Event::AutoAck {
                    consumer: consumer_id,
                    message: core.message_id,
                });
            }
        } else {
            // Recorded before it goes out, or the client could ack first.
            lock(&self.to_ack).insert((consumer_id, core.message_id), length);
            self.out.send(&frame);
        }
        length
    }

    fn deliver_large(
        &self,
        _consumer_id: i64,
        _message: &saranga_server::message::ServerMessage,
        _body_size: i64,
        _delivery_count: i32,
    ) -> i64 {
        0
    }

    fn deliver_continuation(&self, _consumer_id: i64, _chunk: Vec<u8>, _continues: bool) -> i64 {
        0
    }

    fn chunk_size(&self) -> usize {
        usize::MAX
    }

    fn supports_large(&self) -> bool {
        false
    }

    /// `StompSession.disconnect`: the client gets an ERROR frame and the
    /// subscription goes.
    fn disconnect(&self, consumer_id: i64, _reason: &str) {
        if self.subscription.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut frame = Frame::new(names::ERROR, self.kind);
        frame.add_header(names::CONTENT_TYPE, "text/plain");
        frame.set_body(format!(
            "consumer with ID {consumer_id} disconnected by server"
        ));
        self.out.send(&frame);
        let _ = self.events.send(Event::Disconnected {
            consumer: consumer_id,
        });
    }
}

/// `StompSession`: a server session and the subscriptions on it.
struct StompSession {
    session: ServerSession,
    /// The session's one producer, made at its first send (`senderName`).
    sender: Option<String>,
    /// By consumer ID.
    subscriptions: BTreeMap<i64, Arc<Subscription>>,
    to_ack: ToAck,
    no_local: usize,
    tx_pending: bool,
}

/// The heart-beating agreed at CONNECT (`HeartBeater`).
#[derive(Debug, Clone, Copy)]
struct HeartBeat {
    server_ping: i64,
    client_ping_response: i64,
}

struct Connection {
    broker: Arc<Broker>,
    client: Arc<ClientConnection>,
    out: Arc<Outbound>,
    replies: mpsc::UnboundedSender<Reply>,
    events: mpsc::UnboundedSender<Event>,
    settings: Settings,
    /// The connection ID (`getID`), the `session` header of CONNECTED.
    id: String,
    /// The frame handler in use (`frameHandler`).
    handler: Version,
    /// The version agreed at CONNECT (`version`).
    negotiated: Option<Version>,
    /// A login succeeded (`valid`).
    valid: bool,
    login: Option<String>,
    passcode: Option<String>,
    main: Option<StompSession>,
    transacted: HashMap<String, StompSession>,
    ttl: i64,
    heart_beat: Option<HeartBeat>,
    /// A reply that closes the connection is on its way; nothing more is
    /// read.
    closing: bool,
}

/// Serves a STOMP connection until it closes.
pub async fn serve(broker: Arc<Broker>, c: Accepted) {
    let settings = Settings::from(&c.acceptor, &broker);
    let remote = c.info.remote_address.clone();
    let client = Arc::new(
        ClientConnection::new(
            Transport::socket("STOMP", "StompConnection", c.peer, c.local),
            Arc::new(c.info),
            c.acceptor.prefixes.clone(),
        )
        .with_acceptor(&c.acceptor.name),
    );
    let _registered = broker.registry().add(&client);
    let failed = client.clone();
    let (mut rd, wr) = tokio::io::split(c.stream);
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let writer = tokio::spawn(write_loop(wr, out_rx));
    let out = Arc::new(Outbound {
        tx: out_tx,
        destroyed: AtomicBool::new(false),
        remote,
    });
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (replies_tx, replies_rx) = mpsc::unbounded_channel();
    let completer = tokio::spawn(completer(out.clone(), events_tx.clone(), replies_rx));
    let ttl = settings.ttl;
    let mut conn = Connection {
        broker,
        client,
        out,
        replies: replies_tx,
        events: events_tx,
        settings,
        id: uuid::Uuid::new_v4().simple().to_string()[..8].to_owned(),
        handler: Version::V10,
        negotiated: None,
        valid: false,
        login: None,
        passcode: None,
        main: None,
        transacted: HashMap::new(),
        ttl,
        heart_beat: None,
        closing: false,
    };
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    loop {
        let ttl = Duration::from_millis(conn.ttl.max(1) as u64);
        tokio::select! {
            () = failed.failed() => break,
            event = events_rx.recv() => match event {
                Some(Event::AutoAck { consumer, message }) => conn.auto_ack(consumer, message).await,
                Some(Event::AutoAcked { tx, pending, result }) => conn.auto_acked(tx, pending, result),
                Some(Event::Disconnected { consumer }) => conn.disconnected(consumer).await,
                Some(Event::Close) | None => break,
            },
            read = tokio::time::timeout(ttl, rd.read_buf(&mut buf)), if !conn.closing => match read {
                Err(_) => {
                    conn.fail_ttl();
                    break;
                }
                Ok(Ok(0)) | Ok(Err(_)) => break,
                Ok(Ok(_)) => {
                    if !conn.process(&mut buf).await {
                        break;
                    }
                }
            },
        }
    }
    // Acknowledgements the queues answered are committed; any answering
    // later find the channel closed and finish on their own.
    events_rx.close();
    while let Ok(event) = events_rx.try_recv() {
        if let Event::AutoAcked {
            tx,
            pending,
            result,
        } = event
        {
            conn.auto_acked(tx, pending, result);
        }
    }
    conn.destroy().await;
    drop(conn);
    let _ = completer.await;
    let _ = writer.await;
}

impl Connection {
    fn kind(&self) -> Kind {
        self.handler.kind()
    }

    /// Decodes and handles every complete frame in `buf`. False when the
    /// connection must close at once.
    async fn process(&mut self, buf: &mut Vec<u8>) -> bool {
        loop {
            if self.closing {
                return true;
            }
            match decoder::decode(self.handler.rules(), buf) {
                Ok(Some((decoded, used))) => {
                    buf.drain(..used);
                    let frame =
                        Frame::decoded(decoded.command, Kind::V11, decoded.headers, decoded.body);
                    self.handle_frame(frame).await;
                }
                Ok(None) => {
                    // Heart-beats alone: drop them, keeping a trailing `\r`.
                    let eols = decoder::leading_eols(buf);
                    if eols == buf.len() {
                        let keep = usize::from(buf.last() == Some(&b'\r'));
                        buf.drain(..eols - keep);
                    }
                    return true;
                }
                Err(DecodeError::InvalidEolV10(_))
                    if self.negotiated.is_none() && self.handler == Version::V10 =>
                {
                    // A client that ends lines with \r\n speaks 1.2.
                    self.handler = Version::V12;
                }
                Err(DecodeError::InvalidCommand(dump)) => {
                    let mut e =
                        StompError::bundle(format!("AMQ339023: Invalid STOMP frame: {dump}"));
                    e.body = Some(format!("AMQ339024: Invalid STOMP frame: {dump}"));
                    self.on_decode_error(e);
                    return false;
                }
                Err(DecodeError::UndefinedEscape(seq)) => {
                    self.on_decode_error(StompError::bundle(format!(
                        "AMQ339040: Undefined escape sequence: {seq}"
                    )));
                    return false;
                }
                Err(e) => {
                    tracing::error!("AMQ224021: Failed to decode packet: {e:?}");
                    buf.clear();
                    return true;
                }
            }
        }
    }

    /// `onError`: the ERROR goes out and the connection closes.
    fn on_decode_error(&mut self, e: StompError) {
        self.out.send(&e.frame(self.kind()));
    }

    /// The connection TTL passed without a byte from the client
    /// (`RemotingServiceImpl.FailureCheckAndFlushThread`, `fail`).
    fn fail_ttl(&mut self) {
        let message = format!(
            "AMQ229014: Did not receive data from {} within the {}ms connection TTL. The connection will now be closed.",
            self.out.remote, self.ttl
        );
        let mut frame = Frame::new(names::ERROR, self.kind());
        frame.add_header(names::MESSAGE_HEADER, &message);
        self.out.send(&frame);
        tracing::warn!(
            "AMQ222067: Connection failure has been detected: {message} [code=CONNECTION_TIMEDOUT]"
        );
    }

    /// Sends a reply after the journal writes the sessions lined up.
    fn reply(
        &mut self,
        frame: Option<Frame>,
        after: Option<Box<dyn FnOnce() + Send>>,
        close: bool,
    ) {
        let mut completion = Completion::default();
        for s in self.main.iter_mut().chain(self.transacted.values_mut()) {
            completion.merge(s.session.take_completion());
        }
        if close || frame.as_ref().is_some_and(|f| f.needs_disconnect) {
            self.closing = true;
        }
        let _ = self.replies.send(Reply {
            completion,
            frame,
            after,
            close,
            kind: self.kind(),
        });
    }

    /// `StompConnection.handleFrame`.
    async fn handle_frame(&mut self, request: Frame) {
        let cmd = request.command.clone();
        let reply = if self.negotiated.is_none() && cmd != names::CONNECT && cmd != names::STOMP {
            Some(
                StompError::bundle("AMQ339008: Connection has not been established.")
                    .frame(self.kind()),
            )
        } else {
            match self.negotiate(&request) {
                Err(e) => Some(e.frame(self.kind())),
                Ok(()) => {
                    if cmd == names::SUBSCRIBE {
                        // handleSubscribe: the receipt, then the credits.
                        match self.on_subscribe(&request).await {
                            Ok(grant) => {
                                let receipt = self.postprocess(&request);
                                self.reply(receipt, grant, false);
                                return;
                            }
                            Err(e) => Some(e.frame(self.kind())),
                        }
                    } else {
                        self.dispatch(request).await
                    }
                }
            }
        };
        // replySent: the pinger starts once CONNECTED is out.
        let after = match &reply {
            Some(f) if f.command == names::CONNECTED && self.handler != Version::V10 => {
                self.start_heart_beat()
            }
            _ => None,
        };
        self.reply(reply, after, cmd == names::DISCONNECT);
    }

    /// `negotiateVersion`, once: the highest version in `accept-version`.
    fn negotiate(&mut self, frame: &Frame) -> StompResult<()> {
        if self.negotiated.is_some() {
            return Ok(());
        }
        let version = match frame.header(names::ACCEPT_VERSION) {
            None => Version::V10,
            Some(accept) => {
                let offered: Vec<&str> = accept.split(',').filter(|s| !s.is_empty()).collect();
                if offered.contains(&"1.2") {
                    Version::V12
                } else if offered.contains(&"1.1") {
                    Version::V11
                } else if offered.contains(&"1.0") {
                    Version::V10
                } else {
                    let mut e = StompError::bundle(format!(
                        "AMQ339002: Stomp versions not supported: {accept}"
                    ));
                    e.headers
                        .push((names::VERSION.into(), "1.0,1.1,1.2".into()));
                    e.headers
                        .push((names::CONTENT_TYPE.into(), "text/plain".into()));
                    e.body = Some("Supported protocol versions are v1.0 v1.1 v1.2".into());
                    e.disconnect = Some(true);
                    return Err(e);
                }
            }
        };
        if version != Version::V10 {
            self.handler = version;
        }
        self.negotiated = Some(version);
        Ok(())
    }

    /// `VersionedStompFrameHandler.handleFrame` for everything but
    /// SUBSCRIBE.
    async fn dispatch(&mut self, request: Frame) -> Option<Frame> {
        let v10 = self.handler == Version::V10;
        let result = match request.command.as_str() {
            names::SEND => self.on_send(&request).await,
            names::ACK => self.on_ack(&request).await,
            names::NACK if !v10 => self.on_ack(&request).await,
            names::BEGIN => self.on_begin(&request).await,
            names::COMMIT => self.on_commit(&request),
            names::ABORT => self.on_abort(&request).await,
            names::UNSUBSCRIBE => self.on_unsubscribe(&request).await,
            names::CONNECT => Ok(Some(self.on_connect(&request).await)),
            names::STOMP if !v10 => {
                if self.valid {
                    Ok(None)
                } else {
                    Ok(Some(self.on_connect(&request).await))
                }
            }
            names::DISCONNECT => {
                if !v10 {
                    self.heart_beat = None;
                }
                Ok(None)
            }
            other => Err(StompError::bundle(format!(
                "AMQ339029: Unsupported command: {other}"
            ))),
        };
        match result {
            Ok(Some(mut response)) => {
                if let Some(r) = request.header(names::RECEIPT_REQUESTED) {
                    response.add_header(names::RECEIPT_ID, r);
                }
                Some(response)
            }
            Ok(None) => self.postprocess(&request),
            Err(e) => {
                let mut response = e.frame(self.kind());
                if let Some(r) = request.header(names::RECEIPT_REQUESTED) {
                    response.add_header(names::RECEIPT_ID, r);
                }
                Some(response)
            }
        }
    }

    /// `postprocess`: a RECEIPT when one was asked for.
    fn postprocess(&mut self, request: &Frame) -> Option<Frame> {
        let receipt = request.header(names::RECEIPT_REQUESTED)?;
        let mut frame = Frame::new(names::RECEIPT, self.kind());
        frame.add_header(names::RECEIPT_ID, receipt);
        if request.command == names::DISCONNECT {
            frame.needs_disconnect = true;
        }
        Some(frame)
    }

    /// `validateUser` and `createSession`. Login modules may block
    /// (LDAP), so they run off the connection's task.
    fn new_session(
        &self,
        transacted: bool,
    ) -> impl std::future::Future<Output = Result<StompSession, SecurityError>> + Send + 'static
    {
        let broker = self.broker.clone();
        let client = self.client.clone();
        let (login, passcode) = (self.login.clone(), self.passcode.clone());
        async move {
            let authenticated = match broker.security().cloned() {
                None => Authenticated::default(),
                Some(store) => {
                    let info = client.security_info();
                    let (user, password) = (login.clone(), passcode.clone());
                    tokio::task::spawn_blocking(move || {
                        store.authenticate(user.as_deref(), password.as_deref(), &info)
                    })
                    .await
                    .unwrap_or_else(|e| Err(SecurityError(format!("login failed: {e}"))))?
                }
            };
            let session = ServerSession::new(
                broker,
                client,
                SessionOptions {
                    name: uuid::Uuid::new_v4().to_string(),
                    username: login,
                    password: passcode,
                    xa: false,
                    auto_commit_sends: !transacted,
                    auto_commit_acks: false,
                    pre_acknowledge: false,
                    default_address: None,
                },
                authenticated,
            );
            Ok(StompSession {
                session,
                sender: None,
                subscriptions: BTreeMap::new(),
                to_ack: ToAck::default(),
                no_local: 0,
                tx_pending: false,
            })
        }
    }

    /// `getSession`: the connection's session, made (and its user checked)
    /// on first use.
    async fn session(&mut self) -> StompResult<&mut StompSession> {
        if self.main.is_none() {
            let s = self.new_session(false).await.map_err(security_error)?;
            self.main = Some(s);
        }
        Ok(self.main.as_mut().expect("just made"))
    }

    /// `getTransactedSession`.
    async fn tx_session(&mut self, tx: &str) -> StompResult<&mut StompSession> {
        if !self.transacted.contains_key(tx) {
            let s = self.new_session(true).await.map_err(security_error)?;
            self.transacted.insert(tx.to_owned(), s);
        }
        Ok(self.transacted.get_mut(tx).expect("just made"))
    }

    async fn session_for(&mut self, tx: Option<&str>) -> StompResult<&mut StompSession> {
        match tx {
            None => self.session().await,
            Some(tx) => self.tx_session(tx).await,
        }
    }

    /// `onConnect` of the 1.0 and 1.1 handlers.
    async fn on_connect(&mut self, frame: &Frame) -> Frame {
        let login = frame.header(names::LOGIN).map(str::to_owned);
        self.client
            .set_client_id(frame.header(names::CLIENT_ID).map(str::to_owned));
        self.login = login.clone();
        self.passcode = frame.header(names::PASSCODE).map(str::to_owned);
        if let Err(e) = self.session().await {
            return if e.security {
                self.failed_authentication(login.as_deref())
            } else {
                e.frame(self.kind())
            };
        }
        self.valid = true;
        let v10 = self.handler == Version::V10;
        let mut response = Frame::new(names::CONNECTED, self.kind());
        if v10 {
            if frame.has_header(names::ACCEPT_VERSION) {
                response.add_header(names::VERSION, Version::V10.as_str());
            }
            response.add_header(names::SESSION, &self.id);
        } else {
            let version = self.negotiated.unwrap_or(Version::V10);
            response.add_header(names::VERSION, version.as_str());
            response.add_header(names::SESSION, &self.id);
            if !self.settings.disable_server_header {
                response.add_header(names::SERVER, SERVER_NAME);
            }
        }
        if let Some(id) = frame.header(names::REQUEST_ID) {
            response.add_header(names::RESPONSE_ID, id);
        }
        if !v10 && let Some(hb) = frame.header(names::HEART_BEAT) {
            match self.heart_beat(hb) {
                Ok(hb) => response.add_header(
                    names::HEART_BEAT,
                    &format!("{},{}", hb.server_ping, hb.client_ping_response),
                ),
                Err(e) => return e.frame(self.kind()),
            }
        }
        response
    }

    /// `getFailedAuthenticationResponse`.
    fn failed_authentication(&self, login: Option<&str>) -> Frame {
        let mut response = Frame::new(names::ERROR, self.kind());
        response.needs_disconnect = true;
        let text = format!(
            "Security Error occurred: User name [{}] or password is invalid",
            login.unwrap_or("null")
        );
        response.set_body(text.as_bytes().to_vec());
        response.add_header(names::MESSAGE_HEADER, &text);
        if self.handler != Version::V10 {
            response.add_header(names::CONTENT_TYPE, "text/plain");
        }
        response
    }

    /// `handleHeartBeat` and the `HeartBeater` it makes: the server pings
    /// at least every 500 ms if the client wants pings; the client's pings
    /// set the connection TTL.
    fn heart_beat(&mut self, header: &str) -> StompResult<HeartBeat> {
        if let Some(hb) = self.heart_beat {
            return Ok(hb);
        }
        let params: Vec<&str> = header.split(',').collect();
        let parsed = match params.as_slice() {
            [a, b] => a.parse::<i64>().ok().zip(b.parse::<i64>().ok()),
            _ => None,
        };
        let Some((client_ping, client_accept)) = parsed else {
            return Err(StompError::plain(format!(
                "Incorrect heartbeat header {header}"
            )));
        };
        let s = &self.settings;
        let server_ping = if client_accept != 0 {
            client_accept.max(500)
        } else {
            0
        };
        let mut response = (self.ttl as f64 / s.heart_beat_modifier) as i64;
        if client_ping != 0 {
            response = client_ping;
            let mut ttl = (client_ping as f64 * s.heart_beat_modifier) as i64;
            if ttl < s.ttl_min {
                ttl = s.ttl_min;
                response = (s.ttl_min as f64 / s.heart_beat_modifier) as i64;
            } else if ttl > s.ttl_max {
                ttl = s.ttl_max;
                response = (s.ttl_max as f64 / s.heart_beat_modifier) as i64;
            }
            tracing::debug!("Setting STOMP client TTL to: {ttl}");
            self.ttl = ttl;
        }
        let hb = HeartBeat {
            server_ping,
            client_ping_response: response,
        };
        self.heart_beat = Some(hb);
        Ok(hb)
    }

    /// The pinger, once CONNECTED is written.
    fn start_heart_beat(&self) -> Option<Box<dyn FnOnce() + Send>> {
        let period = self.heart_beat?.server_ping;
        if period <= 0 {
            return None;
        }
        let out = self.out.clone();
        let kind = self.kind();
        Some(Box::new(move || {
            tokio::spawn(async move {
                let period = Duration::from_millis(period as u64);
                let mut tick =
                    tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                loop {
                    tick.tick().await;
                    if out.destroyed.load(Ordering::SeqCst) || !out.send(&Frame::ping(kind)) {
                        break;
                    }
                }
            });
        }))
    }

    fn validate(&self) -> StompResult<()> {
        if self.valid {
            Ok(())
        } else {
            Err(StompError::bundle("AMQ339010: Connection is not valid."))
        }
    }

    /// `getDestination`: the header without the acceptor's prefix.
    fn destination(&self, frame: &Frame, header: &str) -> Option<String> {
        let d = frame.header(header)?;
        Some(self.client.strip_prefix(d).0.to_owned())
    }

    /// `getRoutingType`: from the type header, or from the destination's
    /// prefix.
    fn routing_type(
        &self,
        type_header: Option<&str>,
        destination: Option<&str>,
    ) -> StompResult<Option<RoutingType>> {
        match type_header {
            Some("ANYCAST") => Ok(Some(RoutingType::Anycast)),
            Some("MULTICAST") => Ok(Some(RoutingType::Multicast)),
            Some(other) => Err(StompError::bundle(format!(
                "No enum constant org.apache.activemq.artemis.api.core.RoutingType.{other}"
            ))),
            None => Ok(destination.and_then(|d| self.client.strip_prefix(d).1)),
        }
    }

    /// `checkAutoCreate`: the destination must exist or be creatable. With
    /// no routing type known, the address's default applies.
    async fn check_auto_create(
        &mut self,
        destination: Option<&str>,
        routing_type: Option<RoutingType>,
    ) -> StompResult<()> {
        let routing_type = routing_type.unwrap_or_else(|| {
            self.broker
                .post_office()
                .settings(destination.unwrap_or_default())
                .default_address_routing_type
        });
        let Some(destination) = destination else {
            return Err(StompError::caused("null", "null"));
        };
        let session = self.session().await?;
        match session
            .session
            .check_auto_create(destination, Some(routing_type))
        {
            Ok(AutoCreate::NotFound) => Err(StompError::bundle(format!(
                "AMQ339001: Destination does not exist: {destination}"
            ))),
            Ok(_) => Ok(()),
            Err(e) => Err(StompError::caused(e.message.clone(), e.message)),
        }
    }

    /// `onSend` and `sendServerMessage`.
    async fn on_send(&mut self, frame: &Frame) -> StompResult<Option<Frame>> {
        self.validate()?;
        let destination = self.destination(frame, names::DESTINATION);
        let routing_type = self
            .routing_type(
                frame.header(names::DESTINATION_TYPE),
                frame.header(names::DESTINATION),
            )
            .map_err(|_| StompError::bundle("AMQ339031: Error handling send"))?;
        self.check_auto_create(destination.as_deref(), routing_type)
            .await?;
        let tx = frame.header(names::TRANSACTION).map(str::to_owned);
        let now = saranga_server::message::now_millis();
        let ids = self.broker.post_office().ids().clone();
        let mut message = CoreMessage {
            message_id: ids.next(),
            timestamp: now,
            address: destination.as_deref().map(Into::into),
            ..CoreMessage::default()
        };
        if let Some(t) = routing_type {
            message.properties.insert(
                saranga_server::message::HDR_ROUTING_TYPE.into(),
                saranga_buffer::PropertyValue::Byte(t.to_byte()),
            );
        }
        let prefix = frame
            .header(names::DESTINATION)
            .and_then(|d| self.session_prefix(d));
        convert::frame_to_message(frame, &mut message, prefix.as_deref(), now)
            .map_err(|_| StompError::bundle("AMQ339031: Error handling send"))?;
        convert::set_body(frame, &mut message);
        let connection_id = self.id.clone();
        let enable_message_id = self.settings.enable_message_id;
        let min_large = self.settings.min_large_message_size;
        let broker = self.broker.clone();
        let session = self.session_for(tx.as_deref()).await?;
        if session.no_local > 0 {
            message.properties.insert(
                convert::CONNECTION_ID_PROPERTY.into(),
                saranga_buffer::PropertyValue::String(connection_id.as_str().into()),
            );
        }
        if enable_message_id {
            message.properties.insert(
                "amqMessageId".into(),
                saranga_buffer::PropertyValue::String(
                    format!("STOMP{}", message.message_id).as_str().into(),
                ),
            );
        }
        // Described only on a failure: this runs for every message.
        let (id, durable, address) = (message.message_id, message.durable, message.address.clone());
        let send_error = |e: String| {
            let describe = describe(id, durable, address.as_ref());
            StompError::bundle(format!("AMQ339011: Error sending message {describe}")).with_log(e)
        };
        // A body this big goes in a large-message file
        // (`sendInternalLarge`).
        if min_large != -1 && (message.body.len() as i64 + 17) >= min_large {
            let mut w = saranga_buffer::Writer::new();
            message.encode_headers_and_properties(&mut w);
            if w.len() as i64 >= min_large {
                return Err(send_error(
                    "AMQ339028: Message header too big, increase minLargeMessageSize please."
                        .into(),
                ));
            }
            let id = ids.next();
            let io = |e: std::io::Error| send_error(e.to_string());
            let mut writer = broker
                .large_messages()
                .create(id, message.durable)
                .map_err(io)?;
            writer.append(&message.body).map_err(io)?;
            let body = writer.finish().map_err(io)?;
            message.properties.insert(
                saranga_server::message::HDR_LARGE_BODY_SIZE.into(),
                saranga_buffer::PropertyValue::Long(message.body.len() as i64),
            );
            message.body = Vec::new().into();
            message.message_id = id;
            session.start_sending();
            let sent = session.session.send(message, Some(body)).await;
            session.session.set_sender(None);
            sent.map_err(|e| send_error(e.message))?;
        } else {
            session.start_sending();
            let sent = session.session.send(message, None).await;
            session.session.set_sender(None);
            sent.map_err(|e| send_error(e.message))?;
        }
        Ok(None)
    }

    /// `ServerSession.getPrefix`.
    fn session_prefix(&self, destination: &str) -> Option<String> {
        self.client
            .prefixes()
            .iter()
            .find(|(p, _)| destination.starts_with(p.as_str()))
            .map(|(p, _)| p.clone())
    }

    /// `onBegin` and `beginTransaction`.
    async fn on_begin(&mut self, frame: &Frame) -> StompResult<Option<Frame>> {
        let Some(tx) = frame.header(names::TRANSACTION).map(str::to_owned) else {
            return Err(StompError::bundle(
                "AMQ339032: Need a transaction id to begin",
            ));
        };
        let session = self.tx_session(&tx).await?;
        if session.tx_pending {
            tracing::error!("AMQ224024: Stomp Error, tx already exist! {tx}");
            return Err(StompError::plain(format!(
                "Transaction already started: {tx}"
            )));
        }
        session.tx_pending = true;
        Ok(None)
    }

    /// `onCommit`: commits the transacted session. Its errors, including
    /// that no transaction was begun, all read `Error committing`.
    fn on_commit(&mut self, frame: &Frame) -> StompResult<Option<Frame>> {
        let Some(tx) = frame.header(names::TRANSACTION).map(str::to_owned) else {
            return Err(StompError::bundle(
                "AMQ339030: transaction header is mandatory to COMMIT a transaction",
            ));
        };
        let failed = || StompError::bundle(format!("AMQ339013: Error committing {tx}"));
        let Some(session) = self.transacted.get_mut(&tx).filter(|s| s.tx_pending) else {
            return Err(failed());
        };
        let result = session.session.commit();
        session.tx_pending = false;
        result.map_err(|_| failed())?;
        Ok(None)
    }

    /// `onAbort`.
    async fn on_abort(&mut self, frame: &Frame) -> StompResult<Option<Frame>> {
        let Some(tx) = frame.header(names::TRANSACTION).map(str::to_owned) else {
            return Err(StompError::bundle(
                "AMQ339033: transaction header is mandatory to ABORT a transaction",
            ));
        };
        let session = self.tx_session(&tx).await?;
        if !session.tx_pending {
            return Err(StompError::plain(format!("No transaction started: {tx}")));
        }
        session.session.rollback(false, false).await;
        session.tx_pending = false;
        Ok(None)
    }

    /// `onAck` of each version (and `onNack`, which acknowledges too).
    async fn on_ack(&mut self, frame: &Frame) -> StompResult<Option<Frame>> {
        if frame.has_header(names::TRANSACTION) {
            tracing::warn!("AMQ222070: Stomp Transactional acknowledgement is not supported");
        }
        let (message_id, subscription) = match self.handler {
            Version::V10 => (frame.header(names::MESSAGE_ID), None),
            Version::V11 => {
                let Some(sub) = frame.header(names::SUBSCRIPTION) else {
                    return Err(StompError::bundle(
                        "AMQ339037: Must specify the subscription's id",
                    ));
                };
                (frame.header(names::MESSAGE_ID), Some(sub))
            }
            Version::V12 => match frame.header(names::ID) {
                None => {
                    return Err(StompError::bundle(
                        "AMQ339039: No id header in ACK/NACK frame.",
                    ));
                }
                Some(id) => (Some(id), None),
            },
        };
        let message_id = message_id.map(str::to_owned);
        let subscription = subscription.map(str::to_owned);
        self.acknowledge(message_id.as_deref(), subscription.as_deref())
            .await?;
        Ok(None)
    }

    /// `StompSession.acknowledge`.
    async fn acknowledge(
        &mut self,
        message_id: Option<&str>,
        subscription: Option<&str>,
    ) -> StompResult<()> {
        let error = || {
            StompError::bundle(format!(
                "AMQ339018: Error acknowledging message {}",
                message_id.unwrap_or("null")
            ))
        };
        let pair = message_id
            .and_then(|id| id.split_once(','))
            .and_then(|(c, m)| c.parse::<i64>().ok().zip(m.parse::<i64>().ok()))
            .ok_or_else(error)?;
        let session = self.session().await?;
        let Some(length) = lock(&session.to_ack).remove(&pair) else {
            return Err(StompError::bundle(format!(
                "AMQ339025: failed to ack because no message with id: {}",
                message_id.unwrap_or("null")
            )));
        };
        let (consumer, id) = pair;
        let sub = session
            .subscriptions
            .get(&consumer)
            .cloned()
            .ok_or_else(error)?;
        if let Some(given) = subscription
            && given != sub.id
        {
            return Err(StompError::bundle(format!(
                "AMQ339026: subscription id {given} does not match {}",
                sub.id
            )));
        }
        let credit = |session: &StompSession, n: i64| {
            if let Some(grant) = session.session.credit_granter(consumer) {
                grant(n as i32);
            }
        };
        if sub.ack == AckMode::ClientIndividual {
            session
                .session
                .acknowledge(consumer, id, true)
                .await
                .map_err(|_| error())?;
            if sub.window != -1 {
                credit(session, length);
            }
        } else {
            let acked = session
                .session
                .acknowledge(consumer, id, false)
                .await
                .map_err(|_| error())?;
            if sub.window != -1 {
                credit(session, length);
            }
            for other in acked.into_iter().filter(|&a| a != id) {
                let removed = lock(&session.to_ack).remove(&(consumer, other));
                if sub.window != -1
                    && let Some(n) = removed
                {
                    credit(session, n);
                }
            }
        }
        session.session.commit().map_err(|_| error())
    }

    /// An auto-acknowledged delivery went out: acknowledge and commit it.
    /// A subscription's consumer was disconnected by the broker: it goes.
    async fn disconnected(&mut self, consumer: i64) {
        for session in self.main.iter_mut().chain(self.transacted.values_mut()) {
            if let Some(sub) = session.subscriptions.remove(&consumer) {
                if sub.no_local {
                    session.no_local -= 1;
                }
                session.session.close_consumer(consumer, false).await;
            }
        }
    }

    /// `afterDelivery` for `ack:auto`: the acknowledgement, in a transaction
    /// of its own, goes to the queue without waiting for it, so that the
    /// next delivery's need not wait either; [`auto_acked`](Self::auto_acked)
    /// commits once the queue answered.
    async fn auto_ack(&mut self, consumer: i64, message: i64) {
        let Some(session) = self.main.as_mut() else {
            return;
        };
        let tx = session.session.new_transaction();
        let events = self.events.clone();
        let held = tx.clone();
        let sent = session.session.acknowledge_in_then(
            consumer,
            message,
            false,
            &tx,
            move |pending, result| {
                let acked = Event::AutoAcked {
                    tx: held,
                    pending,
                    result,
                };
                if let Err(mpsc::error::SendError(Event::AutoAcked {
                    pending, result, ..
                })) = events.send(acked)
                {
                    pending.finish_alone(result);
                }
            },
        );
        if let Err(e) = sent {
            let _ = session.session.rollback_tx(&tx);
            tracing::warn!("{}", e.message);
        }
    }

    /// The rest of [`auto_ack`](Self::auto_ack).
    fn auto_acked(&mut self, tx: TxRef, pending: AckPending, result: AckResult) {
        let Some(session) = self.main.as_mut() else {
            pending.finish_alone(result);
            return;
        };
        let result = match session.session.acknowledged_tx(pending, &tx, result) {
            Ok(_) => session.session.commit_tx(&tx),
            Err(e) => {
                let _ = session.session.rollback_tx(&tx);
                Err(e)
            }
        };
        if let Err(e) = result {
            tracing::warn!("{}", e.message);
        }
        self.reply(None, None, false);
    }

    /// `onSubscribe` and `StompConnection.subscribe`: returns what grants
    /// the consumer its credits once the receipt is out.
    async fn on_subscribe(
        &mut self,
        frame: &Frame,
    ) -> StompResult<Option<Box<dyn FnOnce() + Send>>> {
        let destination = self.destination(frame, names::DESTINATION);
        let selector = frame.header(names::SELECTOR).map(str::to_owned);
        let ack = AckMode::parse(frame.header(names::ACK_HEADER));
        let id = frame.header(names::ID).map(str::to_owned);
        let durable_name = frame
            .header(names::DURABLE_SUBSCRIPTION_NAME)
            .or_else(|| frame.header(names::DURABLE_SUBSCRIBER_NAME))
            .or_else(|| frame.header(names::ACTIVEMQ_DURABLE_SUBSCRIPTION_NAME))
            .map(str::to_owned);
        let generic = |e: String| StompError::caused(e.clone(), e);
        let routing_type = self
            .routing_type(
                frame.header(names::SUBSCRIPTION_TYPE),
                frame.header(names::DESTINATION),
            )
            .map_err(|e| generic(e.message))?;
        let no_local = match frame.header(names::NO_LOCAL) {
            Some(v) => v.eq_ignore_ascii_case("true"),
            None => frame
                .header(names::ACTIVEMQ_NO_LOCAL)
                .is_some_and(|v| v.eq_ignore_ascii_case("true")),
        };
        let window = match frame
            .header(names::CONSUMER_WINDOW_SIZE)
            .or_else(|| frame.header(names::ACTIVEMQ_PREFETCH_SIZE))
        {
            Some(v) => Some(
                v.parse::<i32>()
                    .map_err(|_| generic(format!("For input string: \"{v}\"")))?,
            ),
            None => None,
        };
        // validateSelector
        if let Some(s) = &selector {
            if s.is_empty() {
                return Err(StompError::bundle("Selector cannot be empty"));
            }
            if let Err(e) = saranga_filter::Filter::parse(s) {
                return Err(StompError::caused(
                    format!("Invalid selector \"{s}\""),
                    e.to_string(),
                ));
            }
        }
        self.check_auto_create(destination.as_deref(), routing_type)
            .await?;
        let subscription_id = match (id, &destination) {
            (Some(id), _) => id,
            (None, Some(d)) => format!("subscription/{d}"),
            (None, None) => {
                return Err(StompError::bundle(
                    "AMQ339015: Client must set destination or id header to a SUBSCRIBE command",
                ));
            }
        };
        let destination = destination.unwrap_or_default();
        // getSelector: no-local subscriptions skip this connection's sends.
        let selector = if no_local {
            let cid = convert::CONNECTION_ID_PROPERTY;
            let filter = format!("({cid} <> '{}' OR {cid} IS NULL)", self.id);
            Some(match selector {
                None => filter,
                Some(s) => format!("({s}) AND {filter}"),
            })
        } else {
            selector
        };
        let window = match window {
            Some(w) => w,
            None if ack == AckMode::Auto => -1,
            None => self.settings.consumer_window_size,
        };
        let consumer_id = self.broker.post_office().ids().next();
        let client_id = self.client.client_id();
        let out = self.out.clone();
        let events = self.events.clone();
        let kind = self.kind();
        let broker = self.broker.clone();
        let session = self.session().await?;
        if session
            .subscriptions
            .values()
            .any(|s| s.id == subscription_id)
        {
            return Err(StompError::plain(format!(
                "There already is a subscription for: {subscription_id}. Either use unique subscription IDs or do not create multiple subscriptions for the same destination"
            )));
        }
        let creating = |e: String| {
            StompError::bundle(format!(
                "AMQ339016: Error creating subscription {subscription_id}"
            ))
            .with_log(e)
        };
        // addSubscription
        let (bare, fqqn_queue) = split_fqqn(&destination);
        let multicast = broker
            .post_office()
            .address_info(session.session.remove_prefix(bare))
            .is_some_and(|i| i.routing_types.multicast && !i.routing_types.anycast);
        let mut queue_name = destination.clone();
        if multicast && fqqn_queue.is_none() {
            match &durable_name {
                Some(name) => {
                    let Some(client_id) = client_id else {
                        return Err(creating(
                            "AMQ339027: Cannot create a subscriber on the durable subscription if the client-id of the connection is not set".into(),
                        ));
                    };
                    queue_name = format!("{client_id}.{name}");
                    if broker.post_office().queue(&queue_name).is_none() {
                        let mut spec = QueueSpec::new(queue_name.as_str(), destination.as_str());
                        spec.filter_string = selector.clone();
                        match session.session.create_queue(spec) {
                            Ok(_) => {}
                            Err(e) if e.kind == saranga_core_protocol::ErrorType::QueueExists => {}
                            Err(e) => return Err(creating(e.message)),
                        }
                    }
                }
                None => {
                    queue_name = uuid::Uuid::new_v4().to_string();
                    let mut spec = QueueSpec::new(queue_name.as_str(), destination.as_str());
                    spec.filter_string = selector.clone();
                    spec.durable = false;
                    spec.temporary = true;
                    session
                        .session
                        .create_queue(spec)
                        .map_err(|e| creating(e.message))?;
                }
            }
        }
        let subscription = Arc::new(Subscription {
            id: subscription_id.clone(),
            ack,
            queue: queue_name.clone(),
            multicast,
            no_local,
            window,
            closed: AtomicBool::new(false),
        });
        let sink = Arc::new(StompSink {
            out,
            events,
            subscription: subscription.clone(),
            kind,
            to_ack: session.to_ack.clone(),
        });
        let filter = if multicast { None } else { selector };
        session
            .session
            .create_consumer(
                consumer_id,
                &queue_name,
                filter,
                0,
                false,
                move |_: &Queue| sink as Arc<dyn DeliverySink>,
            )
            .await
            .map_err(|e| creating(e.message))?;
        if no_local {
            session.no_local += 1;
        }
        session.subscriptions.insert(consumer_id, subscription);
        session.session.set_started(true).await;
        // The credits flow once the receipt is out; a window of 0 still
        // gets one credit (`startSlowConsumer`).
        let credits = if window == 0 { 1 } else { window };
        Ok(session
            .session
            .credit_granter(consumer_id)
            .map(|grant| Box::new(move || grant(credits)) as Box<dyn FnOnce() + Send>))
    }

    /// `onUnsubscribe` of each version, and `StompSession.unsubscribe`.
    async fn on_unsubscribe(&mut self, frame: &Frame) -> StompResult<Option<Frame>> {
        let id = frame.header(names::ID).map(str::to_owned);
        let durable_name = frame
            .header(names::DURABLE_SUBSCRIBER_NAME)
            .or_else(|| frame.header(names::DURABLE_SUBSCRIPTION_NAME))
            .or_else(|| frame.header(names::ACTIVEMQ_DURABLE_SUBSCRIPTION_NAME))
            .map(str::to_owned);
        let subscription_id = match (self.handler, id) {
            (_, Some(id)) => Some(id),
            (Version::V10, None) => match self.destination(frame, names::DESTINATION) {
                Some(d) => Some(format!("subscription/{d}")),
                None => {
                    return Err(StompError::bundle(
                        "AMQ339035: Must specify the subscription's id or the destination you are unsubscribing from",
                    ));
                }
            },
            (_, None) if durable_name.is_none() => {
                return Err(StompError::bundle(
                    "AMQ339037: Must specify the subscription's id",
                ));
            }
            (_, None) => None,
        };
        let client_id = self.client.client_id();
        let broker = self.broker.clone();
        let unsubscribing = |e: String| {
            StompError::bundle(format!(
                "AMQ339017: Error unsubscribing {}",
                subscription_id.as_deref().unwrap_or("null")
            ))
            .with_log(e)
        };
        let session = self.session().await?;
        let mut found = false;
        let matching: Vec<i64> = session
            .subscriptions
            .iter()
            .filter(|(_, s)| subscription_id.as_deref() == Some(s.id.as_str()))
            .map(|(&c, _)| c)
            .collect();
        for consumer in matching {
            let sub = session.subscriptions.remove(&consumer).expect("listed");
            sub.closed.store(true, Ordering::SeqCst);
            if sub.no_local {
                session.no_local -= 1;
            }
            session.session.close_consumer(consumer, false).await;
            let queue = broker.post_office().queue(&sub.queue);
            if sub.multicast
                && durable_name.is_none()
                && queue.is_some_and(|q| !q.is_durable())
                && let Err(e) = session.session.delete_queue(&sub.queue)
                && e.kind != saranga_core_protocol::ErrorType::QueueDoesNotExist
            {
                return Err(unsubscribing(e.message));
            }
            found = true;
        }
        if let (Some(name), Some(client_id)) = (&durable_name, &client_id) {
            let queue_name = format!("{client_id}.{name}");
            if broker.post_office().queue(&queue_name).is_some() {
                session
                    .session
                    .delete_queue(&queue_name)
                    .map_err(|e| unsubscribing(e.message))?;
            }
            found = true;
        }
        if !found {
            return Err(StompError::plain(format!(
                "Cannot unsubscribe as no subscription exists for id: {}",
                subscription_id.as_deref().unwrap_or("null")
            )));
        }
        Ok(None)
    }

    /// `destroy` and `StompProtocolManager.cleanup`: the connection's
    /// session is stopped, rolled back and closed, its transacted sessions
    /// rolled back and closed, and its temporary queues removed.
    async fn destroy(&mut self) {
        self.out.destroyed.store(true, Ordering::SeqCst);
        let mut completion = Completion::default();
        if let Some(mut s) = self.main.take() {
            for sub in s.subscriptions.values() {
                sub.closed.store(true, Ordering::SeqCst);
            }
            s.session.set_started(false).await;
            s.session.rollback(true, false).await;
            s.session.close(false).await;
            completion.merge(s.session.take_completion());
        }
        for (_, mut s) in self.transacted.drain() {
            s.session.rollback(false, false).await;
            s.session.close(false).await;
            completion.merge(s.session.take_completion());
        }
        self.client.remove_temp_queues(&self.broker);
        if let Err(e) = completion.wait().await {
            tracing::warn!("AMQ222069: error cleaning up stomp connection: {e}");
        }
        let _ = self.out.tx.send(Out::Close);
    }
}

impl StompError {
    /// Logs the cause of an error whose message does not carry it.
    fn with_log(self, cause: String) -> Self {
        tracing::debug!("{}: {cause}", self.message);
        self
    }
}

/// A refused login.
fn security_error(e: SecurityError) -> StompError {
    let e = BrokerError::security(e);
    StompError {
        security: true,
        ..StompError::plain(e.message)
    }
}

/// The start of `CoreMessage.toString`, for `Error sending message`.
fn describe(
    message_id: i64,
    durable: bool,
    address: Option<&saranga_buffer::SimpleString>,
) -> String {
    format!(
        "CoreMessage[messageID={}, durable={}, address={}]",
        message_id,
        durable,
        address
            .map(|a| a.to_string_lossy())
            .unwrap_or_else(|| "null".into())
    )
}
