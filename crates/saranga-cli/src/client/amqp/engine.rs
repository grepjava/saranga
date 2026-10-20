//! The AMQP 1.0 connection under Qpid JMS's provider, as Proton-J runs it
//! for `AmqpProvider`: one task owns the socket and the state of the
//! connection, its sessions and their links; the provider's resources
//! talk to it through handles. Sessions and links are numbered as
//! Proton-J numbers them (the lowest free channel and handle).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

use saranga_protocol_amqp::frame::{self, FrameBody};
use saranga_protocol_amqp::messaging::DeliveryState;
use saranga_protocol_amqp::transport::{
    Attach, Begin, Close, Detach, Disposition, End, Error as AmqpCondition, Flow, Open,
    Performative, Role, Transfer,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

/// A byte stream the connection runs on: TCP, or TLS over it.
pub trait Io: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Io for T {}

/// Why an operation failed.
#[derive(Debug, Clone, PartialEq)]
pub enum Failure {
    /// The peer closed the connection, the session or the link, with the
    /// error it gave, if any.
    Remote(Scope, Option<AmqpCondition>),
    /// The transport failed: its message.
    Io(String),
    /// The connection is closed (by this end, or it failed before).
    Closed,
}

/// What the peer ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Connection,
    Session,
    Link,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Remote(_, Some(e)) => match &e.description {
                Some(d) => write!(f, "{d} [condition = {}]", e.condition),
                None => write!(f, "[condition = {}]", e.condition),
            },
            Self::Remote(scope, None) => write!(f, "{scope:?} closed remotely"),
            Self::Io(m) => f.write_str(m),
            Self::Closed => f.write_str("The connection is closed"),
        }
    }
}

pub type Result<T> = std::result::Result<T, Failure>;

/// What happens on a link, for its owner.
#[derive(Debug)]
pub enum LinkEvent {
    /// A whole delivery arrived (on a receiver).
    Delivery(Delivery),
    /// The peer's flow for this link.
    Flow,
    /// A drain this end asked for is done: no credit is left.
    Drained,
    /// The peer detached the link.
    Detached {
        closed: bool,
        error: Option<AmqpCondition>,
    },
    /// The session or connection under the link ended.
    Failed(Failure),
}

/// A delivery received on a link.
#[derive(Debug, Clone)]
pub struct Delivery {
    pub delivery_id: u32,
    pub settled: bool,
    pub state: Option<DeliveryState>,
    /// The message's bytes, its frames joined.
    pub payload: Vec<u8>,
}

/// The settings this end opens the connection with, and what the peer
/// answered.
#[derive(Debug, Clone)]
pub struct Opened {
    pub local: Open,
    pub remote: Open,
}

/// Where a sent delivery's outcome goes.
type Outcome = oneshot::Sender<Result<DeliveryState>>;

enum Cmd {
    Begin {
        begin: Begin,
        reply: oneshot::Sender<Result<(u16, Begin)>>,
    },
    End {
        channel: u16,
        error: Option<AmqpCondition>,
        reply: oneshot::Sender<Result<()>>,
    },
    Attach {
        channel: u16,
        attach: Box<Attach>,
        events: mpsc::UnboundedSender<LinkEvent>,
        reply: oneshot::Sender<Result<(u32, Attach)>>,
    },
    Detach {
        channel: u16,
        handle: u32,
        closed: bool,
        error: Option<AmqpCondition>,
        reply: oneshot::Sender<Result<()>>,
    },
    Flow {
        channel: u16,
        handle: u32,
        credit: u32,
        drain: bool,
    },
    Send {
        channel: u16,
        handle: u32,
        tag: Vec<u8>,
        message_format: u32,
        payload: Vec<u8>,
        settled: bool,
        state: Option<DeliveryState>,
        /// For an unsettled send: the peer's outcome.
        outcome: Option<oneshot::Sender<Result<DeliveryState>>>,
    },
    Disposition {
        channel: u16,
        first: u32,
        last: u32,
        settled: bool,
        state: Option<DeliveryState>,
    },
    Close {
        error: Option<AmqpCondition>,
        timeout: Duration,
        reply: oneshot::Sender<Result<()>>,
    },
}

/// The handle the provider holds on the connection.
#[derive(Clone)]
pub struct Connection {
    tx: mpsc::UnboundedSender<Cmd>,
}

/// A session: its local channel.
#[derive(Clone)]
pub struct Session {
    tx: mpsc::UnboundedSender<Cmd>,
    pub channel: u16,
}

/// A link: its session's channel and its local handle.
pub struct Link {
    tx: mpsc::UnboundedSender<Cmd>,
    pub channel: u16,
    pub handle: u32,
    /// The peer's attach.
    pub remote: Attach,
    pub events: mpsc::UnboundedReceiver<LinkEvent>,
}

async fn ask<T>(
    tx: &mpsc::UnboundedSender<Cmd>,
    make: impl FnOnce(oneshot::Sender<Result<T>>) -> Cmd,
) -> Result<T> {
    let (reply, rx) = oneshot::channel();
    tx.send(make(reply)).map_err(|_| Failure::Closed)?;
    rx.await.unwrap_or(Err(Failure::Closed))
}

impl Connection {
    /// Starts the connection's task on a stream whose open handshake is
    /// done. `max_frame` is the smaller of the two ends' frame sizes.
    pub fn start(io: Box<dyn Io>, opened: Opened, leftover: Vec<u8>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let state = State::new(&opened);
        tokio::spawn(run(io, rx, state, leftover));
        Self { tx }
    }

    /// `session.begin()`, waiting for the peer's begin.
    pub async fn begin(&self, begin: Begin) -> Result<Session> {
        let (channel, _remote) = ask(&self.tx, |reply| Cmd::Begin { begin, reply }).await?;
        Ok(Session {
            tx: self.tx.clone(),
            channel,
        })
    }

    /// `connection.close()`: the close sent, the peer's awaited for
    /// `timeout`.
    pub async fn close(&self, error: Option<AmqpCondition>, timeout: Duration) -> Result<()> {
        ask(&self.tx, |reply| Cmd::Close {
            error,
            timeout,
            reply,
        })
        .await
    }

    /// Whether the task still runs.
    pub fn is_open(&self) -> bool {
        !self.tx.is_closed()
    }
}

impl Session {
    /// Attaches a link and waits for the peer's attach.
    pub async fn attach(&self, attach: Attach) -> Result<Link> {
        let (events_tx, events) = mpsc::unbounded_channel();
        let channel = self.channel;
        let (handle, remote) = ask(&self.tx, |reply| Cmd::Attach {
            channel,
            attach: Box::new(attach),
            events: events_tx,
            reply,
        })
        .await?;
        Ok(Link {
            tx: self.tx.clone(),
            channel,
            handle,
            remote,
            events,
        })
    }

    /// `session.close()`: the end sent, the peer's awaited.
    pub async fn end(&self, error: Option<AmqpCondition>) -> Result<()> {
        let channel = self.channel;
        ask(&self.tx, |reply| Cmd::End {
            channel,
            error,
            reply,
        })
        .await
    }
}

impl Link {
    /// Grants the sender `credit` (replacing what is left), draining it
    /// if `drain`.
    pub fn flow(&self, credit: u32, drain: bool) {
        let _ = self.tx.send(Cmd::Flow {
            channel: self.channel,
            handle: self.handle,
            credit,
            drain,
        });
    }

    /// Sends a message, once there is credit. Unsettled, the returned
    /// receiver gives the peer's outcome.
    pub fn send(
        &self,
        tag: Vec<u8>,
        payload: Vec<u8>,
        settled: bool,
        state: Option<DeliveryState>,
    ) -> Option<oneshot::Receiver<Result<DeliveryState>>> {
        let (outcome, rx) = if settled {
            (None, None)
        } else {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        };
        let _ = self.tx.send(Cmd::Send {
            channel: self.channel,
            handle: self.handle,
            tag,
            message_format: 0,
            payload,
            settled,
            state,
            outcome,
        });
        rx
    }

    /// Settles (or updates) received deliveries `first..=last`.
    pub fn disposition(&self, first: u32, last: u32, settled: bool, state: Option<DeliveryState>) {
        let _ = self.tx.send(Cmd::Disposition {
            channel: self.channel,
            first,
            last,
            settled,
            state,
        });
    }

    /// Detaches the link (`closed`: closes it), waiting for the peer's
    /// detach.
    pub async fn detach(&self, closed: bool, error: Option<AmqpCondition>) -> Result<()> {
        let (channel, handle) = (self.channel, self.handle);
        ask(&self.tx, |reply| Cmd::Detach {
            channel,
            handle,
            closed,
            error,
            reply,
        })
        .await
    }
}

/// A link's state.
struct LinkState {
    name: String,
    role: Role,
    remote_handle: Option<u32>,
    attach_reply: Option<oneshot::Sender<Result<(u32, Attach)>>>,
    detach_reply: Option<oneshot::Sender<Result<()>>>,
    /// Detached by this end, waiting for the peer.
    detaching: bool,
    events: mpsc::UnboundedSender<LinkEvent>,
    delivery_count: u32,
    credit: u32,
    drain: bool,
    /// Sends waiting for credit or session window.
    pending: VecDeque<PendingSend>,
    /// A delivery whose frames are still arriving.
    partial: Option<Delivery>,
}

struct PendingSend {
    tag: Vec<u8>,
    message_format: u32,
    payload: Vec<u8>,
    settled: bool,
    state: Option<DeliveryState>,
    outcome: Option<oneshot::Sender<Result<DeliveryState>>>,
}

/// A session's state.
struct SessionState {
    remote_channel: Option<u16>,
    begin_reply: Option<oneshot::Sender<Result<(u16, Begin)>>>,
    end_reply: Option<oneshot::Sender<Result<()>>>,
    ending: bool,
    next_outgoing_id: u32,
    next_incoming_id: u32,
    incoming_window: u32,
    outgoing_window: u32,
    remote_incoming_window: u32,
    remote_outgoing_window: u32,
    next_delivery_id: u32,
    links: BTreeMap<u32, LinkState>,
    /// The peer's handles, to the local ones.
    remote_handles: HashMap<u32, u32>,
    /// Sent deliveries not settled by the peer: their link and outcome.
    unsettled: BTreeMap<u32, (u32, Option<Outcome>)>,
}

struct State {
    max_frame: usize,
    sessions: BTreeMap<u16, SessionState>,
    /// The peer's channels, to the local ones.
    remote_channels: HashMap<u16, u16>,
    close_reply: Option<oneshot::Sender<Result<()>>>,
    closed: Option<Failure>,
    remote_idle: Option<Duration>,
    local_idle: Option<Duration>,
    channel_max: u16,
}

impl State {
    fn new(opened: &Opened) -> Self {
        let max_frame = opened
            .local
            .max_frame_size
            .min(opened.remote.max_frame_size)
            .max(frame::MIN_MAX_FRAME_SIZE) as usize;
        let idle = |ms: Option<u32>| {
            ms.filter(|m| *m > 0)
                .map(|m| Duration::from_millis(u64::from(m)))
        };
        Self {
            max_frame,
            sessions: BTreeMap::new(),
            remote_channels: HashMap::new(),
            close_reply: None,
            closed: None,
            remote_idle: idle(opened.remote.idle_time_out),
            local_idle: idle(opened.local.idle_time_out),
            channel_max: opened.local.channel_max.min(opened.remote.channel_max),
        }
    }
}

/// Reads frames off the socket for the connection's task.
async fn read_frames(
    mut reader: tokio::io::ReadHalf<Box<dyn Io>>,
    mut buf: Vec<u8>,
    max_frame: usize,
    tx: mpsc::UnboundedSender<std::result::Result<frame::Frame, String>>,
) {
    let mut chunk = vec![0u8; 64 * 1024];
    loop {
        while let Some(size) = frame::frame_size(&buf) {
            let size = size as usize;
            if size < 8 || size > max_frame.max(frame::MIN_MAX_FRAME_SIZE as usize) {
                let _ = tx.send(Err(format!("frame of {size} bytes")));
                return;
            }
            if buf.len() < size {
                break;
            }
            let bytes: Vec<u8> = buf.drain(..size).collect();
            match frame::decode(&bytes) {
                Ok(f) => {
                    if tx.send(Ok(f)).is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e.to_string()));
                    return;
                }
            }
        }
        match reader.read(&mut chunk).await {
            Ok(0) => {
                let _ = tx.send(Err("Transport closed".to_owned()));
                return;
            }
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) => {
                let _ = tx.send(Err(e.to_string()));
                return;
            }
        }
    }
}

async fn run(
    io: Box<dyn Io>,
    mut commands: mpsc::UnboundedReceiver<Cmd>,
    mut state: State,
    leftover: Vec<u8>,
) {
    let (reader, mut writer) = tokio::io::split(io);
    let (frames_tx, mut frames) = mpsc::unbounded_channel();
    // This end's max-frame-size bounds what it reads.
    tokio::spawn(read_frames(reader, leftover, usize::MAX, frames_tx));
    let mut out = Vec::new();
    let mut last_write = tokio::time::Instant::now();
    let mut last_read = tokio::time::Instant::now();
    let mut close_deadline: Option<tokio::time::Instant> = None;
    let tick = state
        .remote_idle
        .map(|d| d / 2)
        .into_iter()
        .chain(state.local_idle)
        .min()
        .unwrap_or(Duration::from_secs(3600))
        .max(Duration::from_millis(10));
    let mut ticker = tokio::time::interval(tick);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            cmd = commands.recv(), if state.closed.is_none() => {
                match cmd {
                    Some(cmd) => {
                        if let Cmd::Close { timeout, .. } = &cmd {
                            close_deadline = Some(tokio::time::Instant::now() + *timeout);
                        }
                        command(&mut state, cmd, &mut out);
                    }
                    // Every handle dropped: close.
                    None => {
                        frame::encode_amqp(&mut out, 0, &Performative::Close(Close::default()), b"");
                        let _ = writer.write_all(&out).await;
                        let _ = writer.shutdown().await;
                        return;
                    }
                }
            }
            f = frames.recv() => {
                last_read = tokio::time::Instant::now();
                match f {
                    Some(Ok(f)) => incoming(&mut state, f, &mut out),
                    Some(Err(e)) => fail(&mut state, Failure::Io(e)),
                    None => fail(&mut state, Failure::Io("Transport closed".to_owned())),
                }
            }
            _ = ticker.tick() => {
                let now = tokio::time::Instant::now();
                if let Some(idle) = state.local_idle
                    && now.duration_since(last_read) >= idle
                    && state.closed.is_none()
                {
                    let error = AmqpCondition::new(
                        "amqp:resource-limit-exceeded",
                        Some("local-idle-timeout expired".to_owned()),
                    );
                    frame::encode_amqp(&mut out, 0, &Performative::Close(Close { error: Some(error) }), b"");
                    fail(&mut state, Failure::Io("Transport closed due to the peer exceeding our requested idle-timeout".to_owned()));
                }
                if let Some(idle) = state.remote_idle
                    && out.is_empty()
                    && now.duration_since(last_write) >= idle / 2
                {
                    frame::encode_empty(&mut out);
                }
                if let Some(deadline) = close_deadline
                    && now >= deadline
                    && let Some(reply) = state.close_reply.take()
                {
                    let _ = reply.send(Ok(()));
                    fail(&mut state, Failure::Closed);
                }
            }
        }
        if !out.is_empty() {
            if writer.write_all(&out).await.is_err() || writer.flush().await.is_err() {
                fail(&mut state, Failure::Io("Transport failed".to_owned()));
            }
            out.clear();
            last_write = tokio::time::Instant::now();
        }
        if state.closed.is_some() && state.close_reply.is_none() {
            let _ = writer.shutdown().await;
            // Later commands fail at once.
            while let Ok(cmd) = commands.try_recv() {
                refuse(cmd, state.closed.clone().unwrap_or(Failure::Closed));
            }
            drop(commands);
            return;
        }
    }
}

/// Answers a command the closed connection cannot run.
fn refuse(cmd: Cmd, failure: Failure) {
    match cmd {
        Cmd::Begin { reply, .. } => {
            let _ = reply.send(Err(failure));
        }
        Cmd::End { reply, .. } | Cmd::Detach { reply, .. } => {
            let _ = reply.send(Err(failure));
        }
        Cmd::Attach { reply, .. } => {
            let _ = reply.send(Err(failure));
        }
        Cmd::Close { reply, .. } => {
            let _ = reply.send(Ok(()));
        }
        Cmd::Send {
            outcome: Some(o), ..
        } => {
            let _ = o.send(Err(failure));
        }
        _ => {}
    }
}

/// Fails everything waiting, once.
fn fail(state: &mut State, failure: Failure) {
    if state.closed.is_some() {
        return;
    }
    state.closed = Some(failure.clone());
    for (_, s) in std::mem::take(&mut state.sessions) {
        fail_session(s, &failure);
    }
    if let Some(reply) = state.close_reply.take() {
        // A close this end asked for is done once the transport is.
        let _ = reply.send(Ok(()));
    }
}

fn fail_session(s: SessionState, failure: &Failure) {
    if let Some(r) = s.begin_reply {
        let _ = r.send(Err(failure.clone()));
    }
    if let Some(r) = s.end_reply {
        let _ = r.send(Ok(()));
    }
    for (_, l) in s.links {
        fail_link(l, failure);
    }
    for (_, (_, outcome)) in s.unsettled {
        if let Some(o) = outcome {
            let _ = o.send(Err(failure.clone()));
        }
    }
}

fn fail_link(l: LinkState, failure: &Failure) {
    if let Some(r) = l.attach_reply {
        let _ = r.send(Err(failure.clone()));
    }
    if let Some(r) = l.detach_reply {
        let _ = r.send(Ok(()));
    }
    for p in l.pending {
        if let Some(o) = p.outcome {
            let _ = o.send(Err(failure.clone()));
        }
    }
    let _ = l.events.send(LinkEvent::Failed(failure.clone()));
}

fn emit(out: &mut Vec<u8>, channel: u16, p: Performative) {
    frame::encode_amqp(out, channel, &p, b"");
}

fn command(state: &mut State, cmd: Cmd, out: &mut Vec<u8>) {
    if let Some(f) = &state.closed {
        refuse(cmd, f.clone());
        return;
    }
    match cmd {
        Cmd::Begin { mut begin, reply } => {
            // TransportImpl: the lowest free channel.
            let Some(channel) = (0..=state.channel_max).find(|c| !state.sessions.contains_key(c))
            else {
                let _ = reply.send(Err(Failure::Io("no free channel".to_owned())));
                return;
            };
            begin.remote_channel = None;
            let s = SessionState {
                remote_channel: None,
                begin_reply: Some(reply),
                end_reply: None,
                ending: false,
                next_outgoing_id: begin.next_outgoing_id,
                next_incoming_id: 0,
                incoming_window: begin.incoming_window,
                outgoing_window: begin.outgoing_window,
                remote_incoming_window: 0,
                remote_outgoing_window: 0,
                next_delivery_id: 0,
                links: BTreeMap::new(),
                remote_handles: HashMap::new(),
                unsettled: BTreeMap::new(),
            };
            state.sessions.insert(channel, s);
            emit(out, channel, Performative::Begin(begin));
        }
        Cmd::End {
            channel,
            error,
            reply,
        } => match state.sessions.get_mut(&channel) {
            Some(s) if !s.ending => {
                s.ending = true;
                s.end_reply = Some(reply);
                emit(out, channel, Performative::End(End { error }));
            }
            _ => {
                let _ = reply.send(Ok(()));
            }
        },
        Cmd::Attach {
            channel,
            mut attach,
            events,
            reply,
        } => {
            let Some(s) = state.sessions.get_mut(&channel) else {
                let _ = reply.send(Err(Failure::Closed));
                return;
            };
            let handle = (0..).find(|h| !s.links.contains_key(h)).unwrap_or(0);
            attach.handle = handle;
            if attach.role == Role::Sender && attach.initial_delivery_count.is_none() {
                attach.initial_delivery_count = Some(0);
            }
            s.links.insert(
                handle,
                LinkState {
                    name: attach.name.clone(),
                    role: attach.role,
                    remote_handle: None,
                    attach_reply: Some(reply),
                    detach_reply: None,
                    detaching: false,
                    events,
                    delivery_count: attach.initial_delivery_count.unwrap_or(0),
                    credit: 0,
                    drain: false,
                    pending: VecDeque::new(),
                    partial: None,
                },
            );
            emit(out, channel, Performative::Attach(*attach));
        }
        Cmd::Detach {
            channel,
            handle,
            closed,
            error,
            reply,
        } => {
            let Some(l) = state
                .sessions
                .get_mut(&channel)
                .and_then(|s| s.links.get_mut(&handle))
            else {
                let _ = reply.send(Ok(()));
                return;
            };
            if l.detaching {
                let _ = reply.send(Ok(()));
                return;
            }
            l.detaching = true;
            l.detach_reply = Some(reply);
            emit(
                out,
                channel,
                Performative::Detach(Detach {
                    handle,
                    closed,
                    error,
                }),
            );
        }
        Cmd::Flow {
            channel,
            handle,
            credit,
            drain,
        } => {
            let Some(s) = state.sessions.get_mut(&channel) else {
                return;
            };
            let Some(l) = s.links.get_mut(&handle) else {
                return;
            };
            l.credit = credit;
            l.drain = drain;
            let flow = link_flow(s, handle);
            emit(out, channel, Performative::Flow(flow));
        }
        Cmd::Send {
            channel,
            handle,
            tag,
            message_format,
            payload,
            settled,
            state: delivery_state,
            outcome,
        } => {
            let max_frame = state.max_frame;
            let Some(s) = state.sessions.get_mut(&channel) else {
                if let Some(o) = outcome {
                    let _ = o.send(Err(Failure::Closed));
                }
                return;
            };
            let Some(l) = s.links.get_mut(&handle) else {
                if let Some(o) = outcome {
                    let _ = o.send(Err(Failure::Closed));
                }
                return;
            };
            l.pending.push_back(PendingSend {
                tag,
                message_format,
                payload,
                settled,
                state: delivery_state,
                outcome,
            });
            pump(s, channel, handle, max_frame, out);
        }
        Cmd::Disposition {
            channel,
            first,
            last,
            settled,
            state: delivery_state,
        } => {
            if state.sessions.contains_key(&channel) {
                emit(
                    out,
                    channel,
                    Performative::Disposition(Disposition {
                        role: Role::Receiver,
                        first,
                        last: (last != first).then_some(last),
                        settled,
                        state: delivery_state,
                        batchable: false,
                    }),
                );
            }
        }
        Cmd::Close { error, reply, .. } => {
            state.close_reply = Some(reply);
            emit(out, 0, Performative::Close(Close { error }));
        }
    }
}

/// The session's state with the link's, for a flow on the link.
fn link_flow(s: &SessionState, handle: u32) -> Flow {
    let l = &s.links[&handle];
    Flow {
        next_incoming_id: s.remote_channel.map(|_| s.next_incoming_id),
        incoming_window: s.incoming_window,
        next_outgoing_id: s.next_outgoing_id,
        outgoing_window: s.outgoing_window,
        handle: Some(handle),
        delivery_count: Some(l.delivery_count),
        link_credit: Some(l.credit),
        available: None,
        drain: l.drain,
        echo: false,
        properties: None,
    }
}

/// Sends what the link's credit and the peer's window allow.
fn pump(s: &mut SessionState, channel: u16, handle: u32, max_frame: usize, out: &mut Vec<u8>) {
    loop {
        let Some(l) = s.links.get_mut(&handle) else {
            return;
        };
        if l.remote_handle.is_none()
            || l.detaching
            || l.credit == 0
            || s.remote_incoming_window == 0
        {
            return;
        }
        let Some(p) = l.pending.pop_front() else {
            return;
        };
        l.credit -= 1;
        l.delivery_count = l.delivery_count.wrapping_add(1);
        let delivery_id = s.next_delivery_id;
        s.next_delivery_id = s.next_delivery_id.wrapping_add(1);
        if !p.settled {
            s.unsettled.insert(delivery_id, (handle, p.outcome));
        }
        // The frames of the message, each as large as the frame size lets
        // it be.
        let mut rest: &[u8] = &p.payload;
        let mut first = true;
        loop {
            let mut transfer = Transfer {
                handle,
                delivery_id: first.then_some(delivery_id),
                delivery_tag: first.then(|| p.tag.clone()),
                message_format: first.then_some(p.message_format),
                settled: (first && p.settled).then_some(true),
                more: false,
                rcv_settle_mode: None,
                state: if first { p.state.clone() } else { None },
                resume: false,
                aborted: false,
                batchable: false,
            };
            let mut head = Vec::new();
            Performative::Transfer(transfer.clone()).encode(&mut head);
            // More frames will follow: the flag takes a byte at most.
            let room = max_frame.saturating_sub(8 + head.len() + 1).max(1);
            let (chunk, after) = rest.split_at(rest.len().min(room));
            transfer.more = !after.is_empty();
            frame::encode_amqp(out, channel, &Performative::Transfer(transfer), chunk);
            s.next_outgoing_id = s.next_outgoing_id.wrapping_add(1);
            s.remote_incoming_window = s.remote_incoming_window.saturating_sub(1);
            rest = after;
            first = false;
            if rest.is_empty() {
                break;
            }
        }
    }
}

fn incoming(state: &mut State, f: frame::Frame, out: &mut Vec<u8>) {
    let FrameBody::Amqp(p, payload) = f.body else {
        // A heartbeat, or a stray SASL frame.
        return;
    };
    let remote_channel = f.channel;
    match p {
        Performative::Open(_) => {}
        Performative::Close(c) => {
            if state.close_reply.is_some() {
                fail(state, Failure::Closed);
            } else {
                // Answer it, then fail what waits.
                emit(out, 0, Performative::Close(Close::default()));
                fail(state, Failure::Remote(Scope::Connection, c.error));
            }
        }
        Performative::Begin(b) => {
            let Some(local) = b.remote_channel else {
                // The peer began a session itself: refused.
                emit(
                    out,
                    remote_channel,
                    Performative::End(End {
                        error: Some(AmqpCondition::new("amqp:not-implemented", None)),
                    }),
                );
                return;
            };
            let Some(s) = state.sessions.get_mut(&local) else {
                return;
            };
            s.remote_channel = Some(remote_channel);
            s.next_incoming_id = b.next_outgoing_id;
            s.remote_incoming_window = b.incoming_window;
            s.remote_outgoing_window = b.outgoing_window;
            state.remote_channels.insert(remote_channel, local);
            if let Some(r) = s.begin_reply.take() {
                let _ = r.send(Ok((local, b)));
            }
        }
        Performative::End(e) => {
            let Some(local) = state.remote_channels.remove(&remote_channel) else {
                return;
            };
            let Some(s) = state.sessions.remove(&local) else {
                return;
            };
            if s.ending {
                let mut s = s;
                if let Some(r) = s.end_reply.take() {
                    let _ = r.send(Ok(()));
                }
                fail_session(s, &Failure::Closed);
            } else {
                emit(out, local, Performative::End(End::default()));
                fail_session(s, &Failure::Remote(Scope::Session, e.error));
            }
        }
        Performative::Attach(a) => {
            let max_frame = state.max_frame;
            let Some(&local) = state.remote_channels.get(&remote_channel) else {
                return;
            };
            let Some(s) = state.sessions.get_mut(&local) else {
                return;
            };
            // The link this end attached with that name and the other role.
            let found = s
                .links
                .iter()
                .find(|(_, l)| l.name == a.name && l.remote_handle.is_none() && l.role != a.role)
                .map(|(h, _)| *h);
            let Some(handle) = found else {
                return;
            };
            s.remote_handles.insert(a.handle, handle);
            let l = s.links.get_mut(&handle).expect("found");
            l.remote_handle = Some(a.handle);
            if l.role == Role::Receiver {
                l.delivery_count = a.initial_delivery_count.unwrap_or(0);
            }
            if let Some(r) = l.attach_reply.take() {
                let _ = r.send(Ok((handle, a)));
            }
            pump(s, local, handle, max_frame, out);
        }
        Performative::Detach(d) => {
            let Some(&local) = state.remote_channels.get(&remote_channel) else {
                return;
            };
            let Some(s) = state.sessions.get_mut(&local) else {
                return;
            };
            let Some(&handle) = s.remote_handles.get(&d.handle) else {
                return;
            };
            s.remote_handles.remove(&d.handle);
            let Some(mut l) = s.links.remove(&handle) else {
                return;
            };
            if l.detaching {
                if let Some(r) = l.detach_reply.take() {
                    let _ = r.send(Ok(()));
                }
                if let Some(r) = l.attach_reply.take() {
                    let _ = r.send(Err(Failure::Remote(Scope::Link, d.error.clone())));
                }
                let _ = l.events.send(LinkEvent::Detached {
                    closed: d.closed,
                    error: d.error,
                });
            } else {
                // Answered in kind; a refused attach fails.
                emit(
                    out,
                    local,
                    Performative::Detach(Detach {
                        handle,
                        closed: d.closed,
                        error: None,
                    }),
                );
                if let Some(r) = l.attach_reply.take() {
                    let _ = r.send(Err(Failure::Remote(Scope::Link, d.error.clone())));
                }
                let failure = Failure::Remote(Scope::Link, d.error.clone());
                for p in std::mem::take(&mut l.pending) {
                    if let Some(o) = p.outcome {
                        let _ = o.send(Err(failure.clone()));
                    }
                }
                let _ = l.events.send(LinkEvent::Detached {
                    closed: d.closed,
                    error: d.error,
                });
            }
            // Sends of the link not yet settled fail.
            let gone: Vec<u32> = s
                .unsettled
                .iter()
                .filter(|(_, (h, _))| *h == handle)
                .map(|(id, _)| *id)
                .collect();
            for id in gone {
                if let Some((_, Some(o))) = s.unsettled.remove(&id) {
                    let _ = o.send(Err(Failure::Remote(Scope::Link, None)));
                }
            }
        }
        Performative::Flow(fl) => {
            let max_frame = state.max_frame;
            let Some(&local) = state.remote_channels.get(&remote_channel) else {
                return;
            };
            let Some(s) = state.sessions.get_mut(&local) else {
                return;
            };
            // The peer's window: what it can still take from this end.
            s.remote_incoming_window = fl
                .next_incoming_id
                .unwrap_or(0)
                .wrapping_add(fl.incoming_window)
                .wrapping_sub(s.next_outgoing_id);
            s.remote_outgoing_window = fl.outgoing_window;
            let Some(remote_handle) = fl.handle else {
                let handles: Vec<u32> = s.links.keys().copied().collect();
                for h in handles {
                    pump(s, local, h, max_frame, out);
                }
                return;
            };
            let Some(&handle) = s.remote_handles.get(&remote_handle) else {
                return;
            };
            let l = s.links.get_mut(&handle).expect("mapped");
            match l.role {
                Role::Sender => {
                    // Its credit, counted from this end's delivery count.
                    let credit = fl
                        .delivery_count
                        .unwrap_or(0)
                        .wrapping_add(fl.link_credit.unwrap_or(0))
                        .wrapping_sub(l.delivery_count);
                    l.credit = credit;
                    l.drain = fl.drain;
                    let _ = l.events.send(LinkEvent::Flow);
                    pump(s, local, handle, max_frame, out);
                    let l = s.links.get_mut(&handle).expect("mapped");
                    if l.drain && l.pending.is_empty() && l.credit > 0 {
                        // Nothing to send: the credit is used up.
                        l.delivery_count = l.delivery_count.wrapping_add(l.credit);
                        l.credit = 0;
                        let flow = link_flow(s, handle);
                        emit(out, local, Performative::Flow(flow));
                    } else if fl.echo {
                        let flow = link_flow(s, handle);
                        emit(out, local, Performative::Flow(flow));
                    }
                }
                Role::Receiver => {
                    if let Some(count) = fl.delivery_count {
                        // The sender's count: credit it skipped (a drain)
                        // is gone.
                        let advanced = count.wrapping_sub(l.delivery_count);
                        l.credit = l.credit.saturating_sub(advanced);
                        l.delivery_count = count;
                    }
                    if l.drain && fl.link_credit == Some(0) {
                        l.drain = false;
                        l.credit = 0;
                        let _ = l.events.send(LinkEvent::Drained);
                    }
                    if fl.echo {
                        let flow = link_flow(s, handle);
                        emit(out, local, Performative::Flow(flow));
                    }
                }
            }
        }
        Performative::Transfer(t) => {
            let Some(&local) = state.remote_channels.get(&remote_channel) else {
                return;
            };
            let Some(s) = state.sessions.get_mut(&local) else {
                return;
            };
            s.next_incoming_id = s.next_incoming_id.wrapping_add(1);
            let Some(&handle) = s.remote_handles.get(&t.handle) else {
                return;
            };
            let l = s.links.get_mut(&handle).expect("mapped");
            let partial = l.partial.get_or_insert_with(|| Delivery {
                delivery_id: t.delivery_id.unwrap_or(0),
                settled: false,
                state: None,
                payload: Vec::new(),
            });
            if t.settled == Some(true) {
                partial.settled = true;
            }
            if t.state.is_some() {
                partial.state = t.state.clone();
            }
            partial.payload.extend_from_slice(&payload);
            if t.aborted {
                l.partial = None;
                return;
            }
            if t.more {
                return;
            }
            let delivery = l.partial.take().expect("assembled");
            l.delivery_count = l.delivery_count.wrapping_add(1);
            l.credit = l.credit.saturating_sub(1);
            let _ = l.events.send(LinkEvent::Delivery(delivery));
            if l.drain && l.credit == 0 {
                l.drain = false;
                let _ = l.events.send(LinkEvent::Drained);
            }
        }
        Performative::Disposition(d) => {
            let Some(&local) = state.remote_channels.get(&remote_channel) else {
                return;
            };
            let Some(s) = state.sessions.get_mut(&local) else {
                return;
            };
            if d.role != Role::Receiver {
                // About deliveries this end received.
                return;
            }
            let last = d.last.unwrap_or(d.first);
            let ids: Vec<u32> = s
                .unsettled
                .range(d.first..=last)
                .map(|(id, _)| *id)
                .collect();
            for id in ids {
                let outcome_state = d.state.clone().unwrap_or(DeliveryState::Accepted);
                if d.settled {
                    if let Some((_, Some(o))) = s.unsettled.remove(&id) {
                        let _ = o.send(Ok(outcome_state));
                    }
                } else if let Some((_, o)) = s.unsettled.get_mut(&id)
                    && !matches!(outcome_state, DeliveryState::Received(_))
                    && let Some(o) = o.take()
                {
                    // An outcome on an unsettled delivery: settled here.
                    let _ = o.send(Ok(outcome_state));
                    s.unsettled.remove(&id);
                    emit(
                        out,
                        local,
                        Performative::Disposition(Disposition {
                            role: Role::Sender,
                            first: id,
                            last: None,
                            settled: true,
                            state: None,
                            batchable: false,
                        }),
                    );
                }
            }
        }
    }
}
