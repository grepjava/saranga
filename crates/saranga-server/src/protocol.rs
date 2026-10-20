//! Protocols on an acceptor (`ProtocolHandler`, `ProtocolManager`). The
//! first 8 bytes a client sends pick the protocol among those the acceptor
//! enables; with no match the connection is Core, as in Artemis.

use std::any::Any;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use saranga_security::ConnectionInfo;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::{Acceptor, Broker};

/// A connection's byte stream: TCP, or TLS over TCP.
pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin + Any + 'static {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Stream for T {}

/// Splits a connection's stream into a half to read and a half to write
/// from different tasks. A plain TCP socket splits into tokio's owned
/// halves, which share no lock; `tokio::io::split` puts one around the
/// whole stream, which the reader and the writer then contend for on every
/// read and write. Anything else (TLS, a WebSocket) splits that way still.
pub fn split<S: AsyncRead + AsyncWrite + Send + 'static>(stream: S) -> (ReadPart, WritePart) {
    if !(&stream as &dyn Any).is::<Rewind>() {
        return locked(stream);
    }
    let any: Box<dyn Any + Send> = Box::new(stream);
    let Ok(rewind) = any.downcast::<Rewind>() else {
        unreachable!("the stream is a Rewind")
    };
    let Rewind { prefix, pos, inner } = *rewind;
    if !(&*inner as &dyn Any).is::<TcpStream>() {
        return locked(Rewind { prefix, pos, inner });
    }
    let inner: Box<dyn Any + Send> = inner;
    let Ok(tcp) = inner.downcast::<TcpStream>() else {
        unreachable!("the stream is a TcpStream")
    };
    let (rd, wr) = tcp.into_split();
    (
        ReadPart::Tcp { prefix, pos, rd },
        WritePart::Tcp(DirectWrite(Arc::new(TcpWrite(Some(wr))))),
    )
}

fn locked<S: AsyncRead + AsyncWrite + Send + 'static>(stream: S) -> (ReadPart, WritePart) {
    let (rd, wr) = tokio::io::split(stream);
    (
        ReadPart::Other(Box::new(rd)),
        WritePart::Other(Box::new(wr)),
    )
}

/// The half of a stream [`split`] gives to read.
pub enum ReadPart {
    /// A TCP socket, after the bytes read to detect the protocol.
    Tcp {
        prefix: Vec<u8>,
        pos: usize,
        rd: OwnedReadHalf,
    },
    Other(Box<dyn AsyncRead + Send + Unpin>),
}

/// The half of a stream [`split`] gives to write.
pub enum WritePart {
    Tcp(DirectWrite),
    Other(Box<dyn AsyncWrite + Send + Unpin>),
}

impl WritePart {
    /// A handle to write to the socket without this half, from another
    /// task, for a TCP socket. Whoever writes through it must know that no
    /// write through this half is under way, or the bytes interleave.
    pub fn direct(&self) -> Option<DirectWrite> {
        match self {
            WritePart::Tcp(d) => Some(d.clone()),
            WritePart::Other(_) => None,
        }
    }
}

/// What a connection's tasks hand its writer task. What the connection's
/// own task sends while the writer has nothing left to write goes straight
/// to the socket instead: the writer, on another thread, is not woken for
/// each response or acknowledgement, and the task has its own output
/// gathered by then. What other tasks send (a queue's deliveries) goes to
/// the writer still, which gathers what arrives meanwhile into one write:
/// written one at a time, each would cost the queue's task a system call,
/// and under load it fell behind its producers.
pub struct DirectOut {
    direct: Option<DirectWrite>,
    /// The connection's task, the one that made this.
    owner: Option<tokio::task::Id>,
    /// Items handed to the writer and not yet written: while it is not
    /// zero the writer may be writing, and what follows goes to it, to keep
    /// the order.
    queued: Arc<AtomicUsize>,
    /// Held to look at `queued` and write, by one sender at a time.
    sending: std::sync::Mutex<()>,
}

/// The most a task writes straight to the socket at once: about a
/// delivery, or a few acknowledgements. More is a task with a lot to send,
/// which goes on with its work faster with the writer, on another thread,
/// making the system calls.
const DIRECT_MAX: usize = 4 * 1024;

/// The writer's side of a [`DirectOut`].
pub struct Written(Arc<AtomicUsize>);

impl Written {
    /// `n` items handed over are written (or failed to be).
    pub fn done(&self, n: usize) {
        self.0.fetch_sub(n, Ordering::AcqRel);
    }
}

impl DirectOut {
    /// For the writer task that writes to `wr`, made by the connection's
    /// task.
    pub fn new(wr: &WritePart) -> (Self, Written) {
        let queued = Arc::new(AtomicUsize::new(0));
        let out = Self {
            direct: wr.direct(),
            owner: tokio::task::try_id(),
            queued: queued.clone(),
            sending: std::sync::Mutex::new(()),
        };
        (out, Written(queued))
    }

    /// Writes `bytes`, which wait for nothing: to the socket while nothing
    /// is queued and it takes them, and what it does not take to the
    /// writer, through `queue`, which says whether it was handed over.
    /// False when it was not.
    pub fn write(&self, bytes: Vec<u8>, queue: impl FnOnce(Vec<u8>) -> bool) -> bool {
        let _sending = self.sending.lock().unwrap_or_else(|e| e.into_inner());
        let mut from = 0;
        if let Some(direct) = &self.direct
            && self.owner.is_some()
            && tokio::task::try_id() == self.owner
            && bytes.len() <= DIRECT_MAX
            && self.queued.load(Ordering::Acquire) == 0
        {
            // An error or a full socket is the writer's to find.
            while from < bytes.len() {
                match direct.try_write(&bytes[from..]) {
                    Ok(n) if n > 0 => from += n,
                    _ => break,
                }
            }
            if from == bytes.len() {
                return true;
            }
        }
        let rest = if from == 0 {
            bytes
        } else {
            bytes[from..].to_vec()
        };
        self.queued.fetch_add(1, Ordering::AcqRel);
        let handed = queue(rest);
        if !handed {
            self.queued.fetch_sub(1, Ordering::AcqRel);
        }
        handed
    }

    /// Hands an item to the writer through `queue`, after what was written
    /// before it. False when it was not.
    pub fn queue(&self, queue: impl FnOnce() -> bool) -> bool {
        let _sending = self.sending.lock().unwrap_or_else(|e| e.into_inner());
        self.queued.fetch_add(1, Ordering::AcqRel);
        let handed = queue();
        if !handed {
            self.queued.fetch_sub(1, Ordering::AcqRel);
        }
        handed
    }
}

/// The write half of a TCP socket, shared.
#[derive(Clone)]
pub struct DirectWrite(Arc<TcpWrite>);

/// Not shut down when dropped, unlike an `OwnedWriteHalf`: the socket
/// closes once both halves are gone, as with `tokio::io::split`. Always
/// `Some` until dropped.
struct TcpWrite(Option<OwnedWriteHalf>);

impl Drop for TcpWrite {
    fn drop(&mut self) {
        if let Some(wr) = self.0.take() {
            wr.forget();
        }
    }
}

impl DirectWrite {
    fn stream(&self) -> &TcpStream {
        self.0
            .0
            .as_ref()
            .expect("a TcpWrite's half until it is dropped")
            .as_ref()
    }

    /// Writes what the socket takes now, without waiting
    /// (`WouldBlock` when it takes nothing).
    pub fn try_write(&self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream().try_write(buf)
    }
}

impl AsyncRead for ReadPart {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ReadPart::Tcp { prefix, pos, rd } => {
                if *pos < prefix.len() {
                    let n = (prefix.len() - *pos).min(buf.remaining());
                    buf.put_slice(&prefix[*pos..*pos + n]);
                    *pos += n;
                    if *pos == prefix.len() {
                        *prefix = Vec::new();
                        *pos = 0;
                    }
                    return Poll::Ready(Ok(()));
                }
                Pin::new(rd).poll_read(cx, buf)
            }
            ReadPart::Other(rd) => Pin::new(rd).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for WritePart {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            WritePart::Tcp(d) => {
                let s = d.stream();
                loop {
                    std::task::ready!(s.poll_write_ready(cx))?;
                    match s.try_write(buf) {
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                        r => return Poll::Ready(r),
                    }
                }
            }
            WritePart::Other(wr) => Pin::new(wr).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            WritePart::Tcp(_) => Poll::Ready(Ok(())),
            WritePart::Other(wr) => Pin::new(wr).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            WritePart::Tcp(d) => {
                Poll::Ready(socket2::SockRef::from(d.stream()).shutdown(std::net::Shutdown::Write))
            }
            WritePart::Other(wr) => Pin::new(wr).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            WritePart::Tcp(d) => {
                let s = d.stream();
                loop {
                    std::task::ready!(s.poll_write_ready(cx))?;
                    match s.try_write_vectored(bufs) {
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                        r => return Poll::Ready(r),
                    }
                }
            }
            WritePart::Other(wr) => Pin::new(wr).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            WritePart::Tcp(_) => true,
            WritePart::Other(wr) => wr.is_write_vectored(),
        }
    }
}

/// A stream that first gives back the bytes read to detect the protocol.
pub struct Rewind {
    prefix: Vec<u8>,
    pos: usize,
    inner: Box<dyn Stream>,
}

impl Rewind {
    pub fn new(prefix: Vec<u8>, inner: Box<dyn Stream>) -> Self {
        Self {
            prefix,
            pos: 0,
            inner,
        }
    }
}

impl AsyncRead for Rewind {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.pos < self.prefix.len() {
            let n = (self.prefix.len() - self.pos).min(buf.remaining());
            let start = self.pos;
            buf.put_slice(&self.prefix[start..start + n]);
            self.pos += n;
            if self.pos == self.prefix.len() {
                self.prefix = Vec::new();
                self.pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Rewind {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// A client connection handed to the protocol that recognised it.
pub struct Connection {
    /// The stream, starting with the bytes the protocol was detected from.
    pub stream: Rewind,
    pub peer: Option<SocketAddr>,
    /// The acceptor's end of the socket.
    pub local: Option<SocketAddr>,
    /// What security sees of the connection.
    pub info: ConnectionInfo,
    pub acceptor: Arc<Acceptor>,
}

pub type ServeFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A protocol the broker serves (`ProtocolManagerFactory` and
/// `ProtocolManager`).
pub trait ProtocolManager: Send + Sync + 'static {
    /// The name acceptors list it under in `protocols`: `CORE`, `AMQP`,
    /// `STOMP`, `MQTT`, `OPENWIRE`, `HORNETQ`.
    fn name(&self) -> &'static str;

    /// `isProtocol`: whether the first 8 bytes a client sent are this
    /// protocol's.
    fn is_protocol(&self, header: &[u8]) -> bool;

    /// `acceptsNoHandshake`: whether a client that matches no protocol is
    /// this one's.
    fn accepts_no_handshake(&self) -> bool {
        false
    }

    /// `websocketSubprotocolIdentifiers`: the WebSocket subprotocols it
    /// answers to.
    fn websocket_subprotocols(&self) -> &'static [&'static str] {
        &[]
    }

    /// Serves the connection until it closes.
    fn serve(&self, broker: Arc<Broker>, connection: Connection) -> ServeFuture;

    /// Reads the journal and page records of the protocol's own message
    /// persisters (`MessagePersister`), when it keeps messages in its own
    /// encoding.
    fn message_decoder(&self) -> Option<Arc<crate::message::NativeDecoder>> {
        None
    }

    /// Called once the broker has loaded its journals
    /// (`loadProtocolServices`): the protocol loads its state and starts
    /// its periodic tasks. The broker accepts no connections until it is
    /// done.
    fn start(&self, _broker: &Arc<Broker>) -> ServeFuture {
        Box::pin(async {})
    }

    /// Called once the broker is active, after federation
    /// (`startProtocolServices`): the protocol starts its services, such
    /// as connections to other brokers.
    fn start_services(&self, _broker: &Arc<Broker>) {}

    /// Called as the broker stops, after the cluster: the services stop.
    fn stop_services(&self, _broker: &Broker) {}

    /// Called by a configuration reload after the acceptors
    /// (`updateProtocolServices`): the services take the reloaded
    /// configuration's broker connections.
    fn update_services(
        &self,
        _broker: &Arc<Broker>,
        _broker_connections: &[saranga_config::AmqpBrokerConnectConfig],
    ) -> Result<(), String> {
        Ok(())
    }
}

/// The Core protocol (`CoreProtocolManager`).
pub struct CoreProtocol;

impl ProtocolManager for CoreProtocol {
    fn name(&self) -> &'static str {
        "CORE"
    }

    /// `isArtemis`: the `ARTEMIS` handshake.
    fn is_protocol(&self, header: &[u8]) -> bool {
        header.starts_with(saranga_core_protocol::HANDSHAKE)
    }

    fn serve(&self, broker: Arc<Broker>, c: Connection) -> ServeFuture {
        Box::pin(crate::core_protocol::serve_stream(
            broker, c.stream, c.peer, c.local, c.info, c.acceptor,
        ))
    }
}

/// The HORNETQ protocol (`HornetQProtocolManager`): Core for HornetQ
/// clients.
pub struct HornetQProtocol;

impl ProtocolManager for HornetQProtocol {
    fn name(&self) -> &'static str {
        "HORNETQ"
    }

    fn is_protocol(&self, header: &[u8]) -> bool {
        header.starts_with(b"HORNETQ")
    }

    /// A client that sends no handshake is taken for an old HornetQ one.
    fn accepts_no_handshake(&self) -> bool {
        true
    }

    fn serve(&self, broker: Arc<Broker>, c: Connection) -> ServeFuture {
        Box::pin(crate::core_protocol::serve_hornetq_stream(
            broker, c.stream, c.peer, c.local, c.info, c.acceptor,
        ))
    }
}

/// `ProtocolHandler.ProtocolDecoder.decode`: waits for 8 bytes, then hands
/// the connection to the first enabled protocol that recognises them, or
/// to the one that takes clients without a handshake, or to Core.
pub async fn detect_and_serve(
    broker: Arc<Broker>,
    stream: Box<dyn Stream>,
    peer: Option<SocketAddr>,
    local: Option<SocketAddr>,
    info: ConnectionInfo,
    acceptor: Arc<Acceptor>,
) {
    if let Some((manager, connection)) = detect(&broker, stream, peer, local, info, acceptor).await
    {
        manager.serve(broker, connection).await;
    }
}

/// The protocol a connection speaks, from its first bytes.
pub(crate) async fn detect(
    broker: &Broker,
    mut stream: Box<dyn Stream>,
    peer: Option<SocketAddr>,
    local: Option<SocketAddr>,
    info: ConnectionInfo,
    acceptor: Arc<Acceptor>,
) -> Option<(Arc<dyn ProtocolManager>, Connection)> {
    let enabled = broker.protocols_for(&acceptor);
    // `handshake-timeout`: a client that sends too little is closed.
    let Some(mut header) = read_header(&mut stream, acceptor.handshake_timeout).await else {
        tracing::debug!(?peer, "connection closed before its protocol was known");
        return None;
    };
    // An HTTP request: a WebSocket upgrade, inside which the protocol is
    // detected again (`switchToHttp`, `WebSocketServerHandler`).
    if saranga_transport::websocket::is_http(&header) {
        stream = upgrade(stream, header, &acceptor, &enabled).await?;
        header = read_header(&mut stream, None).await?;
    }
    let manager = enabled
        .iter()
        .find(|p| p.is_protocol(&header))
        .or_else(|| enabled.iter().find(|p| p.accepts_no_handshake()))
        .or_else(|| enabled.iter().find(|p| p.name() == "CORE"))
        .cloned();
    let Some(manager) = manager else {
        let names: Vec<&str> = enabled.iter().map(|p| p.name()).collect();
        tracing::error!(
            "AMQ224096: Error setting up connection from {} to {}:{}; protocol CORE not found in map: [{}]",
            info.remote_address,
            acceptor.host,
            acceptor.port,
            names.join(", ")
        );
        return None;
    };
    let connection = Connection {
        stream: Rewind::new(header, stream),
        peer,
        local,
        info,
        acceptor,
    };
    Some((manager, connection))
}

/// The first 8 bytes a client sends, within `timeout` if given.
async fn read_header(stream: &mut Box<dyn Stream>, timeout: Option<Duration>) -> Option<Vec<u8>> {
    let mut header = Vec::with_capacity(8);
    let read = async {
        while header.len() < 8 {
            let mut chunk = [0u8; 8];
            let want = 8 - header.len();
            match stream.read(&mut chunk[..want]).await {
                Ok(0) | Err(_) => return false,
                Ok(n) => header.extend_from_slice(&chunk[..n]),
            }
        }
        true
    };
    let complete = match timeout {
        Some(t) => tokio::time::timeout(t, read).await.unwrap_or(false),
        None => read.await,
    };
    complete.then_some(header)
}

/// Answers an HTTP request with the WebSocket handshake, offering the
/// subprotocols of the acceptor's protocols, and returns the stream inside
/// the WebSocket. `None` when the request was refused.
async fn upgrade(
    stream: Box<dyn Stream>,
    header: Vec<u8>,
    acceptor: &Acceptor,
    enabled: &[Arc<dyn ProtocolManager>],
) -> Option<Box<dyn Stream>> {
    use saranga_transport::websocket::{self, EncoderType, Upgrade};
    let mut stream = Rewind::new(header, stream);
    let (request, leftover) = match websocket::read_request(&mut stream).await {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!("reading an HTTP request: {e}");
            return None;
        }
    };
    let p = |k: &str| acceptor.params.get(k).map(String::as_str);
    let stomp_max = p("stompMaxFramePayloadLength").and_then(|v| v.parse::<i64>().ok());
    if stomp_max.is_some() {
        tracing::info!(
            "AMQ221038: Configuration option 'stompMaxFramePayloadLength' is deprecated and will be removed in a future version. Use 'webSocketMaxFramePayloadLength' instead. Consult the manual for details."
        );
    }
    let max_payload = p("webSocketMaxFramePayloadLength")
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v != -1)
        .or(stomp_max.filter(|v| *v != -1))
        .unwrap_or(websocket::DEFAULT_MAX_FRAME_PAYLOAD_LENGTH as i64)
        .max(1) as usize;
    let encoder = match p("webSocketEncoderType") {
        None => EncoderType::Binary,
        Some(t) => match EncoderType::parse(t) {
            Some(e) => e,
            None => {
                tracing::error!("invalid webSocketEncoderType {t}");
                return None;
            }
        },
    };
    let subprotocols: Vec<&str> = enabled
        .iter()
        .flat_map(|m| m.websocket_subprotocols().iter().copied())
        .collect();
    match websocket::handshake(&mut stream, &request, &subprotocols).await {
        Ok(Upgrade::WebSocket) => Some(Box::new(websocket::WebSocketStream::new(
            stream,
            leftover,
            max_payload,
            encoder,
        ))),
        Ok(Upgrade::Closed) => None,
        Err(e) => {
            tracing::debug!("WebSocket handshake: {e}");
            None
        }
    }
}

/// `TransportConstants.DEFAULT_HANDSHAKE_TIMEOUT`, in seconds.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
