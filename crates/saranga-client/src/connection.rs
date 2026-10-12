//! Connections. Ported from `ClientSessionFactoryImpl` (pings and the
//! failure check), `ActiveMQClientProtocolManager` (handshake, topology
//! subscription, session creation, the channel 0 handler),
//! `RemotingConnectionImpl` (channel IDs, frame dispatch) and the topology
//! wait in `ServerLocatorImpl.createSessionFactory`.
//!
//! One task reads frames and dispatches them by channel ID; another writes
//! frames in the order they were queued. Sends never wait for the socket.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use saranga_core_protocol::packets::cluster::{
    ConnectMessage, SubscribeClusterTopologyUpdatesMessageV2, TopologyUpdate,
};
use saranga_core_protocol::packets::cluster_types::{MapValue, PersistedMap};
use saranga_core_protocol::packets::connection::{
    CreateSessionMessage, CreateSessionMessageV2, Ping,
};
use saranga_core_protocol::{
    Context, Direction, FRAME_HEADER_SIZE, HANDSHAKE, Packet, read_frame, types, version,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::channel::{Channel, correlation_id, is_response, set_correlation_id};
use crate::defaults;
use crate::error::{Error, Result};
use crate::session::{Session, SessionInner, SessionOptions};
use crate::uuid;

/// `ChannelImpl.CHANNEL_ID.USER`: the first channel ID handed to sessions.
const FIRST_USER_CHANNEL: i64 = 10;

/// Connection settings, with `ServerLocatorImpl` defaults.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    /// `clientFailureCheckPeriod`: how often to ping and check that data
    /// arrived. `None` is Java's -1: a single ping at connect, no checks.
    pub client_failure_check_period: Option<Duration>,
    /// `connectionTTL`, sent in every ping. `None` is -1.
    pub connection_ttl: Option<Duration>,
    /// `callTimeout`: how long a blocking call waits for its response.
    pub call_timeout: Duration,
    /// `connectionUser` and `connectionPassword`: with a user, the
    /// connection authenticates (a `ConnectMessage` with PLAIN
    /// credentials) before its first session, as the JMS client's
    /// `createConnection(user, password)` makes it.
    pub user: Option<String>,
    pub password: Option<String>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            client_failure_check_period: Some(defaults::CLIENT_FAILURE_CHECK_PERIOD),
            connection_ttl: Some(defaults::CONNECTION_TTL),
            call_timeout: defaults::CALL_TIMEOUT,
            user: None,
            password: None,
        }
    }
}

enum WriterCmd {
    Frame(Vec<u8>),
    Close,
}

pub(crate) struct ConnInner {
    writer: mpsc::UnboundedSender<WriterCmd>,
    writer_task: Mutex<Option<JoinHandle<()>>>,
    /// `CoreRemotingConnection.channelVersion`.
    version: AtomicI32,
    channels: Mutex<HashMap<i64, Arc<Channel>>>,
    next_channel_id: AtomicI64,
    failure: Mutex<Option<Error>>,
    sessions: Mutex<Vec<Weak<SessionInner>>>,
    /// `AbstractRemotingConnection.dataReceived`.
    data_received: AtomicBool,
    /// Set once a topology update with `last` arrives (`topologyReady`).
    topology: watch::Sender<bool>,
    shutdown: watch::Sender<bool>,
    connection_ttl: Option<Duration>,
    pub(crate) call_timeout: Duration,
}

impl ConnInner {
    pub(crate) fn version(&self) -> i32 {
        self.version.load(Ordering::SeqCst)
    }

    pub(crate) fn context(&self) -> Context {
        Context::new(Direction::ToServer, self.version())
    }

    pub(crate) fn failure(&self) -> Option<Error> {
        self.failure.lock().expect("connection").clone()
    }

    /// Writes one packet (`ChannelImpl.send`).
    pub(crate) fn send(&self, channel_id: i64, packet: &Packet) -> Result<()> {
        if let Some(e) = self.failure() {
            return Err(e);
        }
        let bytes = packet.encode_frame(channel_id, &self.context());
        self.writer
            .send(WriterCmd::Frame(bytes))
            .map_err(|_| Error::connection_destroyed())
    }

    /// `ChannelImpl.sendBlocking`.
    pub(crate) async fn send_blocking(
        &self,
        channel: &Channel,
        mut packet: Packet,
        expected: i8,
    ) -> Result<Packet> {
        if channel.is_closed() {
            return Err(Error::connection_destroyed());
        }
        let _guard = channel.blocking.lock().await;
        let id = channel.next_correlation_id();
        set_correlation_id(&mut packet, id);
        let rx = channel.begin(expected, correlation_id(&packet));
        if channel.is_closed() {
            channel.abandon();
            return Err(Error::connection_destroyed());
        }
        if let Err(e) = self.send(channel.id, &packet) {
            channel.abandon();
            return Err(e);
        }
        match tokio::time::timeout(self.call_timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::unblocking_a_call(&Error::connection_destroyed())),
            Err(_) => {
                channel.abandon();
                let e = Error::timed_out_sending_packet(
                    self.call_timeout.as_millis(),
                    packet.packet_type(),
                );
                // `connection.asyncFail(e)`
                self.fail(e.clone());
                Err(e)
            }
        }
    }

    pub(crate) fn remove_channel(&self, id: i64) {
        self.channels.lock().expect("connection").remove(&id);
    }

    fn channel(&self, id: i64) -> Option<Arc<Channel>> {
        self.channels.lock().expect("connection").get(&id).cloned()
    }

    fn ping(&self) {
        let connection_ttl = self.connection_ttl.map_or(-1, |d| d.as_millis() as i64);
        let _ = self.send(0, &Ping { connection_ttl }.into());
    }

    /// `RemotingConnectionImpl.fail`: records the failure, stops the
    /// transport and unblocks every channel and session.
    pub(crate) fn fail(&self, err: Error) {
        {
            let mut failure = self.failure.lock().expect("connection");
            if failure.is_some() {
                return;
            }
            *failure = Some(err.clone());
        }
        self.shutdown.send_replace(true);
        let _ = self.writer.send(WriterCmd::Close);
        self.topology.send_replace(true);
        let channels: Vec<_> = self
            .channels
            .lock()
            .expect("connection")
            .drain()
            .map(|(_, c)| c)
            .collect();
        for channel in channels {
            let session = channel.session();
            channel.close(&err);
            if let Some(s) = session {
                s.connection_failed(&err);
            }
        }
    }

    /// `RemotingConnectionImpl.bufferReceived`: decodes a frame and hands it
    /// to its channel.
    fn handle_frame(&self, buf: &[u8]) -> Result<()> {
        self.data_received.store(true, Ordering::SeqCst);
        let (frame, size) = read_frame(buf)?.ok_or_else(|| {
            Error::Decode(saranga_buffer::DecodeError::Invalid("short frame".into()))
        })?;
        let ctx = Context::new(Direction::ToClient, self.version());
        let packet = Packet::decode(&frame, &ctx)?;
        let Some(channel) = self.channel(frame.channel_id) else {
            return Ok(());
        };
        if packet.packet_type() == types::PACKETS_CONFIRMED {
            // No resend cache with a confirmation window of -1.
        } else if is_response(packet.packet_type()) {
            channel.handle_response(packet);
        } else if frame.channel_id == 0 {
            self.handle_channel0(packet);
        } else if let Some(session) = channel.session() {
            session.handle_packet(packet, size as i32);
        }
        Ok(())
    }

    /// `ActiveMQClientProtocolManager.Channel0Handler`. Topology updates are
    /// only used to know when the topology is complete.
    fn handle_channel0(&self, packet: Packet) {
        match packet {
            Packet::DisconnectMessage(_)
            | Packet::DisconnectMessageV2(_)
            | Packet::DisconnectMessageV3(_) => {
                // nodeDisconnected -> CloseRunnable -> conn.fail(disconnected())
                self.fail(Error::disconnected());
            }
            Packet::ClusterTopologyChangeMessage(p) => self.topology_changed(p.update.as_ref()),
            Packet::ClusterTopologyChangeMessageV2(p) => self.topology_changed(p.update.as_ref()),
            Packet::ClusterTopologyChangeMessageV3(p) => self.topology_changed(p.update.as_ref()),
            Packet::ClusterTopologyChangeMessageV4(p) => {
                self.topology_changed(p.update.as_ref());
                self.version.store(p.server_version, Ordering::SeqCst);
            }
            _ => {}
        }
    }

    fn topology_changed(&self, update: Option<&TopologyUpdate>) {
        // notifyNodeUp: `if (isLast) topologyReady = true`; exits never count.
        if update.is_some_and(|u| u.last) {
            self.topology.send_replace(true);
        }
    }
}

/// A Core protocol connection (`ClientSessionFactory` with its single
/// `RemotingConnection`). There is no reconnection or failover.
pub struct Connection {
    inner: Arc<ConnInner>,
}

impl Connection {
    /// Connects over TCP.
    pub async fn connect(addr: impl ToSocketAddrs, options: ConnectionOptions) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Self::connect_with(stream, options).await
    }

    /// Connects over an already open transport: sends the handshake and the
    /// first pings, subscribes to topology updates and waits for the
    /// topology, as `ServerLocatorImpl.createSessionFactory` does.
    pub async fn connect_with<S>(stream: S, options: ConnectionOptions) -> Result<Self>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (read, write) = tokio::io::split(stream);
        let (writer, writer_rx) = mpsc::unbounded_channel();
        let (topology, mut topology_rx) = watch::channel(false);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let inner = Arc::new(ConnInner {
            writer,
            writer_task: Mutex::new(None),
            version: AtomicI32::new(0),
            channels: Mutex::new(HashMap::from([
                (0, Arc::new(Channel::new(0))),
                (1, Arc::new(Channel::new(1))),
            ])),
            next_channel_id: AtomicI64::new(FIRST_USER_CHANNEL),
            failure: Mutex::new(None),
            sessions: Mutex::new(Vec::new()),
            data_received: AtomicBool::new(false),
            topology,
            shutdown,
            connection_ttl: options.connection_ttl,
            call_timeout: options.call_timeout,
        });

        // ActiveMQClientProtocolManager.sendHandshake
        let _ = inner.writer.send(WriterCmd::Frame(HANDSHAKE.to_vec()));
        let writer_task = tokio::spawn(write_loop(write, writer_rx, Arc::downgrade(&inner)));
        *inner.writer_task.lock().expect("connection") = Some(writer_task);
        tokio::spawn(read_loop(read, Arc::downgrade(&inner), shutdown_rx.clone()));

        // schedulePing: one ping now, then the scheduled pinger's first run
        // (initial delay 0) sends another.
        inner.ping();
        if let Some(period) = options.client_failure_check_period {
            inner.ping();
            tokio::spawn(ping_loop(
                Arc::downgrade(&inner),
                period,
                options.connection_ttl,
                shutdown_rx,
            ));
        }

        // ClientSessionFactoryImpl.getConnection: sendConnect, with the
        // credentials (RFC 4616: authzid NUL authcid NUL passwd).
        if let Some(user) = &options.user {
            let auth = format!("\0{user}\0{}", options.password.as_deref().unwrap_or(""));
            let connect = ConnectMessage {
                node_id: None,
                fields: Some(PersistedMap {
                    entries: vec![
                        (
                            ConnectMessage::FIELD_CLIENT_VERSION,
                            MapValue::Integer(version::CURRENT),
                        ),
                        (
                            ConnectMessage::FIELD_AUTH_MECHANISM,
                            MapValue::String("PLAIN".into()),
                        ),
                        (
                            ConnectMessage::FIELD_AUTH_DATA,
                            MapValue::ByteArray(auth.into_bytes()),
                        ),
                    ],
                }),
            };
            let channel1 = inner.channel(1).ok_or_else(Error::connection_destroyed)?;
            let sent = inner
                .send_blocking(&channel1, connect.into(), types::CONNECT_RESPONSE)
                .await;
            match sent {
                Ok(Packet::ConnectResponseMessage(r)) => {
                    inner.version.store(r.server_version(), Ordering::SeqCst);
                }
                Ok(other) => {
                    inner.fail(Error::UnexpectedResponse(other.name()));
                    return Err(Error::UnexpectedResponse(other.name()));
                }
                Err(e) => {
                    inner.fail(e.clone());
                    return Err(e);
                }
            }
        }

        inner.send(
            0,
            &SubscribeClusterTopologyUpdatesMessageV2 {
                cluster_connection: false,
                client_version: version::CURRENT,
            }
            .into(),
        )?;

        let ready = tokio::time::timeout(options.call_timeout, topology_rx.wait_for(|r| *r));
        let timed_out = !matches!(ready.await, Ok(Ok(_)));
        if let Some(e) = inner.failure() {
            return Err(e);
        }
        if timed_out {
            inner.fail(Error::topology_timed_out());
            return Err(Error::topology_timed_out());
        }
        Ok(Self { inner })
    }

    /// The channel version: the broker's version once known.
    pub fn version(&self) -> i32 {
        self.inner.version()
    }

    /// The error the connection failed with, if it has failed or been closed.
    pub fn failure(&self) -> Option<Error> {
        self.inner.failure()
    }

    /// Creates a session (`ActiveMQClientProtocolManager.createSessionContext`
    /// + `ClientSessionFactoryImpl.createSessionInternal`).
    pub async fn create_session(&self, options: SessionOptions) -> Result<Session> {
        let inner = &self.inner;
        let channel1 = inner.channel(1).ok_or_else(Error::connection_destroyed)?;
        let name = uuid::to_string(&uuid::time_based());
        let channel_id = inner.next_channel_id.fetch_add(1, Ordering::SeqCst);
        let ctx = inner.context();
        let request: Packet = if ctx.is_version_support_client_id() {
            CreateSessionMessageV2 {
                name: name.as_str().into(),
                session_channel_id: channel_id,
                version: version::CURRENT,
                username: options.username.as_deref().map(Into::into),
                password: options.password.as_deref().map(Into::into),
                min_large_message_size: options.min_large_message_size,
                xa: options.xa,
                auto_commit_sends: options.auto_commit_sends,
                auto_commit_acks: options.auto_commit_acks,
                window_size: defaults::CONFIRMATION_WINDOW_SIZE,
                pre_acknowledge: options.pre_acknowledge,
                default_address: None,
                client_id: options.client_id.as_deref().map(Into::into),
            }
            .into()
        } else {
            CreateSessionMessage {
                name: name.as_str().into(),
                session_channel_id: channel_id,
                version: version::CURRENT,
                username: options.username.as_deref().map(Into::into),
                password: options.password.as_deref().map(Into::into),
                min_large_message_size: options.min_large_message_size,
                xa: options.xa,
                auto_commit_sends: options.auto_commit_sends,
                auto_commit_acks: options.auto_commit_acks,
                window_size: defaults::CONFIRMATION_WINDOW_SIZE,
                pre_acknowledge: options.pre_acknowledge,
                default_address: None,
            }
            .into()
        };

        let channel = Arc::new(Channel::new(channel_id));
        inner
            .channels
            .lock()
            .expect("connection")
            .insert(channel_id, channel.clone());
        let response = match inner
            .send_blocking(&channel1, request, types::CREATESESSION_RESP)
            .await
        {
            Ok(Packet::CreateSessionResponseMessage(r)) => r,
            Ok(other) => {
                inner.remove_channel(channel_id);
                return Err(Error::UnexpectedResponse(other.name()));
            }
            Err(e) => {
                inner.remove_channel(channel_id);
                return Err(e);
            }
        };
        inner
            .version
            .store(response.server_version, Ordering::SeqCst);

        let session = SessionInner::new(
            inner.clone(),
            channel.clone(),
            name,
            options,
            response.server_version,
        );
        channel.set_session(Arc::downgrade(&session));
        let mut sessions = inner.sessions.lock().expect("connection");
        sessions.retain(|s| s.strong_count() > 0);
        sessions.push(Arc::downgrade(&session));
        Ok(Session::from_inner(session))
    }

    /// Closes every open session, then the transport. Like the Java client,
    /// no `DisconnectMessage` is sent.
    pub async fn close(&self) {
        let sessions: Vec<_> = self
            .inner
            .sessions
            .lock()
            .expect("connection")
            .drain(..)
            .filter_map(|s| s.upgrade())
            .collect();
        for session in sessions {
            session.close().await;
        }
        self.inner.fail(Error::connection_destroyed());
        let task = self.inner.writer_task.lock().expect("connection").take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

async fn write_loop<W: AsyncWrite>(
    write: W,
    mut rx: mpsc::UnboundedReceiver<WriterCmd>,
    inner: Weak<ConnInner>,
) {
    tokio::pin!(write);
    while let Some(cmd) = rx.recv().await {
        let WriterCmd::Frame(mut bytes) = cmd else {
            break;
        };
        // Batch whatever else is already queued into one write.
        let mut close = false;
        while let Ok(cmd) = rx.try_recv() {
            match cmd {
                WriterCmd::Frame(more) => bytes.extend_from_slice(&more),
                WriterCmd::Close => {
                    close = true;
                    break;
                }
            }
        }
        let result = async {
            write.write_all(&bytes).await?;
            write.flush().await
        }
        .await;
        if let Err(e) = result {
            if let Some(inner) = inner.upgrade() {
                inner.fail(Error::amq(
                    crate::ErrorType::NotConnected,
                    format!("AMQ219006: Channel disconnected ({e})"),
                ));
            }
            return;
        }
        if close {
            break;
        }
    }
    let _ = write.shutdown().await;
}

async fn read_loop<R: AsyncRead>(
    read: R,
    inner: Weak<ConnInner>,
    mut shutdown: watch::Receiver<bool>,
) {
    let read = BufReader::with_capacity(64 * 1024, read);
    tokio::pin!(read);
    loop {
        let frame = tokio::select! {
            f = read_frame_bytes(&mut read) => f,
            _ = shutdown.wait_for(|s| *s) => return,
        };
        let Some(inner) = inner.upgrade() else {
            return;
        };
        match frame {
            Ok(Some(buf)) => {
                if let Err(e) = inner.handle_frame(&buf) {
                    inner.fail(e);
                    return;
                }
            }
            Ok(None) => {
                // connectionDestroyed -> channelDisconnected()
                inner.fail(Error::channel_disconnected());
                return;
            }
            Err(e) => {
                inner.fail(Error::amq(
                    crate::ErrorType::NotConnected,
                    format!("AMQ219006: Channel disconnected ({e})"),
                ));
                return;
            }
        }
    }
}

/// Reads one whole frame, length prefix included. `None` at a clean EOF.
async fn read_frame_bytes<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let n = i32::from_be_bytes(len);
    let n = usize::try_from(n)
        .ok()
        .filter(|&n| n >= FRAME_HEADER_SIZE - 4)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("bad frame length {n}"),
            )
        })?;
    let mut buf = vec![0u8; 4 + n];
    buf[..4].copy_from_slice(&len);
    r.read_exact(&mut buf[4..]).await?;
    Ok(Some(buf))
}

/// `ClientSessionFactoryImpl.PingRunnable`, scheduled with a fixed delay.
async fn ping_loop(
    inner: Weak<ConnInner>,
    period: Duration,
    connection_ttl: Option<Duration>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut last_check = Instant::now();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(period) => {}
            _ = shutdown.wait_for(|s| *s) => return,
        }
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let now = Instant::now();
        if let Some(ttl) = connection_ttl
            && now >= last_check + ttl
        {
            // checkDataReceived() reads and clears the flag.
            if !inner.data_received.swap(false, Ordering::SeqCst) {
                inner.fail(Error::connection_timed_out());
                return;
            }
            last_check = now;
        }
        inner.ping();
    }
}
