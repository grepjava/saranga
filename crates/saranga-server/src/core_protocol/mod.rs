//! The Core protocol server. One task per connection reads frames and
//! handles channel 0 (pings, topology) and channel 1 (session creation);
//! each session runs as its own task fed in channel order. Writes go
//! through one writer task per connection.
//!
//! Ported from `CoreProtocolManager`, `ActiveMQPacketHandler`,
//! `RemotingConnectionImpl`, `ChannelImpl` and `CoreSessionCallback`.

mod session;

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_buffer::JString;
use saranga_core_protocol::packets::cluster::{
    ClusterTopologyChangeMessage, ClusterTopologyChangeMessageV2, ClusterTopologyChangeMessageV4,
    ConnectResponseMessage, TopologyUpdate,
};
use saranga_core_protocol::packets::cluster_types::{MapValue, PersistedMap};
use saranga_core_protocol::packets::connection::{
    ActiveMQExceptionMessage, CreateSessionResponseMessage,
};
use saranga_core_protocol::packets::session::{
    ReattachSessionResponseMessage, SessionReceiveMessage1X, legacy_end_of_message_position,
};
use saranga_core_protocol::{
    Context, Direction, Flagged, HANDSHAKE, Packet, encode_frame, read_frame, types, version,
};
use saranga_security::{Authenticated, ConnectionInfo, SecurityError};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::message::ServerMessage;
use crate::queue::DeliverySink;
use crate::{Broker, BrokerError, Name};
use session::{CoreSession, SessionInput};

/// `ActiveMQClient.DEFAULT_CONNECTION_TTL`, used until the client's first
/// ping says otherwise.
const DEFAULT_CONNECTION_TTL: i64 = 60_000;

/// `ChannelImpl.CHANNEL_ID`.
const CHANNEL_PING: i64 = 0;
const CHANNEL_SESSION: i64 = 1;

enum Outgoing {
    Frame(Vec<u8>),
    Close,
}

/// The write side of a connection, shared by its sessions and by the
/// queues delivering to its consumers.
pub(crate) struct ConnShared {
    out: mpsc::UnboundedSender<Outgoing>,
    /// The negotiated channel version (`getChannelVersion`); 0 until set.
    version: AtomicI32,
    client_id: Mutex<Option<String>>,
    /// Temporary queues, deleted when the connection goes away.
    temp_queues: Mutex<Vec<Name>>,
    remote_address: String,
    /// What the security store knows of this connection.
    security_info: Arc<ConnectionInfo>,
}

impl ConnShared {
    pub(crate) fn version(&self) -> i32 {
        self.version.load(Ordering::SeqCst)
    }

    pub(crate) fn context(&self) -> Context {
        Context::new(Direction::ToClient, self.version())
    }

    pub(crate) fn send_raw(&self, frame: Vec<u8>) {
        let _ = self.out.send(Outgoing::Frame(frame));
    }

    pub(crate) fn send(&self, channel_id: i64, packet: impl Into<Packet>) {
        self.send_raw(packet.into().encode_frame(channel_id, &self.context()));
    }

    pub(crate) fn add_temp_queue(&self, name: Name) {
        self.temp_queues
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(name);
    }

    pub(crate) fn remove_temp_queue(&self, name: &str) {
        self.temp_queues
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|q| &**q != name);
    }

    pub(crate) fn remote_address(&self) -> &str {
        &self.remote_address
    }

    pub(crate) fn security_info(&self) -> Arc<ConnectionInfo> {
        self.security_info.clone()
    }
}

/// Delivers to a Core consumer (`CoreSessionCallback.sendMessage`).
pub(crate) struct CoreDeliverySink {
    conn: Arc<ConnShared>,
    channel_id: i64,
    /// The session's `minLargeMessageSize`: the size of large-message
    /// chunks.
    chunk_size: usize,
}

impl DeliverySink for CoreDeliverySink {
    fn deliver(&self, consumer_id: i64, message: &ServerMessage, delivery_count: i32) -> i64 {
        let ctx = self.conn.context();
        let frame = if ctx.is_version_before_address_change() {
            let core = message.core().clone();
            let packet = SessionReceiveMessage1X {
                end_of_message_position: legacy_end_of_message_position(&core),
                message: core,
                consumer_id,
                delivery_count,
            };
            Packet::from(packet).encode_frame(self.channel_id, &ctx)
        } else {
            encode_frame(types::SESS_RECEIVE_MSG, self.channel_id, |w| {
                w.bytes(message.encoded())
                    .i64(consumer_id)
                    .i32(delivery_count);
            })
        };
        // The credit is the whole packet, as `PacketImpl.getPacketSize`.
        let size = frame.len() as i64;
        self.conn.send_raw(frame);
        size
    }

    fn deliver_large(
        &self,
        consumer_id: i64,
        message: &ServerMessage,
        body_size: i64,
        delivery_count: i32,
    ) -> i64 {
        // SessionReceiveLargeMessage: the headers and properties are the
        // message's encoded form.
        let frame = encode_frame(types::SESS_RECEIVE_LARGE_MSG, self.channel_id, |w| {
            w.i64(consumer_id)
                .i32(delivery_count)
                .i64(body_size)
                .bytes(message.encoded());
        });
        let size = frame.len() as i64;
        self.conn.send_raw(frame);
        size
    }

    fn deliver_continuation(&self, consumer_id: i64, chunk: Vec<u8>, continues: bool) -> i64 {
        let packet = saranga_core_protocol::packets::large::SessionReceiveContinuationMessage {
            body: chunk,
            continues,
            consumer_id,
        };
        let frame = Packet::from(packet).encode_frame(self.channel_id, &self.conn.context());
        let size = frame.len() as i64;
        self.conn.send_raw(frame);
        size
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

/// Serves one plain TCP Core connection until it closes or fails.
pub async fn serve_connection(broker: Arc<Broker>, stream: TcpStream) {
    let peer = stream.peer_addr().ok();
    let _ = stream.set_nodelay(true);
    let info = ConnectionInfo {
        remote_address: peer.map(java_socket_address).unwrap_or_default(),
        certificates: None,
        security_domain: None,
    };
    serve_stream(broker, stream, peer, info).await;
}

/// Serves one Core connection over any byte stream (TCP or TLS) until it
/// closes or fails. `info` is what security sees of it.
pub async fn serve_stream<S>(
    broker: Arc<Broker>,
    stream: S,
    peer: Option<std::net::SocketAddr>,
    info: ConnectionInfo,
) where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let remote_address = peer.map(|a| a.to_string()).unwrap_or_default();
    let security_info = Arc::new(info);
    let (mut rd, wr) = tokio::io::split(stream);
    let (out, rx) = mpsc::unbounded_channel();
    let writer = tokio::spawn(write_loop(wr, rx));
    let shared = Arc::new(ConnShared {
        out,
        version: AtomicI32::new(0),
        client_id: Mutex::default(),
        temp_queues: Mutex::default(),
        remote_address,
        security_info,
    });
    let mut conn = Connection {
        broker,
        shared,
        sessions: HashMap::new(),
        session_tasks: Vec::new(),
        ttl: DEFAULT_CONNECTION_TTL,
        topology_subscribed: false,
    };
    if let Err(e) = conn.read_loop(&mut rd).await {
        tracing::debug!(
            remote = conn.shared.remote_address(),
            "connection ended: {e}"
        );
    }
    conn.destroy().await;
    let _ = writer.await;
}

async fn write_loop<W: AsyncWrite + Unpin>(mut wr: W, mut rx: mpsc::UnboundedReceiver<Outgoing>) {
    let mut buf = Vec::with_capacity(64 * 1024);
    'outer: while let Some(first) = rx.recv().await {
        let mut close = false;
        match first {
            Outgoing::Frame(f) => buf.extend_from_slice(&f),
            Outgoing::Close => close = true,
        }
        // Batch whatever is already queued into one write.
        while !close && buf.len() < 256 * 1024 {
            match rx.try_recv() {
                Ok(Outgoing::Frame(f)) => buf.extend_from_slice(&f),
                Ok(Outgoing::Close) => close = true,
                Err(_) => break,
            }
        }
        if !buf.is_empty() && wr.write_all(&buf).await.is_err() {
            break 'outer;
        }
        buf.clear();
        if close {
            break;
        }
    }
    let _ = wr.shutdown().await;
}

struct Connection {
    broker: Arc<Broker>,
    shared: Arc<ConnShared>,
    sessions: HashMap<i64, mpsc::UnboundedSender<SessionInput>>,
    session_tasks: Vec<JoinHandle<()>>,
    /// The connection TTL in milliseconds; -1 never expires.
    ttl: i64,
    topology_subscribed: bool,
}

#[derive(Debug, thiserror::Error)]
enum ConnError {
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),
    #[error("not a Core client")]
    NotCore,
    #[error("decode: {0}")]
    Decode(#[from] saranga_core_protocol::DecodeError),
    #[error("no data within the connection TTL")]
    TimedOut,
    #[error("{0}")]
    Protocol(String),
}

impl Connection {
    async fn read_loop<R: AsyncRead + Unpin>(&mut self, rd: &mut R) -> Result<(), ConnError> {
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        while buf.len() < HANDSHAKE.len() {
            if self.read_more(rd, &mut buf).await? == 0 {
                return Ok(());
            }
        }
        if !buf.starts_with(HANDSHAKE) {
            return Err(ConnError::NotCore);
        }
        let mut start = HANDSHAKE.len();
        loop {
            while let Some((frame, used)) = read_frame(&buf[start..])? {
                let ctx = Context::new(Direction::ToServer, self.shared.version());
                let packet = Packet::decode(&frame, &ctx)?;
                self.dispatch(frame.channel_id, packet, used).await?;
                start += used;
            }
            buf.drain(..start);
            start = 0;
            if self.read_more(rd, &mut buf).await? == 0 {
                return Ok(());
            }
        }
    }

    /// Reads more bytes, failing the connection if nothing arrives within
    /// the TTL (`RemotingServiceImpl.FailureCheckAndFlushThread`).
    async fn read_more<R: AsyncRead + Unpin>(
        &self,
        rd: &mut R,
        buf: &mut Vec<u8>,
    ) -> Result<usize, ConnError> {
        let read = rd.read_buf(buf);
        if self.ttl < 0 {
            return Ok(read.await?);
        }
        match tokio::time::timeout(Duration::from_millis(self.ttl as u64), read).await {
            Ok(n) => Ok(n?),
            Err(_) => Err(ConnError::TimedOut),
        }
    }

    async fn dispatch(
        &mut self,
        channel_id: i64,
        packet: Packet,
        size: usize,
    ) -> Result<(), ConnError> {
        match channel_id {
            CHANNEL_PING => self.handle_channel0(packet),
            CHANNEL_SESSION => self.handle_channel1(packet).await,
            _ => {
                if let Packet::PacketsConfirmedMessage(_) = packet {
                    // We keep no resend cache, so there is nothing to clear.
                    return Ok(());
                }
                match self.sessions.get(&channel_id) {
                    Some(session) => {
                        if session
                            .send(SessionInput::Packet(Box::new(packet), size))
                            .is_err()
                        {
                            self.sessions.remove(&channel_id);
                        }
                    }
                    None => tracing::debug!(channel_id, "packet for an unknown channel"),
                }
                Ok(())
            }
        }
    }

    /// `CoreProtocolManager.LocalChannelHandler`.
    fn handle_channel0(&mut self, packet: Packet) -> Result<(), ConnError> {
        match packet {
            Packet::Ping(ping) => {
                if self.broker.connection_ttl_override() == -1 {
                    self.ttl = ping.connection_ttl;
                }
                self.shared.send(CHANNEL_PING, ping);
            }
            Packet::SubscribeClusterTopologyUpdatesMessage(_) => self.subscribe_topology()?,
            Packet::SubscribeClusterTopologyUpdatesMessageV2(p) => {
                self.shared
                    .version
                    .store(p.client_version, Ordering::SeqCst);
                self.subscribe_topology()?;
            }
            other => tracing::debug!("ignoring {} on channel 0", other.name()),
        }
        Ok(())
    }

    /// `sendPostAuthTopology` for a broker without a cluster connection: a
    /// single node with no connectors, so the client learns the node ID.
    fn subscribe_topology(&mut self) -> Result<(), ConnError> {
        if self.topology_subscribed {
            return Err(ConnError::Protocol(
                "multiple topology subscriptions".into(),
            ));
        }
        self.topology_subscribed = true;
        let node_id = JString::from(self.broker.node_id());
        let now = now_millis();
        let update = Some(TopologyUpdate {
            primary: Flagged(None),
            backup: Flagged(None),
            last: true,
        });
        let version = self.shared.version();
        let packet: Packet = if version >= version::ARTEMIS_2_18_0 {
            ClusterTopologyChangeMessageV4 {
                node_id,
                unique_event_id: now,
                update,
                backup_group_name: None,
                scale_down_group_name: None,
                server_version: version::CURRENT,
            }
            .into()
        } else if version >= 122 {
            ClusterTopologyChangeMessageV2 {
                node_id,
                unique_event_id: now,
                update,
                backup_group_name: Some(None),
            }
            .into()
        } else {
            ClusterTopologyChangeMessage { node_id, update }.into()
        };
        self.shared.send(CHANNEL_PING, packet);
        Ok(())
    }

    /// `ActiveMQPacketHandler`.
    async fn handle_channel1(&mut self, packet: Packet) -> Result<(), ConnError> {
        match packet {
            Packet::CreateSessionMessage(p) => {
                self.create_session(session::CreateSession {
                    name: p.name.to_string_lossy(),
                    channel_id: p.session_channel_id,
                    version: p.version,
                    username: p.username.map(|u| u.to_string_lossy()),
                    password: p.password.map(|u| u.to_string_lossy()),
                    min_large_message_size: p.min_large_message_size,
                    xa: p.xa,
                    auto_commit_sends: p.auto_commit_sends,
                    auto_commit_acks: p.auto_commit_acks,
                    confirmation_window_size: p.window_size,
                    pre_acknowledge: p.pre_acknowledge,
                    default_address: p.default_address.map(|a| a.to_string_lossy()),
                    client_id: None,
                })
                .await
            }
            Packet::CreateSessionMessageV2(p) => {
                self.create_session(session::CreateSession {
                    name: p.name.to_string_lossy(),
                    channel_id: p.session_channel_id,
                    version: p.version,
                    username: p.username.map(|u| u.to_string_lossy()),
                    password: p.password.map(|u| u.to_string_lossy()),
                    min_large_message_size: p.min_large_message_size,
                    xa: p.xa,
                    auto_commit_sends: p.auto_commit_sends,
                    auto_commit_acks: p.auto_commit_acks,
                    confirmation_window_size: p.window_size,
                    pre_acknowledge: p.pre_acknowledge,
                    default_address: p.default_address.map(|a| a.to_string_lossy()),
                    client_id: p.client_id.map(|c| c.to_string_lossy()),
                })
                .await
            }
            Packet::ConnectMessage(p) => {
                // No connection-level authentication yet: accept.
                if self.shared.version() == 0 {
                    self.shared
                        .version
                        .store(p.client_version(), Ordering::SeqCst);
                }
                self.shared.send(
                    CHANNEL_SESSION,
                    ConnectResponseMessage {
                        ok_to_failover: true,
                        fields: Some(PersistedMap {
                            entries: vec![(
                                ConnectResponseMessage::FIELD_SERVER_VERSION,
                                MapValue::Integer(version::CURRENT),
                            )],
                        }),
                    },
                );
            }
            Packet::ReattachSessionMessage(_) => {
                // Artemis no longer supports reattachment.
                self.shared.send(
                    CHANNEL_SESSION,
                    ReattachSessionResponseMessage {
                        last_confirmed_command_id: -1,
                        reattached: false,
                    },
                );
            }
            other => tracing::debug!("ignoring {} on channel 1", other.name()),
        }
        Ok(())
    }

    /// `ActiveMQPacketHandler.handleCreateSession`.
    async fn create_session(&mut self, request: session::CreateSession) {
        if !version::COMPATIBLE.contains(&request.version) {
            let e = BrokerError::incompatible_client_server();
            self.shared.send(
                CHANNEL_SESSION,
                ActiveMQExceptionMessage {
                    code: e.kind.code(),
                    message: Some(e.message.as_str().into()),
                },
            );
            return;
        }
        if self.shared.version() == 0 {
            self.shared.version.store(request.version, Ordering::SeqCst);
        }
        if let Some(client_id) = &request.client_id {
            *self
                .shared
                .client_id
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(client_id.clone());
        }
        // `validateUser`: a refused login gets the security exception and
        // no session. Login modules may block (LDAP), so they run off the
        // connection's task.
        let authenticated = match self.broker.security() {
            None => Authenticated::default(),
            Some(store) => {
                let store = store.clone();
                let (user, password) = (request.username.clone(), request.password.clone());
                let info = self.shared.security_info();
                let result = tokio::task::spawn_blocking(move || {
                    store.authenticate(user.as_deref(), password.as_deref(), &info)
                })
                .await
                .unwrap_or_else(|e| Err(SecurityError(format!("login failed: {e}"))));
                match result {
                    Ok(a) => a,
                    Err(e) => {
                        let e = BrokerError::security(e);
                        self.shared.send(
                            CHANNEL_SESSION,
                            ActiveMQExceptionMessage {
                                code: e.kind.code(),
                                message: Some(e.message.as_str().into()),
                            },
                        );
                        return;
                    }
                }
            }
        };
        let channel_id = request.channel_id;
        let (tx, rx) = mpsc::unbounded_channel();
        let session = CoreSession::new(
            self.broker.clone(),
            self.shared.clone(),
            request,
            authenticated,
        );
        self.session_tasks.push(tokio::spawn(session.run(rx)));
        self.sessions.insert(channel_id, tx);
        self.shared.send(
            CHANNEL_SESSION,
            CreateSessionResponseMessage {
                server_version: version::CURRENT,
            },
        );
    }

    /// The connection is gone: fail the sessions still open, then remove the
    /// temporary queues (`TempResourceCleanerUpper`).
    async fn destroy(&mut self) {
        for (_, session) in self.sessions.drain() {
            let _ = session.send(SessionInput::ConnectionFailed);
        }
        for task in self.session_tasks.drain(..) {
            let _ = task.await;
        }
        let temp = std::mem::take(
            &mut *self
                .shared
                .temp_queues
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        );
        let post_office = self.broker.post_office();
        for name in temp {
            let auto_delete_address = post_office
                .queue(&name)
                .map(|q| {
                    post_office
                        .settings(&q.config().address)
                        .auto_delete_addresses
                })
                .unwrap_or(false);
            let _ = post_office.delete_queue(&name, false, auto_delete_address);
        }
        let _ = self.shared.out.send(Outgoing::Close);
    }
}

/// `InetSocketAddress.toString` as Netty's `remoteAddress` prints it:
/// `/127.0.0.1:5672`, `/0:0:0:0:0:0:0:1:5672`.
pub(crate) fn java_socket_address(a: std::net::SocketAddr) -> String {
    match a {
        std::net::SocketAddr::V4(v4) => format!("/{}:{}", v4.ip(), v4.port()),
        std::net::SocketAddr::V6(v6) => {
            let s: Vec<String> = v6
                .ip()
                .segments()
                .iter()
                .map(|s| format!("{s:x}"))
                .collect();
            format!("/{}:{}", s.join(":"), v6.port())
        }
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}
