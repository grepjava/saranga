//! The Core protocol server. One task per connection reads frames and
//! handles channel 0 (pings, topology) and channel 1 (session creation);
//! each session runs as its own task fed in channel order. Writes go
//! through one writer task per connection.
//!
//! Ported from `CoreProtocolManager`, `ActiveMQPacketHandler`,
//! `RemotingConnectionImpl`, `ChannelImpl` and `CoreSessionCallback`.

mod session;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::time::Duration;

use saranga_buffer::JString;
use saranga_core_protocol::packets::cluster::{
    ClusterConnectReplyMessage, ClusterTopologyChangeMessage, ClusterTopologyChangeMessageV2,
    ClusterTopologyChangeMessageV3, ClusterTopologyChangeMessageV4, ConnectResponseMessage,
    TopologyUpdate,
};
use saranga_core_protocol::packets::cluster_types::{MapValue, PersistedMap};
use saranga_core_protocol::packets::connection::{
    ActiveMQExceptionMessage, CreateSessionResponseMessage,
};
use saranga_core_protocol::packets::connection::{
    DisconnectMessage, DisconnectMessageV2, DisconnectMessageV3,
};
use saranga_core_protocol::packets::session::{
    ReattachSessionResponseMessage, SessionReceiveMessage1X, legacy_end_of_message_position,
};
use saranga_core_protocol::transport::TransportConfiguration;
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
use crate::session::{ClientConnection, Transport};
use crate::{Broker, BrokerError};
use session::{CoreSession, SessionInput};

/// `ActiveMQClient.DEFAULT_CONNECTION_TTL`, used until the client's first
/// ping says otherwise.
const DEFAULT_CONNECTION_TTL: i64 = 60_000;

/// `ChannelImpl.CHANNEL_ID`.
const CHANNEL_PING: i64 = 0;
const CHANNEL_SESSION: i64 = 1;
const CHANNEL_CLUSTER: i64 = 3;

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
    /// What every protocol's connection keeps.
    pub(crate) client: Arc<ClientConnection>,
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

    /// Closes the connection once what is queued is written.
    pub(crate) fn close(&self) {
        let _ = self.out.send(Outgoing::Close);
    }

    pub(crate) fn security_info(&self) -> Arc<ConnectionInfo> {
        self.client.security_info()
    }
}

/// `ServerSessionImpl.requestProducerCredits`: granted at once, or under
/// BLOCK once a full address has room, or refused under FAIL.
pub(crate) fn producer_credits(
    broker: &Broker,
    conn: &Arc<ConnShared>,
    channel: i64,
    p: saranga_core_protocol::packets::session::SessionRequestProducerCreditsMessage,
) {
    use saranga_core_protocol::packets::session::{
        SessionProducerCreditsFailMessage, SessionProducerCreditsMessage,
    };
    let raw = p.address.to_string_lossy();
    let addr = conn.client.strip_prefix(&raw).0;
    let grant: Box<dyn FnOnce() + Send> = {
        let conn = conn.clone();
        let (credits, address) = (p.credits, p.address.clone());
        Box::new(move || conn.send(channel, SessionProducerCreditsMessage { credits, address }))
    };
    // getPageStore: the management address has no store, so what is sent
    // to it is never blocked.
    let store_name = addr.split_once("::").map_or(addr, |(a, _)| a);
    if store_name.starts_with(broker.management().address()) {
        grant();
    } else if let crate::paging::Credits::Failed = broker
        .post_office()
        .paging_store(addr)
        .request_credits(grant)
    {
        conn.send(
            channel,
            SessionProducerCreditsFailMessage {
                credits: p.credits,
                address: p.address,
            },
        );
    }
}

/// Delivers to a Core consumer (`CoreSessionCallback.sendMessage`).
pub(crate) struct CoreDeliverySink {
    conn: Arc<ConnShared>,
    channel_id: i64,
    /// The session's `minLargeMessageSize`: the size of large-message
    /// chunks.
    chunk_size: usize,
    /// For a 1.x JMS client, the old prefix its messages are addressed with
    /// (`ServerConsumerImpl.applyPrefixForLegacyConsumer`).
    legacy_prefix: Option<&'static str>,
}

impl CoreDeliverySink {
    /// The message with the legacy prefix on its address, if it needs one.
    fn legacy(&self, message: &ServerMessage) -> Option<saranga_message::CoreMessage> {
        let prefix = self.legacy_prefix?;
        let address = message.delivery_core().address.as_ref()?.to_string_lossy();
        if address.starts_with(prefix) {
            return None;
        }
        let mut core = message.delivery_core().clone();
        core.address = Some(saranga_buffer::SimpleString::from(
            format!("{prefix}{address}").as_str(),
        ));
        Some(core)
    }
}

impl DeliverySink for CoreDeliverySink {
    fn deliver(&self, consumer_id: i64, message: &Arc<ServerMessage>, delivery_count: i32) -> i64 {
        let ctx = self.conn.context();
        let frame = if ctx.is_version_before_address_change() {
            let core = self
                .legacy(message)
                .unwrap_or_else(|| message.delivery_core().clone());
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
        let legacy = self.legacy(message).map(|core| {
            let mut w = saranga_buffer::Writer::new();
            core.encode_headers_and_properties(&mut w);
            w.into_vec()
        });
        let headers = legacy.as_deref().unwrap_or(message.encoded());
        let frame = encode_frame(types::SESS_RECEIVE_LARGE_MSG, self.channel_id, |w| {
            w.i64(consumer_id)
                .i32(delivery_count)
                .i64(body_size)
                .bytes(headers);
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

    /// `CoreSessionCallback.disconnect`: a client that knows the packet is
    /// told its consumer is gone.
    fn disconnect(&self, consumer_id: i64, reason: &str) {
        if self.conn.version() >= 124 {
            self.conn.send(
                self.channel_id,
                saranga_core_protocol::packets::session::DisconnectConsumerMessage { consumer_id },
            );
        } else {
            tracing::warn!(
                "AMQ222210: Server is unable to disconnect the consumer of an old client: {reason}"
            );
        }
    }
}

/// Serves one plain TCP Core connection until it closes or fails.
pub async fn serve_connection(broker: Arc<Broker>, stream: TcpStream) {
    let peer = stream.peer_addr().ok();
    let local = stream.local_addr().ok();
    let _ = stream.set_nodelay(true);
    let info = ConnectionInfo {
        remote_address: peer.map(java_socket_address).unwrap_or_default(),
        certificates: None,
        security_domain: None,
        connection_id: None,
    };
    serve_stream(broker, stream, peer, local, info, Arc::default()).await;
}

/// Serves one Core connection over any byte stream (TCP or TLS) until it
/// closes or fails. `info` is what security sees of it.
pub async fn serve_stream<S>(
    broker: Arc<Broker>,
    stream: S,
    peer: Option<std::net::SocketAddr>,
    local: Option<std::net::SocketAddr>,
    info: ConnectionInfo,
    acceptor: Arc<crate::Acceptor>,
) where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let (mut rd, wr) = tokio::io::split(stream);
    let (out, rx) = mpsc::unbounded_channel();
    let writer = tokio::spawn(write_loop(wr, rx));
    let shared = Arc::new(ConnShared {
        out,
        version: AtomicI32::new(0),
        client: Arc::new(
            ClientConnection::new(
                Transport::socket("CORE", "RemotingConnectionImpl", peer, local),
                Arc::new(info),
                acceptor.prefixes.clone(),
            )
            .with_acceptor(&acceptor.name),
        ),
    });
    let _registered = broker.registry().add(&shared.client);
    // `CoreFrameWithSizeLimitDecoder`: with connection security, frames are
    // limited from the start.
    let connection_security = broker.security().is_some() && acceptor.core_connection_security;
    let frame_limit = connection_security.then_some(acceptor.core_initial_max_frame_size);
    let mut conn = Connection {
        broker,
        shared,
        sessions: HashMap::new(),
        session_tasks: Vec::new(),
        ttl: DEFAULT_CONNECTION_TTL,
        topology_subscribed: false,
        acceptor,
        connection_security,
        frame_limit,
        authenticated: false,
        connect_received: false,
        pre_auth_topology_sent: false,
        post_auth_topology_sent: false,
        topology_subscription: None,
        cluster_authorized: false,
        replication: None,
        federations: Vec::new(),
    };
    let ended = conn.read_loop(&mut rd).await;
    if let Some(r) = conn.replication.take() {
        conn.broker.backup_connection_lost(&r, ended.is_err());
    }
    if let Err(e) = ended {
        // RemotingConnectionImpl.killMessage.
        if matches!(e, ConnError::Failed)
            && conn.shared.client.is_killed()
            && conn.shared.version() >= DISCONNECT_CONSUMER_KILL_VERSION
        {
            conn.shared.send(
                1,
                saranga_core_protocol::packets::session::DisconnectConsumerWithKillMessage {
                    node_id: Some(conn.broker.node_id().into()),
                },
            );
        } else if matches!(e, ConnError::Failed) && conn.shared.client.is_disconnecting() {
            conn.send_disconnect();
        }
        tracing::debug!(
            remote = conn.shared.client.remote_address(),
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

/// Producer credits are answered as they are read while a session has at
/// most this many bytes of packets read and not yet handled.
const EARLY_CREDITS_BACKLOG: usize = 256 * 1024;

/// A session's task: its packets, and the bytes of those not yet handled.
struct SessionLink {
    tx: mpsc::UnboundedSender<SessionInput>,
    backlog: Arc<AtomicUsize>,
}

struct Connection {
    broker: Arc<Broker>,
    shared: Arc<ConnShared>,
    sessions: HashMap<i64, SessionLink>,
    session_tasks: Vec<JoinHandle<()>>,
    /// The connection TTL in milliseconds; -1 never expires.
    ttl: i64,
    topology_subscribed: bool,
    acceptor: Arc<crate::Acceptor>,
    /// Security is on and the acceptor applies it to connections.
    connection_security: bool,
    /// The largest frame accepted now, `None` for no limit.
    frame_limit: Option<usize>,
    /// A login succeeded on this connection (`isAuthenticated`).
    authenticated: bool,
    connect_received: bool,
    pre_auth_topology_sent: bool,
    post_auth_topology_sent: bool,
    /// The cluster connection's topology, followed for the client.
    topology_subscription: Option<crate::cluster::Subscription>,
    /// The cluster user logged in on the cluster channel.
    cluster_authorized: bool,
    /// A backup registered on this connection: its replication channel.
    replication: Option<Arc<crate::replication::Replicator>>,
    /// The federations downstreams asked for on this connection, undeployed
    /// when it goes.
    federations: Vec<String>,
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
    #[error("closed by management")]
    Failed,
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
            self.check_frame_size(&buf[start..])?;
            while let Some((frame, used)) = read_frame(&buf[start..])? {
                let ctx = Context::new(Direction::ToServer, self.shared.version());
                let packet = Packet::decode(&frame, &ctx)?;
                self.dispatch(frame.channel_id, packet, used).await?;
                start += used;
                self.check_frame_size(&buf[start..])?;
            }
            buf.drain(..start);
            start = 0;
            if self.read_more(rd, &mut buf).await? == 0 {
                return Ok(());
            }
        }
    }

    /// `CoreFrameWithSizeLimitDecoder.getUnadjustedFrameLength`: a frame
    /// longer than the limit fails the connection.
    fn check_frame_size(&self, pending: &[u8]) -> Result<(), ConnError> {
        if let (Some(limit), Some(prefix)) = (self.frame_limit, pending.get(..4)) {
            let len = u32::from_be_bytes(prefix.try_into().expect("4 bytes")) as usize;
            if len > limit {
                return Err(ConnError::Protocol(format!("Frame size exceeded: {len}")));
            }
        }
        Ok(())
    }

    /// A login succeeded (`setAuthenticated`): the larger frame limit
    /// applies and a subscriber gets the real topology.
    fn on_authenticated(&mut self) {
        if self.authenticated {
            return;
        }
        self.authenticated = true;
        if self.connection_security {
            self.frame_limit = Some(self.acceptor.core_max_frame_size);
        }
        if self.topology_subscribed {
            self.send_post_auth_topology();
        }
    }

    /// Reads more bytes, failing the connection if nothing arrives within
    /// the TTL (`RemotingServiceImpl.FailureCheckAndFlushThread`).
    async fn read_more<R: AsyncRead + Unpin>(
        &self,
        rd: &mut R,
        buf: &mut Vec<u8>,
    ) -> Result<usize, ConnError> {
        let client = &self.shared.client;
        // Management may fail the connection meanwhile.
        let read = async {
            tokio::select! {
                n = rd.read_buf(buf) => Ok(n?),
                () = client.failed() => Err(ConnError::Failed),
            }
        };
        if self.ttl < 0 {
            return read.await;
        }
        match tokio::time::timeout(Duration::from_millis(self.ttl as u64), read).await {
            Ok(n) => n,
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
            CHANNEL_CLUSTER => self.handle_cluster(packet).await,
            crate::federation::CHANNEL_FEDERATION => self.handle_federation(packet),
            crate::replication::CHANNEL_REPLICATION if self.replication.is_some() => {
                // `ReplicationManager.ResponseHandler`.
                match packet {
                    Packet::ReplicationResponseMessage(_)
                    | Packet::ReplicationResponseMessageV2(_) => {
                        if let Some(r) = &self.replication {
                            r.replicated();
                        }
                    }
                    other => {
                        tracing::debug!("ignoring {} on the replication channel", other.name())
                    }
                }
                Ok(())
            }
            _ => {
                if let Packet::PacketsConfirmedMessage(_) = packet {
                    // We keep no resend cache, so there is nothing to clear.
                    return Ok(());
                }
                let Some(session) = self.sessions.get(&channel_id) else {
                    tracing::debug!(channel_id, "packet for an unknown channel");
                    return Ok(());
                };
                // Producer credits are answered as the request is read while
                // the session has little left to handle: the client's
                // producer waits for them with only its window (64 KiB by
                // default) sent ahead, and the trip through the session task
                // and back held it there. Behind a longer backlog they wait
                // their turn, as in Artemis, so a producer cannot send faster
                // than its session handles what it sends.
                if let Packet::SessionRequestProducerCreditsMessage(p) = packet {
                    if session.backlog.load(Ordering::Relaxed) <= EARLY_CREDITS_BACKLOG {
                        producer_credits(&self.broker, &self.shared, channel_id, p);
                        return Ok(());
                    }
                    return self.pass_to_session(
                        channel_id,
                        Packet::SessionRequestProducerCreditsMessage(p),
                        size,
                    );
                }
                self.pass_to_session(channel_id, packet, size)
            }
        }
    }

    /// Hands a packet to its session, counting it until handled.
    fn pass_to_session(
        &mut self,
        channel_id: i64,
        packet: Packet,
        size: usize,
    ) -> Result<(), ConnError> {
        if let Some(session) = self.sessions.get(&channel_id) {
            session.backlog.fetch_add(size, Ordering::Relaxed);
            if session
                .tx
                .send(SessionInput::Packet(Box::new(packet), size))
                .is_err()
            {
                self.sessions.remove(&channel_id);
            }
        }
        Ok(())
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
            Packet::SubscribeClusterTopologyUpdatesMessage(p) => {
                self.subscribe_topology(p.cluster_connection)?
            }
            Packet::SubscribeClusterTopologyUpdatesMessageV2(p) => {
                self.shared
                    .version
                    .store(p.client_version, Ordering::SeqCst);
                self.subscribe_topology(p.cluster_connection)?;
            }
            other => tracing::debug!("ignoring {} on channel 0", other.name()),
        }
        Ok(())
    }

    /// `LocalChannelHandler`: one topology subscription per connection;
    /// before login, only a placeholder topology.
    fn subscribe_topology(&mut self, cluster_connection: bool) -> Result<(), ConnError> {
        if self.topology_subscribed {
            tracing::warn!(
                "AMQ224166: Multiple topology subscriptions to connection from {}. Closing connection.",
                self.shared.security_info().remote_address
            );
            return Err(ConnError::Protocol(
                "multiple topology subscriptions".into(),
            ));
        }
        self.topology_subscribed = true;
        if self.connection_security && !self.authenticated {
            if cluster_connection {
                tracing::warn!(
                    "AMQ224165: Unauthenticated cluster topology subscription request. Update the remote broker to authenticate the cluster connection from {}.",
                    self.shared.security_info().remote_address
                );
            }
            self.send_pre_auth_topology();
        } else {
            self.send_post_auth_topology();
        }
        Ok(())
    }

    /// `sendPreAuthTopology`: a placeholder node, so the client's wait for
    /// a topology ends without learning anything about the broker.
    fn send_pre_auth_topology(&mut self) {
        use saranga_core_protocol::transport::{ParamValue, TransportConfiguration};
        self.pre_auth_topology_sent = true;
        let factory: JString = NETTY_CONNECTOR_FACTORY.into();
        let primary = TransportConfiguration {
            name: JString::from(uuid::Uuid::new_v4().to_string().as_str()),
            factory_class_name: factory.clone(),
            // The acceptor's parameters, in `HashMap` order.
            params: vec![
                ("port".into(), ParamValue::Int(self.acceptor.port as i32)),
                (
                    "host".into(),
                    ParamValue::String(self.acceptor.host.as_str().into()),
                ),
            ],
        };
        let backup = TransportConfiguration {
            name: PRE_AUTH_CONNECTOR_NAME.into(),
            factory_class_name: factory,
            params: Vec::new(),
        };
        let update = Some(TopologyUpdate {
            primary: Flagged(Some(primary)),
            backup: Flagged(Some(backup)),
            last: true,
        });
        self.send_topology(PRE_AUTH_NODE_ID.into(), update);
    }

    /// `sendPostAuthTopology`: the cluster connection's topology, now and
    /// as it changes; without one, a single node with no connectors, so the
    /// client learns the node ID. The placeholder goes first, if one was
    /// sent.
    fn send_post_auth_topology(&mut self) {
        if self.post_auth_topology_sent {
            return;
        }
        self.post_auth_topology_sent = true;
        if self.pre_auth_topology_sent {
            let node_id: JString = PRE_AUTH_NODE_ID.into();
            let packet: Packet = if self.shared.version() >= 122 {
                ClusterTopologyChangeMessageV2 {
                    node_id,
                    unique_event_id: now_millis(),
                    update: None,
                    backup_group_name: Some(None),
                }
                .into()
            } else {
                ClusterTopologyChangeMessage {
                    node_id,
                    update: None,
                }
                .into()
            };
            self.shared.send(CHANNEL_PING, packet);
        }
        if let Some(cluster) = self.broker.cluster() {
            let listener = Arc::new(ClientTopologyListener {
                conn: self.shared.clone(),
            });
            self.topology_subscription = Some(crate::cluster::Subscription::new(cluster, listener));
            return;
        }
        let update = Some(TopologyUpdate {
            primary: Flagged(None),
            backup: Flagged(None),
            last: true,
        });
        let node_id = JString::from(self.broker.node_id());
        self.send_topology(node_id, update);
    }

    /// `ClusterControllerChannelHandler`: the cluster user logs in with
    /// `CLUSTER_CONNECT`, then brokers announce themselves.
    async fn handle_cluster(&mut self, packet: Packet) -> Result<(), ConnError> {
        if !self.cluster_authorized {
            let Packet::ClusterConnectMessage(msg) = packet else {
                return Ok(());
            };
            if self.broker.cluster().is_none() && self.broker.security().is_some() {
                tracing::warn!(
                    "AMQ222290: Failed to find cluster-connection when handling cluster-connect packet. Ignoring: {}",
                    Packet::from(msg).name()
                );
                return Ok(());
            }
            let valid = self
                .authenticate(
                    Some(msg.cluster_user.to_string_lossy()),
                    Some(msg.cluster_password.to_string_lossy()),
                )
                .await
                .is_ok();
            self.cluster_authorized = valid;
            self.shared.send(
                CHANNEL_CLUSTER,
                ClusterConnectReplyMessage { authorized: valid },
            );
            return Ok(());
        }
        match packet {
            Packet::NodeAnnounceMessage(msg) => {
                let (primary, backup) = if msg.backup {
                    (None, msg.connector.0)
                } else {
                    (msg.connector.0, msg.backup_connector.0)
                };
                match self.broker.cluster() {
                    Some(cluster) => cluster.node_announced(
                        msg.current_event_id,
                        &msg.node_id.to_string_lossy(),
                        msg.backup_group_name.map(|n| n.to_string_lossy()),
                        msg.scale_down_group_name.map(|n| n.to_string_lossy()),
                        primary,
                        backup,
                        msg.backup,
                    ),
                    None => tracing::debug!("node announcement without a cluster connection"),
                }
            }
            Packet::BackupRegistrationMessage(msg) => {
                // `getActivationChannelHandler`: only a replicating primary
                // takes a backup.
                if self.replication.is_none() {
                    self.replication = self.broker.backup_registered(&self.shared, msg);
                }
            }
            Packet::QuorumVoteMessage(msg) => {
                // `QuorumManager.handleQuorumVote`.
                if let Some(reply) = crate::replication::quorum::answer(&self.broker, msg) {
                    self.shared.send(CHANNEL_CLUSTER, reply);
                }
            }
            other => tracing::debug!("ignoring {} on the cluster channel", other.name()),
        }
        Ok(())
    }

    /// `RemotingConnectionImpl.disconnect` as `freeze` calls it when the
    /// broker stops: the newest form the client knows, naming this node.
    fn send_disconnect(&self) {
        let version = self.shared.version();
        let node_id = Some(saranga_buffer::SimpleString::from(self.broker.node_id()));
        let packet: Packet = if version >= version::ARTEMIS_2_18_0 {
            DisconnectMessageV3 {
                node_id,
                reason: DISCONNECT_REASON_SCALE_DOWN,
                target_node_id: None,
                target_connector: Flagged(None),
            }
            .into()
        } else if version >= 125 {
            DisconnectMessageV2 {
                node_id,
                scale_down_node_id: None,
            }
            .into()
        } else {
            DisconnectMessage { node_id }.into()
        };
        self.shared.send(CHANNEL_PING, packet);
    }

    /// A topology change in the newest form the client understands.
    fn send_topology(&self, node_id: JString, update: Option<TopologyUpdate>) {
        let now = now_millis();
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
                // `handleConnect`.
                if self.connect_received {
                    tracing::error!("AMQ224017: Invalid packet {}", Packet::from(p).name());
                    return Err(ConnError::Protocol("a second CONNECT".into()));
                }
                self.connect_received = true;
                self.shared
                    .version
                    .store(p.client_version(), Ordering::SeqCst);
                // Older clients send no mechanism and log in with their
                // sessions.
                if self.connection_security
                    && let Some(mechanism) = p.auth_mechanism()
                {
                    let mechanism = mechanism.to_string_lossy();
                    let result = if mechanism == "PLAIN" {
                        let (user, password) = decode_plain(p.auth_data());
                        self.authenticate(Some(user), Some(password)).await
                    } else {
                        Err(SecurityError(format!(
                            "AMQ229261: Authentication mechanism not supported: {mechanism}"
                        )))
                    };
                    match result {
                        Ok(_) => self.on_authenticated(),
                        Err(e) => {
                            let e = BrokerError::security(e);
                            self.shared.send(
                                CHANNEL_SESSION,
                                ActiveMQExceptionMessage {
                                    code: e.kind.code(),
                                    message: Some(e.message.as_str().into()),
                                },
                            );
                            return Err(ConnError::Protocol(
                                "connection authentication failed".into(),
                            ));
                        }
                    }
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
            self.shared.client.set_client_id(Some(client_id.clone()));
        }
        // `validateUser`: a refused login gets the security exception and
        // no session. Login modules may block (LDAP), so they run off the
        // connection's task.
        let authenticated = match self.broker.security() {
            None => Authenticated::default(),
            Some(_) => {
                let result = self
                    .authenticate(request.username.clone(), request.password.clone())
                    .await;
                match result {
                    Ok(a) => {
                        self.on_authenticated();
                        a
                    }
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
        let closer = tx.clone();
        session.entry().set_closer(Box::new(move |force| {
            let _ = closer.send(SessionInput::Close(force));
        }));
        // The session's packets are handled for its connection's subject
        // and address (ActiveMQPacketHandler's AuditLogger thread locals).
        let context = crate::audit::Context::Connection(self.shared.client.clone());
        let backlog = Arc::new(AtomicUsize::new(0));
        self.session_tasks.push(tokio::spawn(crate::audit::scoped(
            context,
            session.run(rx, backlog.clone()),
        )));
        self.sessions
            .insert(channel_id, SessionLink { tx, backlog });
        self.shared.send(
            CHANNEL_SESSION,
            CreateSessionResponseMessage {
                server_version: version::CURRENT,
            },
        );
    }

    /// `validateUser`. Login modules may block (LDAP), so they run off the
    /// connection's task.
    async fn authenticate(
        &self,
        user: Option<String>,
        password: Option<String>,
    ) -> Result<Authenticated, SecurityError> {
        let Some(store) = self.broker.security().cloned() else {
            return Ok(Authenticated::default());
        };
        let info = self.shared.security_info();
        tokio::task::spawn_blocking(move || {
            store.authenticate(user.as_deref(), password.as_deref(), &info)
        })
        .await
        .unwrap_or_else(|e| Err(SecurityError(format!("login failed: {e}"))))
    }

    /// `FederationChannelHandler.handlePacket`: a downstream asks this broker
    /// to federate from the broker that sent it, which must have logged in
    /// as a user with a `downstream-authorization` role when security is
    /// on. The federation goes with the connection.
    fn handle_federation(&mut self, packet: Packet) -> Result<(), ConnError> {
        let Packet::FederationDownstreamConnectMessage(m) = packet else {
            tracing::debug!("ignoring {} on the federation channel", packet.name());
            return Ok(());
        };
        let remote = self.shared.client.remote_address().to_owned();
        let subject = self.shared.client.subject();
        let security = self.broker.security().is_some();
        if security {
            if !self.authenticated {
                tracing::warn!(
                    "AMQ224158: Unable to process Core federation downstream request from {remote}. User is not authenticated. Closing connection."
                );
                return Err(ConnError::Protocol(
                    "federation downstream unauthenticated".into(),
                ));
            }
            if !self
                .broker
                .federation()
                .authorize_downstream(true, subject.as_deref())
            {
                let user = subject
                    .as_deref()
                    .and_then(|s| s.users().next().map(str::to_owned))
                    .unwrap_or_default();
                tracing::warn!(
                    "AMQ224159: Unable to process Core federation downstream request from {remote}. User {user} is not authorized. Closing connection."
                );
                return Err(ConnError::Protocol(
                    "federation downstream unauthorized".into(),
                ));
            }
        }
        let acceptor = (Some(self.acceptor.host.clone()), Some(self.acceptor.port));
        let (config, connector) = crate::federation::downstream_request(&m.0, acceptor);
        let name = config.name.clone();
        self.broker.bridges().add_connector(connector);
        self.broker.federation().deploy(config);
        if !self.federations.contains(&name) {
            self.federations.push(name.clone());
        }
        let user = subject
            .as_deref()
            .and_then(|s| s.users().next().map(str::to_owned))
            .unwrap_or_else(|| "anonymous".to_owned());
        tracing::info!(
            "AMQ224160: Core federation downstream {name} has been deployed. Request sent by user {user} from {remote}."
        );
        Ok(())
    }

    /// The connection is gone: fail the sessions still open, then remove the
    /// temporary queues (`TempResourceCleanerUpper`).
    async fn destroy(&mut self) {
        let remote = self.shared.client.remote_address().to_owned();
        for name in self.federations.drain(..) {
            if self.broker.federation().undeploy(&name) {
                tracing::info!(
                    "AMQ224161: Connection from {remote} closed. Undeployed Core federation {name}."
                );
            }
        }
        for (_, session) in self.sessions.drain() {
            let _ = session.tx.send(SessionInput::ConnectionFailed);
        }
        for task in self.session_tasks.drain(..) {
            let _ = task.await;
        }
        self.shared.client.remove_temp_queues(&self.broker);
        let _ = self.shared.out.send(Outgoing::Close);
    }
}

/// `DisconnectReason.SCALE_DOWN`: what `freeze` gives as the reason, with
/// no target, whenever the broker stops.
const DISCONNECT_REASON_SCALE_DOWN: i8 = 2;

/// `INITIAL_ACTIVEMQ_INCREMENTING_VERSION`: older clients are HornetQ's.
const INITIAL_ACTIVEMQ_VERSION: i32 = 126;

/// Tells a connection subscribed to topology updates of the cluster
/// connection's members (`sendPostAuthTopology`'s listener).
struct ClientTopologyListener {
    conn: Arc<ConnShared>,
}

impl ClientTopologyListener {
    /// `BackwardsCompatibilityUtils.checkTCPPairConversion`: HornetQ
    /// clients are given HornetQ's factory class.
    fn convert(&self, tc: &Option<TransportConfiguration>) -> Flagged<TransportConfiguration> {
        let version = self.conn.version();
        Flagged(tc.clone().map(|mut tc| {
            if version < INITIAL_ACTIVEMQ_VERSION {
                let name = tc
                    .factory_class_name
                    .to_string_lossy()
                    .replace("org.apache.activemq.artemis", "org.hornetq")
                    .replace("ActiveMQ", "HornetQ");
                tc.factory_class_name = name.as_str().into();
            }
            tc
        }))
    }
}

impl crate::cluster::TopologyListener for ClientTopologyListener {
    fn node_up(&self, m: &crate::cluster::Member, last: bool) {
        let version = self.conn.version();
        let node_id = JString::from(m.node_id.as_str());
        let update = Some(TopologyUpdate {
            primary: self.convert(&m.primary),
            backup: self.convert(&m.backup),
            last,
        });
        let backup_group_name = m.backup_group_name.as_deref().map(JString::from);
        let scale_down_group_name = m.scale_down_group_name.as_deref().map(JString::from);
        let packet: Packet = if version >= version::ARTEMIS_2_18_0 {
            ClusterTopologyChangeMessageV4 {
                node_id,
                unique_event_id: m.unique_event_id,
                update,
                backup_group_name,
                scale_down_group_name,
                server_version: version::CURRENT,
            }
            .into()
        } else if version >= 125 {
            ClusterTopologyChangeMessageV3 {
                node_id,
                unique_event_id: m.unique_event_id,
                update,
                backup_group_name,
                scale_down_group_name,
            }
            .into()
        } else if version >= 122 {
            ClusterTopologyChangeMessageV2 {
                node_id,
                unique_event_id: m.unique_event_id,
                update,
                backup_group_name: Some(backup_group_name),
            }
            .into()
        } else {
            ClusterTopologyChangeMessage { node_id, update }.into()
        };
        self.conn.send(CHANNEL_PING, packet);
    }

    fn node_down(&self, unique_event_id: i64, node_id: &str) {
        let node_id = JString::from(node_id);
        let packet: Packet = if self.conn.version() >= 122 {
            ClusterTopologyChangeMessageV2 {
                node_id,
                unique_event_id,
                update: None,
                backup_group_name: Some(None),
            }
            .into()
        } else {
            ClusterTopologyChangeMessage {
                node_id,
                update: None,
            }
            .into()
        };
        self.conn.send(CHANNEL_PING, packet);
    }
}

/// `DisconnectConsumerWithKillMessage.VERSION_INTRODUCED`.
const DISCONNECT_CONSUMER_KILL_VERSION: i32 = 128;

/// `Topology.PRE_AUTH_NODE_ID`.
const PRE_AUTH_NODE_ID: &str = "00000000-0000-0000-0000-000000000000";
/// `Topology.PRE_AUTH_CONNECTOR_NAME`.
const PRE_AUTH_CONNECTOR_NAME: &str = "PRE_AUTH_CONNECTOR";
use crate::cluster::NETTY_CONNECTOR_FACTORY;

/// `ConnectMessage.decodePlainAuthData`: RFC 4616 `authzid NUL authcid NUL
/// passwd`.
fn decode_plain(data: Option<&[u8]>) -> (String, String) {
    let Some(data) = data.filter(|d| !d.is_empty()) else {
        return (String::new(), String::new());
    };
    let text = String::from_utf8_lossy(data);
    let mut fields = text.splitn(3, '\0');
    let _authzid = fields.next();
    let user = fields.next().unwrap_or_default().to_owned();
    let password = fields.next().unwrap_or_default().to_owned();
    (user, password)
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
