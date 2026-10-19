//! Connections. Ported from `ClientSessionFactoryImpl` (pings, the failure
//! check, reconnection and failover), `ServerLocatorImpl` (the initial
//! connectors, the topology, the connector pairs to fail over to and the
//! topology wait in `createSessionFactory`), `ActiveMQClientProtocolManager`
//! (handshake, topology subscription, session creation, the channel 0
//! handler) and `RemotingConnectionImpl` (channel IDs, frame dispatch).
//!
//! A [`Connection`] is a session factory with one transport connection at a
//! time (`ConnInner`, the `RemotingConnection`). One task reads frames and
//! dispatches them by channel ID; another writes frames in the order they
//! were queued. Sends never wait for the socket.
//!
//! When the transport fails and `reconnect_attempts` or `failover_attempts`
//! is not 0, the factory connects again (`failoverOrReconnect`): to the same
//! server or the backup the topology paired with it, then to the other
//! connectors, and recreates its sessions on the new connection
//! (`ClientSessionImpl.handleFailover`). Sends wait while it does, and a
//! blocking call in progress fails with `UNBLOCKED`. Otherwise the sessions
//! fail with the connection.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use saranga_core_protocol::packets::cluster::{
    ClusterConnectMessage, ConnectMessage, NodeAnnounceMessage,
    SubscribeClusterTopologyUpdatesMessageV2, TopologyUpdate,
};
use saranga_core_protocol::packets::cluster_types::{MapValue, PersistedMap};
use saranga_core_protocol::packets::connection::{
    CreateSessionMessage, CreateSessionMessageV2, Ping,
};
use saranga_core_protocol::transport::{ParamValue, TransportConfiguration};
use saranga_core_protocol::{
    Context, Direction, FRAME_HEADER_SIZE, HANDSHAKE, Packet, read_frame, types, version,
};
use saranga_transport::TlsConnector;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::channel::{Channel, correlation_id, is_response, set_correlation_id};
use crate::defaults;
use crate::error::{Error, ErrorType, Result};
use crate::message::now_millis;
use crate::session::{Session, SessionInner, SessionOptions};
use crate::uuid;

/// `ChannelImpl.CHANNEL_ID.CLUSTER`.
const CLUSTER_CHANNEL: i64 = 3;

/// `ChannelImpl.CHANNEL_ID.USER`: the first channel ID handed to sessions.
const FIRST_USER_CHANNEL: i64 = 10;

/// `Topology.PRE_AUTH_NODE_ID`: the node a broker announces before the
/// connection authenticates.
const PRE_AUTH_NODE_ID: &str = "00000000-0000-0000-0000-000000000000";

/// `TransportConstants.DEFAULT_CONNECT_TIMEOUT` (10 s).
const CONNECT_TIMEOUT: Duration = Duration::from_millis(10_000);

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
    /// `callFailoverTimeout`: how long a call waits for a failover in
    /// progress. `None` is -1: as long as the failover takes.
    pub call_failover_timeout: Option<Duration>,
    /// `connectionUser` and `connectionPassword`: with a user, the
    /// connection authenticates (a `ConnectMessage` with PLAIN
    /// credentials) before its first session, as the JMS client's
    /// `createConnection(user, password)` makes it.
    pub user: Option<String>,
    pub password: Option<String>,
    /// `isClusterConnection`: the topology subscription asks for the
    /// cluster's topology, as a broker's cluster and backup connectors do.
    pub cluster_connection: bool,
    /// `ha`: a node leaving the topology is forgotten.
    pub ha: bool,
    /// `reconnectAttempts`: how many times to connect again to the server,
    /// or to the backup the topology paired with it, when the connection
    /// fails. -1 without limit; 0 (the default) fails the sessions at once.
    pub reconnect_attempts: i32,
    /// `failoverAttempts`: then how many times to go round the other
    /// connectors (the topology's, or the initial ones). -1 without limit.
    pub failover_attempts: i32,
    /// `initialConnectAttempts`: how many times each initial connector is
    /// tried when first connecting. -1 without limit.
    pub initial_connect_attempts: i32,
    /// `retryInterval`, `retryIntervalMultiplier` and `maxRetryInterval`:
    /// the wait between attempts, multiplied after each up to the maximum.
    pub retry_interval: Duration,
    pub retry_interval_multiplier: f64,
    pub max_retry_interval: Duration,
    /// Hears each topology change the server tells of, as a locator
    /// sharing its `Topology` with a cluster connection does.
    pub topology_listener: Option<TopologyHook>,
}

/// A change of the topology a server told of (`notifyNodeUp` and
/// `notifyNodeDown` of `ClientSessionFactoryImpl`'s topology handler).
#[derive(Debug, Clone)]
pub enum TopologyEvent {
    Up {
        node_id: String,
        unique_event_id: i64,
        primary: Option<TransportConfiguration>,
        backup: Option<TransportConfiguration>,
        backup_group_name: Option<String>,
        scale_down_group_name: Option<String>,
        last: bool,
    },
    /// The node left the topology; `disconnect` when its server said it
    /// was going (a disconnect packet) rather than in a topology update.
    Down {
        node_id: String,
        unique_event_id: i64,
        disconnect: bool,
    },
}

/// Where [`TopologyEvent`]s go.
#[derive(Clone)]
pub struct TopologyHook(pub Arc<dyn Fn(TopologyEvent) + Send + Sync>);

impl std::fmt::Debug for TopologyHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TopologyHook")
    }
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            client_failure_check_period: Some(defaults::CLIENT_FAILURE_CHECK_PERIOD),
            connection_ttl: Some(defaults::CONNECTION_TTL),
            call_timeout: defaults::CALL_TIMEOUT,
            call_failover_timeout: Some(defaults::CALL_FAILOVER_TIMEOUT),
            user: None,
            password: None,
            cluster_connection: false,
            ha: defaults::HA,
            reconnect_attempts: defaults::RECONNECT_ATTEMPTS,
            failover_attempts: defaults::FAILOVER_ATTEMPTS,
            initial_connect_attempts: defaults::INITIAL_CONNECT_ATTEMPTS,
            retry_interval: defaults::RETRY_INTERVAL,
            retry_interval_multiplier: defaults::RETRY_INTERVAL_MULTIPLIER,
            max_retry_interval: defaults::MAX_RETRY_INTERVAL,
            topology_listener: None,
        }
    }
}

/// A host's addresses, IPv4 ones first, as `InetAddress.getByName` prefers
/// them (`localhost` is 127.0.0.1 before `::1`): otherwise, on Windows, a
/// connector to `localhost` waits seconds for `::1` to be refused.
async fn resolve(addr: impl ToSocketAddrs) -> std::io::Result<Vec<std::net::SocketAddr>> {
    let mut addrs: Vec<_> = tokio::net::lookup_host(addr).await?.collect();
    addrs.sort_by_key(|a| !a.is_ipv4());
    Ok(addrs)
}

/// A transport connection: TCP, or TLS over it.
pub trait Transport: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Transport for T {}

/// `NettyConnector.createConnection`: a TCP connection to the host's
/// addresses in that order.
async fn connect_tcp(host: &str, port: u16) -> std::io::Result<TcpStream> {
    let mut last = None;
    for addr in resolve((host, port)).await? {
        match TcpStream::connect(addr).await {
            Ok(s) => return Ok(s),
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or_else(|| std::io::Error::other(format!("{host} resolves to no address"))))
}

/// Where to connect: the host and port of a `TransportConfiguration`, and
/// its TLS if `sslEnabled`. Connectors are equal when their hosts and ports
/// are.
#[derive(Debug, Clone)]
pub struct Connector {
    pub host: String,
    pub port: u16,
    pub tls: Option<Arc<TlsConnector>>,
}

impl PartialEq for Connector {
    fn eq(&self, other: &Self) -> bool {
        self.host == other.host && self.port == other.port
    }
}

impl Eq for Connector {}

impl Connector {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            tls: None,
        }
    }

    /// The same connector with `tls`.
    pub fn with_tls(mut self, tls: Option<Arc<TlsConnector>>) -> Self {
        self.tls = tls;
        self
    }

    /// `NettyConnector.createConnection`: a TCP connection, with the TLS
    /// handshake on it if the connector has TLS, and the address it is
    /// connected to. The TCP connection is given 10 seconds, the handshake
    /// 30.
    pub async fn connect(&self) -> std::io::Result<(Box<dyn Transport>, std::net::SocketAddr)> {
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, connect_tcp(&self.host, self.port))
            .await
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))??;
        let _ = stream.set_nodelay(true);
        let peer = stream.peer_addr()?;
        Ok(match &self.tls {
            Some(tls) => (Box::new(tls.connect(&self.host, stream).await?), peer),
            None => (Box::new(stream), peer),
        })
    }

    /// The host and port of a connector a broker announced, with Netty's
    /// defaults (`localhost`, 61616), and its TLS.
    pub fn from_transport(tc: &TransportConfiguration) -> Option<Self> {
        let param = |name: &str| {
            tc.params.iter().find_map(|(k, v)| {
                (k.to_string_lossy() == name).then(|| match v {
                    ParamValue::String(s) => s.to_string_lossy(),
                    ParamValue::Int(i) => i.to_string(),
                    ParamValue::Long(l) => l.to_string(),
                    ParamValue::Bool(b) => b.to_string(),
                })
            })
        };
        let host = param("host").unwrap_or_else(|| "localhost".to_owned());
        let port = match param("port") {
            Some(p) => p.parse().ok()?,
            None => 61616,
        };
        let tls = TlsConnector::from_uri_params(param);
        Some(Self {
            host,
            port,
            tls: tls.map(Arc::new),
        })
    }

    /// `NettyConnector.isEquivalent`: the same port, and the same host or
    /// names for the same address.
    pub fn is_equivalent(&self, other: &Self) -> bool {
        if self.port != other.port {
            return false;
        }
        if self.host == other.host {
            return true;
        }
        let resolve = |host: &str| {
            std::net::ToSocketAddrs::to_socket_addrs(&(host, 0))
                .map(|a| a.map(|s| s.ip()).collect::<Vec<_>>())
                .unwrap_or_default()
        };
        let mine = resolve(&self.host);
        resolve(&other.host).iter().any(|ip| mine.contains(ip))
    }

    /// `TransportConfigurationUtil.isSameHost`: the same host and port.
    fn is_same_host(&self, other: &Self) -> bool {
        self.host == other.host && self.port == other.port
    }
}

impl std::fmt::Display for Connector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// `FailoverEventType`: what a failover listener hears.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverEvent {
    FailureDetected,
    FailoverCompleted,
    FailoverFailed,
}

type FailoverListener = Arc<dyn Fn(FailoverEvent) + Send + Sync>;

enum WriterCmd {
    Frame(Vec<u8>),
    Close,
}

/// One transport connection (`RemotingConnectionImpl`).
pub(crate) struct ConnInner {
    writer: mpsc::UnboundedSender<WriterCmd>,
    writer_task: Mutex<Option<JoinHandle<()>>>,
    /// `CoreRemotingConnection.channelVersion`.
    version: AtomicI32,
    channels: Mutex<HashMap<i64, Arc<Channel>>>,
    failure: Mutex<Option<Error>>,
    /// `AbstractRemotingConnection.dataReceived`.
    data_received: AtomicBool,
    /// Set once a topology update with `last` arrives (`topologyReady`).
    topology: watch::Sender<bool>,
    shutdown: watch::Sender<bool>,
    connection_ttl: Option<Duration>,
    pub(crate) call_timeout: Duration,
    factory: Weak<Factory>,
}

impl ConnInner {
    /// Starts the read and write tasks on a transport, sends the handshake
    /// and the first pings (`ActiveMQClientProtocolManager.connect` and
    /// `ClientSessionFactoryImpl.schedulePing`).
    fn start<S>(stream: S, options: &ConnectionOptions, factory: Weak<Factory>) -> Arc<Self>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (read, write) = tokio::io::split(stream);
        let (writer, writer_rx) = mpsc::unbounded_channel();
        let (topology, _) = watch::channel(false);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let inner = Arc::new(ConnInner {
            writer,
            writer_task: Mutex::new(None),
            version: AtomicI32::new(0),
            channels: Mutex::new(HashMap::from([
                (0, Arc::new(Channel::new(0))),
                (1, Arc::new(Channel::new(1))),
            ])),
            failure: Mutex::new(None),
            data_received: AtomicBool::new(false),
            topology,
            shutdown,
            connection_ttl: options.connection_ttl,
            call_timeout: options.call_timeout,
            factory,
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
        inner
    }

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
        // A send in the async response cache keeps the cache's ID.
        if correlation_id(&packet) <= 0 {
            let id = channel.next_correlation_id();
            set_correlation_id(&mut packet, id);
        }
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

    /// Hands a session's channel to this connection
    /// (`ChannelImpl.transferConnection`).
    pub(crate) fn add_channel(&self, channel: Arc<Channel>) {
        self.channels
            .lock()
            .expect("connection")
            .insert(channel.id, channel);
    }

    /// `ChannelImpl.CHANNEL_ID.CLUSTER`, opened on first use.
    fn cluster_channel(&self) -> Arc<Channel> {
        self.channels
            .lock()
            .expect("connection")
            .entry(CLUSTER_CHANNEL)
            .or_insert_with(|| Arc::new(Channel::new(CLUSTER_CHANNEL)))
            .clone()
    }

    fn channel(&self, id: i64) -> Option<Arc<Channel>> {
        self.channels.lock().expect("connection").get(&id).cloned()
    }

    fn ping(&self) {
        let connection_ttl = self.connection_ttl.map_or(-1, |d| d.as_millis() as i64);
        let _ = self.send(0, &Ping { connection_ttl }.into());
    }

    /// `RemotingConnectionImpl.fail`: records the failure, stops the
    /// transport, unblocks every blocking call and tells the factory, which
    /// fails over or fails the sessions.
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
        for channel in &channels {
            channel.return_blocking(&err);
        }
        if let Some(factory) = self.factory.upgrade() {
            factory.connection_failed(self, err);
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
            // No resend cache: a send is acknowledged by its response.
            return Ok(());
        }
        if let Some(confirmed) = channel.confirm(size) {
            let _ = self.send(channel.id, &confirmed);
        }
        if is_response(packet.packet_type()) {
            channel.handle_response(packet);
        } else if frame.channel_id == 0 {
            self.handle_channel0(packet);
        } else if let Err(packet) = channel.handle_raw(packet)
            && let Some(session) = channel.session()
        {
            session.handle_packet(*packet, size as i32);
        }
        Ok(())
    }

    /// `ActiveMQClientProtocolManager.Channel0Handler`.
    fn handle_channel0(&self, packet: Packet) {
        let node_id = |id: Option<saranga_buffer::SimpleString>| id.map(|n| n.to_string_lossy());
        match packet {
            Packet::DisconnectMessage(p) => self.node_disconnected(node_id(p.node_id)),
            Packet::DisconnectMessageV2(p) => self.node_disconnected(node_id(p.node_id)),
            Packet::DisconnectMessageV3(p) => self.node_disconnected(node_id(p.node_id)),
            Packet::ClusterTopologyChangeMessage(p) => self.topology_changed(
                &p.node_id.to_string_lossy(),
                p.update.as_ref(),
                (now_millis(), None, None),
            ),
            Packet::ClusterTopologyChangeMessageV2(p) => self.topology_changed(
                &p.node_id.to_string_lossy(),
                p.update.as_ref(),
                (
                    p.unique_event_id,
                    p.backup_group_name.flatten().map(|n| n.to_string_lossy()),
                    None,
                ),
            ),
            Packet::ClusterTopologyChangeMessageV3(p) => self.topology_changed(
                &p.node_id.to_string_lossy(),
                p.update.as_ref(),
                (
                    p.unique_event_id,
                    p.backup_group_name.map(|n| n.to_string_lossy()),
                    p.scale_down_group_name.map(|n| n.to_string_lossy()),
                ),
            ),
            Packet::ClusterTopologyChangeMessageV4(p) => {
                self.topology_changed(
                    &p.node_id.to_string_lossy(),
                    p.update.as_ref(),
                    (
                        p.unique_event_id,
                        p.backup_group_name.map(|n| n.to_string_lossy()),
                        p.scale_down_group_name.map(|n| n.to_string_lossy()),
                    ),
                );
                self.version.store(p.server_version, Ordering::SeqCst);
            }
            _ => {}
        }
    }

    /// `SessionFactoryTopologyHandler.nodeDisconnected`: the node leaves the
    /// topology, then `CloseRunnable` fails the connection.
    fn node_disconnected(&self, node_id: Option<String>) {
        if let (Some(factory), Some(id)) = (self.factory.upgrade(), node_id) {
            factory.notify_node_down(&id, now_millis(), true);
        }
        self.fail(Error::disconnected());
    }

    /// `notifyTopologyChange`, with the event's ID and group names.
    fn topology_changed(
        &self,
        node_id: &str,
        update: Option<&TopologyUpdate>,
        (unique_event_id, backup_group_name, scale_down_group_name): (
            i64,
            Option<String>,
            Option<String>,
        ),
    ) {
        let factory = self.factory.upgrade();
        match update {
            Some(u) => {
                if let Some(factory) = &factory {
                    factory.notify_node_up(
                        node_id,
                        u.primary.0.as_ref().and_then(Connector::from_transport),
                        u.backup.0.as_ref().and_then(Connector::from_transport),
                    );
                    if node_id != PRE_AUTH_NODE_ID
                        && let Some(hook) = &factory.options.topology_listener
                    {
                        (hook.0)(TopologyEvent::Up {
                            node_id: node_id.to_owned(),
                            unique_event_id,
                            primary: u.primary.0.clone(),
                            backup: u.backup.0.clone(),
                            backup_group_name,
                            scale_down_group_name,
                            last: u.last,
                        });
                    }
                }
                // notifyNodeUp: `if (isLast) topologyReady = true`.
                if u.last {
                    self.topology.send_replace(true);
                }
            }
            None => {
                if let Some(factory) = &factory {
                    factory.notify_node_down(node_id, unique_event_id, false);
                }
            }
        }
    }
}

/// `ClientSessionFactoryImpl` with its `ServerLocatorImpl`.
pub(crate) struct Factory {
    options: ConnectionOptions,
    /// `ServerLocatorImpl.initialConnectors`, when given.
    initial: Vec<Connector>,
    /// `ServerLocatorImpl.discoveryGroup`: with one, the initial connectors
    /// are the nodes it heard, until the topology arrives.
    discovery: Option<(
        Arc<crate::discovery::DiscoveryGroup>,
        crate::discovery::DiscoveryConfig,
    )>,
    /// The current transport connection; `None` while failing over.
    conn: Mutex<Option<Arc<ConnInner>>>,
    /// The latest transport connection, failed or not.
    latest: Mutex<Option<Arc<ConnInner>>>,
    st: Mutex<FactoryState>,
    /// Its locator's topology, shared with the other factories a
    /// [`Locator`] makes.
    locator: Arc<Mutex<LocatorState>>,
    sessions: Mutex<Vec<Weak<SessionInner>>>,
    /// `RemotingConnectionImpl.idGenerator` for session channels, carried
    /// over to each new connection (`syncIDGeneratorSequence`).
    next_channel_id: AtomicI64,
    /// `ChannelImpl.failingOver` on every session channel: sends wait while
    /// it is set.
    failing_over: watch::Sender<bool>,
    /// The factory was closed (`clientProtocolManager.isAlive()` false).
    closed: watch::Sender<bool>,
    /// `failoverLock`.
    failover_lock: tokio::sync::Mutex<()>,
    listener: Mutex<Option<FailoverListener>>,
}

#[derive(Default)]
struct FactoryState {
    /// `connectorConfig`: the connector this factory was made for.
    connector: Option<Connector>,
    /// `currentConnectorConfig`, `backupConnectorConfig` and
    /// `previousConnectorConfig`.
    current: Option<Connector>,
    backup: Option<Connector>,
    previous: Option<Connector>,
    /// `primaryNodeID`: the node of the connector in use, once the topology
    /// named it.
    primary_node_id: Option<String>,
    /// The socket's other end, as `NettyConnection.getRemoteAddress` gives
    /// it.
    remote_address: Option<String>,
}

/// `SocketAddressUtil.toString` of the `InetSocketAddress` a host and the
/// address it resolved to make: `host/ip:port`, or `ip:port` when the host
/// is that address.
fn java_remote_address(host: &str, addr: std::net::SocketAddr) -> String {
    // `Inet6Address.getHostAddress`: every group, without brackets.
    let ip = match addr.ip() {
        std::net::IpAddr::V4(v4) => v4.to_string(),
        std::net::IpAddr::V6(v6) => v6
            .segments()
            .iter()
            .map(|s| format!("{s:x}"))
            .collect::<Vec<_>>()
            .join(":"),
    };
    let literal = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
        .is_ok();
    if literal {
        format!("{ip}:{}", addr.port())
    } else {
        format!("{host}/{ip}:{}", addr.port())
    }
}

/// What a `ServerLocatorImpl` keeps for all the session factories it makes:
/// the topology and where its load balancing stands.
#[derive(Default)]
pub(crate) struct LocatorState {
    /// `ServerLocatorImpl.topology`: each node's primary and backup.
    topology: BTreeMap<String, (Option<Connector>, Option<Connector>)>,
    /// `receivedTopology`.
    received_topology: bool,
    /// `RoundRobinConnectionLoadBalancingPolicy.pos`.
    position: Option<usize>,
    /// The discovery group's connectors as last taken
    /// (`connectorsChanged`, ignored once the topology arrived).
    discovered: Vec<Connector>,
}

impl Factory {
    fn new(options: ConnectionOptions, initial: Vec<Connector>) -> Arc<Self> {
        Self::with_discovery(options, initial, None, Arc::default())
    }

    fn with_discovery(
        options: ConnectionOptions,
        initial: Vec<Connector>,
        discovery: Option<(
            Arc<crate::discovery::DiscoveryGroup>,
            crate::discovery::DiscoveryConfig,
        )>,
        locator: Arc<Mutex<LocatorState>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            options,
            initial,
            discovery,
            conn: Mutex::new(None),
            latest: Mutex::new(None),
            st: Mutex::new(FactoryState::default()),
            locator,
            sessions: Mutex::new(Vec::new()),
            next_channel_id: AtomicI64::new(FIRST_USER_CHANNEL),
            failing_over: watch::channel(false).0,
            closed: watch::channel(false).0,
            failover_lock: tokio::sync::Mutex::new(()),
            listener: Mutex::new(None),
        })
    }

    fn state(&self) -> std::sync::MutexGuard<'_, FactoryState> {
        self.st.lock().expect("factory")
    }

    fn loc(&self) -> std::sync::MutexGuard<'_, LocatorState> {
        self.locator.lock().expect("locator")
    }

    fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// Whether a failed connection is replaced rather than failing the
    /// sessions.
    pub(crate) fn can_failover(&self) -> bool {
        self.options.reconnect_attempts != 0 || self.options.failover_attempts != 0
    }

    fn current(&self) -> Option<Arc<ConnInner>> {
        self.conn.lock().expect("factory").clone()
    }

    fn latest(&self) -> Arc<ConnInner> {
        self.latest
            .lock()
            .expect("factory")
            .clone()
            .expect("a connection was made")
    }

    fn emit(&self, event: FailoverEvent) {
        let listener = self.listener.lock().expect("factory").clone();
        if let Some(listener) = listener {
            listener(event);
        }
    }

    fn live_sessions(&self) -> Vec<Arc<SessionInner>> {
        let mut sessions = self.sessions.lock().expect("factory");
        sessions.retain(|s| s.strong_count() > 0);
        sessions.iter().filter_map(Weak::upgrade).collect()
    }

    /// Waits while a failover is in progress (`ChannelImpl.waitForFailOver`),
    /// at most `callFailoverTimeout`.
    pub(crate) async fn wait_failover(&self) {
        let mut rx = self.failing_over.subscribe();
        let done = rx.wait_for(|f| !*f);
        match self.options.call_failover_timeout {
            None => {
                let _ = done.await;
            }
            Some(t) => {
                let _ = tokio::time::timeout(t, done).await;
            }
        }
    }

    /// `waitForRetry`: true when the factory closed during the wait.
    async fn wait_for_retry(&self, interval: Duration) -> bool {
        let mut closed = self.closed.subscribe();
        tokio::select! {
            _ = tokio::time::sleep(interval) => self.is_closed(),
            _ = closed.wait_for(|c| *c) => true,
        }
    }

    /// `ServerLocatorImpl.getNextRetryInterval`.
    fn next_retry_interval(&self, interval: Duration) -> Duration {
        let next = Duration::from_millis(
            (interval.as_millis() as f64 * self.options.retry_interval_multiplier) as u64,
        );
        next.min(self.options.max_retry_interval)
    }

    /// `ServerLocatorImpl.useInitConnector`.
    fn use_init_connector(loc: &LocatorState) -> bool {
        !loc.received_topology || Self::topology_pairs(loc).is_empty()
    }

    /// `topologyArray`: each node's primary and backup.
    fn topology_pairs(loc: &LocatorState) -> Vec<(Connector, Option<Connector>)> {
        loc.topology
            .values()
            .filter_map(|(p, b)| p.clone().map(|p| (p, b.clone())))
            .collect()
    }

    /// `RoundRobinConnectionLoadBalancingPolicy.select`: a random start,
    /// then each in turn.
    fn select(loc: &mut LocatorState, max: usize) -> usize {
        let pos = match loc.position {
            None => random_below(max),
            Some(p) if p + 1 >= max => 0,
            Some(p) => p + 1,
        };
        loc.position = Some(pos);
        pos
    }

    /// `initialConnectors`: those given, or those discovered.
    fn initial_connectors(&self, loc: &mut LocatorState) -> Vec<Connector> {
        let Some((group, _)) = &self.discovery else {
            return self.initial.clone();
        };
        if !loc.received_topology {
            loc.discovered = group
                .entries()
                .iter()
                .filter_map(|e| Connector::from_transport(&e.connector))
                .collect();
        }
        loc.discovered.clone()
    }

    /// `ServerLocatorImpl.selectConnector`, from the topology unless
    /// `useInitConnector` says otherwise.
    fn select_connector(&self) -> Option<(Connector, Option<Connector>)> {
        let mut loc = self.loc();
        let use_init = Self::use_init_connector(&loc);
        let pairs = Self::topology_pairs(&loc);
        let initial = self.initial_connectors(&mut loc);
        if !use_init && !pairs.is_empty() {
            let pos = Self::select(&mut loc, pairs.len());
            Some(pairs[pos].clone())
        } else if initial.is_empty() {
            None
        } else {
            let pos = Self::select(&mut loc, initial.len());
            Some((initial[pos].clone(), None))
        }
    }

    /// `ServerLocatorImpl.getConnectorsSize`.
    fn connectors_size(&self) -> usize {
        let mut loc = self.loc();
        let pairs = Self::topology_pairs(&loc).len();
        if pairs > 0 {
            pairs
        } else {
            self.initial_connectors(&mut loc).len()
        }
    }

    /// `executeDiscovery`: with a discovery group and no node heard yet,
    /// waits `initialWaitTimeout` for a broadcast, again for each of the
    /// `initialConnectAttempts`, and fails with `AMQ219012`.
    async fn execute_discovery(&self) -> Result<()> {
        let Some((group, config)) = &self.discovery else {
            return Ok(());
        };
        let mut try_number = 0;
        loop {
            {
                let mut loc = self.loc();
                if !self.initial_connectors(&mut loc).is_empty() {
                    return Ok(());
                }
            }
            if group
                .wait_for_broadcast(Some(config.initial_wait_timeout))
                .await
            {
                return Ok(());
            }
            let attempts = self.options.initial_connect_attempts;
            let retry = attempts > 0 && try_number < attempts;
            try_number += 1;
            if !retry {
                return Err(Error::amq(
                    ErrorType::ConnectionTimedOut,
                    "AMQ219012: Timed out waiting to receive initial broadcast from cluster",
                ));
            }
            tracing::warn!(
                "AMQ212077: Timed out waiting to receive initial broadcast from cluster. Retry {try_number} of {attempts}"
            );
        }
    }

    /// `SessionFactoryTopologyHandler.notifyNodeUp` +
    /// `ServerLocatorImpl.notifyNodeUp`.
    fn notify_node_up(&self, node_id: &str, primary: Option<Connector>, backup: Option<Connector>) {
        if node_id == PRE_AUTH_NODE_ID {
            return;
        }
        let mut st = self.state();
        if let (Some(p), Some(current)) = (&primary, &st.current)
            && p.is_same_host(current)
        {
            st.primary_node_id = Some(node_id.to_owned());
        }
        self.loc()
            .topology
            .insert(node_id.to_owned(), (primary.clone(), backup.clone()));
        if let (Some(p), Some(b)) = (primary, backup) {
            Self::set_backup_connector(&mut st, &p, b);
        }
    }

    /// `ClientSessionFactoryImpl.setBackupConnector`: the backup of the
    /// server in use.
    fn set_backup_connector(st: &mut FactoryState, primary: &Connector, backup: Connector) {
        let Some(current) = &st.current else {
            return;
        };
        if current.is_equivalent(primary) && !current.is_equivalent(&backup) {
            tracing::debug!("Setting up backup config = {backup} for primary = {primary}");
            st.backup = Some(backup);
        }
    }

    /// `ServerLocatorImpl.notifyNodeDown`.
    fn notify_node_down(&self, node_id: &str, unique_event_id: i64, disconnect: bool) {
        if !self.options.ha {
            return;
        }
        if let Some(hook) = &self.options.topology_listener {
            (hook.0)(TopologyEvent::Down {
                node_id: node_id.to_owned(),
                unique_event_id,
                disconnect,
            });
        }
        let mut loc = self.loc();
        loc.topology.remove(node_id);
        if loc.topology.is_empty() {
            loc.received_topology = false;
        }
    }

    /// Opens a TCP connection to the first of the current connector, the
    /// backup, the previous connector and the initial one that answers
    /// (`createTransportConnection`).
    async fn create_transport_connection(&self) -> Option<Box<dyn Transport>> {
        let candidates = {
            let st = self.state();
            let mut c: Vec<Connector> = Vec::new();
            c.extend(st.current.clone());
            c.extend(st.backup.clone());
            if let Some(p) = &st.previous
                && st.current.as_ref() != Some(p)
            {
                c.push(p.clone());
            }
            if let Some(i) = &st.connector
                && st.current.as_ref() != Some(i)
            {
                c.push(i.clone());
            }
            c
        };
        for connector in candidates {
            if let Ok((stream, peer)) = connector.connect().await {
                let mut st = self.state();
                st.remote_address = Some(java_remote_address(&connector.host, peer));
                st.previous = st.current.take();
                st.current = Some(connector);
                return Some(stream);
            }
        }
        None
    }

    /// `internalConnect`: a new connection on the transport, authenticated
    /// when there is a user or a primary node to ask for
    /// (`sendConnect`), and subscribed to the topology.
    async fn internal_connect(self: &Arc<Self>) -> Result<Option<Arc<ConnInner>>> {
        if self.is_closed() {
            return Ok(None);
        }
        if let Some(conn) = self.current() {
            return Ok(Some(conn));
        }
        let Some(stream) = self.create_transport_connection().await else {
            return Ok(None);
        };
        Ok(Some(self.establish(stream).await?))
    }

    /// Sets up a connection on an open transport.
    async fn establish<S>(self: &Arc<Self>, stream: S) -> Result<Arc<ConnInner>>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let inner = ConnInner::start(stream, &self.options, Arc::downgrade(self));
        *self.conn.lock().expect("factory") = Some(inner.clone());
        *self.latest.lock().expect("factory") = Some(inner.clone());

        // sendConnect, with the credentials (RFC 4616: authzid NUL authcid
        // NUL passwd).
        let node_id = self.state().primary_node_id.clone();
        if node_id.is_some() || self.options.user.is_some() {
            let mut entries = vec![(
                ConnectMessage::FIELD_CLIENT_VERSION,
                MapValue::Integer(version::CURRENT),
            )];
            if let Some(user) = &self.options.user {
                let password = self.options.password.as_deref().unwrap_or("");
                entries.push((
                    ConnectMessage::FIELD_AUTH_MECHANISM,
                    MapValue::String("PLAIN".into()),
                ));
                entries.push((
                    ConnectMessage::FIELD_AUTH_DATA,
                    MapValue::ByteArray(format!("\0{user}\0{password}").into_bytes()),
                ));
            }
            let connect = ConnectMessage {
                node_id: node_id.map(|n| n.as_str().into()),
                fields: Some(PersistedMap { entries }),
            };
            let channel1 = inner.channel(1).ok_or_else(Error::connection_destroyed)?;
            let sent = inner
                .send_blocking(&channel1, connect.into(), types::CONNECT_RESPONSE)
                .await;
            let refused = match sent {
                Ok(Packet::ConnectResponseMessage(r)) => {
                    inner.version.store(r.server_version(), Ordering::SeqCst);
                    (!r.ok_to_failover).then(Error::connection_destroyed)
                }
                Ok(other) => Some(Error::UnexpectedResponse(other.name())),
                Err(e) => Some(e),
            };
            if let Some(e) = refused {
                self.forget(&inner);
                inner.fail(e.clone());
                return Err(e);
            }
        }

        inner.send(
            0,
            &SubscribeClusterTopologyUpdatesMessageV2 {
                cluster_connection: self.options.cluster_connection,
                client_version: version::CURRENT,
            }
            .into(),
        )?;
        Ok(inner)
    }

    /// Drops a connection that is no longer the factory's.
    fn forget(&self, conn: &Arc<ConnInner>) {
        let mut current = self.conn.lock().expect("factory");
        if current.as_ref().is_some_and(|c| Arc::ptr_eq(c, conn)) {
            *current = None;
        }
    }

    /// `internalConnectWithRetry`: up to `attempts` tries (-1 without
    /// limit) a retry interval apart. Returns the tries that failed.
    async fn internal_connect_with_retry(
        self: &Arc<Self>,
        attempts: i32,
        old: Option<&Arc<ConnInner>>,
    ) -> i32 {
        let mut interval = self.options.retry_interval;
        let mut count = 0;
        while !self.is_closed() {
            if let Ok(Some(conn)) = self.internal_connect().await {
                // transferring old connection version into the new connection
                if let Some(old) = old {
                    conn.version.store(old.version(), Ordering::SeqCst);
                }
                tracing::debug!("Reconnection successful");
                return count;
            }
            if attempts == 0 {
                return count;
            }
            count += 1;
            if attempts != -1 && count == attempts {
                if attempts != 1 {
                    tracing::warn!(
                        "AMQ212007: Tried {attempts} times to connect. Now giving up on reconnecting it."
                    );
                }
                return count;
            }
            if self.wait_for_retry(interval).await {
                return count;
            }
            interval = self.next_retry_interval(interval);
        }
        count
    }

    /// `ServerLocatorImpl.createSessionFactory`: tries the initial
    /// connectors in turn, each `initialConnectAttempts` times, then waits
    /// for the topology.
    async fn connect_initial(self: &Arc<Self>) -> Result<()> {
        self.execute_discovery().await?;
        let max_attempts = if self.options.initial_connect_attempts == 0 {
            1
        } else {
            self.options.initial_connect_attempts
        };
        let mut attempts = 0;
        let mut interval = self.options.retry_interval;
        loop {
            let Some((connector, _)) = self.select_connector() else {
                return Err(Error::amq(
                    ErrorType::IllegalState,
                    "AMQ219003: Cannot create session factory, no connectors",
                ));
            };
            {
                let mut st = self.state();
                st.connector = Some(connector.clone());
                st.current = Some(connector);
                st.previous = None;
                st.backup = None;
            }
            match self.internal_connect().await {
                Ok(Some(_)) => break,
                Ok(None) => {}
                Err(e) => return Err(e),
            }
            attempts += 1;
            if self.options.initial_connect_attempts >= 0
                && attempts >= max_attempts * self.connectors_size().max(1) as i32
            {
                return Err(Error::cannot_connect_to_servers());
            }
            if self.wait_for_retry(interval).await {
                return Err(Error::cannot_connect_to_servers());
            }
            interval = self.next_retry_interval(interval);
        }
        self.wait_for_topology().await
    }

    /// `waitForTopology`: the topology's last update within `callTimeout`.
    async fn wait_for_topology(&self) -> Result<()> {
        let inner = self.latest();
        let mut ready = inner.topology.subscribe();
        let ready = tokio::time::timeout(self.options.call_timeout, ready.wait_for(|r| *r));
        let timed_out = !matches!(ready.await, Ok(Ok(_)));
        if let Some(e) = inner.failure() {
            return Err(e);
        }
        if timed_out {
            inner.fail(Error::topology_timed_out());
            return Err(Error::topology_timed_out());
        }
        self.loc().received_topology = true;
        Ok(())
    }

    /// The `DelegatingFailureListener`: a failure of the current connection
    /// starts a failover when one may happen, and otherwise fails every
    /// session.
    fn connection_failed(self: &Arc<Self>, conn: &ConnInner, err: Error) {
        if self.is_closed() {
            return;
        }
        let current = self
            .current()
            .filter(|c| std::ptr::eq(Arc::as_ptr(c), conn));
        let Some(current) = current else {
            return;
        };
        if self.can_failover() {
            // ChannelImpl.lock on every session channel, before anything
            // else can send on the failed connection.
            self.failing_over.send_replace(true);
            let factory = self.clone();
            tokio::spawn(async move { factory.failover_or_reconnect(current, err).await });
        } else {
            self.emit(FailoverEvent::FailureDetected);
            self.forget(&current);
            for session in self.live_sessions() {
                session.connection_failed(&err);
            }
            self.emit(FailoverEvent::FailoverFailed);
        }
    }

    /// `failoverOrReconnect`: connects again to the server or its backup,
    /// up to `reconnectAttempts` times, then to the other connector pairs,
    /// up to `failoverAttempts` rounds; recreates the sessions on the new
    /// connection, or fails them if there is none.
    async fn failover_or_reconnect(self: Arc<Self>, failed: Arc<ConnInner>, err: Error) {
        let _guard = self.failover_lock.lock().await;
        if self.is_closed() || !self.current().is_some_and(|c| Arc::ptr_eq(&c, &failed)) {
            self.failing_over.send_replace(false);
            return;
        }
        tracing::debug!("Failure captured on connection, performing failover or reconnection now");
        self.emit(FailoverEvent::FailureDetected);
        self.forget(&failed);
        let sessions = self.live_sessions();
        let mut old = failed;

        // Try to reconnect to the current connector pair.
        let reconnect_attempts = self.options.reconnect_attempts;
        let may_retry = |reconnected: bool, retries: i32, attempts: i32, closed: bool| {
            !closed && !reconnected && (attempts == -1 || retries < attempts)
        };
        let mut retries = 0;
        let mut reconnected = false;
        while may_retry(reconnected, retries, reconnect_attempts, self.is_closed()) {
            let remaining = if reconnect_attempts == -1 {
                -1
            } else {
                reconnect_attempts - retries
            };
            retries += self
                .internal_connect_with_retry(remaining, Some(&old))
                .await;
            if let Some(conn) = self.current() {
                reconnected = self.reconnect_sessions(&sessions, &conn, &err).await;
                if !reconnected {
                    self.forget(&conn);
                    conn.fail(Error::connection_destroyed());
                    old = conn;
                }
            }
            retries += 1;
            if may_retry(reconnected, retries, reconnect_attempts, self.is_closed()) {
                self.wait_for_retry(self.options.retry_interval).await;
            }
        }

        // Try to connect to the other connector pairs.
        let failover_attempts = self.options.failover_attempts;
        let mut connectors_count = 0;
        let mut failover_retries = 0;
        let mut interval = self.options.retry_interval;
        while may_retry(
            reconnected,
            failover_retries,
            failover_attempts,
            self.is_closed(),
        ) {
            connectors_count += 1;
            if let Some((primary, backup)) = self.select_connector() {
                {
                    let mut st = self.state();
                    st.connector = Some(primary.clone());
                    st.current = Some(primary);
                    if backup.is_some() {
                        st.backup = backup;
                    }
                }
                let _ = self.internal_connect().await;
            }
            if let Some(conn) = self.current() {
                reconnected = self.reconnect_sessions(&sessions, &conn, &err).await;
                if !reconnected {
                    self.forget(&conn);
                    conn.fail(Error::connection_destroyed());
                }
            }
            if connectors_count >= self.connectors_size() {
                connectors_count = 0;
                failover_retries += 1;
                if may_retry(false, failover_retries, failover_attempts, self.is_closed()) {
                    self.wait_for_retry(interval).await;
                    interval = self.next_retry_interval(interval);
                }
            }
        }

        if reconnected {
            for session in &sessions {
                session.post_handle_failover();
            }
            self.failing_over.send_replace(false);
            self.emit(FailoverEvent::FailoverCompleted);
        } else {
            for session in &sessions {
                session.connection_failed(&err);
            }
            self.failing_over.send_replace(false);
            self.emit(FailoverEvent::FailoverFailed);
        }
    }

    /// `reconnectSessions`: every session moves to the new connection; if
    /// one fails, the rest only move.
    async fn reconnect_sessions(
        &self,
        sessions: &[Arc<SessionInner>],
        conn: &Arc<ConnInner>,
        cause: &Error,
    ) -> bool {
        let mut ok = true;
        for session in sessions {
            if ok {
                if let Err(e) = session.handle_failover(conn, cause).await {
                    tracing::warn!("AMQ212051: Failed to handle failover: {e}");
                    ok = false;
                }
            } else {
                session.transfer_connection(conn);
            }
        }
        ok
    }

    /// `ActiveMQSessionContext.newCreateSession`, sent on channel 1 of the
    /// current connection.
    pub(crate) async fn send_create_session(
        &self,
        conn: &ConnInner,
        name: &str,
        channel_id: i64,
        options: &SessionOptions,
    ) -> Result<i32> {
        let channel1 = conn.channel(1).ok_or_else(Error::connection_destroyed)?;
        let request: Packet = if conn.context().is_version_support_client_id() {
            CreateSessionMessageV2 {
                name: name.into(),
                session_channel_id: channel_id,
                version: version::CURRENT,
                username: options.username.as_deref().map(Into::into),
                password: options.password.as_deref().map(Into::into),
                min_large_message_size: options.min_large_message_size,
                xa: options.xa,
                auto_commit_sends: options.auto_commit_sends,
                auto_commit_acks: options.auto_commit_acks,
                window_size: options.confirmation_window_size,
                pre_acknowledge: options.pre_acknowledge,
                default_address: None,
                client_id: options.client_id.as_deref().map(Into::into),
            }
            .into()
        } else {
            CreateSessionMessage {
                name: name.into(),
                session_channel_id: channel_id,
                version: version::CURRENT,
                username: options.username.as_deref().map(Into::into),
                password: options.password.as_deref().map(Into::into),
                min_large_message_size: options.min_large_message_size,
                xa: options.xa,
                auto_commit_sends: options.auto_commit_sends,
                auto_commit_acks: options.auto_commit_acks,
                window_size: options.confirmation_window_size,
                pre_acknowledge: options.pre_acknowledge,
                default_address: None,
            }
            .into()
        };
        match conn
            .send_blocking(&channel1, request, types::CREATESESSION_RESP)
            .await?
        {
            Packet::CreateSessionResponseMessage(r) => {
                conn.version.store(r.server_version, Ordering::SeqCst);
                if let Some(channel) = conn.channel(channel_id) {
                    channel.set_confirmation_window(options.confirmation_window_size);
                }
                Ok(r.server_version)
            }
            other => Err(Error::UnexpectedResponse(other.name())),
        }
    }
}

/// A Core protocol connection: a `ClientSessionFactory` with its
/// `ServerLocator`, reconnecting and failing over as its options say.
/// Clones are handles to the same connection.
#[derive(Clone)]
pub struct Connection {
    factory: Arc<Factory>,
}

impl Connection {
    /// Connects over TCP to one address, the factory's only connector.
    pub async fn connect(addr: impl ToSocketAddrs, options: ConnectionOptions) -> Result<Self> {
        let mut last = None;
        for addr in resolve(addr).await? {
            match TcpStream::connect(addr).await {
                Ok(stream) => {
                    stream.set_nodelay(true)?;
                    let connector = Connector::new(addr.ip().to_string(), addr.port());
                    let factory = Factory::new(options, vec![connector.clone()]);
                    {
                        let mut st = factory.state();
                        st.remote_address = Some(java_remote_address(&connector.host, addr));
                        st.connector = Some(connector.clone());
                        st.current = Some(connector);
                    }
                    return Self::start(factory, stream).await;
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last
            .unwrap_or_else(|| std::io::Error::other("no address to connect to"))
            .into())
    }

    /// As [`Connection::connect`], to a connector's host and port, with its
    /// TLS if it has any.
    pub async fn connect_connector(
        connector: Connector,
        options: ConnectionOptions,
    ) -> Result<Self> {
        let (stream, peer) = connector.connect().await?;
        let factory = Factory::new(options, vec![connector.clone()]);
        {
            let mut st = factory.state();
            st.remote_address = Some(java_remote_address(&connector.host, peer));
            st.connector = Some(connector.clone());
            st.current = Some(connector);
        }
        Self::start(factory, stream).await
    }

    /// Connects to the first of `connectors` that answers
    /// (`ServerLocatorImpl.createSessionFactory`, as a URL with a list of
    /// servers makes it), and waits for the topology. Fails with
    /// `AMQ219007` when none does.
    pub async fn connect_to(
        connectors: Vec<Connector>,
        options: ConnectionOptions,
    ) -> Result<Self> {
        let factory = Factory::new(options, connectors);
        factory.connect_initial().await?;
        Ok(Self { factory })
    }

    /// As [`Connection::connect_to`], for a [`Locator`]: its topology is
    /// shared with the locator's other connections.
    async fn connect_located(
        connectors: Vec<Connector>,
        options: ConnectionOptions,
        locator: Arc<Mutex<LocatorState>>,
    ) -> Result<Self> {
        let factory = Factory::with_discovery(options, connectors, None, locator);
        factory.connect_initial().await?;
        Ok(Self { factory })
    }

    /// Connects through a discovery group (a `udp://` URL): to the nodes
    /// it hears, as `ServerLocatorImpl` with a `DiscoveryGroupConfiguration`
    /// does, waiting `initial_wait_timeout` for the first broadcast. Fails
    /// with `AMQ219070` while discovery is disabled
    /// ([`crate::discovery::discovery_enabled`]).
    pub async fn connect_discovery(
        config: crate::discovery::DiscoveryConfig,
        options: ConnectionOptions,
    ) -> Result<Self> {
        crate::discovery::ensure_discovery_enabled()?;
        let group = crate::discovery::DiscoveryGroup::start(&config, "")?;
        let factory =
            Factory::with_discovery(options, Vec::new(), Some((group, config)), Arc::default());
        if let Err(e) = factory.connect_initial().await {
            if let Some((group, _)) = &factory.discovery {
                group.stop();
            }
            return Err(e);
        }
        Ok(Self { factory })
    }

    /// Connects over an already open transport: sends the handshake and the
    /// first pings, subscribes to topology updates and waits for the
    /// topology, as `ServerLocatorImpl.createSessionFactory` does. With no
    /// connector to go back to, the connection cannot reconnect.
    pub async fn connect_with<S>(stream: S, options: ConnectionOptions) -> Result<Self>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        Self::start(Factory::new(options, Vec::new()), stream).await
    }

    async fn start<S>(factory: Arc<Factory>, stream: S) -> Result<Self>
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        factory.establish(stream).await?;
        factory.wait_for_topology().await?;
        Ok(Self { factory })
    }

    /// The channel version: the broker's version once known.
    pub fn version(&self) -> i32 {
        self.factory.latest().version()
    }

    /// The error the connection failed with, if it has failed or been
    /// closed. While a failover is in progress, the failed connection's.
    pub fn failure(&self) -> Option<Error> {
        self.factory.latest().failure()
    }

    /// Completes once the connection in use has failed or been closed.
    pub async fn failed(&self) {
        let mut rx = self.factory.latest().shutdown.subscribe();
        let _ = rx.wait_for(|s| *s).await;
    }

    /// The server in use (`getConnectorConfiguration`).
    pub fn connector(&self) -> Option<Connector> {
        self.factory.state().current.clone()
    }

    /// The backup the topology paired with the server in use
    /// (`getBackupConnector`).
    pub fn backup_connector(&self) -> Option<Connector> {
        self.factory.state().backup.clone()
    }

    /// The server's end of the socket in use (`getRemoteAddress`), as Java
    /// writes it: `localhost/127.0.0.1:61616`.
    pub fn remote_address(&self) -> Option<String> {
        self.factory.state().remote_address.clone()
    }

    /// `setFailoverListener`: told when a failure is detected and when the
    /// failover that follows completes or fails.
    pub fn set_failover_listener(&self, listener: impl Fn(FailoverEvent) + Send + Sync + 'static) {
        *self.factory.listener.lock().expect("factory") = Some(Arc::new(listener));
    }

    /// Creates a session (`ActiveMQClientProtocolManager.createSessionContext`
    /// and `ClientSessionFactoryImpl.createSessionInternal`), after a
    /// failover in progress.
    pub async fn create_session(&self, options: SessionOptions) -> Result<Session> {
        let factory = &self.factory;
        factory.wait_failover().await;
        let conn = factory.current().unwrap_or_else(|| factory.latest());
        if let Some(e) = conn.failure() {
            return Err(e);
        }
        let name = uuid::to_string(&uuid::time_based());
        let channel_id = factory.next_channel_id.fetch_add(1, Ordering::SeqCst);
        let channel = Arc::new(Channel::new(channel_id));
        conn.add_channel(channel.clone());
        let server_version = match factory
            .send_create_session(&conn, &name, channel_id, &options)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                conn.remove_channel(channel_id);
                return Err(e);
            }
        };
        let session = SessionInner::new(
            factory.clone(),
            conn,
            channel.clone(),
            name,
            options,
            server_version,
        );
        channel.set_session(Arc::downgrade(&session));
        let mut sessions = factory.sessions.lock().expect("factory");
        sessions.retain(|s| s.strong_count() > 0);
        sessions.push(Arc::downgrade(&session));
        Ok(Session::from_inner(session))
    }

    /// `ClusterControl.authorize`: the cluster user and password on the
    /// cluster channel. `Ok(false)` when the broker refuses them.
    pub async fn cluster_connect(&self, user: &str, password: &str) -> Result<bool> {
        let conn = self.factory.latest();
        let channel = conn.cluster_channel();
        let request = ClusterConnectMessage {
            cluster_user: user.into(),
            cluster_password: password.into(),
        };
        match conn
            .send_blocking(&channel, request.into(), types::CLUSTER_CONNECT_REPLY)
            .await?
        {
            Packet::ClusterConnectReplyMessage(r) => Ok(r.authorized),
            other => Err(Error::UnexpectedResponse(other.name())),
        }
    }

    /// A blocking request on the cluster channel (`ClusterControl`'s
    /// `sendBlocking`), answered by a packet of type `expected`.
    pub async fn cluster_request(&self, packet: Packet, expected: i8) -> Result<Packet> {
        let conn = self.factory.latest();
        let channel = conn.cluster_channel();
        conn.send_blocking(&channel, packet, expected).await
    }

    /// Sends a packet on a channel the application handles itself.
    pub fn send_packet(&self, channel_id: i64, packet: Packet) -> Result<()> {
        self.factory.latest().send(channel_id, &packet)
    }

    /// The packets that arrive on a channel from now on, other than the
    /// responses to blocking requests: the application handles the channel
    /// itself (as the cluster channel, or replication's).
    pub fn channel_packets(&self, channel_id: i64) -> tokio::sync::mpsc::UnboundedReceiver<Packet> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let conn = self.factory.latest();
        let channel = if channel_id == CLUSTER_CHANNEL {
            conn.cluster_channel()
        } else {
            let mut channels = conn.channels.lock().expect("connection");
            channels
                .entry(channel_id)
                .or_insert_with(|| Arc::new(Channel::new(channel_id)))
                .clone()
        };
        channel.set_raw(tx);
        rx
    }

    /// The topology the server sent: each node with the connectors of its
    /// primary and backup.
    pub fn topology(&self) -> Vec<(String, Option<Connector>, Option<Connector>)> {
        self.factory
            .loc()
            .topology
            .iter()
            .map(|(id, (p, b))| (id.clone(), p.clone(), b.clone()))
            .collect()
    }

    /// `ClusterControl.sendNodeAnnounce`.
    pub fn node_announce(&self, announce: NodeAnnounceMessage) -> Result<()> {
        let conn = self.factory.latest();
        let channel = conn.cluster_channel();
        conn.send(channel.id, &announce.into())
    }

    /// Closes every open session, then the transport. Like the Java client,
    /// no `DisconnectMessage` is sent. A failover in progress stops.
    pub async fn close(&self) {
        self.factory.closed.send_replace(true);
        if let Some((group, _)) = &self.factory.discovery {
            group.stop();
        }
        let sessions: Vec<_> = self
            .factory
            .sessions
            .lock()
            .expect("factory")
            .drain(..)
            .filter_map(|s| s.upgrade())
            .collect();
        for session in sessions {
            session.close().await;
        }
        let conn = self.factory.latest();
        conn.fail(Error::connection_destroyed());
        let task = conn.writer_task.lock().expect("connection").take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

/// A number in `0..max` for the load-balancing policy's random start.
fn random_below(max: usize) -> usize {
    use std::hash::{BuildHasher, Hasher};
    if max <= 1 {
        return 0;
    }
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64),
    );
    (h.finish() % max as u64) as usize
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
                    ErrorType::NotConnected,
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
                    ErrorType::NotConnected,
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

/// A `ServerLocator`: the connections it makes share one topology, so that
/// once a server has told it of the cluster, each new connection goes to
/// the next node in turn (`RoundRobinConnectionLoadBalancingPolicy`),
/// before that to the initial connectors.
pub struct Locator {
    initial: Vec<Connector>,
    options: ConnectionOptions,
    state: Arc<Mutex<LocatorState>>,
}

impl Locator {
    pub fn new(initial: Vec<Connector>, options: ConnectionOptions) -> Self {
        Self {
            initial,
            options,
            state: Arc::default(),
        }
    }

    /// `createSessionFactory`.
    pub async fn connect(&self) -> Result<Connection> {
        Connection::connect_located(
            self.initial.clone(),
            self.options.clone(),
            self.state.clone(),
        )
        .await
    }

    /// The topology as the locator's connections heard it: each node with
    /// the connectors of its primary and backup.
    pub fn topology(&self) -> Vec<(String, Option<Connector>, Option<Connector>)> {
        self.state
            .lock()
            .expect("locator")
            .topology
            .iter()
            .map(|(id, (p, b))| (id.clone(), p.clone(), b.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equivalent_connectors() {
        let a = Connector::new("localhost", 61616);
        assert!(a.is_equivalent(&Connector::new("localhost", 61616)));
        assert!(a.is_equivalent(&Connector::new("127.0.0.1", 61616)));
        assert!(!a.is_equivalent(&Connector::new("localhost", 61617)));
        assert!(!a.is_same_host(&Connector::new("127.0.0.1", 61616)));
    }

    #[test]
    fn round_robin_starts_anywhere_then_goes_in_turn() {
        let mut st = LocatorState::default();
        let first = Factory::select(&mut st, 3);
        assert!(first < 3);
        assert_eq!(Factory::select(&mut st, 3), (first + 1) % 3);
        assert_eq!(Factory::select(&mut st, 3), (first + 2) % 3);
    }

    #[test]
    fn connector_from_transport() {
        let tc = TransportConfiguration {
            params: vec![
                ("host".into(), ParamValue::String("h".into())),
                ("port".into(), ParamValue::String("62616".into())),
            ],
            ..TransportConfiguration::default()
        };
        assert_eq!(
            Connector::from_transport(&tc),
            Some(Connector::new("h", 62616))
        );
        assert_eq!(
            Connector::from_transport(&TransportConfiguration::default()),
            Some(Connector::new("localhost", 61616))
        );
    }
}
