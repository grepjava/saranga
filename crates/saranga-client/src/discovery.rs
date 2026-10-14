//! Server discovery over UDP multicast: the datagrams brokers broadcast
//! their connectors in, and the discovery group that collects them.
//!
//! Ported from `UDPBroadcastEndpointFactory`, `BroadcastGroupImpl`
//! (`broadcastConnectors`, the datagram's layout), `DiscoveryGroup` and
//! `ServerLocatorConfig.isDiscoveryEnabled`. JGroups endpoints are not
//! supported.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use saranga_buffer::{Codec, JString, Reader, Writer};
use saranga_core_protocol::transport::TransportConfiguration;
use tokio::net::UdpSocket;
use tokio::sync::{Notify, watch};

/// `ServerLocatorConfig.DISCOVERY_ENABLED_PROPERTY`.
pub const DISCOVERY_ENABLED_PROPERTY: &str = "artemis.discovery.enabled";
/// `ServerLocatorConfig.DISCOVERY_ENABLED_ENV_VAR`.
pub const DISCOVERY_ENABLED_ENV_VAR: &str = "ARTEMIS_DISCOVERY_ENABLED";

/// `ActiveMQClient.DEFAULT_DISCOVERY_REFRESH_TIMEOUT`.
pub const DEFAULT_REFRESH_TIMEOUT: Duration = Duration::from_millis(10_000);
/// `ActiveMQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT`.
pub const DEFAULT_INITIAL_WAIT_TIMEOUT: Duration = Duration::from_millis(10_000);

/// Unknown, disabled or enabled.
static ENABLED: AtomicU8 = AtomicU8::new(0);

/// `ServerLocatorConfig.isDiscoveryEnabled`: whether discovery may be used,
/// as [`set_discovery_enabled`] set it, else as the
/// `ARTEMIS_DISCOVERY_ENABLED` environment variable says. Off by default.
pub fn discovery_enabled() -> bool {
    match ENABLED.load(Ordering::Acquire) {
        1 => false,
        2 => true,
        _ => {
            let on = std::env::var(DISCOVERY_ENABLED_ENV_VAR)
                .is_ok_and(|v| v.trim().eq_ignore_ascii_case("true"));
            set_discovery_enabled(on);
            on
        }
    }
}

/// Sets the switch, as the `artemis.discovery.enabled` system property
/// does (it wins over the environment variable).
pub fn set_discovery_enabled(enabled: bool) {
    ENABLED.store(if enabled { 2 } else { 1 }, Ordering::Release);
}

/// `AMQ219070`, refusing discovery while it is disabled.
pub(crate) fn ensure_discovery_enabled() -> crate::Result<()> {
    if discovery_enabled() {
        Ok(())
    } else {
        Err(crate::Error::amq(
            crate::ErrorType::IllegalState,
            "AMQ219070: The server discovery is disabled by default. See the Server Discovery section in the user manual for security requirements and enablement.",
        ))
    }
}

/// `UDPBroadcastEndpointFactory`'s settings.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EndpointConfig {
    /// `groupAddress`.
    pub group_address: String,
    /// `groupPort`.
    pub group_port: u16,
    /// `localBindAddress`.
    pub local_bind_address: Option<String>,
    /// `localBindPort`; -1 for none.
    pub local_bind_port: i32,
}

impl EndpointConfig {
    pub fn new(group_address: impl Into<String>, group_port: u16) -> Self {
        Self {
            group_address: group_address.into(),
            group_port,
            local_bind_address: None,
            local_bind_port: -1,
        }
    }

    fn group(&self) -> io::Result<SocketAddr> {
        resolve(&self.group_address, self.group_port)
    }

    fn local_address(&self) -> io::Result<Option<IpAddr>> {
        self.local_bind_address
            .as_deref()
            .map(|a| resolve(a, 0).map(|s| s.ip()))
            .transpose()
    }
}

/// `InetAddress.getByName`.
fn resolve(host: &str, port: u16) -> io::Result<SocketAddr> {
    std::net::ToSocketAddrs::to_socket_addrs(&(host, port))?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("unknown host {host}")))
}

fn unspecified(like: IpAddr) -> IpAddr {
    match like {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    }
}

/// `UDPBroadcastEndpoint` opened as a broadcaster (`openBroadcaster`).
pub struct Broadcaster {
    socket: std::net::UdpSocket,
    group: SocketAddr,
}

impl Broadcaster {
    /// `openBroadcaster`: bound to `localBindPort` if set; with only a
    /// `localBindAddress`, to a random port from 3000 to 4000 on it;
    /// else anywhere.
    pub fn open(config: &EndpointConfig) -> io::Result<Self> {
        let group = config.group()?;
        let local = config.local_address()?;
        let socket = if config.local_bind_port != -1 {
            std::net::UdpSocket::bind((
                local.unwrap_or_else(|| unspecified(group.ip())),
                config.local_bind_port as u16,
            ))?
        } else {
            let mut found = None;
            if let Some(local) = local {
                for _ in 0..100 {
                    let port = random_port();
                    match std::net::UdpSocket::bind((local, port)) {
                        Ok(s) => {
                            tracing::warn!(
                                "AMQ212056: local-bind-address specified for broadcast group but no local-bind-port. Using random port for UDP Broadcast ({})",
                                SocketAddr::new(local, port)
                            );
                            found = Some(s);
                            break;
                        }
                        Err(e) => tracing::warn!(
                            "AMQ212057: Error creating broadcast socket on {}, retrying with a different port: {e}",
                            SocketAddr::new(local, port)
                        ),
                    }
                }
            }
            match found {
                Some(s) => s,
                None => std::net::UdpSocket::bind((unspecified(group.ip()), 0))?,
            }
        };
        Ok(Self { socket, group })
    }

    /// `broadcast`: one datagram to the group.
    pub fn broadcast(&self, data: &[u8]) -> io::Result<()> {
        self.socket.send_to(data, self.group).map(|_| ())
    }
}

/// `RandomUtil.randomInterval(3000, 4000)`.
fn random_port() -> u16 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    3000 + (nanos % 1000) as u16
}

/// `openClient`: a socket that receives the group's datagrams. Like
/// `MulticastSocket`, it shares its port (`SO_REUSEADDR`); on Linux it is
/// bound to the group address so that other groups on the port are not
/// heard, as Java binds it there.
fn open_client(config: &EndpointConfig) -> io::Result<UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};
    let group = config.group()?;
    let local = config.local_address()?;
    let socket = Socket::new(Domain::for_address(group), Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    let wildcard = SocketAddr::new(unspecified(group.ip()), group.port());
    if cfg!(any(
        target_os = "linux",
        target_os = "solaris",
        target_os = "illumos"
    )) {
        if let Err(e) = socket.bind(&group.into()) {
            tracing::warn!(
                "AMQ212049: Could not bind to {} ({} address); make sure your discovery group-address is of the same type as the IP stack (IPv4 or IPv6).\nIgnoring discovery group-address, but this may lead to cross talking. {e}",
                group.ip(),
                if group.is_ipv4() { "IPv4" } else { "IPv6" }
            );
            socket.bind(&wildcard.into())?;
        }
    } else {
        socket.bind(&wildcard.into())?;
    }
    match (group.ip(), local) {
        (IpAddr::V4(g), Some(IpAddr::V4(l))) => {
            socket.set_multicast_if_v4(&l)?;
            socket.join_multicast_v4(&g, &l)?;
        }
        (IpAddr::V4(g), _) => socket.join_multicast_v4(&g, &Ipv4Addr::UNSPECIFIED)?,
        (IpAddr::V6(g), _) => socket.join_multicast_v6(&g, 0)?,
    }
    socket.set_nonblocking(true)?;
    UdpSocket::from_std(socket.into())
}

/// What a broker broadcasts (`BroadcastGroupImpl.broadcastConnectors`): its
/// node ID, the broadcast group's unique ID and its connectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Broadcast {
    pub node_id: String,
    pub unique_id: String,
    pub connectors: Vec<TransportConfiguration>,
}

impl Broadcast {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.string(&JString::from(self.node_id.as_str()))
            .string(&JString::from(self.unique_id.as_str()))
            .i32(self.connectors.len() as i32);
        for c in &self.connectors {
            c.write(&mut w);
        }
        w.into_vec()
    }

    pub fn decode(data: &[u8]) -> saranga_buffer::Result<Self> {
        let mut r = Reader::new(data);
        let node_id = r.string()?.to_string_lossy();
        let unique_id = r.string()?.to_string_lossy();
        let count = r.i32()?;
        let mut connectors = Vec::new();
        for _ in 0..count.max(0) {
            connectors.push(TransportConfiguration::read(&mut r)?);
        }
        Ok(Self {
            node_id,
            unique_id,
            connectors,
        })
    }
}

/// `DiscoveryGroupConfiguration`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryConfig {
    pub name: String,
    pub endpoint: EndpointConfig,
    /// `refreshTimeout`: how long a node is kept without a broadcast.
    pub refresh_timeout: Duration,
    /// `discoveryInitialWaitTimeout`: how long a client waits for the first
    /// broadcast.
    pub initial_wait_timeout: Duration,
}

impl DiscoveryConfig {
    pub fn new(endpoint: EndpointConfig) -> Self {
        Self {
            name: crate::uuid::to_string(&crate::uuid::time_based()),
            endpoint,
            refresh_timeout: DEFAULT_REFRESH_TIMEOUT,
            initial_wait_timeout: DEFAULT_INITIAL_WAIT_TIMEOUT,
        }
    }
}

/// `DiscoveryEntry`: a node's connector, as last broadcast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryEntry {
    pub node_id: String,
    pub connector: TransportConfiguration,
    pub last_update: Instant,
}

#[derive(Default)]
struct State {
    /// By node ID: the last of the connectors it broadcast, as `put`
    /// keeps it.
    connectors: BTreeMap<String, DiscoveryEntry>,
    /// `uniqueIDMap`.
    unique_ids: HashMap<String, String>,
    /// `received`, reset by `waitForBroadcast`.
    received: bool,
}

/// `DiscoveryGroup`: listens on the group and keeps every other node's
/// connectors, forgetting a node whose broadcasts stop for
/// `refresh-timeout` (checked as datagrams arrive).
pub struct DiscoveryGroup {
    name: String,
    node_id: String,
    refresh_timeout: Duration,
    state: Mutex<State>,
    received: Notify,
    /// Bumped whenever the entries change (`callListeners`).
    changes: watch::Sender<u64>,
    stop: watch::Sender<bool>,
}

impl DiscoveryGroup {
    /// `start`: joins the group and receives until stopped. `node_id` is
    /// this node's, whose own broadcasts are ignored.
    pub fn start(config: &DiscoveryConfig, node_id: &str) -> io::Result<Arc<Self>> {
        let socket = open_client(&config.endpoint)?;
        let group = Arc::new(Self {
            name: config.name.clone(),
            node_id: node_id.to_owned(),
            refresh_timeout: config.refresh_timeout,
            state: Mutex::new(State::default()),
            received: Notify::new(),
            changes: watch::channel(0).0,
            stop: watch::channel(false).0,
        });
        tracing::debug!("Starting Discovery Group for {}", config.name);
        let weak = Arc::downgrade(&group);
        let mut stop = group.stop.subscribe();
        tokio::spawn(async move {
            let mut data = vec![0u8; 65535];
            loop {
                let received = tokio::select! {
                    r = socket.recv_from(&mut data) => r,
                    _ = stop.wait_for(|s| *s) => return,
                };
                let Some(group) = weak.upgrade() else { return };
                match received {
                    Ok((len, _)) => group.handle(&data[..len]),
                    Err(e) => tracing::warn!(
                        "AMQ212065: {} getting exception when receiving broadcasting: {e}",
                        group.name
                    ),
                }
            }
        });
        Ok(group)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `DiscoveryRunnable.run`, for one datagram.
    fn handle(&self, data: &[u8]) {
        let broadcast = match Broadcast::decode(data) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("AMQ214010: Failed to receive datagram: {e}");
                return;
            }
        };
        let changed = {
            let mut st = self.state();
            // `checkUniqueID`.
            match st.unique_ids.get(&broadcast.node_id) {
                Some(current) if *current != broadcast.unique_id => {
                    tracing::warn!(
                        "AMQ212034: There are more than one servers on the network broadcasting the same node id. You will see this message exactly once (per node) if a node is restarted, in which case it can be safely ignored. But if it is logged continuously it means you really do have more than one node on the same network active concurrently with the same node id. This could occur if you have a backup node active at the same time as its primary node. nodeID={}",
                        broadcast.node_id
                    );
                    st.unique_ids
                        .insert(broadcast.node_id.clone(), broadcast.unique_id.clone());
                }
                Some(_) => {}
                None => {
                    st.unique_ids
                        .insert(broadcast.node_id.clone(), broadcast.unique_id.clone());
                }
            }
            if broadcast.node_id == self.node_id {
                let expired = self.check_expiration(&mut st);
                drop(st);
                if expired {
                    self.changes.send_modify(|v| *v += 1);
                }
                return;
            }
            let now = Instant::now();
            let mut changed = false;
            for connector in broadcast.connectors {
                let entry = DiscoveryEntry {
                    node_id: broadcast.node_id.clone(),
                    connector,
                    last_update: now,
                };
                if st
                    .connectors
                    .insert(broadcast.node_id.clone(), entry)
                    .is_none()
                {
                    changed = true;
                }
            }
            changed | self.check_expiration(&mut st)
        };
        if changed && !*self.stop.borrow() {
            self.changes.send_modify(|v| *v += 1);
        }
        self.state().received = true;
        self.received.notify_waiters();
    }

    /// `checkExpiration`.
    fn check_expiration(&self, st: &mut State) -> bool {
        let now = Instant::now();
        let before = st.connectors.len();
        st.connectors
            .retain(|_, e| e.last_update + self.refresh_timeout > now);
        st.connectors.len() != before
    }

    /// `getDiscoveryEntries`.
    pub fn entries(&self) -> Vec<DiscoveryEntry> {
        self.state().connectors.values().cloned().collect()
    }

    /// `waitForBroadcast`: whether a broadcast from another node arrived
    /// since the last call, waiting up to `timeout` for one (without
    /// limit when `None`, Java's 0).
    pub async fn wait_for_broadcast(&self, timeout: Option<Duration>) -> bool {
        let deadline = timeout.map(|t| tokio::time::Instant::now() + t);
        let mut stop = self.stop.subscribe();
        loop {
            let notified = self.received.notified();
            {
                let mut st = self.state();
                if st.received || *stop.borrow() {
                    let r = st.received;
                    st.received = false;
                    return r;
                }
            }
            let wait = async {
                tokio::select! {
                    _ = notified => {}
                    _ = stop.wait_for(|s| *s) => {}
                }
            };
            match deadline {
                Some(d) => {
                    if tokio::time::timeout_at(d, wait).await.is_err() {
                        let mut st = self.state();
                        let r = st.received;
                        st.received = false;
                        return r;
                    }
                }
                None => wait.await,
            }
        }
    }

    /// Told whenever the entries change (`registerListener`).
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changes.subscribe()
    }

    /// `stop`.
    pub fn stop(&self) {
        self.stop.send_replace(true);
        self.state().received = false;
    }
}

impl Drop for DiscoveryGroup {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use saranga_core_protocol::transport::ParamValue;

    fn connector(port: i32) -> TransportConfiguration {
        TransportConfiguration {
            name: "artemis".into(),
            factory_class_name:
                "org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory".into(),
            params: vec![
                (
                    "port".into(),
                    ParamValue::String(port.to_string().as_str().into()),
                ),
                ("host".into(), ParamValue::String("localhost".into())),
            ],
        }
    }

    #[test]
    fn a_broadcast_round_trips() {
        let b = Broadcast {
            node_id: "n1".to_owned(),
            unique_id: "u1".to_owned(),
            connectors: vec![connector(61616), connector(61617)],
        };
        let data = b.encode();
        // As Java reads it, out of a 65535-byte datagram buffer.
        let mut padded = data.clone();
        padded.resize(65535, 0);
        assert_eq!(Broadcast::decode(&padded).unwrap(), b);
    }

    #[tokio::test]
    async fn a_group_hears_another_node_and_not_itself() {
        let mut endpoint = EndpointConfig::new("231.7.7.7", 0);
        // A free port for the group.
        endpoint.group_port = std::net::UdpSocket::bind("0.0.0.0:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut config = DiscoveryConfig::new(endpoint.clone());
        config.refresh_timeout = Duration::from_secs(10);
        let Ok(group) = DiscoveryGroup::start(&config, "me") else {
            // No multicast here.
            return;
        };
        let broadcaster = Broadcaster::open(&endpoint).unwrap();
        let mut changes = group.subscribe();
        let own = Broadcast {
            node_id: "me".to_owned(),
            unique_id: "u0".to_owned(),
            connectors: vec![connector(1)],
        };
        let other = Broadcast {
            node_id: "other".to_owned(),
            unique_id: "u1".to_owned(),
            connectors: vec![connector(2)],
        };
        let mut heard = false;
        for _ in 0..20 {
            broadcaster.broadcast(&own.encode()).unwrap();
            broadcaster.broadcast(&other.encode()).unwrap();
            if group
                .wait_for_broadcast(Some(Duration::from_millis(250)))
                .await
            {
                heard = true;
                break;
            }
        }
        if !heard {
            // Multicast loopback is not available on this host.
            return;
        }
        assert!(changes.has_changed().unwrap());
        changes.mark_unchanged();
        let entries = group.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].node_id, "other");
        assert_eq!(entries[0].connector, connector(2));
        group.stop();
        assert!(!group.wait_for_broadcast(None).await);
    }
}
