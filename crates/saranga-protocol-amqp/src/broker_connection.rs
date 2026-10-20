//! AMQP broker connections (`AMQPBrokerConnectionManager` and
//! `AMQPBrokerConnection`): the broker connects out to another AMQP peer
//! and, over that connection, sends the messages of its matching queues
//! (`sender`), takes the peer's into them (`receiver`), or both through a
//! router (`peer`).
//!
//! A connection is made again after `retry-interval` when it fails or the
//! peer closes one of its links, `reconnect-attempts` times (-1: always).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use saranga_config::{AmqpBrokerConnectConfig, AmqpConnectionElement, AmqpMatchElement};
use saranga_server::Broker;
use saranga_server::plugin::BrokerPlugin;
use saranga_server::queue::Queue;
use saranga_transport::TlsConnector;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::bridge::BridgeManager;
use crate::composite::Fields;
use crate::connection::{
    self, ConnectionHandle, ContainerIds, Ended, LocalLink, LocalLinkKind, Outgoing,
};
use crate::federation::Federation;
use crate::messaging::{Source, Target, TargetKind};
use crate::mirror::source::MirrorSource;
use crate::transport::{Attach, ReceiverSettleMode, Role, SenderSettleMode};
use crate::types::{Symbol, Value};
use crate::{AmqpConfig, Settings};

/// `AMQPMirrorControllerSource.QPID_DISPATCH_WAYPOINT_CAPABILITY`.
pub const WAYPOINT: &str = "qd.waypoint";
/// `AMQPBrokerConnectionConstants.BROKER_CONNECTION_INFO`.
pub const BROKER_CONNECTION_INFO: &str = "broker-connection-info";
/// `NettyConnector`'s connect timeout without `connect-timeout-millis`
/// (Netty's default).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Where a broker connection connects: one server of its URI, with its TLS
/// if `sslEnabled`. Endpoints are equal when their hosts, ports and
/// parameters are.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    /// The URI's parameters, the protocol's settings among them.
    pub params: HashMap<String, String>,
    pub tls: Option<Arc<TlsConnector>>,
}

impl PartialEq for Endpoint {
    fn eq(&self, other: &Self) -> bool {
        self.host == other.host && self.port == other.port && self.params == other.params
    }
}

/// `ConnectorTransportConfigurationParser`: the servers of a URI,
/// `tcp://host:port?params` or `(tcp://a:1,tcp://b:2)?params`, each with
/// its own parameters and those after the list, with Netty's defaults
/// (`localhost`, 61616).
pub fn parse_uri(uri: &str) -> Result<Vec<Endpoint>, String> {
    let uri = uri.trim();
    let (list, shared) = match uri.strip_prefix('(') {
        Some(rest) => {
            let (list, after) = rest
                .split_once(')')
                .ok_or_else(|| format!("invalid URI {uri}"))?;
            (list, after.strip_prefix('?').unwrap_or(""))
        }
        None => (uri, ""),
    };
    // `URISupport.parseQuery`: split on `&` and `;`, each name and value
    // URL-decoded.
    let params = |q: &str| -> Result<Vec<(String, String)>, String> {
        Ok(saranga_config::query_params(q)?
            .into_iter()
            .map(|(k, v)| (k, v.unwrap_or_default()))
            .collect())
    };
    let mut out = Vec::new();
    for item in list.split(',') {
        let (scheme, rest) = item
            .split_once("://")
            .ok_or_else(|| format!("invalid URI {uri}"))?;
        if scheme != "tcp" {
            return Err(format!("schema {scheme} not found"));
        }
        let (authority, query) = rest.split_once('?').unwrap_or((rest, ""));
        let authority = authority.trim_end_matches('/');
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) if !h.ends_with(']') || authority.starts_with('[') => (
                h.to_owned(),
                p.parse().map_err(|_| format!("invalid port in {uri}"))?,
            ),
            _ => (authority.to_owned(), 61616),
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let mut p: HashMap<String, String> = params(query)?.into_iter().collect();
        for (k, v) in params(shared)? {
            p.entry(k).or_insert(v);
        }
        out.push(Endpoint {
            host: if host.is_empty() {
                "localhost".to_owned()
            } else {
                host.to_owned()
            },
            port,
            tls: TlsConnector::from_uri_params(|k| p.get(k).cloned()).map(Arc::new),
            params: p,
        });
    }
    Ok(out)
}

/// The broker connections of a broker: `AMQPBrokerConnectionManager`, as
/// `ProtonProtocolManagerFactory` holds it (made when there are broker
/// connections, dropped when a reload leaves none).
#[derive(Default)]
pub struct BrokerConnectionManager(Mutex<Manager>);

#[derive(Default)]
struct Manager {
    /// `brokerConnectionManager != null`.
    present: bool,
    started: bool,
    /// `amqpConnectionsConfig`, a `HashMap` by name.
    configs: JavaMap,
    /// `amqpBrokerConnections`, by name.
    connections: HashMap<String, Arc<AmqpBrokerConnection>>,
}

/// What a broker connection is made with.
pub struct Context<'a> {
    pub broker: &'a Arc<Broker>,
    pub amqp: &'a AmqpConfig,
    pub containers: &'a Arc<ContainerIds>,
}

/// A `java.util.HashMap` of configurations by name: kept in the order they
/// were put (one put again keeps its place), with its table's capacity,
/// which grows and never shrinks, so that it is walked in Java's order.
struct JavaMap {
    entries: Vec<AmqpBrokerConnectConfig>,
    capacity: usize,
}

impl Default for JavaMap {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            capacity: 16,
        }
    }
}

impl JavaMap {
    /// `Collectors.toMap(c -> c.getName(), identity())`: a name given twice
    /// throws its `IllegalStateException`.
    fn collect(configs: &[AmqpBrokerConnectConfig]) -> Result<Self, String> {
        let mut map = Self::default();
        for c in configs {
            if let Some(previous) = map.get(&c.name) {
                return Err(format!(
                    "java.lang.IllegalStateException: Duplicate key {} (attempted merging values {} and {})",
                    c.name,
                    identity(previous),
                    identity(c)
                ));
            }
            map.put(c.clone());
        }
        Ok(map)
    }

    fn get(&self, name: &str) -> Option<&AmqpBrokerConnectConfig> {
        self.entries.iter().find(|c| c.name == name)
    }

    fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    fn put(&mut self, config: AmqpBrokerConnectConfig) {
        match self.entries.iter_mut().find(|c| c.name == config.name) {
            Some(slot) => *slot = config,
            None => {
                self.entries.push(config);
                // `resize`: past three quarters of the table.
                while self.entries.len() * 4 > self.capacity * 3 {
                    self.capacity *= 2;
                }
            }
        }
    }

    fn remove(&mut self, name: &str) {
        self.entries.retain(|c| c.name != name);
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    /// `values()`, in the order the table walks them.
    fn values(&self) -> Vec<AmqpBrokerConnectConfig> {
        let hashes: Vec<i32> = self
            .entries
            .iter()
            .map(|c| saranga_buffer::java_hash::string_hash(&c.name))
            .collect();
        saranga_buffer::java_hash::hash_map_order(&hashes, Some((self.capacity, 0.75)))
            .into_iter()
            .map(|i| self.entries[i].clone())
            .collect()
    }
}

/// `Object.toString` of an `AMQPBrokerConnectConfiguration`: its class and
/// `hashCode`, here `BrokerConnectConfiguration`'s part of it (Java's adds
/// the elements', whose types hash by identity, and the transports').
fn identity(c: &AmqpBrokerConnectConfig) -> String {
    use saranga_buffer::java_hash::string_hash;
    let s = |v: Option<&str>| v.map_or(0, string_hash);
    // Objects.hash(autostart, name, password, reconnectAttempts,
    // retryInterval, uri, user)
    let hash = [
        if c.auto_start { 1231 } else { 1237 },
        string_hash(&c.name),
        s(c.password.as_deref()),
        c.reconnect_attempts,
        c.retry_interval,
        string_hash(&c.uri),
        s(c.user.as_deref()),
    ]
    .iter()
    .fold(1i32, |h, v| h.wrapping_mul(31).wrapping_add(*v));
    format!(
        "org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration@{:x}",
        hash as u32
    )
}

/// `AMQPBrokerConnectionManager`'s logger.
const MANAGER_LOGGER: &str =
    "org.apache.activemq.artemis.protocol.amqp.connect.AMQPBrokerConnectionManager";

/// Whether a configuration has a `<mirror>`, which a reload neither
/// changes nor removes.
fn contains_mirror(c: &AmqpBrokerConnectConfig) -> bool {
    c.elements
        .iter()
        .any(|e| matches!(e, AmqpConnectionElement::Mirror(_)))
}

fn lock_manager(m: &BrokerConnectionManager) -> std::sync::MutexGuard<'_, Manager> {
    lock(&m.0)
}

impl BrokerConnectionManager {
    /// `loadProtocolServices` and `start`: with broker connections
    /// configured, the manager is made and each connection made,
    /// registered and, with `auto-start`, started.
    pub fn start(&self, cx: &Context<'_>, configs: &[AmqpBrokerConnectConfig]) {
        if configs.is_empty() {
            return;
        }
        let map = match JavaMap::collect(configs) {
            Ok(map) => map,
            Err(e) => {
                tracing::warn!(
                    target: "org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl",
                    "Unable to load protocol services for: [AMQP]\n{e}"
                );
                return;
            }
        };
        let mut m = lock_manager(self);
        m.present = true;
        m.configs = map;
        m.start(cx);
    }

    /// `updateProtocolServices`, on a configuration reload: with no manager
    /// yet one is made and started for the configured connections
    /// (`checkAddNewBrokerConnectionManager`); otherwise each new or
    /// changed connection is shut down and made again and each one no
    /// longer configured shut down (`updateConfiguration`), and the manager
    /// stops and goes when none are left.
    pub fn update(
        &self,
        cx: &Context<'_>,
        configs: &[AmqpBrokerConnectConfig],
    ) -> Result<(), String> {
        // As at startup, a TLS setting Saranga cannot honour is refused
        // rather than connected without.
        for c in configs {
            for e in parse_uri(&c.uri).unwrap_or_default() {
                saranga_transport::connector::check_params(|k| e.params.get(k).cloned())
                    .map_err(|err| format!("broker connection {}: {err}", c.name))?;
            }
        }
        let mut m = lock_manager(self);
        if !m.present {
            if configs.is_empty() {
                return Ok(());
            }
            m.configs = JavaMap::collect(configs)?;
            m.present = true;
            m.start(cx);
            return Ok(());
        }
        m.update_configuration(cx, configs)?;
        if m.configs.len() == 0 {
            m.stop(cx.broker);
            *m = Manager::default();
        }
        Ok(())
    }

    /// `stop`: each connection shuts down.
    pub fn stop(&self, broker: &Broker) {
        lock_manager(self).stop(broker);
    }
}

impl Manager {
    fn start(&mut self, cx: &Context<'_>) {
        if self.started {
            return;
        }
        self.started = true;
        for c in self.configs.values() {
            let auto_start = c.auto_start;
            self.create(cx, c, auto_start);
        }
    }

    fn stop(&mut self, broker: &Broker) {
        if !self.started {
            return;
        }
        self.started = false;
        for (_, c) in self.connections.drain() {
            c.shutdown(broker);
        }
    }

    /// `updateConfiguration`. Connections are all shut down before any is
    /// made, so that an old and a new one never connect to the same peer
    /// together.
    fn update_configuration(
        &mut self,
        cx: &Context<'_>,
        configs: &[AmqpBrokerConnectConfig],
    ) -> Result<(), String> {
        let mut new_connections: Vec<(AmqpBrokerConnectConfig, bool)> = Vec::new();
        for c in configs {
            let previous = self.configs.get(&c.name);
            if previous.is_some_and(|p| c.same_as(p)) {
                continue;
            }
            if let Some(p) = previous.filter(|p| contains_mirror(p)) {
                tracing::info!(
                    target: MANAGER_LOGGER,
                    "Skipping update of broker connection {} which contains a mirror configuration which are not reloadable.",
                    p.name
                );
                continue;
            }
            self.configs.put(c.clone());
            let connection = self.connections.remove(&c.name);
            // A connection that was started is started again.
            let auto_start = match &connection {
                None => c.auto_start,
                Some(old) => old.is_started_now() || c.auto_start,
            };
            if let Some(old) = connection {
                old.shutdown(cx.broker);
            }
            if self.started {
                // A HashMap keyed by the configuration: an equal one is
                // made once.
                match new_connections.iter_mut().find(|(n, _)| n.same_as(c)) {
                    Some(slot) => slot.1 = auto_start,
                    None => new_connections.push((c.clone(), auto_start)),
                }
            }
        }
        let configured = JavaMap::collect(configs)?;
        let removed: Vec<_> = self
            .configs
            .values()
            .into_iter()
            .filter(|c| !configured.contains(&c.name))
            .collect();
        for c in removed {
            if contains_mirror(&c) {
                tracing::info!(
                    target: MANAGER_LOGGER,
                    "Skipping remove of broker connection {} which contains a mirror configuration which are not reloadable.",
                    c.name
                );
                continue;
            }
            self.configs.remove(&c.name);
            if let Some(old) = self.connections.remove(&c.name) {
                old.shutdown(cx.broker);
            }
        }
        for (c, auto_start) in new_connections {
            self.create(cx, c, auto_start);
        }
        Ok(())
    }

    /// `createBrokerConnection`: the connection is made, registered with
    /// management and its federation and bridges installed
    /// (`initialize`), and started if asked.
    fn create(&mut self, cx: &Context<'_>, c: AmqpBrokerConnectConfig, start: bool) {
        let broker = cx.broker;
        let endpoints = match parse_uri(&c.uri) {
            Ok(e) if !e.is_empty() => e,
            Ok(_) => return,
            Err(e) => {
                tracing::warn!("broker connection {}: {e}", c.name);
                return;
            }
        };
        let mut federation = None;
        let mut bridges = Vec::new();
        for e in &c.elements {
            match e {
                // installFederation: the last one is the connection's.
                AmqpConnectionElement::Federation(f) => {
                    federation = Some(Federation::source(broker, f));
                }
                // installBridgeManager
                AmqpConnectionElement::Bridge(b) => {
                    bridges.push(BridgeManager::new(broker, b));
                }
                _ => {}
            }
        }
        let name = c.name.clone();
        let connection = Arc::new(AmqpBrokerConnection {
            config: c,
            endpoints,
            broker: Arc::downgrade(broker),
            amqp: cx.amqp.clone(),
            containers: cx.containers.clone(),
            state: Mutex::new(State::default()),
            plugin: Mutex::new(None),
            me: Mutex::new(Weak::new()),
            federation,
            bridges,
            mirrors: Mutex::new(None),
        });
        *lock(&connection.me) = Arc::downgrade(&connection);
        // initialize: registered, with management.
        broker
            .broker_connection_registry()
            .register(connection.clone());
        if start {
            connection.start_inner();
        }
        self.connections.insert(name, connection);
    }
}

#[derive(Default)]
struct State {
    started: bool,
    /// The TCP connection is up.
    connected: bool,
    /// The connection's task, while there is one.
    handle: Option<ConnectionHandle>,
    /// The connect-and-retry task.
    task: Option<tokio::task::JoinHandle<()>>,
    /// Queues with a sender or receiver on this connection
    /// (`senders`, `receivers`), by name and ID.
    senders: HashSet<(String, i64)>,
    receivers: HashSet<(String, i64)>,
}

/// One `<amqp-connection>` (`AMQPBrokerConnection`).
pub struct AmqpBrokerConnection {
    config: AmqpBrokerConnectConfig,
    endpoints: Vec<Endpoint>,
    broker: Weak<Broker>,
    amqp: AmqpConfig,
    containers: Arc<ContainerIds>,
    state: Mutex<State>,
    /// The plugin that makes links for new queues while started.
    plugin: Mutex<Option<Arc<dyn BrokerPlugin>>>,
    /// The `<federation>`, if any (`brokerFederation`).
    federation: Option<Arc<Federation>>,
    /// The `<bridge>`s (`bridgeManagers`).
    bridges: Vec<Arc<BridgeManager>>,
    /// The `<mirror>`s' sources, installed the first time the connection
    /// starts and kept from then on, the connection stopped or not.
    mirrors: Mutex<Option<Vec<Arc<MirrorSource>>>>,
    me: Mutex<Weak<AmqpBrokerConnection>>,
}

impl AmqpBrokerConnection {
    pub fn config(&self) -> &AmqpBrokerConnectConfig {
        &self.config
    }

    /// `internalStart`: registers the queue plugin and connects.
    fn start_inner(&self) {
        let Some(broker) = self.broker.upgrade() else {
            return;
        };
        let Some(me) = lock(&self.me).upgrade() else {
            return;
        };
        let mut st = lock(&self.state);
        if st.started {
            return;
        }
        st.started = true;
        // installMirrorController, the first time only.
        {
            let mut mirrors = lock(&self.mirrors);
            if mirrors.is_none() {
                let mut installed = Vec::new();
                for e in &self.config.elements {
                    if let AmqpConnectionElement::Mirror(m) = e {
                        match MirrorSource::install(&broker, &self.config.name, m) {
                            Ok(source) => installed.push(source),
                            Err(e) => tracing::warn!("{}", e.message),
                        }
                    }
                }
                *mirrors = Some(installed);
            }
        }
        let plugin: Arc<dyn BrokerPlugin> = Arc::new(QueuePlugin(Arc::downgrade(&me)));
        broker.register_plugin(plugin.clone());
        *lock(&self.plugin) = Some(plugin);
        if let Some(f) = &self.federation {
            f.start();
        }
        for b in &self.bridges {
            b.start();
        }
        st.task = Some(tokio::spawn(me.clone().run()));
    }

    /// `stop`: the plugin goes, the connection fails and is not made
    /// again.
    fn stop_inner(&self) {
        let (task, handle) = {
            let mut st = lock(&self.state);
            if !st.started {
                return;
            }
            st.started = false;
            st.connected = false;
            (st.task.take(), st.handle.take())
        };
        if let (Some(p), Some(b)) = (lock(&self.plugin).take(), self.broker.upgrade()) {
            b.unregister_plugin(&p);
        }
        if let Some(f) = &self.federation {
            f.stop();
        }
        for b in &self.bridges {
            b.stop();
        }
        if let Some(h) = handle {
            h.fail("Stopping Broker Connection");
        }
        if let Some(t) = task {
            // The connection's own task ends once it has closed.
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                t.abort();
            });
        }
    }

    /// `shutdown`: stopped, its federation and bridges shut down, and
    /// unregistered from management.
    fn shutdown(&self, broker: &Broker) {
        self.stop_inner();
        if let Some(f) = &self.federation {
            f.shutdown();
        }
        for b in &self.bridges {
            b.shutdown();
        }
        broker
            .broker_connection_registry()
            .unregister(&self.config.name);
    }

    fn is_started_now(&self) -> bool {
        lock(&self.state).started
    }

    fn host_port(&self, e: &Endpoint) -> String {
        format!("{}:{}", e.host, e.port)
    }

    /// `doConnect` and `retryConnection`, until stopped or out of
    /// attempts.
    async fn run(self: Arc<Self>) {
        let mut retry_counter: i32 = 0;
        let mut last_retry_counter: i32;
        loop {
            if !self.is_started_now() {
                return;
            }
            let endpoint = self.endpoints[retry_counter as usize % self.endpoints.len()].clone();
            let host_port = self.host_port(&endpoint);
            if let Ok(stream) = connect(&endpoint).await {
                last_retry_counter = retry_counter;
                retry_counter = 0;
                let ended = self.connected(stream, &endpoint, last_retry_counter).await;
                match ended {
                    Ended::Stopped => return,
                    // A link failed: the attempts made before count.
                    Ended::LinkFailed(_) => retry_counter = last_retry_counter,
                    Ended::Closed | Ended::Refused(_) => {}
                }
            } else {
                tracing::debug!("connecting {host_port} failed");
            }
            // retryConnection
            last_retry_counter = retry_counter;
            if !self.is_started_now() {
                return;
            }
            let attempts = self.config.reconnect_attempts;
            if attempts < 0 || retry_counter < attempts {
                retry_counter += 1;
                let next = &self.endpoints[retry_counter as usize % self.endpoints.len()];
                tracing::info!(
                    "AMQ111002: \n*******************************************************************************************************************************\nRetrying Server AMQP Connection {} on {} retry {} of {}\n*******************************************************************************************************************************\n",
                    self.config.name,
                    self.host_port(next),
                    retry_counter,
                    attempts
                );
                tokio::time::sleep(Duration::from_millis(
                    self.config.retry_interval.max(0) as u64
                ))
                .await;
            } else {
                tracing::warn!(
                    "AMQ111001: \n*******************************************************************************************************************************\nCould not re-establish AMQP Server Connection {} on {} after {} retries\n*******************************************************************************************************************************\n",
                    self.config.name,
                    host_port,
                    last_retry_counter
                );
                lock(&self.state).started = false;
                return;
            }
        }
    }

    /// A connection is up: SASL, Open and Begin go out, then a link for
    /// each matching queue. Returns how it ended.
    async fn connected(&self, stream: Connected, endpoint: &Endpoint, retries: i32) -> Ended {
        let Some(broker) = self.broker.upgrade() else {
            return Ended::Stopped;
        };
        let Connected {
            stream,
            peer,
            local,
            local_certificate,
        } = stream;
        let settings = Settings::from_params(&endpoint.params);
        let info = vec![
            ("connectionName", Value::string(self.config.name.clone())),
            ("nodeId", Value::string(broker.node_id())),
        ];
        let mut properties = Fields::default();
        properties.insert(
            BROKER_CONNECTION_INFO,
            Value::Map(
                java_hash_order(info)
                    .into_iter()
                    .map(|(k, v)| (Value::string(k), v))
                    .collect(),
            ),
        );
        // Bridges are restored once the peer's Open is in, knowing what it
        // offers (addRemoteOpenedListener).
        let handle_slot: Arc<Mutex<Option<ConnectionHandle>>> = Arc::default();
        let on_open: Option<connection::OnOpen> = (!self.bridges.is_empty()).then(|| {
            let bridges = self.bridges.clone();
            let slot = handle_slot.clone();
            let params = endpoint.params.clone();
            Box::new(move |offered: &[Symbol]| {
                let Some(handle) = lock(&slot).clone() else {
                    return;
                };
                let shared_subs = offered
                    .iter()
                    .any(|s| s.as_str() == crate::link::capability::SHARED_SUBS);
                for b in &bridges {
                    b.connection_restored(
                        handle.clone(),
                        Settings::from_params(&params),
                        shared_subs,
                    );
                }
            }) as connection::OnOpen
        });
        let outgoing = Outgoing {
            user: self.config.user.clone(),
            password: self.config.password.clone(),
            sasl_mechanisms: endpoint
                .params
                .contains_key("saslMechanisms")
                .then(|| settings.sasl_mechanisms.clone()),
            local_certificate,
            properties,
            desired_capabilities: (!self.bridges.is_empty())
                .then(|| vec![Symbol::from(crate::link::capability::SHARED_SUBS)]),
            on_open,
        };
        let (handle, task) = connection::connect_out(
            broker.clone(),
            stream,
            peer,
            local,
            settings,
            self.amqp.clone(),
            self.containers.clone(),
            outgoing,
        );
        {
            let mut st = lock(&self.state);
            if !st.started {
                handle.fail("Stopping Broker Connection");
            }
            st.connected = true;
            st.handle = Some(handle.clone());
            // Links are made again on the new connection.
            st.senders.clear();
            st.receivers.clear();
        }
        *lock(&handle_slot) = Some(handle.clone());
        let task = tokio::spawn(task);
        if let Some(f) = &self.federation {
            f.connection_restored(handle.clone(), Settings::from_params(&endpoint.params));
        }
        for q in broker.post_office().queues() {
            self.validate_matching(&broker, &q);
        }
        let mirrors = lock(&self.mirrors).clone().unwrap_or_default();
        for m in mirrors {
            self.connect_mirror(&handle, &broker, m);
        }
        tracing::info!(
            "AMQ111003: \n*******************************************************************************************************************************\nConnected on Server AMQP Connection {} on {} after {} retries\n*******************************************************************************************************************************\n",
            self.config.name,
            self.host_port(endpoint),
            retries
        );
        let ended = task.await.unwrap_or(Ended::Closed);
        if let Some(f) = &self.federation {
            f.connection_interrupted();
        }
        for b in &self.bridges {
            b.connection_interrupted();
        }
        let mut st = lock(&self.state);
        st.connected = false;
        st.handle = None;
        ended
    }

    /// `validateMatching`: the links the elements make for a queue.
    fn validate_matching(&self, broker: &Broker, queue: &Queue) {
        for e in &self.config.elements {
            let (m, sender, receiver, peer) = match e {
                AmqpConnectionElement::Sender(m) => (m, true, false, false),
                AmqpConnectionElement::Receiver(m) => (m, false, true, false),
                AmqpConnectionElement::Peer(m) => (m, true, true, true),
                _ => continue,
            };
            if !matches(broker, m, queue) {
                continue;
            }
            // createLink
            if sender {
                self.connect_sender(queue, peer);
            }
            if receiver {
                self.connect_receiver(queue, peer);
            }
        }
    }

    /// `connectSender`: a link sending the queue's messages to its
    /// address on the peer.
    fn connect_sender(&self, queue: &Queue, peer: bool) {
        let mut st = lock(&self.state);
        let Some(handle) = st.handle.clone() else {
            return;
        };
        if !st.senders.insert((queue.name().to_string(), queue.id())) {
            return;
        }
        drop(st);
        let address = queue.config().address.to_string();
        let attach = Attach {
            name: format!("{address}:{}", uuid::Uuid::new_v4()),
            handle: 0,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(Source {
                address: Some(address.clone()),
                ..Default::default()
            }),
            target: Some(TargetKind::Target(Target {
                address: Some(address),
                capabilities: peer.then(|| vec![Symbol::from(WAYPOINT)]),
                ..Default::default()
            })),
            initial_delivery_count: Some(0),
            ..Default::default()
        };
        handle.open(LocalLink {
            attach,
            kind: LocalLinkKind::Sender {
                queue: queue.name().to_string(),
            },
        });
    }

    /// `connectSender` for a `<mirror>`: a link sending the
    /// store-and-forward queue to the peer's address of the same name,
    /// naming this broker and desiring `amq.mirror`.
    fn connect_mirror(
        &self,
        handle: &ConnectionHandle,
        broker: &Broker,
        source: Arc<MirrorSource>,
    ) {
        let queue = source.snf().name().to_string();
        let mut properties = Fields::default();
        properties.insert(crate::mirror::BROKER_ID, Value::string(broker.node_id()));
        let attach = Attach {
            name: format!("{queue}:{}", uuid::Uuid::new_v4()),
            handle: 0,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(Source {
                address: Some(source.snf_address().to_owned()),
                ..Default::default()
            }),
            target: Some(TargetKind::Target(Target {
                address: Some(queue),
                ..Default::default()
            })),
            initial_delivery_count: Some(0),
            desired_capabilities: Some(vec![Symbol::from(crate::mirror::MIRROR_CAPABILITY)]),
            properties: Some(properties),
            ..Default::default()
        };
        handle.open(LocalLink {
            attach,
            kind: LocalLinkKind::Mirror(source),
        });
    }

    /// `connectReceiver`: a link taking the peer's messages for the
    /// queue's address, routed here with the queue's routing type.
    fn connect_receiver(&self, queue: &Queue, peer: bool) {
        let mut st = lock(&self.state);
        let Some(handle) = st.handle.clone() else {
            return;
        };
        if !st.receivers.insert((queue.name().to_string(), queue.id())) {
            return;
        }
        drop(st);
        let c = queue.config();
        let address = c.address.to_string();
        let attach = Attach {
            name: format!("{address}:{}", uuid::Uuid::new_v4()),
            handle: 0,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(Source {
                address: Some(address.clone()),
                capabilities: peer.then(|| vec![Symbol::from(WAYPOINT)]),
                ..Default::default()
            }),
            target: Some(TargetKind::Target(Target {
                address: Some(address.clone()),
                ..Default::default()
            })),
            ..Default::default()
        };
        handle.open(LocalLink {
            attach,
            kind: LocalLinkKind::Receiver {
                queue: queue.name().to_string(),
                address,
                routing: c.routing_type,
            },
        });
    }
}

impl saranga_server::broker_connection::BrokerConnection for AmqpBrokerConnection {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn protocol(&self) -> &'static str {
        "AMQP"
    }

    fn uri(&self) -> &str {
        &self.config.uri
    }

    fn user(&self) -> Option<&str> {
        self.config.user.as_deref()
    }

    fn retry_interval(&self) -> i64 {
        i64::from(self.config.retry_interval)
    }

    fn reconnect_attempts(&self) -> i32 {
        self.config.reconnect_attempts
    }

    fn is_started(&self) -> bool {
        self.is_started_now()
    }

    fn is_connected(&self) -> bool {
        lock(&self.state).connected
    }

    fn start(&self) {
        self.start_inner();
    }

    fn stop(&self) {
        self.stop_inner();
    }
}

/// `AMQPBrokerConnectionElement.match`, or the element's `queue-name`.
fn matches(broker: &Broker, m: &AmqpMatchElement, queue: &Queue) -> bool {
    if let Some(name) = &m.queue_name {
        return queue.name().to_string() == *name;
    }
    let Some(pattern) = &m.address_match else {
        return false;
    };
    broker
        .post_office()
        .wildcard()
        .matches(&queue.config().address, pattern)
}

/// `afterCreateQueue`: links for a queue made while the connection is
/// up.
struct QueuePlugin(Weak<AmqpBrokerConnection>);

impl BrokerPlugin for QueuePlugin {
    fn after_create_queue(&self, queue: &Queue) {
        let Some(c) = self.0.upgrade() else { return };
        let Some(broker) = c.broker.upgrade() else {
            return;
        };
        c.validate_matching(&broker, queue);
    }
}

/// A `HashMap<String, Object>`'s entries in Java's iteration order.
fn java_hash_order(entries: Vec<(&'static str, Value)>) -> Vec<(&'static str, Value)> {
    let hashes: Vec<i32> = entries
        .iter()
        .map(|(k, _)| {
            k.encode_utf16()
                .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(i32::from(c)))
        })
        .collect();
    let order = saranga_buffer::java_hash::hash_map_order(&hashes, None);
    let mut slots: Vec<Option<(&'static str, Value)>> = entries.into_iter().map(Some).collect();
    order.into_iter().filter_map(|i| slots[i].take()).collect()
}

/// `NettyConnector.createConnection`: the host's addresses, IPv4 first as
/// Java prefers them.
async fn connect_tcp(host: &str, port: u16) -> std::io::Result<TcpStream> {
    let mut addrs: Vec<_> = tokio::net::lookup_host((host, port)).await?.collect();
    addrs.sort_by_key(|a| !a.is_ipv4());
    let mut last = None;
    for addr in addrs {
        match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(s)) => return Ok(s),
            Ok(Err(e)) => last = Some(e),
            Err(_) => last = Some(std::io::Error::from(std::io::ErrorKind::TimedOut)),
        }
    }
    Err(last.unwrap_or_else(|| std::io::Error::other(format!("{host} resolves to no address"))))
}

trait Io: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Io for T {}

/// A connection made, with its addresses.
struct Connected {
    stream: Box<dyn Io>,
    peer: Option<std::net::SocketAddr>,
    local: Option<std::net::SocketAddr>,
    /// Whether the TLS handshake sent this end's certificate.
    local_certificate: bool,
}

/// A TCP connection to the endpoint, with the TLS handshake on it if the
/// URI has `sslEnabled`.
async fn connect(endpoint: &Endpoint) -> std::io::Result<Connected> {
    let stream = connect_tcp(&endpoint.host, endpoint.port).await?;
    let peer = stream.peer_addr().ok();
    let local = stream.local_addr().ok();
    let _ = stream.set_nodelay(true);
    let (stream, local_certificate): (Box<dyn Io>, bool) = match &endpoint.tls {
        Some(tls) => {
            let (s, sent) = tls.connect_reporting(&endpoint.host, stream).await?;
            (Box::new(s), sent)
        }
        None => (Box::new(stream), false),
    };
    Ok(Connected {
        stream,
        peer,
        local,
        local_certificate,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uris_as_uri_support_parses_them() {
        let e =
            parse_uri("tcp://h:5672?sslEnabled=true;trustStorePath=a%20b.jks&saslMechanisms=PLAIN")
                .unwrap();
        assert_eq!((e[0].host.as_str(), e[0].port), ("h", 5672));
        assert_eq!(e[0].params["sslEnabled"], "true");
        assert_eq!(e[0].params["trustStorePath"], "a b.jks");
        assert_eq!(e[0].params["saslMechanisms"], "PLAIN");
        assert!(e[0].tls.is_some());

        // The list's parameters apply to each server, its own first.
        let e = parse_uri("(tcp://a:1?x=1,tcp://b:2)?x=2;y=3").unwrap();
        assert_eq!(e[0].params["x"], "1");
        assert_eq!(e[1].params["x"], "2");
        assert_eq!(e[1].params["y"], "3");
        assert!(e[1].tls.is_none());
    }
}
