//! Cluster connections, as far as a primary and its backup need them: the
//! topology clients are given, the cluster channel a backup announces
//! itself on, and the connector a backup announces itself through.
//! The bridges between primaries are in [`crate::cluster_bridge`].
//!
//! Ported from `ClusterConnectionImpl`, `Topology`, `TopologyMemberImpl`,
//! `ClusterController.ClusterControllerChannelHandler`, `BackupManager` and
//! `ConfigurationUtils.parseConnectorURI`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use saranga_buffer::JString;
use saranga_client::{Connection, ConnectionOptions};
use saranga_core_protocol::Flagged;
use saranga_core_protocol::packets::cluster::NodeAnnounceMessage;
use saranga_core_protocol::transport::{ParamValue, TransportConfiguration};

/// `NettyConnectorFactory`.
pub(crate) const NETTY_CONNECTOR_FACTORY: &str =
    "org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory";

/// The default cluster connection (`ClusterManager.getDefaultConnection`):
/// the first in `broker.xml`, with its connectors resolved.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub name: String,
    /// `connector-ref`: what this broker is announced as.
    pub connector: TransportConfiguration,
    /// `static-connectors`, in order.
    pub static_connectors: Vec<TransportConfiguration>,
    /// `discovery-group-ref`: the discovery group the other brokers are
    /// found through, in place of `static-connectors`.
    pub discovery_group: Option<String>,
    pub discovery: Option<saranga_client::discovery::DiscoveryConfig>,
    pub retry_interval: Duration,
    pub call_timeout: Duration,
    pub check_period: Duration,
    pub connection_ttl: Duration,
    /// `notification-interval` and `notification-attempts`: how often an
    /// active broker announces itself again (`PrimaryNotifier`).
    pub notification_interval: Duration,
    pub notification_attempts: u32,
    pub cluster_user: String,
    pub cluster_password: String,
    /// The rest of its `<cluster-connection>`: what its bridges are made
    /// with and how messages are balanced.
    pub settings: saranga_config::ClusterConnectionConfig,
}

impl ClusterConfig {
    /// `ClusterManager.deploy`: every cluster connection of the
    /// configuration that can be deployed, in its order; the first is the
    /// default one. One whose connector is missing (`AMQ222129`), whose
    /// name was taken (`AMQ222130`) or whose discovery group is missing
    /// (`AMQ222131`) is left out.
    pub fn all_from_file(config: &saranga_config::BrokerConfig) -> Vec<Self> {
        let mut out: Vec<Self> = Vec::new();
        for cc in &config.cluster_connections {
            // `getTransportConfiguration`.
            let Some(connector) = connector(config, &cc.connector_name) else {
                let name = if cc.connector_name.is_empty() {
                    "null"
                } else {
                    &cc.connector_name
                };
                tracing::warn!(
                    "AMQ222129: No connector with name {name}. The cluster connection will not be deployed."
                );
                continue;
            };
            if out.iter().any(|c| c.name == cc.name) {
                tracing::warn!(
                    "AMQ222130: Cluster Configuration  {} already exists. The cluster connection will not be deployed.",
                    cc.connector_name
                );
                continue;
            }
            if let Some(c) = Self::deploy(config, cc, connector) {
                out.push(c);
            }
        }
        out
    }

    /// One cluster connection, with its connector resolved.
    fn deploy(
        config: &saranga_config::BrokerConfig,
        cc: &saranga_config::ClusterConnectionConfig,
        connector: TransportConfiguration,
    ) -> Option<Self> {
        // `getTransportConfigurations`: a URI's members, else the named
        // connectors.
        let static_connectors = if !cc.static_members.is_empty() {
            cc.static_members
                .iter()
                .map(|m| transport_configuration(m.name.as_deref().unwrap_or_default(), m))
                .collect()
        } else {
            cc.static_connectors
                .iter()
                .filter_map(|name| {
                    let found = self::connector(config, name);
                    if found.is_none() {
                        tracing::warn!(
                            "AMQ222024: Could not find connector {name} for cluster connection {}",
                            cc.name
                        );
                    }
                    found
                })
                .collect()
        };
        let millis = |v: i64| Duration::from_millis(v.max(0) as u64);
        let discovery = match &cc.discovery_group {
            Some(name) => match crate::discovery::discovery_config(config, name) {
                Some(d) => Some(d),
                None => {
                    // `ClusterConnectionConfiguration.getDiscoveryGroupConfiguration`.
                    tracing::warn!(
                        "AMQ222131: No discovery group with name {name}. The cluster connection will not be deployed."
                    );
                    return None;
                }
            },
            None => None,
        };
        Some(Self {
            name: cc.name.clone(),
            connector,
            static_connectors,
            discovery_group: cc.discovery_group.clone(),
            discovery,
            retry_interval: millis(cc.retry_interval),
            call_timeout: millis(cc.call_timeout),
            check_period: millis(cc.check_period),
            connection_ttl: millis(cc.connection_ttl),
            notification_interval: millis(cc.notification_interval),
            notification_attempts: cc.notification_attempts.max(0) as u32,
            cluster_user: config.cluster_user.clone(),
            cluster_password: config.cluster_password.clone(),
            settings: cc.clone(),
        })
    }
}

/// The connector of that name, as announced.
fn connector(config: &saranga_config::BrokerConfig, name: &str) -> Option<TransportConfiguration> {
    config
        .connectors
        .iter()
        .find(|c| c.name.as_deref() == Some(name))
        .map(|c| transport_configuration(name, c))
}

/// `TCPTransportConfigurationSchema`: a connector's URI as the
/// `TransportConfiguration` announced for it. Every parameter is a string
/// (`BeanSupport.setData`) but the broker's mask-password, a Boolean, in
/// the order of the Java `HashMap` holding them.
pub fn transport_configuration(
    name: &str,
    uri: &saranga_config::AcceptorConfig,
) -> TransportConfiguration {
    let mut params: Vec<(String, String)> = vec![
        (
            "host".to_owned(),
            uri.host.clone().unwrap_or_else(|| "null".to_owned()),
        ),
        (
            "port".to_owned(),
            uri.port.map_or_else(|| "-1".to_owned(), |p| p.to_string()),
        ),
    ];
    for (key, value) in &uri.params {
        if key == "name" {
            continue;
        }
        let value = value.clone().unwrap_or_default();
        match params.iter_mut().find(|(k, _)| k == key) {
            Some(slot) => slot.1 = value,
            None => params.push((key.clone(), value)),
        }
    }
    let order = java_hash_order(params.iter().map(|(k, _)| k.as_str()).collect());
    TransportConfiguration {
        name: name.into(),
        factory_class_name: NETTY_CONNECTOR_FACTORY.into(),
        params: order
            .into_iter()
            .map(|i| {
                let (k, v) = &params[i];
                // The broker's mask-password, put in as a Boolean.
                let value = if k == "activemq.usemaskedpassword" {
                    ParamValue::Bool(v == "true")
                } else {
                    ParamValue::String(v.as_str().into())
                };
                (JString::from(k.as_str()), value)
            })
            .collect(),
    }
}

/// The cluster connections in the order `ClusterManager`'s `HashMap` of
/// them by name iterates.
pub(crate) fn start_order(clusters: &[Arc<ClusterConnection>]) -> Vec<&Arc<ClusterConnection>> {
    java_hash_order(clusters.iter().map(|c| c.config().name.as_str()).collect())
        .into_iter()
        .map(|i| &clusters[i])
        .collect()
}

/// The indexes of `keys` in the order a Java `HashMap` of them iterates:
/// by bucket, then by insertion within a bucket. The table starts at 16
/// and doubles past three quarters full.
fn java_hash_order(keys: Vec<&str>) -> Vec<usize> {
    let mut capacity = 16usize;
    while keys.len() as f32 > capacity as f32 * 0.75 {
        capacity *= 2;
    }
    let bucket = |s: &str| {
        let h = s
            .encode_utf16()
            .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(i32::from(c)));
        let h = h ^ ((h as u32) >> 16) as i32;
        (h as u32 as usize) & (capacity - 1)
    };
    let mut indexed: Vec<(usize, usize)> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (bucket(k), i))
        .collect();
    indexed.sort();
    indexed.into_iter().map(|(_, i)| i).collect()
}

/// The host and port a connector reaches.
pub(crate) fn socket_address(tc: &TransportConfiguration) -> Option<(String, u16)> {
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
    let port = param("port")?.parse().ok()?;
    Some((host, port))
}

/// The connector to dial for a `TransportConfiguration`, its TLS included;
/// `None` without a port.
pub(crate) fn dial(tc: &TransportConfiguration) -> Option<saranga_client::Connector> {
    let (host, port) = socket_address(tc)?;
    let tls = saranga_client::Connector::from_transport(tc).and_then(|c| c.tls);
    Some(saranga_client::Connector::new(host, port).with_tls(tls))
}

/// How a connector is shown in logs: `TransportConfiguration.toString`'s
/// gist.
fn describe(tc: &TransportConfiguration) -> String {
    match socket_address(tc) {
        Some((host, port)) => format!("{}?host={host}&port={port}", tc.name.to_string_lossy()),
        None => tc.name.to_string_lossy(),
    }
}

/// `TopologyMemberImpl`: a node, with the connectors of its primary and its
/// backup.
#[derive(Debug, Clone, PartialEq)]
pub struct Member {
    pub node_id: String,
    pub backup_group_name: Option<String>,
    pub scale_down_group_name: Option<String>,
    pub primary: Option<TransportConfiguration>,
    pub backup: Option<TransportConfiguration>,
    pub unique_event_id: i64,
}

/// `ClusterTopologyListener`.
pub trait TopologyListener: Send + Sync {
    fn node_up(&self, member: &Member, last: bool);
    fn node_down(&self, unique_event_id: i64, node_id: &str);
}

#[derive(Default)]
struct Topology {
    /// Keyed by node ID, in insertion order (a node updated moves to the
    /// end, as `remove` then `put` does).
    members: Vec<Member>,
    listeners: Vec<(u64, Arc<dyn TopologyListener>)>,
    /// `mapDelete`: when each node was last removed, so that older news of
    /// it is not taken.
    deleted: HashMap<String, i64>,
}

impl Topology {
    fn get(&self, node_id: &str) -> Option<&Member> {
        self.members.iter().find(|m| m.node_id == node_id)
    }

    fn put(&mut self, member: Member) {
        self.members.retain(|m| m.node_id != member.node_id);
        self.members.push(member);
    }

    fn listeners(&self) -> Vec<Arc<dyn TopologyListener>> {
        self.listeners.iter().map(|(_, l)| l.clone()).collect()
    }
}

/// A started cluster connection (`ClusterConnectionImpl`): its topology,
/// which every client subscribed to topology updates on an acceptor is
/// told about.
pub struct ClusterConnection {
    config: ClusterConfig,
    node_id: String,
    topology: Mutex<Topology>,
    next_listener: AtomicU64,
    stopping: tokio::sync::watch::Sender<bool>,
    /// The broker, once bridging started.
    broker: std::sync::OnceLock<Weak<crate::Broker>>,
    /// `records`: the flow record of each node bridged to.
    records: Mutex<HashMap<String, Arc<crate::cluster_bridge::Record>>>,
    /// The cluster's locator and its topology scanner, while running.
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    scanner: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The listener that bridges each primary the topology brings.
    bridging_listener: Mutex<Option<u64>>,
}

impl ClusterConnection {
    pub fn new(config: ClusterConfig, node_id: &str) -> Arc<Self> {
        Arc::new(Self {
            config,
            node_id: node_id.to_owned(),
            topology: Mutex::default(),
            next_listener: AtomicU64::new(1),
            stopping: tokio::sync::watch::channel(false).0,
            broker: std::sync::OnceLock::new(),
            records: Mutex::default(),
            tasks: Mutex::default(),
            scanner: Mutex::default(),
            bridging_listener: Mutex::default(),
        })
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub(crate) fn set_broker(&self, broker: &Arc<crate::Broker>) {
        let _ = self.broker.set(Arc::downgrade(broker));
    }

    pub(crate) fn broker(&self) -> Option<Arc<crate::Broker>> {
        self.broker.get()?.upgrade()
    }

    pub(crate) fn records_mutex(
        &self,
    ) -> &Mutex<HashMap<String, Arc<crate::cluster_bridge::Record>>> {
        &self.records
    }

    pub(crate) fn set_locator_task(&self, task: tokio::task::JoinHandle<()>) {
        self.tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(task);
    }

    /// A scanner started again replaces the one running.
    pub(crate) fn set_scanner_task(&self, task: tokio::task::JoinHandle<()>) {
        if let Some(old) = self
            .scanner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .replace(task)
        {
            old.abort();
        }
    }

    pub(crate) fn set_bridging_listener(&self, id: u64) {
        *self
            .bridging_listener
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(id);
    }

    pub(crate) fn is_stopping(&self) -> bool {
        *self.stopping.borrow()
    }

    pub(crate) fn stopping_rx(&self) -> tokio::sync::watch::Receiver<bool> {
        self.stopping.subscribe()
    }

    /// The topology's member for a node.
    pub fn member(&self, node_id: &str) -> Option<Member> {
        self.topology().get(node_id).cloned()
    }

    /// The node's member, or one made of the connector it was found at.
    pub(crate) fn member_or_known(
        &self,
        node_id: &str,
        connector: &TransportConfiguration,
    ) -> Option<Member> {
        Some(self.member(node_id).unwrap_or_else(|| Member {
            node_id: node_id.to_owned(),
            backup_group_name: None,
            scale_down_group_name: None,
            primary: Some(connector.clone()),
            backup: None,
            unique_event_id: 0,
        }))
    }

    /// `ServerLocatorImpl.notifyNodeUp` on the shared topology.
    pub(crate) fn update_from_locator(&self, unique_event_id: i64, member: Member) {
        self.update_member(unique_event_id, member);
    }

    /// `Topology.removeMember`, after `ClusterConnectionImpl.removeMember`:
    /// this node never leaves its own topology.
    pub fn remove_member(&self, unique_event_id: i64, node_id: &str, disconnect: bool) -> bool {
        if node_id == self.node_id {
            if disconnect {
                tracing::info!("AMQ224135: nodeID {node_id} is closing. Topology update ignored");
            } else {
                tracing::warn!(
                    "AMQ222295: There is a possible split brain on nodeID {node_id}. Topology update ignored"
                );
            }
            return false;
        }
        let listeners = {
            let mut t = self.topology();
            let Some(m) = t.get(node_id) else {
                return false;
            };
            if m.unique_event_id > unique_event_id {
                tracing::debug!(
                    "The removeMember was issued before the node {node_id} was started, ignoring call"
                );
                return false;
            }
            t.deleted.insert(node_id.to_owned(), unique_event_id);
            t.members.retain(|m| m.node_id != node_id);
            t.listeners()
        };
        for l in listeners {
            l.node_down(unique_event_id, node_id);
        }
        true
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// `getConnector`: what this broker is announced as.
    pub fn connector(&self) -> &TransportConfiguration {
        &self.config.connector
    }

    fn topology(&self) -> std::sync::MutexGuard<'_, Topology> {
        self.topology.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `activate`: this broker is the node's primary
    /// (`PrimaryNotifier.updateAsPrimary`), announced again
    /// `notification-attempts` times.
    pub fn activate(self: &Arc<Self>) {
        self.update_as_primary();
        let weak = Arc::downgrade(self);
        let (interval, attempts) = (
            self.config.notification_interval,
            self.config.notification_attempts,
        );
        tokio::spawn(async move {
            for _ in 0..attempts {
                tokio::time::sleep(interval).await;
                let Some(this) = weak.upgrade() else { return };
                if this.is_stopping() {
                    return;
                }
                this.resend_node();
            }
        });
    }

    /// `isStarted`: bridging, and not stopped.
    pub fn is_started(&self) -> bool {
        self.broker.get().is_some() && !self.is_stopping()
    }

    /// `ClusterConnectionControl.start`: a cluster connection stopped
    /// through management activates and bridges again. One that never
    /// started (a backup's) waits for its broker to be active.
    pub fn start(self: &Arc<Self>) {
        let Some(broker) = self.broker() else { return };
        if !self.stopping.send_replace(false) {
            return;
        }
        self.activate();
        self.start_bridging(&broker);
    }

    /// `ClusterConnectionControl.stop`: [`stop`](Self::stop), returning once
    /// the records are closed (`flushExecutor`).
    pub async fn stop_flushed(self: &Arc<Self>) {
        if let Some(t) = self.stop_records() {
            let _ = t.await;
        }
    }

    /// `stop`: no more announcements, no more bridges; the records close
    /// on a task of their own.
    pub fn stop(self: &Arc<Self>) {
        self.stop_records();
    }

    /// [`stop`](Self::stop), with the task closing the records.
    fn stop_records(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        if self.stopping.send_replace(true) {
            return None;
        }
        for t in self
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
        {
            t.abort();
        }
        if let Some(t) = self
            .scanner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            t.abort();
        }
        let listener = self
            .bridging_listener
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let id = listener?;
        self.remove_listener(id);
        let this = self.clone();
        Some(tokio::spawn(async move { this.stop_bridging().await }))
    }

    /// `ClusterConnectionImpl.toString`.
    pub fn describe(&self, broker: &crate::Broker) -> String {
        format!(
            "ClusterConnectionImpl@{}[nodeUUID={}, connector={}, address={}, server={}]",
            crate::audit::identity(("cluster-connection", &self.config.name)),
            self.node_id,
            crate::replication::quorum::transport_to_string(&self.config.connector),
            self.config.settings.address,
            crate::audit::server_string(broker)
        )
    }

    /// `Topology.describe`: each member, in the order of a `HashMap` of
    /// them, then their count.
    pub fn describe_topology(&self, broker: &crate::Broker) -> String {
        let members = self.members();
        let tc = |c: &Option<TransportConfiguration>| {
            c.as_ref().map_or_else(
                || "null".to_owned(),
                crate::replication::quorum::transport_to_string,
            )
        };
        let mut out = format!(
            "topology on Topology@{:x}[owner={}]:\n",
            crate::audit::identity(("topology", &self.config.name)),
            self.describe(broker)
        );
        let order = java_hash_order(members.iter().map(|m| m.node_id.as_str()).collect());
        for i in order {
            let m = &members[i];
            out.push_str(&format!(
                "\t{} => TopologyMember[id={}, connector=Pair[a={}, b={}], backupGroupName={}, scaleDownGroupName={}]\n",
                m.node_id,
                m.node_id,
                tc(&m.primary),
                tc(&m.backup),
                m.backup_group_name.as_deref().unwrap_or("null"),
                m.scale_down_group_name.as_deref().unwrap_or("null"),
            ));
        }
        // `nodes`: the primaries and backups.
        let nodes: usize = members
            .iter()
            .map(|m| usize::from(m.primary.is_some()) + usize::from(m.backup.is_some()))
            .sum();
        out.push_str(&format!("\tnodes={nodes}\tmembers={}", members.len()));
        if members.is_empty() {
            out.push_str("\tEmpty");
        }
        out
    }

    /// `Topology.updateAsPrimary`.
    fn update_as_primary(&self) {
        let member = Member {
            node_id: self.node_id.clone(),
            backup_group_name: None,
            scale_down_group_name: None,
            primary: Some(self.config.connector.clone()),
            backup: None,
            unique_event_id: crate::message::now_millis(),
        };
        let listeners = {
            let mut t = self.topology();
            t.put(member.clone());
            t.listeners()
        };
        send_member_up(&listeners, &member);
    }

    /// `Topology.resendNode`.
    fn resend_node(&self) {
        let (member, listeners) = {
            let mut t = self.topology();
            let Some(m) = t.members.iter_mut().find(|m| m.node_id == self.node_id) else {
                return;
            };
            m.unique_event_id = crate::message::now_millis();
            (m.clone(), t.listeners())
        };
        send_member_up(&listeners, &member);
    }

    /// `addClusterTopologyListener`: the listener is told of every member
    /// now (`sendTopology`, the last one marked), then of every change.
    /// The returned ID removes it.
    pub fn add_listener(&self, listener: Arc<dyn TopologyListener>) -> u64 {
        let id = self.next_listener.fetch_add(1, Ordering::Relaxed);
        let members = {
            let mut t = self.topology();
            t.listeners.push((id, listener.clone()));
            t.members.clone()
        };
        // `new HashMap<>(topology)`: node IDs in hash order.
        let order = java_hash_order(members.iter().map(|m| m.node_id.as_str()).collect());
        let count = order.len();
        for (n, i) in order.into_iter().enumerate() {
            listener.node_up(&members[i], n + 1 == count);
        }
        id
    }

    pub fn remove_listener(&self, id: u64) {
        self.topology().listeners.retain(|(i, _)| *i != id);
    }

    /// The members, for management.
    pub fn members(&self) -> Vec<Member> {
        self.topology().members.clone()
    }

    /// `nodeAnnounced`: a broker announced itself on the cluster channel.
    #[allow(clippy::too_many_arguments)] // NodeAnnounceMessage's fields
    pub fn node_announced(
        &self,
        unique_event_id: i64,
        node_id: &str,
        backup_group_name: Option<String>,
        scale_down_group_name: Option<String>,
        primary: Option<TransportConfiguration>,
        backup_connector: Option<TransportConfiguration>,
        backup: bool,
    ) {
        let member = Member {
            node_id: node_id.to_owned(),
            backup_group_name,
            scale_down_group_name,
            primary,
            backup: backup_connector,
            unique_event_id,
        };
        if backup {
            self.update_backup(member);
        } else {
            self.update_member(unique_event_id, member);
        }
    }

    /// `Topology.updateBackup`: the node's backup is now this connector.
    fn update_backup(&self, input: Member) {
        let (member, listeners) = {
            let mut t = self.topology();
            let current = t
                .get(&input.node_id)
                .cloned()
                .unwrap_or_else(|| input.clone());
            let member = Member {
                node_id: input.node_id.clone(),
                backup_group_name: current.backup_group_name,
                scale_down_group_name: current.scale_down_group_name,
                primary: current.primary,
                backup: input.backup,
                unique_event_id: crate::message::now_millis(),
            };
            t.put(member.clone());
            (member, t.listeners())
        };
        send_member_up(&listeners, &member);
    }

    /// `Topology.updateMember`, after the split-brain check of
    /// `ClusterConnectionImpl.updateMember`.
    fn update_member(&self, unique_event_id: i64, mut input: Member) {
        // News from before the node was last removed.
        if unique_event_id != 0
            && let Some(deleted) = self.topology().deleted.get(&input.node_id)
            && unique_event_id < *deleted
        {
            tracing::debug!(
                "Update uniqueEvent={unique_event_id}, nodeId={} being rejected as there was a delete done after that",
                input.node_id
            );
            return;
        }
        if input.node_id == self.node_id {
            match &input.primary {
                Some(p) if !same_params(p, &self.config.connector) => {
                    tracing::warn!(
                        "AMQ222294: There is a possible split brain on nodeID {}, coming from connectors {}. Topology update ignored.",
                        input.node_id,
                        describe(p)
                    );
                    return;
                }
                Some(_) => {}
                None => input.primary = Some(self.config.connector.clone()),
            }
        }
        let (member, listeners) = {
            let mut t = self.topology();
            let Some(current) = t.get(&input.node_id).cloned() else {
                input.unique_event_id = unique_event_id;
                t.put(input.clone());
                let listeners = t.listeners();
                drop(t);
                send_member_up(&listeners, &input);
                return;
            };
            if unique_event_id > current.unique_event_id
                || (current.primary.is_none() && input.primary.is_some())
            {
                let member = Member {
                    node_id: input.node_id.clone(),
                    backup_group_name: input.backup_group_name,
                    scale_down_group_name: input.scale_down_group_name,
                    primary: input.primary.or(current.primary),
                    backup: input.backup.or(current.backup),
                    unique_event_id: unique_event_id.max(current.unique_event_id),
                };
                t.put(member.clone());
                (member, t.listeners())
            } else {
                // Always add the backup.
                if current.backup.is_none()
                    && input.backup.is_some()
                    && let Some(m) = t.members.iter_mut().find(|m| m.node_id == input.node_id)
                {
                    m.backup = input.backup;
                }
                return;
            }
        };
        send_member_up(&listeners, &member);
    }
}

fn send_member_up(listeners: &[Arc<dyn TopologyListener>], member: &Member) {
    for l in listeners {
        l.node_up(member, false);
    }
}

/// `TransportConfiguration.isSameParams`.
fn same_params(a: &TransportConfiguration, b: &TransportConfiguration) -> bool {
    let sorted = |tc: &TransportConfiguration| {
        let mut p = tc.params.clone();
        p.sort_by(|x, y| x.0.cmp(&y.0));
        p
    };
    a.factory_class_name == b.factory_class_name && sorted(a) == sorted(b)
}

/// `TransportConfiguration.equals`: the same parameters and name.
pub fn same_connector(a: &TransportConfiguration, b: &TransportConfiguration) -> bool {
    a.name == b.name && same_params(a, b)
}

/// `BackupTopologyListener`: hears the node's primary announce itself as
/// the backup of this node, which is how a restarted primary asks the
/// active backup to fail back.
pub struct FailbackListener {
    node_id: String,
    connector: TransportConfiguration,
    heard: tokio::sync::watch::Sender<bool>,
}

impl FailbackListener {
    pub fn new(node_id: &str, connector: TransportConfiguration) -> Arc<Self> {
        Arc::new(Self {
            node_id: node_id.to_owned(),
            connector,
            heard: tokio::sync::watch::channel(false).0,
        })
    }

    /// `waitForBackup`: whether it was heard within `timeout`.
    pub async fn wait(&self, timeout: Duration) -> bool {
        let mut rx = self.heard.subscribe();
        matches!(
            tokio::time::timeout(timeout, rx.wait_for(|h| *h)).await,
            Ok(Ok(_))
        )
    }
}

impl TopologyListener for FailbackListener {
    fn node_up(&self, member: &Member, _last: bool) {
        if member
            .backup
            .as_ref()
            .is_some_and(|b| same_connector(b, &self.connector))
        {
            return;
        }
        if member.node_id == self.node_id && member.backup.is_some() {
            self.heard.send_replace(true);
        }
    }

    fn node_down(&self, _unique_event_id: i64, _node_id: &str) {}
}

/// The brokers a cluster connection reaches (its `ClusterController`
/// locator): its static connectors, or those its discovery group hears,
/// less this broker's own connector (`clusterTransportConfiguration`).
pub struct Peers {
    config: ClusterConfig,
    group: Option<Arc<saranga_client::discovery::DiscoveryGroup>>,
}

impl Peers {
    pub fn new(config: &ClusterConfig) -> Self {
        Self {
            config: config.clone(),
            group: None,
        }
    }

    /// The brokers to try now. With discovery, joins the group on first
    /// use, and waits up to `wait` for a broadcast while none was heard.
    pub async fn targets(&mut self, wait: Duration) -> Vec<TransportConfiguration> {
        let Some(discovery) = &self.config.discovery else {
            return self.config.static_connectors.clone();
        };
        if self.group.is_none() {
            // The locator's own random node ID: every broker is heard.
            let id = saranga_client::uuid::to_string(&saranga_client::uuid::time_based());
            match saranga_client::discovery::DiscoveryGroup::start(discovery, &id) {
                Ok(g) => self.group = Some(g),
                Err(e) => {
                    tracing::warn!(
                        "AMQ212065: discovery group {} getting exception when receiving broadcasting: {e}",
                        discovery.name
                    );
                    return Vec::new();
                }
            }
        }
        let group = self.group.as_ref().expect("started");
        let entries =
            |g: &saranga_client::discovery::DiscoveryGroup| -> Vec<TransportConfiguration> {
                g.entries()
                    .into_iter()
                    .map(|e| e.connector)
                    .filter(|c| !same_connector(c, &self.config.connector))
                    .collect()
            };
        let found = entries(group);
        if !found.is_empty() {
            return found;
        }
        group.wait_for_broadcast(Some(wait)).await;
        entries(group)
    }
}

impl Drop for Peers {
    fn drop(&mut self) {
        if let Some(g) = &self.group {
            g.stop();
        }
    }
}

/// `BackupManager.BackupConnector`: connects to the cluster through its
/// static connectors and announces this broker as the backup of
/// `node_id`, again whenever the connection is lost, until stopped.
pub struct BackupAnnouncer {
    stop: tokio::sync::watch::Sender<bool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl BackupAnnouncer {
    pub fn start(config: ClusterConfig, node_id: String) -> Arc<Self> {
        let (stop, rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(announce_loop(config, node_id, rx));
        Arc::new(Self {
            stop,
            task: Mutex::new(Some(task)),
        })
    }

    /// `BackupConnector.close`.
    pub async fn close(&self) {
        self.stop.send_replace(true);
        let task = self.task.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

/// Completes once the announcer is closed.
async fn stopped(stop: &mut tokio::sync::watch::Receiver<bool>) {
    let _ = stop.wait_for(|s| *s).await;
}

async fn announce_loop(
    config: ClusterConfig,
    node_id: String,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    if config.static_connectors.is_empty() && config.discovery.is_none() {
        // No one to announce to.
        return;
    }
    let mut peers = Peers::new(&config);
    let mut next = 0usize;
    loop {
        let targets = tokio::select! {
            t = peers.targets(config.retry_interval.max(Duration::from_millis(100))) => t,
            _ = stopped(&mut stop) => return,
        };
        if targets.is_empty() {
            continue;
        }
        let target = &targets[next % targets.len()];
        next += 1;
        let attempt = async {
            let connector = dial(target)
                .ok_or_else(|| format!("connector {} has no port", describe(target)))?;
            let options = ConnectionOptions {
                client_failure_check_period: Some(config.check_period),
                connection_ttl: Some(config.connection_ttl),
                call_timeout: config.call_timeout,
                user: Some(config.cluster_user.clone()),
                password: Some(config.cluster_password.clone()),
                cluster_connection: true,
                ..ConnectionOptions::default()
            };
            let conn = Connection::connect_connector(connector, options)
                .await
                .map_err(|e| e.to_string())?;
            // `ClusterControl.authorize`.
            if !conn
                .cluster_connect(&config.cluster_user, &config.cluster_password)
                .await
                .map_err(|e| e.to_string())?
            {
                conn.close().await;
                return Err(format!(
                    "AMQ229099: Unable to validate user from {}. Username: {}; SSL certificate subject DN: unavailable",
                    describe(target),
                    config.cluster_user
                ));
            }
            conn.node_announce(NodeAnnounceMessage {
                node_id: node_id.as_str().into(),
                backup_group_name: None,
                backup: true,
                current_event_id: crate::message::now_millis(),
                connector: Flagged(Some(config.connector.clone())),
                backup_connector: Flagged(None),
                scale_down_group_name: None,
            })
            .map_err(|e| e.to_string())?;
            tracing::info!("AMQ221031: backup announced");
            Ok::<_, String>(conn)
        };
        let result = tokio::select! {
            r = attempt => r,
            _ = stopped(&mut stop) => return,
        };
        match result {
            Ok(conn) => {
                tokio::select! {
                    _ = conn.failed() => {}
                    _ = stopped(&mut stop) => {
                        conn.close().await;
                        return;
                    }
                }
            }
            Err(e) => tracing::debug!("announcing the backup to {}: {e}", describe(target)),
        }
        tokio::select! {
            _ = tokio::time::sleep(config.retry_interval) => {}
            _ = stopped(&mut stop) => return,
        }
    }
}

/// The listeners a connection added, removed when it closes.
pub(crate) struct Subscription {
    cluster: Weak<ClusterConnection>,
    id: u64,
}

impl Subscription {
    pub(crate) fn new(
        cluster: &Arc<ClusterConnection>,
        listener: Arc<dyn TopologyListener>,
    ) -> Self {
        let id = cluster.add_listener(listener);
        Self {
            cluster: Arc::downgrade(cluster),
            id,
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(c) = self.cluster.upgrade() {
            c.remove_listener(self.id);
        }
    }
}

/// `ClusterConnectionImpl.getTopology().describe()`-like listing for tests
/// and management: node ID to (primary, backup) as `host:port`.
pub fn describe_members(members: &[Member]) -> HashMap<String, (Option<String>, Option<String>)> {
    members
        .iter()
        .map(|m| {
            let addr = |tc: &Option<TransportConfiguration>| {
                tc.as_ref()
                    .and_then(socket_address)
                    .map(|(h, p)| format!("{h}:{p}"))
            };
            (m.node_id.clone(), (addr(&m.primary), addr(&m.backup)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connector(name: &str, port: u16) -> TransportConfiguration {
        let uri =
            saranga_config::AcceptorConfig::parse(Some(name), &format!("tcp://localhost:{port}"))
                .unwrap();
        transport_configuration(name, &uri)
    }

    fn config(port: u16) -> ClusterConfig {
        ClusterConfig {
            name: "my-cluster".into(),
            connector: connector("artemis", port),
            static_connectors: Vec::new(),
            discovery_group: None,
            discovery: None,
            retry_interval: Duration::from_millis(500),
            call_timeout: Duration::from_secs(30),
            check_period: Duration::from_secs(30),
            connection_ttl: Duration::from_secs(60),
            notification_interval: Duration::from_secs(1),
            notification_attempts: 0,
            cluster_user: "u".into(),
            cluster_password: "p".into(),
            settings: saranga_config::ClusterConnectionConfig::new("my-cluster", "artemis"),
        }
    }

    #[derive(Default)]
    struct Recorder(Mutex<Vec<(Member, bool)>>);

    impl TopologyListener for Recorder {
        fn node_up(&self, member: &Member, last: bool) {
            self.0.lock().unwrap().push((member.clone(), last));
        }
        fn node_down(&self, _: i64, _: &str) {}
    }

    #[test]
    fn connector_params_are_strings_in_hash_order() {
        let tc = connector("artemis", 61616);
        // "port" hashes before "host" in a 16-slot table.
        assert_eq!(
            tc.params,
            vec![
                ("port".into(), ParamValue::String("61616".into())),
                ("host".into(), ParamValue::String("localhost".into())),
            ]
        );
        assert_eq!(socket_address(&tc), Some(("localhost".into(), 61616)));
    }

    #[tokio::test]
    async fn a_backup_announcement_pairs_with_the_primary() {
        let cc = ClusterConnection::new(config(61616), "node-a");
        cc.activate();
        let recorder = Arc::new(Recorder::default());
        let _sub = Subscription::new(&cc, recorder.clone());
        {
            let seen = recorder.0.lock().unwrap();
            assert_eq!(seen.len(), 1);
            assert!(seen[0].1, "the initial topology ends with last");
            assert_eq!(seen[0].0.backup, None);
        }
        cc.node_announced(
            1,
            "node-a",
            None,
            None,
            None,
            Some(connector("artemis", 61716)),
            true,
        );
        let seen = recorder.0.lock().unwrap();
        let (m, last) = seen.last().unwrap();
        assert!(!last);
        assert_eq!(m.primary, Some(connector("artemis", 61616)));
        assert_eq!(m.backup, Some(connector("artemis", 61716)));
    }

    #[tokio::test]
    async fn failback_is_heard_from_the_restarted_primary_only() {
        let cc = ClusterConnection::new(config(61716), "node-a");
        cc.activate();
        let listener = FailbackListener::new("node-a", connector("artemis", 61716));
        let _sub = Subscription::new(&cc, listener.clone());
        assert!(!listener.wait(Duration::from_millis(10)).await);
        // Its own connector as the backup is not a failback request.
        cc.node_announced(
            1,
            "node-a",
            None,
            None,
            None,
            Some(connector("artemis", 61716)),
            true,
        );
        assert!(!listener.wait(Duration::from_millis(10)).await);
        cc.node_announced(
            2,
            "node-a",
            None,
            None,
            None,
            Some(connector("artemis", 61616)),
            true,
        );
        assert!(listener.wait(Duration::from_millis(10)).await);
    }

    #[tokio::test]
    async fn a_split_brain_announcement_is_ignored() {
        let cc = ClusterConnection::new(config(61616), "node-a");
        cc.activate();
        cc.node_announced(
            i64::MAX,
            "node-a",
            None,
            None,
            Some(connector("artemis", 9999)),
            None,
            false,
        );
        assert_eq!(cc.members()[0].primary, Some(connector("artemis", 61616)));
    }

    /// `ClusterManager.deploy`: every cluster connection, but not one with
    /// no connector, a taken name or a missing discovery group; a URI's
    /// members in place of named connectors.
    #[test]
    fn deploys_every_cluster_connection() {
        let xml = r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
            <connectors>
              <connector name="me">tcp://localhost:61616</connector>
              <connector name="other">tcp://localhost:61617</connector>
            </connectors>
            <cluster-connections>
              <cluster-connection name="orders">
                <address>orders</address><connector-ref>me</connector-ref>
                <static-connectors><connector-ref>other</connector-ref></static-connectors>
              </cluster-connection>
              <cluster-connection name="news">
                <address>news</address><connector-ref>me</connector-ref>
                <message-load-balancing>STRICT</message-load-balancing>
                <static-connectors><connector-ref>other</connector-ref></static-connectors>
              </cluster-connection>
              <cluster-connection name="orders">
                <connector-ref>me</connector-ref>
              </cluster-connection>
              <cluster-connection name="found">
                <connector-ref>me</connector-ref>
                <discovery-group-ref discovery-group-name="nowhere"/>
              </cluster-connection>
              <cluster-connection-uri name="by-uri"
                address="static://(tcp://localhost:61618,tcp://localhost:61619)?connectorName=me&amp;address=events"/>
              <cluster-connection-uri name="no-connector" address="static://(tcp://localhost:61618)"/>
            </cluster-connections>
        </core></configuration>"#;
        // The taken name and the missing group are not valid against the
        // schema, which a deployment refuses: read as a reload reads it.
        let file = saranga_config::BrokerConfig::parse_validated(
            xml,
            |_| None,
            saranga_config::Validation::Reload,
        )
        .unwrap();
        let all = ClusterConfig::all_from_file(&file);
        let names: Vec<&str> = all.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["orders", "news", "by-uri"]);
        assert_eq!(all[1].settings.address, "news");
        assert_eq!(
            all[1].settings.message_load_balancing,
            saranga_config::MessageLoadBalancing::Strict
        );
        let uri = &all[2];
        assert_eq!(uri.connector, connector("me", 61616));
        assert_eq!(uri.settings.address, "events");
        let ports: Vec<_> = uri
            .static_connectors
            .iter()
            .filter_map(socket_address)
            .collect();
        assert_eq!(
            ports,
            [
                ("localhost".to_owned(), 61618),
                ("localhost".to_owned(), 61619)
            ]
        );
    }
}
