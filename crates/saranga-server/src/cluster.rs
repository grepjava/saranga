//! Cluster connections, as far as a primary and its backup need them: the
//! topology clients are given, the cluster channel a backup announces
//! itself on, and the connector a backup announces itself through.
//! Bridges between primaries (message load balancing) are not done yet.
//!
//! Ported from `ClusterConnectionImpl`, `Topology`, `TopologyMemberImpl`,
//! `ClusterController.ClusterControllerChannelHandler`, `BackupManager` and
//! `ConfigurationUtils.parseConnectorURI`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    /// `discovery-group-ref`: Saranga has no discovery yet, so a cluster
    /// connection using it reaches no other broker.
    pub discovery_group: Option<String>,
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
}

impl ClusterConfig {
    /// The first cluster connection of the configuration, if it has one.
    pub fn from_file(config: &saranga_config::BrokerConfig) -> Option<Self> {
        let cc = config.cluster_connections.first()?;
        let connector = |name: &str| {
            config
                .connectors
                .iter()
                .find(|c| c.name.as_deref() == Some(name))
                .map(|c| transport_configuration(name, c))
        };
        let static_connectors = cc
            .static_connectors
            .iter()
            .filter_map(|name| {
                let found = connector(name);
                if found.is_none() {
                    // `ClusterConnectionConfiguration.getTransportConfigurations`.
                    tracing::warn!(
                        "AMQ222024: Could not find connector {name} for cluster connection {}",
                        cc.name
                    );
                }
                found
            })
            .collect();
        let millis = |v: i64| Duration::from_millis(v.max(0) as u64);
        Some(Self {
            name: cc.name.clone(),
            connector: connector(&cc.connector_name)?,
            static_connectors,
            discovery_group: cc.discovery_group.clone(),
            retry_interval: millis(cc.retry_interval),
            call_timeout: millis(cc.call_timeout),
            check_period: millis(cc.check_period),
            connection_ttl: millis(cc.connection_ttl),
            notification_interval: millis(cc.notification_interval),
            notification_attempts: cc.notification_attempts.max(0) as u32,
            cluster_user: config.cluster_user.clone(),
            cluster_password: config.cluster_password.clone(),
        })
    }
}

/// `TCPTransportConfigurationSchema`: a connector's URI as the
/// `TransportConfiguration` announced for it. Every parameter is a string
/// (`BeanSupport.setData`), in the order of the Java `HashMap` holding them.
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
                (
                    JString::from(k.as_str()),
                    ParamValue::String(v.as_str().into()),
                )
            })
            .collect(),
    }
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
    stopping: AtomicBool,
    /// Another primary was announced: told once that no bridge is made.
    bridges_warned: AtomicBool,
}

impl ClusterConnection {
    pub fn new(config: ClusterConfig, node_id: &str) -> Arc<Self> {
        Arc::new(Self {
            config,
            node_id: node_id.to_owned(),
            topology: Mutex::default(),
            next_listener: AtomicU64::new(1),
            stopping: AtomicBool::new(false),
            bridges_warned: AtomicBool::new(false),
        })
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
                if this.stopping.load(Ordering::Acquire) {
                    return;
                }
                this.resend_node();
            }
        });
    }

    pub fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
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
        } else if input.primary.is_some() && !self.bridges_warned.swap(true, Ordering::AcqRel) {
            tracing::warn!(
                "Cluster connection {} learned of primary {}; Saranga does not create cluster bridges yet, so messages are not load-balanced to it",
                self.config.name,
                input.node_id
            );
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
    if config.static_connectors.is_empty() {
        // A discovery group (not supported yet) or no connectors: there
        // is no one to announce to.
        return;
    }
    let mut next = 0usize;
    loop {
        let target = &config.static_connectors[next % config.static_connectors.len()];
        next += 1;
        let attempt = async {
            let (host, port) = socket_address(target)
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
            let conn = Connection::connect((host.as_str(), port), options)
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
            retry_interval: Duration::from_millis(500),
            call_timeout: Duration::from_secs(30),
            check_period: Duration::from_secs(30),
            connection_ttl: Duration::from_secs(60),
            notification_interval: Duration::from_secs(1),
            notification_attempts: 0,
            cluster_user: "u".into(),
            cluster_password: "p".into(),
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
}
