//! A router's pool (`AbstractPool` and its static, discovery and cluster
//! kinds): its targets, each watched by a `TargetMonitor` that connects
//! it, checks it every `check-period` and keeps whether it is ready.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use saranga_client::discovery::{DiscoveryConfig, DiscoveryGroup};
use saranga_core_protocol::transport::TransportConfiguration;

use super::policy::Policy;
use super::target::{Listener, Target};
use crate::cluster::{ClusterConnection, Member, TopologyListener};

/// `TargetMonitor`.
pub(crate) struct Monitor {
    target: Arc<Target>,
    ready: AtomicBool,
    check_period: Duration,
    probes: Arc<Mutex<Vec<Arc<Policy>>>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    runtime: tokio::runtime::Handle,
}

impl Listener for Monitor {
    /// `targetDisconnected`.
    fn disconnected(&self) {
        self.ready.store(false, Ordering::Release);
    }
}

impl Monitor {
    fn start(self: &Arc<Self>) {
        let weak: Weak<dyn Listener> = Arc::downgrade(self) as Weak<dyn Listener>;
        self.target.set_listener(Some(weak));
        let me = Arc::downgrade(self);
        let period = self.check_period;
        let task = self.runtime.spawn(async move {
            loop {
                let Some(m) = me.upgrade() else {
                    return;
                };
                m.run().await;
                drop(m);
                tokio::time::sleep(period).await;
            }
        });
        *self.task.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
    }

    fn stop(&self) {
        if let Some(t) = self.task.lock().unwrap_or_else(|e| e.into_inner()).take() {
            t.abort();
        }
        self.ready.store(false, Ordering::Release);
        self.target.set_listener(None);
        let target = self.target.clone();
        self.runtime.spawn(async move { target.disconnect().await });
    }

    /// `run`: connects the target if it is not, then checks it and the
    /// policy's probes.
    async fn run(&self) {
        let target = &self.target;
        if !target.is_connected() {
            tracing::debug!("Connecting to {target}");
            if let Err(e) = target.connect().await {
                tracing::warn!("Error monitoring {target}: {e}");
                target.disconnect().await;
                self.ready.store(false, Ordering::Release);
                return;
            }
        }
        let mut ready = target.check_readiness().await;
        if ready {
            let probes = self
                .probes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            for probe in probes {
                if !probe.probe(target).await {
                    tracing::info!("UPDATE_CONNECTION_COUNT_PROBE has failed on {target}");
                    ready = false;
                    break;
                }
            }
        }
        if ready {
            tracing::debug!("{target} is ready");
        } else {
            tracing::debug!("{target} is not ready");
        }
        self.ready.store(ready, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

/// Where a pool's targets come from.
pub enum Source {
    /// `StaticPool`: these connectors, added at start.
    Static(Vec<TransportConfiguration>),
    /// `DiscoveryPool`: the brokers a discovery group hears of; `None`
    /// for a group the configuration does not have.
    Discovery(Option<DiscoveryConfig>),
    /// `ClusterPool`: the other members of a cluster connection's
    /// topology; `None` for one the broker does not have.
    Cluster(Option<Arc<ClusterConnection>>),
}

pub struct Pool {
    source: Source,
    username: Option<String>,
    password: Option<String>,
    check_period: Duration,
    quorum_size: usize,
    quorum_timeout: Duration,
    probes: Arc<Mutex<Vec<Arc<Policy>>>>,
    /// `targetMonitors`, in the order the targets were added.
    monitors: Mutex<Vec<Arc<Monitor>>>,
    started: AtomicBool,
    runtime: tokio::runtime::Handle,
    /// The broker's node ID, which a discovery group ignores.
    local_node_id: Mutex<Option<String>>,
    discovery: Mutex<Option<(Arc<DiscoveryGroup>, tokio::task::JoinHandle<()>)>>,
    cluster_listener: Mutex<Option<u64>>,
    /// `ClusterPool.clusterMembers`.
    cluster_members: Mutex<HashSet<String>>,
    /// `DiscoveryGroupService.entries`: each node's connector.
    discovered: Mutex<Vec<(String, TransportConfiguration)>>,
}

impl Pool {
    pub fn new(
        source: Source,
        config: &saranga_config::configuration::PoolConfiguration,
    ) -> Arc<Self> {
        let millis = |v: i32| Duration::from_millis(v.max(0) as u64);
        Arc::new(Self {
            source,
            username: config.username.clone(),
            password: config.password.clone(),
            check_period: millis(config.check_period),
            quorum_size: config.quorum_size.max(0) as usize,
            quorum_timeout: millis(config.quorum_timeout),
            probes: Arc::default(),
            monitors: Mutex::default(),
            started: AtomicBool::new(false),
            runtime: tokio::runtime::Handle::current(),
            local_node_id: Mutex::default(),
            discovery: Mutex::default(),
            cluster_listener: Mutex::default(),
            cluster_members: Mutex::default(),
            discovered: Mutex::default(),
        })
    }

    fn monitors(&self) -> Vec<Arc<Monitor>> {
        self.monitors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn add_probe(&self, policy: Arc<Policy>) {
        self.probes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(policy);
    }

    /// `getAllTargets`.
    pub fn all_targets(&self) -> Vec<Arc<Target>> {
        self.monitors().iter().map(|m| m.target.clone()).collect()
    }

    fn ready_targets(&self) -> Vec<Arc<Target>> {
        self.monitors()
            .iter()
            .filter(|m| m.is_ready())
            .map(|m| m.target.clone())
            .collect()
    }

    /// `getTargets`: the ready targets, waiting up to `quorum-timeout` for
    /// `quorum-size` of them; none while there are fewer.
    pub async fn targets(&self) -> Vec<Arc<Target>> {
        let mut targets = self.ready_targets();
        if !self.quorum_timeout.is_zero() && targets.len() < self.quorum_size {
            let deadline = Instant::now() + self.quorum_timeout;
            while targets.len() < self.quorum_size && Instant::now() < deadline {
                targets = self.ready_targets();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        if targets.len() < self.quorum_size {
            Vec::new()
        } else {
            targets
        }
    }

    /// `getTarget`: the target of this node, ready or not.
    pub fn target(&self, node_id: &str) -> Option<Arc<Target>> {
        self.monitors()
            .iter()
            .find(|m| m.target.node_id().as_deref() == Some(node_id))
            .map(|m| m.target.clone())
    }

    /// `getReadyTarget`: the target of this node if it is ready, looked
    /// for until `quorum-timeout` while fewer than `quorum-size` are.
    pub async fn ready_target(&self, node_id: &str) -> Option<Arc<Target>> {
        let deadline =
            (!self.quorum_timeout.is_zero()).then(|| Instant::now() + self.quorum_timeout);
        loop {
            let mut ready = 0;
            for m in self.monitors() {
                if m.is_ready() {
                    ready += 1;
                    if m.target.node_id().as_deref() == Some(node_id) {
                        return Some(m.target.clone());
                    }
                }
            }
            match deadline {
                Some(d) if ready < self.quorum_size && Instant::now() < d => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                _ => return None,
            }
        }
    }

    /// `addTarget`: a target already there is not added again.
    pub fn add_target(&self, target: Arc<Target>) -> bool {
        let monitor = {
            let mut monitors = self.monitors.lock().unwrap_or_else(|e| e.into_inner());
            if monitors.iter().any(|m| Arc::ptr_eq(&m.target, &target)) {
                return false;
            }
            let monitor = Arc::new(Monitor {
                target,
                ready: AtomicBool::new(false),
                check_period: self.check_period,
                probes: self.probes.clone(),
                task: Mutex::new(None),
                runtime: self.runtime.clone(),
            });
            monitors.push(monitor.clone());
            monitor
        };
        if self.started.load(Ordering::Acquire) {
            monitor.start();
        }
        true
    }

    fn add_connector(&self, connector: TransportConfiguration, node_id: Option<String>) {
        self.add_target(Target::remote(
            connector,
            node_id,
            self.username.clone(),
            self.password.clone(),
        ));
    }

    /// `removeTarget`: its monitor stops, and the target disconnects.
    pub fn remove_target(&self, target: &Arc<Target>) -> bool {
        let monitor = {
            let mut monitors = self.monitors.lock().unwrap_or_else(|e| e.into_inner());
            let Some(i) = monitors.iter().position(|m| Arc::ptr_eq(&m.target, target)) else {
                return false;
            };
            monitors.remove(i)
        };
        monitor.stop();
        true
    }

    /// `start`: the targets there are start being watched, then the
    /// source's are added.
    pub fn start(self: &Arc<Self>, local_node_id: Option<String>) -> Result<(), String> {
        self.started.store(true, Ordering::Release);
        *self.local_node_id.lock().unwrap_or_else(|e| e.into_inner()) = local_node_id.clone();
        for m in self.monitors() {
            m.start();
        }
        match &self.source {
            Source::Static(connectors) => {
                for c in connectors {
                    self.add_connector(c.clone(), None);
                }
            }
            Source::Discovery(config) => {
                // DiscoveryGroupService.start: `config.getName()` of none.
                let config = config.as_ref().ok_or(
                    "java.lang.NullPointerException: Cannot invoke \"org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration.getName()\" because \"this.config\" is null",
                )?;
                let group = DiscoveryGroup::start(config, local_node_id.as_deref().unwrap_or(""))
                    .map_err(|e| e.to_string())?;
                let mut changes = group.subscribe();
                let me = Arc::downgrade(self);
                let watched = group.clone();
                let task = self.runtime.spawn(async move {
                    loop {
                        let Some(pool) = me.upgrade() else {
                            return;
                        };
                        pool.connectors_changed(&watched);
                        drop(pool);
                        if changes.changed().await.is_err() {
                            return;
                        }
                    }
                });
                *self.discovery.lock().unwrap_or_else(|e| e.into_inner()) = Some((group, task));
            }
            Source::Cluster(cluster) => {
                let cluster = cluster.as_ref().ok_or(
                    "java.lang.NullPointerException: Cannot invoke \"org.apache.activemq.artemis.core.server.cluster.ClusterConnection.addClusterTopologyListener(org.apache.activemq.artemis.api.core.client.ClusterTopologyListener)\" because \"this.clusterConnection\" is null",
                )?;
                let listener: Arc<dyn TopologyListener> = Arc::new(ClusterPoolListener {
                    pool: Arc::downgrade(self),
                    node_id: local_node_id.unwrap_or_default(),
                });
                let id = cluster.add_listener(listener);
                *self
                    .cluster_listener
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(id);
            }
        }
        Ok(())
    }

    /// `stop`: the source stops, then every target is removed.
    pub fn stop(&self) {
        match &self.source {
            Source::Cluster(Some(cluster)) => {
                if let Some(id) = self
                    .cluster_listener
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                {
                    cluster.remove_listener(id);
                }
                self.cluster_members
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clear();
            }
            Source::Discovery(_) => {
                if let Some((group, task)) = self
                    .discovery
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                {
                    task.abort();
                    group.stop();
                }
                self.discovered
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clear();
            }
            _ => {}
        }
        self.started.store(false, Ordering::Release);
        for m in self.monitors() {
            self.remove_target(&m.target);
        }
    }

    /// `DiscoveryGroupService.connectorsChanged` and `DiscoveryPool`'s
    /// handling of it: a new node is added, one whose connector changed
    /// replaced, one gone removed.
    fn connectors_changed(&self, group: &DiscoveryGroup) {
        let entries = group.entries();
        let local = self
            .local_node_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut known = self.discovered.lock().unwrap_or_else(|e| e.into_inner());
        let mut old: Vec<(String, TransportConfiguration)> = std::mem::take(&mut *known);
        for e in entries {
            if local.as_deref() == Some(e.node_id.as_str()) {
                continue;
            }
            match old.iter().position(|(n, _)| *n == e.node_id) {
                None => {
                    known.push((e.node_id.clone(), e.connector.clone()));
                    self.add_connector(e.connector, Some(e.node_id));
                }
                Some(i) => {
                    let (node_id, connector) = old.remove(i);
                    if connector != e.connector {
                        if let Some(t) = self.target(&node_id) {
                            self.remove_target(&t);
                        }
                        known.push((node_id.clone(), e.connector.clone()));
                        self.add_connector(e.connector, Some(node_id));
                    } else {
                        known.push((node_id, connector));
                    }
                }
            }
        }
        for (node_id, _) in old {
            if let Some(t) = self.target(&node_id) {
                self.remove_target(&t);
            }
        }
    }
}

/// `ClusterPool` as a `ClusterTopologyListener`: each other node's primary
/// is a target while it is up.
struct ClusterPoolListener {
    pool: Weak<Pool>,
    node_id: String,
}

impl TopologyListener for ClusterPoolListener {
    fn node_up(&self, member: &Member, _last: bool) {
        let Some(pool) = self.pool.upgrade() else {
            return;
        };
        if member.node_id == self.node_id {
            return;
        }
        let Some(primary) = member.primary.clone() else {
            return;
        };
        if pool
            .cluster_members
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(member.node_id.clone())
        {
            pool.add_connector(primary, Some(member.node_id.clone()));
        }
    }

    fn node_down(&self, _unique_event_id: i64, node_id: &str) {
        let Some(pool) = self.pool.upgrade() else {
            return;
        };
        if pool
            .cluster_members
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(node_id)
            && let Some(t) = pool.target(node_id)
        {
            pool.remove_target(&t);
        }
    }
}
