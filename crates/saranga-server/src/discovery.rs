//! Broadcast groups: an active broker sends its connectors to a UDP
//! multicast group every `broadcast-period`, for discovery groups to hear.
//! And the discovery groups a cluster connection finds the others through.
//!
//! Ported from `BroadcastGroupImpl`, `ClusterManager.deployBroadcastGroup`
//! and `ConfigurationUtils.validateStartupConfiguration`; the datagram and
//! the discovery group itself are saranga-client's
//! ([`saranga_client::discovery`]).

use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use saranga_client::discovery::{
    Broadcast, Broadcaster, DiscoveryConfig, EndpointConfig, discovery_enabled,
};
use saranga_config as cfg;
use saranga_core_protocol::transport::TransportConfiguration;

use crate::notifications::{self, NotificationType};
use crate::postoffice::PostOffice;

/// `validateStartupConfiguration`: with discovery disabled, a broker with
/// broadcast or discovery groups does not start (`AMQ229262`,
/// `AMQ229263`).
pub fn validate_startup(config: &cfg::BrokerConfig) -> Result<(), String> {
    if discovery_enabled() {
        return Ok(());
    }
    if !config.broadcast_groups.is_empty() {
        return Err("AMQ229262: Broadcast groups are disabled by default. See the Server Discovery section in the user manual for security requirements and enablement.".to_owned());
    }
    if !config.discovery_groups.is_empty() {
        return Err("AMQ229263: Discovery groups are disabled by default. See the Server Discovery section in the user manual for security requirements and enablement.".to_owned());
    }
    Ok(())
}

/// A group's UDP endpoint. An unset group port is Java's -1, which no
/// socket takes.
fn endpoint(e: &cfg::UdpEndpointConfig) -> EndpointConfig {
    EndpointConfig {
        group_address: e.group_address.clone().unwrap_or_default(),
        group_port: u16::try_from(e.group_port).unwrap_or(0),
        local_bind_address: e.local_bind_address.clone(),
        local_bind_port: e.local_bind_port,
    }
}

/// The discovery group `name` of the configuration, as a cluster connection
/// or bridge uses it.
pub fn discovery_config(config: &cfg::BrokerConfig, name: &str) -> Option<DiscoveryConfig> {
    let g = config.discovery_groups.iter().find(|g| g.name == name)?;
    let millis = |v: i64| Duration::from_millis(v.max(0) as u64);
    Some(DiscoveryConfig {
        name: g.name.clone(),
        endpoint: endpoint(&g.endpoint),
        refresh_timeout: millis(g.refresh_timeout),
        initial_wait_timeout: millis(g.initial_wait_timeout),
    })
}

/// A broadcast group of the configuration, its connectors resolved.
#[derive(Debug, Clone)]
pub struct BroadcastGroupConfig {
    pub name: String,
    pub broadcast_period: Duration,
    /// `connector-ref`s, as `getConnectorPairs` lists them.
    pub connector_names: Vec<String>,
    pub connectors: Vec<TransportConfiguration>,
    pub endpoint: cfg::UdpEndpointConfig,
}

/// `ClusterManager.deploy`'s broadcast groups: one whose name was used
/// before is skipped (`AMQ222132`), as is one naming a connector that does
/// not exist or none at all (`AMQ222133`).
pub fn broadcast_groups(config: &cfg::BrokerConfig) -> Vec<BroadcastGroupConfig> {
    let mut out: Vec<BroadcastGroupConfig> = Vec::new();
    'groups: for g in &config.broadcast_groups {
        if out.iter().any(|o| o.name == g.name) {
            tracing::warn!(
                "AMQ222132: There is already a broadcast-group with name {} deployed. This one will not be deployed.",
                g.name
            );
            continue;
        }
        let mut connectors = Vec::new();
        for name in &g.connectors {
            match config
                .connectors
                .iter()
                .find(|c| c.name.as_deref() == Some(name.as_str()))
            {
                Some(c) => connectors.push(crate::cluster::transport_configuration(name, c)),
                None => {
                    tracing::warn!(
                        "AMQ222133: There is no connector deployed with name {name}. The broadcast group with name {} will not be deployed.",
                        g.name
                    );
                    continue 'groups;
                }
            }
        }
        if connectors.is_empty() {
            tracing::warn!(
                "AMQ222133: There is no connector deployed with name {:?}. The broadcast group with name {} will not be deployed.",
                g.connectors,
                g.name
            );
            continue;
        }
        out.push(BroadcastGroupConfig {
            name: g.name.clone(),
            broadcast_period: Duration::from_millis(g.broadcast_period.max(1) as u64),
            connector_names: g.connectors.clone(),
            connectors,
            endpoint: g.endpoint.clone(),
        });
    }
    out
}

/// `BroadcastGroupImpl`: once started, broadcasts the node ID, its own
/// unique ID and the connectors every period.
pub struct BroadcastGroup {
    config: BroadcastGroupConfig,
    node_id: String,
    /// `uniqueID`: tells a restarted broker from another with its node ID.
    unique_id: String,
    post_office: Weak<PostOffice>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl BroadcastGroup {
    pub fn new(
        config: BroadcastGroupConfig,
        node_id: &str,
        post_office: &Arc<PostOffice>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            node_id: node_id.to_owned(),
            unique_id: saranga_client::uuid::to_string(&saranga_client::uuid::time_based()),
            post_office: Arc::downgrade(post_office),
            task: Mutex::new(None),
        })
    }

    pub fn config(&self) -> &BroadcastGroupConfig {
        &self.config
    }

    fn task(&self) -> std::sync::MutexGuard<'_, Option<tokio::task::JoinHandle<()>>> {
        self.task.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn is_started(&self) -> bool {
        self.task().is_some()
    }

    fn notify(&self, kind: NotificationType) {
        if let Some(po) = self.post_office.upgrade() {
            let name = self.config.name.clone();
            po.notify(kind, move |_| {
                vec![("name", notifications::string(Some(&name)))]
            });
        }
    }

    /// `start`: opens the socket (`openBroadcaster`), tells
    /// `BROADCAST_GROUP_STARTED` and broadcasts at once, then every period.
    pub fn start(&self) -> std::io::Result<()> {
        let mut task = self.task();
        if task.is_some() {
            return Ok(());
        }
        let broadcaster = Broadcaster::open(&endpoint(&self.config.endpoint))?;
        let data = Broadcast {
            node_id: self.node_id.clone(),
            unique_id: self.unique_id.clone(),
            connectors: self.config.connectors.clone(),
        }
        .encode();
        let period = self.config.broadcast_period;
        *task = Some(tokio::spawn(async move {
            let mut logged = false;
            loop {
                match broadcaster.broadcast(&data) {
                    Ok(()) => logged = false,
                    Err(e) if !logged => {
                        tracing::error!("AMQ224033: Failed to broadcast connector configs: {e}");
                        logged = true;
                    }
                    Err(e) => tracing::debug!("Failed to broadcast connector configs again: {e}"),
                }
                // scheduleWithFixedDelay.
                tokio::time::sleep(period).await;
            }
        }));
        drop(task);
        self.notify(NotificationType::BroadcastGroupStarted);
        Ok(())
    }

    /// `stop`: no more broadcasts, and `BROADCAST_GROUP_STOPPED`.
    pub fn stop(&self) {
        let Some(task) = self.task().take() else {
            return;
        };
        task.abort();
        self.notify(NotificationType::BroadcastGroupStopped);
    }
}

impl Drop for BroadcastGroup {
    fn drop(&mut self) {
        if let Some(task) = self.task().take() {
            task.abort();
        }
    }
}
