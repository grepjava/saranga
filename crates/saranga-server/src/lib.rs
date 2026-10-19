//! The Saranga broker: addresses, queues and routing, and the Core
//! protocol server that drives them.
//!
//! With persistence enabled, durable state goes to the Artemis journals
//! ([`storage`]) and is recovered on start ([`recovery`]). The structure
//! follows ADR 0001: one task per connection and per session, one actor
//! per queue.

pub mod acceptors;
pub mod audit;
pub mod bridge;
pub mod broker_connection;
pub mod cluster;
pub mod cluster_bridge;
pub mod config;
pub mod core_protocol;
pub mod critical;
pub mod discovery;
pub mod disk;
pub mod duplicate;
pub mod error;
pub mod federation;
pub mod grouping;
pub mod jvm;
pub mod large;
pub mod localtime;
pub mod management;
pub mod message;
pub mod mirror;
pub mod node;
pub mod notifications;
pub mod paging;
pub mod plugin;
pub mod postoffice;
pub mod protocol;
pub mod queue;
pub mod recovery;
pub mod registry;
pub mod reload;
pub mod remote_binding;
pub mod replay;
pub mod replication;
pub mod session;
pub mod settings;
mod slow_consumers;
pub mod storage;
pub mod transaction;
pub mod users;
pub mod wildcard;

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;

pub use error::{BrokerError, Result};
use postoffice::{FilterFactory, PostOffice, QueueSpec, RoutingTypes};
use settings::{SettingsRepository, UniformSettings};
use wildcard::WildcardConfig;

/// Address and queue names.
pub type Name = Arc<str>;

/// `RoutingType`. The discriminants are the wire byte values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RoutingType {
    Multicast = 0,
    Anycast = 1,
}

impl RoutingType {
    pub fn from_byte(b: i8) -> Option<Self> {
        match b {
            0 => Some(Self::Multicast),
            1 => Some(Self::Anycast),
            _ => None,
        }
    }

    pub fn to_byte(self) -> i8 {
        self as i8
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Multicast => "MULTICAST",
            Self::Anycast => "ANYCAST",
        }
    }
}

/// Message, queue, address and transaction IDs
/// (`StorageManager.generateID`), from the journal's batching generator when
/// the broker persists, from a counter otherwise. It also carries the
/// storage to everything that holds it.
pub struct IdGenerator {
    counter: AtomicI64,
    storage: Option<Arc<storage::Storage>>,
}

impl std::fmt::Debug for IdGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdGenerator")
            .field("persistent", &self.storage.is_some())
            .finish()
    }
}

impl IdGenerator {
    pub fn new(first: i64) -> Self {
        Self {
            counter: AtomicI64::new(first),
            storage: None,
        }
    }

    pub fn persistent(storage: Arc<storage::Storage>) -> Self {
        Self {
            counter: AtomicI64::new(0),
            storage: Some(storage),
        }
    }

    pub fn next(&self) -> i64 {
        match &self.storage {
            Some(s) => s.generate_id(),
            None => self.counter.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// The journals, when the broker persists (`persistence-enabled`).
    pub fn storage(&self) -> Option<&Arc<storage::Storage>> {
        self.storage.as_ref()
    }
}

/// A queue to create at startup (`<addresses>` in `broker.xml`).
#[derive(Debug, Clone)]
pub struct AddressDeployment {
    pub name: String,
    pub routing_types: RoutingTypes,
    pub queues: Vec<QueueSpec>,
}

/// How an acceptor treats its connections.
#[derive(Clone)]
pub struct Acceptor {
    /// The acceptor's `name`.
    pub name: String,
    /// `sslEnabled`: the handshake to run first.
    pub tls: Option<Arc<saranga_transport::TlsAcceptor>>,
    /// `securityDomain`: the JAAS domain its clients log in through.
    pub security_domain: Option<String>,
    /// The acceptor's `host` and `port`, which the topology given to
    /// clients before they log in names.
    pub host: String,
    pub port: u16,
    /// `coreConnectionSecurityEnabled`: with security on, frames are
    /// limited and the topology withheld until a connection logs in.
    pub core_connection_security: bool,
    /// `coreInitialMaxFrameSize`: the largest frame before login.
    pub core_initial_max_frame_size: usize,
    /// `coreMaxFrameSize`: the largest frame after login.
    pub core_max_frame_size: usize,
    /// `anycastPrefix` and `multicastPrefix`: a name a client gives with
    /// one of these prefixes means the name without it, with that routing
    /// type (`CoreProtocolManager.getPrefixes`).
    pub prefixes: Vec<(String, RoutingType)>,
    /// `protocols`: the protocols it serves; `None` for every protocol the
    /// broker has.
    pub protocols: Option<Vec<String>>,
    /// `handshake-timeout`: how long a client has to send enough bytes to
    /// pick its protocol; `None` for no limit.
    pub handshake_timeout: Option<std::time::Duration>,
    /// Every parameter of the acceptor's URI, for the protocols' own
    /// settings.
    pub params: HashMap<String, String>,
}

impl Acceptor {
    /// `CoreProtocolManager.CORE_INITIAL_MAX_FRAME_SIZE`.
    pub const CORE_INITIAL_MAX_FRAME_SIZE: usize = 4 * 1024;
    /// `CoreProtocolManager.CORE_MAX_FRAME_SIZE`.
    pub const CORE_MAX_FRAME_SIZE: usize = 128 * 1024;
}

impl Default for Acceptor {
    fn default() -> Self {
        Self {
            name: String::new(),
            tls: None,
            security_domain: None,
            host: "localhost".to_owned(),
            port: 61616,
            core_connection_security: true,
            core_initial_max_frame_size: Self::CORE_INITIAL_MAX_FRAME_SIZE,
            core_max_frame_size: Self::CORE_MAX_FRAME_SIZE,
            prefixes: Vec::new(),
            protocols: None,
            handshake_timeout: Some(protocol::DEFAULT_HANDSHAKE_TIMEOUT),
            params: HashMap::new(),
        }
    }
}

pub struct BrokerConfig {
    pub settings: Arc<dyn SettingsRepository>,
    pub filters: Arc<dyn FilterFactory>,
    pub wildcard: WildcardConfig,
    /// `connection-ttl-override`; -1 lets clients choose.
    pub connection_ttl_override: i64,
    pub addresses: Vec<AddressDeployment>,
    /// The deprecated `<queues>` of `<core>`, deployed after the addresses,
    /// each on its own address.
    pub legacy_queues: Vec<QueueSpec>,
    pub diverts: Vec<postoffice::DivertDeployment>,
    /// Divert transformers by the class name `broker.xml` refers to them by.
    pub transformers: HashMap<String, postoffice::TransformerFactory>,
    /// The journals; `None` keeps everything in memory
    /// (`persistence-enabled` false).
    pub storage: Option<storage::StorageConfig>,
    /// `message-expiry-scan-period` in milliseconds; `-1` for none.
    pub message_expiry_scan_period: i64,
    /// `transaction-timeout` in milliseconds.
    pub transaction_timeout: i64,
    /// `transaction-timeout-scan-period` in milliseconds.
    pub transaction_timeout_scan_period: i64,
    /// `address-queue-scan-period` in milliseconds: how often unused
    /// auto-created queues and addresses are looked for; -1 for never.
    pub address_queue_scan_period: i64,
    /// `id-cache-size`.
    pub id_cache_size: i32,
    /// `persist-id-cache`.
    pub persist_id_cache: bool,
    /// `global-max-size`: the memory all addresses' messages may take
    /// before addresses page (or block, fail, drop); -1 for no limit.
    pub global_max_size: i64,
    /// `global-max-messages`; -1 for no limit.
    pub global_max_messages: i64,
    /// Authentication and authorization; `None` lets everyone do anything.
    pub security: Option<Arc<saranga_security::SecurityStore>>,
    /// `populate-validated-user`: sent messages carry the user that sent
    /// them (`_AMQ_VALIDATED_USER`).
    pub populate_validated_user: bool,
    /// `reject-empty-validated-user`: messages without a validated user are
    /// refused.
    pub reject_empty_validated_user: bool,
    /// `mirror-ack-manager-*`.
    pub mirror_ack_manager: mirror::AckManagerSettings,
    /// The basic security manager's users, loaded from and stored in the
    /// bindings journal; `None` when JAAS does security.
    pub users: Option<Arc<users::Users>>,
    /// Its bootstrap credentials: user, password and roles, added or
    /// updated once the journals are loaded (`completeInit`).
    pub bootstrap_users: Vec<(String, String, Vec<String>)>,
    /// The protocols besides Core the broker serves.
    pub protocols: Vec<Arc<dyn protocol::ProtocolManager>>,
    /// The management address, the broker's name and what management
    /// reports of the configuration.
    pub management: management::ManagementConfig,
    /// The critical analyzer.
    pub critical: critical::CriticalConfig,
    /// The HA policy and the node manager's settings.
    pub ha: node::HaConfig,
    /// The cluster connections to deploy, in the configuration's order:
    /// the first is the default one.
    pub clusters: Vec<cluster::ClusterConfig>,
    /// `<broadcast-groups>`, started once the broker is active.
    pub broadcast_groups: Vec<discovery::BroadcastGroupConfig>,
    /// `<bridges>`, deployed once the broker is active.
    pub bridges: Vec<saranga_config::BridgeConfig>,
    /// `<grouping-handler>`.
    pub grouping_handler: Option<saranga_config::GroupingHandlerConfig>,
    /// `<federations>`, deployed once the broker is active.
    pub federations: Vec<saranga_config::FederationConfig>,
    /// `downstream-authorization`.
    pub federation_downstream_authorization: Vec<String>,
    /// The connectors and discovery groups bridges name.
    pub bridge_targets: bridge::Targets,
    /// Plugins (`<broker-plugins>`, or the embedding application's own).
    pub plugins: Vec<Arc<dyn plugin::BrokerPlugin>>,
    /// `max-disk-usage` or `min-disk-free` with `disk-scan-period`: producers
    /// are held back while the stores' disks are full. Only with
    /// persistence.
    pub disk_monitor: Option<disk::MonitorConfig>,
}

impl BrokerConfig {
    /// The default cluster connection (`ClusterManager.getDefaultConnection`):
    /// the first deployed.
    pub fn cluster(&self) -> Option<&cluster::ClusterConfig> {
        self.clusters.first()
    }

    /// Artemis's built-in defaults, with the given filter parser.
    pub fn new(filters: Arc<dyn FilterFactory>) -> Self {
        Self {
            settings: Arc::new(UniformSettings(Arc::default())),
            filters,
            wildcard: WildcardConfig::default(),
            connection_ttl_override: -1,
            addresses: Vec::new(),
            legacy_queues: Vec::new(),
            diverts: Vec::new(),
            transformers: HashMap::new(),
            storage: None,
            message_expiry_scan_period: 30000,
            transaction_timeout: 300000,
            transaction_timeout_scan_period: 1000,
            address_queue_scan_period: 30000,
            id_cache_size: 20000,
            persist_id_cache: true,
            global_max_size: -1,
            global_max_messages: -1,
            security: None,
            populate_validated_user: false,
            reject_empty_validated_user: false,
            mirror_ack_manager: mirror::AckManagerSettings::default(),
            users: None,
            bootstrap_users: Vec::new(),
            protocols: Vec::new(),
            management: management::ManagementConfig::default(),
            critical: critical::CriticalConfig::default(),
            ha: node::HaConfig::default(),
            clusters: Vec::new(),
            broadcast_groups: Vec::new(),
            bridges: Vec::new(),
            grouping_handler: None,
            federations: Vec::new(),
            federation_downstream_authorization: Vec::new(),
            bridge_targets: bridge::Targets::default(),
            plugins: Vec::new(),
            disk_monitor: None,
        }
    }
}

/// `PostOfficeImpl.ExpiryReaper`: every period, each queue expires the
/// messages whose time has passed. It stops with the post office.
fn spawn_expiry_reaper(post_office: &Arc<PostOffice>, period_ms: i64) {
    let post_office = Arc::downgrade(post_office);
    let period = std::time::Duration::from_millis(period_ms as u64);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(period).await;
            let Some(po) = post_office.upgrade() else {
                break;
            };
            for q in po.queues() {
                q.expire_scan();
            }
        }
    });
}

/// `PostOfficeImpl.AddressQueueReaper`: every period, removes the
/// auto-created queues and addresses no longer used. It stops with the post
/// office.
fn spawn_address_queue_reaper(post_office: &Arc<PostOffice>, period_ms: i64) {
    let post_office = Arc::downgrade(post_office);
    let period = std::time::Duration::from_millis(period_ms as u64);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(period).await;
            let Some(po) = post_office.upgrade() else {
                break;
            };
            po.reap(false);
        }
    });
}

/// `ResourceManagerImpl.TxTimeoutHandler`: every period, XA branches past
/// their timeout are rolled back. It stops with the broker.
/// `MonitorLock`: every `journal-lock-monitor-timeout`, the primary lock is
/// checked; lost more than `journal-lock-monitor-max-retries` times in a
/// row, the broker stops as for a critical IO error.
fn spawn_lock_monitor(broker: &Arc<Broker>, n: Arc<node::NodeManager>, ha: &node::HaConfig) {
    let weak = Arc::downgrade(broker);
    // Held weakly: the lock files close with the broker, not at the next
    // check.
    let n = Arc::downgrade(&n);
    let (period, max_retries) = (ha.lock_monitor_period, ha.lock_monitor_max_retries);
    tokio::spawn(async move {
        let mut failures = 0u32;
        loop {
            tokio::time::sleep(period).await;
            let Some(broker) = weak.upgrade() else { break };
            if broker.is_stopped() {
                break;
            }
            let Some(check) = n.upgrade() else { break };
            drop(broker);
            let lost = tokio::task::spawn_blocking(move || check.primary_lock_lost())
                .await
                .unwrap_or(true);
            if !lost {
                if failures > 0 {
                    tracing::info!(
                        "Lock check succeeded after {failures} previous failure(s), resetting failure counter"
                    );
                }
                failures = 0;
                continue;
            }
            failures += 1;
            if failures <= max_retries {
                tracing::warn!(
                    "Lock check failed ({failures}/{}), will retry on next check period",
                    max_retries + 1
                );
                continue;
            }
            let Some(broker) = weak.upgrade() else { break };
            tracing::warn!(
                "Lost the lock according to the monitor after {failures} failed check(s), notifying listeners"
            );
            tracing::error!(
                "AMQ222010: Critical IO Error, shutting down the server. file=null, message=Lost NodeManager lock"
            );
            broker
                .lock_lost
                .store(true, std::sync::atomic::Ordering::Release);
            broker.request_shutdown();
            break;
        }
    });
}

fn spawn_transaction_reaper(broker: &Arc<Broker>, period_ms: i64) {
    let broker = Arc::downgrade(broker);
    let period = std::time::Duration::from_millis(period_ms.max(1) as u64);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(period).await;
            let Some(b) = broker.upgrade() else { break };
            for tx in b.resource_manager.take_timed_out(message::now_millis()) {
                let outcome = transaction::lock(&tx).rollback();
                match outcome {
                    Ok(mut outcome) => {
                        if let Err(e) = outcome.write(b.storage.as_deref()) {
                            tracing::error!("AMQ224053: failed to timeout transaction: {e}");
                        }
                        outcome.apply();
                    }
                    Err(e) => tracing::error!("AMQ224053: failed to timeout transaction: {e}"),
                }
            }
        }
    });
}

/// The broker an instance runs now. The network check stops a broker and
/// starts a new one in its place, and what serves the instance (the web
/// server, the configuration reloader) follows it here.
pub struct Current(std::sync::RwLock<Arc<Broker>>);

impl Current {
    pub fn new(broker: Arc<Broker>) -> Arc<Self> {
        Arc::new(Current(std::sync::RwLock::new(broker)))
    }

    pub fn get(&self) -> Arc<Broker> {
        self.0.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn set(&self, broker: Arc<Broker>) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = broker;
    }
}

pub struct Broker {
    node_id: String,
    post_office: Arc<PostOffice>,
    connection_ttl_override: i64,
    next_consumer_id: AtomicU64,
    /// Session metadata by session name, for `addUniqueMetaData`
    /// (`ActiveMQServerImpl.lookupSession`).
    session_metadata: Mutex<HashMap<String, HashMap<String, String>>>,
    resource_manager: transaction::ResourceManager,
    storage: Option<Arc<storage::Storage>>,
    /// Where large-message bodies go.
    large_messages: Arc<large::LargeMessageStore>,
    /// The node manager of a persistent broker, holding the primary lock.
    node_manager: Option<Arc<node::NodeManager>>,
    /// `failover-on-shutdown`.
    failover_on_shutdown: bool,
    /// The lock monitor found the primary lock lost.
    lock_lost: std::sync::atomic::AtomicBool,
    /// A store hit an I/O error (`DefaultCriticalErrorListener.failedAlready`).
    critical_io: std::sync::atomic::AtomicBool,
    /// `ClusterManager.clusterConnections`, in the order they were deployed:
    /// the first is the default one. Started with the broker.
    clusters: Vec<Arc<cluster::ClusterConnection>>,
    /// `ClusterManager.broadcastGroups`.
    broadcast_groups: Vec<Arc<discovery::BroadcastGroup>>,
    /// `ClusterManager.bridges`.
    bridges: bridge::Bridges,
    /// `FederationManager`.
    federation: federation::FederationManager,
    /// Connections to other brokers that protocols made (`brokerConnectionMap`).
    broker_connections: broker_connection::BrokerConnections,
    /// `replication` `primary`: a backup may register to replicate this
    /// broker's data.
    replication_policy: Option<saranga_config::Replication>,
    /// The backup this broker replicates to.
    replicating: Mutex<Option<Arc<replication::Replicator>>>,
    /// Replication with a lock manager: the node's lock and sequence.
    coordination: Option<Arc<replication::pluggable::Coordination>>,
    /// A restarted primary asked this active backup to give way: it stops
    /// as if it had failed, and waits as a backup again.
    failing_back: std::sync::atomic::AtomicBool,
    failback: tokio::sync::Notify,
    security: Option<Arc<saranga_security::SecurityStore>>,
    /// For the diverts a reload deploys.
    transformers: HashMap<String, postoffice::TransformerFactory>,
    populate_validated_user: bool,
    reject_empty_validated_user: bool,
    /// Every protocol the broker serves, Core first.
    protocols: Vec<Arc<dyn protocol::ProtocolManager>>,
    /// Message-journal records the broker does not know, until the
    /// protocol that owns them takes them.
    extra_records: Mutex<Vec<storage::ExtraRecord>>,
    management: management::Management,
    /// Reads the configuration again, for `reloadConfigurationFile`.
    config_loader: Mutex<Option<ConfigLoader>>,
    /// The connections, sessions, consumers and producers.
    registry: Arc<registry::Registry>,
    /// The basic security manager's users.
    users: Option<Arc<users::Users>>,
    /// The records of the security settings management added, by match
    /// (`mapPersistedSecuritySettings`).
    security_records: Mutex<HashMap<String, i64>>,
    stopped: std::sync::atomic::AtomicBool,
    /// [`Broker::stop_services`] ran.
    services_stopped: std::sync::atomic::AtomicBool,
    /// The critical analyzer asked for the broker to stop.
    shutdown: tokio::sync::Notify,
    acceptors: acceptors::Acceptors,
    /// The mirror targets' `AckManager`, once there is one.
    ack_manager: std::sync::OnceLock<Arc<mirror::AckManager>>,
    ack_manager_settings: mirror::AckManagerSettings,
}

/// Reads the configuration files again (`ReloadManager`).
pub type ConfigLoader = Arc<dyn Fn() -> std::result::Result<reload::Reload, String> + Send + Sync>;

impl Broker {
    /// Starts the broker: with persistence, takes the journal directory's
    /// lock, recovers addresses and queues from the bindings journal, deploys
    /// the configured ones, then recovers messages and prepared transactions
    /// (`ActiveMQServerImpl.initialisePart2` / `loadJournals`).
    ///
    /// The primary lock is waited for first, as `startPrimaryNode` waits
    /// for it, for `journal-lock-acquisition-timeout`.
    pub async fn start(config: BrokerConfig) -> Result<Arc<Self>> {
        Self::start_activated(config, None).await
    }

    /// Starts a broker whose node manager already holds the primary lock:
    /// a backup that took over from its primary.
    pub async fn start_activated(
        config: BrokerConfig,
        activated: Option<Arc<node::NodeManager>>,
    ) -> Result<Arc<Self>> {
        // `ServerStatus`: one per process, cleared as a broker starts, and
        // the configuration's status first (`internalStart`).
        saranga_buffer::status::clear();
        if let Some(file) = &config.management.file {
            saranga_buffer::status::update(
                saranga_buffer::status::CONFIGURATION_COMPONENT,
                file.status.clone(),
            );
        }
        let internal =
            |what: &str, e: &dyn std::fmt::Display| BrokerError::internal(format!("{what}: {e}"));
        // `SharedNothingPrimaryActivation`: each broker of a replicating
        // pair has its own data, and takes no primary lock on it.
        let replicating = config.ha.policy.replication().is_some();
        let started_primary = activated.is_none() && !replicating;
        let (ids, storage, node_manager) = match &config.storage {
            Some(sc) => {
                let node_manager = match activated {
                    Some(n) => n,
                    None => {
                        let dir = config
                            .ha
                            .lock_directory
                            .clone()
                            .unwrap_or_else(|| sc.messages.directory.clone());
                        let timeout = config.ha.lock_acquisition_timeout;
                        let n = tokio::task::spawn_blocking(move || {
                            node::NodeManager::open(&dir, timeout).map(Arc::new)
                        })
                        .await
                        .map_err(|e| internal("journal directory lock", &e))?
                        .map_err(|e| internal("journal directory lock", &e))?;
                        // SharedStorePrimaryActivation: a backup is active
                        // on the data, so this primary announces itself as
                        // its backup, which asks it to fail back.
                        let announcer = match config.cluster() {
                            Some(cc) if !replicating && n.is_backup_active().unwrap_or(false) => {
                                Some(cluster::BackupAnnouncer::start(cc.clone(), n.node_id()))
                            }
                            _ => None,
                        };
                        let n2 = n.clone();
                        let locked = tokio::task::spawn_blocking(move || {
                            if replicating {
                                Ok(())
                            } else {
                                n2.start_primary()
                            }
                        })
                        .await
                        .map_err(|e| internal("journal directory lock", &e));
                        if let Some(a) = announcer {
                            a.close().await;
                        }
                        locked?.map_err(|e| internal("journal directory lock", &e))?;
                        n
                    }
                };
                let storage = Arc::new(
                    storage::Storage::open(sc.clone())
                        .map_err(|e| internal("opening the journals", &e))?,
                );
                (
                    Arc::new(IdGenerator::persistent(storage.clone())),
                    Some(storage),
                    Some(node_manager),
                )
            }
            None => (Arc::new(IdGenerator::new(1)), None, None),
        };
        let large_messages = match &config.storage {
            Some(sc) => {
                large::LargeMessageStore::new(sc.large_messages.clone(), sc.large_message_sync)
            }
            None => large::LargeMessageStore::in_memory(),
        };
        if let Some(storage) = &storage {
            large_messages.set_history(storage.clone());
        }
        // recoverLargeMessagesOnFolder: the files there before loading.
        let mut stored_large = large_messages
            .start()
            .map_err(|e| internal("large-messages directory", &e))?;
        let paging = paging::PagingManager::new(
            config.storage.as_ref().map(|sc| sc.paging.clone()),
            storage.clone(),
            large_messages.clone(),
            config.global_max_size,
            config.global_max_messages,
        );
        let decoders = message::MessageDecoders::new(
            config
                .protocols
                .iter()
                .filter_map(|p| p.message_decoder())
                .collect(),
        );
        paging.set_decoders(decoders.clone());
        // The page stores are found before the queues subscribe to them.
        paging
            .reload_stores(|a| config.settings.get_match(a))
            .map_err(|e| internal("paging directory", &e))?;
        let post_office = PostOffice::new(
            config.settings,
            config.filters,
            ids,
            config.wildcard,
            duplicate::DuplicateIdCaches::new(
                config.id_cache_size,
                storage.clone().filter(|_| config.persist_id_cache),
            ),
            paging.clone(),
        );
        // registerBrokerPlugins: before anything is loaded, so the plugins
        // see the addresses and queues recovered and deployed.
        for p in &config.plugins {
            post_office.plugins().register(p.clone());
        }
        // deployGroupingHandlerConfiguration: before the journal's groups
        // are loaded into it.
        if let Some(g) = &config.grouping_handler {
            post_office.set_grouping(grouping::GroupingHandler::new(g.clone(), &post_office));
        }
        let recovered = match &storage {
            Some(s) => recovery::load_bindings(s, &post_office, config.users.as_deref())?,
            None => Default::default(),
        };
        reload::deploy_addresses(&post_office, &config.addresses);
        // deployQueuesFromConfiguration.
        reload::deploy_queue_specs(&post_office, &config.legacy_queues);
        // recoverStoredDiverts: a journaled divert no longer configured is
        // deployed again, or forgotten under config-delete-diverts FORCE.
        for d in &recovered.diverts {
            if config.diverts.iter().any(|c| c.name == d.name) {
                continue;
            }
            if post_office.settings(&d.address).config_delete_diverts {
                post_office.forget_divert_record(&d.name);
            } else if let Err(e) = post_office.deploy_divert(d, &config.transformers) {
                tracing::warn!(
                    "AMQ222314: Failed to recover stored divert configuration {}: {e}",
                    d.name
                );
            }
        }
        for d in &config.diverts {
            if let Err(e) = post_office.deploy_divert_persisted(d, &config.transformers) {
                tracing::warn!("divert {}: {e}", d.name);
            }
        }
        let resource_manager =
            transaction::ResourceManager::new((config.transaction_timeout / 1000) as i32);
        let mut extra_records = Vec::new();
        if let Some(s) = &storage {
            extra_records = recovery::load_messages(
                s,
                &post_office,
                &resource_manager,
                &large_messages,
                &mut stored_large,
                &decoders,
            )
            .await?;
            // Large messages in pages keep their files; the rest are the
            // leftovers of a crash (`rebuildCounters`).
            paging.after_load(&mut stored_large);
            large_messages.delete_files(stored_large);
        } else {
            paging.set_loaded();
        }
        reload::undeploy_not_configured(&post_office, &config.addresses);
        // ActiveMQBasicSecurityManager.completeInit.
        if let (Some(users), Some(s)) = (&config.users, &storage) {
            users.attach(s.clone());
            for (user, password, roles) in &config.bootstrap_users {
                if let Err(e) = users.add_or_update(user, password, roles) {
                    tracing::warn!(
                        "AMQ222298: Failed to create bootstrap user \"{user}\". User management may not function. {e}"
                    );
                }
            }
        }
        if config.message_expiry_scan_period > 0 {
            spawn_expiry_reaper(&post_office, config.message_expiry_scan_period);
        }
        // deployFileStoreMonitor.
        if let (Some(monitor), Some(sc)) = (config.disk_monitor, &config.storage) {
            disk::spawn_monitor(
                monitor,
                vec![
                    sc.paging.clone(),
                    sc.messages.directory.clone(),
                    sc.large_messages.clone(),
                    sc.bindings.directory.clone(),
                ],
                post_office.paging(),
            );
        }
        // Empty auto-created queues and addresses go before clients come.
        post_office.reap(true);
        if config.address_queue_scan_period > 0 {
            spawn_address_queue_reaper(&post_office, config.address_queue_scan_period);
        }
        let coordination = config.ha.coordination.clone();
        let node_id = match (&coordination, &node_manager) {
            // A `coordination-id`, which `server.lock` cannot hold as it is.
            (Some(c), _) => c.node_id().to_owned(),
            (None, Some(n)) => n.node_id(),
            // `InVMNodeManager`: a time-based ID.
            (None, None) => saranga_storage::java::uuid(&saranga_client::uuid::time_based()),
        };
        let registry = Arc::new(registry::Registry::new(post_office.plugins().clone()));
        let broadcast_groups = config
            .broadcast_groups
            .iter()
            .map(|g| discovery::BroadcastGroup::new(g.clone(), &node_id, &post_office))
            .collect();
        let broker = Arc::new(Self {
            broadcast_groups,
            bridges: bridge::Bridges::new(config.bridge_targets.clone()),
            federation: federation::FederationManager::default(),
            broker_connections: broker_connection::BrokerConnections::default(),
            node_id: node_id.clone(),
            post_office,
            connection_ttl_override: config.connection_ttl_override,
            next_consumer_id: AtomicU64::new(1),
            session_metadata: Mutex::default(),
            resource_manager,
            storage,
            large_messages,
            node_manager: node_manager.clone(),
            failover_on_shutdown: config.ha.failover_on_shutdown(),
            lock_lost: std::sync::atomic::AtomicBool::new(false),
            critical_io: std::sync::atomic::AtomicBool::new(false),
            replication_policy: match &config.ha.policy {
                saranga_config::HaPolicy::ReplicationPrimary(r) => Some(r.clone()),
                _ => None,
            },
            replicating: Mutex::new(None),
            coordination: coordination.clone(),
            clusters: config
                .clusters
                .iter()
                .map(|c| cluster::ClusterConnection::new(c.clone(), &node_id))
                .collect(),
            failing_back: std::sync::atomic::AtomicBool::new(false),
            failback: tokio::sync::Notify::new(),
            security: config.security.clone(),
            transformers: config.transformers,
            populate_validated_user: config.populate_validated_user,
            reject_empty_validated_user: config.reject_empty_validated_user,
            protocols: [
                Arc::new(protocol::CoreProtocol) as Arc<dyn protocol::ProtocolManager>,
                Arc::new(protocol::HornetQProtocol),
            ]
            .into_iter()
            .chain(config.protocols)
            .collect(),
            extra_records: Mutex::new(extra_records),
            management: management::Management::new(config.management),
            config_loader: Mutex::new(None),
            registry,
            users: config.users.clone(),
            security_records: Mutex::default(),
            stopped: Default::default(),
            services_stopped: Default::default(),
            shutdown: tokio::sync::Notify::new(),
            acceptors: Default::default(),
            ack_manager: std::sync::OnceLock::new(),
            ack_manager_settings: config.mirror_ack_manager,
        });
        if let Some(s) = &broker.storage {
            let weak = Arc::downgrade(&broker);
            s.set_critical_listener(Arc::new(move |e, file| {
                let message = match e {
                    saranga_journal::JournalError::Io(e) => e.to_string(),
                    e => e.to_string(),
                };
                if let Some(b) = weak.upgrade() {
                    b.critical_io_error(&message, file);
                }
            }));
            broker.large_messages.set_critical(s);
        }
        for p in broker.post_office.plugins().all() {
            p.registered(&broker);
        }
        if let Some(c) = &coordination {
            c.watch(Arc::downgrade(&broker));
        }
        // recoverStoredSecuritySettings.
        for (id, s) in &recovered.security_settings {
            let address_match = s.address_match.to_string_lossy();
            if let Some(store) = &broker.security
                && let Err(e) =
                    store.add_match(&address_match, management::recovered_security_setting(s))
            {
                tracing::warn!("security setting {address_match}: {e}");
            }
            broker.security_records().insert(address_match, *id);
        }
        spawn_transaction_reaper(&broker, config.transaction_timeout_scan_period);
        // The mirror targets' acknowledgements still to retry
        // (`AckManager.reload`).
        let retries = broker.take_records(saranga_storage::ids::ACK_RETRY);
        if !retries.is_empty() {
            broker.ack_manager().reload(retries);
        }
        slow_consumers::spawn(&broker);
        if broker.management.counters().enabled() {
            broker.management.counters().start(&broker);
        }
        // The broker is active: notifications go out from now on, to an
        // address that exists so cluster bridges can connect
        // (`ManagementServiceImpl.start`). `addAddressInfo` journals it.
        let notifications = broker.management.config().notification_address.clone();
        if broker.post_office.address_info(&notifications).is_none()
            && let Err(e) = broker.post_office.add_or_update_address(
                &notifications,
                RoutingTypes::of(RoutingType::Multicast),
                false,
                false,
            )
        {
            tracing::warn!(
                "AMQ222270: Unable to create management notification address: {notifications}: {e}"
            );
        }
        broker
            .post_office
            .set_notifier(notifications::Notifier::start(
                &broker.post_office,
                broker.management.config().notification_address.clone(),
                broker.node_id.clone(),
                broker.management.address().to_owned(),
            ));
        if let Some(store) = &broker.security {
            let po = Arc::downgrade(&broker.post_office);
            store.set_violation_listener(Arc::new(move |v| {
                if let Some(po) = po.upgrade() {
                    notifications::security_violation(&po, v);
                }
            }));
        }
        // loadProtocolServices: each protocol sets itself up once the
        // journals are loaded.
        for p in broker.protocols.clone() {
            p.start(&broker).await;
        }
        critical::start(&broker, config.critical);
        if let Some(n) = &node_manager
            && started_primary
        {
            // The activation callback: active, and the lock watched.
            let n2 = n.clone();
            tokio::task::spawn_blocking(move || n2.activation_complete())
                .await
                .map_err(|e| internal("server.lock", &e))?
                .map_err(|e| internal("server.lock", &e))?;
            spawn_lock_monitor(&broker, n.clone(), &config.ha);
        }
        let grouping = broker.post_office.grouping().cloned();
        if let Some(g) = &grouping {
            g.start();
        }
        // `ClusterManager.start`: in the order of its `HashMap` of names.
        for c in cluster::start_order(&broker.clusters) {
            c.activate();
            c.start_bridging(&broker);
        }
        // A LOCAL grouping handler waits for its groups' bindings before
        // clients come.
        if let Some(g) = grouping.filter(|g| g.is_local()) {
            let bound: Vec<String> = broker
                .post_office
                .queues()
                .iter()
                .map(|q| format!("{}{}", q.name(), broker.node_id))
                .collect();
            g.await_bindings(bound).await;
        }
        // `ClusterManager.start`.
        for g in &broker.broadcast_groups {
            if let Err(e) = g.start() {
                tracing::warn!(
                    "AMQ222116: unable to start broadcast group {}: {e}",
                    g.config().name
                );
            }
        }
        for (record, b) in recovered.bridges {
            broker.bridges.reload_record(record, b);
        }
        broker.deploy_bridges(&config.bridges);
        // `FederationManager.start`, after the cluster's.
        broker.federation.start(
            &broker,
            &config.federations,
            &config.federation_downstream_authorization,
        );
        // startProtocolServices: AMQP's broker connections.
        for p in broker.protocols.clone() {
            p.start_services(&broker);
        }
        Ok(broker)
    }

    /// Whether a protocol of this name is loaded.
    pub fn has_protocol(&self, name: &str) -> bool {
        self.protocols.iter().any(|p| p.name() == name)
    }

    /// The default cluster connection (`getDefaultConnection(null)`): the
    /// first deployed.
    pub fn cluster(&self) -> Option<&Arc<cluster::ClusterConnection>> {
        self.clusters.first()
    }

    /// Every cluster connection, in the order they were deployed.
    pub fn clusters(&self) -> &[Arc<cluster::ClusterConnection>] {
        &self.clusters
    }

    /// `ClusterManager.getClusterConnection`.
    pub fn cluster_named(&self, name: &str) -> Option<&Arc<cluster::ClusterConnection>> {
        self.clusters.iter().find(|c| c.config().name == name)
    }

    /// `RemotingServiceImpl.lookupClusterConnection`: the cluster connection
    /// an acceptor's `clusterConnection` parameter names, else the default
    /// one (every TCP acceptor is equivalent to its connector).
    pub fn acceptor_cluster(
        &self,
        acceptor: &Acceptor,
    ) -> Option<&Arc<cluster::ClusterConnection>> {
        acceptor
            .params
            .get("clusterConnection")
            .and_then(|n| self.cluster_named(n))
            .or_else(|| self.cluster())
    }

    /// The broadcast groups, in the configuration's order.
    pub fn broadcast_groups(&self) -> &[Arc<discovery::BroadcastGroup>] {
        &self.broadcast_groups
    }

    pub fn broadcast_group(&self, name: &str) -> Option<&Arc<discovery::BroadcastGroup>> {
        self.broadcast_groups
            .iter()
            .find(|g| g.config().name == name)
    }

    /// `SharedStoreBackupActivation.FailbackChecker`: every second, an
    /// active backup checks whether its primary has restarted (`server.lock`
    /// says failing back) and announced itself as this node's backup; then
    /// it asks to be stopped and restarted as a backup
    /// ([`Broker::failback_requested`]). Without a cluster connection the
    /// primary cannot be heard, as Artemis warns (AMQ222308).
    pub fn start_failback_checker(self: &Arc<Self>) {
        let Some(n) = self.node_manager.clone() else {
            return;
        };
        let Some(cluster) = self.cluster().cloned() else {
            tracing::warn!(
                "AMQ222308: Unable to listen for incoming fail-back request because cluster connection is null. Ensure the broker has the proper cluster-connection configuration."
            );
            return;
        };
        let listener = cluster::FailbackListener::new(&self.node_id, cluster.connector().clone());
        let subscription = cluster::Subscription::new(&cluster, listener.clone());
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let _subscription = subscription;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                match weak.upgrade() {
                    Some(b) if !b.is_stopped() => {}
                    _ => return,
                }
                let n2 = n.clone();
                let awaiting = tokio::task::spawn_blocking(move || n2.is_awaiting_failback())
                    .await
                    .ok()
                    .and_then(std::result::Result::ok)
                    .unwrap_or(false);
                // `BackupTopologyListener.waitForBackup`: up to a minute.
                if !awaiting || !listener.wait(std::time::Duration::from_secs(60)).await {
                    continue;
                }
                let Some(b) = weak.upgrade() else { return };
                if b.is_stopped() {
                    return;
                }
                tracing::info!(
                    "AMQ221008: primary server wants to restart, restarting server in backup"
                );
                b.failing_back
                    .store(true, std::sync::atomic::Ordering::Release);
                b.failback.notify_one();
                return;
            }
        });
    }

    /// Waits for the failback checker to ask for this broker to stop and
    /// wait as a backup again.
    pub async fn failback_requested(&self) {
        self.failback.notified().await;
    }

    /// Whether [`Broker::stop`] has run.
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(std::sync::atomic::Ordering::Acquire)
    }

    /// `DefaultCriticalErrorListener.onIOException`: the first I/O error
    /// writing the journals, a page file or a large message file stops the
    /// broker (`stopTheServer(true)`); later ones are ignored.
    pub fn critical_io_error(&self, message: &str, file: Option<&std::path::Path>) {
        if self
            .critical_io
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let file = file.map_or_else(|| "NULL".to_owned(), |f| f.display().to_string());
        tracing::error!(
            "AMQ222010: Critical IO Error, shutting down the server. file={file}, message={message}"
        );
        self.request_shutdown();
    }

    /// Whether a store hit an I/O error ([`Broker::critical_io_error`]).
    pub fn is_critical_io_failed(&self) -> bool {
        self.critical_io.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Asks whoever runs the broker to stop it (the critical analyzer's
    /// `SHUTDOWN` policy).
    pub fn request_shutdown(&self) {
        self.shutdown.notify_one();
    }

    /// Waits for [`Broker::request_shutdown`].
    pub async fn shutdown_requested(&self) {
        self.shutdown.notified().await;
    }

    /// `registerBrokerPlugin`: installs a plugin in the running broker.
    pub fn register_plugin(self: &Arc<Self>, plugin: Arc<dyn plugin::BrokerPlugin>) {
        self.post_office.plugins().register(plugin.clone());
        plugin.registered(self);
    }

    /// `unRegisterBrokerPlugin`.
    pub fn unregister_plugin(&self, plugin: &Arc<dyn plugin::BrokerPlugin>) {
        if self.post_office.plugins().unregister(plugin) {
            plugin.unregistered(self);
        }
    }

    /// What `ActiveMQServerImpl.stop` stops before it pauses the acceptors
    /// and closes the connections: the grouping handler (before the cluster,
    /// so its groups outlive the bindings the cluster takes away),
    /// federation, the cluster manager (broadcast groups, cluster
    /// connections, bridges) and the protocols' services. Done once; a
    /// cluster connection stopped first is not told by the other nodes that
    /// this one went away.
    pub fn stop_services(&self) {
        if self
            .services_stopped
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        if let Some(g) = self.post_office.grouping() {
            g.stop();
        }
        self.federation.stop();
        for g in &self.broadcast_groups {
            g.stop();
        }
        // `ClusterManager.stop`: in the order of its `HashMap` of names.
        for c in cluster::start_order(&self.clusters) {
            c.stop();
        }
        self.stop_bridges();
        for p in &self.protocols {
            p.stop_services(self);
        }
    }

    /// A clean stop: the ID generator's checkpoint and the journals are
    /// written out, and `server.lock` marked paused.
    pub fn stop(&self) -> Result<()> {
        if !self.stopped.swap(true, std::sync::atomic::Ordering::AcqRel) {
            audit::management_stopped(self);
            for p in self.post_office.plugins().all() {
                p.unregistered(self);
            }
        }
        self.stop_services();
        self.stop_replicating();
        self.large_messages.stop();
        self.post_office.paging().stop();
        // After a critical I/O error the stores fail whatever they are
        // asked to write: the stop goes on regardless.
        let critical = self.is_critical_io_failed();
        if let Some(s) = &self.storage {
            match s.stop() {
                Err(e) if !critical => {
                    return Err(BrokerError::internal(format!("stopping the journals: {e}")));
                }
                _ => {}
            }
        }
        // Each queue's actor holds its queue, its pages and the storage:
        // they end with the broker. What they drop is in the journals, and
        // large message files are no longer deleted.
        for q in self.post_office.queues() {
            q.stop();
        }
        // The activation's close: the lock let go for a backup to take
        // over, or the broker marked paused.
        if let Some(n) = &self.node_manager
            && n.holds_primary_lock()
        {
            if self.failover_on_shutdown
                || self.lock_lost.load(std::sync::atomic::Ordering::Acquire)
                || self.failing_back.load(std::sync::atomic::Ordering::Acquire)
            {
                n.crash_primary();
            } else if let Err(e) = n.pause_primary()
                && !critical
            {
                return Err(BrokerError::internal(format!("server.lock: {e}")));
            }
        }
        Ok(())
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// With a lock manager: the node's lock and sequence this broker holds.
    pub fn coordination(&self) -> Option<&Arc<replication::pluggable::Coordination>> {
        self.coordination.as_ref()
    }

    /// The management service.
    pub fn management(&self) -> &management::Management {
        &self.management
    }

    /// The connections and their sessions, consumers and producers.
    pub fn registry(&self) -> &Arc<registry::Registry> {
        &self.registry
    }

    /// `getFederationManager`.
    /// The connections to other brokers, as registered.
    pub fn broker_connections(&self) -> Vec<Arc<dyn broker_connection::BrokerConnection>> {
        self.broker_connections.all()
    }

    /// Where protocols register their connections to other brokers.
    pub fn broker_connection_registry(&self) -> &broker_connection::BrokerConnections {
        &self.broker_connections
    }

    pub fn federation(&self) -> &federation::FederationManager {
        &self.federation
    }

    /// Divert transformers by class name.
    pub fn transformers(&self) -> &HashMap<String, postoffice::TransformerFactory> {
        &self.transformers
    }

    /// How `reloadConfigurationFile` reads the configuration again.
    pub fn set_config_loader(&self, loader: ConfigLoader) {
        *self.config_loader.lock().unwrap_or_else(|e| e.into_inner()) = Some(loader);
    }

    /// `reloadConfigurationFile`: the configuration is read again and
    /// reloaded now.
    pub async fn reload_configuration_file(self: &Arc<Self>) -> std::result::Result<(), String> {
        let loader = self
            .config_loader
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(load) = loader {
            self.reload(load()?).await;
        }
        Ok(())
    }

    /// `removeConsumers` of `destroyQueue`: the queue's consumers are
    /// closed and their clients told (`ServerConsumer.disconnect`).
    pub async fn disconnect_consumers(&self, queue: &queue::Queue) {
        queue.disconnect_consumers().await;
    }

    /// The security store, whether or not security is enabled.
    pub fn security_store(&self) -> Option<&Arc<saranga_security::SecurityStore>> {
        self.security.as_ref()
    }

    /// The basic security manager's users, when it is the one in use.
    pub fn users(&self) -> Option<&Arc<users::Users>> {
        self.users.as_ref()
    }

    pub(crate) fn security_records(&self) -> std::sync::MutexGuard<'_, HashMap<String, i64>> {
        self.security_records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// The security store, when security is enabled.
    pub fn security(&self) -> Option<&Arc<saranga_security::SecurityStore>> {
        self.security.as_ref().filter(|s| s.is_enabled())
    }

    pub fn populate_validated_user(&self) -> bool {
        self.populate_validated_user
    }

    pub fn reject_empty_validated_user(&self) -> bool {
        self.reject_empty_validated_user
    }

    pub fn post_office(&self) -> &Arc<PostOffice> {
        &self.post_office
    }

    /// `PagingManager.getPageStore`: the address's store, created on first
    /// use, but none for the management address, so that what is sent to
    /// it is never held back. A fully qualified name is its address's.
    pub fn page_store(&self, address: &str) -> Option<Arc<paging::PagingStore>> {
        let name = address.split_once("::").map_or(address, |(a, _)| a);
        if name.starts_with(self.management().address()) {
            return None;
        }
        Some(self.post_office.paging_store(name))
    }

    pub fn large_messages(&self) -> &Arc<large::LargeMessageStore> {
        &self.large_messages
    }

    /// `getMaxRecordSize`: larger messages are stored as large messages.
    pub fn max_record_size(&self) -> Option<usize> {
        self.storage.as_ref().map(|s| s.max_record_size())
    }

    /// XA transactions by Xid.
    pub fn resource_manager(&self) -> &transaction::ResourceManager {
        &self.resource_manager
    }

    pub fn connection_ttl_override(&self) -> i64 {
        self.connection_ttl_override
    }

    pub fn next_consumer_id(&self) -> queue::ConsumerId {
        self.next_consumer_id.fetch_add(1, Ordering::Relaxed)
    }

    fn metadata(&self) -> std::sync::MutexGuard<'_, HashMap<String, HashMap<String, String>>> {
        self.session_metadata
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// `ServerSessionImpl.addMetaData`.
    pub fn add_session_metadata(&self, session: &str, key: &str, data: &str) {
        self.metadata()
            .entry(session.to_owned())
            .or_default()
            .insert(key.to_owned(), data.to_owned());
        self.post_office
            .plugins()
            .each(|p| p.after_session_metadata_added(session, key, data));
    }

    /// `addUniqueMetaData`: refuses a key/value pair another session holds.
    pub fn add_unique_session_metadata(&self, session: &str, key: &str, data: &str) -> bool {
        let mut all = self.metadata();
        let taken = all
            .iter()
            .any(|(s, m)| s != session && m.get(key).is_some_and(|v| v == data));
        if taken {
            return false;
        }
        all.entry(session.to_owned())
            .or_default()
            .insert(key.to_owned(), data.to_owned());
        drop(all);
        self.post_office
            .plugins()
            .each(|p| p.after_session_metadata_added(session, key, data));
        true
    }

    pub fn session_metadata(&self, session: &str, key: &str) -> Option<String> {
        self.metadata().get(session)?.get(key).cloned()
    }

    /// Every key and value of a session's metadata, by key.
    pub fn session_metadata_all(&self, session: &str) -> Option<Vec<(String, String)>> {
        let mut all: Vec<_> = self.metadata().get(session)?.clone().into_iter().collect();
        all.sort();
        Some(all)
    }

    pub fn remove_session_metadata(&self, session: &str) {
        self.metadata().remove(session);
    }

    /// Accepts Core connections until the listener fails.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> std::io::Result<()> {
        self.serve_acceptor(listener, Acceptor::default()).await
    }

    /// The protocols `acceptor` serves (`RemotingServiceImpl.locateProtocols`):
    /// those it lists that the broker has, or every one when it lists none
    /// of them.
    pub fn protocols_for(&self, acceptor: &Acceptor) -> Vec<Arc<dyn protocol::ProtocolManager>> {
        let listed: Vec<_> = match &acceptor.protocols {
            None => Vec::new(),
            Some(names) => self
                .protocols
                .iter()
                .filter(|p| names.iter().any(|n| n == p.name()))
                .cloned()
                .collect(),
        };
        if listed.is_empty() {
            self.protocols.clone()
        } else {
            listed
        }
    }

    /// Takes the recovered message-journal records of a protocol's own type
    /// (`registerRecordsLoader`): ID and body, in journal order.
    pub fn take_records(&self, record_type: u8) -> Vec<(i64, Vec<u8>)> {
        let mut records = self.extra_records.lock().unwrap_or_else(|e| e.into_inner());
        let (mine, rest) = std::mem::take(&mut *records)
            .into_iter()
            .partition(|r| r.record_type == record_type);
        *records = rest;
        mine.into_iter()
            .map(|r: storage::ExtraRecord| (r.id, r.data))
            .collect()
    }

    /// The mirror targets' `AckManager` (`AckManagerProvider.getManager`),
    /// made and started when first needed.
    pub fn ack_manager(&self) -> &Arc<mirror::AckManager> {
        self.ack_manager.get_or_init(|| {
            let m = mirror::AckManager::new(
                &self.post_office,
                &self.node_id,
                self.ack_manager_settings,
            );
            m.start();
            m
        })
    }

    /// The broker's journals; `None` without persistence.
    pub fn storage(&self) -> Option<&Arc<storage::Storage>> {
        self.storage.as_ref()
    }

    /// The names of every protocol the broker serves.
    pub fn protocol_names(&self) -> Vec<&'static str> {
        self.protocols.iter().map(|p| p.name()).collect()
    }

    /// Accepts connections the way `acceptor` says: with TLS, under a
    /// security domain, for the protocols it enables.
    pub async fn serve_acceptor(
        self: Arc<Self>,
        listener: TcpListener,
        acceptor: Acceptor,
    ) -> std::io::Result<()> {
        let closing = Arc::new(tokio::sync::Notify::new());
        self.serve_shared(listener, Arc::new(acceptor), closing)
            .await;
        Ok(())
    }

    /// Accepts until aborted. `closing` ends the connections still in
    /// their TLS handshake or protocol detection; the others are failed.
    async fn serve_shared(
        self: Arc<Self>,
        listener: TcpListener,
        acceptor: Arc<Acceptor>,
        closing: Arc<tokio::sync::Notify>,
    ) {
        // NettyAcceptor.start: ACCEPTOR_STARTED with the port it listens on.
        let port = listener
            .local_addr()
            .map_or(i32::from(acceptor.port), |a| i32::from(a.port()));
        let host = acceptor.host.clone();
        self.post_office.notify(
            notifications::NotificationType::AcceptorStarted,
            move |_| {
                use saranga_buffer::PropertyValue as V;
                vec![
                    ("factory", notifications::string(Some(acceptors::FACTORY))),
                    ("host", notifications::string(Some(&host))),
                    ("port", V::Int(port)),
                ]
            },
        );
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(e) => {
                    // Netty logs a failed accept and goes on accepting.
                    tracing::warn!("acceptor {}: accept failed: {e}", acceptor.name);
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            tracing::debug!(%peer, "connection accepted");
            socket_options(&stream, &acceptor);
            let local = stream.local_addr().ok();
            let mut info = saranga_security::ConnectionInfo {
                remote_address: core_protocol::java_socket_address(peer),
                certificates: None,
                security_domain: acceptor.security_domain.clone(),
                connection_id: None,
            };
            let broker = self.clone();
            let acceptor = acceptor.clone();
            let closing = closing.clone();
            tokio::spawn(async move {
                let closed = closing.notified();
                let context = audit::Context::Remote(info.remote_address.clone());
                let detector = broker.clone();
                let setup = async move {
                    let stream: Box<dyn protocol::Stream> = match &acceptor.tls {
                        None => Box::new(stream),
                        Some(tls) => {
                            let stream = match tls.accept(stream).await {
                                Ok(s) => s,
                                Err(e) => {
                                    tracing::debug!(%peer, "TLS handshake failed: {e}");
                                    return None;
                                }
                            };
                            info.certificates = saranga_transport::tls::peer_certificates(
                                stream.get_ref().1.peer_certificates(),
                            )
                            .map(Arc::from);
                            Box::new(stream)
                        }
                    };
                    protocol::detect(&detector, stream, Some(peer), local, info, acceptor).await
                };
                let serve = async move {
                    let detected = tokio::select! {
                        d = setup => d,
                        () = closed => None,
                    };
                    if let Some((manager, connection)) = detected {
                        manager.serve(broker, connection).await;
                    }
                };
                audit::scoped(context, serve).await;
            });
        }
    }
}

/// `NettyAcceptor`'s options for each connection it accepts: `tcpNoDelay`
/// (default true) and `tcpSendBufferSize` and `tcpReceiveBufferSize`
/// (default 1 MiB; -1 leaves the system's).
fn socket_options(stream: &tokio::net::TcpStream, acceptor: &Acceptor) {
    let param = |key: &str| acceptor.params.get(key).map(String::as_str);
    // Boolean.valueOf: true only for "true", in any case.
    let no_delay = param("tcpNoDelay").is_none_or(|v| v.eq_ignore_ascii_case("true"));
    let _ = stream.set_nodelay(no_delay);
    let size = |key: &str| {
        param(key)
            .and_then(|v| v.trim().parse::<i32>().ok())
            .unwrap_or(1024 * 1024)
    };
    let socket = socket2::SockRef::from(stream);
    let send = size("tcpSendBufferSize");
    if send != -1 {
        let _ = socket.set_send_buffer_size(send.max(0) as usize);
    }
    let receive = size("tcpReceiveBufferSize");
    if receive != -1 {
        let _ = socket.set_recv_buffer_size(receive.max(0) as usize);
    }
}
