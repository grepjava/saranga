//! The Saranga broker: addresses, queues and routing, and the Core
//! protocol server that drives them.
//!
//! With persistence enabled, durable state goes to the Artemis journals
//! ([`storage`]) and is recovered on start ([`recovery`]). The structure
//! follows ADR 0001: one task per connection and per session, one actor
//! per queue.

pub mod audit;
pub mod config;
pub mod core_protocol;
pub mod critical;
mod disk;
pub mod duplicate;
pub mod error;
pub mod large;
pub mod management;
pub mod message;
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
    /// Plugins (`<broker-plugins>`, or the embedding application's own).
    pub plugins: Vec<Arc<dyn plugin::BrokerPlugin>>,
}

impl BrokerConfig {
    /// Artemis's built-in defaults, with the given filter parser.
    pub fn new(filters: Arc<dyn FilterFactory>) -> Self {
        Self {
            settings: Arc::new(UniformSettings(Arc::default())),
            filters,
            wildcard: WildcardConfig::default(),
            connection_ttl_override: -1,
            addresses: Vec::new(),
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
            users: None,
            bootstrap_users: Vec::new(),
            protocols: Vec::new(),
            management: management::ManagementConfig::default(),
            critical: critical::CriticalConfig::default(),
            plugins: Vec::new(),
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
    node_lock: Mutex<Option<node::NodeLock>>,
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
    /// The critical analyzer asked for the broker to stop.
    shutdown: tokio::sync::Notify,
}

/// Reads the configuration files again (`ReloadManager`).
pub type ConfigLoader = Arc<dyn Fn() -> std::result::Result<reload::Reload, String> + Send + Sync>;

impl Broker {
    /// Starts the broker: with persistence, takes the journal directory's
    /// lock, recovers addresses and queues from the bindings journal, deploys
    /// the configured ones, then recovers messages and prepared transactions
    /// (`ActiveMQServerImpl.initialisePart2` / `loadJournals`).
    pub async fn start(config: BrokerConfig) -> Result<Arc<Self>> {
        let internal =
            |what: &str, e: &dyn std::fmt::Display| BrokerError::internal(format!("{what}: {e}"));
        let (ids, storage, node_lock) = match &config.storage {
            Some(sc) => {
                let lock = node::NodeLock::acquire(&sc.messages.directory)
                    .map_err(|e| internal("journal directory lock", &e))?;
                tracing::info!("AMQ221035: Primary Server Obtained primary lock");
                let storage = Arc::new(
                    storage::Storage::open(sc.clone())
                        .map_err(|e| internal("opening the journals", &e))?,
                );
                (
                    Arc::new(IdGenerator::persistent(storage.clone())),
                    Some(storage),
                    Some(lock),
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
        let recovered = match &storage {
            Some(s) => recovery::load_bindings(s, &post_office, config.users.as_deref())?,
            None => Default::default(),
        };
        reload::deploy_addresses(&post_office, &config.addresses);
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
        // Empty auto-created queues and addresses go before clients come.
        post_office.reap(true);
        if config.address_queue_scan_period > 0 {
            spawn_address_queue_reaper(&post_office, config.address_queue_scan_period);
        }
        let node_id = match &node_lock {
            Some(l) => l.node_id().to_owned(),
            None => uuid::Uuid::new_v4().to_string(),
        };
        let registry = Arc::new(registry::Registry::new(post_office.plugins().clone()));
        let broker = Arc::new(Self {
            node_id,
            post_office,
            connection_ttl_override: config.connection_ttl_override,
            next_consumer_id: AtomicU64::new(1),
            session_metadata: Mutex::default(),
            resource_manager,
            storage,
            large_messages,
            node_lock: Mutex::new(node_lock),
            security: config.security.clone(),
            transformers: config.transformers,
            populate_validated_user: config.populate_validated_user,
            reject_empty_validated_user: config.reject_empty_validated_user,
            protocols: std::iter::once(
                Arc::new(protocol::CoreProtocol) as Arc<dyn protocol::ProtocolManager>
            )
            .chain(config.protocols)
            .collect(),
            extra_records: Mutex::new(extra_records),
            management: management::Management::new(config.management),
            config_loader: Mutex::new(None),
            registry,
            users: config.users.clone(),
            security_records: Mutex::default(),
            stopped: Default::default(),
            shutdown: tokio::sync::Notify::new(),
        });
        for p in broker.post_office.plugins().all() {
            p.registered(&broker);
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
        Ok(broker)
    }

    /// Whether [`Broker::stop`] has run.
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(std::sync::atomic::Ordering::Acquire)
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

    /// A clean stop: the ID generator's checkpoint and the journals are
    /// written out, and `server.lock` marked paused.
    pub fn stop(&self) -> Result<()> {
        if !self.stopped.swap(true, std::sync::atomic::Ordering::AcqRel) {
            for p in self.post_office.plugins().all() {
                p.unregistered(self);
            }
        }
        self.large_messages.stop();
        self.post_office.paging().stop();
        if let Some(s) = &self.storage {
            s.stop()
                .map_err(|e| BrokerError::internal(format!("stopping the journals: {e}")))?;
        }
        let lock = self
            .node_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(l) = lock {
            l.pause()
                .map_err(|e| BrokerError::internal(format!("server.lock: {e}")))?;
        }
        Ok(())
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// The management service.
    pub fn management(&self) -> &management::Management {
        &self.management
    }

    /// The connections and their sessions, consumers and producers.
    pub fn registry(&self) -> &Arc<registry::Registry> {
        &self.registry
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
    pub async fn reload_configuration_file(&self) -> std::result::Result<(), String> {
        let loader = self
            .config_loader
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(load) = loader {
            self.reload(load()?);
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
        let acceptor = Arc::new(acceptor);
        // NettyAcceptor.start: ACCEPTOR_STARTED with the port it listens on.
        let port = listener
            .local_addr()
            .map_or(i32::from(acceptor.port), |a| i32::from(a.port()));
        let host = acceptor.host.clone();
        self.post_office.notify(notifications::NotificationType::AcceptorStarted, move |_| {
            use saranga_buffer::PropertyValue as V;
            vec![
                ("factory", notifications::string(Some("org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory"))),
                ("host", notifications::string(Some(&host))),
                ("port", V::Int(port)),
            ]
        });
        loop {
            let (stream, peer) = listener.accept().await?;
            tracing::debug!(%peer, "connection accepted");
            let _ = stream.set_nodelay(true);
            let local = stream.local_addr().ok();
            let mut info = saranga_security::ConnectionInfo {
                remote_address: core_protocol::java_socket_address(peer),
                certificates: None,
                security_domain: acceptor.security_domain.clone(),
                connection_id: None,
            };
            let broker = self.clone();
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let stream: Box<dyn protocol::Stream> = match &acceptor.tls {
                    None => Box::new(stream),
                    Some(tls) => {
                        let stream = match tls.accept(stream).await {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::debug!(%peer, "TLS handshake failed: {e}");
                                return;
                            }
                        };
                        info.certificates = saranga_transport::tls::peer_certificates(
                            stream.get_ref().1.peer_certificates(),
                        )
                        .map(Arc::from);
                        Box::new(stream)
                    }
                };
                protocol::detect_and_serve(broker, stream, Some(peer), local, info, acceptor).await;
            });
        }
    }
}
