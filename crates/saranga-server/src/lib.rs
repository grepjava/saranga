//! The Saranga broker: addresses, queues and routing, and the Core
//! protocol server that drives them.
//!
//! With persistence enabled, durable state goes to the Artemis journals
//! ([`storage`]) and is recovered on start ([`recovery`]). The structure
//! follows ADR 0001: one task per connection and per session, one actor
//! per queue.

pub mod config;
pub mod core_protocol;
pub mod duplicate;
pub mod error;
pub mod message;
pub mod node;
pub mod postoffice;
pub mod queue;
pub mod recovery;
pub mod settings;
pub mod storage;
pub mod transaction;
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

pub struct BrokerConfig {
    pub settings: Arc<dyn SettingsRepository>,
    pub filters: Arc<dyn FilterFactory>,
    pub wildcard: WildcardConfig,
    /// `connection-ttl-override`; -1 lets clients choose.
    pub connection_ttl_override: i64,
    pub addresses: Vec<AddressDeployment>,
    pub diverts: Vec<postoffice::DivertDeployment>,
    /// The journals; `None` keeps everything in memory
    /// (`persistence-enabled` false).
    pub storage: Option<storage::StorageConfig>,
    /// `message-expiry-scan-period` in milliseconds; `-1` for none.
    pub message_expiry_scan_period: i64,
    /// `id-cache-size`.
    pub id_cache_size: i32,
    /// `persist-id-cache`.
    pub persist_id_cache: bool,
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
            storage: None,
            message_expiry_scan_period: 30000,
            id_cache_size: 20000,
            persist_id_cache: true,
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
    node_lock: Mutex<Option<node::NodeLock>>,
}

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
        let post_office = PostOffice::new(
            config.settings,
            config.filters,
            ids,
            config.wildcard,
            duplicate::DuplicateIdCaches::new(
                config.id_cache_size,
                storage.clone().filter(|_| config.persist_id_cache),
            ),
        );
        if let Some(s) = &storage {
            recovery::load_bindings(s, &post_office)?;
        }
        for address in &config.addresses {
            post_office.add_or_update_address(
                &address.name,
                address.routing_types,
                false,
                false,
            )?;
            for queue in &address.queues {
                post_office.create_queue(queue.clone(), true)?;
            }
        }
        for d in &config.diverts {
            if let Err(e) = post_office.deploy_divert(d) {
                tracing::warn!("divert {}: {e}", d.name);
            }
        }
        let resource_manager = transaction::ResourceManager::default();
        if let Some(s) = &storage {
            recovery::load_messages(s, &post_office, &resource_manager).await?;
        }
        if config.message_expiry_scan_period > 0 {
            spawn_expiry_reaper(&post_office, config.message_expiry_scan_period);
        }
        let node_id = match &node_lock {
            Some(l) => l.node_id().to_owned(),
            None => uuid::Uuid::new_v4().to_string(),
        };
        Ok(Arc::new(Self {
            node_id,
            post_office,
            connection_ttl_override: config.connection_ttl_override,
            next_consumer_id: AtomicU64::new(1),
            session_metadata: Mutex::default(),
            resource_manager,
            storage,
            node_lock: Mutex::new(node_lock),
        }))
    }

    /// A clean stop: the ID generator's checkpoint and the journals are
    /// written out, and `server.lock` marked paused.
    pub fn stop(&self) -> Result<()> {
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

    pub fn post_office(&self) -> &Arc<PostOffice> {
        &self.post_office
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
        true
    }

    pub fn remove_session_metadata(&self, session: &str) {
        self.metadata().remove(session);
    }

    /// Accepts Core connections until the listener fails.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (stream, peer) = listener.accept().await?;
            tracing::debug!(%peer, "connection accepted");
            tokio::spawn(core_protocol::serve_connection(self.clone(), stream));
        }
    }
}
