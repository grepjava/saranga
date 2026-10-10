//! The Saranga broker: addresses, queues and routing, and the Core
//! protocol server that drives them.
//!
//! Phase 2 keeps everything in memory. The structure follows ADR 0001:
//! one task per connection and per session, one actor per queue.

pub mod config;
pub mod core_protocol;
pub mod error;
pub mod message;
pub mod postoffice;
pub mod queue;
pub mod settings;
pub mod storage;
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

/// Message, queue and address IDs (`StorageManager.generateID`).
#[derive(Debug)]
pub struct IdGenerator(AtomicI64);

impl IdGenerator {
    pub fn new(first: i64) -> Self {
        Self(AtomicI64::new(first))
    }

    pub fn next(&self) -> i64 {
        self.0.fetch_add(1, Ordering::Relaxed)
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
        }
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
}

impl Broker {
    pub fn new(config: BrokerConfig) -> Result<Arc<Self>> {
        let ids = Arc::new(IdGenerator::new(1));
        let post_office = Arc::new(PostOffice::new(
            config.settings,
            config.filters,
            ids,
            config.wildcard,
        ));
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
        Ok(Arc::new(Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            post_office,
            connection_ttl_override: config.connection_ttl_override,
            next_consumer_id: AtomicU64::new(1),
            session_metadata: Mutex::default(),
        }))
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn post_office(&self) -> &Arc<PostOffice> {
        &self.post_office
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
