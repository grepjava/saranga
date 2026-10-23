//! `ConfigurationImpl` and the configuration beans it holds, as
//! `FileConfigurationParser` fills them from `broker.xml` (see [`parse`]),
//! with the getters `ConfigurationImpl.exportAsProperties` reads (see
//! [`export`]): what `artemis properties` writes.
//!
//! The broker itself runs on [`crate::BrokerConfig`]; this model keeps
//! everything Artemis's does, including what Saranga does not act on, in the
//! shape of the Java beans: a field Java leaves `null` is `None`, a Java
//! `HashMap` or `HashSet` is kept in insertion order and walked in Java's
//! order where it is written out.

mod apply;
mod export;
mod parse;

pub use apply::apply_property;
pub(crate) use apply::scalar_element;
pub use export::{ExportContext, ExportError, export_as_properties};
pub use parse::parse_main_config;

use crate::acceptor::UriTransport;
use crate::address_settings::AddressSettings;
use crate::{AmqpBrokerConnectConfig, FederationConfig, TransformerConfig};

/// A value of a `Map<String, Object>` of parameters, as Java holds it.
#[derive(Debug, Clone, PartialEq)]
pub enum JavaValue {
    Null,
    Str(String),
    Bool(bool),
}

impl JavaValue {
    /// `String.valueOf`, `null` given as Java's export gives it: empty.
    pub fn export_text(&self) -> String {
        match self {
            JavaValue::Null => String::new(),
            JavaValue::Str(s) => s.clone(),
            JavaValue::Bool(b) => b.to_string(),
        }
    }

    /// `Object.hashCode`.
    pub fn hash_code(&self) -> i32 {
        match self {
            JavaValue::Null => 0,
            JavaValue::Str(s) => saranga_buffer::java_hash::string_hash(s),
            JavaValue::Bool(b) => bool_hash(*b),
        }
    }
}

/// `Boolean.hashCode`.
pub(crate) fn bool_hash(b: bool) -> i32 {
    if b { 1231 } else { 1237 }
}

/// A Java `HashMap<String, V>`: entries in insertion order; `put` on a key
/// already there replaces its value in place.
#[derive(Debug, Clone, PartialEq)]
pub struct JavaMap<V> {
    pub entries: Vec<(String, V)>,
}

impl<V> Default for JavaMap<V> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<V> JavaMap<V> {
    pub fn put(&mut self, key: impl Into<String>, value: V) {
        let key = key.into();
        match self.entries.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => self.entries.push((key, value)),
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.iter().any(|(k, _)| k == key)
    }

    pub fn get(&self, key: &str) -> Option<&V> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut V> {
        self.entries
            .iter_mut()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    pub fn remove(&mut self, key: &str) -> Option<V> {
        let i = self.entries.iter().position(|(k, _)| k == key)?;
        Some(self.entries.remove(i).1)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The entries in the order a `new HashMap<>()` iterates them.
    pub fn java_order(&self) -> Vec<&(String, V)> {
        let hashes: Vec<i32> = self
            .entries
            .iter()
            .map(|(k, _)| saranga_buffer::java_hash::string_hash(k))
            .collect();
        saranga_buffer::java_hash::hash_map_order(&hashes, None)
            .into_iter()
            .map(|i| &self.entries[i])
            .collect()
    }
}

/// `TransportConfiguration`: an acceptor or connector, its parameters split
/// into `params` and `extraProps` as the URI schema splits them.
#[derive(Debug, Clone, PartialEq)]
pub struct TransportConfiguration {
    pub name: Option<String>,
    pub factory_class_name: String,
    pub lock_coordinator: Option<String>,
    pub params: JavaMap<JavaValue>,
    /// `null` when the schema gave none.
    pub extra_props: Option<JavaMap<JavaValue>>,
    /// The [`hash_code`](Self::hash_code) it had when it went into the
    /// acceptors' `HashSet`, which places it by that hash however it
    /// changes after: set once broker properties change it, `None` while
    /// it is the hash it has.
    pub set_hash: Option<i32>,
}

impl TransportConfiguration {
    /// `Objects.hash(name, factoryClassName, params)`, `params` hashed as
    /// `AbstractMap.hashCode` hashes it.
    pub fn hash_code(&self) -> i32 {
        let params = self.params.entries.iter().fold(0i32, |h, (k, v)| {
            h.wrapping_add(saranga_buffer::java_hash::string_hash(k) ^ v.hash_code())
        });
        [
            self.name
                .as_deref()
                .map_or(0, saranga_buffer::java_hash::string_hash),
            saranga_buffer::java_hash::string_hash(&self.factory_class_name),
            params,
        ]
        .into_iter()
        .fold(1i32, |h, x| h.wrapping_mul(31).wrapping_add(x))
    }

    /// A `TransportConfiguration` a URI schema made.
    pub(crate) fn from_uri(t: UriTransport) -> Self {
        let map = |v: Vec<(String, Option<String>)>| JavaMap {
            entries: v
                .into_iter()
                .map(|(k, v)| (k, v.map_or(JavaValue::Null, JavaValue::Str)))
                .collect(),
        };
        Self {
            name: t.name,
            factory_class_name: t.factory_class_name,
            lock_coordinator: None,
            params: map(t.params),
            extra_props: Some(map(t.extra)),
            set_hash: None,
        }
    }
}

/// `QueueConfiguration`, as the parser and `CoreAddressConfiguration` set
/// it: `None` is a field left `null`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueueConfiguration {
    pub name: String,
    pub address: Option<String>,
    pub routing_type: Option<String>,
    pub filter_string: Option<String>,
    pub durable: Option<bool>,
    pub user: Option<String>,
    pub max_consumers: Option<i32>,
    pub exclusive: Option<bool>,
    pub group_rebalance: Option<bool>,
    pub group_rebalance_pause_dispatch: Option<bool>,
    pub group_buckets: Option<i32>,
    pub group_first_key: Option<String>,
    pub last_value: Option<bool>,
    pub last_value_key: Option<String>,
    pub non_destructive: Option<bool>,
    pub purge_on_no_consumers: Option<bool>,
    pub enabled: Option<bool>,
    pub consumers_before_dispatch: Option<i32>,
    pub delay_before_dispatch: Option<i64>,
    pub ring_size: Option<i64>,
}

/// `CoreAddressConfiguration`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CoreAddressConfiguration {
    pub name: Option<String>,
    /// An `EnumSet<RoutingType>`: `MULTICAST` before `ANYCAST`.
    pub multicast: bool,
    pub anycast: bool,
    pub queue_configs: Vec<QueueConfiguration>,
}

/// `Role`: a role's permissions on a match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    pub name: String,
    pub send: bool,
    pub consume: bool,
    pub create_durable_queue: bool,
    pub delete_durable_queue: bool,
    pub create_non_durable_queue: bool,
    pub delete_non_durable_queue: bool,
    pub manage: bool,
    pub browse: bool,
    pub create_address: bool,
    pub delete_address: bool,
    pub view: bool,
    pub edit: bool,
}

impl Role {
    /// `new Role()` named `name`: no permission.
    pub fn named(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            send: false,
            consume: false,
            create_durable_queue: false,
            delete_durable_queue: false,
            create_non_durable_queue: false,
            delete_non_durable_queue: false,
            manage: false,
            browse: false,
            create_address: false,
            delete_address: false,
            view: false,
            edit: false,
        }
    }

    /// `Objects.hash(name, send, consume, createDurableQueue,
    /// deleteDurableQueue, createNonDurableQueue, deleteNonDurableQueue,
    /// manage, browse, createAddress, deleteAddress, view, edit)`.
    pub fn hash_code(&self) -> i32 {
        let flags = [
            self.send,
            self.consume,
            self.create_durable_queue,
            self.delete_durable_queue,
            self.create_non_durable_queue,
            self.delete_non_durable_queue,
            self.manage,
            self.browse,
            self.create_address,
            self.delete_address,
            self.view,
            self.edit,
        ];
        flags.into_iter().fold(
            31i32.wrapping_add(saranga_buffer::java_hash::string_hash(&self.name)),
            |h, b| h.wrapping_mul(31).wrapping_add(bool_hash(b)),
        )
    }
}

/// `ResourceLimitSettings`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ResourceLimitSettings {
    pub r#match: Option<String>,
    pub max_sessions: Option<i32>,
    pub max_queues: Option<i32>,
}

/// `BroadcastEndpointFactory`: UDP or a JGroups file.
#[derive(Debug, Clone, PartialEq)]
pub enum BroadcastEndpoint {
    Udp {
        group_address: Option<String>,
        group_port: i32,
        local_bind_address: Option<String>,
        local_bind_port: i32,
    },
    JGroupsFile {
        file: String,
        channel_name: Option<String>,
    },
}

/// `BroadcastGroupConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct BroadcastGroupConfiguration {
    pub name: String,
    pub broadcast_period: i64,
    pub connector_infos: Vec<String>,
    pub endpoint_factory: BroadcastEndpoint,
}

/// `DiscoveryGroupConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveryGroupConfiguration {
    pub name: String,
    pub refresh_timeout: i64,
    pub discovery_initial_wait_timeout: i64,
    pub stopping_timeout: i64,
    pub broadcast_endpoint_factory: BroadcastEndpoint,
}

/// `BridgeConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct BridgeConfiguration {
    pub name: String,
    pub queue_name: String,
    pub forwarding_address: Option<String>,
    pub filter_string: Option<String>,
    pub static_connectors: Option<Vec<String>>,
    pub discovery_group_name: Option<String>,
    pub ha: bool,
    pub transformer_configuration: Option<TransformerConfig>,
    pub retry_interval: i64,
    pub retry_interval_multiplier: f64,
    pub initial_connect_attempts: i32,
    pub reconnect_attempts: i32,
    pub reconnect_attempts_on_same_node: i32,
    pub use_duplicate_detection: bool,
    pub confirmation_window_size: i32,
    pub producer_window_size: i32,
    pub client_failure_check_period: i64,
    pub user: Option<String>,
    pub password: Option<String>,
    pub connection_ttl: i64,
    pub max_retry_interval: i64,
    pub min_large_message_size: i32,
    pub call_timeout: i64,
    pub routing_type: String,
    pub concurrency: i32,
    pub pending_ack_timeout: i64,
    pub parent_name: Option<String>,
    pub configuration_managed: bool,
    pub client_id: Option<String>,
}

impl BridgeConfiguration {
    /// A `new BridgeConfiguration()` given its name, which `setName` makes
    /// its parent's too: Artemis's defaults.
    pub fn new(name: String) -> Self {
        Self {
            parent_name: Some(name.clone()),
            name,
            queue_name: String::new(),
            forwarding_address: None,
            filter_string: None,
            static_connectors: None,
            discovery_group_name: None,
            ha: false,
            transformer_configuration: None,
            retry_interval: 2000,
            retry_interval_multiplier: 1.0,
            initial_connect_attempts: -1,
            reconnect_attempts: -1,
            reconnect_attempts_on_same_node: 10,
            use_duplicate_detection: true,
            confirmation_window_size: 1024 * 1024 * 10,
            producer_window_size: 1024 * 1024,
            client_failure_check_period: 30000,
            user: Some("ACTIVEMQ.CLUSTER.ADMIN.USER".to_owned()),
            password: Some("CHANGE ME!!".to_owned()),
            connection_ttl: 60000,
            max_retry_interval: 2000,
            min_large_message_size: 100 * 1024,
            call_timeout: 30000,
            routing_type: "PASS".to_owned(),
            concurrency: 1,
            pending_ack_timeout: 60000,
            configuration_managed: true,
            client_id: None,
        }
    }
}

/// `JaasAppConfiguration`: a JAAS realm broker properties configure.
#[derive(Debug, Clone, PartialEq)]
pub struct JaasAppConfiguration {
    pub name: String,
    pub modules: Vec<JaasAppConfigurationEntry>,
}

/// `JaasAppConfigurationEntry`: one login module of a realm.
#[derive(Debug, Clone, PartialEq)]
pub struct JaasAppConfigurationEntry {
    pub name: String,
    pub login_module_class: Option<String>,
    pub control_flag: Option<String>,
    pub params: JavaMap<String>,
}

/// `ClusterConnectionConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterConnectionConfiguration {
    pub name: String,
    pub address: Option<String>,
    pub connector_name: Option<String>,
    pub client_failure_check_period: i64,
    pub connection_ttl: i64,
    pub retry_interval: i64,
    pub retry_interval_multiplier: f64,
    pub max_retry_interval: i64,
    pub initial_connect_attempts: i32,
    pub reconnect_attempts: i32,
    pub call_timeout: i64,
    pub call_failover_timeout: i64,
    pub duplicate_detection: bool,
    pub message_load_balancing_type: String,
    /// `URISupport.CompositeData.parameters` of a `<cluster-connection-uri>`.
    pub composite_members: Option<JavaMap<String>>,
    pub static_connectors: Vec<String>,
    pub discovery_group_name: Option<String>,
    pub max_hops: i32,
    pub confirmation_window_size: i32,
    pub producer_window_size: i32,
    pub allow_direct_connections_only: bool,
    pub min_large_message_size: i32,
    pub cluster_notification_interval: i64,
    pub cluster_notification_attempts: i32,
    pub client_id: Option<String>,
    pub topology_scanner_attempts: i32,
}

impl ClusterConnectionConfiguration {
    /// A `new ClusterConnectionConfiguration()`: Artemis's defaults.
    pub fn new(name: String) -> Self {
        Self {
            name,
            address: Some(String::new()),
            connector_name: None,
            client_failure_check_period: 30000,
            connection_ttl: 60000,
            retry_interval: 500,
            retry_interval_multiplier: 1.0,
            max_retry_interval: 2000,
            initial_connect_attempts: -1,
            reconnect_attempts: -1,
            call_timeout: 30000,
            call_failover_timeout: -1,
            duplicate_detection: true,
            message_load_balancing_type: "ON_DEMAND".to_owned(),
            composite_members: None,
            static_connectors: Vec::new(),
            discovery_group_name: None,
            max_hops: 1,
            confirmation_window_size: 1024 * 1024 * 10,
            producer_window_size: 1024 * 1024,
            allow_direct_connections_only: false,
            min_large_message_size: 100 * 1024,
            cluster_notification_interval: 1000,
            cluster_notification_attempts: 2,
            client_id: None,
            topology_scanner_attempts: 30,
        }
    }
}

/// `DivertConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct DivertConfiguration {
    pub name: String,
    /// `None` only when the XML gave no `routing-name`: Java's field then
    /// holds a random UUID, which [`export`] makes up the same way.
    pub routing_name: Option<String>,
    pub address: String,
    pub forwarding_address: String,
    pub exclusive: bool,
    pub filter_string: Option<String>,
    pub transformer_configuration: Option<TransformerConfig>,
    pub routing_type: String,
}

/// `GroupingHandlerConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupingHandlerConfiguration {
    pub name: String,
    pub address: String,
    pub timeout: i64,
    pub group_timeout: i64,
    pub reaper_period: i64,
}

/// `ConnectorServiceConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectorServiceConfiguration {
    pub name: Option<String>,
    pub factory_class_name: String,
    pub params: JavaMap<String>,
}

/// `ConnectionRouterConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionRouterConfiguration {
    pub name: String,
    pub key_type: String,
    pub key_filter: Option<String>,
    pub local_target_filter: Option<String>,
    pub cache_configuration: Option<CacheConfiguration>,
    pub pool_configuration: Option<PoolConfiguration>,
    pub policy_configuration: Option<NamedPropertyConfiguration>,
}

/// `CacheConfiguration`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CacheConfiguration {
    pub persisted: bool,
    pub timeout: i32,
}

/// `NamedPropertyConfiguration`: a policy's name and properties.
#[derive(Debug, Clone, PartialEq)]
pub struct NamedPropertyConfiguration {
    pub name: String,
    pub properties: JavaMap<String>,
}

/// `PoolConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolConfiguration {
    pub username: Option<String>,
    pub password: Option<String>,
    pub local_target_enabled: bool,
    pub cluster_connection: Option<String>,
    pub static_connectors: Option<Vec<String>>,
    pub discovery_group_name: Option<String>,
    pub check_period: i32,
    pub quorum_size: i32,
    pub quorum_timeout: i32,
}

impl Default for PoolConfiguration {
    fn default() -> Self {
        Self {
            username: None,
            password: None,
            local_target_enabled: false,
            cluster_connection: None,
            static_connectors: None,
            discovery_group_name: None,
            check_period: 5000,
            quorum_size: 1,
            quorum_timeout: 3000,
        }
    }
}

/// `LockCoordinatorConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct LockCoordinatorConfiguration {
    pub name: String,
    pub lock_id: String,
    pub class_name: String,
    pub check_period: i32,
    pub auto_start: bool,
    pub properties: JavaMap<String>,
}

/// `MetricsConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricsConfiguration {
    pub jvm_memory: bool,
    pub jvm_gc: bool,
    pub jvm_thread: bool,
    pub netty_pool: bool,
    pub file_descriptors: bool,
    pub processor: bool,
    pub uptime: bool,
    pub logging: bool,
    pub security_caches: bool,
    pub executor_services: bool,
    pub plugin: Option<MetricsPlugin>,
}

impl Default for MetricsConfiguration {
    fn default() -> Self {
        Self {
            jvm_memory: true,
            jvm_gc: false,
            jvm_thread: false,
            netty_pool: false,
            file_descriptors: false,
            processor: false,
            uptime: false,
            logging: false,
            security_caches: false,
            executor_services: false,
            plugin: None,
        }
    }
}

/// An `ActiveMQMetricsPlugin`: its class and the properties `init` got.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricsPlugin {
    pub class_name: String,
    pub properties: JavaMap<String>,
}

/// A broker plugin: its class and the properties `init` got.
#[derive(Debug, Clone, PartialEq)]
pub struct BrokerPlugin {
    pub class_name: String,
    pub properties: JavaMap<String>,
}

/// A `SecuritySettingPlugin`: its class and settings.
#[derive(Debug, Clone, PartialEq)]
pub struct SecuritySettingPlugin {
    pub class_name: String,
    pub settings: JavaMap<Option<String>>,
}

/// `ScaleDownConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct ScaleDownConfiguration {
    pub connectors: Vec<String>,
    pub discovery_group: Option<String>,
    pub group_name: Option<String>,
    pub cluster_name: Option<String>,
    pub enabled: bool,
    pub commit_interval: i32,
}

impl Default for ScaleDownConfiguration {
    fn default() -> Self {
        Self {
            connectors: Vec::new(),
            discovery_group: None,
            group_name: None,
            cluster_name: None,
            enabled: true,
            commit_interval: -1,
        }
    }
}

/// `DistributedLockManagerConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct DistributedLockManagerConfiguration {
    pub class_name: String,
    pub properties: JavaMap<String>,
}

/// `ReplicatedPolicyConfiguration`: a replicating primary with quorum
/// voting.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicatedPolicy {
    pub check_for_active_server: bool,
    pub group_name: Option<String>,
    pub cluster_name: Option<String>,
    pub max_saved_replicated_journals_size: i32,
    pub initial_replication_sync_timeout: i64,
    pub vote_on_replication_failure: bool,
    pub quorum_size: i32,
    pub vote_retries: i32,
    pub vote_retry_wait: i64,
    pub quorum_vote_wait: i32,
    pub retry_replication_wait: Option<i64>,
}

impl Default for ReplicatedPolicy {
    fn default() -> Self {
        Self {
            check_for_active_server: false,
            group_name: None,
            cluster_name: None,
            max_saved_replicated_journals_size: 2,
            initial_replication_sync_timeout: 30000,
            vote_on_replication_failure: false,
            quorum_size: -1,
            vote_retries: 12,
            vote_retry_wait: 5000,
            quorum_vote_wait: 30,
            retry_replication_wait: Some(2000),
        }
    }
}

/// `ReplicaPolicyConfiguration`: a replicating backup with quorum voting.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaPolicy {
    pub cluster_name: Option<String>,
    pub max_saved_replicated_journals_size: i32,
    pub group_name: Option<String>,
    pub restart_backup: bool,
    pub scale_down_configuration: Option<ScaleDownConfiguration>,
    pub allow_fail_back: bool,
    pub initial_replication_sync_timeout: i64,
    pub vote_on_replication_failure: bool,
    pub quorum_size: i32,
    pub vote_retries: i32,
    pub vote_retry_wait: i64,
    pub quorum_vote_wait: i32,
    pub retry_replication_wait: i64,
}

impl Default for ReplicaPolicy {
    fn default() -> Self {
        Self {
            cluster_name: None,
            max_saved_replicated_journals_size: 2,
            group_name: None,
            restart_backup: true,
            scale_down_configuration: None,
            allow_fail_back: false,
            initial_replication_sync_timeout: 30000,
            vote_on_replication_failure: false,
            quorum_size: -1,
            vote_retries: 12,
            vote_retry_wait: 5000,
            quorum_vote_wait: 30,
            retry_replication_wait: 2000,
        }
    }
}

/// `ReplicationPrimaryPolicyConfiguration`: a replicating primary with a
/// lock manager.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicationPrimaryPolicy {
    pub group_name: Option<String>,
    pub cluster_name: Option<String>,
    pub initial_replication_sync_timeout: i64,
    pub retry_replication_wait: Option<i64>,
    pub distributed_manager_configuration: Option<DistributedLockManagerConfiguration>,
    pub coordination_id: Option<String>,
    pub max_saved_replicated_journals_size: i32,
}

/// `ReplicationBackupPolicyConfiguration`: a replicating backup with a lock
/// manager.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicationBackupPolicy {
    pub cluster_name: Option<String>,
    pub max_saved_replicated_journals_size: i32,
    pub group_name: Option<String>,
    pub allow_fail_back: bool,
    pub initial_replication_sync_timeout: i64,
    pub retry_replication_wait: i64,
    pub distributed_manager_configuration: Option<DistributedLockManagerConfiguration>,
}

/// `SharedStorePrimaryPolicyConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct SharedStorePrimaryPolicy {
    pub failover_on_server_shutdown: bool,
    pub wait_for_activation: bool,
}

/// `SharedStoreBackupPolicyConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct SharedStoreBackupPolicy {
    pub failover_on_server_shutdown: bool,
    pub restart_backup: bool,
    pub allow_fail_back: bool,
    pub scale_down_configuration: Option<ScaleDownConfiguration>,
}

/// `ColocatedPolicyConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColocatedPolicy {
    pub request_backup: bool,
    pub backup_request_retries: i32,
    pub backup_request_retry_interval: i64,
    pub max_backups: i32,
    pub backup_port_offset: i32,
    pub excluded_connectors: Vec<String>,
    pub port_offset: i32,
    pub primary_config: Option<Box<HaPolicyConfiguration>>,
    pub backup_config: Option<Box<HaPolicyConfiguration>>,
}

/// A `HAPolicyConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub enum HaPolicyConfiguration {
    PrimaryOnly {
        scale_down_configuration: Option<ScaleDownConfiguration>,
    },
    Replicated(ReplicatedPolicy),
    Replica(ReplicaPolicy),
    ReplicationPrimary(ReplicationPrimaryPolicy),
    ReplicationBackup(ReplicationBackupPolicy),
    SharedStorePrimary(SharedStorePrimaryPolicy),
    SharedStoreBackup(SharedStoreBackupPolicy),
    Colocated(ColocatedPolicy),
}

impl HaPolicyConfiguration {
    /// `getType().name()`.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::PrimaryOnly { .. } => "PRIMARY_ONLY",
            Self::Replicated(_) => "REPLICATION_PRIMARY_QUORUM_VOTING",
            Self::Replica(_) => "REPLICATION_BACKUP_QUORUM_VOTING",
            Self::ReplicationPrimary(_) => "REPLICATION_PRIMARY_LOCK_MANAGER",
            Self::ReplicationBackup(_) => "REPLICATION_BACKUP_LOCK_MANAGER",
            Self::SharedStorePrimary(_) => "SHARED_STORE_PRIMARY",
            Self::SharedStoreBackup(_) => "SHARED_STORE_BACKUP",
            Self::Colocated(_) => "COLOCATED",
        }
    }
}

/// `DatabaseStorageConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct DatabaseStorage {
    pub message_table_name: String,
    pub bindings_table_name: String,
    pub large_messages_table_name: String,
    pub page_store_table_name: String,
    pub node_manager_store_table_name: String,
    pub jdbc_connection_url: Option<String>,
    pub jdbc_user: Option<String>,
    pub jdbc_password: Option<String>,
    pub jdbc_driver_class_name: String,
    pub data_source_class_name: String,
    pub data_source_properties: JavaMap<String>,
    pub jdbc_network_timeout: i32,
    pub jdbc_lock_renew_period_millis: i64,
    pub jdbc_lock_expiration_millis: i64,
    pub jdbc_lock_acquisition_timeout_millis: i64,
    pub jdbc_journal_sync_period_millis: i64,
    pub jdbc_allowed_time_diff: i64,
    pub max_page_size_bytes: i32,
}

impl Default for DatabaseStorage {
    fn default() -> Self {
        Self {
            message_table_name: "MESSAGES".to_owned(),
            bindings_table_name: "BINDINGS".to_owned(),
            large_messages_table_name: "LARGE_MESSAGES".to_owned(),
            page_store_table_name: "PAGE_STORE".to_owned(),
            node_manager_store_table_name: "NODE_MANAGER_STORE".to_owned(),
            jdbc_connection_url: None,
            jdbc_user: None,
            jdbc_password: None,
            jdbc_driver_class_name: "org.apache.derby.jdbc.EmbeddedDriver".to_owned(),
            data_source_class_name: "org.apache.commons.dbcp2.BasicDataSource".to_owned(),
            data_source_properties: JavaMap::default(),
            jdbc_network_timeout: 20000,
            jdbc_lock_renew_period_millis: 4000,
            jdbc_lock_expiration_millis: 20000,
            jdbc_lock_acquisition_timeout_millis: -1,
            jdbc_journal_sync_period_millis: 5,
            jdbc_allowed_time_diff: 250,
            max_page_size_bytes: 100 * 1024,
        }
    }
}

/// A `StoreConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub enum StoreConfiguration {
    File,
    Database(Box<DatabaseStorage>),
}

/// `ConfigurationImpl`, its fields as `FileConfigurationParser` leaves them.
#[derive(Debug, Clone, PartialEq)]
pub struct Configuration {
    pub name: String,
    pub persistence_enabled: bool,
    pub max_redelivery_records: i32,
    pub journal_datasync: bool,
    pub file_deployment_scan_period: i64,
    pub persist_delivery_count_before_delivery: bool,
    pub scheduled_thread_pool_max_size: i32,
    pub thread_pool_max_size: i32,
    pub security_invalidation_interval: i64,
    pub authentication_cache_size: i64,
    pub authorization_cache_size: i64,
    pub security_enabled: bool,
    pub graceful_shutdown_enabled: bool,
    pub graceful_shutdown_timeout: i64,
    pub jmx_management_enabled: bool,
    pub jmx_notification_enabled: bool,
    pub jmx_domain: String,
    pub jmx_use_broker_name: bool,
    pub connection_ttl_override: i64,
    pub async_connection_execution_enabled: bool,
    pub message_expiry_scan_period: i64,
    pub message_expiry_thread_priority: i32,
    pub address_queue_scan_period: i64,
    pub id_cache_size: i32,
    pub persist_id_cache: bool,
    /// A `HashSet` of objects without `hashCode`: Java walks it in no fixed
    /// order; this keeps document order.
    pub lock_coordinator_configurations: Vec<LockCoordinatorConfiguration>,
    pub incoming_interceptor_class_names: Vec<String>,
    pub outgoing_interceptor_class_names: Vec<String>,
    pub connector_configs: JavaMap<TransportConfiguration>,
    /// A `HashSet`, walked by [`TransportConfiguration::hash_code`].
    pub acceptor_configs: Vec<TransportConfiguration>,
    pub bridge_configurations: Vec<BridgeConfiguration>,
    pub divert_configurations: Vec<DivertConfiguration>,
    pub connection_routers: Vec<ConnectionRouterConfiguration>,
    pub cluster_configurations: Vec<ClusterConnectionConfiguration>,
    pub amqp_broker_connect_configurations: Vec<AmqpBrokerConnectConfig>,
    /// Each AMQP connection's `getTransportConfigurations()`, as
    /// `parseURI` made them.
    pub amqp_transport_configurations: Vec<Vec<TransportConfiguration>>,
    pub federation_configurations: Vec<FederationConfig>,
    pub downstream_authorization: Vec<String>,
    pub address_configurations: Vec<CoreAddressConfiguration>,
    pub broadcast_group_configurations: Vec<BroadcastGroupConfiguration>,
    /// A `LinkedHashMap`: insertion order.
    pub discovery_group_configurations: Vec<DiscoveryGroupConfiguration>,
    pub paging_directory: String,
    pub max_concurrent_page_io: i32,
    pub read_whole_page: bool,
    pub large_messages_directory: String,
    pub bindings_directory: String,
    pub create_bindings_dir: bool,
    pub journal_directory: String,
    pub journal_retention_directory: Option<String>,
    pub journal_retention_max_bytes: i64,
    pub journal_retention_period: i64,
    pub node_manager_lock_directory: Option<String>,
    pub create_journal_dir: bool,
    pub journal_type: String,
    pub large_message_sync: bool,
    pub journal_sync_transactional: bool,
    pub journal_sync_non_transactional: bool,
    pub journal_compact_min_files: i32,
    pub journal_compact_percentage: i32,
    pub journal_file_open_timeout: i32,
    pub journal_file_size: i32,
    pub journal_pool_files: i32,
    pub journal_min_files: i32,
    pub journal_max_attic_files: i32,
    pub journal_max_io_aio: i32,
    pub journal_buffer_timeout_aio: i32,
    pub journal_device_block_size: Option<i32>,
    pub journal_buffer_size_aio: i32,
    pub journal_max_io_nio: i32,
    pub journal_buffer_timeout_nio: i32,
    pub journal_buffer_size_nio: i32,
    pub log_journal_write_rate: bool,
    pub wildcard_routing_enabled: bool,
    pub message_counter_enabled: bool,
    pub message_counter_sample_period: i64,
    pub message_counter_max_day_history: i32,
    pub transaction_timeout: i64,
    pub transaction_timeout_scan_period: i64,
    pub management_address: String,
    pub management_notification_address: String,
    pub cluster_user: String,
    pub cluster_password: String,
    pub server_dump_interval: i64,
    pub failover_on_server_shutdown: bool,
    pub memory_warning_threshold: i32,
    pub memory_measure_interval: i64,
    pub grouping_handler_configuration: Option<GroupingHandlerConfiguration>,
    pub address_settings: JavaMap<AddressSettings>,
    pub resource_limit_settings: JavaMap<ResourceLimitSettings>,
    /// Each match's roles, a `HashSet<Role>` kept in insertion order.
    pub security_settings: JavaMap<Vec<Role>>,
    /// The JAAS realms broker properties configure: a `ConcurrentHashMap`,
    /// walked as a `HashMap` of the same keys is.
    pub jaas_configs: JavaMap<JaasAppConfiguration>,
    pub security_setting_plugins: Vec<SecuritySettingPlugin>,
    pub metrics_configuration: Option<MetricsConfiguration>,
    pub broker_plugins: Vec<BrokerPlugin>,
    pub connector_service_configurations: Vec<ConnectorServiceConfiguration>,
    pub mask_password: Option<bool>,
    pub password_codec: Option<String>,
    pub resolve_protocols: bool,
    pub journal_lock_acquisition_timeout: i64,
    pub journal_lock_monitor_timeout: i64,
    pub journal_lock_monitor_max_retries: i32,
    pub ha_policy_configuration: Option<HaPolicyConfiguration>,
    pub store_configuration: Option<StoreConfiguration>,
    pub populate_validated_user: bool,
    pub reject_empty_validated_user: bool,
    pub connection_ttl_check_interval: i64,
    /// The URL `broker.xml` was read from.
    pub configuration_url: Option<String>,
    pub configuration_file_refresh_period: i64,
    pub global_max_size: Option<i64>,
    pub global_max_messages: Option<i64>,
    pub amqp_use_core_subscription_naming: bool,
    pub max_disk_usage: i32,
    pub min_disk_free: i64,
    pub disk_scan_period: i32,
    pub system_property_prefix: String,
    pub broker_properties_key_surround: String,
    pub network_check_list: Option<String>,
    pub network_url_list: Option<String>,
    pub network_check_period: i64,
    pub network_check_timeout: i32,
    pub network_check_nic: Option<String>,
    pub network_check_ping_command: Option<String>,
    pub network_check_ping6_command: Option<String>,
    pub internal_naming_prefix: String,
    pub critical_analyzer: bool,
    pub critical_analyzer_policy: String,
    pub critical_analyzer_timeout: i64,
    pub critical_analyzer_check_period: i64,
    pub page_sync_timeout: i32,
    pub uuid_namespace: String,
    pub mqtt_session_scan_interval: i64,
    pub mqtt_session_state_persistence_timeout: i64,
    pub mqtt_subscription_persistence_enabled: bool,
    pub suppress_session_notifications: bool,
    pub literal_match_markers: Option<String>,
    pub view_permission_method_match_pattern: String,
    pub management_rbac_prefix: String,
    pub management_messages_rbac: bool,
    pub mirror_ack_manager_queue_attempts: i32,
    pub mirror_ack_manager_page_attempts: i32,
    pub mirror_ack_manager_warn_unacked: bool,
    pub mirror_ack_manager_retry_delay: i32,
    pub mirror_page_transaction: bool,
    pub global_max_size_percent_of_jvm_max_memory: i32,
    pub purge_page_folders: bool,
    /// `artemisInstance`: the instance directory as `new File` holds it.
    pub broker_instance: Option<String>,
}

impl Configuration {
    /// `ActiveMQServerImpl.updateReloadableConfigurationFrom`: what a
    /// reload replaces in the configuration the broker runs on, from the
    /// configuration it read again. The rest stays as the broker started.
    pub fn update_reloadable_from(&mut self, other: Configuration) {
        self.security_settings = other.security_settings;
        self.address_settings = other.address_settings;
        self.divert_configurations = other.divert_configurations;
        self.address_configurations = other.address_configurations;
        self.bridge_configurations = other.bridge_configurations;
        self.connector_configs = other.connector_configs;
        self.acceptor_configs = other.acceptor_configs;
        self.amqp_broker_connect_configurations = other.amqp_broker_connect_configurations;
        self.amqp_transport_configurations = other.amqp_transport_configurations;
        self.purge_page_folders = other.purge_page_folders;
        self.connection_routers = other.connection_routers;
        self.jaas_configs = other.jaas_configs;
    }
}

/// `NetworkHealthCheck.IPV4_DEFAULT_COMMAND` and `IPV6_DEFAULT_COMMAND`, for
/// the platform: Windows or not.
pub fn default_ping_commands() -> (&'static str, &'static str) {
    if cfg!(windows) {
        (
            "cmd /C ping -n 1 -w %d000 %s | findstr /i TTL",
            "ping -n 1 -w %d000 %s",
        )
    } else if cfg!(target_os = "macos") {
        ("ping -c 1 -t %d %s", "ping6 -c 1 %2$s")
    } else {
        ("ping -c 1 -w %d %s", "ping6 -c 1 %2$s")
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            name: "localhost".to_owned(),
            persistence_enabled: true,
            max_redelivery_records: 10,
            journal_datasync: true,
            file_deployment_scan_period: 5000,
            persist_delivery_count_before_delivery: false,
            scheduled_thread_pool_max_size: 5,
            thread_pool_max_size: 30,
            security_invalidation_interval: 10000,
            authentication_cache_size: 1000,
            authorization_cache_size: 1000,
            security_enabled: true,
            graceful_shutdown_enabled: false,
            graceful_shutdown_timeout: -1,
            jmx_management_enabled: true,
            jmx_notification_enabled: true,
            jmx_domain: "org.apache.activemq.artemis".to_owned(),
            jmx_use_broker_name: true,
            connection_ttl_override: -1,
            async_connection_execution_enabled: true,
            message_expiry_scan_period: 30000,
            message_expiry_thread_priority: 3,
            address_queue_scan_period: 30000,
            id_cache_size: 20000,
            persist_id_cache: true,
            lock_coordinator_configurations: Vec::new(),
            incoming_interceptor_class_names: Vec::new(),
            outgoing_interceptor_class_names: Vec::new(),
            connector_configs: JavaMap::default(),
            acceptor_configs: Vec::new(),
            bridge_configurations: Vec::new(),
            divert_configurations: Vec::new(),
            connection_routers: Vec::new(),
            cluster_configurations: Vec::new(),
            amqp_broker_connect_configurations: Vec::new(),
            amqp_transport_configurations: Vec::new(),
            federation_configurations: Vec::new(),
            downstream_authorization: Vec::new(),
            address_configurations: Vec::new(),
            broadcast_group_configurations: Vec::new(),
            discovery_group_configurations: Vec::new(),
            paging_directory: "data/paging".to_owned(),
            max_concurrent_page_io: 5,
            read_whole_page: false,
            large_messages_directory: "data/largemessages".to_owned(),
            bindings_directory: "data/bindings".to_owned(),
            create_bindings_dir: true,
            journal_directory: "data/journal".to_owned(),
            journal_retention_directory: None,
            journal_retention_max_bytes: 0,
            journal_retention_period: 0,
            node_manager_lock_directory: None,
            create_journal_dir: true,
            journal_type: "ASYNCIO".to_owned(),
            large_message_sync: true,
            journal_sync_transactional: true,
            journal_sync_non_transactional: true,
            journal_compact_min_files: 10,
            journal_compact_percentage: 30,
            journal_file_open_timeout: 5,
            journal_file_size: 10485760,
            journal_pool_files: -1,
            journal_min_files: 2,
            journal_max_attic_files: 10,
            journal_max_io_aio: 4096,
            journal_buffer_timeout_aio: 500_000,
            journal_device_block_size: None,
            journal_buffer_size_aio: 490 * 1024,
            journal_max_io_nio: 1,
            journal_buffer_timeout_nio: 3_333_333,
            journal_buffer_size_nio: 490 * 1024,
            log_journal_write_rate: false,
            wildcard_routing_enabled: true,
            message_counter_enabled: false,
            message_counter_sample_period: 10000,
            message_counter_max_day_history: 10,
            transaction_timeout: 300000,
            transaction_timeout_scan_period: 1000,
            management_address: "activemq.management".to_owned(),
            management_notification_address: "activemq.notifications".to_owned(),
            cluster_user: "ACTIVEMQ.CLUSTER.ADMIN.USER".to_owned(),
            cluster_password: "CHANGE ME!!".to_owned(),
            server_dump_interval: -1,
            failover_on_server_shutdown: false,
            memory_warning_threshold: 25,
            memory_measure_interval: -1,
            grouping_handler_configuration: None,
            address_settings: JavaMap::default(),
            resource_limit_settings: JavaMap::default(),
            security_settings: JavaMap::default(),
            jaas_configs: JavaMap::default(),
            security_setting_plugins: Vec::new(),
            metrics_configuration: None,
            broker_plugins: Vec::new(),
            connector_service_configurations: Vec::new(),
            mask_password: None,
            password_codec: None,
            resolve_protocols: true,
            journal_lock_acquisition_timeout: -1,
            journal_lock_monitor_timeout: 2000,
            journal_lock_monitor_max_retries: 0,
            ha_policy_configuration: None,
            store_configuration: None,
            populate_validated_user: false,
            reject_empty_validated_user: false,
            connection_ttl_check_interval: 2000,
            configuration_url: None,
            configuration_file_refresh_period: 5000,
            global_max_size: None,
            global_max_messages: None,
            amqp_use_core_subscription_naming: false,
            max_disk_usage: 90,
            min_disk_free: -1,
            disk_scan_period: 5000,
            system_property_prefix: "brokerconfig.".to_owned(),
            broker_properties_key_surround: "\"".to_owned(),
            network_check_list: None,
            network_url_list: None,
            network_check_period: 5000,
            network_check_timeout: 1000,
            network_check_nic: None,
            network_check_ping_command: None,
            network_check_ping6_command: None,
            internal_naming_prefix: "$.artemis.internal.".to_owned(),
            critical_analyzer: true,
            critical_analyzer_policy: "LOG".to_owned(),
            critical_analyzer_timeout: 120000,
            critical_analyzer_check_period: 0,
            page_sync_timeout: 3_333_333,
            uuid_namespace: String::new(),
            mqtt_session_scan_interval: 5000,
            mqtt_session_state_persistence_timeout: 5000,
            mqtt_subscription_persistence_enabled: true,
            suppress_session_notifications: false,
            literal_match_markers: None,
            view_permission_method_match_pattern: "^(get|is|count|list|browse|query).*$".to_owned(),
            management_rbac_prefix: "mops".to_owned(),
            management_messages_rbac: false,
            mirror_ack_manager_queue_attempts: 5,
            mirror_ack_manager_page_attempts: 2,
            mirror_ack_manager_warn_unacked: false,
            mirror_ack_manager_retry_delay: 100,
            mirror_page_transaction: false,
            global_max_size_percent_of_jvm_max_memory: 50,
            purge_page_folders: false,
            broker_instance: None,
        }
    }
}

/// `FileDeploymentManager.readConfiguration` for the configuration alone:
/// `broker.xml` read as the broker reads it (properties substituted, well
/// formed, includes expanded, validated), then the first `<core>` below the
/// root parsed into a [`Configuration`], whose `configurationUrl` is `url`.
/// Without a `<core>`, the defaults.
pub fn read_configuration(
    xml: &str,
    lookup: impl Fn(&str) -> Option<String>,
    validation: crate::Validation,
    url: Option<&str>,
) -> crate::Result<Configuration> {
    let text = crate::substitute::replace_system_props(xml, &lookup);
    let text = crate::broker::well_formed(&text, true)?;
    let text = crate::xinclude::expand(&text, None, &lookup)?;
    crate::parser::validate_document(&text, validation)?;
    let options = roxmltree::ParsingOptions {
        allow_dtd: true,
        ..roxmltree::ParsingOptions::default()
    };
    let doc = roxmltree::Document::parse_with_options(&text, options)?;
    let core = doc
        .root_element()
        .descendants()
        .skip(1)
        .find(|n| n.is_element() && n.tag_name().name() == "core");
    match core {
        Some(core) => parse_main_config(core, url),
        None => Ok(Configuration::default()),
    }
}
