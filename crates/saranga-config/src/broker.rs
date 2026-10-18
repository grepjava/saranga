//! The parsed broker configuration. Ported from the parts of
//! `ConfigurationImpl`, `CoreAddressConfiguration` and `QueueConfiguration`
//! the broker uses so far; defaults come from `ConfigurationImpl` and
//! `ActiveMQDefaultConfiguration`.

use std::fmt;
use std::path::Path;
use std::str::FromStr;

use crate::acceptor::AcceptorConfig;
use crate::address_settings::AddressSettings;
use crate::repository::{HierarchicalRepository, extract_address_name};
use crate::security::{SecuritySetting, SecuritySettingPluginConfig};
use crate::wildcard::WildcardConfiguration;
use crate::{ConfigError, Result, RoutingType};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JournalType {
    Nio,
    Asyncio,
    Mapped,
}

impl FromStr for JournalType {
    type Err = ConfigError;

    /// `JournalType.getType`: case-sensitive.
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "NIO" => Ok(Self::Nio),
            "ASYNCIO" => Ok(Self::Asyncio),
            "MAPPED" => Ok(Self::Mapped),
            _ => Err(ConfigError::invalid(
                "journal-type",
                s,
                "expected NIO, ASYNCIO or MAPPED",
            )),
        }
    }
}

/// Journal and storage settings. Unset fields mean the Artemis default,
/// listed in the constants on [`JournalConfig`]. Some defaults depend on the
/// journal type (`ASYNCIO` or not), which is why they stay unresolved here.
#[derive(Debug, Clone, PartialEq)]
pub struct JournalConfig {
    /// As configured. Artemis silently falls back from `ASYNCIO` to `NIO`
    /// when libaio is unavailable; that decision belongs to the journal.
    pub journal_type: JournalType,
    pub datasync: Option<bool>,
    pub sync_transactional: Option<bool>,
    pub sync_non_transactional: Option<bool>,
    pub file_size: Option<i32>,
    pub min_files: Option<i32>,
    pub pool_files: Option<i32>,
    pub compact_min_files: Option<i32>,
    pub compact_percentage: Option<i32>,
    /// Nanoseconds.
    pub buffer_timeout: Option<i32>,
    pub buffer_size: Option<i32>,
    pub max_io: Option<i32>,
    pub device_block_size: Option<i32>,
    pub file_open_timeout: Option<i32>,
    pub max_attic_files: Option<i32>,
    pub create_journal_dir: Option<bool>,
    pub create_bindings_dir: Option<bool>,
    pub max_redelivery_records: Option<i32>,
    pub page_sync_timeout: Option<i32>,
    pub purge_page_folders: Option<bool>,
    pub max_disk_usage: Option<i32>,
    /// `min-disk-free`, in bytes; it overrides `max-disk-usage`.
    pub min_disk_free: Option<i64>,
    pub disk_scan_period: Option<i32>,
    /// `<journal-retention-directory>`.
    pub retention: Option<JournalRetention>,
}

/// `<journal-retention-directory>`: where a copy of each journal file is
/// kept, and for how long.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalRetention {
    /// As configured; relative to the instance.
    pub directory: String,
    /// `storage-limit` in bytes, -1 for none.
    pub max_bytes: i64,
    /// `period` in `unit`, in milliseconds; -1 for none.
    pub period_millis: i64,
}

impl JournalConfig {
    pub const DEFAULT_JOURNAL_TYPE: JournalType = JournalType::Asyncio;
    pub const DEFAULT_DATASYNC: bool = true;
    pub const DEFAULT_SYNC_TRANSACTIONAL: bool = true;
    pub const DEFAULT_SYNC_NON_TRANSACTIONAL: bool = true;
    pub const DEFAULT_FILE_SIZE: i32 = 10 * 1024 * 1024;
    pub const DEFAULT_MIN_FILES: i32 = 2;
    pub const DEFAULT_POOL_FILES: i32 = -1;
    pub const DEFAULT_COMPACT_MIN_FILES: i32 = 10;
    pub const DEFAULT_COMPACT_PERCENTAGE: i32 = 30;
    pub const DEFAULT_BUFFER_TIMEOUT_AIO: i32 = 500_000;
    pub const DEFAULT_BUFFER_TIMEOUT_NIO: i32 = 3_333_333;
    pub const DEFAULT_BUFFER_SIZE: i32 = 490 * 1024;
    pub const DEFAULT_MAX_IO_AIO: i32 = 4096;
    pub const DEFAULT_MAX_IO_NIO: i32 = 1;
    pub const DEFAULT_FILE_OPEN_TIMEOUT: i32 = 5;
    pub const DEFAULT_MAX_ATTIC_FILES: i32 = 10;
    pub const DEFAULT_MAX_REDELIVERY_RECORDS: i32 = 10;
    pub const DEFAULT_DISK_SCAN_PERIOD: i32 = 5000;
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            journal_type: Self::DEFAULT_JOURNAL_TYPE,
            datasync: None,
            sync_transactional: None,
            sync_non_transactional: None,
            file_size: None,
            min_files: None,
            pool_files: None,
            compact_min_files: None,
            compact_percentage: None,
            buffer_timeout: None,
            buffer_size: None,
            max_io: None,
            device_block_size: None,
            file_open_timeout: None,
            max_attic_files: None,
            create_journal_dir: None,
            create_bindings_dir: None,
            max_redelivery_records: None,
            page_sync_timeout: None,
            purge_page_folders: None,
            max_disk_usage: None,
            min_disk_free: None,
            disk_scan_period: None,
            retention: None,
        }
    }
}

/// A `<queue>` inside an `<address>`. Fields the XML leaves unset stay
/// `None` so the address settings can supply them when the queue is created,
/// as `QueueConfiguration` does.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueConfig {
    pub name: String,
    pub address: String,
    pub routing_type: RoutingType,
    pub filter: Option<String>,
    pub durable: bool,
    pub user: Option<String>,
    pub max_consumers: Option<i32>,
    pub purge_on_no_consumers: bool,
    pub exclusive: Option<bool>,
    pub group_rebalance: Option<bool>,
    pub group_rebalance_pause_dispatch: Option<bool>,
    pub group_buckets: Option<i32>,
    pub group_first_key: Option<String>,
    pub last_value: Option<bool>,
    pub last_value_key: Option<String>,
    pub non_destructive: Option<bool>,
    pub consumers_before_dispatch: Option<i32>,
    pub delay_before_dispatch: Option<i64>,
    /// Java sets `-1` (`DEFAULT_RING_SIZE`) explicitly when absent.
    pub ring_size: i64,
    pub enabled: Option<bool>,
}

impl QueueConfig {
    pub fn new(name: &str, address: &str, routing_type: RoutingType) -> Self {
        Self {
            name: name.to_owned(),
            address: address.to_owned(),
            routing_type,
            filter: None,
            durable: true,
            user: None,
            max_consumers: None,
            purge_on_no_consumers: AddressSettings::DEFAULT_PURGE_ON_NO_CONSUMERS,
            exclusive: None,
            group_rebalance: None,
            group_rebalance_pause_dispatch: None,
            group_buckets: None,
            group_first_key: None,
            last_value: None,
            last_value_key: None,
            non_destructive: None,
            consumers_before_dispatch: None,
            delay_before_dispatch: None,
            ring_size: AddressSettings::DEFAULT_RING_SIZE,
            enabled: None,
        }
    }
}

/// An `<address>` with its routing types (in document order, without
/// duplicates) and pre-configured queues.
#[derive(Debug, Clone, PartialEq)]
pub struct AddressConfig {
    pub name: String,
    pub routing_types: Vec<RoutingType>,
    pub queues: Vec<QueueConfig>,
}

/// Something in the file that was skipped or is suspect. Never fatal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Warning {
    /// An element the schema does not define at this position.
    UnknownElement { parent: String, name: String },
    /// `xi:include` is not supported; the referenced file was not read.
    XIncludeUnsupported { href: Option<String> },
    /// No `<core>` element: every setting is the default.
    NoCoreElement,
    /// A second `<address-setting>` with the same match. Artemis keeps the
    /// first one and so does Saranga.
    DuplicateAddressSetting { r#match: String },
    /// A schema element Saranga parses but does not act on yet.
    Unsupported { name: String, reason: &'static str },
    /// A `<permission type>` that names no permission; its roles get none.
    UnknownPermission { r#type: String },
    /// A broker property that could not be applied. Artemis records these
    /// in its configuration status instead of failing.
    BrokerProperty {
        source: String,
        key: String,
        reason: String,
    },
    /// A deprecated setting was given (`AMQ221038`).
    Deprecated {
        name: String,
        alternative: &'static str,
    },
    /// A cluster connection's `connector-ref` names no connector, so it is
    /// left out (`AMQ222217`).
    ConnectorRefNotFound {
        connector: String,
        cluster_connection: String,
    },
    /// A discovery group's name was used before, so it is left out
    /// (`AMQ222019`).
    DiscoveryGroupAlreadyDeployed { name: String },
}

impl fmt::Display for Warning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownElement { parent, name } => {
                write!(f, "unknown element <{name}> in <{parent}> was ignored")
            }
            Self::XIncludeUnsupported { href: Some(href) } => {
                write!(f, "xi:include of {href:?} is not supported and was ignored")
            }
            Self::XIncludeUnsupported { href: None } => {
                f.write_str("xi:include is not supported and was ignored")
            }
            Self::NoCoreElement => f.write_str("no <core> element found; using defaults"),
            Self::DuplicateAddressSetting { r#match } => {
                write!(
                    f,
                    "duplicate address-setting match {match:?}; keeping the first"
                )
            }
            Self::Unsupported { name, reason } => write!(f, "<{name}> was ignored: {reason}"),
            Self::UnknownPermission { r#type } => {
                write!(f, "unknown permission type {type:?} in a security-setting")
            }
            Self::BrokerProperty {
                source,
                key,
                reason,
            } => write!(f, "{source}: {key} was not applied: {reason}"),
            Self::Deprecated { name, alternative } => write!(
                f,
                "AMQ221038: Configuration option '{name}' is deprecated and will be removed in a future version. Use '{alternative}' instead. Consult the manual for details."
            ),
            Self::ConnectorRefNotFound {
                connector,
                cluster_connection,
            } => write!(
                f,
                "AMQ222217: Cannot find connector-ref {connector}. The cluster-connection {cluster_connection} will not be deployed."
            ),
            Self::DiscoveryGroupAlreadyDeployed { name } => write!(
                f,
                "AMQ222019: There is already a discovery group with name {name} deployed. This one will not be deployed."
            ),
        }
    }
}

/// One login module of a `jaasConfigs` realm (`JaasAppConfiguration`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JaasModuleConfig {
    /// The key it was given in the properties; modules run in the order
    /// they were first named.
    pub name: String,
    pub login_module_class: String,
    /// `required`, `requisite`, `sufficient` or `optional`.
    pub control_flag: String,
    pub params: Vec<(String, String)>,
}

/// `ComponentConfigurationRoutingType`: what a divert does to the routing
/// type of the copies it makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DivertRoutingType {
    Multicast,
    Anycast,
    /// The copy has no routing type (the default).
    #[default]
    Strip,
    /// The copy keeps the original's.
    Pass,
}

impl std::str::FromStr for DivertRoutingType {
    type Err = crate::ConfigError;

    fn from_str(s: &str) -> crate::Result<Self> {
        match s {
            "MULTICAST" => Ok(Self::Multicast),
            "ANYCAST" => Ok(Self::Anycast),
            "STRIP" => Ok(Self::Strip),
            "PASS" => Ok(Self::Pass),
            _ => Err(crate::ConfigError::invalid(
                "routing-type",
                s,
                "expected one of: MULTICAST ANYCAST STRIP PASS",
            )),
        }
    }
}

/// One `<divert>` (`DivertConfiguration`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DivertConfig {
    pub name: String,
    /// `None` gets a random one when deployed.
    pub routing_name: Option<String>,
    pub address: String,
    /// One address, or several separated by commas.
    pub forwarding_address: String,
    pub exclusive: bool,
    pub filter: Option<String>,
    pub routing_type: DivertRoutingType,
    pub transformer: Option<TransformerConfig>,
}

/// `ComponentConfigurationRoutingType`, as bridges use it.
pub type ComponentRoutingType = DivertRoutingType;

/// `ActiveMQDefaultConfiguration.DEFAULT_CLUSTER_USER`, a bridge's user
/// unless it names one.
pub const DEFAULT_CLUSTER_USER: &str = "ACTIVEMQ.CLUSTER.ADMIN.USER";

/// `DEFAULT_CLUSTER_PASSWORD`.
pub const DEFAULT_CLUSTER_PASSWORD: &str = "CHANGE ME!!";

/// One `<bridge>` (`BridgeConfiguration`), with the defaults a new
/// `BridgeConfiguration` has.
#[derive(Debug, Clone, PartialEq)]
pub struct BridgeConfig {
    pub name: String,
    /// The name it was deployed under; a bridge with `concurrency` above 1
    /// is deployed as `<name>-0`, `<name>-1`, ... with this parent name.
    pub parent_name: String,
    /// `None` when `<queue-name>` is missing: the bridge is not deployed
    /// (`AMQ222122`).
    pub queue_name: Option<String>,
    /// `None`: each message goes to its own address.
    pub forwarding_address: Option<String>,
    pub filter: Option<String>,
    /// `None` when none was given (`null`), unlike an empty list.
    pub static_connectors: Option<Vec<String>>,
    pub discovery_group: Option<String>,
    pub ha: bool,
    pub transformer: Option<TransformerConfig>,
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
    /// Resolved: an `ENC(...)` value is decoded.
    pub password: Option<String>,
    pub connection_ttl: i64,
    pub max_retry_interval: i64,
    pub min_large_message_size: i32,
    pub call_timeout: i64,
    pub routing_type: ComponentRoutingType,
    pub concurrency: i32,
    pub pending_ack_timeout: i64,
    /// Deployed from the configuration file (true) rather than by
    /// management (false); only these follow a reload.
    pub configuration_managed: bool,
    pub client_id: Option<String>,
}

impl BridgeConfig {
    /// `new BridgeConfiguration().setName(name)`.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            parent_name: name.to_owned(),
            queue_name: None,
            forwarding_address: None,
            filter: None,
            static_connectors: None,
            discovery_group: None,
            ha: false,
            transformer: None,
            retry_interval: 2000,
            retry_interval_multiplier: 1.0,
            initial_connect_attempts: -1,
            reconnect_attempts: -1,
            reconnect_attempts_on_same_node: 10,
            use_duplicate_detection: true,
            confirmation_window_size: 10 * 1024 * 1024,
            producer_window_size: 1024 * 1024,
            client_failure_check_period: 30_000,
            user: Some(DEFAULT_CLUSTER_USER.to_owned()),
            password: Some(DEFAULT_CLUSTER_PASSWORD.to_owned()),
            connection_ttl: 60_000,
            max_retry_interval: 2000,
            min_large_message_size: 100 * 1024,
            call_timeout: 30_000,
            routing_type: ComponentRoutingType::Pass,
            concurrency: 1,
            pending_ack_timeout: 60_000,
            configuration_managed: true,
            client_id: None,
        }
    }

    /// `BridgeConfiguration.equals`, which leaves out
    /// `reconnect-attempts-on-same-node` and the routing type: whether a
    /// reload redeploys the bridge.
    pub fn same_as(&self, other: &Self) -> bool {
        let mut a = self.clone();
        a.reconnect_attempts_on_same_node = other.reconnect_attempts_on_same_node;
        a.routing_type = other.routing_type;
        a.retry_interval_multiplier = other.retry_interval_multiplier;
        a == *other
            && self.retry_interval_multiplier.to_bits() == other.retry_interval_multiplier.to_bits()
    }
}

/// `TransformerConfiguration`: a transformer by class name, with its
/// properties. Saranga looks the name up among the transformers the
/// embedding application registered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformerConfig {
    pub class_name: String,
    /// `<property key value>` pairs, in document order.
    pub properties: Vec<(String, String)>,
}

/// A `<broker-plugin>`: the Java class it names and its `<property>`
/// values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerPluginConfig {
    pub class_name: String,
    pub properties: std::collections::HashMap<String, String>,
}

/// What the critical analyzer does when a component stops responding
/// (`CriticalAnalyzerPolicy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CriticalAnalyzerPolicy {
    /// Ends the process at once, with status 70.
    Halt,
    /// Stops the broker.
    Shutdown,
    /// Logs.
    Log,
}

impl CriticalAnalyzerPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "HALT" => Self::Halt,
            "SHUTDOWN" => Self::Shutdown,
            "LOG" => Self::Log,
            _ => return None,
        })
    }
}

/// `ha-policy` (`HAPolicyConfiguration`): what the broker is in a primary
/// and backup pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HaPolicy {
    /// `primary-only` (or `live-only`), or no `ha-policy`.
    PrimaryOnly,
    /// `shared-store` `primary` (`SharedStorePrimaryPolicyConfiguration`).
    SharedStorePrimary {
        /// `failover-on-shutdown`: a clean stop lets the backup take over.
        failover_on_shutdown: bool,
        /// `wait-for-activation`.
        wait_for_activation: bool,
    },
    /// `shared-store` `backup` (`SharedStoreBackupPolicyConfiguration`).
    SharedStoreBackup {
        /// `allow-failback`: the active backup gives way to a restarted
        /// primary.
        allow_failback: bool,
        /// `failover-on-shutdown`, once it is active.
        failover_on_shutdown: bool,
        /// `restart-backup`: after failing back, it waits as a backup again.
        restart_backup: bool,
    },
    /// `replication` `primary` (`ReplicatedPolicyConfiguration`).
    ReplicationPrimary(Replication),
    /// `replication` `backup` (`ReplicaPolicyConfiguration`).
    ReplicationBackup(Replication),
}

/// The options of a replicating primary or backup. Those only one of them
/// has keep their default in the other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Replication {
    /// `group-name`: a backup replicates only a primary of its group.
    pub group_name: Option<String>,
    /// `cluster-name`: the cluster connection used to find the primary.
    pub cluster_name: Option<String>,
    /// `check-for-active-server` (primary).
    pub check_for_active_server: bool,
    /// `allow-failback` (backup): once active, it hands back to a primary
    /// that restarts and asks for it. A primary always does
    /// (`ReplicatedPolicy.allowAutoFailBack`).
    pub allow_failback: bool,
    /// `restart-backup` (backup).
    pub restart_backup: bool,
    /// `max-saved-replicated-journals-size`: how many copies of old data a
    /// backup keeps (-1 for all).
    pub max_saved_replicated_journals_size: i32,
    /// `initial-replication-sync-timeout`, in milliseconds.
    pub initial_replication_sync_timeout: i64,
    /// `vote-on-replication-failure`.
    pub vote_on_replication_failure: bool,
    /// `quorum-size`: -1 for the cluster's size.
    pub quorum_size: i32,
    /// `vote-retries`.
    pub vote_retries: i32,
    /// `vote-retry-wait`, in milliseconds.
    pub vote_retry_wait: i64,
    /// `quorum-vote-wait`, in seconds.
    pub quorum_vote_wait: i32,
    /// `retry-replication-wait`, in milliseconds.
    pub retry_replication_wait: i64,
    /// `manager`: replication coordinated through a pluggable lock
    /// manager (`ReplicationPrimaryPolicyConfiguration`,
    /// `ReplicationBackupPolicyConfiguration`) instead of quorum votes.
    pub manager: Option<LockManagerConfig>,
    /// `coordination-id` (primary, with a manager): the node ID peers
    /// competing to be primary share, as Artemis normalizes it.
    pub coordination_id: Option<String>,
}

/// `DistributedLockManagerConfiguration`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockManagerConfig {
    /// `class-name`, Curator's ZooKeeper manager by default.
    pub class_name: String,
    /// `properties`.
    pub properties: std::collections::BTreeMap<String, String>,
}

impl Default for Replication {
    fn default() -> Self {
        Self {
            group_name: None,
            cluster_name: None,
            check_for_active_server: false,
            // `ReplicaPolicyConfiguration.allowFailBack`.
            allow_failback: false,
            restart_backup: true,
            max_saved_replicated_journals_size: 2,
            initial_replication_sync_timeout: 30_000,
            vote_on_replication_failure: false,
            quorum_size: -1,
            vote_retries: 12,
            vote_retry_wait: 5_000,
            quorum_vote_wait: 30,
            retry_replication_wait: 2_000,
            manager: None,
            coordination_id: None,
        }
    }
}

impl HaPolicy {
    pub const DEFAULT_FAILOVER_ON_SHUTDOWN: bool = false;
    pub const DEFAULT_WAIT_FOR_ACTIVATION: bool = true;
    pub const DEFAULT_ALLOW_FAILBACK: bool = true;
    pub const DEFAULT_RESTART_BACKUP: bool = true;

    pub fn is_backup(&self) -> bool {
        matches!(
            self,
            Self::SharedStoreBackup { .. } | Self::ReplicationBackup(_)
        )
    }

    /// `replication`: each broker has its own data and the backup's is a
    /// copy of the primary's.
    pub fn replication(&self) -> Option<&Replication> {
        match self {
            Self::ReplicationPrimary(r) | Self::ReplicationBackup(r) => Some(r),
            _ => None,
        }
    }
}

/// `message-load-balancing` (`MessageLoadBalancingType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageLoadBalancing {
    Off,
    Strict,
    OnDemand,
    OffWithRedistribution,
    /// For Artemis's internal use (a mirror routes with it), and settable
    /// only through a cluster connection's URI or broker properties:
    /// remote bindings take nothing.
    LocalOnly,
}

impl MessageLoadBalancing {
    /// The enum constant's name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::Strict => "STRICT",
            Self::OnDemand => "ON_DEMAND",
            Self::OffWithRedistribution => "OFF_WITH_REDISTRIBUTION",
            Self::LocalOnly => "LOCAL_ONLY",
        }
    }

    /// `Enum.valueOf`, as bean properties convert it: every constant.
    pub fn value_of(s: &str) -> Option<Self> {
        [
            Self::Off,
            Self::Strict,
            Self::OnDemand,
            Self::OffWithRedistribution,
            Self::LocalOnly,
        ]
        .into_iter()
        .find(|m| m.name() == s)
    }
}

impl std::str::FromStr for MessageLoadBalancing {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "OFF" => Ok(Self::Off),
            "STRICT" => Ok(Self::Strict),
            "ON_DEMAND" => Ok(Self::OnDemand),
            "OFF_WITH_REDISTRIBUTION" => Ok(Self::OffWithRedistribution),
            _ => Err(ConfigError::invalid(
                "message-load-balancing",
                s,
                "must be OFF, STRICT, ON_DEMAND or OFF_WITH_REDISTRIBUTION",
            )),
        }
    }
}

/// Where a broadcast or discovery group sends or listens: a UDP multicast
/// group (`UDPBroadcastEndpointFactory`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UdpEndpointConfig {
    /// `group-address`.
    pub group_address: Option<String>,
    /// `group-port`; -1 when unset.
    pub group_port: i32,
    /// `local-bind-address`.
    pub local_bind_address: Option<String>,
    /// `local-bind-port`; -1 when unset.
    pub local_bind_port: i32,
}

/// `<broadcast-group>` (`BroadcastGroupConfiguration`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcastGroupConfig {
    pub name: String,
    /// `broadcast-period` in milliseconds.
    pub broadcast_period: i64,
    /// `connector-ref`s. As `parseBroadcastGroupConfiguration` reads them,
    /// each is the first `connector-ref` of the group.
    pub connectors: Vec<String>,
    pub endpoint: UdpEndpointConfig,
}

/// `<discovery-group>` (`DiscoveryGroupConfiguration`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryGroupConfig {
    pub name: String,
    /// `refresh-timeout` in milliseconds: how long a broker's broadcast is
    /// kept without another.
    pub refresh_timeout: i64,
    /// `initial-wait-timeout` in milliseconds: how long a client waits for
    /// the first broadcast.
    pub initial_wait_timeout: i64,
    pub endpoint: UdpEndpointConfig,
}

/// `<cluster-connection>` (`ClusterConnectionConfiguration`), with the
/// `ActiveMQDefaultConfiguration` defaults.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterConnectionConfig {
    pub name: String,
    pub address: String,
    /// `connector-ref`: the connector announced for this broker.
    pub connector_name: String,
    pub check_period: i64,
    pub connection_ttl: i64,
    pub min_large_message_size: i32,
    pub call_timeout: i64,
    pub call_failover_timeout: i64,
    pub retry_interval: i64,
    pub retry_interval_multiplier: f64,
    pub max_retry_interval: i64,
    pub initial_connect_attempts: i32,
    pub reconnect_attempts: i32,
    pub use_duplicate_detection: bool,
    pub message_load_balancing: MessageLoadBalancing,
    pub max_hops: i32,
    pub confirmation_window_size: i32,
    pub producer_window_size: i32,
    pub notification_interval: i64,
    pub notification_attempts: i32,
    pub topology_scanner_attempts: i32,
    /// `static-connectors`: connector names, in order.
    pub static_connectors: Vec<String>,
    /// `compositeMembers`: a `<cluster-connection-uri>`'s
    /// `static://(tcp://a:61616,tcp://b:61616)` members, which it connects
    /// to in place of named connectors.
    pub static_members: Vec<crate::AcceptorConfig>,
    pub allow_direct_connections_only: bool,
    /// `discovery-group-ref`'s `discovery-group-name`.
    pub discovery_group: Option<String>,
    pub client_id: Option<String>,
}

impl ClusterConnectionConfig {
    pub fn new(name: &str, connector_name: &str) -> Self {
        Self {
            name: name.to_owned(),
            address: String::new(),
            connector_name: connector_name.to_owned(),
            check_period: 30_000,
            connection_ttl: 60_000,
            min_large_message_size: 100 * 1024,
            call_timeout: 30_000,
            call_failover_timeout: -1,
            retry_interval: 500,
            retry_interval_multiplier: 1.0,
            max_retry_interval: 2000,
            initial_connect_attempts: -1,
            reconnect_attempts: -1,
            use_duplicate_detection: true,
            message_load_balancing: MessageLoadBalancing::OnDemand,
            max_hops: 1,
            confirmation_window_size: 10 * 1024 * 1024,
            producer_window_size: 1024 * 1024,
            notification_interval: 1000,
            notification_attempts: 2,
            topology_scanner_attempts: 30,
            static_connectors: Vec::new(),
            static_members: Vec::new(),
            allow_direct_connections_only: false,
            discovery_group: None,
            client_id: None,
        }
    }
}

/// `<grouping-handler>` (`GroupingHandlerConfiguration`): which node of a
/// cluster decides where each message group goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupingHandlerConfig {
    pub name: String,
    /// `LOCAL`: this node decides; `REMOTE`: it asks the node that does.
    pub local: bool,
    /// The cluster address its proposals concern.
    pub address: String,
    /// `timeout` in milliseconds: how long a proposal waits for its answer,
    /// and a `LOCAL` handler at startup for the groups' bindings.
    pub timeout: i64,
    /// `group-timeout` in milliseconds: a group unused this long is
    /// forgotten; -1 keeps groups.
    pub group_timeout: i64,
    /// `reaper-period` in milliseconds: how often unused groups are looked
    /// for.
    pub reaper_period: i64,
}

impl GroupingHandlerConfig {
    pub const DEFAULT_TIMEOUT: i64 = 5000;
    pub const DEFAULT_GROUP_TIMEOUT: i64 = -1;
    pub const DEFAULT_REAPER_PERIOD: i64 = 30_000;
}

/// The broker configuration read from `broker.xml`. Every field the file
/// leaves out holds the Artemis default.
#[derive(Debug, Clone, PartialEq)]
pub struct BrokerConfig {
    pub name: String,
    pub persistence_enabled: bool,
    pub security_enabled: bool,
    pub journal_directory: String,
    pub bindings_directory: String,
    pub paging_directory: String,
    pub large_messages_directory: String,
    pub journal: JournalConfig,
    /// `None` when unset or `-1`; Artemis then uses a share of the JVM heap
    /// (`global-max-size-percent-of-jvm-max-memory`, 50% by default).
    pub global_max_size: Option<i64>,
    pub global_max_messages: i64,
    pub wildcard_addresses: WildcardConfiguration,
    /// `literal-match-markers`, e.g. `()`.
    pub literal_match_markers: Option<(char, char)>,
    pub acceptors: Vec<AcceptorConfig>,
    /// `<connectors>`: each named by its `name`.
    pub connectors: Vec<AcceptorConfig>,
    /// `<cluster-connections>`, in document order; the first is the
    /// default one.
    pub cluster_connections: Vec<ClusterConnectionConfig>,
    /// `<broadcast-groups>`, in document order.
    pub broadcast_groups: Vec<BroadcastGroupConfig>,
    /// `<discovery-groups>`, in document order; a name used again is
    /// skipped (`AMQ222019`).
    pub discovery_groups: Vec<DiscoveryGroupConfig>,
    /// Match string and settings, in document order.
    pub address_settings: Vec<(String, AddressSettings)>,
    pub addresses: Vec<AddressConfig>,
    pub diverts: Vec<DivertConfig>,
    /// `<bridges>`, in document order.
    pub bridges: Vec<BridgeConfig>,
    /// `<grouping-handler>`: the last one given.
    pub grouping_handler: Option<GroupingHandlerConfig>,
    /// The `<federation>`s of `<federations>`, in document order.
    pub federations: Vec<crate::FederationConfig>,
    /// `downstream-authorization`: with security on, a user asking this
    /// broker to federate from another (a downstream) must have one of
    /// these roles.
    pub federation_downstream_authorization: Vec<String>,
    /// `<broker-connections>`: the `<amqp-connection>`s, in document order.
    pub broker_connections: Vec<crate::AmqpBrokerConnectConfig>,
    /// `message-expiry-scan-period` in milliseconds; `-1` turns the expiry
    /// reaper off.
    pub message_expiry_scan_period: i64,
    /// `transaction-timeout` in milliseconds.
    pub transaction_timeout: i64,
    /// `transaction-timeout-scan-period` in milliseconds.
    pub transaction_timeout_scan_period: i64,
    /// `address-queue-scan-period` in milliseconds; -1 turns the reaper off.
    pub address_queue_scan_period: i64,
    /// `id-cache-size`: duplicate IDs remembered per address.
    pub id_cache_size: i32,
    /// `persist-id-cache`: duplicate IDs are journaled.
    pub persist_id_cache: bool,
    /// `large-message-sync`: a large message's file is synced before it is
    /// routed.
    pub large_message_sync: bool,
    /// `configuration-file-refresh-period`: how often, in milliseconds,
    /// files are checked for changes.
    pub configuration_file_refresh_period: i64,
    /// Milliseconds an authentication or authorization stays cached.
    pub security_invalidation_interval: i64,
    /// Entries in the authentication cache; 0 disables it.
    pub authentication_cache_size: i64,
    /// Entries in the authorization cache; 0 disables it.
    pub authorization_cache_size: i64,
    pub cluster_user: String,
    /// Resolved: an `ENC(...)` value is decoded.
    pub cluster_password: String,
    /// `mask-password`: unset means `ENC(...)` marks masked values.
    pub mask_password: Option<bool>,
    /// `password-codec`: the codec class and its `;key=value` settings.
    pub password_codec: String,
    pub populate_validated_user: bool,
    pub reject_empty_validated_user: bool,
    /// `management-address`.
    pub management_address: String,
    /// `jmx-domain`: the domain of the management MBeans' names.
    pub jmx_domain: String,
    /// `management-notification-address`.
    pub management_notification_address: String,
    /// `suppress-session-notifications`: no `SESSION_CREATED` and
    /// `SESSION_CLOSED` notifications.
    pub suppress_session_notifications: bool,
    /// `mirror-ack-manager-queue-attempts`: how often a mirror target looks
    /// for an acknowledged message among its queue's messages in memory
    /// before looking in its pages too.
    pub mirror_ack_manager_queue_attempts: i32,
    /// `mirror-ack-manager-page-attempts`: page scans before giving up.
    pub mirror_ack_manager_page_attempts: i32,
    /// `mirror-ack-manager-retry-delay`, in milliseconds.
    pub mirror_ack_manager_retry_delay: i32,
    /// `mirror-ack-manager-warn-unacked`.
    pub mirror_ack_manager_warn_unacked: bool,
    /// `mirror-page-transaction`.
    pub mirror_page_transaction: bool,
    /// `graceful-shutdown-enabled`: a stopping broker waits for its clients
    /// to disconnect.
    pub graceful_shutdown_enabled: bool,
    /// `graceful-shutdown-timeout`, in milliseconds; -1 waits for ever.
    pub graceful_shutdown_timeout: i64,
    /// `network-check-list`: comma-separated hosts, any one of which
    /// answering keeps the broker running.
    pub network_check_list: Option<String>,
    /// `network-check-URL-list`: comma-separated URLs, as the list.
    pub network_check_url_list: Option<String>,
    /// `network-check-period`, in milliseconds.
    pub network_check_period: i64,
    /// `network-check-timeout`, in milliseconds.
    pub network_check_timeout: i32,
    /// `network-check-NIC`.
    pub network_check_nic: Option<String>,
    /// `network-check-ping-command`; `None` is the platform's default.
    pub network_check_ping_command: Option<String>,
    /// `network-check-ping6-command`; `None` is the platform's default.
    pub network_check_ping6_command: Option<String>,
    /// `critical-analyzer`.
    pub critical_analyzer: bool,
    /// `critical-analyzer-timeout`, in milliseconds.
    pub critical_analyzer_timeout: i64,
    /// `critical-analyzer-check-period`, in milliseconds; 0 for half the
    /// timeout.
    pub critical_analyzer_check_period: i64,
    /// `critical-analyzer-policy`.
    pub critical_analyzer_policy: CriticalAnalyzerPolicy,
    /// `message-counter-enabled`.
    pub message_counter_enabled: bool,
    /// `message-counter-sample-period`, in milliseconds.
    pub message_counter_sample_period: i64,
    /// `message-counter-max-day-history`.
    pub message_counter_max_day_history: i32,
    /// `management-message-rbac`: management messages also need the view
    /// or edit permission on `<prefix>.<resource>.<method>`.
    pub management_message_rbac: bool,
    /// `management-rbac-prefix`.
    pub management_rbac_prefix: String,
    /// `view-permission-method-match-pattern`: the methods that need only
    /// the view permission.
    pub view_permission_method_match_pattern: String,
    /// `internal-naming-prefix`: the start of the names of the broker's own
    /// resources.
    pub internal_naming_prefix: String,
    /// `mqtt-session-scan-interval`: how often, in milliseconds, MQTT
    /// sessions are checked for expiry and delayed wills.
    pub mqtt_session_scan_interval: i64,
    /// `mqtt-session-state-persistence-timeout`, deprecated.
    pub mqtt_session_state_persistence_timeout: i64,
    /// `mqtt-subscription-persistence-enabled`: MQTT sessions are kept in
    /// `$sys.mqtt.sessions`.
    pub mqtt_subscription_persistence_enabled: bool,
    /// `amqp-use-core-subscription-naming`: AMQP subscription queues are
    /// named as Core names them.
    pub amqp_use_core_subscription_naming: bool,
    /// `ha-policy`.
    pub ha_policy: HaPolicy,
    /// `node-manager-lock-directory`: where `server.lock` and the lock
    /// files are; the journal directory when unset.
    pub node_manager_lock_directory: Option<String>,
    /// `journal-lock-acquisition-timeout`, in milliseconds; -1 waits for
    /// ever.
    pub journal_lock_acquisition_timeout: i64,
    /// `journal-lock-monitor-timeout`: how often, in milliseconds, an
    /// active broker checks that it still holds the primary lock.
    pub journal_lock_monitor_timeout: i64,
    /// `journal-lock-monitor-max-retries`: failed checks tolerated before
    /// the lock counts as lost.
    pub journal_lock_monitor_max_retries: i32,
    /// Match string and roles, in document order; a repeated match
    /// replaces the earlier one.
    pub security_settings: Vec<(String, SecuritySetting)>,
    pub security_setting_plugins: Vec<SecuritySettingPluginConfig>,
    /// `<broker-plugins>`, in document order.
    pub broker_plugins: Vec<BrokerPluginConfig>,
    /// `jaasConfigs` from broker properties: login configurations by realm,
    /// which take the place of the same realm in `login.config`.
    pub jaas_configs: Vec<(String, Vec<JaasModuleConfig>)>,
    /// `system-property-prefix`: system properties under it are broker
    /// properties.
    pub system_property_prefix: String,
    pub warnings: Vec<Warning>,
    /// `ConfigurationImpl.jsonStatus`: what was read and applied from each
    /// broker properties source, under `properties`.
    pub status: saranga_buffer::json::Json,
}

impl BrokerConfig {
    pub const DEFAULT_NAME: &str = "localhost";
    pub const DEFAULT_PERSISTENCE_ENABLED: bool = true;
    pub const DEFAULT_SECURITY_ENABLED: bool = true;
    pub const DEFAULT_JOURNAL_DIR: &str = "data/journal";
    pub const DEFAULT_BINDINGS_DIRECTORY: &str = "data/bindings";
    pub const DEFAULT_PAGING_DIR: &str = "data/paging";
    pub const DEFAULT_LARGE_MESSAGES_DIR: &str = "data/largemessages";
    pub const DEFAULT_GLOBAL_MAX_MESSAGES: i64 = -1;
    pub const DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD: i64 = 30000;
    pub const DEFAULT_ID_CACHE_SIZE: i32 = 20000;
    pub const DEFAULT_TRANSACTION_TIMEOUT: i64 = 300000;
    pub const DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD: i64 = 1000;
    pub const DEFAULT_ADDRESS_QUEUE_SCAN_PERIOD: i64 = 30000;
    pub const DEFAULT_PERSIST_ID_CACHE: bool = true;
    pub const DEFAULT_SECURITY_INVALIDATION_INTERVAL: i64 = 10000;
    pub const DEFAULT_CONFIGURATION_FILE_REFRESH_PERIOD: i64 = 5000;
    pub const DEFAULT_AUTHENTICATION_CACHE_SIZE: i64 = 1000;
    pub const DEFAULT_AUTHORIZATION_CACHE_SIZE: i64 = 1000;
    pub const DEFAULT_CLUSTER_USER: &str = "ACTIVEMQ.CLUSTER.ADMIN.USER";
    pub const DEFAULT_CLUSTER_PASSWORD: &str = "CHANGE ME!!";

    /// Parses `broker.xml` text, resolving `${...}` from environment
    /// variables.
    pub fn parse(xml: &str) -> Result<Self> {
        Self::parse_with(xml, crate::substitute::env_lookup)
    }

    /// Parses `broker.xml` text, resolving `${...}` through `lookup`.
    pub fn parse_with(xml: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let text = crate::substitute::replace_system_props(xml, lookup);
        crate::parser::parse_document(&text)
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let xml = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::parse(&xml)
    }

    /// The address-settings repository the broker resolves addresses
    /// against, set up as `ActiveMQServerImpl` does: this config's wildcard
    /// syntax and literal markers, fully qualified names reduced to their
    /// address, and an empty `AddressSettings` as the default.
    pub fn address_settings_repository(&self) -> Result<HierarchicalRepository<AddressSettings>> {
        let mut repo = HierarchicalRepository::new(self.wildcard_addresses)
            .with_match_modifier(|s: &str| extract_address_name(s).to_owned());
        if let Some((start, end)) = self.literal_match_markers {
            repo = repo.with_literal_markers(start, end);
        }
        repo.set_default(Some(AddressSettings::default()));
        for (m, settings) in &self.address_settings {
            repo.add_match(m, settings.clone())?;
        }
        Ok(repo)
    }
}

impl BrokerConfig {
    /// The security-settings repository, set up as `ActiveMQServerImpl`
    /// does: this config's wildcard syntax, no literal markers, and no
    /// roles as the default.
    pub fn security_repository(&self) -> Result<HierarchicalRepository<SecuritySetting>> {
        let mut repo = HierarchicalRepository::new(self.wildcard_addresses);
        repo.set_default(Some(SecuritySetting::default()));
        for (m, setting) in &self.security_settings {
            repo.add_match(m, setting.clone())?;
        }
        Ok(repo)
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            name: Self::DEFAULT_NAME.to_owned(),
            persistence_enabled: Self::DEFAULT_PERSISTENCE_ENABLED,
            security_enabled: Self::DEFAULT_SECURITY_ENABLED,
            journal_directory: Self::DEFAULT_JOURNAL_DIR.to_owned(),
            bindings_directory: Self::DEFAULT_BINDINGS_DIRECTORY.to_owned(),
            paging_directory: Self::DEFAULT_PAGING_DIR.to_owned(),
            large_messages_directory: Self::DEFAULT_LARGE_MESSAGES_DIR.to_owned(),
            journal: JournalConfig::default(),
            global_max_size: None,
            global_max_messages: Self::DEFAULT_GLOBAL_MAX_MESSAGES,
            wildcard_addresses: WildcardConfiguration::default(),
            literal_match_markers: None,
            acceptors: Vec::new(),
            connectors: Vec::new(),
            cluster_connections: Vec::new(),
            broadcast_groups: Vec::new(),
            discovery_groups: Vec::new(),
            address_settings: Vec::new(),
            addresses: Vec::new(),
            diverts: Vec::new(),
            bridges: Vec::new(),
            grouping_handler: None,
            federations: Vec::new(),
            federation_downstream_authorization: Vec::new(),
            broker_connections: Vec::new(),
            message_expiry_scan_period: Self::DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD,
            transaction_timeout: Self::DEFAULT_TRANSACTION_TIMEOUT,
            transaction_timeout_scan_period: Self::DEFAULT_TRANSACTION_TIMEOUT_SCAN_PERIOD,
            address_queue_scan_period: Self::DEFAULT_ADDRESS_QUEUE_SCAN_PERIOD,
            id_cache_size: Self::DEFAULT_ID_CACHE_SIZE,
            persist_id_cache: Self::DEFAULT_PERSIST_ID_CACHE,
            large_message_sync: true,
            configuration_file_refresh_period: Self::DEFAULT_CONFIGURATION_FILE_REFRESH_PERIOD,
            security_invalidation_interval: Self::DEFAULT_SECURITY_INVALIDATION_INTERVAL,
            authentication_cache_size: Self::DEFAULT_AUTHENTICATION_CACHE_SIZE,
            authorization_cache_size: Self::DEFAULT_AUTHORIZATION_CACHE_SIZE,
            cluster_user: Self::DEFAULT_CLUSTER_USER.to_owned(),
            cluster_password: Self::DEFAULT_CLUSTER_PASSWORD.to_owned(),
            mask_password: None,
            password_codec: crate::mask::DEFAULT_CODEC.to_owned(),
            populate_validated_user: false,
            reject_empty_validated_user: false,
            management_address: "activemq.management".to_owned(),
            jmx_domain: "org.apache.activemq.artemis".to_owned(),
            management_notification_address: "activemq.notifications".to_owned(),
            suppress_session_notifications: false,
            mirror_ack_manager_queue_attempts: 5,
            mirror_ack_manager_page_attempts: 2,
            mirror_ack_manager_retry_delay: 100,
            mirror_ack_manager_warn_unacked: false,
            mirror_page_transaction: false,
            graceful_shutdown_enabled: false,
            graceful_shutdown_timeout: -1,
            network_check_list: None,
            network_check_url_list: None,
            network_check_period: 5000,
            network_check_timeout: 1000,
            network_check_nic: None,
            network_check_ping_command: None,
            network_check_ping6_command: None,
            critical_analyzer: true,
            critical_analyzer_timeout: 120_000,
            critical_analyzer_check_period: 0,
            critical_analyzer_policy: CriticalAnalyzerPolicy::Log,
            message_counter_enabled: false,
            message_counter_sample_period: 10_000,
            message_counter_max_day_history: 10,
            management_message_rbac: false,
            management_rbac_prefix: "mops".to_owned(),
            view_permission_method_match_pattern: "^(get|is|count|list|browse|query).*$".to_owned(),
            internal_naming_prefix: "$.artemis.internal.".to_owned(),
            mqtt_session_scan_interval: 5000,
            mqtt_session_state_persistence_timeout: 5000,
            mqtt_subscription_persistence_enabled: true,
            amqp_use_core_subscription_naming: false,
            ha_policy: HaPolicy::PrimaryOnly,
            node_manager_lock_directory: None,
            journal_lock_acquisition_timeout: -1,
            journal_lock_monitor_timeout: 2000,
            journal_lock_monitor_max_retries: 0,
            security_settings: Vec::new(),
            security_setting_plugins: Vec::new(),
            broker_plugins: Vec::new(),
            jaas_configs: Vec::new(),
            system_property_prefix: crate::broker_properties::DEFAULT_SYSTEM_PROPERTY_PREFIX
                .to_owned(),
            warnings: Vec::new(),
            status: saranga_buffer::json::Json::object(),
        }
    }
}
