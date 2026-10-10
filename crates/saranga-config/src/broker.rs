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
    pub disk_scan_period: Option<i32>,
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
            disk_scan_period: None,
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
        }
    }
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
    /// Match string and settings, in document order.
    pub address_settings: Vec<(String, AddressSettings)>,
    pub addresses: Vec<AddressConfig>,
    pub diverts: Vec<DivertConfig>,
    /// `message-expiry-scan-period` in milliseconds; `-1` turns the expiry
    /// reaper off.
    pub message_expiry_scan_period: i64,
    /// `id-cache-size`: duplicate IDs remembered per address.
    pub id_cache_size: i32,
    /// `persist-id-cache`: duplicate IDs are journaled.
    pub persist_id_cache: bool,
    pub warnings: Vec<Warning>,
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
    pub const DEFAULT_PERSIST_ID_CACHE: bool = true;

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
            address_settings: Vec::new(),
            addresses: Vec::new(),
            diverts: Vec::new(),
            message_expiry_scan_period: Self::DEFAULT_MESSAGE_EXPIRY_SCAN_PERIOD,
            id_cache_size: Self::DEFAULT_ID_CACHE_SIZE,
            persist_id_cache: Self::DEFAULT_PERSIST_ID_CACHE,
            warnings: Vec::new(),
        }
    }
}
