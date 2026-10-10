//! Broker configuration: the `broker.xml` subset the broker needs today,
//! address settings and the wildcard match hierarchy used to resolve them.
//!
//! Ported from `FileConfigurationParser`, `FileConfiguration`,
//! `ConfigurationImpl`, `XMLUtil`, `XMLConfigurationUtil`, `AddressSettings`,
//! `HierarchicalObjectRepository`, `Match`, `WildcardConfiguration` and
//! `ByteUtil.convertTextBytes`, with `artemis-configuration.xsd` as the
//! element list.
//!
//! Elements the parser does not know produce a [`Warning`], never an error.
//! Malformed values in elements it does interpret are errors, as they are in
//! Artemis.

mod acceptor;
mod address_settings;
mod broker;
mod bytes;
mod matcher;
mod parser;
mod repository;
mod substitute;
mod wildcard;

pub use acceptor::AcceptorConfig;
pub use address_settings::{
    AddressFullMessagePolicy, AddressSettings, DeletionPolicy, DiskFullMessagePolicy,
    PageFullMessagePolicy, SlowConsumerPolicy, SlowConsumerThresholdMeasurementUnit,
};
pub use broker::{
    AddressConfig, BrokerConfig, DivertConfig, DivertRoutingType, JournalConfig, JournalType,
    QueueConfig, Warning,
};
pub use bytes::convert_text_bytes;
pub use matcher::Match;
pub use repository::{HierarchicalRepository, Mergeable, extract_address_name};
pub use substitute::{replace_system_props, replace_system_props_from_env};
pub use wildcard::WildcardConfiguration;

use std::fmt;
use std::str::FromStr;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("malformed XML: {0}")]
    Xml(#[from] roxmltree::Error),
    #[error("invalid value {value:?} for {name}: {reason}")]
    InvalidValue {
        name: String,
        value: String,
        reason: String,
    },
    #[error("<{element}> is missing the required attribute '{attribute}'")]
    MissingAttribute { element: String, attribute: String },
    #[error("address {0:?} has no routing type")]
    AddressWithNoRoutingType(String),
    #[error("invalid match {0:?}: the any-words wildcard may only be the last character")]
    InvalidMatch(String),
    #[error("invalid acceptor URI {uri:?}: {reason}")]
    InvalidUri { uri: String, reason: String },
}

impl ConfigError {
    pub(crate) fn invalid(name: &str, value: impl fmt::Display, reason: impl Into<String>) -> Self {
        Self::InvalidValue {
            name: name.to_owned(),
            value: value.to_string(),
            reason: reason.into(),
        }
    }
}

pub type Result<T> = std::result::Result<T, ConfigError>;

/// Artemis `RoutingType`. The discriminants are the bytes Artemis writes on
/// the wire and in the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RoutingType {
    Multicast = 0,
    Anycast = 1,
}

impl RoutingType {
    pub fn as_byte(self) -> u8 {
        self as u8
    }

    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Multicast),
            1 => Some(Self::Anycast),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Multicast => "MULTICAST",
            Self::Anycast => "ANYCAST",
        }
    }
}

impl FromStr for RoutingType {
    type Err = ConfigError;

    /// Case-sensitive, as `RoutingType.valueOf` is.
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "MULTICAST" => Ok(Self::Multicast),
            "ANYCAST" => Ok(Self::Anycast),
            _ => Err(ConfigError::invalid(
                "routing-type",
                s,
                "expected ANYCAST or MULTICAST",
            )),
        }
    }
}

impl fmt::Display for RoutingType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `String.trim()`: strips every char up to and including U+0020, which is
/// not the same set as `str::trim`.
pub(crate) fn java_trim(s: &str) -> &str {
    s.trim_matches(|c: char| c <= ' ')
}

/// `String.length()`: the number of UTF-16 code units.
pub(crate) fn java_len(s: &str) -> usize {
    s.encode_utf16().count()
}

/// `String.split(regex)` for a literal one-char separator: trailing empty
/// strings are dropped, leading ones are kept.
pub(crate) fn java_split(s: &str, sep: char) -> Vec<&str> {
    let mut parts: Vec<&str> = s.split(sep).collect();
    while parts.len() > 1 && parts.last() == Some(&"") {
        parts.pop();
    }
    if parts.len() == 1 && parts[0].is_empty() && !s.is_empty() {
        // Input made only of separators: Java returns an empty array.
        parts.clear();
    }
    parts
}

/// `Boolean.parseBoolean`: true only for "true" ignoring case, never fails.
pub(crate) fn java_parse_bool(s: &str) -> bool {
    s.eq_ignore_ascii_case("true")
}
