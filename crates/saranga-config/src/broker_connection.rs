//! `<broker-connections>`: AMQP broker connections
//! (`AMQPBrokerConnectConfiguration` and the classes of
//! `org.apache.activemq.artemis.core.config.amqpBrokerConnectivity`).
//!
//! The broker connects out to another AMQP peer and, over that one
//! connection, sends its queues' messages (`sender`), takes the peer's
//! (`receiver`), both through a router (`peer`), mirrors itself
//! (`mirror`), federates (`federation`) or bridges (`bridge`).

use crate::TransformerConfig;

/// `AMQPBrokerConnectConfiguration`.
#[derive(Debug, Clone, PartialEq)]
pub struct AmqpBrokerConnectConfig {
    pub name: String,
    /// The connector URI, as written: `tcp://host:port?params`, or a list
    /// of them in parentheses.
    pub uri: String,
    /// `retry-interval`, in milliseconds.
    pub retry_interval: i32,
    /// `reconnect-attempts`: -1 for ever.
    pub reconnect_attempts: i32,
    pub user: Option<String>,
    pub password: Option<String>,
    /// `auto-start`.
    pub auto_start: bool,
    /// `lock-coordinator`.
    pub lock_coordinator: Option<String>,
    /// The elements, in document order.
    pub elements: Vec<AmqpConnectionElement>,
}

impl AmqpBrokerConnectConfig {
    pub fn new(name: impl Into<String>, uri: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            uri: uri.into(),
            retry_interval: 5000,
            reconnect_attempts: -1,
            user: None,
            password: None,
            auto_start: true,
            lock_coordinator: None,
            elements: Vec::new(),
        }
    }
}

/// `AMQPBrokerConnectionAddressType`, with each type's element.
#[derive(Debug, Clone, PartialEq)]
pub enum AmqpConnectionElement {
    /// Sends the messages of matching queues to the peer.
    Sender(AmqpMatchElement),
    /// Takes messages from the peer for matching queues.
    Receiver(AmqpMatchElement),
    /// A sender and a receiver through a router (`qd.waypoint`).
    Peer(AmqpMatchElement),
    Mirror(AmqpMirrorElement),
    Federation(AmqpFederationElement),
    Bridge(AmqpBridgeElement),
}

impl AmqpConnectionElement {
    /// `AMQPBrokerConnectionAddressType.name()`.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Sender(_) => "SENDER",
            Self::Receiver(_) => "RECEIVER",
            Self::Peer(_) => "PEER",
            Self::Mirror(_) => "MIRROR",
            Self::Federation(_) => "FEDERATION",
            Self::Bridge(_) => "BRIDGE",
        }
    }
}

/// `AMQPBrokerConnectionElement` of a sender, receiver or peer: the queues
/// it applies to, by name or by their address.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AmqpMatchElement {
    /// `address-match`.
    pub address_match: Option<String>,
    /// `queue-name`: when given, the only queue it applies to.
    pub queue_name: Option<String>,
}

/// `AMQPMirrorBrokerConnectionElement`.
#[derive(Debug, Clone, PartialEq)]
pub struct AmqpMirrorElement {
    pub message_acknowledgements: bool,
    pub queue_creation: bool,
    pub queue_removal: bool,
    pub durable: bool,
    pub sync: bool,
    pub address_filter: Option<String>,
    pub properties: Vec<(String, String)>,
}

impl Default for AmqpMirrorElement {
    fn default() -> Self {
        Self {
            message_acknowledgements: true,
            queue_creation: true,
            queue_removal: true,
            durable: true,
            sync: false,
            address_filter: None,
            properties: Vec::new(),
        }
    }
}

/// `AMQPFederatedBrokerConnectionElement`. Each set keeps one of equal
/// policies, as the `HashSet`s do.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AmqpFederationElement {
    /// The broker connection's name.
    pub name: String,
    /// Policies for this broker's demand, met from the peer.
    pub local_address_policies: Vec<AmqpFederationAddressPolicy>,
    pub local_queue_policies: Vec<AmqpFederationQueuePolicy>,
    /// Policies sent to the peer, for its demand to be met from here.
    pub remote_address_policies: Vec<AmqpFederationAddressPolicy>,
    pub remote_queue_policies: Vec<AmqpFederationQueuePolicy>,
    pub properties: Vec<(String, String)>,
}

/// `AMQPFederationAddressPolicyElement`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AmqpFederationAddressPolicy {
    pub name: String,
    /// `include`s' `address-match`es, one of each.
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub properties: Vec<(String, String)>,
    pub auto_delete: Option<bool>,
    pub auto_delete_delay: Option<i64>,
    pub auto_delete_message_count: Option<i64>,
    pub max_hops: i32,
    pub enable_divert_bindings: Option<bool>,
    pub allow_wildcard_groupings: Option<bool>,
    pub transformer: Option<TransformerConfig>,
}

/// An `include` or `exclude` of a queue policy.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AmqpQueueMatch {
    pub address_match: String,
    pub queue_match: String,
}

/// `AMQPFederationQueuePolicyElement`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AmqpFederationQueuePolicy {
    pub name: String,
    pub includes: Vec<AmqpQueueMatch>,
    pub excludes: Vec<AmqpQueueMatch>,
    pub properties: Vec<(String, String)>,
    pub include_federated: bool,
    pub priority_adjustment: Option<i32>,
    pub transformer: Option<TransformerConfig>,
}

/// `AMQPBridgeBrokerConnectionElement`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AmqpBridgeElement {
    /// The element's `name`, else the broker connection's.
    pub name: String,
    pub from_address_policies: Vec<AmqpBridgeAddressPolicy>,
    pub to_address_policies: Vec<AmqpBridgeAddressPolicy>,
    pub from_queue_policies: Vec<AmqpBridgeQueuePolicy>,
    pub to_queue_policies: Vec<AmqpBridgeQueuePolicy>,
    pub properties: Vec<(String, String)>,
}

/// `AMQPBridgeAddressPolicyElement`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AmqpBridgeAddressPolicy {
    pub name: String,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub properties: Vec<(String, String)>,
    pub remote_address: Option<String>,
    pub remote_address_prefix: Option<String>,
    pub remote_address_suffix: Option<String>,
    pub remote_terminus_capabilities: Option<Vec<String>>,
    pub include_divert_bindings: bool,
    pub use_durable_subscriptions: bool,
    pub priority: Option<i32>,
    pub filter: Option<String>,
    pub transformer: Option<TransformerConfig>,
}

/// `AMQPBridgeQueuePolicyElement`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AmqpBridgeQueuePolicy {
    pub name: String,
    pub includes: Vec<AmqpQueueMatch>,
    pub excludes: Vec<AmqpQueueMatch>,
    pub properties: Vec<(String, String)>,
    pub remote_address: Option<String>,
    pub remote_address_prefix: Option<String>,
    pub remote_address_suffix: Option<String>,
    pub remote_terminus_capabilities: Option<Vec<String>>,
    pub priority: Option<i32>,
    pub priority_adjustment: Option<i32>,
    pub filter: Option<String>,
    pub transformer: Option<TransformerConfig>,
}

/// `addProperty`: a key given again replaces its value.
pub fn add_property(properties: &mut Vec<(String, String)>, key: String, value: String) {
    match properties.iter_mut().find(|(k, _)| *k == key) {
        Some(slot) => slot.1 = value,
        None => properties.push((key, value)),
    }
}

/// `Set.add`: an equal item is not added twice.
pub fn add_unique<T: PartialEq>(set: &mut Vec<T>, item: T) {
    if !set.contains(&item) {
        set.push(item);
    }
}
