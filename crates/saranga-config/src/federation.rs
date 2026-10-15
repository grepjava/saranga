//! `<federations>`: Core federation (`FederationConfiguration` and the
//! classes of `org.apache.activemq.artemis.core.config.federation`).
//!
//! A federation names upstreams (brokers this one consumes from) and
//! downstreams (brokers asked to consume from this one), the policies
//! their `<policy ref>`s name, and the transformers the policies name.

use crate::TransformerConfig;

/// `FederationConfiguration`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FederationConfig {
    pub name: String,
    /// `Credentials.user`: what a stream without a user of its own logs in
    /// as. The parser gives the attribute as written, empty when absent.
    pub user: Option<String>,
    /// `Credentials.password`, when not empty.
    pub password: Option<String>,
    pub upstreams: Vec<FederationStreamConfig>,
    pub downstreams: Vec<FederationDownstreamConfig>,
    /// `federationPolicyMap`: one policy per name, the last given winning,
    /// in the order the names first came.
    pub policies: Vec<FederationPolicyConfig>,
    /// `transformerConfigurationMap`: one transformer per name.
    pub transformers: Vec<(String, TransformerConfig)>,
}

impl FederationConfig {
    /// `addFederationPolicy`: a policy of the same name is replaced.
    pub fn add_policy(&mut self, policy: FederationPolicyConfig) {
        match self.policies.iter_mut().find(|p| p.name() == policy.name()) {
            Some(slot) => *slot = policy,
            None => self.policies.push(policy),
        }
    }

    /// `addTransformerConfiguration`.
    pub fn add_transformer(&mut self, name: String, transformer: TransformerConfig) {
        match self.transformers.iter_mut().find(|(n, _)| *n == name) {
            Some(slot) => slot.1 = transformer,
            None => self.transformers.push((name, transformer)),
        }
    }

    pub fn policy(&self, name: &str) -> Option<&FederationPolicyConfig> {
        self.policies.iter().find(|p| p.name() == name)
    }

    pub fn transformer(&self, name: &str) -> Option<&TransformerConfig> {
        self.transformers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, t)| t)
    }
}

/// `FederationConnectionConfiguration`: how a stream connects.
#[derive(Debug, Clone, PartialEq)]
pub struct FederationConnectionConfig {
    pub ha: bool,
    pub discovery_group: Option<String>,
    pub static_connectors: Option<Vec<String>>,
    pub priority_adjustment: i32,
    /// `circuit-breaker-timeout`: after a failed connection, how long the
    /// failure is given again without trying; -1 always tries.
    pub circuit_breaker_timeout: i64,
    pub username: Option<String>,
    pub password: Option<String>,
    pub share_connection: bool,
    pub client_failure_check_period: i64,
    pub connection_ttl: i64,
    pub retry_interval: i64,
    pub retry_interval_multiplier: f64,
    pub max_retry_interval: i64,
    pub initial_connect_attempts: i32,
    pub reconnect_attempts: i32,
    pub call_timeout: i64,
    pub call_failover_timeout: i64,
}

impl FederationConnectionConfig {
    pub const DEFAULT_CIRCUIT_BREAKER_TIMEOUT: i64 = 30_000;
    pub const DEFAULT_CLIENT_FAILURE_CHECK_PERIOD: i64 = 30_000;
    pub const DEFAULT_CONNECTION_TTL: i64 = 60_000;
    pub const DEFAULT_RETRY_INTERVAL: i64 = 500;
    pub const DEFAULT_RETRY_INTERVAL_MULTIPLIER: f64 = 1.0;
    pub const DEFAULT_MAX_RETRY_INTERVAL: i64 = 2000;
    pub const DEFAULT_INITIAL_CONNECT_ATTEMPTS: i32 = -1;
    pub const DEFAULT_RECONNECT_ATTEMPTS: i32 = -1;
    pub const DEFAULT_CALL_TIMEOUT: i64 = 30_000;
    /// The class's default; `broker.xml` gives `ActiveMQClient`'s 30000.
    pub const DEFAULT_CALL_FAILOVER_TIMEOUT: i64 = -1;
}

impl Default for FederationConnectionConfig {
    fn default() -> Self {
        Self {
            ha: false,
            discovery_group: None,
            static_connectors: None,
            priority_adjustment: 0,
            circuit_breaker_timeout: Self::DEFAULT_CIRCUIT_BREAKER_TIMEOUT,
            username: None,
            password: None,
            share_connection: false,
            client_failure_check_period: Self::DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
            connection_ttl: Self::DEFAULT_CONNECTION_TTL,
            retry_interval: Self::DEFAULT_RETRY_INTERVAL,
            retry_interval_multiplier: Self::DEFAULT_RETRY_INTERVAL_MULTIPLIER,
            max_retry_interval: Self::DEFAULT_MAX_RETRY_INTERVAL,
            initial_connect_attempts: Self::DEFAULT_INITIAL_CONNECT_ATTEMPTS,
            reconnect_attempts: Self::DEFAULT_RECONNECT_ATTEMPTS,
            call_timeout: Self::DEFAULT_CALL_TIMEOUT,
            call_failover_timeout: Self::DEFAULT_CALL_FAILOVER_TIMEOUT,
        }
    }
}

/// `FederationStreamConfiguration`: an `<upstream>`, or the part of a
/// `<downstream>` it shares.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FederationStreamConfig {
    pub name: String,
    pub connection: FederationConnectionConfig,
    /// `policyRefs`, a set: each name once.
    pub policy_refs: Vec<String>,
}

impl FederationStreamConfig {
    /// `addPolicyRefs`.
    pub fn add_policy_ref(&mut self, name: String) {
        if !self.policy_refs.contains(&name) {
            self.policy_refs.push(name);
        }
    }
}

/// `FederationDownstreamConfiguration`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FederationDownstreamConfig {
    pub stream: FederationStreamConfig,
    /// `upstream-connector-ref`: the connector the other broker connects
    /// back to this one with.
    pub upstream_connector_ref: String,
}

/// A `FederationPolicy`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FederationPolicyConfig {
    Address(FederationAddressPolicyConfig),
    Queue(FederationQueuePolicyConfig),
    /// `FederationPolicySet`: other policies by name.
    Set {
        name: String,
        policy_refs: Vec<String>,
    },
}

impl FederationPolicyConfig {
    pub fn name(&self) -> &str {
        match self {
            Self::Address(p) => &p.name,
            Self::Queue(p) => &p.name,
            Self::Set { name, .. } => name,
        }
    }
}

/// `FederationAddressPolicyConfiguration`: the multicast addresses whose
/// messages come here while they have queues here.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FederationAddressPolicyConfig {
    pub name: String,
    /// `address-match`es, each once.
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    pub auto_delete: Option<bool>,
    pub auto_delete_delay: Option<i64>,
    pub auto_delete_message_count: Option<i64>,
    /// How many federations a message may have crossed and still be
    /// taken; -1 for any. `broker.xml` cannot set it (see `parser.rs`).
    pub max_hops: i32,
    pub transformer_ref: Option<String>,
    pub enable_divert_bindings: bool,
}

/// `FederationQueuePolicyConfiguration.Matcher`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FederationQueueMatcher {
    pub queue_match: Option<String>,
    pub address_match: Option<String>,
}

/// `FederationQueuePolicyConfiguration`: the queues whose messages come
/// here while they have consumers here.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FederationQueuePolicyConfig {
    pub name: String,
    pub include_federated: bool,
    /// Added to the stream's to give the remote consumer's priority; -1
    /// when unset.
    pub priority_adjustment: Option<i32>,
    pub transformer_ref: Option<String>,
    pub includes: Vec<FederationQueueMatcher>,
    pub excludes: Vec<FederationQueueMatcher>,
}
