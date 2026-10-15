//! `AMQPFederationConfiguration` and `AMQPFederationConsumerConfiguration`:
//! the federation's settings, from its `<property>`s (or, on a target,
//! the map the source sent), then the policy's, then the connection's.

use super::constants::*;
use crate::Settings;
use crate::types::Value;

pub const DEFAULT_LINK_ATTACH_TIMEOUT: i32 = 30;
pub const DEFAULT_PULL_CREDIT_BATCH_SIZE: i32 = 100;
pub const DEFAULT_RECEIVER_QUIESCE_TIMEOUT: i32 = 60_000;
pub const DEFAULT_ADDRESS_RECEIVER_IDLE_TIMEOUT: i32 = 5_000;
pub const DEFAULT_QUEUE_RECEIVER_IDLE_TIMEOUT: i32 = 60_000;
/// `AmqpSupport.AMQP_LINK_QUIESCE_TIMEOUT`.
pub const DEFAULT_LINK_QUIESCE_TIMEOUT: i32 = 600_000;

/// A property as a number: a Java `Number`, or a `String` parsed.
fn int_of(v: &Value) -> Option<i32> {
    match v {
        Value::String(s) => s.trim().parse().ok(),
        Value::Boolean(_) => None,
        v => v.as_i64().map(|n| n as i32),
    }
}

/// A property as a flag: a `Boolean`, or a `String` as
/// `Boolean.parseBoolean` reads it.
fn bool_of(v: &Value) -> Option<bool> {
    match v {
        Value::Boolean(b) => Some(*b),
        Value::String(s) => Some(s.eq_ignore_ascii_case("true")),
        _ => None,
    }
}

/// The federation's settings.
#[derive(Debug, Clone)]
pub struct Configuration {
    properties: Vec<(String, Value)>,
    settings: Settings,
}

impl Configuration {
    pub fn new(properties: Vec<(String, Value)>, settings: Settings) -> Self {
        Self {
            properties,
            settings,
        }
    }

    fn get(&self, key: &str) -> Option<&Value> {
        self.properties
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    fn int(&self, key: &str, default: i32) -> i32 {
        self.get(key).and_then(int_of).unwrap_or(default)
    }

    fn flag(&self, key: &str, default: bool) -> bool {
        self.get(key).and_then(bool_of).unwrap_or(default)
    }

    pub fn receiver_credits(&self) -> i32 {
        self.int(RECEIVER_CREDITS, self.settings.credits)
    }

    pub fn receiver_credits_low(&self) -> i32 {
        self.int(RECEIVER_CREDITS_LOW, self.settings.low_credits)
    }

    pub fn pull_receiver_batch_size(&self) -> i32 {
        self.int(PULL_RECEIVER_BATCH_SIZE, DEFAULT_PULL_CREDIT_BATCH_SIZE)
    }

    pub fn large_message_threshold(&self) -> i32 {
        self.int(
            LARGE_MESSAGE_THRESHOLD,
            self.settings.min_large_message_size as i32,
        )
    }

    pub fn link_attach_timeout(&self) -> i32 {
        self.int(LINK_ATTACH_TIMEOUT, DEFAULT_LINK_ATTACH_TIMEOUT)
    }

    pub fn core_message_tunneling(&self) -> bool {
        self.flag(TUNNEL_CORE_MESSAGES, true)
    }

    pub fn ignore_address_binding_filters(&self) -> bool {
        self.flag(IGNORE_ADDRESS_BINDING_FILTERS, true)
    }

    pub fn ignore_subscription_filters(&self) -> bool {
        self.flag(IGNORE_QUEUE_CONSUMER_FILTERS, false)
    }

    pub fn ignore_subscription_priorities(&self) -> bool {
        self.flag(IGNORE_QUEUE_CONSUMER_PRIORITIES, true)
    }

    pub fn receiver_quiesce_timeout(&self) -> i32 {
        self.int(RECEIVER_QUIESCE_TIMEOUT, DEFAULT_RECEIVER_QUIESCE_TIMEOUT)
    }

    pub fn address_receiver_idle_timeout(&self) -> i32 {
        self.int(
            ADDRESS_RECEIVER_IDLE_TIMEOUT,
            DEFAULT_ADDRESS_RECEIVER_IDLE_TIMEOUT,
        )
    }

    pub fn queue_receiver_idle_timeout(&self) -> i32 {
        self.int(
            QUEUE_RECEIVER_IDLE_TIMEOUT,
            DEFAULT_QUEUE_RECEIVER_IDLE_TIMEOUT,
        )
    }

    pub fn use_modified_for_transient_delivery_errors(&self) -> bool {
        self.flag(
            USE_MODIFIED_FOR_TRANSIENT_DELIVERY_ERRORS,
            self.settings.use_modified_for_transient_delivery_errors,
        )
    }

    pub fn drain_on_transient_delivery_errors(&self) -> bool {
        self.flag(
            RECEIVER_DRAIN_ON_TRANSIENT_DELIVERY_ERRORS,
            self.settings.drain_on_transient_delivery_errors,
        )
    }

    pub fn link_quiesce_timeout(&self) -> i32 {
        self.int(RECEIVER_LINK_QUIESCE_TIMEOUT, DEFAULT_LINK_QUIESCE_TIMEOUT)
    }

    /// `toConfigurationMap`: what the control link tells the target, in
    /// its `HashMap`'s order.
    pub fn to_map(&self) -> Value {
        let entries: Vec<(String, Value)> = vec![
            (RECEIVER_CREDITS.into(), Value::Int(self.receiver_credits())),
            (
                RECEIVER_CREDITS_LOW.into(),
                Value::Int(self.receiver_credits_low()),
            ),
            (
                RECEIVER_QUIESCE_TIMEOUT.into(),
                Value::Int(self.receiver_quiesce_timeout()),
            ),
            (
                ADDRESS_RECEIVER_IDLE_TIMEOUT.into(),
                Value::Int(self.address_receiver_idle_timeout()),
            ),
            (
                QUEUE_RECEIVER_IDLE_TIMEOUT.into(),
                Value::Int(self.queue_receiver_idle_timeout()),
            ),
            (
                PULL_RECEIVER_BATCH_SIZE.into(),
                Value::Int(self.pull_receiver_batch_size()),
            ),
            (
                LARGE_MESSAGE_THRESHOLD.into(),
                Value::Int(self.large_message_threshold()),
            ),
            (
                LINK_ATTACH_TIMEOUT.into(),
                Value::Int(self.link_attach_timeout()),
            ),
            (
                IGNORE_ADDRESS_BINDING_FILTERS.into(),
                Value::Boolean(self.ignore_address_binding_filters()),
            ),
            (
                IGNORE_QUEUE_CONSUMER_FILTERS.into(),
                Value::Boolean(self.ignore_subscription_filters()),
            ),
            (
                IGNORE_QUEUE_CONSUMER_PRIORITIES.into(),
                Value::Boolean(self.ignore_subscription_priorities()),
            ),
            (
                USE_MODIFIED_FOR_TRANSIENT_DELIVERY_ERRORS.into(),
                Value::Boolean(self.use_modified_for_transient_delivery_errors()),
            ),
            (
                RECEIVER_LINK_QUIESCE_TIMEOUT.into(),
                Value::Int(self.link_quiesce_timeout()),
            ),
            (
                RECEIVER_DRAIN_ON_TRANSIENT_DELIVERY_ERRORS.into(),
                Value::Boolean(self.drain_on_transient_delivery_errors()),
            ),
            (
                TUNNEL_CORE_MESSAGES.into(),
                Value::Boolean(self.core_message_tunneling()),
            ),
        ];
        Value::Map(
            super::policy::java_hash_order(entries)
                .into_iter()
                .map(|(k, v)| (Value::string(k), v))
                .collect(),
        )
    }
}

/// A policy's settings over its federation's
/// (`AMQPFederationConsumerConfiguration`).
#[derive(Debug, Clone)]
pub struct ConsumerConfiguration {
    properties: Vec<(String, Value)>,
    base: Configuration,
}

impl ConsumerConfiguration {
    pub fn new(base: Configuration, properties: Vec<(String, Value)>) -> Self {
        Self { properties, base }
    }

    fn get(&self, key: &str) -> Option<&Value> {
        self.properties
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    fn int(&self, key: &str, base: i32) -> i32 {
        self.get(key).and_then(int_of).unwrap_or(base)
    }

    fn flag(&self, key: &str, base: bool) -> bool {
        self.get(key).and_then(bool_of).unwrap_or(base)
    }

    pub fn receiver_credits(&self) -> i32 {
        self.int(RECEIVER_CREDITS, self.base.receiver_credits())
    }

    pub fn receiver_credits_low(&self) -> i32 {
        self.int(RECEIVER_CREDITS_LOW, self.base.receiver_credits_low())
    }

    pub fn receiver_quiesce_timeout(&self) -> i32 {
        self.int(
            RECEIVER_QUIESCE_TIMEOUT,
            self.base.receiver_quiesce_timeout(),
        )
    }

    pub fn address_receiver_idle_timeout(&self) -> i32 {
        self.int(
            ADDRESS_RECEIVER_IDLE_TIMEOUT,
            self.base.address_receiver_idle_timeout(),
        )
    }

    pub fn queue_receiver_idle_timeout(&self) -> i32 {
        self.int(
            QUEUE_RECEIVER_IDLE_TIMEOUT,
            self.base.queue_receiver_idle_timeout(),
        )
    }

    pub fn large_message_threshold(&self) -> i32 {
        self.int(LARGE_MESSAGE_THRESHOLD, self.base.large_message_threshold())
    }

    pub fn link_attach_timeout(&self) -> i32 {
        self.int(LINK_ATTACH_TIMEOUT, self.base.link_attach_timeout())
    }

    pub fn ignore_address_binding_filters(&self) -> bool {
        self.flag(
            IGNORE_ADDRESS_BINDING_FILTERS,
            self.base.ignore_address_binding_filters(),
        )
    }

    pub fn ignore_subscription_filters(&self) -> bool {
        self.flag(
            IGNORE_QUEUE_CONSUMER_FILTERS,
            self.base.ignore_subscription_filters(),
        )
    }

    pub fn ignore_subscription_priorities(&self) -> bool {
        self.flag(
            IGNORE_QUEUE_CONSUMER_PRIORITIES,
            self.base.ignore_subscription_priorities(),
        )
    }

    pub fn use_modified_for_transient_delivery_errors(&self) -> bool {
        self.flag(
            USE_MODIFIED_FOR_TRANSIENT_DELIVERY_ERRORS,
            self.base.use_modified_for_transient_delivery_errors(),
        )
    }
}
