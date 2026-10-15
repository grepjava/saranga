//! `AMQPBridgeConfiguration`, `AMQPBridgeReceiverConfiguration` and
//! `AMQPBridgeSenderConfiguration`: a bridge's settings, from its
//! `<property>`s, a policy's over them, else the connection's or the
//! defaults (`AMQPBridgeConstants`).

use crate::Settings;
use crate::types::Value;

pub const LINK_ATTACH_TIMEOUT: &str = "attach-timeout";
pub const DEFAULT_LINK_ATTACH_TIMEOUT: i32 = 30;
pub const RECEIVER_CREDITS: &str = "amqpCredits";
pub const RECEIVER_CREDITS_LOW: &str = "amqpLowCredits";
pub const PULL_RECEIVER_BATCH_SIZE: &str = "amqpPullConsumerCredits";
pub const DEFAULT_PULL_CREDIT_BATCH_SIZE: i32 = 10;
pub const LARGE_MESSAGE_THRESHOLD: &str = "minLargeMessageSize";
pub const IGNORE_QUEUE_CONSUMER_FILTERS: &str = "ignoreQueueConsumerFilters";
pub const IGNORE_QUEUE_FILTERS: &str = "ignoreQueueFilters";
pub const DISABLE_RECEIVER_PRIORITY: &str = "disableReceiverPriority";
pub const DISABLE_RECEIVER_DEMAND_TRACKING: &str = "disableReceiverDemandTracking";
pub const PREFER_SHARED_DURABLE_SUBSCRIPTIONS: &str = "preferSharedDurableSubscriptions";
pub const PRESETTLE_SEND_MODE: &str = "presettle";
pub const MAX_LINK_RECOVERY_ATTEMPTS: &str = "maxLinkRecoveryAttempts";
pub const DEFAULT_MAX_LINK_RECOVERY_ATTEMPTS: i32 = -1;
pub const LINK_RECOVERY_INITIAL_DELAY: &str = "linkRecoveryInitialDelay";
pub const DEFAULT_LINK_RECOVERY_INITIAL_DELAY: i64 = 1_000;
pub const LINK_RECOVERY_DELAY: &str = "linkRecoveryDelay";
pub const DEFAULT_LINK_RECOVERY_DELAY: i64 = 60_000;
pub const TUNNEL_CORE_MESSAGES: &str = "tunnel-core-messages";
pub const DEFAULT_PRIORITY_ADJUSTMENT_VALUE: i32 = -1;
pub const RECEIVER_QUIESCE_TIMEOUT: &str = "receiverQuiesceTimeout";
pub const DEFAULT_RECEIVER_QUIESCE_TIMEOUT: i32 = 60_000;
pub const ADDRESS_RECEIVER_IDLE_TIMEOUT: &str = "addressReceiverIdleTimeout";
pub const DEFAULT_ADDRESS_RECEIVER_IDLE_TIMEOUT: i32 = 5_000;
pub const QUEUE_RECEIVER_IDLE_TIMEOUT: &str = "queueReceiverIdleTimeout";
pub const DEFAULT_QUEUE_RECEIVER_IDLE_TIMEOUT: i32 = 60_000;
pub const AUTO_DELETE_DURABLE_SUBSCRIPTION: &str = "auto-delete-durable-subscription";
pub const AUTO_DELETE_DURABLE_SUBSCRIPTION_MSG_COUNT: &str =
    "auto-delete-durable-subscription-message-count";
pub const AUTO_DELETE_DURABLE_SUBSCRIPTION_DELAY: &str = "auto-delete-durable-subscription-delay";
pub const USE_MODIFIED_FOR_TRANSIENT_DELIVERY_ERRORS: &str =
    "amqpUseModifiedForTransientDeliveryErrors";
pub const RECEIVER_LINK_QUIESCE_TIMEOUT: &str = "amqpLinkQuiesceTimeout";
pub const RECEIVER_DRAIN_ON_TRANSIENT_DELIVERY_ERRORS: &str = "amqpDrainOnTransientDeliveryErrors";

/// A property as a number: a Java `Number`, or a `String` parsed.
fn int_of(v: &Value) -> Option<i64> {
    match v {
        Value::String(s) => s.trim().parse().ok(),
        Value::Boolean(_) => None,
        v => v.as_i64(),
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

/// A bridge's settings on one connection (`AMQPBridgeConfiguration`).
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

    /// The bridge's value for a key, else the connection's or the default.
    fn int(&self, key: &str) -> i64 {
        if let Some(v) = self.get(key).and_then(int_of) {
            return v;
        }
        let s = &self.settings;
        match key {
            RECEIVER_CREDITS => i64::from(s.credits),
            RECEIVER_CREDITS_LOW => i64::from(s.low_credits),
            PULL_RECEIVER_BATCH_SIZE => i64::from(DEFAULT_PULL_CREDIT_BATCH_SIZE),
            LARGE_MESSAGE_THRESHOLD => s.min_large_message_size as i64,
            LINK_ATTACH_TIMEOUT => i64::from(DEFAULT_LINK_ATTACH_TIMEOUT),
            RECEIVER_QUIESCE_TIMEOUT => i64::from(DEFAULT_RECEIVER_QUIESCE_TIMEOUT),
            ADDRESS_RECEIVER_IDLE_TIMEOUT => i64::from(DEFAULT_ADDRESS_RECEIVER_IDLE_TIMEOUT),
            QUEUE_RECEIVER_IDLE_TIMEOUT => i64::from(DEFAULT_QUEUE_RECEIVER_IDLE_TIMEOUT),
            MAX_LINK_RECOVERY_ATTEMPTS => i64::from(DEFAULT_MAX_LINK_RECOVERY_ATTEMPTS),
            LINK_RECOVERY_INITIAL_DELAY => DEFAULT_LINK_RECOVERY_INITIAL_DELAY,
            LINK_RECOVERY_DELAY => DEFAULT_LINK_RECOVERY_DELAY,
            _ => 0,
        }
    }

    fn flag(&self, key: &str) -> bool {
        if let Some(v) = self.get(key).and_then(bool_of) {
            return v;
        }
        match key {
            TUNNEL_CORE_MESSAGES | PREFER_SHARED_DURABLE_SUBSCRIPTIONS => true,
            USE_MODIFIED_FOR_TRANSIENT_DELIVERY_ERRORS => true,
            RECEIVER_DRAIN_ON_TRANSIENT_DELIVERY_ERRORS => {
                self.settings.drain_on_transient_delivery_errors
            }
            _ => false,
        }
    }
}

/// A policy's links' settings, the policy's properties over the bridge's
/// (`AMQPBridgeReceiverConfiguration`, `AMQPBridgeSenderConfiguration`).
#[derive(Debug, Clone)]
pub struct LinkConfiguration {
    properties: Vec<(String, Value)>,
    base: Configuration,
}

impl LinkConfiguration {
    pub fn new(base: Configuration, properties: Vec<(String, Value)>) -> Self {
        Self { properties, base }
    }

    fn get(&self, key: &str) -> Option<&Value> {
        self.properties
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    fn int(&self, key: &str) -> i64 {
        self.get(key)
            .and_then(int_of)
            .unwrap_or_else(|| self.base.int(key))
    }

    fn flag(&self, key: &str) -> bool {
        self.get(key)
            .and_then(bool_of)
            .unwrap_or_else(|| self.base.flag(key))
    }

    pub fn link_attach_timeout(&self) -> i32 {
        self.int(LINK_ATTACH_TIMEOUT) as i32
    }

    pub fn is_using_presettled_senders(&self) -> bool {
        self.flag(PRESETTLE_SEND_MODE)
    }

    pub fn is_link_recovery_enabled(&self) -> bool {
        self.max_link_recovery_attempts() != 0
    }

    pub fn max_link_recovery_attempts(&self) -> i32 {
        self.int(MAX_LINK_RECOVERY_ATTEMPTS) as i32
    }

    pub fn link_recovery_initial_delay(&self) -> i64 {
        self.int(LINK_RECOVERY_INITIAL_DELAY)
    }

    pub fn link_recovery_delay(&self) -> i64 {
        self.int(LINK_RECOVERY_DELAY)
    }

    pub fn receiver_credits(&self) -> i32 {
        self.int(RECEIVER_CREDITS) as i32
    }

    pub fn receiver_credits_low(&self) -> i32 {
        self.int(RECEIVER_CREDITS_LOW) as i32
    }

    pub fn pull_receiver_batch_size(&self) -> i32 {
        self.int(PULL_RECEIVER_BATCH_SIZE) as i32
    }

    pub fn receiver_quiesce_timeout(&self) -> i32 {
        self.int(RECEIVER_QUIESCE_TIMEOUT) as i32
    }

    pub fn address_receiver_idle_timeout(&self) -> i32 {
        self.int(ADDRESS_RECEIVER_IDLE_TIMEOUT) as i32
    }

    pub fn queue_receiver_idle_timeout(&self) -> i32 {
        self.int(QUEUE_RECEIVER_IDLE_TIMEOUT) as i32
    }

    pub fn large_message_threshold(&self) -> i32 {
        self.int(LARGE_MESSAGE_THRESHOLD) as i32
    }

    pub fn is_ignore_subscription_filters(&self) -> bool {
        self.flag(IGNORE_QUEUE_CONSUMER_FILTERS)
    }

    pub fn is_ignore_queue_filters(&self) -> bool {
        self.flag(IGNORE_QUEUE_FILTERS)
    }

    pub fn is_receiver_priority_disabled(&self) -> bool {
        self.flag(DISABLE_RECEIVER_PRIORITY)
    }

    pub fn is_receiver_demand_tracking_disabled(&self) -> bool {
        self.flag(DISABLE_RECEIVER_DEMAND_TRACKING)
    }

    pub fn is_prefer_shared_durable_subscriptions(&self) -> bool {
        self.flag(PREFER_SHARED_DURABLE_SUBSCRIPTIONS)
    }

    pub fn is_use_modified_for_transient_delivery_errors(&self) -> bool {
        self.flag(USE_MODIFIED_FOR_TRANSIENT_DELIVERY_ERRORS)
    }

    pub fn is_auto_delete_durable_subscriptions(&self) -> bool {
        self.flag(AUTO_DELETE_DURABLE_SUBSCRIPTION)
    }

    pub fn auto_delete_durable_subscription_msg_count(&self) -> i64 {
        self.int(AUTO_DELETE_DURABLE_SUBSCRIPTION_MSG_COUNT)
    }

    pub fn auto_delete_durable_subscription_delay(&self) -> i64 {
        self.int(AUTO_DELETE_DURABLE_SUBSCRIPTION_DELAY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_over_bridge_over_defaults() {
        let base = Configuration::new(
            vec![
                (RECEIVER_CREDITS.into(), Value::string("50")),
                (PRESETTLE_SEND_MODE.into(), Value::string("TRUE")),
            ],
            Settings::from_params(&Default::default()),
        );
        let c = LinkConfiguration::new(base, vec![(RECEIVER_CREDITS.into(), Value::string("7"))]);
        assert_eq!(c.receiver_credits(), 7);
        assert!(c.is_using_presettled_senders());
        assert_eq!(c.max_link_recovery_attempts(), -1);
        assert_eq!(c.link_recovery_delay(), 60_000);
        assert_eq!(c.pull_receiver_batch_size(), 10);
        assert!(c.is_prefer_shared_durable_subscriptions());
        assert!(!c.is_receiver_demand_tracking_disabled());
    }
}
