//! Ported from `AddressSettings` and the policy enums it uses.
//!
//! Every field is optional so that settings from several matches can be
//! merged; the accessor methods of the same name return the effective value,
//! applying the `DEFAULT_*` constant from `AddressSettings` (or
//! `ActiveMQDefaultConfiguration`) when the field is unset.

use std::fmt;
use std::str::FromStr;

use crate::repository::Mergeable;
use crate::{ConfigError, Result, RoutingType};

macro_rules! string_enum {
    ($(#[$meta:meta])* $name:ident { $($variant:ident = $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text),+
                }
            }
        }

        impl FromStr for $name {
            type Err = ConfigError;

            /// Case-sensitive, as `Enum.valueOf` is.
            fn from_str(s: &str) -> Result<Self> {
                match s {
                    $($text => Ok(Self::$variant),)+
                    _ => Err(ConfigError::invalid(
                        stringify!($name),
                        s,
                        concat!("expected one of:" $(, " ", $text)+),
                    )),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl AsSettingValue for $name {
            const KIND: &'static str = "string";
            const CHOICES: &'static [&'static str] = &[$($text),+];

            fn setting_value(&self) -> SettingValue {
                SettingValue::Text(self.as_str().to_owned())
            }
        }
    };
}

/// A setting's value as `AddressSettings.toJSON` writes it.
#[derive(Debug, Clone, PartialEq)]
pub enum SettingValue {
    Text(String),
    Long(i64),
    Double(f64),
    Bool(bool),
}

/// Turns a field into the value `toJSON` writes.
pub trait AsSettingValue {
    /// The JSON type `toJSON` writes the field as: `string`, `integer`,
    /// `number` or `boolean`.
    const KIND: &'static str;
    /// An enum's values; none for anything else.
    const CHOICES: &'static [&'static str] = &[];

    fn setting_value(&self) -> SettingValue;
}

impl AsSettingValue for String {
    const KIND: &'static str = "string";
    fn setting_value(&self) -> SettingValue {
        SettingValue::Text(self.clone())
    }
}

impl AsSettingValue for i64 {
    const KIND: &'static str = "integer";
    fn setting_value(&self) -> SettingValue {
        SettingValue::Long(*self)
    }
}

impl AsSettingValue for i32 {
    const KIND: &'static str = "integer";
    fn setting_value(&self) -> SettingValue {
        SettingValue::Long(i64::from(*self))
    }
}

impl AsSettingValue for f64 {
    const KIND: &'static str = "number";
    fn setting_value(&self) -> SettingValue {
        SettingValue::Double(*self)
    }
}

impl AsSettingValue for bool {
    const KIND: &'static str = "boolean";
    fn setting_value(&self) -> SettingValue {
        SettingValue::Bool(*self)
    }
}

/// The `<address-setting>` element that sets a field.
fn element(field: &str) -> String {
    match field {
        "redelivery_multiplier" => "redelivery-delay-multiplier".to_owned(),
        "page_cache_max_size" => "page-max-cache-size".to_owned(),
        "address_full_message_policy" => "address-full-policy".to_owned(),
        "disk_full_message_policy" => "disk-full-policy".to_owned(),
        "page_full_message_policy" => "page-full-policy".to_owned(),
        _ => field.replace('_', "-"),
    }
}

/// The `metaBean` name of a field: its Rust name in camel case.
pub fn java_name(field: &str) -> String {
    if field == "send_to_dla_on_no_route" {
        return "sendToDLAOnNoRoute".to_owned();
    }
    let mut out = String::with_capacity(field.len());
    let mut upper = false;
    for c in field.chars() {
        if c == '_' {
            upper = true;
        } else if upper {
            out.push(c.to_ascii_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

impl AddressSettings {
    /// `metaBean.fromJSON`: settings from `(metaBean name, value)` pairs,
    /// each value as text. Names it does not know are left out, as
    /// `fromJSON` leaves them; a value that does not parse is an error.
    pub fn from_named_values(values: &[(String, String)]) -> Result<Self> {
        let mut out = Self::default();
        for (name, value) in values {
            let Some(field) = Self::FIELDS.iter().find(|f| java_name(f) == *name) else {
                continue;
            };
            if let Some(fragment) = crate::parser::address_setting_fragment(&element(field), value)?
            {
                out = Mergeable::merge_copy(&fragment, &out);
            }
        }
        Ok(out)
    }
}

string_enum!(
    /// What happens when an address reaches its size limit.
    AddressFullMessagePolicy {
        Drop = "DROP",
        Page = "PAGE",
        Block = "BLOCK",
        Fail = "FAIL",
    }
);

string_enum!(
    /// What happens when paging reaches `page-limit-bytes`/`-messages`.
    PageFullMessagePolicy {
        Drop = "DROP",
        Fail = "FAIL",
    }
);

string_enum!(
    /// What happens when the disk is full.
    DiskFullMessagePolicy {
        Drop = "DROP",
        Block = "BLOCK",
        Fail = "FAIL",
    }
);

string_enum!(
    /// What the broker does with a slow consumer.
    SlowConsumerPolicy {
        Kill = "KILL",
        Notify = "NOTIFY",
    }
);

string_enum!(
    /// The unit of `slow-consumer-threshold`.
    SlowConsumerThresholdMeasurementUnit {
        MessagesPerSecond = "MESSAGES_PER_SECOND",
        MessagesPerMinute = "MESSAGES_PER_MINUTE",
        MessagesPerHour = "MESSAGES_PER_HOUR",
        MessagesPerDay = "MESSAGES_PER_DAY",
    }
);

string_enum!(
    /// Whether removing a queue, address or divert from the config deletes it.
    DeletionPolicy {
        Off = "OFF",
        Force = "FORCE",
    }
);

macro_rules! address_settings {
    ($($(#[$meta:meta])* $field:ident: $ty:ty,)+) => {
        /// One `<address-setting>`. Field order follows the Java `metaBean`
        /// registration order.
        #[derive(Debug, Clone, Default, PartialEq)]
        pub struct AddressSettings {
            $($(#[$meta])* pub $field: Option<$ty>,)+
        }

        impl AddressSettings {
            /// `merge`: fills every unset field from `other`.
            pub fn merge(&mut self, other: &AddressSettings) {
                $(if self.$field.is_none() {
                    self.$field = other.$field.clone();
                })+
            }
        }

        impl AddressSettings {
            /// The Rust names of the fields, in `metaBean` order.
            pub const FIELDS: &'static [&'static str] = &[$(stringify!($field)),+];

            /// Every field by its `metaBean` name, with the JSON type
            /// `toJSON` writes it as and an enum's values.
            pub fn kinds() -> Vec<(String, &'static str, &'static [&'static str])> {
                vec![$((
                    java_name(stringify!($field)),
                    <$ty as AsSettingValue>::KIND,
                    <$ty as AsSettingValue>::CHOICES,
                )),+]
            }

            /// The fields that are set, by their `metaBean` names, in
            /// registration order (`toJSON` ignoring null attributes).
            pub fn named_values(&self) -> Vec<(String, SettingValue)> {
                let mut out = Vec::new();
                $(if let Some(v) = &self.$field {
                    out.push((java_name(stringify!($field)), v.setting_value()));
                })+
                out
            }
        }

        impl Mergeable for AddressSettings {
            /// `mergeCopy`.
            fn merge_copy(&self, other: &Self) -> Self {
                Self {
                    $($field: self.$field.clone().or_else(|| other.$field.clone()),)+
                }
            }
        }
    };
}

address_settings! {
    address_full_message_policy: AddressFullMessagePolicy,
    max_size_bytes: i64,
    max_read_page_bytes: i32,
    max_read_page_messages: i32,
    prefetch_page_bytes: i32,
    prefetch_page_messages: i32,
    page_limit_bytes: i64,
    page_limit_messages: i64,
    page_full_message_policy: PageFullMessagePolicy,
    disk_full_message_policy: DiskFullMessagePolicy,
    max_size_messages: i64,
    page_size_bytes: i32,
    page_cache_max_size: i32,
    max_delivery_attempts: i32,
    message_counter_history_day_limit: i32,
    redelivery_delay: i64,
    redelivery_multiplier: f64,
    redelivery_collision_avoidance_factor: f64,
    max_redelivery_delay: i64,
    dead_letter_address: String,
    expiry_address: String,
    expiry_delay: i64,
    min_expiry_delay: i64,
    max_expiry_delay: i64,
    no_expiry: bool,
    default_last_value_queue: bool,
    default_last_value_key: String,
    default_non_destructive: bool,
    default_exclusive_queue: bool,
    default_group_rebalance: bool,
    default_group_rebalance_pause_dispatch: bool,
    default_group_buckets: i32,
    default_group_first_key: String,
    redistribution_delay: i64,
    send_to_dla_on_no_route: bool,
    slow_consumer_threshold: i64,
    slow_consumer_threshold_measurement_unit: SlowConsumerThresholdMeasurementUnit,
    slow_consumer_check_period: i64,
    slow_consumer_policy: SlowConsumerPolicy,
    /// Deprecated.
    auto_create_jms_queues: bool,
    /// Deprecated.
    auto_delete_jms_queues: bool,
    /// Deprecated.
    auto_create_jms_topics: bool,
    /// Deprecated.
    auto_delete_jms_topics: bool,
    auto_create_queues: bool,
    auto_delete_queues: bool,
    auto_delete_created_queues: bool,
    auto_delete_queues_delay: i64,
    auto_delete_queues_skip_usage_check: bool,
    auto_delete_queues_message_count: i64,
    default_ring_size: i64,
    retroactive_message_count: i64,
    config_delete_queues: DeletionPolicy,
    auto_create_addresses: bool,
    auto_delete_addresses: bool,
    auto_delete_addresses_delay: i64,
    auto_delete_addresses_skip_usage_check: bool,
    config_delete_addresses: DeletionPolicy,
    config_delete_diverts: DeletionPolicy,
    management_browse_page_size: i32,
    max_size_bytes_reject_threshold: i64,
    default_max_consumers: i32,
    default_purge_on_no_consumers: bool,
    default_consumers_before_dispatch: i32,
    default_delay_before_dispatch: i64,
    default_queue_routing_type: RoutingType,
    default_address_routing_type: RoutingType,
    default_consumer_window_size: i32,
    auto_create_dead_letter_resources: bool,
    dead_letter_queue_prefix: String,
    dead_letter_queue_suffix: String,
    auto_create_expiry_resources: bool,
    expiry_queue_prefix: String,
    expiry_queue_suffix: String,
    enable_metrics: bool,
    management_message_attribute_size_limit: i32,
    enable_ingress_timestamp: bool,
    id_cache_size: i32,
    initial_queue_buffer_size: i32,
    /// Deprecated, from ActiveMQ 5; there is no XML element for it.
    queue_prefetch: i32,
}

impl AddressSettings {
    pub const DEFAULT_MAX_SIZE_BYTES: i64 = -1;
    pub const DEFAULT_MAX_SIZE_MESSAGES: i64 = -1;
    pub const DEFAULT_MAX_READ_PAGE_MESSAGES: i32 = -1;
    pub const DEFAULT_ADDRESS_FULL_MESSAGE_POLICY: AddressFullMessagePolicy =
        AddressFullMessagePolicy::Page;
    pub const DEFAULT_PAGE_SIZE: i32 = 10 * 1024 * 1024;
    pub const DEFAULT_MAX_DELIVERY_ATTEMPTS: i32 = 10;
    pub const DEFAULT_PAGE_MAX_CACHE: i32 = 5;
    pub const DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT: i32 = 0;
    pub const DEFAULT_REDELIVER_DELAY: i64 = 0;
    pub const DEFAULT_REDELIVER_MULTIPLIER: f64 = 1.0;
    pub const DEFAULT_REDELIVER_COLLISION_AVOIDANCE_FACTOR: f64 = 0.0;
    pub const DEFAULT_LAST_VALUE_QUEUE: bool = false;
    pub const DEFAULT_AUTO_CREATE_JMS_QUEUES: bool = true;
    pub const DEFAULT_AUTO_DELETE_JMS_QUEUES: bool = true;
    pub const DEFAULT_AUTO_CREATE_TOPICS: bool = true;
    pub const DEFAULT_AUTO_DELETE_TOPICS: bool = true;
    pub const DEFAULT_AUTO_CREATE_QUEUES: bool = true;
    pub const DEFAULT_AUTO_DELETE_QUEUES: bool = true;
    pub const DEFAULT_AUTO_DELETE_CREATED_QUEUES: bool = false;
    pub const DEFAULT_AUTO_DELETE_QUEUES_DELAY: i64 = 0;
    pub const DEFAULT_AUTO_DELETE_QUEUES_SKIP_USAGE_CHECK: bool = false;
    pub const DEFAULT_AUTO_DELETE_QUEUES_MESSAGE_COUNT: i64 = 0;
    pub const DEFAULT_CONFIG_DELETE_QUEUES: DeletionPolicy = DeletionPolicy::Off;
    pub const DEFAULT_AUTO_CREATE_ADDRESSES: bool = true;
    pub const DEFAULT_AUTO_DELETE_ADDRESSES: bool = true;
    pub const DEFAULT_AUTO_DELETE_ADDRESSES_DELAY: i64 = 0;
    pub const DEFAULT_AUTO_DELETE_ADDRESSES_SKIP_USAGE_CHECK: bool = false;
    pub const DEFAULT_CONFIG_DELETE_ADDRESSES: DeletionPolicy = DeletionPolicy::Off;
    pub const DEFAULT_CONFIG_DELETE_DIVERTS: DeletionPolicy = DeletionPolicy::Off;
    pub const DEFAULT_REDISTRIBUTION_DELAY: i64 = -1;
    pub const DEFAULT_AUTO_CREATE_EXPIRY_RESOURCES: bool = false;
    pub const DEFAULT_EXPIRY_QUEUE_PREFIX: &str = "EXP.";
    pub const DEFAULT_EXPIRY_QUEUE_SUFFIX: &str = "";
    pub const DEFAULT_EXPIRY_DELAY: i64 = -1;
    pub const DEFAULT_MIN_EXPIRY_DELAY: i64 = -1;
    pub const DEFAULT_MAX_EXPIRY_DELAY: i64 = -1;
    pub const DEFAULT_NO_EXPIRY: bool = false;
    pub const DEFAULT_SEND_TO_DLA_ON_NO_ROUTE: bool = false;
    pub const DEFAULT_SLOW_CONSUMER_THRESHOLD: i64 = -1;
    pub const DEFAULT_SLOW_CONSUMER_CHECK_PERIOD: i64 = 5;
    pub const MANAGEMENT_BROWSE_PAGE_SIZE: i32 = 200;
    pub const DEFAULT_SLOW_CONSUMER_POLICY: SlowConsumerPolicy = SlowConsumerPolicy::Notify;
    pub const DEFAULT_QUEUE_PREFETCH: i32 = 1000;
    pub const DEFAULT_ADDRESS_REJECT_THRESHOLD: i64 = -1;
    pub const DEFAULT_AUTO_CREATE_DEAD_LETTER_RESOURCES: bool = false;
    pub const DEFAULT_DEAD_LETTER_QUEUE_PREFIX: &str = "DLQ.";
    pub const DEFAULT_DEAD_LETTER_QUEUE_SUFFIX: &str = "";
    pub const DEFAULT_ENABLE_METRICS: bool = true;
    pub const MANAGEMENT_MESSAGE_ATTRIBUTE_SIZE_LIMIT: i32 = 256;
    pub const DEFAULT_SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT:
        SlowConsumerThresholdMeasurementUnit =
        SlowConsumerThresholdMeasurementUnit::MessagesPerSecond;
    pub const DEFAULT_ENABLE_INGRESS_TIMESTAMP: bool = false;

    // Defaults that live in `ActiveMQDefaultConfiguration` / `ActiveMQClient`.
    pub const DEFAULT_MAX_QUEUE_CONSUMERS: i32 = -1;
    pub const DEFAULT_CONSUMERS_BEFORE_DISPATCH: i32 = 0;
    pub const DEFAULT_DELAY_BEFORE_DISPATCH: i64 = -1;
    pub const DEFAULT_PURGE_ON_NO_CONSUMERS: bool = false;
    pub const DEFAULT_ROUTING_TYPE: RoutingType = RoutingType::Multicast;
    pub const DEFAULT_NON_DESTRUCTIVE: bool = false;
    pub const DEFAULT_EXCLUSIVE: bool = false;
    pub const DEFAULT_GROUP_REBALANCE: bool = false;
    pub const DEFAULT_GROUP_REBALANCE_PAUSE_DISPATCH: bool = false;
    pub const DEFAULT_GROUP_BUCKETS: i32 = -1;
    pub const DEFAULT_RING_SIZE: i64 = -1;
    pub const DEFAULT_RETROACTIVE_MESSAGE_COUNT: i64 = 0;
    pub const DEFAULT_CONSUMER_WINDOW_SIZE: i32 = 1024 * 1024;
}

macro_rules! getters {
    ($($field:ident: $ty:ty = $default:expr;)+) => {
        impl AddressSettings {
            $(
                #[doc = concat!("Effective `", stringify!($field), "`, defaulting to `", stringify!($default), "`.")]
                pub fn $field(&self) -> $ty {
                    self.$field.unwrap_or($default)
                }
            )+
        }
    };
}

getters! {
    address_full_message_policy: AddressFullMessagePolicy = Self::DEFAULT_ADDRESS_FULL_MESSAGE_POLICY;
    max_size_bytes: i64 = Self::DEFAULT_MAX_SIZE_BYTES;
    max_size_messages: i64 = Self::DEFAULT_MAX_SIZE_MESSAGES;
    max_read_page_messages: i32 = Self::DEFAULT_MAX_READ_PAGE_MESSAGES;
    page_size_bytes: i32 = Self::DEFAULT_PAGE_SIZE;
    page_cache_max_size: i32 = Self::DEFAULT_PAGE_MAX_CACHE;
    max_delivery_attempts: i32 = Self::DEFAULT_MAX_DELIVERY_ATTEMPTS;
    message_counter_history_day_limit: i32 = Self::DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT;
    redelivery_delay: i64 = Self::DEFAULT_REDELIVER_DELAY;
    redelivery_multiplier: f64 = Self::DEFAULT_REDELIVER_MULTIPLIER;
    redelivery_collision_avoidance_factor: f64 = Self::DEFAULT_REDELIVER_COLLISION_AVOIDANCE_FACTOR;
    expiry_delay: i64 = Self::DEFAULT_EXPIRY_DELAY;
    min_expiry_delay: i64 = Self::DEFAULT_MIN_EXPIRY_DELAY;
    max_expiry_delay: i64 = Self::DEFAULT_MAX_EXPIRY_DELAY;
    no_expiry: bool = Self::DEFAULT_NO_EXPIRY;
    default_last_value_queue: bool = Self::DEFAULT_LAST_VALUE_QUEUE;
    default_non_destructive: bool = Self::DEFAULT_NON_DESTRUCTIVE;
    default_exclusive_queue: bool = Self::DEFAULT_EXCLUSIVE;
    default_group_rebalance: bool = Self::DEFAULT_GROUP_REBALANCE;
    default_group_rebalance_pause_dispatch: bool = Self::DEFAULT_GROUP_REBALANCE_PAUSE_DISPATCH;
    default_group_buckets: i32 = Self::DEFAULT_GROUP_BUCKETS;
    redistribution_delay: i64 = Self::DEFAULT_REDISTRIBUTION_DELAY;
    send_to_dla_on_no_route: bool = Self::DEFAULT_SEND_TO_DLA_ON_NO_ROUTE;
    slow_consumer_threshold: i64 = Self::DEFAULT_SLOW_CONSUMER_THRESHOLD;
    slow_consumer_threshold_measurement_unit: SlowConsumerThresholdMeasurementUnit =
        Self::DEFAULT_SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT;
    slow_consumer_check_period: i64 = Self::DEFAULT_SLOW_CONSUMER_CHECK_PERIOD;
    slow_consumer_policy: SlowConsumerPolicy = Self::DEFAULT_SLOW_CONSUMER_POLICY;
    auto_create_jms_queues: bool = Self::DEFAULT_AUTO_CREATE_JMS_QUEUES;
    auto_delete_jms_queues: bool = Self::DEFAULT_AUTO_DELETE_JMS_QUEUES;
    auto_create_jms_topics: bool = Self::DEFAULT_AUTO_CREATE_TOPICS;
    auto_delete_jms_topics: bool = Self::DEFAULT_AUTO_DELETE_TOPICS;
    auto_create_queues: bool = Self::DEFAULT_AUTO_CREATE_QUEUES;
    auto_delete_queues: bool = Self::DEFAULT_AUTO_DELETE_QUEUES;
    auto_delete_created_queues: bool = Self::DEFAULT_AUTO_DELETE_CREATED_QUEUES;
    auto_delete_queues_delay: i64 = Self::DEFAULT_AUTO_DELETE_QUEUES_DELAY;
    auto_delete_queues_skip_usage_check: bool = Self::DEFAULT_AUTO_DELETE_QUEUES_SKIP_USAGE_CHECK;
    auto_delete_queues_message_count: i64 = Self::DEFAULT_AUTO_DELETE_QUEUES_MESSAGE_COUNT;
    default_ring_size: i64 = Self::DEFAULT_RING_SIZE;
    retroactive_message_count: i64 = Self::DEFAULT_RETROACTIVE_MESSAGE_COUNT;
    config_delete_queues: DeletionPolicy = Self::DEFAULT_CONFIG_DELETE_QUEUES;
    auto_create_addresses: bool = Self::DEFAULT_AUTO_CREATE_ADDRESSES;
    auto_delete_addresses: bool = Self::DEFAULT_AUTO_DELETE_ADDRESSES;
    auto_delete_addresses_delay: i64 = Self::DEFAULT_AUTO_DELETE_ADDRESSES_DELAY;
    auto_delete_addresses_skip_usage_check: bool = Self::DEFAULT_AUTO_DELETE_ADDRESSES_SKIP_USAGE_CHECK;
    config_delete_addresses: DeletionPolicy = Self::DEFAULT_CONFIG_DELETE_ADDRESSES;
    config_delete_diverts: DeletionPolicy = Self::DEFAULT_CONFIG_DELETE_DIVERTS;
    management_browse_page_size: i32 = Self::MANAGEMENT_BROWSE_PAGE_SIZE;
    max_size_bytes_reject_threshold: i64 = Self::DEFAULT_ADDRESS_REJECT_THRESHOLD;
    default_max_consumers: i32 = Self::DEFAULT_MAX_QUEUE_CONSUMERS;
    default_purge_on_no_consumers: bool = Self::DEFAULT_PURGE_ON_NO_CONSUMERS;
    default_consumers_before_dispatch: i32 = Self::DEFAULT_CONSUMERS_BEFORE_DISPATCH;
    default_delay_before_dispatch: i64 = Self::DEFAULT_DELAY_BEFORE_DISPATCH;
    default_queue_routing_type: RoutingType = Self::DEFAULT_ROUTING_TYPE;
    default_address_routing_type: RoutingType = Self::DEFAULT_ROUTING_TYPE;
    default_consumer_window_size: i32 = Self::DEFAULT_CONSUMER_WINDOW_SIZE;
    auto_create_dead_letter_resources: bool = Self::DEFAULT_AUTO_CREATE_DEAD_LETTER_RESOURCES;
    auto_create_expiry_resources: bool = Self::DEFAULT_AUTO_CREATE_EXPIRY_RESOURCES;
    enable_metrics: bool = Self::DEFAULT_ENABLE_METRICS;
    management_message_attribute_size_limit: i32 = Self::MANAGEMENT_MESSAGE_ATTRIBUTE_SIZE_LIMIT;
    enable_ingress_timestamp: bool = Self::DEFAULT_ENABLE_INGRESS_TIMESTAMP;
    queue_prefetch: i32 = Self::DEFAULT_QUEUE_PREFETCH;
}

impl AddressSettings {
    /// Effective `prefetch_page_messages`: falls back to
    /// [`max_read_page_messages`](Self::max_read_page_messages).
    pub fn prefetch_page_messages(&self) -> i32 {
        self.prefetch_page_messages
            .unwrap_or_else(|| self.max_read_page_messages())
    }

    /// Effective `max_read_page_bytes`: twice the page size when unset (int
    /// arithmetic, so it wraps as in Java).
    pub fn max_read_page_bytes(&self) -> i32 {
        self.max_read_page_bytes
            .unwrap_or_else(|| self.page_size_bytes().wrapping_mul(2))
    }

    /// Effective `prefetch_page_bytes`: falls back to
    /// [`max_read_page_bytes`](Self::max_read_page_bytes).
    pub fn prefetch_page_bytes(&self) -> i32 {
        self.prefetch_page_bytes
            .unwrap_or_else(|| self.max_read_page_bytes())
    }

    /// Effective `max_redelivery_delay`: ten times the redelivery delay when
    /// unset.
    pub fn max_redelivery_delay(&self) -> i64 {
        self.max_redelivery_delay
            .unwrap_or_else(|| self.redelivery_delay().wrapping_mul(10))
    }

    pub fn expiry_queue_prefix(&self) -> &str {
        self.expiry_queue_prefix
            .as_deref()
            .unwrap_or(Self::DEFAULT_EXPIRY_QUEUE_PREFIX)
    }

    pub fn expiry_queue_suffix(&self) -> &str {
        self.expiry_queue_suffix
            .as_deref()
            .unwrap_or(Self::DEFAULT_EXPIRY_QUEUE_SUFFIX)
    }

    pub fn dead_letter_queue_prefix(&self) -> &str {
        self.dead_letter_queue_prefix
            .as_deref()
            .unwrap_or(Self::DEFAULT_DEAD_LETTER_QUEUE_PREFIX)
    }

    pub fn dead_letter_queue_suffix(&self) -> &str {
        self.dead_letter_queue_suffix
            .as_deref()
            .unwrap_or(Self::DEFAULT_DEAD_LETTER_QUEUE_SUFFIX)
    }

    // The remaining fields have no default in Java (the getter returns null),
    // so they are read directly: `dead_letter_address`, `expiry_address`,
    // `default_last_value_key`, `default_group_first_key`, `page_limit_bytes`,
    // `page_limit_messages`, `page_full_message_policy`,
    // `disk_full_message_policy`, `id_cache_size`, `initial_queue_buffer_size`.
}

impl AsSettingValue for RoutingType {
    const KIND: &'static str = "string";
    const CHOICES: &'static [&'static str] = &["MULTICAST", "ANYCAST"];

    fn setting_value(&self) -> SettingValue {
        SettingValue::Text(
            match self {
                RoutingType::Multicast => "MULTICAST",
                RoutingType::Anycast => "ANYCAST",
            }
            .to_owned(),
        )
    }
}
