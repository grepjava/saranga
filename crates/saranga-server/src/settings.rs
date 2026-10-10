//! Address settings, resolved for one address. Ported from the subset of
//! `AddressSettings` that the broker acts on; the values and defaults are
//! Artemis's.

use std::sync::Arc;

use crate::RoutingType;

/// The effective settings for an address (`AddressSettings` after
/// `HierarchicalRepository.getMatch`), with defaults applied.
#[derive(Debug, Clone, PartialEq)]
pub struct AddressSettings {
    pub auto_create_queues: bool,
    pub auto_delete_queues: bool,
    pub auto_delete_created_queues: bool,
    pub auto_delete_queues_delay: i64,
    pub auto_delete_queues_message_count: i64,
    pub auto_create_addresses: bool,
    pub auto_delete_addresses: bool,
    pub auto_delete_addresses_delay: i64,
    pub default_purge_on_no_consumers: bool,
    pub default_max_consumers: i32,
    pub default_exclusive_queue: bool,
    pub default_last_value_queue: bool,
    pub default_last_value_key: Option<String>,
    pub default_non_destructive: bool,
    pub default_consumers_before_dispatch: i32,
    pub default_delay_before_dispatch: i64,
    pub default_consumer_window_size: i32,
    pub default_group_rebalance: bool,
    pub default_group_rebalance_pause_dispatch: bool,
    pub default_group_buckets: i32,
    pub default_group_first_key: Option<String>,
    pub default_ring_size: i64,
    pub default_address_routing_type: RoutingType,
    pub default_queue_routing_type: RoutingType,
    pub max_delivery_attempts: i32,
    pub dead_letter_address: Option<String>,
    pub expiry_address: Option<String>,
    pub redelivery_delay: i64,
    pub redelivery_multiplier: f64,
    pub redelivery_collision_avoidance_factor: f64,
    pub max_redelivery_delay: i64,
    pub expiry_delay: i64,
    pub min_expiry_delay: i64,
    pub max_expiry_delay: i64,
    pub no_expiry: bool,
    pub send_to_dla_on_no_route: bool,
    pub auto_create_dead_letter_resources: bool,
    pub dead_letter_queue_prefix: String,
    pub dead_letter_queue_suffix: String,
    pub auto_create_expiry_resources: bool,
    pub expiry_queue_prefix: String,
    pub expiry_queue_suffix: String,
    /// `id-cache-size` for this address; the broker's when unset.
    pub id_cache_size: Option<i32>,
    /// `address-full-policy`.
    pub address_full_policy: AddressFullPolicy,
    /// `max-size-bytes`; -1 for no limit of the address's own.
    pub max_size_bytes: i64,
    /// `max-size-messages`; -1 for no limit.
    pub max_size_messages: i64,
    /// `page-size-bytes`.
    pub page_size_bytes: i64,
    /// `max-read-page-bytes`: paged bytes a queue holds in memory.
    pub max_read_page_bytes: i64,
    /// `max-read-page-messages`.
    pub max_read_page_messages: i64,
    /// `prefetch-page-bytes`.
    pub prefetch_page_bytes: i64,
    /// `prefetch-page-messages`.
    pub prefetch_page_messages: i64,
    /// `page-limit-bytes`: the pages an address may hold, in bytes.
    pub page_limit_bytes: Option<i64>,
    /// `page-limit-messages`: the paged messages a queue may have.
    pub page_limit_messages: Option<i64>,
    /// `page-full-policy`: what a paging address does past its limits.
    pub page_full_policy: Option<PageFullPolicy>,
}

/// `PageFullMessagePolicy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageFullPolicy {
    Drop,
    Fail,
}

/// `AddressFullMessagePolicy`: what an address does at its size limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AddressFullPolicy {
    /// New messages go to page files.
    #[default]
    Page,
    /// Producers get no more credits until memory is freed.
    Block,
    /// Sends fail.
    Fail,
    /// New messages are dropped.
    Drop,
}

impl AddressSettings {
    /// `DEFAULT_MIN_EXPIRY_DELAY` / `DEFAULT_MAX_EXPIRY_DELAY`: unset.
    pub const NO_EXPIRY_DELAY: i64 = -1;
}

impl Default for AddressSettings {
    /// The `AddressSettings` / `ActiveMQDefaultConfiguration` defaults.
    fn default() -> Self {
        Self {
            auto_create_queues: true,
            auto_delete_queues: true,
            auto_delete_created_queues: false,
            auto_delete_queues_delay: 0,
            auto_delete_queues_message_count: 0,
            auto_create_addresses: true,
            auto_delete_addresses: true,
            auto_delete_addresses_delay: 0,
            default_purge_on_no_consumers: false,
            default_max_consumers: -1,
            default_exclusive_queue: false,
            default_last_value_queue: false,
            default_last_value_key: None,
            default_non_destructive: false,
            default_consumers_before_dispatch: 0,
            default_delay_before_dispatch: -1,
            default_consumer_window_size: 1024 * 1024,
            default_group_rebalance: false,
            default_group_rebalance_pause_dispatch: false,
            default_group_buckets: -1,
            default_group_first_key: None,
            default_ring_size: -1,
            default_address_routing_type: RoutingType::Multicast,
            default_queue_routing_type: RoutingType::Multicast,
            max_delivery_attempts: 10,
            dead_letter_address: None,
            expiry_address: None,
            redelivery_delay: 0,
            redelivery_multiplier: 1.0,
            redelivery_collision_avoidance_factor: 0.0,
            max_redelivery_delay: 0,
            expiry_delay: -1,
            min_expiry_delay: -1,
            max_expiry_delay: -1,
            no_expiry: false,
            send_to_dla_on_no_route: false,
            auto_create_dead_letter_resources: false,
            dead_letter_queue_prefix: "DLQ.".into(),
            dead_letter_queue_suffix: String::new(),
            auto_create_expiry_resources: false,
            expiry_queue_prefix: "EXP.".into(),
            expiry_queue_suffix: String::new(),
            id_cache_size: None,
            address_full_policy: AddressFullPolicy::Page,
            max_size_bytes: -1,
            max_size_messages: -1,
            page_size_bytes: 10 * 1024 * 1024,
            max_read_page_bytes: 20 * 1024 * 1024,
            max_read_page_messages: -1,
            prefetch_page_bytes: 20 * 1024 * 1024,
            prefetch_page_messages: -1,
            page_limit_bytes: None,
            page_limit_messages: None,
            page_full_policy: None,
        }
    }
}

/// Resolves the settings for an address (`HierarchicalRepository`).
pub trait SettingsRepository: Send + Sync {
    fn get_match(&self, address: &str) -> Arc<AddressSettings>;
}

/// One set of settings for every address.
pub struct UniformSettings(pub Arc<AddressSettings>);

impl SettingsRepository for UniformSettings {
    fn get_match(&self, _address: &str) -> Arc<AddressSettings> {
        self.0.clone()
    }
}
