//! Builds the broker's configuration from a parsed `broker.xml`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use saranga_config as cfg;

use crate::postoffice::{FilterFactory, QueueSpec, RoutingTypes};
use crate::settings::{AddressSettings, SettingsRepository};
use crate::wildcard::WildcardConfig;
use crate::{AddressDeployment, BrokerConfig, RoutingType};

/// Address settings resolved through `<address-settings>` match rules,
/// cached per address (the settings do not change while the broker runs).
pub struct ConfiguredSettings {
    repository: cfg::HierarchicalRepository<cfg::AddressSettings>,
    cache: Mutex<HashMap<String, Arc<AddressSettings>>>,
}

impl ConfiguredSettings {
    pub fn new(repository: cfg::HierarchicalRepository<cfg::AddressSettings>) -> Self {
        Self {
            repository,
            cache: Mutex::default(),
        }
    }
}

impl SettingsRepository for ConfiguredSettings {
    fn get_match(&self, address: &str) -> Arc<AddressSettings> {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(s) = cache.get(address) {
            return s.clone();
        }
        let s = Arc::new(
            self.repository
                .get_match(address)
                .map(|s| resolve(&s))
                .unwrap_or_default(),
        );
        cache.insert(address.to_owned(), s.clone());
        s
    }
}

fn routing_type(t: cfg::RoutingType) -> RoutingType {
    match t {
        cfg::RoutingType::Multicast => RoutingType::Multicast,
        cfg::RoutingType::Anycast => RoutingType::Anycast,
    }
}

/// The effective values the broker uses, with Artemis's defaults.
fn resolve(s: &cfg::AddressSettings) -> AddressSettings {
    AddressSettings {
        auto_create_queues: s.auto_create_queues(),
        auto_delete_queues: s.auto_delete_queues(),
        auto_delete_created_queues: s.auto_delete_created_queues(),
        auto_delete_queues_delay: s.auto_delete_queues_delay(),
        auto_delete_queues_message_count: s.auto_delete_queues_message_count(),
        auto_create_addresses: s.auto_create_addresses(),
        auto_delete_addresses: s.auto_delete_addresses(),
        auto_delete_addresses_delay: s.auto_delete_addresses_delay(),
        default_purge_on_no_consumers: s.default_purge_on_no_consumers(),
        default_max_consumers: s.default_max_consumers(),
        default_exclusive_queue: s.default_exclusive_queue(),
        default_last_value_queue: s.default_last_value_queue(),
        default_last_value_key: s.default_last_value_key.clone(),
        default_non_destructive: s.default_non_destructive(),
        default_consumers_before_dispatch: s.default_consumers_before_dispatch(),
        default_delay_before_dispatch: s.default_delay_before_dispatch(),
        default_consumer_window_size: s.default_consumer_window_size(),
        default_group_rebalance: s.default_group_rebalance(),
        default_group_rebalance_pause_dispatch: s.default_group_rebalance_pause_dispatch(),
        default_group_buckets: s.default_group_buckets(),
        default_group_first_key: s.default_group_first_key.clone(),
        default_ring_size: s.default_ring_size(),
        default_address_routing_type: routing_type(s.default_address_routing_type()),
        default_queue_routing_type: routing_type(s.default_queue_routing_type()),
        max_delivery_attempts: s.max_delivery_attempts(),
        dead_letter_address: s.dead_letter_address.clone(),
        expiry_address: s.expiry_address.clone(),
    }
}

/// `<addresses>`: queues declared in configuration are
/// `configuration-managed`.
fn deployment(a: &cfg::AddressConfig) -> AddressDeployment {
    let mut routing_types = RoutingTypes::default();
    a.routing_types
        .iter()
        .for_each(|&t| routing_types.add(routing_type(t)));
    let queues = a
        .queues
        .iter()
        .map(|q| {
            let mut spec = QueueSpec::new(q.name.as_str(), a.name.as_str());
            spec.routing_type = Some(routing_type(q.routing_type));
            spec.filter_string = q.filter.clone();
            spec.durable = q.durable;
            spec.user = q.user.clone();
            spec.configuration_managed = true;
            spec.max_consumers = q.max_consumers;
            spec.purge_on_no_consumers = Some(q.purge_on_no_consumers);
            spec.exclusive = q.exclusive;
            spec.group_rebalance = q.group_rebalance;
            spec.group_rebalance_pause_dispatch = q.group_rebalance_pause_dispatch;
            spec.group_buckets = q.group_buckets;
            spec.group_first_key = q.group_first_key.clone();
            spec.last_value = q.last_value;
            spec.last_value_key = q.last_value_key.clone();
            spec.non_destructive = q.non_destructive;
            spec.consumers_before_dispatch = q.consumers_before_dispatch;
            spec.delay_before_dispatch = q.delay_before_dispatch;
            spec.ring_size = Some(q.ring_size);
            spec.enabled = q.enabled;
            spec
        })
        .collect();
    AddressDeployment {
        name: a.name.clone(),
        routing_types,
        queues,
    }
}

impl BrokerConfig {
    /// The broker configuration a `broker.xml` describes.
    pub fn from_file_config(
        config: &cfg::BrokerConfig,
        filters: Arc<dyn FilterFactory>,
    ) -> cfg::Result<Self> {
        let w = &config.wildcard_addresses;
        Ok(Self {
            settings: Arc::new(ConfiguredSettings::new(
                config.address_settings_repository()?,
            )),
            filters,
            wildcard: WildcardConfig {
                routing_enabled: w.routing_enabled,
                delimiter: w.delimiter,
                any_words: w.any_words,
                single_word: w.single_word,
            },
            connection_ttl_override: -1,
            addresses: config.addresses.iter().map(deployment).collect(),
        })
    }
}
