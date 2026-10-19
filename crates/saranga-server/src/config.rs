//! Builds the broker's configuration from a parsed `broker.xml`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use saranga_journal::JournalConfig;

use saranga_config as cfg;

use crate::postoffice::{
    DivertDeployment, DivertRoutingType, FilterFactory, QueueSpec, RoutingTypes,
};
use crate::settings::{
    AddressFullPolicy, AddressSettings, DiskFullPolicy, PageFullPolicy, SettingsRepository,
};
use crate::storage::StorageConfig;
use crate::wildcard::WildcardConfig;
use crate::{AddressDeployment, BrokerConfig, RoutingType};

/// Address settings resolved through `<address-settings>` match rules,
/// cached per address until the rules are swapped.
pub struct ConfiguredSettings {
    state: Mutex<Rules>,
}

struct Rules {
    repository: cfg::HierarchicalRepository<cfg::AddressSettings>,
    cache: HashMap<String, Arc<AddressSettings>>,
    /// Matches management added, kept over a configuration reload.
    added: Vec<(String, cfg::AddressSettings)>,
}

impl ConfiguredSettings {
    pub fn new(repository: cfg::HierarchicalRepository<cfg::AddressSettings>) -> Self {
        Self {
            state: Mutex::new(Rules {
                repository,
                cache: HashMap::new(),
                added: Vec::new(),
            }),
        }
    }
}

impl SettingsRepository for ConfiguredSettings {
    fn get_match(&self, address: &str) -> Arc<AddressSettings> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(s) = state.cache.get(address) {
            return s.clone();
        }
        let s = Arc::new(
            state
                .repository
                .get_match(address)
                .map(|s| resolve(&s))
                .unwrap_or_default(),
        );
        state.cache.insert(address.to_owned(), s.clone());
        s
    }

    fn swap(&self, rules: cfg::HierarchicalRepository<cfg::AddressSettings>) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // Matches added through management outlive a reload.
        let mut rules = rules;
        for (m, s) in &state.added {
            let _ = rules.add_match(m, s.clone());
        }
        state.repository = rules;
        state.cache.clear();
        true
    }

    fn configured(&self, address: &str) -> Option<cfg::AddressSettings> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.repository.get_match(address)
    }

    fn add_match(&self, m: &str, settings: cfg::AddressSettings) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.repository.add_match(m, settings.clone()).is_err() {
            return false;
        }
        state.added.retain(|(k, _)| k != m);
        state.added.push((m.to_owned(), settings));
        state.cache.clear();
        true
    }

    fn remove_match(&self, m: &str) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.repository.remove_match(m);
        state.added.retain(|(k, _)| k != m);
        state.cache.clear();
        true
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
        auto_delete_queues_skip_usage_check: s.auto_delete_queues_skip_usage_check(),
        auto_create_addresses: s.auto_create_addresses(),
        auto_delete_addresses: s.auto_delete_addresses(),
        auto_delete_addresses_delay: s.auto_delete_addresses_delay(),
        auto_delete_addresses_skip_usage_check: s.auto_delete_addresses_skip_usage_check(),
        config_delete_queues: s.config_delete_queues() == cfg::DeletionPolicy::Force,
        config_delete_addresses: s.config_delete_addresses() == cfg::DeletionPolicy::Force,
        config_delete_diverts: s.config_delete_diverts() == cfg::DeletionPolicy::Force,
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
        redelivery_delay: s.redelivery_delay(),
        redelivery_multiplier: s.redelivery_multiplier(),
        redelivery_collision_avoidance_factor: s.redelivery_collision_avoidance_factor(),
        max_redelivery_delay: s.max_redelivery_delay(),
        expiry_delay: s.expiry_delay(),
        min_expiry_delay: s.min_expiry_delay(),
        max_expiry_delay: s.max_expiry_delay(),
        no_expiry: s.no_expiry(),
        send_to_dla_on_no_route: s.send_to_dla_on_no_route(),
        auto_create_dead_letter_resources: s.auto_create_dead_letter_resources(),
        dead_letter_queue_prefix: s.dead_letter_queue_prefix().to_owned(),
        dead_letter_queue_suffix: s.dead_letter_queue_suffix().to_owned(),
        auto_create_expiry_resources: s.auto_create_expiry_resources(),
        expiry_queue_prefix: s.expiry_queue_prefix().to_owned(),
        expiry_queue_suffix: s.expiry_queue_suffix().to_owned(),
        id_cache_size: s.id_cache_size,
        address_full_policy: match s.address_full_message_policy() {
            cfg::AddressFullMessagePolicy::Page => AddressFullPolicy::Page,
            cfg::AddressFullMessagePolicy::Block => AddressFullPolicy::Block,
            cfg::AddressFullMessagePolicy::Fail => AddressFullPolicy::Fail,
            cfg::AddressFullMessagePolicy::Drop => AddressFullPolicy::Drop,
        },
        disk_full_policy: match s.disk_full_message_policy {
            Some(cfg::DiskFullMessagePolicy::Fail) => DiskFullPolicy::Fail,
            Some(cfg::DiskFullMessagePolicy::Drop) => DiskFullPolicy::Drop,
            Some(cfg::DiskFullMessagePolicy::Block) | None => DiskFullPolicy::Block,
        },
        max_size_bytes: s.max_size_bytes(),
        max_size_bytes_reject_threshold: s.max_size_bytes_reject_threshold(),
        max_size_messages: s.max_size_messages(),
        page_size_bytes: i64::from(s.page_size_bytes()),
        max_read_page_bytes: i64::from(s.max_read_page_bytes()),
        max_read_page_messages: i64::from(s.max_read_page_messages()),
        prefetch_page_bytes: i64::from(s.prefetch_page_bytes()),
        prefetch_page_messages: i64::from(s.prefetch_page_messages()),
        page_limit_bytes: s.page_limit_bytes,
        page_limit_messages: s.page_limit_messages,
        page_full_policy: s.page_full_message_policy.map(|p| match p {
            cfg::PageFullMessagePolicy::Drop => PageFullPolicy::Drop,
            cfg::PageFullMessagePolicy::Fail => PageFullPolicy::Fail,
        }),
        management_browse_page_size: s.management_browse_page_size(),
        slow_consumer_threshold: s.slow_consumer_threshold(),
        slow_consumer_threshold_measurement_unit: s.slow_consumer_threshold_measurement_unit(),
        slow_consumer_check_period: s.slow_consumer_check_period(),
        slow_consumer_policy: s.slow_consumer_policy(),
        enable_metrics: s.enable_metrics(),
        redistribution_delay: s.redistribution_delay(),
        management_message_attribute_size_limit: s.management_message_attribute_size_limit(),
    }
}

/// `<addresses>`: queues declared in configuration are
/// `configuration-managed`.
pub(crate) fn deployment(a: &cfg::AddressConfig) -> AddressDeployment {
    let mut routing_types = RoutingTypes::default();
    a.routing_types
        .iter()
        .for_each(|&t| routing_types.add(routing_type(t)));
    let queues = a.queues.iter().map(|q| queue_spec(q, &a.name)).collect();
    AddressDeployment {
        name: a.name.clone(),
        routing_types,
        queues,
    }
}

/// A queue of the configuration, on `address`.
pub(crate) fn queue_spec(q: &cfg::QueueConfig, address: &str) -> QueueSpec {
    let mut spec = QueueSpec::new(q.name.as_str(), address);
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
}

/// The deprecated `<queues>` of `<core>`, each on its own address.
pub(crate) fn legacy_queues(config: &cfg::BrokerConfig) -> Vec<QueueSpec> {
    config
        .queues
        .iter()
        .map(|q| queue_spec(q, &q.address))
        .collect()
}

pub(crate) fn divert(d: &cfg::DivertConfig) -> DivertDeployment {
    DivertDeployment {
        name: d.name.clone(),
        routing_name: d.routing_name.clone(),
        address: d.address.clone(),
        forwarding_address: d.forwarding_address.clone(),
        exclusive: d.exclusive,
        filter: d.filter.clone(),
        transformer: d
            .transformer
            .as_ref()
            .map(|t| (t.class_name.clone(), t.properties.iter().cloned().collect())),
        routing_type: match d.routing_type {
            cfg::DivertRoutingType::Multicast => DivertRoutingType::Multicast,
            cfg::DivertRoutingType::Anycast => DivertRoutingType::Anycast,
            cfg::DivertRoutingType::Strip => DivertRoutingType::Strip,
            cfg::DivertRoutingType::Pass => DivertRoutingType::Pass,
        },
    }
}

/// The journals' settings. Relative directories are resolved against the
/// broker instance (`artemis.instance`), as Artemis does.
pub fn storage_config(config: &cfg::BrokerConfig, instance: &Path) -> StorageConfig {
    let j = &config.journal;
    type J = cfg::JournalConfig;
    let dir = |d: &str| {
        let p = Path::new(d);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            instance.join(p)
        }
    };
    let datasync = j.datasync.unwrap_or(J::DEFAULT_DATASYNC);
    let pool_size = j.pool_files.unwrap_or(J::DEFAULT_POOL_FILES);
    let compact_min_files = j
        .compact_min_files
        .unwrap_or(J::DEFAULT_COMPACT_MIN_FILES)
        .max(0) as usize;
    let compact_percentage = j
        .compact_percentage
        .unwrap_or(J::DEFAULT_COMPACT_PERCENTAGE)
        .clamp(0, 100) as u32;
    let max_attic_files = j
        .max_attic_files
        .unwrap_or(J::DEFAULT_MAX_ATTIC_FILES)
        .max(0) as usize;
    StorageConfig {
        messages: JournalConfig {
            file_size: j.file_size.unwrap_or(J::DEFAULT_FILE_SIZE).max(1024) as usize,
            min_files: j.min_files.unwrap_or(J::DEFAULT_MIN_FILES).max(2) as usize,
            pool_size,
            compact_min_files,
            compact_percentage,
            datasync,
            buffer_size: j.buffer_size.unwrap_or(J::DEFAULT_BUFFER_SIZE).max(0) as usize,
            max_attic_files,
            retention: j.retention.as_ref().map(|r| saranga_journal::Retention {
                directory: dir(&r.directory),
                max_bytes: r.max_bytes,
                period_millis: r.period_millis,
                local_offset: crate::localtime::offset_millis,
            }),
            ..JournalConfig::messages(dir(&config.journal_directory))
        },
        bindings: JournalConfig {
            pool_size,
            compact_min_files,
            compact_percentage,
            datasync,
            max_attic_files,
            ..JournalConfig::bindings(dir(&config.bindings_directory))
        },
        sync_transactional: j
            .sync_transactional
            .unwrap_or(J::DEFAULT_SYNC_TRANSACTIONAL),
        sync_non_transactional: j
            .sync_non_transactional
            .unwrap_or(J::DEFAULT_SYNC_NON_TRANSACTIONAL),
        max_redelivery_records: j
            .max_redelivery_records
            .unwrap_or(J::DEFAULT_MAX_REDELIVERY_RECORDS),
        large_messages: dir(&config.large_messages_directory),
        paging: dir(&config.paging_directory),
        large_message_sync: config.large_message_sync,
    }
}

/// `<broker-plugins>`: Saranga's ports of Artemis's own plugins. Another
/// class is Java code Saranga cannot run, and starting without it could
/// leave out what it enforces, so the broker does not start.
fn plugins(config: &cfg::BrokerConfig) -> cfg::Result<Vec<Arc<dyn crate::plugin::BrokerPlugin>>> {
    config
        .broker_plugins
        .iter()
        .map(|p| {
            let invalid = |reason: String| cfg::ConfigError::InvalidValue {
                name: "broker-plugin class-name".to_owned(),
                value: p.class_name.clone(),
                reason,
            };
            match crate::plugin::from_config(&p.class_name, &p.properties) {
                Some(Ok(plugin)) => Ok(plugin),
                Some(Err(e)) => Err(invalid(e)),
                None => Err(invalid(
                    "Saranga has no port of this plugin, and cannot run Java plugins".to_owned(),
                )),
            }
        })
        .collect()
}

impl BrokerConfig {
    /// The broker configuration a `broker.xml` describes, with the broker
    /// instance directory its relative data directories are under.
    pub fn from_file_config(
        config: &cfg::BrokerConfig,
        filters: Arc<dyn FilterFactory>,
        instance: &Path,
    ) -> cfg::Result<Self> {
        crate::discovery::validate_startup(config).map_err(cfg::ConfigError::IllegalState)?;
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
            // FileBroker deploys the legacy <jms> configuration after the
            // core's addresses.
            addresses: config
                .addresses
                .iter()
                .chain(&config.jms_addresses)
                .map(deployment)
                .collect(),
            legacy_queues: legacy_queues(config),
            diverts: config.diverts.iter().map(divert).collect(),
            transformers: HashMap::new(),
            storage: config
                .persistence_enabled
                .then(|| storage_config(config, instance)),
            message_expiry_scan_period: config.message_expiry_scan_period,
            transaction_timeout: config.transaction_timeout,
            transaction_timeout_scan_period: config.transaction_timeout_scan_period,
            address_queue_scan_period: config.address_queue_scan_period,
            id_cache_size: config.id_cache_size,
            persist_id_cache: config.persist_id_cache,
            // When unset, a share of the heap of the JVM Artemis would run
            // in (`ConfigurationImpl.getGlobalMaxSize`): 1 GiB with the 2
            // GiB of an instance `artemis create` makes.
            global_max_size: config.global_max_size.unwrap_or_else(|| {
                crate::jvm::percent_of(
                    crate::jvm::max_memory(),
                    config.global_max_size_percent_of_jvm_max_memory,
                )
            }),
            global_max_messages: config.global_max_messages,
            // The security manager comes from outside broker.xml
            // (`login.config`, `bootstrap.xml`): whoever starts the broker
            // sets it.
            security: None,
            populate_validated_user: config.populate_validated_user,
            reject_empty_validated_user: config.reject_empty_validated_user,
            mirror_ack_manager: crate::mirror::AckManagerSettings {
                queue_attempts: config.mirror_ack_manager_queue_attempts,
                page_attempts: config.mirror_ack_manager_page_attempts,
                retry_delay: i64::from(config.mirror_ack_manager_retry_delay),
                warn_unacked: config.mirror_ack_manager_warn_unacked,
            },
            users: None,
            bootstrap_users: Vec::new(),
            // Protocol crates register themselves: whoever starts the
            // broker adds them.
            protocols: Vec::new(),
            critical: crate::critical::CriticalConfig::from_file(config),
            ha: crate::node::HaConfig::from_file(config, instance),
            clusters: crate::cluster::ClusterConfig::all_from_file(config),
            broadcast_groups: crate::discovery::broadcast_groups(config),
            bridges: config.bridges.clone(),
            grouping_handler: config.grouping_handler.clone(),
            federations: config.federations.clone(),
            federation_downstream_authorization: config.federation_downstream_authorization.clone(),
            bridge_targets: crate::bridge::Targets::from_file(config),
            plugins: plugins(config)?,
            disk_monitor: crate::disk::MonitorConfig::new(
                config.journal.max_disk_usage.unwrap_or(90),
                config.journal.min_disk_free.unwrap_or(-1),
                config.journal.disk_scan_period.map_or(5000, i64::from),
            ),
            management: crate::management::ManagementConfig {
                address: config.management_address.clone(),
                notification_address: config.management_notification_address.clone(),
                suppress_session_notifications: config.suppress_session_notifications,
                message_counter_enabled: config.message_counter_enabled,
                message_counter_sample_period: config.message_counter_sample_period,
                message_counter_max_day_history: config.message_counter_max_day_history,
                rbac_prefix: config
                    .management_message_rbac
                    .then(|| config.management_rbac_prefix.clone()),
                view_permission: crate::management::view_permission(
                    &config.view_permission_method_match_pattern,
                ),
                name: config.name.clone(),
                file: Some(Arc::new(config.clone())),
                file_path: None,
            },
        })
    }
}
