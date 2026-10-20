//! `FileConfigurationParser.parseMainConfig`: `<core>` into a
//! [`Configuration`], in the order Artemis reads it, logging what it logs.

use roxmltree::Node;

use super::*;
use crate::acceptor::transport_configurations;
use crate::mask;
use crate::parser::{
    Check, MaskSettings, Settings, all, bool_attribute_or, elements, first, int_attribute_or,
    mask_error, node_bool, node_i32, parse_num, text_content, trimmed_text,
};
use crate::{ConfigError, Result, java_trim};

/// The logger `ActiveMQServerLogger` logs under.
const SERVER_LOGGER: &str = "org.apache.activemq.artemis.core.server";

/// `DefaultSensitiveStringCodec`'s class name: the default `password-codec`.
const DEFAULT_CODEC: &str = "org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec";

impl MaskSettings for Configuration {
    fn mask_password(&self) -> Option<bool> {
        self.mask_password
    }

    fn password_codec(&self) -> &str {
        self.password_codec.as_deref().unwrap_or(DEFAULT_CODEC)
    }
}

/// `parseMainConfig(e, config)` on a new `ConfigurationImpl`, then
/// `setConfigurationUrl(url)` as `FileConfiguration.parse` does.
pub fn parse_main_config(core: Node<'_, '_>, url: Option<&str>) -> Result<Configuration> {
    let mut config = Configuration::default();
    Parser {
        page_max_size_used: false,
    }
    .parse(core, &mut config)?;
    config.configuration_url = url.map(str::to_owned);
    Ok(config)
}

struct Parser {
    /// `printPageMaxSizeUsed`: AMQ224117 is logged once.
    page_max_size_used: bool,
}

/// `getString(e, name, def, NOT_NULL_OR_EMPTY)`.
fn non_empty(s: &Settings<'_, '_>, name: &str, def: &str) -> Result<String> {
    Ok(s.non_empty_string(name)?.unwrap_or_else(|| def.to_owned()))
}

/// `getString(e, name, def, NO_CHECK)`: Java's `trim` of the text.
fn string_or(s: &Settings<'_, '_>, name: &str, def: Option<&str>) -> Option<String> {
    s.string(name).or_else(|| def.map(str::to_owned))
}

/// `getDouble` with `GT_ZERO`.
fn double_gt_zero(n: Node<'_, '_>, name: &str, def: f64) -> Result<f64> {
    match first(n, name) {
        Some(node) => {
            let text = trimmed_text(node);
            let v: f64 = parse_num(name, &text)?;
            if v > 0.0 {
                Ok(v)
            } else {
                Err(ConfigError::Runtime(format!(
                    "AMQ229039: {name}  must be greater than 0 (actual value: {})",
                    saranga_buffer::json::java_double(v)
                )))
            }
        }
        None => Ok(def),
    }
}

/// `getTextBytesAsIntBytes`: the bytes as a long, checked, then
/// `intValue()`.
fn text_bytes_int(s: &Settings<'_, '_>, name: &str, def: i32, check: Check) -> Result<i32> {
    Ok(s.text_bytes(name, check)?.map_or(def, |v| v as i32))
}

/// `getAttributeValue`.
fn attribute(n: Node<'_, '_>, name: &str) -> Option<String> {
    n.attribute(name).map(str::to_owned)
}

/// `Element.getAttribute`: `""` when absent.
fn attribute_or_empty(n: Node<'_, '_>, name: &str) -> String {
    n.attribute(name).unwrap_or_default().to_owned()
}

/// The child nodes' names, as `getChildNodes()` walks them: elements only
/// matter to the parser.
fn children<'a, 'input>(n: Node<'a, 'input>) -> impl Iterator<Item = Node<'a, 'input>> {
    elements(n)
}

/// `getMapOfChildPropertyElements`: each `<property key value>` child.
fn child_properties(n: Node<'_, '_>) -> JavaMap<String> {
    let mut map = JavaMap::default();
    for child in children(n).filter(|c| c.tag_name().name() == "property") {
        map.put(
            attribute_or_empty(child, "key"),
            attribute_or_empty(child, "value"),
        );
    }
    map
}

/// `getStaticConnectors`: each `<connector-ref>` below, its first child's
/// text as written.
fn static_connectors(n: Node<'_, '_>) -> Vec<String> {
    all(n, "connector-ref")
        .map(|r| {
            r.first_child()
                .and_then(|c| c.text())
                .unwrap_or_default()
                .to_owned()
        })
        .collect()
}

/// `PasswordMaskingUtil.resolveMask(maskPassword, text, codec)`.
fn resolve(config: &Configuration, name: &str, text: &str) -> Result<String> {
    mask::resolve_mask(config.mask_password(), text, Some(config.password_codec()))
        .map_err(|e| mask_error(name, text, e))
}

fn deprecated(option: &str, alternative: &str) {
    tracing::info!(
        target: SERVER_LOGGER,
        "AMQ221038: Configuration option '{option}' is deprecated and will be removed in a future version. Use '{alternative}' instead. Consult the manual for details."
    );
}

impl Parser {
    fn parse(&mut self, e: Node<'_, '_>, config: &mut Configuration) -> Result<()> {
        let s = Settings { core: e };
        config.name = string_or(&s, "name", Some(&config.name)).unwrap_or_default();
        config.system_property_prefix =
            non_empty(&s, "system-property-prefix", &config.system_property_prefix)?;

        if let Some(ha) = first(e, "ha-policy") {
            parse_ha_policy(ha, config)?;
        }
        if config.ha_policy_configuration.is_none() {
            config.ha_policy_configuration = Some(HaPolicyConfiguration::PrimaryOnly {
                scale_down_configuration: None,
            });
        }

        if let Some(v) = s.bool("resolve-protocols") {
            config.resolve_protocols = v;
        }
        if let Some(v) = s.bool("persistence-enabled") {
            config.persistence_enabled = v;
        }
        if let Some(v) = s.bool("persist-delivery-count-before-delivery") {
            config.persist_delivery_count_before_delivery = v;
        }
        if let Some(v) = s.i32("max-redelivery-records", Check::MinusOneOrGeZero)? {
            config.max_redelivery_records = v;
        }
        if let Some(v) = s.i32("scheduled-thread-pool-max-size", Check::GtZero)? {
            config.scheduled_thread_pool_max_size = v;
        }
        if let Some(v) = s.i32("thread-pool-max-size", Check::MinusOneOrGtZero)? {
            config.thread_pool_max_size = v;
        }
        if let Some(v) = s.bool("security-enabled") {
            config.security_enabled = v;
        }
        if let Some(v) = s.bool("graceful-shutdown-enabled") {
            config.graceful_shutdown_enabled = v;
        }
        if let Some(v) = s.i64("graceful-shutdown-timeout", Check::MinusOneOrGeZero)? {
            config.graceful_shutdown_timeout = v;
        }
        if let Some(v) = s.bool("jmx-management-enabled") {
            config.jmx_management_enabled = v;
        }
        if let Some(v) = s.bool("jmx-notification-enabled") {
            config.jmx_notification_enabled = v;
        }
        config.jmx_domain = non_empty(&s, "jmx-domain", &config.jmx_domain)?;
        if let Some(v) = s.bool("jmx-use-broker-name") {
            config.jmx_use_broker_name = v;
        }
        if let Some(v) = s.i64("security-invalidation-interval", Check::GeZero)? {
            config.security_invalidation_interval = v;
        }
        if let Some(v) = s.i64("authentication-cache-size", Check::GeZero)? {
            config.authentication_cache_size = v;
        }
        if let Some(v) = s.i64("authorization-cache-size", Check::GeZero)? {
            config.authorization_cache_size = v;
        }
        if let Some(v) = s.i64("connection-ttl-override", Check::MinusOneOrGtZero)? {
            config.connection_ttl_override = v;
        }
        if let Some(v) = s.bool("async-connection-execution-enabled") {
            config.async_connection_execution_enabled = v;
        }
        if let Some(v) = s.i64("transaction-timeout", Check::GtZero)? {
            config.transaction_timeout = v;
        }
        if let Some(v) = s.i64("transaction-timeout-scan-period", Check::GtZero)? {
            config.transaction_timeout_scan_period = v;
        }
        if let Some(v) = s.i64("message-expiry-scan-period", Check::MinusOneOrGtZero)? {
            config.message_expiry_scan_period = v;
        }
        if let Some(v) = s.i64("address-queue-scan-period", Check::MinusOneOrGtZero)? {
            config.address_queue_scan_period = v;
        }
        if let Some(v) = s.i32("id-cache-size", Check::GeZero)? {
            config.id_cache_size = v;
        }
        if let Some(v) = s.bool("persist-id-cache") {
            config.persist_id_cache = v;
        }
        config.management_address =
            non_empty(&s, "management-address", &config.management_address)?;
        config.management_notification_address = non_empty(
            &s,
            "management-notification-address",
            &config.management_notification_address,
        )?;
        config.mask_password = s.bool("mask-password");
        config.password_codec = Some(non_empty(&s, "password-codec", DEFAULT_CODEC)?);
        if let Some(v) = s.bool("populate-validated-user") {
            config.populate_validated_user = v;
        }
        if let Some(v) = s.bool("reject-empty-validated-user") {
            config.reject_empty_validated_user = v;
        }
        if let Some(v) = s.i64("connection-ttl-check-interval", Check::GtZero)? {
            config.connection_ttl_check_interval = v;
        }
        if let Some(v) = s.i64("configuration-file-refresh-period", Check::MinusOneOrGtZero)? {
            config.configuration_file_refresh_period = v;
        }
        // getTemporaryQueueNamespace() is getUuidNamespace().
        let temporary = non_empty(&s, "temporary-queue-namespace", &config.uuid_namespace)?;
        config.uuid_namespace = temporary;
        config.uuid_namespace = non_empty(&s, "uuid-namespace", &config.uuid_namespace)?;
        if let Some(v) = s.i64("mqtt-session-scan-interval", Check::GtZero)? {
            config.mqtt_session_scan_interval = v;
        }
        if let Some(v) = s.i64("mqtt-session-state-persistence-timeout", Check::GtZero)? {
            deprecated(
                "mqtt-session-state-persistence-timeout",
                "mqtt-subscription-persistence-enabled",
            );
            config.mqtt_session_state_persistence_timeout = v;
        }
        if let Some(v) = s.bool("mqtt-subscription-persistence-enabled") {
            config.mqtt_subscription_persistence_enabled = v;
        }
        if let Some(v) = s.i32("global-max-size-percent-of-jvm-max-memory", Check::GtZero)? {
            config.global_max_size_percent_of_jvm_max_memory = v;
        }
        let global_max_size = s
            .text_bytes("global-max-size", Check::MinusOneOrGtZero)?
            .unwrap_or(-1);
        if global_max_size > 0 {
            config.global_max_size = Some(global_max_size);
        }
        config.global_max_messages = Some(
            s.i64("global-max-messages", Check::MinusOneOrGtZero)?
                .unwrap_or(-1),
        );
        if let Some(v) = s.text_bytes("min-disk-free", Check::MinusOneOrGtZero)? {
            config.min_disk_free = v;
        }
        if let Some(v) = s.i32("max-disk-usage", Check::PercentageOrMinusOne)? {
            config.max_disk_usage = v;
        }
        if let Some(v) = s.i32("disk-scan-period", Check::MinusOneOrGtZero)? {
            config.disk_scan_period = v;
        }
        config.internal_naming_prefix = string_or(
            &s,
            "internal-naming-prefix",
            Some(&config.internal_naming_prefix),
        )
        .unwrap_or_default();
        if let Some(v) = s.bool("amqp-use-core-subscription-naming") {
            config.amqp_use_core_subscription_naming = v;
        }

        if let Some(text) = s.string("cluster-password") {
            config.cluster_password = resolve(config, "cluster-password", &text)?;
        }
        config.cluster_user =
            string_or(&s, "cluster-user", Some(&config.cluster_user)).unwrap_or_default();

        if let Some(store) = first(e, "store") {
            parse_store(store, config)?;
        }

        // Interceptors: `remoting-interceptors` and the incoming ones
        // trimmed, the outgoing ones as written.
        let mut incoming = Vec::new();
        for list in ["remoting-interceptors", "remoting-incoming-interceptors"] {
            if let Some(n) = first(e, list) {
                for child in children(n) {
                    if child.tag_name().name().eq_ignore_ascii_case("class-name") {
                        incoming.push(trimmed_text(child));
                    }
                }
            }
        }
        config.incoming_interceptor_class_names = incoming;
        let mut outgoing = Vec::new();
        if let Some(n) = first(e, "remoting-outgoing-interceptors") {
            for child in children(n) {
                if child.tag_name().name().eq_ignore_ascii_case("class-name") {
                    outgoing.push(text_content(child));
                }
            }
        }
        config.outgoing_interceptor_class_names = outgoing;

        for node in all(e, "connector") {
            let connector = parse_connector(node, config)?;
            let Some(name) = connector.name.clone() else {
                tracing::warn!(
                    target: SERVER_LOGGER,
                    "AMQ222016: Cannot deploy a connector with no name specified."
                );
                continue;
            };
            if config.connector_configs.contains_key(&name) {
                tracing::warn!(
                    target: SERVER_LOGGER,
                    "AMQ222017: There is already a connector with name {name} deployed. This one will not be deployed."
                );
                continue;
            }
            config.connector_configs.put(name, connector);
        }

        for node in all(e, "acceptor") {
            let acceptor = parse_acceptor(node, config)?;
            // `HashSet.add`: an equal one is not added again.
            if !config.acceptor_configs.contains(&acceptor) {
                config.acceptor_configs.push(acceptor);
            }
        }

        for node in all(e, "broadcast-group") {
            let group = parse_broadcast_group(node)?;
            config.broadcast_group_configurations.push(group);
        }
        for node in all(e, "discovery-group") {
            let group = parse_discovery_group(node)?;
            if config
                .discovery_group_configurations
                .iter()
                .any(|g| g.name == group.name)
            {
                tracing::warn!(
                    target: SERVER_LOGGER,
                    "AMQ222019: There is already a discovery group with name {} deployed. This one will not be deployed.",
                    group.name
                );
            } else {
                config.discovery_group_configurations.push(group);
            }
        }

        for bridges in all(e, "bridges") {
            for node in children(bridges) {
                if node.tag_name().name().eq_ignore_ascii_case("bridge") {
                    let bridge = parse_bridge(node, config)?;
                    config.bridge_configurations.push(bridge);
                }
            }
        }

        for federations in all(e, "federations") {
            let roles = attribute_or_empty(federations, "downstream-authorization");
            if !roles.is_empty() {
                for role in roles.split(',') {
                    let role = java_trim(role);
                    if !role.is_empty() {
                        config.downstream_authorization.push(role.to_owned());
                    }
                }
            }
            for node in children(federations) {
                if node.tag_name().name().eq_ignore_ascii_case("federation") {
                    let federation = crate::parser::parse_federation(node, config)?;
                    config.federation_configurations.push(federation);
                }
            }
        }

        for node in all(e, "grouping-handler") {
            config.grouping_handler_configuration = Some(parse_grouping_handler(node)?);
        }

        for node in all(e, "cluster-connection") {
            if let Some(cluster) = parse_cluster_connection(node, config)? {
                config.cluster_configurations.push(cluster);
            }
        }
        for node in all(e, "cluster-connection-uri") {
            let name = attribute_or_empty(node, "name");
            let uri = attribute_or_empty(node, "address");
            config
                .cluster_configurations
                .push(cluster_connection_from_uri(&name, &uri)?);
        }

        // `getElementsByTagName` never gives null: every `amqp-connection`.
        for node in all(e, "amqp-connection") {
            let connection = crate::parser::parse_amqp_connection(node, config)?;
            // `parseURI`: a `ConnectorTransportConfigurationParser` without
            // the in-VM schema.
            let transports =
                transport_configurations(Some(&connection.name), &connection.uri, true, false)
                    .map_err(|e| uri_thrown(e, true))?;
            config.amqp_transport_configurations.push(
                transports
                    .into_iter()
                    .map(TransportConfiguration::from_uri)
                    .collect(),
            );
            config.amqp_broker_connect_configurations.push(connection);
        }

        for node in all(e, "divert") {
            config.divert_configurations.push(parse_divert(node)?);
        }

        for node in all(e, "connection-router") {
            config
                .connection_routers
                .push(parse_connection_router(node, config)?);
        }

        for list in all(e, "lock-coordinators") {
            for node in children(list) {
                if node
                    .tag_name()
                    .name()
                    .eq_ignore_ascii_case("lock-coordinator")
                {
                    config
                        .lock_coordinator_configurations
                        .push(parse_lock_coordinator(node)?);
                }
            }
        }

        // Persistence.
        config.large_messages_directory = non_empty(
            &s,
            "large-messages-directory",
            &config.large_messages_directory,
        )?;
        config.bindings_directory =
            non_empty(&s, "bindings-directory", &config.bindings_directory)?;
        if let Some(v) = s.bool("create-bindings-dir") {
            config.create_bindings_dir = v;
        }
        config.journal_directory = non_empty(&s, "journal-directory", &config.journal_directory)?;
        parse_journal_retention(e, config)?;
        config.node_manager_lock_directory = s.string("node-manager-lock-directory");
        if let Some(v) = s.i32("page-max-concurrent-io", Check::MinusOneOrGtZero)? {
            config.max_concurrent_page_io = v;
        }
        if let Some(v) = s.bool("read-whole-page") {
            config.read_whole_page = v;
        }
        config.paging_directory = non_empty(&s, "paging-directory", &config.paging_directory)?;
        if let Some(v) = s.bool("purge-page-folders") {
            config.purge_page_folders = v;
        }
        if let Some(v) = s.bool("create-journal-dir") {
            config.create_journal_dir = v;
        }
        if let Some(t) = s.string("journal-type") {
            if !["NIO", "ASYNCIO", "MAPPED"].contains(&t.as_str()) {
                return Err(ConfigError::invalid(
                    "journal-type",
                    &t,
                    "expected NIO, ASYNCIO or MAPPED",
                ));
            }
            config.journal_type = t;
        }
        if config.journal_type == "ASYNCIO" {
            // AIOSequentialFileFactory.isSupported(): Saranga has no
            // libaio, as an Artemis without it.
            tracing::warn!(
                target: SERVER_LOGGER,
                "AMQ222018: AIO was not located on this platform, it will fall back to using pure Java NIO. If your platform is Linux, install LibAIO to enable the AIO journal"
            );
            config.journal_type = "NIO".to_owned();
        }
        if let Some(v) = s.bool("journal-datasync") {
            config.journal_datasync = v;
        }
        if let Some(v) = s.bool("journal-sync-transactional") {
            config.journal_sync_transactional = v;
        }
        if let Some(v) = s.bool("journal-sync-non-transactional") {
            config.journal_sync_non_transactional = v;
        }
        config.journal_file_size = text_bytes_int(
            &s,
            "journal-file-size",
            config.journal_file_size,
            Check::PositiveInt,
        )?;
        if let Some(v) = s.i32("journal-max-attic-files", Check::Any)? {
            config.journal_max_attic_files = v;
        }
        let aio = config.journal_type == "ASYNCIO";
        let buffer_timeout = s
            .i32("journal-buffer-timeout", Check::GeZero)?
            .unwrap_or(if aio { 500_000 } else { 3_333_333 });
        let buffer_size =
            text_bytes_int(&s, "journal-buffer-size", 490 * 1024, Check::PositiveInt)?;
        let max_io = s
            .i32("journal-max-io", Check::GtZero)?
            .unwrap_or(if aio { 4096 } else { 1 });
        config.journal_device_block_size =
            s.i32("journal-device-block-size", Check::MinusOneOrGeZero)?;
        if aio {
            config.journal_buffer_timeout_aio = buffer_timeout;
            config.journal_buffer_size_aio = buffer_size;
            config.journal_max_io_aio = max_io;
        } else {
            config.journal_buffer_timeout_nio = buffer_timeout;
            config.journal_buffer_size_nio = buffer_size;
            config.journal_max_io_nio = max_io;
        }
        config.journal_file_open_timeout = s
            .i32("journal-file-open-timeout", Check::GtZero)?
            .unwrap_or(5);
        if let Some(v) = s.i32("journal-min-files", Check::GtZero)? {
            config.journal_min_files = v;
        }
        // setJournalPoolFiles logs when the value is negative.
        let pool_files = s
            .i32("journal-pool-files", Check::MinusOneOrGtZero)?
            .unwrap_or(config.journal_pool_files);
        config.journal_pool_files = pool_files;
        if pool_files < 0 {
            tracing::warn!(
                target: SERVER_LOGGER,
                "AMQ222269: Please use a fixed value for \"journal-pool-files\". Default changed per https://issues.apache.org/jira/browse/ARTEMIS-1628"
            );
        }
        if let Some(v) = s.i32("journal-compact-min-files", Check::GeZero)? {
            config.journal_compact_min_files = v;
        }
        if let Some(v) = s.i32("journal-compact-percentage", Check::Percentage)? {
            config.journal_compact_percentage = v;
        }
        config.log_journal_write_rate = s.bool("log-journal-write-rate").unwrap_or(false);
        if let Some(v) = s.i64("journal-lock-acquisition-timeout", Check::MinusOneOrGtZero)? {
            config.journal_lock_acquisition_timeout = v;
        }
        if let Some(v) = s.i64("journal-lock-monitor-timeout", Check::GtZero)? {
            config.journal_lock_monitor_timeout = v;
        }
        if let Some(v) = s.i32("journal-lock-monitor-max-retries", Check::GeZero)? {
            config.journal_lock_monitor_max_retries = v;
        }
        if e.has_attribute("wild-card-routing-enabled")
            && let Some(v) = s.bool("wild-card-routing-enabled")
        {
            config.wildcard_routing_enabled = v;
        }
        if let Some(v) = s.bool("message-counter-enabled") {
            config.message_counter_enabled = v;
        }
        if let Some(v) = s.i64("message-counter-sample-period", Check::GtZero)? {
            config.message_counter_sample_period = v;
        }
        if let Some(v) = s.i32("message-counter-max-day-history", Check::GtZero)? {
            config.message_counter_max_day_history = v;
        }
        if let Some(v) = s.i64("server-dump-interval", Check::MinusOneOrGtZero)? {
            config.server_dump_interval = v;
        }
        if let Some(v) = s.i32("memory-warning-threshold", Check::Percentage)? {
            config.memory_warning_threshold = v;
        }
        if let Some(v) = s.i64("memory-measure-interval", Check::MinusOneOrGtZero)? {
            config.memory_measure_interval = v;
        }
        if let Some(v) = s.string("network-check-list") {
            config.network_check_list = Some(v);
        }
        if let Some(v) = s.string("network-check-URL-list") {
            config.network_url_list = Some(v);
        }
        if let Some(v) = s.i64("network-check-period", Check::GtZero)? {
            config.network_check_period = v;
        }
        if let Some(v) = s.i32("network-check-timeout", Check::GtZero)? {
            config.network_check_timeout = v;
        }
        if let Some(v) = s.string("network-check-NIC") {
            config.network_check_nic = Some(v);
        }
        if let Some(v) = s.string("network-check-ping6-command") {
            config.network_check_ping6_command = Some(v);
        }
        if let Some(v) = s.string("network-check-ping-command") {
            config.network_check_ping_command = Some(v);
        }
        if let Some(v) = s.bool("critical-analyzer") {
            config.critical_analyzer = v;
        }
        if let Some(v) = s.i64("critical-analyzer-timeout", Check::GeZero)? {
            config.critical_analyzer_timeout = v;
        }
        if let Some(v) = s.i64("critical-analyzer-check-period", Check::GeZero)? {
            config.critical_analyzer_check_period = v;
        }
        let policy = non_empty(
            &s,
            "critical-analyzer-policy",
            &config.critical_analyzer_policy,
        )?;
        if !["HALT", "SHUTDOWN", "LOG"].contains(&policy.as_str()) {
            return Err(ConfigError::invalid(
                "critical-analyzer-policy",
                &policy,
                "No enum constant org.apache.activemq.artemis.utils.critical.CriticalAnalyzerPolicy",
            ));
        }
        config.critical_analyzer_policy = policy;
        config.page_sync_timeout = s
            .i32("page-sync-timeout", Check::GeZero)?
            .unwrap_or(config.journal_buffer_timeout_nio);
        if let Some(v) = s.bool("suppress-session-notifications") {
            config.suppress_session_notifications = v;
        }
        if let Some(v) = s.string("literal-match-markers") {
            // NULL_OR_TWO_CHARACTERS
            if v.encode_utf16().count() != 2 {
                return Err(ConfigError::invalid(
                    "literal-match-markers",
                    &v,
                    format!("length is {}, must be 2", v.encode_utf16().count()),
                ));
            }
            config.literal_match_markers = Some(v);
        }
        if let Some(v) = s.bool("large-message-sync") {
            config.large_message_sync = v;
        }
        if let Some(v) = s.string("view-permission-method-match-pattern") {
            config.view_permission_method_match_pattern = v;
        }
        if let Some(v) = s.bool("management-message-rbac") {
            config.management_messages_rbac = v;
        }
        if let Some(v) = s.string("management-rbac-prefix") {
            config.management_rbac_prefix = v;
        }
        if let Some(v) = s.bool("mirror-page-transaction") {
            config.mirror_page_transaction = v;
        }
        if let Some(v) = s.i32("mirror-ack-manager-page-attempts", Check::GtZero)? {
            config.mirror_ack_manager_page_attempts = v;
        }
        if let Some(v) = s.i32("mirror-ack-manager-queue-attempts", Check::GtZero)? {
            config.mirror_ack_manager_queue_attempts = v;
        }
        if let Some(v) = s.i32("mirror-ack-manager-retry-delay", Check::GtZero)? {
            config.mirror_ack_manager_retry_delay = v;
        }
        if let Some(v) = s.bool("mirror-ack-manager-warn-unacked") {
            config.mirror_ack_manager_warn_unacked = v;
        }

        self.parse_address_settings(e, config)?;
        parse_resource_limits(e, config)?;
        parse_queues(e);
        parse_addresses(e, config)?;
        parse_security(e, config)?;
        parse_broker_plugins(e, config)?;
        parse_metrics(e, config)?;

        let mut services = Vec::new();
        for node in all(e, "connector-service") {
            services.push(parse_connector_service(node)?);
        }
        config.connector_service_configurations = services;

        if let Some(w) = first(e, "wildcard-addresses") {
            parse_wildcards(w, config);
        }
        Ok(())
    }

    fn parse_address_settings(
        &mut self,
        e: Node<'_, '_>,
        config: &mut Configuration,
    ) -> Result<()> {
        let Some(list) = first(e, "address-settings") else {
            return Ok(());
        };
        for node in all(list, "address-setting") {
            let m = attribute(node, "match").unwrap_or_default();
            if !self.page_max_size_used
                && children(node).any(|c| {
                    c.tag_name()
                        .name()
                        .eq_ignore_ascii_case("page-max-cache-size")
                })
            {
                self.page_max_size_used = true;
                tracing::info!(
                    target: SERVER_LOGGER,
                    "AMQ224117: \"page-max-cache-size\" being used on broker.xml. This configuration attribute is no longer used and it will be ignored."
                );
            }
            let settings = crate::parser::parse_address_setting(node, &mut Vec::new())?;
            if config.address_settings.contains_key(&m) {
                tracing::warn!(
                    target: SERVER_LOGGER,
                    "AMQ222301: Duplicate address-setting match found: {m}. These settings will be ignored! Please review your broker.xml and consolidate any duplicate address-setting elements."
                );
            } else {
                config.address_settings.put(m, settings);
            }
        }
        Ok(())
    }
}

/// `parseJournalRetention`.
fn parse_journal_retention(e: Node<'_, '_>, config: &mut Configuration) -> Result<()> {
    let Some(node) = first(e, "journal-retention-directory") else {
        return Ok(());
    };
    let directory = java_trim(&text_content(node)).to_owned();
    let storage_limit = match attribute(node, "storage-limit") {
        None => -1,
        Some(text) => crate::convert_text_bytes(java_trim(&text))
            .map_err(|_| ConfigError::invalid("storage-limit", &text, "not a byte size"))?,
    };
    let period = int_attribute_or(node, "period", -1, Check::GtZero)?;
    let unit = attribute(node, "unit").unwrap_or_else(|| "DAYS".to_owned());
    let unit_millis: i64 = match unit.to_uppercase().as_str() {
        "NANOSECONDS" => 0,
        "MICROSECONDS" => 0,
        "MILLISECONDS" => 1,
        "SECONDS" => 1000,
        "MINUTES" => 60_000,
        "HOURS" => 3_600_000,
        "DAYS" => 86_400_000,
        _ => {
            return Err(ConfigError::invalid(
                "unit",
                &unit,
                "No enum constant java.util.concurrent.TimeUnit",
            ));
        }
    };
    config.journal_retention_directory = Some(directory.clone());
    config.journal_retention_max_bytes = storage_limit;
    // setJournalRetentionPeriod(unit, period): unit.toMillis(period), -1
    // for a period that is not positive.
    config.journal_retention_period = if period <= 0 {
        -1
    } else {
        match unit.to_uppercase().as_str() {
            "NANOSECONDS" => i64::from(period) / 1_000_000,
            "MICROSECONDS" => i64::from(period) / 1000,
            _ => i64::from(period).saturating_mul(unit_millis),
        }
    };
    if directory.is_empty() {
        return Err(ConfigError::Runtime(
            "journal-retention-directory=null".to_owned(),
        ));
    }
    if storage_limit == -1 && period == -1 {
        return Err(ConfigError::Runtime(
            "configure either storage-limit or period on journal-retention-directory".to_owned(),
        ));
    }
    Ok(())
}

/// `parseHAPolicyConfiguration`: the first of `live-only`, `primary-only`,
/// `shared-store` and `replication` found under `<ha-policy>`.
fn parse_ha_policy(e: Node<'_, '_>, config: &mut Configuration) -> Result<()> {
    for ha_type in ["live-only", "primary-only", "shared-store", "replication"] {
        let Some(ha_node) = first(e, ha_type) else {
            continue;
        };
        match ha_type {
            "replication" => {
                if let Some(colocated) = first(e, "colocated") {
                    config.ha_policy_configuration = Some(HaPolicyConfiguration::Colocated(
                        colocated_policy(colocated, true)?,
                    ));
                    return Ok(());
                }
                if let Some(n) = first(e, "master") {
                    deprecated("master", "primary");
                    config.ha_policy_configuration =
                        Some(HaPolicyConfiguration::Replicated(replicated_policy(n)?));
                    return Ok(());
                }
                if let Some(n) = first(e, "slave") {
                    deprecated("slave", "backup");
                    config.ha_policy_configuration =
                        Some(HaPolicyConfiguration::Replica(replica_policy(n)?));
                    return Ok(());
                }
                if let Some(n) = first(e, "primary") {
                    config.ha_policy_configuration = Some(if first(n, "manager").is_some() {
                        HaPolicyConfiguration::ReplicationPrimary(replication_primary_policy(n)?)
                    } else {
                        HaPolicyConfiguration::Replicated(replicated_policy(n)?)
                    });
                    return Ok(());
                }
                if let Some(n) = first(e, "backup") {
                    config.ha_policy_configuration = Some(if first(n, "manager").is_some() {
                        HaPolicyConfiguration::ReplicationBackup(replication_backup_policy(n)?)
                    } else {
                        HaPolicyConfiguration::Replica(replica_policy(n)?)
                    });
                    return Ok(());
                }
            }
            "shared-store" => {
                if let Some(colocated) = first(e, "colocated") {
                    config.ha_policy_configuration = Some(HaPolicyConfiguration::Colocated(
                        colocated_policy(colocated, false)?,
                    ));
                    return Ok(());
                }
                if let Some(n) = first(e, "master") {
                    deprecated("master", "primary");
                    config.ha_policy_configuration = Some(
                        HaPolicyConfiguration::SharedStorePrimary(shared_store_primary(n)),
                    );
                    return Ok(());
                }
                if let Some(n) = first(e, "slave") {
                    deprecated("slave", "backup");
                    config.ha_policy_configuration = Some(
                        HaPolicyConfiguration::SharedStoreBackup(shared_store_backup(n)?),
                    );
                    return Ok(());
                }
                if let Some(n) = first(e, "primary") {
                    config.ha_policy_configuration = Some(
                        HaPolicyConfiguration::SharedStorePrimary(shared_store_primary(n)),
                    );
                    return Ok(());
                }
                if let Some(n) = first(e, "backup") {
                    config.ha_policy_configuration = Some(
                        HaPolicyConfiguration::SharedStoreBackup(shared_store_backup(n)?),
                    );
                    return Ok(());
                }
            }
            // `live-only` and `primary-only`: no return, the loop goes on.
            _ => {
                config.ha_policy_configuration = Some(HaPolicyConfiguration::PrimaryOnly {
                    scale_down_configuration: scale_down(ha_node)?,
                });
            }
        }
    }
    Ok(())
}

/// `createReplicationPrimaryHaPolicy`.
fn replicated_policy(n: Node<'_, '_>) -> Result<ReplicatedPolicy> {
    let s = Settings { core: n };
    let mut c = ReplicatedPolicy {
        quorum_vote_wait: s.i32("quorum-vote-wait", Check::GtZero)?.unwrap_or(30),
        ..ReplicatedPolicy::default()
    };
    if first(n, "check-for-live-server").is_some() {
        deprecated("check-for-live-server", "check-for-active-server");
        if let Some(v) = s.bool("check-for-live-server") {
            c.check_for_active_server = v;
        }
    } else if let Some(v) = s.bool("check-for-active-server") {
        c.check_for_active_server = v;
    }
    if let Some(v) = s.string("group-name") {
        c.group_name = Some(v);
    }
    if let Some(v) = s.string("cluster-name") {
        c.cluster_name = Some(v);
    }
    if let Some(v) = s.i32(
        "max-saved-replicated-journals-size",
        Check::MinusOneOrGeZero,
    )? {
        c.max_saved_replicated_journals_size = v;
    }
    if let Some(v) = s.i64("initial-replication-sync-timeout", Check::GtZero)? {
        c.initial_replication_sync_timeout = v;
    }
    if let Some(v) = s.bool("vote-on-replication-failure") {
        c.vote_on_replication_failure = v;
    }
    if let Some(v) = s.i32("vote-retries", Check::MinusOneOrGeZero)? {
        c.vote_retries = v;
    }
    if let Some(v) = s.i64("vote-retry-wait", Check::GtZero)? {
        c.vote_retry_wait = v;
    }
    // Its default is the vote retry wait, as Artemis passes it.
    c.retry_replication_wait = Some(
        s.i64("retry-replication-wait", Check::GtZero)?
            .unwrap_or(c.vote_retry_wait),
    );
    if let Some(v) = s.i32("quorum-size", Check::MinusOneOrGtZero)? {
        c.quorum_size = v;
    }
    Ok(c)
}

/// `createReplicationBackupHaPolicy`.
fn replica_policy(n: Node<'_, '_>) -> Result<ReplicaPolicy> {
    let s = Settings { core: n };
    let mut c = ReplicaPolicy {
        quorum_vote_wait: s.i32("quorum-vote-wait", Check::GtZero)?.unwrap_or(30),
        ..ReplicaPolicy::default()
    };
    if let Some(v) = s.bool("restart-backup") {
        c.restart_backup = v;
    }
    if let Some(v) = s.string("group-name") {
        c.group_name = Some(v);
    }
    if let Some(v) = s.bool("allow-failback") {
        c.allow_fail_back = v;
    }
    if let Some(v) = s.i64("initial-replication-sync-timeout", Check::GtZero)? {
        c.initial_replication_sync_timeout = v;
    }
    if let Some(v) = s.string("cluster-name") {
        c.cluster_name = Some(v);
    }
    if let Some(v) = s.i32(
        "max-saved-replicated-journals-size",
        Check::MinusOneOrGeZero,
    )? {
        c.max_saved_replicated_journals_size = v;
    }
    c.scale_down_configuration = scale_down(n)?;
    if let Some(v) = s.bool("vote-on-replication-failure") {
        c.vote_on_replication_failure = v;
    }
    if let Some(v) = s.i32("vote-retries", Check::MinusOneOrGeZero)? {
        c.vote_retries = v;
    }
    if let Some(v) = s.i64("vote-retry-wait", Check::GtZero)? {
        c.vote_retry_wait = v;
    }
    c.retry_replication_wait = s
        .i64("retry-replication-wait", Check::GtZero)?
        .unwrap_or(c.vote_retry_wait);
    if let Some(v) = s.i32("quorum-size", Check::MinusOneOrGtZero)? {
        c.quorum_size = v;
    }
    Ok(c)
}

/// `createPluggableReplicationPrimaryHaPolicy`.
fn replication_primary_policy(n: Node<'_, '_>) -> Result<ReplicationPrimaryPolicy> {
    let s = Settings { core: n };
    let mut c = ReplicationPrimaryPolicy {
        group_name: s.string("group-name"),
        cluster_name: s.string("cluster-name"),
        initial_replication_sync_timeout: 30000,
        retry_replication_wait: Some(2000),
        distributed_manager_configuration: None,
        coordination_id: None,
        max_saved_replicated_journals_size: 2,
    };
    if let Some(v) = s.i64("initial-replication-sync-timeout", Check::GtZero)? {
        c.initial_replication_sync_timeout = v;
    }
    if let Some(v) = s.i64("retry-replication-wait", Check::GtZero)? {
        c.retry_replication_wait = Some(v);
    }
    c.distributed_manager_configuration = Some(lock_manager(n)?);
    if let Some(id) = s.non_empty_string("coordination-id")? {
        // setCoordinationId: at most 16 characters, an odd length made even
        // with '+', '-' turned into '.'.
        let len = id.encode_utf16().count();
        let id = if len >= 16 {
            String::from_utf16_lossy(&id.encode_utf16().take(16).collect::<Vec<u16>>())
        } else if len % 2 != 0 {
            format!("{id}+")
        } else {
            id
        };
        c.coordination_id = Some(id.replace('-', "."));
    }
    if let Some(v) = s.i32(
        "max-saved-replicated-journals-size",
        Check::MinusOneOrGeZero,
    )? {
        c.max_saved_replicated_journals_size = v;
    }
    Ok(c)
}

/// `createPluggableReplicationBackupHaPolicy`.
fn replication_backup_policy(n: Node<'_, '_>) -> Result<ReplicationBackupPolicy> {
    let s = Settings { core: n };
    let mut c = ReplicationBackupPolicy {
        cluster_name: None,
        max_saved_replicated_journals_size: 2,
        group_name: s.string("group-name"),
        allow_fail_back: false,
        initial_replication_sync_timeout: 30000,
        retry_replication_wait: 2000,
        distributed_manager_configuration: None,
    };
    if let Some(v) = s.bool("allow-failback") {
        c.allow_fail_back = v;
    }
    if let Some(v) = s.i64("initial-replication-sync-timeout", Check::GtZero)? {
        c.initial_replication_sync_timeout = v;
    }
    c.cluster_name = s.string("cluster-name");
    if let Some(v) = s.i32(
        "max-saved-replicated-journals-size",
        Check::MinusOneOrGeZero,
    )? {
        c.max_saved_replicated_journals_size = v;
    }
    if let Some(v) = s.i64("retry-replication-wait", Check::GtZero)? {
        c.retry_replication_wait = v;
    }
    c.distributed_manager_configuration = Some(lock_manager(n)?);
    Ok(c)
}

/// `createDistributedLockManagerConfiguration`: the first `<manager>`.
fn lock_manager(n: Node<'_, '_>) -> Result<DistributedLockManagerConfiguration> {
    let manager = first(n, "manager").ok_or_else(|| {
        ConfigError::NullPointer("java.lang.NullPointerException: manager".to_owned())
    })?;
    let s = Settings { core: manager };
    let class_name = s.string("class-name").unwrap_or_else(|| {
        "org.apache.activemq.artemis.lockmanager.zookeeper.CuratorDistributedLockManager".to_owned()
    });
    let mut properties = JavaMap::default();
    if first(manager, "properties").is_some() {
        for p in all(manager, "property") {
            properties.put(attribute_or_empty(p, "key"), attribute_or_empty(p, "value"));
        }
    }
    Ok(DistributedLockManagerConfiguration {
        class_name,
        properties,
    })
}

/// `createSharedStorePrimaryHaPolicy`.
fn shared_store_primary(n: Node<'_, '_>) -> SharedStorePrimaryPolicy {
    let s = Settings { core: n };
    SharedStorePrimaryPolicy {
        failover_on_server_shutdown: s.bool("failover-on-shutdown").unwrap_or(false),
        wait_for_activation: s.bool("wait-for-activation").unwrap_or(true),
    }
}

/// `createSharedStoreBackupHaPolicy`.
fn shared_store_backup(n: Node<'_, '_>) -> Result<SharedStoreBackupPolicy> {
    let s = Settings { core: n };
    Ok(SharedStoreBackupPolicy {
        allow_fail_back: s.bool("allow-failback").unwrap_or(true),
        failover_on_server_shutdown: s.bool("failover-on-shutdown").unwrap_or(false),
        restart_backup: s.bool("restart-backup").unwrap_or(true),
        scale_down_configuration: scale_down(n)?,
    })
}

/// `createColocatedHaPolicy`.
fn colocated_policy(n: Node<'_, '_>, replicated: bool) -> Result<ColocatedPolicy> {
    let s = Settings { core: n };
    let mut c = ColocatedPolicy {
        request_backup: s.bool("request-backup").unwrap_or(false),
        backup_request_retries: s
            .i32("backup-request-retries", Check::MinusOneOrGeZero)?
            .unwrap_or(-1),
        backup_request_retry_interval: s
            .i64("backup-request-retry-interval", Check::GtZero)?
            .unwrap_or(5000),
        max_backups: s.i32("max-backups", Check::GeZero)?.unwrap_or(1),
        backup_port_offset: s.i32("backup-port-offset", Check::GtZero)?.unwrap_or(100),
        excluded_connectors: Vec::new(),
        port_offset: 100,
        primary_config: None,
        backup_config: None,
    };
    if let Some(excludes) = first(n, "excludes") {
        for child in children(excludes) {
            if child.tag_name().name() == "connector-ref" {
                c.excluded_connectors.push(trimmed_text(child));
            }
        }
    }
    let primary = |p: Node<'_, '_>| -> Result<HaPolicyConfiguration> {
        Ok(if replicated {
            HaPolicyConfiguration::Replicated(replicated_policy(p)?)
        } else {
            HaPolicyConfiguration::SharedStorePrimary(shared_store_primary(p))
        })
    };
    let backup = |b: Node<'_, '_>| -> Result<HaPolicyConfiguration> {
        Ok(if replicated {
            HaPolicyConfiguration::Replica(replica_policy(b)?)
        } else {
            HaPolicyConfiguration::SharedStoreBackup(shared_store_backup(b)?)
        })
    };
    if let Some(p) = first(n, "master") {
        deprecated("master", "primary");
        c.primary_config = Some(Box::new(primary(p)?));
    }
    if let Some(b) = first(n, "slave") {
        deprecated("slave", "backup");
        c.backup_config = Some(Box::new(backup(b)?));
    }
    if let Some(p) = first(n, "primary") {
        c.primary_config = Some(Box::new(primary(p)?));
    }
    if let Some(b) = first(n, "backup") {
        c.backup_config = Some(Box::new(backup(b)?));
    }
    Ok(c)
}

/// `parseScaleDownConfig`.
fn scale_down(n: Node<'_, '_>) -> Result<Option<ScaleDownConfiguration>> {
    let Some(element) = first(n, "scale-down") else {
        return Ok(None);
    };
    let s = Settings { core: element };
    let mut c = ScaleDownConfiguration::default();
    if let Some(v) = s.bool("enabled") {
        c.enabled = v;
    }
    if let Some(v) = s.i32("commit-interval", Check::MinusOneOrGtZero)? {
        c.commit_interval = v;
    }
    if let Some(r) = first(element, "discovery-group-ref") {
        c.discovery_group = attribute(r, "discovery-group-name");
    }
    c.group_name = s.string("group-name");
    if let Some(list) = first(element, "connectors") {
        for child in children(list) {
            if child.tag_name().name() == "connector-ref" {
                c.connectors.push(trimmed_text(child));
            }
        }
    }
    Ok(Some(c))
}

/// `parseStoreConfiguration`: `database-store`, then `file-store`, the last
/// found winning.
fn parse_store(e: Node<'_, '_>, config: &mut Configuration) -> Result<()> {
    for store_type in ["database-store", "file-store"] {
        let Some(node) = first(e, store_type) else {
            continue;
        };
        config.store_configuration = Some(if store_type == "database-store" {
            StoreConfiguration::Database(Box::new(database_store(node, config)?))
        } else {
            StoreConfiguration::File
        });
    }
    Ok(())
}

/// `createDatabaseStoreConfig`.
fn database_store(n: Node<'_, '_>, config: &Configuration) -> Result<DatabaseStorage> {
    let s = Settings { core: n };
    let mut c = DatabaseStorage::default();
    if let Some(v) = s.string("bindings-table-name") {
        c.bindings_table_name = v;
    }
    if let Some(v) = s.string("message-table-name") {
        c.message_table_name = v;
    }
    if let Some(v) = s.string("large-message-table-name") {
        c.large_messages_table_name = v;
    }
    if let Some(v) = s.string("page-store-table-name") {
        c.page_store_table_name = v;
    }
    if let Some(v) = s.string("node-manager-store-table-name") {
        c.node_manager_store_table_name = v;
    }
    if let Some(url) = s.string("jdbc-connection-url") {
        c.jdbc_connection_url = Some(if url.starts_with("jdbc:") {
            url
        } else {
            // resolveMask(text, codec): unmasked only when ENC(...).
            mask::resolve_mask(None, &url, Some(config.password_codec()))
                .map_err(|e| mask_error("jdbc-connection-url", &url, e))?
        });
    }
    if let Some(v) = s.string("jdbc-driver-class-name") {
        c.jdbc_driver_class_name = v;
    }
    if let Some(v) = s.i32("jdbc-network-timeout", Check::Any)? {
        c.jdbc_network_timeout = v;
    }
    if let Some(v) = s.i64("jdbc-lock-renew-period", Check::Any)? {
        c.jdbc_lock_renew_period_millis = v;
    }
    if let Some(v) = s.i64("jdbc-lock-expiration", Check::Any)? {
        c.jdbc_lock_expiration_millis = v;
    }
    if let Some(v) = s.i64("jdbc-journal-sync-period", Check::Any)? {
        c.jdbc_journal_sync_period_millis = v;
    }
    if let Some(v) = s.i64("jdbc-allowed-time-diff", Check::Any)? {
        c.jdbc_allowed_time_diff = v;
    }
    c.max_page_size_bytes = text_bytes_int(
        &s,
        "jdbc-max-page-size-bytes",
        c.max_page_size_bytes,
        Check::Any,
    )?;
    if let Some(user) = s.string("jdbc-user") {
        c.jdbc_user = Some(resolve(config, "jdbc-user", &user)?);
    }
    if let Some(password) = s.string("jdbc-password") {
        c.jdbc_password = Some(resolve(config, "jdbc-password", &password)?);
    }
    if let Some(v) = s.string("data-source-class-name") {
        c.data_source_class_name = v;
    }
    if first(n, "data-source-properties").is_some() {
        for p in all(n, "data-source-property") {
            let key = attribute_or_empty(p, "key");
            let mut value = attribute_or_empty(p, "value");
            if mask::is_enc_masked(&value) {
                value = resolve(config, "data-source-property", &value)?;
            }
            c.data_source_properties.put(key, value);
        }
    }
    Ok(c)
}

/// The parameters `parseAcceptorTransportConfiguration` and
/// `parseConnectorTransportConfiguration` add when `mask-password` is set.
fn add_mask_params(params: &mut JavaMap<JavaValue>, config: &Configuration) {
    if let Some(mask) = config.mask_password {
        params.put("activemq.usemaskedpassword", JavaValue::Bool(mask));
        if mask {
            params.put(
                "activemq.passwordcodec",
                JavaValue::Str(config.password_codec().to_owned()),
            );
        }
    }
}

/// The URI an `<acceptor>` or `<connector>` holds: its first child's text.
fn transport_uri(e: Node<'_, '_>) -> String {
    e.first_child()
        .and_then(|c| c.text())
        .unwrap_or_default()
        .to_owned()
}

/// `parseAcceptorTransportConfiguration`.
fn parse_acceptor(e: Node<'_, '_>, config: &Configuration) -> Result<TransportConfiguration> {
    let name = attribute(e, "name");
    let uri = transport_uri(e);
    let first = transport_configurations(name.as_deref(), &uri, false, true)
        .map_err(|e| uri_thrown(e, false))?
        .swap_remove(0);
    let mut t = TransportConfiguration::from_uri(first);
    // `getAttribute` gives "" when absent, which `setLockCoordinator`
    // takes as `null`, as it takes any blank name.
    let lock_coordinator = attribute_or_empty(e, "lock-coordinator");
    t.lock_coordinator = (!java_trim(&lock_coordinator).is_empty()).then_some(lock_coordinator);
    add_mask_params(&mut t.params, config);
    Ok(t)
}

/// `parseConnectorTransportConfiguration`: the first configuration of the
/// URI's hosts.
fn parse_connector(e: Node<'_, '_>, config: &Configuration) -> Result<TransportConfiguration> {
    let name = attribute(e, "name");
    let uri = transport_uri(e);
    let first = transport_configurations(name.as_deref(), &uri, true, true)
        .map_err(|e| uri_thrown(e, false))?
        .swap_remove(0);
    let mut t = TransportConfiguration::from_uri(first);
    add_mask_params(&mut t.params, config);
    Ok(t)
}

/// The endpoint of a broadcast or discovery group: JGroups when it names
/// a file, else UDP.
fn broadcast_endpoint(e: Node<'_, '_>) -> Result<BroadcastEndpoint> {
    let s = Settings { core: e };
    let local_address = s.string("local-bind-address");
    let local_bind_port = s
        .i32("local-bind-port", Check::MinusOneOrGtZero)?
        .unwrap_or(-1);
    let group_address = s.string("group-address");
    let group_port = s.i32("group-port", Check::MinusOneOrGtZero)?.unwrap_or(-1);
    let jgroups_file = s.string("jgroups-file");
    let jgroups_channel = s.string("jgroups-channel");
    Ok(match jgroups_file {
        Some(file) => BroadcastEndpoint::JGroupsFile {
            file,
            channel_name: jgroups_channel,
        },
        None => BroadcastEndpoint::Udp {
            group_address,
            group_port,
            local_bind_address: local_address,
            local_bind_port,
        },
    })
}

/// `parseBroadcastGroupConfiguration`.
fn parse_broadcast_group(e: Node<'_, '_>) -> Result<BroadcastGroupConfiguration> {
    let s = Settings { core: e };
    let name = attribute_or_empty(e, "name");
    let mut connector_infos = Vec::new();
    for child in children(e) {
        if child.tag_name().name() == "connector-ref" {
            // Java reads the first connector-ref each time.
            connector_infos.push(s.non_empty_string("connector-ref")?.unwrap_or_default());
        }
    }
    let broadcast_period = s.i64("broadcast-period", Check::GtZero)?.unwrap_or(2000);
    let endpoint = broadcast_endpoint(e)?;
    Ok(BroadcastGroupConfiguration {
        name,
        broadcast_period,
        connector_infos,
        endpoint_factory: endpoint,
    })
}

/// `parseDiscoveryGroupConfiguration`.
fn parse_discovery_group(e: Node<'_, '_>) -> Result<DiscoveryGroupConfiguration> {
    let s = Settings { core: e };
    let name = attribute_or_empty(e, "name");
    let initial_wait = s
        .i64("initial-wait-timeout", Check::GtZero)?
        .unwrap_or(10000);
    let refresh = s.i64("refresh-timeout", Check::GtZero)?.unwrap_or(10000);
    let endpoint = broadcast_endpoint(e)?;
    Ok(DiscoveryGroupConfiguration {
        name,
        refresh_timeout: refresh,
        discovery_initial_wait_timeout: initial_wait,
        stopping_timeout: 10000,
        broadcast_endpoint_factory: endpoint,
    })
}

/// `new ClusterConnectionConfiguration(new URI(uri)).setName(name)`.
fn cluster_connection_from_uri(name: &str, uri: &str) -> Result<ClusterConnectionConfiguration> {
    let (c, composite) = crate::cluster_uri::from_uri_parts(name, uri)?;
    let mut out = ClusterConnectionConfiguration::new(c.name);
    out.address = Some(c.address);
    out.connector_name = (!c.connector_name.is_empty()).then_some(c.connector_name);
    out.client_failure_check_period = c.check_period;
    out.connection_ttl = c.connection_ttl;
    out.retry_interval = c.retry_interval;
    out.retry_interval_multiplier = c.retry_interval_multiplier;
    out.max_retry_interval = c.max_retry_interval;
    out.initial_connect_attempts = c.initial_connect_attempts;
    out.reconnect_attempts = c.reconnect_attempts;
    out.call_timeout = c.call_timeout;
    out.call_failover_timeout = c.call_failover_timeout;
    out.duplicate_detection = c.use_duplicate_detection;
    out.message_load_balancing_type = c.message_load_balancing.name().to_owned();
    out.composite_members = composite.map(|params| JavaMap {
        entries: params
            .into_iter()
            .map(|(k, v)| (k, v.unwrap_or_default()))
            .collect(),
    });
    out.static_connectors = c.static_connectors;
    out.discovery_group_name = c.discovery_group;
    out.max_hops = c.max_hops;
    out.confirmation_window_size = c.confirmation_window_size;
    out.producer_window_size = c.producer_window_size;
    out.allow_direct_connections_only = c.allow_direct_connections_only;
    out.min_large_message_size = c.min_large_message_size;
    out.cluster_notification_interval = c.notification_interval;
    out.cluster_notification_attempts = c.notification_attempts;
    out.client_id = c.client_id;
    out.topology_scanner_attempts = c.topology_scanner_attempts;
    Ok(out)
}

/// `parseClusterConnectionConfiguration`: `None` when its connector is not
/// configured (AMQ222217).
fn parse_cluster_connection(
    e: Node<'_, '_>,
    config: &Configuration,
) -> Result<Option<ClusterConnectionConfiguration>> {
    let s = Settings { core: e };
    let name = attribute_or_empty(e, "name");
    let address = string_or(&s, "address", Some("")).unwrap_or_default();
    let connector_name = s.non_empty_string("connector-ref")?;
    if !connector_name
        .as_deref()
        .is_some_and(|c| config.connector_configs.contains_key(c))
    {
        tracing::warn!(
            target: SERVER_LOGGER,
            "AMQ222217: Cannot find connector-ref {}. The cluster-connection {name} will not be deployed.",
            connector_name.as_deref().unwrap_or("null")
        );
        return Ok(None);
    }
    let mut c = ClusterConnectionConfiguration::new(name);
    c.address = Some(address);
    c.connector_name = connector_name;
    c.duplicate_detection = s.bool("use-duplicate-detection").unwrap_or(true);
    c.message_load_balancing_type = if first(e, "forward-when-no-consumers").is_some() {
        if s.bool("forward-when-no-consumers").unwrap_or(false) {
            "STRICT".to_owned()
        } else {
            "ON_DEMAND".to_owned()
        }
    } else {
        let t = s
            .string("message-load-balancing")
            .unwrap_or_else(|| "ON_DEMAND".to_owned());
        if !["OFF", "STRICT", "ON_DEMAND", "OFF_WITH_REDISTRIBUTION"].contains(&t.as_str()) {
            return Err(ConfigError::invalid(
                "message-load-balancing",
                &t,
                "invalid message load balancing type",
            ));
        }
        t
    };
    c.max_hops = s.i32("max-hops", Check::GeZero)?.unwrap_or(1);
    c.client_failure_check_period = s.i64("check-period", Check::GtZero)?.unwrap_or(30000);
    c.connection_ttl = s.i64("connection-ttl", Check::GtZero)?.unwrap_or(60000);
    c.retry_interval = s.i64("retry-interval", Check::GtZero)?.unwrap_or(500);
    c.call_timeout = s.i64("call-timeout", Check::GtZero)?.unwrap_or(30000);
    c.call_failover_timeout = s
        .i64("call-failover-timeout", Check::MinusOneOrGtZero)?
        .unwrap_or(-1);
    c.retry_interval_multiplier = double_gt_zero(e, "retry-interval-multiplier", 1.0)?;
    c.min_large_message_size =
        text_bytes_int(&s, "min-large-message-size", 100 * 1024, Check::PositiveInt)?;
    c.max_retry_interval = s.i64("max-retry-interval", Check::GtZero)?.unwrap_or(2000);
    c.initial_connect_attempts = s
        .i32("initial-connect-attempts", Check::MinusOneOrGeZero)?
        .unwrap_or(-1);
    c.reconnect_attempts = s
        .i32("reconnect-attempts", Check::MinusOneOrGeZero)?
        .unwrap_or(-1);
    c.confirmation_window_size = text_bytes_int(
        &s,
        "confirmation-window-size",
        1024 * 1024 * 10,
        Check::PositiveInt,
    )?;
    c.producer_window_size = text_bytes_int(
        &s,
        "producer-window-size",
        1024 * 1024,
        Check::MinusOneOrPositiveInt,
    )?;
    c.cluster_notification_interval = s
        .i64("notification-interval", Check::GtZero)?
        .unwrap_or(1000);
    c.cluster_notification_attempts = s.i32("notification-attempts", Check::GtZero)?.unwrap_or(2);
    c.topology_scanner_attempts = s
        .i32("topology-scanner-attempts", Check::MinusOneOrGeZero)?
        .unwrap_or(30);
    let mut discovery_group_name = None;
    let mut statics = Vec::new();
    for child in children(e) {
        match child.tag_name().name() {
            "discovery-group-ref" => {
                discovery_group_name = attribute(child, "discovery-group-name");
            }
            "static-connectors" => {
                if let Some(v) = child.attribute("allow-direct-connections-only") {
                    c.allow_direct_connections_only =
                        v.eq_ignore_ascii_case("true") || c.allow_direct_connections_only;
                }
                statics.extend(static_connectors(child));
            }
            _ => {}
        }
    }
    c.client_id = s.string("client-id");
    match discovery_group_name {
        None => c.static_connectors = statics,
        Some(g) => c.discovery_group_name = Some(g),
    }
    Ok(Some(c))
}

/// `parseGroupingHandlerConfiguration`.
fn parse_grouping_handler(n: Node<'_, '_>) -> Result<GroupingHandlerConfiguration> {
    let s = Settings { core: n };
    let name = attribute_or_empty(n, "name");
    // The type is required, and decides nothing the export shows.
    s.non_empty_string("type")?;
    let address = string_or(&s, "address", Some("")).unwrap_or_default();
    let timeout = s.i32("timeout", Check::GtZero)?.unwrap_or(5000);
    let group_timeout = s
        .i64("group-timeout", Check::MinusOneOrGtZero)?
        .unwrap_or(-1);
    let reaper_period = s.i64("reaper-period", Check::GtZero)?.unwrap_or(30000);
    Ok(GroupingHandlerConfiguration {
        name,
        address,
        timeout: i64::from(timeout),
        group_timeout,
        reaper_period,
    })
}

/// `getTransformerConfiguration(String)`.
fn transformer_named(class_name: String) -> TransformerConfig {
    TransformerConfig {
        class_name,
        properties: Vec::new(),
    }
}

/// `parseBridgeConfiguration`.
fn parse_bridge(n: Node<'_, '_>, config: &Configuration) -> Result<BridgeConfiguration> {
    let s = Settings { core: n };
    let name = attribute_or_empty(n, "name");
    let queue_name = s.non_empty_string("queue-name")?.unwrap_or_default();
    let forwarding_address = s.string("forwarding-address");
    let transformer_class_name = s.string("transformer-class-name");
    let confirmation_window_size = text_bytes_int(
        &s,
        "confirmation-window-size",
        1024 * 1024 * 10,
        Check::MinusOneOrPositiveInt,
    )?;
    let producer_window_size = text_bytes_int(
        &s,
        "producer-window-size",
        1024 * 1024,
        Check::MinusOneOrPositiveInt,
    )?;
    let retry_interval = s.i64("retry-interval", Check::GtZero)?.unwrap_or(2000);
    let client_failure_check_period = s.i64("check-period", Check::GtZero)?.unwrap_or(30000);
    let connection_ttl = s.i64("connection-ttl", Check::GtZero)?.unwrap_or(60000);
    let min_large_message_size =
        text_bytes_int(&s, "min-large-message-size", 100 * 1024, Check::PositiveInt)?;
    let max_retry_interval = s.i64("max-retry-interval", Check::GtZero)?.unwrap_or(2000);
    let retry_interval_multiplier = double_gt_zero(n, "retry-interval-multiplier", 1.0)?;
    let initial_connect_attempts = s
        .i32("initial-connect-attempts", Check::MinusOneOrGeZero)?
        .unwrap_or(-1);
    let reconnect_attempts = s
        .i32("reconnect-attempts", Check::MinusOneOrGeZero)?
        .unwrap_or(-1);
    let reconnect_attempts_on_same_node = s
        .i32("reconnect-attempts-same-node", Check::MinusOneOrGeZero)?
        .unwrap_or(10);
    let use_duplicate_detection = s.bool("use-duplicate-detection").unwrap_or(true);
    let user = string_or(&s, "user", Some("ACTIVEMQ.CLUSTER.ADMIN.USER"));
    let routing_type = s
        .string("routing-type")
        .unwrap_or_else(|| "PASS".to_owned());
    if !["MULTICAST", "ANYCAST", "STRIP", "PASS"].contains(&routing_type.as_str()) {
        return Err(ConfigError::invalid(
            "routing-type",
            &routing_type,
            "invalid routing type",
        ));
    }
    let concurrency = s.i32("concurrency", Check::GtZero)?.unwrap_or(1);
    let pending_ack_timeout = s
        .i64("pending-ack-timeout", Check::GtZero)?
        .unwrap_or(60000);
    let client_id = s.string("client-id");
    let password = match first(n, "password") {
        Some(p) => resolve(config, "password", &text_content(p))?,
        None => "CHANGE ME!!".to_owned(),
    };
    let ha = s.bool("ha").unwrap_or(false);
    let mut transformer = None;
    let mut filter_string = None;
    let mut statics = Vec::new();
    let mut discovery_group_name = None;
    for child in children(n) {
        match child.tag_name().name() {
            "filter" => filter_string = attribute(child, "string"),
            "discovery-group-ref" => {
                discovery_group_name = attribute(child, "discovery-group-name");
            }
            "static-connectors" => statics.extend(static_connectors(child)),
            "transformer" => transformer = Some(crate::parser::transformer(child)),
            _ => {}
        }
    }
    if transformer.is_none()
        && let Some(class_name) = transformer_class_name
    {
        transformer = Some(transformer_named(class_name));
    }
    let (static_connectors, discovery_group_name) = if statics.is_empty() {
        (None, discovery_group_name)
    } else {
        (Some(statics), None)
    };
    Ok(BridgeConfiguration {
        name: name.clone(),
        queue_name,
        forwarding_address,
        filter_string,
        static_connectors,
        discovery_group_name,
        ha,
        transformer_configuration: transformer,
        retry_interval,
        retry_interval_multiplier,
        initial_connect_attempts,
        reconnect_attempts,
        reconnect_attempts_on_same_node,
        use_duplicate_detection,
        confirmation_window_size,
        producer_window_size,
        client_failure_check_period,
        user,
        password: Some(password),
        connection_ttl,
        max_retry_interval,
        min_large_message_size,
        call_timeout: 30000,
        routing_type,
        concurrency,
        pending_ack_timeout,
        // `setName` sets the parent's name while it is unset.
        parent_name: Some(name.clone()),
        configuration_managed: true,
        client_id,
    })
}

/// `parseDivertConfiguration`.
fn parse_divert(e: Node<'_, '_>) -> Result<DivertConfiguration> {
    let s = Settings { core: e };
    let name = attribute_or_empty(e, "name");
    let routing_name = s.string("routing-name");
    let address = s.non_empty_string("address")?.unwrap_or_default();
    let forwarding_address = s
        .non_empty_string("forwarding-address")?
        .unwrap_or_default();
    let exclusive = s.bool("exclusive").unwrap_or(false);
    let transformer_class_name = s.string("transformer-class-name");
    let routing_type = s
        .string("routing-type")
        .unwrap_or_else(|| "STRIP".to_owned());
    if !["MULTICAST", "ANYCAST", "STRIP", "PASS"].contains(&routing_type.as_str()) {
        return Err(ConfigError::invalid(
            "routing-type",
            &routing_type,
            "invalid routing type",
        ));
    }
    let mut transformer = None;
    let mut filter_string = None;
    for child in children(e) {
        match child.tag_name().name() {
            "filter" => filter_string = attribute(child, "string"),
            "transformer" => transformer = Some(crate::parser::transformer(child)),
            _ => {}
        }
    }
    if transformer.is_none()
        && let Some(class_name) = transformer_class_name
    {
        transformer = Some(transformer_named(class_name));
    }
    Ok(DivertConfiguration {
        name,
        routing_name,
        address,
        forwarding_address,
        exclusive,
        filter_string,
        transformer_configuration: transformer,
        routing_type,
    })
}

/// `parseConnectionRouterConfiguration`.
fn parse_connection_router(
    e: Node<'_, '_>,
    config: &Configuration,
) -> Result<ConnectionRouterConfiguration> {
    let s = Settings { core: e };
    let name = attribute_or_empty(e, "name");
    let key_type = s
        .string("key-type")
        .unwrap_or_else(|| "SOURCE_IP".to_owned());
    if ![
        "CLIENT_ID",
        "SNI_HOST",
        "SOURCE_IP",
        "USER_NAME",
        "ROLE_NAME",
    ]
    .contains(&key_type.as_str())
    {
        return Err(ConfigError::invalid(
            "key-type",
            &key_type,
            "invalid key type",
        ));
    }
    let mut router = ConnectionRouterConfiguration {
        name,
        key_type,
        key_filter: s.string("key-filter"),
        local_target_filter: s.string("local-target-filter"),
        cache_configuration: None,
        pool_configuration: None,
        policy_configuration: None,
    };
    for child in children(e) {
        match child.tag_name().name() {
            "cache" => {
                let cs = Settings { core: child };
                router.cache_configuration = Some(CacheConfiguration {
                    persisted: cs.bool("persisted").unwrap_or(false),
                    timeout: cs.i32("timeout", Check::GeZero)?.unwrap_or(0),
                });
            }
            "policy" => {
                let name = attribute_or_empty(child, "name");
                const POLICIES: [&str; 4] = [
                    "CONSISTENT_HASH",
                    "FIRST_ELEMENT",
                    "LEAST_CONNECTIONS",
                    "ROUND_ROBIN",
                ];
                if !POLICIES.contains(&name.as_str()) && name != "CONSISTENT_HASH_MODULO" {
                    return Err(ConfigError::Checked(format!(
                        "java.lang.ClassNotFoundException: No ConnectionRouterPolicyFactory found for {name}"
                    )));
                }
                router.policy_configuration = Some(NamedPropertyConfiguration {
                    name,
                    properties: child_properties(child),
                });
            }
            "pool" => router.pool_configuration = Some(parse_pool(child, config)?),
            _ => {}
        }
    }
    Ok(router)
}

/// `parsePoolConfiguration`.
fn parse_pool(e: Node<'_, '_>, config: &Configuration) -> Result<PoolConfiguration> {
    let s = Settings { core: e };
    let mut p = PoolConfiguration {
        username: s.string("username"),
        ..PoolConfiguration::default()
    };
    if let Some(password) = s.string("password") {
        p.password = Some(resolve(config, "password", &password)?);
    }
    if let Some(v) = s.i32("check-period", Check::GtZero)? {
        p.check_period = v;
    }
    if let Some(v) = s.i32("quorum-size", Check::GtZero)? {
        p.quorum_size = v;
    }
    if let Some(v) = s.i32("quorum-timeout", Check::GeZero)? {
        p.quorum_timeout = v;
    }
    if let Some(v) = s.bool("local-target-enabled") {
        p.local_target_enabled = v;
    }
    p.cluster_connection = s.string("cluster-connection");
    for child in children(e) {
        match child.tag_name().name() {
            "discovery-group-ref" => {
                p.discovery_group_name = attribute(child, "discovery-group-name");
            }
            "static-connectors" => p.static_connectors = Some(static_connectors(child)),
            _ => {}
        }
    }
    Ok(p)
}

/// `parseLockCoordinator`.
fn parse_lock_coordinator(e: Node<'_, '_>) -> Result<LockCoordinatorConfiguration> {
    let s = Settings { core: e };
    let name = attribute_or_empty(e, "name");
    let lock_id = string_or(&s, "lock-id", Some(&name)).unwrap_or_default();
    let class_name = s.non_empty_string("class-name")?.unwrap_or_default();
    let check_period = s.i32("check-period", Check::Any)?.unwrap_or(5000);
    let auto_start = bool_attribute_or(e, "auto-start", true);
    let mut properties = JavaMap::default();
    if first(e, "properties").is_some() {
        for p in all(e, "property") {
            properties.put(attribute_or_empty(p, "key"), attribute_or_empty(p, "value"));
        }
    }
    Ok(LockCoordinatorConfiguration {
        name,
        lock_id,
        class_name,
        check_period,
        auto_start,
        properties,
    })
}

/// `parseResourceLimits`.
fn parse_resource_limits(e: Node<'_, '_>, config: &mut Configuration) -> Result<()> {
    let Some(list) = first(e, "resource-limit-settings") else {
        return Ok(());
    };
    for node in all(list, "resource-limit-setting") {
        let mut r = ResourceLimitSettings {
            r#match: attribute(node, "match"),
            ..ResourceLimitSettings::default()
        };
        for child in children(node) {
            let name = child.tag_name().name();
            if name.eq_ignore_ascii_case("max-connections") {
                r.max_sessions = Some(node_i32(child)?);
            } else if name.eq_ignore_ascii_case("max-queues") {
                r.max_queues = Some(node_i32(child)?);
            }
        }
        // addResourceLimitSettings: put by match.
        config
            .resource_limit_settings
            .put(r.r#match.clone().unwrap_or_else(|| "null".to_owned()), r);
    }
    Ok(())
}

/// `parseQueues`: the deprecated `<queues>` go to `queueConfigs`, which
/// the export leaves out; only the warning shows.
fn parse_queues(e: Node<'_, '_>) {
    if first(e, "queues").is_some() {
        tracing::warn!(
            target: SERVER_LOGGER,
            "AMQ222307: The queues element is deprecated and replaced by the addresses element"
        );
    }
}

/// `parseAddresses`.
fn parse_addresses(e: Node<'_, '_>, config: &mut Configuration) -> Result<()> {
    let Some(list) = first(e, "addresses") else {
        return Ok(());
    };
    for node in all(list, "address") {
        let address = crate::parser::parse_address(node, &mut Vec::new())?;
        let queue_configs = address
            .queues
            .iter()
            .map(|q| QueueConfiguration {
                name: q.name.clone(),
                address: Some(address.name.clone()),
                routing_type: Some(q.routing_type.as_str().to_owned()),
                filter_string: q.filter.clone(),
                durable: Some(q.durable),
                user: q.user.clone(),
                max_consumers: q.max_consumers,
                exclusive: q.exclusive,
                group_rebalance: q.group_rebalance,
                group_rebalance_pause_dispatch: q.group_rebalance_pause_dispatch,
                group_buckets: q.group_buckets,
                group_first_key: q.group_first_key.clone(),
                last_value: q.last_value,
                last_value_key: q.last_value_key.clone(),
                non_destructive: q.non_destructive,
                purge_on_no_consumers: Some(q.purge_on_no_consumers),
                enabled: q.enabled,
                consumers_before_dispatch: q.consumers_before_dispatch,
                delay_before_dispatch: q.delay_before_dispatch,
                ring_size: Some(q.ring_size),
            })
            .collect();
        config
            .address_configurations
            .push(CoreAddressConfiguration {
                name: Some(address.name.clone()),
                multicast: address
                    .routing_types
                    .contains(&crate::RoutingType::Multicast),
                anycast: address.routing_types.contains(&crate::RoutingType::Anycast),
                queue_configs,
            });
    }
    Ok(())
}

/// `parseSecurity`: role mappings, then each `<security-setting>`, then the
/// plugins.
fn parse_security(e: Node<'_, '_>, config: &mut Configuration) -> Result<()> {
    let Some(node) = first(e, "security-settings") else {
        return Ok(());
    };
    // securityRoleNameMappings: internal role -> external roles.
    let mut mappings: Vec<(String, Vec<String>)> = Vec::new();
    for m in all(node, "role-mapping") {
        let external = java_trim(&attribute(m, "from").unwrap_or_default()).to_owned();
        let to = attribute(m, "to").unwrap_or_default();
        let mut internal_names: Vec<String> = Vec::new();
        for r in to.split(',') {
            if !internal_names.iter().any(|x| x == r) {
                internal_names.push(r.to_owned());
            }
        }
        for internal in internal_names {
            let internal = java_trim(&internal).to_owned();
            if !internal.is_empty() && !external.is_empty() {
                match mappings.iter_mut().find(|(k, _)| *k == internal) {
                    Some((_, v)) => {
                        if !v.contains(&external) {
                            v.push(external.clone());
                        }
                    }
                    None => mappings.push((internal, vec![external.clone()])),
                }
            }
        }
    }
    for setting in all(node, "security-setting") {
        let m = attribute(setting, "match").unwrap_or_default();
        let roles = parse_security_roles(setting, &mappings);
        config.security_settings.put(m, roles);
    }
    for plugin in all(node, "security-setting-plugin") {
        let class_name = attribute(plugin, "class-name").unwrap_or_default();
        let mut settings = JavaMap::default();
        for child in children(plugin) {
            if child.tag_name().name().eq_ignore_ascii_case("setting") {
                let name = attribute(child, "name").unwrap_or_else(|| "null".to_owned());
                let mut value = attribute(child, "value");
                if let Some(v) = &value
                    && mask::is_enc_masked(v)
                {
                    value = Some(resolve(config, "value", v)?);
                }
                settings.put(name, value);
            }
        }
        check_security_setting_plugin(&class_name, &settings)?;
        config.security_setting_plugins.push(SecuritySettingPlugin {
            class_name,
            settings,
        });
    }
    Ok(())
}

/// The class made, as a `SecuritySettingPlugin`, then `init` of
/// `LegacyLDAPSecuritySettingPlugin`, the one Artemis ships: its refresh
/// interval an `int`, each wildcard conversion one character.
fn check_security_setting_plugin(
    class_name: &str,
    settings: &JavaMap<Option<String>>,
) -> Result<()> {
    if class_name != "org.apache.activemq.artemis.core.server.impl.LegacyLDAPSecuritySettingPlugin"
    {
        return Err(class_not_found(class_name));
    }
    let get = |k: &str| settings.get(k).cloned().flatten();
    if let Some(v) = get("refreshInterval")
        && v.parse::<i32>().is_err()
    {
        return Err(ConfigError::Runtime(format!("For input string: \"{v}\"")));
    }
    for (key, def) in [
        ("anyWordsWildcardConversion", "#"),
        ("singleWordWildcardConversion", "*"),
        ("delimiterWildcardConversion", "."),
    ] {
        let v = get(key).unwrap_or_else(|| def.to_owned());
        if v.encode_utf16().count() != 1 {
            return Err(ConfigError::Runtime(format!(
                "Value for {key} is longer than 1 character: {v}"
            )));
        }
    }
    Ok(())
}

/// `parseSecurityRoles`: each role named by a `<permission>`, with every
/// permission it was given.
fn parse_security_roles(node: Node<'_, '_>, mappings: &[(String, Vec<String>)]) -> Vec<Role> {
    let mut granted: Vec<(String, Vec<&'static str>)> = Vec::new();
    let mut all_roles: Vec<String> = Vec::new();
    for child in children(node) {
        if !child.tag_name().name().eq_ignore_ascii_case("permission") {
            continue;
        }
        let permission = attribute(child, "type").unwrap_or_default();
        let roles = attribute(child, "roles").unwrap_or_default();
        // getMappedRoleNames: a HashSet of the names and their mappings.
        let mut mapped: Vec<String> = Vec::new();
        for role in roles.split(',') {
            if let Some((_, external)) = mappings.iter().find(|(k, _)| k == role) {
                for x in external {
                    if !mapped.contains(x) {
                        mapped.push(x.clone());
                    }
                }
            }
            if !mapped.iter().any(|x| x == role) {
                mapped.push(role.to_owned());
            }
        }
        let hashes: Vec<i32> = mapped
            .iter()
            .map(|r| saranga_buffer::java_hash::string_hash(r))
            .collect();
        let order = saranga_buffer::java_hash::hash_map_order(&hashes, None);
        for i in order {
            let role = java_trim(&mapped[i]).to_owned();
            let kind = match permission.as_str() {
                "send" => Some("send"),
                "consume" => Some("consume"),
                "createDurableQueue" => Some("createDurableQueue"),
                "deleteDurableQueue" => Some("deleteDurableQueue"),
                "createNonDurableQueue" | "createTempQueue" => Some("createNonDurableQueue"),
                "deleteNonDurableQueue" | "deleteTempQueue" => Some("deleteNonDurableQueue"),
                "manage" => Some("manage"),
                "browse" => Some("browse"),
                "createAddress" => Some("createAddress"),
                "deleteAddress" => Some("deleteAddress"),
                "view" => Some("view"),
                "edit" => Some("edit"),
                _ => {
                    tracing::warn!(
                        target: SERVER_LOGGER,
                        "AMQ222177: Wrong configuration for role, {permission} is not a valid permission"
                    );
                    None
                }
            };
            if let Some(kind) = kind {
                match granted.iter_mut().find(|(r, _)| *r == role) {
                    Some((_, kinds)) => kinds.push(kind),
                    None => granted.push((role.clone(), vec![kind])),
                }
            }
            if !all_roles.contains(&role) {
                all_roles.push(role);
            }
        }
    }
    let mut roles: Vec<Role> = Vec::new();
    for name in all_roles {
        let kinds: &[&str] = granted
            .iter()
            .find(|(r, _)| *r == name)
            .map(|(_, k)| k.as_slice())
            .unwrap_or(&[]);
        let has = |k: &str| kinds.contains(&k);
        let role = Role {
            send: has("send"),
            consume: has("consume"),
            create_durable_queue: has("createDurableQueue"),
            delete_durable_queue: has("deleteDurableQueue"),
            create_non_durable_queue: has("createNonDurableQueue"),
            delete_non_durable_queue: has("deleteNonDurableQueue"),
            manage: has("manage"),
            browse: has("browse"),
            create_address: has("createAddress"),
            delete_address: has("deleteAddress"),
            view: has("view"),
            edit: has("edit"),
            name,
        };
        // HashSet.add: an equal role is there once.
        if !roles.contains(&role) {
            roles.push(role);
        }
    }
    roles
}

/// `parseBrokerPlugins`.
fn parse_broker_plugins(e: Node<'_, '_>, config: &mut Configuration) -> Result<()> {
    let Some(list) = first(e, "broker-plugins") else {
        return Ok(());
    };
    for node in all(list, "broker-plugin") {
        let class_name = attribute(node, "class-name").unwrap_or_default();
        let properties = child_properties(node);
        check_broker_plugin(&class_name)?;
        config.broker_plugins.push(BrokerPlugin {
            class_name,
            properties,
        });
    }
    Ok(())
}

/// The broker plugins Artemis ships that `ClassloadingUtil` makes as an
/// `ActiveMQServerPlugin`.
const BROKER_PLUGINS: [&str; 3] = [
    "org.apache.activemq.artemis.core.server.plugin.impl.LoggingActiveMQServerPlugin",
    "org.apache.activemq.artemis.core.server.plugin.impl.NotificationActiveMQServerPlugin",
    "org.apache.activemq.artemis.core.server.plugin.impl.BrokerMessageAuthorizationPlugin",
];

/// `ClassloadingUtil.newInstanceFromClassLoader(..., ActiveMQServerPlugin)`:
/// `ConnectionPeriodicExpiryPlugin` is only an `ActiveMQServerBasePlugin`,
/// and a class Artemis does not have is not found.
fn check_broker_plugin(class_name: &str) -> Result<()> {
    if BROKER_PLUGINS.contains(&class_name) {
        Ok(())
    } else if class_name
        == "org.apache.activemq.artemis.core.server.plugin.impl.ConnectionPeriodicExpiryPlugin"
    {
        Err(ConfigError::Runtime(format!(
            "clazz [{class_name}] is not assignable from expected type: interface org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin"
        )))
    } else {
        Err(class_not_found(class_name))
    }
}

/// What `ClassloadingUtil` throws for a class it cannot load: an
/// `IllegalStateException` around the `ClassNotFoundException`.
fn class_not_found(class_name: &str) -> ConfigError {
    ConfigError::Runtime(format!("java.lang.ClassNotFoundException: {class_name}"))
}

/// `parseMetrics`.
fn parse_metrics(e: Node<'_, '_>, config: &mut Configuration) -> Result<()> {
    let metrics = first(e, "metrics");
    let metrics_plugin = first(e, "metrics-plugin");
    let mut m = MetricsConfiguration::default();
    if let Some(node) = metrics {
        for child in children(node) {
            match child.tag_name().name() {
                "jvm-gc" => m.jvm_gc = node_bool(child),
                "jvm-memory" => m.jvm_memory = node_bool(child),
                "jvm-threads" => m.jvm_thread = node_bool(child),
                "netty-pool" => m.netty_pool = node_bool(child),
                "file-descriptors" => m.file_descriptors = node_bool(child),
                "processor" => m.processor = node_bool(child),
                "uptime" => m.uptime = node_bool(child),
                "logging" => m.logging = node_bool(child),
                "security-caches" => m.security_caches = node_bool(child),
                "executor-services" => m.executor_services = node_bool(child),
                "plugin" => m.plugin = Some(metrics_plugin_of(child)?),
                _ => {}
            }
        }
        if metrics_plugin.is_some() {
            tracing::warn!(
                target: SERVER_LOGGER,
                "AMQ222292: The metrics-plugin element is ignored because the metrics element is defined"
            );
        }
    } else if let Some(node) = metrics_plugin {
        tracing::warn!(
            target: SERVER_LOGGER,
            "AMQ222291: The metrics-plugin element is deprecated and replaced by the metrics element"
        );
        m.plugin = Some(metrics_plugin_of(node)?);
    }
    config.metrics_configuration = Some(m);
    Ok(())
}

/// The metrics plugins Artemis ships.
const METRICS_PLUGINS: [&str; 2] = [
    "org.apache.activemq.artemis.core.server.metrics.plugins.LoggingMetricsPlugin",
    "org.apache.activemq.artemis.core.server.metrics.plugins.SimpleMetricsPlugin",
];

/// `parseMetricsPlugin`: the class made, then its initialisation logged.
fn metrics_plugin_of(n: Node<'_, '_>) -> Result<MetricsPlugin> {
    let class_name = attribute(n, "class-name").unwrap_or_default();
    let properties = child_properties(n);
    if !METRICS_PLUGINS.contains(&class_name.as_str()) {
        return Err(class_not_found(&class_name));
    }
    tracing::info!(
        target: SERVER_LOGGER,
        "AMQ221082: Initializing metrics plugin {class_name} with properties: {}",
        map_text(&properties)
    );
    Ok(MetricsPlugin {
        class_name,
        properties,
    })
}

/// `HashMap.toString`.
fn map_text(map: &JavaMap<String>) -> String {
    let entries: Vec<String> = map
        .java_order()
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    format!("{{{}}}", entries.join(", "))
}

/// `parseConnectorService`.
fn parse_connector_service(e: Node<'_, '_>) -> Result<ConnectorServiceConfiguration> {
    let s = Settings { core: e };
    let name = attribute(e, "name");
    let factory_class_name = s.non_empty_string("factory-class")?.unwrap_or_default();
    let mut params = JavaMap::default();
    for p in all(e, "param") {
        params.put(attribute_or_empty(p, "key"), attribute_or_empty(p, "value"));
    }
    Ok(ConnectorServiceConfiguration {
        name,
        factory_class_name,
        params,
    })
}

/// `parseWildcardConfiguration`: only `routing-enabled` (and the older
/// `enabled`) shows in the export.
fn parse_wildcards(e: Node<'_, '_>, config: &mut Configuration) {
    let s = Settings { core: e };
    if let Some(v) = s.bool("enabled") {
        config.wildcard_routing_enabled = v;
    }
    if let Some(v) = s.bool("routing-enabled") {
        config.wildcard_routing_enabled = v;
    }
}

/// What reading a transport URI threw. `ConfigurationUtils` wraps any
/// exception in a `RuntimeException` with its message; an AMQP
/// connection's `parseURI` lets `URIFactory`'s `NullPointerException` for
/// an unknown schema out as it is.
fn uri_thrown(e: ConfigError, amqp: bool) -> ConfigError {
    match e {
        ConfigError::InvalidUri { reason, .. } if amqp && reason.starts_with("Schema ") => {
            ConfigError::NullPointer(format!("java.lang.NullPointerException: {reason}"))
        }
        ConfigError::InvalidUri { reason, .. } => ConfigError::Runtime(reason),
        e => e,
    }
}
