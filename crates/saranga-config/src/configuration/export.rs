//! `ConfigurationImpl.exportAsProperties`: the configuration as broker
//! properties, one `key=value` line each, as `artemis properties` writes
//! them.
//!
//! Java walks the beans by reflection: each bean's readable properties that
//! have a setter (or are a `Map` or `Collection`), sorted by name ignoring
//! case, `null` ones left out; a collection of strings or an `EnumSet`
//! joined with commas, any other collection walked element by element keyed
//! by the element's `getName()`, a map walked by key. Here each bean gives
//! the properties its getters give, as a [`Node`] tree, and [`write`] walks
//! it as `export` does.

use saranga_buffer::java_hash::{hash_map_order, string_hash};
use saranga_buffer::json::java_double;

use super::*;
use crate::address_settings::AddressSettings;
use crate::broker_connection::{
    AmqpBridgeAddressPolicy, AmqpBridgeElement, AmqpBridgeQueuePolicy, AmqpConnectionElement,
    AmqpFederationAddressPolicy, AmqpFederationElement, AmqpFederationQueuePolicy,
    AmqpMatchElement, AmqpMirrorElement, AmqpQueueMatch,
};
use crate::federation::{FederationDownstreamConfig, FederationStreamConfig};

/// The logger `ActiveMQServerLogger` logs under.
const SERVER_LOGGER: &str = "org.apache.activemq.artemis.core.server";

/// `ConfigurationImpl.REDACTED`.
const REDACTED: &str = "**redacted**";

/// What `extractName` gives an element whose `getName()` is `null`.
const NAME_MUST_BE_SET: &str = "name-attribute-must-be-set-for-properties-key-export";

/// What the export reads from outside the configuration.
pub struct ExportContext {
    /// `Runtime.maxMemory()`, of which an unset `global-max-size` takes its
    /// percentage.
    pub max_memory: i64,
    /// `getBrokerInstance()` as `File.toString` prints it.
    pub broker_instance: String,
    /// `UUIDGenerator.generateStringUUID`, for a divert without a
    /// `routing-name`.
    pub uuid: fn() -> String,
    /// `Object.toString` of an object without its own: the class name,
    /// `@` and the identity hash in hex.
    pub identity: fn(&str) -> String,
}

/// An exported value: text, a bean, or a collection or map walked by key.
#[derive(Clone)]
enum Node {
    Text(String),
    Bean(Bean),
    /// A non-empty collection or map; an empty one exports nothing and is
    /// never made.
    Entries(Vec<(String, Node)>),
    /// A property whose getter, or `export` on its value, throws: the
    /// export stops there, what it wrote kept.
    Fail(ExportError),
}

/// A bean: the lines it writes before its properties (at its own key, or
/// at a child key), then its properties.
#[derive(Clone, Default)]
struct Bean {
    first: Vec<(Option<&'static str>, String)>,
    props: Vec<(String, Node)>,
}

impl Bean {
    fn new() -> Self {
        Self::default()
    }

    /// A line `export` writes before the properties.
    fn first(mut self, key: Option<&'static str>, value: impl Into<String>) -> Self {
        self.first.push((key, value.into()));
        self
    }

    /// A property whose getter never gives `null`.
    fn v(mut self, name: &str, value: impl Value) -> Self {
        self.props.push((name.to_owned(), Node::Text(value.text())));
        self
    }

    /// A property left out when `null`.
    fn o<T: Value>(mut self, name: &str, value: Option<T>) -> Self {
        if let Some(v) = value {
            self.props.push((name.to_owned(), Node::Text(v.text())));
        }
        self
    }

    /// A nested node, left out when `None` (a `null`, or an empty
    /// collection).
    fn n(mut self, name: &str, node: Option<Node>) -> Self {
        if let Some(node) = node {
            self.props.push((name.to_owned(), node));
        }
        self
    }

    fn node(self) -> Node {
        Node::Bean(self)
    }
}

/// `String.valueOf` of a property's value.
trait Value {
    fn text(&self) -> String;
}

impl Value for bool {
    fn text(&self) -> String {
        self.to_string()
    }
}

impl Value for i32 {
    fn text(&self) -> String {
        self.to_string()
    }
}

impl Value for i64 {
    fn text(&self) -> String {
        self.to_string()
    }
}

impl Value for f64 {
    fn text(&self) -> String {
        java_double(*self)
    }
}

impl Value for &str {
    fn text(&self) -> String {
        (*self).to_owned()
    }
}

impl Value for String {
    fn text(&self) -> String {
        self.clone()
    }
}

impl<T: Value> Value for &T {
    fn text(&self) -> String {
        (*self).text()
    }
}

/// A collection of strings: joined with commas, nothing when empty.
fn strings<S: AsRef<str>>(items: impl IntoIterator<Item = S>) -> Option<Node> {
    let items: Vec<String> = items.into_iter().map(|s| s.as_ref().to_owned()).collect();
    (!items.is_empty()).then(|| Node::Text(items.join(",")))
}

/// A collection or map of nodes, nothing when empty.
fn entries(items: Vec<(String, Node)>) -> Option<Node> {
    (!items.is_empty()).then_some(Node::Entries(items))
}

/// A `HashMap<String, String>` built by putting `pairs` in order, walked as
/// Java walks it.
fn string_map<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Option<Node> {
    let mut map: JavaMap<String> = JavaMap::default();
    for (k, v) in pairs {
        map.put(k, v.to_owned());
    }
    java_map(&map, |v| Node::Text(v.clone()))
}

/// A Java `HashMap` walked in its order.
fn java_map<V>(map: &JavaMap<V>, node: impl Fn(&V) -> Node) -> Option<Node> {
    entries(
        map.java_order()
            .into_iter()
            .map(|(k, v)| (k.clone(), node(v)))
            .collect(),
    )
}

/// The elements of a Java `HashSet`, given each one's `hashCode`, in the
/// order it walks them.
fn hash_set_order<T>(items: &[T], hash: impl Fn(&T) -> i32) -> Vec<&T> {
    let hashes: Vec<i32> = items.iter().map(hash).collect();
    hash_map_order(&hashes, None)
        .into_iter()
        .map(|i| &items[i])
        .collect()
}

// Java hash codes.

/// `Objects.hash`: `Arrays.hashCode` of the values.
fn objects_hash(values: &[i32]) -> i32 {
    values
        .iter()
        .fold(1i32, |h, v| h.wrapping_mul(31).wrapping_add(*v))
}

fn str_hash(s: Option<&str>) -> i32 {
    s.map_or(0, string_hash)
}

/// `Long.hashCode`.
fn long_hash(v: i64) -> i32 {
    (v ^ ((v as u64) >> 32) as i64) as i32
}

/// `AbstractMap.hashCode` of a `Map<String, String>`.
fn string_map_hash(pairs: &[(String, String)]) -> i32 {
    let mut map: JavaMap<String> = JavaMap::default();
    for (k, v) in pairs {
        map.put(k.clone(), v.clone());
    }
    map.entries.iter().fold(0i32, |h, (k, v)| {
        h.wrapping_add(string_hash(k) ^ string_hash(v))
    })
}

/// `TransformerConfiguration.hashCode`: `Objects.hash(className,
/// properties)`.
fn transformer_hash(t: Option<&TransformerConfig>) -> i32 {
    t.map_or(0, |t| {
        objects_hash(&[string_hash(&t.class_name), string_map_hash(&t.properties)])
    })
}

/// `AbstractSet.hashCode`: the sum of the elements' hash codes.
fn set_hash<T>(items: &[T], hash: impl Fn(&T) -> i32) -> i32 {
    items.iter().fold(0i32, |h, i| h.wrapping_add(hash(i)))
}

/// `Arrays.hashCode` of a `String[]`, 0 for `null`.
fn strings_array_hash(items: Option<&[String]>) -> i32 {
    items.map_or(0, |items| {
        items
            .iter()
            .fold(1i32, |h, s| h.wrapping_mul(31).wrapping_add(string_hash(s)))
    })
}

fn bool_object_hash(b: Option<bool>) -> i32 {
    b.map_or(0, bool_hash)
}

fn long_object_hash(v: Option<i64>) -> i32 {
    v.map_or(0, long_hash)
}

/// `ConfigurationImpl.quote`: `=` and `:` escaped with two backslashes, a
/// key with a `.` in double quotes.
fn quote(s: &str) -> String {
    let escaped = s.replace('=', "\\\\=").replace(':', "\\\\:");
    if s.contains('.') {
        format!("\"{escaped}\"")
    } else {
        escaped
    }
}

/// `exportKeyValue`: the key's parts quoted and joined with `.`, the value
/// redacted when the key names a password.
fn key_value(out: &mut String, nested: &[String], value: &str) {
    let key = nested
        .iter()
        .map(|s| quote(s))
        .collect::<Vec<_>>()
        .join(".");
    out.push_str(&key);
    out.push('=');
    if key.to_uppercase().contains("PASSWORD") {
        out.push_str(REDACTED);
    } else {
        out.push_str(value);
    }
    out.push_str(LINE_SEPARATOR);
}

/// `BufferedWriter.newLine`: the platform's line separator.
const LINE_SEPARATOR: &str = if cfg!(windows) { "\r\n" } else { "\n" };

/// `export`.
fn write(
    out: &mut String,
    nested: &mut Vec<String>,
    node: &Node,
) -> std::result::Result<(), ExportError> {
    match node {
        Node::Text(text) => key_value(out, nested, text),
        Node::Fail(e) => return Err(e.clone()),
        Node::Entries(items) => {
            for (key, item) in items {
                nested.push(key.clone());
                write(out, nested, item)?;
                nested.pop();
            }
        }
        Node::Bean(bean) => {
            for (key, value) in &bean.first {
                match key {
                    Some(k) => {
                        nested.push((*k).to_owned());
                        key_value(out, nested, value);
                        nested.pop();
                    }
                    None => key_value(out, nested, value),
                }
            }
            // `String.CASE_INSENSITIVE_ORDER`, stably.
            let mut props: Vec<&(String, Node)> = bean.props.iter().collect();
            props.sort_by_cached_key(|(name, _)| case_insensitive_key(name));
            for (name, value) in props {
                nested.push(name.clone());
                write(out, nested, value)?;
                nested.pop();
            }
        }
    }
    Ok(())
}

/// The key `String.CASE_INSENSITIVE_ORDER` compares by: each character
/// upper-cased, then lower-cased.
fn case_insensitive_key(s: &str) -> Vec<char> {
    s.chars()
        .map(|c| {
            let upper = c.to_uppercase().next().unwrap_or(c);
            upper.to_lowercase().next().unwrap_or(upper)
        })
        .collect()
}

/// Why the export failed: the exception Java's throws.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportError {
    /// A `RuntimeException`, by its `getMessage`.
    Runtime(String),
    /// A `NullPointerException`, by its `toString`.
    NullPointer(String),
}

impl std::fmt::Display for ExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(m) | Self::NullPointer(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for ExportError {}

/// `exportAsProperties`: the text of the file, and what stopped the
/// export, the lines written before it kept as the closed `BufferedWriter`
/// keeps them.
pub fn export_as_properties(
    c: &Configuration,
    ctx: &ExportContext,
) -> (String, Option<ExportError>) {
    let root = configuration(c, ctx);
    let mut out = String::new();
    let failed = write(&mut out, &mut Vec::new(), &root).err();
    (out, failed)
}

fn configuration(c: &Configuration, ctx: &ExportContext) -> Node {
    // `isCheckForPrimaryServer` casts a `ReplicaPolicyConfiguration` to
    // `ReplicatedPolicyConfiguration`: the `ClassCastException` comes out of
    // the reflective call as `export`'s `RuntimeException`, at the root.
    let check_for_primary_server = match &c.ha_policy_configuration {
        Some(HaPolicyConfiguration::Replica(_)) => Node::Fail(ExportError::Runtime(
            "accessing: checkForPrimaryServer@[]".to_owned(),
        )),
        _ => Node::Text("false".to_owned()),
    };

    let (ping, ping6) = default_ping_commands();
    let address_settings = || java_map(&c.address_settings, |s| address_settings_bean(s).node());
    let global_max_size = match c.global_max_size {
        Some(v) => v,
        None => {
            // `getDefaultMaxGlobalSizeAsPercentOfJvmMaxMemory`: a long times
            // a float, in float.
            let percent = c.global_max_size_percent_of_jvm_max_memory as f32 / 100.0f32;
            let v = (ctx.max_memory as f32 * percent) as i64;
            tracing::info!(
                target: SERVER_LOGGER,
                "AMQ221057: Global Max Size is being adjusted to 1/2 of the JVM max size (-Xmx). being defined as {v}"
            );
            v
        }
    };
    let critical_analyzer_check_period = if c.critical_analyzer_check_period <= 0 {
        c.critical_analyzer_timeout / 2
    } else {
        c.critical_analyzer_check_period
    };
    let metrics_plugin = c
        .metrics_configuration
        .as_ref()
        .and_then(|m| m.plugin.as_ref())
        .map(|p| metrics_plugin(p).node());

    let mut bean = Bean::new()
        .n("acceptorConfigurations", acceptors(&c.acceptor_configs))
        .n(
            "addressConfigurations",
            entries(
                c.address_configurations
                    .iter()
                    .map(|a| (element_name(a.name.as_deref()), address_bean(a).node()))
                    .collect(),
            ),
        )
        .n("addressesSettings", address_settings())
        .v("addressQueueScanPeriod", c.address_queue_scan_period)
        .n("addressSettings", address_settings())
        .n("AMQPConnections", amqp_connections(c))
        .v(
            "amqpUseCoreSubscriptionNaming",
            c.amqp_use_core_subscription_naming,
        )
        .v("authenticationCacheSize", c.authentication_cache_size)
        .v("authorizationCacheSize", c.authorization_cache_size)
        .v("bindingsDirectory", &c.bindings_directory)
        .n(
            "bridgeConfigurations",
            entries(
                c.bridge_configurations
                    .iter()
                    .map(|b| (b.name.clone(), bridge_bean(b).node()))
                    .collect(),
            ),
        )
        .n(
            "broadcastGroupConfigurations",
            entries(
                c.broadcast_group_configurations
                    .iter()
                    .map(|b| (b.name.clone(), broadcast_group_bean(b).node()))
                    .collect(),
            ),
        )
        .v("brokerInstance", &ctx.broker_instance)
        .v(
            "brokerPropertiesKeySurround",
            &c.broker_properties_key_surround,
        )
        .n("checkForPrimaryServer", Some(check_for_primary_server))
        .n(
            "clusterConfigurations",
            entries(
                c.cluster_configurations
                    .iter()
                    .map(|cc| (cc.name.clone(), cluster_bean(cc).node()))
                    .collect(),
            ),
        )
        .v("clusterPassword", &c.cluster_password)
        .v("clusterUser", &c.cluster_user)
        .v(
            "configurationFileRefreshPeriod",
            c.configuration_file_refresh_period,
        )
        .o("configurationUrl", c.configuration_url.as_deref())
        .n(
            "connectionRouters",
            entries(
                c.connection_routers
                    .iter()
                    .map(|r| (r.name.clone(), router_bean(r).node()))
                    .collect(),
            ),
        )
        .v(
            "connectionTtlCheckInterval",
            c.connection_ttl_check_interval,
        )
        .v("connectionTTLOverride", c.connection_ttl_override)
        .n(
            "connectorConfigurations",
            java_map(&c.connector_configs, |t| transport_bean(t).node()),
        )
        .n(
            "connectorServiceConfigurations",
            entries(
                c.connector_service_configurations
                    .iter()
                    .map(|s| {
                        (
                            element_name(s.name.as_deref()),
                            connector_service_bean(s).node(),
                        )
                    })
                    .collect(),
            ),
        )
        .v("createBindingsDir", c.create_bindings_dir)
        .v("createJournalDir", c.create_journal_dir)
        .v("criticalAnalyzer", c.critical_analyzer)
        .v(
            "criticalAnalyzerCheckPeriod",
            critical_analyzer_check_period,
        )
        .v("criticalAnalyzerPolicy", &c.critical_analyzer_policy)
        .v("criticalAnalyzerTimeout", c.critical_analyzer_timeout)
        .n(
            "discoveryGroupConfigurations",
            entries(
                c.discovery_group_configurations
                    .iter()
                    .map(|d| (d.name.clone(), discovery_group_bean(d).node()))
                    .collect(),
            ),
        )
        .v("diskScanPeriod", c.disk_scan_period)
        .n(
            "divertConfigurations",
            entries(
                c.divert_configurations
                    .iter()
                    .map(|d| (d.name.clone(), divert_bean(d, ctx).node()))
                    .collect(),
            ),
        )
        .v("failoverOnServerShutdown", c.failover_on_server_shutdown)
        .n(
            "federationConfigurations",
            entries(
                c.federation_configurations
                    .iter()
                    .map(|f| (f.name.clone(), federation_bean(f).node()))
                    .collect(),
            ),
        )
        .n(
            "federationDownstreamAuthorization",
            strings(&c.downstream_authorization),
        )
        .v("fileDeployerScanPeriod", c.file_deployment_scan_period)
        .v("globalMaxMessages", c.global_max_messages.unwrap_or(-1))
        .v("globalMaxSize", global_max_size)
        .v(
            "globalMaxSizePercentOfJvmMaxMemory",
            c.global_max_size_percent_of_jvm_max_memory,
        )
        .v("gracefulShutdownEnabled", c.graceful_shutdown_enabled)
        .v("gracefulShutdownTimeout", c.graceful_shutdown_timeout)
        .n(
            "groupingHandlerConfiguration",
            c.grouping_handler_configuration
                .as_ref()
                .map(|g| grouping_handler_bean(g).node()),
        )
        .n(
            "HAPolicyConfiguration",
            c.ha_policy_configuration
                .as_ref()
                .map(|h| ha_policy_bean(h).node()),
        )
        .v("IDCacheSize", c.id_cache_size)
        .n(
            "incomingInterceptorClassNames",
            strings(&c.incoming_interceptor_class_names),
        )
        .v("internalNamingPrefix", &c.internal_naming_prefix)
        .v("JMXDomain", &c.jmx_domain)
        .v("JMXManagementEnabled", c.jmx_management_enabled)
        .v("JMXNotificationEnabled", c.jmx_notification_enabled)
        .v("JMXUseBrokerName", c.jmx_use_broker_name)
        .v("journalBufferSize_AIO", c.journal_buffer_size_aio)
        .v("journalBufferSize_NIO", c.journal_buffer_size_nio)
        .v("journalBufferTimeout_AIO", c.journal_buffer_timeout_aio)
        .v("journalBufferTimeout_NIO", c.journal_buffer_timeout_nio)
        .v("journalCompactMinFiles", c.journal_compact_min_files)
        .v("journalCompactPercentage", c.journal_compact_percentage)
        .v("journalDatasync", c.journal_datasync)
        .o("journalDeviceBlockSize", c.journal_device_block_size)
        .v("journalDirectory", &c.journal_directory)
        .v("journalFileOpenTimeout", c.journal_file_open_timeout)
        .v("journalFileSize", c.journal_file_size)
        .v(
            "journalLockAcquisitionTimeout",
            c.journal_lock_acquisition_timeout,
        )
        .v(
            "journalLockMonitorMaxRetries",
            c.journal_lock_monitor_max_retries,
        )
        .v("journalLockMonitorTimeout", c.journal_lock_monitor_timeout)
        .v("journalMaxAtticFiles", c.journal_max_attic_files)
        .v("journalMaxIO_AIO", c.journal_max_io_aio)
        .v("journalMaxIO_NIO", c.journal_max_io_nio)
        .v("journalMinFiles", c.journal_min_files)
        .v("journalPoolFiles", c.journal_pool_files)
        .o(
            "journalRetentionDirectory",
            c.journal_retention_directory.as_deref(),
        )
        .v("journalRetentionMaxBytes", c.journal_retention_max_bytes)
        .v("journalRetentionPeriod", c.journal_retention_period)
        .v(
            "journalSyncNonTransactional",
            c.journal_sync_non_transactional,
        )
        .v("journalSyncTransactional", c.journal_sync_transactional)
        .v("journalType", &c.journal_type)
        .v("largeMessagesDirectory", &c.large_messages_directory)
        .v("largeMessageSync", c.large_message_sync)
        .o("literalMatchMarkers", c.literal_match_markers.as_deref())
        .n(
            "lockCoordinatorConfigurations",
            entries(
                c.lock_coordinator_configurations
                    .iter()
                    .map(|l| (l.name.clone(), lock_coordinator_bean(l).node()))
                    .collect(),
            ),
        )
        .v("logJournalWriteRate", c.log_journal_write_rate)
        .v("managementAddress", &c.management_address)
        .v("managementMessageRbac", c.management_messages_rbac)
        .v(
            "managementNotificationAddress",
            &c.management_notification_address,
        )
        .v("managementRbacPrefix", &c.management_rbac_prefix)
        .o("maskPassword", c.mask_password)
        .v("maxDiskUsage", c.max_disk_usage)
        .v("maxRedeliveryRecords", c.max_redelivery_records)
        .v("memoryMeasureInterval", c.memory_measure_interval)
        .v("memoryWarningThreshold", c.memory_warning_threshold)
        .v("messageCounterEnabled", c.message_counter_enabled)
        .v(
            "messageCounterMaxDayHistory",
            c.message_counter_max_day_history,
        )
        .v(
            "messageCounterSamplePeriod",
            c.message_counter_sample_period,
        )
        .v("messageExpiryScanPeriod", c.message_expiry_scan_period)
        .v(
            "messageExpiryThreadPriority",
            c.message_expiry_thread_priority,
        )
        .n(
            "metricsConfiguration",
            c.metrics_configuration
                .as_ref()
                .map(|m| metrics_bean(m).node()),
        )
        .n("metricsPlugin", metrics_plugin)
        .v("minDiskFree", c.min_disk_free)
        .v(
            "mirrorAckManagerPageAttempts",
            c.mirror_ack_manager_page_attempts,
        )
        .v(
            "mirrorAckManagerQueueAttempts",
            c.mirror_ack_manager_queue_attempts,
        )
        .v(
            "mirrorAckManagerRetryDelay",
            c.mirror_ack_manager_retry_delay,
        )
        .v(
            "mirrorAckManagerWarnUnacked",
            c.mirror_ack_manager_warn_unacked,
        )
        .v("mirrorPageTransaction", c.mirror_page_transaction)
        .v("mqttSessionScanInterval", c.mqtt_session_scan_interval)
        .v(
            "mqttSessionStatePersistenceTimeout",
            c.mqtt_session_state_persistence_timeout,
        )
        .v(
            "mqttSubscriptionPersistenceEnabled",
            c.mqtt_subscription_persistence_enabled,
        )
        .v("name", &c.name)
        .o("networkCheckList", c.network_check_list.as_deref())
        .o("networkCheckNIC", c.network_check_nic.as_deref())
        .v("networkCheckPeriod", c.network_check_period)
        .v(
            "networkCheckPing6Command",
            c.network_check_ping6_command.as_deref().unwrap_or(ping6),
        )
        .v(
            "networkCheckPingCommand",
            c.network_check_ping_command.as_deref().unwrap_or(ping),
        )
        .v("networkCheckTimeout", c.network_check_timeout)
        .o("networkCheckURLList", c.network_url_list.as_deref())
        .o(
            "nodeManagerLockDirectory",
            c.node_manager_lock_directory.as_deref(),
        )
        .n(
            "outgoingInterceptorClassNames",
            strings(&c.outgoing_interceptor_class_names),
        )
        .v("pageMaxConcurrentIO", c.max_concurrent_page_io)
        .v("pageSyncTimeout", c.page_sync_timeout)
        .v("pagingDirectory", &c.paging_directory)
        .o("passwordCodec", c.password_codec.as_deref())
        .v(
            "persistDeliveryCountBeforeDelivery",
            c.persist_delivery_count_before_delivery,
        )
        .v("persistenceEnabled", c.persistence_enabled)
        .v("persistIDCache", c.persist_id_cache)
        .v("populateValidatedUser", c.populate_validated_user)
        .v("purgePageFolders", c.purge_page_folders)
        .v("readWholePage", c.read_whole_page)
        .v("rejectEmptyValidatedUser", c.reject_empty_validated_user)
        .v("resolveProtocols", c.resolve_protocols)
        .n(
            "resourceLimitSettings",
            java_map(&c.resource_limit_settings, |r| {
                resource_limit_bean(r).node()
            }),
        )
        .v(
            "scheduledThreadPoolMaxSize",
            c.scheduled_thread_pool_max_size,
        )
        .v("securityEnabled", c.security_enabled)
        .v(
            "securityInvalidationInterval",
            c.security_invalidation_interval,
        )
        .n("securityRoles", security_roles(&c.security_settings))
        .n(
            "securitySettingPlugins",
            entries(
                c.security_setting_plugins
                    .iter()
                    .map(|p| {
                        (
                            (ctx.identity)(&p.class_name),
                            security_setting_plugin_bean(p),
                        )
                    })
                    .filter_map(|(k, b)| b.map(|b| (k, b.node())))
                    .collect(),
            ),
        )
        .v("serverDumpInterval", c.server_dump_interval)
        .n(
            "storeConfiguration",
            c.store_configuration.as_ref().map(|s| store_bean(s).node()),
        )
        .v(
            "suppressSessionNotifications",
            c.suppress_session_notifications,
        )
        .v("systemPropertyPrefix", &c.system_property_prefix)
        .v("temporaryQueueNamespace", &c.uuid_namespace)
        .v("threadPoolMaxSize", c.thread_pool_max_size)
        .v("transactionTimeout", c.transaction_timeout)
        .v(
            "transactionTimeoutScanPeriod",
            c.transaction_timeout_scan_period,
        )
        .v("uuidNamespace", &c.uuid_namespace)
        .v(
            "viewPermissionMethodMatchPattern",
            &c.view_permission_method_match_pattern,
        )
        .v("wildcardRoutingEnabled", c.wildcard_routing_enabled);

    // One identity per plugin object, the same in every list it is in.
    let plugins: Vec<(String, Node)> = c
        .broker_plugins
        .iter()
        .filter_map(|p| broker_plugin(p, ctx))
        .collect();
    for (list, holds) in BROKER_PLUGIN_LISTS {
        let entries = if holds {
            plugins
                .iter()
                .map(|(k, n)| (k.clone(), n.clone()))
                .collect()
        } else {
            Vec::new()
        };
        bean = bean.n(list, self::entries(entries));
    }
    bean.node()
}

/// `extractName` of an element whose `getName()` may be `null`.
fn element_name(name: Option<&str>) -> String {
    name.unwrap_or(NAME_MUST_BE_SET).to_owned()
}

/// The `HashSet` of acceptors, walked by `TransportConfiguration.hashCode`.
fn acceptors(acceptors: &[TransportConfiguration]) -> Option<Node> {
    entries(
        hash_set_order(acceptors, TransportConfiguration::hash_code)
            .into_iter()
            .map(|t| (element_name(t.name.as_deref()), transport_bean(t).node()))
            .collect(),
    )
}

/// `TransportConfiguration`.
fn transport_bean(t: &TransportConfiguration) -> Bean {
    let params = |m: &JavaMap<JavaValue>| java_map(m, |v| Node::Text(v.export_text()));
    Bean::new()
        .n("extraParams", t.extra_props.as_ref().and_then(params))
        .v("factoryClassName", &t.factory_class_name)
        .o("lockCoordinator", t.lock_coordinator.as_deref())
        .o("name", t.name.as_deref())
        .n("params", params(&t.params))
}

/// `CoreAddressConfiguration`.
fn address_bean(a: &CoreAddressConfiguration) -> Bean {
    let mut routing_types = Vec::new();
    if a.multicast {
        routing_types.push("MULTICAST");
    }
    if a.anycast {
        routing_types.push("ANYCAST");
    }
    Bean::new()
        .o("name", a.name.as_deref())
        .n(
            "queueConfigs",
            entries(
                a.queue_configs
                    .iter()
                    .map(|q| (q.name.clone(), queue_bean(q).node()))
                    .collect(),
            ),
        )
        .n("routingTypes", strings(routing_types))
}

/// `QueueConfiguration`, with the defaults its getters give.
fn queue_bean(q: &QueueConfiguration) -> Bean {
    Bean::new()
        .v("address", q.address.as_deref().unwrap_or(&q.name))
        .v("autoCreated", false)
        .v("configurationManaged", false)
        .o("consumersBeforeDispatch", q.consumers_before_dispatch)
        .o("delayBeforeDispatch", q.delay_before_dispatch)
        .v("durable", q.durable.unwrap_or(true))
        .o("enabled", q.enabled)
        .o("exclusive", q.exclusive)
        .o("filterString", q.filter_string.as_deref())
        .o("groupBuckets", q.group_buckets)
        .o("groupFirstKey", q.group_first_key.as_deref())
        .o("groupRebalance", q.group_rebalance)
        .o(
            "groupRebalancePauseDispatch",
            q.group_rebalance_pause_dispatch,
        )
        .v("internal", false)
        .o("lastValue", q.last_value)
        .o("lastValueKey", q.last_value_key.as_deref())
        .o("maxConsumers", q.max_consumers)
        .v("name", &q.name)
        .o("nonDestructive", q.non_destructive)
        .o("purgeOnNoConsumers", q.purge_on_no_consumers)
        .o("ringSize", q.ring_size)
        .o("routingType", q.routing_type.as_deref())
        .v("temporary", false)
        .v("transient", false)
        .o("user", q.user.as_deref())
}

/// `AddressSettings`, with the defaults its getters give.
fn address_settings_bean(s: &AddressSettings) -> Bean {
    type A = AddressSettings;
    let page_size_bytes = s.page_size_bytes.unwrap_or(A::DEFAULT_PAGE_SIZE);
    let max_read_page_bytes = s.max_read_page_bytes.unwrap_or(2 * page_size_bytes);
    let max_read_page_messages = s
        .max_read_page_messages
        .unwrap_or(A::DEFAULT_MAX_READ_PAGE_MESSAGES);
    let redelivery_delay = s.redelivery_delay.unwrap_or(A::DEFAULT_REDELIVER_DELAY);
    Bean::new()
        .v(
            "addressFullMessagePolicy",
            s.address_full_message_policy
                .unwrap_or(A::DEFAULT_ADDRESS_FULL_MESSAGE_POLICY)
                .as_str(),
        )
        .v(
            "autoCreateAddresses",
            s.auto_create_addresses
                .unwrap_or(A::DEFAULT_AUTO_CREATE_ADDRESSES),
        )
        .v(
            "autoCreateDeadLetterResources",
            s.auto_create_dead_letter_resources
                .unwrap_or(A::DEFAULT_AUTO_CREATE_DEAD_LETTER_RESOURCES),
        )
        .v(
            "autoCreateExpiryResources",
            s.auto_create_expiry_resources
                .unwrap_or(A::DEFAULT_AUTO_CREATE_EXPIRY_RESOURCES),
        )
        .v(
            "autoCreateJmsQueues",
            s.auto_create_jms_queues
                .unwrap_or(A::DEFAULT_AUTO_CREATE_JMS_QUEUES),
        )
        .v(
            "autoCreateJmsTopics",
            s.auto_create_jms_topics
                .unwrap_or(A::DEFAULT_AUTO_CREATE_TOPICS),
        )
        .v(
            "autoCreateQueues",
            s.auto_create_queues
                .unwrap_or(A::DEFAULT_AUTO_CREATE_QUEUES),
        )
        .v(
            "autoDeleteAddresses",
            s.auto_delete_addresses
                .unwrap_or(A::DEFAULT_AUTO_DELETE_ADDRESSES),
        )
        .v(
            "autoDeleteAddressesDelay",
            s.auto_delete_addresses_delay
                .unwrap_or(A::DEFAULT_AUTO_DELETE_ADDRESSES_DELAY),
        )
        .v(
            "autoDeleteAddressesSkipUsageCheck",
            s.auto_delete_addresses_skip_usage_check
                .unwrap_or(A::DEFAULT_AUTO_DELETE_ADDRESSES_SKIP_USAGE_CHECK),
        )
        .v(
            "autoDeleteCreatedQueues",
            s.auto_delete_created_queues
                .unwrap_or(A::DEFAULT_AUTO_DELETE_CREATED_QUEUES),
        )
        .v(
            "autoDeleteJmsQueues",
            s.auto_delete_jms_queues
                .unwrap_or(A::DEFAULT_AUTO_DELETE_JMS_QUEUES),
        )
        .v(
            "autoDeleteJmsTopics",
            s.auto_delete_jms_topics
                .unwrap_or(A::DEFAULT_AUTO_DELETE_TOPICS),
        )
        .v(
            "autoDeleteQueues",
            s.auto_delete_queues
                .unwrap_or(A::DEFAULT_AUTO_DELETE_QUEUES),
        )
        .v(
            "autoDeleteQueuesDelay",
            s.auto_delete_queues_delay
                .unwrap_or(A::DEFAULT_AUTO_DELETE_QUEUES_DELAY),
        )
        .v(
            "autoDeleteQueuesMessageCount",
            s.auto_delete_queues_message_count
                .unwrap_or(A::DEFAULT_AUTO_DELETE_QUEUES_MESSAGE_COUNT),
        )
        .v(
            "autoDeleteQueuesSkipUsageCheck",
            s.auto_delete_queues_skip_usage_check
                .unwrap_or(A::DEFAULT_AUTO_DELETE_QUEUES_SKIP_USAGE_CHECK),
        )
        .v(
            "configDeleteAddresses",
            s.config_delete_addresses
                .unwrap_or(A::DEFAULT_CONFIG_DELETE_ADDRESSES)
                .as_str(),
        )
        .v(
            "configDeleteDiverts",
            s.config_delete_diverts
                .unwrap_or(A::DEFAULT_CONFIG_DELETE_DIVERTS)
                .as_str(),
        )
        .v(
            "configDeleteQueues",
            s.config_delete_queues
                .unwrap_or(A::DEFAULT_CONFIG_DELETE_QUEUES)
                .as_str(),
        )
        .o("deadLetterAddress", s.dead_letter_address.as_deref())
        .v(
            "deadLetterQueuePrefix",
            s.dead_letter_queue_prefix
                .as_deref()
                .unwrap_or(A::DEFAULT_DEAD_LETTER_QUEUE_PREFIX),
        )
        .v(
            "deadLetterQueueSuffix",
            s.dead_letter_queue_suffix
                .as_deref()
                .unwrap_or(A::DEFAULT_DEAD_LETTER_QUEUE_SUFFIX),
        )
        .v(
            "defaultAddressRoutingType",
            s.default_address_routing_type
                .unwrap_or(A::DEFAULT_ROUTING_TYPE)
                .as_str(),
        )
        .v(
            "defaultConsumersBeforeDispatch",
            s.default_consumers_before_dispatch
                .unwrap_or(A::DEFAULT_CONSUMERS_BEFORE_DISPATCH),
        )
        .v(
            "defaultConsumerWindowSize",
            s.default_consumer_window_size
                .unwrap_or(A::DEFAULT_CONSUMER_WINDOW_SIZE),
        )
        .v(
            "defaultDelayBeforeDispatch",
            s.default_delay_before_dispatch
                .unwrap_or(A::DEFAULT_DELAY_BEFORE_DISPATCH),
        )
        .v(
            "defaultExclusiveQueue",
            s.default_exclusive_queue.unwrap_or(A::DEFAULT_EXCLUSIVE),
        )
        .v(
            "defaultGroupBuckets",
            s.default_group_buckets.unwrap_or(A::DEFAULT_GROUP_BUCKETS),
        )
        .o("defaultGroupFirstKey", s.default_group_first_key.as_deref())
        .v(
            "defaultGroupRebalance",
            s.default_group_rebalance
                .unwrap_or(A::DEFAULT_GROUP_REBALANCE),
        )
        .v(
            "defaultGroupRebalancePauseDispatch",
            s.default_group_rebalance_pause_dispatch
                .unwrap_or(A::DEFAULT_GROUP_REBALANCE_PAUSE_DISPATCH),
        )
        .o("defaultLastValueKey", s.default_last_value_key.as_deref())
        .v(
            "defaultLastValueQueue",
            s.default_last_value_queue
                .unwrap_or(A::DEFAULT_LAST_VALUE_QUEUE),
        )
        .v(
            "defaultMaxConsumers",
            s.default_max_consumers
                .unwrap_or(A::DEFAULT_MAX_QUEUE_CONSUMERS),
        )
        .v(
            "defaultNonDestructive",
            s.default_non_destructive
                .unwrap_or(A::DEFAULT_NON_DESTRUCTIVE),
        )
        .v(
            "defaultPurgeOnNoConsumers",
            s.default_purge_on_no_consumers
                .unwrap_or(A::DEFAULT_PURGE_ON_NO_CONSUMERS),
        )
        .v(
            "defaultQueueRoutingType",
            s.default_queue_routing_type
                .unwrap_or(A::DEFAULT_ROUTING_TYPE)
                .as_str(),
        )
        .v(
            "defaultRingSize",
            s.default_ring_size.unwrap_or(A::DEFAULT_RING_SIZE),
        )
        .o(
            "diskFullMessagePolicy",
            s.disk_full_message_policy.map(|p| p.as_str()),
        )
        .v(
            "enableIngressTimestamp",
            s.enable_ingress_timestamp
                .unwrap_or(A::DEFAULT_ENABLE_INGRESS_TIMESTAMP),
        )
        .v(
            "enableMetrics",
            s.enable_metrics.unwrap_or(A::DEFAULT_ENABLE_METRICS),
        )
        .o("expiryAddress", s.expiry_address.as_deref())
        .v(
            "expiryDelay",
            s.expiry_delay.unwrap_or(A::DEFAULT_EXPIRY_DELAY),
        )
        .v(
            "expiryQueuePrefix",
            s.expiry_queue_prefix
                .as_deref()
                .unwrap_or(A::DEFAULT_EXPIRY_QUEUE_PREFIX),
        )
        .v(
            "expiryQueueSuffix",
            s.expiry_queue_suffix
                .as_deref()
                .unwrap_or(A::DEFAULT_EXPIRY_QUEUE_SUFFIX),
        )
        .o("IDCacheSize", s.id_cache_size)
        .o("initialQueueBufferSize", s.initial_queue_buffer_size)
        .v(
            "managementBrowsePageSize",
            s.management_browse_page_size
                .unwrap_or(A::MANAGEMENT_BROWSE_PAGE_SIZE),
        )
        .v(
            "managementMessageAttributeSizeLimit",
            s.management_message_attribute_size_limit
                .unwrap_or(A::MANAGEMENT_MESSAGE_ATTRIBUTE_SIZE_LIMIT),
        )
        .v(
            "maxDeliveryAttempts",
            s.max_delivery_attempts
                .unwrap_or(A::DEFAULT_MAX_DELIVERY_ATTEMPTS),
        )
        .v(
            "maxExpiryDelay",
            s.max_expiry_delay.unwrap_or(A::DEFAULT_MAX_EXPIRY_DELAY),
        )
        .v("maxReadPageBytes", max_read_page_bytes)
        .v("maxReadPageMessages", max_read_page_messages)
        .v(
            "maxRedeliveryDelay",
            s.max_redelivery_delay
                .unwrap_or(redelivery_delay.wrapping_mul(10)),
        )
        .v(
            "maxSizeBytes",
            s.max_size_bytes.unwrap_or(A::DEFAULT_MAX_SIZE_BYTES),
        )
        .v(
            "maxSizeBytesRejectThreshold",
            s.max_size_bytes_reject_threshold
                .unwrap_or(A::DEFAULT_ADDRESS_REJECT_THRESHOLD),
        )
        .v(
            "maxSizeMessages",
            s.max_size_messages.unwrap_or(A::DEFAULT_MAX_SIZE_MESSAGES),
        )
        .v(
            "messageCounterHistoryDayLimit",
            s.message_counter_history_day_limit
                .unwrap_or(A::DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT),
        )
        .v(
            "minExpiryDelay",
            s.min_expiry_delay.unwrap_or(A::DEFAULT_MIN_EXPIRY_DELAY),
        )
        .v("noExpiry", s.no_expiry.unwrap_or(A::DEFAULT_NO_EXPIRY))
        .v(
            "pageCacheMaxSize",
            s.page_cache_max_size.unwrap_or(A::DEFAULT_PAGE_MAX_CACHE),
        )
        .o(
            "pageFullMessagePolicy",
            s.page_full_message_policy.map(|p| p.as_str()),
        )
        .o("pageLimitBytes", s.page_limit_bytes)
        .o("pageLimitMessages", s.page_limit_messages)
        .v("pageSizeBytes", page_size_bytes)
        .v(
            "prefetchPageBytes",
            s.prefetch_page_bytes.unwrap_or(max_read_page_bytes),
        )
        .v(
            "prefetchPageMessages",
            s.prefetch_page_messages.unwrap_or(max_read_page_messages),
        )
        .v(
            "queuePrefetch",
            s.queue_prefetch.unwrap_or(A::DEFAULT_QUEUE_PREFETCH),
        )
        .v(
            "redeliveryCollisionAvoidanceFactor",
            s.redelivery_collision_avoidance_factor
                .unwrap_or(A::DEFAULT_REDELIVER_COLLISION_AVOIDANCE_FACTOR),
        )
        .v("redeliveryDelay", redelivery_delay)
        .v(
            "redeliveryMultiplier",
            s.redelivery_multiplier
                .unwrap_or(A::DEFAULT_REDELIVER_MULTIPLIER),
        )
        .v(
            "redistributionDelay",
            s.redistribution_delay
                .unwrap_or(A::DEFAULT_REDISTRIBUTION_DELAY),
        )
        .v(
            "retroactiveMessageCount",
            s.retroactive_message_count
                .unwrap_or(A::DEFAULT_RETROACTIVE_MESSAGE_COUNT),
        )
        .v(
            "sendToDLAOnNoRoute",
            s.send_to_dla_on_no_route
                .unwrap_or(A::DEFAULT_SEND_TO_DLA_ON_NO_ROUTE),
        )
        .v(
            "slowConsumerCheckPeriod",
            s.slow_consumer_check_period
                .unwrap_or(A::DEFAULT_SLOW_CONSUMER_CHECK_PERIOD),
        )
        .v(
            "slowConsumerPolicy",
            s.slow_consumer_policy
                .unwrap_or(A::DEFAULT_SLOW_CONSUMER_POLICY)
                .as_str(),
        )
        .v(
            "slowConsumerThreshold",
            s.slow_consumer_threshold
                .unwrap_or(A::DEFAULT_SLOW_CONSUMER_THRESHOLD),
        )
        .v(
            "slowConsumerThresholdMeasurementUnit",
            s.slow_consumer_threshold_measurement_unit
                .unwrap_or(A::DEFAULT_SLOW_CONSUMER_THRESHOLD_MEASUREMENT_UNIT)
                .as_str(),
        )
}

/// `TransformerConfiguration`.
fn transformer_bean(t: &TransformerConfig) -> Bean {
    Bean::new().v("className", &t.class_name).n(
        "properties",
        string_map(t.properties.iter().map(|(k, v)| (k.as_str(), v.as_str()))),
    )
}

/// `BridgeConfiguration`.
fn bridge_bean(b: &BridgeConfiguration) -> Bean {
    Bean::new()
        .v("callTimeout", b.call_timeout)
        .v("clientFailureCheckPeriod", b.client_failure_check_period)
        .o("clientId", b.client_id.as_deref())
        .v("concurrency", b.concurrency)
        .v("configurationManaged", b.configuration_managed)
        .v("confirmationWindowSize", b.confirmation_window_size)
        .v("connectionTTL", b.connection_ttl)
        .o("discoveryGroupName", b.discovery_group_name.as_deref())
        .o("filterString", b.filter_string.as_deref())
        .o("forwardingAddress", b.forwarding_address.as_deref())
        .v("HA", b.ha)
        .v("initialConnectAttempts", b.initial_connect_attempts)
        .v("maxRetryInterval", b.max_retry_interval)
        .v("minLargeMessageSize", b.min_large_message_size)
        .v("name", &b.name)
        .o("parentName", b.parent_name.as_deref())
        .o("password", b.password.as_deref())
        .v("pendingAckTimeout", b.pending_ack_timeout)
        .v("producerWindowSize", b.producer_window_size)
        .v("queueName", &b.queue_name)
        .v("reconnectAttempts", b.reconnect_attempts)
        .v(
            "reconnectAttemptsOnSameNode",
            b.reconnect_attempts_on_same_node,
        )
        .v("retryInterval", b.retry_interval)
        .v("retryIntervalMultiplier", b.retry_interval_multiplier)
        .v("routingType", &b.routing_type)
        .n(
            "staticConnectors",
            b.static_connectors.as_ref().and_then(strings),
        )
        .n(
            "transformerConfiguration",
            b.transformer_configuration
                .as_ref()
                .map(|t| transformer_bean(t).node()),
        )
        .v("useDuplicateDetection", b.use_duplicate_detection)
        .o("user", b.user.as_deref())
}

/// A `BroadcastEndpointFactory`: its class first, then its properties.
fn endpoint_bean(e: &BroadcastEndpoint) -> Bean {
    match e {
        BroadcastEndpoint::Udp {
            group_address,
            group_port,
            local_bind_address,
            local_bind_port,
        } => Bean::new()
            .first(
                None,
                "org.apache.activemq.artemis.api.core.UDPBroadcastEndpointFactory.class",
            )
            .o("groupAddress", group_address.as_deref())
            .v("groupPort", *group_port)
            .o("localBindAddress", local_bind_address.as_deref())
            .v("localBindPort", *local_bind_port),
        BroadcastEndpoint::JGroupsFile { file, channel_name } => Bean::new()
            .first(
                None,
                "org.apache.activemq.artemis.api.core.JGroupsFileBroadcastEndpointFactory.class",
            )
            .o("channelName", channel_name.as_deref())
            .v("file", file),
    }
}

/// `BroadcastGroupConfiguration`.
fn broadcast_group_bean(b: &BroadcastGroupConfiguration) -> Bean {
    Bean::new()
        .v("broadcastPeriod", b.broadcast_period)
        .n("connectorInfos", strings(&b.connector_infos))
        .n(
            "endpointFactory",
            Some(endpoint_bean(&b.endpoint_factory).node()),
        )
        .v("name", &b.name)
}

/// `DiscoveryGroupConfiguration`.
fn discovery_group_bean(d: &DiscoveryGroupConfiguration) -> Bean {
    Bean::new()
        .n(
            "broadcastEndpointFactory",
            Some(endpoint_bean(&d.broadcast_endpoint_factory).node()),
        )
        .v(
            "discoveryInitialWaitTimeout",
            d.discovery_initial_wait_timeout,
        )
        .v("name", &d.name)
        .v("refreshTimeout", d.refresh_timeout)
        .v("stoppingTimeout", d.stopping_timeout)
}

/// `ClusterConnectionConfiguration`.
fn cluster_bean(c: &ClusterConnectionConfiguration) -> Bean {
    Bean::new()
        .o("address", c.address.as_deref())
        .v(
            "allowDirectConnectionsOnly",
            c.allow_direct_connections_only,
        )
        .v("callFailoverTimeout", c.call_failover_timeout)
        .v("callTimeout", c.call_timeout)
        .v("clientFailureCheckPeriod", c.client_failure_check_period)
        .o("clientId", c.client_id.as_deref())
        .v(
            "clusterNotificationAttempts",
            c.cluster_notification_attempts,
        )
        .v(
            "clusterNotificationInterval",
            c.cluster_notification_interval,
        )
        .n(
            "compositeMembers",
            c.composite_members.as_ref().map(|m| {
                // `URISupport.CompositeData`: only `parameters` exports.
                Bean::new()
                    .n("parameters", java_map(m, |v| Node::Text(v.clone())))
                    .node()
            }),
        )
        .v("confirmationWindowSize", c.confirmation_window_size)
        .v("connectionTTL", c.connection_ttl)
        .o("connectorName", c.connector_name.as_deref())
        .o("discoveryGroupName", c.discovery_group_name.as_deref())
        .v("duplicateDetection", c.duplicate_detection)
        .v("initialConnectAttempts", c.initial_connect_attempts)
        .v("maxHops", c.max_hops)
        .v("maxRetryInterval", c.max_retry_interval)
        .v("messageLoadBalancingType", &c.message_load_balancing_type)
        .v("minLargeMessageSize", c.min_large_message_size)
        .v("name", &c.name)
        .v("producerWindowSize", c.producer_window_size)
        .v("reconnectAttempts", c.reconnect_attempts)
        .v("retryInterval", c.retry_interval)
        .v("retryIntervalMultiplier", c.retry_interval_multiplier)
        .n("staticConnectors", strings(&c.static_connectors))
        .v("topologyScannerAttempts", c.topology_scanner_attempts)
}

/// `DivertConfiguration`: an unset `routing-name` is the random UUID the
/// constructor gives.
fn divert_bean(d: &DivertConfiguration, ctx: &ExportContext) -> Bean {
    Bean::new()
        .v("address", &d.address)
        .v("exclusive", d.exclusive)
        .o("filterString", d.filter_string.as_deref())
        .v("forwardingAddress", &d.forwarding_address)
        .v("name", &d.name)
        .v(
            "routingName",
            d.routing_name.clone().unwrap_or_else(ctx.uuid),
        )
        .v("routingType", &d.routing_type)
        .n(
            "transformerConfiguration",
            d.transformer_configuration
                .as_ref()
                .map(|t| transformer_bean(t).node()),
        )
}

/// `ConnectionRouterConfiguration`.
fn router_bean(r: &ConnectionRouterConfiguration) -> Bean {
    Bean::new()
        .n(
            "cacheConfiguration",
            r.cache_configuration.as_ref().map(|c| {
                Bean::new()
                    .v("persisted", c.persisted)
                    .v("timeout", c.timeout)
                    .node()
            }),
        )
        .o("keyFilter", r.key_filter.as_deref())
        .v("keyType", &r.key_type)
        .o("localTargetFilter", r.local_target_filter.as_deref())
        .v("name", &r.name)
        .n(
            "policyConfiguration",
            r.policy_configuration
                .as_ref()
                .map(|p| named_property_bean(p).node()),
        )
        .n(
            "poolConfiguration",
            r.pool_configuration.as_ref().map(|p| {
                Bean::new()
                    .v("checkPeriod", p.check_period)
                    .o("clusterConnection", p.cluster_connection.as_deref())
                    .o("discoveryGroupName", p.discovery_group_name.as_deref())
                    .v("localTargetEnabled", p.local_target_enabled)
                    .o("password", p.password.as_deref())
                    .v("quorumSize", p.quorum_size)
                    .v("quorumTimeout", p.quorum_timeout)
                    .n(
                        "staticConnectors",
                        p.static_connectors.as_ref().and_then(strings),
                    )
                    .o("username", p.username.as_deref())
                    .node()
            }),
        )
}

/// `NamedPropertyConfiguration`: its name first.
fn named_property_bean(p: &NamedPropertyConfiguration) -> Bean {
    Bean::new()
        .first(None, p.name.clone())
        .v("name", &p.name)
        .n(
            "properties",
            java_map(&p.properties, |v| Node::Text(v.clone())),
        )
}

/// `ConnectorServiceConfiguration`.
fn connector_service_bean(s: &ConnectorServiceConfiguration) -> Bean {
    Bean::new()
        .v("factoryClassName", &s.factory_class_name)
        .o("name", s.name.as_deref())
        .n("params", java_map(&s.params, |v| Node::Text(v.clone())))
}

/// `GroupingHandlerConfiguration`.
fn grouping_handler_bean(g: &GroupingHandlerConfiguration) -> Bean {
    Bean::new()
        .v("address", &g.address)
        .v("groupTimeout", g.group_timeout)
        .v("name", &g.name)
        .v("reaperPeriod", g.reaper_period)
        .v("timeout", g.timeout)
}

/// `LockCoordinatorConfiguration`.
fn lock_coordinator_bean(l: &LockCoordinatorConfiguration) -> Bean {
    Bean::new()
        .v("autoStart", l.auto_start)
        .v("checkPeriod", l.check_period)
        .v("className", &l.class_name)
        .v("lockId", &l.lock_id)
        .v("name", &l.name)
        .n(
            "properties",
            java_map(&l.properties, |v| Node::Text(v.clone())),
        )
}

/// `MetricsConfiguration`.
fn metrics_bean(m: &MetricsConfiguration) -> Bean {
    Bean::new()
        .v("executorServices", m.executor_services)
        .v("fileDescriptors", m.file_descriptors)
        .v("jvmGc", m.jvm_gc)
        .v("jvmMemory", m.jvm_memory)
        .v("jvmThread", m.jvm_thread)
        .v("logging", m.logging)
        .v("nettyPool", m.netty_pool)
        .n(
            "plugin",
            m.plugin.as_ref().map(|p| metrics_plugin(p).node()),
        )
        .v("processor", m.processor)
        .v("securityCaches", m.security_caches)
        .v("uptime", m.uptime)
}

const SIMPLE_METRICS_PLUGIN: &str =
    "org.apache.activemq.artemis.core.server.metrics.plugins.SimpleMetricsPlugin";

/// An `ActiveMQMetricsPlugin`: its class and `init=` first, then what its
/// getters give (`SimpleMetricsPlugin`'s options).
fn metrics_plugin(p: &MetricsPlugin) -> Bean {
    let bean = Bean::new()
        .first(None, format!("{}.class", p.class_name))
        .first(Some("init"), "");
    if p.class_name == SIMPLE_METRICS_PLUGIN {
        bean.n(
            "options",
            java_map(&p.properties, |v| Node::Text(v.clone())),
        )
    } else {
        bean
    }
}

/// `ResourceLimitSettings`, with the defaults its getters give.
fn resource_limit_bean(r: &ResourceLimitSettings) -> Bean {
    let max_sessions = r.max_sessions.unwrap_or(-1);
    Bean::new()
        .o("match", r.r#match.as_deref())
        .v("maxConnections", max_sessions)
        .v("maxQueues", r.max_queues.unwrap_or(-1))
        .v("maxSessions", max_sessions)
}

/// `securityRoles`: each match's `HashSet<Role>`, keyed by role name.
fn security_roles(settings: &JavaMap<Vec<Role>>) -> Option<Node> {
    java_map(settings, |roles| {
        Node::Entries(
            hash_set_order(roles, Role::hash_code)
                .into_iter()
                .map(|r| (r.name.clone(), role_bean(r).node()))
                .collect(),
        )
    })
}

/// `Role`.
fn role_bean(r: &Role) -> Bean {
    Bean::new()
        .v("browse", r.browse)
        .v("consume", r.consume)
        .v("createAddress", r.create_address)
        .v("createDurableQueue", r.create_durable_queue)
        .v("createNonDurableQueue", r.create_non_durable_queue)
        .v("deleteAddress", r.delete_address)
        .v("deleteDurableQueue", r.delete_durable_queue)
        .v("deleteNonDurableQueue", r.delete_non_durable_queue)
        .v("edit", r.edit)
        .v("manage", r.manage)
        .v("name", &r.name)
        .v("send", r.send)
        .v("view", r.view)
}

const LEGACY_LDAP_PLUGIN: &str =
    "org.apache.activemq.artemis.core.server.impl.LegacyLDAPSecuritySettingPlugin";

/// A `SecuritySettingPlugin`: `LegacyLDAPSecuritySettingPlugin`'s settings
/// as its `init` takes them; another class exports nothing Saranga knows.
fn security_setting_plugin_bean(p: &SecuritySettingPlugin) -> Option<Bean> {
    if p.class_name != LEGACY_LDAP_PLUGIN {
        return None;
    }
    let get = |k: &str| p.settings.get(k).cloned().flatten();
    let string = |k: &str, def: &str| get(k).unwrap_or_else(|| def.to_owned());
    // `getOption(...).equalsIgnoreCase("true")`.
    let boolean = |k: &str, def: bool| get(k).map_or(def, |v| v.eq_ignore_ascii_case("true"));
    let wildcard = |k: &str, def: char| {
        get(k)
            .and_then(|v| v.chars().next())
            .unwrap_or(def)
            .to_string()
    };
    Some(
        Bean::new()
            .v(
                "adminPermissionValue",
                string("adminPermissionValue", "admin"),
            )
            .v(
                "allowQueueAdminOnRead",
                boolean("allowQueueAdminOnRead", false),
            )
            .v(
                "anyWordsWildcardConversion",
                wildcard("anyWordsWildcardConversion", '#'),
            )
            .v("authentication", string("authentication", "simple"))
            .o("connectionPassword", get("connectionPassword"))
            .o("connectionProtocol", get("connectionProtocol"))
            .v(
                "connectionURL",
                string("connectionURL", "ldap://localhost:1024"),
            )
            .o("connectionUsername", get("connectionUsername"))
            .v(
                "delimiterWildcardConversion",
                wildcard("delimiterWildcardConversion", '.'),
            )
            .v(
                "destinationBase",
                string("destinationBase", "ou=destinations,o=ActiveMQ,ou=system"),
            )
            .v("enableListener", boolean("enableListener", true))
            .v("filter", string("filter", "(cn=*)"))
            .v(
                "initialContextFactory",
                string("initialContextFactory", "com.sun.jndi.ldap.LdapCtxFactory"),
            )
            .v("mapAdminToManage", boolean("mapAdminToManage", false))
            .v("readPermissionValue", string("readPermissionValue", "read"))
            .v(
                "refreshInterval",
                get("refreshInterval")
                    .and_then(|v| v.trim().parse::<i32>().ok())
                    .unwrap_or(0),
            )
            .v("roleAttribute", string("roleAttribute", "uniqueMember"))
            .v(
                "singleWordWildcardConversion",
                wildcard("singleWordWildcardConversion", '*'),
            )
            .v(
                "writePermissionValue",
                string("writePermissionValue", "write"),
            ),
    )
}

/// The broker plugin lists. Every plugin the parser makes is an
/// `ActiveMQServerPlugin`, which is in each list but
/// `brokerAMQPFederationPlugins`.
const BROKER_PLUGIN_LISTS: [(&str, bool); 13] = [
    ("brokerAddressPlugins", true),
    ("brokerAMQPFederationPlugins", false),
    ("brokerBindingPlugins", true),
    ("brokerBridgePlugins", true),
    ("brokerConnectionPlugins", true),
    ("brokerConsumerPlugins", true),
    ("brokerCriticalPlugins", true),
    ("brokerFederationPlugins", true),
    ("brokerMessagePlugins", true),
    ("brokerPlugins", true),
    ("brokerQueuePlugins", true),
    ("brokerResourcePlugins", true),
    ("brokerSessionPlugins", true),
];

const NOTIFICATION_PLUGIN: &str =
    "org.apache.activemq.artemis.core.server.plugin.impl.NotificationActiveMQServerPlugin";

/// A broker plugin keyed as `extractName` keys it (no `getName()`: its
/// `Object.toString`), with what its getters give; `None` when it exports
/// nothing.
fn broker_plugin(p: &BrokerPlugin, ctx: &ExportContext) -> Option<(String, Node)> {
    if p.class_name != NOTIFICATION_PLUGIN {
        // `LoggingActiveMQServerPlugin` and
        // `BrokerMessageAuthorizationPlugin` have no property to export.
        return None;
    }
    // `init`: `Boolean.parseBoolean` of each setting, false by default.
    let flag = |k: &str| {
        p.properties
            .get(k)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    };
    let bean = Bean::new()
        .v(
            "sendAddressNotifications",
            flag("SEND_ADDRESS_NOTIFICATIONS"),
        )
        .v(
            "sendConnectionNotifications",
            flag("SEND_CONNECTION_NOTIFICATIONS"),
        )
        .v(
            "sendDeliveredNotifications",
            flag("SEND_DELIVERED_NOTIFICATIONS"),
        )
        .v(
            "sendExpiredNotifications",
            flag("SEND_EXPIRED_NOTIFICATIONS"),
        );
    Some(((ctx.identity)(&p.class_name), bean.node()))
}

/// A `HAPolicyConfiguration`: its type first, then its properties.
fn ha_policy_bean(h: &HaPolicyConfiguration) -> Bean {
    let scale_down = |s: &Option<ScaleDownConfiguration>| {
        s.as_ref().map(|s| {
            Bean::new()
                .o("clusterName", s.cluster_name.as_deref())
                .v("commitInterval", s.commit_interval)
                .n("connectors", strings(&s.connectors))
                .o("discoveryGroup", s.discovery_group.as_deref())
                .v("enabled", s.enabled)
                .o("groupName", s.group_name.as_deref())
                .node()
        })
    };
    let lock_manager = |d: &Option<DistributedLockManagerConfiguration>| {
        d.as_ref().map(|d| {
            Bean::new()
                .v("className", &d.class_name)
                .n(
                    "properties",
                    java_map(&d.properties, |v| Node::Text(v.clone())),
                )
                .node()
        })
    };
    let bean = Bean::new().first(None, h.type_name());
    match h {
        HaPolicyConfiguration::PrimaryOnly {
            scale_down_configuration,
        } => bean.n(
            "scaleDownConfiguration",
            scale_down(scale_down_configuration),
        ),
        HaPolicyConfiguration::Replicated(p) => bean
            .v("checkForActiveServer", p.check_for_active_server)
            .o("clusterName", p.cluster_name.as_deref())
            .o("groupName", p.group_name.as_deref())
            .v(
                "initialReplicationSyncTimeout",
                p.initial_replication_sync_timeout,
            )
            .v(
                "maxSavedReplicatedJournalsSize",
                p.max_saved_replicated_journals_size,
            )
            .v("quorumSize", p.quorum_size)
            .v("quorumVoteWait", p.quorum_vote_wait)
            .o("retryReplicationWait", p.retry_replication_wait)
            .v("voteOnReplicationFailure", p.vote_on_replication_failure)
            .v("voteRetries", p.vote_retries)
            .v("voteRetryWait", p.vote_retry_wait),
        HaPolicyConfiguration::Replica(p) => bean
            .v("allowFailBack", p.allow_fail_back)
            .o("clusterName", p.cluster_name.as_deref())
            .v("failbackDelay", -1i64)
            .o("groupName", p.group_name.as_deref())
            .v(
                "initialReplicationSyncTimeout",
                p.initial_replication_sync_timeout,
            )
            .v(
                "maxSavedReplicatedJournalsSize",
                p.max_saved_replicated_journals_size,
            )
            .v("quorumSize", p.quorum_size)
            .v("quorumVoteWait", p.quorum_vote_wait)
            .v("restartBackup", p.restart_backup)
            .v("retryReplicationWait", p.retry_replication_wait)
            .n(
                "scaleDownConfiguration",
                scale_down(&p.scale_down_configuration),
            )
            .v("voteOnReplicationFailure", p.vote_on_replication_failure)
            .v("voteRetries", p.vote_retries)
            .v("voteRetryWait", p.vote_retry_wait),
        HaPolicyConfiguration::ReplicationPrimary(p) => bean
            .o("clusterName", p.cluster_name.as_deref())
            .o("coordinationId", p.coordination_id.as_deref())
            .n(
                "distributedManagerConfiguration",
                lock_manager(&p.distributed_manager_configuration),
            )
            .o("groupName", p.group_name.as_deref())
            .v(
                "initialReplicationSyncTimeout",
                p.initial_replication_sync_timeout,
            )
            .v(
                "maxSavedReplicatedJournalsSize",
                p.max_saved_replicated_journals_size,
            )
            .o("retryReplicationWait", p.retry_replication_wait),
        HaPolicyConfiguration::ReplicationBackup(p) => bean
            .v("allowFailBack", p.allow_fail_back)
            .o("clusterName", p.cluster_name.as_deref())
            .n(
                "distributedManagerConfiguration",
                lock_manager(&p.distributed_manager_configuration),
            )
            .o("groupName", p.group_name.as_deref())
            .v(
                "initialReplicationSyncTimeout",
                p.initial_replication_sync_timeout,
            )
            .v(
                "maxSavedReplicatedJournalsSize",
                p.max_saved_replicated_journals_size,
            )
            .v("retryReplicationWait", p.retry_replication_wait),
        HaPolicyConfiguration::SharedStorePrimary(p) => bean
            .v("failbackDelay", -1i64)
            .v("failoverOnServerShutdown", p.failover_on_server_shutdown)
            .v("waitForActivation", p.wait_for_activation),
        HaPolicyConfiguration::SharedStoreBackup(p) => bean
            .v("allowFailBack", p.allow_fail_back)
            .v("failbackDelay", -1i64)
            .v("failoverOnServerShutdown", p.failover_on_server_shutdown)
            .v("restartBackup", p.restart_backup)
            .n(
                "scaleDownConfiguration",
                scale_down(&p.scale_down_configuration),
            ),
        HaPolicyConfiguration::Colocated(p) => bean
            .n(
                "backupConfig",
                p.backup_config.as_ref().map(|b| ha_policy_bean(b).node()),
            )
            .v("backupPortOffset", p.backup_port_offset)
            .v("backupRequestRetries", p.backup_request_retries)
            .v(
                "backupRequestRetryInterval",
                p.backup_request_retry_interval,
            )
            .n("excludedConnectors", strings(&p.excluded_connectors))
            .v("maxBackups", p.max_backups)
            .v("portOffset", p.port_offset)
            .n(
                "primaryConfig",
                p.primary_config.as_ref().map(|b| ha_policy_bean(b).node()),
            )
            .v("requestBackup", p.request_backup),
    }
}

/// A `StoreConfiguration`: its type first, then its properties.
fn store_bean(s: &StoreConfiguration) -> Bean {
    match s {
        // `StoreConfiguration`'s defaults, which `FileStorageConfiguration`
        // keeps.
        StoreConfiguration::File => Bean::new()
            .first(None, "FILE")
            .v("bindingsTableName", "BINDINGS")
            .v("messageTableName", "MESSAGES"),
        StoreConfiguration::Database(d) => Bean::new()
            .first(None, "DATABASE")
            .v("bindingsTableName", &d.bindings_table_name)
            .v("dataSourceClassName", &d.data_source_class_name)
            .n(
                "dataSourceProperties",
                java_map(&d.data_source_properties, |v| Node::Text(v.clone())),
            )
            .v("jdbcAllowedTimeDiff", d.jdbc_allowed_time_diff)
            .o("jdbcConnectionUrl", d.jdbc_connection_url.as_deref())
            .v("jdbcDriverClassName", &d.jdbc_driver_class_name)
            .v(
                "jdbcJournalSyncPeriodMillis",
                d.jdbc_journal_sync_period_millis,
            )
            .v(
                "jdbcLockAcquisitionTimeoutMillis",
                d.jdbc_lock_acquisition_timeout_millis,
            )
            .v("jdbcLockExpirationMillis", d.jdbc_lock_expiration_millis)
            .v("jdbcLockRenewPeriodMillis", d.jdbc_lock_renew_period_millis)
            .v("jdbcNetworkTimeout", d.jdbc_network_timeout)
            .o("jdbcPassword", d.jdbc_password.as_deref())
            .o("jdbcUser", d.jdbc_user.as_deref())
            .v("largeMessageTableName", &d.large_messages_table_name)
            .v("maxPageSizeBytes", d.max_page_size_bytes)
            .v("messageTableName", &d.message_table_name)
            .v(
                "nodeManagerStoreTableName",
                &d.node_manager_store_table_name,
            )
            .v("pageStoreTableName", &d.page_store_table_name),
    }
}

/// `FederationConfiguration`: `addressPolicies` and `queuePolicies` export
/// nothing, `export` filtering their entries by the entry's class.
fn federation_bean(f: &FederationConfig) -> Bean {
    let stream = |s: &FederationStreamConfig| {
        Bean::new()
            .v("name", &s.name)
            .n("policyRefs", strings(java_string_set(&s.policy_refs)))
    };
    let downstream = |d: &FederationDownstreamConfig| {
        stream(&d.stream)
            .v("upstreamConfigurationRef", &d.upstream_connector_ref)
            .node()
    };
    // `FederationTransformerConfiguration`: only `name` exports.
    let transformers = || {
        let mut map: JavaMap<String> = JavaMap::default();
        for (name, _) in &f.transformers {
            map.put(name.clone(), name.clone());
        }
        java_map(&map, |name| Bean::new().v("name", name).node())
    };
    Bean::new()
        .n(
            "credentials",
            Some(
                Bean::new()
                    .o("password", f.password.as_deref())
                    .o("user", f.user.as_deref())
                    .node(),
            ),
        )
        .n(
            "downstreamConfigurations",
            entries(
                f.downstreams
                    .iter()
                    .map(|d| (d.stream.name.clone(), downstream(d)))
                    .collect(),
            ),
        )
        .v("name", &f.name)
        .n("transformerConfigurationMap", transformers())
        .n("transformerConfigurations", transformers())
        .n(
            "upstreamConfigurations",
            entries(
                f.upstreams
                    .iter()
                    .map(|u| (u.name.clone(), stream(u).node()))
                    .collect(),
            ),
        )
}

/// A `HashSet<String>` holding `items`, in the order Java walks it.
fn java_string_set(items: &[String]) -> Vec<&String> {
    hash_set_order(items, |s| string_hash(s))
}

/// `AMQPConnections`: each `AMQPBrokerConnectConfiguration`, its URI
/// first.
fn amqp_connections(c: &Configuration) -> Option<Node> {
    entries(
        c.amqp_broker_connect_configurations
            .iter()
            .zip(&c.amqp_transport_configurations)
            .map(|(a, transports)| (a.name.clone(), amqp_connection_bean(a, transports).node()))
            .collect(),
    )
}

fn amqp_connection_bean(
    a: &crate::AmqpBrokerConnectConfig,
    transports: &[TransportConfiguration],
) -> Bean {
    // Each list getter gives every element; `export` keeps those of the
    // type the property's name gives.
    let of_type = |kind: &str| {
        entries(
            a.elements
                .iter()
                .filter(|e| e.type_name() == kind)
                .map(|e| amqp_element(e, &a.name))
                .collect(),
        )
    };
    Bean::new()
        .first(Some("uri"), a.uri.clone())
        .v("autostart", a.auto_start)
        .n("bridges", of_type("BRIDGE"))
        .n("federations", of_type("FEDERATION"))
        .o("lockCoordinator", a.lock_coordinator.as_deref())
        .n("mirrors", of_type("MIRROR"))
        .v("name", &a.name)
        .o("password", a.password.as_deref())
        .n("peers", of_type("PEER"))
        .n("receivers", of_type("RECEIVER"))
        .v("reconnectAttempts", a.reconnect_attempts)
        .v("retryInterval", a.retry_interval)
        .n("senders", of_type("SENDER"))
        .n(
            "transportConfigurations",
            entries(
                transports
                    .iter()
                    .map(|t| (element_name(t.name.as_deref()), transport_bean(t).node()))
                    .collect(),
            ),
        )
        .o("user", a.user.as_deref())
}

/// An `AMQPBrokerConnectionElement`, keyed by its name, its type first.
fn amqp_element(e: &AmqpConnectionElement, connection: &str) -> (String, Node) {
    let properties =
        |p: &[(String, String)]| string_map(p.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    let (name, bean) = match e {
        AmqpConnectionElement::Sender(m)
        | AmqpConnectionElement::Receiver(m)
        | AmqpConnectionElement::Peer(m) => ("default".to_owned(), match_element(m)),
        AmqpConnectionElement::Mirror(m) => ("default".to_owned(), mirror_element(m)),
        AmqpConnectionElement::Federation(f) => (
            f.name.clone(),
            federation_element(f, connection).n("properties", properties(&f.properties)),
        ),
        AmqpConnectionElement::Bridge(b) => (
            b.name.clone(),
            bridge_element(b).n("properties", properties(&b.properties)),
        ),
    };
    (
        name.clone(),
        bean.first(Some("type"), e.type_name())
            .v("name", &name)
            .node(),
    )
}

fn match_element(m: &AmqpMatchElement) -> Bean {
    Bean::new()
        .o("matchAddress", m.address_match.as_deref())
        .o("queueName", m.queue_name.as_deref())
}

fn mirror_element(m: &AmqpMirrorElement) -> Bean {
    Bean::new()
        .o("addressFilter", m.address_filter.as_deref())
        .v("durable", m.durable)
        .v("messageAcknowledgements", m.message_acknowledgements)
        .n(
            "properties",
            string_map(m.properties.iter().map(|(k, v)| (k.as_str(), v.as_str()))),
        )
        .v("queueCreation", m.queue_creation)
        .v("queueRemoval", m.queue_removal)
        .v("sync", m.sync)
}

fn federation_element(f: &AmqpFederationElement, _connection: &str) -> Bean {
    let address_policies = |p: &[AmqpFederationAddressPolicy]| {
        entries(
            hash_set_order(p, federation_address_policy_hash)
                .into_iter()
                .map(|p| (p.name.clone(), federation_address_policy(p).node()))
                .collect(),
        )
    };
    let queue_policies = |p: &[AmqpFederationQueuePolicy]| {
        entries(
            hash_set_order(p, federation_queue_policy_hash)
                .into_iter()
                .map(|p| (p.name.clone(), federation_queue_policy(p).node()))
                .collect(),
        )
    };
    Bean::new()
        .n(
            "localAddressPolicies",
            address_policies(&f.local_address_policies),
        )
        .n(
            "localQueuePolicies",
            queue_policies(&f.local_queue_policies),
        )
        .n(
            "remoteAddressPolicies",
            address_policies(&f.remote_address_policies),
        )
        .n(
            "remoteQueuePolicies",
            queue_policies(&f.remote_queue_policies),
        )
}

fn bridge_element(b: &AmqpBridgeElement) -> Bean {
    let address_policies = |p: &[AmqpBridgeAddressPolicy]| {
        entries(
            hash_set_order(p, bridge_address_policy_hash)
                .into_iter()
                .map(|p| (p.name.clone(), bridge_address_policy(p).node()))
                .collect(),
        )
    };
    let queue_policies = |p: &[AmqpBridgeQueuePolicy]| {
        entries(
            hash_set_order(p, bridge_queue_policy_hash)
                .into_iter()
                .map(|p| (p.name.clone(), bridge_queue_policy(p).node()))
                .collect(),
        )
    };
    Bean::new()
        .n(
            "bridgeFromAddressPolicies",
            address_policies(&b.from_address_policies),
        )
        .n(
            "bridgeFromQueuePolicies",
            queue_policies(&b.from_queue_policies),
        )
        .n(
            "bridgeToAddressPolicies",
            address_policies(&b.to_address_policies),
        )
        .n(
            "bridgeToQueuePolicies",
            queue_policies(&b.to_queue_policies),
        )
}

/// An address match: the `HashSet<AddressMatch>` keyed by `getName()`,
/// which a federation's gives as its address and a bridge's as `null`.
fn address_matches(matches: &[String], named: bool) -> Option<Node> {
    entries(
        hash_set_order(matches, |m| string_hash(m))
            .into_iter()
            .map(|m| {
                let name = if named {
                    m.clone()
                } else {
                    NAME_MUST_BE_SET.to_owned()
                };
                let bean = Bean::new().v("addressMatch", m);
                let bean = if named { bean.v("name", m) } else { bean };
                (name, bean.node())
            })
            .collect(),
    )
}

/// A queue match: a federation's is named by its address and queue
/// joined, a bridge's not at all.
fn queue_matches(matches: &[AmqpQueueMatch], named: bool) -> Option<Node> {
    entries(
        hash_set_order(matches, queue_match_hash)
            .into_iter()
            .map(|m| {
                let joined = format!("{}{}", m.address_match, m.queue_match);
                let bean = Bean::new()
                    .v("addressMatch", &m.address_match)
                    .v("queueMatch", &m.queue_match);
                if named {
                    (joined.clone(), bean.v("name", joined).node())
                } else {
                    (NAME_MUST_BE_SET.to_owned(), bean.node())
                }
            })
            .collect(),
    )
}

fn queue_match_hash(m: &AmqpQueueMatch) -> i32 {
    objects_hash(&[string_hash(&m.queue_match), string_hash(&m.address_match)])
}

fn policy_properties(p: &[(String, String)]) -> Option<Node> {
    string_map(p.iter().map(|(k, v)| (k.as_str(), v.as_str())))
}

fn transformer_node(t: &Option<TransformerConfig>) -> Option<Node> {
    t.as_ref().map(|t| transformer_bean(t).node())
}

fn federation_address_policy(p: &AmqpFederationAddressPolicy) -> Bean {
    Bean::new()
        .o("allowWildcardGroupings", p.allow_wildcard_groupings)
        .o("autoDelete", p.auto_delete)
        .o("autoDeleteDelay", p.auto_delete_delay)
        .o("autoDeleteMessageCount", p.auto_delete_message_count)
        .o("enableDivertBindings", p.enable_divert_bindings)
        .n("excludes", address_matches(&p.excludes, true))
        .n("includes", address_matches(&p.includes, true))
        .v("maxHops", p.max_hops)
        .v("name", &p.name)
        .n("properties", policy_properties(&p.properties))
        .n("transformerConfiguration", transformer_node(&p.transformer))
}

fn federation_address_policy_hash(p: &AmqpFederationAddressPolicy) -> i32 {
    objects_hash(&[
        string_hash(&p.name),
        set_hash(&p.includes, |m| string_hash(m)),
        set_hash(&p.excludes, |m| string_hash(m)),
        string_map_hash(&p.properties),
        bool_object_hash(p.auto_delete),
        long_object_hash(p.auto_delete_delay),
        long_object_hash(p.auto_delete_message_count),
        p.max_hops,
        bool_object_hash(p.enable_divert_bindings),
        bool_object_hash(p.allow_wildcard_groupings),
        transformer_hash(p.transformer.as_ref()),
    ])
}

fn federation_queue_policy(p: &AmqpFederationQueuePolicy) -> Bean {
    Bean::new()
        .n("excludes", queue_matches(&p.excludes, true))
        .v("includeFederated", p.include_federated)
        .n("includes", queue_matches(&p.includes, true))
        .v("name", &p.name)
        .o("priorityAdjustment", p.priority_adjustment)
        .n("properties", policy_properties(&p.properties))
        .n("transformerConfiguration", transformer_node(&p.transformer))
}

fn federation_queue_policy_hash(p: &AmqpFederationQueuePolicy) -> i32 {
    objects_hash(&[
        string_hash(&p.name),
        bool_hash(p.include_federated),
        set_hash(&p.includes, queue_match_hash),
        set_hash(&p.excludes, queue_match_hash),
        string_map_hash(&p.properties),
        p.priority_adjustment.unwrap_or(0),
        transformer_hash(p.transformer.as_ref()),
    ])
}

fn bridge_address_policy(p: &AmqpBridgeAddressPolicy) -> Bean {
    Bean::new()
        .n("excludes", address_matches(&p.excludes, false))
        .o("filter", p.filter.as_deref())
        .v("includeDivertBindings", p.include_divert_bindings)
        .n("includes", address_matches(&p.includes, false))
        .v("name", &p.name)
        .o("priority", p.priority)
        .n("properties", policy_properties(&p.properties))
        .o("remoteAddress", p.remote_address.as_deref())
        .o("remoteAddressPrefix", p.remote_address_prefix.as_deref())
        .o("remoteAddressSuffix", p.remote_address_suffix.as_deref())
        .n(
            "remoteTerminusCapabilities",
            terminus_capabilities(p.remote_terminus_capabilities.as_deref()),
        )
        .n("transformerConfiguration", transformer_node(&p.transformer))
        .v("useDurableSubscriptions", p.use_durable_subscriptions)
}

/// `remoteTerminusCapabilities`, a `String[]`: `isComplexConfigObject`
/// asks its class for its package, which an array class has none of.
fn terminus_capabilities(capabilities: Option<&[String]>) -> Option<Node> {
    capabilities.map(|_| {
        Node::Fail(ExportError::NullPointer(
            "java.lang.NullPointerException: Cannot invoke \"java.lang.Package.getName()\" because the return value of \"java.lang.Class.getPackage()\" is null".to_owned(),
        ))
    })
}

fn bridge_address_policy_hash(p: &AmqpBridgeAddressPolicy) -> i32 {
    objects_hash(&[
        string_hash(&p.name),
        set_hash(&p.includes, |m| string_hash(m)),
        set_hash(&p.excludes, |m| string_hash(m)),
        string_map_hash(&p.properties),
        str_hash(p.filter.as_deref()),
        p.priority.unwrap_or(0),
        str_hash(p.remote_address.as_deref()),
        str_hash(p.remote_address_prefix.as_deref()),
        str_hash(p.remote_address_suffix.as_deref()),
        bool_hash(p.include_divert_bindings),
        bool_hash(p.use_durable_subscriptions),
        transformer_hash(p.transformer.as_ref()),
        strings_array_hash(p.remote_terminus_capabilities.as_deref()),
    ])
}

fn bridge_queue_policy(p: &AmqpBridgeQueuePolicy) -> Bean {
    Bean::new()
        .n("excludes", queue_matches(&p.excludes, false))
        .o("filter", p.filter.as_deref())
        .n("includes", queue_matches(&p.includes, false))
        .v("name", &p.name)
        .o("priority", p.priority)
        .o("priorityAdjustment", p.priority_adjustment)
        .n("properties", policy_properties(&p.properties))
        .o("remoteAddress", p.remote_address.as_deref())
        .o("remoteAddressPrefix", p.remote_address_prefix.as_deref())
        .o("remoteAddressSuffix", p.remote_address_suffix.as_deref())
        .n(
            "remoteTerminusCapabilities",
            terminus_capabilities(p.remote_terminus_capabilities.as_deref()),
        )
        .n("transformerConfiguration", transformer_node(&p.transformer))
}

fn bridge_queue_policy_hash(p: &AmqpBridgeQueuePolicy) -> i32 {
    objects_hash(&[
        string_hash(&p.name),
        set_hash(&p.includes, queue_match_hash),
        set_hash(&p.excludes, queue_match_hash),
        string_map_hash(&p.properties),
        p.priority.unwrap_or(0),
        p.priority_adjustment.unwrap_or(0),
        str_hash(p.filter.as_deref()),
        str_hash(p.remote_address.as_deref()),
        str_hash(p.remote_address_prefix.as_deref()),
        str_hash(p.remote_address_suffix.as_deref()),
        strings_array_hash(p.remote_terminus_capabilities.as_deref()),
        transformer_hash(p.transformer.as_ref()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_escapes_and_wraps() {
        assert_eq!(quote("a.b"), "\"a.b\"");
        assert_eq!(quote("a=b:c"), "a\\\\=b\\\\:c");
        assert_eq!(quote("plain"), "plain");
    }

    #[test]
    fn password_keys_are_redacted() {
        let mut out = String::new();
        key_value(&mut out, &["clusterPassword".to_owned()], "secret");
        assert_eq!(out, format!("clusterPassword=**redacted**{LINE_SEPARATOR}"));
    }

    #[test]
    fn properties_sort_ignoring_case() {
        let bean = Bean::new().v("b", 1).v("A", 2).v("a_", 3).v("C", 4);
        let mut out = String::new();
        write(&mut out, &mut Vec::new(), &bean.node()).unwrap();
        let keys: Vec<&str> = out.lines().map(|l| l.split('=').next().unwrap()).collect();
        assert_eq!(keys, ["A", "a_", "b", "C"]);
    }
}
