//! Broker properties applied to a [`Configuration`], as
//! `ConfigurationImpl.populateWithProperties` sets them on the
//! configuration the broker runs on, which `exportConfigAsProperties`
//! then exports.
//!
//! [`crate::broker_properties`] reads the sources and applies each entry
//! to the broker's own [`crate::BrokerConfig`] first; an entry the broker
//! took is applied here too, so the export holds what the broker runs.
//! Values convert as BeanUtils converts them where broker properties set
//! them: a `long` from a byte size (`10M`), a number or a boolean that does
//! not convert to its default.

use super::*;
use crate::broker_properties::kebab;

/// One entry, `path` its key split into segments: `Ok` when it was set,
/// else why not.
pub fn apply_property(
    config: &mut Configuration,
    path: &[&str],
    value: &str,
    remove: bool,
) -> std::result::Result<(), String> {
    match path {
        [] => Err("empty key".into()),
        [bean] => {
            let bean = scalar_bean(bean).ok_or_else(|| format!("{bean} is not a property"))?;
            set_scalar(config, bean, value).expect("a scalar")
        }
        ["addressSettings", m] if remove => {
            config.address_settings.remove(m);
            Ok(())
        }
        ["addressSettings", m, setting] => {
            let fragment = crate::parser::address_setting_fragment(&kebab(setting), value)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("{setting} is not an address setting"))?;
            let merged = match config.address_settings.get(m) {
                Some(existing) => crate::repository::Mergeable::merge_copy(&fragment, existing),
                None => fragment,
            };
            config.address_settings.put(*m, merged);
            Ok(())
        }
        ["securityRoles", m] if remove => {
            config.security_settings.remove(m);
            Ok(())
        }
        ["securityRoles", m, role, permission] => security_role(config, m, role, permission, value),
        ["addressConfigurations", name] if remove => {
            config
                .address_configurations
                .retain(|a| a.name.as_deref() != Some(*name));
            Ok(())
        }
        ["addressConfigurations", name, rest @ ..] => address(config, name, rest, value, remove),
        ["acceptorConfigurations", name] if remove => {
            config
                .acceptor_configs
                .retain(|a| a.name.as_deref() != Some(*name));
            Ok(())
        }
        ["acceptorConfigurations", name, rest @ ..] => acceptor(config, name, rest, value, remove),
        ["divertConfigurations", name] if remove => {
            config.divert_configurations.retain(|d| d.name != *name);
            Ok(())
        }
        ["divertConfigurations", name, field] => divert(config, name, field, value),
        ["bridgeConfigurations", name] if remove => {
            config.bridge_configurations.retain(|b| b.name != *name);
            Ok(())
        }
        ["bridgeConfigurations", name, field] => bridge(config, name, field, value),
        ["clusterConfigurations", name] if remove => {
            config.cluster_configurations.retain(|c| c.name != *name);
            Ok(())
        }
        ["clusterConfigurations", name, field] => cluster(config, name, field, value),
        ["jaasConfigs", realm] if remove => {
            config.jaas_configs.remove(realm);
            Ok(())
        }
        ["jaasConfigs", realm, "modules", module, rest @ ..] => {
            jaas(config, realm, module, rest, value, remove)
        }
        [collection, ..] => Err(format!("{collection} is not supported")),
    }
}

/// The `<core>` element that sets the top-level property `bean` in
/// broker.xml, by which the broker's own configuration takes it:
/// `Some(None)` for one no element sets the same way, which the broker
/// does not act on, and `None` when `bean` is no top-level property. Most
/// elements are the property's name in kebab case; these are not.
pub(crate) fn scalar_element(bean: &str) -> Option<Option<String>> {
    if !SCALARS.contains(&bean) {
        return None;
    }
    let element = match bean {
        "IDCacheSize" => "id-cache-size",
        "JMXDomain" => "jmx-domain",
        "JMXManagementEnabled" => "jmx-management-enabled",
        "JMXNotificationEnabled" => "jmx-notification-enabled",
        "JMXUseBrokerName" => "jmx-use-broker-name",
        "connectionTTLOverride" => "connection-ttl-override",
        "persistIDCache" => "persist-id-cache",
        "pageMaxConcurrentIO" => "page-max-concurrent-io",
        "networkCheckNIC" => "network-check-NIC",
        "networkCheckURLList" => "network-check-URL-list",
        // Saranga's journal is NIO's, as Artemis's is without libaio.
        "journalBufferSize_NIO" => "journal-buffer-size",
        "journalBufferTimeout_NIO" => "journal-buffer-timeout",
        "journalMaxIO_NIO" => "journal-max-io",
        "journalBufferSize_AIO"
        | "journalBufferTimeout_AIO"
        | "journalMaxIO_AIO"
        | "fileDeployerScanPeriod"
        | "brokerPropertiesKeySurround"
        | "wildcardRoutingEnabled" => return Some(None),
        _ => return Some(Some(kebab(bean))),
    };
    Some(Some(element.to_owned()))
}

/// `bean` as a top-level property: itself, or the property whose element
/// is `bean` in kebab case (`idCacheSize` for `IDCacheSize`), as the
/// broker takes it.
fn scalar_bean(bean: &str) -> Option<&'static str> {
    if let Some(b) = SCALARS.iter().find(|b| **b == bean) {
        return Some(b);
    }
    let element = kebab(bean);
    SCALARS
        .iter()
        .find(|b| scalar_element(b) == Some(Some(element.clone())))
        .copied()
}

/// BeanUtils' `BooleanConverter` with its default.
fn boolean(v: &str) -> bool {
    crate::cluster_uri::boolean(Some(v))
}

/// An `int`: a number, or a byte size as the broker reads it; anything
/// else BeanUtils' default, 0.
fn int(v: &str) -> i32 {
    let t = v.trim();
    t.parse()
        .ok()
        .or_else(|| crate::bytes::convert_text_bytes(t).ok().map(|b| b as i32))
        .unwrap_or_default()
}

/// A `long`: `ByteUtil.convertTextBytes`, which throws on what is not one.
fn long(v: &str) -> std::result::Result<i64, String> {
    crate::bytes::convert_text_bytes(v.trim()).map_err(|e| e.to_string())
}

/// A `double`, 0 when it does not convert.
fn double(v: &str) -> f64 {
    v.trim().parse().unwrap_or_default()
}

/// A `List<String>`: the value split at commas, as given.
fn list(v: &str) -> Vec<String> {
    v.split(',').map(str::to_owned).collect()
}

macro_rules! scalars {
    ($($bean:literal => |$c:ident, $v:ident| $set:expr;)*) => {
        /// The top-level bean properties broker properties set.
        const SCALARS: &[&str] = &[$($bean),*];

        /// Sets the top-level property `bean`; `None` if there is none.
        fn set_scalar(
            config: &mut Configuration,
            bean: &str,
            value: &str,
        ) -> Option<std::result::Result<(), String>> {
            match bean {
                $($bean => {
                    fn set(
                        $c: &mut Configuration,
                        $v: &str,
                    ) -> std::result::Result<(), String> {
                        $set;
                        Ok(())
                    }
                    Some(set(config, value))
                })*
                _ => None,
            }
        }
    };
}

scalars! {
    "addressQueueScanPeriod" => |c, v| c.address_queue_scan_period = long(v)?;
    "amqpUseCoreSubscriptionNaming" => |c, v| c.amqp_use_core_subscription_naming = boolean(v);
    "authenticationCacheSize" => |c, v| c.authentication_cache_size = long(v)?;
    "authorizationCacheSize" => |c, v| c.authorization_cache_size = long(v)?;
    "bindingsDirectory" => |c, v| c.bindings_directory = v.to_owned();
    "brokerPropertiesKeySurround" => |c, v| c.broker_properties_key_surround = v.to_owned();
    "clusterPassword" => |c, v| c.cluster_password = v.to_owned();
    "clusterUser" => |c, v| c.cluster_user = v.to_owned();
    "configurationFileRefreshPeriod" => |c, v| c.configuration_file_refresh_period = long(v)?;
    "connectionTtlCheckInterval" => |c, v| c.connection_ttl_check_interval = long(v)?;
    "connectionTTLOverride" => |c, v| c.connection_ttl_override = long(v)?;
    "createBindingsDir" => |c, v| c.create_bindings_dir = boolean(v);
    "createJournalDir" => |c, v| c.create_journal_dir = boolean(v);
    "criticalAnalyzer" => |c, v| c.critical_analyzer = boolean(v);
    "criticalAnalyzerCheckPeriod" => |c, v| c.critical_analyzer_check_period = long(v)?;
    "criticalAnalyzerPolicy" => |c, v| c.critical_analyzer_policy = v.to_owned();
    "criticalAnalyzerTimeout" => |c, v| c.critical_analyzer_timeout = long(v)?;
    "diskScanPeriod" => |c, v| c.disk_scan_period = int(v);
    "failoverOnServerShutdown" => |c, v| c.failover_on_server_shutdown = boolean(v);
    "fileDeployerScanPeriod" => |c, v| c.file_deployment_scan_period = long(v)?;
    "globalMaxMessages" => |c, v| c.global_max_messages = Some(long(v)?);
    "globalMaxSize" => |c, v| c.global_max_size = Some(long(v)?);
    "globalMaxSizePercentOfJvmMaxMemory" => |c, v| c.global_max_size_percent_of_jvm_max_memory = int(v);
    "gracefulShutdownEnabled" => |c, v| c.graceful_shutdown_enabled = boolean(v);
    "gracefulShutdownTimeout" => |c, v| c.graceful_shutdown_timeout = long(v)?;
    "IDCacheSize" => |c, v| c.id_cache_size = int(v);
    "internalNamingPrefix" => |c, v| c.internal_naming_prefix = v.to_owned();
    "JMXDomain" => |c, v| c.jmx_domain = v.to_owned();
    "JMXManagementEnabled" => |c, v| c.jmx_management_enabled = boolean(v);
    "JMXNotificationEnabled" => |c, v| c.jmx_notification_enabled = boolean(v);
    "JMXUseBrokerName" => |c, v| c.jmx_use_broker_name = boolean(v);
    "journalBufferSize_AIO" => |c, v| c.journal_buffer_size_aio = int(v);
    "journalBufferSize_NIO" => |c, v| c.journal_buffer_size_nio = int(v);
    "journalBufferTimeout_AIO" => |c, v| c.journal_buffer_timeout_aio = int(v);
    "journalBufferTimeout_NIO" => |c, v| c.journal_buffer_timeout_nio = int(v);
    "journalCompactMinFiles" => |c, v| c.journal_compact_min_files = int(v);
    "journalCompactPercentage" => |c, v| c.journal_compact_percentage = int(v);
    "journalDatasync" => |c, v| c.journal_datasync = boolean(v);
    "journalDeviceBlockSize" => |c, v| c.journal_device_block_size = Some(int(v));
    "journalDirectory" => |c, v| c.journal_directory = v.to_owned();
    "journalFileOpenTimeout" => |c, v| c.journal_file_open_timeout = int(v);
    "journalFileSize" => |c, v| c.journal_file_size = int(v);
    "journalLockAcquisitionTimeout" => |c, v| c.journal_lock_acquisition_timeout = long(v)?;
    "journalLockMonitorMaxRetries" => |c, v| c.journal_lock_monitor_max_retries = int(v);
    "journalLockMonitorTimeout" => |c, v| c.journal_lock_monitor_timeout = long(v)?;
    "journalMaxAtticFiles" => |c, v| c.journal_max_attic_files = int(v);
    "journalMaxIO_AIO" => |c, v| c.journal_max_io_aio = int(v);
    "journalMaxIO_NIO" => |c, v| c.journal_max_io_nio = int(v);
    "journalMinFiles" => |c, v| c.journal_min_files = int(v);
    "journalPoolFiles" => |c, v| c.journal_pool_files = int(v);
    "journalRetentionDirectory" => |c, v| c.journal_retention_directory = Some(v.to_owned());
    "journalRetentionMaxBytes" => |c, v| c.journal_retention_max_bytes = long(v)?;
    "journalRetentionPeriod" => |c, v| c.journal_retention_period = long(v)?;
    "journalSyncNonTransactional" => |c, v| c.journal_sync_non_transactional = boolean(v);
    "journalSyncTransactional" => |c, v| c.journal_sync_transactional = boolean(v);
    "journalType" => |c, v| c.journal_type = v.to_owned();
    "largeMessagesDirectory" => |c, v| c.large_messages_directory = v.to_owned();
    "largeMessageSync" => |c, v| c.large_message_sync = boolean(v);
    "literalMatchMarkers" => |c, v| c.literal_match_markers = Some(v.to_owned());
    "logJournalWriteRate" => |c, v| c.log_journal_write_rate = boolean(v);
    "managementAddress" => |c, v| c.management_address = v.to_owned();
    "managementMessageRbac" => |c, v| c.management_messages_rbac = boolean(v);
    "managementNotificationAddress" => |c, v| c.management_notification_address = v.to_owned();
    "managementRbacPrefix" => |c, v| c.management_rbac_prefix = v.to_owned();
    "maskPassword" => |c, v| c.mask_password = Some(boolean(v));
    "maxDiskUsage" => |c, v| c.max_disk_usage = int(v);
    "maxRedeliveryRecords" => |c, v| c.max_redelivery_records = int(v);
    "memoryMeasureInterval" => |c, v| c.memory_measure_interval = long(v)?;
    "memoryWarningThreshold" => |c, v| c.memory_warning_threshold = int(v);
    "messageCounterEnabled" => |c, v| c.message_counter_enabled = boolean(v);
    "messageCounterMaxDayHistory" => |c, v| c.message_counter_max_day_history = int(v);
    "messageCounterSamplePeriod" => |c, v| c.message_counter_sample_period = long(v)?;
    "messageExpiryScanPeriod" => |c, v| c.message_expiry_scan_period = long(v)?;
    "messageExpiryThreadPriority" => |c, v| c.message_expiry_thread_priority = int(v);
    "minDiskFree" => |c, v| c.min_disk_free = long(v)?;
    "mirrorAckManagerPageAttempts" => |c, v| c.mirror_ack_manager_page_attempts = int(v);
    "mirrorAckManagerQueueAttempts" => |c, v| c.mirror_ack_manager_queue_attempts = int(v);
    "mirrorAckManagerRetryDelay" => |c, v| c.mirror_ack_manager_retry_delay = int(v);
    "mirrorAckManagerWarnUnacked" => |c, v| c.mirror_ack_manager_warn_unacked = boolean(v);
    "mirrorPageTransaction" => |c, v| c.mirror_page_transaction = boolean(v);
    "mqttSessionScanInterval" => |c, v| c.mqtt_session_scan_interval = long(v)?;
    "mqttSessionStatePersistenceTimeout" => |c, v| c.mqtt_session_state_persistence_timeout = long(v)?;
    "mqttSubscriptionPersistenceEnabled" => |c, v| c.mqtt_subscription_persistence_enabled = boolean(v);
    "name" => |c, v| c.name = v.to_owned();
    "networkCheckList" => |c, v| c.network_check_list = Some(v.to_owned());
    "networkCheckNIC" => |c, v| c.network_check_nic = Some(v.to_owned());
    "networkCheckPeriod" => |c, v| c.network_check_period = long(v)?;
    "networkCheckPing6Command" => |c, v| c.network_check_ping6_command = Some(v.to_owned());
    "networkCheckPingCommand" => |c, v| c.network_check_ping_command = Some(v.to_owned());
    "networkCheckTimeout" => |c, v| c.network_check_timeout = int(v);
    "networkCheckURLList" => |c, v| c.network_url_list = Some(v.to_owned());
    "nodeManagerLockDirectory" => |c, v| c.node_manager_lock_directory = Some(v.to_owned());
    "pageMaxConcurrentIO" => |c, v| c.max_concurrent_page_io = int(v);
    "pageSyncTimeout" => |c, v| c.page_sync_timeout = int(v);
    "pagingDirectory" => |c, v| c.paging_directory = v.to_owned();
    "passwordCodec" => |c, v| c.password_codec = Some(v.to_owned());
    "persistDeliveryCountBeforeDelivery" => |c, v| c.persist_delivery_count_before_delivery = boolean(v);
    "persistenceEnabled" => |c, v| c.persistence_enabled = boolean(v);
    "persistIDCache" => |c, v| c.persist_id_cache = boolean(v);
    "populateValidatedUser" => |c, v| c.populate_validated_user = boolean(v);
    "purgePageFolders" => |c, v| c.purge_page_folders = boolean(v);
    "readWholePage" => |c, v| c.read_whole_page = boolean(v);
    "rejectEmptyValidatedUser" => |c, v| c.reject_empty_validated_user = boolean(v);
    "resolveProtocols" => |c, v| c.resolve_protocols = boolean(v);
    "scheduledThreadPoolMaxSize" => |c, v| c.scheduled_thread_pool_max_size = int(v);
    "securityEnabled" => |c, v| c.security_enabled = boolean(v);
    "securityInvalidationInterval" => |c, v| c.security_invalidation_interval = long(v)?;
    "serverDumpInterval" => |c, v| c.server_dump_interval = long(v)?;
    "suppressSessionNotifications" => |c, v| c.suppress_session_notifications = boolean(v);
    "systemPropertyPrefix" => |c, v| c.system_property_prefix = v.to_owned();
    "temporaryQueueNamespace" => |c, v| c.uuid_namespace = v.to_owned();
    "threadPoolMaxSize" => |c, v| c.thread_pool_max_size = int(v);
    "transactionTimeout" => |c, v| c.transaction_timeout = long(v)?;
    "transactionTimeoutScanPeriod" => |c, v| c.transaction_timeout_scan_period = long(v)?;
    "uuidNamespace" => |c, v| c.uuid_namespace = v.to_owned();
    "viewPermissionMethodMatchPattern" => |c, v| c.view_permission_method_match_pattern = v.to_owned();
    "wildcardRoutingEnabled" => |c, v| c.wildcard_routing_enabled = boolean(v);
}

/// `securityRoles.<match>.<role>.<permission>`: the role made on the match
/// when it has none of that name, with no permission.
fn security_role(
    config: &mut Configuration,
    m: &str,
    role: &str,
    permission: &str,
    value: &str,
) -> std::result::Result<(), String> {
    let granted = boolean(value);
    if !config.security_settings.contains_key(m) {
        config.security_settings.put(m, Vec::new());
    }
    let roles = config.security_settings.get_mut(m).expect("put");
    let i = match roles.iter().position(|r| r.name == role) {
        Some(i) => i,
        None => {
            roles.push(Role::named(role));
            roles.len() - 1
        }
    };
    let r = &mut roles[i];
    let slot = match permission {
        "send" => &mut r.send,
        "consume" => &mut r.consume,
        "createAddress" => &mut r.create_address,
        "deleteAddress" => &mut r.delete_address,
        "createDurableQueue" => &mut r.create_durable_queue,
        "deleteDurableQueue" => &mut r.delete_durable_queue,
        "createNonDurableQueue" => &mut r.create_non_durable_queue,
        "deleteNonDurableQueue" => &mut r.delete_non_durable_queue,
        "manage" => &mut r.manage,
        "browse" => &mut r.browse,
        "view" => &mut r.view,
        "edit" => &mut r.edit,
        _ => return Err(format!("{permission} is not a permission")),
    };
    *slot = granted;
    Ok(())
}

/// `addressConfigurations.<name>.routingTypes` and
/// `.queueConfigs.<queue>.<field>`. A queue made here has no address of
/// its own, so `getAddress` gives its name.
fn address(
    config: &mut Configuration,
    name: &str,
    rest: &[&str],
    value: &str,
    remove: bool,
) -> std::result::Result<(), String> {
    let i = match config
        .address_configurations
        .iter()
        .position(|a| a.name.as_deref() == Some(name))
    {
        Some(i) => i,
        None => {
            config
                .address_configurations
                .push(CoreAddressConfiguration {
                    name: Some(name.to_owned()),
                    ..CoreAddressConfiguration::default()
                });
            config.address_configurations.len() - 1
        }
    };
    let a = &mut config.address_configurations[i];
    match rest {
        ["routingTypes"] => {
            let types: Vec<String> = value
                .split(',')
                .map(|t| t.trim().to_ascii_uppercase())
                .collect();
            a.multicast = types.iter().any(|t| t == "MULTICAST");
            a.anycast = types.iter().any(|t| t == "ANYCAST");
        }
        ["queueConfigs", queue] if remove => a.queue_configs.retain(|q| q.name != *queue),
        ["queueConfigs", queue, field] => {
            let q = match a.queue_configs.iter().position(|q| q.name == *queue) {
                Some(i) => &mut a.queue_configs[i],
                None => {
                    a.queue_configs.push(QueueConfiguration {
                        name: (*queue).to_owned(),
                        ..QueueConfiguration::default()
                    });
                    a.queue_configs.last_mut().expect("pushed")
                }
            };
            queue_field(q, field, value)?;
        }
        _ => return Err(format!("{} is not an address property", rest.join("."))),
    }
    Ok(())
}

fn queue_field(
    q: &mut QueueConfiguration,
    field: &str,
    v: &str,
) -> std::result::Result<(), String> {
    match field {
        "routingType" => q.routing_type = Some(v.trim().to_owned()),
        "address" => q.address = Some(v.to_owned()),
        "filterString" => q.filter_string = Some(v.to_owned()),
        "durable" => q.durable = Some(boolean(v)),
        "user" => q.user = Some(v.to_owned()),
        "maxConsumers" => q.max_consumers = Some(int(v)),
        "purgeOnNoConsumers" => q.purge_on_no_consumers = Some(boolean(v)),
        "exclusive" => q.exclusive = Some(boolean(v)),
        "groupRebalance" => q.group_rebalance = Some(boolean(v)),
        "groupRebalancePauseDispatch" => q.group_rebalance_pause_dispatch = Some(boolean(v)),
        "groupBuckets" => q.group_buckets = Some(int(v)),
        "groupFirstKey" => q.group_first_key = Some(v.to_owned()),
        "lastValue" => q.last_value = Some(boolean(v)),
        "lastValueKey" => q.last_value_key = Some(v.to_owned()),
        "nonDestructive" => q.non_destructive = Some(boolean(v)),
        "consumersBeforeDispatch" => q.consumers_before_dispatch = Some(int(v)),
        "delayBeforeDispatch" => q.delay_before_dispatch = Some(long(v)?),
        "ringSize" => q.ring_size = Some(long(v)?),
        "enabled" => q.enabled = Some(boolean(v)),
        "name" => {}
        other => return Err(format!("{other} is not a queue property")),
    }
    Ok(())
}

/// `acceptorConfigurations.<name>.factoryClassName` and `.params.<key>`.
fn acceptor(
    config: &mut Configuration,
    name: &str,
    rest: &[&str],
    value: &str,
    remove: bool,
) -> std::result::Result<(), String> {
    let made = !config
        .acceptor_configs
        .iter()
        .any(|a| a.name.as_deref() == Some(name));
    let i = match config
        .acceptor_configs
        .iter()
        .position(|a| a.name.as_deref() == Some(name))
    {
        Some(i) => i,
        None => {
            config.acceptor_configs.push(TransportConfiguration {
                name: Some(name.to_owned()),
                // The field's initial value, `"null"`.
                factory_class_name: "null".to_owned(),
                lock_coordinator: None,
                params: JavaMap::default(),
                extra_props: None,
                set_hash: None,
            });
            config.acceptor_configs.len() - 1
        }
    };
    let a = &mut config.acceptor_configs[i];
    // The set places it by the hash it had when added: one read, as it
    // was before this; one this makes, named, once a property of its own
    // is set but before a parameter is (as BeanUtils adds it).
    let direct = matches!(rest, ["factoryClassName"]);
    if a.set_hash.is_none() && !(made && direct) {
        a.set_hash = Some(a.hash_code());
    }
    match rest {
        ["factoryClassName"] => a.factory_class_name = value.to_owned(),
        ["params", key] if remove => {
            a.params.remove(key);
        }
        ["params", key] => a.params.put(*key, JavaValue::Str(value.to_owned())),
        _ => return Err(format!("{} is not an acceptor property", rest.join("."))),
    }
    if a.set_hash.is_none() {
        a.set_hash = Some(a.hash_code());
    }
    Ok(())
}

fn divert(
    config: &mut Configuration,
    name: &str,
    field: &str,
    v: &str,
) -> std::result::Result<(), String> {
    let i = match config
        .divert_configurations
        .iter()
        .position(|d| d.name == name)
    {
        Some(i) => i,
        None => {
            config.divert_configurations.push(DivertConfiguration {
                name: name.to_owned(),
                routing_name: None,
                address: String::new(),
                forwarding_address: String::new(),
                exclusive: false,
                filter_string: None,
                transformer_configuration: None,
                routing_type: "STRIP".to_owned(),
            });
            config.divert_configurations.len() - 1
        }
    };
    let d = &mut config.divert_configurations[i];
    match field {
        "routingName" => d.routing_name = Some(v.to_owned()),
        "address" => d.address = v.to_owned(),
        "forwardingAddress" => d.forwarding_address = v.to_owned(),
        "exclusive" => d.exclusive = boolean(v),
        "filterString" => d.filter_string = Some(v.to_owned()),
        "routingType" => d.routing_type = v.trim().to_owned(),
        other => return Err(format!("{other} is not a divert property")),
    }
    Ok(())
}

fn bridge(
    config: &mut Configuration,
    name: &str,
    field: &str,
    v: &str,
) -> std::result::Result<(), String> {
    let i = match config
        .bridge_configurations
        .iter()
        .position(|b| b.name == name)
    {
        Some(i) => i,
        None => {
            config
                .bridge_configurations
                .push(BridgeConfiguration::new(name.to_owned()));
            config.bridge_configurations.len() - 1
        }
    };
    let b = &mut config.bridge_configurations[i];
    match field {
        "parentName" => b.parent_name = Some(v.to_owned()),
        "queueName" => b.queue_name = v.to_owned(),
        "forwardingAddress" => b.forwarding_address = Some(v.to_owned()),
        "filterString" => b.filter_string = Some(v.to_owned()),
        "staticConnectors" => b.static_connectors = Some(list(v)),
        "discoveryGroupName" => b.discovery_group_name = Some(v.to_owned()),
        "HA" | "ha" => b.ha = boolean(v),
        "retryInterval" => b.retry_interval = long(v)?,
        "retryIntervalMultiplier" => b.retry_interval_multiplier = double(v),
        "initialConnectAttempts" => b.initial_connect_attempts = int(v),
        "reconnectAttempts" => b.reconnect_attempts = int(v),
        "reconnectAttemptsOnSameNode" => b.reconnect_attempts_on_same_node = int(v),
        "useDuplicateDetection" => b.use_duplicate_detection = boolean(v),
        "confirmationWindowSize" => b.confirmation_window_size = int(v),
        "producerWindowSize" => b.producer_window_size = int(v),
        "clientFailureCheckPeriod" => b.client_failure_check_period = long(v)?,
        "user" => b.user = Some(v.to_owned()),
        "password" => b.password = Some(v.to_owned()),
        "connectionTTL" => b.connection_ttl = long(v)?,
        "maxRetryInterval" => b.max_retry_interval = long(v)?,
        "minLargeMessageSize" => b.min_large_message_size = int(v),
        "callTimeout" => b.call_timeout = long(v)?,
        "routingType" => b.routing_type = v.trim().to_owned(),
        "concurrency" => b.concurrency = int(v),
        "pendingAckTimeout" => b.pending_ack_timeout = long(v)?,
        "clientId" => b.client_id = Some(v.to_owned()),
        other => return Err(format!("{other} is not a bridge property")),
    }
    Ok(())
}

fn cluster(
    config: &mut Configuration,
    name: &str,
    field: &str,
    v: &str,
) -> std::result::Result<(), String> {
    let i = match config
        .cluster_configurations
        .iter()
        .position(|c| c.name == name)
    {
        Some(i) => i,
        None => {
            config
                .cluster_configurations
                .push(ClusterConnectionConfiguration::new(name.to_owned()));
            config.cluster_configurations.len() - 1
        }
    };
    let c = &mut config.cluster_configurations[i];
    match field {
        "name" => c.name = v.to_owned(),
        "address" => c.address = Some(v.to_owned()),
        "connectorName" => c.connector_name = Some(v.to_owned()),
        "clientFailureCheckPeriod" => c.client_failure_check_period = long(v)?,
        "connectionTTL" => c.connection_ttl = long(v)?,
        "retryInterval" => c.retry_interval = long(v)?,
        "retryIntervalMultiplier" => c.retry_interval_multiplier = double(v),
        "maxRetryInterval" => c.max_retry_interval = long(v)?,
        "initialConnectAttempts" => c.initial_connect_attempts = int(v),
        "reconnectAttempts" => c.reconnect_attempts = int(v),
        "callTimeout" => c.call_timeout = long(v)?,
        "callFailoverTimeout" => c.call_failover_timeout = long(v)?,
        "duplicateDetection" => c.duplicate_detection = boolean(v),
        "messageLoadBalancingType" => c.message_load_balancing_type = v.to_owned(),
        "staticConnectors" => c.static_connectors = list(v),
        "discoveryGroupName" => c.discovery_group_name = Some(v.to_owned()),
        "maxHops" => c.max_hops = int(v),
        "confirmationWindowSize" => c.confirmation_window_size = int(v),
        "producerWindowSize" => c.producer_window_size = int(v),
        "allowDirectConnectionsOnly" => c.allow_direct_connections_only = boolean(v),
        "minLargeMessageSize" => c.min_large_message_size = int(v),
        "clusterNotificationInterval" => c.cluster_notification_interval = long(v)?,
        "clusterNotificationAttempts" => c.cluster_notification_attempts = int(v),
        "clientId" => c.client_id = Some(v.to_owned()),
        "topologyScannerAttempts" => c.topology_scanner_attempts = int(v),
        other => return Err(format!("{other} is not a cluster connection property")),
    }
    Ok(())
}

/// `jaasConfigs.<realm>.modules.<module>.loginModuleClass`, `.controlFlag`
/// and `.params.<key>`.
fn jaas(
    config: &mut Configuration,
    realm: &str,
    module: &str,
    rest: &[&str],
    value: &str,
    remove: bool,
) -> std::result::Result<(), String> {
    if !config.jaas_configs.contains_key(realm) {
        config.jaas_configs.put(
            realm,
            JaasAppConfiguration {
                name: realm.to_owned(),
                modules: Vec::new(),
            },
        );
    }
    let modules = &mut config.jaas_configs.get_mut(realm).expect("put").modules;
    if rest.is_empty() && remove {
        modules.retain(|m| m.name != module);
        return Ok(());
    }
    let m = match modules.iter().position(|m| m.name == module) {
        Some(i) => &mut modules[i],
        None => {
            modules.push(JaasAppConfigurationEntry {
                name: module.to_owned(),
                login_module_class: None,
                control_flag: None,
                params: JavaMap::default(),
            });
            modules.last_mut().expect("pushed")
        }
    };
    match rest {
        ["loginModuleClass"] => m.login_module_class = Some(value.to_owned()),
        ["controlFlag"] => m.control_flag = Some(value.to_owned()),
        ["params", key] if remove => {
            m.params.remove(key);
        }
        ["params", key] => m.params.put(*key, value.to_owned()),
        _ => return Err(format!("{} is not a JAAS module property", rest.join("."))),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(config: &mut Configuration, key: &str, value: &str) -> std::result::Result<(), String> {
        let path = crate::broker_properties::split_key(key, "\"");
        let path: Vec<&str> = path.iter().map(String::as_str).collect();
        apply_property(config, &path, value, value == "-")
    }

    #[test]
    fn every_scalar_is_one_the_export_writes() {
        let c = Configuration::default();
        let ctx = ExportContext {
            max_memory: 1 << 30,
            broker_instance: String::new(),
            uuid: String::new,
            identity: |c| c.to_owned(),
        };
        let (text, _) = export_as_properties(&c, &ctx);
        for bean in SCALARS {
            let mut c = Configuration::default();
            assert!(set_scalar(&mut c, bean, "1").is_some(), "{bean}");
            // Each but those left `null` by default is exported.
            if !text.contains(&format!("\n{bean}=")) && !text.starts_with(&format!("{bean}=")) {
                assert!(
                    [
                        "journalDeviceBlockSize",
                        "journalRetentionDirectory",
                        "literalMatchMarkers",
                        "maskPassword",
                        "networkCheckList",
                        "networkCheckNIC",
                        "networkCheckURLList",
                        "nodeManagerLockDirectory",
                        "passwordCodec",
                    ]
                    .contains(bean),
                    "{bean}"
                );
            }
        }
    }

    #[test]
    fn elements_and_lenient_names() {
        assert_eq!(
            scalar_element("IDCacheSize"),
            Some(Some("id-cache-size".to_owned()))
        );
        assert_eq!(
            scalar_element("globalMaxSize"),
            Some(Some("global-max-size".to_owned()))
        );
        assert_eq!(scalar_element("journalMaxIO_AIO"), Some(None));
        assert_eq!(scalar_element("nonsense"), None);
        assert_eq!(scalar_bean("idCacheSize"), Some("IDCacheSize"));
        assert_eq!(scalar_bean("jmxDomain"), Some("JMXDomain"));
    }

    #[test]
    fn values_convert_as_bean_utils_does() {
        let mut c = Configuration::default();
        set(&mut c, "globalMaxSize", "10M").unwrap();
        set(&mut c, "IDCacheSize", "abc").unwrap();
        set(&mut c, "persistenceEnabled", "maybe").unwrap();
        set(&mut c, "journalFileSize", "1M").unwrap();
        assert_eq!(c.global_max_size, Some(10 << 20));
        assert_eq!(c.id_cache_size, 0);
        assert!(!c.persistence_enabled);
        assert_eq!(c.journal_file_size, 1 << 20);
        assert!(set(&mut c, "minDiskFree", "lots").is_err());
        assert!(set(&mut c, "brokerPlugins.p.className", "x").is_err());
    }

    #[test]
    fn collections() {
        let mut c = Configuration::default();
        set(&mut c, "securityRoles.\"a.#\".r.send", "true").unwrap();
        set(&mut c, "securityRoles.\"a.#\".r.consume", "true").unwrap();
        set(&mut c, "securityRoles.\"a.#\".r.consume", "false").unwrap();
        let r = &c.security_settings.get("a.#").unwrap()[0];
        assert!(r.send && !r.consume);
        set(&mut c, "securityRoles.\"a.#\"", "-").unwrap();
        assert!(c.security_settings.is_empty());

        set(
            &mut c,
            "addressConfigurations.e.queueConfigs.q.durable",
            "false",
        )
        .unwrap();
        let q = &c.address_configurations[0].queue_configs[0];
        assert_eq!((q.address.as_deref(), q.durable), (None, Some(false)));
        set(&mut c, "addressConfigurations.e.queueConfigs.q", "-").unwrap();
        assert!(c.address_configurations[0].queue_configs.is_empty());

        set(&mut c, "jaasConfigs.r.modules.m.controlFlag", "optional").unwrap();
        set(&mut c, "jaasConfigs.r.modules.m.params.k", "v").unwrap();
        let m = &c.jaas_configs.get("r").unwrap().modules[0];
        assert_eq!(m.control_flag.as_deref(), Some("optional"));
        assert_eq!(m.params.get("k").map(String::as_str), Some("v"));
        set(&mut c, "jaasConfigs.r.modules.m", "-").unwrap();
        assert!(c.jaas_configs.get("r").unwrap().modules.is_empty());

        set(&mut c, "bridgeConfigurations.b.staticConnectors", "a, b").unwrap();
        let b = &c.bridge_configurations[0];
        assert_eq!(b.parent_name.as_deref(), Some("b"));
        assert_eq!(
            b.static_connectors.as_deref(),
            Some(&["a".to_owned(), " b".to_owned()][..])
        );
    }
}
