//! `broker.xml` parsing. Ported from `FileDeploymentManager.readConfiguration`,
//! `FileConfigurationParser.parseMainConfig` and `XMLConfigurationUtil`.
//!
//! Elements are matched on their local name, so the usual
//! `xmlns="urn:activemq:core"` declarations make no difference. Like Artemis,
//! a top-level setting such as `<journal-directory>` is looked up as the
//! first element of that name anywhere under `<core>`
//! (`getElementsByTagName`), not only among its direct children.

use roxmltree::{Document, Node, ParsingOptions};

use crate::acceptor::AcceptorConfig;
use crate::address_settings::AddressSettings;
use crate::broker::{
    AddressConfig, BrokerConfig, DivertConfig, DivertRoutingType, JournalConfig, QueueConfig,
    TransformerConfig, Warning,
};
use crate::bytes::convert_text_bytes;
use crate::mask::{self, MaskError};
use crate::security::{CheckType, Role, SecuritySetting, SecuritySettingPluginConfig};
use crate::{ConfigError, Result, RoutingType, java_len, java_parse_bool, java_trim};

const XINCLUDE_NS: &str = "http://www.w3.org/2001/XInclude";

/// Every direct child of `<core>` in `artemis-configuration.xsd`, plus
/// `remoting-interceptors`, which the parser still reads.
const CORE_ELEMENTS: &[&str] = &[
    "name",
    "system-property-prefix",
    "internal-naming-prefix",
    "amqp-use-core-subscription-naming",
    "resolve-protocols",
    "journal-datasync",
    "large-message-sync",
    "persistence-enabled",
    "scheduled-thread-pool-max-size",
    "thread-pool-max-size",
    "graceful-shutdown-enabled",
    "graceful-shutdown-timeout",
    "security-enabled",
    "security-invalidation-interval",
    "authentication-cache-size",
    "authorization-cache-size",
    "journal-lock-acquisition-timeout",
    "journal-lock-monitor-timeout",
    "journal-lock-monitor-max-retries",
    "wild-card-routing-enabled",
    "management-address",
    "management-notification-address",
    "cluster-user",
    "cluster-password",
    "password-codec",
    "mask-password",
    "log-delegate-factory-class-name",
    "jmx-management-enabled",
    "jmx-notification-enabled",
    "jmx-domain",
    "jmx-use-broker-name",
    "message-counter-enabled",
    "message-counter-sample-period",
    "message-counter-max-day-history",
    "connection-ttl-override",
    "connection-ttl-check-interval",
    "configuration-file-refresh-period",
    "temporary-queue-namespace",
    "uuid-namespace",
    "async-connection-execution-enabled",
    "transaction-timeout",
    "transaction-timeout-scan-period",
    "message-expiry-scan-period",
    "message-expiry-thread-priority",
    "address-queue-scan-period",
    "id-cache-size",
    "persist-id-cache",
    "remoting-interceptors",
    "remoting-incoming-interceptors",
    "remoting-outgoing-interceptors",
    "persist-delivery-count-before-delivery",
    "max-redelivery-records",
    "populate-validated-user",
    "reject-empty-validated-user",
    "mqtt-session-scan-interval",
    "mqtt-session-state-persistence-timeout",
    "mqtt-subscription-persistence-enabled",
    "connectors",
    "acceptors",
    "broadcast-groups",
    "discovery-groups",
    "diverts",
    "queues",
    "lock-coordinators",
    "bridges",
    "federations",
    "ha-policy",
    "cluster-connections",
    "broker-connections",
    "connection-routers",
    "grouping-handler",
    "purge-page-folders",
    "paging-directory",
    "bindings-directory",
    "create-bindings-dir",
    "page-max-concurrent-io",
    "read-whole-page",
    "journal-directory",
    "journal-retention-directory",
    "node-manager-lock-directory",
    "create-journal-dir",
    "journal-type",
    "journal-buffer-timeout",
    "journal-device-block-size",
    "journal-buffer-size",
    "journal-sync-transactional",
    "journal-sync-non-transactional",
    "log-journal-write-rate",
    "journal-file-size",
    "journal-min-files",
    "journal-pool-files",
    "journal-compact-percentage",
    "journal-compact-min-files",
    "journal-max-io",
    "journal-file-open-timeout",
    "server-dump-interval",
    "global-max-messages",
    "global-max-size-percent-of-jvm-max-memory",
    "global-max-size",
    "max-disk-usage",
    "min-disk-free",
    "disk-scan-period",
    "memory-warning-threshold",
    "memory-measure-interval",
    "large-messages-directory",
    "store",
    "critical-analyzer",
    "critical-analyzer-timeout",
    "critical-analyzer-check-period",
    "critical-analyzer-policy",
    "page-sync-timeout",
    "mirror-ack-manager-queue-attempts",
    "mirror-ack-manager-page-attempts",
    "mirror-ack-manager-retry-delay",
    "mirror-ack-manager-warn-unacked",
    "mirror-page-transaction",
    "suppress-session-notifications",
    "literal-match-markers",
    "view-permission-method-match-pattern",
    "management-message-rbac",
    "management-rbac-prefix",
    "security-settings",
    "broker-plugins",
    "metrics-plugin",
    "metrics",
    "address-settings",
    "resource-limit-settings",
    "connector-services",
    "addresses",
    "network-check-list",
    "network-check-URL-list",
    "network-check-period",
    "network-check-timeout",
    "network-check-NIC",
    "network-check-ping-command",
    "network-check-ping6-command",
    "wildcard-addresses",
];

/// `Validators`: the range checks Artemis applies to numeric settings.
#[derive(Clone, Copy)]
enum Check {
    Any,
    GtZero,
    GeZero,
    MinusOneOrGtZero,
    MinusOneOrGeZero,
    PositiveInt,
    MinusOneOrPositiveInt,
    Percentage,
    PercentageOrMinusOne,
    PositivePowerOfTwo,
    MaxQueueConsumers,
}

impl Check {
    fn apply(self, name: &str, v: i64) -> Result<i64> {
        let int_max = i32::MAX as i64;
        let pct = v as i32; // `Number.intValue()`
        let (ok, reason) = match self {
            Check::Any => (true, ""),
            Check::GtZero => (v > 0, "must be greater than 0"),
            Check::GeZero => (v >= 0, "must be greater than or equal to 0"),
            Check::MinusOneOrGtZero => (v == -1 || v > 0, "must be -1 or greater than 0"),
            Check::MinusOneOrGeZero => (v >= -1, "must be -1 or greater than or equal to 0"),
            Check::PositiveInt => (v > 0 && v <= int_max, "must be a positive int"),
            Check::MinusOneOrPositiveInt => (
                v == -1 || (v > 0 && v <= int_max),
                "must be -1 or a positive int",
            ),
            Check::Percentage => ((0..=100).contains(&pct), "must be a percentage (0 to 100)"),
            Check::PercentageOrMinusOne => (
                (0..=100).contains(&pct) || pct == -1,
                "must be a percentage (0 to 100) or -1",
            ),
            Check::PositivePowerOfTwo => {
                (v > 0 && v & (v - 1) == 0, "must be a positive power of two")
            }
            Check::MaxQueueConsumers => (pct >= -1, "must be -1 or greater"),
        };
        if ok {
            Ok(v)
        } else {
            Err(ConfigError::invalid(name, v, reason))
        }
    }
}

fn is_named(n: &Node<'_, '_>, name: &str) -> bool {
    n.is_element() && n.tag_name().name() == name
}

/// `getElementsByTagName(name).item(0)`: the first descendant (not `n`
/// itself) with that local name, in document order.
fn first<'a, 'input>(n: Node<'a, 'input>, name: &str) -> Option<Node<'a, 'input>> {
    n.descendants().skip(1).find(|d| is_named(d, name))
}

fn all<'a, 'input>(
    n: Node<'a, 'input>,
    name: &'a str,
) -> impl Iterator<Item = Node<'a, 'input>> + 'a {
    n.descendants().skip(1).filter(move |d| is_named(d, name))
}

/// `Node.getTextContent()`: every descendant text node, concatenated.
fn text_content(n: Node<'_, '_>) -> String {
    n.descendants()
        .filter(|d| d.is_text())
        .filter_map(|d| d.text())
        .collect()
}

fn trimmed_text(n: Node<'_, '_>) -> String {
    java_trim(&text_content(n)).to_owned()
}

fn parse_num<T: std::str::FromStr>(name: &str, text: &str) -> Result<T> {
    text.parse()
        .map_err(|_| ConfigError::invalid(name, text, "not a valid number"))
}

fn node_i32(n: Node<'_, '_>) -> Result<i32> {
    parse_num(n.tag_name().name(), java_trim(&text_content(n)))
}

fn node_i64(n: Node<'_, '_>) -> Result<i64> {
    parse_num(n.tag_name().name(), java_trim(&text_content(n)))
}

fn node_f64(n: Node<'_, '_>) -> Result<f64> {
    parse_num(n.tag_name().name(), java_trim(&text_content(n)))
}

fn node_bool(n: Node<'_, '_>) -> bool {
    java_parse_bool(java_trim(&text_content(n)))
}

fn node_text_bytes(n: Node<'_, '_>) -> Result<i64> {
    let text = trimmed_text(n);
    convert_text_bytes(&text)
        .map_err(|_| ConfigError::invalid(n.tag_name().name(), &text, "not a byte size"))
}

fn node_enum<T: std::str::FromStr<Err = ConfigError>>(n: Node<'_, '_>) -> Result<T> {
    let text = trimmed_text(n);
    text.parse().map_err(|e| match e {
        ConfigError::InvalidValue { reason, .. } => {
            ConfigError::invalid(n.tag_name().name(), &text, reason)
        }
        other => other,
    })
}

/// Typed lookups of top-level settings, in the style of
/// `XMLConfigurationUtil`.
struct Settings<'a, 'input> {
    core: Node<'a, 'input>,
}

impl Settings<'_, '_> {
    fn string(&self, name: &str) -> Option<String> {
        first(self.core, name).map(trimmed_text)
    }

    fn non_empty_string(&self, name: &str) -> Result<Option<String>> {
        match self.string(name) {
            Some(s) if s.is_empty() => Err(ConfigError::invalid(name, "", "must not be empty")),
            other => Ok(other),
        }
    }

    fn bool(&self, name: &str) -> Option<bool> {
        first(self.core, name).map(node_bool)
    }

    fn i32(&self, name: &str, check: Check) -> Result<Option<i32>> {
        first(self.core, name)
            .map(|n| {
                node_i32(n)
                    .and_then(|v| check.apply(name, v.into()))
                    .map(|v| v as i32)
            })
            .transpose()
    }

    fn i64(&self, name: &str, check: Check) -> Result<Option<i64>> {
        first(self.core, name)
            .map(|n| node_i64(n).and_then(|v| check.apply(name, v)))
            .transpose()
    }

    fn text_bytes(&self, name: &str, check: Check) -> Result<Option<i64>> {
        first(self.core, name)
            .map(|n| node_text_bytes(n).and_then(|v| check.apply(name, v)))
            .transpose()
    }
}

pub(crate) fn parse_document(text: &str) -> Result<BrokerConfig> {
    let options = ParsingOptions {
        allow_dtd: true,
        ..ParsingOptions::default()
    };
    let doc = Document::parse_with_options(text, options)?;
    let mut config = BrokerConfig::default();
    let mut warnings = Vec::new();

    for include in doc
        .descendants()
        .filter(|n| n.is_element() && n.tag_name().namespace() == Some(XINCLUDE_NS))
    {
        warnings.push(Warning::XIncludeUnsupported {
            href: include.attribute("href").map(str::to_owned),
        });
    }

    // The root itself is never taken as `<core>` (Artemis searches only its
    // descendants), so a root `<core>` yields defaults and a warning.
    let root = doc.root_element();
    let root_children = elements(root).filter(|_| root.tag_name().name() != "core");
    for child in root_children {
        match child.tag_name().name() {
            "core" => {}
            "jms" => warnings.push(Warning::Unsupported {
                name: "jms".to_owned(),
                reason: "legacy JMS configuration is not supported",
            }),
            _ => warn_unknown(&mut warnings, root, child),
        }
    }

    match first(root, "core") {
        Some(core) => parse_core(core, &mut config, &mut warnings)?,
        None => warnings.push(Warning::NoCoreElement),
    }
    config.warnings = warnings;
    Ok(config)
}

/// Element children, skipping text, comments and `xi:` elements (which are
/// reported separately).
fn elements<'a, 'input>(n: Node<'a, 'input>) -> impl Iterator<Item = Node<'a, 'input>> {
    n.children()
        .filter(|c| c.is_element() && c.tag_name().namespace() != Some(XINCLUDE_NS))
}

fn warn_unknown(warnings: &mut Vec<Warning>, parent: Node<'_, '_>, child: Node<'_, '_>) {
    warnings.push(Warning::UnknownElement {
        parent: parent.tag_name().name().to_owned(),
        name: child.tag_name().name().to_owned(),
    });
}

/// Warns about every element child of `n` not named `allowed`.
fn warn_unknown_children(warnings: &mut Vec<Warning>, n: Node<'_, '_>, allowed: &[&str]) {
    for child in elements(n) {
        if !allowed.contains(&child.tag_name().name()) {
            warn_unknown(warnings, n, child);
        }
    }
}

fn parse_core(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    warn_unknown_children(warnings, core, CORE_ELEMENTS);
    if first(core, "queues").is_some() {
        warnings.push(Warning::Unsupported {
            name: "queues".to_owned(),
            reason: "deprecated; declare queues under <addresses>",
        });
    }

    let s = Settings { core };
    if let Some(name) = s.string("name") {
        config.name = name;
    }
    if let Some(v) = s.bool("persistence-enabled") {
        config.persistence_enabled = v;
    }
    if let Some(v) = s.bool("security-enabled") {
        config.security_enabled = v;
    }
    if let Some(v) = s.text_bytes("global-max-size", Check::MinusOneOrGtZero)?
        && v > 0
    {
        config.global_max_size = Some(v);
    }
    if let Some(v) = s.i64("global-max-messages", Check::MinusOneOrGtZero)? {
        config.global_max_messages = v;
    }
    if let Some(v) = s.i64("message-expiry-scan-period", Check::MinusOneOrGtZero)? {
        config.message_expiry_scan_period = v;
    }
    if let Some(v) = s.i64("transaction-timeout", Check::GtZero)? {
        config.transaction_timeout = v;
    }
    if let Some(v) = s.i64("transaction-timeout-scan-period", Check::GtZero)? {
        config.transaction_timeout_scan_period = v;
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
    if let Some(v) = s.bool("large-message-sync") {
        config.large_message_sync = v;
    }
    parse_security_options(&s, config)?;
    if let Some(v) = s.i64("configuration-file-refresh-period", Check::GtZero)? {
        config.configuration_file_refresh_period = v;
    }

    if let Some(v) = s.non_empty_string("large-messages-directory")? {
        config.large_messages_directory = v;
    }
    if let Some(v) = s.non_empty_string("bindings-directory")? {
        config.bindings_directory = v;
    }
    if let Some(v) = s.non_empty_string("journal-directory")? {
        config.journal_directory = v;
    }
    if let Some(v) = s.non_empty_string("paging-directory")? {
        config.paging_directory = v;
    }
    config.journal = parse_journal(&s)?;

    // Artemis only honours <wild-card-routing-enabled> when <core> also
    // carries an attribute of that name.
    if core.has_attribute("wild-card-routing-enabled") {
        if let Some(v) = s.bool("wild-card-routing-enabled") {
            config.wildcard_addresses.routing_enabled = v;
        }
    } else if first(core, "wild-card-routing-enabled").is_some() {
        warnings.push(Warning::Unsupported {
            name: "wild-card-routing-enabled".to_owned(),
            reason: "Artemis ignores it; use <wildcard-addresses><routing-enabled>",
        });
    }

    if let Some(markers) = s.string("literal-match-markers") {
        let chars: Vec<char> = markers.chars().collect();
        if java_len(&markers) != 2 || chars.len() != 2 {
            return Err(ConfigError::invalid(
                "literal-match-markers",
                markers,
                "must be two characters",
            ));
        }
        config.literal_match_markers = Some((chars[0], chars[1]));
    }

    parse_acceptors(core, config, warnings)?;
    parse_security_settings(core, config, warnings)?;
    parse_address_settings(core, config, warnings)?;
    parse_addresses(core, config, warnings)?;
    parse_diverts(core, config, warnings)?;
    parse_wildcards(core, config, warnings)?;
    Ok(())
}

fn parse_journal(s: &Settings<'_, '_>) -> Result<JournalConfig> {
    let mut j = JournalConfig::default();
    if let Some(t) = first(s.core, "journal-type") {
        j.journal_type = node_enum(t)?;
    }
    j.datasync = s.bool("journal-datasync");
    j.sync_transactional = s.bool("journal-sync-transactional");
    j.sync_non_transactional = s.bool("journal-sync-non-transactional");
    j.file_size = s
        .text_bytes("journal-file-size", Check::PositiveInt)?
        .map(|v| v as i32);
    j.max_attic_files = s.i32("journal-max-attic-files", Check::Any)?;
    j.buffer_timeout = s.i32("journal-buffer-timeout", Check::GeZero)?;
    j.buffer_size = s
        .text_bytes("journal-buffer-size", Check::PositiveInt)?
        .map(|v| v as i32);
    j.max_io = s.i32("journal-max-io", Check::GtZero)?;
    j.device_block_size = s.i32("journal-device-block-size", Check::MinusOneOrGeZero)?;
    j.file_open_timeout = s.i32("journal-file-open-timeout", Check::GtZero)?;
    j.min_files = s.i32("journal-min-files", Check::GtZero)?;
    j.pool_files = s.i32("journal-pool-files", Check::MinusOneOrGtZero)?;
    j.compact_min_files = s.i32("journal-compact-min-files", Check::GeZero)?;
    j.compact_percentage = s.i32("journal-compact-percentage", Check::Percentage)?;
    j.create_journal_dir = s.bool("create-journal-dir");
    j.create_bindings_dir = s.bool("create-bindings-dir");
    j.max_redelivery_records = s.i32("max-redelivery-records", Check::MinusOneOrGeZero)?;
    j.page_sync_timeout = s.i32("page-sync-timeout", Check::GeZero)?;
    j.purge_page_folders = s.bool("purge-page-folders");
    j.max_disk_usage = s.i32("max-disk-usage", Check::PercentageOrMinusOne)?;
    j.disk_scan_period = s.i32("disk-scan-period", Check::MinusOneOrGtZero)?;
    Ok(j)
}

fn parse_acceptors(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    if let Some(list) = first(core, "acceptors") {
        warn_unknown_children(warnings, list, &["acceptor"]);
    }
    for node in all(core, "acceptor") {
        let name = node.attribute("name");
        // `getChildNodes().item(0).getNodeValue()`: the first text node.
        let uri = node
            .first_child()
            .filter(|c| c.is_text())
            .and_then(|c| c.text())
            .ok_or_else(|| ConfigError::InvalidUri {
                uri: String::new(),
                reason: format!("acceptor {:?} has no URI", name.unwrap_or("")),
            })?;
        let mut acceptor = AcceptorConfig::parse(name, uri)?;
        acceptor.lock_coordinator = node.attribute("lock-coordinator").map(str::to_owned);
        config.acceptors.push(acceptor);
    }
    Ok(())
}

fn mask_error(name: &str, value: &str, e: MaskError) -> ConfigError {
    ConfigError::invalid(name, value, e.to_string())
}

/// The security options of `<core>`, in the order `parseMainConfig` reads
/// them: the codec settings come before `cluster-password`, which they
/// decode.
fn parse_security_options(s: &Settings<'_, '_>, config: &mut BrokerConfig) -> Result<()> {
    if let Some(v) = s.i64("security-invalidation-interval", Check::GeZero)? {
        config.security_invalidation_interval = v;
    }
    if let Some(v) = s.i64("authentication-cache-size", Check::GeZero)? {
        config.authentication_cache_size = v;
    }
    if let Some(v) = s.i64("authorization-cache-size", Check::GeZero)? {
        config.authorization_cache_size = v;
    }
    config.mask_password = s.bool("mask-password");
    if let Some(v) = s.non_empty_string("password-codec")? {
        config.password_codec = v;
    }
    if let Some(v) = s.bool("populate-validated-user") {
        config.populate_validated_user = v;
    }
    if let Some(v) = s.bool("reject-empty-validated-user") {
        config.reject_empty_validated_user = v;
    }
    if let Some(text) = s.string("cluster-password") {
        config.cluster_password =
            mask::resolve_mask(config.mask_password, &text, Some(&config.password_codec))
                .map_err(|e| mask_error("cluster-password", &text, e))?;
    }
    if let Some(v) = s.string("cluster-user") {
        config.cluster_user = v;
    }
    Ok(())
}

fn required_attribute(n: Node<'_, '_>, attribute: &str) -> Result<String> {
    n.attribute(attribute)
        .map(str::to_owned)
        .ok_or_else(|| ConfigError::MissingAttribute {
            element: n.tag_name().name().to_owned(),
            attribute: attribute.to_owned(),
        })
}

/// `parseSecurity`: role mappings first, then each `<security-setting>`
/// with its role names mapped, then the plugins.
fn parse_security_settings(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    let Some(list) = first(core, "security-settings") else {
        return Ok(());
    };
    warn_unknown_children(
        warnings,
        list,
        &[
            "security-setting",
            "security-setting-plugin",
            "role-mapping",
        ],
    );

    // Internal role -> the external role names mapped onto it.
    let mut mappings: Vec<(String, Vec<String>)> = Vec::new();
    for node in all(list, "role-mapping") {
        let external = java_trim(&required_attribute(node, "from")?).to_owned();
        let to = required_attribute(node, "to")?;
        for internal in crate::java_split(&to, ',') {
            let internal = java_trim(internal);
            match mappings.iter_mut().find(|(k, _)| k == internal) {
                Some((_, names)) => {
                    if !names.contains(&external) {
                        names.push(external.clone());
                    }
                }
                None if !internal.is_empty() && !external.is_empty() => {
                    mappings.push((internal.to_owned(), vec![external.clone()]));
                }
                None => {}
            }
        }
    }

    for node in all(list, "security-setting") {
        let m = required_attribute(node, "match")?;
        let mut roles: Vec<Role> = Vec::new();
        let permissions =
            elements(node).filter(|c| c.tag_name().name().eq_ignore_ascii_case("permission"));
        for permission in permissions {
            let kind = required_attribute(permission, "type")?;
            let names = required_attribute(permission, "roles")?;
            let check = CheckType::from_permission(&kind);
            if check.is_none() {
                warnings.push(Warning::UnknownPermission {
                    r#type: kind.clone(),
                });
            }
            // `getMappedRoleNames`: the mapped names, then the name itself.
            let mut mapped: Vec<&str> = Vec::new();
            for name in crate::java_split(&names, ',') {
                if let Some((_, external)) = mappings.iter().find(|(k, _)| k == name) {
                    mapped.extend(external.iter().map(String::as_str));
                }
                mapped.push(name);
            }
            for name in mapped {
                let name = java_trim(name);
                let i = match roles.iter().position(|r| r.name == name) {
                    Some(i) => i,
                    None => {
                        roles.push(Role::new(name, []));
                        roles.len() - 1
                    }
                };
                if let Some(check) = check {
                    roles[i].grant(check);
                }
            }
        }
        // `putSecurityRoles` is a map put: a repeated match replaces.
        let setting = SecuritySetting { roles };
        match config.security_settings.iter_mut().find(|(k, _)| *k == m) {
            Some(entry) => entry.1 = setting,
            None => config.security_settings.push((m, setting)),
        }
    }

    for node in all(list, "security-setting-plugin") {
        let class_name = required_attribute(node, "class-name")?;
        let mut settings = Vec::new();
        let children =
            elements(node).filter(|c| c.tag_name().name().eq_ignore_ascii_case("setting"));
        for setting in children {
            let name = setting.attribute("name").unwrap_or_default().to_owned();
            let mut value = setting.attribute("value").unwrap_or_default().to_owned();
            if mask::is_enc_masked(&value) {
                value =
                    mask::resolve_mask(config.mask_password, &value, Some(&config.password_codec))
                        .map_err(|e| mask_error(&name, &value, e))?;
            }
            settings.push((name, value));
        }
        warnings.push(Warning::Unsupported {
            name: "security-setting-plugin".to_owned(),
            reason: "security setting plugins are not supported; use <security-setting>",
        });
        config
            .security_setting_plugins
            .push(SecuritySettingPluginConfig {
                class_name,
                settings,
            });
    }
    Ok(())
}

fn parse_address_settings(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    let Some(list) = first(core, "address-settings") else {
        return Ok(());
    };
    warn_unknown_children(warnings, list, &["address-setting"]);
    for node in all(list, "address-setting") {
        let m = node
            .attribute("match")
            .ok_or_else(|| ConfigError::MissingAttribute {
                element: "address-setting".to_owned(),
                attribute: "match".to_owned(),
            })?;
        let settings = parse_address_setting(node, warnings)?;
        if config
            .address_settings
            .iter()
            .any(|(existing, _)| existing == m)
        {
            warnings.push(Warning::DuplicateAddressSetting {
                r#match: m.to_owned(),
            });
        } else {
            config.address_settings.push((m.to_owned(), settings));
        }
    }
    Ok(())
}

/// `FileConfigurationParser.parseAddressSettings(Node)`. Child names are
/// compared ignoring case, as in Java.
fn parse_address_setting(
    node: Node<'_, '_>,
    warnings: &mut Vec<Warning>,
) -> Result<AddressSettings> {
    let mut a = AddressSettings::default();
    for child in elements(node) {
        let name = child.tag_name().name().to_ascii_lowercase();
        let check = |check: Check, v: i64| check.apply(&name, v);
        let text = || trimmed_text(child);
        match name.as_str() {
            "dead-letter-address" => a.dead_letter_address = Some(text()),
            "expiry-address" => a.expiry_address = Some(text()),
            "expiry-delay" => a.expiry_delay = Some(node_i64(child)?),
            "min-expiry-delay" => a.min_expiry_delay = Some(node_i64(child)?),
            "max-expiry-delay" => a.max_expiry_delay = Some(node_i64(child)?),
            "no-expiry" => a.no_expiry = Some(node_bool(child)),
            "redelivery-delay" => a.redelivery_delay = Some(node_i64(child)?),
            "redelivery-delay-multiplier" => a.redelivery_multiplier = Some(node_f64(child)?),
            "redelivery-collision-avoidance-factor" => {
                let v = node_f64(child)?;
                if !(0.0..=1.0).contains(&v) {
                    return Err(ConfigError::invalid(&name, v, "must be between 0 and 1"));
                }
                a.redelivery_collision_avoidance_factor = Some(v);
            }
            "max-redelivery-delay" => a.max_redelivery_delay = Some(node_i64(child)?),
            "max-size-bytes" => a.max_size_bytes = Some(node_text_bytes(child)?),
            "max-size-messages" => a.max_size_messages = Some(node_i32(child)?.into()),
            "max-size-bytes-reject-threshold" => {
                a.max_size_bytes_reject_threshold = Some(node_text_bytes(child)?)
            }
            "page-size-bytes" => {
                // `setPageSizeBytes` stores null for negatives; the check
                // already rules them out.
                a.page_size_bytes =
                    Some(check(Check::PositiveInt, node_text_bytes(child)?)? as i32);
            }
            "max-read-page-messages" => {
                a.max_read_page_messages =
                    Some(check(Check::MinusOneOrPositiveInt, node_i64(child)?)? as i32);
            }
            "max-read-page-bytes" => {
                a.max_read_page_bytes =
                    Some(check(Check::MinusOneOrPositiveInt, node_text_bytes(child)?)? as i32);
            }
            "prefetch-page-messages" => {
                // The setter treats anything <= 0 as unset.
                let v = check(Check::MinusOneOrPositiveInt, node_i64(child)?)? as i32;
                a.prefetch_page_messages = (v > 0).then_some(v);
            }
            "prefetch-page-bytes" => {
                let v = check(Check::MinusOneOrPositiveInt, node_text_bytes(child)?)? as i32;
                a.prefetch_page_bytes = (v > 0).then_some(v);
            }
            "page-max-cache-size" => a.page_cache_max_size = Some(node_i32(child)?),
            "page-limit-bytes" => {
                a.page_limit_bytes = Some(check(Check::MinusOneOrGtZero, node_text_bytes(child)?)?)
            }
            "page-limit-messages" => {
                a.page_limit_messages = Some(check(Check::MinusOneOrGtZero, node_i64(child)?)?)
            }
            "message-counter-history-day-limit" => {
                a.message_counter_history_day_limit = Some(node_i32(child)?)
            }
            "address-full-policy" => a.address_full_message_policy = Some(node_enum(child)?),
            "disk-full-policy" => a.disk_full_message_policy = Some(node_enum(child)?),
            "page-full-policy" => a.page_full_message_policy = Some(node_enum(child)?),
            "last-value-queue" | "default-last-value-queue" => {
                a.default_last_value_queue = Some(node_bool(child))
            }
            "default-last-value-key" => a.default_last_value_key = Some(text()),
            "default-non-destructive" => a.default_non_destructive = Some(node_bool(child)),
            "default-exclusive-queue" => a.default_exclusive_queue = Some(node_bool(child)),
            "default-group-rebalance" => a.default_group_rebalance = Some(node_bool(child)),
            "default-group-rebalance-pause-dispatch" => {
                a.default_group_rebalance_pause_dispatch = Some(node_bool(child));
            }
            "default-group-buckets" => a.default_group_buckets = Some(node_i32(child)?),
            "default-group-first-key" => a.default_group_first_key = Some(text()),
            "max-delivery-attempts" => a.max_delivery_attempts = Some(node_i32(child)?),
            "redistribution-delay" => a.redistribution_delay = Some(node_i64(child)?),
            "send-to-dla-on-no-route" => a.send_to_dla_on_no_route = Some(node_bool(child)),
            "slow-consumer-threshold" => {
                a.slow_consumer_threshold = Some(check(Check::MinusOneOrGtZero, node_i64(child)?)?);
            }
            "slow-consumer-threshold-measurement-unit" => {
                a.slow_consumer_threshold_measurement_unit = Some(node_enum(child)?);
            }
            "slow-consumer-check-period" => {
                a.slow_consumer_check_period = Some(check(Check::GtZero, node_i64(child)?)?);
            }
            "slow-consumer-policy" => a.slow_consumer_policy = Some(node_enum(child)?),
            "auto-create-jms-queues" => a.auto_create_jms_queues = Some(node_bool(child)),
            "auto-delete-jms-queues" => a.auto_delete_jms_queues = Some(node_bool(child)),
            "auto-create-jms-topics" => a.auto_create_jms_topics = Some(node_bool(child)),
            "auto-delete-jms-topics" => a.auto_delete_jms_topics = Some(node_bool(child)),
            "auto-create-queues" => a.auto_create_queues = Some(node_bool(child)),
            "auto-delete-queues" => a.auto_delete_queues = Some(node_bool(child)),
            "auto-delete-created-queues" => a.auto_delete_created_queues = Some(node_bool(child)),
            "auto-delete-queues-delay" => {
                a.auto_delete_queues_delay = Some(check(Check::GeZero, node_i64(child)?)?)
            }
            "auto-delete-queues-message-count" => {
                a.auto_delete_queues_message_count =
                    Some(check(Check::MinusOneOrGeZero, node_i64(child)?)?);
            }
            "auto-delete-queues-skip-usage-check" => {
                a.auto_delete_queues_skip_usage_check = Some(node_bool(child))
            }
            "config-delete-queues" => a.config_delete_queues = Some(node_enum(child)?),
            "auto-create-addresses" => a.auto_create_addresses = Some(node_bool(child)),
            "auto-delete-addresses" => a.auto_delete_addresses = Some(node_bool(child)),
            "auto-delete-addresses-delay" => {
                a.auto_delete_addresses_delay = Some(check(Check::GeZero, node_i64(child)?)?);
            }
            "auto-delete-addresses-skip-usage-check" => {
                a.auto_delete_addresses_skip_usage_check = Some(node_bool(child));
            }
            "config-delete-addresses" => a.config_delete_addresses = Some(node_enum(child)?),
            "config-delete-diverts" => a.config_delete_diverts = Some(node_enum(child)?),
            "management-browse-page-size" => a.management_browse_page_size = Some(node_i32(child)?),
            "management-message-attribute-size-limit" => {
                a.management_message_attribute_size_limit = Some(node_i32(child)?);
            }
            "default-purge-on-no-consumers" => {
                a.default_purge_on_no_consumers = Some(node_bool(child))
            }
            "default-max-consumers" => a.default_max_consumers = Some(node_i32(child)?),
            "default-consumers-before-dispatch" => {
                a.default_consumers_before_dispatch = Some(node_i32(child)?)
            }
            "default-delay-before-dispatch" => {
                a.default_delay_before_dispatch = Some(node_i64(child)?)
            }
            "default-queue-routing-type" => a.default_queue_routing_type = Some(node_enum(child)?),
            "default-address-routing-type" => {
                a.default_address_routing_type = Some(node_enum(child)?)
            }
            "default-consumer-window-size" => {
                a.default_consumer_window_size = Some(node_i32(child)?)
            }
            "default-ring-size" => a.default_ring_size = Some(node_i64(child)?),
            "retroactive-message-count" => {
                a.retroactive_message_count = Some(check(Check::GeZero, node_i64(child)?)?);
            }
            "auto-create-dead-letter-resources" => {
                a.auto_create_dead_letter_resources = Some(node_bool(child))
            }
            "dead-letter-queue-prefix" => a.dead_letter_queue_prefix = Some(text()),
            "dead-letter-queue-suffix" => a.dead_letter_queue_suffix = Some(text()),
            "auto-create-expiry-resources" => {
                a.auto_create_expiry_resources = Some(node_bool(child))
            }
            "expiry-queue-prefix" => a.expiry_queue_prefix = Some(text()),
            "expiry-queue-suffix" => a.expiry_queue_suffix = Some(text()),
            "enable-metrics" => a.enable_metrics = Some(node_bool(child)),
            "enable-ingress-timestamp" => a.enable_ingress_timestamp = Some(node_bool(child)),
            "id-cache-size" => {
                a.id_cache_size = Some(check(Check::GeZero, node_i32(child)?.into())? as i32)
            }
            "initial-queue-buffer-size" => {
                a.initial_queue_buffer_size =
                    Some(check(Check::PositivePowerOfTwo, node_i32(child)?.into())? as i32);
            }
            _ => warn_unknown(warnings, node, child),
        }
    }
    Ok(a)
}

fn parse_addresses(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    let Some(list) = first(core, "addresses") else {
        return Ok(());
    };
    warn_unknown_children(warnings, list, &["address"]);
    for node in all(list, "address") {
        config.addresses.push(parse_address(node, warnings)?);
    }
    Ok(())
}

fn parse_diverts(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    let Some(list) = first(core, "diverts") else {
        return Ok(());
    };
    warn_unknown_children(warnings, list, &["divert"]);
    for node in all(list, "divert") {
        config.diverts.push(parse_divert(node, warnings)?);
    }
    Ok(())
}

/// `parseDivertConfiguration`.
fn parse_divert(node: Node<'_, '_>, warnings: &mut Vec<Warning>) -> Result<DivertConfig> {
    warn_unknown_children(
        warnings,
        node,
        &[
            "routing-name",
            "address",
            "forwarding-address",
            "exclusive",
            "transformer-class-name",
            "routing-type",
            "filter",
            "transformer",
        ],
    );
    let required = |name: &str| -> Result<String> {
        match first(node, name).map(trimmed_text) {
            Some(v) if !v.is_empty() => Ok(v),
            v => Err(ConfigError::invalid(
                name,
                v.unwrap_or_default(),
                "must not be null or empty",
            )),
        }
    };
    let transformer = match first(node, "transformer") {
        Some(t) => first(t, "class-name").map(|c| TransformerConfig {
            class_name: trimmed_text(c),
            properties: all(t, "property")
                .map(|p| {
                    (
                        p.attribute("key").unwrap_or_default().to_owned(),
                        p.attribute("value").unwrap_or_default().to_owned(),
                    )
                })
                .collect(),
        }),
        None => first(node, "transformer-class-name").map(|c| TransformerConfig {
            class_name: trimmed_text(c),
            properties: Vec::new(),
        }),
    };
    Ok(DivertConfig {
        name: node.attribute("name").unwrap_or_default().to_owned(),
        routing_name: first(node, "routing-name").map(trimmed_text),
        address: required("address")?,
        forwarding_address: required("forwarding-address")?,
        exclusive: first(node, "exclusive").is_some_and(node_bool),
        filter: first(node, "filter").and_then(|f| f.attribute("string").map(str::to_owned)),
        routing_type: match first(node, "routing-type") {
            Some(t) => node_enum(t)?,
            None => DivertRoutingType::default(),
        },
        transformer,
    })
}

/// `parseAddressConfiguration`.
fn parse_address(node: Node<'_, '_>, warnings: &mut Vec<Warning>) -> Result<AddressConfig> {
    let name = node
        .attribute("name")
        .ok_or_else(|| ConfigError::MissingAttribute {
            element: "address".to_owned(),
            attribute: "name".to_owned(),
        })?;
    // Java rejects an address without child nodes at all; whitespace counts.
    if node.first_child().is_none() {
        return Err(ConfigError::AddressWithNoRoutingType(name.to_owned()));
    }
    let mut address = AddressConfig {
        name: name.to_owned(),
        routing_types: Vec::new(),
        queues: Vec::new(),
    };
    for child in elements(node) {
        let routing_type = match child.tag_name().name() {
            "multicast" => RoutingType::Multicast,
            "anycast" => RoutingType::Anycast,
            _ => {
                warn_unknown(warnings, node, child);
                continue;
            }
        };
        if !address.routing_types.contains(&routing_type) {
            address.routing_types.push(routing_type);
        }
        warn_unknown_children(warnings, child, &["queue"]);
        for q in all(child, "queue") {
            address
                .queues
                .push(parse_queue(q, name, routing_type, warnings)?);
        }
    }
    Ok(address)
}

/// `parseQueueConfiguration`. Attribute values are not trimmed, as in Java.
fn parse_queue(
    node: Node<'_, '_>,
    address: &str,
    routing_type: RoutingType,
    warnings: &mut Vec<Warning>,
) -> Result<QueueConfig> {
    let name = node
        .attribute("name")
        .ok_or_else(|| ConfigError::MissingAttribute {
            element: "queue".to_owned(),
            attribute: "name".to_owned(),
        })?;
    let mut q = QueueConfig::new(name, address, routing_type);
    for attr in node.attributes().filter(|a| a.namespace().is_none()) {
        let (key, v) = (attr.name(), attr.value());
        match key {
            "max-consumers" => {
                q.max_consumers = Some(
                    Check::MaxQueueConsumers.apply(name, parse_num::<i32>(key, v)?.into())? as i32,
                );
            }
            "purge-on-no-consumers" => q.purge_on_no_consumers = java_parse_bool(v),
            "exclusive" => q.exclusive = Some(java_parse_bool(v)),
            "group-rebalance" => q.group_rebalance = Some(java_parse_bool(v)),
            "group-rebalance-pause-dispatch" => {
                q.group_rebalance_pause_dispatch = Some(java_parse_bool(v))
            }
            "group-buckets" => q.group_buckets = Some(parse_num(key, v)?),
            "group-first-key" => q.group_first_key = Some(v.to_owned()),
            "last-value" => q.last_value = Some(java_parse_bool(v)),
            "last-value-key" => q.last_value_key = Some(v.to_owned()),
            "non-destructive" => q.non_destructive = Some(java_parse_bool(v)),
            "consumers-before-dispatch" => q.consumers_before_dispatch = Some(parse_num(key, v)?),
            "delay-before-dispatch" => q.delay_before_dispatch = Some(parse_num(key, v)?),
            "enabled" => q.enabled = Some(java_parse_bool(v)),
            "ring-size" => q.ring_size = parse_num(key, v)?,
            _ => {}
        }
    }
    for child in elements(node) {
        match child.tag_name().name() {
            // Overwritten by the enclosing address, as in Java.
            "address" => {}
            "filter" => q.filter = child.attribute("string").map(str::to_owned),
            "durable" => q.durable = node_bool(child),
            "user" => q.user = Some(trimmed_text(child)),
            _ => warn_unknown(warnings, node, child),
        }
    }
    Ok(q)
}

/// `parseWildcardConfiguration`.
fn parse_wildcards(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    let Some(node) = first(core, "wildcard-addresses") else {
        return Ok(());
    };
    warn_unknown_children(
        warnings,
        node,
        &[
            "enabled",
            "routing-enabled",
            "delimiter",
            "any-words",
            "single-word",
        ],
    );
    let s = Settings { core: node };
    let wc = &mut config.wildcard_addresses;
    let first_char = |name: &str| -> Result<Option<char>> {
        match s.string(name) {
            None => Ok(None),
            Some(v) => v
                .chars()
                .next()
                .map(Some)
                .ok_or_else(|| ConfigError::invalid(name, "", "must not be empty")),
        }
    };
    if let Some(c) = first_char("delimiter")? {
        wc.delimiter = c;
    }
    if let Some(c) = first_char("any-words")? {
        wc.any_words = c;
    }
    if let Some(c) = first_char("single-word")? {
        wc.single_word = c;
    }
    if let Some(v) = s.bool("enabled") {
        wc.routing_enabled = v;
    }
    if let Some(v) = s.bool("routing-enabled") {
        wc.routing_enabled = v;
    }
    Ok(())
}
