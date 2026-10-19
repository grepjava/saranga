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
    AddressConfig, BrokerConfig, BrokerPluginConfig, CriticalAnalyzerPolicy, DivertConfig,
    DivertRoutingType, HaPolicy, JournalConfig, QueueConfig, TransformerConfig, Warning,
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

/// The document validated as `validation` says: deployed, `<core>` and
/// maybe `<jms>` (the first descendants so named, as
/// `getElementsByTagName` finds them) each against its schema, the first
/// error logged as `XMLUtil.validate` logs it and failing the
/// configuration; reloaded, the whole document against
/// `artemis-server.xsd`, an error logged by `FileConfigurationParser` and
/// the configuration read all the same.
pub(crate) fn validate_document(text: &str, validation: crate::Validation) -> Result<()> {
    match schema_error(text, validation)? {
        Some(message) if validation == crate::Validation::Reload => {
            tracing::error!(
                target: "org.apache.activemq.artemis.core.deployers.impl.FileConfigurationParser",
                "{message}"
            );
            Ok(())
        }
        Some(message) => {
            tracing::error!(
                "AMQ214019: Invalid configuration\norg.xml.sax.SAXParseException; {message}"
            );
            Err(ConfigError::Invalid(message))
        }
        None => Ok(()),
    }
}

/// The message of the first error validating the document as `validation`
/// says, if there is one.
pub(crate) fn schema_error(text: &str, validation: crate::Validation) -> Result<Option<String>> {
    use crate::schema::{Xsd, validate};
    let options = ParsingOptions {
        allow_dtd: true,
        ..ParsingOptions::default()
    };
    let doc = Document::parse_with_options(text, options)?;
    let root = doc.root_element();
    match validation {
        crate::Validation::Deploy { jms } => {
            let deployables = [("core", Xsd::Configuration, true), ("jms", Xsd::Jms, jms)];
            for (name, xsd, deployed) in deployables {
                // getElementsByTagName matches the qualified name: a
                // prefixed <c:core> is not the deployable's.
                let node = root
                    .descendants()
                    .skip(1)
                    .find(|d| d.is_element() && qualified_name(text, *d) == name);
                let Some(node) = node.filter(|_| deployed) else {
                    continue;
                };
                if let Err(f) = validate(xsd, text, node) {
                    return Ok(Some(f.message()));
                }
            }
            Ok(None)
        }
        crate::Validation::Reload => {
            Ok(validate(Xsd::Server, text, root).err().map(|f| f.message()))
        }
    }
}

/// An element's name as written, its prefix included.
fn qualified_name<'t>(text: &'t str, n: Node<'_, '_>) -> &'t str {
    let rest = &text[n.range().start + 1..];
    let end = rest
        .find(|c: char| c.is_whitespace() || c == '/' || c == '>')
        .unwrap_or(rest.len());
    &rest[..end]
}

pub(crate) fn parse_document(text: &str) -> Result<BrokerConfig> {
    let options = ParsingOptions {
        allow_dtd: true,
        ..ParsingOptions::default()
    };
    let doc = Document::parse_with_options(text, options)?;
    let mut config = BrokerConfig::default();
    let mut warnings = Vec::new();

    // The root itself is never taken as `<core>` (Artemis searches only its
    // descendants), so a root `<core>` yields defaults and a warning.
    let root = doc.root_element();
    let root_children = elements(root).filter(|_| root.tag_name().name() != "core");
    for child in root_children {
        match child.tag_name().name() {
            "core" | "jms" => {}
            _ => warn_unknown(&mut warnings, root, child),
        }
    }

    match first(root, "core") {
        Some(core) => parse_core(core, &mut config, &mut warnings)?,
        None => warnings.push(Warning::NoCoreElement),
    }
    // `FileBroker` deploys `LegacyJMSConfiguration` after the core: the
    // first <jms> in the document.
    if let Some(jms) = root
        .descendants()
        .find(|n| n.is_element() && n.tag_name().name() == "jms")
    {
        parse_jms(jms, &mut config, &mut warnings);
    }
    config.warnings = warnings;
    Ok(config)
}

/// Element children, skipping text, comments and any `xi:` element left
/// (`xi:include`s are expanded before the document is parsed).
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
    parse_legacy_queues(core, config, warnings)?;

    check_unsupported(core, warnings)?;
    parse_ha_policy(core, config, warnings)?;
    parse_scalars(core, config, warnings)?;
    parse_acceptors(core, config, warnings)?;
    parse_connectors(core, config, warnings)?;
    parse_broadcast_groups(core, config)?;
    parse_discovery_groups(core, config, warnings)?;
    parse_cluster_connections(core, config, warnings)?;
    parse_grouping_handler(core, config)?;
    parse_security_settings(core, config, warnings)?;
    parse_broker_plugins(core, config)?;
    parse_address_settings(core, config, warnings)?;
    parse_addresses(core, config, warnings)?;
    parse_bridges(core, config)?;
    parse_federations(core, config)?;
    parse_broker_connections(core, config)?;
    parse_diverts(core, config, warnings)?;
    parse_wildcards(core, config, warnings)?;
    Ok(())
}

/// `<core>` children Saranga reads but does not act on, and why.
const NOT_ACTED_ON: &[(&str, &str)] = &[
    ("connection-routers", "connection routers are not supported"),
    ("lock-coordinators", "lock coordinators are not supported"),
    (
        "metrics",
        "its settings are not applied; the web server's metrics app exports the broker, address and queue metrics",
    ),
    (
        "metrics-plugin",
        "its settings are not applied; the web server's metrics app exports the broker, address and queue metrics",
    ),
    (
        "remoting-interceptors",
        "interceptors are Java classes, which Saranga cannot load",
    ),
    (
        "remoting-incoming-interceptors",
        "interceptors are Java classes, which Saranga cannot load",
    ),
    (
        "remoting-outgoing-interceptors",
        "interceptors are Java classes, which Saranga cannot load",
    ),
    (
        "connector-services",
        "connector services are Java classes, which Saranga cannot load",
    ),
    (
        "resource-limit-settings",
        "resource limits are not enforced",
    ),
];

/// `parseHAPolicyConfiguration`: the first of `live-only`, `primary-only`,
/// `shared-store` and `replication` found decides, and within it
/// `colocated`, then `master`, `slave`, `primary` and `backup`. Colocated
/// backups are refused.
fn parse_ha_policy(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    let Some(ha) = first(core, "ha-policy") else {
        return Ok(());
    };
    let bool_or =
        |n: Node<'_, '_>, name: &str, default: bool| first(n, name).map_or(default, node_bool);
    let refuse = |reason: &'static str| ConfigError::Unsupported {
        element: "ha-policy".to_owned(),
        reason,
    };
    let deprecated = |warnings: &mut Vec<Warning>, name: &str, alternative: &'static str| {
        warnings.push(Warning::Deprecated {
            name: name.to_owned(),
            alternative,
        });
    };
    for kind in ["live-only", "primary-only", "shared-store", "replication"] {
        if first(ha, kind).is_none() {
            continue;
        }
        match kind {
            "shared-store" => {
                if first(ha, "colocated").is_some() {
                    return Err(refuse("a colocated backup is not supported"));
                }
                let primary = |n: Node<'_, '_>| HaPolicy::SharedStorePrimary {
                    failover_on_shutdown: bool_or(
                        n,
                        "failover-on-shutdown",
                        HaPolicy::DEFAULT_FAILOVER_ON_SHUTDOWN,
                    ),
                    wait_for_activation: bool_or(
                        n,
                        "wait-for-activation",
                        HaPolicy::DEFAULT_WAIT_FOR_ACTIVATION,
                    ),
                };
                let backup = |n: Node<'_, '_>, warnings: &mut Vec<Warning>| {
                    if first(n, "scale-down").is_some() {
                        warnings.push(Warning::Unsupported {
                            name: "scale-down".to_owned(),
                            reason: "a backup does not scale down; it becomes active on the primary's data",
                        });
                    }
                    HaPolicy::SharedStoreBackup {
                        allow_failback: bool_or(
                            n,
                            "allow-failback",
                            HaPolicy::DEFAULT_ALLOW_FAILBACK,
                        ),
                        failover_on_shutdown: bool_or(
                            n,
                            "failover-on-shutdown",
                            HaPolicy::DEFAULT_FAILOVER_ON_SHUTDOWN,
                        ),
                        restart_backup: bool_or(
                            n,
                            "restart-backup",
                            HaPolicy::DEFAULT_RESTART_BACKUP,
                        ),
                    }
                };
                if let Some(n) = first(ha, "master") {
                    deprecated(warnings, "master", "primary");
                    config.ha_policy = primary(n);
                } else if let Some(n) = first(ha, "slave") {
                    deprecated(warnings, "slave", "backup");
                    config.ha_policy = backup(n, warnings);
                } else if let Some(n) = first(ha, "primary") {
                    config.ha_policy = primary(n);
                } else if let Some(n) = first(ha, "backup") {
                    config.ha_policy = backup(n, warnings);
                }
            }
            "replication" => {
                if first(ha, "colocated").is_some() {
                    return Err(refuse("a colocated backup is not supported"));
                }
                let (node, backup) = if let Some(n) = first(ha, "master") {
                    deprecated(warnings, "master", "primary");
                    (n, false)
                } else if let Some(n) = first(ha, "slave") {
                    deprecated(warnings, "slave", "backup");
                    (n, true)
                } else if let Some(n) = first(ha, "primary") {
                    (n, false)
                } else if let Some(n) = first(ha, "backup") {
                    (n, true)
                } else {
                    return Ok(());
                };
                // Only `primary` and `backup` take a manager, as Artemis
                // reads them.
                let pluggable = matches!(node.tag_name().name(), "primary" | "backup");
                let mut replication = parse_replication(node, backup, warnings)?;
                if !pluggable && replication.manager.is_some() {
                    replication = parse_replication_classic(node, backup, warnings)?;
                }
                config.ha_policy = if backup {
                    HaPolicy::ReplicationBackup(replication)
                } else {
                    HaPolicy::ReplicationPrimary(replication)
                };
            }
            _ => {
                if first(ha, "scale-down").is_some() {
                    warnings.push(Warning::Unsupported {
                        name: "scale-down".to_owned(),
                        reason: "a primary does not scale down to another broker when it stops",
                    });
                }
            }
        }
        return Ok(());
    }
    Ok(())
}

/// `createReplicationPrimaryHaPolicy` and `createReplicationBackupHaPolicy`.
fn parse_replication(
    n: Node<'_, '_>,
    backup: bool,
    warnings: &mut Vec<Warning>,
) -> Result<crate::broker::Replication> {
    parse_replication_with(n, backup, false, warnings)
}

/// `master` and `slave`, whose `manager` Artemis ignores.
fn parse_replication_classic(
    n: Node<'_, '_>,
    backup: bool,
    warnings: &mut Vec<Warning>,
) -> Result<crate::broker::Replication> {
    parse_replication_with(n, backup, true, warnings)
}

fn parse_replication_with(
    n: Node<'_, '_>,
    backup: bool,
    classic: bool,
    warnings: &mut Vec<Warning>,
) -> Result<crate::broker::Replication> {
    let mut r = crate::broker::Replication::default();
    let int = |name: &str, default: i32| first(n, name).map_or(Ok(default), node_i32);
    let long = |name: &str, default: i64| first(n, name).map_or(Ok(default), node_i64);
    let bool_or = |name: &str, default: bool| first(n, name).map_or(default, node_bool);
    let string = |name: &str| first(n, name).map(trimmed_text);
    r.group_name = string("group-name");
    r.cluster_name = string("cluster-name");
    r.max_saved_replicated_journals_size = int(
        "max-saved-replicated-journals-size",
        r.max_saved_replicated_journals_size,
    )?;
    r.initial_replication_sync_timeout = long(
        "initial-replication-sync-timeout",
        r.initial_replication_sync_timeout,
    )?;
    r.vote_on_replication_failure =
        bool_or("vote-on-replication-failure", r.vote_on_replication_failure);
    r.quorum_size = int("quorum-size", r.quorum_size)?;
    r.vote_retries = int("vote-retries", r.vote_retries)?;
    r.vote_retry_wait = long("vote-retry-wait", r.vote_retry_wait)?;
    r.quorum_vote_wait = int("quorum-vote-wait", r.quorum_vote_wait)?;
    // Artemis defaults it to the vote retry wait's default.
    r.retry_replication_wait = long("retry-replication-wait", r.vote_retry_wait)?;
    if let Some(m) = first(n, "manager").filter(|_| !classic) {
        // `createPluggableReplication*HaPolicy`: its own defaults, and no
        // votes.
        r.manager = Some(crate::broker::LockManagerConfig {
            class_name: first(m, "class-name")
                .map_or_else(|| DEFAULT_LOCK_MANAGER.to_owned(), trimmed_text),
            properties: first(m, "properties")
                .map(|p| {
                    elements(p)
                        .filter(|e| e.tag_name().name() == "property")
                        .filter_map(|e| {
                            Some((
                                e.attribute("key")?.to_owned(),
                                e.attribute("value")?.to_owned(),
                            ))
                        })
                        .collect()
                })
                .unwrap_or_default(),
        });
        r.retry_replication_wait = long("retry-replication-wait", 2_000)?;
        if backup {
            r.allow_failback = bool_or("allow-failback", false);
        } else {
            // A primary never hands back (`ReplicationPrimaryPolicy.with`).
            r.allow_failback = false;
            r.coordination_id = string("coordination-id").map(|c| coordination_id(&c));
        }
        return Ok(r);
    }
    if backup {
        r.allow_failback = bool_or("allow-failback", r.allow_failback);
        r.restart_backup = bool_or("restart-backup", r.restart_backup);
        if first(n, "scale-down").is_some() {
            warnings.push(Warning::Unsupported {
                name: "scale-down".to_owned(),
                reason: "a backup does not scale down; it becomes active on its copy of the primary's data",
            });
        }
    } else {
        // `ReplicatedPolicy.allowAutoFailBack`, which nothing configures.
        r.allow_failback = true;
        if let Some(c) = first(n, "check-for-live-server") {
            warnings.push(Warning::Deprecated {
                name: "check-for-live-server".to_owned(),
                alternative: "check-for-active-server",
            });
            r.check_for_active_server = node_bool(c);
        } else {
            r.check_for_active_server = bool_or("check-for-active-server", false);
        }
    }
    Ok(r)
}

/// `ActiveMQDefaultConfiguration.getDefaultDistributedLockManagerClassName`.
const DEFAULT_LOCK_MANAGER: &str =
    "org.apache.activemq.artemis.lockmanager.zookeeper.CuratorDistributedLockManager";

/// `ReplicationPrimaryPolicyConfiguration.setCoordinationId`: at most 16
/// characters, an odd length padded with `+`, dashes made dots.
fn coordination_id(id: &str) -> String {
    let n = id.chars().count();
    let id = if n >= 16 {
        id.chars().take(16).collect()
    } else if !n.is_multiple_of(2) {
        format!("{id}+")
    } else {
        id.to_owned()
    };
    id.replace('-', ".")
}

/// Refuses what would make the broker something Saranga cannot be (a JDBC
/// store) and warns about features it does not act on.
fn check_unsupported(core: Node<'_, '_>, warnings: &mut Vec<Warning>) -> Result<()> {
    if let Some(store) = first(core, "store")
        && first(store, "database-store").is_some()
    {
        return Err(ConfigError::Unsupported {
            element: "database-store".to_owned(),
            reason: "JDBC persistence is not supported; use the file journal",
        });
    }
    for (name, reason) in NOT_ACTED_ON {
        if elements(core).any(|c| c.tag_name().name() == *name) {
            warnings.push(Warning::Unsupported {
                name: (*name).to_owned(),
                reason,
            });
        }
    }
    Ok(())
}

/// The single-valued settings of `<core>`. Only the elements present are
/// applied, so broker properties can run it over one element at a time.
fn parse_scalars(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    let s = Settings { core };
    if let Some(name) = s.string("name") {
        config.name = name;
    }
    if let Some(domain) = s.string("jmx-domain") {
        config.jmx_domain = domain;
    }
    if let Some(prefix) = s.string("system-property-prefix") {
        config.system_property_prefix = prefix;
    }
    if let Some(v) = s.bool("persistence-enabled") {
        config.persistence_enabled = v;
    }
    if let Some(v) = s.bool("security-enabled") {
        config.security_enabled = v;
    }
    if let Some(v) = s.i32("global-max-size-percent-of-jvm-max-memory", Check::GtZero)? {
        config.global_max_size_percent_of_jvm_max_memory = v;
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
    if let Some(v) = s.non_empty_string("management-address")? {
        config.management_address = v;
    }
    if let Some(v) = s.non_empty_string("management-notification-address")? {
        config.management_notification_address = v;
    }
    if let Some(v) = s.bool("suppress-session-notifications") {
        config.suppress_session_notifications = v;
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
    if let Some(v) = s.bool("graceful-shutdown-enabled") {
        config.graceful_shutdown_enabled = v;
    }
    if let Some(v) = s.i64("graceful-shutdown-timeout", Check::MinusOneOrGeZero)? {
        config.graceful_shutdown_timeout = v;
    }
    set(
        &mut config.network_check_list,
        s.string("network-check-list"),
    );
    set(
        &mut config.network_check_url_list,
        s.string("network-check-URL-list"),
    );
    if let Some(v) = s.i64("network-check-period", Check::GtZero)? {
        config.network_check_period = v;
    }
    if let Some(v) = s.i32("network-check-timeout", Check::GtZero)? {
        config.network_check_timeout = v;
    }
    set(&mut config.network_check_nic, s.string("network-check-NIC"));
    set(
        &mut config.network_check_ping_command,
        s.string("network-check-ping-command"),
    );
    set(
        &mut config.network_check_ping6_command,
        s.string("network-check-ping6-command"),
    );
    if let Some(v) = s.bool("critical-analyzer") {
        config.critical_analyzer = v;
    }
    if let Some(v) = s.i64("critical-analyzer-timeout", Check::GeZero)? {
        config.critical_analyzer_timeout = v;
    }
    if let Some(v) = s.i64("critical-analyzer-check-period", Check::GeZero)? {
        config.critical_analyzer_check_period = v;
    }
    if let Some(v) = s.string("critical-analyzer-policy") {
        // CriticalAnalyzerPolicy.valueOf
        config.critical_analyzer_policy = CriticalAnalyzerPolicy::parse(&v).ok_or_else(|| {
            ConfigError::invalid(
                "critical-analyzer-policy",
                &v,
                "must be one of HALT, SHUTDOWN or LOG".to_owned(),
            )
        })?;
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
    if let Some(v) = s.string("view-permission-method-match-pattern") {
        // Pattern.compile at startup.
        let name = "view-permission-method-match-pattern";
        regex::Regex::new(&v).map_err(|e| ConfigError::invalid(name, &v, e.to_string()))?;
        config.view_permission_method_match_pattern = v;
    }
    if let Some(v) = s.bool("management-message-rbac") {
        config.management_message_rbac = v;
    }
    if let Some(v) = s.string("management-rbac-prefix") {
        config.management_rbac_prefix = v;
    }
    if let Some(v) = s.bool("large-message-sync") {
        config.large_message_sync = v;
    }
    if let Some(v) = s.string("internal-naming-prefix") {
        config.internal_naming_prefix = v;
    }
    if let Some(v) = s.i64("mqtt-session-scan-interval", Check::GtZero)? {
        config.mqtt_session_scan_interval = v;
    }
    if let Some(v) = s.i64("mqtt-session-state-persistence-timeout", Check::GtZero)? {
        config.warnings.push(Warning::Deprecated {
            name: "mqtt-session-state-persistence-timeout".into(),
            alternative: "mqtt-subscription-persistence-enabled",
        });
        config.mqtt_session_state_persistence_timeout = v;
    }
    if let Some(v) = s.bool("mqtt-subscription-persistence-enabled") {
        config.mqtt_subscription_persistence_enabled = v;
    }
    if let Some(v) = s.bool("amqp-use-core-subscription-naming") {
        config.amqp_use_core_subscription_naming = v;
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
    parse_journal(&s, &mut config.journal)?;
    if let Some(v) = s.string("node-manager-lock-directory") {
        config.node_manager_lock_directory = Some(v);
    }
    if let Some(v) = s.i64("journal-lock-acquisition-timeout", Check::MinusOneOrGtZero)? {
        config.journal_lock_acquisition_timeout = v;
    }
    if let Some(v) = s.i64("journal-lock-monitor-timeout", Check::GtZero)? {
        config.journal_lock_monitor_timeout = v;
    }
    if let Some(v) = s.i32("journal-lock-monitor-max-retries", Check::GeZero)? {
        config.journal_lock_monitor_max_retries = v;
    }

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
    Ok(())
}

/// Sets the journal settings present in `<core>`, leaving the rest.
fn parse_journal(s: &Settings<'_, '_>, j: &mut JournalConfig) -> Result<()> {
    if let Some(t) = first(s.core, "journal-type") {
        j.journal_type = node_enum(t)?;
    }
    set(&mut j.datasync, s.bool("journal-datasync"));
    set(
        &mut j.sync_transactional,
        s.bool("journal-sync-transactional"),
    );
    set(
        &mut j.sync_non_transactional,
        s.bool("journal-sync-non-transactional"),
    );
    set(
        &mut j.file_size,
        s.text_bytes("journal-file-size", Check::PositiveInt)?
            .map(|v| v as i32),
    );
    set(
        &mut j.max_attic_files,
        s.i32("journal-max-attic-files", Check::Any)?,
    );
    set(
        &mut j.buffer_timeout,
        s.i32("journal-buffer-timeout", Check::GeZero)?,
    );
    set(
        &mut j.buffer_size,
        s.text_bytes("journal-buffer-size", Check::PositiveInt)?
            .map(|v| v as i32),
    );
    set(&mut j.max_io, s.i32("journal-max-io", Check::GtZero)?);
    set(
        &mut j.device_block_size,
        s.i32("journal-device-block-size", Check::MinusOneOrGeZero)?,
    );
    set(
        &mut j.file_open_timeout,
        s.i32("journal-file-open-timeout", Check::GtZero)?,
    );
    set(&mut j.min_files, s.i32("journal-min-files", Check::GtZero)?);
    set(
        &mut j.pool_files,
        s.i32("journal-pool-files", Check::MinusOneOrGtZero)?,
    );
    set(
        &mut j.compact_min_files,
        s.i32("journal-compact-min-files", Check::GeZero)?,
    );
    set(
        &mut j.compact_percentage,
        s.i32("journal-compact-percentage", Check::Percentage)?,
    );
    set(&mut j.create_journal_dir, s.bool("create-journal-dir"));
    set(&mut j.create_bindings_dir, s.bool("create-bindings-dir"));
    set(
        &mut j.max_redelivery_records,
        s.i32("max-redelivery-records", Check::MinusOneOrGeZero)?,
    );
    set(
        &mut j.page_sync_timeout,
        s.i32("page-sync-timeout", Check::GeZero)?,
    );
    set(&mut j.purge_page_folders, s.bool("purge-page-folders"));
    set(
        &mut j.max_disk_usage,
        s.i32("max-disk-usage", Check::PercentageOrMinusOne)?,
    );
    set(
        &mut j.min_disk_free,
        s.text_bytes("min-disk-free", Check::MinusOneOrGtZero)?,
    );
    set(
        &mut j.disk_scan_period,
        s.i32("disk-scan-period", Check::MinusOneOrGtZero)?,
    );
    if let Some(n) = first(s.core, "journal-retention-directory") {
        j.retention = Some(parse_journal_retention(n)?);
    }
    Ok(())
}

/// `parseJournalRetention`.
fn parse_journal_retention(n: Node<'_, '_>) -> Result<crate::broker::JournalRetention> {
    const NAME: &str = "journal-retention-directory";
    let directory = trimmed_text(n);
    let max_bytes = match n.attribute("storage-limit") {
        None => -1,
        Some(t) => convert_text_bytes(t.trim())
            .map_err(|_| ConfigError::invalid("storage-limit", t, "not a byte size"))?,
    };
    let period: i64 = match n.attribute("period") {
        None => -1,
        Some(t) => {
            let v: i32 = parse_num("period", java_trim(t))?;
            if v <= 0 {
                return Err(ConfigError::invalid("period", v, "must be greater than 0"));
            }
            i64::from(v)
        }
    };
    let unit_millis: i64 = match n
        .attribute("unit")
        .unwrap_or("DAYS")
        .to_uppercase()
        .as_str()
    {
        "NANOSECONDS" | "MICROSECONDS" => 0,
        "MILLISECONDS" => 1,
        "SECONDS" => 1000,
        "MINUTES" => 60_000,
        "HOURS" => 3_600_000,
        "DAYS" => 86_400_000,
        other => {
            return Err(ConfigError::invalid(
                "unit",
                other,
                "not a java.util.concurrent.TimeUnit",
            ));
        }
    };
    if directory.is_empty() {
        return Err(ConfigError::invalid(
            NAME,
            "null",
            "journal-retention-directory=null",
        ));
    }
    if max_bytes == -1 && period == -1 {
        return Err(ConfigError::invalid(
            NAME,
            &directory,
            "configure either storage-limit or period on journal-retention-directory",
        ));
    }
    // `setJournalRetentionPeriod(unit, period)`; a sub-millisecond unit
    // rounds down, as `TimeUnit.toMillis` does.
    let period_millis = if period <= 0 {
        -1
    } else {
        match n
            .attribute("unit")
            .unwrap_or("DAYS")
            .to_uppercase()
            .as_str()
        {
            "NANOSECONDS" => period / 1_000_000,
            "MICROSECONDS" => period / 1000,
            _ => period * unit_millis,
        }
    };
    Ok(crate::broker::JournalRetention {
        directory,
        max_bytes,
        period_millis,
    })
}

/// The `<core>` elements [`parse_scalars`] reads, which broker properties
/// can set by their camel-case names.
const SCALAR_ELEMENTS: &[&str] = &[
    "address-queue-scan-period",
    "authentication-cache-size",
    "authorization-cache-size",
    "bindings-directory",
    "cluster-password",
    "cluster-user",
    "configuration-file-refresh-period",
    "create-bindings-dir",
    "create-journal-dir",
    "disk-scan-period",
    "global-max-messages",
    "global-max-size",
    "global-max-size-percent-of-jvm-max-memory",
    "id-cache-size",
    "journal-buffer-size",
    "journal-buffer-timeout",
    "journal-compact-min-files",
    "journal-compact-percentage",
    "journal-datasync",
    "journal-device-block-size",
    "journal-directory",
    "journal-file-open-timeout",
    "journal-file-size",
    "journal-max-attic-files",
    "journal-max-io",
    "journal-min-files",
    "journal-pool-files",
    "journal-sync-non-transactional",
    "journal-sync-transactional",
    "journal-type",
    "large-message-sync",
    "large-messages-directory",
    "literal-match-markers",
    "mask-password",
    "max-disk-usage",
    "max-redelivery-records",
    "message-expiry-scan-period",
    "name",
    "network-check-list",
    "network-check-period",
    "network-check-ping-command",
    "network-check-ping6-command",
    "network-check-timeout",
    "page-sync-timeout",
    "paging-directory",
    "password-codec",
    "persist-id-cache",
    "persistence-enabled",
    "populate-validated-user",
    "purge-page-folders",
    "reject-empty-validated-user",
    "security-enabled",
    "security-invalidation-interval",
    "system-property-prefix",
    "transaction-timeout",
    "transaction-timeout-scan-period",
];

fn xml_escape(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Applies one top-level setting given by its element name, as broker
/// properties do. `Ok(false)` if `<core>` has no such element; one it has
/// but Saranga does not act on is accepted and ignored, as in `broker.xml`.
pub(crate) fn apply_scalar(config: &mut BrokerConfig, element: &str, value: &str) -> Result<bool> {
    if !SCALAR_ELEMENTS.contains(&element) {
        return Ok(CORE_ELEMENTS.contains(&element));
    }
    let text = format!("<core><{element}>{}</{element}></core>", xml_escape(value));
    let doc = Document::parse(&text)?;
    parse_scalars(doc.root_element(), config, &mut Vec::new())?;
    Ok(true)
}

/// An address setting holding only `element`, or `None` if
/// `<address-setting>` has no such element.
pub(crate) fn address_setting_fragment(
    element: &str,
    value: &str,
) -> Result<Option<AddressSettings>> {
    let text = format!(
        "<address-setting><{element}>{}</{element}></address-setting>",
        xml_escape(value)
    );
    let doc = Document::parse(&text)?;
    let mut warnings = Vec::new();
    let settings = parse_address_setting(doc.root_element(), &mut warnings)?;
    Ok(warnings.is_empty().then_some(settings))
}

fn set<T>(slot: &mut Option<T>, value: Option<T>) {
    if value.is_some() {
        *slot = value;
    }
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

/// `<connectors>`: `ConfigurationUtils.parseConnectorURI` for each.
fn parse_connectors(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    let Some(list) = elements(core).find(|c| c.tag_name().name() == "connectors") else {
        return Ok(());
    };
    warn_unknown_children(warnings, list, &["connector"]);
    for node in elements(list).filter(|c| c.tag_name().name() == "connector") {
        let name = node.attribute("name");
        let uri = node
            .first_child()
            .filter(|c| c.is_text())
            .and_then(|c| c.text())
            .ok_or_else(|| ConfigError::InvalidUri {
                uri: String::new(),
                reason: format!("connector {:?} has no URI", name.unwrap_or("")),
            })?;
        let mut connector = AcceptorConfig::parse(name, uri)?;
        // `parseConnectorTransportConfiguration`: the connector unmasks its
        // passwords as the broker does.
        if let Some(mask) = config.mask_password {
            set_param(
                &mut connector,
                "activemq.usemaskedpassword",
                mask.to_string(),
            );
            if mask && config.password_codec != crate::mask::DEFAULT_CODEC {
                set_param(
                    &mut connector,
                    "activemq.passwordcodec",
                    config.password_codec.clone(),
                );
            }
        }
        config.connectors.push(connector);
    }
    Ok(())
}

fn set_param(uri: &mut AcceptorConfig, key: &str, value: String) {
    match uri.params.iter_mut().find(|(k, _)| k == key) {
        Some(slot) => slot.1 = Some(value),
        None => uri.params.push((key.to_owned(), Some(value))),
    }
}

/// `parseClusterConnectionConfiguration` for each `<cluster-connection>`.
/// The UDP endpoint of a broadcast or discovery group. A JGroups one
/// (`jgroups-file`) is refused: Saranga only has UDP.
fn parse_udp_endpoint(group: Node<'_, '_>, element: &str) -> Result<crate::UdpEndpointConfig> {
    let s = Settings { core: group };
    if s.string("jgroups-file").is_some() {
        return Err(ConfigError::Unsupported {
            element: element.to_owned(),
            reason: "JGroups (jgroups-file) is not supported; broadcast over UDP with group-address and group-port",
        });
    }
    Ok(crate::UdpEndpointConfig {
        group_address: s.string("group-address"),
        group_port: s.i32("group-port", Check::MinusOneOrGtZero)?.unwrap_or(-1),
        local_bind_address: s.string("local-bind-address"),
        local_bind_port: s
            .i32("local-bind-port", Check::MinusOneOrGtZero)?
            .unwrap_or(-1),
    })
}

/// `parseBroadcastGroupConfiguration`.
fn parse_broadcast_groups(core: Node<'_, '_>, config: &mut BrokerConfig) -> Result<()> {
    let Some(list) = first(core, "broadcast-groups") else {
        return Ok(());
    };
    for group in elements(list).filter(|c| c.tag_name().name() == "broadcast-group") {
        let s = Settings { core: group };
        // `getString(e, "connector-ref", ...)` once per connector-ref: the
        // first one each time.
        let mut connectors = Vec::new();
        for _ in elements(group).filter(|c| c.tag_name().name() == "connector-ref") {
            let name = s
                .non_empty_string("connector-ref")?
                .ok_or_else(|| ConfigError::invalid("connector-ref", "", "must not be empty"))?;
            connectors.push(name);
        }
        config.broadcast_groups.push(crate::BroadcastGroupConfig {
            name: group.attribute("name").unwrap_or_default().to_owned(),
            broadcast_period: s.i64("broadcast-period", Check::GtZero)?.unwrap_or(2000),
            connectors,
            endpoint: parse_udp_endpoint(group, "broadcast-group")?,
        });
    }
    Ok(())
}

/// `parseDiscoveryGroupConfiguration`.
fn parse_discovery_groups(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    let Some(list) = first(core, "discovery-groups") else {
        return Ok(());
    };
    for group in elements(list).filter(|c| c.tag_name().name() == "discovery-group") {
        let s = Settings { core: group };
        let name = group.attribute("name").unwrap_or_default().to_owned();
        let parsed = crate::DiscoveryGroupConfig {
            initial_wait_timeout: s
                .i64("initial-wait-timeout", Check::GtZero)?
                .unwrap_or(10_000),
            refresh_timeout: s.i64("refresh-timeout", Check::GtZero)?.unwrap_or(10_000),
            endpoint: parse_udp_endpoint(group, "discovery-group")?,
            name,
        };
        if config
            .discovery_groups
            .iter()
            .any(|g| g.name == parsed.name)
        {
            warnings.push(Warning::DiscoveryGroupAlreadyDeployed { name: parsed.name });
        } else {
            config.discovery_groups.push(parsed);
        }
    }
    Ok(())
}

fn parse_cluster_connections(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    let Some(list) = elements(core).find(|c| c.tag_name().name() == "cluster-connections") else {
        return Ok(());
    };
    for node in elements(list).filter(|c| c.tag_name().name() == "cluster-connection") {
        let name = node.attribute("name").unwrap_or_default();
        let s = Settings { core: node };
        let connector = s
            .non_empty_string("connector-ref")?
            .ok_or_else(|| ConfigError::invalid("connector-ref", "", "must not be empty"))?;
        if !config
            .connectors
            .iter()
            .any(|c| c.name.as_deref() == Some(connector.as_str()))
        {
            warnings.push(Warning::ConnectorRefNotFound {
                connector,
                cluster_connection: name.to_owned(),
            });
            continue;
        }
        let mut c = crate::ClusterConnectionConfig::new(name, &connector);
        if let Some(v) = s.string("address") {
            c.address = v;
        }
        if let Some(v) = s.bool("use-duplicate-detection") {
            c.use_duplicate_detection = v;
        }
        if let Some(forward) = s.bool("forward-when-no-consumers") {
            c.message_load_balancing = if forward {
                crate::MessageLoadBalancing::Strict
            } else {
                crate::MessageLoadBalancing::OnDemand
            };
        } else if let Some(n) = first(node, "message-load-balancing") {
            c.message_load_balancing = node_enum(n)?;
        }
        set_value(&mut c.max_hops, s.i32("max-hops", Check::GeZero)?);
        set_value(&mut c.check_period, s.i64("check-period", Check::GtZero)?);
        set_value(
            &mut c.connection_ttl,
            s.i64("connection-ttl", Check::GtZero)?,
        );
        set_value(
            &mut c.retry_interval,
            s.i64("retry-interval", Check::GtZero)?,
        );
        set_value(&mut c.call_timeout, s.i64("call-timeout", Check::GtZero)?);
        set_value(
            &mut c.call_failover_timeout,
            s.i64("call-failover-timeout", Check::MinusOneOrGtZero)?,
        );
        if let Some(n) = first(node, "retry-interval-multiplier") {
            let v = node_f64(n)?;
            if v <= 0.0 {
                return Err(ConfigError::invalid(
                    "retry-interval-multiplier",
                    v,
                    "must be greater than 0",
                ));
            }
            c.retry_interval_multiplier = v;
        }
        if let Some(v) = s.text_bytes("min-large-message-size", Check::PositiveInt)? {
            c.min_large_message_size = v as i32;
        }
        set_value(
            &mut c.max_retry_interval,
            s.i64("max-retry-interval", Check::GtZero)?,
        );
        set_value(
            &mut c.initial_connect_attempts,
            s.i32("initial-connect-attempts", Check::MinusOneOrGeZero)?,
        );
        set_value(
            &mut c.reconnect_attempts,
            s.i32("reconnect-attempts", Check::MinusOneOrGeZero)?,
        );
        if let Some(v) = s.text_bytes("confirmation-window-size", Check::PositiveInt)? {
            c.confirmation_window_size = v as i32;
        }
        if let Some(v) = s.text_bytes("producer-window-size", Check::MinusOneOrPositiveInt)? {
            c.producer_window_size = v as i32;
        }
        set_value(
            &mut c.notification_interval,
            s.i64("notification-interval", Check::GtZero)?,
        );
        set_value(
            &mut c.notification_attempts,
            s.i32("notification-attempts", Check::GtZero)?,
        );
        set_value(
            &mut c.topology_scanner_attempts,
            s.i32("topology-scanner-attempts", Check::MinusOneOrGeZero)?,
        );
        let mut discovery = None;
        for child in elements(node) {
            match child.tag_name().name() {
                "discovery-group-ref" => {
                    discovery = child.attribute("discovery-group-name").map(str::to_owned);
                }
                "static-connectors" => {
                    if child
                        .attribute("allow-direct-connections-only")
                        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
                    {
                        c.allow_direct_connections_only = true;
                    }
                    // `getStaticConnectors`.
                    for r in elements(child).filter(|r| r.tag_name().name() == "connector-ref") {
                        c.static_connectors.push(trimmed_text(r));
                    }
                }
                _ => {}
            }
        }
        c.client_id = s.string("client-id");
        if discovery.is_some() {
            c.static_connectors.clear();
            c.discovery_group = discovery;
        }
        config.cluster_connections.push(c);
    }
    // `parseClusterConnectionConfigurationURI`, after every
    // `<cluster-connection>`.
    for node in elements(list).filter(|c| c.tag_name().name() == "cluster-connection-uri") {
        let name = node.attribute("name").unwrap_or_default();
        let uri = node.attribute("address").unwrap_or_default();
        config
            .cluster_connections
            .push(crate::cluster_uri::from_uri(name, uri)?);
    }
    Ok(())
}

/// `parseGroupingHandlerConfiguration`, for each `<grouping-handler>`:
/// the last one stays.
fn parse_grouping_handler(core: Node<'_, '_>, config: &mut BrokerConfig) -> Result<()> {
    for node in all(core, "grouping-handler") {
        let s = Settings { core: node };
        let kind = s
            .non_empty_string("type")?
            .ok_or_else(|| ConfigError::invalid("type", "", "must not be empty"))?;
        config.grouping_handler = Some(crate::GroupingHandlerConfig {
            name: node.attribute("name").unwrap_or_default().to_owned(),
            local: kind == "LOCAL",
            address: s.string("address").unwrap_or_default(),
            timeout: s
                .i32("timeout", Check::GtZero)?
                .map_or(crate::GroupingHandlerConfig::DEFAULT_TIMEOUT, i64::from),
            group_timeout: s
                .i64("group-timeout", Check::MinusOneOrGtZero)?
                .unwrap_or(crate::GroupingHandlerConfig::DEFAULT_GROUP_TIMEOUT),
            reaper_period: s
                .i64("reaper-period", Check::GtZero)?
                .unwrap_or(crate::GroupingHandlerConfig::DEFAULT_REAPER_PERIOD),
        });
    }
    Ok(())
}

fn set_value<T>(slot: &mut T, value: Option<T>) {
    if let Some(v) = value {
        *slot = v;
    }
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
    if let Some(v) = s.bool("mask-password") {
        config.mask_password = Some(v);
    }
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
/// `parseBrokerPlugins`: each `<broker-plugin>`'s class and properties.
fn parse_broker_plugins(core: Node<'_, '_>, config: &mut BrokerConfig) -> Result<()> {
    let Some(list) = first(core, "broker-plugins") else {
        return Ok(());
    };
    for node in list
        .descendants()
        .filter(|n| n.is_element() && n.tag_name().name() == "broker-plugin")
    {
        let class_name = required_attribute(node, "class-name")?;
        let properties = elements(node)
            .filter(|c| c.tag_name().name() == "property")
            .map(|p| {
                (
                    p.attribute("key").unwrap_or_default().to_owned(),
                    p.attribute("value").unwrap_or_default().to_owned(),
                )
            })
            .collect();
        config.broker_plugins.push(BrokerPluginConfig {
            class_name,
            properties,
        });
    }
    Ok(())
}

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

/// `parseQueues`: the first `<queues>` under `<core>`, deprecated
/// (`AMQ222307`); each `queue` in it, at any depth, multicast, on its
/// `<address>` or else its name.
fn parse_legacy_queues(
    core: Node<'_, '_>,
    config: &mut BrokerConfig,
    warnings: &mut Vec<Warning>,
) -> Result<()> {
    let Some(list) = core
        .descendants()
        .find(|n| n.is_element() && n.tag_name().name() == "queues")
    else {
        return Ok(());
    };
    warnings.push(Warning::QueuesDeprecated);
    for node in list
        .descendants()
        .filter(|n| n.is_element() && n.tag_name().name() == "queue")
    {
        let mut q = parse_queue(node, "", RoutingType::Multicast, warnings)?;
        q.address = elements(node)
            .filter(|c| c.tag_name().name() == "address")
            .last()
            .map(trimmed_text)
            .unwrap_or_else(|| q.name.clone());
        config.queues.push(q);
    }
    Ok(())
}

/// `LegacyJMSConfiguration.parseConfiguration`: the `queue`s and then the
/// `topic`s of a `<jms>` element, at any depth. A queue is an anycast
/// address with an anycast queue of its name, durable unless `<durable>`
/// says otherwise, with its `<selector string>` as the filter; a topic is
/// a multicast address.
fn parse_jms(jms: Node<'_, '_>, config: &mut BrokerConfig, warnings: &mut Vec<Warning>) {
    for kind in ["queue", "topic"] {
        for node in jms
            .descendants()
            .filter(|n| n.is_element() && n.tag_name().name() == kind)
        {
            let Some(name) = node.attribute("name") else {
                warnings.push(Warning::JmsKeyMissing {
                    element: kind.to_owned(),
                });
                continue;
            };
            let address = if kind == "topic" {
                AddressConfig {
                    name: name.to_owned(),
                    routing_types: vec![RoutingType::Multicast],
                    queues: Vec::new(),
                }
            } else {
                let mut q = QueueConfig::new(name, name, RoutingType::Anycast);
                // `XMLConfigurationUtil.getBoolean`: the first <durable> below.
                q.durable = node
                    .descendants()
                    .find(|n| n.is_element() && n.tag_name().name() == "durable")
                    .is_none_or(node_bool);
                q.filter = elements(node)
                    .filter(|c| c.tag_name().name() == "selector")
                    .filter_map(|c| c.attribute("string"))
                    .last()
                    .map(str::to_owned);
                AddressConfig {
                    name: name.to_owned(),
                    routing_types: vec![RoutingType::Anycast],
                    queues: vec![q],
                }
            };
            config.jms_addresses.push(address);
        }
    }
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

/// `<bridges>`: the `bridge` children of every `bridges` element.
fn parse_bridges(core: Node<'_, '_>, config: &mut BrokerConfig) -> Result<()> {
    let lists: Vec<_> = all(core, "bridges").collect();
    for list in lists {
        for node in elements(list).filter(|c| c.tag_name().name().eq_ignore_ascii_case("bridge")) {
            let bridge = parse_bridge(node, config)?;
            config.bridges.push(bridge);
        }
    }
    Ok(())
}

/// `parseBridgeConfiguration`.
fn parse_bridge(node: Node<'_, '_>, config: &BrokerConfig) -> Result<crate::BridgeConfig> {
    let s = Settings { core: node };
    let mut b = crate::BridgeConfig::new(node.attribute("name").unwrap_or_default());
    b.queue_name = s.non_empty_string("queue-name")?;
    b.forwarding_address = s.string("forwarding-address");
    let transformer_class_name = s.string("transformer-class-name");
    if let Some(v) = s.text_bytes("confirmation-window-size", Check::MinusOneOrPositiveInt)? {
        b.confirmation_window_size = v as i32;
    }
    if let Some(v) = s.text_bytes("producer-window-size", Check::MinusOneOrPositiveInt)? {
        b.producer_window_size = v as i32;
    }
    set_value(
        &mut b.retry_interval,
        s.i64("retry-interval", Check::GtZero)?,
    );
    set_value(
        &mut b.client_failure_check_period,
        s.i64("check-period", Check::GtZero)?,
    );
    set_value(
        &mut b.connection_ttl,
        s.i64("connection-ttl", Check::GtZero)?,
    );
    if let Some(v) = s.text_bytes("min-large-message-size", Check::PositiveInt)? {
        b.min_large_message_size = v as i32;
    }
    set_value(
        &mut b.max_retry_interval,
        s.i64("max-retry-interval", Check::GtZero)?,
    );
    if let Some(n) = first(node, "retry-interval-multiplier") {
        let v = node_f64(n)?;
        if v <= 0.0 {
            return Err(ConfigError::invalid(
                "retry-interval-multiplier",
                v,
                "must be greater than 0",
            ));
        }
        b.retry_interval_multiplier = v;
    }
    set_value(
        &mut b.initial_connect_attempts,
        s.i32("initial-connect-attempts", Check::MinusOneOrGeZero)?,
    );
    set_value(
        &mut b.reconnect_attempts,
        s.i32("reconnect-attempts", Check::MinusOneOrGeZero)?,
    );
    set_value(
        &mut b.reconnect_attempts_on_same_node,
        s.i32("reconnect-attempts-same-node", Check::MinusOneOrGeZero)?,
    );
    set_value(
        &mut b.use_duplicate_detection,
        s.bool("use-duplicate-detection"),
    );
    if let Some(user) = s.string("user") {
        b.user = Some(user);
    }
    if let Some(n) = first(node, "routing-type") {
        b.routing_type = node_enum(n)?;
    }
    set_value(&mut b.concurrency, s.i32("concurrency", Check::GtZero)?);
    set_value(
        &mut b.pending_ack_timeout,
        s.i64("pending-ack-timeout", Check::GtZero)?,
    );
    b.client_id = s.string("client-id");
    // The first password element, untrimmed.
    if let Some(n) = first(node, "password") {
        let text = text_content(n);
        b.password = Some(
            mask::resolve_mask(config.mask_password, &text, Some(&config.password_codec))
                .map_err(|e| mask_error("password", &text, e))?,
        );
    }
    set_value(&mut b.ha, s.bool("ha"));
    let mut static_connectors = Vec::new();
    for child in elements(node) {
        match child.tag_name().name() {
            "filter" => b.filter = child.attribute("string").map(str::to_owned),
            "discovery-group-ref" => {
                b.discovery_group = child.attribute("discovery-group-name").map(str::to_owned);
            }
            "static-connectors" => {
                for r in elements(child).filter(|r| r.tag_name().name() == "connector-ref") {
                    static_connectors.push(trimmed_text(r));
                }
            }
            "transformer" => b.transformer = Some(transformer(child)),
            _ => {}
        }
    }
    if b.transformer.is_none()
        && let Some(class_name) = transformer_class_name
    {
        b.transformer = Some(TransformerConfig {
            class_name,
            properties: Vec::new(),
        });
    }
    if !static_connectors.is_empty() {
        b.static_connectors = Some(static_connectors);
        b.discovery_group = None;
    }
    Ok(b)
}

/// `<federations>`: `parseFederationsConfiguration` (its
/// `downstream-authorization`) and each `<federation>` child
/// (`parseFederationConfiguration`).
fn parse_federations(core: Node<'_, '_>, config: &mut BrokerConfig) -> Result<()> {
    let lists: Vec<_> = all(core, "federations").collect();
    for list in lists {
        if let Some(roles) = list.attribute("downstream-authorization") {
            config.federation_downstream_authorization.extend(
                roles
                    .split(',')
                    .map(java_trim)
                    .filter(|r| !r.is_empty())
                    .map(str::to_owned),
            );
        }
        for node in
            elements(list).filter(|c| c.tag_name().name().eq_ignore_ascii_case("federation"))
        {
            let federation = parse_federation(node, config)?;
            config.federations.push(federation);
        }
    }
    Ok(())
}

/// `<broker-connections>`: each `<amqp-connection>` below it, as
/// `parseAMQPBrokerConnections` reads them.
fn parse_broker_connections(core: Node<'_, '_>, config: &mut BrokerConfig) -> Result<()> {
    if first(core, "broker-connections").is_none() {
        return Ok(());
    }
    let nodes: Vec<_> = all(core, "amqp-connection").collect();
    for node in nodes {
        let c = parse_amqp_connection(node, config)?;
        config.broker_connections.push(c);
    }
    Ok(())
}

/// An attribute that may be masked (`ENC(...)`), resolved as
/// `PasswordMaskingUtil.resolveMask` resolves it.
fn enc_attribute(node: Node<'_, '_>, name: &str, config: &BrokerConfig) -> Result<Option<String>> {
    match node.attribute(name) {
        Some(text) if mask::is_enc_masked(text) => Ok(Some(
            mask::resolve_mask(config.mask_password, text, Some(&config.password_codec))
                .map_err(|e| mask_error(name, text, e))?,
        )),
        v => Ok(v.map(str::to_owned)),
    }
}

/// `getAttributeInteger`: an empty or absent attribute is the default.
fn int_attribute_or(node: Node<'_, '_>, name: &str, default: i32, check: Check) -> Result<i32> {
    match node.attribute(name).filter(|v| !v.is_empty()) {
        Some(v) => Ok(check.apply(name, i64::from(int_attribute(name, java_trim(v))?))? as i32),
        None => Ok(default),
    }
}

/// `getBooleanAttribute`.
fn bool_attribute_or(node: Node<'_, '_>, name: &str, default: bool) -> bool {
    match node.attribute(name).filter(|v| !v.is_empty()) {
        Some(v) => java_parse_bool(v),
        None => default,
    }
}

/// `parseAMQPBrokerConnections`.
fn parse_amqp_connection(
    node: Node<'_, '_>,
    config: &BrokerConfig,
) -> Result<crate::AmqpBrokerConnectConfig> {
    use crate::broker_connection::{add_property, add_unique};
    use crate::{AmqpConnectionElement as E, AmqpMatchElement};
    let name = node.attribute("name").unwrap_or_default().to_owned();
    let mut c = crate::AmqpBrokerConnectConfig::new(
        name.clone(),
        node.attribute("uri").unwrap_or_default(),
    );
    c.lock_coordinator = node.attribute("lock-coordinator").map(str::to_owned);
    c.retry_interval = int_attribute_or(node, "retry-interval", 5000, Check::GtZero)?;
    c.reconnect_attempts =
        int_attribute_or(node, "reconnect-attempts", -1, Check::MinusOneOrGtZero)?;
    c.user = enc_attribute(node, "user", config)?;
    c.password = enc_attribute(node, "password", config)?;
    c.auto_start = bool_attribute_or(node, "auto-start", true);
    // getInteger(e, "local-bind-port", -1, MINUS_ONE_OR_GT_ZERO): checked,
    // then not used.
    if let Some(n) = first(node, "local-bind-port") {
        Check::MinusOneOrGtZero.apply("local-bind-port", i64::from(node_i32(n)?))?;
    }
    let properties = |n: Node<'_, '_>, into: &mut Vec<(String, String)>| {
        for p in elements(n).filter(|p| p.tag_name().name() == "property") {
            add_property(
                into,
                p.attribute("key").unwrap_or_default().to_owned(),
                p.attribute("value").unwrap_or_default().to_owned(),
            );
        }
    };
    for e in elements(node) {
        let tag = e.tag_name().name();
        let element = match tag.to_ascii_uppercase().as_str() {
            "MIRROR" => {
                let mut m = crate::AmqpMirrorElement {
                    message_acknowledgements: bool_attribute_or(
                        e,
                        "message-acknowledgements",
                        true,
                    ),
                    queue_creation: bool_attribute_or(e, "queue-creation", true),
                    durable: bool_attribute_or(e, "durable", true),
                    queue_removal: bool_attribute_or(e, "queue-removal", true),
                    sync: bool_attribute_or(e, "sync", false),
                    address_filter: e.attribute("address-filter").map(str::to_owned),
                    properties: Vec::new(),
                };
                properties(e, &mut m.properties);
                E::Mirror(m)
            }
            "FEDERATION" => {
                let mut f = crate::AmqpFederationElement {
                    name: name.clone(),
                    ..Default::default()
                };
                for p in elements(e) {
                    match p.tag_name().name() {
                        "remote-queue-policy" => add_unique(
                            &mut f.remote_queue_policies,
                            parse_amqp_federation_queue_policy(p)?,
                        ),
                        "local-queue-policy" => add_unique(
                            &mut f.local_queue_policies,
                            parse_amqp_federation_queue_policy(p)?,
                        ),
                        "remote-address-policy" => add_unique(
                            &mut f.remote_address_policies,
                            parse_amqp_federation_address_policy(p)?,
                        ),
                        "local-address-policy" => add_unique(
                            &mut f.local_address_policies,
                            parse_amqp_federation_address_policy(p)?,
                        ),
                        "property" => add_property(
                            &mut f.properties,
                            p.attribute("key").unwrap_or_default().to_owned(),
                            p.attribute("value").unwrap_or_default().to_owned(),
                        ),
                        _ => {}
                    }
                }
                E::Federation(f)
            }
            "BRIDGE" => {
                let mut b = crate::AmqpBridgeElement {
                    name: e.attribute("name").unwrap_or(&name).to_owned(),
                    ..Default::default()
                };
                for p in elements(e) {
                    match p.tag_name().name() {
                        "bridge-from-address" => add_unique(
                            &mut b.from_address_policies,
                            parse_amqp_bridge_address_policy(p)?,
                        ),
                        "bridge-to-address" => add_unique(
                            &mut b.to_address_policies,
                            parse_amqp_bridge_address_policy(p)?,
                        ),
                        "bridge-from-queue" => add_unique(
                            &mut b.from_queue_policies,
                            parse_amqp_bridge_queue_policy(p)?,
                        ),
                        "bridge-to-queue" => {
                            add_unique(&mut b.to_queue_policies, parse_amqp_bridge_queue_policy(p)?)
                        }
                        "property" => add_property(
                            &mut b.properties,
                            p.attribute("key").unwrap_or_default().to_owned(),
                            p.attribute("value").unwrap_or_default().to_owned(),
                        ),
                        _ => {}
                    }
                }
                E::Bridge(b)
            }
            t @ ("SENDER" | "RECEIVER" | "PEER") => {
                let m = AmqpMatchElement {
                    address_match: e.attribute("address-match").map(str::to_owned),
                    queue_name: e.attribute("queue-name").map(str::to_owned),
                };
                match t {
                    "SENDER" => E::Sender(m),
                    "RECEIVER" => E::Receiver(m),
                    _ => E::Peer(m),
                }
            }
            // AMQPBrokerConnectionAddressType.valueOf.
            _ => {
                return Err(ConfigError::invalid(
                    "amqp-connection",
                    tag,
                    "No enum constant org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectionAddressType",
                ));
            }
        };
        c.elements.push(element);
    }
    Ok(c)
}

/// A policy's transformer: `transformer-class-name`, then a
/// `<transformer>` child in its place.
fn policy_transformer(node: Node<'_, '_>) -> Option<TransformerConfig> {
    let mut t = first(node, "transformer-class-name")
        .map(trimmed_text)
        .filter(|c| !c.is_empty())
        .map(|class_name| TransformerConfig {
            class_name,
            properties: Vec::new(),
        });
    for child in elements(node).filter(|c| c.tag_name().name() == "transformer") {
        t = Some(transformer(child));
    }
    t
}

/// A policy's `<property>` children.
fn policy_properties(node: Node<'_, '_>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for p in elements(node).filter(|p| p.tag_name().name() == "property") {
        crate::broker_connection::add_property(
            &mut out,
            p.attribute("key").unwrap_or_default().to_owned(),
            p.attribute("value").unwrap_or_default().to_owned(),
        );
    }
    out
}

/// A policy's `include`s or `exclude`s of `address-match`es.
fn address_matches(node: Node<'_, '_>, tag: &str) -> Vec<String> {
    let mut out = Vec::new();
    for c in elements(node).filter(|c| c.tag_name().name() == tag) {
        crate::broker_connection::add_unique(
            &mut out,
            c.attribute("address-match").unwrap_or_default().to_owned(),
        );
    }
    out
}

/// A policy's `include`s or `exclude`s of address and queue matches.
fn queue_matches(node: Node<'_, '_>, tag: &str) -> Vec<crate::AmqpQueueMatch> {
    let mut out = Vec::new();
    for c in elements(node).filter(|c| c.tag_name().name() == tag) {
        crate::broker_connection::add_unique(
            &mut out,
            crate::AmqpQueueMatch {
                address_match: c.attribute("address-match").unwrap_or_default().to_owned(),
                queue_match: c.attribute("queue-match").unwrap_or_default().to_owned(),
            },
        );
    }
    out
}

/// `parseAMQPFederatedFromAddressPolicy`.
fn parse_amqp_federation_address_policy(
    node: Node<'_, '_>,
) -> Result<crate::AmqpFederationAddressPolicy> {
    let mut p = crate::AmqpFederationAddressPolicy {
        name: node.attribute("name").unwrap_or_default().to_owned(),
        ..Default::default()
    };
    for a in node.attributes() {
        let (name, value) = (a.name(), a.value());
        match name {
            "max-hops" => {
                let v = int_attribute(name, value)?;
                p.max_hops = Check::MinusOneOrGeZero.apply(name, v.into())? as i32;
            }
            "auto-delete" => p.auto_delete = Some(java_parse_bool(value)),
            "auto-delete-delay" => {
                p.auto_delete_delay = Some(long_attribute(name, value, Check::GeZero)?);
            }
            "auto-delete-message-count" => {
                p.auto_delete_message_count =
                    Some(long_attribute(name, value, Check::MinusOneOrGeZero)?);
            }
            "enable-divert-bindings" => p.enable_divert_bindings = Some(java_parse_bool(value)),
            "allow-wildcard-groupings" => {
                p.allow_wildcard_groupings = Some(java_parse_bool(value));
            }
            _ => {}
        }
    }
    p.transformer = policy_transformer(node);
    p.includes = address_matches(node, "include");
    p.excludes = address_matches(node, "exclude");
    p.properties = policy_properties(node);
    Ok(p)
}

/// `parseAMQPFederatedFromQueuePolicy`.
fn parse_amqp_federation_queue_policy(
    node: Node<'_, '_>,
) -> Result<crate::AmqpFederationQueuePolicy> {
    let mut p = crate::AmqpFederationQueuePolicy {
        name: node.attribute("name").unwrap_or_default().to_owned(),
        ..Default::default()
    };
    for a in node.attributes() {
        match a.name() {
            "include-federated" => p.include_federated = java_parse_bool(a.value()),
            "priority-adjustment" => {
                p.priority_adjustment = Some(int_attribute(a.name(), a.value())?);
            }
            _ => {}
        }
    }
    p.transformer = policy_transformer(node);
    p.includes = queue_matches(node, "include");
    p.excludes = queue_matches(node, "exclude");
    p.properties = policy_properties(node);
    Ok(p)
}

/// `remote-terminus-capabilities`: split on commas, when not blank.
fn capabilities_attribute(value: &str) -> Option<Vec<String>> {
    (!value.trim().is_empty()).then(|| value.split(',').map(str::to_owned).collect())
}

/// `parseAMQPBridgeAddressPolicy`.
fn parse_amqp_bridge_address_policy(node: Node<'_, '_>) -> Result<crate::AmqpBridgeAddressPolicy> {
    let mut p = crate::AmqpBridgeAddressPolicy {
        name: node.attribute("name").unwrap_or_default().to_owned(),
        ..Default::default()
    };
    for a in node.attributes() {
        let (name, value) = (a.name(), a.value());
        match name {
            "filter" => p.filter = Some(value.to_owned()),
            "priority" => p.priority = Some(int_attribute(name, value)?),
            "enable-divert-bindings" => p.include_divert_bindings = java_parse_bool(value),
            "use-durable-subscriptions" => p.use_durable_subscriptions = java_parse_bool(value),
            "remote-address" => p.remote_address = Some(value.to_owned()),
            "remote-address-prefix" => p.remote_address_prefix = Some(value.to_owned()),
            "remote-address-suffix" => p.remote_address_suffix = Some(value.to_owned()),
            "remote-terminus-capabilities" => {
                if let Some(c) = capabilities_attribute(value) {
                    p.remote_terminus_capabilities = Some(c);
                }
            }
            _ => {}
        }
    }
    p.transformer = policy_transformer(node);
    p.includes = address_matches(node, "include");
    p.excludes = address_matches(node, "exclude");
    p.properties = policy_properties(node);
    Ok(p)
}

/// `parseAMQPBridgeQueuePolicy`.
fn parse_amqp_bridge_queue_policy(node: Node<'_, '_>) -> Result<crate::AmqpBridgeQueuePolicy> {
    let mut p = crate::AmqpBridgeQueuePolicy {
        name: node.attribute("name").unwrap_or_default().to_owned(),
        ..Default::default()
    };
    for a in node.attributes() {
        let (name, value) = (a.name(), a.value());
        match name {
            "filter" => p.filter = Some(value.to_owned()),
            "priority" => p.priority = Some(int_attribute(name, value)?),
            "priority-adjustment" => p.priority_adjustment = Some(int_attribute(name, value)?),
            "remote-address" => p.remote_address = Some(value.to_owned()),
            "remote-address-prefix" => p.remote_address_prefix = Some(value.to_owned()),
            "remote-address-suffix" => p.remote_address_suffix = Some(value.to_owned()),
            "remote-terminus-capabilities" => {
                if let Some(c) = capabilities_attribute(value) {
                    p.remote_terminus_capabilities = Some(c);
                }
            }
            _ => {}
        }
    }
    p.transformer = policy_transformer(node);
    p.includes = queue_matches(node, "include");
    p.excludes = queue_matches(node, "exclude");
    p.properties = policy_properties(node);
    Ok(p)
}

/// A password attribute, resolved as `PasswordMaskingUtil.resolveMask`
/// resolves it; an empty one is none.
fn password_attribute(node: Node<'_, '_>, config: &BrokerConfig) -> Result<Option<String>> {
    match node.attribute("password") {
        Some(text) if !text.is_empty() => Ok(Some(
            mask::resolve_mask(config.mask_password, text, Some(&config.password_codec))
                .map_err(|e| mask_error("password", text, e))?,
        )),
        _ => Ok(None),
    }
}

/// `Integer.parseInt` of an attribute.
fn int_attribute(name: &str, text: &str) -> Result<i32> {
    parse_num(name, text)
}

fn long_attribute(name: &str, text: &str, check: Check) -> Result<i64> {
    parse_num::<i64>(name, text).and_then(|v| check.apply(name, v))
}

/// `parseFederationConfiguration`.
fn parse_federation(node: Node<'_, '_>, config: &BrokerConfig) -> Result<crate::FederationConfig> {
    let mut f = crate::FederationConfig {
        name: node.attribute("name").unwrap_or_default().to_owned(),
        user: Some(node.attribute("user").unwrap_or_default().to_owned()),
        password: password_attribute(node, config)?,
        ..Default::default()
    };
    for child in elements(node) {
        match child.tag_name().name() {
            "upstream" => f.upstreams.push(parse_federation_stream(child, config)?),
            "downstream" => {
                let stream = parse_federation_stream(child, config)?;
                // getString(..., NOT_NULL_OR_EMPTY).
                let upstream_connector_ref =
                    match first(child, "upstream-connector-ref").map(trimmed_text) {
                        Some(v) if !v.is_empty() => v,
                        v => {
                            return Err(ConfigError::invalid(
                                "upstream-connector-ref",
                                v.unwrap_or_default(),
                                "must not be null or empty",
                            ));
                        }
                    };
                f.downstreams.push(crate::FederationDownstreamConfig {
                    stream,
                    upstream_connector_ref,
                });
            }
            "policy-set" => {
                let mut policy_refs = Vec::new();
                for p in elements(child).filter(|p| p.tag_name().name() == "policy") {
                    let r = p.attribute("ref").unwrap_or_default().to_owned();
                    if !policy_refs.contains(&r) {
                        policy_refs.push(r);
                    }
                }
                f.add_policy(crate::FederationPolicyConfig::Set {
                    name: child.attribute("name").unwrap_or_default().to_owned(),
                    policy_refs,
                });
            }
            "queue-policy" => f.add_policy(crate::FederationPolicyConfig::Queue(
                parse_queue_policy(child)?,
            )),
            "address-policy" => f.add_policy(crate::FederationPolicyConfig::Address(
                parse_address_policy(child)?,
            )),
            "transformer" => f.add_transformer(
                child.attribute("name").unwrap_or_default().to_owned(),
                transformer(child),
            ),
            _ => {}
        }
    }
    Ok(f)
}

/// `getQueuePolicy`.
fn parse_queue_policy(node: Node<'_, '_>) -> Result<crate::FederationQueuePolicyConfig> {
    let mut p = crate::FederationQueuePolicyConfig {
        name: node.attribute("name").unwrap_or_default().to_owned(),
        ..Default::default()
    };
    for a in node.attributes() {
        match a.name() {
            "include-federated" => p.include_federated = java_parse_bool(a.value()),
            "priority-adjustment" => {
                p.priority_adjustment = Some(int_attribute(a.name(), a.value())?);
            }
            "transformer-ref" => p.transformer_ref = Some(a.value().to_owned()),
            _ => {}
        }
    }
    for child in elements(node) {
        let matcher = || crate::FederationQueueMatcher {
            queue_match: Some(
                child
                    .attribute("queue-match")
                    .unwrap_or_default()
                    .to_owned(),
            ),
            address_match: Some(
                child
                    .attribute("address-match")
                    .unwrap_or_default()
                    .to_owned(),
            ),
        };
        let list = match child.tag_name().name() {
            "include" => &mut p.includes,
            "exclude" => &mut p.excludes,
            _ => continue,
        };
        let m = matcher();
        if !list.contains(&m) {
            list.push(m);
        }
    }
    Ok(p)
}

/// `getAddressPolicy`. As in Artemis 2.57, the hops are read from a
/// `max-consumers` attribute, which the schema does not have, and the
/// schema's `max-hops` is not read: `max_hops` stays 0.
fn parse_address_policy(node: Node<'_, '_>) -> Result<crate::FederationAddressPolicyConfig> {
    let mut p = crate::FederationAddressPolicyConfig {
        name: node.attribute("name").unwrap_or_default().to_owned(),
        ..Default::default()
    };
    for a in node.attributes() {
        let (name, value) = (a.name(), a.value());
        match name {
            "max-consumers" => {
                let v = int_attribute(name, value)?;
                p.max_hops = Check::MinusOneOrGeZero.apply(name, v.into())? as i32;
            }
            "auto-delete" => p.auto_delete = Some(java_parse_bool(value)),
            "auto-delete-delay" => {
                p.auto_delete_delay = Some(long_attribute(name, value, Check::GeZero)?);
            }
            "auto-delete-message-count" => {
                p.auto_delete_message_count =
                    Some(long_attribute(name, value, Check::MinusOneOrGeZero)?);
            }
            "transformer-ref" => p.transformer_ref = Some(value.to_owned()),
            "enable-divert-bindings" => p.enable_divert_bindings = java_parse_bool(value),
            _ => {}
        }
    }
    for child in elements(node) {
        let list = match child.tag_name().name() {
            "include" => &mut p.includes,
            "exclude" => &mut p.excludes,
            _ => continue,
        };
        let m = child
            .attribute("address-match")
            .unwrap_or_default()
            .to_owned();
        if !list.contains(&m) {
            list.push(m);
        }
    }
    Ok(p)
}

/// `getFederationStream`: an `<upstream>` or `<downstream>`. As in Artemis
/// 2.57, `share-connection` is not read.
fn parse_federation_stream(
    node: Node<'_, '_>,
    config: &BrokerConfig,
) -> Result<crate::FederationStreamConfig> {
    use crate::FederationConnectionConfig as C;
    let s = Settings { core: node };
    let mut c = C {
        password: password_attribute(node, config)?,
        username: Some(node.attribute("user").unwrap_or_default().to_owned()),
        ..C::default()
    };
    if let Some(v) = node.attribute("priority-adjustment") {
        c.priority_adjustment = int_attribute("priority-adjustment", v)?;
    }
    c.ha = s.bool("ha").unwrap_or(false);
    set_value(
        &mut c.circuit_breaker_timeout,
        s.i64("circuit-breaker-timeout", Check::MinusOneOrGeZero)?,
    );
    set_value(
        &mut c.client_failure_check_period,
        s.i64("check-period", Check::GtZero)?,
    );
    set_value(
        &mut c.connection_ttl,
        s.i64("connection-ttl", Check::GtZero)?,
    );
    set_value(
        &mut c.retry_interval,
        s.i64("retry-interval", Check::GtZero)?,
    );
    set_value(&mut c.call_timeout, s.i64("call-timeout", Check::GtZero)?);
    // ActiveMQClient.DEFAULT_CALL_FAILOVER_TIMEOUT, not the class's -1.
    c.call_failover_timeout = s
        .i64("call-failover-timeout", Check::MinusOneOrGtZero)?
        .unwrap_or(30_000);
    if let Some(n) = first(node, "retry-interval-multiplier") {
        let v = node_f64(n)?;
        if v <= 0.0 {
            return Err(ConfigError::invalid(
                "retry-interval-multiplier",
                v,
                "must be greater than 0",
            ));
        }
        c.retry_interval_multiplier = v;
    }
    set_value(
        &mut c.max_retry_interval,
        s.i64("max-retry-interval", Check::GtZero)?,
    );
    set_value(
        &mut c.initial_connect_attempts,
        s.i32("initial-connect-attempts", Check::MinusOneOrGeZero)?,
    );
    set_value(
        &mut c.reconnect_attempts,
        s.i32("reconnect-attempts", Check::MinusOneOrGeZero)?,
    );
    let mut stream = crate::FederationStreamConfig {
        name: node.attribute("name").unwrap_or_default().to_owned(),
        ..Default::default()
    };
    let mut static_connectors = Vec::new();
    let mut discovery_group = None;
    for child in elements(node) {
        match child.tag_name().name() {
            "discovery-group-ref" => {
                discovery_group = child.attribute("discovery-group-name").map(str::to_owned);
            }
            "static-connectors" => {
                for r in all(child, "connector-ref") {
                    static_connectors.push(trimmed_text(r));
                }
            }
            "policy" => {
                stream.add_policy_ref(child.attribute("ref").unwrap_or_default().to_owned())
            }
            _ => {}
        }
    }
    if static_connectors.is_empty() {
        c.discovery_group = discovery_group;
    } else {
        c.static_connectors = Some(static_connectors);
    }
    stream.connection = c;
    Ok(stream)
}

/// `getTransformerConfiguration`: `<class-name>` and the `<property>`
/// children.
fn transformer(t: Node<'_, '_>) -> TransformerConfig {
    TransformerConfig {
        class_name: first(t, "class-name").map(trimmed_text).unwrap_or_default(),
        properties: all(t, "property")
            .map(|p| {
                (
                    p.attribute("key").unwrap_or_default().to_owned(),
                    p.attribute("value").unwrap_or_default().to_owned(),
                )
            })
            .collect(),
    }
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
