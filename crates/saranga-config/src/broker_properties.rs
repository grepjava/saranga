//! Broker properties: `broker.properties` and `brokerconfig.`-prefixed
//! system properties, applied over the parsed `broker.xml`. Ported from
//! `ConfigurationImpl.parsePrefixedProperties` and `populateWithProperties`.
//!
//! Keys are bean paths over the configuration: `globalMaxSize=1G`,
//! `addressSettings."orders.#".maxSizeBytes=10M`,
//! `acceptorConfigurations.tcp.params.port=61617`. A segment holding dots
//! is wrapped in the key surround (`"` unless `key.surround` says
//! otherwise). A collection entry whose value is the remove value (`-`
//! unless `remove.value` says otherwise) is removed.
//!
//! Top-level settings take the camel-case form of their `broker.xml`
//! element (`journalFileSize` for `<journal-file-size>`) and go through the
//! same parsing; so do address settings. Saranga supports the collections
//! `addressSettings`, `securityRoles`, `addressConfigurations`,
//! `acceptorConfigurations`, `divertConfigurations` and `jaasConfigs`.
//! Anything else is reported as a [`Warning`], as Artemis reports what it
//! cannot apply in its status instead of failing.

use crate::acceptor::AcceptorConfig;
use crate::broker::{
    AddressConfig, BrokerConfig, DivertConfig, DivertRoutingType, JaasModuleConfig, QueueConfig,
    Warning,
};
use crate::security::{CheckType, Role, SecuritySetting};
use crate::{ConfigError, Result, RoutingType, mask};
use std::path::Path;

pub const DEFAULT_KEY_SURROUND: &str = "\"";
pub const DEFAULT_REMOVE_VALUE: &str = "-";
/// `system-property-prefix`'s default.
pub const DEFAULT_SYSTEM_PROPERTY_PREFIX: &str = "brokerconfig.";

/// `ConfigurationImpl.parseProperties`: applies the comma-separated
/// `sources` in order. A source ending in `/` is a directory whose files
/// matching `.*\.(json|yaml|properties)` (or the regex of a `?filter=`
/// query) are applied in name order. JSON and YAML files are reported and
/// skipped. A file that cannot be read is an error, as it is in Artemis.
pub fn apply_sources(
    config: &mut BrokerConfig,
    sources: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<Vec<Warning>> {
    let mut warnings = Vec::new();
    for source in sources.split(',') {
        let (path, query) = match source.split_once('?') {
            Some((p, q)) => (p, Some(q)),
            None => (source, None),
        };
        let filter = query
            .into_iter()
            .flat_map(|q| q.split(['&', ';']))
            .find_map(|kv| kv.strip_prefix("filter="))
            .unwrap_or(r".*\.(json|yaml|properties)");
        let filter = regex::Regex::new(&format!("^(?:{filter})$"))
            .map_err(|e| ConfigError::invalid("broker properties filter", filter, e.to_string()))?;
        if path.ends_with('/') {
            let Ok(dir) = std::fs::read_dir(path) else {
                continue;
            };
            let mut names: Vec<String> = dir
                .filter_map(|e| e.ok()?.file_name().into_string().ok())
                .filter(|n| filter.is_match(n))
                .collect();
            names.sort();
            for name in names {
                let file = Path::new(path).join(&name);
                warnings.extend(apply_file(config, &file, lookup)?);
            }
        } else {
            warnings.extend(apply_file(config, Path::new(path), lookup)?);
        }
    }
    Ok(warnings)
}

/// `parseFileProperties`: one properties file, named in warnings by its
/// file name.
pub fn apply_file(
    config: &mut BrokerConfig,
    file: &Path,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<Vec<Warning>> {
    let name = file
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let bytes = std::fs::read(file).map_err(|source| ConfigError::Io {
        path: file.display().to_string(),
        source,
    })?;
    if name.ends_with(".json") || name.ends_with(".yaml") {
        return Ok(vec![problem(
            &name,
            "",
            "JSON and YAML broker properties are not supported",
        )]);
    }
    Ok(apply(
        config,
        &name,
        &crate::properties::parse(&bytes),
        lookup,
    ))
}

/// `parsePrefixedProperties(System.getProperties(), prefix)`: the system
/// properties whose names start with `config.system_property_prefix`,
/// without it.
pub fn apply_system(
    config: &mut BrokerConfig,
    system: &[(String, String)],
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Vec<Warning> {
    let prefix = config.system_property_prefix.clone();
    let entries: Vec<(String, String)> = system
        .iter()
        .filter_map(|(k, v)| Some((k.strip_prefix(&prefix)?.to_owned(), v.clone())))
        .collect();
    if entries.is_empty() {
        return Vec::new();
    }
    apply(config, &format!("system-{prefix}"), &entries, lookup)
}

/// Applies `entries` (in file order) to `config`. `source` names them in
/// warnings; `lookup` resolves `${...}` in keys and values.
pub fn apply(
    config: &mut BrokerConfig,
    source: &str,
    entries: &[(String, String)],
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Vec<Warning> {
    let mut warnings = Vec::new();
    let mut entries: Vec<(String, String)> = entries.to_vec();
    let take = |entries: &mut Vec<(String, String)>, key: &str| {
        let i = entries.iter().position(|(k, _)| k == key)?;
        Some(entries.remove(i).1)
    };
    let surround =
        take(&mut entries, "key.surround").unwrap_or_else(|| DEFAULT_KEY_SURROUND.into());
    let remove = take(&mut entries, "remove.value").unwrap_or_else(|| DEFAULT_REMOVE_VALUE.into());
    for (key, value) in entries {
        let key = crate::substitute::replace_system_props(&key, lookup);
        let mut value = crate::substitute::replace_system_props(&value, lookup);
        if mask::is_enc_masked(&value) {
            match mask::resolve_mask(None, &value, Some(&config.password_codec)) {
                Ok(v) => value = v,
                Err(e) => {
                    warnings.push(problem(source, &key, e.to_string()));
                    continue;
                }
            }
        }
        let path = split_key(&key, &surround);
        let p: Vec<&str> = path.iter().map(String::as_str).collect();
        if let Err(reason) = apply_one(config, &p, &value, value == remove) {
            warnings.push(problem(source, &key, reason));
        }
    }
    warnings
}

fn problem(source: &str, key: &str, reason: impl Into<String>) -> Warning {
    Warning::BrokerProperty {
        source: source.to_owned(),
        key: key.to_owned(),
        reason: reason.into(),
    }
}

/// Splits a key on dots, keeping a surrounded segment whole.
pub(crate) fn split_key(key: &str, surround: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = key;
    while !rest.is_empty() {
        if !surround.is_empty()
            && let Some(inner) = rest.strip_prefix(surround)
            && let Some(end) = inner.find(surround)
        {
            out.push(inner[..end].to_owned());
            rest = &inner[end + surround.len()..];
            rest = rest.strip_prefix('.').unwrap_or(rest);
            continue;
        }
        match rest.find('.') {
            Some(i) => {
                out.push(rest[..i].to_owned());
                rest = &rest[i + 1..];
            }
            None => {
                out.push(rest.to_owned());
                rest = "";
            }
        }
    }
    out
}

/// `fooBarBaz` -> `foo-bar-baz`.
pub(crate) fn kebab(camel: &str) -> String {
    let mut out = String::with_capacity(camel.len() + 4);
    for c in camel.chars() {
        if c.is_ascii_uppercase() {
            out.push('-');
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

fn parse_bool(v: &str) -> std::result::Result<bool, String> {
    match v.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{v:?} is not a boolean")),
    }
}

fn parse_num<T: std::str::FromStr>(v: &str) -> std::result::Result<T, String> {
    v.trim()
        .parse()
        .map_err(|_| format!("{v:?} is not a number"))
}

fn err_text(e: ConfigError) -> String {
    e.to_string()
}

fn apply_one(
    config: &mut BrokerConfig,
    path: &[&str],
    value: &str,
    remove: bool,
) -> std::result::Result<(), String> {
    match path {
        [] => Err("empty key".into()),
        [scalar] => match crate::parser::apply_scalar(config, &kebab(scalar), value) {
            Ok(true) => Ok(()),
            Ok(false) => Err(format!(
                "{scalar} is not a configuration property Saranga knows"
            )),
            Err(e) => Err(err_text(e)),
        },
        ["addressSettings", m] if remove => {
            config.address_settings.retain(|(k, _)| k != m);
            Ok(())
        }
        ["addressSettings", m, setting] => {
            let fragment = crate::parser::address_setting_fragment(&kebab(setting), value)
                .map_err(err_text)?
                .ok_or_else(|| format!("{setting} is not an address setting"))?;
            match config.address_settings.iter_mut().find(|(k, _)| k == m) {
                Some((_, existing)) => {
                    *existing = crate::repository::Mergeable::merge_copy(&fragment, existing)
                }
                None => config.address_settings.push(((*m).to_owned(), fragment)),
            }
            Ok(())
        }
        ["securityRoles", m] if remove => {
            config.security_settings.retain(|(k, _)| k != m);
            Ok(())
        }
        ["securityRoles", m, role, permission] => {
            let check = permission_check(permission)
                .ok_or_else(|| format!("{permission} is not a permission"))?;
            let grant = parse_bool(value)?;
            let setting = match config
                .security_settings
                .iter_mut()
                .position(|(k, _)| k == m)
            {
                Some(i) => &mut config.security_settings[i].1,
                None => {
                    config
                        .security_settings
                        .push(((*m).to_owned(), SecuritySetting::default()));
                    &mut config.security_settings.last_mut().expect("pushed").1
                }
            };
            let i = match setting.roles.iter().position(|r| r.name == *role) {
                Some(i) => i,
                None => {
                    setting.roles.push(Role::new(*role, []));
                    setting.roles.len() - 1
                }
            };
            let old = setting.roles[i].clone();
            let mut rebuilt = Role::new(
                old.name.clone(),
                CheckType::ALL
                    .into_iter()
                    .filter(|c| if *c == check { grant } else { old.allows(*c) }),
            );
            std::mem::swap(&mut setting.roles[i], &mut rebuilt);
            Ok(())
        }
        ["addressConfigurations", name] if remove => {
            config.addresses.retain(|a| a.name != *name);
            Ok(())
        }
        ["addressConfigurations", name, rest @ ..] => {
            address_property(config, name, rest, value, remove)
        }
        ["acceptorConfigurations", name] if remove => {
            config
                .acceptors
                .retain(|a| a.name.as_deref() != Some(*name));
            Ok(())
        }
        ["acceptorConfigurations", name, rest @ ..] => {
            acceptor_property(config, name, rest, value, remove)
        }
        ["divertConfigurations", name] if remove => {
            config.diverts.retain(|d| d.name != *name);
            Ok(())
        }
        ["divertConfigurations", name, field] => divert_property(config, name, field, value),
        ["jaasConfigs", realm] if remove => {
            config.jaas_configs.retain(|(r, _)| r != realm);
            Ok(())
        }
        ["jaasConfigs", realm, "modules", module, rest @ ..] => {
            jaas_property(config, realm, module, rest, value, remove)
        }
        [collection, ..] => Err(format!("{collection} is not supported by Saranga")),
    }
}

/// `securityRoles.<match>.<role>.<permission>`: the `Role` bean's boolean
/// properties.
fn permission_check(name: &str) -> Option<CheckType> {
    Some(match name {
        "send" => CheckType::Send,
        "consume" => CheckType::Consume,
        "createAddress" => CheckType::CreateAddress,
        "deleteAddress" => CheckType::DeleteAddress,
        "createDurableQueue" => CheckType::CreateDurableQueue,
        "deleteDurableQueue" => CheckType::DeleteDurableQueue,
        "createNonDurableQueue" => CheckType::CreateNonDurableQueue,
        "deleteNonDurableQueue" => CheckType::DeleteNonDurableQueue,
        "manage" => CheckType::Manage,
        "browse" => CheckType::Browse,
        "view" => CheckType::View,
        "edit" => CheckType::Edit,
        _ => return None,
    })
}

fn routing_type(v: &str) -> std::result::Result<RoutingType, String> {
    v.trim().parse::<RoutingType>().map_err(err_text)
}

/// `addressConfigurations.<name>.routingTypes` and
/// `.queueConfigs.<queue>.<field>`.
fn address_property(
    config: &mut BrokerConfig,
    name: &str,
    rest: &[&str],
    value: &str,
    remove: bool,
) -> std::result::Result<(), String> {
    let i = match config.addresses.iter().position(|a| a.name == name) {
        Some(i) => i,
        None => {
            config.addresses.push(AddressConfig {
                name: name.to_owned(),
                routing_types: Vec::new(),
                queues: Vec::new(),
            });
            config.addresses.len() - 1
        }
    };
    let address = &mut config.addresses[i];
    match rest {
        ["routingTypes"] => {
            address.routing_types.clear();
            for t in value.split(',').filter(|t| !t.trim().is_empty()) {
                let t = routing_type(t)?;
                if !address.routing_types.contains(&t) {
                    address.routing_types.push(t);
                }
            }
            Ok(())
        }
        ["queueConfigs", queue] if remove => {
            address.queues.retain(|q| q.name != *queue);
            Ok(())
        }
        ["queueConfigs", queue, field] => {
            let q = match address.queues.iter().position(|q| q.name == *queue) {
                Some(i) => &mut address.queues[i],
                None => {
                    let rt = address
                        .routing_types
                        .first()
                        .copied()
                        .unwrap_or(RoutingType::Multicast);
                    address.queues.push(QueueConfig::new(queue, name, rt));
                    address.queues.last_mut().expect("pushed")
                }
            };
            queue_field(q, field, value)?;
            if *field == "routingType" && !address.routing_types.contains(&q.routing_type) {
                let rt = q.routing_type;
                address.routing_types.push(rt);
            }
            Ok(())
        }
        _ => Err(format!("{} is not an address property", rest.join("."))),
    }
}

fn queue_field(q: &mut QueueConfig, field: &str, v: &str) -> std::result::Result<(), String> {
    match field {
        "routingType" => q.routing_type = routing_type(v)?,
        "address" => q.address = v.to_owned(),
        "filterString" => q.filter = Some(v.to_owned()),
        "durable" => q.durable = parse_bool(v)?,
        "user" => q.user = Some(v.to_owned()),
        "maxConsumers" => q.max_consumers = Some(parse_num(v)?),
        "purgeOnNoConsumers" => q.purge_on_no_consumers = parse_bool(v)?,
        "exclusive" => q.exclusive = Some(parse_bool(v)?),
        "groupRebalance" => q.group_rebalance = Some(parse_bool(v)?),
        "groupRebalancePauseDispatch" => q.group_rebalance_pause_dispatch = Some(parse_bool(v)?),
        "groupBuckets" => q.group_buckets = Some(parse_num(v)?),
        "groupFirstKey" => q.group_first_key = Some(v.to_owned()),
        "lastValue" => q.last_value = Some(parse_bool(v)?),
        "lastValueKey" => q.last_value_key = Some(v.to_owned()),
        "nonDestructive" => q.non_destructive = Some(parse_bool(v)?),
        "consumersBeforeDispatch" => q.consumers_before_dispatch = Some(parse_num(v)?),
        "delayBeforeDispatch" => q.delay_before_dispatch = Some(parse_num(v)?),
        "ringSize" => q.ring_size = parse_num(v)?,
        "enabled" => q.enabled = Some(parse_bool(v)?),
        "name" => {}
        other => return Err(format!("{other} is not a queue property")),
    }
    Ok(())
}

/// `acceptorConfigurations.<name>.params.<key>` and `.factoryClassName`.
fn acceptor_property(
    config: &mut BrokerConfig,
    name: &str,
    rest: &[&str],
    value: &str,
    remove: bool,
) -> std::result::Result<(), String> {
    let i = match config
        .acceptors
        .iter()
        .position(|a| a.name.as_deref() == Some(name))
    {
        Some(i) => i,
        None => {
            config
                .acceptors
                .push(AcceptorConfig::from_params(name, Vec::new()));
            config.acceptors.len() - 1
        }
    };
    let acceptor = &mut config.acceptors[i];
    match rest {
        ["factoryClassName"] => {
            if !value.ends_with("NettyAcceptorFactory") {
                return Err(format!("acceptor factory {value} is not supported"));
            }
            Ok(())
        }
        ["params", key] => {
            acceptor.set_param(key, if remove { None } else { Some(value) });
            Ok(())
        }
        _ => Err(format!("{} is not an acceptor property", rest.join("."))),
    }
}

fn divert_property(
    config: &mut BrokerConfig,
    name: &str,
    field: &str,
    v: &str,
) -> std::result::Result<(), String> {
    let i = match config.diverts.iter().position(|d| d.name == name) {
        Some(i) => i,
        None => {
            config.diverts.push(DivertConfig {
                name: name.to_owned(),
                routing_name: None,
                address: String::new(),
                forwarding_address: String::new(),
                exclusive: false,
                filter: None,
                routing_type: DivertRoutingType::default(),
                transformer: None,
            });
            config.diverts.len() - 1
        }
    };
    let d = &mut config.diverts[i];
    match field {
        "routingName" => d.routing_name = Some(v.to_owned()),
        "address" => d.address = v.to_owned(),
        "forwardingAddress" => d.forwarding_address = v.to_owned(),
        "exclusive" => d.exclusive = parse_bool(v)?,
        "filterString" => d.filter = Some(v.to_owned()),
        "routingType" => d.routing_type = v.parse().map_err(err_text)?,
        other => return Err(format!("{other} is not a divert property")),
    }
    Ok(())
}

/// `jaasConfigs.<realm>.modules.<module>.loginModuleClass`, `.controlFlag`
/// and `.params.<key>`.
fn jaas_property(
    config: &mut BrokerConfig,
    realm: &str,
    module: &str,
    rest: &[&str],
    value: &str,
    remove: bool,
) -> std::result::Result<(), String> {
    let r = match config.jaas_configs.iter().position(|(n, _)| n == realm) {
        Some(i) => i,
        None => {
            config.jaas_configs.push((realm.to_owned(), Vec::new()));
            config.jaas_configs.len() - 1
        }
    };
    let modules = &mut config.jaas_configs[r].1;
    if rest.is_empty() && remove {
        modules.retain(|m| m.name != module);
        return Ok(());
    }
    let m = match modules.iter().position(|m| m.name == module) {
        Some(i) => &mut modules[i],
        None => {
            modules.push(JaasModuleConfig {
                name: module.to_owned(),
                login_module_class: String::new(),
                control_flag: "required".into(),
                params: Vec::new(),
            });
            modules.last_mut().expect("pushed")
        }
    };
    match rest {
        ["loginModuleClass"] => m.login_module_class = value.to_owned(),
        // `JaasAppConfigurationEntry.getLoginModuleControlFlag`.
        ["controlFlag"] => match value {
            "" | "required" | "requisite" | "optional" | "sufficient" => {
                m.control_flag = value.to_owned()
            }
            _ => return Err(format!("Unknown control flag: {value}")),
        },
        ["params", key] => {
            m.params.retain(|(k, _)| k != key);
            if !remove {
                m.params.push(((*key).to_owned(), value.to_owned()));
            }
        }
        _ => return Err(format!("{} is not a JAAS module property", rest.join("."))),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(s: &str) -> Vec<(String, String)> {
        s.lines()
            .filter_map(|l| l.split_once('='))
            .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
            .collect()
    }

    fn apply_str(config: &mut BrokerConfig, s: &str) -> Vec<Warning> {
        apply(config, "broker.properties", &props(s), &|n| {
            (n == "PORT").then(|| "61699".to_owned())
        })
    }

    #[test]
    fn sources() {
        let dir = std::env::temp_dir().join(format!("saranga-bp-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("d")).unwrap();
        let write = |name: &str, text: &str| std::fs::write(dir.join(name), text).unwrap();
        write("a.properties", "name=a\nglobalMaxSize=1M\n");
        write("d/2.properties", "name=two\n");
        write("d/1.properties", "name=one\nidCacheSize=7\n");
        write("d/x.txt", "name=txt\n");
        let d = dir.display().to_string().replace('\\', "/");
        let mut c = BrokerConfig::default();
        let w = apply_sources(&mut c, &format!("{d}/a.properties,{d}/d/"), &|_| None).unwrap();
        assert!(w.is_empty(), "{w:?}");
        assert_eq!((c.name.as_str(), c.id_cache_size), ("two", 7));
        assert_eq!(c.global_max_size, Some(1024 * 1024));
        let w = apply_sources(&mut c, &format!("{d}/d/?filter=.*\\.txt"), &|_| None).unwrap();
        assert!(w.is_empty() && c.name == "txt");
        assert!(apply_sources(&mut c, &format!("{d}/missing.properties"), &|_| None).is_err());

        let system = [
            ("brokerconfig.name".to_owned(), "sys".to_owned()),
            ("other".to_owned(), "x".to_owned()),
        ];
        assert!(apply_system(&mut c, &system, &|_| None).is_empty());
        assert_eq!(c.name, "sys");
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn keys() {
        assert_eq!(
            split_key("addressSettings.\"a.b.#\".maxSizeBytes", "\""),
            vec!["addressSettings", "a.b.#", "maxSizeBytes"]
        );
        assert_eq!(split_key("a.b", "\""), vec!["a", "b"]);
        assert_eq!(kebab("globalMaxSize"), "global-max-size");
    }

    #[test]
    fn scalars() {
        let mut c = BrokerConfig::default();
        let w = apply_str(
            &mut c,
            "globalMaxSize=10M\nsecurityEnabled=false\nname=props\njournalFileSize=1M\nnoSuchThing=1\npersistenceEnabled=maybe",
        );
        assert_eq!(c.global_max_size, Some(10 * 1024 * 1024));
        assert!(!c.security_enabled);
        assert_eq!(c.name, "props");
        assert_eq!(c.journal.file_size, Some(1024 * 1024));
        // An unknown key is reported; `persistenceEnabled=maybe` is false,
        // as `Boolean.parseBoolean` has it.
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(!c.persistence_enabled);
    }

    #[test]
    fn collections() {
        let mut c = BrokerConfig::default();
        let w = apply_str(
            &mut c,
            r#"addressSettings."orders.#".maxSizeBytes=10M
addressSettings."orders.#".deadLetterAddress=DLQ
securityRoles."orders.#".clerks.send=true
securityRoles."orders.#".clerks.consume=true
securityRoles."orders.#".clerks.consume=false
addressConfigurations.orders.queueConfigs.orders.routingType=ANYCAST
addressConfigurations.orders.queueConfigs.orders.durable=false
acceptorConfigurations.tcp.factoryClassName=org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory
acceptorConfigurations.tcp.params.host=localhost
acceptorConfigurations.tcp.params.port=${PORT}
jaasConfigs.guestRealm.modules.guest.loginModuleClass=org.apache.activemq.artemis.spi.core.security.jaas.GuestLoginModule
jaasConfigs.guestRealm.modules.guest.controlFlag=sufficient
clusterConfigurations.c.name=c"#,
        );
        assert_eq!(w.len(), 1, "only the cluster: {w:?}");
        let (m, s) = &c.address_settings[0];
        assert_eq!(m, "orders.#");
        assert_eq!(s.max_size_bytes, Some(10 * 1024 * 1024));
        assert_eq!(s.dead_letter_address.as_deref(), Some("DLQ"));
        let roles = &c.security_settings[0].1.roles;
        assert!(roles[0].allows(CheckType::Send) && !roles[0].allows(CheckType::Consume));
        let a = &c.addresses[0];
        assert_eq!(a.routing_types, vec![RoutingType::Anycast]);
        assert!(!a.queues[0].durable);
        let acc = &c.acceptors[0];
        assert_eq!(
            (acc.host.as_deref(), acc.port),
            (Some("localhost"), Some(61699))
        );
        assert_eq!(c.jaas_configs[0].1[0].control_flag, "sufficient");

        let w = apply_str(
            &mut c,
            "addressSettings.\"orders.#\"=-\nacceptorConfigurations.tcp=-",
        );
        assert!(w.is_empty());
        assert!(c.address_settings.is_empty() && c.acceptors.is_empty());
    }
}
