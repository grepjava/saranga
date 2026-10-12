//! `ActiveMQServerControlImpl`: the broker's attributes, and the
//! operations on its addresses, queues, diverts and address settings.

use std::sync::Arc;

use super::args::{Args, Call, Ty::*};
use super::json::Json;
use super::{Management, Outcome};
use crate::postoffice::{DivertDeployment, DivertRoutingType, QueueSpec, RoutingTypes};
use crate::{Broker, RoutingType};

/// The Artemis release whose management interface Saranga answers as
/// (`getVersion`); see COMPAT.md.
pub const VERSION: &str = "2.57.0";

/// `TimeUtils.printDuration`.
pub fn print_duration(millis: i64) -> String {
    let group = |n: i64| {
        let s = n.to_string();
        let mut out = String::new();
        for (i, c) in s.chars().enumerate() {
            if i > 0 && (s.len() - i).is_multiple_of(3) {
                out.push(',');
            }
            out.push(c);
        }
        out
    };
    let plural =
        |n: i64, unit: &str| format!("{} {unit}{}", group(n), if n > 1 { "s" } else { "" });
    let mut uptime = millis as f64 / 1000.0;
    if uptime < 60.0 {
        return format!("{uptime:.3} seconds");
    }
    uptime /= 60.0;
    if uptime < 60.0 {
        return plural(uptime as i64, "minute");
    }
    uptime /= 60.0;
    if uptime < 24.0 {
        let hours = uptime as i64;
        let minutes = ((uptime - hours as f64) * 60.0) as i64;
        let mut s = plural(hours, "hour");
        if minutes != 0 {
            s = format!("{s} {}", plural(minutes, "minute"));
        }
        return s;
    }
    uptime /= 24.0;
    let days = uptime as i64;
    let hours = ((uptime - days as f64) * 24.0) as i64;
    let mut s = plural(days, "day");
    if hours != 0 {
        s = format!("{s} {}", plural(hours, "hour"));
    }
    s
}

pub(super) async fn attribute(
    m: &Management,
    broker: &Arc<Broker>,
    name: &str,
) -> Result<Option<Json>, String> {
    let po = broker.post_office();
    let file = m.config.file.as_deref();
    let journal = file.map(|f| &f.journal);
    let journal_type = journal
        .map(|j| j.journal_type)
        .unwrap_or(saranga_config::JournalConfig::DEFAULT_JOURNAL_TYPE);
    let jd = |f: fn(&saranga_config::JournalConfig) -> Option<i32>, default: i32| {
        journal.and_then(f).unwrap_or(default)
    };
    let jb = |f: fn(&saranga_config::JournalConfig) -> Option<bool>, default: bool| {
        journal.and_then(f).unwrap_or(default)
    };
    let dir = |f: fn(&saranga_config::BrokerConfig) -> &String, default: &str| {
        file.map_or(default.to_owned(), |c| f(c).clone())
    };
    let uptime = crate::message::now_millis() - m.started_at();
    let queues = po.queues();
    if let Some(v) = super::connections::attribute(broker, name) {
        return Ok(Some(v));
    }
    if let Some(v) = super::security::attribute(broker, name) {
        return Ok(Some(v));
    }
    Ok(Some(match name {
        "Name" => Json::from(m.config.name.clone()),
        "Version" => Json::from(VERSION),
        "NodeID" => Json::from(broker.node_id()),
        "Started" | "Active" => Json::from(true),
        "Backup" | "SharedStore" | "Clustered" | "ReplicaSync" | "FailoverOnServerShutdown" => {
            Json::from(false)
        }
        "HAPolicy" => Json::Null,
        "Uptime" => Json::from(print_duration(uptime)),
        "UptimeMillis" => Json::from(uptime),
        "CurrentTimeMillis" => Json::from(crate::message::now_millis()),
        "Status" => Json::from(status(m, broker).to_string()),
        "ActivationSequence" => Json::from(0i64),
        "AddressNames" => Json::from(
            po.addresses()
                .into_iter()
                .map(|a| a.name.to_string())
                .collect::<Vec<_>>(),
        ),
        "AddressCount" => Json::from(po.addresses().len() as i32),
        "QueueNames" => Json::from(
            queues
                .iter()
                .map(|q| q.name().to_string())
                .collect::<Vec<_>>(),
        ),
        "QueueCount" => Json::from(queues.len() as i32),
        "DivertNames" => Json::from(
            po.diverts()
                .iter()
                .map(|d| d.name.to_string())
                .collect::<Vec<_>>(),
        ),
        "TotalMessageCount" => Json::from(queues.iter().map(|q| q.message_count()).sum::<i64>()),
        "TotalMessagesAdded" => Json::from(queues.iter().map(|q| q.messages_added()).sum::<i64>()),
        "TotalMessagesAcknowledged" => Json::from(
            queues
                .iter()
                .map(|q| q.messages_acknowledged())
                .sum::<i64>(),
        ),
        "TotalConsumerCount" => Json::from(queues.iter().map(|q| q.consumer_count()).sum::<i32>()),
        "ManagementAddress" => Json::from(m.config.address.clone()),
        "ManagementNotificationAddress" => Json::from(m.config.notification_address.clone()),
        "PersistenceEnabled" => Json::from(broker.storage().is_some()),
        "SecurityEnabled" => Json::from(broker.security().is_some()),
        "JournalDirectory" => Json::from(dir(|c| &c.journal_directory, "data/journal")),
        "BindingsDirectory" => Json::from(dir(|c| &c.bindings_directory, "data/bindings")),
        "PagingDirectory" => Json::from(dir(|c| &c.paging_directory, "data/paging")),
        "LargeMessagesDirectory" => {
            Json::from(dir(|c| &c.large_messages_directory, "data/largemessages"))
        }
        "JournalType" => Json::from(format!("{journal_type:?}").to_uppercase()),
        "JournalFileSize" => Json::from(jd(|j| j.file_size, 10 * 1024 * 1024)),
        "JournalMinFiles" => Json::from(jd(|j| j.min_files, 2)),
        "JournalPoolFiles" => Json::from(jd(|j| j.pool_files, -1)),
        "JournalCompactMinFiles" => Json::from(jd(|j| j.compact_min_files, 10)),
        "JournalCompactPercentage" => Json::from(jd(|j| j.compact_percentage, 30)),
        "JournalMaxIO" => Json::from(jd(|j| j.max_io, 1)),
        "JournalBufferSize" => Json::from(jd(|j| j.buffer_size, 490 * 1024)),
        "JournalBufferTimeout" => Json::from(jd(|j| j.buffer_timeout, 3_333_333)),
        "JournalSyncTransactional" => Json::from(jb(|j| j.sync_transactional, true)),
        "JournalSyncNonTransactional" => Json::from(jb(|j| j.sync_non_transactional, true)),
        "CreateJournalDir" => Json::from(jb(|j| j.create_journal_dir, true)),
        "CreateBindingsDir" => Json::from(jb(|j| j.create_bindings_dir, true)),
        "MaxDiskUsage" => Json::from(jd(|j| j.max_disk_usage, 90)),
        "DiskScanPeriod" => Json::from(jd(|j| j.disk_scan_period, 5000)),
        "DiskStoreUsage" => Json::from(
            broker
                .storage()
                .and_then(|s| crate::disk::usage(s.journal_directory()))
                .unwrap_or(0.0),
        ),
        "GlobalMaxSize" => Json::from(po.paging().global_max_size()),
        "AddressMemoryUsage" => Json::from(po.paging().global_size()),
        "AddressMemoryUsagePercentage" => {
            let max = po.paging().global_max_size();
            Json::from(if max <= 0 {
                0
            } else {
                (po.paging().global_size() as f64 * 100.0 / max as f64) as i32
            })
        }
        "WildcardRoutingEnabled" => Json::from(po.wildcard().routing_enabled),
        "IDCacheSize" => Json::from(file.map_or(20000, |f| f.id_cache_size)),
        "PersistIDCache" => Json::from(file.is_none_or(|f| f.persist_id_cache)),
        "TransactionTimeout" => Json::from(file.map_or(300_000, |f| f.transaction_timeout)),
        "TransactionTimeoutScanPeriod" => {
            Json::from(file.map_or(1000, |f| f.transaction_timeout_scan_period))
        }
        "MessageExpiryScanPeriod" => {
            Json::from(file.map_or(30_000, |f| f.message_expiry_scan_period))
        }
        "MessageExpiryThreadPriority" => Json::from(3i64),
        "ConnectionTTLOverride" => Json::from(broker.connection_ttl_override()),
        "PersistDeliveryCountBeforeDelivery" => Json::from(false),
        "ThreadPoolMaxSize" => Json::from(30i32),
        "ScheduledThreadPoolMaxSize" => Json::from(5i32),
        "SecurityInvalidationInterval" => {
            Json::from(file.map_or(10_000, |f| f.security_invalidation_interval))
        }
        "MessageCounterEnabled" => Json::from(m.counters().enabled()),
        "MessageCounterSamplePeriod" => Json::from(m.counters().sample_period()),
        "MessageCounterMaxDayCount" => Json::from(m.counters().max_day_count()),
        "AsyncConnectionExecutionEnabled" => Json::from(true),
        "IncomingInterceptorClassNames"
        | "OutgoingInterceptorClassNames"
        | "InterceptorClassNames"
        | "BrokerPluginClassNames"
        | "BridgeNames"
        | "ClusterConnectionNames"
        | "ConnectorServices"
        | "Connectors" => Json::Array(Vec::new()),
        "ConnectorsAsJSON" => Json::from("[]"),
        "Acceptors" | "AcceptorsAsJSON" => {
            let acceptors: Vec<Json> = file
                .map(|f| f.acceptors.as_slice())
                .unwrap_or_default()
                .iter()
                .map(|a| {
                    Json::object()
                        .with("name", a.name.clone())
                        .with("factoryClassName", "org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory")
                        .with("params", Json::Object(a.params.iter().map(|(k, v)| (k.clone(), Json::from(v.clone()))).collect()))
                })
                .collect();
            if name == "AcceptorsAsJSON" {
                Json::from(Json::Array(acceptors).to_string())
            } else {
                Json::Array(acceptors)
            }
        }
        "PendingMirrorAcks" => Json::from(0i64),
        "EmbeddedWebServerStarted" => Json::from(false),
        _ => return Ok(None),
    }))
}

/// `getStatus`: the broker's state as JSON (`ServerStatus`).
fn status(m: &Management, broker: &Broker) -> Json {
    Json::object().with(
        "server",
        Json::object()
            .with("version", VERSION)
            .with("name", m.config.name.clone())
            .with("identity", Json::Null)
            .with("nodeId", broker.node_id())
            .with(
                "uptime",
                print_duration(crate::message::now_millis() - m.started_at()),
            )
            .with("state", "STARTED"),
    )
}

/// `AddressInfoTextFormatter.Long`.
fn address_text(info: &crate::postoffice::AddressInfo) -> String {
    format!(
        "Address [name={}, routingTypes={{{}}}, autoCreated={}]",
        info.name,
        super::address::routing_type_names(info.routing_types).join(","),
        info.auto_created
    )
}

/// `QueueTextFormatter.Long`.
fn queue_text(q: &crate::queue::Queue) -> String {
    let c = q.config();
    let mut s = format!(
        "Queue [name={}, address={}, routingType={}",
        c.name,
        c.address,
        c.routing_type.name()
    );
    if let Some(f) = q.filter_string() {
        s.push_str(&format!(", filter={f}"));
    }
    s.push_str(&format!(", durable={}", c.durable));
    if c.max_consumers != -1 {
        s.push_str(&format!(", maxConsumers={}", c.max_consumers));
    }
    s.push_str(&format!(
        ", purgeOnNoConsumers={}, autoCreateAddress={}, exclusive={}, lastValue={}, lastValueKey={}, nonDestructive={}, consumersBeforeDispatch={}, delayBeforeDispatch={}, autoCreateAddress={}]",
        c.purge_on_no_consumers,
        c.auto_created,
        c.exclusive,
        c.last_value || c.last_value_key.is_some(),
        c.last_value_key.as_deref().unwrap_or("null"),
        c.non_destructive,
        c.consumers_before_dispatch,
        c.delay_before_dispatch,
        c.auto_created
    ));
    s
}

/// `QueueConfiguration.toJSON` of a queue's configuration.
fn queue_config_json(q: &crate::queue::Queue) -> Json {
    let c = q.config();
    let mut o = Json::object()
        .with("id", q.id())
        .with("name", &*c.name)
        .with("address", &*c.address)
        .with("routing-type", c.routing_type.name());
    if let Some(f) = q.filter_string() {
        o.put("filter-string", f);
    }
    o.put("durable", c.durable);
    if let Some(u) = &c.user {
        o.put("user", u.clone());
    }
    o.put("max-consumers", c.max_consumers);
    o.put("exclusive", c.exclusive);
    o.put("group-rebalance", c.group_rebalance);
    o.put(
        "group-rebalance-pause-dispatch",
        c.group_rebalance_pause_dispatch,
    );
    o.put("group-buckets", c.group_buckets);
    if let Some(k) = &c.group_first_key {
        o.put("group-first-key", k.clone());
    }
    o.put("last-value", c.last_value);
    if let Some(k) = &c.last_value_key {
        o.put("last-value-key", k.clone());
    }
    o.put("non-destructive", c.non_destructive);
    o.put("purge-on-no-consumers", c.purge_on_no_consumers);
    o.put("enabled", c.enabled);
    o.put("consumers-before-dispatch", c.consumers_before_dispatch);
    o.put("delay-before-dispatch", c.delay_before_dispatch);
    o.put("auto-delete", c.auto_delete);
    o.put("auto-delete-delay", c.auto_delete_delay);
    o.put("auto-delete-message-count", c.auto_delete_message_count);
    o.put("ring-size", c.ring_size);
    o.put("configuration-managed", c.configuration_managed);
    o.put("temporary", c.temporary);
    o.put("internal", c.internal);
    o.put("auto-created", c.auto_created);
    o
}

fn routing_type(name: &str) -> Result<RoutingType, String> {
    match name.to_uppercase().as_str() {
        "MULTICAST" => Ok(RoutingType::Multicast),
        "ANYCAST" => Ok(RoutingType::Anycast),
        _ => Err(format!(
            "No enum constant org.apache.activemq.artemis.api.core.RoutingType.{name}"
        )),
    }
}

/// `RoutingType.valueOf` over a comma-separated list.
fn routing_types(list: Option<&str>) -> Result<RoutingTypes, String> {
    let mut types = RoutingTypes::default();
    for t in list.unwrap_or_default().split(',').map(str::trim) {
        if t.is_empty() {
            continue;
        }
        types.add(routing_type(t)?);
    }
    Ok(types)
}

/// `QueueConfiguration.fromJSON`: every member as text, `name` required.
fn queue_spec_from_json(text: &str) -> Result<QueueSpec, String> {
    let json = Json::parse(text)
        .map_err(|e| format!("AMQ229228: Failed to parse JSON queue configuration: {text}: {e}"))?;
    let Json::Object(members) = &json else {
        return Err(format!(
            "AMQ229228: Failed to parse JSON queue configuration: {text}"
        ));
    };
    let Some(name) = json.get("name").map(json_text) else {
        return Err(format!(
            "AMQ229228: Failed to parse JSON queue configuration: {text}"
        ));
    };
    let mut spec = QueueSpec::new(name.as_str(), name.as_str());
    for (k, v) in members {
        if v.is_null() {
            continue;
        }
        let s = json_text(v);
        let b = || s.eq_ignore_ascii_case("true");
        let n = || {
            s.parse::<i64>()
                .map_err(|_| format!("For input string: \"{s}\""))
        };
        match k.as_str() {
            "address" => spec.address = s.as_str().into(),
            "routing-type" => spec.routing_type = Some(routing_type(&s)?),
            "filter-string" => spec.filter_string = Some(s.clone()),
            "durable" => spec.durable = b(),
            "user" => spec.user = Some(s.clone()),
            "max-consumers" => spec.max_consumers = Some(n()? as i32),
            "exclusive" => spec.exclusive = Some(b()),
            "group-rebalance" => spec.group_rebalance = Some(b()),
            "group-rebalance-pause-dispatch" => spec.group_rebalance_pause_dispatch = Some(b()),
            "group-buckets" => spec.group_buckets = Some(n()? as i32),
            "group-first-key" => spec.group_first_key = Some(s.clone()),
            "last-value" => spec.last_value = Some(b()),
            "last-value-key" => spec.last_value_key = Some(s.clone()),
            "non-destructive" => spec.non_destructive = Some(b()),
            "purge-on-no-consumers" => spec.purge_on_no_consumers = Some(b()),
            "enabled" => spec.enabled = Some(b()),
            "consumers-before-dispatch" => spec.consumers_before_dispatch = Some(n()? as i32),
            "delay-before-dispatch" => spec.delay_before_dispatch = Some(n()?),
            "auto-delete" => spec.auto_delete = Some(b()),
            "auto-delete-delay" => spec.auto_delete_delay = Some(n()?),
            "auto-delete-message-count" => spec.auto_delete_message_count = Some(n()?),
            "ring-size" => spec.ring_size = Some(n()?),
            "configuration-managed" => spec.configuration_managed = b(),
            "temporary" => spec.temporary = b(),
            "auto-create-address" => spec.auto_create_address = Some(b()),
            "internal" => spec.internal = b(),
            "auto-created" => spec.auto_created = b(),
            _ => {}
        }
    }
    Ok(spec)
}

/// A JSON member as `fromJSON` reads it: a string's text, anything else
/// as written.
fn json_text(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// `DivertConfiguration.fromJSON`.
fn divert_from_json(text: &str) -> Result<DivertDeployment, String> {
    let json = Json::parse(text).map_err(|e| e.to_string())?;
    let get = |k: &str| json.get(k).filter(|v| !v.is_null()).map(json_text);
    let transformer = match json.get("transformer-configuration") {
        Some(t @ Json::Object(_)) => t.get("class-name").map(json_text).map(|class| {
            let props = match t.get("properties") {
                Some(Json::Object(p)) => p.iter().map(|(k, v)| (k.clone(), json_text(v))).collect(),
                _ => Default::default(),
            };
            (class, props)
        }),
        Some(Json::String(s)) => {
            let t = Json::parse(s).map_err(|e| e.to_string())?;
            t.get("class-name")
                .map(json_text)
                .map(|c| (c, Default::default()))
        }
        _ => None,
    };
    Ok(DivertDeployment {
        name: get("name").unwrap_or_default(),
        routing_name: get("routing-name"),
        address: get("address").unwrap_or_default(),
        forwarding_address: get("forwarding-address").unwrap_or_default(),
        exclusive: get("exclusive").is_some_and(|e| e.eq_ignore_ascii_case("true")),
        filter: get("filter-string"),
        routing_type: divert_routing_type(get("routing-type").as_deref())?,
        transformer,
    })
}

fn divert_routing_type(name: Option<&str>) -> Result<DivertRoutingType, String> {
    Ok(match name {
        None | Some("STRIP") => DivertRoutingType::Strip,
        Some("MULTICAST") => DivertRoutingType::Multicast,
        Some("ANYCAST") => DivertRoutingType::Anycast,
        Some("PASS") => DivertRoutingType::Pass,
        Some(other) => {
            return Err(format!(
                "No enum constant org.apache.activemq.artemis.core.server.ComponentConfigurationRoutingType.{other}"
            ));
        }
    })
}

/// `createQueue(... ringSize)`: the full form the shorter ones fill in.
#[allow(clippy::too_many_arguments)]
fn create_queue(
    broker: &Broker,
    address: Option<String>,
    routing: Option<String>,
    name: Option<String>,
    filter: Option<String>,
    durable: bool,
    fill: impl FnOnce(&mut QueueSpec, &crate::settings::AddressSettings),
) -> Outcome {
    let name = name.unwrap_or_default();
    let address = address.unwrap_or_else(|| name.clone());
    let settings = broker.post_office().settings(&address);
    let mut spec = QueueSpec::new(name.as_str(), address.as_str());
    spec.routing_type = Some(match routing {
        Some(r) => routing_type(&r)?,
        None => return Err("routingType".to_owned()),
    });
    spec.filter_string = filter.filter(|f| !f.trim().is_empty());
    spec.durable = durable;
    fill(&mut spec, &settings);
    let q = broker
        .post_office()
        .create_queue(spec, false)
        .map_err(|e| e.message)?;
    Ok(Json::from(queue_text(&q)))
}

/// The defaults the short `createQueue` forms take from the address
/// settings.
fn settings_defaults(
    spec: &mut QueueSpec,
    s: &crate::settings::AddressSettings,
    max: i32,
    purge: bool,
) {
    spec.max_consumers = Some(max);
    spec.purge_on_no_consumers = Some(purge);
    spec.exclusive.get_or_insert(s.default_exclusive_queue);
    spec.group_rebalance
        .get_or_insert(s.default_group_rebalance);
    spec.group_buckets.get_or_insert(s.default_group_buckets);
    if spec.group_first_key.is_none() {
        spec.group_first_key = s.default_group_first_key.clone();
    }
    spec.last_value.get_or_insert(s.default_last_value_queue);
    if spec.last_value_key.is_none() {
        spec.last_value_key = s.default_last_value_key.clone();
    }
    spec.non_destructive
        .get_or_insert(s.default_non_destructive);
    spec.consumers_before_dispatch
        .get_or_insert(s.default_consumers_before_dispatch);
    spec.delay_before_dispatch
        .get_or_insert(s.default_delay_before_dispatch);
    spec.auto_delete.get_or_insert(s.auto_delete_created_queues);
    spec.auto_delete_delay
        .get_or_insert(s.auto_delete_queues_delay);
    spec.auto_delete_message_count
        .get_or_insert(s.auto_delete_queues_message_count);
    spec.ring_size.get_or_insert(s.default_ring_size);
}

pub(super) async fn invoke(
    m: &Management,
    broker: &Arc<Broker>,
    call: &Call<'_>,
) -> Option<Outcome> {
    let po = broker.post_office();
    if let Some(outcome) = super::connections::invoke(broker, call).await {
        return Some(outcome);
    }
    if let Some(outcome) = super::transactions::invoke(broker, call).await {
        return Some(outcome);
    }
    if let Some(outcome) = super::security::invoke(m, broker, call).await {
        return Some(outcome);
    }

    // Message counters.
    let counters = m.counters();
    if call.is("enableMessageCounters", &[]).is_some() {
        counters.set_enabled(broker, true);
        return Some(Ok(Json::Null));
    }
    if call.is("disableMessageCounters", &[]).is_some() {
        counters.set_enabled(broker, false);
        return Some(Ok(Json::Null));
    }
    if call.is("resetAllMessageCounters", &[]).is_some() {
        counters.reset_all_counters(broker);
        return Some(Ok(Json::Null));
    }
    if call.is("resetAllMessageCounterHistories", &[]).is_some() {
        counters.reset_all_histories(broker);
        return Some(Ok(Json::Null));
    }
    if let Some(a) = call.is("setMessageCounterSamplePeriod", &[L]) {
        let period = a.l(0);
        if period < super::counters::MIN_SAMPLE_PERIOD {
            if period <= 0 {
                return Some(Err(format!(
                    "AMQ229047: invalid value: {period} sample period must be greater than 0"
                )));
            }
            tracing::warn!("AMQ224072: Message Counter Sample Period too short: {period}");
        }
        if period != counters.sample_period() {
            counters.reschedule(broker, period);
        }
        return Some(Ok(Json::Null));
    }
    if let Some(a) = call.is("setMessageCounterMaxDayCount", &[I]) {
        let count = a.i(0);
        if count <= 0 {
            return Some(Err(format!(
                "AMQ229046: invalid value: {count} count must be greater than 0"
            )));
        }
        counters.set_max_day_count(count);
        return Some(Ok(Json::Null));
    }

    // Addresses.
    if let Some(a) = call.is("createAddress", &[S, S]) {
        let name = a.s_or_empty(0);
        let types = match routing_types(a.s(1).as_deref()) {
            Ok(t) => t,
            Err(e) => return Some(Err(e)),
        };
        if po.address_info(&name).is_some() {
            return Some(Err(format!("AMQ229204: Address already exists: {name}")));
        }
        return Some(
            po.add_or_update_address(&name, types, false, false)
                .map(|info| Json::from(address_text(&info)))
                .map_err(|e| e.message),
        );
    }
    if let Some(a) = call.is("updateAddress", &[S, S]) {
        let name = a.s_or_empty(0);
        let Some(info) = po.address_info(&name) else {
            return Some(Err(format!("AMQ229203: Address Does Not Exist: {name}")));
        };
        let types = match a.s(1) {
            None => info.routing_types,
            Some(t) => match routing_types(Some(&t)) {
                Ok(t) => t,
                Err(e) => return Some(Err(e)),
            },
        };
        return Some(
            po.add_or_update_address(&name, types, info.auto_created, info.temporary)
                .map(|info| Json::from(address_text(&info)))
                .map_err(|e| e.message),
        );
    }
    if let Some(a) = call.is("deleteAddress", &[S]) {
        return Some(delete_address(broker, &a.s_or_empty(0), false).await);
    }
    if let Some(a) = call.is("deleteAddress", &[S, B]) {
        return Some(delete_address(broker, &a.s_or_empty(0), a.b(1)).await);
    }
    if let Some(a) = call.is("getAddressInfo", &[S]) {
        let name = a.s_or_empty(0);
        return Some(match po.address_info(&name) {
            Some(info) => Ok(Json::from(address_text(&info))),
            None => Err(format!("AMQ229203: Address Does Not Exist: {name}")),
        });
    }
    if let Some(a) = call.is("listBindingsForAddress", &[S]) {
        let name = a.s_or_empty(0);
        let text = po.address_status(&name).map_or(String::new(), |st| {
            st.queues
                .iter()
                .map(|q| format!("LocalQueueBinding [address={name}, queue={}]", q.name()))
                .chain(
                    st.diverts
                        .iter()
                        .map(|d| format!("DivertBinding [id={}]", d.name)),
                )
                .collect::<Vec<_>>()
                .join(",")
        });
        return Some(Ok(Json::from(text)));
    }
    if let Some(a) = call.is("listAddresses", &[S]) {
        let mut names: Vec<String> = po
            .addresses()
            .into_iter()
            .map(|i| i.name.to_string())
            .collect();
        names.sort_by_key(|n| n.to_lowercase());
        return Some(Ok(Json::from(names.join(&a.s_or_empty(0)))));
    }
    // Queues.
    if let Some(a) = call.is("createQueue", &[S, S]) {
        return Some(create_simple(broker, a.s(0), a.s(1), None, true, None));
    }
    if let Some(a) = call.is("createQueue", &[S, S, S]) {
        return Some(create_simple(broker, a.s(0), a.s(1), None, true, a.s(2)));
    }
    if let Some(a) = call.is("createQueue", &[S, S, B]) {
        return Some(create_simple(broker, a.s(0), a.s(1), None, a.b(2), None));
    }
    if let Some(a) = call.is("createQueue", &[S, S, B, S]) {
        return Some(create_simple(broker, a.s(0), a.s(1), None, a.b(2), a.s(3)));
    }
    if let Some(a) = call.is("createQueue", &[S, S, S, B]) {
        return Some(create_simple(broker, a.s(0), a.s(1), a.s(2), a.b(3), None));
    }
    if let Some(a) = call.is("createQueue", &[S, S, S, B, S]) {
        return Some(create_simple(
            broker,
            a.s(0),
            a.s(1),
            a.s(2),
            a.b(3),
            a.s(4),
        ));
    }
    if let Some(a) = call.is("createQueue", &[S, S, S, S, B, I, B, B]) {
        return Some(create_queue(
            broker,
            a.s(0),
            a.s(1),
            a.s(2),
            a.s(3),
            a.b(4),
            |spec, s| {
                spec.auto_create_address = Some(a.b(7));
                settings_defaults(spec, s, a.i(5), a.b(6));
            },
        ));
    }
    if let Some(a) = call.is(
        "createQueue",
        &[S, S, S, S, B, I, B, B, B, I, B, S, B, I, L, B],
    ) {
        return Some(create_queue(
            broker,
            a.s(0),
            a.s(1),
            a.s(2),
            a.s(3),
            a.b(4),
            |spec, s| {
                spec.exclusive = Some(a.b(7));
                spec.group_rebalance = Some(a.b(8));
                spec.group_buckets = Some(a.i(9));
                spec.last_value = Some(a.b(10));
                spec.last_value_key = a.s(11);
                spec.non_destructive = Some(a.b(12));
                spec.consumers_before_dispatch = Some(a.i(13));
                spec.delay_before_dispatch = Some(a.l(14));
                spec.auto_create_address = Some(a.b(15));
                settings_defaults(spec, s, a.i(5), a.b(6));
            },
        ));
    }
    if let Some(a) = call.is(
        "createQueue",
        &[S, S, S, S, B, I, B, B, B, I, B, S, B, I, L, B, L, L, B],
    ) {
        return Some(create_queue(
            broker,
            a.s(0),
            a.s(1),
            a.s(2),
            a.s(3),
            a.b(4),
            |spec, s| {
                spec.exclusive = Some(a.b(7));
                spec.group_rebalance = Some(a.b(8));
                spec.group_buckets = Some(a.i(9));
                spec.last_value = Some(a.b(10));
                spec.last_value_key = a.s(11);
                spec.non_destructive = Some(a.b(12));
                spec.consumers_before_dispatch = Some(a.i(13));
                spec.delay_before_dispatch = Some(a.l(14));
                spec.auto_delete = Some(a.b(15));
                spec.auto_delete_delay = Some(a.l(16));
                spec.auto_delete_message_count = Some(a.l(17));
                spec.auto_create_address = Some(a.b(18));
                settings_defaults(spec, s, a.i(5), a.b(6));
            },
        ));
    }
    for long in [false, true] {
        let mut types = vec![S, S, S, S, B, I, B, B, B, I, S, B, S, B, I, L, B, L, L, B];
        if long {
            types.push(L);
        }
        if let Some(a) = call.is("createQueue", &types) {
            return Some(create_queue(
                broker,
                a.s(0),
                a.s(1),
                a.s(2),
                a.s(3),
                a.b(4),
                |spec, s| {
                    spec.exclusive = Some(a.b(7));
                    spec.group_rebalance = Some(a.b(8));
                    spec.group_buckets = Some(a.i(9));
                    spec.group_first_key = a.s(10);
                    spec.last_value = Some(a.b(11));
                    spec.last_value_key = a.s(12);
                    spec.non_destructive = Some(a.b(13));
                    spec.consumers_before_dispatch = Some(a.i(14));
                    spec.delay_before_dispatch = Some(a.l(15));
                    spec.auto_delete = Some(a.b(16));
                    spec.auto_delete_delay = Some(a.l(17));
                    spec.auto_delete_message_count = Some(a.l(18));
                    spec.auto_create_address = Some(a.b(19));
                    if long {
                        spec.ring_size = Some(a.l(20));
                    }
                    settings_defaults(spec, s, a.i(5), a.b(6));
                },
            ));
        }
    }
    if let Some(a) = call.is("createQueue", &[S]) {
        return Some(create_from_json(broker, &a.s_or_empty(0), false));
    }
    if let Some(a) = call.is("createQueue", &[S, B]) {
        return Some(create_from_json(broker, &a.s_or_empty(0), a.b(1)));
    }
    if let Some(a) = call.is("deployQueue", &[S, S, S]) {
        return Some(deploy_queue(broker, a.s(0), a.s(1), a.s(2), true));
    }
    if let Some(a) = call.is("deployQueue", &[S, S, S, B]) {
        return Some(deploy_queue(broker, a.s(0), a.s(1), a.s(2), a.b(3)));
    }
    if let Some(a) = call.is("updateQueue", &[S]) {
        let spec = match queue_spec_from_json(&a.s_or_empty(0)) {
            Ok(s) => s,
            Err(e) => return Some(Err(e)),
        };
        let name = spec.name.clone();
        return Some(
            match update_queue_with(broker, &name, |s| merge_spec(s, &spec)) {
                Ok(q) => Ok(Json::from(queue_config_json(&q).to_string())),
                Err(e) => Err(e),
            },
        );
    }
    if let Some(a) = call.is("updateQueue", &[S, S, I, B]) {
        return Some(update_queue(
            broker,
            &a,
            &[1, -1, 2, 3, -1, -1, -1, -1, -1, -1, -1, -1, -1],
        ));
    }
    if let Some(a) = call.is("updateQueue", &[S, S, I, B, B]) {
        return Some(update_queue(
            broker,
            &a,
            &[1, -1, 2, 3, 4, -1, -1, -1, -1, -1, -1, -1, -1],
        ));
    }
    if let Some(a) = call.is("updateQueue", &[S, S, I, B, B, S]) {
        return Some(update_queue(
            broker,
            &a,
            &[1, -1, 2, 3, 4, -1, -1, -1, -1, -1, -1, 5, -1],
        ));
    }
    if let Some(a) = call.is("updateQueue", &[S, S, S, I, B, B, B, I, B, I, L, S]) {
        return Some(update_queue(
            broker,
            &a,
            &[1, 2, 3, 4, 5, 6, 7, -1, 8, 9, 10, 11, -1],
        ));
    }
    if let Some(a) = call.is("updateQueue", &[S, S, S, I, B, B, B, I, S, B, I, L, S]) {
        return Some(update_queue(
            broker,
            &a,
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, -1],
        ));
    }
    if let Some(a) = call.is("updateQueue", &[S, S, S, I, B, B, B, I, S, B, I, L, S, L]) {
        return Some(update_queue(
            broker,
            &a,
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        ));
    }
    if let Some(a) = call.is("destroyQueue", &[S]) {
        return Some(destroy_queue(broker, &a.s_or_empty(0), false, false).await);
    }
    if let Some(a) = call.is("destroyQueue", &[S, B]) {
        return Some(destroy_queue(broker, &a.s_or_empty(0), a.b(1), false).await);
    }
    if let Some(a) = call.is("destroyQueue", &[S, B, B]) {
        return Some(destroy_queue(broker, &a.s_or_empty(0), a.b(1), a.b(2)).await);
    }
    if let Some(a) = call.is("getQueueNames", &[S]) {
        let t = a.s(0).filter(|t| !t.is_empty());
        let names: Vec<String> = po
            .queues()
            .iter()
            .filter(|q| {
                t.as_deref()
                    .is_none_or(|t| q.config().routing_type.name() == t)
            })
            .map(|q| q.name().to_string())
            .collect();
        return Some(Ok(Json::from(names)));
    }
    // The members of the cluster connections' topologies: none until
    // clustering.
    if call.is("listNetworkTopology", &[]).is_some() {
        return Some(Ok(Json::from("[]")));
    }
    if let Some(a) = call.is("listQueues", &[S, I, I]) {
        return Some(super::views::list_queues(broker, a.s(0).as_deref(), a.i(1), a.i(2)).await);
    }
    if let Some(a) = call.is("listAddresses", &[S, I, I]) {
        return Some(super::views::list_addresses(broker, a.s(0).as_deref(), a.i(1), a.i(2)).await);
    }
    // Diverts.
    if call.is("listDivertNames", &[]).is_some() {
        return Some(Ok(Json::from(
            po.diverts()
                .iter()
                .map(|d| d.name.to_string())
                .collect::<Vec<_>>(),
        )));
    }
    if let Some(a) = call.is("createDivert", &[S, S, S, S, B, S, S]) {
        return Some(create_divert(broker, &a, None, Some("STRIP".to_owned())));
    }
    if let Some(a) = call.is("createDivert", &[S, S, S, S, B, S, S, S]) {
        return Some(create_divert(broker, &a, None, a.s(7)));
    }
    if let Some(a) = call.is("createDivert", &[S, S, S, S, B, S, S, S, S]) {
        let props = a
            .s(7)
            .map(|t| match Json::parse(&t) {
                Ok(Json::Object(m)) => m.into_iter().map(|(k, v)| (k, json_text(&v))).collect(),
                _ => Vec::new(),
            })
            .unwrap_or_default();
        return Some(create_divert(broker, &a, Some(props), a.s(8)));
    }
    if let Some(a) = call.is("createDivert", &[S, S, S, S, B, S, S, M, S]) {
        return Some(create_divert(broker, &a, a.string_map(7), a.s(8)));
    }
    if let Some(a) = call.is("createDivert", &[S]) {
        return Some(match divert_from_json(&a.s_or_empty(0)) {
            Ok(d) => deploy_divert(broker, &d),
            Err(e) => Err(e),
        });
    }
    if let Some(a) = call.is("updateDivert", &[S]) {
        return Some(match divert_from_json(&a.s_or_empty(0)) {
            Ok(d) => update_divert(broker, d),
            Err(e) => Err(e),
        });
    }
    if let Some(a) = call.is("updateDivert", &[S, S, S, S, M, S]) {
        let routing = match divert_routing_type(a.s(5).as_deref()) {
            Ok(t) => t,
            Err(e) => return Some(Err(e)),
        };
        let d = DivertDeployment {
            name: a.s_or_empty(0),
            routing_name: None,
            address: String::new(),
            forwarding_address: a.s_or_empty(1),
            exclusive: false,
            filter: a.s(2),
            routing_type: routing,
            transformer: a
                .s(3)
                .filter(|c| !c.is_empty())
                .map(|c| (c, a.string_map(4).unwrap_or_default().into_iter().collect())),
        };
        return Some(update_divert(broker, d));
    }
    if let Some(a) = call.is("destroyDivert", &[S]) {
        let name = a.s_or_empty(0);
        return Some(if po.destroy_divert(&name) {
            Ok(Json::Null)
        } else {
            Err(format!("AMQ229012: No binding for divert {name}"))
        });
    }
    // Address settings.
    if let Some(a) = call.is("getAddressSettingsAsJSON", &[S]) {
        return Some(Ok(Json::from(
            settings_json(po.settings_configured(&a.s_or_empty(0))).to_string(),
        )));
    }
    if let Some(a) = call.is("addAddressSettings", &[S, S]) {
        let text = a.s_or_empty(1);
        return Some(add_address_settings(broker, &a.s_or_empty(0), &text));
    }
    if let Some(a) = call.is("removeAddressSettings", &[S]) {
        return Some(
            po.remove_address_settings(&a.s_or_empty(0))
                .map(|()| Json::Null)
                .map_err(|e| e.message),
        );
    }
    // Configuration.
    if call.is("reloadConfigurationFile", &[]).is_some() {
        return Some(
            broker
                .reload_configuration_file()
                .await
                .map(|()| Json::Null),
        );
    }
    // Journal retention.
    if let Some(a) = call.is("replay", &[S, S, S]) {
        return Some(
            crate::replay::replay(broker, None, None, a.s(0), a.s(1), a.s(2))
                .await
                .map(|()| Json::Null),
        );
    }
    if let Some(a) = call.is("replay", &[S, S, S, S, S]) {
        // `ReplayManager.newRetentionSimpleDateFormat().parse`.
        let date = |s: Option<String>| -> Result<i64, String> {
            let s = s.ok_or_else(|| {
                "Cannot invoke \"String.length()\" because \"text\" is null".to_owned()
            })?;
            saranga_journal::local_date_millis(&s, crate::localtime::offset_millis)
                .ok_or_else(|| format!("Unparseable date: \"{s}\""))
        };
        let dates = date(a.s(0)).and_then(|start| Ok((start, date(a.s(1))?)));
        return Some(match dates {
            Ok((start, end)) => {
                crate::replay::replay(broker, Some(start), Some(end), a.s(2), a.s(3), a.s(4))
                    .await
                    .map(|()| Json::Null)
            }
            Err(e) => Err(e),
        });
    }
    if call.is("getStatus", &[]).is_some() {
        return Some(Ok(Json::from(status(m, broker).to_string())));
    }
    if call.is("getUptime", &[]).is_some() {
        return Some(Ok(Json::from(print_duration(
            crate::message::now_millis() - m.started_at(),
        ))));
    }
    None
}

fn create_simple(
    broker: &Broker,
    address: Option<String>,
    name: Option<String>,
    filter: Option<String>,
    durable: bool,
    routing: Option<String>,
) -> Outcome {
    let key = address.clone().or_else(|| name.clone()).unwrap_or_default();
    let s = broker.post_office().settings(&key);
    let routing = routing.or_else(|| Some(s.default_queue_routing_type.name().to_owned()));
    let (max, purge, auto) = (
        s.default_max_consumers,
        s.default_purge_on_no_consumers,
        s.auto_create_addresses,
    );
    create_queue(
        broker,
        address,
        routing,
        name,
        filter,
        durable,
        |spec, s| {
            spec.auto_create_address = Some(auto);
            settings_defaults(spec, s, max, purge);
        },
    )
    .map(|_| Json::Null)
}

fn create_from_json(broker: &Broker, text: &str, ignore_if_exists: bool) -> Outcome {
    let spec = queue_spec_from_json(text)?;
    let q = broker
        .post_office()
        .create_queue(spec, ignore_if_exists)
        .map_err(|e| e.message)?;
    Ok(Json::from(queue_config_json(&q).to_string()))
}

fn deploy_queue(
    broker: &Broker,
    address: Option<String>,
    name: Option<String>,
    filter: Option<String>,
    durable: bool,
) -> Outcome {
    let name = name.unwrap_or_default();
    let mut spec = QueueSpec::new(name.as_str(), address.unwrap_or_default().as_str());
    spec.filter_string = filter;
    spec.durable = durable;
    broker
        .post_office()
        .create_queue(spec, false)
        .map(|_| Json::Null)
        .map_err(|e| e.message)
}

/// `updateQueue(QueueConfiguration)`: what the configuration sets
/// replaces the queue's.
fn merge_spec(spec: &mut QueueSpec, from: &QueueSpec) {
    macro_rules! take {
        ($($f:ident),*) => {$(if from.$f.is_some() { spec.$f = from.$f.clone(); })*};
    }
    take!(
        routing_type,
        filter_string,
        max_consumers,
        purge_on_no_consumers,
        exclusive,
        group_rebalance,
        group_rebalance_pause_dispatch,
        group_buckets,
        group_first_key,
        non_destructive,
        consumers_before_dispatch,
        delay_before_dispatch,
        ring_size,
        enabled,
        user
    );
}

/// The queue's current settings as a spec, changed by `change`, then
/// applied (`PostOfficeImpl.updateQueue`).
fn update_queue_with(
    broker: &Broker,
    name: &str,
    change: impl FnOnce(&mut QueueSpec),
) -> Result<crate::queue::Queue, String> {
    let po = broker.post_office();
    let Some(q) = po.queue(name) else {
        return Err(format!("AMQ229017: Queue {name} does not exist"));
    };
    let mut spec = super::queue::spec_of(&q);
    change(&mut spec);
    match po.update_queue(spec) {
        Ok(Some(q)) => Ok(q),
        Ok(None) => Err(format!("AMQ229017: Queue {name} does not exist")),
        Err(e) => Err(e.message),
    }
}

/// The positional `updateQueue` forms. `at` gives, for routing type,
/// filter, max consumers, purge on no consumers, exclusive, group
/// rebalance, group buckets, group first key, non-destructive, consumers
/// before dispatch, delay before dispatch, user and ring size, the
/// parameter that holds it (-1: not given).
fn update_queue(broker: &Broker, a: &Args, at: &[i32; 13]) -> Outcome {
    let name = a.s_or_empty(0);
    let get = |i: usize| (at[i] >= 0).then_some(at[i] as usize);
    let routing = match get(0).and_then(|i| a.s(i)) {
        Some(r) => Some(routing_type(&r)?),
        None => None,
    };
    let q = update_queue_with(broker, &name, |spec| {
        if let Some(r) = routing {
            spec.routing_type = Some(r);
        }
        if let Some(f) = get(1).and_then(|i| a.s(i)) {
            spec.filter_string = Some(f);
        }
        if let Some(v) = get(2).and_then(|i| a.oi(i)) {
            spec.max_consumers = Some(v);
        }
        if let Some(v) = get(3).and_then(|i| a.ob(i)) {
            spec.purge_on_no_consumers = Some(v);
        }
        if let Some(v) = get(4).and_then(|i| a.ob(i)) {
            spec.exclusive = Some(v);
        }
        if let Some(v) = get(5).and_then(|i| a.ob(i)) {
            spec.group_rebalance = Some(v);
        }
        if let Some(v) = get(6).and_then(|i| a.oi(i)) {
            spec.group_buckets = Some(v);
        }
        if let Some(v) = get(7).and_then(|i| a.s(i)) {
            spec.group_first_key = Some(v);
        }
        if let Some(v) = get(8).and_then(|i| a.ob(i)) {
            spec.non_destructive = Some(v);
        }
        if let Some(v) = get(9).and_then(|i| a.oi(i)) {
            spec.consumers_before_dispatch = Some(v);
        }
        if let Some(v) = get(10).and_then(|i| a.ol(i)) {
            spec.delay_before_dispatch = Some(v);
        }
        if let Some(v) = get(11).and_then(|i| a.s(i)) {
            spec.user = Some(v);
        }
        if let Some(v) = get(12).and_then(|i| a.ol(i)) {
            spec.ring_size = Some(v);
        }
    })?;
    Ok(Json::from(queue_text(&q)))
}

/// `destroyQueue(name, removeConsumers, forceAutoDeleteAddress)`.
async fn destroy_queue(
    broker: &Arc<Broker>,
    name: &str,
    remove_consumers: bool,
    auto_delete_address: bool,
) -> Outcome {
    let po = broker.post_office();
    let Some(q) = po.queue(name) else {
        return Err(format!("AMQ229017: Queue {name} does not exist"));
    };
    if !remove_consumers && q.consumer_count() > 0 {
        return Err(crate::BrokerError::cannot_delete_queue_with_consumers(
            name,
            q.consumer_count(),
        )
        .message);
    }
    if remove_consumers {
        broker.disconnect_consumers(&q).await;
    }
    // checkConsumerCount is !removeConsumers, as in Artemis.
    po.delete_queue(name, !remove_consumers, auto_delete_address)
        .map(|()| Json::Null)
        .map_err(|e| e.message)
}

/// `removeAddressInfo(name, null, force)`.
async fn delete_address(broker: &Arc<Broker>, name: &str, force: bool) -> Outcome {
    let po = broker.post_office();
    let Some(st) = po.address_status(name) else {
        return Err(format!("AMQ229203: Address Does Not Exist: {name}"));
    };
    if !force && (!st.queues.is_empty() || !st.diverts.is_empty()) {
        return Err(format!("AMQ229205: Address {name} has bindings"));
    }
    for q in st.queues {
        broker.disconnect_consumers(&q).await;
        let _ = po.delete_queue(q.name(), false, false);
    }
    if !po.remove_address(name) {
        return Err(format!("AMQ229205: Address {name} has bindings"));
    }
    Ok(Json::Null)
}

fn create_divert(
    broker: &Broker,
    a: &Args,
    properties: Option<Vec<(String, String)>>,
    routing: Option<String>,
) -> Outcome {
    let routing = divert_routing_type(routing.as_deref())?;
    let d = DivertDeployment {
        name: a.s_or_empty(0),
        routing_name: a.s(1),
        address: a.s_or_empty(2),
        forwarding_address: a.s_or_empty(3),
        exclusive: a.b(4),
        filter: a.s(5),
        routing_type: routing,
        transformer: a
            .s(6)
            .filter(|c| !c.is_empty())
            .map(|c| (c, properties.unwrap_or_default().into_iter().collect())),
    };
    deploy_divert(broker, &d)
}

fn deploy_divert(broker: &Broker, d: &DivertDeployment) -> Outcome {
    broker
        .post_office()
        .deploy_divert_persisted(d, broker.transformers())
        .map(|_| Json::Null)
        .map_err(|e| e.message)
}

fn update_divert(broker: &Broker, d: DivertDeployment) -> Outcome {
    match broker
        .post_office()
        .update_divert(d.clone(), broker.transformers())
    {
        Ok(true) => Ok(Json::Null),
        Ok(false) => Err(format!("AMQ229231: Divert Does Not Exist: {}", d.name)),
        Err(e) => Err(e.message),
    }
}

/// `AddressSettings.toJSON`: the settings that are set.
pub(super) fn settings_json(s: Option<saranga_config::AddressSettings>) -> Json {
    use saranga_config::SettingValue as V;
    let mut o = Json::object();
    for (name, v) in s.unwrap_or_default().named_values() {
        o.put(
            &name,
            match v {
                V::Text(t) => Json::from(t),
                V::Long(n) => Json::from(n),
                V::Double(d) => Json::from(d),
                V::Bool(b) => Json::from(b),
            },
        );
    }
    o
}

/// `addAddressSettings(address, json)`.
fn add_address_settings(broker: &Broker, address: &str, text: &str) -> Outcome {
    let parse_error = || format!("AMQ229228: Failed to parse JSON queue configuration: {text}");
    let json = Json::parse(text).map_err(|_| parse_error())?;
    let Json::Object(members) = json else {
        return Err(parse_error());
    };
    let values: Vec<(String, String)> = members
        .iter()
        .filter(|(_, v)| !v.is_null())
        .map(|(k, v)| (k.clone(), json_text(v)))
        .collect();
    let settings =
        saranga_config::AddressSettings::from_named_values(&values).map_err(|e| e.to_string())?;
    let max = settings.max_size_bytes.unwrap_or(-1);
    let page = i64::from(settings.page_size_bytes.unwrap_or(10 * 1024 * 1024));
    if page > max && max > 0 {
        return Err(format!(
            "pageSize has to be lower than maxSizeBytes. Invalid argument ({page} < {max})"
        ));
    }
    if max < -1 {
        return Err("Invalid argument on maxSizeBytes".to_owned());
    }
    let json = settings_json(Some(settings.clone()));
    broker
        .post_office()
        .add_address_settings(address, settings, text)
        .map_err(|e| e.message)?;
    Ok(Json::from(json.to_string()))
}
