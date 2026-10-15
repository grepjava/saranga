//! What the broker writes to the audit log, as Artemis's `AuditLogger`
//! calls go: management methods (from the generated catalog of what each
//! `*ControlImpl` method logs), sessions, consumers, queues and addresses
//! made by clients, and messages sent, consumed and acknowledged. The
//! loggers and the caller's format are in `saranga_security::audit`.
//!
//! Sources and messages are shown as their Java classes' `toString`
//! shows them, with Saranga's values; Java's identity hash codes are
//! stand-ins that stay the same for an object.

mod catalog;

use std::fmt::Write as _;

use saranga_security::audit as log;
use saranga_security::jaas::Subject;

pub use saranga_security::audit::{base_enabled, message_enabled, resource_enabled};

use crate::Broker;
use crate::management::json::{Json, java_double};
use crate::message::{MessageRef, ServerMessage};
use crate::transaction::{Transaction, TxState};

/// The management controls whose methods are audited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Control {
    Broker,
    Address,
    Queue,
    Divert,
    Acceptor,
    BroadcastGroup,
    Bridge,
    BrokerConnection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Logger {
    Base,
    Resource,
    Message,
}

/// A value an audited method shows among its parameters.
#[derive(Debug)]
pub(crate) enum Arg {
    /// The method's parameter at this index.
    Param(usize),
    Text(&'static str),
    Null,
    /// A value the method computes: a constant, or an address setting's
    /// default (named by the Java expression).
    Computed(&'static str),
}

/// What a management method logs.
#[derive(Debug)]
pub(crate) struct Entry {
    pub control: Control,
    pub method: &'static str,
    /// The parameters' Java types and names.
    pub types: &'static [&'static str],
    pub names: &'static [&'static str],
    pub logger: Logger,
    pub id: u32,
    pub text: &'static str,
    /// What `parametersList` shows; `None` when the message has no
    /// parameters.
    pub params: Option<&'static [Arg]>,
    /// The parameters are a list (`Object...`), shown in brackets.
    pub list: bool,
    /// The caller is given as no subject: `anonymous`.
    pub anonymous: bool,
}

/// Who makes a call: the subject it authenticated as (none without
/// security) and its remote address.
#[derive(Debug, Clone, Copy, Default)]
pub struct Who<'a> {
    pub subject: Option<&'a Subject>,
    pub remote: Option<&'a str>,
}

impl Who<'_> {
    /// `getCaller`.
    pub fn caller(&self) -> String {
        log::caller(self.subject, self.remote)
    }
}

fn enabled(logger: Logger) -> bool {
    match logger {
        Logger::Base => log::base_enabled(),
        Logger::Resource => log::resource_enabled(),
        Logger::Message => log::message_enabled(),
    }
}

fn emit(logger: Logger, line: String) {
    match logger {
        Logger::Base => log::base(line),
        Logger::Resource => log::resource(line),
        Logger::Message => log::message(line),
    }
}

/// An audit message by its `AuditLogger` method's name, with its code.
fn text(name: &str, args: &[&str]) -> String {
    match catalog::MESSAGES.iter().find(|(n, _, _)| *n == name) {
        Some((_, id, t)) => format!("AMQ{id}: {}", log::fill(t, args)),
        None => format!("{name}: {}", args.join(", ")),
    }
}

/// Logs the message an `AuditLogger` method logs, if its logger is on.
fn audit(logger: Logger, name: &str, args: &[&str]) {
    if enabled(logger) {
        emit(logger, text(name, args));
    }
}

/// Who the current task acts for, as Artemis's `AuditLogger` thread locals
/// hold it while a connection's packets are handled: a Core connection
/// (its subject and address), or only an address (the other protocols'
/// handlers set no subject).
#[derive(Clone)]
pub enum Context {
    Connection(std::sync::Arc<crate::session::ClientConnection>),
    Remote(String),
}

tokio::task_local! {
    static CONTEXT: Context;
}

/// Runs `f` acting for `context`.
pub async fn scoped<F: std::future::Future>(context: Context, f: F) -> F::Output {
    CONTEXT.scope(context, f).await
}

/// `getCaller()` with the task's context: outside any connection, the
/// MBean server's `internal`.
fn current_caller() -> String {
    CONTEXT
        .try_with(|c| match c {
            Context::Connection(conn) => {
                log::caller(conn.subject().as_deref(), Some(conn.remote_address()))
            }
            Context::Remote(r) => log::caller(None, Some(r)),
        })
        .unwrap_or_else(|_| log::caller(None, Some("internal")))
}

/// `ManagementServiceImpl.registerInJMX`: registering a control asks it
/// for its MBean info, audited for whoever the task acts for.
pub(crate) fn registered(class: &str, key: &str) {
    if log::base_enabled() {
        let source = control_string(class, key);
        audit(Logger::Base, "getMBeanInfo", &[&current_caller(), &source]);
    }
}

/// A control's `Object.toString`.
fn control_string(class: &str, key: &str) -> String {
    format!(
        "org.apache.activemq.artemis.core.management.impl.{class}@{:x}",
        identity(("control", class, key))
    )
}

/// `AbstractControl.getMBeanInfo`, as the MBean server's guard and Jolokia
/// call it: `class` is the control's (`QueueControlImpl`), and the source
/// is the control's `Object.toString`.
pub fn mbean_info(class: &str, key: &str, who: Who<'_>) {
    if log::base_enabled() {
        let source = control_string(class, key);
        audit(Logger::Base, "getMBeanInfo", &[&who.caller(), &source]);
    }
}

/// `ManagementServiceImpl.stop`: each queue control is unregistered by its
/// name, address and routing type, and each divert control by its unique
/// name and address, the getters audited as the stopping thread calls
/// them (`anonymous@internal`), in the order of the registries' maps.
pub(crate) fn management_stopped(broker: &Broker) {
    if !log::base_enabled() {
        return;
    }
    let who = || {
        Some(Who {
            subject: None,
            remote: Some("internal"),
        })
    };
    let mut queues = broker.post_office().queues();
    queues.sort_by_key(|q| q.id());
    let names = queues.iter().map(|q| q.name().to_string()).collect();
    for name in concurrent_hash_map_order(names) {
        for getter in ["getName", "getAddress", "getRoutingType"] {
            management_call(broker, Control::Queue, &name, getter, &[], who());
        }
    }
    let names = broker
        .post_office()
        .diverts()
        .iter()
        .map(|d| d.name.to_string())
        .collect();
    for name in concurrent_hash_map_order(names) {
        for getter in ["getUniqueName", "getAddress"] {
            management_call(broker, Control::Divert, &name, getter, &[], who());
        }
    }
}

/// The iteration order of a Java `ConcurrentHashMap` with these string
/// keys, put in this order: by bucket of the spread hash, in a table that
/// grows from 16 as keys are added, then by insertion within a bucket.
fn concurrent_hash_map_order(keys: Vec<String>) -> Vec<String> {
    let mut n = 16usize;
    while keys.len() >= n - (n >> 2) {
        n *= 2;
    }
    let bucket = |s: &str| {
        let h = s
            .encode_utf16()
            .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(i32::from(c)));
        ((h ^ ((h as u32) >> 16) as i32) & 0x7fff_ffff) as usize & (n - 1)
    };
    let mut indexed: Vec<(usize, usize, String)> = keys
        .into_iter()
        .enumerate()
        .map(|(i, k)| (bucket(&k), i, k))
        .collect();
    indexed.sort();
    indexed.into_iter().map(|(_, _, k)| k).collect()
}

// ---------------------------------------------------------------------------
// Java's toString of the objects audit messages show

/// A stand-in for `System.identityHashCode`, the same for the same key.
pub(crate) fn identity(key: impl std::hash::Hash) -> u32 {
    use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};
    let h = BuildHasherDefault::<DefaultHasher>::default().hash_one(key);
    (h as u32) & 0x7fff_ffff
}

/// `ActiveMQServerImpl.toString`.
pub(crate) fn server_string(broker: &Broker) -> String {
    format!(
        "ActiveMQServerImpl::name={}",
        broker.management().config().name
    )
}

/// `QueueImpl.toString`.
fn queue_string(broker: &Broker, name: &str) -> String {
    match broker.post_office().queue(name) {
        Some(q) => queue_display(broker, &q),
        None => "null".to_owned(),
    }
}

/// `QueueImpl.toString` of a queue, which may be gone from the broker.
pub(crate) fn queue_display(broker: &Broker, q: &crate::queue::Queue) -> String {
    format!(
        "QueueImpl[name={}, postOffice=PostOfficeImpl [server={}], temp={}]@{:x}",
        q.name(),
        server_string(broker),
        q.config().temporary,
        identity(("queue", q.id()))
    )
}

/// `AddressInfo.toString`.
fn address_string(broker: &Broker, name: &str) -> String {
    let Some(st) = broker.post_office().address_status(name) else {
        return "null".to_owned();
    };
    let info = &st.info;
    let mut types = Vec::new();
    // EnumSet order: MULTICAST, then ANYCAST.
    if info.routing_types.contains(crate::RoutingType::Multicast) {
        types.push("MULTICAST");
    }
    if info.routing_types.contains(crate::RoutingType::Anycast) {
        types.push("ANYCAST");
    }
    format!(
        "Address [name={}, id={}, routingTypes={{{}}}, autoCreated={}, paused={}, bindingRemovedTimestamp=-1, swept=false, createdTimestamp={}, temporary={}]",
        info.name,
        info.id,
        types.join(","),
        info.auto_created,
        st.paused,
        st.created_at,
        info.temporary
    )
}

/// `DivertImpl.toString`.
fn divert_string(broker: &Broker, name: &str) -> String {
    let Some(d) = broker.post_office().divert(name) else {
        return "null".to_owned();
    };
    let filter = d.filter.as_ref().map_or_else(
        || "null".to_owned(),
        |f| format!("FilterImpl [sfilterString={}]", f.text()),
    );
    let transformer = match &d.deployment.transformer {
        Some((class, _)) => format!("{class}@{:x}", identity(("transformer", name))),
        None => "null".to_owned(),
    };
    format!(
        "DivertImpl [routingName={}, uniqueName={}, forwardAddress={}, exclusive={}, filter={filter}, transformer={transformer}]",
        d.routing_name, d.name, d.deployment.forwarding_address, d.exclusive
    )
}

/// The source a control's audit messages show.
fn source(broker: &Broker, control: Control, name: &str) -> String {
    match control {
        Control::Broker => server_string(broker),
        Control::Queue => queue_string(broker, name),
        Control::Address => address_string(broker, name),
        Control::Divert => divert_string(broker, name),
        // NettyAcceptor has no toString: Object's.
        Control::Acceptor => format!(
            "org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptor@{:x}",
            identity(("acceptor", name))
        ),
        // BroadcastGroupImpl has no toString either.
        Control::BroadcastGroup => format!(
            "org.apache.activemq.artemis.core.server.cluster.impl.BroadcastGroupImpl@{:x}",
            identity(("broadcast-group", name))
        ),
        Control::Bridge => broker
            .bridges()
            .get(name)
            .map_or_else(|| "null".to_owned(), |b| b.describe()),
        // AMQPBrokerConnection has no toString either.
        Control::BrokerConnection => format!(
            "org.apache.activemq.artemis.protocol.amqp.connect.AMQPBrokerConnection@{:x}",
            identity(("broker-connection", name))
        ),
    }
}

/// `CoreMessage.toString` (an AMQP or OpenWire message as converted to
/// Core).
pub(crate) fn message_string(m: &ServerMessage) -> String {
    let core = m.core();
    let size = m.encode_size();
    let s = saranga_storage::describe::core_message_to_string(core, size, &[]);
    // The identity hash: per message, not per encoding.
    match s.rfind("]@") {
        Some(i) => format!("{}]@{}", &s[..i], identity(("message", core.message_id))),
        None => s,
    }
}

/// `MessageReferenceImpl.toString`.
pub(crate) fn reference_string(r: &MessageRef) -> String {
    format!(
        "Reference[{}]:{}:{}",
        r.message_id(),
        if r.message.core().durable {
            "RELIABLE"
        } else {
            "NON-RELIABLE"
        },
        message_string(&r.message)
    )
}

/// `TransactionImpl.toString`.
pub(crate) fn transaction_string(t: &Transaction) -> String {
    let xid = t
        .xid
        .as_ref()
        .map_or_else(|| "null".to_owned(), crate::transaction::xid_string);
    let state = match t.state {
        TxState::Active => "ACTIVE",
        TxState::Prepared => "PREPARED",
        TxState::Committed => "COMMITTED",
        TxState::RolledBack => "ROLLEDBACK",
        TxState::Suspended => "SUSPENDED",
        TxState::RollbackOnly => "ROLLBACK_ONLY",
    };
    format!(
        "TransactionImpl [xid={xid}, txID={}, xid={xid}, state={state}, createTime={}({}), timeoutSeconds={}, nr operations = {}]@{:x}",
        t.id,
        t.created,
        crate::localtime::java_date(t.created),
        if t.timeout_seconds < 0 {
            300
        } else {
            t.timeout_seconds
        },
        t.sends.len() + t.acks.len(),
        identity(("tx", t.id))
    )
}

/// The acknowledgements of references the broker removes in a transaction
/// of its own (`QueueImpl.iterQueue`: a queue deleted, messages removed
/// through management). With no consumer, the caller is `system@internal`;
/// each is added to the transaction (`nr operations` counting the refs
/// operation and the audit operations before it), then logged once it
/// commits, when the transaction's operations are gone.
pub(crate) fn removed(queue: &str, messages: &[&ServerMessage], tx_id: i64) {
    if !log::message_enabled() || messages.is_empty() {
        return;
    }
    let mut system = Subject::default();
    system.add(saranga_security::jaas::Principal::User("system".to_owned()));
    let caller = log::caller(Some(&system), Some("internal"));
    let created = crate::message::now_millis();
    let tx = |state: &str, operations: usize| {
        format!(
            "TransactionImpl [xid=null, txID={tx_id}, xid=null, state={state}, createTime={created}({}), timeoutSeconds=-1, nr operations = {operations}]@{:x}",
            crate::localtime::java_date(created),
            identity(("tx", tx_id))
        )
    };
    for (i, m) in messages.iter().enumerate() {
        let tx = tx("ACTIVE", 1 + i);
        audit(
            Logger::Message,
            "addAckToTransaction",
            &[&caller, queue, &message_string(m), &tx],
        );
    }
    let committed = tx("COMMITTED", 0);
    for m in messages {
        audit(
            Logger::Message,
            "coreAcknowledgeMessage",
            &[&caller, queue, &message_string(m), &committed],
        );
    }
}

// ---------------------------------------------------------------------------
// Management

/// Whether a JSON value can be the parameter of a Java type.
fn fits(java: &str, v: &Json) -> bool {
    let number = matches!(
        java,
        "int"
            | "long"
            | "short"
            | "byte"
            | "double"
            | "float"
            | "Integer"
            | "Long"
            | "Double"
            | "Float"
    );
    match v {
        Json::Null => !matches!(
            java,
            "int" | "long" | "short" | "byte" | "double" | "float" | "boolean"
        ),
        Json::Bool(_) => matches!(java, "boolean" | "Boolean" | "Object"),
        Json::Long(_) | Json::Double(_) => number || java == "Object",
        Json::String(_) => !number && !matches!(java, "boolean"),
        Json::Array(_) => java.ends_with("[]") || java == "Object",
        Json::Object(_) => java.starts_with("Map") || java == "Object",
    }
}

fn find(control: Control, method: &str, params: &[Json]) -> Option<&'static Entry> {
    let candidates: Vec<&Entry> = catalog::CONTROLS
        .iter()
        .filter(|e| e.control == control && e.method == method && e.types.len() == params.len())
        .collect();
    candidates
        .iter()
        .find(|e| e.types.iter().zip(params).all(|(t, v)| fits(t, v)))
        .or(candidates.first())
        .copied()
}

/// A parameter as Java's `String.valueOf` shows the value it became.
fn java_value(java: Option<&str>, v: Option<&Json>) -> String {
    let java = java.unwrap_or("Object");
    let floating = matches!(java, "double" | "float" | "Double" | "Float");
    match v {
        None | Some(Json::Null) => "null".to_owned(),
        Some(Json::String(s)) => s.clone(),
        Some(Json::Bool(b)) => b.to_string(),
        Some(Json::Long(n)) if floating => java_double(*n as f64),
        Some(Json::Long(n)) => n.to_string(),
        Some(Json::Double(d)) if floating => java_double(*d),
        Some(Json::Double(d)) => (*d as i64).to_string(),
        Some(Json::Array(items)) => {
            // An array's Object.toString.
            let class = match java.trim_end_matches("[]") {
                "String" => "[Ljava.lang.String;",
                "long" => "[J",
                "int" => "[I",
                "double" => "[D",
                "boolean" => "[Z",
                _ => "[Ljava.lang.Object;",
            };
            format!(
                "{class}@{:x}",
                identity(Json::Array(items.clone()).to_string())
            )
        }
        Some(Json::Object(kv)) => {
            let entries: Vec<String> = kv
                .iter()
                .map(|(k, v)| format!("{k}={}", java_value(None, Some(v))))
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
    }
}

/// A value an overload fills in: an address setting's default for the
/// call's address, or a constant.
fn computed(broker: &Broker, entry: &Entry, params: &[Json], expr: &str) -> String {
    let expr = expr
        .strip_prefix("Objects.toString(")
        .and_then(|e| e.strip_suffix(", null)"))
        .unwrap_or(expr);
    if let Some(getter) = expr
        .strip_prefix("addressSettings.")
        .or_else(|| expr.rsplit_once(").").map(|(_, g)| g))
        .and_then(|g| g.strip_suffix("()"))
    {
        // getMatch(address == null ? name : address)
        let param = |n: &str| {
            entry
                .names
                .iter()
                .position(|p| *p == n)
                .and_then(|i| params.get(i))
                .and_then(Json::as_str)
                .map(str::to_owned)
        };
        let Some(address) = param("address").or_else(|| param("name")) else {
            return "null".to_owned();
        };
        let s = broker.post_office().settings(&address);
        let opt = |v: &Option<String>| v.clone().unwrap_or_else(|| "null".to_owned());
        return match getter {
            "getDefaultMaxConsumers" => s.default_max_consumers.to_string(),
            "isDefaultPurgeOnNoConsumers" => s.default_purge_on_no_consumers.to_string(),
            "isDefaultExclusiveQueue" => s.default_exclusive_queue.to_string(),
            "isDefaultGroupRebalance" => s.default_group_rebalance.to_string(),
            "getDefaultGroupBuckets" => s.default_group_buckets.to_string(),
            "getDefaultGroupFirstKey" => opt(&s.default_group_first_key),
            "isDefaultLastValueQueue" => s.default_last_value_queue.to_string(),
            "getDefaultLastValueKey" => opt(&s.default_last_value_key),
            "isDefaultNonDestructive" => s.default_non_destructive.to_string(),
            "getDefaultConsumersBeforeDispatch" => s.default_consumers_before_dispatch.to_string(),
            "getDefaultDelayBeforeDispatch" => s.default_delay_before_dispatch.to_string(),
            "isAutoDeleteCreatedQueues" => s.auto_delete_created_queues.to_string(),
            "getAutoDeleteQueuesDelay" => s.auto_delete_queues_delay.to_string(),
            "getAutoDeleteQueuesMessageCount" => s.auto_delete_queues_message_count.to_string(),
            "isAutoCreateAddresses" => s.auto_create_addresses.to_string(),
            "getDefaultRingSize" => s.default_ring_size.to_string(),
            "getDefaultQueueRoutingType" => s.default_queue_routing_type.name().to_owned(),
            _ => "null".to_owned(),
        };
    }
    match expr.rsplit('.').next().unwrap_or(expr) {
        "FLUSH_LIMIT" => "500",
        "DEFAULT_MIN_EXPIRY_DELAY"
        | "DEFAULT_MAX_EXPIRY_DELAY"
        | "DEFAULT_ADDRESS_REJECT_THRESHOLD" => "-1",
        "DEFAULT_EXPIRY_QUEUE_SUFFIX" | "DEFAULT_DEAD_LETTER_QUEUE_SUFFIX" => "",
        "DEFAULT_EXPIRY_QUEUE_PREFIX" => "EXP.",
        "DEFAULT_DEAD_LETTER_QUEUE_PREFIX" => "DLQ.",
        "DEFAULT_ENABLE_METRICS" => "true",
        "DEFAULT_AUTO_CREATE_EXPIRY_RESOURCES"
        | "DEFAULT_AUTO_CREATE_DEAD_LETTER_RESOURCES"
        | "DEFAULT_AUTO_DELETE_CREATED_QUEUES" => "false",
        "DEFAULT_REDELIVER_COLLISION_AVOIDANCE_FACTOR" => "0.0",
        "DEFAULT_CONFIG_DELETE_QUEUES" | "DEFAULT_CONFIG_DELETE_ADDRESSES" => "OFF",
        "DEFAULT_AUTO_DELETE_QUEUES_MESSAGE_COUNT"
        | "DEFAULT_AUTO_DELETE_QUEUES_DELAY"
        | "DEFAULT_AUTO_DELETE_ADDRESSES_DELAY" => "0",
        "getDefaultRoutingType()" => "MULTICAST",
        "getDefaultRingSize()" => "-1",
        _ => "null",
    }
    .to_owned()
}

/// `AuditLogger.parametersList`.
fn parameters(broker: &Broker, entry: &Entry, args: &[Arg], params: &[Json]) -> String {
    let values: Vec<String> = args
        .iter()
        .map(|a| match a {
            Arg::Param(i) => java_value(entry.types.get(*i).copied(), params.get(*i)),
            Arg::Text(t) => (*t).to_owned(),
            Arg::Null => "null".to_owned(),
            Arg::Computed(expr) => computed(broker, entry, params, expr),
        })
        .collect();
    if entry.list {
        format!("with parameters: [{}]", values.join(", "))
    } else {
        match values.first() {
            // parametersList(null)
            None => String::new(),
            Some(v) if v == "null" => String::new(),
            Some(v) => format!("with parameters: {v}"),
        }
    }
}

/// The audit message of a management method, as its control logs it when
/// called: `name` is the queue, address or divert (unused for the broker).
pub(crate) fn management_call(
    broker: &Broker,
    control: Control,
    name: &str,
    method: &str,
    params: &[Json],
    who: Option<Who<'_>>,
) {
    let Some(who) = who else { return };
    if !(log::base_enabled() || log::resource_enabled()) {
        return;
    }
    let Some(entry) = find(control, method, params) else {
        return;
    };
    if !enabled(entry.logger) {
        return;
    }
    let caller = if entry.anonymous {
        log::caller(None, who.remote)
    } else {
        who.caller()
    };
    // BrokerConnectionControl's start and stop log the connection's name.
    let source = if control == Control::BrokerConnection && matches!(method, "start" | "stop") {
        name.to_owned()
    } else {
        source(broker, control, name)
    };
    let mut args = vec![caller, source];
    if let Some(p) = entry.params {
        args.push(parameters(broker, entry, p, params));
    }
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    emit(
        entry.logger,
        format!("AMQ{}: {}", entry.id, log::fill(entry.text, &refs)),
    );
}

/// What a management method logs once it has run: the resource logger's
/// success or failure messages.
pub(crate) fn management_result(
    control: Control,
    name: &str,
    method: &str,
    params: &[Json],
    result: &Result<Json, String>,
    who: Option<Who<'_>>,
) {
    let Some(who) = who else { return };
    if !log::resource_enabled() {
        return;
    }
    let ok = result.is_ok();
    let caller = who.caller();
    let p = |i: usize| java_value(None, params.get(i));
    let emit_pair = |success: &str, failure: &str, args: Vec<String>| {
        let mut all = vec![caller.clone()];
        all.extend(args);
        let refs: Vec<&str> = all.iter().map(String::as_str).collect();
        log::resource(text(if ok { success } else { failure }, &refs));
    };
    match (control, method) {
        (Control::Broker, "createAddress") => {
            // createAddress(name, routingTypes) or createAddress(json)
            if params.len() == 2 {
                emit_pair(
                    "createAddressSuccess",
                    "createAddressFailure",
                    vec![p(0), p(1)],
                );
            }
        }
        (Control::Broker, "updateAddress") if params.len() == 2 => {
            emit_pair(
                "updateAddressSuccess",
                "updateAddressFailure",
                vec![p(0), p(1)],
            );
        }
        (Control::Broker, "deleteAddress") => {
            emit_pair("deleteAddressSuccess", "deleteAddressFailure", vec![p(0)]);
        }
        (Control::Broker, "createQueue") if params.len() > 2 => {
            // The long form: address, routingType, name, ...
            if params.get(1).and_then(Json::as_str).is_some_and(|t| {
                t.eq_ignore_ascii_case("ANYCAST") || t.eq_ignore_ascii_case("MULTICAST")
            }) {
                emit_pair(
                    "createQueueSuccess",
                    "createQueueFailure",
                    vec![p(2), p(0), p(1)],
                );
            }
        }
        (Control::Broker, "updateQueue") if params.len() > 2 => {
            emit_pair("updateQueueSuccess", "updateQueueFailure", vec![p(0), p(1)]);
        }
        (Control::Broker, "destroyQueue") => {
            emit_pair("destroyQueueSuccess", "destroyQueueFailure", vec![p(0)]);
        }
        (Control::Queue, "removeMessages" | "removeAllMessages") => match result {
            Ok(removed) => {
                let removed = removed.as_i64().unwrap_or(0).to_string();
                log::resource(text("removeMessagesSuccess", &[&caller, &removed, name]));
            }
            Err(_) => log::resource(text("removeMessagesFailure", &[&caller, name])),
        },
        (Control::Queue, "pause") => {
            emit_pair(
                "pauseQueueSuccess",
                "pauseQueueFailure",
                vec![name.to_owned()],
            );
        }
        (Control::Queue, "resume") => {
            emit_pair(
                "resumeQueueSuccess",
                "resumeQueueFailure",
                vec![name.to_owned()],
            );
        }
        (Control::Queue, "sendMessage") => {
            let user = params
                .get(4)
                .map_or_else(|| "null".to_owned(), |u| java_value(None, Some(u)));
            emit_pair(
                "sendMessageSuccess",
                "sendMessageFailure",
                vec![name.to_owned(), user],
            );
        }
        (Control::Address, "pause") => {
            emit_pair(
                "pauseAddressSuccess",
                "pauseAddressFailure",
                vec![name.to_owned()],
            );
        }
        (Control::Address, "resume") => {
            emit_pair(
                "resumeAddressSuccess",
                "resumeAddressFailure",
                vec![name.to_owned()],
            );
        }
        (Control::Address, "purge") => {
            emit_pair(
                "purgeAddressSuccess",
                "purgeAddressFailure",
                vec![name.to_owned()],
            );
        }
        _ => {}
    }
}

/// `browse`'s audit: the messages it showed, or its failure.
pub(crate) fn browsed(queue: &str, result: &Result<Json, String>, who: Option<Who<'_>>) {
    let Some(who) = who else { return };
    if !log::resource_enabled() {
        return;
    }
    let caller = who.caller();
    match result {
        Ok(Json::Array(items)) => log::resource(text(
            "browseMessagesSuccess",
            &[&caller, &items.len().to_string(), queue],
        )),
        _ => log::resource(text("browseMessagesFailure", &[&caller, queue])),
    }
}

// ---------------------------------------------------------------------------
// Sessions

/// `createCoreSession`, as `ActiveMQServerImpl.createSession` logs it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn session_created(
    broker: &Broker,
    who: Who<'_>,
    name: &str,
    username: Option<&str>,
    connection: &crate::session::ClientConnection,
    auto_commit_sends: bool,
    auto_commit_acks: bool,
    pre_acknowledge: bool,
    xa: bool,
    default_address: Option<&str>,
) {
    if !log::base_enabled() {
        return;
    }
    let params = format!(
        "with parameters: [{name}, {}, ****, 102400, {}, {auto_commit_sends}, {auto_commit_acks}, {pre_acknowledge}, {xa}, {}, null, true, null]",
        username.unwrap_or("null"),
        connection_string(connection, broker.node_id()),
        default_address.unwrap_or("null"),
    );
    audit(
        Logger::Base,
        "createCoreSession",
        &[&who.caller(), &server_string(broker), &params],
    );
}

/// `RemotingConnectionImpl.toString`, the transport connection named as
/// Netty's (`NettyServerConnection@hash`), its hash taken from the ID.
pub(crate) fn connection_string(c: &crate::session::ClientConnection, node_id: &str) -> String {
    let hash = u32::from_str_radix(c.id(), 16).unwrap_or(0) & 0x7fff_ffff;
    format!(
        "RemotingConnectionImpl [ID={}, clientID={}, nodeID={node_id}, transportConnection=org.apache.activemq.artemis.core.remoting.impl.netty.NettyServerConnection@{hash:x}[ID={}, local= {}, remote={}]]",
        c.id(),
        c.client_id().as_deref().unwrap_or("null"),
        c.id(),
        c.local_address(),
        c.remote_address()
    )
}

/// `ServerSessionImpl.toString`: `tx_id` is its transaction's, -1 for none.
fn session_source(session: &str, connection: &str, consumers: usize, tx_id: i64) -> String {
    format!(
        "ServerSessionImpl(name={session},consumers={consumers},txID={tx_id},remotingConnection={connection})"
    )
}

/// The session-level calls `ServerSessionImpl` audits.
pub(crate) struct SessionAudit<'a> {
    pub who: Who<'a>,
    pub session: &'a str,
    pub connection: &'a crate::session::ClientConnection,
    pub node_id: &'a str,
    /// The session's transaction's ID, -1 for none.
    pub tx_id: i64,
    pub consumers: usize,
}

impl SessionAudit<'_> {
    fn base(&self, name: &str, params: String) {
        if !log::base_enabled() {
            return;
        }
        let source = session_source(
            self.session,
            &connection_string(self.connection, self.node_id),
            self.consumers,
            self.tx_id,
        );
        audit(Logger::Base, name, &[&self.who.caller(), &source, &params]);
    }

    /// `createCoreConsumer`.
    #[allow(clippy::too_many_arguments)]
    pub fn consumer(
        &self,
        id: i64,
        queue: &str,
        filter: Option<&str>,
        priority: i32,
        browse_only: bool,
        credits: Option<i32>,
    ) {
        if log::base_enabled() {
            self.base(
                "createCoreConsumer",
                format!(
                    "with parameters: [{id}, {queue}, {}, {priority}, {browse_only}, true, {}]",
                    filter.unwrap_or("null"),
                    credits.map_or_else(|| "null".to_owned(), |c| c.to_string())
                ),
            );
        }
    }

    /// `createQueue` (logged on the resource logger, as Artemis does).
    pub fn queue(&self, spec: &crate::postoffice::QueueSpec, shared: bool) {
        let logger = if shared {
            Logger::Base
        } else {
            Logger::Resource
        };
        if !enabled(logger) {
            return;
        }
        let source = session_source(
            self.session,
            &connection_string(self.connection, self.node_id),
            self.consumers,
            self.tx_id,
        );
        let params = format!("with parameters: [{}]", queue_configuration(spec));
        audit(
            logger,
            if shared {
                "createSharedQueue"
            } else {
                "createQueue"
            },
            &[&self.who.caller(), &source, &params],
        );
    }

    /// `destroyQueue`.
    pub fn delete_queue(&self, queue: &str) {
        if log::base_enabled() {
            self.base("destroyQueue", format!("with parameters: [{queue}]"));
        }
    }

    /// `serverSessionCreateAddress`: the session's name is the source.
    pub fn address(&self, broker: &Broker, name: &str, routing: &str, auto_created: bool) {
        if !log::base_enabled() {
            return;
        }
        let info = format!(
            "Address [name={name}, id=0, routingTypes={{{routing}}}, autoCreated={auto_created}, paused=false, bindingRemovedTimestamp=-1, swept=false, createdTimestamp={}, temporary=false]",
            crate::message::now_millis()
        );
        let _ = broker;
        audit(
            Logger::Base,
            "serverSessionCreateAddress2",
            &[
                &self.who.caller(),
                self.session,
                &format!("with parameters: [{info}, {auto_created}]"),
            ],
        );
    }

    /// `handleManagementMessage`: the session's name is the source.
    pub fn management_message(&self, tx: Option<&Transaction>, message: &ServerMessage) {
        if !log::base_enabled() {
            return;
        }
        let tx = tx.map_or_else(|| "null".to_owned(), transaction_string);
        audit(
            Logger::Base,
            "handleManagementMessage2",
            &[
                &self.who.caller(),
                self.session,
                &format!(
                    "with parameters: [{tx}, {}, false]",
                    message_string(message)
                ),
            ],
        );
    }
}

/// `QueueConfiguration.toString`.
fn queue_configuration(spec: &crate::postoffice::QueueSpec) -> String {
    let mut s = String::from("QueueConfiguration [");
    let _ = write!(s, "name={}, address={}", spec.name, spec.address);
    if let Some(t) = spec.routing_type {
        let _ = write!(s, ", routingType={}", t.name());
    }
    if let Some(f) = &spec.filter_string {
        let _ = write!(s, ", filterString={f}");
    }
    let _ = write!(
        s,
        ", durable={}, temporary={}, autoCreated={}]",
        spec.durable, spec.temporary, spec.auto_created
    );
    s
}

// ---------------------------------------------------------------------------
// Messages

/// `coreSendMessage`: a message routed outside a transaction, or when its
/// transaction commits.
pub(crate) fn sent(who: Who<'_>, message: &ServerMessage, context: &str, tx: Option<&str>) {
    if log::message_enabled() {
        audit(
            Logger::Message,
            "coreSendMessage",
            &[
                &who.caller(),
                &message_string(message),
                context,
                tx.unwrap_or("null"),
            ],
        );
    }
}

/// `addSendToTransaction`.
pub(crate) fn send_in_transaction(who: Who<'_>, message: &ServerMessage, tx: &str) {
    if log::message_enabled() {
        audit(
            Logger::Message,
            "addSendToTransaction",
            &[&who.caller(), &message_string(message), tx],
        );
    }
}

/// `rolledBackTransaction`.
pub(crate) fn rolled_back(who: Who<'_>, tx: &str, involving: &str) {
    if log::message_enabled() {
        audit(
            Logger::Message,
            "rolledBackTransaction",
            &[&who.caller(), tx, involving],
        );
    }
}

/// `coreAcknowledgeMessage`.
pub(crate) fn acknowledged(who: Who<'_>, queue: &str, message: &ServerMessage, tx: Option<&str>) {
    if log::message_enabled() {
        audit(
            Logger::Message,
            "coreAcknowledgeMessage",
            &[
                &who.caller(),
                queue,
                &message_string(message),
                tx.unwrap_or("null"),
            ],
        );
    }
}

/// `addAckToTransaction`.
pub(crate) fn ack_in_transaction(who: Who<'_>, queue: &str, message: &ServerMessage, tx: &str) {
    if log::message_enabled() {
        audit(
            Logger::Message,
            "addAckToTransaction",
            &[&who.caller(), queue, &message_string(message), tx],
        );
    }
}

/// `RoutingContextImpl.toString` of a message's routing: the address,
/// then its durable and non-durable queues.
pub(crate) fn routing_context(address: &str, queues: &[crate::queue::Queue]) -> String {
    let mut s = format!(
        "RoutingContextImpl(Address={address}, routingType=null, PreviousAddress=null previousRoute:null, reusable=false, version=0)\n"
    );
    if !queues.is_empty() {
        let line = |s: &mut String, q: &crate::queue::Queue| {
            let c = q.config();
            let _ = writeln!(
                s,
                "- queueID={} address:{} name:{} filter:{}",
                q.id(),
                c.address,
                q.name(),
                c.filter_string.as_ref().map_or_else(
                    || "null".to_owned(),
                    |f| format!("FilterImpl [sfilterString={f}]")
                )
            );
        };
        s.push_str("..................................................\n");
        let _ = writeln!(s, "***** durable queues {address}:");
        for q in queues.iter().filter(|q| q.config().durable) {
            line(&mut s, q);
        }
        let _ = writeln!(s, "***** non durable for {address}:");
        for q in queues.iter().filter(|q| !q.config().durable) {
            line(&mut s, q);
        }
    }
    s.push_str("..................................................\n");
    s
}

/// What a transaction did, kept before it ends for the messages logged
/// when it has (`afterCommit`, `afterRollback`).
pub(crate) struct Ending {
    sends: Vec<std::sync::Arc<ServerMessage>>,
    acks: Vec<(String, MessageRef)>,
}

/// The transaction's sends and acknowledgements, when messages are
/// audited.
pub(crate) fn ending(t: &Transaction) -> Option<Ending> {
    if !log::message_enabled() {
        return None;
    }
    let mut sends: Vec<std::sync::Arc<ServerMessage>> = Vec::new();
    for (_, r) in &t.sends {
        if !sends.iter().any(|m| m.message_id() == r.message_id()) {
            sends.push(r.message.clone());
        }
    }
    let acks = t
        .acks
        .iter()
        .map(|(q, r)| (q.name().to_string(), r.clone()))
        .collect();
    Some(Ending { sends, acks })
}

/// Logs what a transaction's end means for its messages: sent and
/// acknowledged once committed, rolled back otherwise.
pub(crate) fn ended(who: Who<'_>, ending: Ending, t: &Transaction, committed: bool) {
    let tx = transaction_string(t);
    for m in &ending.sends {
        if committed {
            let address = m.core().address.as_ref().map(ToString::to_string);
            let context = routing_context(address.as_deref().unwrap_or("null"), &[]);
            sent(who, m, &context, Some(&tx));
        } else {
            rolled_back(who, &tx, &message_string(m));
        }
    }
    for (queue, r) in &ending.acks {
        if committed {
            acknowledged(who, queue, &r.message, Some(&tx));
        } else {
            rolled_back(who, &tx, &reference_string(r));
        }
    }
}

/// A consumer's deliveries, logged as they go (`proceedDeliver`).
pub(crate) struct AuditedSink {
    pub inner: std::sync::Arc<dyn crate::queue::DeliverySink>,
    pub subject: Option<std::sync::Arc<Subject>>,
    pub remote: String,
    pub queue: String,
    /// The broker's plugins, told of each delivery.
    pub plugins: std::sync::Arc<crate::plugin::Plugins>,
    /// The consumer, for the plugins.
    pub consumer: std::sync::OnceLock<std::sync::Weak<crate::registry::ConsumerEntry>>,
}

impl AuditedSink {
    fn consumer(&self) -> Option<std::sync::Arc<crate::registry::ConsumerEntry>> {
        if self.plugins.is_empty() {
            return None;
        }
        self.consumer.get().and_then(std::sync::Weak::upgrade)
    }

    /// Delivers with the plugins' `beforeDeliver` and `afterDeliver`.
    fn delivering(&self, message: &ServerMessage, deliver: impl FnOnce() -> i64) -> i64 {
        self.log(message);
        let consumer = self.consumer();
        if let Some(c) = &consumer {
            self.plugins.each(|p| p.before_deliver(c, message));
        }
        let credits = deliver();
        if let Some(c) = &consumer {
            self.plugins.each(|p| p.after_deliver(c, message));
        }
        credits
    }

    fn log(&self, message: &ServerMessage) {
        if log::message_enabled() {
            let who = Who {
                subject: self.subject.as_deref(),
                remote: Some(&self.remote),
            };
            let reference = format!(
                "Reference[{}]:{}:{}",
                message.message_id(),
                if message.core().durable {
                    "RELIABLE"
                } else {
                    "NON-RELIABLE"
                },
                message_string(message)
            );
            audit(
                Logger::Message,
                "consumeMessage",
                &[&who.caller(), &self.queue, &reference],
            );
        }
    }
}

impl crate::queue::DeliverySink for AuditedSink {
    fn deliver(&self, consumer_id: i64, message: &ServerMessage, delivery_count: i32) -> i64 {
        self.delivering(message, || {
            self.inner.deliver(consumer_id, message, delivery_count)
        })
    }

    fn deliver_large(
        &self,
        consumer_id: i64,
        message: &ServerMessage,
        body_size: i64,
        delivery_count: i32,
    ) -> i64 {
        self.delivering(message, || {
            self.inner
                .deliver_large(consumer_id, message, body_size, delivery_count)
        })
    }

    fn deliver_continuation(&self, consumer_id: i64, chunk: Vec<u8>, continues: bool) -> i64 {
        self.inner
            .deliver_continuation(consumer_id, chunk, continues)
    }

    fn chunk_size(&self) -> usize {
        self.inner.chunk_size()
    }

    fn supports_large(&self) -> bool {
        self.inner.supports_large()
    }

    fn has_credits(&self, message: &ServerMessage) -> bool {
        self.inner.has_credits(message)
    }

    fn disconnect(&self, consumer_id: i64, reason: &str) {
        self.inner.disconnect(consumer_id, reason)
    }

    fn accepts(&self, message: &ServerMessage) -> bool {
        if self.plugins.is_empty() {
            return true;
        }
        // Until the consumer is known it takes nothing a plugin might
        // refuse it.
        self.consumer()
            .is_some_and(|c| self.plugins.can_accept(&c, message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_finds_overloads_by_type() {
        let e = find(
            Control::Broker,
            "createQueue",
            &[Json::from("{\"name\":\"q\"}"), Json::Bool(true)],
        )
        .expect("createQueue(json, boolean)");
        assert_eq!(e.types, ["String", "boolean"]);
        let e = find(
            Control::Broker,
            "createQueue",
            &[Json::from("a"), Json::from("q")],
        )
        .expect("createQueue(address, name)");
        assert_eq!(e.types, ["String", "String"]);
        assert_eq!(e.id, 601065);
        assert!(e.anonymous);
        let e = find(Control::Queue, "getName", &[]).expect("getName");
        assert_eq!(e.id, 601014);
        assert!(e.params.is_none());
    }

    #[test]
    fn java_values() {
        assert_eq!(java_value(Some("long"), Some(&Json::Long(5))), "5");
        assert_eq!(java_value(Some("double"), Some(&Json::Long(5))), "5.0");
        assert_eq!(java_value(Some("String"), Some(&Json::Null)), "null");
        assert_eq!(
            java_value(
                Some("Map"),
                Some(&Json::Object(vec![("a".into(), Json::Long(1))]))
            ),
            "{a=1}"
        );
        assert!(
            java_value(Some("String[]"), Some(&Json::Array(vec![])))
                .starts_with("[Ljava.lang.String;@")
        );
    }

    #[test]
    fn messages_by_name() {
        assert_eq!(
            text("createQueueSuccess", &["u@x", "q", "a", "ANYCAST"]),
            "AMQ601707: User u@x successfully created queue: q on address: a with routing type ANYCAST"
        );
    }
}
