//! The paged, filtered and sorted listings (`ActiveMQAbstractView` with
//! `QueueView`, `AddressView`, `ConnectionView`, `SessionView`,
//! `ConsumerView` and `ProducerView`): `{"data": [...], "count": n}`.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;

use super::Outcome;
use super::json::Json;
use crate::Broker;
use crate::registry::{ConsumerEntry, ProducerEntry, SessionEntry};
use crate::session::ClientConnection;

/// A field's value, as the view's `getField` gives it for filtering and
/// sorting.
#[derive(Debug, Clone)]
pub(super) enum Field {
    Long(i64),
    Bool(bool),
    Text(Option<String>),
    /// Several values, any of which may match (`matchAny`); not sortable.
    Any(Vec<String>),
}

impl Field {
    /// As `toJson` writes it: `toString`, null as empty.
    fn text(&self) -> String {
        match self {
            Field::Long(n) => n.to_string(),
            Field::Bool(b) => b.to_string(),
            Field::Text(t) => t.clone().unwrap_or_default(),
            Field::Any(v) => v.join(","),
        }
    }

    fn compare(&self, other: &Field) -> Ordering {
        match (self, other) {
            (Field::Long(a), Field::Long(b)) => a.cmp(b),
            (Field::Bool(a), Field::Bool(b)) => a.cmp(b),
            (Field::Text(Some(a)), Field::Text(Some(b))) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

/// One filter part (`PredicateFilterPart`).
struct Part {
    field: String,
    operation: Option<String>,
    value: String,
}

impl Part {
    /// `matches` for text, `matchesLong` for numbers, `matchAny` for
    /// sets.
    fn accepts(&self, f: Option<&Field>) -> bool {
        let Some(op) = self.operation.as_deref() else {
            return true;
        };
        let Some(f) = f else {
            return true;
        };
        let text_matches = |text: Option<&str>| {
            let equals = || match text {
                None => self.value.is_empty(),
                Some(t) => t == self.value,
            };
            let contains = || match text {
                None => self.value.is_empty(),
                Some(t) => t.contains(self.value.as_str()),
            };
            match op {
                "EQUALS" => equals(),
                "NOT_EQUALS" => !equals(),
                "CONTAINS" => contains(),
                "NOT_CONTAINS" => !contains(),
                _ => false,
            }
        };
        match f {
            Field::Long(n) => {
                let Ok(v) = self.value.parse::<i64>() else {
                    return matches!(op, "NOT_EQUALS" | "NOT_CONTAINS");
                };
                match op {
                    "EQUALS" => *n == v,
                    "NOT_EQUALS" => *n != v,
                    "CONTAINS" => false,
                    "NOT_CONTAINS" => true,
                    "LESS_THAN" => *n < v,
                    "GREATER_THAN" => *n > v,
                    _ => true,
                }
            }
            Field::Any(values) => values.iter().any(|v| text_matches(Some(v))),
            Field::Text(None) => text_matches(None),
            other => text_matches(Some(&other.text())),
        }
    }
}

/// The view's options: the filter parts and the sort.
struct Options {
    parts: Vec<Part>,
    sort_field: Option<String>,
    descending: bool,
}

fn options(text: Option<&str>) -> Result<Options, String> {
    let json = match text.filter(|t| !t.trim().is_empty()) {
        None => Json::object(),
        Some(t) => Json::parse(t)?,
    };
    let s = |o: &Json, k: &str| {
        o.get(k)
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let part = |o: &Json| Part {
        field: s(o, "field"),
        operation: Some(s(o, "operation")).filter(|o| !o.trim().is_empty()),
        value: s(o, "value"),
    };
    let parts = match json.get("searchFilters") {
        Some(Json::Array(items)) => items.iter().map(part).collect(),
        _ => vec![part(&json)],
    };
    let sort_field = json
        .get("sortColumn")
        .or_else(|| json.get("sortField"))
        .and_then(Json::as_str)
        .filter(|_| json.get("sortOrder").is_some())
        .map(str::to_owned);
    let descending = json
        .get("sortOrder")
        .and_then(Json::as_str)
        .is_some_and(|o| o.eq_ignore_ascii_case("desc"));
    for p in &parts {
        if let Some(op) = &p.operation
            && !matches!(
                op.as_str(),
                "EQUALS"
                    | "NOT_EQUALS"
                    | "CONTAINS"
                    | "NOT_CONTAINS"
                    | "GREATER_THAN"
                    | "LESS_THAN"
            )
        {
            return Err(format!(
                "No enum constant org.apache.activemq.artemis.core.management.impl.view.predicate.ActiveMQFilterPredicate.Operation.{op}"
            ));
        }
    }
    Ok(Options {
        parts,
        sort_field,
        descending,
    })
}

/// A view's field enum: each constant and its name.
struct View {
    /// The enum's class (`QueueField`).
    class: &'static str,
    fields: &'static [(&'static str, &'static str)],
    default_sort: &'static str,
}

impl View {
    /// `valueOfName` (the name, any case) for sorting.
    fn by_name(&self, field: &str) -> Option<&'static str> {
        self.fields
            .iter()
            .find(|(_, n)| n.eq_ignore_ascii_case(field))
            .map(|(_, n)| *n)
    }

    /// The field a filter names: `valueOfName`, then `valueOf` (the
    /// constant, exactly).
    fn filter_field(&self, field: &str) -> Result<&'static str, String> {
        self.by_name(field)
            .or_else(|| {
                self.fields
                    .iter()
                    .find(|(c, _)| *c == field)
                    .map(|(_, n)| *n)
            })
            .ok_or_else(|| {
                format!(
                    "No enum constant org.apache.activemq.artemis.core.management.impl.view.{}.{field}",
                    self.class
                )
            })
    }
}

/// One element of a view: its fields for filtering and sorting, and its
/// JSON (`toJson`).
struct Item {
    fields: Vec<(&'static str, Field)>,
    json: Json,
}

impl Item {
    fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|(n, _)| *n == name).map(|(_, f)| f)
    }

    /// Fields that `toJson` writes as their `toString`.
    fn strings(fields: Vec<(&'static str, Field)>) -> Self {
        let json = Json::Object(
            fields
                .iter()
                .map(|(n, f)| ((*n).to_owned(), Json::from(f.text())))
                .collect(),
        );
        Self { fields, json }
    }
}

/// Filters, sorts and pages `items` (`getResultsAsJson`).
fn render(
    items: Vec<Item>,
    view: &View,
    opts: &Options,
    page: i32,
    page_size: i32,
) -> Result<Json, String> {
    let mut parts = Vec::new();
    for p in &opts.parts {
        if !p.field.is_empty() {
            parts.push((view.filter_field(&p.field)?, p));
        }
    }
    let mut kept: Vec<Item> = items
        .into_iter()
        .filter(|item| parts.iter().all(|(name, p)| p.accepts(item.field(name))))
        .collect();
    let count = kept.len();
    // getField(sortField) takes only a field's name; another sorts nothing.
    let sort = match &opts.sort_field {
        Some(f) => view.by_name(f),
        None => Some(view.default_sort),
    };
    if let Some(sort) = sort {
        kept.sort_by(|a, b| {
            let o = match (a.field(sort), b.field(sort)) {
                (Some(x), Some(y)) => x.compare(y),
                _ => Ordering::Equal,
            };
            if opts.descending { o.reverse() } else { o }
        });
    }
    let (start, end) = if page == -1 || page_size == -1 {
        (0, count)
    } else {
        let start = ((page as i64 - 1) * page_size as i64).max(0) as usize;
        (
            start,
            (page as i64 * page_size as i64).clamp(0, count as i64) as usize,
        )
    };
    let data: Vec<Json> = kept
        .into_iter()
        .enumerate()
        .filter(|(i, _)| *i >= start && *i < end)
        .map(|(_, item)| item.json)
        .collect();
    Ok(Json::object()
        .with("data", Json::Array(data))
        .with("count", count))
}

async fn list(
    items: Vec<Item>,
    view: &View,
    options_text: Option<&str>,
    page: i32,
    page_size: i32,
) -> Outcome {
    let opts = options(options_text)?;
    let json = render(items, view, &opts, page, page_size)?;
    Ok(Json::from(json.to_string()))
}

const QUEUE_VIEW: View = View {
    class: "QueueField",
    fields: &[
        ("ID", "id"),
        ("NAME", "name"),
        ("CONSUMER_ID", "consumerID"),
        ("ADDRESS", "address"),
        ("MAX_CONSUMERS", "maxConsumers"),
        ("FILTER", "filter"),
        ("MESSAGE_COUNT", "messageCount"),
        ("CONSUMER_COUNT", "consumerCount"),
        ("DELIVERING_COUNT", "deliveringCount"),
        ("MESSAGES_ADDED", "messagesAdded"),
        ("MESSAGES_ACKED", "messagesAcked"),
        ("MESSAGES_EXPIRED", "messagesExpired"),
        ("ROUTING_TYPE", "routingType"),
        ("USER", "user"),
        ("AUTO_CREATED", "autoCreated"),
        ("DURABLE", "durable"),
        ("PAUSED", "paused"),
        ("PERSISTEDPAUSE", "persistedPause"),
        ("TEMPORARY", "temporary"),
        ("PURGE_ON_NO_CONSUMERS", "purgeOnNoConsumers"),
        ("MESSAGES_KILLED", "messagesKilled"),
        ("DIRECT_DELIVER", "directDeliver"),
        ("LAST_VALUE", "lastValue"),
        ("EXCLUSIVE", "exclusive"),
        ("SCHEDULED_COUNT", "scheduledCount"),
        ("LAST_VALUE_KEY", "lastValueKey"),
        ("GROUP_REBALANCE", "groupRebalance"),
        (
            "GROUP_REBALANCE_PAUSE_DISPATCH",
            "groupRebalancePauseDispatch",
        ),
        ("GROUP_BUCKETS", "groupBuckets"),
        ("GROUP_FIRST_KEY", "groupFirstKey"),
        ("ENABLED", "enabled"),
        ("RING_SIZE", "ringSize"),
        ("CONSUMERS_BEFORE_DISPATCH", "consumersBeforeDispatch"),
        ("DELAY_BEFORE_DISPATCH", "delayBeforeDispatch"),
        ("AUTO_DELETE", "autoDelete"),
        ("INTERNAL_QUEUE", "internalQueue"),
    ],
    default_sort: "name",
};

/// `listQueues(options, page, pageSize)`.
pub(super) async fn list_queues(
    broker: &Arc<Broker>,
    options_text: Option<&str>,
    page: i32,
    page_size: i32,
) -> Outcome {
    let mut items = Vec::new();
    for q in broker.post_office().queues() {
        let c = q.config();
        let stats = q.stats().await;
        use Field::*;
        let text = |s: Option<String>| Text(s);
        items.push(Item::strings(vec![
            ("id", Long(q.id())),
            ("name", text(Some(c.name.to_string()))),
            ("address", text(Some(c.address.to_string()))),
            ("filter", text(q.filter_string())),
            ("durable", Bool(c.durable)),
            ("paused", Bool(q.is_paused())),
            ("persistedPause", Bool(q.is_persisted_pause())),
            ("temporary", Bool(c.temporary)),
            ("purgeOnNoConsumers", Bool(c.purge_on_no_consumers)),
            ("consumerCount", Long(i64::from(q.consumer_count()))),
            ("maxConsumers", Long(i64::from(c.max_consumers))),
            ("autoCreated", Bool(c.auto_created)),
            ("user", text(c.user.clone())),
            ("routingType", text(Some(c.routing_type.name().to_owned()))),
            ("messagesAdded", Long(q.messages_added())),
            ("messageCount", Long(q.message_count())),
            ("messagesAcked", Long(q.messages_acknowledged())),
            ("messagesExpired", Long(q.messages_expired())),
            ("deliveringCount", Long(i64::from(stats.delivering_count))),
            ("messagesKilled", Long(q.messages_killed())),
            ("directDeliver", Bool(false)),
            ("exclusive", Bool(c.exclusive)),
            (
                "lastValue",
                Bool(c.last_value || c.last_value_key.is_some()),
            ),
            ("lastValueKey", text(c.last_value_key.clone())),
            ("scheduledCount", Long(stats.scheduled_count)),
            ("groupRebalance", Bool(c.group_rebalance)),
            (
                "groupRebalancePauseDispatch",
                Bool(c.group_rebalance_pause_dispatch),
            ),
            ("groupBuckets", Long(i64::from(c.group_buckets))),
            ("groupFirstKey", text(c.group_first_key.clone())),
            ("enabled", Bool(c.enabled)),
            ("ringSize", Long(c.ring_size)),
            (
                "consumersBeforeDispatch",
                Long(i64::from(c.consumers_before_dispatch)),
            ),
            ("delayBeforeDispatch", Long(c.delay_before_dispatch)),
            ("autoDelete", Bool(c.auto_delete)),
            ("internalQueue", Bool(c.internal)),
        ]));
    }
    list(items, &QUEUE_VIEW, options_text, page, page_size).await
}

const ADDRESS_VIEW: View = View {
    class: "AddressField",
    fields: &[
        ("ID", "id"),
        ("NAME", "name"),
        ("ROUTING_TYPES", "routingTypes"),
        ("QUEUE_COUNT", "queueCount"),
        ("INTERNAL", "internal"),
        ("TEMPORARY", "temporary"),
        ("AUTO_CREATED", "autoCreated"),
        ("PAUSED", "paused"),
        (
            "CURRENT_DUPLICATE_ID_CACHE_SIZE",
            "currentDuplicateIdCacheSize",
        ),
        ("RETROACTIVE_RESOURCE", "retroactiveResource"),
        ("UNROUTED_MESSAGE_COUNT", "unroutedMessageCount"),
        ("ROUTED_MESSAGE_COUNT", "routedMessageCount"),
        ("MESSAGE_COUNT", "messageCount"),
        ("NUMBER_OF_BYTES_PER_PAGE", "numberOfBytesPerPage"),
        ("ADDRESS_LIMIT_PERCENT", "addressLimitPercent"),
        ("PAGING", "paging"),
        ("NUMBER_OF_PAGES", "numberOfPages"),
        ("ADDRESS_SIZE", "addressSize"),
        ("MAX_PAGE_READ_BYTES", "maxPageReadBytes"),
        ("MAX_PAGE_READ_MESSAGES", "maxPageReadMessages"),
        ("PREFETCH_PAGE_BYTES", "prefetchPageBytes"),
        ("PREFETCH_PAGE_MESSAGES", "prefetchPageMessages"),
    ],
    default_sort: "id",
};

/// `listAddresses(options, page, pageSize)`.
pub(super) async fn list_addresses(
    broker: &Arc<Broker>,
    options_text: Option<&str>,
    page: i32,
    page_size: i32,
) -> Outcome {
    let po = broker.post_office();
    let prefix = super::queue::internal_prefix(broker);
    let mut items = Vec::new();
    for info in po.addresses() {
        let Some(st) = po.address_status(&info.name) else {
            continue;
        };
        let store = po.paging().existing_store(&info.name);
        let settings = store.as_ref().map(|s| s.address_settings());
        let dup = if po.duplicate_cache_exists(&info.name) {
            crate::duplicate::lock(&po.duplicate_cache(&info.name)).len() as i64
        } else {
            0
        };
        use Field::*;
        let types = Json::from(super::address::routing_type_names(info.routing_types)).to_string();
        items.push(Item::strings(vec![
            ("id", Long(info.id)),
            ("name", Text(Some(info.name.to_string()))),
            ("routingTypes", Text(Some(types))),
            ("queueCount", Long(st.queues.len() as i64)),
            ("internal", Bool(info.internal)),
            ("temporary", Bool(info.temporary)),
            ("autoCreated", Bool(info.auto_created)),
            ("paused", Bool(st.paused)),
            ("currentDuplicateIdCacheSize", Long(dup)),
            ("retroactiveResource", Bool(info.name.starts_with(&*prefix))),
            ("unroutedMessageCount", Long(st.unrouted)),
            ("routedMessageCount", Long(st.routed)),
            (
                "messageCount",
                Long(st.queues.iter().map(|q| q.message_count()).sum()),
            ),
            (
                "addressLimitPercent",
                Long(
                    store
                        .as_ref()
                        .map_or(0, |s| i64::from(s.address_limit_percent())),
                ),
            ),
            (
                "numberOfPages",
                Long(
                    store
                        .as_ref()
                        .filter(|s| s.is_paging())
                        .map_or(0, |s| s.number_of_pages()),
                ),
            ),
            (
                "addressSize",
                Long(store.as_ref().map_or(0, |s| s.address_size())),
            ),
            (
                "maxPageReadBytes",
                Long(settings.as_ref().map_or(0, |s| s.max_read_page_bytes)),
            ),
            (
                "maxPageReadMessages",
                Long(settings.as_ref().map_or(0, |s| s.max_read_page_messages)),
            ),
            (
                "prefetchPageBytes",
                Long(settings.as_ref().map_or(0, |s| s.prefetch_page_bytes)),
            ),
            // AddressView writes the prefetch page bytes here too.
            (
                "prefetchPageMessages",
                Long(settings.as_ref().map_or(0, |s| s.prefetch_page_bytes)),
            ),
            (
                "numberOfBytesPerPage",
                Long(store.as_ref().map_or(0, |s| s.page_size_bytes())),
            ),
            (
                "paging",
                Bool(store.as_ref().is_some_and(|s| s.is_paging())),
            ),
        ]));
    }
    list(items, &ADDRESS_VIEW, options_text, page, page_size).await
}

/// `new Date(millis).toString()`, in the local time zone.
pub(super) fn date(millis: i64) -> String {
    crate::localtime::java_date(millis)
}

/// The users of a connection's sessions, sorted and without repeats, a
/// session without one counting as empty.
fn users(c: &ClientConnection) -> BTreeSet<String> {
    c.sessions()
        .iter()
        .map(|s| s.username.clone().unwrap_or_default())
        .collect()
}

const CONNECTION_VIEW: View = View {
    class: "ConnectionField",
    fields: &[
        ("CONNECTION_ID", "connectionID"),
        ("CLIENT_ID", "clientID"),
        ("USERS", "users"),
        ("PROTOCOL", "protocol"),
        ("SESSION_COUNT", "sessionCount"),
        ("REMOTE_ADDRESS", "remoteAddress"),
        ("LOCAL_ADDRESS", "localAddress"),
        ("SESSION_ID", "sessionID"),
        ("CREATION_TIME", "creationTime"),
        ("IMPLEMENTATION", "implementation"),
        ("PROXY_ADDRESS", "proxyAddress"),
        ("PROXY_PROTOCOL_VERSION", "proxyProtocolVersion"),
    ],
    default_sort: "connectionID",
};

/// `listConnections(options, page, pageSize)`.
pub(super) async fn list_connections(
    broker: &Arc<Broker>,
    options_text: Option<&str>,
    page: i32,
    page_size: i32,
) -> Outcome {
    let items = broker
        .registry()
        .connections()
        .iter()
        .map(|c| {
            let users: Vec<String> = users(c).into_iter().collect();
            let sessions = c.sessions();
            use Field::*;
            Item {
                fields: vec![
                    ("connectionID", Text(Some(c.id().to_owned()))),
                    ("clientID", Text(c.client_id())),
                    ("users", Any(users.clone())),
                    ("protocol", Text(Some(c.protocol().to_owned()))),
                    ("sessionCount", Long(sessions.len() as i64)),
                    ("remoteAddress", Text(Some(c.remote_address().to_owned()))),
                    ("localAddress", Text(Some(c.local_address().to_owned()))),
                    (
                        "sessionID",
                        Any(sessions.iter().map(|s| s.name.clone()).collect()),
                    ),
                    ("creationTime", Long(c.created())),
                    ("implementation", Text(Some(c.implementation().to_owned()))),
                    ("proxyAddress", Text(None)),
                    ("proxyProtocolVersion", Text(None)),
                ],
                json: Json::object()
                    .with("connectionID", c.id())
                    .with("remoteAddress", c.remote_address())
                    .with("users", users.join(","))
                    .with("creationTime", date(c.created()))
                    .with("implementation", c.implementation())
                    .with("protocol", c.protocol())
                    .with("clientID", c.client_id().unwrap_or_default())
                    .with("localAddress", c.local_address())
                    .with("sessionCount", sessions.len() as i32)
                    .with("proxyAddress", "")
                    .with("proxyProtocolVersion", ""),
            }
        })
        .collect();
    list(items, &CONNECTION_VIEW, options_text, page, page_size).await
}

const SESSION_VIEW: View = View {
    class: "SessionField",
    fields: &[
        ("ID", "id"),
        ("CONNECTION_ID", "connectionID"),
        ("CONSUMER_COUNT", "consumerCount"),
        ("PRODUCER_COUNT", "producerCount"),
        ("USER", "user"),
        ("VALIDATED_USER", "validatedUser"),
        ("PROTOCOL", "protocol"),
        ("CLIENT_ID", "clientID"),
        ("LOCAL_ADDRESS", "localAddress"),
        ("REMOTE_ADDRESS", "remoteAddress"),
        ("CREATION_TIME", "creationTime"),
    ],
    default_sort: "id",
};

/// `listSessions(options, page, pageSize)`.
pub(super) async fn list_sessions(
    broker: &Arc<Broker>,
    options_text: Option<&str>,
    page: i32,
    page_size: i32,
) -> Outcome {
    let mut items = Vec::new();
    for c in broker.registry().connections() {
        for s in c.sessions() {
            let consumers = s.consumers().len() as i64;
            let producers = s.producers().len() as i64;
            use Field::*;
            items.push(Item {
                // SessionPredicateFilterPart has no case for the users or
                // the creation time: they match anything.
                fields: vec![
                    ("id", Text(Some(s.name.clone()))),
                    ("connectionID", Text(Some(c.id().to_owned()))),
                    ("consumerCount", Long(consumers)),
                    ("producerCount", Long(producers)),
                    ("protocol", Text(Some(c.protocol().to_owned()))),
                    ("clientID", Text(c.client_id())),
                    ("localAddress", Text(Some(c.local_address().to_owned()))),
                    ("remoteAddress", Text(Some(c.remote_address().to_owned()))),
                ],
                json: Json::object()
                    .with("id", s.name.clone())
                    .with("user", s.username.clone().unwrap_or_default())
                    .with(
                        "validatedUser",
                        s.validated_user.clone().unwrap_or_default(),
                    )
                    .with("creationTime", date(s.created))
                    .with("consumerCount", consumers as i32)
                    .with("producerCount", producers as i32)
                    .with("connectionID", c.id())
                    .with("clientID", c.client_id().unwrap_or_default()),
            });
        }
    }
    list(items, &SESSION_VIEW, options_text, page, page_size).await
}

/// Every consumer, with its session and connection.
pub(super) fn all_consumers(
    broker: &Broker,
) -> Vec<(Arc<ClientConnection>, Arc<SessionEntry>, Arc<ConsumerEntry>)> {
    let mut all = Vec::new();
    for c in broker.registry().connections() {
        for s in c.sessions() {
            for consumer in s.consumers() {
                all.push((c.clone(), s.clone(), consumer));
            }
        }
    }
    all
}

/// `ConsumerView.checkConsumerStatus`.
fn consumer_status(c: &ClientConnection) -> &'static str {
    if c.is_failed() { "Orphaned" } else { "OK" }
}

/// What a consumer is delivering, by sequential ID (`getMessagesInTransit`).
async fn in_transit(
    consumers: &[(Arc<ClientConnection>, Arc<SessionEntry>, Arc<ConsumerEntry>)],
) -> std::collections::HashMap<u64, usize> {
    let mut queues: Vec<crate::queue::Queue> = Vec::new();
    for (_, _, c) in consumers {
        if !queues.iter().any(|q| q.id() == c.queue.id()) {
            queues.push(c.queue.clone());
        }
    }
    let mut counts = std::collections::HashMap::new();
    for q in queues {
        for info in q.consumers().await {
            counts.insert(info.id, info.delivering);
        }
    }
    counts
}

const CONSUMER_VIEW: View = View {
    class: "ConsumerField",
    fields: &[
        ("ID", "id"),
        ("SEQUENTIAL_ID", "sequentialId"),
        ("SESSION", "session"),
        ("CONNECTION", "connection"),
        ("QUEUE", "queue"),
        ("FILTER", "filter"),
        ("ADDRESS", "address"),
        ("USER", "user"),
        ("VALIDATED_USER", "validatedUser"),
        ("PROTOCOL", "protocol"),
        ("CLIENT_ID", "clientID"),
        ("LOCAL_ADDRESS", "localAddress"),
        ("REMOTE_ADDRESS", "remoteAddress"),
        ("QUEUE_TYPE", "queueType"),
        ("BROWSE_ONLY", "browseOnly"),
        ("CREATION_TIME", "creationTime"),
        ("MESSAGES_IN_TRANSIT", "messagesInTransit"),
        ("MESSAGES_IN_TRANSIT_SIZE", "messagesInTransitSize"),
        ("MESSAGES_DELIVERED", "messagesDelivered"),
        ("MESSAGES_DELIVERED_SIZE", "messagesDeliveredSize"),
        ("MESSAGES_ACKNOWLEDGED", "messagesAcknowledged"),
        (
            "MESSAGES_ACKNOWLEDGED_AWAITING_COMMIT",
            "messagesAcknowledgedAwaitingCommit",
        ),
        ("LAST_DELIVERED_TIME", "lastDeliveredTime"),
        ("LAST_ACKNOWLEDGED_TIME", "lastAcknowledgedTime"),
        ("STATUS", "status"),
    ],
    default_sort: "id",
};

/// `listConsumers(options, page, pageSize)`.
pub(super) async fn list_consumers(
    broker: &Arc<Broker>,
    options_text: Option<&str>,
    page: i32,
    page_size: i32,
) -> Outcome {
    let all = all_consumers(broker);
    let transit = in_transit(&all).await;
    let items = all
        .iter()
        .map(|(conn, s, c)| {
            let m = &c.metrics;
            let qc = c.queue.config();
            let delivering = transit.get(&c.sequential_id).copied().unwrap_or(0) as i64;
            use Field::*;
            let text = |v: &str| Text(Some(v.to_owned()));
            Item {
                fields: vec![
                    ("id", Long(c.sequential_id as i64)),
                    ("session", text(&s.name)),
                    ("user", Text(s.username.clone())),
                    ("validatedUser", Text(s.validated_user.clone())),
                    ("address", text(&qc.address)),
                    ("queue", text(&qc.name)),
                    ("filter", Text(c.filter.clone())),
                    ("protocol", text(conn.protocol())),
                    ("clientID", Text(conn.client_id())),
                    ("localAddress", text(conn.local_address())),
                    ("remoteAddress", text(conn.remote_address())),
                    ("messagesInTransit", Long(delivering)),
                    ("messagesInTransitSize", Long(m.in_transit_size())),
                    // ConsumerPredicateFilterPart compares it as text.
                    ("messagesDelivered", Text(Some(m.delivered().to_string()))),
                    ("messagesDeliveredSize", Long(m.delivered_size())),
                    ("messagesAcknowledged", Long(m.acknowledged())),
                    (
                        "messagesAcknowledgedAwaitingCommit",
                        Long(i64::from(m.awaiting_commit())),
                    ),
                    ("lastAcknowledgedTime", Long(m.last_acknowledged())),
                    ("lastDeliveredTime", Long(m.last_delivered())),
                    ("creationTime", Long(c.created)),
                    ("status", text(consumer_status(conn))),
                    ("queueType", text(&qc.routing_type.name().to_lowercase())),
                    ("connection", text(conn.id())),
                ],
                json: Json::object()
                    .with("id", c.sequential_id.to_string())
                    .with("session", s.name.clone())
                    .with("clientID", conn.client_id().unwrap_or_default())
                    .with("user", s.username.clone().unwrap_or_default())
                    .with(
                        "validatedUser",
                        s.validated_user.clone().unwrap_or_default(),
                    )
                    .with("protocol", conn.protocol())
                    .with("queue", qc.name.to_string())
                    .with("queueType", qc.routing_type.name().to_lowercase())
                    .with("filter", c.filter.clone().unwrap_or_default())
                    .with("address", qc.address.to_string())
                    .with("localAddress", conn.local_address())
                    .with("remoteAddress", conn.remote_address())
                    .with("creationTime", date(c.created))
                    .with("messagesInTransit", delivering.to_string())
                    .with("messagesInTransitSize", m.in_transit_size().to_string())
                    .with("messagesDelivered", m.delivered().to_string())
                    .with("messagesDeliveredSize", m.delivered_size().to_string())
                    .with("messagesAcknowledged", m.acknowledged().to_string())
                    .with(
                        "messagesAcknowledgedAwaitingCommit",
                        m.awaiting_commit().to_string(),
                    )
                    .with("lastDeliveredTime", m.last_delivered())
                    .with("lastAcknowledgedTime", m.last_acknowledged())
                    .with("status", consumer_status(conn)),
            }
        })
        .collect();
    list(items, &CONSUMER_VIEW, options_text, page, page_size).await
}

/// `toJSONObject(ServerConsumer)` of `listConsumersAsJSON` and
/// `listAllConsumersAsJSON`.
pub(super) async fn consumers_json(
    consumers: &[(Arc<ClientConnection>, Arc<SessionEntry>, Arc<ConsumerEntry>)],
) -> Json {
    let transit = in_transit(consumers).await;
    Json::Array(
        consumers
            .iter()
            .map(|(conn, s, c)| {
                let m = &c.metrics;
                let delivering = transit.get(&c.sequential_id).copied().unwrap_or(0) as i32;
                let mut o = Json::object()
                    .with("consumerID", c.id)
                    .with("sequentialId", c.sequential_id as i64)
                    .with("connectionID", conn.id())
                    .with("sessionID", s.name.clone())
                    .with("queueName", c.queue.name().to_string())
                    .with("browseOnly", c.browse_only)
                    .with("creationTime", c.created)
                    .with("deliveringCount", delivering)
                    .with("messagesInTransit", delivering)
                    .with("messagesInTransitSize", m.in_transit_size())
                    .with("messagesDelivered", m.delivered())
                    .with("messagesDeliveredSize", m.delivered_size())
                    .with("messagesAcknowledged", m.acknowledged())
                    .with("messagesAcknowledgedAwaitingCommit", m.awaiting_commit())
                    .with("lastDeliveredTime", m.last_delivered())
                    .with("lastAcknowledgedTime", m.last_acknowledged())
                    .with("status", consumer_status(conn));
                if let Some(f) = &c.filter {
                    o.put("filter", f.clone());
                }
                o
            })
            .collect(),
    )
}

/// `QueueControl.listConsumersAsJSON`: the queue's consumers that a
/// session made (`ServerConsumer`s; a bridge's or a cluster connection's
/// are left out), in the queue's order.
pub(super) async fn queue_consumers_json(broker: &Broker, q: &crate::queue::Queue) -> Json {
    let mine: Vec<_> = all_consumers(broker)
        .into_iter()
        .filter(|(_, _, c)| c.queue.id() == q.id())
        .collect();
    Json::Array(
        q.consumers()
            .await
            .into_iter()
            .filter_map(|info| {
                let (conn, s, c) = mine.iter().find(|(_, _, c)| c.sequential_id == info.id)?;
                let m = &c.metrics;
                Some(
                    Json::object()
                        .with("consumerID", c.id)
                        .with("sequentialId", c.sequential_id as i64)
                        .with("connectionID", conn.id())
                        .with("sessionID", s.name.clone())
                        .with("browseOnly", c.browse_only)
                        .with("creationTime", c.created)
                        .with("messagesInTransit", info.delivering as i32)
                        .with("messagesInTransitSize", m.in_transit_size())
                        .with("messagesDelivered", m.delivered())
                        .with("messagesDeliveredSize", m.delivered_size())
                        .with("messagesAcknowledged", m.acknowledged())
                        .with("messagesAcknowledgedAwaitingCommit", m.awaiting_commit())
                        .with("lastDeliveredTime", m.last_delivered())
                        .with("lastAcknowledgedTime", m.last_acknowledged())
                        .with("status", consumer_status(conn)),
                )
            })
            .collect(),
    )
}

/// Every producer, with its session and connection.
pub(super) fn all_producers(
    broker: &Broker,
) -> Vec<(Arc<ClientConnection>, Arc<SessionEntry>, Arc<ProducerEntry>)> {
    let mut all = Vec::new();
    for c in broker.registry().connections() {
        for s in c.sessions() {
            for p in s.producers() {
                all.push((c.clone(), s.clone(), p));
            }
        }
    }
    all
}

const PRODUCER_VIEW: View = View {
    class: "ProducerField",
    fields: &[
        ("ID", "id"),
        ("NAME", "name"),
        ("SESSION", "session"),
        ("CONNECTION_ID", "connectionID"),
        ("ADDRESS", "address"),
        ("USER", "user"),
        ("VALIDATED_USER", "validatedUser"),
        ("PROTOCOL", "protocol"),
        ("CLIENT_ID", "clientID"),
        ("LOCAL_ADDRESS", "localAddress"),
        ("REMOTE_ADDRESS", "remoteAddress"),
        ("CREATION_TIME", "creationTime"),
        ("MESSAGE_SENT", "msgSent"),
        ("MESSAGE_SENT_SIZE", "msgSizeSent"),
        ("LAST_PRODUCED_MESSAGE_ID", "lastProducedMessageID"),
    ],
    default_sort: "id",
};

/// `listProducers(options, page, pageSize)`.
pub(super) async fn list_producers(
    broker: &Arc<Broker>,
    options_text: Option<&str>,
    page: i32,
    page_size: i32,
) -> Outcome {
    let items = all_producers(broker)
        .iter()
        .map(|(conn, s, p)| {
            let address = p.address.clone();
            use Field::*;
            let text = |v: &str| Text(Some(v.to_owned()));
            Item {
                fields: vec![
                    ("id", Long(p.id)),
                    ("connectionID", text(conn.id())),
                    ("session", text(&s.name)),
                    ("user", Text(s.username.clone())),
                    ("validatedUser", Text(s.validated_user.clone())),
                    ("address", text(&address)),
                    ("protocol", text(&p.protocol)),
                    ("clientID", Text(conn.client_id())),
                    ("localAddress", text(conn.local_address())),
                    ("remoteAddress", text(conn.remote_address())),
                    ("name", text(&p.name)),
                    ("creationTime", Long(p.created)),
                    ("msgSent", Long(p.sent())),
                    ("msgSizeSent", Long(p.sent_size())),
                    ("lastProducedMessageID", Text(p.last_message_id())),
                ],
                json: Json::object()
                    .with("id", p.id.to_string())
                    .with("name", p.name.clone())
                    .with("session", s.name.clone())
                    .with("clientID", conn.client_id().unwrap_or_default())
                    .with("user", s.username.clone().unwrap_or_default())
                    .with(
                        "validatedUser",
                        s.validated_user.clone().unwrap_or_default(),
                    )
                    .with("protocol", conn.protocol())
                    .with("address", address)
                    .with("localAddress", conn.local_address())
                    .with("remoteAddress", conn.remote_address())
                    .with("creationTime", p.created.to_string())
                    .with("msgSent", p.sent())
                    .with("msgSizeSent", p.sent_size())
                    .with(
                        "lastProducedMessageID",
                        p.last_message_id().unwrap_or_default(),
                    ),
            }
        })
        .collect();
    list(items, &PRODUCER_VIEW, options_text, page, page_size).await
}

/// `ServerSessionImpl.describeProducersInfo`, for every session
/// (`listProducersInfoAsJSON`).
pub(super) fn producers_info(broker: &Broker) -> Json {
    Json::Array(
        all_producers(broker)
            .iter()
            .map(|(conn, s, p)| {
                Json::object()
                    .with("id", p.id.to_string())
                    .with("name", p.name.clone())
                    .with("connectionID", conn.id())
                    .with("sessionID", s.name.clone())
                    .with("creationTime", p.created.to_string())
                    .with("destination", p.address.clone())
                    .with(
                        "lastProducedMessageID",
                        p.last_message_id().map_or(Json::Null, Json::from),
                    )
                    .with("msgSent", p.sent())
                    .with("msgSizeSent", p.sent_size())
            })
            .collect(),
    )
}
