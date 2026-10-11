//! The paged, filtered and sorted listings (`ActiveMQAbstractView` with
//! `QueueView` and `AddressView`): `{"data": [...], "count": n}`, each
//! item an object of string fields.

use std::cmp::Ordering;
use std::sync::Arc;

use super::Outcome;
use super::json::Json;
use crate::Broker;

/// A field's value, as the view's `getField` gives it for filtering and
/// sorting.
#[derive(Debug, Clone)]
pub(super) enum Field {
    Long(i64),
    Bool(bool),
    Text(Option<String>),
}

impl Field {
    /// As `toJson` writes it: `toString`, null as empty.
    fn text(&self) -> String {
        match self {
            Field::Long(n) => n.to_string(),
            Field::Bool(b) => b.to_string(),
            Field::Text(t) => t.clone().unwrap_or_default(),
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
    /// `matches` for text and booleans, `matchesLong` for numbers.
    fn accepts(&self, f: Option<&Field>) -> bool {
        let Some(op) = self.operation.as_deref() else {
            return true;
        };
        let Some(f) = f else {
            return true;
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
            other => {
                let text = match other {
                    Field::Text(None) => None,
                    other => Some(other.text()),
                };
                let equals = || match &text {
                    None => self.value.is_empty(),
                    Some(t) => *t == self.value,
                };
                let contains = || match &text {
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
            }
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

/// A field name as the options give it: the view's name (`messageCount`)
/// or the enum constant's (`MESSAGE_COUNT`).
fn canonical(field: &str, names: &[&str]) -> Option<String> {
    if names.contains(&field) {
        return Some(field.to_owned());
    }
    let wanted = field.replace('_', "").to_lowercase();
    names
        .iter()
        .find(|n| n.to_lowercase() == wanted)
        .map(|n| (*n).to_owned())
}

/// Filters, sorts and pages `items` (each its fields in output order).
fn render(
    items: Vec<Vec<(&'static str, Field)>>,
    names: &[&str],
    default_sort: &str,
    opts: &Options,
    page: i32,
    page_size: i32,
) -> Result<Json, String> {
    for p in &opts.parts {
        if !p.field.is_empty() && canonical(&p.field, names).is_none() {
            return Err(format!("Unsupported field, {}", p.field));
        }
    }
    let field = |item: &Vec<(&'static str, Field)>, name: &str| {
        item.iter()
            .find(|(n, _)| *n == name)
            .map(|(_, f)| f.clone())
    };
    let mut kept: Vec<_> = items
        .into_iter()
        .filter(|item| {
            opts.parts.iter().all(|p| {
                if p.field.is_empty() {
                    return true;
                }
                let name = canonical(&p.field, names).unwrap_or_default();
                p.accepts(field(item, &name).as_ref())
            })
        })
        .collect();
    let count = kept.len();
    let sort = opts
        .sort_field
        .as_deref()
        .and_then(|f| canonical(f, names))
        .unwrap_or_else(|| default_sort.to_owned());
    kept.sort_by(|a, b| {
        let (x, y) = (field(a, &sort), field(b, &sort));
        let o = match (x, y) {
            (Some(x), Some(y)) => x.compare(&y),
            _ => Ordering::Equal,
        };
        if opts.descending { o.reverse() } else { o }
    });
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
        .map(|(_, item)| {
            Json::Object(
                item.into_iter()
                    .map(|(n, f)| (n.to_owned(), Json::from(f.text())))
                    .collect(),
            )
        })
        .collect();
    Ok(Json::object()
        .with("data", Json::Array(data))
        .with("count", count))
}

const QUEUE_FIELDS: &[&str] = &[
    "id",
    "name",
    "address",
    "filter",
    "durable",
    "paused",
    "persistedPause",
    "temporary",
    "purgeOnNoConsumers",
    "consumerCount",
    "maxConsumers",
    "autoCreated",
    "user",
    "routingType",
    "messagesAdded",
    "messageCount",
    "messagesAcked",
    "messagesExpired",
    "deliveringCount",
    "messagesKilled",
    "directDeliver",
    "exclusive",
    "lastValue",
    "lastValueKey",
    "scheduledCount",
    "groupRebalance",
    "groupRebalancePauseDispatch",
    "groupBuckets",
    "groupFirstKey",
    "enabled",
    "ringSize",
    "consumersBeforeDispatch",
    "delayBeforeDispatch",
    "autoDelete",
    "internalQueue",
    "consumerID",
];

/// `listQueues(options, page, pageSize)`.
pub(super) async fn list_queues(
    broker: &Arc<Broker>,
    options_text: Option<&str>,
    page: i32,
    page_size: i32,
) -> Outcome {
    let opts = options(options_text)?;
    let mut items = Vec::new();
    for q in broker.post_office().queues() {
        let c = q.config();
        let stats = q.stats().await;
        use Field::*;
        let text = |s: Option<String>| Text(s);
        items.push(vec![
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
        ]);
    }
    let json = render(items, QUEUE_FIELDS, "name", &opts, page, page_size)?;
    Ok(Json::from(json.to_string()))
}

const ADDRESS_FIELDS: &[&str] = &[
    "id",
    "name",
    "routingTypes",
    "queueCount",
    "internal",
    "temporary",
    "autoCreated",
    "paused",
    "currentDuplicateIdCacheSize",
    "retroactiveResource",
    "unroutedMessageCount",
    "routedMessageCount",
    "messageCount",
    "addressLimitPercent",
    "numberOfPages",
    "addressSize",
    "maxPageReadBytes",
    "maxPageReadMessages",
    "prefetchPageBytes",
    "prefetchPageMessages",
    "numberOfBytesPerPage",
    "paging",
];

/// `listAddresses(options, page, pageSize)`.
pub(super) async fn list_addresses(
    broker: &Arc<Broker>,
    options_text: Option<&str>,
    page: i32,
    page_size: i32,
) -> Outcome {
    let opts = options(options_text)?;
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
        items.push(vec![
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
        ]);
    }
    let json = render(items, ADDRESS_FIELDS, "id", &opts, page, page_size)?;
    Ok(Json::from(json.to_string()))
}
