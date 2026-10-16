//! What a link attaches to: the target of a link the client sends on
//! (`ProtonServerReceiverContext`) and the source of one it receives from
//! (`DefaultSenderController`), with the queue names Artemis gives
//! subscriptions (`AmqpSupport.createQueueName`).

use saranga_core_protocol::ErrorType;
use saranga_server::postoffice::{AutoCreate, QueueSpec, RoutingTypes, split_fqqn};
use saranga_server::session::ServerSession;
use saranga_server::{BrokerError, RoutingType};

use crate::composite::Fields;
use crate::messaging::{Source, Target, TerminusDurability, TerminusExpiryPolicy};
use crate::transport::condition;
use crate::types::{Descriptor, Symbol, Value};

/// Capabilities and filters (`AmqpSupport`).
pub mod capability {
    pub const QUEUE: &str = "queue";
    pub const TOPIC: &str = "topic";
    pub const TEMP_QUEUE: &str = "temporary-queue";
    pub const TEMP_TOPIC: &str = "temporary-topic";
    pub const SHARED: &str = "shared";
    pub const GLOBAL: &str = "global";
    pub const DELAYED_DELIVERY: &str = "DELAYED_DELIVERY";
    pub const SHARED_SUBS: &str = "SHARED-SUBS";
    pub const ANONYMOUS_RELAY: &str = "ANONYMOUS-RELAY";
    pub const SOLE_CONNECTION: &str = "sole-connection-for-container";
}

pub const JMS_SELECTOR_CODE: u64 = 0x0000_468C_0000_0004;
pub const JMS_SELECTOR_NAME: &str = "apache.org:selector-filter:string";
pub const NO_LOCAL_CODE: u64 = 0x0000_468C_0000_0003;
pub const NO_LOCAL_NAME: &str = "apache.org:no-local-filter:list";
/// `MessageUtil.CONNECTION_ID_PROPERTY_NAME`.
pub const CONNECTION_ID_PROPERTY: &str = "__AMQ_CID";

/// A link refused: the condition and description of its detach.
#[derive(Debug, Clone)]
pub struct LinkError {
    pub condition: &'static str,
    pub description: String,
}

impl LinkError {
    pub fn new(condition: &'static str, description: impl Into<String>) -> Self {
        Self {
            condition,
            description: description.into(),
        }
    }

    /// A broker failure that escapes the link handlers: Artemis closes
    /// the connection for it; here only the link is refused.
    pub fn internal(e: &BrokerError) -> Self {
        Self::new(condition::INTERNAL_ERROR, e.message.clone())
    }
}

fn is_security(e: &BrokerError) -> bool {
    e.kind == ErrorType::SecurityException
}

/// Whether `caps` holds `name`.
pub fn has(caps: Option<&Vec<Symbol>>, name: &str) -> bool {
    caps.is_some_and(|c| c.iter().any(|s| s.as_str() == name))
}

// ------------------------------------------------------------ receivers

/// Where a client's messages go (`ProtonServerReceiverContext.initialize`).
#[derive(Debug, Clone)]
pub struct ReceiverTarget {
    /// The fixed address, or `None` for an anonymous relay.
    pub address: Option<String>,
    /// From the target's capabilities or the acceptor's prefix.
    pub explicit: Option<RoutingType>,
    /// `getImplicitRoutingType` at attach.
    pub implicit: Option<RoutingType>,
    /// A dynamic target's temporary queue or address, removed with the
    /// link.
    pub dynamic: bool,
}

/// The routing type a target's capabilities ask for: the first of
/// topic/queue in their order.
fn capability_routing_type(caps: Option<&Vec<Symbol>>) -> Option<RoutingType> {
    for c in caps.into_iter().flatten() {
        match c.as_str() {
            capability::TOPIC | capability::TEMP_TOPIC => return Some(RoutingType::Multicast),
            capability::QUEUE | capability::TEMP_QUEUE => return Some(RoutingType::Anycast),
            _ => {}
        }
    }
    None
}

/// `getImplicitRoutingType`: an existing address's only routing type
/// (`None` with both), else the default for a new address.
pub fn implicit_routing_type(session: &ServerSession, address: &str) -> Option<RoutingType> {
    let bare = session.remove_prefix(address);
    let (real, _) = split_fqqn(bare);
    let po = session.broker().post_office();
    match po.address_info(real) {
        Some(info) => match (info.routing_types.anycast, info.routing_types.multicast) {
            (true, false) => Some(RoutingType::Anycast),
            (false, true) => Some(RoutingType::Multicast),
            _ => None,
        },
        None => Some(po.settings(real).default_address_routing_type),
    }
}

/// The routing type the acceptor's prefix on `address` gives, if any.
fn prefix_routing_type(session: &ServerSession, address: &str) -> Option<RoutingType> {
    session.strip_prefix(address).1
}

/// Resolves a receiver link's target (`ProtonServerReceiverContext`).
pub fn resolve_target(
    session: &mut ServerSession,
    target: &mut Target,
) -> Result<ReceiverTarget, LinkError> {
    if target.dynamic {
        let address = uuid::Uuid::new_v4().to_string();
        let explicit = capability_routing_type(target.capabilities.as_ref())
            .or_else(|| prefix_routing_type(session, &address));
        let implicit = implicit_routing_type(session, &address);
        let created = match explicit.or(implicit) {
            Some(RoutingType::Anycast) => {
                create_temporary_queue(session, &address, &address, RoutingType::Anycast, None)
            }
            _ => create_temporary_address(session, &address),
        };
        created.map_err(|e| {
            if is_security(&e) {
                LinkError::new(
                    condition::UNAUTHORIZED_ACCESS,
                    format!(
                        "AMQ119016: not authorized to create temporary destination, {}",
                        e.message
                    ),
                )
            } else {
                LinkError::internal(&e)
            }
        })?;
        target.address = Some(address.clone());
        return Ok(ReceiverTarget {
            address: Some(address),
            explicit,
            implicit,
            dynamic: true,
        });
    }
    match target.address.clone().filter(|a| !a.is_empty()) {
        Some(address) => {
            let explicit = capability_routing_type(target.capabilities.as_ref())
                .or_else(|| prefix_routing_type(session, &address));
            let implicit = implicit_routing_type(session, &address);
            match session.check_auto_create(&address, explicit.or(implicit)) {
                Ok(AutoCreate::NotFound) => {
                    return Err(LinkError::new(
                        condition::NOT_FOUND,
                        format!("AMQ119002: target address {address} does not exist"),
                    ));
                }
                Ok(_) => {}
                Err(e) if is_security(&e) => {
                    return Err(LinkError::new(
                        condition::UNAUTHORIZED_ACCESS,
                        format!(
                            "AMQ119017: not authorized to create producer, {}",
                            e.message
                        ),
                    ));
                }
                Err(e) => return Err(LinkError::internal(&e)),
            }
            let bare = session.remove_prefix(&address).to_owned();
            let (real, queue) = split_fqqn(&bare);
            if let Err(e) = session.security_check(real, queue, saranga_config::CheckType::Send) {
                return Err(LinkError::new(
                    condition::UNAUTHORIZED_ACCESS,
                    format!(
                        "AMQ119017: not authorized to create producer, {}",
                        e.message
                    ),
                ));
            }
            Ok(ReceiverTarget {
                address: Some(address),
                explicit,
                implicit,
                dynamic: false,
            })
        }
        None => Ok(ReceiverTarget {
            address: None,
            explicit: capability_routing_type(target.capabilities.as_ref()),
            implicit: None,
            dynamic: false,
        }),
    }
}

/// `createTemporaryAddress`: a multicast address that is not stored,
/// removed with the link.
pub fn create_temporary_address(session: &ServerSession, address: &str) -> Result<(), BrokerError> {
    session.security_check(address, None, saranga_config::CheckType::CreateAddress)?;
    session
        .broker()
        .post_office()
        .add_or_update_address(
            address,
            RoutingTypes::of(RoutingType::Multicast),
            true,
            true,
        )
        .map(|_| ())
}

/// `createTemporaryQueue`: a temporary, non-durable queue the connection
/// removes when it closes.
pub fn create_temporary_queue(
    session: &mut ServerSession,
    address: &str,
    name: &str,
    routing_type: RoutingType,
    filter: Option<String>,
) -> Result<(), BrokerError> {
    let mut spec = QueueSpec::new(name, address);
    spec.routing_type = Some(routing_type);
    spec.durable = false;
    spec.temporary = true;
    spec.filter_string = filter;
    session.create_queue(spec).map(|_| ())
}

// ------------------------------------------------------------ senders

/// What a client receives from (`DefaultSenderController.init`).
#[derive(Debug, Clone)]
pub struct SenderSource {
    /// The queue the consumer takes from.
    pub queue: String,
    /// A topic subscription.
    pub multicast: bool,
    /// The consumer's own filter (anycast queues).
    pub selector: Option<String>,
    pub browse: bool,
    pub priority: i32,
    /// A non-shared, non-durable subscription's temporary queue.
    pub temp_queue: Option<String>,
    pub shared: bool,
    pub global: bool,
    pub is_volatile: bool,
    /// The routing type of the queue.
    pub routing_type: Option<RoutingType>,
}

/// `AmqpSupport.createQueueName`.
pub fn create_queue_name(
    core_naming: bool,
    client_id: Option<&str>,
    link: &str,
    shared: bool,
    global: bool,
    is_volatile: bool,
) -> String {
    let client_id = client_id.filter(|c| !c.is_empty());
    if core_naming {
        let durable = !is_volatile;
        let sub = link.split('|').next().unwrap_or(link);
        let esc = |s: &str| s.replace('\\', "\\\\").replace('.', "\\.");
        let client = if global { None } else { client_id };
        return match (client, durable) {
            (Some(c), true) => format!("{}.{}", esc(c), esc(sub)),
            (Some(c), false) => format!("nonDurable.{}.{}", esc(c), esc(sub)),
            (None, true) => esc(sub),
            (None, false) => format!("nonDurable.{}", esc(sub)),
        };
    }
    let mut base = match client_id {
        Some(c) if !global => format!("{c}.{link}"),
        _ => link.to_owned(),
    };
    if shared {
        if let Some(i) = base.find('|') {
            base.truncate(i);
        }
        if is_volatile {
            base.push_str(":shared-volatile");
        }
        if global {
            base.push_str(":global");
        }
    }
    base
}

fn described_code(v: &Value) -> Option<(&Descriptor, &Value)> {
    match v {
        Value::Described(d, body) => Some((d, body)),
        _ => None,
    }
}

/// The selector and no-local entries of a filter set; the selector's text.
/// A filter set's entry: its key and described value.
pub(crate) type FilterEntry = (Symbol, Value);

pub(crate) fn find_filters(filter: Option<&Fields>) -> (Option<FilterEntry>, Option<FilterEntry>) {
    let mut selector = None;
    let mut no_local = None;
    for (k, v) in filter.map(|f| f.0.as_slice()).unwrap_or_default() {
        if let Some((d, _)) = described_code(v) {
            if d.is(JMS_SELECTOR_CODE, JMS_SELECTOR_NAME) && selector.is_none() {
                selector = Some((k.clone(), v.clone()));
            } else if d.is(NO_LOCAL_CODE, NO_LOCAL_NAME) && no_local.is_none() {
                no_local = Some((k.clone(), v.clone()));
            }
        }
    }
    (selector, no_local)
}

/// `described.toString()` of a selector filter: its string.
pub(crate) fn selector_text(v: &Value) -> Option<String> {
    match described_code(v)?.1 {
        Value::String(s) => Some(s.clone()),
        Value::Symbol(s) => Some(s.0.clone()),
        _ => None,
    }
}

fn consumer_error(e: &BrokerError) -> LinkError {
    if is_security(e) {
        LinkError::new(
            condition::UNAUTHORIZED_ACCESS,
            format!(
                "AMQ119015: not authorized to create consumer, {}",
                e.message
            ),
        )
    } else {
        LinkError::new(
            condition::INTERNAL_ERROR,
            format!("AMQ119005: error creating consumer, {}", e.message),
        )
    }
}

fn not_exists() -> LinkError {
    LinkError::new(
        condition::NOT_FOUND,
        "AMQ119010: source address does not exist",
    )
}

/// Resolves a sender link's source, changing it to what is echoed.
/// `source` is `None` when the client recovers a subscription by link name.
pub fn resolve_source(
    session: &mut ServerSession,
    source: &mut Option<Source>,
    link_name: &str,
    container_id: Option<&str>,
    desired: Option<&Vec<Symbol>>,
    link_properties: Option<&Fields>,
    core_naming: bool,
) -> Result<SenderSource, LinkError> {
    // The selector, checked first.
    let (selector_entry, no_local_entry) =
        find_filters(source.as_ref().and_then(|s| s.filter.as_ref()));
    let mut selector = selector_entry.as_ref().and_then(|(_, v)| selector_text(v));
    if let Some(sel) = &selector
        && saranga_filter::Filter::parse(sel).is_err()
    {
        return Err(LinkError::new(condition::INVALID_FIELD, "Invalid filter"));
    }
    let po = session.broker().post_office().clone();
    let link_priority = link_properties
        .and_then(|p| p.get("priority"))
        .and_then(Value::as_i64)
        .map(|p| p as i32);

    let Some(src) = source.as_mut() else {
        // Subscription recovery: the name as for a shared durable one.
        let global = has(desired, capability::GLOBAL);
        let queue = create_queue_name(core_naming, container_id, link_name, true, global, false);
        let q = po.queue_query(&queue);
        if !q.exists {
            return Err(LinkError::new(
                condition::NOT_FOUND,
                format!("Unknown subscription link: {link_name}"),
            ));
        }
        let mut filter = Fields::default();
        let mut remaining = q.filter_string.clone();
        let no_local_clause = format!(
            "{CONNECTION_ID_PROPERTY}<>'{}'",
            container_id.unwrap_or_default()
        );
        if let Some(f) = &remaining {
            if f == &no_local_clause {
                remaining = None;
                filter.insert(NO_LOCAL_NAME, no_local_value());
            } else if let Some(rest) = f.strip_suffix(&format!(" AND {no_local_clause}")) {
                remaining = Some(rest.to_owned());
                filter.insert(NO_LOCAL_NAME, no_local_value());
            }
        }
        if let Some(s) = remaining.filter(|s| !s.trim().is_empty()) {
            filter.insert(
                JMS_SELECTOR_NAME,
                Value::described(Descriptor::Code(JMS_SELECTOR_CODE), Value::String(s)),
            );
        }
        *source = Some(Source {
            address: Some(queue.clone()),
            durable: TerminusDurability::UnsettledState,
            expiry_policy: TerminusExpiryPolicy::Never,
            distribution_mode: Some(Symbol::from("copy")),
            capabilities: Some(vec![Symbol::from(capability::TOPIC)]),
            filter: (!filter.is_empty()).then_some(filter),
            ..Default::default()
        });
        return Ok(SenderSource {
            queue,
            multicast: true,
            selector: None,
            browse: false,
            priority: link_priority.unwrap_or(0),
            temp_queue: None,
            shared: true,
            global,
            is_volatile: false,
            routing_type: Some(RoutingType::Multicast),
        });
    };

    if src.dynamic {
        let queue = uuid::Uuid::new_v4().to_string();
        create_temporary_queue(session, &queue, &queue, RoutingType::Anycast, None).map_err(
            |e| {
                let msg = if is_security(&e) {
                    format!(
                        "AMQ119016: not authorized to create temporary destination, {}",
                        e.message
                    )
                } else {
                    e.message.clone()
                };
                LinkError::new(
                    condition::INTERNAL_ERROR,
                    format!("AMQ119001: error creating temporary queue, {msg}"),
                )
            },
        )?;
        src.address = Some(queue.clone());
        src.filter = None;
        let browse = src
            .distribution_mode
            .as_ref()
            .is_some_and(|m| m.as_str() == "copy");
        return Ok(SenderSource {
            queue,
            multicast: false,
            selector,
            browse,
            priority: link_priority.unwrap_or(0),
            temp_queue: None,
            shared: false,
            global: false,
            is_volatile: false,
            routing_type: Some(RoutingType::Anycast),
        });
    }

    let full = src.address.clone().unwrap_or_default();
    let (source_address, query) = match full.split_once('?') {
        Some((a, q)) => (a.to_owned(), Some(q.to_owned())),
        None => (full.clone(), None),
    };
    let priority = link_priority
        .or_else(|| {
            query.as_deref().and_then(|q| {
                q.split('&').find_map(|kv| {
                    let (k, v) = kv.split_once('=')?;
                    (k == "consumer-priority").then(|| v.parse().ok()).flatten()
                })
            })
        })
        .unwrap_or(0);
    let shared = has(src.capabilities.as_ref(), capability::SHARED);
    let global = has(src.capabilities.as_ref(), capability::GLOBAL);
    let (address, fqqn_queue) = match split_fqqn(&source_address) {
        (a, Some(q)) => (a.to_owned(), Some(q.to_owned())),
        (a, None) => (a.to_owned(), None),
    };
    let caps = src.capabilities.as_ref();
    let client_defined = has(caps, capability::TOPIC) || has(caps, capability::QUEUE);
    let multicast;
    if client_defined {
        multicast = has(caps, capability::TOPIC);
        let wanted = if multicast {
            RoutingType::Multicast
        } else {
            RoutingType::Anycast
        };
        let info = address_query(session, &address, wanted).map_err(|e| consumer_error(&e))?;
        let Some(types) = info else {
            return Err(not_exists());
        };
        if multicast && !types.multicast {
            return Err(LinkError::new(
                condition::ILLEGAL_STATE,
                format!("Address {address} is not configured for topic support"),
            ));
        }
        if !multicast && !types.anycast && fqqn_queue.is_none() {
            return Err(LinkError::new(
                condition::ILLEGAL_STATE,
                format!("Address {address} is not configured for queue support"),
            ));
        }
    } else {
        let bare = session.remove_prefix(&address).to_owned();
        let default = po.settings(&bare).default_address_routing_type;
        let info = address_query(session, &address, default).map_err(|e| consumer_error(&e))?;
        let Some(types) = info else {
            return Err(not_exists());
        };
        multicast = types.multicast && !types.anycast;
    }

    let mut echoed_filter = Fields::default();
    if let Some((k, v)) = &selector_entry {
        echoed_filter.0.push((k.clone(), v.clone()));
    }

    let result = if multicast {
        if let Some((k, v)) = &no_local_entry {
            let clause = format!(
                "{CONNECTION_ID_PROPERTY}<>'{}'",
                container_id.unwrap_or_default()
            );
            selector = Some(match selector.take() {
                Some(s) => format!("{s} AND {clause}"),
                None => clause,
            });
            echoed_filter.0.push((k.clone(), v.clone()));
        }
        if let Some(q) = &fqqn_queue {
            // An existing queue of a topic, consumed like a queue.
            let name = matching_queue(
                session,
                &address,
                q,
                Some(RoutingType::Multicast),
                selector.clone(),
            )?;
            SenderSource {
                queue: name,
                multicast: false,
                selector,
                browse: false,
                priority,
                temp_queue: None,
                shared,
                global,
                is_volatile: false,
                routing_type: Some(RoutingType::Multicast),
            }
        } else if matches!(
            src.durable,
            TerminusDurability::UnsettledState | TerminusDurability::Configuration
        ) {
            let queue =
                create_queue_name(core_naming, container_id, link_name, shared, global, false);
            let q = po.queue_query(&queue);
            let bare_address = session.remove_prefix(&address).to_owned();
            let mut create = !q.exists;
            if q.exists
                && !is_configuration_managed(session, &queue)
                && (*q.address != *bare_address || q.filter_string != selector)
            {
                if q.consumer_count == 0 {
                    session
                        .delete_queue(&queue)
                        .map_err(|e| consumer_error(&e))?;
                    create = true;
                } else {
                    return Err(LinkError::new(
                        condition::ILLEGAL_STATE,
                        "Unable to recreate subscription, consumers already exist",
                    ));
                }
            }
            if create {
                let mut spec = QueueSpec::new(queue.as_str(), address.as_str());
                spec.routing_type = Some(RoutingType::Multicast);
                spec.filter_string = selector.clone();
                spec.durable = true;
                let r = if shared {
                    session.create_shared_queue(spec)
                } else {
                    spec.max_consumers = Some(1);
                    session.create_queue(spec).map(|_| ())
                };
                match r {
                    Err(e) if e.kind != ErrorType::QueueExists => return Err(consumer_error(&e)),
                    _ => {}
                }
            }
            SenderSource {
                queue,
                multicast: true,
                selector: None,
                browse: false,
                priority,
                temp_queue: None,
                shared,
                global,
                is_volatile: false,
                routing_type: Some(RoutingType::Multicast),
            }
        } else if shared {
            let queue = create_queue_name(core_naming, container_id, link_name, true, global, true);
            let q = po.queue_query(&queue);
            if !(q.exists && *q.address == *address && q.filter_string == selector)
                && !is_configuration_managed(session, &queue)
            {
                let mut spec = QueueSpec::new(queue.as_str(), address.as_str());
                spec.routing_type = Some(RoutingType::Multicast);
                spec.filter_string = selector.clone();
                spec.durable = false;
                match session.create_shared_queue(spec) {
                    Err(e) if e.kind != ErrorType::QueueExists => return Err(consumer_error(&e)),
                    _ => {}
                }
            }
            SenderSource {
                queue,
                multicast: true,
                selector: None,
                browse: false,
                priority,
                temp_queue: None,
                shared,
                global,
                is_volatile: true,
                routing_type: Some(RoutingType::Multicast),
            }
        } else {
            let queue = uuid::Uuid::new_v4().to_string();
            create_temporary_queue(
                session,
                &address,
                &queue,
                RoutingType::Multicast,
                selector.clone(),
            )
            .map_err(|e| {
                LinkError::new(
                    condition::INTERNAL_ERROR,
                    format!("AMQ119001: error creating temporary queue, {}", e.message),
                )
            })?;
            SenderSource {
                queue: queue.clone(),
                multicast: true,
                selector: None,
                browse: false,
                priority,
                temp_queue: Some(queue),
                shared,
                global,
                is_volatile: true,
                routing_type: Some(RoutingType::Multicast),
            }
        }
    } else {
        let queue = match &fqqn_queue {
            // Auto-created as anycast (getMatchingQueue with ANYCAST; the
            // routing type only matters for a queue not there yet).
            Some(q) => matching_queue(session, &address, q, Some(RoutingType::Anycast), None)?,
            None => {
                let bare = session.remove_prefix(&address).to_owned();
                matching_anycast_queue(session, &bare).unwrap_or(address.clone())
            }
        };
        SenderSource {
            queue,
            multicast: false,
            selector,
            browse: src
                .distribution_mode
                .as_ref()
                .is_some_and(|m| m.as_str() == "copy"),
            priority,
            temp_queue: None,
            shared,
            global,
            is_volatile: false,
            routing_type: Some(RoutingType::Anycast),
        }
    };

    // The final check: the queue exists, an anycast one auto-created.
    if !result.multicast || result.temp_queue.is_none() {
        let routing = if result.multicast {
            Some(RoutingType::Multicast)
        } else if fqqn_queue.is_some() {
            None
        } else {
            Some(RoutingType::Anycast)
        };
        if !result.multicast && fqqn_queue.is_none() {
            match session.check_auto_create(&result.queue, routing) {
                Ok(AutoCreate::NotFound) => return Err(not_exists()),
                Err(e) => return Err(consumer_error(&e)),
                Ok(_) => {}
            }
        }
        let q = session.queue_query(&result.queue);
        if !q.exists {
            return Err(not_exists());
        }
        if let Some(rt) = routing
            && !q.auto_created
            && q.routing_type != rt
            && fqqn_queue.is_none()
        {
            return Err(LinkError::new(
                condition::INTERNAL_ERROR,
                format!(
                    "AMQ119005: error creating consumer, Incorrect Routing Type for queried queue {}, expecting: {} while the actual type was: {}",
                    result.queue,
                    rt.name(),
                    q.routing_type.name()
                ),
            ));
        }
    }
    src.filter = (!echoed_filter.is_empty()).then_some(echoed_filter);
    Ok(result)
}

pub(crate) fn no_local_value() -> Value {
    Value::described(
        Descriptor::Code(NO_LOCAL_CODE),
        Value::String("NoLocalFilter{}".into()),
    )
}

fn is_configuration_managed(session: &ServerSession, queue: &str) -> bool {
    session
        .broker()
        .post_office()
        .queue(queue)
        .is_some_and(|q| q.config().configuration_managed)
}

/// `addressQuery(address, routingType, autoCreate=true)`: the address's
/// routing types, after auto-creating it if it is missing.
fn address_query(
    session: &mut ServerSession,
    address: &str,
    routing_type: RoutingType,
) -> Result<Option<RoutingTypes>, BrokerError> {
    let po = session.broker().post_office().clone();
    let bare = session.remove_prefix(address).to_owned();
    if po.address_info(&bare).is_none() && po.settings(&bare).auto_create_addresses {
        session.create_address(address, RoutingTypes::of(routing_type), true)?;
    }
    Ok(po.address_info(&bare).map(|i| i.routing_types))
}

/// `getMatchingQueue` for a fully qualified source `address::queue`.
fn matching_queue(
    session: &mut ServerSession,
    address: &str,
    queue: &str,
    routing_type: Option<RoutingType>,
    filter: Option<String>,
) -> Result<String, LinkError> {
    let po = session.broker().post_office().clone();
    let bare_address = session.remove_prefix(address).to_owned();
    let fqqn = format!("{bare_address}::{queue}");
    let mut q = po.queue_query(queue);
    if !q.exists {
        // Auto-created where the settings allow.
        let rt =
            routing_type.or_else(|| Some(po.settings(&bare_address).default_queue_routing_type));
        if let Err(e) = session.check_auto_create(&fqqn, rt) {
            return Err(consumer_error(&e));
        }
        q = po.queue_query(queue);
    }
    if !q.exists {
        return Err(LinkError::new(
            condition::NOT_FOUND,
            format!("Queue: '{queue}' does not exist"),
        ));
    }
    if *q.address != *bare_address {
        return Err(LinkError::new(
            condition::NOT_FOUND,
            format!("Queue: '{queue}' does not exist for address '{bare_address}'"),
        ));
    }
    if let (Some(sel), Some(existing)) = (&filter, &q.filter_string)
        && sel != existing
    {
        return Err(LinkError::new(
            condition::INTERNAL_ERROR,
            format!(
                "Queue: {queue} filter mismatch [{sel}] is different than existing filter [{existing}]"
            ),
        ));
    }
    Ok(queue.to_owned())
}

/// `ServerSession.getMatchingQueue(address, ANYCAST)`: a local anycast
/// queue bound to the address under another name.
fn matching_anycast_queue(session: &ServerSession, address: &str) -> Option<String> {
    let b = session.binding_query(address, true);
    if !b.exists {
        return None;
    }
    let po = session.broker().post_office();
    if po.queue(address).is_some() {
        return None;
    }
    b.queue_names
        .iter()
        .find(|n| {
            po.queue(n)
                .is_some_and(|q| q.config().routing_type == RoutingType::Anycast)
        })
        .map(|n| n.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_queue_names() {
        let name = |shared, global, vol| {
            create_queue_name(false, Some("cid"), "sub|1", shared, global, vol)
        };
        assert_eq!(name(false, false, false), "cid.sub|1");
        assert_eq!(name(true, false, false), "cid.sub");
        assert_eq!(name(true, true, false), "sub:global");
        assert_eq!(name(true, false, true), "cid.sub:shared-volatile");
        assert_eq!(name(true, true, true), "sub:shared-volatile:global");
        assert_eq!(
            create_queue_name(true, Some("c.d"), "s", false, false, false),
            "c\\.d.s"
        );
        assert_eq!(
            create_queue_name(true, Some("c"), "s", true, false, true),
            "nonDurable.c.s"
        );
    }
}
