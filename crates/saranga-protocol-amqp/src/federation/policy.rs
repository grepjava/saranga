//! Federation policies (`FederationReceiveFromAddressPolicy`,
//! `FederationReceiveFromQueuePolicy`) and the control and event messages
//! that carry them (`AMQPFederationPolicySupport`,
//! `AMQPFederationEventSupport`).

use saranga_config::{
    AmqpFederationAddressPolicy, AmqpFederationQueuePolicy, Match, TransformerConfig,
    WildcardConfiguration,
};
use saranga_server::RoutingType;

use super::constants::*;
use crate::message::{AmqpMessage, encode_section};
use crate::messaging::section;
use crate::types::Value;

/// `DEFAULT_QUEUE_RECEIVER_PRIORITY_ADJUSTMENT`.
pub const DEFAULT_PRIORITY_ADJUSTMENT: i32 = -1;

/// `Match.getPattern().asPredicate()`: anchored at the end only.
fn finds(m: &Match, name: &str) -> bool {
    saranga_server::federation::pattern_finds(m, name)
}

/// An address or queue name pattern; empty matches anything
/// (`AddressMatcher`, `QueueMatcher`).
#[derive(Debug, Clone)]
struct Pattern {
    text: String,
    matcher: Option<Match>,
}

impl Pattern {
    fn new(text: &str, wildcards: &WildcardConfiguration) -> Self {
        Self {
            text: text.to_owned(),
            matcher: (!text.is_empty()).then(|| Match::new(text, wildcards)),
        }
    }

    fn test(&self, name: &str) -> bool {
        self.matcher.as_ref().is_none_or(|m| finds(m, name))
    }
}

/// `FederationReceiveFromAddressPolicy`.
#[derive(Debug, Clone)]
pub struct AddressPolicy {
    pub name: String,
    pub auto_delete: bool,
    pub auto_delete_delay: i64,
    pub auto_delete_message_count: i64,
    pub max_hops: i32,
    pub enable_divert_bindings: bool,
    pub allow_wildcard_groupings: bool,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
    /// `policy-properties-map`, values as given.
    pub properties: Vec<(String, Value)>,
    pub transformer: Option<TransformerConfig>,
    include_matchers: Vec<Pattern>,
    exclude_matchers: Vec<Pattern>,
    any_words: char,
}

impl AddressPolicy {
    /// `AMQPFederationPolicySupport.create(AMQPFederationAddressPolicyElement)`.
    pub fn from_config(e: &AmqpFederationAddressPolicy, wildcards: &WildcardConfiguration) -> Self {
        Self::new(
            e.name.clone(),
            e.auto_delete.unwrap_or(false),
            e.auto_delete_delay.unwrap_or(0),
            e.auto_delete_message_count.unwrap_or(0),
            e.max_hops,
            e.enable_divert_bindings.unwrap_or(false),
            e.allow_wildcard_groupings.unwrap_or(false),
            e.includes.clone(),
            e.excludes.clone(),
            string_properties(&e.properties),
            e.transformer.clone(),
            wildcards,
        )
    }

    #[allow(clippy::too_many_arguments)] // the policy's constructor
    pub fn new(
        name: String,
        auto_delete: bool,
        auto_delete_delay: i64,
        auto_delete_message_count: i64,
        max_hops: i32,
        enable_divert_bindings: bool,
        allow_wildcard_groupings: bool,
        includes: Vec<String>,
        excludes: Vec<String>,
        properties: Vec<(String, Value)>,
        transformer: Option<TransformerConfig>,
        wildcards: &WildcardConfiguration,
    ) -> Self {
        Self {
            include_matchers: includes
                .iter()
                .map(|a| Pattern::new(a, wildcards))
                .collect(),
            exclude_matchers: excludes
                .iter()
                .map(|a| Pattern::new(a, wildcards))
                .collect(),
            any_words: wildcards.any_words,
            name,
            auto_delete,
            auto_delete_delay,
            auto_delete_message_count,
            max_hops,
            enable_divert_bindings,
            allow_wildcard_groupings,
            includes,
            excludes,
            properties,
            transformer,
        }
    }

    /// `test(address, routingType)`: multicast addresses only, not
    /// excluded, included.
    pub fn test(&self, address: &str, routing_type: RoutingType) -> bool {
        self.first_matching_pattern(address, routing_type).is_some()
    }

    /// `getFirstMatchingAddressPattern`.
    pub fn first_matching_pattern(
        &self,
        address: &str,
        routing_type: RoutingType,
    ) -> Option<String> {
        if routing_type != RoutingType::Multicast {
            return None;
        }
        if self.exclude_matchers.iter().any(|m| m.test(address)) {
            return None;
        }
        self.include_matchers
            .iter()
            .find(|m| m.test(address))
            .map(|m| {
                if m.text.is_empty() {
                    self.any_words.to_string()
                } else {
                    m.text.clone()
                }
            })
    }
}

/// `FederationReceiveFromQueuePolicy`.
#[derive(Debug, Clone)]
pub struct QueuePolicy {
    pub name: String,
    pub include_federated: bool,
    pub priority_adjustment: i32,
    /// (address match, queue match) pairs.
    pub includes: Vec<(String, String)>,
    pub excludes: Vec<(String, String)>,
    pub properties: Vec<(String, Value)>,
    pub transformer: Option<TransformerConfig>,
    include_matchers: Vec<(Pattern, Pattern)>,
    exclude_matchers: Vec<(Pattern, Pattern)>,
}

impl QueuePolicy {
    /// `AMQPFederationPolicySupport.create(AMQPFederationQueuePolicyElement)`.
    pub fn from_config(e: &AmqpFederationQueuePolicy, wildcards: &WildcardConfiguration) -> Self {
        let pairs = |v: &[saranga_config::AmqpQueueMatch]| {
            v.iter()
                .map(|m| (m.address_match.clone(), m.queue_match.clone()))
                .collect()
        };
        Self::new(
            e.name.clone(),
            e.include_federated,
            e.priority_adjustment.unwrap_or(DEFAULT_PRIORITY_ADJUSTMENT),
            pairs(&e.includes),
            pairs(&e.excludes),
            string_properties(&e.properties),
            e.transformer.clone(),
            wildcards,
        )
    }

    #[allow(clippy::too_many_arguments)] // the policy's constructor
    pub fn new(
        name: String,
        include_federated: bool,
        priority_adjustment: i32,
        includes: Vec<(String, String)>,
        excludes: Vec<(String, String)>,
        properties: Vec<(String, Value)>,
        transformer: Option<TransformerConfig>,
        wildcards: &WildcardConfiguration,
    ) -> Self {
        let compile = |v: &[(String, String)]| {
            v.iter()
                .map(|(a, q)| (Pattern::new(a, wildcards), Pattern::new(q, wildcards)))
                .collect()
        };
        Self {
            include_matchers: compile(&includes),
            exclude_matchers: compile(&excludes),
            name,
            include_federated,
            priority_adjustment,
            includes,
            excludes,
            properties,
            transformer,
        }
    }

    /// `test(address, queue)`.
    pub fn test(&self, address: &str, queue: &str) -> bool {
        if self
            .exclude_matchers
            .iter()
            .any(|(a, q)| a.test(address) && q.test(queue))
        {
            return false;
        }
        self.include_matchers
            .iter()
            .any(|(a, q)| a.test(address) && q.test(queue))
    }

    /// `testQueue(queue)`.
    pub fn test_queue(&self, queue: &str) -> bool {
        if self.exclude_matchers.iter().any(|(_, q)| q.test(queue)) {
            return false;
        }
        self.include_matchers.iter().any(|(_, q)| q.test(queue))
    }
}

fn string_properties(p: &[(String, String)]) -> Vec<(String, Value)> {
    p.iter()
        .map(|(k, v)| (k.clone(), Value::string(v.clone())))
        .collect()
}

/// `generateAddressFilter`: messages that have made fewer than `max_hops`
/// hops, by the AMQP annotation or the Core property.
pub fn address_filter(max_hops: i32) -> Option<String> {
    (max_hops > 0).then(|| {
        format!(
            "((\"m.{MESSAGE_HOPS_ANNOTATION}\" IS NULL OR \"m.{MESSAGE_HOPS_ANNOTATION}\"<{max_hops}) AND ({MESSAGE_HOPS_PROPERTY} IS NULL OR {MESSAGE_HOPS_PROPERTY}<{max_hops}))"
        )
    })
}

// ------------------------------------------------------------ encoding

/// A `java.util.HashMap<String, ?>`'s entries in Java's iteration order.
pub fn java_hash_order<V>(entries: Vec<(String, V)>) -> Vec<(String, V)> {
    let hashes: Vec<i32> = entries
        .iter()
        .map(|(k, _)| {
            k.encode_utf16()
                .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(i32::from(c)))
        })
        .collect();
    let order = saranga_buffer::java_hash::hash_map_order(&hashes, None);
    let mut slots: Vec<Option<(String, V)>> = entries.into_iter().map(Some).collect();
    order.into_iter().filter_map(|i| slots[i].take()).collect()
}

fn string_map(entries: Vec<(String, Value)>) -> Value {
    Value::Map(
        entries
            .into_iter()
            .map(|(k, v)| (Value::string(k), v))
            .collect(),
    )
}

/// A message of message annotations and an `amqp-value` map body, as
/// the control and event messages are.
fn annotated_map(annotation: &str, kind: &str, body: Vec<(String, Value)>) -> Vec<u8> {
    let mut out = Vec::new();
    encode_section(
        section::MESSAGE_ANNOTATIONS,
        Value::Map(vec![(Value::symbol(annotation), Value::string(kind))]),
        &mut out,
    );
    encode_section(section::AMQP_VALUE, string_map(body), &mut out);
    out
}

fn transformer_entries(t: &Option<TransformerConfig>, map: &mut Vec<(String, Value)>) {
    if let Some(t) = t {
        map.push((
            TRANSFORMER_CLASS_NAME.into(),
            Value::string(t.class_name.clone()),
        ));
        if !t.properties.is_empty() {
            map.push((
                TRANSFORMER_PROPERTIES_MAP.into(),
                string_map(java_hash_order(
                    t.properties
                        .iter()
                        .map(|(k, v)| (k.clone(), Value::string(v.clone())))
                        .collect(),
                )),
            ));
        }
    }
}

fn properties_entry(p: &[(String, Value)], map: &mut Vec<(String, Value)>) {
    if !p.is_empty() {
        map.push((
            POLICY_PROPERTIES_MAP.into(),
            string_map(java_hash_order(p.to_vec())),
        ));
    }
}

/// `encodeQueuePolicyControlMessage`.
pub fn encode_queue_policy(p: &QueuePolicy) -> Vec<u8> {
    let mut map: Vec<(String, Value)> = vec![
        (POLICY_NAME.into(), Value::string(p.name.clone())),
        (
            QUEUE_INCLUDE_FEDERATED.into(),
            Value::Boolean(p.include_federated),
        ),
        (
            QUEUE_PRIORITY_ADJUSTMENT.into(),
            Value::Int(p.priority_adjustment),
        ),
    ];
    let flatten = |v: &[(String, String)]| {
        Value::List(
            v.iter()
                .flat_map(|(a, q)| [Value::string(a.clone()), Value::string(q.clone())])
                .collect(),
        )
    };
    if !p.includes.is_empty() {
        map.push((QUEUE_INCLUDES.into(), flatten(&p.includes)));
    }
    if !p.excludes.is_empty() {
        map.push((QUEUE_EXCLUDES.into(), flatten(&p.excludes)));
    }
    properties_entry(&p.properties, &mut map);
    transformer_entries(&p.transformer, &mut map);
    annotated_map(OPERATION_TYPE, ADD_QUEUE_POLICY, map)
}

/// `encodeAddressPolicyControlMessage`.
pub fn encode_address_policy(p: &AddressPolicy) -> Vec<u8> {
    let mut map: Vec<(String, Value)> = vec![
        (POLICY_NAME.into(), Value::string(p.name.clone())),
        (ADDRESS_AUTO_DELETE.into(), Value::Boolean(p.auto_delete)),
        (
            ADDRESS_AUTO_DELETE_DELAY.into(),
            Value::Long(p.auto_delete_delay),
        ),
        (
            ADDRESS_AUTO_DELETE_MSG_COUNT.into(),
            Value::Long(p.auto_delete_message_count),
        ),
        (ADDRESS_MAX_HOPS.into(), Value::Int(p.max_hops)),
        (
            ADDRESS_ENABLE_DIVERT_BINDINGS.into(),
            Value::Boolean(p.enable_divert_bindings),
        ),
        (
            ADDRESS_ALLOW_WILDCARD_GROUPINGS.into(),
            Value::Boolean(p.allow_wildcard_groupings),
        ),
    ];
    let list = |v: &[String]| Value::List(v.iter().map(|s| Value::string(s.clone())).collect());
    if !p.includes.is_empty() {
        map.push((ADDRESS_INCLUDES.into(), list(&p.includes)));
    }
    if !p.excludes.is_empty() {
        map.push((ADDRESS_EXCLUDES.into(), list(&p.excludes)));
    }
    properties_entry(&p.properties, &mut map);
    transformer_entries(&p.transformer, &mut map);
    annotated_map(OPERATION_TYPE, ADD_ADDRESS_POLICY, map)
}

/// `encodeQueueAddedEvent`.
pub fn encode_queue_added(address: &str, queue: &str) -> Vec<u8> {
    annotated_map(
        EVENT_TYPE,
        REQUESTED_QUEUE_ADDED,
        vec![
            (REQUESTED_ADDRESS_NAME.into(), Value::string(address)),
            (REQUESTED_QUEUE_NAME.into(), Value::string(queue)),
        ],
    )
}

/// `encodeAddressAddedEvent`.
pub fn encode_address_added(address: &str) -> Vec<u8> {
    annotated_map(
        EVENT_TYPE,
        REQUESTED_ADDRESS_ADDED,
        vec![(REQUESTED_ADDRESS_NAME.into(), Value::string(address))],
    )
}

// ------------------------------------------------------------ decoding

/// A command or event a control or events link carried.
#[derive(Debug)]
pub enum Received {
    AddQueuePolicy(QueuePolicy),
    AddAddressPolicy(AddressPolicy),
    QueueAdded { address: String, queue: String },
    AddressAdded { address: String },
}

/// The body's map: `amqp-value` holding a map with string keys.
fn body_map(m: &AmqpMessage, what: &str) -> Result<Vec<(String, Value)>, String> {
    let malformed = |s: &str| format!("AMQ{what}: {s}");
    let Some((code, body)) = m.body() else {
        return Err(malformed("Message body was not an AmqpValue type"));
    };
    if code != section::AMQP_VALUE {
        return Err(malformed("Message body was not an AmqpValue type"));
    }
    let Value::Map(entries) = body else {
        return Err(malformed(
            "Message body AmqpValue did not carry an encoded Map",
        ));
    };
    Ok(entries
        .into_iter()
        .filter_map(|(k, v)| k.as_str().map(|k| (k.to_owned(), v)))
        .collect())
}

fn get<'a>(map: &'a [(String, Value)], key: &str) -> Option<&'a Value> {
    map.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

fn value_properties(v: Option<&Value>) -> Vec<(String, Value)> {
    match v {
        Some(Value::Map(m)) => m
            .iter()
            .filter_map(|(k, v)| k.as_str().map(|k| (k.to_owned(), v.clone())))
            .collect(),
        _ => Vec::new(),
    }
}

fn transformer_of(map: &[(String, Value)]) -> Option<TransformerConfig> {
    let class = get(map, TRANSFORMER_CLASS_NAME)?.as_str()?.to_owned();
    let properties = value_properties(get(map, TRANSFORMER_PROPERTIES_MAP))
        .into_iter()
        .map(|(k, v)| {
            let v = v.as_str().map(str::to_owned).unwrap_or_default();
            (k, v)
        })
        .collect();
    Some(TransformerConfig {
        class_name: class,
        properties,
    })
}

/// The control message's command (`AMQPFederationCommandProcessor`).
pub fn decode_command(
    m: &AmqpMessage,
    wildcards: &WildcardConfiguration,
) -> Result<Received, String> {
    let op = m.annotation(OPERATION_TYPE).and_then(Value::as_str);
    let control = "119026: Malformed Federation control message";
    match op {
        Some(ADD_QUEUE_POLICY) => {
            let map = body_map(m, control)?;
            let name = get(&map, POLICY_NAME)
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    format!("AMQ{control}: Message body did not carry the required policy name")
                })?
                .to_owned();
            let flattened = |key: &str| -> Result<Vec<(String, String)>, String> {
                match get(&map, key) {
                    None | Some(Value::Null) => Ok(Vec::new()),
                    Some(Value::List(l)) => {
                        if l.len() % 2 != 0 {
                            return Err(format!(
                                "AMQ{control}: Encoded queue policy entry was must contain an even number of elements : {key}"
                            ));
                        }
                        let mut out: Vec<(String, String)> = Vec::new();
                        for pair in l.chunks(2) {
                            let s = |v: &Value| v.as_str().unwrap_or_default().to_owned();
                            let e = (s(&pair[0]), s(&pair[1]));
                            if !out.contains(&e) {
                                out.push(e);
                            }
                        }
                        Ok(out)
                    }
                    Some(_) => Err(format!(
                        "AMQ{control}: Encoded queue policy entry was not the expected List type : {key}"
                    )),
                }
            };
            Ok(Received::AddQueuePolicy(QueuePolicy::new(
                name,
                get(&map, QUEUE_INCLUDE_FEDERATED)
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                get(&map, QUEUE_PRIORITY_ADJUSTMENT)
                    .and_then(Value::as_i64)
                    .map_or(DEFAULT_PRIORITY_ADJUSTMENT, |v| v as i32),
                flattened(QUEUE_INCLUDES)?,
                flattened(QUEUE_EXCLUDES)?,
                value_properties(get(&map, POLICY_PROPERTIES_MAP)),
                transformer_of(&map),
                wildcards,
            )))
        }
        Some(ADD_ADDRESS_POLICY) => {
            let map = body_map(m, control)?;
            let name = get(&map, POLICY_NAME)
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    format!("AMQ{control}: Message body did not carry the required policy name")
                })?
                .to_owned();
            let max_hops = get(&map, ADDRESS_MAX_HOPS)
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    format!("AMQ{control}: Message body did not carry the required max hops configuration")
                })? as i32;
            let list = |key: &str| -> Vec<String> {
                let mut out = Vec::new();
                if let Some(Value::List(l)) = get(&map, key) {
                    for v in l {
                        let s = v.as_str().unwrap_or_default().to_owned();
                        if !out.contains(&s) {
                            out.push(s);
                        }
                    }
                }
                out
            };
            let flag = |key: &str| get(&map, key).and_then(Value::as_bool).unwrap_or(false);
            let long = |key: &str| get(&map, key).and_then(Value::as_i64).unwrap_or(0);
            Ok(Received::AddAddressPolicy(AddressPolicy::new(
                name,
                flag(ADDRESS_AUTO_DELETE),
                long(ADDRESS_AUTO_DELETE_DELAY),
                long(ADDRESS_AUTO_DELETE_MSG_COUNT),
                max_hops,
                flag(ADDRESS_ENABLE_DIVERT_BINDINGS),
                flag(ADDRESS_ALLOW_WILDCARD_GROUPINGS),
                list(ADDRESS_INCLUDES),
                list(ADDRESS_EXCLUDES),
                value_properties(get(&map, POLICY_PROPERTIES_MAP)),
                transformer_of(&map),
                wildcards,
            )))
        }
        _ => Err("Remote sent unknown command.".into()),
    }
}

/// The event message's event (`AMQPFederationEventProcessor`).
pub fn decode_event(m: &AmqpMessage) -> Result<Received, String> {
    let kind = m.annotation(EVENT_TYPE).and_then(Value::as_str);
    let event = "119028: Malformed Federation event message";
    let text = |map: &[(String, Value)], key: &str, what: &str| -> Result<String, String> {
        match get(map, key) {
            Some(v) => Ok(crate::describe::to_string(v)),
            None => Err(format!(
                "AMQ{event}: Message body did not carry the required {what}"
            )),
        }
    };
    match kind {
        Some(REQUESTED_QUEUE_ADDED) => {
            let map = body_map(m, "119026: Malformed Federation control message")?;
            Ok(Received::QueueAdded {
                address: text(&map, REQUESTED_ADDRESS_NAME, "address name")?,
                queue: text(&map, REQUESTED_QUEUE_NAME, "queue name")?,
            })
        }
        Some(REQUESTED_ADDRESS_ADDED) => {
            let map = body_map(m, "119026: Malformed Federation control message")?;
            Ok(Received::AddressAdded {
                address: text(&map, REQUESTED_ADDRESS_NAME, "address name")?,
            })
        }
        _ => Err("Remote sent unknown event.".into()),
    }
}
