//! Bridge policies (`AMQPBridgeAddressPolicy`, `AMQPBridgeQueuePolicy`), as
//! `AMQPBridgeSupport` makes them from the configuration.

use saranga_config::{
    AmqpBridgeAddressPolicy, AmqpBridgeQueuePolicy, TransformerConfig, WildcardConfiguration,
};
use saranga_server::RoutingType;

use super::config::DEFAULT_PRIORITY_ADJUSTMENT_VALUE;
use crate::federation::policy::Pattern;
use crate::types::{Symbol, Value};

/// What both policies have (`AMQPBridgePolicy`).
#[derive(Debug, Clone)]
pub struct Common {
    pub name: String,
    pub priority: Option<i32>,
    pub filter: Option<String>,
    pub remote_address: Option<String>,
    pub remote_address_prefix: Option<String>,
    pub remote_address_suffix: Option<String>,
    /// `toSymbolCollection`: blanks dropped, none is no capabilities.
    pub remote_terminus_capabilities: Vec<Symbol>,
    pub properties: Vec<(String, Value)>,
    pub transformer: Option<TransformerConfig>,
}

impl Common {
    /// The peer's address for a local name: the prefix, the policy's
    /// `remote-address` (or the name), the suffix.
    pub fn remote_address_for(&self, local: &str) -> String {
        let mut out = String::new();
        if let Some(p) = &self.remote_address_prefix {
            out.push_str(p);
        }
        match &self.remote_address {
            Some(a) if !a.trim().is_empty() => out.push_str(a),
            _ => out.push_str(local),
        }
        if let Some(s) = &self.remote_address_suffix {
            out.push_str(s);
        }
        out
    }

    /// `getRemoteTerminusCapabilities`: null when there are none.
    pub fn terminus_capabilities(&self) -> Option<Vec<Symbol>> {
        (!self.remote_terminus_capabilities.is_empty())
            .then(|| self.remote_terminus_capabilities.clone())
    }
}

fn symbols(v: &Option<Vec<String>>) -> Vec<Symbol> {
    let mut out: Vec<Symbol> = Vec::new();
    for s in v.iter().flatten() {
        if s.trim().is_empty() {
            continue;
        }
        let sym = Symbol::from(s.as_str());
        if !out.contains(&sym) {
            out.push(sym);
        }
    }
    out
}

fn string_properties(p: &[(String, String)]) -> Vec<(String, Value)> {
    p.iter()
        .map(|(k, v)| (k.clone(), Value::string(v.clone())))
        .collect()
}

/// `AMQPBridgeAddressPolicy`.
#[derive(Debug, Clone)]
pub struct AddressPolicy {
    pub common: Common,
    pub include_divert_bindings: bool,
    pub use_durable_subscriptions: bool,
    include_matchers: Vec<Pattern>,
    exclude_matchers: Vec<Pattern>,
}

impl AddressPolicy {
    pub fn from_config(e: &AmqpBridgeAddressPolicy, wildcards: &WildcardConfiguration) -> Self {
        Self {
            common: Common {
                name: e.name.clone(),
                priority: e.priority,
                filter: e.filter.clone(),
                remote_address: e.remote_address.clone(),
                remote_address_prefix: e.remote_address_prefix.clone(),
                remote_address_suffix: e.remote_address_suffix.clone(),
                remote_terminus_capabilities: symbols(&e.remote_terminus_capabilities),
                properties: string_properties(&e.properties),
                transformer: e.transformer.clone(),
            },
            include_divert_bindings: e.include_divert_bindings,
            use_durable_subscriptions: e.use_durable_subscriptions,
            include_matchers: e
                .includes
                .iter()
                .map(|a| Pattern::new(a, wildcards))
                .collect(),
            exclude_matchers: e
                .excludes
                .iter()
                .map(|a| Pattern::new(a, wildcards))
                .collect(),
        }
    }

    pub fn name(&self) -> &str {
        &self.common.name
    }

    /// `test(address, routingType)`: multicast addresses only, not
    /// excluded, included.
    pub fn test(&self, address: &str, routing_type: RoutingType) -> bool {
        routing_type == RoutingType::Multicast
            && !self.exclude_matchers.iter().any(|m| m.test(address))
            && self.include_matchers.iter().any(|m| m.test(address))
    }
}

/// `AMQPBridgeQueuePolicy`.
#[derive(Debug, Clone)]
pub struct QueuePolicy {
    pub common: Common,
    pub priority_adjustment: i32,
    include_matchers: Vec<(Pattern, Pattern)>,
    exclude_matchers: Vec<(Pattern, Pattern)>,
}

impl QueuePolicy {
    pub fn from_config(e: &AmqpBridgeQueuePolicy, wildcards: &WildcardConfiguration) -> Self {
        let compile = |v: &[saranga_config::AmqpQueueMatch]| {
            v.iter()
                .map(|m| {
                    (
                        Pattern::new(&m.address_match, wildcards),
                        Pattern::new(&m.queue_match, wildcards),
                    )
                })
                .collect()
        };
        Self {
            common: Common {
                name: e.name.clone(),
                priority: e.priority,
                filter: e.filter.clone(),
                remote_address: e.remote_address.clone(),
                remote_address_prefix: e.remote_address_prefix.clone(),
                remote_address_suffix: e.remote_address_suffix.clone(),
                remote_terminus_capabilities: symbols(&e.remote_terminus_capabilities),
                properties: string_properties(&e.properties),
                transformer: e.transformer.clone(),
            },
            priority_adjustment: e
                .priority_adjustment
                .unwrap_or(DEFAULT_PRIORITY_ADJUSTMENT_VALUE),
            include_matchers: compile(&e.includes),
            exclude_matchers: compile(&e.excludes),
        }
    }

    pub fn name(&self) -> &str {
        &self.common.name
    }

    /// `test(address, queue)`.
    pub fn test(&self, address: &str, queue: &str) -> bool {
        !self
            .exclude_matchers
            .iter()
            .any(|(a, q)| a.test(address) && q.test(queue))
            && self
                .include_matchers
                .iter()
                .any(|(a, q)| a.test(address) && q.test(queue))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use saranga_config::AmqpQueueMatch;

    fn wildcards() -> WildcardConfiguration {
        WildcardConfiguration {
            routing_enabled: true,
            delimiter: '.',
            any_words: '#',
            single_word: '*',
        }
    }

    #[test]
    fn address_policy() {
        let p = AddressPolicy::from_config(
            &AmqpBridgeAddressPolicy {
                name: "p".into(),
                includes: vec!["news.#".into()],
                excludes: vec!["news.secret".into()],
                remote_address_prefix: Some("bridged.".into()),
                remote_terminus_capabilities: Some(vec!["topic".into(), " ".into()]),
                ..Default::default()
            },
            &wildcards(),
        );
        assert!(p.test("news.a", RoutingType::Multicast));
        assert!(!p.test("news.a", RoutingType::Anycast));
        assert!(!p.test("news.secret", RoutingType::Multicast));
        assert!(!p.test("sport", RoutingType::Multicast));
        assert_eq!(p.common.remote_address_for("news.a"), "bridged.news.a");
        assert_eq!(
            p.common.terminus_capabilities(),
            Some(vec![Symbol::from("topic")])
        );
    }

    #[test]
    fn queue_policy() {
        let p = QueuePolicy::from_config(
            &AmqpBridgeQueuePolicy {
                name: "q".into(),
                includes: vec![AmqpQueueMatch {
                    address_match: "".into(),
                    queue_match: "orders".into(),
                }],
                remote_address: Some("incoming".into()),
                remote_address_suffix: Some(".x".into()),
                ..Default::default()
            },
            &wildcards(),
        );
        assert!(p.test("orders", "orders"));
        assert!(!p.test("orders", "returns"));
        assert_eq!(p.priority_adjustment, -1);
        assert_eq!(p.common.remote_address_for("orders"), "incoming.x");
        assert_eq!(p.common.terminus_capabilities(), None);
    }
}
