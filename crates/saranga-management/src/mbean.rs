//! The MBeans Artemis registers, as names and as the management resources
//! behind them: `ObjectName` parsing, quoting, canonical names and
//! patterns, and `ObjectNameBuilder`'s names for the broker, its addresses,
//! queues and diverts.

use std::sync::Arc;

use saranga_server::Broker;

/// An MBean interface: its attributes and operations, as `MBeanInfoHelper`
/// describes them (see `catalog.rs`).
pub struct Control {
    /// The Java interface, for the record.
    #[allow(dead_code)]
    pub interface: &'static str,
    /// The public constructors of the class implementing it, each as its
    /// parameters' types, which `StandardMBean` lists in the MBeanInfo.
    pub ctors: &'static [&'static [&'static str]],
    pub attrs: &'static [Attr],
    pub ops: &'static [Op],
}

pub struct Attr {
    pub name: &'static str,
    pub type_name: &'static str,
    pub desc: &'static str,
    pub readable: bool,
    pub writable: bool,
    pub is: bool,
}

pub struct Op {
    pub name: &'static str,
    pub desc: &'static str,
    /// `MBeanOperationInfo`'s impact, which Jolokia does not list.
    #[allow(dead_code)]
    pub impact: u8,
    pub ret: &'static str,
    pub params: &'static [Param],
}

pub struct Param {
    pub name: &'static str,
    pub type_name: &'static str,
    pub desc: &'static str,
}

impl Op {
    /// `(type,type)`, as Jolokia lists signatures.
    pub fn signature(&self) -> String {
        let types: Vec<&str> = self.params.iter().map(|p| p.type_name).collect();
        format!("({})", types.join(","))
    }
}

impl Control {
    pub fn attr(&self, name: &str) -> Option<&'static Attr> {
        self.attrs.iter().find(|a| a.name == name)
    }

    pub fn ops_named(&self, name: &str) -> Vec<&'static Op> {
        self.ops.iter().filter(|o| o.name == name).collect()
    }
}

/// `ObjectName`: a domain and key properties, possibly a pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectName {
    pub domain: String,
    /// The key properties as written: values keep their quotes.
    pub props: Vec<(String, String)>,
    /// `,*`: other keys may be present.
    pub props_pattern: bool,
}

/// `ObjectName.quote`.
pub fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' | '*' | '?' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `ObjectName.unquote`, or the value itself when it is not quoted.
#[cfg(test)]
pub fn unquote(s: &str) -> String {
    let Some(inner) = s.strip_prefix('"').and_then(|s| s.strip_suffix('"')) else {
        return s.to_owned();
    };
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some(c) => out.push(c),
                None => {}
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn is_value_pattern(v: &str) -> bool {
    if v.starts_with('"') {
        // A quoted value is a pattern when it holds an unescaped * or ?.
        let mut escaped = false;
        for c in v[1..v.len().saturating_sub(1)].chars() {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '*' || c == '?' {
                return true;
            }
        }
        false
    } else {
        v.contains(['*', '?'])
    }
}

/// `*` and `?` wildcards over a whole string.
pub fn glob(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0, 0);
    let (mut star, mut mark) = (None, 0);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

impl ObjectName {
    /// `ObjectName.getInstance`. The error texts are `ObjectName`'s.
    pub fn parse(name: &str) -> Result<Self, String> {
        let Some((domain, rest)) = name.split_once(':') else {
            return Err("Key properties cannot be empty".to_owned());
        };
        if domain.contains(['\n']) {
            return Err("Invalid character '\\n' in domain name".to_owned());
        }
        let mut props = Vec::new();
        let mut props_pattern = false;
        let chars: Vec<char> = rest.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '*' && (i + 1 == chars.len() || chars[i + 1] == ',') {
                props_pattern = true;
                i += 2;
                continue;
            }
            let start = i;
            while i < chars.len() && chars[i] != '=' {
                if matches!(chars[i], ',' | ':' | '*' | '?' | '\n') {
                    return Err(format!(
                        "Invalid character '{}' in key part of property",
                        chars[i]
                    ));
                }
                i += 1;
            }
            if i == chars.len() {
                return Err("Unterminated key property part".to_owned());
            }
            let key: String = chars[start..i].iter().collect();
            if key.is_empty() {
                return Err("Invalid key (empty)".to_owned());
            }
            i += 1;
            let vstart = i;
            if i < chars.len() && chars[i] == '"' {
                i += 1;
                while i < chars.len() && chars[i] != '"' {
                    if chars[i] == '\\' {
                        i += 1;
                    }
                    i += 1;
                }
                if i >= chars.len() {
                    return Err("Missing termination quote".to_owned());
                }
                i += 1;
            } else {
                while i < chars.len() && chars[i] != ',' {
                    if matches!(chars[i], '=' | ':' | '"' | '\n') {
                        return Err(format!(
                            "Invalid character '{}' in value part of property",
                            chars[i]
                        ));
                    }
                    i += 1;
                }
            }
            let value: String = chars[vstart..i].iter().collect();
            if value.is_empty() {
                return Err(format!("Invalid value (empty) for key {key}"));
            }
            if props.iter().any(|(k, _)| *k == key) {
                return Err(format!("key `{key}' already defined"));
            }
            props.push((key, value));
            if i < chars.len() {
                if chars[i] != ',' {
                    return Err("Invalid ending comma".to_owned());
                }
                i += 1;
                if i == chars.len() {
                    return Err("Invalid ending comma".to_owned());
                }
            }
        }
        if props.is_empty() && !props_pattern {
            return Err("Key properties cannot be empty".to_owned());
        }
        Ok(Self {
            domain: domain.to_owned(),
            props,
            props_pattern,
        })
    }

    pub fn is_pattern(&self) -> bool {
        self.props_pattern
            || self.domain.contains(['*', '?'])
            || self.props.iter().any(|(_, v)| is_value_pattern(v))
    }

    /// `getCanonicalName`: the keys in order.
    pub fn canonical(&self) -> String {
        let mut props: Vec<&(String, String)> = self.props.iter().collect();
        props.sort_by(|a, b| a.0.cmp(&b.0));
        let mut list: Vec<String> = props.iter().map(|(k, v)| format!("{k}={v}")).collect();
        if self.props_pattern {
            list.push("*".to_owned());
        }
        format!("{}:{}", self.domain, list.join(","))
    }

    /// `toString`: the keys as written.
    pub fn serialized(&self) -> String {
        let mut list: Vec<String> = self.props.iter().map(|(k, v)| format!("{k}={v}")).collect();
        if self.props_pattern {
            list.push("*".to_owned());
        }
        format!("{}:{}", self.domain, list.join(","))
    }

    /// `getCanonicalKeyPropertyListString`.
    pub fn canonical_props(&self) -> String {
        let c = self.canonical();
        c[self.domain.len() + 1..].to_owned()
    }

    /// The raw value of a key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.props
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// `ObjectName.apply`: does this pattern (or name) match `name`?
    pub fn matches(&self, name: &ObjectName) -> bool {
        if !glob(&self.domain, &name.domain) {
            return false;
        }
        for (k, v) in &self.props {
            let Some(theirs) = name.get(k) else {
                return false;
            };
            if is_value_pattern(v) {
                if !glob(v, theirs) {
                    return false;
                }
            } else if v != theirs {
                return false;
            }
        }
        self.props_pattern || name.props.len() == self.props.len()
    }
}

impl std::fmt::Display for ObjectName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list: Vec<String> = self.props.iter().map(|(k, v)| format!("{k}={v}")).collect();
        if self.props_pattern {
            list.push("*".to_owned());
        }
        write!(f, "{}:{}", self.domain, list.join(","))
    }
}

/// A registered MBean and the management resource it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bean {
    Broker,
    Address(String),
    Queue(String),
    Divert(String),
    /// An acceptor: its name and URI parameters.
    Acceptor(String, Vec<(String, String)>),
    BroadcastGroup(String),
    Bridge(String),
    BrokerConnection(String),
    ClusterConnection(String),
    /// hawtio's permission checks (`HawtioSecurityControl`), which
    /// `artemis run` registers for the console.
    HawtioSecurity,
    /// One of the JDK's platform MBeans, answered from the process.
    Platform(crate::platform::Platform),
}

/// `ObjectNameBuilder.getSecurityObjectName`.
pub const HAWTIO_SECURITY: &str = "hawtio:type=security,area=jmx,name=ArtemisJMXSecurity";

impl Bean {
    pub fn control(&self) -> &'static Control {
        match self {
            Self::Broker => &crate::catalog::BROKER,
            Self::Address(_) => &crate::catalog::ADDRESS,
            Self::Queue(_) => &crate::catalog::QUEUE,
            Self::Divert(_) => &crate::catalog::DIVERT,
            Self::Acceptor(..) => &crate::catalog::ACCEPTOR,
            Self::BroadcastGroup(_) => &crate::catalog::BROADCAST_GROUP,
            Self::Bridge(_) => &crate::catalog::BRIDGE,
            Self::BrokerConnection(_) => &crate::catalog::BROKER_CONNECTION,
            Self::ClusterConnection(_) => &crate::catalog::CLUSTER_CONNECTION,
            Self::HawtioSecurity => &HAWTIO_SECURITY_CONTROL,
            Self::Platform(p) => p.control(),
        }
    }

    /// The management resource name (`ResourceNames`).
    pub fn resource(&self) -> String {
        match self {
            Self::Broker => "broker".to_owned(),
            Self::Address(a) => format!("address.{a}"),
            Self::Queue(q) => format!("queue.{q}"),
            Self::Divert(d) => format!("divert.{d}"),
            Self::Acceptor(a, _) => format!("acceptor.{a}"),
            Self::BroadcastGroup(g) => format!("broadcastgroup.{g}"),
            Self::Bridge(b) => format!("bridge.{b}"),
            Self::BrokerConnection(c) => format!("brokerconnection.{c}"),
            Self::ClusterConnection(c) => format!("clusterconnection.{c}"),
            Self::HawtioSecurity => "hawtio.security".to_owned(),
            Self::Platform(p) => format!("java.lang.{}", p.type_name()),
        }
    }

    /// The implementing class, as `list` names it.
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Broker => {
                "org.apache.activemq.artemis.core.management.impl.ActiveMQServerControlImpl"
            }
            Self::Address(_) => {
                "org.apache.activemq.artemis.core.management.impl.AddressControlImpl"
            }
            Self::Queue(_) => "org.apache.activemq.artemis.core.management.impl.QueueControlImpl",
            Self::Divert(_) => "org.apache.activemq.artemis.core.management.impl.DivertControlImpl",
            Self::Acceptor(..) => {
                "org.apache.activemq.artemis.core.management.impl.AcceptorControlImpl"
            }
            Self::BroadcastGroup(_) => {
                "org.apache.activemq.artemis.core.management.impl.BroadcastGroupControlImpl"
            }
            Self::Bridge(_) => "org.apache.activemq.artemis.core.management.impl.BridgeControlImpl",
            Self::BrokerConnection(_) => {
                "org.apache.activemq.artemis.core.management.impl.BrokerConnectionControlImpl"
            }
            Self::ClusterConnection(_) => {
                "org.apache.activemq.artemis.core.management.impl.ClusterConnectionControlImpl"
            }
            Self::HawtioSecurity => {
                "org.apache.activemq.artemis.core.server.management.impl.HawtioSecurityControlImpl"
            }
            Self::Platform(p) => p.class_name(),
        }
    }
}

/// The MBeans of a broker, by name (`ObjectNameBuilder`): the broker, its
/// acceptors, then each address, queue and divert, hawtio's permission
/// checks and the platform MBeans.
pub fn registered(
    broker: &Arc<Broker>,
    domain: &str,
    broker_name: &str,
    acceptors: &[(String, Vec<(String, String)>)],
) -> Vec<(ObjectName, Bean)> {
    let base = format!("{domain}:broker={}", quote(broker_name));
    let name = |s: String| ObjectName::parse(&s).ok();
    let mut out = Vec::new();
    if let Some(n) = name(base.clone()) {
        out.push((n, Bean::Broker));
    }
    for (a, params) in acceptors {
        if let Some(n) = name(format!("{base},component=acceptors,name={}", quote(a))) {
            out.push((n, Bean::Acceptor(a.clone(), params.clone())));
        }
    }
    for g in broker.broadcast_groups() {
        let g = &g.config().name;
        if let Some(n) = name(format!(
            "{base},component=broadcast-groups,name={}",
            quote(g)
        )) {
            out.push((n, Bean::BroadcastGroup(g.clone())));
        }
    }
    for c in broker.clusters() {
        let c = c.config().name.clone();
        if let Some(n) = name(format!(
            "{base},component=cluster-connections,name={}",
            quote(&c)
        )) {
            out.push((n, Bean::ClusterConnection(c)));
        }
    }
    for b in broker.bridges().names() {
        if let Some(n) = name(format!("{base},component=bridges,name={}", quote(&b))) {
            out.push((n, Bean::Bridge(b)));
        }
    }
    for c in broker.broker_connections() {
        let c = c.name().to_owned();
        if let Some(n) = name(format!(
            "{base},component=broker-connections,name={}",
            quote(&c)
        )) {
            out.push((n, Bean::BrokerConnection(c)));
        }
    }
    let po = broker.post_office();
    for a in po.addresses() {
        if let Some(n) = name(format!(
            "{base},component=addresses,address={}",
            quote(&a.name)
        )) {
            out.push((n, Bean::Address(a.name.to_string())));
        }
    }
    for q in po.queues() {
        let c = q.config();
        if let Some(n) = name(format!(
            "{base},component=addresses,address={},subcomponent=queues,routing-type={},queue={}",
            quote(&c.address),
            quote(&c.routing_type.name().to_lowercase()),
            quote(q.name())
        )) {
            out.push((n, Bean::Queue(q.name().to_string())));
        }
    }
    for d in po.diverts() {
        if let Some(n) = name(format!(
            "{base},component=addresses,address={},subcomponent=diverts,divert={}",
            quote(&d.address),
            quote(&d.name)
        )) {
            out.push((n, Bean::Divert(d.name.to_string())));
        }
    }
    if let Some(n) = name(HAWTIO_SECURITY.to_owned()) {
        out.push((n, Bean::HawtioSecurity));
    }
    for p in crate::platform::Platform::ALL {
        if let Some(n) = name(format!("java.lang:type={}", p.type_name())) {
            out.push((n, Bean::Platform(p)));
        }
    }
    out
}

/// `HawtioSecurityControl`, the console's permission checks
/// (`hawtio:type=security,area=jmx,name=ArtemisJMXSecurity`).
pub static HAWTIO_SECURITY_CONTROL: Control = Control {
    interface: "HawtioSecurityControl",
    ctors: crate::catalog::HAWTIO_SECURITY_CTORS,
    attrs: &[],
    ops: &[
        Op {
            name: "canInvoke",
            desc: "Can invoke a number of Objects",
            impact: 1,
            ret: "javax.management.openmbean.TabularData",
            params: &[Param {
                name: "p 1",
                type_name: "java.util.Map",
                desc: "parameter 1",
            }],
        },
        Op {
            name: "canInvoke",
            desc: "Can invoke an Object",
            impact: 1,
            ret: "boolean",
            params: &[
                Param {
                    name: "p 1",
                    type_name: "java.lang.String",
                    desc: "parameter 1",
                },
                Param {
                    name: "p 2",
                    type_name: "java.lang.String",
                    desc: "parameter 2",
                },
                Param {
                    name: "p 3",
                    type_name: "[Ljava.lang.String;",
                    desc: "parameter 3",
                },
            ],
        },
        Op {
            name: "canInvoke",
            desc: "Can invoke an Object",
            impact: 1,
            ret: "boolean",
            params: &[
                Param {
                    name: "p 1",
                    type_name: "java.lang.String",
                    desc: "parameter 1",
                },
                Param {
                    name: "p 2",
                    type_name: "java.lang.String",
                    desc: "parameter 2",
                },
            ],
        },
        Op {
            name: "canInvoke",
            desc: "Can invoke an Object",
            impact: 1,
            ret: "boolean",
            params: &[Param {
                name: "p 1",
                type_name: "java.lang.String",
                desc: "parameter 1",
            }],
        },
    ],
};

#[cfg(test)]
mod tests {
    use super::{ObjectName, glob, quote, unquote};

    #[test]
    fn names() {
        let n = ObjectName::parse(
            "org.apache.activemq.artemis:broker=\"0.0.0.0\",component=addresses,address=\"DLQ\"",
        )
        .unwrap();
        assert_eq!(
            n.canonical(),
            "org.apache.activemq.artemis:address=\"DLQ\",broker=\"0.0.0.0\",component=addresses"
        );
        let p =
            ObjectName::parse("org.apache.activemq.artemis:broker=*,component=addresses,address=*")
                .unwrap();
        assert!(p.is_pattern());
        assert_eq!(
            p.canonical(),
            "org.apache.activemq.artemis:address=*,broker=*,component=addresses"
        );
        assert!(p.matches(&n));
        let all = ObjectName::parse("org.apache.activemq.artemis:*").unwrap();
        assert!(all.matches(&n));
        assert_eq!(all.canonical(), "org.apache.activemq.artemis:*");
        assert!(
            !ObjectName::parse("org.apache.activemq.artemis:broker=*")
                .unwrap()
                .matches(&n)
        );
        assert_eq!(
            ObjectName::parse("nocolon").unwrap_err(),
            "Key properties cannot be empty"
        );
    }

    #[test]
    fn quoting() {
        assert_eq!(quote("a\"b*"), "\"a\\\"b\\*\"");
        assert_eq!(unquote(&quote("a\"b*?\\\n")), "a\"b*?\\\n");
        assert!(glob("a*c", "abbc"));
        assert!(!glob("a?c", "abbc"));
    }
}
