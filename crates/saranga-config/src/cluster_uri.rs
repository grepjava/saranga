//! Cluster connections made from a URI (`<cluster-connection-uri>`,
//! `ClusterConnectionConfigurationParser`) and the bean properties a URI's
//! query and broker properties (`clusterConfigurations.<name>.<property>`)
//! set on them.

use crate::acceptor::percent_decode;
use crate::{AcceptorConfig, ClusterConnectionConfig, ConfigError, MessageLoadBalancing, Result};

/// `new ClusterConnectionConfiguration(new URI(uri)).setName(name)`:
/// `static://(<connector URI>,...)?<properties>` connects to its members,
/// `multicast://<discovery group>?<properties>` finds them through the
/// group; a composite `multicast://(...)` is a `static://` one.
pub fn from_uri(name: &str, uri: &str) -> Result<ClusterConnectionConfig> {
    from_uri_parts(name, uri).map(|(c, _)| c)
}

/// [`from_uri`], with the query of a static (composite) URI as the
/// `CompositeData` the schema keeps (`compositeMembers`): its parameters,
/// in the order given.
pub fn from_uri_parts(name: &str, uri: &str) -> Result<(ClusterConnectionConfig, Option<Params>)> {
    let fail = |reason: String| ConfigError::InvalidUri {
        uri: uri.to_owned(),
        reason,
    };
    // `new URI` takes no whitespace.
    if let Some(i) = uri.find(char::is_whitespace) {
        return Err(fail(format!("Illegal character at index {i}")));
    }
    let colon = uri.find(':').ok_or_else(|| fail("no scheme".to_owned()))?;
    let scheme = &uri[..colon];
    if scheme != "static" && scheme != "multicast" {
        return Err(fail(format!("Schema {scheme} not found")));
    }
    // The scheme-specific part, without its fragment.
    let ssp = uri[colon + 1..].split('#').next().unwrap_or_default();
    let ssp = ssp.trim();
    let ssp = ssp.strip_prefix("//").unwrap_or(ssp).trim();
    let mut c = ClusterConnectionConfig::new(name, "");
    let composite = ssp.starts_with('(') && balanced(ssp);
    let static_schema = scheme == "static" || composite;
    let params = if scheme == "static" || composite {
        // `ClusterConnectionStaticSchema`: `URISupport.parseComposite`.
        let (members, params) = parse_composite(ssp).map_err(fail)?;
        for m in members {
            c.static_members
                .push(AcceptorConfig::parse(None, m.trim())?);
        }
        params
    } else {
        // `ClusterConnectionMulticastSchema`: the group is the URI's host.
        let (authority, query) = match ssp.find('?') {
            Some(q) => (&ssp[..q], Some(&ssp[q + 1..])),
            None => (ssp, None),
        };
        let authority = authority.split('/').next().unwrap_or_default();
        c.discovery_group = java_host(authority);
        match query {
            Some(q) => parse_query(&percent_decode(q, false).map_err(fail)?).map_err(fail)?,
            None => Vec::new(),
        }
    };
    for (key, value) in &params {
        set_property(&mut c, key, value.as_deref(), false).map_err(|reason| {
            ConfigError::invalid(key, value.as_deref().unwrap_or("null"), reason)
        })?;
    }
    // `setName` comes after the URI's own properties.
    c.name = name.to_owned();
    Ok((c, static_schema.then_some(params)))
}

/// `URISupport.checkParenthesis`: as many `(` as `)`.
fn balanced(s: &str) -> bool {
    s.matches('(').count() == s.matches(')').count()
}

type Params = Vec<(String, Option<String>)>;

/// `URISupport.parseComposite`: the members in the outermost parentheses
/// (or the whole part, without them), and the query after them.
fn parse_composite(ssp: &str) -> std::result::Result<(Vec<&str>, Params), String> {
    if !balanced(ssp) {
        return Err("Not a matching number of '(' and ')' parenthesis".to_owned());
    }
    let (components, params) = if ssp.starts_with('(') {
        let close = matching_paren(ssp).ok_or("URI did not contain a matching parenthesis.")?;
        (&ssp[1..close], ssp[close + 1..].trim())
    } else {
        (ssp, "")
    };
    let members = split_components(components);
    let params = match params.find('?') {
        Some(p) => parse_query(&params[p + 1..])?,
        None => Vec::new(),
    };
    Ok((members, params))
}

/// `URISupport.indexOfParenthesisMatch` of the first character.
fn matching_paren(s: &str) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// `URISupport.splitComponents`: on the commas outside parentheses.
fn split_components(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let (mut last, mut depth) = (0, 0i32);
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                out.push(&s[last..i]);
                last = i + 1;
            }
            _ => {}
        }
    }
    if last < s.len() {
        out.push(&s[last..]);
    }
    out
}

/// `URISupport.parseQuery`: after the last `?`, split on `&` and `;`, each
/// name and value URL-decoded; a parameter without `=` has no value, and a
/// repeated one keeps its last (a `HashMap`).
fn parse_query(query: &str) -> std::result::Result<Params, String> {
    let query = query.rsplit('?').next().unwrap_or_default();
    let mut out: Params = Vec::new();
    if query.is_empty() {
        return Ok(out);
    }
    for parameter in query.split(['&', ';']) {
        let (k, v) = match parameter.find('=') {
            Some(p) => (
                percent_decode(&parameter[..p], true)?,
                Some(percent_decode(&parameter[p + 1..], true)?),
            ),
            None => (parameter.to_owned(), None),
        };
        match out.iter_mut().find(|(key, _)| *key == k) {
            Some(e) => e.1 = v,
            None => out.push((k, v)),
        }
    }
    Ok(out)
}

/// `URI.getHost`: the authority's host, when it is a valid host name
/// (`my_group` is not, so a group named so is no group).
fn java_host(authority: &str) -> Option<String> {
    let host = authority.rsplit('@').next().unwrap_or_default();
    let host = match host.rfind(':') {
        Some(i) if host[i + 1..].chars().all(|c| c.is_ascii_digit()) => &host[..i],
        _ => host,
    };
    let label_ok = |l: &str| {
        !l.is_empty()
            && !l.starts_with('-')
            && !l.ends_with('-')
            && l.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    };
    let labels = host.strip_suffix('.').unwrap_or(host);
    (!host.is_empty() && labels.split('.').all(label_ok)).then(|| host.to_owned())
}

/// BeanUtils' `IntegerConverter` and `LongConverter` with their default: a
/// value that is not a number is 0.
fn number<T: std::str::FromStr + Default>(v: Option<&str>) -> T {
    v.and_then(|v| v.trim().parse().ok()).unwrap_or_default()
}

/// BeanUtils' `BooleanConverter` with its default: anything it does not
/// know is false.
pub(crate) fn boolean(v: Option<&str>) -> bool {
    let v = v.unwrap_or_default().trim().to_ascii_lowercase();
    matches!(v.as_str(), "true" | "yes" | "y" | "on" | "1")
}

/// Sets one bean property of `ClusterConnectionConfiguration`, as BeanUtils
/// converts it: numbers and booleans that do not convert take their
/// default, an enum must be one of its constants. `staticConnectors` is a
/// comma-separated list where broker properties set it (`lists`), and does
/// not convert from a URI. `Ok(false)`: no such property.
pub(crate) fn set_property(
    c: &mut ClusterConnectionConfig,
    property: &str,
    v: Option<&str>,
    lists: bool,
) -> std::result::Result<bool, String> {
    let string = |v: Option<&str>| v.map(str::to_owned);
    match property {
        "name" => c.name = v.unwrap_or_default().to_owned(),
        "address" => c.address = v.unwrap_or_default().to_owned(),
        "connectorName" => c.connector_name = v.unwrap_or_default().to_owned(),
        "clientFailureCheckPeriod" => c.check_period = number(v),
        "connectionTTL" => c.connection_ttl = number(v),
        "retryInterval" => c.retry_interval = number(v),
        "retryIntervalMultiplier" => c.retry_interval_multiplier = number(v),
        "maxRetryInterval" => c.max_retry_interval = number(v),
        "initialConnectAttempts" => c.initial_connect_attempts = number(v),
        "reconnectAttempts" => c.reconnect_attempts = number(v),
        "callTimeout" => c.call_timeout = number(v),
        "callFailoverTimeout" => c.call_failover_timeout = number(v),
        "duplicateDetection" => c.use_duplicate_detection = boolean(v),
        "messageLoadBalancingType" => {
            let v = v.unwrap_or("null");
            c.message_load_balancing = MessageLoadBalancing::value_of(v).ok_or_else(|| {
                format!(
                    "No enum constant org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType.{v}"
                )
            })?;
        }
        "staticConnectors" if lists => {
            c.static_connectors = v
                .unwrap_or_default()
                .split(',')
                .map(str::to_owned)
                .collect();
        }
        "staticConnectors" => {
            return Err(format!(
                "Can't convert value '{}' to type interface java.util.List",
                v.unwrap_or("null")
            ));
        }
        "discoveryGroupName" => c.discovery_group = string(v),
        "maxHops" => c.max_hops = number(v),
        "confirmationWindowSize" => c.confirmation_window_size = number(v),
        "producerWindowSize" => c.producer_window_size = number(v),
        "allowDirectConnectionsOnly" => c.allow_direct_connections_only = boolean(v),
        "minLargeMessageSize" => c.min_large_message_size = number(v),
        "clusterNotificationInterval" => c.notification_interval = number(v),
        "clusterNotificationAttempts" => c.notification_attempts = number(v),
        "clientId" => c.client_id = string(v),
        "topologyScannerAttempts" => c.topology_scanner_attempts = number(v),
        _ => return Ok(false),
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_members_and_properties() {
        let c = from_uri(
            "cc",
            "static://(tcp://localhost:61616,tcp://localhost:61617)?connectorName=netty&maxHops=2&messageLoadBalancingType=STRICT&address=orders&name=other",
        )
        .unwrap();
        assert_eq!(c.name, "cc");
        assert_eq!(c.address, "orders");
        assert_eq!(c.connector_name, "netty");
        assert_eq!(c.max_hops, 2);
        assert_eq!(c.message_load_balancing, MessageLoadBalancing::Strict);
        assert!(c.static_connectors.is_empty());
        let ports: Vec<_> = c.static_members.iter().map(|m| m.port).collect();
        assert_eq!(ports, [Some(61616), Some(61617)]);
        assert_eq!(c.static_members[0].name, None);
    }

    #[test]
    fn conversions_as_bean_utils_does() {
        let c = from_uri(
            "cc",
            "static:(tcp://localhost:61616?name=one)?maxHops=abc;duplicateDetection=maybe;retryIntervalMultiplier=x;retryInterval=%20750%20;clientId",
        )
        .unwrap();
        assert_eq!(c.max_hops, 0);
        assert!(!c.use_duplicate_detection);
        assert_eq!(c.retry_interval_multiplier, 0.0);
        assert_eq!(c.retry_interval, 750);
        assert_eq!(c.client_id, None);
        assert_eq!(c.static_members[0].name.as_deref(), Some("one"));
        let c = from_uri("cc", "static://tcp://localhost:61616?x=1").unwrap();
        assert_eq!(c.static_members.len(), 1);
        let c = from_uri(
            "cc",
            "static://(tcp://localhost:61616)?messageLoadBalancingType=LOCAL_ONLY",
        )
        .unwrap();
        assert_eq!(c.message_load_balancing, MessageLoadBalancing::LocalOnly);
    }

    #[test]
    fn multicast() {
        let c = from_uri("cc", "multicast://my-group?connectorName=netty&maxHops=4").unwrap();
        assert_eq!(c.discovery_group.as_deref(), Some("my-group"));
        assert_eq!(c.max_hops, 4);
        assert!(c.static_members.is_empty());
        let c = from_uri("cc", "multicast://my_group?connectorName=netty").unwrap();
        assert_eq!(c.discovery_group, None);
        let c = from_uri(
            "cc",
            "multicast://(tcp://localhost:61616)?connectorName=netty",
        )
        .unwrap();
        assert_eq!((c.discovery_group, c.static_members.len()), (None, 1));
    }

    #[test]
    fn refused() {
        for uri in [
            "udp://my-group",
            "static://(tcp://localhost:61616) ?maxHops=1",
            "static://(tcp://localhost:61616)?messageLoadBalancingType=strict",
            "static://(tcp://localhost:61616)?staticConnectors=a,b",
            "static://(tcp://localhost:61616",
        ] {
            assert!(from_uri("cc", uri).is_err(), "{uri}");
        }
    }
}
