//! Acceptor URIs. Ported from `ConfigurationUtils.parseAcceptorURI`,
//! `URIFactory.normalise`, `URISchema.parseQuery`,
//! `TCPTransportConfigurationSchema` and the protocol selection in
//! `RemotingServiceImpl.createAcceptor`.
//!
//! The URI itself is split the way `java.net.URI` splits it, but without its
//! full RFC 2396 validation: a host is accepted when it is a bracketed IPv6
//! literal or made of letters, digits, `-` and `.`; anything else (an
//! underscore, say) leaves the host unset, which is what `URI.getHost()`
//! returns for such authorities.

use crate::{ConfigError, Result, java_split};

/// One `<acceptor>`: `tcp://0.0.0.0:61616?protocols=CORE,AMQP;useEpoll=true`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptorConfig {
    /// The `name` attribute, or the `name` query parameter when the attribute
    /// is absent.
    pub name: Option<String>,
    /// The URI text exactly as written in the XML.
    pub uri: String,
    /// `tcp` or `vm`.
    pub scheme: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    /// Query parameters, decoded, in the order written. A repeated key keeps
    /// its first position and its last value, like the Java `HashMap`. A
    /// parameter without `=` has no value.
    pub params: Vec<(String, Option<String>)>,
    /// The `lock-coordinator` attribute.
    pub lock_coordinator: Option<String>,
    /// Whether it was written as a URI, whose scheme, host and port are
    /// parameters too, or given as parameters by broker properties.
    pub from_uri: bool,
}

/// The parameters `TransportConfiguration` holds for an acceptor, as
/// `TCPTransportConfigurationSchema` splits them: `params`, the keys
/// `TransportConstants.ALLOWABLE_ACCEPTOR_KEYS` allows, and `extraProps`,
/// the others. Each is in the order Java's `HashMap` iterates it; a
/// parameter without `=` has no value (`null`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportParams {
    pub params: Vec<(String, Option<String>)>,
    pub extra: Vec<(String, Option<String>)>,
}

impl TransportParams {
    /// `TransportConfiguration.equals`, for one name: both maps are equal.
    pub fn same(&self, other: &Self) -> bool {
        let map = |v: &[(String, Option<String>)]| {
            v.iter()
                .cloned()
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        map(&self.params) == map(&other.params) && map(&self.extra) == map(&other.extra)
    }

    /// `TransportConfiguration.hashCode` for the acceptor called `name`:
    /// `Objects.hash(name, factoryClassName, params)`.
    pub fn hash_code(&self, name: &str) -> i32 {
        use saranga_buffer::java_hash::string_hash;
        const FACTORY: &str =
            "org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory";
        // AbstractMap.hashCode: the sum of key ^ value.
        let params = self.params.iter().fold(0i32, |h, (k, v)| {
            h.wrapping_add(string_hash(k) ^ v.as_deref().map_or(0, string_hash))
        });
        [string_hash(name), string_hash(FACTORY), params]
            .into_iter()
            .fold(1i32, |h, x| h.wrapping_mul(31).wrapping_add(x))
    }

    /// `params` as `HashMap.toString` prints it: `{host=0.0.0.0, port=61616}`.
    pub fn params_text(&self) -> String {
        let entries: Vec<String> = self
            .params
            .iter()
            .map(|(k, v)| format!("{k}={}", v.as_deref().unwrap_or("null")))
            .collect();
        format!("{{{}}}", entries.join(", "))
    }
}

/// `TransportConstants.ALLOWABLE_ACCEPTOR_KEYS`.
const ALLOWABLE_ACCEPTOR_KEYS: [&str; 70] = [
    "sslEnabled",
    "sslAutoReload",
    "sslHandshakeTimeout",
    "httpResponseTime",
    "httpServerScanPeriod",
    "httpUpgradeEnabled",
    "useNio",
    "useEpoll",
    "useKQueue",
    "useInvm",
    "protocol",
    "protocols",
    "scheme",
    "host",
    "port",
    "keyStoreProvider",
    "keyStoreType",
    "keyStorePath",
    "keyStorePassword",
    "keyPassword",
    "keyStoreAlias",
    "trustStoreProvider",
    "trustStoreType",
    "trustStorePath",
    "trustStorePassword",
    "enabledCipherSuites",
    "enabledProtocols",
    "needClientAuth",
    "wantClientAuth",
    "verifyHost",
    "sniHost",
    "tcpNoDelay",
    "tcpSendBufferSize",
    "tcpReceiveBufferSize",
    "writeBufferHighWaterMark",
    "writeBufferLowWaterMark",
    "nioRemotingThreads",
    "remotingThreads",
    "batchDelay",
    "directDeliver",
    "clusterConnection",
    "stompConsumerCredits",
    "stompConsumerWindowSize",
    "stomp-min-large-message-size",
    "stompMinLargeMessageSize",
    "connectionTtl",
    "connectionTtlMax",
    "connectionTtlMin",
    "heartBeatToConnectionTtlModifier",
    "stomp-enable-message-id",
    "stompEnableMessageId",
    "connectionsAllowed",
    "stompMaxFramePayloadLength",
    "webSocketMaxFramePayloadLength",
    "webSocketCompressionSupported",
    "activemq.usemaskedpassword",
    "activemq.passwordcodec",
    "backlog",
    "crlPath",
    "handshake-timeout",
    "sslProvider",
    "trustManagerFactoryPlugin",
    "shutdownTimeout",
    "quietPeriod",
    "disableStompServerHeader",
    "autoStart",
    "router",
    "proxyProtocolEnabled",
    "crcOptions",
    "ocspResponderURL",
];

/// Puts into a list kept as a Java `HashMap`: a key keeps its place.
fn put(map: &mut Vec<(String, Option<String>)>, key: &str, value: Option<String>) {
    match map.iter_mut().find(|(k, _)| k == key) {
        Some(entry) => entry.1 = value,
        None => map.push((key.to_owned(), value)),
    }
}

/// The entries in the order a `new HashMap<>()` they were put into in this
/// order iterates them.
fn java_order(map: Vec<(String, Option<String>)>) -> Vec<(String, Option<String>)> {
    let hashes: Vec<i32> = map
        .iter()
        .map(|(k, _)| saranga_buffer::java_hash::string_hash(k))
        .collect();
    let order = saranga_buffer::java_hash::hash_map_order(&hashes, None);
    order.into_iter().map(|i| map[i].clone()).collect()
}

const SCHEMES: [&str; 2] = ["tcp", "vm"];

impl AcceptorConfig {
    /// Parses an acceptor URI. `name` is the element's `name` attribute.
    pub fn parse(name: Option<&str>, uri: &str) -> Result<Self> {
        let fail = |reason: &str| ConfigError::InvalidUri {
            uri: uri.to_owned(),
            reason: reason.to_owned(),
        };

        // `parseAcceptorURI` strips all whitespace, `normalise` turns `;`
        // into `&`.
        let compact: String = uri.chars().filter(|c| !is_java_space(*c)).collect();
        let normalised = normalise(&compact).replace(';', "&");

        let (without_fragment, _fragment) = split_once_or_all(&normalised, '#');
        let colon = without_fragment
            .find(':')
            .ok_or_else(|| fail("no scheme"))?;
        let scheme = &without_fragment[..colon];
        if !is_valid_scheme(scheme) {
            return Err(fail("invalid scheme"));
        }
        if !SCHEMES.contains(&scheme) {
            return Err(fail(&format!("schema {scheme} not found")));
        }
        let rest = &without_fragment[colon + 1..];

        let mut host = None;
        let mut port = None;
        let mut params = Vec::new();
        // An opaque URI (`tcp:x`) has no authority and no query in Java.
        if let Some(hier) = rest.strip_prefix("//") {
            let (hier, query) = split_once_or_all(hier, '?');
            let authority = &hier[..hier.find('/').unwrap_or(hier.len())];
            (host, port) = parse_authority(authority).map_err(|r| fail(&r))?;
            if let Some(query) = query {
                params = parse_query(query).map_err(|r| fail(&r))?;
            }
        }

        let mut config = Self {
            name: name.map(str::to_owned),
            uri: uri.to_owned(),
            scheme: scheme.to_owned(),
            host,
            port,
            params,
            lock_coordinator: None,
            from_uri: true,
        };
        if let Some(Some(from_query)) = config.param_entry("name") {
            match &config.name {
                Some(n) if n != from_query => return Err(fail("name doesn't match query param")),
                Some(_) => {}
                None => config.name = Some(from_query.to_owned()),
            }
        }
        Ok(config)
    }

    /// An acceptor given as a name and parameters, as broker properties
    /// give `acceptorConfigurations`: always TCP, with `host` and `port`
    /// taken from the parameters.
    pub fn from_params(name: &str, params: Vec<(String, Option<String>)>) -> Self {
        let mut config = Self {
            name: Some(name.to_owned()),
            uri: String::new(),
            scheme: "tcp".to_owned(),
            host: None,
            port: None,
            params: Vec::new(),
            lock_coordinator: None,
            from_uri: false,
        };
        for (k, v) in params {
            config.set_param(&k, v.as_deref());
        }
        config
    }

    /// Sets (or with `None` removes) a parameter, keeping `host` and `port`
    /// and the URI text in step.
    pub fn set_param(&mut self, key: &str, value: Option<&str>) {
        match value {
            Some(v) => match self.params.iter_mut().find(|(k, _)| k == key) {
                Some((_, slot)) => *slot = Some(v.to_owned()),
                None => self.params.push((key.to_owned(), Some(v.to_owned()))),
            },
            None => self.params.retain(|(k, _)| k != key),
        }
        match key {
            "host" => self.host = value.map(str::to_owned),
            "port" => self.port = value.and_then(|v| v.trim().parse().ok()),
            _ => {}
        }
        let query: Vec<String> = self
            .params
            .iter()
            .filter(|(k, _)| k != "host" && k != "port")
            .map(|(k, v)| match v {
                Some(v) => format!("{k}={v}"),
                None => k.clone(),
            })
            .collect();
        self.uri = format!(
            "tcp://{}:{}{}{}",
            self.host.as_deref().unwrap_or("localhost"),
            self.port.unwrap_or(61616),
            if query.is_empty() { "" } else { "?" },
            query.join(";")
        );
    }

    /// The `TransportConfiguration` Artemis makes of it, which a reload
    /// compares (`RemotingServiceImpl.updateAcceptors`). A URI gives
    /// `scheme`, `host` and `port` (`"null"` and `"-1"` when absent), then
    /// its query (`BeanSupport.setData`); broker properties put each
    /// parameter into `params`.
    pub fn transport_params(&self) -> TransportParams {
        if !self.from_uri {
            return TransportParams {
                params: java_order(self.params.clone()),
                extra: Vec::new(),
            };
        }
        let mut params = Vec::new();
        let mut extra = Vec::new();
        put(&mut params, "scheme", Some(self.scheme.clone()));
        put(
            &mut params,
            "host",
            Some(self.host.clone().unwrap_or_else(|| "null".to_owned())),
        );
        put(
            &mut params,
            "port",
            Some(self.port.map_or_else(|| "-1".to_owned(), |p| p.to_string())),
        );
        // `parseQuery` gives a HashMap, which setData walks.
        for (k, v) in java_order(self.params.clone()) {
            if ALLOWABLE_ACCEPTOR_KEYS.contains(&k.as_str()) {
                put(&mut params, &k, v);
            } else {
                put(&mut extra, &k, v);
            }
        }
        TransportParams {
            params: java_order(params),
            extra: java_order(extra),
        }
    }

    fn param_entry(&self, key: &str) -> Option<Option<&str>> {
        self.params
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_deref())
    }

    /// The value of a query parameter, `None` if absent or valueless.
    pub fn param(&self, key: &str) -> Option<&str> {
        self.param_entry(key).flatten()
    }

    /// The protocols this acceptor enables: the deprecated `protocol`
    /// parameter, then `protocols`, each split on `,` without trimming.
    /// Empty means every available protocol. Names are matched against the
    /// protocol registry case-sensitively (`CORE`, `AMQP`, ...); unknown ones
    /// are skipped with a warning by the broker.
    pub fn protocols(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for key in ["protocol", "protocols"] {
            if let Some(list) = self.param(key) {
                for p in java_split(list, ',') {
                    if !out.iter().any(|x| x == p) {
                        out.push(p.to_owned());
                    }
                }
            }
        }
        out
    }
}

fn is_java_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\u{0b}' | '\u{0c}' | '\r')
}

fn split_once_or_all(s: &str, sep: char) -> (&str, Option<&str>) {
    match s.split_once(sep) {
        Some((a, b)) => (a, Some(b)),
        None => (s, None),
    }
}

/// `URIFactory.normalise` for the `(uri1,uri2)?query` form: keeps the first
/// URI with the shared query appended. The other URIs go into the fragment,
/// which an acceptor ignores.
fn normalise(uri: &str) -> String {
    if !uri.starts_with('(') {
        return uri.to_owned();
    }
    let (inside, factory_query) = match uri.split_once(')') {
        Some((a, b)) => (a, b),
        None => (uri, ""),
    };
    let inside = &inside[inside.find('(').map_or(0, |i| i + 1)..];
    let connectors: Vec<&str> = inside.split(',').collect();
    let mut out = connectors[0].to_owned();
    if !factory_query.is_empty() {
        if connectors[0].contains('?') {
            out.push('&');
            out.push_str(&factory_query[factory_query.chars().next().map_or(0, char::len_utf8)..]);
        } else {
            out.push_str(factory_query);
        }
    }
    if connectors.len() > 1 {
        out.push('#');
        out.push_str(&connectors[1..].join(","));
    }
    out
}

fn is_valid_scheme(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

type HostPort = (Option<String>, Option<u16>);

fn parse_authority(authority: &str) -> std::result::Result<HostPort, String> {
    let server = match authority.rfind('@') {
        Some(i) => &authority[i + 1..],
        None => authority,
    };
    let (host, port_text) = if server.starts_with('[') {
        match server.find(']') {
            Some(end) => {
                let rest = &server[end + 1..];
                match rest.strip_prefix(':') {
                    Some(p) => (&server[..=end], Some(p)),
                    None if rest.is_empty() => (&server[..=end], None),
                    None => return Ok((None, None)),
                }
            }
            None => return Err("unterminated IPv6 address".to_owned()),
        }
    } else {
        match server.rfind(':') {
            Some(i) => (&server[..i], Some(&server[i + 1..])),
            None => (server, None),
        }
    };
    let port = match port_text {
        None | Some("") => None,
        Some(p) if p.bytes().all(|b| b.is_ascii_digit()) => Some(
            p.parse::<u16>()
                .map_err(|_| format!("port {p} out of range"))?,
        ),
        // Not a server-based authority: Java reports neither host nor port.
        Some(_) => return Ok((None, None)),
    };
    let valid_host = host.starts_with('[')
        || (!host.is_empty()
            && host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.'));
    if !valid_host {
        return Ok((None, if host.is_empty() { port } else { None }));
    }
    Ok((Some(host.to_owned()), port))
}

/// The parameters of a connector URI's query, as `ConfigurationUtils`
/// reads them: `;` taken as `&`, then [`parse_query`]'s decoding. A name
/// without `=` has no value.
pub fn query_params(raw: &str) -> std::result::Result<Vec<(String, Option<String>)>, String> {
    parse_query(&raw.replace(';', "&"))
}

/// `URI.getQuery()` (percent-decoding) followed by `URISchema.parseQuery`,
/// which splits on `&` and URL-decodes each name and value a second time.
fn parse_query(raw: &str) -> std::result::Result<Vec<(String, Option<String>)>, String> {
    let query = percent_decode(raw, false)?;
    let mut params: Vec<(String, Option<String>)> = Vec::new();
    for parameter in query.split('&') {
        let (key, value) = match parameter.find('=') {
            Some(p) => (
                percent_decode(&parameter[..p], true)?,
                Some(percent_decode(&parameter[p + 1..], true)?),
            ),
            None if crate::java_trim(parameter).is_empty() => continue,
            None => (parameter.to_owned(), None),
        };
        match params.iter_mut().find(|(k, _)| *k == key) {
            Some(entry) => entry.1 = value,
            None => params.push((key, value)),
        }
    }
    Ok(params)
}

/// Decodes `%XX` escapes as UTF-8; with `plus_as_space` also `+`, as
/// `URLDecoder.decode` does.
pub(crate) fn percent_decode(s: &str, plus_as_space: bool) -> std::result::Result<String, String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = bytes
                    .get(i + 1..i + 3)
                    .filter(|h| h.iter().all(u8::is_ascii_hexdigit))
                    .ok_or_else(|| format!("malformed escape in {s:?}"))?;
                let digit = |b: u8| (b as char).to_digit(16).unwrap_or(0) as u8;
                out.push(digit(hex[0]) << 4 | digit(hex[1]));
                i += 3;
            }
            b'+' if plus_as_space => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}
