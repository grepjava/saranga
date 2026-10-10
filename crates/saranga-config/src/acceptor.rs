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
fn percent_decode(s: &str, plus_as_space: bool) -> std::result::Result<String, String> {
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
