//! `NetworkHealthCheck`: the hosts of `network-check-list` and the URLs of
//! `network-check-URL-list`, checked every `network-check-period`. While
//! none answers the broker is stopped, and it is started again once one
//! does.

use std::io::{BufRead, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::process::{Command, Stdio};
use std::time::Duration;

/// `NetworkHealthCheck.IPV4_DEFAULT_COMMAND`.
pub const IPV4_DEFAULT_COMMAND: &str = if cfg!(target_os = "macos") {
    "ping -c 1 -t %d %s"
} else if cfg!(windows) {
    "cmd /C ping -n 1 -w %d000 %s | findstr /i TTL"
} else {
    "ping -c 1 -w %d %s"
};

/// `NetworkHealthCheck.IPV6_DEFAULT_COMMAND`.
pub const IPV6_DEFAULT_COMMAND: &str = if cfg!(windows) {
    "ping -n 1 -w %d000 %s"
} else {
    "ping6 -c 1 %2$s"
};

/// What is checked, and how.
#[derive(Debug)]
pub struct NetworkCheck {
    addresses: Vec<String>,
    urls: Vec<Url>,
    period: Duration,
    timeout: Duration,
    ipv4_command: String,
    ipv6_command: String,
    /// `https` URLs are read with the JVM's default TLS context.
    tls: saranga_transport::TlsConnector,
}

impl NetworkCheck {
    /// The check the configuration asks for, or `None` with nothing to
    /// check. The addresses and URLs are checked once as they are added,
    /// as `addAddress` and `addURL` do: one that does not answer is kept,
    /// with a warning, and a loopback address that does is discarded.
    pub fn from_config(c: &saranga_config::BrokerConfig) -> Option<Self> {
        if let Some(nic) = &c.network_check_nic {
            tracing::warn!(
                "network-check-NIC {nic} is not supported: the addresses are checked through any interface"
            );
        }
        let mut check = NetworkCheck {
            addresses: Vec::new(),
            urls: Vec::new(),
            period: Duration::from_millis(c.network_check_period.max(1) as u64),
            timeout: Duration::from_millis(c.network_check_timeout.max(1) as u64),
            ipv4_command: c
                .network_check_ping_command
                .clone()
                .unwrap_or_else(|| IPV4_DEFAULT_COMMAND.to_owned()),
            ipv6_command: c
                .network_check_ping6_command
                .clone()
                .unwrap_or_else(|| IPV6_DEFAULT_COMMAND.to_owned()),
            tls: saranga_transport::TlsConnector::new(
                saranga_transport::ConnectorTlsSettings::jvm_default(),
            ),
        };
        for address in split(c.network_check_list.as_deref()) {
            check.add_address(address);
        }
        if let Some(list) = c.network_check_url_list.as_deref() {
            for url in split(Some(list)) {
                match Url::parse(url) {
                    Ok(url) => check.add_url(url),
                    Err(e) => {
                        tracing::warn!("AMQ202009: Failed to check Url list {list}.: {e}")
                    }
                }
            }
        }
        (!check.addresses.is_empty() || !check.urls.is_empty()).then_some(check)
    }

    pub fn period(&self) -> Duration {
        self.period
    }

    fn add_address(&mut self, address: &str) {
        let reached = self.check_host(address);
        if reached.is_none() {
            tracing::warn!("AMQ202002: Ping Address {address} wasn't reacheable.");
        }
        if reached.is_some_and(|ip| ip.is_loopback()) {
            tracing::warn!("AMQ202001: {address} is a loopback address and will be discarded.");
        } else {
            self.addresses.push(address.to_owned());
        }
    }

    fn add_url(&mut self, url: Url) {
        if !self.check_url(&url) {
            tracing::warn!("AMQ202003: Ping Url {url} wasn't reacheable.");
        }
        self.urls.push(url);
    }

    /// `check()`: whether any address or URL answers. Blocking.
    pub fn check(&self) -> bool {
        self.addresses.iter().any(|a| self.check_host(a).is_some())
            || self.urls.iter().any(|u| self.check_url(u))
    }

    /// `internalCheck`: the address, if it answers.
    fn check_host(&self, host: &str) -> Option<IpAddr> {
        let checked = resolve(host, 0)
            .map(|a| a.ip())
            .and_then(|ip| Ok(self.check_ip(ip)?.then_some(ip)));
        match checked {
            Ok(reached) => reached,
            Err(e) => {
                tracing::warn!("AMQ202007: Failed to check Address {host}.: {e}");
                None
            }
        }
    }

    /// `check(InetAddress)`: the reachability test first, unless the ping
    /// commands are custom, then the ping command.
    fn check_ip(&self, ip: IpAddr) -> std::io::Result<bool> {
        let custom =
            self.ipv4_command != IPV4_DEFAULT_COMMAND || self.ipv6_command != IPV6_DEFAULT_COMMAND;
        if !custom && self.reachable(ip) {
            tracing::trace!("{ip} OK");
            return Ok(true);
        }
        self.pure_ping(ip)
    }

    /// `InetAddress.isReachable` without privileges: a connection to the
    /// echo port, which a refusal answers as well as an acceptance.
    fn reachable(&self, ip: IpAddr) -> bool {
        match TcpStream::connect_timeout(&SocketAddr::new(ip, 7), self.timeout) {
            Ok(_) => true,
            Err(e) => e.kind() == std::io::ErrorKind::ConnectionRefused,
        }
    }

    /// `purePing`: the ping command, split at spaces; it answered if it
    /// exits with 0.
    fn pure_ping(&self, ip: IpAddr) -> std::io::Result<bool> {
        let seconds = self.timeout.as_secs().max(1);
        let template = match ip {
            IpAddr::V4(_) => &self.ipv4_command,
            IpAddr::V6(_) => &self.ipv6_command,
        };
        let command = java_format(template, seconds, &ip.to_string())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        tracing::debug!("executing ping:: {command}");
        let mut args = command.split(' ').collect::<Vec<_>>();
        // String.split drops the trailing empty strings.
        while args.last() == Some(&"") {
            args.pop();
        }
        let Some((program, args)) = args.split_first() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "empty command",
            ));
        };
        let output = Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .output()?;
        for line in output.stdout.lines().map_while(Result::ok) {
            tracing::debug!("{line}");
        }
        for line in output.stderr.lines().map_while(Result::ok) {
            tracing::warn!("AMQ202011: Failed to read from stream {line}.");
        }
        Ok(output.status.success())
    }

    /// `check(URL)`: the URL can be read.
    fn check_url(&self, url: &Url) -> bool {
        let read = match url {
            Url::Http(h) => h.get(self.timeout, &self.tls),
            Url::File(path) => std::fs::File::open(path)
                .map(drop)
                .map_err(|e| format!("java.io.FileNotFoundException: {} ({e})", path.display())),
        };
        match read {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!("AMQ202006: Failed to check Url {url}.\n{e}");
                false
            }
        }
    }
}

/// `InetAddress.getByName`: the first IPv4 address of the name when it has
/// one, as the JVM prefers them (`java.net.preferIPv6Addresses`).
fn resolve(host: &str, port: u16) -> std::io::Result<SocketAddr> {
    let mut addresses: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();
    addresses.sort_by_key(|a| !a.is_ipv4());
    addresses
        .first()
        .copied()
        .ok_or_else(|| std::io::Error::other("no address"))
}

/// `parseAddressList`, `parseURIList`: comma-separated, trimmed.
fn split(list: Option<&str>) -> impl Iterator<Item = &str> {
    list.into_iter()
        .flat_map(|l| l.split(','))
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// The URLs a check reads.
#[derive(Debug)]
enum Url {
    Http(HttpUrl),
    File(std::path::PathBuf),
}

/// An `http` or `https` URL.
#[derive(Debug, Clone)]
struct HttpUrl {
    text: String,
    secure: bool,
    host: String,
    port: u16,
    /// The path and query, `/` when there is none.
    path: String,
}

impl Url {
    fn parse(text: &str) -> Result<Url, String> {
        let (scheme, rest) = text
            .split_once(':')
            .ok_or_else(|| format!("no protocol: {text}"))?;
        match scheme.to_ascii_lowercase().as_str() {
            scheme @ ("http" | "https") => {
                HttpUrl::parse(text, scheme == "https", rest).map(Url::Http)
            }
            "file" => {
                let path = rest.trim_start_matches("//");
                // file:/C:/x on Windows.
                let path = if cfg!(windows) && path.starts_with('/') && path.get(2..3) == Some(":")
                {
                    &path[1..]
                } else {
                    path
                };
                Ok(Url::File(path.into()))
            }
            _ => Err(format!("unknown protocol: {scheme}")),
        }
    }
}

impl std::fmt::Display for Url {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Url::Http(h) => f.write_str(&h.text),
            Url::File(p) => write!(f, "file:{}", p.display()),
        }
    }
}

/// `HttpURLConnection`'s limit on redirects (`http.maxRedirects`).
const MAX_REDIRECTS: usize = 20;

/// What a server answered: the status and the `Location` header.
struct Answer {
    status: u16,
    location: Option<String>,
}

impl HttpUrl {
    /// `rest` is what follows the scheme's colon.
    fn parse(text: &str, secure: bool, rest: &str) -> Result<HttpUrl, String> {
        let rest = rest
            .strip_prefix("//")
            .ok_or_else(|| format!("no host: {text}"))?;
        let (authority, path) = match rest.find(['/', '?', '#']) {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        let path = path.split('#').next().unwrap_or("/");
        let path = if path.starts_with('?') || path.is_empty() {
            format!("/{path}")
        } else {
            path.to_owned()
        };
        let authority = authority.rsplit('@').next().unwrap_or(authority);
        let default_port = if secure { 443 } else { 80 };
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) if !h.ends_with(']') || authority.starts_with('[') => {
                let port = if p.is_empty() {
                    default_port
                } else {
                    p.parse::<u16>()
                        .map_err(|_| format!("invalid port: {text}"))?
                };
                (h, port)
            }
            _ => (authority, default_port),
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() {
            return Err(format!("no host: {text}"));
        }
        Ok(HttpUrl {
            text: text.to_owned(),
            secure,
            host: host.to_owned(),
            port,
            path,
        })
    }

    fn scheme(&self) -> &'static str {
        if self.secure { "https" } else { "http" }
    }

    /// The host as a URL writes it, an IPv6 address in brackets.
    fn host_text(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        }
    }

    /// `host[:port]`, the port left out when it is the scheme's.
    fn authority(&self) -> String {
        let default_port = if self.secure { 443 } else { 80 };
        if self.port == default_port {
            self.host_text()
        } else {
            format!("{}:{}", self.host_text(), self.port)
        }
    }

    /// `new URL(this, location)`: a redirect's target.
    fn resolve(&self, location: &str) -> Result<Url, String> {
        let has_scheme = location.split_once(':').is_some_and(|(s, _)| {
            !s.is_empty()
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c))
        });
        if has_scheme {
            return Url::parse(location);
        }
        let absolute = if let Some(rest) = location.strip_prefix("//") {
            format!("{}://{rest}", self.scheme())
        } else if location.starts_with('/') {
            format!("{}://{}{location}", self.scheme(), self.authority())
        } else {
            let base = self.path.split('?').next().unwrap_or("/");
            let dir = &base[..base.rfind('/').map_or(0, |i| i + 1)];
            format!("{}://{}{dir}{location}", self.scheme(), self.authority())
        };
        Url::parse(&absolute)
    }

    /// The proxy the JVM's `DefaultProxySelector` picks: `http.proxyHost`
    /// and `http.proxyPort` (80), or `https.proxyHost` and
    /// `https.proxyPort` (443) for `https`, unless the host matches
    /// `http.nonProxyHosts`.
    fn proxy(&self) -> Option<(String, u16)> {
        use saranga_transport::connector::system_property;
        let (host, port, default_port) = if self.secure {
            ("https.proxyHost", "https.proxyPort", 443)
        } else {
            ("http.proxyHost", "http.proxyPort", 80)
        };
        let proxy = system_property(host).filter(|h| !h.trim().is_empty())?;
        let exempt = system_property("http.nonProxyHosts")
            .unwrap_or_else(|| "localhost|127.*|[::1]|0.0.0.0|[::0]".to_owned());
        let host_name = self.host_text().to_ascii_lowercase();
        if exempt
            .split('|')
            .map(|p| p.trim().to_ascii_lowercase())
            .filter(|p| !p.is_empty())
            .any(|p| glob(&p, &host_name))
        {
            return None;
        }
        let port = system_property(port)
            .and_then(|p| p.trim().parse::<u16>().ok())
            .unwrap_or(default_port);
        Some((proxy.trim().to_owned(), port))
    }

    /// `URLConnection.getInputStream`, as `NetworkHealthCheck.check(URL)`
    /// reads it: redirects to the same protocol followed, and an error
    /// status thrown as `HttpURLConnection` throws it. Java's exception is
    /// the error.
    fn get(&self, timeout: Duration, tls: &saranga_transport::TlsConnector) -> Result<(), String> {
        let mut url = self.clone();
        for _ in 0..=MAX_REDIRECTS {
            let answer = url.request(timeout, tls)?;
            // HttpURLConnection.followRedirect0.
            let follows = (300..=307).contains(&answer.status)
                && answer.status != 304
                && answer.status != 306;
            if follows && let Some(location) = &answer.location {
                match url.resolve(location) {
                    Ok(Url::Http(next)) if next.secure == url.secure => {
                        url = next;
                        continue;
                    }
                    // Another protocol: the redirect is the answer.
                    _ => return Ok(()),
                }
            }
            return match answer.status {
                404 | 410 => Err(format!("java.io.FileNotFoundException: {}", url.text)),
                s if s >= 400 => Err(format!(
                    "java.io.IOException: Server returned HTTP response code: {s} for URL: {}",
                    url.text
                )),
                _ => Ok(()),
            };
        }
        Err(format!(
            "java.net.ProtocolException: Server redirected too many times ({MAX_REDIRECTS})"
        ))
    }

    /// One GET: through the proxy if there is one (a `CONNECT` tunnel for
    /// `https`), with TLS for `https`.
    fn request(
        &self,
        timeout: Duration,
        tls: &saranga_transport::TlsConnector,
    ) -> Result<Answer, String> {
        let proxy = self.proxy();
        let (host, port) = proxy
            .clone()
            .unwrap_or_else(|| (self.host.clone(), self.port));
        let address =
            resolve(&host, port).map_err(|_| format!("java.net.UnknownHostException: {host}"))?;
        // NetworkHealthCheck sets the read timeout only: connecting takes
        // as long as the platform lets it.
        let mut stream = TcpStream::connect(address).map_err(|e| io_exception(&e))?;
        let _ = stream.set_read_timeout(Some(timeout));
        let _ = stream.set_write_timeout(Some(timeout));
        let target = format!("{}:{}", self.host_text(), self.port);
        if !self.secure {
            // A proxy is given the whole URL.
            let line = if proxy.is_some() {
                format!("http://{}{}", self.authority(), self.path)
            } else {
                self.path.clone()
            };
            return exchange(&mut stream, &line, &self.authority());
        }
        if proxy.is_some() {
            write!(
                stream,
                "CONNECT {target} HTTP/1.1\r\nHost: {target}\r\nAccept: */*\r\n\r\n"
            )
            .map_err(|e| io_exception(&e))?;
            let (status_line, _) = read_head(&mut stream)?;
            if status_line.split(' ').nth(1) != Some("200") {
                return Err(format!(
                    "java.io.IOException: Unable to tunnel through proxy. Proxy returns \"{status_line}\""
                ));
            }
        }
        let (config, name) = tls
            .client_config_for(&self.host)
            .map_err(|e| format!("javax.net.ssl.SSLException: {e}"))?;
        let connection = rustls::ClientConnection::new(config, name)
            .map_err(|e| format!("javax.net.ssl.SSLException: {e}"))?;
        let mut tls_stream = rustls::StreamOwned::new(connection, stream);
        exchange(&mut tls_stream, &self.path, &self.authority())
    }
}

/// Sends the GET and reads the answer's head.
fn exchange(stream: &mut (impl Read + Write), target: &str, host: &str) -> Result<Answer, String> {
    write!(
        stream,
        "GET {target} HTTP/1.1\r\nHost: {host}\r\nAccept: */*\r\nConnection: close\r\n\r\n"
    )
    .and_then(|()| stream.flush())
    .map_err(|e| tls_or_io(&e))?;
    let (status_line, headers) = read_head(stream)?;
    let status = status_line
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| format!("java.io.IOException: Invalid Http response: {status_line:?}"))?;
    let location = headers
        .into_iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("location"))
        .map(|(_, v)| v);
    Ok(Answer { status, location })
}

/// Reads an HTTP head a byte at a time, so that nothing after it is taken
/// (a tunnel's TLS follows its head): the status line and the headers.
fn read_head(stream: &mut impl Read) -> Result<(String, Vec<(String, String)>), String> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") && !head.ends_with(b"\n\n") {
        if head.len() > 65536 {
            return Err("java.io.IOException: the answer's head is too long".to_owned());
        }
        match stream.read(&mut byte) {
            Ok(0) if head.is_empty() => {
                return Err(
                    "java.net.SocketException: Unexpected end of file from server".to_owned(),
                );
            }
            Ok(0) => break,
            Ok(_) => head.push(byte[0]),
            Err(e) => return Err(tls_or_io(&e)),
        }
    }
    let text = String::from_utf8_lossy(&head);
    let mut lines = text.lines();
    let status_line = lines.next().unwrap_or_default().trim().to_owned();
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
        .collect();
    Ok((status_line, headers))
}

/// An I/O error as the exception Java's socket throws.
fn io_exception(e: &std::io::Error) -> String {
    use std::io::ErrorKind as K;
    match e.kind() {
        // PlainSocketImpl names the call on Windows.
        K::ConnectionRefused if cfg!(windows) => {
            "java.net.ConnectException: Connection refused: connect".to_owned()
        }
        K::ConnectionRefused => "java.net.ConnectException: Connection refused".to_owned(),
        K::TimedOut | K::WouldBlock => "java.net.SocketTimeoutException: Read timed out".to_owned(),
        K::ConnectionReset => "java.net.SocketException: Connection reset".to_owned(),
        _ => format!("java.net.SocketException: {e}"),
    }
}

/// A rustls failure (wrapped in an I/O error) as the
/// `SSLHandshakeException` the JDK throws for it; others are socket
/// exceptions.
fn tls_or_io(e: &std::io::Error) -> String {
    use rustls::CertificateError as C;
    let Some(tls) = e.get_ref().and_then(|i| i.downcast_ref::<rustls::Error>()) else {
        return io_exception(e);
    };
    let reason = match tls {
        rustls::Error::InvalidCertificate(C::UnknownIssuer) => "(certificate_unknown) PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target".to_owned(),
        rustls::Error::InvalidCertificate(C::Expired | C::ExpiredContext { .. } | C::NotValidYet | C::NotValidYetContext { .. }) => "(certificate_unknown) PKIX path validation failed: java.security.cert.CertPathValidatorException: validity check failed".to_owned(),
        // HostnameChecker's messages.
        rustls::Error::InvalidCertificate(C::NotValidForNameContext { expected, .. }) => {
            match expected {
                rustls::pki_types::ServerName::IpAddress(ip) => format!(
                    "(certificate_unknown) No subject alternative names matching IP address {} found",
                    std::net::IpAddr::from(*ip)
                ),
                name => format!(
                    "(certificate_unknown) No subject alternative DNS name matching {} found.",
                    name.to_str()
                ),
            }
        }
        other => other.to_string(),
    };
    format!("javax.net.ssl.SSLHandshakeException: {reason}")
}

/// `DefaultProxySelector`'s match of a `http.nonProxyHosts` pattern, where
/// `*` stands for any text.
fn glob(pattern: &str, text: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == text,
        Some((head, tail)) => {
            text.starts_with(head)
                && (0..=text.len() - head.len()).any(|i| {
                    text.is_char_boundary(head.len() + i) && glob(tail, &text[head.len() + i..])
                })
        }
    }
}

/// `String.format(template, timeout, host)` for the conversions a ping
/// command uses: `%d`, `%s` (each optionally `%n$`-indexed), `%%`, `%n`.
fn java_format(template: &str, timeout: u64, host: &str) -> Result<String, String> {
    let mut out = String::with_capacity(template.len() + host.len());
    let mut next = 0;
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let mut digits = String::new();
        while let Some(d) = chars.peek().filter(|d| d.is_ascii_digit()) {
            digits.push(*d);
            chars.next();
        }
        let index = if digits.is_empty() {
            None
        } else if chars.peek() == Some(&'$') {
            chars.next();
            Some(digits.parse::<usize>().map_err(|e| e.to_string())?)
        } else {
            return Err(format!("unsupported format specifier '%{digits}'"));
        };
        let conversion = chars
            .next()
            .ok_or_else(|| format!("Format specifier '%{digits}'"))?;
        if index.is_none() && matches!(conversion, '%' | 'n') {
            out.push(if conversion == '%' { '%' } else { '\n' });
            continue;
        }
        let i = index.unwrap_or_else(|| {
            next += 1;
            next
        });
        let argument = match i {
            1 => timeout.to_string(),
            2 => host.to_owned(),
            _ => return Err(format!("Format specifier '%{conversion}'")),
        };
        match conversion {
            's' => out.push_str(&argument),
            'S' => out.push_str(&argument.to_uppercase()),
            // The host is a String.
            'd' if i == 1 => out.push_str(&argument),
            'd' => return Err("d != java.lang.String".to_owned()),
            other => return Err(format!("Conversion = '{other}'")),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn ping_commands_format_as_java_does() {
        assert_eq!(
            java_format("ping -c 1 -t %d %s", 1, "10.0.0.1").unwrap(),
            "ping -c 1 -t 1 10.0.0.1"
        );
        assert_eq!(
            java_format("ping6 -c 1 %2$s", 2, "::1").unwrap(),
            "ping6 -c 1 ::1"
        );
        assert_eq!(
            java_format("cmd /C ping -n 1 -w %d000 %s | findstr /i TTL", 3, "h").unwrap(),
            "cmd /C ping -n 1 -w 3000 h | findstr /i TTL"
        );
        assert_eq!(
            java_format("test -e /x 100%%", 1, "h").unwrap(),
            "test -e /x 100%"
        );
        assert!(java_format("ping %s %d", 1, "h").is_err());
        assert!(java_format("ping %x", 1, "h").is_err());
    }

    #[test]
    fn urls_parse() {
        let Url::Http(h) = Url::parse("http://www.apache.org").unwrap() else {
            panic!()
        };
        assert_eq!(
            (h.host.as_str(), h.port, h.path.as_str()),
            ("www.apache.org", 80, "/")
        );
        let Url::Http(h) = Url::parse("http://[::1]:8161/x?y#z").unwrap() else {
            panic!()
        };
        assert_eq!(
            (h.host.as_str(), h.port, h.path.as_str()),
            ("::1", 8161, "/x?y")
        );
        let Url::Http(h) = Url::parse("https://a").unwrap() else {
            panic!()
        };
        assert_eq!(
            (h.secure, h.port, h.authority()),
            (true, 443, "a".to_owned())
        );
        assert!(Url::parse("nothing").is_err());
        assert!(Url::parse("gopher://a").is_err());
        let Url::Http(h) = Url::parse("http://h:81/a/b?q").unwrap() else {
            panic!()
        };
        for (location, resolved) in [
            ("c", "http://h:81/a/c"),
            ("/c", "http://h:81/c"),
            ("//g/c", "http://g/c"),
            ("https://g/c", "https://g/c"),
        ] {
            assert_eq!(h.resolve(location).unwrap().to_string(), resolved);
        }
    }

    #[test]
    fn non_proxy_hosts_match_as_the_jvm_matches_them() {
        assert!(glob("localhost", "localhost"));
        assert!(glob("127.*", "127.0.0.1"));
        assert!(glob("*.example.com", "a.example.com"));
        assert!(!glob("*.example.com", "example.com"));
        assert!(glob("[::1]", "[::1]"));
        assert!(!glob("127.*", "10.0.0.1"));
    }

    /// Serves `count` connections on a thread: each gets what `answer`
    /// makes of the request's head, which is kept.
    fn serve(
        count: usize,
        answer: impl Fn(&str) -> String + Send + 'static,
    ) -> (u16, std::sync::mpsc::Receiver<String>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming().take(count) {
                let mut stream = stream.unwrap();
                let (line, _) = read_head(&mut stream).unwrap();
                let reply = answer(&line);
                let _ = stream.write_all(reply.as_bytes());
                let _ = tx.send(line);
            }
        });
        (port, rx)
    }

    /// Serves one TLS connection with the test server certificate.
    fn serve_tls(address: &str) -> u16 {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/tls");
        let certs = CertificateDer::pem_file_iter(dir.join("server-cert.pem"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = PrivateKeyDer::from_pem_file(dir.join("server-key.pem")).unwrap();
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
        let config = Arc::new(config);
        let listener = std::net::TcpListener::bind(address).unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let connection = rustls::ServerConnection::new(config).unwrap();
            let mut tls = rustls::StreamOwned::new(connection, stream);
            if read_head(&mut tls).is_ok() {
                let _ = tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
                let _ = tls.flush();
            }
        });
        port
    }

    fn get(url: &str) -> Result<(), String> {
        let Url::Http(h) = Url::parse(url).unwrap() else {
            panic!()
        };
        let tls = saranga_transport::TlsConnector::new(
            saranga_transport::ConnectorTlsSettings::jvm_default(),
        );
        h.get(Duration::from_secs(5), &tls)
    }

    fn properties(p: &[(&str, &str)]) {
        saranga_transport::connector::set_system_properties(
            p.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
    }

    /// `NetworkHealthCheck.check(URL)` reads a URL through `URLConnection`:
    /// redirects within the protocol followed, error statuses thrown,
    /// `https` checked with the JVM's default trust, and the proxies the
    /// system properties name. One test, as the properties are global.
    #[test]
    fn urls_are_read_as_java_reads_them() {
        properties(&[]);
        let ok = |_: &str| "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".to_owned();
        let (port, _) = serve(1, ok);
        assert_eq!(get(&format!("http://127.0.0.1:{port}/")), Ok(()));

        let (port, _) = serve(1, |_| "HTTP/1.1 404 Not Found\r\n\r\n".to_owned());
        let url = format!("http://127.0.0.1:{port}/missing");
        assert_eq!(
            get(&url),
            Err(format!("java.io.FileNotFoundException: {url}"))
        );
        let (port, _) = serve(1, |_| "HTTP/1.1 500 Oops\r\n\r\n".to_owned());
        let url = format!("http://127.0.0.1:{port}/x");
        assert_eq!(
            get(&url),
            Err(format!(
                "java.io.IOException: Server returned HTTP response code: 500 for URL: {url}"
            ))
        );

        // A relative redirect is followed; one to https is the answer.
        let (port, seen) = serve(2, |line| {
            if line.starts_with("GET /a/b ") {
                "HTTP/1.1 302 Found\r\nLocation: c\r\n\r\n".to_owned()
            } else {
                "HTTP/1.1 200 OK\r\n\r\n".to_owned()
            }
        });
        assert_eq!(get(&format!("http://127.0.0.1:{port}/a/b")), Ok(()));
        assert_eq!(seen.recv().unwrap(), "GET /a/b HTTP/1.1");
        assert_eq!(seen.recv().unwrap(), "GET /a/c HTTP/1.1");
        let (port, _) = serve(1, |_| {
            "HTTP/1.1 301 Moved\r\nLocation: https://127.0.0.1:1/\r\n\r\n".to_owned()
        });
        assert_eq!(get(&format!("http://127.0.0.1:{port}/")), Ok(()));
        let (port, _) = serve(MAX_REDIRECTS + 1, |_| {
            "HTTP/1.1 302 Found\r\nLocation: /again\r\n\r\n".to_owned()
        });
        assert_eq!(
            get(&format!("http://127.0.0.1:{port}/")),
            Err("java.net.ProtocolException: Server redirected too many times (20)".to_owned())
        );

        // https: the test CA is trusted through javax.net.ssl.trustStore,
        // and the platform's authorities do not know it.
        let store = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/tls/truststore.p12")
            .display()
            .to_string();
        properties(&[
            ("javax.net.ssl.trustStore", &store),
            ("javax.net.ssl.trustStorePassword", "changeit"),
            ("javax.net.ssl.trustStoreType", "PKCS12"),
        ]);
        let port = serve_tls("127.0.0.1:0");
        assert_eq!(get(&format!("https://localhost:{port}/")), Ok(()));
        // The certificate names 127.0.0.1, not ::1.
        let port = serve_tls("[::1]:0");
        assert_eq!(
            get(&format!("https://[::1]:{port}/")),
            Err("javax.net.ssl.SSLHandshakeException: (certificate_unknown) No subject alternative names matching IP address ::1 found".to_owned())
        );
        properties(&[]);
        let port = serve_tls("127.0.0.1:0");
        let e = get(&format!("https://localhost:{port}/")).unwrap_err();
        assert_eq!(
            e,
            "javax.net.ssl.SSLHandshakeException: (certificate_unknown) PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target"
        );

        // Through a proxy: http gets the whole URL, https a CONNECT.
        let (proxy, seen) = serve(2, |line| {
            if line.starts_with("CONNECT ") {
                "HTTP/1.1 407 Proxy Authentication Required\r\n\r\n".to_owned()
            } else {
                "HTTP/1.1 200 OK\r\n\r\n".to_owned()
            }
        });
        let proxy = proxy.to_string();
        properties(&[
            ("http.proxyHost", "127.0.0.1"),
            ("http.proxyPort", &proxy),
            ("https.proxyHost", "127.0.0.1"),
            ("https.proxyPort", &proxy),
            ("http.nonProxyHosts", ""),
        ]);
        assert_eq!(get("http://localhost:9/x?y"), Ok(()));
        assert_eq!(seen.recv().unwrap(), "GET http://localhost:9/x?y HTTP/1.1");
        assert_eq!(
            get("https://localhost:9/"),
            Err("java.io.IOException: Unable to tunnel through proxy. Proxy returns \"HTTP/1.1 407 Proxy Authentication Required\"".to_owned())
        );
        assert_eq!(seen.recv().unwrap(), "CONNECT localhost:9 HTTP/1.1");
        // The default http.nonProxyHosts leaves local hosts direct.
        properties(&[("http.proxyHost", "127.0.0.1"), ("http.proxyPort", "1")]);
        let (port, _) = serve(1, ok);
        assert_eq!(get(&format!("http://127.0.0.1:{port}/")), Ok(()));
        properties(&[]);
    }

    fn config(list: &str, command: &str) -> saranga_config::BrokerConfig {
        saranga_config::BrokerConfig {
            network_check_list: Some(list.to_owned()),
            network_check_ping_command: Some(command.to_owned()),
            network_check_period: 100,
            ..Default::default()
        }
    }

    /// A command that succeeds while `flag` exists.
    fn flag_command(flag: &std::path::Path) -> String {
        if cfg!(windows) {
            format!("cmd /C if exist {} (exit 0) else (exit 1)", flag.display())
        } else {
            format!("test -e {}", flag.display())
        }
    }

    #[test]
    fn an_address_answers_while_its_ping_command_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let flag = dir.path().join("up");
        let check = NetworkCheck::from_config(&config("192.0.2.1", &flag_command(&flag))).unwrap();
        assert!(!check.check());
        std::fs::write(&flag, "").unwrap();
        assert!(check.check());
        std::fs::remove_file(&flag).unwrap();
        assert!(!check.check());
    }

    #[test]
    fn a_reachable_loopback_address_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let flag = dir.path().join("up");
        std::fs::write(&flag, "").unwrap();
        let command = flag_command(&flag);
        assert!(NetworkCheck::from_config(&config("127.0.0.1, ,", &command)).is_none());
        // One that does not answer is kept.
        std::fs::remove_file(&flag).unwrap();
        assert!(NetworkCheck::from_config(&config("127.0.0.1", &command)).is_some());
        assert!(NetworkCheck::from_config(&config("", &command)).is_none());
    }

    #[test]
    fn a_url_answers_while_it_can_be_read() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            for status in ["200 OK", "404 Not Found"] {
                let (mut s, _) = listener.accept().unwrap();
                let mut request = [0u8; 1024];
                let _ = s.read(&mut request).unwrap();
                write!(s, "HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n").unwrap();
            }
        });
        let mut c = saranga_config::BrokerConfig {
            network_check_url_list: Some(format!("http://127.0.0.1:{port}/health")),
            ..Default::default()
        };
        let check = NetworkCheck::from_config(&c).unwrap();
        // Checked once as it was added, which the server answered with 200.
        assert!(!check.check());
        server.join().unwrap();
        assert!(!check.check());
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, "").unwrap();
        c.network_check_url_list = Some(format!("file:{}", file.display()));
        assert!(NetworkCheck::from_config(&c).unwrap().check());
    }
}
