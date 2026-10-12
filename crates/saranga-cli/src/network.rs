//! `NetworkHealthCheck`: the hosts of `network-check-list` and the URLs of
//! `network-check-URL-list`, checked every `network-check-period`. While
//! none answers the broker is stopped, and it is started again once one
//! does.

use std::io::{BufRead, BufReader, Read, Write};
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
        };
        for address in split(c.network_check_list.as_deref()) {
            check.add_address(address);
        }
        if let Some(list) = c.network_check_url_list.as_deref() {
            for url in split(Some(list)) {
                match Url::parse(url) {
                    Ok(Url::Https(_)) => tracing::warn!(
                        "network-check-URL-list: {url} is not checked, as https URLs are not supported"
                    ),
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
        let checked = (host, 0)
            .to_socket_addrs()
            .and_then(|mut a| {
                a.next()
                    .map(|a| a.ip())
                    .ok_or_else(|| std::io::Error::other("no address"))
            })
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
            Url::Http(h) => h.get(self.timeout),
            Url::File(path) => std::fs::File::open(path).map(drop),
            Url::Https(_) => return false,
        };
        match read {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!("AMQ202006: Failed to check Url {url}.: {e}");
                false
            }
        }
    }
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
    Https(String),
    File(std::path::PathBuf),
}

#[derive(Debug)]
struct HttpUrl {
    text: String,
    host: String,
    port: u16,
    path: String,
}

impl Url {
    fn parse(text: &str) -> Result<Url, String> {
        let (scheme, rest) = text
            .split_once(':')
            .ok_or_else(|| format!("no protocol: {text}"))?;
        match scheme.to_ascii_lowercase().as_str() {
            "http" => {
                let rest = rest
                    .strip_prefix("//")
                    .ok_or_else(|| format!("no host: {text}"))?;
                let (authority, path) = match rest.find(['/', '?', '#']) {
                    Some(i) => (&rest[..i], &rest[i..]),
                    None => (rest, "/"),
                };
                let path = path.split('#').next().unwrap_or("/");
                let path = if path.starts_with('?') {
                    format!("/{path}")
                } else {
                    path.to_owned()
                };
                let authority = authority.rsplit('@').next().unwrap_or(authority);
                let (host, port) = match authority.rsplit_once(':') {
                    Some((h, p)) if !h.ends_with(']') || authority.starts_with('[') => {
                        let port = p
                            .parse::<u16>()
                            .map_err(|_| format!("invalid port: {text}"))?;
                        (h, port)
                    }
                    _ => (authority, 80),
                };
                let host = host.trim_start_matches('[').trim_end_matches(']');
                if host.is_empty() {
                    return Err(format!("no host: {text}"));
                }
                Ok(Url::Http(HttpUrl {
                    text: text.to_owned(),
                    host: host.to_owned(),
                    port,
                    path,
                }))
            }
            "https" => Ok(Url::Https(text.to_owned())),
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
            Url::Https(t) => f.write_str(t),
            Url::File(p) => write!(f, "file:{}", p.display()),
        }
    }
}

impl HttpUrl {
    /// A GET whose answer is not an error status, as
    /// `URLConnection.getInputStream` needs.
    fn get(&self, timeout: Duration) -> std::io::Result<()> {
        let address = (self.host.as_str(), self.port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| std::io::Error::other(format!("{}: no address", self.host)))?;
        let mut stream = TcpStream::connect_timeout(&address, timeout)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        write!(
            stream,
            "GET {} HTTP/1.1\r\nHost: {host}:{}\r\nAccept: */*\r\nConnection: close\r\n\r\n",
            self.path, self.port
        )?;
        let mut line = String::new();
        BufReader::new((&mut stream).take(8192)).read_line(&mut line)?;
        let status = line
            .split(' ')
            .nth(1)
            .and_then(|s| s.parse::<u16>().ok())
            .ok_or_else(|| std::io::Error::other(format!("not an HTTP answer: {line:?}")))?;
        if status >= 400 {
            return Err(std::io::Error::other(format!(
                "Server returned HTTP response code: {status} for URL: {}",
                self.text
            )));
        }
        Ok(())
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
        assert!(matches!(Url::parse("https://a"), Ok(Url::Https(_))));
        assert!(Url::parse("nothing").is_err());
        assert!(Url::parse("gopher://a").is_err());
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
