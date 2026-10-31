//! The HTTP client of the login modules that call a server: HTTP/1.1
//! requests, redirects not followed (Java's `HttpClient` default).
//! `OIDCLoginModule` reaches its provider with it (`SharedHttpClientAccess`),
//! with a connect timeout and a timeout for the whole exchange; `https`
//! trusts the `caCertificate` file (PEM, every certificate in it) over
//! `tlsVersion`, or else the platform's trusted certificates over TLS 1.2,
//! as Java's default `SSLContext.getInstance("TLSv1.2")` does.
//! `KubernetesLoginModule` posts its token reviews with it, with no timeout,
//! as `KubernetesClientImpl`'s client has none.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A client: how it connects.
pub(crate) struct HttpClient {
    timeout: Option<Duration>,
    tls: Result<Arc<rustls::ClientConfig>, String>,
}

/// A response: the status and the body.
pub(crate) struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

impl HttpClient {
    pub(crate) fn new(
        timeout_ms: i32,
        tls_version: Option<&str>,
        ca_certificate: Option<&str>,
    ) -> Self {
        Self {
            timeout: Some(Duration::from_millis(timeout_ms.max(1) as u64)),
            tls: tls_config(tls_version, ca_certificate).map(Arc::new),
        }
    }

    /// A client with no timeout, over this TLS configuration.
    pub(crate) fn untimed(tls: Arc<rustls::ClientConfig>) -> Self {
        Self {
            timeout: None,
            tls: Ok(tls),
        }
    }

    /// One GET of an `http` or `https` URL.
    pub(crate) fn get(&self, url: &str) -> Result<Response, String> {
        self.send("GET", url, &[("Accept", "*/*")], None)
    }

    /// One POST of `body`.
    pub(crate) fn post(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Result<Response, String> {
        self.send("POST", url, headers, Some(body))
    }

    fn send(
        &self,
        method: &str,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<Response, String> {
        let (secure, rest) = if let Some(r) = url.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = url.strip_prefix("http://") {
            (false, r)
        } else {
            return Err(format!("invalid URI scheme {url}"));
        };
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        let (host, port) = split_authority(authority, if secure { 443 } else { 80 })?;
        let deadline = self.timeout.map(|t| Instant::now() + t);
        // Each address the name has, in turn, IPv4 first, as Java orders
        // them unless `java.net.preferIPv6Addresses` (`localhost` may be
        // ::1 first, where a refusal takes seconds on Windows).
        let mut last = format!("{host}: no address");
        let mut connected = None;
        let mut addrs: Vec<_> = (host.as_str(), port)
            .to_socket_addrs()
            .map_err(|e| e.to_string())?
            .collect();
        addrs.sort_by_key(|a| a.is_ipv6());
        for addr in addrs {
            let attempt = match self.timeout {
                Some(t) => TcpStream::connect_timeout(&addr, t),
                None => TcpStream::connect(addr),
            };
            match attempt {
                Ok(s) => {
                    connected = Some(s);
                    break;
                }
                Err(e) => last = e.to_string(),
            }
        }
        let tcp = connected.ok_or(last)?;
        tcp.set_read_timeout(self.timeout).ok();
        tcp.set_write_timeout(self.timeout).ok();
        let mut request =
            format!("{method} {path} HTTP/1.1\r\nHost: {authority}\r\nUser-Agent: saranga\r\n");
        for (name, value) in headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        if let Some(body) = body {
            request.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        request.push_str("Connection: close\r\n\r\n");
        let mut request = request.into_bytes();
        request.extend_from_slice(body.unwrap_or_default());
        if secure {
            let config = self.tls.clone()?;
            let name =
                rustls::pki_types::ServerName::try_from(host.clone()).map_err(|e| e.to_string())?;
            let conn = rustls::ClientConnection::new(config, name).map_err(|e| e.to_string())?;
            let mut stream = rustls::StreamOwned::new(conn, tcp);
            exchange(&mut stream, &request, deadline)
        } else {
            let mut stream = tcp;
            exchange(&mut stream, &request, deadline)
        }
    }
}

fn split_authority(authority: &str, default_port: u16) -> Result<(String, u16), String> {
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, after) = rest.split_once(']').ok_or("invalid IPv6 host")?;
        let port = match after.strip_prefix(':') {
            Some(p) => p.parse().map_err(|_| format!("invalid port {p}"))?,
            None => default_port,
        };
        return Ok((host.to_owned(), port));
    }
    match authority.rsplit_once(':') {
        Some((h, p)) => Ok((
            h.to_owned(),
            p.parse().map_err(|_| format!("invalid port {p}"))?,
        )),
        None => Ok((authority.to_owned(), default_port)),
    }
}

fn exchange(
    stream: &mut (impl Read + Write),
    request: &[u8],
    deadline: Option<Instant>,
) -> Result<Response, String> {
    stream.write_all(request).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;
    let mut data = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        if deadline.is_some_and(|d| Instant::now() > d) {
            return Err("request timed out".into());
        }
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => data.extend_from_slice(&buf[..n]),
            // A TLS peer that closes without close_notify.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                return Err("request timed out".into());
            }
            Err(e) => return Err(e.to_string()),
        }
        if let Some(r) = complete(&data, false)? {
            return Ok(r);
        }
    }
    complete(&data, true)?.ok_or_else(|| "the connection closed before the response ended".into())
}

/// The response, once `data` holds all of it.
fn complete(data: &[u8], eof: bool) -> Result<Option<Response>, String> {
    let Some(end) = data.windows(4).position(|w| w == b"\r\n\r\n") else {
        return Ok(None);
    };
    let head = String::from_utf8_lossy(&data[..end]);
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status: u16 = status_line
        .split(' ')
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("invalid status line: {status_line}"))?;
    let mut length = None;
    let mut chunked = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let v = v.trim();
            if k.eq_ignore_ascii_case("content-length") {
                length = v.parse::<usize>().ok();
            } else if k.eq_ignore_ascii_case("transfer-encoding")
                && v.eq_ignore_ascii_case("chunked")
            {
                chunked = true;
            }
        }
    }
    let body = &data[end + 4..];
    if chunked {
        return Ok(dechunk(body).map(|body| Response { status, body }));
    }
    match length {
        Some(n) if body.len() >= n => Ok(Some(Response {
            status,
            body: body[..n].to_vec(),
        })),
        Some(_) => Ok(None),
        // Read to the end of the connection.
        None => Ok(eof.then(|| Response {
            status,
            body: body.to_vec(),
        })),
    }
}

fn dechunk(mut body: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let line_end = body.windows(2).position(|w| w == b"\r\n")?;
        let size_text = String::from_utf8_lossy(&body[..line_end]);
        let size = usize::from_str_radix(size_text.split(';').next()?.trim(), 16).ok()?;
        body = &body[line_end + 2..];
        if size == 0 {
            return Some(out);
        }
        if body.len() < size + 2 {
            return None;
        }
        out.extend_from_slice(&body[..size]);
        body = &body[size + 2..];
    }
}

pub(crate) fn tls_config(
    tls_version: Option<&str>,
    ca_certificate: Option<&str>,
) -> Result<rustls::ClientConfig, String> {
    let mut roots = rustls::RootCertStore::empty();
    let mut version = "TLSv1.2";
    let ca = match (tls_version, ca_certificate) {
        (Some(v), Some(ca)) => {
            if std::path::Path::new(ca).is_file() {
                version = v;
                Some(ca)
            } else {
                tracing::warn!("The certificate file {ca} does not exist");
                None
            }
        }
        _ => None,
    };
    match ca {
        Some(ca) => {
            let store = crate::keystore::KeyStore::load("PEMCA", Some(ca), None, None)
                .map_err(|e| format!("Can't configure SSL Context for HTTP Client: {e}"))?;
            roots.add_parsable_certificates(store.trusted_certificates());
        }
        None => {
            let found = rustls_native_certs::load_native_certs();
            roots.add_parsable_certificates(found.certs);
        }
    }
    let versions: &[&rustls::SupportedProtocolVersion] = match version {
        "TLSv1.2" => &[&rustls::version::TLS12],
        "TLSv1.3" | "TLS" => &[&rustls::version::TLS13, &rustls::version::TLS12],
        other => return Err(format!("{other} SSLContext not available")),
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Ok(rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(versions)
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_are_read_by_length_chunks_or_close() {
        let r = complete(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi", false)
            .unwrap()
            .unwrap();
        assert_eq!((r.status, r.body.as_slice()), (200, &b"hi"[..]));
        assert!(
            complete(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhi", false)
                .unwrap()
                .is_none()
        );
        let r = complete(b"HTTP/1.1 200 OK\r\n\r\nall", true)
            .unwrap()
            .unwrap();
        assert_eq!(r.body, b"all");
        let r = complete(
            b"HTTP/1.1 404 x\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n1\r\n!\r\n0\r\n\r\n",
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!((r.status, r.body.as_slice()), (404, &b"hi!"[..]));
        assert_eq!(split_authority("h:8080", 80).unwrap(), ("h".into(), 8080));
        assert_eq!(split_authority("[::1]", 443).unwrap(), ("::1".into(), 443));
    }
}
