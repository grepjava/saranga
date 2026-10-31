//! The HTTP client `OIDCLoginModule` reaches its provider with
//! (`SharedHttpClientAccess`): HTTP/1.1 GETs, redirects not followed (Java's
//! `HttpClient` default), with a connect timeout and a timeout for the whole
//! exchange. `https` trusts the `caCertificate` file (PEM, every
//! certificate in it) over `tlsVersion`, or else the platform's trusted
//! certificates over TLS 1.2, as Java's default `SSLContext.getInstance("TLSv1.2")`
//! does.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A client for one provider: how it connects.
pub(crate) struct HttpClient {
    timeout: Duration,
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
            timeout: Duration::from_millis(timeout_ms.max(1) as u64),
            tls: tls_config(tls_version, ca_certificate).map(Arc::new),
        }
    }

    /// One GET of an `http` or `https` URL.
    pub(crate) fn get(&self, url: &str) -> Result<Response, String> {
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
        let deadline = Instant::now() + self.timeout;
        // Each address the name has, in turn (`localhost` may be ::1 first).
        let mut last = format!("{host}: no address");
        let mut connected = None;
        for addr in (host.as_str(), port)
            .to_socket_addrs()
            .map_err(|e| e.to_string())?
        {
            match TcpStream::connect_timeout(&addr, self.timeout) {
                Ok(s) => {
                    connected = Some(s);
                    break;
                }
                Err(e) => last = e.to_string(),
            }
        }
        let tcp = connected.ok_or(last)?;
        tcp.set_read_timeout(Some(self.timeout)).ok();
        tcp.set_write_timeout(Some(self.timeout)).ok();
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {authority}\r\nUser-Agent: saranga\r\nAccept: */*\r\nConnection: close\r\n\r\n"
        );
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
    request: &str,
    deadline: Instant,
) -> Result<Response, String> {
    stream
        .write_all(request.as_bytes())
        .map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;
    let mut data = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        if Instant::now() > deadline {
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

fn tls_config(
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
