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
            let attempt = match remaining(deadline)? {
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

/// What is left of the exchange's time: `None` for no limit, an error once
/// it is up.
fn remaining(deadline: Option<Instant>) -> Result<Option<Duration>, String> {
    let Some(d) = deadline else {
        return Ok(None);
    };
    match d.checked_duration_since(Instant::now()) {
        Some(left) if !left.is_zero() => Ok(Some(left)),
        _ => Err("request timed out".into()),
    }
}

/// A stream whose socket waits at most so long for each read or write.
trait Timed {
    fn wait_at_most(&self, t: Option<Duration>);
}

impl Timed for TcpStream {
    fn wait_at_most(&self, t: Option<Duration>) {
        self.set_read_timeout(t).ok();
        self.set_write_timeout(t).ok();
    }
}

impl Timed for rustls::StreamOwned<rustls::ClientConnection, TcpStream> {
    fn wait_at_most(&self, t: Option<Duration>) {
        self.sock.wait_at_most(t);
    }
}

/// Sends the request and reads the response, the whole of it within the
/// deadline: each wait on the socket gets only what is left, and a
/// response completed after it is refused.
fn exchange(
    stream: &mut (impl Read + Write + Timed),
    request: &[u8],
    deadline: Option<Instant>,
) -> Result<Response, String> {
    stream.wait_at_most(remaining(deadline)?);
    stream.write_all(request).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;
    let mut data = Vec::new();
    let mut reading = Reading::default();
    let mut buf = [0u8; 8192];
    loop {
        stream.wait_at_most(remaining(deadline)?);
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                data.extend_from_slice(&buf[..n]);
                if data.len() > MAX_RESPONSE {
                    return Err(format!("the response is over {MAX_RESPONSE} bytes"));
                }
            }
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
        if let Some(r) = reading.advance(&data, false)? {
            remaining(deadline)?;
            return Ok(r);
        }
    }
    let r = reading.advance(&data, true)?;
    remaining(deadline)?;
    r.ok_or_else(|| "the connection closed before the response ended".into())
}

/// The most bytes a response may take, its head included: the metadata,
/// keys and reviews the login modules fetch are far smaller.
const MAX_RESPONSE: usize = 8 * 1024 * 1024;

/// The longest chunk-size line read (`size[;extensions]`).
const MAX_CHUNK_LINE: usize = 4096;

/// The response, once `data` holds all of it, read at once.
#[cfg(test)]
fn complete(data: &[u8], eof: bool) -> Result<Option<Response>, String> {
    Reading::default().advance(data, eof)
}

/// A response as its bytes come: each call goes on from where the last
/// stopped, so a body is decoded once however it arrives.
#[derive(Default)]
struct Reading {
    /// How far the search for the end of the head has gone.
    searched: usize,
    head: Option<Head>,
    /// A chunked body decoded so far, and where its next chunk starts.
    decoded: Vec<u8>,
    next_chunk: usize,
}

struct Head {
    status: u16,
    length: Option<usize>,
    chunked: bool,
    /// Where the body starts.
    body: usize,
}

impl Reading {
    /// The response, once `data` (everything read so far) holds all of it.
    fn advance(&mut self, data: &[u8], eof: bool) -> Result<Option<Response>, String> {
        if self.head.is_none() {
            let from = self.searched.saturating_sub(3);
            let Some(at) = data[from..].windows(4).position(|w| w == b"\r\n\r\n") else {
                self.searched = data.len();
                return Ok(None);
            };
            let end = from + at;
            let head = parse_head(&data[..end])?;
            self.next_chunk = end + 4;
            self.head = Some(Head {
                body: end + 4,
                ..head
            });
        }
        let head = self.head.as_ref().expect("read above");
        let (status, start) = (head.status, head.body);
        if head.chunked {
            return Ok(self.dechunk(data)?.map(|body| Response { status, body }));
        }
        let body = &data[start..];
        match head.length {
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

    /// The chunks of `data` from [`Self::next_chunk`] on: the whole body
    /// once its last chunk is there, an error for one that cannot be read.
    fn dechunk(&mut self, data: &[u8]) -> Result<Option<Vec<u8>>, String> {
        loop {
            let rest = &data[self.next_chunk..];
            let line = &rest[..rest.len().min(MAX_CHUNK_LINE + 2)];
            let Some(line_end) = line.windows(2).position(|w| w == b"\r\n") else {
                if rest.len() > MAX_CHUNK_LINE {
                    return Err("invalid chunk: its size line is too long".into());
                }
                return Ok(None);
            };
            let size_line = String::from_utf8_lossy(&rest[..line_end]);
            let size_text = size_line.split(';').next().unwrap_or_default().trim();
            // The chunk and the CRLF after it must fit in an address.
            let size = usize::from_str_radix(size_text, 16)
                .ok()
                .filter(|s| s.checked_add(2).is_some())
                .ok_or_else(|| format!("invalid chunk size {size_text}"))?;
            let chunk = &rest[line_end + 2..];
            if size == 0 {
                return Ok(Some(std::mem::take(&mut self.decoded)));
            }
            if chunk.len() < size + 2 {
                return Ok(None);
            }
            if &chunk[size..size + 2] != b"\r\n" {
                return Err("invalid chunk: no CRLF after its data".into());
            }
            self.decoded.extend_from_slice(&chunk[..size]);
            self.next_chunk += line_end + 2 + size + 2;
        }
    }
}

/// A response's status line and headers.
fn parse_head(head: &[u8]) -> Result<Head, String> {
    let head = String::from_utf8_lossy(head);
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
    Ok(Head {
        status,
        length,
        chunked,
        body: 0,
    })
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

    /// A chunk size no body could have, or a chunk not followed by its
    /// CRLF, is an error, not a panic or a wait for more.
    #[test]
    fn a_chunk_that_cannot_be_read_is_an_error() {
        for body in [
            &b"ffffffffffffffff\r\nhi\r\n0\r\n\r\n"[..],
            b"fffffffffffffffe\r\nhi\r\n0\r\n\r\n",
            b"zz\r\nhi\r\n0\r\n\r\n",
            b"2\r\nhiXX1\r\n!\r\n0\r\n\r\n",
        ] {
            let mut data = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
            data.extend_from_slice(body);
            assert!(
                complete(&data, false).is_err(),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    /// The timeout bounds the whole exchange: a response whose pieces each
    /// come within it, but whose last comes after it, is refused.
    #[test]
    fn a_response_completed_after_the_timeout_is_refused() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut seen = Vec::new();
            let mut buf = [0u8; 1024];
            while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = s.read(&mut buf).unwrap();
                seen.extend_from_slice(&buf[..n]);
            }
            let start = Instant::now();
            s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n")
                .unwrap();
            std::thread::sleep(Duration::from_millis(150));
            let _ = s.write_all(b"a");
            std::thread::sleep(Duration::from_millis(130));
            let _ = s.write_all(b"b");
            std::thread::sleep(Duration::from_millis(200));
            start.elapsed()
        });
        let started = Instant::now();
        let result = HttpClient::new(200, None, None).get(&format!("http://127.0.0.1:{port}/"));
        let took = started.elapsed();
        assert!(
            server.join().unwrap() >= Duration::from_millis(280),
            "the server sent its last byte after the timeout"
        );
        assert_eq!(
            result.err().as_deref(),
            Some("request timed out"),
            "after {took:?}"
        );
        assert!(
            took < Duration::from_millis(270),
            "gave up at the timeout, not later: {took:?}"
        );
    }

    /// A chunked response of `size` bytes in 1 KiB chunks.
    fn chunked(size: usize) -> Vec<u8> {
        let mut data = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
        for _ in 0..size / 1024 {
            data.extend_from_slice(b"400\r\n");
            data.extend_from_slice(&[b'x'; 1024]);
            data.extend_from_slice(b"\r\n");
        }
        data.extend_from_slice(b"0\r\n\r\n");
        data
    }

    /// The time to read `data` as it comes in 8 KiB reads.
    fn read_in_pieces(data: &[u8], size: usize) -> Duration {
        let start = Instant::now();
        let mut reading = Reading::default();
        let mut done = None;
        for end in (8192..data.len()).step_by(8192).chain([data.len()]) {
            if let Some(r) = reading.advance(&data[..end], false).unwrap() {
                done = Some(r);
                break;
            }
        }
        assert_eq!(done.expect("read").body.len(), size);
        start.elapsed()
    }

    /// A chunked body is decoded once as it comes: eight times the body
    /// takes about eight times as long, not sixty-four.
    #[test]
    fn a_chunked_body_is_read_in_linear_time() {
        let small = chunked(512 * 1024);
        let large = chunked(4 * 1024 * 1024);
        let (mut a, mut b) = (Duration::MAX, Duration::MAX);
        for _ in 0..3 {
            a = a.min(read_in_pieces(&small, 512 * 1024));
            b = b.min(read_in_pieces(&large, 4 * 1024 * 1024));
        }
        assert!(b < a * 24, "512 KiB took {a:?}, 4 MiB took {b:?}");
    }

    /// A response over `MAX_RESPONSE` bytes is refused, however it is
    /// framed, rather than held in memory.
    #[test]
    fn a_response_too_large_is_refused() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut seen = Vec::new();
            let mut buf = [0u8; 1024];
            while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = s.read(&mut buf).unwrap();
                seen.extend_from_slice(&buf[..n]);
            }
            let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 99999999\r\n\r\n");
            let piece = vec![b'x'; 64 * 1024];
            for _ in 0..(MAX_RESPONSE / piece.len() + 8) {
                if s.write_all(&piece).is_err() {
                    break;
                }
            }
        });
        let result = HttpClient::new(10_000, None, None).get(&format!("http://127.0.0.1:{port}/"));
        let e = result.err().expect("refused");
        assert!(e.contains("over"), "{e}");
    }
}
