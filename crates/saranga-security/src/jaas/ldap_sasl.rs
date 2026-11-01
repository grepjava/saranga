//! `LDAPLoginModule`'s GSSAPI bind (`authentication=GSSAPI`): the broker's
//! Kerberos identity from a login through `saslLoginConfigScope`
//! (`broker-sasl-gssapi` unless given), then a SASL bind as JNDI's
//! `LdapSasl` makes one with the JDK's `GssKrb5Client` to `ldap@<host>`.
//! ldap3 has no SASL bind of its own that does not link a Kerberos library,
//! so the bind is made here on the socket, which ldap3 then takes: as it is
//! for `ldap://`, through a relay of the TLS session for `ldaps://`.
//!
//! The options JNDI takes from the environment apply:
//! `java.naming.security.sasl.authorizationId`, `javax.security.sasl.qop`
//! and `javax.security.sasl.server.authentication`. The security layer
//! must be `auth`: integrity and privacy would wrap every LDAP message
//! after the bind, which ldap3 cannot do.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ldap3::{LdapConn, LdapConnSettings, StdStream};

use super::ldap::{DirError, Options};
use super::{Callbacks, LoginError};
use crate::kerberos::{self, Initiator, layer};

const SUCCESS: u32 = 0;
const SASL_BIND_IN_PROGRESS: u32 = 14;

/// The broker's identity: `new LoginContext(configScope).login()`.
pub(crate) fn identity(o: &Options) -> Result<Option<Initiator>, LoginError> {
    let scope = o
        .set("saslLoginConfigScope")
        .unwrap_or("broker-sasl-gssapi");
    let callbacks = Callbacks {
        no_handler: true,
        ..Callbacks::default()
    };
    match super::jvm_login(scope, &callbacks) {
        Ok(subject) => Ok(subject.kerberos_initiator().cloned()),
        Err(e) => {
            tracing::debug!("{e}");
            Err(LoginError::new(format!(
                "Error contacting LDAP using GSSAPI in JAAS loginConfigScope: {scope}"
            )))
        }
    }
}

/// Connects to `url` and binds with GSSAPI, for ldap3 to go on with.
pub(crate) fn connect(
    url: &str,
    o: &Options,
    identity: Option<&Initiator>,
    tls: impl FnOnce() -> Result<Arc<rustls::ClientConfig>, String>,
) -> Result<LdapConn, DirError> {
    let (secure, rest) = if let Some(r) = url.strip_prefix("ldaps://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("ldap://") {
        (false, r)
    } else {
        return Err(DirError::naming(format!("unsupported URL {url}")));
    };
    let authority = rest.split('/').next().unwrap_or_default();
    let (host, port) = split_authority(authority, if secure { 636 } else { 389 })?;
    let tcp = connect_tcp(&host, port, o.millis("connectionTimeout"))?;
    let read_timeout = o.millis("readTimeout");
    tcp.set_read_timeout(read_timeout).ok();
    let identity = identity.ok_or_else(|| {
        DirError::authentication(
            "GSSAPI: No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt)",
        )
    })?;
    let bound = if secure {
        let config = tls().map_err(DirError::naming)?;
        let name = rustls::pki_types::ServerName::try_from(host.clone())
            .map_err(|e| DirError::naming(e.to_string()))?;
        let conn = rustls::ClientConnection::new(config, name)
            .map_err(|e| DirError::naming(e.to_string()))?;
        let mut stream = rustls::StreamOwned::new(conn, tcp);
        bind(&mut stream, o, identity, &host)?;
        relay(stream).map_err(|e| DirError::naming(e.to_string()))?
    } else {
        let mut tcp = tcp;
        bind(&mut tcp, o, identity, &host)?;
        tcp
    };
    bound.set_read_timeout(None).ok();
    let settings = LdapConnSettings::new().set_std_stream(StdStream::Tcp(bound));
    // ldap3 takes the stream as it is: the URL only says plain LDAP.
    LdapConn::with_settings(settings, &format!("ldap://{authority}")).map_err(DirError::from_ldap)
}

fn split_authority(authority: &str, default_port: u16) -> Result<(String, u16), DirError> {
    let bad = || DirError::naming(format!("invalid LDAP URL authority {authority}"));
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, after) = rest.split_once(']').ok_or_else(bad)?;
        let port = match after.strip_prefix(':') {
            Some(p) => p.parse().map_err(|_| bad())?,
            None => default_port,
        };
        return Ok((host.to_owned(), port));
    }
    match authority.rsplit_once(':') {
        Some((h, p)) => Ok((h.to_owned(), p.parse().map_err(|_| bad())?)),
        None => Ok((authority.to_owned(), default_port)),
    }
}

fn connect_tcp(host: &str, port: u16, timeout: Option<Duration>) -> Result<TcpStream, DirError> {
    let mut last = DirError::communication(format!("{host}:{port}: no address"));
    let addrs = (host, port)
        .to_socket_addrs()
        .map_err(|e| DirError::communication(format!("{host}:{port}: {e}")))?;
    for addr in addrs {
        let r = match timeout {
            Some(t) => TcpStream::connect_timeout(&addr, t),
            None => TcpStream::connect(addr),
        };
        match r {
            Ok(s) => return Ok(s),
            Err(e) => last = DirError::communication(format!("{host}:{port}: {e}")),
        }
    }
    Err(last)
}

/// The client's side of the exchange (`GssKrb5Client`).
struct Client {
    ctx: Box<dyn kerberos::SecurityContext>,
    final_handshake: bool,
    complete: bool,
    authzid: Option<Vec<u8>>,
    /// `javax.security.sasl.qop`, in the client's order of preference.
    qop: Vec<u8>,
}

impl Client {
    /// `evaluateChallenge`.
    fn evaluate(&mut self, challenge: &[u8]) -> Result<Option<Vec<u8>>, String> {
        if !self.final_handshake {
            let out = self
                .ctx
                .step(challenge)
                .map_err(|e| format!("GSS initiate failed: {e}"))?;
            if self.ctx.is_established() {
                self.final_handshake = true;
                if out.is_none() {
                    return Ok(Some(Vec::new()));
                }
            }
            return Ok(out);
        }
        if challenge.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let offered = self
            .ctx
            .unwrap(challenge)
            .map_err(|e| format!("Final handshake failed: {e}"))?;
        let mask = *offered
            .first()
            .ok_or("Final handshake failed: empty token")?;
        // `findPreferredMask`.
        let selected = self
            .qop
            .iter()
            .copied()
            .find(|q| mask & q != 0)
            .unwrap_or(0);
        if selected == 0 {
            return Err("No common protection layer between client and server".into());
        }
        if selected != layer::NO_PROTECTION {
            return Err(format!(
                "the SASL security layer {} is not supported: javax.security.sasl.qop must allow auth",
                if selected == layer::PRIVACY_PROTECTION {
                    "auth-conf"
                } else {
                    "auth-int"
                }
            ));
        }
        let mut token = vec![selected, 0, 0, 0];
        if let Some(a) = &self.authzid {
            token.extend_from_slice(a);
        }
        let wrapped = self
            .ctx
            .wrap(&token)
            .map_err(|e| format!("Final handshake failed: {e}"))?;
        self.complete = true;
        Ok(Some(wrapped))
    }
}

/// `javax.security.sasl.qop`: `auth`, `auth-int` and `auth-conf` in order.
fn qop(option: Option<&str>) -> Result<Vec<u8>, String> {
    let Some(text) = option else {
        return Ok(vec![layer::NO_PROTECTION]);
    };
    text.split(',')
        .map(|q| match q.trim() {
            "auth" => Ok(layer::NO_PROTECTION),
            "auth-int" => Ok(layer::INTEGRITY_ONLY_PROTECTION),
            "auth-conf" => Ok(layer::PRIVACY_PROTECTION),
            other => Err(format!("Invalid QOP: {other}")),
        })
        .collect()
}

/// `LdapSasl.saslBind`.
fn bind(
    stream: &mut (impl Read + Write),
    o: &Options,
    identity: &Initiator,
    host: &str,
) -> Result<(), DirError> {
    let auth = |m: String| DirError::authentication(m);
    let qop = qop(o.get("javax.security.sasl.qop")).map_err(auth)?;
    let integrity = qop.iter().any(|q| *q != layer::NO_PROTECTION);
    let mutual = match o.get("javax.security.sasl.server.authentication") {
        Some(v) => v.eq_ignore_ascii_case("true"),
        None => integrity,
    };
    let ctx = kerberos::initiate(identity, &format!("ldap@{host}"), mutual)
        .map_err(|e| auth(format!("Failure to initialize security context: {e}")))?;
    let mut client = Client {
        ctx,
        final_handshake: false,
        complete: false,
        authzid: o
            .get("java.naming.security.sasl.authorizationId")
            .filter(|a| !a.is_empty())
            .map(|a| a.as_bytes().to_vec()),
        qop,
    };
    let mut id = 0;
    let mut response = client.evaluate(&[]).map_err(auth)?;
    let mut result = exchange(stream, &mut id, response.as_deref())?;
    while !client.complete && (result.code == SASL_BIND_IN_PROGRESS || result.code == SUCCESS) {
        response = client
            .evaluate(result.credentials.as_deref().unwrap_or_default())
            .map_err(auth)?;
        if result.code == SUCCESS {
            if response.is_some() {
                return Err(auth("SASL client generated response after success".into()));
            }
            break;
        }
        result = exchange(stream, &mut id, response.as_deref())?;
    }
    if result.code == SUCCESS {
        if !client.complete {
            return Err(auth(
                "SASL authentication not complete despite server claims".into(),
            ));
        }
        return Ok(());
    }
    Err(DirError::ldap_result(result.code, &result.message))
}

struct BindResult {
    code: u32,
    message: String,
    credentials: Option<Vec<u8>>,
}

fn exchange(
    stream: &mut (impl Read + Write),
    id: &mut i32,
    credentials: Option<&[u8]>,
) -> Result<BindResult, DirError> {
    *id += 1;
    let io = |e: std::io::Error| DirError::communication(e.to_string());
    stream
        .write_all(&bind_request(*id, credentials))
        .map_err(io)?;
    stream.flush().map_err(io)?;
    loop {
        let message = read_message(stream).map_err(io)?;
        let r = parse_bind_response(&message)
            .ok_or_else(|| DirError::communication("invalid LDAP bind response"))?;
        match r {
            (0, _) => {
                return Err(DirError::communication(
                    "the LDAP server sent a notice of disconnection",
                ));
            }
            (n, r) if n == *id => return Ok(r),
            _ => {}
        }
    }
}

// ------------------------------------------------------------ BER

fn tlv(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let n = content.len();
    if n < 0x80 {
        out.push(n as u8);
    } else {
        let bytes: Vec<u8> = n
            .to_be_bytes()
            .into_iter()
            .skip_while(|b| *b == 0)
            .collect();
        out.push(0x80 | bytes.len() as u8);
        out.extend(bytes);
    }
    out.extend_from_slice(content);
    out
}

fn integer(n: i32) -> Vec<u8> {
    let bytes = n.to_be_bytes();
    let mut i = 0;
    while i < 3
        && ((bytes[i] == 0 && bytes[i + 1] & 0x80 == 0)
            || (bytes[i] == 0xff && bytes[i + 1] & 0x80 != 0))
    {
        i += 1;
    }
    tlv(0x02, &bytes[i..])
}

/// `LDAPMessage { id, BindRequest { 3, "", sasl { "GSSAPI", creds } } }`.
fn bind_request(id: i32, credentials: Option<&[u8]>) -> Vec<u8> {
    let mut sasl = tlv(0x04, b"GSSAPI");
    if let Some(c) = credentials {
        sasl.extend(tlv(0x04, c));
    }
    let op = [integer(3), tlv(0x04, b""), tlv(0xa3, &sasl)].concat();
    tlv(0x30, &[integer(id), tlv(0x60, &op)].concat())
}

fn read_message(stream: &mut impl Read) -> std::io::Result<Vec<u8>> {
    let mut head = [0u8; 2];
    stream.read_exact(&mut head)?;
    let mut message = head.to_vec();
    let len = if head[1] & 0x80 == 0 {
        head[1] as usize
    } else {
        let n = (head[1] & 0x7f) as usize;
        if n == 0 || n > 4 {
            return Err(std::io::Error::other("invalid BER length"));
        }
        let mut bytes = vec![0u8; n];
        stream.read_exact(&mut bytes)?;
        message.extend_from_slice(&bytes);
        bytes.iter().fold(0usize, |a, b| (a << 8) | *b as usize)
    };
    let start = message.len();
    message.resize(start + len, 0);
    stream.read_exact(&mut message[start..])?;
    Ok(message)
}

/// One TLV: its tag, its content and what follows it.
fn next(data: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let tag = *data.first()?;
    let first = *data.get(1)?;
    let (len, at) = if first & 0x80 == 0 {
        (first as usize, 2)
    } else {
        let n = (first & 0x7f) as usize;
        let bytes = data.get(2..2 + n)?;
        (
            bytes.iter().fold(0usize, |a, b| (a << 8) | *b as usize),
            2 + n,
        )
    };
    let content = data.get(at..at + len)?;
    Some((tag, content, &data[at + len..]))
}

fn parse_int(content: &[u8]) -> i64 {
    content.iter().fold(
        if content.first().is_some_and(|b| b & 0x80 != 0) {
            -1
        } else {
            0
        },
        |a, b| (a << 8) | *b as i64,
    )
}

fn parse_bind_response(message: &[u8]) -> Option<(i32, BindResult)> {
    let (0x30, body, _) = next(message)? else {
        return None;
    };
    let (0x02, id, rest) = next(body)? else {
        return None;
    };
    let id = parse_int(id) as i32;
    let (tag, op, _) = next(rest)?;
    if id == 0 {
        return Some((
            0,
            BindResult {
                code: 0,
                message: String::new(),
                credentials: None,
            },
        ));
    }
    if tag != 0x61 {
        return None;
    }
    let (0x0a, code, rest) = next(op)? else {
        return None;
    };
    let (_, _matched, rest) = next(rest)?;
    let (_, diagnostic, mut rest) = next(rest)?;
    let mut credentials = None;
    while let Some((tag, content, more)) = next(rest) {
        if tag == 0x87 {
            credentials = Some(content.to_vec());
        }
        rest = more;
    }
    Some((
        id,
        BindResult {
            code: parse_int(code) as u32,
            message: String::from_utf8_lossy(diagnostic).into_owned(),
            credentials,
        },
    ))
}

// ------------------------------------------------------------ TLS relay

/// A local socket whose other end is relayed to and from the TLS session,
/// for ldap3 to take as a plain connection.
fn relay(
    stream: rustls::StreamOwned<rustls::ClientConnection, TcpStream>,
) -> std::io::Result<TcpStream> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let local = TcpStream::connect(listener.local_addr()?)?;
    let (inner, _) = listener.accept()?;
    let rustls::StreamOwned { conn, sock } = stream;
    let conn = Arc::new(Mutex::new(conn));
    let (sock_in, sock_out) = (sock.try_clone()?, sock);
    let (inner_in, inner_out) = (inner.try_clone()?, inner);
    // Server to client.
    {
        let conn = conn.clone();
        let mut sock = sock_in;
        let mut inner = inner_out;
        std::thread::spawn(move || {
            let mut buf = [0u8; 16384];
            loop {
                let n = match sock.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                let mut plain = Vec::new();
                {
                    let mut c = conn.lock().unwrap_or_else(|e| e.into_inner());
                    let mut data = &buf[..n];
                    while !data.is_empty() {
                        if c.read_tls(&mut data).is_err() {
                            break;
                        }
                    }
                    if c.process_new_packets().is_err() {
                        break;
                    }
                    let _ = c.reader().read_to_end(&mut plain);
                    // Anything TLS needs to send in answer.
                    let mut s = &sock;
                    while c.wants_write() {
                        if c.write_tls(&mut s).is_err() {
                            break;
                        }
                    }
                }
                if !plain.is_empty() && inner.write_all(&plain).is_err() {
                    break;
                }
            }
            let _ = inner.shutdown(Shutdown::Both);
            let _ = sock.shutdown(Shutdown::Both);
        });
    }
    // Client to server.
    {
        let mut sock = sock_out;
        let mut inner = inner_in;
        std::thread::spawn(move || {
            let mut buf = [0u8; 16384];
            loop {
                let n = match inner.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                let mut c = conn.lock().unwrap_or_else(|e| e.into_inner());
                if c.writer().write_all(&buf[..n]).is_err() {
                    break;
                }
                let mut failed = false;
                while c.wants_write() {
                    if c.write_tls(&mut sock).is_err() {
                        failed = true;
                        break;
                    }
                }
                if failed {
                    break;
                }
            }
            {
                let mut c = conn.lock().unwrap_or_else(|e| e.into_inner());
                c.send_close_notify();
                while c.wants_write() {
                    if c.write_tls(&mut sock).is_err() {
                        break;
                    }
                }
            }
            let _ = sock.shutdown(Shutdown::Both);
        });
    }
    Ok(local)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_requests_and_responses_are_ber() {
        assert_eq!(integer(3), [2, 1, 3]);
        assert_eq!(integer(128), [2, 2, 0, 128]);
        let r = bind_request(1, Some(b"tok"));
        assert_eq!(
            r,
            [
                0x30, 0x19, 2, 1, 1, 0x60, 0x14, 2, 1, 3, 4, 0, 0xa3, 0x0d, 4, 6, b'G', b'S', b'S',
                b'A', b'P', b'I', 4, 3, b't', b'o', b'k'
            ]
        );
        let long = tlv(4, &[0u8; 300]);
        assert_eq!(&long[..4], &[4, 0x82, 1, 44]);
        // BindResponse { 14, "", "", serverSaslCreds "ab" }
        let resp = tlv(
            0x30,
            &[
                integer(2),
                tlv(
                    0x61,
                    &[tlv(0x0a, &[14]), tlv(4, b""), tlv(4, b""), tlv(0x87, b"ab")].concat(),
                ),
            ]
            .concat(),
        );
        let mut cursor = std::io::Cursor::new(resp.clone());
        assert_eq!(read_message(&mut cursor).unwrap(), resp);
        let (id, r) = parse_bind_response(&resp).unwrap();
        assert_eq!(
            (id, r.code, r.credentials.as_deref()),
            (2, 14, Some(&b"ab"[..]))
        );
        assert_eq!(
            qop(Some("auth-conf, auth")).unwrap(),
            [layer::PRIVACY_PROTECTION, layer::NO_PROTECTION]
        );
        assert!(qop(Some("none")).is_err());
    }
}
