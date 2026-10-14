//! A ZooKeeper client: one session kept over the ensemble's servers, with
//! pings, reconnection within the session and the connection states
//! Curator reports (`CONNECTED`, `SUSPENDED`, `RECONNECTED`, `LOST`),
//! including Curator's simulated session expiration. It has the
//! operations the lock managers need; watches are not kept.

pub mod proto;

use std::collections::VecDeque;
use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::Instant;

pub use proto::{CreateMode, Stat};
use proto::{In, Out, code, op, xid};

/// A failed operation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// `KeeperException` with its code.
    #[error("KeeperErrorCode = {} for {path}", code_name(*.code))]
    Api { code: i32, path: String },
    #[error("KeeperErrorCode = ConnectionLoss")]
    ConnectionLoss,
    #[error("KeeperErrorCode = Session expired")]
    SessionExpired,
    #[error("the ZooKeeper client is closed")]
    Closed,
    #[error("{0}")]
    Config(String),
}

impl Error {
    pub fn code(&self) -> i32 {
        match self {
            Self::Api { code, .. } => *code,
            Self::ConnectionLoss => code::CONNECTION_LOSS,
            Self::SessionExpired => code::SESSION_EXPIRED,
            Self::Closed | Self::Config(_) => code::CONNECTION_LOSS,
        }
    }
    /// Worth trying again (`RetryLoop.shouldRetry`).
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.code(),
            code::CONNECTION_LOSS | code::OPERATION_TIMEOUT | code::SESSION_MOVED
        ) && !matches!(self, Self::Closed | Self::Config(_))
    }
}

fn code_name(code: i32) -> &'static str {
    match code {
        code::NO_NODE => "NoNode",
        code::NODE_EXISTS => "NodeExists",
        code::NOT_EMPTY => "Directory not empty",
        code::BAD_VERSION => "BadVersion",
        code::NO_CHILDREN_FOR_EPHEMERALS => "NoChildrenForEphemerals",
        code::SESSION_EXPIRED => "Session expired",
        code::CONNECTION_LOSS => "ConnectionLoss",
        _ => "APIError",
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Curator's `ConnectionState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    Suspended,
    Reconnected,
    Lost,
}

/// What the session is connected with.
#[derive(Debug, Clone)]
pub struct Config {
    /// `host:port,host:port[/chroot]`.
    pub connect_string: String,
    pub session_timeout: Duration,
    /// How long an operation waits for a connection.
    pub connection_timeout: Duration,
    /// Curator's `simulatedSessionExpirationPercent`: a session suspended
    /// for this part of its timeout is taken as lost.
    pub session_percent: u32,
}

/// The servers and the chroot of a connect string.
fn parse_connect_string(s: &str) -> Result<(Vec<(String, u16)>, String)> {
    let (hosts, chroot) = match s.find('/') {
        Some(i) => (&s[..i], s[i..].trim_end_matches('/')),
        None => (s, ""),
    };
    let mut servers = Vec::new();
    for h in hosts.split(',').map(str::trim).filter(|h| !h.is_empty()) {
        let (host, port) = if let Some(rest) = h.strip_prefix('[') {
            let end = rest
                .find(']')
                .ok_or_else(|| Error::Config(format!("bad host {h}")))?;
            let port = rest[end + 1..].strip_prefix(':');
            (rest[..end].to_owned(), port)
        } else {
            match h.rsplit_once(':') {
                Some((host, port)) => (host.to_owned(), Some(port)),
                None => (h.to_owned(), None),
            }
        };
        let port = match port {
            Some(p) => p
                .parse()
                .map_err(|_| Error::Config(format!("bad port in {h}")))?,
            None => 2181,
        };
        servers.push((host, port));
    }
    if servers.is_empty() {
        return Err(Error::Config(
            "no ZooKeeper server in the connect string".into(),
        ));
    }
    Ok((servers, chroot.to_owned()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Connecting,
    Connected,
    Closed,
}

/// The requests sent, waiting for their replies in order: xid, opcode
/// and who waits.
type Pending = VecDeque<(i32, i32, oneshot::Sender<Result<Vec<u8>>>)>;

struct Call {
    op: i32,
    body: Vec<u8>,
    reply: oneshot::Sender<Result<Vec<u8>>>,
}

/// A session with an ensemble. Clones share it.
#[derive(Clone)]
pub struct ZooKeeper {
    calls: mpsc::UnboundedSender<Call>,
    status: watch::Receiver<Status>,
    events: broadcast::Sender<ConnectionState>,
    chroot: String,
    connection_timeout: Duration,
}

impl ZooKeeper {
    /// Starts connecting; [`ZooKeeper::wait_connected`] waits for it.
    pub fn start(config: Config) -> Result<Self> {
        let (servers, chroot) = parse_connect_string(&config.connect_string)?;
        let (calls, rx) = mpsc::unbounded_channel();
        let (status_tx, status) = watch::channel(Status::Connecting);
        let (events, _) = broadcast::channel(64);
        let session = Session {
            servers,
            next: 0,
            config: config.clone(),
            calls: rx,
            status: status_tx,
            events: events.clone(),
            session_id: 0,
            passwd: vec![0; 16],
            last_zxid: 0,
            negotiated: config.session_timeout,
            ever_connected: false,
            lost: false,
            closing: None,
        };
        tokio::spawn(session.run());
        Ok(Self {
            calls,
            status,
            events,
            chroot,
            connection_timeout: config.connection_timeout,
        })
    }

    /// `blockUntilConnected`: `None` waits for ever.
    pub async fn wait_connected(&self, timeout: Option<Duration>) -> bool {
        let mut s = self.status.clone();
        let wait = s.wait_for(|s| *s != Status::Connecting);
        let r = match timeout {
            Some(t) => match tokio::time::timeout(t, wait).await {
                Ok(r) => r.map(|s| *s),
                Err(_) => return false,
            },
            None => wait.await.map(|s| *s),
        };
        matches!(r, Ok(Status::Connected))
    }

    pub fn is_connected(&self) -> bool {
        *self.status.borrow() == Status::Connected
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ConnectionState> {
        self.events.subscribe()
    }

    fn path(&self, path: &str) -> String {
        if self.chroot.is_empty() {
            path.to_owned()
        } else if path == "/" {
            self.chroot.clone()
        } else {
            format!("{}{path}", self.chroot)
        }
    }

    fn unchroot(&self, path: String) -> String {
        match path.strip_prefix(&self.chroot) {
            Some("") => "/".to_owned(),
            Some(p) if !self.chroot.is_empty() => p.to_owned(),
            _ => path,
        }
    }

    async fn call(&self, op: i32, body: Vec<u8>, path: &str) -> Result<Vec<u8>> {
        // Sent once connected, as the client queues it meanwhile, for the
        // connection timeout at most.
        let mut s = self.status.clone();
        match tokio::time::timeout(
            self.connection_timeout,
            s.wait_for(|s| *s != Status::Connecting),
        )
        .await
        {
            Ok(Ok(s)) if *s == Status::Connected => {}
            Ok(Ok(_)) | Ok(Err(_)) => return Err(Error::Closed),
            Err(_) => return Err(Error::ConnectionLoss),
        }
        let (reply, rx) = oneshot::channel();
        self.calls
            .send(Call { op, body, reply })
            .map_err(|_| Error::Closed)?;
        match rx.await {
            Ok(Ok(r)) => Ok(r),
            Ok(Err(Error::Api { code, .. })) => Err(Error::Api {
                code,
                path: path.to_owned(),
            }),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::ConnectionLoss),
        }
    }

    /// The path created, a sequence number appended for a sequential mode.
    pub async fn create(&self, path: &str, data: &[u8], mode: CreateMode) -> Result<String> {
        let mut o = Out::default();
        o.string(&self.path(path))
            .buffer(Some(data))
            .open_acl()
            .int(mode.flag());
        let opcode = if mode == CreateMode::Container {
            op::CREATE_CONTAINER
        } else {
            op::CREATE
        };
        let r = self.call(opcode, o.0, path).await?;
        let created = In::new(&r).string().map_err(|_| Error::ConnectionLoss)?;
        Ok(self.unchroot(created))
    }

    /// `version` -1 deletes any version.
    pub async fn delete(&self, path: &str, version: i32) -> Result<()> {
        let mut o = Out::default();
        o.string(&self.path(path)).int(version);
        self.call(op::DELETE, o.0, path).await.map(|_| ())
    }

    pub async fn exists(&self, path: &str) -> Result<Option<Stat>> {
        let mut o = Out::default();
        o.string(&self.path(path)).bool(false);
        match self.call(op::EXISTS, o.0, path).await {
            Ok(r) => Ok(Some(In::new(&r).stat().map_err(|_| Error::ConnectionLoss)?)),
            Err(Error::Api {
                code: code::NO_NODE,
                ..
            }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn get_data(&self, path: &str) -> Result<(Vec<u8>, Stat)> {
        let mut o = Out::default();
        o.string(&self.path(path)).bool(false);
        let r = self.call(op::GET_DATA, o.0, path).await?;
        let mut i = In::new(&r);
        let mut parse =
            || -> io::Result<(Vec<u8>, Stat)> { Ok((i.buffer()?.unwrap_or_default(), i.stat()?)) };
        parse().map_err(|_| Error::ConnectionLoss)
    }

    pub async fn set_data(&self, path: &str, data: &[u8], version: i32) -> Result<Stat> {
        let mut o = Out::default();
        o.string(&self.path(path)).buffer(Some(data)).int(version);
        let r = self.call(op::SET_DATA, o.0, path).await?;
        In::new(&r).stat().map_err(|_| Error::ConnectionLoss)
    }

    pub async fn get_children(&self, path: &str) -> Result<Vec<String>> {
        let mut o = Out::default();
        o.string(&self.path(path)).bool(false);
        let r = self.call(op::GET_CHILDREN, o.0, path).await?;
        In::new(&r).strings().map_err(|_| Error::ConnectionLoss)
    }

    /// `ZooKeeper.close`: the session ends on the server, its ephemeral
    /// nodes with it.
    pub async fn close(&self) {
        if *self.status.borrow() == Status::Closed {
            return;
        }
        let (reply, rx) = oneshot::channel();
        if self
            .calls
            .send(Call {
                op: op::CLOSE_SESSION,
                body: Vec::new(),
                reply,
            })
            .is_ok()
        {
            let _ = tokio::time::timeout(Duration::from_secs(5), rx).await;
        }
        let mut s = self.status.clone();
        let _ = tokio::time::timeout(Duration::from_secs(5), s.wait_for(|s| *s == Status::Closed))
            .await;
    }
}

/// Connects and opens or resumes the session.
async fn handshake(
    host: String,
    port: u16,
    request: Vec<u8>,
) -> io::Result<(TcpStream, proto::ConnectResponse)> {
    let mut s = TcpStream::connect((host.as_str(), port)).await?;
    s.set_nodelay(true)?;
    s.write_all(&request).await?;
    let n = s.read_i32().await?;
    if !(0..=1 << 20).contains(&n) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad packet length",
        ));
    }
    let mut buf = vec![0; n as usize];
    s.read_exact(&mut buf).await?;
    Ok((s, proto::connect_response(&buf)?))
}

/// The task that owns the connection.
struct Session {
    servers: Vec<(String, u16)>,
    next: usize,
    config: Config,
    calls: mpsc::UnboundedReceiver<Call>,
    status: watch::Sender<Status>,
    events: broadcast::Sender<ConnectionState>,
    session_id: i64,
    passwd: Vec<u8>,
    last_zxid: i64,
    negotiated: Duration,
    ever_connected: bool,
    /// `LOST` reported for the session; a new one is made.
    lost: bool,
    /// `close` was called; answered once the session is closed.
    closing: Option<oneshot::Sender<Result<Vec<u8>>>>,
}

enum Ended {
    Disconnected,
    Closed,
}

impl Session {
    fn emit(&self, s: ConnectionState) {
        let _ = self.events.send(s);
    }

    fn lose_session(&mut self) {
        if self.ever_connected && !self.lost {
            self.lost = true;
            self.emit(ConnectionState::Lost);
        }
        self.session_id = 0;
        self.passwd = vec![0; 16];
    }

    async fn run(mut self) {
        let mut suspended_at: Option<Instant> = None;
        let mut failures = 0usize;
        loop {
            if self.closing.is_some() {
                break;
            }
            // Curator's simulated expiration: suspended too long, the
            // session is taken as lost.
            if let Some(at) = suspended_at {
                let limit = self.negotiated * self.config.session_percent.clamp(1, 100) / 100;
                if at.elapsed() >= limit && self.session_id != 0 {
                    self.lose_session();
                }
            }
            let (host, port) = self.servers[self.next % self.servers.len()].clone();
            self.next += 1;
            let per_host =
                (self.negotiated / self.servers.len() as u32).max(Duration::from_millis(500));
            let connect = tokio::time::timeout(
                per_host,
                handshake(
                    host.clone(),
                    port,
                    proto::connect_request(
                        self.last_zxid,
                        self.config.session_timeout.as_millis() as i32,
                        self.session_id,
                        &self.passwd,
                    ),
                ),
            );
            let result = tokio::select! {
                r = connect => r.unwrap_or_else(|_| Err(io::Error::from(io::ErrorKind::TimedOut))),
                c = self.calls.recv() => {
                    match c {
                        None => break,
                        Some(c) if c.op == op::CLOSE_SESSION => {
                            let _ = c.reply.send(Ok(Vec::new()));
                            break;
                        }
                        Some(c) => {
                            let _ = c.reply.send(Err(Error::ConnectionLoss));
                        }
                    }
                    continue;
                }
            };
            let (stream, resp) = match result {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!("ZooKeeper {host}:{port}: {e}");
                    failures += 1;
                    if failures.is_multiple_of(self.servers.len()) {
                        // `StaticHostProvider`'s spin delay once all were tried.
                        self.idle(Duration::from_millis(1000)).await;
                    }
                    continue;
                }
            };
            failures = 0;
            if resp.timeout_ms <= 0 {
                // The server expired the session.
                tracing::debug!("ZooKeeper session 0x{:x} expired", self.session_id);
                self.lose_session();
                continue;
            }
            let was_connected = self.ever_connected;
            self.negotiated = Duration::from_millis(resp.timeout_ms as u64);
            self.session_id = resp.session_id;
            self.passwd = resp.passwd;
            self.ever_connected = true;
            self.status.send_replace(Status::Connected);
            self.emit(if was_connected {
                ConnectionState::Reconnected
            } else {
                ConnectionState::Connected
            });
            if !was_connected || self.lost {
                self.lost = false;
            }
            let ended = self.connected(stream).await;
            if matches!(ended, Ended::Closed) {
                break;
            }
            self.status.send_replace(Status::Connecting);
            self.emit(ConnectionState::Suspended);
            suspended_at = Some(Instant::now());
        }
        self.status.send_replace(Status::Closed);
        if let Some(c) = self.closing.take() {
            let _ = c.send(Ok(Vec::new()));
        }
        // What was still asked fails.
        self.calls.close();
        while let Ok(c) = self.calls.try_recv() {
            let _ = c.reply.send(Err(Error::Closed));
        }
    }

    /// Waits, failing what is asked meanwhile.
    async fn idle(&mut self, d: Duration) {
        let until = Instant::now() + d;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(until) => return,
                c = self.calls.recv() => match c {
                    None => return,
                    Some(c) if c.op == op::CLOSE_SESSION => {
                        self.closing = Some(c.reply);
                        return;
                    }
                    Some(c) => {
                        let _ = c.reply.send(Err(Error::ConnectionLoss));
                    }
                },
            }
        }
    }

    /// `SendThread` while connected: requests out in order, replies matched
    /// to them, pings while idle, and the read timeout.
    async fn connected(&mut self, stream: TcpStream) -> Ended {
        let (mut rd, mut wr) = stream.into_split();
        let read_timeout = self.negotiated * 2 / 3;
        let ping_every = (read_timeout / 2)
            .saturating_sub(Duration::from_secs(1))
            .max(read_timeout / 3);
        let mut pending: Pending = VecDeque::new();
        let mut next_xid = 1i32;
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = vec![0u8; 64 * 1024];
        let mut last_send = Instant::now();
        let mut last_recv = Instant::now();
        let fail_all = |pending: &mut Pending| {
            for (_, _, r) in pending.drain(..) {
                let _ = r.send(Err(Error::ConnectionLoss));
            }
        };
        loop {
            tokio::select! {
                n = rd.read(&mut chunk) => {
                    let n = match n {
                        Ok(0) | Err(_) => {
                            fail_all(&mut pending);
                            return if self.closing.is_some() { Ended::Closed } else { Ended::Disconnected };
                        }
                        Ok(n) => n,
                    };
                    last_recv = Instant::now();
                    buf.extend_from_slice(&chunk[..n]);
                    while buf.len() >= 4 {
                        let len = i32::from_be_bytes(buf[..4].try_into().unwrap());
                        if len < 0 {
                            fail_all(&mut pending);
                            return Ended::Disconnected;
                        }
                        let len = len as usize;
                        if buf.len() < 4 + len {
                            break;
                        }
                        let frame: Vec<u8> = buf.drain(..4 + len).skip(4).collect();
                        let mut i = In::new(&frame);
                        let Ok(h) = proto::reply_header(&mut i) else {
                            fail_all(&mut pending);
                            return Ended::Disconnected;
                        };
                        if h.xid == xid::PING || h.xid == xid::NOTIFICATION {
                            continue;
                        }
                        if h.zxid > 0 {
                            self.last_zxid = h.zxid;
                        }
                        let Some((x, opcode, reply)) = pending.pop_front() else {
                            fail_all(&mut pending);
                            return Ended::Disconnected;
                        };
                        if x != h.xid {
                            let _ = reply.send(Err(Error::ConnectionLoss));
                            fail_all(&mut pending);
                            return Ended::Disconnected;
                        }
                        let body = frame[frame.len() - i.remaining()..].to_vec();
                        let _ = reply.send(if h.err == code::OK {
                            Ok(body)
                        } else {
                            Err(Error::Api { code: h.err, path: String::new() })
                        });
                        if opcode == op::CLOSE_SESSION {
                            fail_all(&mut pending);
                            return Ended::Closed;
                        }
                    }
                }
                c = self.calls.recv(), if self.closing.is_none() => {
                    let Some(c) = c else {
                        // Every handle dropped: the session is closed.
                        let _ = wr.write_all(&proto::request(next_xid, op::CLOSE_SESSION, &[])).await;
                        fail_all(&mut pending);
                        return Ended::Closed;
                    };
                    let x = next_xid;
                    next_xid = next_xid.wrapping_add(1).max(1);
                    if wr.write_all(&proto::request(x, c.op, &c.body)).await.is_err() {
                        let _ = c.reply.send(Err(Error::ConnectionLoss));
                        fail_all(&mut pending);
                        return Ended::Disconnected;
                    }
                    last_send = Instant::now();
                    pending.push_back((x, c.op, c.reply));
                }
                _ = tokio::time::sleep_until(last_send + ping_every) => {
                    if wr.write_all(&proto::request(xid::PING, op::PING, &[])).await.is_err() {
                        fail_all(&mut pending);
                        return Ended::Disconnected;
                    }
                    last_send = Instant::now();
                }
                _ = tokio::time::sleep_until(last_recv + read_timeout) => {
                    tracing::debug!(
                        "Client session timed out, have not heard from server in {}ms",
                        last_recv.elapsed().as_millis()
                    );
                    fail_all(&mut pending);
                    return Ended::Disconnected;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_strings() {
        let (s, c) = parse_connect_string("127.0.0.1:6666,localhost,[::1]:2182/artemis").unwrap();
        assert_eq!(
            s,
            vec![
                ("127.0.0.1".to_owned(), 6666),
                ("localhost".to_owned(), 2181),
                ("::1".to_owned(), 2182)
            ]
        );
        assert_eq!(c, "/artemis");
        assert!(parse_connect_string("").is_err());
    }
}
