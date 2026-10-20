//! Qpid JMS's connection factory and connection over AMQP
//! (`JmsConnectionFactory`, `JmsConnection`, `AmqpProvider`,
//! `AmqpConnectionBuilder`, `AmqpClosedConnectionBuilder`): the URI and its
//! options, the IDs, the transport and SASL, the lazy open, and close.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_protocol_amqp::composite::Fields;
use saranga_protocol_amqp::frame::{self, FrameBody};
use saranga_protocol_amqp::sasl::{SaslFrame, SaslInit, SaslResponse};
use saranga_protocol_amqp::transport::{Begin, Close, Open, Performative};
use saranga_protocol_amqp::types::{Symbol, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex as AsyncMutex;

use super::engine::{self, Failure, Io, Opened};
use super::jms::{AmqpJmsSession, JmsError, Result, Subscriptions, condition_error, error_message};
use super::sasl;
use crate::client::jms::Destination;

/// `JmsConnectionInfo`'s and `AmqpProvider`'s settings, from the URI.
#[derive(Debug, Clone)]
struct Options {
    client_id: Option<String>,
    client_id_prefix: String,
    connection_id_prefix: String,
    await_client_id: bool,
    queue_prefix: Option<String>,
    topic_prefix: Option<String>,
    close_timeout: Duration,
    connect_timeout: Duration,
    pub prefetch: Prefetch,
    idle_timeout: u32,
    max_frame_size: u32,
    channel_max: u16,
    vhost: Option<String>,
    sasl_layer: bool,
    sasl_mechanisms: Option<Vec<String>>,
    transport: HashMap<String, String>,
}

/// `JmsDefaultPrefetchPolicy`.
#[derive(Debug, Clone, Copy)]
pub struct Prefetch {
    pub queue: u32,
    pub topic: u32,
    pub durable_topic: u32,
    pub browser: u32,
}

/// The `jms.*` options `JmsConnectionFactory` has setters for.
const JMS_OPTIONS: [&str; 34] = [
    "username",
    "password",
    "clientID",
    "clientIDPrefix",
    "connectionIDPrefix",
    "forceAsyncSend",
    "forceSyncSend",
    "forceAsyncAcks",
    "localMessagePriority",
    "localMessageExpiry",
    "receiveLocalOnly",
    "receiveNoWaitLocalOnly",
    "populateJMSXUserID",
    "closeLinksThatFailOnReconnect",
    "queuePrefix",
    "topicPrefix",
    "validatePropertyNames",
    "validateSelector",
    "awaitClientID",
    "useDaemonThread",
    "sendTimeout",
    "requestTimeout",
    "closeTimeout",
    "connectTimeout",
    "tracing",
    "remoteURI",
    "prefetchPolicy.queuePrefetch",
    "prefetchPolicy.topicPrefetch",
    "prefetchPolicy.durableTopicPrefetch",
    "prefetchPolicy.queueBrowserPrefetch",
    "prefetchPolicy.maxPrefetchSize",
    "prefetchPolicy.all",
    "redeliveryPolicy.maxRedeliveries",
    "messageIDPolicy.messageIDType",
];

/// The `amqp.*` options `AmqpProvider` has setters for.
const AMQP_OPTIONS: [&str; 17] = [
    "idleTimeout",
    "maxFrameSize",
    "channelMax",
    "vhost",
    "saslLayer",
    "saslMechanisms",
    "drainTimeout",
    "sessionOutgoingWindow",
    "maxWriteBytesBeforeFlush",
    "anonymousFallbackCacheSize",
    "anonymousFallbackCacheTimeout",
    "maxTransfersPerDelivery",
    "maxDecodeDepth",
    "zeroWidthArrayElementLimit",
    "allowNonSecureRedirects",
    "traceFrames",
    "traceBytes",
];

/// The `transport.*` options `TransportOptions` has setters for.
const TRANSPORT_OPTIONS: [&str; 35] = [
    "sendBufferSize",
    "receiveBufferSize",
    "trafficClass",
    "connectTimeout",
    "soTimeout",
    "soLinger",
    "tcpKeepAlive",
    "tcpNoDelay",
    "defaultTcpPort",
    "defaultSslPort",
    "localAddress",
    "localPort",
    "useEpoll",
    "useKQueue",
    "traceBytes",
    "sharedEventLoopThreads",
    "useOpenSSL",
    "keyStoreLocation",
    "keyStorePassword",
    "keyStoreType",
    "trustStoreLocation",
    "trustStorePassword",
    "trustStoreType",
    "storeType",
    "enabledCipherSuites",
    "disabledCipherSuites",
    "enabledProtocols",
    "disabledProtocols",
    "contextProtocol",
    "trustAll",
    "verifyHost",
    "keyAlias",
    "ws.httpHeader",
    "keyStoreAlias",
    "trustStoreAlias",
];

/// An exception thrown as the unchecked `IllegalArgumentException`.
fn illegal_argument(message: String) -> JmsError {
    JmsError {
        class: "java.lang.IllegalArgumentException",
        message,
    }
}

/// `PropertyUtil.parseQuery`: `&`-separated, URL-decoded, the last of a
/// key kept in the place of the first.
fn parse_query(query: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for part in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        let (k, v) = (url_decode(k), url_decode(v));
        match out.iter_mut().find(|(n, _)| *n == k) {
            Some(e) => e.1 = v,
            None => out.push((k, v)),
        }
    }
    out
}

fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                match u8::from_str_radix(
                    std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("zz"),
                    16,
                ) {
                    Ok(b) => {
                        out.push(b);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `URLEncoder.encode`.
fn url_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'-' | b'*' | b'_' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// `Map.toString` of the unused options.
fn unused_text(unused: &[(String, String)]) -> String {
    let entries: Vec<String> = unused.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{{{}}}", entries.join(", "))
}

/// A `JmsConnectionFactory(user, password, uri)`, with its ID generators.
pub struct Factory {
    user: Option<String>,
    password: Option<String>,
    /// The URI without its `jms.*` options (`configuredURI`).
    configured_uri: String,
    secure: bool,
    host: String,
    port: u16,
    options: Options,
    /// Leftover `amqp.*` and `transport.*` options, checked when a
    /// connection is made.
    unused_amqp: Vec<(String, String)>,
    unused_transport: Vec<(String, String)>,
    connection_ids: AtomicU64,
    client_ids: AtomicU64,
}

impl Factory {
    /// `new JmsConnectionFactory(user, password, uri)` and
    /// `setClientID`: the URI parsed and its `jms.*` options applied.
    pub fn new(
        user: Option<String>,
        password: Option<String>,
        uri: &str,
        client_id: Option<String>,
    ) -> Result<Self> {
        if uri.is_empty() {
            return Err(illegal_argument(
                "Invalid URI: cannot be null or empty".into(),
            ));
        }
        let (base, query) = uri.split_once('?').unwrap_or((uri, ""));
        let (scheme, rest) = base
            .split_once("://")
            .ok_or_else(|| illegal_argument(format!("Invalid remote URI: {uri}")))?;
        if rest.contains('@') {
            return Err(illegal_argument(
                "The supplied URI cannot contain a User-Info section".into(),
            ));
        }
        let mut user = user;
        let mut password = password;
        let mut options = Options {
            client_id,
            client_id_prefix: "ID:".into(),
            connection_id_prefix: "ID:".into(),
            await_client_id: true,
            queue_prefix: None,
            topic_prefix: None,
            close_timeout: Duration::from_millis(60_000),
            connect_timeout: Duration::from_millis(15_000),
            prefetch: Prefetch {
                queue: 1000,
                topic: 1000,
                durable_topic: 1000,
                browser: 1000,
            },
            idle_timeout: 60_000,
            max_frame_size: 1024 * 1024,
            channel_max: 32767,
            vhost: None,
            sasl_layer: true,
            sasl_mechanisms: None,
            transport: HashMap::new(),
        };
        let mut unused = Vec::new();
        let mut kept = Vec::new();
        let mut unused_amqp = Vec::new();
        let mut unused_transport = Vec::new();
        for (k, v) in parse_query(query) {
            if let Some(name) = k.strip_prefix("jms.") {
                if !JMS_OPTIONS.contains(&name)
                    && !name.starts_with("presettlePolicy.")
                    && !name.starts_with("deserializationPolicy.")
                    && !name.starts_with("redeliveryPolicy.")
                {
                    unused.push((name.to_owned(), v));
                    continue;
                }
                let int = |v: &str| v.trim().parse::<i64>().ok();
                let flag = v.eq_ignore_ascii_case("true");
                match name {
                    "username" => user = Some(v),
                    "password" => password = Some(v),
                    "clientID" => options.client_id = Some(v),
                    "clientIDPrefix" => options.client_id_prefix = v,
                    "connectionIDPrefix" => options.connection_id_prefix = v,
                    "awaitClientID" => options.await_client_id = flag,
                    "queuePrefix" => options.queue_prefix = Some(v),
                    "topicPrefix" => options.topic_prefix = Some(v),
                    "closeTimeout" | "connectTimeout" => match int(&v) {
                        Some(ms) => {
                            let d = Duration::from_millis(ms.max(0) as u64);
                            if name == "closeTimeout" {
                                options.close_timeout = d;
                            } else {
                                options.connect_timeout = d;
                            }
                        }
                        None => unused.push((name.to_owned(), v)),
                    },
                    n if n.starts_with("prefetchPolicy.") => match int(&v) {
                        Some(p) => {
                            let p = p.clamp(0, 32767) as u32;
                            let pf = &mut options.prefetch;
                            match &n["prefetchPolicy.".len()..] {
                                "queuePrefetch" => pf.queue = p,
                                "topicPrefetch" => pf.topic = p,
                                "durableTopicPrefetch" => pf.durable_topic = p,
                                "queueBrowserPrefetch" => pf.browser = p,
                                "all" => {
                                    *pf = Prefetch {
                                        queue: p,
                                        topic: p,
                                        durable_topic: p,
                                        browser: p,
                                    }
                                }
                                _ => {}
                            }
                        }
                        None => unused.push((name.to_owned(), v)),
                    },
                    _ => {}
                }
            } else {
                if let Some(name) = k.strip_prefix("amqp.") {
                    if !AMQP_OPTIONS.contains(&name) {
                        unused_amqp.push((name.to_owned(), v.clone()));
                    } else {
                        let int = v.trim().parse::<i64>().ok();
                        match (name, int) {
                            ("idleTimeout", Some(n)) => options.idle_timeout = n.max(0) as u32,
                            ("maxFrameSize", Some(n)) if n > 0 => options.max_frame_size = n as u32,
                            ("channelMax", Some(n)) => options.channel_max = n as u16,
                            ("vhost", _) => options.vhost = Some(v.clone()),
                            ("saslLayer", _) => options.sasl_layer = v.eq_ignore_ascii_case("true"),
                            ("saslMechanisms", _) => {
                                options.sasl_mechanisms =
                                    Some(v.split(',').map(|s| s.trim().to_owned()).collect())
                            }
                            ("idleTimeout" | "maxFrameSize" | "channelMax", None) => {
                                unused_amqp.push((name.to_owned(), v.clone()))
                            }
                            _ => {}
                        }
                    }
                } else if let Some(name) = k.strip_prefix("transport.") {
                    if TRANSPORT_OPTIONS.contains(&name) || name.starts_with("ws.httpHeader.") {
                        options.transport.insert(name.to_owned(), v.clone());
                    } else {
                        unused_transport.push((name.to_owned(), v.clone()));
                    }
                }
                kept.push((k, v));
            }
        }
        if !unused.is_empty() {
            return Err(illegal_argument(format!(
                " Not all jms options could be set on the ConnectionFactory. Check the options are spelled correctly. Unused parameters=[{}]. This connection factory cannot be started.",
                unused_text(&unused)
            )));
        }
        let query: Vec<String> = kept
            .iter()
            .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
            .collect();
        let configured_uri = if query.is_empty() {
            base.to_owned()
        } else {
            format!("{base}?{}", query.join("&"))
        };
        let secure = match scheme {
            "amqp" | "amqpws" => false,
            "amqps" | "amqpwss" => true,
            other => {
                return Err(JmsError::jms(format!(
                    "Provider scheme NOT recognized: [{other}]"
                )));
            }
        };
        let authority = rest.split(['/', '#']).next().unwrap_or(rest);
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) if !h.is_empty() && !p.contains(']') => (
                h.trim_start_matches('[').trim_end_matches(']').to_owned(),
                p.parse::<u16>().ok(),
            ),
            _ => (
                authority
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .to_owned(),
                None,
            ),
        };
        let port = port.unwrap_or_else(|| {
            let key = if secure {
                "defaultSslPort"
            } else {
                "defaultTcpPort"
            };
            options
                .transport
                .get(key)
                .and_then(|p| p.parse().ok())
                .unwrap_or(if secure { 5671 } else { 5672 })
        });
        Ok(Self {
            user,
            password,
            configured_uri,
            secure,
            host,
            port,
            options,
            unused_amqp,
            unused_transport,
            connection_ids: AtomicU64::new(1),
            client_ids: AtomicU64::new(1),
        })
    }

    /// `IdGenerator.generateId`.
    fn generate(prefix: &str, seq: &AtomicU64) -> String {
        format!(
            "{prefix}{}:{}",
            sasl::random_uuid(),
            seq.fetch_add(1, Ordering::SeqCst)
        )
    }

    /// `createConnection()`: the transport, SASL and the AMQP header; the
    /// open waits for the client ID unless it is explicit.
    pub async fn create_connection(&self) -> Result<Arc<AmqpJmsConnection>> {
        self.create_connection_as(None).await
    }

    /// `createConnection()` then `setClientID(id)`: the generated client
    /// ID is drawn all the same, then replaced, and the connection opened.
    pub async fn create_connection_as(
        &self,
        set_client_id: Option<String>,
    ) -> Result<Arc<AmqpJmsConnection>> {
        if !self.unused_transport.is_empty() {
            return Err(self.provider_failure(format!(
                " Not all transport options could be set on the {} Transport. Check the options are spelled correctly. Unused parameters=[{}]. This provider instance cannot be started.",
                if self.secure { "SSL" } else { "TCP" },
                unused_text(&self.unused_transport)
            )));
        }
        if !self.unused_amqp.is_empty() {
            return Err(self.provider_failure(format!(
                " Not all AMQP provider options could be set on the AMQP Provider. Check the options are spelled correctly. Unused parameters=[{}]. This provider instance cannot be started.",
                unused_text(&self.unused_amqp)
            )));
        }
        let id = Self::generate(&self.options.connection_id_prefix, &self.connection_ids);
        let mut explicit = self.options.client_id.is_some();
        let mut client_id = match &self.options.client_id {
            Some(c) => c.clone(),
            None => Self::generate(&self.options.client_id_prefix, &self.client_ids),
        };
        if let Some(id) = set_client_id {
            client_id = id;
            explicit = true;
        }
        let failed = |e: JmsError| {
            tracing::error!(
                target: "org.apache.qpid.jms.JmsConnection",
                "Failed to connect to remote at: {}://{}:{}",
                if self.secure { "amqps" } else { "amqp" },
                self.host,
                self.port
            );
            e
        };
        let (io, leftover) =
            tokio::time::timeout(self.options.connect_timeout, self.connect_transport())
                .await
                .map_err(|_| JmsError {
                    class: "org.apache.qpid.jms.JmsOperationTimedOutException",
                    message: "Timed out while waiting to connect".into(),
                })
                .and_then(|r| r)
                .map_err(failed)?;
        let conn = Arc::new(AmqpJmsConnection {
            id,
            client_id,
            explicit_client_id: explicit,
            configured_uri: self.configured_uri.clone(),
            host: self.host.clone(),
            options: self.options.clone(),
            state: AsyncMutex::new(State::Connected { io, leftover }),
            remote: Mutex::new(None),
            sessions: AtomicU64::new(0),
            subscriptions: Subscriptions::default(),
        });
        if explicit || !self.options.await_client_id {
            conn.ensure_open().await?;
        }
        Ok(conn)
    }

    /// What a provider that cannot be made logs and throws.
    fn provider_failure(&self, message: String) -> JmsError {
        let scheme = if self.secure { "amqps" } else { "amqp" };
        let e = format!("java.lang.IllegalArgumentException: {message}");
        tracing::error!(target: "org.apache.qpid.jms.transports.TransportFactory", "Failed to create Transport instance for {scheme}, due to: {e}");
        tracing::error!(target: "org.apache.qpid.jms.provider.ProviderFactory", "Failed to create Provider instance for {scheme}, due to: {e}");
        tracing::error!(target: "org.apache.qpid.jms.JmsConnectionFactory", "Failed to create JMS Provider instance for: {scheme}");
        JmsError::jms(message)
    }

    /// TCP (or TLS), then SASL and the AMQP header.
    async fn connect_transport(&self) -> Result<(Box<dyn Io>, Vec<u8>)> {
        let address = (self.host.as_str(), self.port);
        let tcp = tokio::net::TcpStream::connect(address).await.map_err(|e| {
            JmsError::jms(if e.kind() == std::io::ErrorKind::ConnectionRefused {
                if cfg!(windows) {
                    format!(
                        "Connection refused: no further information: {}/{}:{}",
                        self.host,
                        resolve_text(&self.host),
                        self.port
                    )
                } else {
                    format!(
                        "Connection refused: {}/{}:{}",
                        self.host,
                        resolve_text(&self.host),
                        self.port
                    )
                }
            } else {
                e.to_string()
            })
        })?;
        let _ = tcp.set_nodelay(
            self.options
                .transport
                .get("tcpNoDelay")
                .is_none_or(|v| v.eq_ignore_ascii_case("true")),
        );
        let mut io: Box<dyn Io> = if self.secure {
            let tls = saranga_transport::TlsConnector::new(self.tls_settings());
            let (stream, principal) = tls
                .connect_reporting(&self.host, tcp)
                .await
                .map_err(|e| JmsError::jms(e.to_string()))?;
            let _ = principal;
            Box::new(stream)
        } else {
            Box::new(tcp)
        };
        let mut buf = Vec::new();
        if self.options.sasl_layer {
            self.sasl(&mut io, &mut buf).await?;
        }
        io.write_all(&frame::AMQP_HEADER)
            .await
            .map_err(|e| JmsError::jms(e.to_string()))?;
        Ok((io, buf))
    }

    /// The `transport.*` TLS options over the JVM's defaults.
    fn tls_settings(&self) -> saranga_transport::ConnectorTlsSettings {
        let t = &self.options.transport;
        let mut s = saranga_transport::ConnectorTlsSettings::jvm_default();
        if let Some(v) = t.get("keyStoreLocation") {
            s.key_store_path = Some(v.clone());
        }
        if let Some(v) = t.get("keyStorePassword") {
            s.key_store_password = Some(v.clone());
        }
        if let Some(v) = t.get("trustStoreLocation") {
            s.trust_store_path = Some(v.clone());
        }
        if let Some(v) = t.get("trustStorePassword") {
            s.trust_store_password = Some(v.clone());
        }
        if let Some(v) = t.get("storeType") {
            s.key_store_type = v.clone();
            s.trust_store_type = v.clone();
        }
        if let Some(v) = t.get("keyStoreType") {
            s.key_store_type = v.clone();
        }
        if let Some(v) = t.get("trustStoreType") {
            s.trust_store_type = v.clone();
        }
        if let Some(v) = t.get("keyAlias") {
            s.key_store_alias = Some(v.clone());
        }
        if let Some(v) = t.get("trustAll") {
            s.trust_all = v.eq_ignore_ascii_case("true");
        }
        if let Some(v) = t.get("verifyHost") {
            s.verify_host = v.eq_ignore_ascii_case("true");
        }
        let list = |v: &String| {
            v.split(',')
                .map(|x| x.trim().to_owned())
                .filter(|x| !x.is_empty())
                .collect()
        };
        if let Some(v) = t.get("enabledProtocols") {
            s.enabled_protocols = Some(list(v));
        }
        if let Some(v) = t.get("enabledCipherSuites") {
            s.enabled_cipher_suites = Some(list(v));
        }
        s
    }

    /// The SASL layer (`AmqpSaslAuthenticator`).
    async fn sasl(&self, io: &mut Box<dyn Io>, buf: &mut Vec<u8>) -> Result<()> {
        let io_error = |e: std::io::Error| JmsError::jms(e.to_string());
        io.write_all(&frame::SASL_HEADER).await.map_err(io_error)?;
        let mut header = [0u8; 8];
        read_exact(io, buf, &mut header).await?;
        if header != frame::SASL_HEADER {
            return Err(JmsError::jms("Transport connection remotely closed."));
        }
        let hostname = match &self.options.vhost {
            Some(v) if v.is_empty() => None,
            Some(v) => Some(v.clone()),
            None => Some(self.host.clone()),
        };
        let fail = |m: String| JmsError {
            class: "org.apache.qpid.jms.exceptions.JMSSecuritySaslException",
            message: m,
        };
        let mut exchange: Option<sasl::Exchange> = None;
        loop {
            let f = read_frame(io, buf).await?;
            let FrameBody::Sasl(f) = f.body else {
                continue;
            };
            match f {
                SaslFrame::Mechanisms(m) => {
                    let offered: Vec<String> = m
                        .sasl_server_mechanisms
                        .iter()
                        .map(|s| s.as_str().to_owned())
                        .collect();
                    if offered.is_empty() {
                        continue;
                    }
                    let mechanism = sasl::choose(
                        &offered,
                        self.user.as_deref(),
                        self.password.as_deref(),
                        false,
                        self.options.sasl_mechanisms.as_deref(),
                    )
                    .map_err(fail)?;
                    let mut e = sasl::Exchange::new(
                        mechanism,
                        self.user.as_deref(),
                        self.password.as_deref(),
                    );
                    let initial = e
                        .initial_response(self.user.as_deref(), self.password.as_deref())
                        .map_err(|m| fail(format!("Exception while processing SASL init: {m}")))?;
                    let mut out = Vec::new();
                    frame::encode_sasl(
                        &mut out,
                        &SaslFrame::Init(SaslInit {
                            mechanism: Symbol::from(mechanism.name()),
                            initial_response: initial,
                            hostname: hostname.clone(),
                        }),
                    );
                    io.write_all(&out).await.map_err(io_error)?;
                    exchange = Some(e);
                }
                SaslFrame::Challenge(c) => {
                    let Some(e) = exchange.as_mut() else { continue };
                    let response = e
                        .challenge(&c.challenge)
                        .map_err(|m| fail(format!("Exception while processing SASL step: {m}")))?;
                    if let Some(r) = response {
                        let mut out = Vec::new();
                        frame::encode_sasl(
                            &mut out,
                            &SaslFrame::Response(SaslResponse { response: r }),
                        );
                        io.write_all(&out).await.map_err(io_error)?;
                    }
                }
                SaslFrame::Outcome(o) => {
                    let mechanism = exchange.as_ref().map(|e| e.mechanism.name());
                    if o.code != saranga_protocol_amqp::sasl::code::OK {
                        let mut m = "Client failed to authenticate".to_owned();
                        if let Some(n) = mechanism {
                            m.push_str(&format!(" using SASL: {n}"));
                        }
                        if o.code == saranga_protocol_amqp::sasl::code::SYS_TEMP {
                            m.push_str(", due to temporary system error.");
                        }
                        return Err(fail(m));
                    }
                    if let Some(e) = exchange.as_mut() {
                        if let Some(data) = &o.additional_data {
                            e.challenge(data).map_err(|m| {
                                fail(format!(
                                    "Exception while processing SASL exchange completion: {m}"
                                ))
                            })?;
                        }
                        e.verify_completion().map_err(|m| {
                            fail(format!(
                                "Exception while processing SASL exchange completion: {m}"
                            ))
                        })?;
                    }
                    return Ok(());
                }
                SaslFrame::Init(_) | SaslFrame::Response(_) => {}
            }
        }
    }
}

/// The address a host name resolves to, as Netty prints it.
fn resolve_text(host: &str) -> String {
    use std::net::ToSocketAddrs;
    let mut all: Vec<std::net::SocketAddr> = (host, 0)
        .to_socket_addrs()
        .map(Iterator::collect)
        .unwrap_or_default();
    all.sort_by_key(|a| !a.is_ipv4());
    all.first()
        .map_or_else(|| host.to_owned(), |a| a.ip().to_string())
}

async fn read_exact(io: &mut Box<dyn Io>, buf: &mut Vec<u8>, out: &mut [u8]) -> Result<()> {
    while buf.len() < out.len() {
        let mut chunk = [0u8; 4096];
        let n = io
            .read(&mut chunk)
            .await
            .map_err(|e| JmsError::jms(e.to_string()))?;
        if n == 0 {
            return Err(JmsError {
                class: "org.apache.qpid.jms.JmsConnectionFailedException",
                message: "The JMS connection has failed: Transport connection remotely closed."
                    .into(),
            });
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    out.copy_from_slice(&buf[..out.len()]);
    buf.drain(..out.len());
    Ok(())
}

async fn read_frame(io: &mut Box<dyn Io>, buf: &mut Vec<u8>) -> Result<frame::Frame> {
    let mut size = [0u8; 4];
    read_exact(io, buf, &mut size).await?;
    let len = u32::from_be_bytes(size) as usize;
    if len < 8 {
        return Err(JmsError::jms(format!("frame of {len} bytes")));
    }
    let mut rest = vec![0u8; len - 4];
    read_exact(io, buf, &mut rest).await?;
    let mut whole = size.to_vec();
    whole.extend_from_slice(&rest);
    frame::decode(&whole).map_err(|e| JmsError::jms(e.to_string()))
}

enum State {
    /// The transport is up, the open not sent.
    Connected {
        io: Box<dyn Io>,
        leftover: Vec<u8>,
    },
    /// Opened: the engine and the internal connection session.
    Open {
        conn: engine::Connection,
        _connection_session: engine::Session,
    },
    Closed,
}

/// A `JmsConnection` over AMQP.
pub struct AmqpJmsConnection {
    /// `ID:<uuid>:<n>`.
    pub id: String,
    /// The container ID: the client ID, explicit or generated.
    pub client_id: String,
    pub explicit_client_id: bool,
    configured_uri: String,
    host: String,
    options: Options,
    state: AsyncMutex<State>,
    remote: Mutex<Option<Open>>,
    sessions: AtomicU64,
    pub subscriptions: Subscriptions,
}

impl AmqpJmsConnection {
    /// Whether the open connection failed, as its exception listener would
    /// hear.
    pub fn failed(&self) -> bool {
        match self.state.try_lock() {
            Ok(state) => matches!(&*state, State::Open { conn, .. } if !conn.is_open()),
            Err(_) => false,
        }
    }

    /// `JmsConnectionInfo.toString`.
    fn info_text(&self) -> String {
        format!(
            "JmsConnectionInfo {{ {}, configuredURI = {}, connectedURI = null }}",
            self.id, self.configured_uri
        )
    }

    /// This end's open (`AmqpConnectionBuilder.createEndpoint`).
    fn local_open(&self) -> Open {
        let platform = format!(
            "JVM: {}, {}, {}, OS: {}, {}, {}",
            "25.0.4.1",
            "25.0.4.1+1-LTS",
            "Microsoft",
            std::env::consts::OS,
            "",
            std::env::consts::ARCH
        );
        let hostname = match &self.options.vhost {
            Some(v) if v.is_empty() => None,
            Some(v) => Some(v.clone()),
            None => Some(self.host.clone()),
        };
        Open {
            container_id: self.client_id.clone(),
            hostname,
            max_frame_size: self.options.max_frame_size,
            channel_max: self.options.channel_max,
            idle_time_out: (self.options.idle_timeout > 0).then_some(self.options.idle_timeout / 2),
            outgoing_locales: None,
            incoming_locales: None,
            offered_capabilities: None,
            desired_capabilities: Some(
                [
                    "sole-connection-for-container",
                    "DELAYED_DELIVERY",
                    "ANONYMOUS-RELAY",
                    "SHARED-SUBS",
                ]
                .into_iter()
                .map(Symbol::from)
                .collect(),
            ),
            properties: Some(Fields(vec![
                (Symbol::from("product"), Value::string("QpidJMS")),
                (Symbol::from("version"), Value::string("1.17.0")),
                (Symbol::from("platform"), Value::string(platform)),
            ])),
        }
    }

    /// Exchanges opens on the raw transport: the peer's open, or why not.
    async fn exchange_open(
        &self,
        io: &mut Box<dyn Io>,
        leftover: &mut Vec<u8>,
    ) -> std::result::Result<Open, JmsError> {
        let mut out = Vec::new();
        frame::encode_amqp(&mut out, 0, &Performative::Open(self.local_open()), b"");
        io.write_all(&out)
            .await
            .map_err(|e| JmsError::jms(e.to_string()))?;
        let mut header = [0u8; 8];
        read_exact(io, leftover, &mut header).await?;
        loop {
            let f = read_frame(io, leftover).await?;
            match f.body {
                FrameBody::Amqp(Performative::Open(o), _) => return Ok(o),
                FrameBody::Amqp(Performative::Close(c), _) => {
                    let error = c.error;
                    let message = match &error {
                        Some(e) => error_message(Some(e)),
                        None => "Open failed unexpectedly.".to_owned(),
                    };
                    tracing::warn!(
                        target: "org.apache.qpid.jms.provider.amqp.builders.AmqpResourceBuilder",
                        "Open of resource:({}) failed: {message}",
                        self.info_text()
                    );
                    return Err(match error {
                        Some(e) => connection_closed_error(&e),
                        None => JmsError {
                            class: "org.apache.qpid.jms.JmsConnectionRemotelyClosedException",
                            message,
                        },
                    });
                }
                _ => {}
            }
        }
    }

    /// `createJmsConnection`: the open and the connection session, once.
    pub(super) async fn ensure_open(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        let (mut io, mut leftover) = match std::mem::replace(&mut *state, State::Closed) {
            State::Connected { io, leftover } => (io, leftover),
            open @ State::Open { .. } => {
                *state = open;
                return Ok(());
            }
            State::Closed => return Err(JmsError::illegal_state("The Connection is closed")),
        };
        let remote = self.exchange_open(&mut io, &mut leftover).await?;
        *self.remote.lock().unwrap_or_else(|e| e.into_inner()) = Some(remote.clone());
        let opened = Opened {
            local: self.local_open(),
            remote,
        };
        let conn = engine::Connection::start(io, opened, leftover);
        let connection_session = conn
            .begin(begin())
            .await
            .map_err(|f| super::jms::failure_error(&f))?;
        *state = State::Open {
            conn,
            _connection_session: connection_session,
        };
        tracing::info!(
            target: "org.apache.qpid.jms.JmsConnection",
            "Connection {} connected to server: {}",
            self.id,
            self.configured_uri.split('?').next().unwrap_or_default()
        );
        Ok(())
    }

    pub(super) async fn begin_session(&self) -> Result<engine::Session> {
        self.ensure_open().await?;
        let state = self.state.lock().await;
        match &*state {
            State::Open { conn, .. } => conn
                .begin(begin())
                .await
                .map_err(|f| super::jms::failure_error(&f)),
            _ => Err(JmsError::illegal_state("The Connection is closed")),
        }
    }

    /// `createSession(transacted, ...)`.
    pub async fn session(self: &Arc<Self>, transacted: bool) -> Result<AmqpJmsSession> {
        let n = self.sessions.fetch_add(1, Ordering::SeqCst) + 1;
        AmqpJmsSession::begin(self.clone(), n, transacted).await
    }

    /// The consumer prefetch for a destination (`JmsDefaultPrefetchPolicy`).
    pub fn prefetch(&self, dest: &Destination, durable: bool, browser: bool) -> u32 {
        let p = self.options.prefetch;
        match (dest.is_queue(), durable, browser) {
            (true, _, true) => p.browser,
            (true, _, false) => p.queue,
            (false, true, _) => p.durable_topic,
            (false, false, _) => p.topic,
        }
    }

    /// `AmqpDestinationHelper.getDestinationAddress`.
    pub(super) fn address(&self, dest: &Destination) -> String {
        let name = dest.address();
        let remote = self.remote.lock().unwrap_or_else(|e| e.into_inner());
        let prop = |key: &str| {
            remote
                .as_ref()
                .and_then(|o| o.properties.as_ref())
                .and_then(|p| p.0.iter().find(|(k, _)| k.as_str() == key))
                .and_then(|(_, v)| v.as_str().map(str::to_owned))
        };
        let prefix = if dest.is_queue() {
            prop("queue-prefix").or_else(|| self.options.queue_prefix.clone())
        } else {
            prop("topic-prefix").or_else(|| self.options.topic_prefix.clone())
        };
        match prefix {
            Some(p) if !name.starts_with(&p) => format!("{p}{name}"),
            _ => name.to_owned(),
        }
    }

    /// Whether the peer's open offered a capability.
    pub(super) fn offers(&self, capability: &str) -> bool {
        self.remote
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .and_then(|o| o.offered_capabilities.as_ref())
            .is_some_and(|c| c.iter().any(|s| s.as_str() == capability))
    }

    /// A refused attach: logged as `AmqpResourceBuilder` logs it.
    pub(super) fn open_failed(&self, f: &Failure) -> JmsError {
        let e = match f {
            Failure::Remote(_, None) => JmsError::jms("Open failed unexpectedly."),
            Failure::Remote(_, Some(e)) => condition_error(Some(e)),
            other => super::jms::failure_error(other),
        };
        self.warn_open_failed(e)
    }

    pub(super) fn warn_open_failed(&self, e: JmsError) -> JmsError {
        tracing::warn!(
            target: "org.apache.qpid.jms.provider.amqp.builders.AmqpResourceBuilder",
            "Open of resource:(link) failed: {}",
            e.message
        );
        e
    }

    /// `unsubscribe(name)`: a receiver with no source, closed again if the
    /// peer finds the subscription.
    pub async fn unsubscribe(self: &Arc<Self>, name: &str) -> Result<()> {
        self.ensure_open().await?;
        let session = self.begin_session().await?;
        let link_name = if self.explicit_client_id {
            name.to_owned()
        } else {
            format!("{name}|global")
        };
        let attach = saranga_protocol_amqp::transport::Attach {
            name: link_name,
            role: saranga_protocol_amqp::transport::Role::Receiver,
            snd_settle_mode: saranga_protocol_amqp::transport::SenderSettleMode::Unsettled,
            rcv_settle_mode: saranga_protocol_amqp::transport::ReceiverSettleMode::First,
            source: None,
            target: Some(saranga_protocol_amqp::messaging::TargetKind::Target(
                saranga_protocol_amqp::messaging::Target::default(),
            )),
            desired_capabilities: (!self.explicit_client_id)
                .then(|| vec![Symbol::from("shared"), Symbol::from("global")]),
            ..Default::default()
        };
        let result = match session.attach(attach).await {
            Ok(link) if link.remote.source.is_some() => link
                .detach(true, None)
                .await
                .map_err(|f| super::jms::failure_error(&f)),
            Ok(mut link) => loop {
                match link.events.recv().await {
                    Some(engine::LinkEvent::Detached { error, .. }) => {
                        break Err(match error {
                            Some(e) => condition_error(Some(&e)),
                            None => JmsError::jms("Open failed unexpectedly."),
                        });
                    }
                    Some(engine::LinkEvent::Failed(f)) => break Err(super::jms::failure_error(&f)),
                    Some(_) => continue,
                    None => break Err(JmsError::jms("Open failed unexpectedly.")),
                }
            },
            Err(f) => Err(self.open_failed(&f)),
        };
        let _ = session.end(None).await;
        result
    }

    /// `close()`: only the close on the wire, or, for a connection never
    /// opened, the open and close `AmqpClosedConnectionBuilder` exchanges.
    pub async fn close(&self) {
        let mut state = self.state.lock().await;
        match std::mem::replace(&mut *state, State::Closed) {
            State::Open { conn, .. } => {
                let _ = conn.close(None, self.options.close_timeout).await;
            }
            State::Connected {
                mut io,
                mut leftover,
            } => {
                let opened = tokio::time::timeout(
                    self.options.connect_timeout,
                    self.exchange_open_then_close(&mut io, &mut leftover),
                )
                .await;
                if opened.is_err() {
                    tracing::warn!(
                        target: "org.apache.qpid.jms.provider.amqp.builders.AmqpResourceBuilder",
                        "Open of resource:({}) failed: Request to open resource AmqpConnection {{ {} }} timed out",
                        self.info_text(),
                        self.id
                    );
                }
                let _ = io.shutdown().await;
            }
            State::Closed => {}
        }
    }

    async fn exchange_open_then_close(&self, io: &mut Box<dyn Io>, leftover: &mut Vec<u8>) {
        if self.exchange_open(io, leftover).await.is_err() {
            return;
        }
        let mut out = Vec::new();
        frame::encode_amqp(&mut out, 0, &Performative::Close(Close::default()), b"");
        if io.write_all(&out).await.is_err() {
            return;
        }
        loop {
            match read_frame(io, leftover).await {
                Ok(f) => {
                    if let FrameBody::Amqp(Performative::Close(c), _) = f.body {
                        let message = match &c.error {
                            Some(e) => error_message(Some(e)),
                            None => "Open failed unexpectedly.".to_owned(),
                        };
                        tracing::warn!(
                            target: "org.apache.qpid.jms.provider.amqp.builders.AmqpResourceBuilder",
                            "Open of resource:({}) failed: {message}",
                            self.info_text()
                        );
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    }
}

/// proton-j's begin for a Qpid session.
fn begin() -> Begin {
    Begin {
        remote_channel: None,
        next_outgoing_id: 1,
        incoming_window: 2047,
        outgoing_window: 2_147_483_647,
        handle_max: 65535,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

/// `AmqpSupport.convertToConnectionClosedException`.
fn connection_closed_error(e: &saranga_protocol_amqp::transport::Error) -> JmsError {
    let message = error_message(Some(e));
    let class = match e.condition.as_str() {
        "amqp:unauthorized-access" => "javax.jms.JMSSecurityException",
        "amqp:resource-limit-exceeded" => "javax.jms.ResourceAllocationException",
        "amqp:not-found" => "org.apache.qpid.jms.JmsResourceNotFoundException",
        "amqp:invalid-field"
            if e.info.as_ref().is_some_and(|i| {
                i.0.iter().any(|(k, v)| {
                    k.as_str() == "invalid-field"
                        && v.as_symbol().is_some_and(|s| s.as_str() == "container-id")
                })
            }) =>
        {
            "javax.jms.InvalidClientIDException"
        }
        _ => "org.apache.qpid.jms.JmsConnectionRemotelyClosedException",
    };
    JmsError { class, message }
}
