//! The JMS calls the CLI's message commands make, over Core (Artemis's
//! JMS client) or AMQP (Qpid JMS), as `--protocol` chooses
//! (`ConnectionAbstract.createConnectionFactory`).

use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use saranga_client::message::{bytes_message, new_message, text_message};
use saranga_client::{CoreMessage, Delivery, MessageExt, PropertyValue, message_type};

use super::amqp::connection::{AmqpJmsConnection, Factory};
use super::amqp::jms::{AmqpConsumer, AmqpJmsSession, AmqpProducer, ConsumerKind, JmsError};
use super::amqp::message::{AmqpJmsMessage, kind};
use super::jms::{Destination, JmsConnection, JmsSession, Setter, set_core_property, text_or_id};
use super::{Common, ConnectionArgs};

/// `ConnectionProtocol`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Core,
    Amqp,
}

impl Protocol {
    /// `ConnectionProtocol.fromString`: the name in any case.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "CORE" => Some(Self::Core),
            "AMQP" => Some(Self::Amqp),
            _ => None,
        }
    }
}

/// A JMS connection.
pub enum Conn {
    Core(JmsConnection),
    Amqp(Arc<AmqpJmsConnection>),
}

/// A JMS session.
#[derive(Clone)]
pub enum Sess {
    Core(JmsSession),
    Amqp(Arc<AmqpJmsSession>, Arc<AmqpJmsConnection>),
}

/// A message producer.
pub enum Prod {
    Core(saranga_client::Producer, Destination),
    Amqp(AmqpProducer),
}

/// A message consumer (or a browser's).
pub enum Cons {
    Core(saranga_client::Consumer),
    Amqp(Box<AmqpConsumer>, Arc<AmqpJmsConnection>),
}

/// A message received, or sent.
pub enum Msg {
    Core { message: CoreMessage, large: bool },
    Amqp(Box<AmqpJmsMessage>),
}

/// The body `createTextMessage`, `createBytesMessage` or
/// `createObjectMessage` gives a message.
#[derive(Debug, Clone)]
pub enum Body {
    Text(String),
    Bytes(Vec<u8>),
    /// An `ObjectMessage` of this string.
    Object(String),
}

/// The Java serialization of a `String` (`ObjectOutputStream.writeObject`).
pub fn serialized_string(text: &str) -> Vec<u8> {
    let mut utf = Vec::new();
    for c in text.encode_utf16() {
        // Modified UTF-8, as `DataOutput.writeUTF` writes it.
        match c {
            0x0001..=0x007f => utf.push(c as u8),
            0x0000 | 0x0080..=0x07ff => {
                utf.push((0xc0 | ((c >> 6) & 0x1f)) as u8);
                utf.push((0x80 | (c & 0x3f)) as u8);
            }
            _ => {
                utf.push((0xe0 | ((c >> 12) & 0x0f)) as u8);
                utf.push((0x80 | ((c >> 6) & 0x3f)) as u8);
                utf.push((0x80 | (c & 0x3f)) as u8);
            }
        }
    }
    let mut data = vec![0xac, 0xed, 0x00, 0x05];
    if utf.len() <= 0xffff {
        data.push(0x74);
        data.extend_from_slice(&(utf.len() as u16).to_be_bytes());
    } else {
        data.push(0x7c);
        data.extend_from_slice(&(utf.len() as u64).to_be_bytes());
    }
    data.extend_from_slice(&utf);
    data
}

/// The length of a serialized `String`, in UTF-16 units.
pub fn serialized_string_len(data: &[u8]) -> Option<usize> {
    let (start, len) = match data.get(4)? {
        0x74 => (
            7,
            u16::from_be_bytes([*data.get(5)?, *data.get(6)?]) as usize,
        ),
        0x7c => (
            13,
            u64::from_be_bytes(data.get(5..13)?.try_into().ok()?) as usize,
        ),
        _ => return None,
    };
    let utf = data.get(start..start + len)?;
    // Modified UTF-8: count the UTF-16 units.
    Some(utf.iter().filter(|b| (**b & 0xc0) != 0x80).count())
}

/// `ObjectInputStream.readObject` of a serialized `String`.
pub fn deserialized_string(data: &[u8]) -> Option<String> {
    let (start, len) = match data.get(4)? {
        0x74 => (
            7,
            u16::from_be_bytes([*data.get(5)?, *data.get(6)?]) as usize,
        ),
        0x7c => (
            13,
            u64::from_be_bytes(data.get(5..13)?.try_into().ok()?) as usize,
        ),
        _ => return None,
    };
    if data.get(..4)? != [0xac, 0xed, 0x00, 0x05] || data.len() != start + len {
        return None;
    }
    // Modified UTF-8 to UTF-16.
    let utf = &data[start..];
    let mut units = Vec::with_capacity(utf.len());
    let mut i = 0;
    while i < utf.len() {
        let b = utf[i];
        let (unit, n) = match b {
            0x01..=0x7f => (u16::from(b), 1),
            0xc0..=0xdf => (
                (u16::from(b & 0x1f) << 6) | u16::from(*utf.get(i + 1)? & 0x3f),
                2,
            ),
            0xe0..=0xef => (
                (u16::from(b & 0x0f) << 12)
                    | (u16::from(*utf.get(i + 1)? & 0x3f) << 6)
                    | u16::from(*utf.get(i + 2)? & 0x3f),
                3,
            ),
            _ => return None,
        };
        units.push(unit);
        i += n;
    }
    Some(String::from_utf16_lossy(&units))
}

impl Msg {
    /// `session.createXxxMessage(body)` of the protocol's client.
    pub fn create(protocol: Protocol, body: Body) -> Self {
        match protocol {
            Protocol::Core => Self::Core {
                message: match body {
                    Body::Text(t) => text_message(&t),
                    Body::Bytes(b) => bytes_message(&b),
                    Body::Object(t) => {
                        let data = serialized_string(&t);
                        let mut m = new_message(message_type::OBJECT, true);
                        let mut body = (data.len() as i32).to_be_bytes().to_vec();
                        body.extend_from_slice(&data);
                        m.body = body.into();
                        m
                    }
                },
                large: false,
            },
            Protocol::Amqp => Self::Amqp(Box::new(match body {
                Body::Text(t) => AmqpJmsMessage::new_text(Some(&t)),
                Body::Bytes(b) => AmqpJmsMessage::bytes(&b),
                Body::Object(t) => AmqpJmsMessage::object(Some(serialized_string(&t))),
            })),
        }
    }

    /// `setBooleanProperty`, `setIntProperty` and the others, by the
    /// value's type, as the client checks and maps the name.
    pub fn set_property(&mut self, name: &str, value: &PropertyValue) -> Result<(), JmsError> {
        match self {
            Self::Core { message, .. } => set_core_property(message, name, value, Setter::Typed),
            Self::Amqp(m) => super::amqp::properties::set_property(m, name, value),
        }
    }

    /// The text of a TEXT message, else the JMS message ID, as the
    /// commands print them.
    pub fn text_or_id(&self) -> String {
        match self {
            Self::Core { message, .. } => text_or_id(message),
            Self::Amqp(m) => {
                if m.kind == kind::TEXT {
                    match m.text() {
                        Ok(t) => t.unwrap_or_else(|| "null".to_owned()),
                        Err(e) => e,
                    }
                } else {
                    m.jms_message_id().unwrap_or_else(|| "null".to_owned())
                }
            }
        }
    }

    pub fn jms_message_id(&self) -> String {
        match self {
            Self::Core { message, .. } => message.jms_message_id(),
            Self::Amqp(m) => m.jms_message_id(),
        }
        .unwrap_or_else(|| "null".to_owned())
    }

    /// `ConsumerThread.handle` with `--verbose`: the ID, then a text's or
    /// an object's size.
    pub fn print_verbose(&self) {
        println!("JMS Message ID:{}", self.jms_message_id());
        match self {
            Self::Core { message, .. } => match message.message_type {
                message_type::TEXT => {
                    let text = message.text().ok().flatten().unwrap_or_default();
                    println!("Received text sized at {}", text.encode_utf16().count());
                }
                message_type::OBJECT => {
                    if let Some(len) = message.body.get(4..).and_then(serialized_string_len) {
                        println!("Received object {len}");
                    }
                }
                _ => {}
            },
            Self::Amqp(m) => match m.kind {
                kind::TEXT => {
                    let text = m.text().ok().flatten().unwrap_or_default();
                    println!("Received text sized at {}", text.encode_utf16().count());
                }
                kind::OBJECT => {
                    if let Some(len) = m.serialized_object().and_then(serialized_string_len) {
                        println!("Received object {len}");
                    }
                }
                _ => {}
            },
        }
    }

    /// `"..." + message`: the message's `toString`, as a browser prints it.
    pub fn browse_text(&self) -> String {
        match self {
            Self::Core { message, .. } => format!(
                "ActiveMQMessage[{}]:{}",
                message
                    .jms_message_id()
                    .unwrap_or_else(|| "null".to_owned()),
                if message.durable {
                    "PERSISTENT"
                } else {
                    "NON-PERSISTENT"
                }
            ),
            Self::Amqp(m) => m.to_jms_string(),
        }
    }
}

impl Conn {
    /// `createConnection` through the command's factory: a connection
    /// made and closed (`tryConnect`), then the command's own.
    pub async fn connect(args: &ConnectionArgs) -> anyhow::Result<Self> {
        match Protocol::parse(&args.protocol) {
            Some(Protocol::Amqp) => {
                let factory = amqp_factory(&mut args.clone()).await?;
                Ok(Self::Amqp(factory.create_connection().await?))
            }
            _ => Ok(Self::Core(super::messages::connect(args).await?)),
        }
    }

    /// `createConnectionFactory()` alone, its `tryConnect` made: the user
    /// and password asked for kept, and for Core the URL the retry used.
    pub async fn try_connect(args: &mut ConnectionArgs) -> anyhow::Result<()> {
        match Protocol::parse(&args.protocol) {
            Some(Protocol::Amqp) => amqp_factory(args).await.map(drop),
            _ => args.try_connect().await,
        }
    }

    /// `Transfer.createConnectionFactory` then `createConnection`.
    pub async fn connect_for_transfer(
        common: &Common,
        role: &str,
        protocol: &str,
        url: &str,
        user: Option<String>,
        password: Option<String>,
        client_id: Option<String>,
    ) -> anyhow::Result<Self> {
        match protocol {
            "core" => {
                if common.verbose {
                    println!("Creating {role} CORE Connection towards {url}");
                }
                if let Some(id) = &client_id {
                    println!("Consumer:: clientID = {id}");
                }
                let connect = |user: Option<String>, password: Option<String>| {
                    JmsConnection::connect(url, user, password, client_id.clone())
                };
                match connect(user.clone(), password.clone()).await {
                    Ok(c) => {
                        c.close().await;
                    }
                    Err(e) => {
                        let failed = match e.downcast::<super::jms::ConnectFailed>() {
                            Ok(f) => f,
                            Err(_) => super::jms::ConnectFailed {
                                reason: "AMQ219007: Cannot connect to server(s). Tried with all available servers.".to_owned(),
                                security: false,
                            },
                        };
                        eprintln!("Connection failed::{failed}");
                        let url = if failed.security {
                            url.to_owned()
                        } else {
                            common
                                .input(
                                    "--url",
                                    "Type in the broker URL for a retry (e.g. tcp://localhost:61616)",
                                    Some(url),
                                )?
                                .unwrap_or_else(|| url.to_owned())
                        };
                        let (user, password) =
                            transfer_user_password(common, &url, user, password)?;
                        return Ok(Self::Core(
                            JmsConnection::connect(&url, user, password, client_id).await?,
                        ));
                    }
                }
                Ok(Self::Core(
                    JmsConnection::connect(url, user, password, client_id).await?,
                ))
            }
            "amqp" => {
                if common.verbose {
                    println!("Creating {role} AMQP Connection towards {url}");
                }
                let mut url = amqp_url(url);
                let factory =
                    Factory::new(user.clone(), password.clone(), &url, client_id.clone())?;
                let factory = match factory.create_connection().await {
                    Ok(c) => {
                        c.close().await;
                        factory
                    }
                    Err(e) => {
                        eprintln!("Connection failed::{}", e.message);
                        if !is_security(&e) {
                            url = common
                                .input(
                                    "--url",
                                    "Type in the broker URL for a retry (e.g. tcp://localhost:61616)",
                                    Some(&url),
                                )?
                                .unwrap_or(url);
                        }
                        let (user, password) =
                            transfer_user_password(common, &url, user, password)?;
                        Factory::new(user, password, &url, client_id)?
                    }
                };
                Ok(Self::Amqp(factory.create_connection().await?))
            }
            other => bail!(JmsError::illegal_state(format!(
                "protocol {other} not supported"
            ))),
        }
    }

    /// `createSession(transacted, AUTO_ACKNOWLEDGE)`.
    pub async fn session(&self, transacted: bool) -> anyhow::Result<Sess> {
        Ok(match self {
            Self::Core(c) => Sess::Core(c.session(transacted).await?),
            Self::Amqp(c) => Sess::Amqp(Arc::new(c.session(transacted).await?), c.clone()),
        })
    }

    /// How the commands name a destination (`Destination.toString`).
    pub fn dest_text(&self, dest: &Destination) -> String {
        match self {
            Self::Core(_) => dest.to_string(),
            Self::Amqp(_) => dest.address().to_owned(),
        }
    }

    pub fn protocol(&self) -> Protocol {
        match self {
            Self::Core(_) => Protocol::Core,
            Self::Amqp(_) => Protocol::Amqp,
        }
    }

    pub async fn close(self) {
        match self {
            Self::Core(c) => c.close().await,
            Self::Amqp(c) => c.close().await,
        }
    }

    pub async fn shutdown(&self) {
        match self {
            Self::Core(c) => c.shutdown().await,
            Self::Amqp(c) => c.close().await,
        }
    }
}

/// The URL Qpid is given: `tcp://` becomes `amqp://`.
fn amqp_url(url: &str) -> String {
    match url.strip_prefix("tcp://") {
        Some(rest) => format!("amqp://{rest}"),
        None => url.to_owned(),
    }
}

fn is_security(e: &JmsError) -> bool {
    matches!(
        e.class,
        "javax.jms.JMSSecurityException"
            | "org.apache.qpid.jms.exceptions.JMSSecuritySaslException"
    )
}

/// `Transfer.userPassword`.
fn transfer_user_password(
    common: &Common,
    uri: &str,
    user: Option<String>,
    password: Option<String>,
) -> anyhow::Result<(Option<String>, Option<String>)> {
    println!("Type in user/password towards {uri}");
    let user = common
        .input("--user", "Type the username for a retry", user.as_deref())?
        .or(user);
    let password = if common.is_silent_input() {
        password
    } else {
        Some(common.input_password("--password", "Type the password for a retry")?)
    };
    Ok((user, password))
}

/// `ConnectionAbstract.createAMQPConnectionFactory`: a Qpid factory whose
/// connection is tried at once; when that fails, `Connection failed::`,
/// then the user and password (and for a connection failure the URL)
/// asked for and tried once more, its failure ignored.
/// The user and password asked for are kept on the command, as
/// `inputUser` and `inputPassword` keep them.
async fn amqp_factory(args: &mut ConnectionArgs) -> anyhow::Result<Factory> {
    crate::shell::recover(args);
    let mut url = amqp_url(args.url());
    let factory = Factory::new(
        args.user.clone(),
        args.password.clone(),
        &url,
        args.client_id.clone(),
    )?;
    let e = match factory.create_connection().await {
        Ok(c) => {
            c.close().await;
            crate::shell::save_connection(&url, args.user.as_deref(), args.password.as_deref());
            return Ok(factory);
        }
        Err(e) => e,
    };
    eprintln!("Connection failed::{}", e.message);
    if !is_security(&e) {
        url = args
            .common
            .input(
                "--url",
                "Type in the connection URL for a retry (e.g. tcp://localhost:61616)",
                Some(&url),
            )?
            .unwrap_or(url);
    }
    if args.user.is_none() {
        args.user = args
            .common
            .input("--user", "Type the username for a retry", None)?;
    }
    if args.password.is_none() {
        args.password = Some(
            args.common
                .input_password("--password", "Type the password for a retry")?,
        );
    }
    let (user, password) = (args.user.clone(), args.password.clone());
    let factory = Factory::new(user.clone(), password.clone(), &url, args.client_id.clone())?;
    if let Ok(c) = factory.create_connection().await {
        c.close().await;
        crate::shell::save_connection(&url, user.as_deref(), password.as_deref());
    }
    Ok(factory)
}

impl Sess {
    pub fn transacted(&self) -> bool {
        match self {
            Self::Core(s) => s.transacted,
            Self::Amqp(s, _) => s.transacted,
        }
    }

    /// `createProducer(destination)`.
    pub async fn producer(&self, dest: &Destination) -> anyhow::Result<Prod> {
        Ok(match self {
            Self::Core(s) => Prod::Core(s.producer(dest).await?, dest.clone()),
            Self::Amqp(s, _) => Prod::Amqp(s.producer(dest).await?),
        })
    }

    /// `producer.send(message)` with the delivery mode and time to live:
    /// the message as sent.
    pub async fn send(
        &self,
        p: &Prod,
        message: &mut Msg,
        persistent: bool,
        ttl: i64,
    ) -> anyhow::Result<()> {
        match (self, p, message) {
            (Self::Core(s), Prod::Core(p, dest), Msg::Core { message, .. }) => {
                *message = s
                    .send(p, dest, std::mem::take(message), persistent, ttl)
                    .await?;
                Ok(())
            }
            (Self::Amqp(s, _), Prod::Amqp(p), Msg::Amqp(m)) => {
                s.send(p, m, persistent, 4, ttl, true, true).await?;
                Ok(())
            }
            _ => bail!("a message of another protocol"),
        }
    }

    /// `producer.send(received)` of a message a consumer of either client
    /// received: the producer's client copies a foreign one first.
    pub async fn send_received(&self, p: &Prod, message: Msg) -> anyhow::Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        match (self, p, message) {
            (Self::Core(s), Prod::Core(p, dest), Msg::Core { mut message, .. }) => {
                message.message_id = 0;
                s.send(p, dest, message, true, 0).await?;
            }
            (Self::Core(s), Prod::Core(p, dest), Msg::Amqp(mut m)) => {
                let message = super::foreign::qpid_to_core(&mut m, true, now)?;
                s.send(p, dest, message, true, 0).await?;
            }
            (Self::Amqp(s, _), Prod::Amqp(p), Msg::Amqp(mut m)) => {
                s.send(p, &mut m, true, 4, 0, true, true).await?;
            }
            (Self::Amqp(s, _), Prod::Amqp(p), Msg::Core { mut message, .. }) => {
                let mut m = super::foreign::core_to_qpid(&mut message, true, now)?;
                s.send(p, &mut m, true, 4, 0, true, true).await?;
            }
            _ => bail!("a producer of another protocol"),
        }
        Ok(())
    }

    /// `createConsumer(dest, selector)`, `createDurableSubscriber(topic,
    /// name, selector, noLocal)`, `createSharedConsumer` or
    /// `createSharedDurableConsumer`, as `kind` says.
    pub async fn consumer(
        &self,
        dest: &Destination,
        selector: Option<&str>,
        kind: ConsumerKind,
    ) -> anyhow::Result<Cons> {
        Ok(match self {
            Self::Core(s) => Cons::Core(match kind {
                ConsumerKind::Plain | ConsumerKind::Browser => {
                    s.consumer(dest, selector, None).await?
                }
                ConsumerKind::Durable { name, no_local } => {
                    let mut selector = selector.filter(|s| !s.is_empty()).map(str::to_owned);
                    if no_local {
                        let filter = s.no_local_filter();
                        selector = Some(match selector {
                            Some(s) => format!("{s} AND {filter}"),
                            None => filter,
                        });
                    }
                    let client_id = s.client_id();
                    s.consumer(
                        dest,
                        selector.as_deref(),
                        Some((&name, client_id.as_deref())),
                    )
                    .await?
                }
                ConsumerKind::Shared { name, durable } => {
                    s.shared_consumer(dest.address(), &name, durable, selector)
                        .await?
                }
            }),
            Self::Amqp(s, c) => {
                Cons::Amqp(Box::new(s.consumer(dest, selector, kind).await?), c.clone())
            }
        })
    }

    /// `createBrowser(queue, selector)`.
    pub async fn browser(&self, queue: &str, selector: Option<&str>) -> anyhow::Result<Cons> {
        Ok(match self {
            Self::Core(s) => Cons::Core(s.browser(queue, selector).await?),
            Self::Amqp(s, c) => Cons::Amqp(
                Box::new(
                    s.browser(&Destination::Queue(queue.to_owned()), selector)
                        .await?,
                ),
                c.clone(),
            ),
        })
    }

    /// `receive` (none: for ever; zero: `receiveNoWait`).
    pub async fn receive(
        &self,
        c: &Cons,
        timeout: Option<Duration>,
    ) -> anyhow::Result<Option<Msg>> {
        Ok(match (self, c) {
            (Self::Core(s), Cons::Core(c)) => s.receive(c, timeout).await?.map(core_msg),
            (Self::Amqp(s, _), Cons::Amqp(c, _)) => {
                s.receive(c, timeout).await?.map(|m| Msg::Amqp(Box::new(m)))
            }
            _ => bail!("a consumer of another protocol"),
        })
    }

    /// The enumeration's next message.
    pub async fn browse_next(&self, c: &Cons) -> anyhow::Result<Option<Msg>> {
        Ok(match (self, c) {
            (Self::Core(_), Cons::Core(c)) => c
                .receive_immediate()
                .await
                .map_err(|e| anyhow::anyhow!(super::jms::message(&e)))?
                .map(core_msg),
            // `JmsQueueBrowser.hasMoreElements`: `receiveNoWait`, its failure
            // logged and taken as the end.
            (Self::Amqp(s, _), Cons::Amqp(c, _)) => {
                match s.receive(c, Some(Duration::ZERO)).await {
                    Ok(m) => m.map(|m| Msg::Amqp(Box::new(m))),
                    Err(e) => {
                        tracing::warn!(
                            target: "org.apache.qpid.jms.JmsQueueBrowser",
                            "Error while receive the next message: {}",
                            e.message
                        );
                        None
                    }
                }
            }
            _ => bail!("a consumer of another protocol"),
        })
    }

    pub async fn commit(&self) -> anyhow::Result<()> {
        match self {
            Self::Core(s) => s.commit().await,
            Self::Amqp(s, _) => Ok(s.commit().await?),
        }
    }

    pub async fn rollback(&self) -> anyhow::Result<()> {
        match self {
            Self::Core(s) => s.rollback().await,
            Self::Amqp(s, _) => Ok(s.rollback().await?),
        }
    }

    /// `connection.start()`: a Qpid consumer has its credit already.
    pub async fn start(&self) -> anyhow::Result<()> {
        match self {
            Self::Core(s) => s.start().await,
            Self::Amqp(..) => Ok(()),
        }
    }

    pub async fn close(&self) {
        match self {
            Self::Core(s) => s.close().await,
            Self::Amqp(s, _) => s.close().await,
        }
    }

    /// What closing the connection does to a session a command left open:
    /// `ActiveMQConnection.close` closes a Core one; Qpid's only shuts its
    /// own down, sending nothing.
    pub async fn finish(&self) {
        if let Self::Core(s) = self {
            s.close().await;
        }
    }

    /// `Destination.toString` of the session's client.
    pub fn dest_text(&self, dest: &Destination) -> String {
        match self {
            Self::Core(_) => dest.to_string(),
            Self::Amqp(..) => dest.address().to_owned(),
        }
    }
}

fn core_msg(d: Delivery) -> Msg {
    let large = d.is_large();
    Msg::Core {
        message: d.message,
        large,
    }
}

impl Prod {
    pub async fn close(&self) {
        match self {
            Self::Core(p, _) => p.close(),
            Self::Amqp(p) => p.close().await,
        }
    }
}

impl Cons {
    pub async fn close(&self) {
        match self {
            Self::Core(c) => c.close().await,
            Self::Amqp(c, conn) => c.close(conn).await,
        }
    }
}

/// A connection factory `createConnectionFactory` made, its `tryConnect`
/// done: what the perf commands make their connections with.
pub enum ConnFactory {
    Core(ConnectionArgs),
    Amqp(Box<Factory>),
}

impl ConnFactory {
    /// `createConnectionFactory(url, user, password, null, protocol)`.
    pub async fn create(args: &ConnectionArgs) -> anyhow::Result<Self> {
        let mut args = args.clone();
        args.client_id = None;
        match Protocol::parse(&args.protocol) {
            Some(Protocol::Amqp) => Ok(Self::Amqp(Box::new(amqp_factory(&mut args).await?))),
            _ => {
                args.try_connect().await?;
                Ok(Self::Core(args))
            }
        }
    }

    /// `createConnection()`, then `setClientID(id)` when there is one.
    pub async fn connect(&self, client_id: Option<String>) -> anyhow::Result<Conn> {
        match self {
            Self::Core(args) => {
                let mut args = args.clone();
                args.client_id = client_id;
                Ok(Conn::Core(args.connect().await?))
            }
            Self::Amqp(f) => Ok(Conn::Amqp(f.create_connection_as(client_id).await?)),
        }
    }

    pub fn protocol(&self) -> Protocol {
        match self {
            Self::Core(_) => Protocol::Core,
            Self::Amqp(_) => Protocol::Amqp,
        }
    }
}

/// A message a listener was given, to be acknowledged once handled.
pub enum Ack {
    Core(Delivery),
    Amqp(super::amqp::engine::Delivery),
}

/// A send's completion: its outcome, once known.
pub type Completion =
    std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>;

impl Conn {
    /// A session for sends with a `CompletionListener`: a Core one does not
    /// block on durable sends; Qpid's sends are asynchronous already.
    pub async fn async_send_session(&self, transacted: bool) -> anyhow::Result<Sess> {
        Ok(match self {
            Self::Core(c) => Sess::Core(c.async_send_session(transacted).await?),
            Self::Amqp(c) => Sess::Amqp(Arc::new(c.session(transacted).await?), c.clone()),
        })
    }

    /// Whether the connection broke, as its exception listener would hear.
    pub fn failed(&self) -> bool {
        match self {
            Self::Core(c) => c.conn.failure().is_some(),
            Self::Amqp(c) => c.failed(),
        }
    }
}

impl Sess {
    /// `send(message, completionListener)`: the send made, its completion
    /// to come. A Core send completes once written.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_completion(
        &self,
        p: &Prod,
        message: Msg,
        persistent: bool,
        ttl: i64,
        message_id: bool,
        timestamp: bool,
    ) -> anyhow::Result<Completion> {
        match (self, p, message) {
            (Self::Core(s), Prod::Core(p, dest), Msg::Core { message, .. }) => {
                s.send_with(p, dest, message, persistent, ttl, message_id, timestamp)
                    .await?;
                Ok(Box::pin(async { Ok(()) }))
            }
            (Self::Amqp(s, _), Prod::Amqp(p), Msg::Amqp(mut m)) => {
                let done = s
                    .send_completion(p, &mut m, persistent, 4, ttl, message_id, timestamp)
                    .await;
                Ok(Box::pin(
                    async move { done.await.map_err(anyhow::Error::from) },
                ))
            }
            _ => bail!("a message of another protocol"),
        }
    }

    /// What a `MessageListener` is given next, waiting up to `wait`.
    pub async fn listened(&self, c: &Cons, wait: Duration) -> anyhow::Result<Option<(Msg, Ack)>> {
        Ok(match (self, c) {
            (Self::Core(_), Cons::Core(c)) => c.receive(wait).await?.map(|d| {
                let m = Msg::Core {
                    message: d.message.clone(),
                    large: d.is_large(),
                };
                (m, Ack::Core(d))
            }),
            (Self::Amqp(s, _), Cons::Amqp(c, _)) => s
                .listened(c, wait)
                .await?
                .map(|(m, d)| (Msg::Amqp(Box::new(m)), Ack::Amqp(d))),
            _ => bail!("a consumer of another protocol"),
        })
    }

    /// The acknowledgement after `onMessage`.
    pub async fn acknowledge(&self, c: &Cons, ack: Ack) -> anyhow::Result<()> {
        match (self, c, ack) {
            (Self::Core(_), Cons::Core(c), Ack::Core(d)) => Ok(c.acknowledge(&d).await?),
            (Self::Amqp(s, _), Cons::Amqp(c, _), Ack::Amqp(d)) => {
                s.acknowledge(c, d).await;
                Ok(())
            }
            _ => bail!("a consumer of another protocol"),
        }
    }

    /// `unsubscribe(name)`.
    pub async fn unsubscribe(&self, name: &str) -> anyhow::Result<()> {
        match self {
            Self::Core(s) => s.unsubscribe(name).await,
            Self::Amqp(_, c) => Ok(c.unsubscribe(name).await?),
        }
    }
}

impl Msg {
    /// A long property's value (`getLongProperty`), if it has one.
    pub fn long_property(&self, name: &str) -> Option<i64> {
        match self {
            Self::Core { message, .. } => match message.property(name) {
                Some(PropertyValue::Long(l)) => Some(*l),
                _ => None,
            },
            Self::Amqp(m) => match m.property(name) {
                Some(saranga_protocol_amqp::types::Value::Long(l)) => Some(*l),
                _ => None,
            },
        }
    }
}
