//! What the Artemis Core JMS client does on the wire for the CLI's JMS
//! calls: `ActiveMQConnection` (the initial session, the client ID),
//! `ActiveMQSession` (sessions per acknowledge mode, `checkDestination`,
//! `createConsumer`, `createBrowser`), `AutoCreateUtil.autoCreateQueue` and
//! `ActiveMQMessageProducer.doSendx`.

use std::time::Duration;

use anyhow::bail;
use saranga_client::PropertyValue;
use saranga_client::message::{CONNECTION_ID, DEFAULT_PRIORITY, HDR_GROUP_ID};
use saranga_client::{
    Connection, Consumer, ConsumerOptions, CoreMessage, Delivery, Error, ErrorType, MessageExt,
    Producer, QueueConfiguration, RoutingType, Session, SessionOptions,
};

/// `ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY`.
const JMS_SESSION: &str = "jms-session";
/// `ClientSession.JMS_SESSION_CLIENT_ID_PROPERTY`.
const JMS_CLIENT_ID: &str = "jms-client-id";
/// `ActiveMQClient.DEFAULT_ACK_BATCH_SIZE`: the batch of a transacted session.
const TRANSACTION_BATCH_SIZE: i32 = 1024 * 1024;

/// The text of a client error, as `getMessage` gives it.
pub fn message(e: &Error) -> String {
    match e {
        Error::ActiveMq { message, .. } => message.clone(),
        other => other.to_string(),
    }
}

/// A JMS queue or topic (`ActiveMQQueue`, `ActiveMQTopic`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Destination {
    Queue(String),
    Topic(String),
}

impl Destination {
    /// `DestAbstract.getDestination`: `topic://` makes a topic, anything
    /// else (`queue://`, no prefix) a queue.
    pub fn parse(destination: &str) -> Self {
        let name = match destination.find("://") {
            Some(i) => destination[i + 3..].to_owned(),
            None => destination.to_owned(),
        };
        if destination.starts_with("topic://") {
            Self::Topic(name)
        } else {
            Self::Queue(name)
        }
    }

    pub fn address(&self) -> &str {
        match self {
            Self::Queue(a) | Self::Topic(a) => a,
        }
    }

    pub fn is_queue(&self) -> bool {
        matches!(self, Self::Queue(_))
    }

    fn routing_type(&self) -> RoutingType {
        if self.is_queue() {
            RoutingType::Anycast
        } else {
            RoutingType::Multicast
        }
    }
}

impl std::fmt::Display for Destination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queue(a) => write!(f, "ActiveMQQueue[{a}]"),
            Self::Topic(a) => write!(f, "ActiveMQTopic[{a}]"),
        }
    }
}

/// An `ActiveMQConnection`: a Core connection and its initial session,
/// which validated the user and holds the client ID.
pub struct JmsConnection {
    pub conn: Connection,
    initial: Session,
    /// `connID`: the client ID, else the connection's UID.
    pub conn_id: String,
    pub client_id: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

impl JmsConnection {
    /// `createConnection`: a connection and its initial session; with a
    /// client ID, `jms-client-id` unique metadata.
    pub async fn connect(
        url: &str,
        user: Option<String>,
        password: Option<String>,
        client_id: Option<String>,
    ) -> anyhow::Result<Self> {
        let conn = super::open(url, user.as_deref(), password.as_deref()).await?;
        let options = SessionOptions {
            username: user.clone(),
            password: password.clone(),
            ..SessionOptions::default()
        };
        let initial = match conn.create_session(options).await {
            Ok(s) => s,
            Err(e) => {
                conn.close().await;
                bail!(ConnectFailed::from(&e));
            }
        };
        if let Some(id) = &client_id
            && let Err(e) = initial.add_unique_metadata(JMS_CLIENT_ID, id).await
        {
            conn.close().await;
            bail!(ConnectFailed::from(&e));
        }
        let uid = saranga_client::uuid::to_string(&saranga_client::uuid::time_based());
        Ok(Self {
            conn,
            initial,
            conn_id: client_id.clone().unwrap_or(uid),
            client_id,
            user,
            password,
        })
    }

    /// `createSession(transacted, AUTO_ACKNOWLEDGE)`.
    pub async fn session(&self, transacted: bool) -> anyhow::Result<JmsSession> {
        let options = SessionOptions {
            username: self.user.clone(),
            password: self.password.clone(),
            auto_commit_sends: !transacted,
            auto_commit_acks: !transacted,
            ack_batch_size: if transacted {
                TRANSACTION_BATCH_SIZE
            } else {
                0
            },
            ..SessionOptions::default()
        };
        self.jms_session(options, transacted).await
    }

    /// A session whose sends do not block, as a JMS producer sending with
    /// a `CompletionListener` does.
    pub async fn async_send_session(&self, transacted: bool) -> anyhow::Result<JmsSession> {
        let options = SessionOptions {
            username: self.user.clone(),
            password: self.password.clone(),
            auto_commit_sends: !transacted,
            auto_commit_acks: !transacted,
            ack_batch_size: if transacted {
                TRANSACTION_BATCH_SIZE
            } else {
                0
            },
            block_on_durable_send: false,
            ..SessionOptions::default()
        };
        self.jms_session(options, transacted).await
    }

    async fn jms_session(
        &self,
        options: SessionOptions,
        transacted: bool,
    ) -> anyhow::Result<JmsSession> {
        let session = self.conn.create_session(options).await?;
        session.add_metadata(JMS_SESSION, "").await?;
        if let Some(id) = &self.client_id {
            session.add_metadata(JMS_CLIENT_ID, id).await?;
        }
        Ok(JmsSession {
            session,
            transacted,
            conn_id: self.conn_id.clone(),
            client_id: self.client_id.clone(),
        })
    }

    /// `close`, for a connection that is shared.
    pub async fn shutdown(&self) {
        self.initial.close().await;
        self.conn.close().await;
    }

    /// The user and password the connection was made with.
    pub fn credentials(&self) -> (Option<String>, Option<String>) {
        (self.user.clone(), self.password.clone())
    }

    pub async fn close(self) {
        self.initial.close().await;
        self.conn.close().await;
    }
}

/// The connection could not be made: `Connection failed::<reason>`. A
/// `JMSSecurityException` when the broker refused the user
/// (`SECURITY_EXCEPTION`).
#[derive(Debug)]
pub struct ConnectFailed {
    pub reason: String,
    pub security: bool,
}

impl From<&Error> for ConnectFailed {
    fn from(e: &Error) -> Self {
        Self {
            reason: message(e),
            security: matches!(e, Error::ActiveMq { code: 105, .. }),
        }
    }
}

impl std::fmt::Display for ConnectFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for ConnectFailed {}

/// An `ActiveMQSession` in `AUTO_ACKNOWLEDGE` or `SESSION_TRANSACTED` mode.
#[derive(Clone)]
pub struct JmsSession {
    pub session: Session,
    pub transacted: bool,
    conn_id: String,
    client_id: Option<String>,
}

/// `ActiveMQDestination.createQueueNameForSubscription` for a durable
/// subscription: `<clientID>.<name>`, each with `\` and `.` escaped.
pub fn subscription_queue(client_id: &str, name: &str) -> String {
    format!("{}.{}", escape(client_id), escape(name))
}

/// `DestinationUtil.escape`.
fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('.', "\\.")
}

/// `DestinationUtil.createQueueNameForSubscription`: the queue of a
/// (shared or not) subscription.
pub fn subscription_queue_name(durable: bool, client_id: Option<&str>, name: &str) -> String {
    match (client_id, durable) {
        (Some(id), true) => subscription_queue(id, name),
        (Some(id), false) => format!("nonDurable.{}.{}", escape(id), escape(name)),
        (None, true) => escape(name),
        (None, false) => format!("nonDurable.{}", escape(name)),
    }
}

impl JmsSession {
    /// `ActiveMQSession.checkDestination`: a producer's destination exists,
    /// or is auto-created.
    pub async fn check_destination(&self, dest: &Destination) -> anyhow::Result<()> {
        let address = dest.address();
        let s = &self.session;
        let query = s.address_query(address).await?;
        if !query.exists {
            if dest.is_queue() {
                if query.auto_create_addresses && query.auto_create_queues {
                    s.create_address(address, &[RoutingType::Anycast], true)
                        .await?;
                } else {
                    bail!(
                        "Destination {address} does not exist, autoCreateAddresses={} , autoCreateQueues={}",
                        query.auto_create_addresses,
                        query.auto_create_queues
                    );
                }
            } else if query.auto_create_addresses {
                s.create_address(address, &[RoutingType::Multicast], true)
                    .await?;
            } else {
                bail!(
                    "Destination {address} does not exist, autoCreateAddresses={}",
                    query.auto_create_addresses
                );
            }
        } else if (!query.supports_multicast && !dest.is_queue())
            || (!query.supports_anycast && dest.is_queue())
        {
            bail!(
                "Destination {address} exists, but does not support {} routing",
                if dest.is_queue() {
                    "ANYCAST"
                } else {
                    "MULTICAST"
                }
            );
        }
        if dest.is_queue() && !s.queue_query(address).await?.exists {
            if query.auto_create_queues {
                let config = self
                    .queue_config(address, address, RoutingType::Anycast, None, true, &query)
                    .auto_created(true);
                ignore_exists(s.create_queue(config).await)?;
            } else {
                bail!(
                    "Destination {address} does not exist, address exists but autoCreateQueues={}",
                    query.auto_create_queues
                );
            }
        }
        Ok(())
    }

    /// `AutoCreateUtil.setRequiredQueueConfigurationIfNotSet`.
    fn queue_config(
        &self,
        name: &str,
        address: &str,
        routing_type: RoutingType,
        filter: Option<&str>,
        durable: bool,
        query: &saranga_client::AddressQuery,
    ) -> QueueConfiguration {
        let mut config = QueueConfiguration::new(name)
            .address(address)
            .routing_type(routing_type)
            .durable(durable);
        if let Some(f) = filter {
            config = config.filter(f);
        }
        config.max_consumers = Some(query.default_max_consumers);
        config.purge_on_no_consumers = Some(query.default_purge_on_no_consumers);
        config
    }

    pub async fn producer(&self, dest: &Destination) -> anyhow::Result<Producer> {
        self.check_destination(dest).await?;
        Ok(self.session.create_producer(Some(dest.address())).await?)
    }

    /// `ActiveMQMessageProducer.doSendx` with the producer's defaults.
    pub async fn send(
        &self,
        producer: &Producer,
        dest: &Destination,
        message: CoreMessage,
        persistent: bool,
        ttl: i64,
    ) -> anyhow::Result<CoreMessage> {
        self.send_with(producer, dest, message, persistent, ttl, true, true)
            .await
    }

    /// `doSendx` of a producer that may disable the message ID and the
    /// timestamp (`setDisableMessageID`, `setDisableMessageTimestamp`).
    #[allow(clippy::too_many_arguments)]
    pub async fn send_with(
        &self,
        producer: &Producer,
        dest: &Destination,
        mut message: CoreMessage,
        persistent: bool,
        ttl: i64,
        message_id: bool,
        timestamp: bool,
    ) -> anyhow::Result<CoreMessage> {
        let now = now_millis();
        message
            .set_durable(persistent)
            .set_priority(DEFAULT_PRIORITY)
            .set_expiration(if ttl == 0 { 0 } else { now + ttl })
            .set_timestamp(if timestamp { now } else { 0 });
        if message_id {
            message.set_new_user_id();
        } else {
            message.set_user_id(None);
        }
        message
            .put_string_property(CONNECTION_ID, &self.conn_id)
            .set_routing_type(dest.routing_type());
        producer.send(message.clone()).await?;
        Ok(message)
    }

    pub fn client_id(&self) -> Option<String> {
        self.client_id.clone()
    }

    /// The filter `noLocal` adds: the connection's client ID, else its ID,
    /// against the connection ID producers set.
    pub fn no_local_filter(&self) -> String {
        format!(
            "{CONNECTION_ID}<>'{}'",
            self.client_id.as_deref().unwrap_or(&self.conn_id)
        )
    }

    /// `ActiveMQSession.createSharedConsumer` / `createSharedDurableConsumer`:
    /// the subscription's queue, made shared when it is missing or
    /// different.
    pub async fn shared_consumer(
        &self,
        topic: &str,
        name: &str,
        durable: bool,
        selector: Option<&str>,
    ) -> anyhow::Result<Consumer> {
        let s = &self.session;
        let filter = selector
            .filter(|f| !f.is_empty())
            .map(saranga_filter::translate::convert_to_activemq_filter_string);
        let query = s.address_query(topic).await?;
        if !query.exists && !query.auto_create_addresses {
            bail!("Destination {topic} does not exist");
        }
        let queue = subscription_queue_name(durable, self.client_id.as_deref(), name);
        let sub = s.queue_query(&queue).await?;
        if (!sub.exists
            || sub.address.as_deref() != Some(topic)
            || sub.filter_string.as_deref() != filter.as_deref())
            && sub.configuration_managed != Some(true)
        {
            let config = self.queue_config(
                &queue,
                topic,
                RoutingType::Multicast,
                filter.as_deref(),
                durable,
                &query,
            );
            ignore_exists(s.create_shared_queue(config).await)?;
        }
        Ok(s.create_consumer(&queue, None, false).await?)
    }

    /// `ActiveMQSession.unsubscribe`: the durable subscription's queue
    /// deleted, unless it has consumers. Its errors read as the JMS
    /// exceptions' `toString`.
    pub async fn unsubscribe(&self, name: &str) -> anyhow::Result<()> {
        let queue = subscription_queue_name(true, self.client_id.as_deref(), name);
        let s = &self.session;
        let response = s.queue_query(&queue).await?;
        if !response.exists {
            bail!(
                "javax.jms.InvalidDestinationException: Cannot unsubscribe, subscription with name {name} does not exist"
            );
        }
        if response.consumer_count != 0 {
            bail!(
                "javax.jms.IllegalStateException: Cannot unsubscribe durable subscription {name} since it has active subscribers"
            );
        }
        s.delete_queue(&queue).await?;
        Ok(())
    }

    /// `ActiveMQSession.createConsumer` / `createDurableSubscriber`: a
    /// queue consumer, a non-durable topic subscription on a temporary
    /// queue, or a durable one (`subscription`, which needs a client ID).
    pub async fn consumer(
        &self,
        dest: &Destination,
        selector: Option<&str>,
        subscription: Option<(&str, Option<&str>)>,
    ) -> anyhow::Result<Consumer> {
        let s = &self.session;
        let filter = selector
            .filter(|f| !f.is_empty())
            .map(saranga_filter::translate::convert_to_activemq_filter_string);
        let queue = match dest {
            Destination::Queue(address) => {
                self.auto_create_queue(address).await?;
                address.clone()
            }
            Destination::Topic(address) => {
                let query = s.address_query(address).await?;
                if !query.exists {
                    if query.auto_create_addresses {
                        s.create_address(address, &[RoutingType::Multicast], true)
                            .await?;
                    } else {
                        bail!("Topic {address} does not exist");
                    }
                }
                match subscription {
                    None => {
                        let name =
                            saranga_client::uuid::to_string(&saranga_client::uuid::time_based());
                        let config = self
                            .queue_config(
                                &name,
                                address,
                                RoutingType::Multicast,
                                filter.as_deref(),
                                false,
                                &query,
                            )
                            .temporary(true);
                        s.create_queue(config).await?;
                        name
                    }
                    Some((name, client_id)) => {
                        let Some(client_id) = client_id else {
                            bail!(
                                "Cannot create durable subscription - client ID has not been set"
                            );
                        };
                        let queue = subscription_queue(client_id, name);
                        let sub = s.queue_query(&queue).await?;
                        if !sub.exists {
                            let config = self.queue_config(
                                &queue,
                                address,
                                RoutingType::Multicast,
                                filter.as_deref(),
                                true,
                                &query,
                            );
                            s.create_queue(config).await?;
                        } else {
                            if sub.consumer_count > 0 {
                                bail!(
                                    "Cannot create a subscriber on the durable subscription since it already has subscriber(s)"
                                );
                            }
                            let changed = sub.filter_string.as_deref() != filter.as_deref()
                                || sub.address.as_deref() != Some(address.as_str());
                            if changed {
                                // Changing a durable subscription is
                                // unsubscribing and subscribing again.
                                s.delete_queue(&queue).await?;
                                let config = self.queue_config(
                                    &queue,
                                    address,
                                    RoutingType::Multicast,
                                    filter.as_deref(),
                                    true,
                                    &query,
                                );
                                s.create_queue(config).await?;
                            }
                        }
                        queue
                    }
                }
            }
        };
        Ok(s.create_consumer_with(
            &queue,
            ConsumerOptions {
                filter: if dest.is_queue() || subscription.is_none() {
                    filter
                } else {
                    None
                },
                ..ConsumerOptions::default()
            },
        )
        .await?)
    }

    /// `AutoCreateUtil.autoCreateQueue` for a JMS queue.
    async fn auto_create_queue(&self, address: &str) -> anyhow::Result<()> {
        let s = &self.session;
        let query = s.address_query(address).await?;
        if !query.exists || !query.queue_names.iter().any(|q| q == address) {
            if query.auto_create_queues {
                let config = self
                    .queue_config(address, address, RoutingType::Anycast, None, true, &query)
                    .auto_created(true);
                ignore_exists(s.create_queue(config).await)?;
            } else {
                bail!("Destination {address} does not exist");
            }
        } else {
            let q = s.queue_query(address).await?;
            if q.routing_type.is_some_and(|t| t != RoutingType::Anycast) {
                bail!("Destination {address} does not support JMS queue semantics");
            }
        }
        Ok(())
    }

    /// `ActiveMQSession.createBrowser`: a browse-only consumer, made when
    /// the enumeration is.
    pub async fn browser(&self, queue: &str, selector: Option<&str>) -> anyhow::Result<Consumer> {
        let filter = selector
            .filter(|f| !f.is_empty())
            .map(saranga_filter::translate::convert_to_activemq_filter_string);
        self.auto_create_queue(queue).await?;
        Ok(self
            .session
            .create_consumer_with(
                queue,
                ConsumerOptions {
                    filter,
                    browse_only: true,
                    ..ConsumerOptions::default()
                },
            )
            .await?)
    }

    /// `MessageConsumer.receive(timeout)` (none: wait for ever; zero:
    /// `receiveNoWait`), acknowledged as the session's mode says.
    pub async fn receive(
        &self,
        consumer: &Consumer,
        timeout: Option<Duration>,
    ) -> anyhow::Result<Option<Delivery>> {
        let delivery = match timeout {
            Some(t) if t.is_zero() => consumer.receive_immediate().await?,
            Some(t) => consumer.receive(t).await?,
            None => loop {
                if let Some(d) = consumer.receive(Duration::from_secs(3600)).await? {
                    break Some(d);
                }
            },
        };
        if let Some(d) = &delivery {
            consumer.acknowledge(d).await?;
        }
        Ok(delivery)
    }

    pub async fn commit(&self) -> anyhow::Result<()> {
        Ok(self.session.commit().await?)
    }

    pub async fn rollback(&self) -> anyhow::Result<()> {
        Ok(self.session.rollback(false).await?)
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        Ok(self.session.start().await?)
    }

    pub async fn close(&self) {
        self.session.close().await;
    }
}

/// A queue made by someone else between the query and the create is fine.
fn ignore_exists(r: saranga_client::Result<()>) -> saranga_client::Result<()> {
    match r {
        Err(e) if e.kind() == Some(ErrorType::QueueExists) => Ok(()),
        r => r,
    }
}

pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// The text of a JMS message for the CLI's output: a TEXT body's text, else
/// the JMS message ID.
/// Which `ActiveMQMessage` setter sets a property: the typed one for the
/// value's class, or `setObjectProperty`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Setter {
    Typed,
    Object,
}

/// `ActiveMQMessage.checkProperty` then the setter, through `MessageUtil`'s
/// mapping of `JMSXGroupID`, `JMSXGroupSeq` and `JMSXUserID` to the
/// message's group and validated user.
pub fn set_core_property(
    m: &mut CoreMessage,
    name: &str,
    value: &PropertyValue,
    setter: Setter,
) -> Result<(), crate::client::amqp::jms::JmsError> {
    use crate::client::amqp::jms::JmsError;
    use crate::client::java_chars::{is_java_identifier_part, is_java_identifier_start};
    let units: Vec<u16> = name.encode_utf16().collect();
    if units.is_empty() {
        return Err(JmsError {
            class: "java.lang.IllegalArgumentException",
            message: "The name of a property must not be an empty String.".to_owned(),
        });
    }
    let runtime = |message: String| JmsError {
        class: "javax.jms.JMSRuntimeException",
        message,
    };
    if !is_java_identifier_start(units[0])
        || !units[1..].iter().all(|u| is_java_identifier_part(*u))
    {
        return Err(runtime(format!(
            "AMQ139012: The property name '{name}' is not a valid java identifier."
        )));
    }
    if matches!(
        name,
        "NULL"
            | "TRUE"
            | "FALSE"
            | "NOT"
            | "AND"
            | "OR"
            | "BETWEEN"
            | "LIKE"
            | "IN"
            | "IS"
            | "ESCAPE"
    ) {
        return Err(runtime(format!(
            "The property name '{name}' is reserved due to selector syntax."
        )));
    }
    if name.starts_with("JMS_ACTIVEMQ") {
        return Err(runtime(format!(
            "The property name '{name}' is illegal since it starts with JMS_ACTIVEMQ"
        )));
    }
    // `MessageUtil.getInteger`: a number's int value, else the string's.
    let integer = |v: &PropertyValue| -> Result<i32, JmsError> {
        Ok(match v {
            PropertyValue::Byte(b) => i32::from(*b),
            PropertyValue::Short(s) => i32::from(*s),
            PropertyValue::Int(i) => *i,
            PropertyValue::Long(l) => *l as i32,
            // `(int)` of a float or double saturates, NaN is 0.
            PropertyValue::Float(f) => *f as i32,
            PropertyValue::Double(d) => *d as i32,
            other => {
                let text = crate::client::amqp::properties::java_to_string(other)
                    .unwrap_or_else(|| "null".to_owned());
                saranga_filter::java_parse_int::<i32>(&text).ok_or_else(|| JmsError {
                    class: "java.lang.NumberFormatException",
                    message: format!("For input string: \"{text}\""),
                })?
            }
        })
    };
    let text = |v: &PropertyValue| crate::client::amqp::properties::java_to_string(v);
    let as_string = |v: &PropertyValue| {
        text(v).map_or(PropertyValue::Null, |t| {
            PropertyValue::String(t.as_str().into())
        })
    };
    let (key, v) = match (setter, value) {
        (Setter::Typed, PropertyValue::Int(i)) if name == "JMSXGroupSeq" => {
            (HDR_GROUP_SEQUENCE, PropertyValue::Int(*i))
        }
        (Setter::Typed, PropertyValue::Long(l)) if name == "JMSXGroupSeq" => {
            (HDR_GROUP_SEQUENCE, PropertyValue::Int(*l as i32))
        }
        (Setter::Typed, PropertyValue::String(_)) | (Setter::Object, _) => match name {
            "JMSXGroupID" => (HDR_GROUP_ID, as_string(value)),
            "JMSXGroupSeq" => (HDR_GROUP_SEQUENCE, PropertyValue::Int(integer(value)?)),
            "JMSXUserID" => (HDR_VALIDATED_USER, as_string(value)),
            _ => (name, value.clone()),
        },
        (Setter::Typed, _) => (name, value.clone()),
    };
    m.put_property(key, v);
    Ok(())
}

/// `Message.HDR_GROUP_SEQUENCE` and `HDR_VALIDATED_USER`.
const HDR_GROUP_SEQUENCE: &str = "_AMQ_GROUP_SEQUENCE";
const HDR_VALIDATED_USER: &str = "_AMQ_VALIDATED_USER";

pub fn text_or_id(m: &CoreMessage) -> String {
    if m.message_type == saranga_client::message_type::TEXT {
        m.text().ok().flatten().unwrap_or_else(|| "null".to_owned())
    } else {
        m.jms_message_id().unwrap_or_else(|| "null".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::{Destination, subscription_queue};

    #[test]
    fn destinations() {
        assert_eq!(
            Destination::parse("queue://a"),
            Destination::Queue("a".into())
        );
        assert_eq!(
            Destination::parse("topic://a.b"),
            Destination::Topic("a.b".into())
        );
        assert_eq!(Destination::parse("a"), Destination::Queue("a".into()));
        assert_eq!(
            Destination::parse("fqqn://a::b"),
            Destination::Queue("a::b".into())
        );
        assert_eq!(
            Destination::Topic("t".into()).to_string(),
            "ActiveMQTopic[t]"
        );
        assert_eq!(subscription_queue("c.i", "s\\1"), "c\\.i.s\\\\1");
    }
}
