//! An MQTT connection: `MQTTProtocolHandler`, `MQTTConnectionManager`,
//! `MQTTSession`, `MQTTPublishManager`, `MQTTSubscriptionManager`,
//! `MQTTRetainMessageManager` and `MQTTSessionCallback`.
//!
//! One task reads and handles packets in order. Everything sent goes
//! through one ordered queue whose entries wait for the journal writes
//! before them (`runAfterStorageOperations`); the queues delivering to the
//! client's subscriptions add to it too.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use saranga_buffer::{PropertyValue, Reader, SimpleString};
use saranga_core_protocol::ErrorType;
use saranga_message::{CoreMessage, message_type};
use saranga_security::Authenticated;
use saranga_server::message::{ServerMessage, now_millis};
use saranga_server::postoffice::{QueueSpec, RoutingTypes};
use saranga_server::protocol::Connection as Accepted;
use saranga_server::queue::{DeliverySink, Queue};
use saranga_server::session::{
    ClientConnection, Completion, ServerSession, SessionOptions, Transport,
};
use saranga_server::wildcard::WildcardConfig;
use saranga_server::{Broker, BrokerError, RoutingType};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::Settings;
use crate::codec::{self, Packet, Properties, Publish, Subscription, Value, Version, prop};
use crate::state::{
    self, ConnectedClient, Control, CoreDeliveryInfo, CorrelationKey, SessionState, StateManager,
    StateRef, SubscriptionItem, TopicSubscription, WillStatus, lock,
};
use crate::util::{self, reason};

static NEXT_CONNECTION: AtomicU64 = AtomicU64::new(1);

/// What goes to the client, in order.
enum Out {
    /// Bytes to write once these journal writes are done; with no bytes,
    /// only the queue updates waiting on the writes.
    Send {
        wait: Completion,
        bytes: Option<Vec<u8>>,
    },
    /// Everything before is written: close.
    Close,
}

/// The ordered output, shared with the delivering queues.
pub struct Outbound {
    tx: mpsc::UnboundedSender<Out>,
}

impl Outbound {
    fn send(&self, wait: Completion, bytes: Option<Vec<u8>>) {
        let _ = self.tx.send(Out::Send { wait, bytes });
    }

    fn close(&self) {
        let _ = self.tx.send(Out::Close);
    }
}

async fn write_loop<W: AsyncWrite + Unpin>(mut wr: W, mut rx: mpsc::UnboundedReceiver<Out>) {
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut broken = false;
    while let Some(out) = rx.recv().await {
        let close = match out {
            Out::Send { wait, bytes } => {
                match wait.wait().await {
                    Ok(()) => {
                        if let Some(b) = bytes {
                            buf.extend_from_slice(&b);
                        }
                    }
                    Err(e) => tracing::error!(
                        "AMQ834016: Storage operation failed. Error code: 0; message: {e}"
                    ),
                }
                false
            }
            Out::Close => true,
        };
        if !broken && !buf.is_empty() && (close || rx.is_empty() || buf.len() > 256 * 1024) {
            broken = wr.write_all(&buf).await.is_err();
            buf.clear();
        }
        if close {
            break;
        }
    }
    let _ = wr.shutdown().await;
}

/// What the delivering queues ask of the connection's task.
enum Event {
    /// A QoS 0 delivery went out: acknowledge it (`individualAcknowledge`).
    Ack { consumer: i64, message: i64 },
    /// A delivery for a subscription that is gone goes back
    /// (`individualCancel`).
    Cancel { consumer: i64, message: i64 },
    /// A QoS 1 or 2 delivery too large for the client is acknowledged as
    /// if sent ([MQTT-3.1.2-25]).
    Discard { packet_id: i32 },
    /// The broker disconnected a consumer (`MQTTSessionCallback.disconnect`):
    /// it goes (`removeItself`).
    Disconnect { consumer: i64 },
}

/// A packet handler's failure.
enum Failure {
    /// `DisconnectException`: an MQTT 5 client is told why.
    Disconnect(u8),
    /// Anything else: logged, and the connection fails.
    Error(String),
}

impl From<BrokerError> for Failure {
    fn from(e: BrokerError) -> Self {
        Self::Error(e.message)
    }
}

type Handled = Result<(), Failure>;

fn is_security(e: &BrokerError) -> bool {
    e.kind == ErrorType::SecurityException
}

/// What a client's subscriptions deliver through, shared by its sinks.
pub struct Shared {
    out: Arc<Outbound>,
    events: mpsc::UnboundedSender<Event>,
    state: StateRef,
    manager: Arc<StateManager>,
    version: Version,
    client_id: String,
    wildcard: WildcardConfig,
    /// QoS by consumer (`consumerQoSLevels`).
    consumer_qos: Mutex<HashMap<i64, i32>>,
    /// The client's Receive Maximum, or -1.
    receive_maximum: i32,
    default_max_in_flight: i32,
    /// The session stopped: deliveries in flight are dropped.
    closed: AtomicBool,
}

/// `MQTTSessionCallback` for one consumer.
struct MqttSink {
    shared: Arc<Shared>,
    consumer_id: i64,
    durable_queue: bool,
}

impl DeliverySink for MqttSink {
    fn deliver(&self, _consumer_id: i64, message: &ServerMessage, _delivery_count: i32) -> i64 {
        if self.shared.closed.load(Ordering::SeqCst) {
            return 0;
        }
        if let Err(e) = publish_to_client(&self.shared, message.delivery_core(), self.consumer_id) {
            tracing::warn!(
                "AMQ832000: Unable to send message to MQTT client {}: {}: {e}",
                self.shared.client_id,
                message.message_id()
            );
        }
        1
    }

    fn deliver_large(&self, _: i64, _: &ServerMessage, _: i64, _: i32) -> i64 {
        0
    }

    fn deliver_continuation(&self, _: i64, _: Vec<u8>, _: bool) -> i64 {
        0
    }

    fn chunk_size(&self) -> usize {
        usize::MAX
    }

    fn supports_large(&self) -> bool {
        false
    }

    fn disconnect(&self, consumer_id: i64, _reason: &str) {
        let _ = self.shared.events.send(Event::Disconnect {
            consumer: consumer_id,
        });
    }

    /// `hasCredits`: no more durable QoS 1 and 2 messages than the client's
    /// Receive Maximum may be in flight ([MQTT-3.3.4-9]).
    fn has_credits(&self, message: &ServerMessage) -> bool {
        let s = &self.shared;
        let max = if s.receive_maximum > 0 {
            s.receive_maximum
        } else {
            s.default_max_in_flight
        };
        !(message.is_durable() && self.durable_queue && max > 0 && lock(&s.state).send_quota >= max)
    }
}

/// `decideQoS`: the lower of the subscription's and the message's; -1
/// when the subscription is gone.
fn decide_qos(shared: &Shared, message: &CoreMessage, consumer: i64) -> i32 {
    let Some(sub) = lock(&shared.consumer_qos).get(&consumer).copied() else {
        return -1;
    };
    let qos = match message
        .properties
        .get(&SimpleString::from(util::MQTT_QOS_LEVEL_KEY))
    {
        Some(v) => int_value(v).unwrap_or(2),
        None => 2,
    };
    sub.min(qos)
}

fn int_value(v: &PropertyValue) -> Option<i32> {
    match v {
        PropertyValue::Int(i) => Some(*i),
        PropertyValue::Byte(b) => Some(i32::from(*b)),
        PropertyValue::Short(s) => Some(i32::from(*s)),
        PropertyValue::Long(l) => Some(*l as i32),
        PropertyValue::String(s) => s.to_string_lossy().trim().parse().ok(),
        _ => None,
    }
}

fn bool_value(v: Option<&PropertyValue>) -> bool {
    match v {
        Some(PropertyValue::Bool(b)) => *b,
        Some(PropertyValue::String(s)) => s.to_string_lossy().eq_ignore_ascii_case("true"),
        _ => false,
    }
}

fn string_value(v: &PropertyValue) -> Option<String> {
    match v {
        PropertyValue::Null => None,
        PropertyValue::String(s) => Some(saranga_storage::java::simple_string(s)),
        other => Some(format!("{other:?}")),
    }
}

/// `MQTTPublishManager.publishToClient`.
fn publish_to_client(shared: &Shared, message: &CoreMessage, consumer: i64) -> Result<(), String> {
    let qos = decide_qos(shared, message, consumer);
    match qos {
        0 => {
            // [MQTT-2.2.1-2] and [MQTT-3.3.1-2]: no packet ID, no DUP.
            if publish(shared, 0, message, false, 0)? {
                let _ = shared.events.send(Event::Ack {
                    consumer,
                    message: message.message_id,
                });
            }
            Ok(())
        }
        1 | 2 => {
            let broker = shared.manager.broker().ok_or("broker stopped")?;
            let key = CorrelationKey {
                message_id: message.message_id,
                address: message.address.clone().unwrap_or_default(),
            };
            let correlations = &shared.manager.correlations;
            let mut pending = None;
            let (packet_id, redelivery) = {
                let mut s = lock(&shared.state);
                let existing = correlations.get(&shared.client_id, &key);
                let chosen = match existing {
                    // A redelivery after a reconnect or restart reuses the
                    // packet ID.
                    Some(id) if !s.core_delivery_info_exists(id) => (id, true),
                    _ => {
                        let id = s.generate_packet_id(
                            &broker,
                            correlations,
                            &shared.manager.internal_prefix,
                        )?;
                        pending = correlations.put(&broker, &shared.client_id, key.clone(), id);
                        (id, false)
                    }
                };
                s.put_core_delivery_info(
                    chosen.0,
                    CoreDeliveryInfo {
                        consumer_id: consumer,
                        key,
                    },
                );
                s.send_quota += 1;
                chosen
            };
            let mut wait = Completion::default();
            if let Some(p) = pending {
                wait.push(p);
            }
            publish_after(shared, wait, packet_id, message, redelivery, qos)?;
            Ok(())
        }
        _ => {
            // The client went and its subscription's QoS with it.
            let _ = shared.events.send(Event::Cancel {
                consumer,
                message: message.message_id,
            });
            Ok(())
        }
    }
}

fn publish(
    shared: &Shared,
    packet_id: i32,
    message: &CoreMessage,
    redelivery: bool,
    qos: i32,
) -> Result<bool, String> {
    publish_after(
        shared,
        Completion::default(),
        packet_id,
        message,
        redelivery,
        qos,
    )
}

/// `publishToClient(packetId, message, redelivery, qos)`: false when a
/// QoS 1 or 2 message was too large to send.
fn publish_after(
    shared: &Shared,
    wait: Completion,
    packet_id: i32,
    message: &CoreMessage,
    redelivery: bool,
    qos: i32,
) -> Result<bool, String> {
    let address = message
        .address
        .as_ref()
        .map(saranga_storage::java::simple_string)
        .unwrap_or_default();
    let mut topic = util::mqtt_topic(&address, &shared.wildcard);
    let payload = match message.message_type {
        message_type::TEXT => {
            let mut r = Reader::new(&message.body);
            let text = r
                .nullable_simple_string()
                .map_err(|e| e.to_string())?
                .ok_or("null text body")?;
            saranga_storage::java::simple_string(&text).into_bytes()
        }
        _ => message.body.clone(),
    };
    let has = |k: &str| message.properties.get(&SimpleString::from(k)).is_some();
    let mut is_retain = has(util::MQTT_MESSAGE_RETAIN_INITIAL_DISTRIBUTION_KEY);
    let mut properties = Properties::default();
    if shared.version == Version::V5 {
        let mut s = lock(&shared.state);
        properties = publish_properties(&s, message, &topic)?;
        if !is_retain
            && bool_value(
                message
                    .properties
                    .get(&SimpleString::from(util::MQTT_MESSAGE_RETAIN_KEY)),
            )
        {
            // Artemis looks the subscription up by the topic and fails when
            // it was made with a filter that is not the topic itself; here
            // that subscription just is not retain-as-published.
            if s.subscriptions
                .get(&topic)
                .is_some_and(|i| i.subscription.option.retain_as_published)
            {
                is_retain = true;
            }
        }
        if s.client_topic_alias_maximum.is_some() {
            match s.server_topic_alias(&topic) {
                None => {
                    if let Some(alias) = s.add_server_topic_alias(&topic) {
                        properties.add_int(prop::TOPIC_ALIAS, alias);
                    }
                }
                Some(alias) => {
                    properties.add_int(prop::TOPIC_ALIAS, alias);
                    topic = String::new();
                }
            }
        }
    }
    let max = lock(&shared.state).client_max_packet_size;
    if shared.version == Version::V5 && max != 0 {
        let size = artemis_message_size(&topic, &properties, payload.len());
        if size > i64::from(max) {
            // [MQTT-3.1.2-25] discard it, as if sent.
            tracing::debug!(
                "Not sending message {} to client as its size ({size}) exceeds the max ({max})",
                message.message_id
            );
            if qos == 0 {
                return Ok(true);
            }
            let _ = shared.events.send(Event::Discard { packet_id });
            return Ok(false);
        }
    }
    let dup = qos != 0 && redelivery;
    match codec::publish(
        shared.version,
        dup,
        qos as u8,
        is_retain,
        &topic,
        packet_id,
        &properties,
        &payload,
    ) {
        Some(bytes) => shared.out.send(wait, Some(bytes)),
        None => {
            shared.out.send(wait, None);
            return Err(format!("invalid topicName {topic}"));
        }
    }
    Ok(true)
}

/// `MQTTUtil.calculateMessageSize` of the PUBLISH as Artemis estimates it:
/// a packet ID even at QoS 0, no topic length, and properties counted
/// without their string lengths.
fn artemis_message_size(topic: &str, properties: &Properties, payload: usize) -> i64 {
    let mut props = 0i64;
    if properties.has(prop::PAYLOAD_FORMAT_INDICATOR) {
        props += 2;
    }
    if properties.has(prop::TOPIC_ALIAS) {
        props += 3;
    }
    if properties.has(prop::MESSAGE_EXPIRY_INTERVAL) {
        props += 5;
    }
    for id in properties.subscription_ids() {
        props += 1 + util::variable_byte_integer_size(i64::from(*id));
    }
    for k in [prop::CONTENT_TYPE, prop::RESPONSE_TOPIC] {
        if let Some(s) = properties.string(k) {
            props += 1 + s.len() as i64;
        }
    }
    for (k, v) in properties.user_properties() {
        props += 1 + k.len() as i64 + v.len() as i64;
    }
    if let Some(b) = properties.binary(prop::CORRELATION_DATA) {
        props += 1 + 2 + b.len() as i64;
    }
    props += util::variable_byte_integer_size(props);
    let remaining = 2 + topic.len() as i64 + props + payload as i64;
    1 + util::variable_byte_integer_size(remaining) + remaining
}

/// `getPublishProperties`.
fn publish_properties(
    s: &SessionState,
    message: &CoreMessage,
    topic: &str,
) -> Result<Properties, String> {
    let get = |k: &str| message.properties.get(&SimpleString::from(k));
    let mut props = Properties::default();
    if let Some(v) = get(util::MQTT_PAYLOAD_FORMAT_INDICATOR_KEY) {
        props.add_int(prop::PAYLOAD_FORMAT_INDICATOR, int_value(v).unwrap_or(0));
    }
    if let Some(v) = get(util::MQTT_RESPONSE_TOPIC_KEY).and_then(string_value) {
        props.add(prop::RESPONSE_TOPIC, Value::Str(v));
    }
    if let Some(PropertyValue::Bytes(b)) = get(util::MQTT_CORRELATION_DATA_KEY) {
        props.add(prop::CORRELATION_DATA, Value::Binary(b.clone()));
    }
    if let Some(n) = get(util::MQTT_USER_PROPERTY_EXISTS_KEY).and_then(int_value) {
        let mut ordered: Vec<Option<(String, String)>> = vec![None; n.max(0) as usize];
        for (name, value) in message.properties.entries() {
            let name = saranga_storage::java::simple_string(name);
            let Some(rest) = name.strip_prefix(util::MQTT_USER_PROPERTY_KEY_PREFIX) else {
                continue;
            };
            let (index, key) = rest.split_once('.').unwrap_or((rest, ""));
            let position: usize = index
                .parse()
                .map_err(|_| format!("For input string: \"{index}\""))?;
            let slot = ordered
                .get_mut(position)
                .ok_or_else(|| format!("Index {position} out of bounds for length {n}"))?;
            *slot = Some((key.to_owned(), string_value(value).unwrap_or_default()));
        }
        for (k, v) in ordered.into_iter().flatten() {
            props.add_user(k, v);
        }
    }
    if let Some(v) = get(util::MQTT_CONTENT_TYPE_KEY).and_then(string_value) {
        props.add(prop::CONTENT_TYPE, Value::Str(v));
    }
    for id in s.matching_subscription_identifiers(topic) {
        props.add_int(prop::SUBSCRIPTION_IDENTIFIER, id);
    }
    if message.expiration != 0 {
        // [MQTT-3.3.2-6] what is left of the expiry, to the nearest second.
        let left = ((message.expiration - now_millis()) as f64 / 1000.0).round() as i32;
        props.add_int(prop::MESSAGE_EXPIRY_INTERVAL, left);
    }
    Ok(props)
}

/// `MQTTSession` with its managers: what the client's connection drives
/// while it is connected, and what is kept in its state once it is not.
pub struct Core {
    session: ServerSession,
    state: StateRef,
    manager: Arc<StateManager>,
    version: Version,
    /// `isClean`.
    clean: bool,
    settings: Settings,
    shared: Arc<Shared>,
    /// `MQTTSubscriptionManager.started`.
    subscriptions_started: bool,
    client_id: String,
    /// The session's one producer, made at its first publish
    /// (`MQTTPublishManager.senderName`).
    sender: Option<String>,
}

impl Core {
    fn broker(&self) -> Arc<Broker> {
        self.session.broker().clone()
    }

    fn prefix(&self) -> String {
        self.manager.internal_prefix.clone()
    }

    /// `sendToClient`: once the session's journal writes are done.
    fn send(&mut self, bytes: Vec<u8>) {
        let wait = self.session.take_completion();
        self.shared.out.send(wait, Some(bytes));
    }

    /// Lets the queue updates waiting on journal writes happen.
    fn flush(&mut self) {
        let wait = self.session.take_completion();
        if !wait.is_empty() {
            self.shared.out.send(wait, None);
        }
    }

    fn send_pub_reply(&mut self, kind: u8, packet_id: i32, reason: u8) {
        let bytes = codec::pub_reply(self.version, kind, packet_id, reason);
        self.send(bytes);
    }

    /// `MQTTPublishManager.sendToQueue`: a PUBLISH from the client, or a
    /// will (`internal`), which is not acknowledged and skips the QoS 2
    /// bookkeeping.
    async fn send_to_queue(
        &mut self,
        publish: Publish,
        internal: bool,
    ) -> Result<(), PublishError> {
        let mut topic = publish.topic.clone();
        if self.version == Version::V5
            && let Some(alias) = publish.properties.int(prop::TOPIC_ALIAS)
        {
            if alias == 0 || alias > self.settings.topic_alias_maximum {
                // [MQTT-3.3.2-8], [MQTT-3.3.2-9]
                return Err(PublishError::Disconnect(reason::TOPIC_ALIAS_INVALID));
            }
            let mut s = lock(&self.state);
            match s.client_topic_alias(alias).cloned() {
                None => {
                    if topic.is_empty() {
                        return Err(PublishError::Disconnect(reason::TOPIC_ALIAS_INVALID));
                    }
                    tracing::debug!("Adding new alias {alias} for topic {topic}");
                    s.put_client_topic_alias(alias, topic.clone());
                }
                Some(_) if !topic.is_empty() => {
                    s.put_client_topic_alias(alias, topic.clone());
                }
                Some(existing) => topic = existing,
            }
        }
        let broker = self.broker();
        let wildcard = *broker.post_office().wildcard();
        let core_address = util::core_address(&topic, &wildcard);
        let mut message = create_server_message(&broker, &self.client_id, &core_address, &publish);
        let qos = publish.qos;
        if qos > 0 {
            message.durable = true;
        }
        let tx =
            ((qos == 2 && !internal) || publish.retain).then(|| self.session.new_transaction());
        let result: Result<(), BrokerError> = async {
            let po = broker.post_office();
            let info = po.address_info(&core_address);
            if info.is_none() && po.settings(&core_address).auto_create_addresses {
                self.session.create_address(
                    &core_address,
                    RoutingTypes::of(RoutingType::Multicast),
                    true,
                )?;
                set_routing_type(&mut message, RoutingType::Multicast);
            }
            if let Some(info) = info {
                let t = if info.routing_types.multicast {
                    RoutingType::Multicast
                } else {
                    RoutingType::Anycast
                };
                set_routing_type(&mut message, t);
            }
            let retained = message.clone();
            let sender = self.sender.get_or_insert_with(|| {
                let name = uuid::Uuid::new_v4().to_string();
                self.session.add_producer(&name, "MQTT", None);
                name
            });
            self.session.set_sender(Some(sender.clone()));
            let sent = self.session.send_in(message, None, tx.clone()).await;
            self.session.set_sender(None);
            sent?;
            if qos == 2 && !internal {
                let prefix = self.prefix();
                let mut s = lock(&self.state);
                let tx = tx.as_ref().expect("QoS 2 has a transaction");
                s.publish_cache(&prefix)
                    .add(&self.session, publish.packet_id, tx)?;
            }
            if publish.retain {
                let reset = publish.payload.is_empty();
                self.handle_retained_message(retained, &topic, reset, tx.as_ref())
                    .await?;
            }
            if let Some(tx) = &tx {
                self.session.commit_tx(tx)?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            if let Some(tx) = &tx {
                let _ = self.session.rollback_tx(tx);
            }
            if !is_security(&e) {
                tracing::error!(
                    "AMQ834006: Failed to publish MQTT message. Client ID: {}. Packet ID: {}. Exception message: {}",
                    self.client_id,
                    publish.packet_id,
                    e.message
                );
                return Err(PublishError::Other(e));
            }
            if internal {
                return Err(PublishError::Other(e));
            }
            match self.version {
                Version::V5 => {
                    self.send_message_ack(qos, publish.packet_id, reason::NOT_AUTHORIZED);
                    return Ok(());
                }
                // [MQTT-3.3.5-2] acknowledge or close; closing is the default.
                Version::V3_1_1 if self.settings.close_on_publish_authorization_failure => {
                    return Err(PublishError::Disconnect(reason::UNSPECIFIED_ERROR));
                }
                Version::V3_1_1 => {
                    tracing::debug!("MQTT 3.1.1 client not authorized to publish message.")
                }
                Version::V3_1 => {
                    tracing::debug!("MQTT 3.1 client not authorized to publish message.")
                }
            }
        }
        if !internal {
            self.send_message_ack(qos, publish.packet_id, reason::SUCCESS);
        } else {
            self.flush();
        }
        Ok(())
    }

    fn send_message_ack(&mut self, qos: u8, packet_id: i32, code: u8) {
        match qos {
            1 => self.send_pub_reply(4, packet_id, code),
            2 => self.send_pub_reply(5, packet_id, code),
            _ => self.flush(),
        }
    }

    /// `MQTTRetainMessageManager.handleRetainedMessage`: the topic's
    /// retained message is replaced, or removed by an empty one.
    async fn handle_retained_message(
        &mut self,
        message: CoreMessage,
        topic: &str,
        reset: bool,
        tx: Option<&saranga_server::transaction::TxRef>,
    ) -> Result<(), BrokerError> {
        let broker = self.broker();
        let po = broker.post_office();
        let retain_address = util::core_retain_address(topic, po.wildcard());
        let queue = match po.queue(&retain_address) {
            Some(q) => q,
            None => {
                let mut spec = QueueSpec::new(retain_address.as_str(), retain_address.as_str());
                spec.auto_created = true;
                po.create_queue(spec, false)?
            }
        };
        queue.delete_matching(None).await;
        if !reset {
            let mut copy = message;
            copy.message_id = po.ids().next();
            self.session
                .send_to_queue(copy, None, &queue, tx.cloned())?;
        }
        Ok(())
    }

    /// `addRetainedMessagesToQueue`: the retained messages of every topic
    /// the filter matches go to the new subscription's queue.
    async fn add_retained_messages_to_queue(
        &mut self,
        queue: &Queue,
        filter: &str,
    ) -> Result<(), BrokerError> {
        let broker = self.broker();
        let po = broker.post_office();
        let retain_address = util::core_retain_address(filter, po.wildcard());
        let names = self
            .session
            .binding_query(&retain_address, true)
            .queue_names;
        let tx = self.session.new_transaction();
        let result: Result<(), BrokerError> = async {
            for name in names {
                let Some(retained) = po.queue(&name) else {
                    continue;
                };
                let Some(last) = retained.browse().await.pop() else {
                    continue;
                };
                let mut copy = last.message.delivery_core().clone();
                copy.message_id = po.ids().next();
                copy.properties.insert(
                    util::MQTT_MESSAGE_RETAIN_INITIAL_DISTRIBUTION_KEY.into(),
                    PropertyValue::Null,
                );
                self.session.send_to_queue(
                    copy,
                    last.message.large().cloned(),
                    queue,
                    Some(tx.clone()),
                )?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => self.session.commit_tx(&tx),
            Err(e) => {
                let _ = self.session.rollback_tx(&tx);
                Err(e)
            }
        }
    }

    /// `MQTTSubscriptionManager.start`: the stored subscriptions get their
    /// consumers back, and QoS 2 deliveries waiting for PUBCOMP get their
    /// PUBREL again.
    async fn start_subscriptions(&mut self) -> Result<(), BrokerError> {
        if self.subscriptions_started {
            return Ok(());
        }
        let items: Vec<TopicSubscription> = lock(&self.state)
            .subscriptions
            .values()
            .map(|i| i.subscription.clone())
            .collect();
        for sub in items {
            self.add_subscription(sub, None, true).await?;
        }
        let broker = self.broker();
        let prefix = self.prefix();
        let ids = lock(&self.state).pubrec_cache(&prefix).packet_ids(&broker);
        for id in ids {
            self.send_pub_reply(6, id, reason::SUCCESS);
        }
        self.subscriptions_started = true;
        Ok(())
    }

    /// `addSubscription`.
    async fn add_subscription(
        &mut self,
        sub: TopicSubscription,
        id: Option<i32>,
        initial: bool,
    ) -> Result<(), BrokerError> {
        let filter = sub.filter.clone();
        let qos = i32::from(sub.option.qos);
        let no_local = sub.option.no_local;
        let raw = util::extract_address_name(&filter).to_owned();
        if util::is_shared(&raw) && !raw[util::SHARED_SUBSCRIPTION_PREFIX.len()..].contains('/') {
            return Err(BrokerError::internal(format!(
                "begin 7, end -1, length {}",
                raw.encode_utf16().count()
            )));
        }
        let parsed = util::decompose_shared(&raw).1.to_owned();
        let queue = self
            .queue_for_subscription(&raw, &parsed, util::is_fully_qualified(&filter))
            .await?;
        let result: Result<(), BrokerError> = async {
            if initial {
                let consumer = self.create_consumer(&queue, qos, no_local, None).await?;
                if let Some(item) = lock(&self.state).subscriptions.get_mut(&filter) {
                    item.consumer = Some(consumer);
                }
                return Ok(());
            }
            let existing = lock(&self.state)
                .subscriptions
                .get(&filter)
                .map(|i| (i.consumer, i.subscription.option.no_local));
            match existing {
                None => {
                    let consumer = self.create_consumer(&queue, qos, no_local, None).await?;
                    lock(&self.state).subscriptions.insert(
                        filter.clone(),
                        SubscriptionItem::new(sub.clone(), id, Some(consumer)),
                    );
                }
                Some((consumer, old_no_local)) => {
                    if let Some(c) = consumer {
                        lock(&self.shared.consumer_qos).insert(c, qos);
                        if old_no_local != no_local {
                            self.session.close_consumer(c, false).await;
                            let c = self.create_consumer(&queue, qos, no_local, Some(c)).await?;
                            if let Some(item) = lock(&self.state).subscriptions.get_mut(&filter) {
                                item.consumer = Some(c);
                            }
                        }
                    }
                    if let Some(item) = lock(&self.state).subscriptions.get_mut(&filter) {
                        item.update(sub.clone(), id);
                    }
                }
            }
            let handling = sub.option.retain_handling;
            if handling == 0 || (handling == 1 && existing.is_none()) {
                self.add_retained_messages_to_queue(&queue, &parsed).await?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            // Whatever broke, the subscription's queue goes.
            let _ = self
                .broker()
                .post_office()
                .delete_queue(queue.name(), false, false);
            return Err(e);
        }
        Ok(())
    }

    /// `createQueueForSubscription`.
    async fn queue_for_subscription(
        &mut self,
        raw: &str,
        parsed: &str,
        fully_qualified: bool,
    ) -> Result<Queue, BrokerError> {
        let broker = self.broker();
        let po = broker.post_office();
        let wildcard = *po.wildcard();
        let core_address = util::core_address(parsed, &wildcard);
        let core_queue = util::core_queue(raw, &self.client_id, &wildcard);
        if let Some(q) = po.queue(&core_queue) {
            return Ok(q);
        }
        let bq = self.session.binding_query(&core_address, true);
        if fully_qualified && !bq.auto_create_queues {
            return Err(BrokerError::no_such_queue(&core_address));
        }
        let info = match po.address_info(self.session.remove_prefix(&core_address)) {
            Some(i) => i,
            None => {
                if !bq.auto_create_addresses {
                    return Err(BrokerError::address_does_not_exist(&core_address));
                }
                self.session.create_address(
                    &core_address,
                    RoutingTypes::of(RoutingType::Multicast),
                    true,
                )?;
                po.address_info(self.session.remove_prefix(&core_address))
                    .ok_or_else(|| BrokerError::address_does_not_exist(&core_address))?
            }
        };
        // findOrCreateQueue: 3.x clean sessions get a non-durable queue
        // ([MQTT-3.1.2-6]).
        let durable = self.version == Version::V5 || !self.clean;
        let address: &str = &info.name;
        let filter = self.message_filter(address);
        if info.routing_types.multicast {
            let mut spec = QueueSpec::new(core_queue.as_str(), address);
            spec.routing_type = Some(RoutingType::Multicast);
            spec.filter_string = Some(filter);
            spec.durable = durable;
            return self.session.create_queue(spec);
        }
        if info.routing_types.anycast {
            if !bq.queue_names.is_empty() {
                let mut name = None;
                for q in &bq.queue_names {
                    if name.is_none() || &**q == address {
                        name = Some(q.clone());
                    }
                }
                let name = name.expect("not empty");
                return po
                    .queue(&name)
                    .ok_or_else(|| BrokerError::no_such_queue(&name));
            }
            let mut spec = QueueSpec::new(address, address);
            spec.routing_type = Some(RoutingType::Anycast);
            spec.filter_string = Some(filter);
            spec.durable = durable;
            return match self.session.create_queue(spec) {
                Err(e) if e.kind == ErrorType::QueueExists => po
                    .queue(address)
                    .ok_or_else(|| BrokerError::no_such_queue(address)),
                other => other,
            };
        }
        Err(BrokerError::invalid_routing_type_for_address(
            "MULTICAST",
            address,
            &info.routing_types.describe(),
        ))
    }

    /// `getMessageFilter`: subscriptions starting with a wildcard do not
    /// see `$` topics ([MQTT-4.7.2-1]).
    fn message_filter(&self, address: &str) -> String {
        let w = *self.broker().post_office().wildcard();
        if address.starts_with(w.single_word) || address.starts_with(w.any_words) {
            self.settings.message_filter_no_dollar.clone()
        } else {
            self.settings.message_filter.clone()
        }
    }

    /// `createConsumer`: no flow control; no-local subscriptions skip the
    /// client's own messages by its client ID.
    async fn create_consumer(
        &mut self,
        queue: &Queue,
        qos: i32,
        no_local: bool,
        existing: Option<i64>,
    ) -> Result<i64, BrokerError> {
        let broker = self.broker();
        let cid = existing.unwrap_or_else(|| broker.post_office().ids().next());
        let filter =
            no_local.then(|| format!("{} <> '{}'", util::CONNECTION_ID_PROPERTY, self.client_id));
        let sink = Arc::new(MqttSink {
            shared: self.shared.clone(),
            consumer_id: cid,
            durable_queue: queue.is_durable(),
        });
        self.session
            .create_consumer(cid, queue.name(), filter, 0, false, move |_: &Queue| {
                sink as Arc<dyn DeliverySink>
            })
            .await?;
        self.session.consumer_credits(cid, -1);
        lock(&self.shared.consumer_qos).insert(cid, qos);
        Ok(cid)
    }

    /// `MQTTSubscriptionManager.stop`.
    async fn stop_subscriptions(&mut self) {
        let consumers: Vec<i64> = {
            let mut s = lock(&self.state);
            s.subscriptions
                .values_mut()
                .filter_map(|i| i.consumer.take())
                .collect()
        };
        for c in consumers {
            self.session.close_consumer(c, false).await;
        }
        self.subscriptions_started = false;
    }

    /// `addSubscriptions`: the granted QoS (or failure code) of each.
    async fn add_subscriptions(
        &mut self,
        subscriptions: Vec<Subscription>,
        id: Option<i32>,
    ) -> Result<Vec<u8>, BrokerError> {
        self.start_subscriptions().await?;
        let mut codes = Vec::with_capacity(subscriptions.len());
        for s in subscriptions {
            let qos = s.option.qos;
            let sub = TopicSubscription {
                filter: s.filter,
                option: s.option,
            };
            match self.add_subscription(sub, id, false).await {
                Ok(()) => codes.push(qos),
                Err(e) if is_security(&e) => codes.push(match self.version {
                    Version::V5 => reason::NOT_AUTHORIZED,
                    Version::V3_1_1 => reason::UNSPECIFIED_ERROR,
                    // MQTT 3.1 cannot say no.
                    Version::V3_1 => qos,
                }),
                Err(e) => return Err(e),
            }
        }
        self.store_durable_state()?;
        Ok(codes)
    }

    fn store_durable_state(&mut self) -> Result<(), BrokerError> {
        let state = self.state.clone();
        let s = lock(&state);
        self.manager.store_durable_state(&mut self.session, &s)
    }

    /// `removeSubscriptions`: a reason code for each topic.
    async fn remove_subscriptions(&mut self, topics: &[String], enforce_security: bool) -> Vec<u8> {
        if topics.is_empty() {
            return Vec::new();
        }
        let broker = self.broker();
        let po = broker.post_office();
        let wildcard = *po.wildcard();
        let mut codes = Vec::with_capacity(topics.len());
        for topic in topics {
            let item = lock(&self.state).subscriptions.remove(topic);
            let Some(item) = item else {
                codes.push(reason::NO_SUBSCRIPTION_EXISTED);
                continue;
            };
            if let Some(c) = item.consumer {
                self.session.close_consumer(c, false).await;
                lock(&self.shared.consumer_qos).remove(&c);
            }
            let result: Result<(), BrokerError> = async {
                if util::is_shared(topic)
                    && !topic[util::SHARED_SUBSCRIPTION_PREFIX.len()..].contains('/')
                {
                    return Err(BrokerError::internal("begin 7, end -1"));
                }
                let name = util::core_queue(topic, &self.client_id, &wildcard);
                if let Some(queue) = po.queue(&name) {
                    if queue.config().configuration_managed {
                        queue.delete_matching(None).await;
                    } else if !util::is_shared(topic) || queue.consumer_count() == 0 {
                        self.session.delete_queue_with(&name, enforce_security)?;
                    }
                }
                Ok(())
            }
            .await;
            codes.push(match result {
                Ok(()) => reason::SUCCESS,
                Err(e) => {
                    tracing::error!("AMQ834000: Error removing subscription. {}", e.message);
                    reason::UNSPECIFIED_ERROR
                }
            });
        }
        if lock(&self.state).subscriptions.is_empty() {
            self.manager.remove_durable_state(&self.client_id).await;
        } else if let Err(e) = self.store_durable_state() {
            tracing::warn!("{}", e.message);
        }
        codes
    }

    /// `MQTTSession.clean`: every subscription goes, then the state.
    async fn clean(&mut self, enforce_security: bool) {
        let topics: Vec<String> = lock(&self.state).subscriptions.keys().cloned().collect();
        self.remove_subscriptions(&topics, enforce_security).await;
        let broker = self.broker();
        self.manager.correlations.clear(&broker, &self.client_id);
        lock(&self.state).clear(&broker);
    }

    /// `sendWillMessage`.
    async fn send_will_message(&mut self) {
        let will = {
            let mut s = lock(&self.state);
            if s.will_status != WillStatus::NotSent {
                return;
            }
            s.will_status = WillStatus::Sending;
            s.will.clone()
        };
        let mut properties = Properties::default();
        for (k, v) in will.user_properties.iter().flatten() {
            properties.add_user(k.clone(), v.clone());
        }
        let publish = Publish {
            dup: false,
            qos: will.qos,
            retain: will.retain,
            remaining_length: 0,
            topic: will.topic.unwrap_or_default(),
            packet_id: 0,
            properties,
            payload: will.message.unwrap_or_default(),
        };
        tracing::debug!("{} sending will message", self.client_id);
        match self.send_to_queue(publish, true).await {
            Ok(()) => {
                let mut s = lock(&self.state);
                s.will_status = WillStatus::Sent;
                s.will.message = None;
            }
            Err(e) => {
                lock(&self.state).will_status = WillStatus::NotSent;
                match e {
                    PublishError::Other(e) if is_security(&e) => tracing::error!(
                        "AMQ834007: Authorization failure sending will message: {}",
                        e.message
                    ),
                    PublishError::Other(e) => {
                        tracing::error!("AMQ834003: Error sending will message. {}", e.message)
                    }
                    PublishError::Disconnect(_) => {
                        tracing::error!("AMQ834003: Error sending will message.")
                    }
                }
            }
        }
    }

    /// `MQTTPublishManager.acknowledgeDelivery`: the client confirmed a
    /// QoS 1 or 2 delivery; for QoS 2 the PUBREL follows.
    async fn acknowledge_delivery(&mut self, packet_id: i32, needs_pubrel: bool) {
        let info = lock(&self.state).core_delivery_info(packet_id).cloned();
        let Some(info) = info else {
            self.ack_reply(packet_id, reason::PACKET_IDENTIFIER_NOT_FOUND, needs_pubrel);
            return;
        };
        if !self.session.has_consumer(info.consumer_id) {
            tracing::warn!(
                "AMQ834014: MQTT client {} sent PUBREC for packet {packet_id}, but acknowledgement failed. Internal consumer {} not found. Internal session is {}.",
                self.client_id,
                info.consumer_id,
                if self.session.is_closed() {
                    "closed"
                } else {
                    "not closed"
                }
            );
            self.ack_reply(packet_id, reason::PACKET_IDENTIFIER_NOT_FOUND, needs_pubrel);
            return;
        }
        let tx = self.session.new_transaction();
        let result: Result<(), BrokerError> = async {
            if needs_pubrel {
                let prefix = self.prefix();
                lock(&self.state)
                    .pubrec_cache(&prefix)
                    .add(&self.session, packet_id, &tx)?;
            }
            self.manager
                .correlations
                .remove_in(&self.session, &self.client_id, &info.key, &tx)?;
            self.session
                .acknowledge_in(
                    info.consumer_id,
                    info.key.message_id,
                    true,
                    Some(tx.clone()),
                )
                .await?;
            self.session.commit_tx(&tx)
        }
        .await;
        match result {
            Ok(()) => {
                {
                    let mut s = lock(&self.state);
                    s.remove_core_delivery_info(packet_id);
                    s.send_quota -= 1;
                }
                self.session.prompt_delivery(info.consumer_id);
                self.ack_reply(packet_id, reason::SUCCESS, needs_pubrel);
            }
            Err(e) => {
                let _ = self.session.rollback_tx(&tx);
                tracing::warn!(
                    "AMQ832001: MQTT client {} failed to acknowledge message: {}",
                    self.client_id,
                    e.message
                );
                self.ack_reply(packet_id, reason::PACKET_IDENTIFIER_NOT_FOUND, needs_pubrel);
            }
        }
    }

    fn ack_reply(&mut self, packet_id: i32, code: u8, needs_pubrel: bool) {
        if needs_pubrel {
            self.send_pub_reply(6, packet_id, code);
        } else {
            self.flush();
        }
    }

    /// `MQTTSession.stop`: the session closes; a clean or expiring session
    /// goes, and a failed client's will goes out unless it waits for its
    /// delay. False when the state was removed.
    async fn stop(&mut self, failure: bool) -> bool {
        lock(&self.state).failed = failure;
        self.shared.closed.store(true, Ordering::SeqCst);
        self.stop_subscriptions().await;
        self.session.set_started(false).await;
        self.session.close(failure).await;
        let (expiry, is_will) = {
            let mut s = lock(&self.state);
            s.attached = false;
            s.disconnected_time = now_millis();
            s.clear_topic_aliases();
            s.send_quota = 0;
            s.clear_core_delivery_info();
            (s.client_session_expiry_interval, s.is_will)
        };
        let mut removed = false;
        if self.version == Version::V5 {
            if expiry == 0 {
                // A session that ends now sends its will whatever its delay.
                if is_will && failure {
                    self.send_will_message().await;
                }
                self.clean(false).await;
                self.manager.remove_session_state(&self.client_id);
                removed = true;
            }
        } else {
            if is_will && failure {
                self.send_will_message().await;
            }
            if self.clean || expiry == 0 {
                self.clean(false).await;
                self.manager.remove_session_state(&self.client_id);
                removed = true;
            }
        }
        self.flush();
        !removed
    }
}

/// `sendToQueue`'s failures.
enum PublishError {
    Disconnect(u8),
    Other(BrokerError),
}

fn set_routing_type(message: &mut CoreMessage, t: RoutingType) {
    message.properties.insert(
        saranga_server::message::HDR_ROUTING_TYPE.into(),
        PropertyValue::Byte(t.to_byte()),
    );
}

/// `MQTTUtil.createServerMessage` with its body.
fn create_server_message(
    broker: &Broker,
    client_id: &str,
    address: &str,
    publish: &Publish,
) -> CoreMessage {
    let now = now_millis();
    let mut m = CoreMessage {
        message_id: broker.post_office().ids().next(),
        address: Some(address.into()),
        message_type: message_type::BYTES,
        timestamp: now,
        body: publish.payload.clone(),
        ..CoreMessage::default()
    };
    let p = &mut m.properties;
    let mut put = |k: &str, v: PropertyValue| p.insert(k.into(), v);
    put(
        util::MQTT_MESSAGE_RETAIN_KEY,
        PropertyValue::Bool(publish.retain),
    );
    put(
        util::MQTT_QOS_LEVEL_KEY,
        PropertyValue::Int(i32::from(publish.qos)),
    );
    put(
        util::CONNECTION_ID_PROPERTY,
        PropertyValue::String(client_id.into()),
    );
    let props = &publish.properties;
    if let Some(v) = props.int(prop::PAYLOAD_FORMAT_INDICATOR) {
        put(
            util::MQTT_PAYLOAD_FORMAT_INDICATOR_KEY,
            PropertyValue::Int(v),
        );
    }
    if let Some(v) = props.string(prop::RESPONSE_TOPIC) {
        put(
            util::MQTT_RESPONSE_TOPIC_KEY,
            PropertyValue::String(v.into()),
        );
    }
    if let Some(v) = props.binary(prop::CORRELATION_DATA) {
        put(
            util::MQTT_CORRELATION_DATA_KEY,
            PropertyValue::Bytes(v.to_vec()),
        );
    }
    // [MQTT-3.3.2-18] user properties keep their order in their names.
    let user = props.user_properties();
    if !user.is_empty() {
        put(
            util::MQTT_USER_PROPERTY_EXISTS_KEY,
            PropertyValue::Int(user.len() as i32),
        );
        for (i, (k, v)) in user.iter().enumerate() {
            put(
                &format!("{}{i}.{k}", util::MQTT_USER_PROPERTY_KEY_PREFIX),
                PropertyValue::String(v.as_str().into()),
            );
        }
    }
    if let Some(v) = props.string(prop::CONTENT_TYPE) {
        put(util::MQTT_CONTENT_TYPE_KEY, PropertyValue::String(v.into()));
    }
    if let Some(v) = props.int(prop::MESSAGE_EXPIRY_INTERVAL) {
        // Seconds times 1000 as a Java int, overflow and all.
        m.expiration = now + i64::from(v.wrapping_mul(1000));
    }
    m
}

/// The connection's task state (`MQTTProtocolHandler` and
/// `MQTTConnectionManager`).
struct Conn {
    broker: Arc<Broker>,
    manager: Arc<StateManager>,
    settings: Settings,
    id: u64,
    client: Arc<ClientConnection>,
    remote: String,
    out: Arc<Outbound>,
    events: mpsc::UnboundedSender<Event>,
    control: mpsc::UnboundedSender<Control>,
    decoder: codec::Decoder,
    version: Option<Version>,
    client_id: Option<String>,
    client_id_assigned: bool,
    /// The connection TTL in milliseconds; -1 for none.
    ttl: i64,
    using_server_keep_alive: bool,
    core: Option<Core>,
    /// `MQTTSession.stopped`.
    stopped: bool,
    /// The connection is closing: nothing more is read.
    closing: bool,
}

/// Serves an MQTT connection until it closes.
pub async fn serve(
    broker: Arc<Broker>,
    c: Accepted,
    manager: Arc<StateManager>,
    settings: Settings,
) {
    let remote = c.info.remote_address.clone();
    let client = Arc::new(
        ClientConnection::new(
            Transport::socket("MQTT", "MQTTConnection", c.peer, c.local),
            Arc::new(c.info),
            c.acceptor.prefixes.clone(),
        )
        .with_acceptor(&c.acceptor.name),
    );
    let _registered = broker.registry().add(&client);
    let failed = client.clone();
    let (mut rd, wr) = tokio::io::split(c.stream);
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let writer = tokio::spawn(write_loop(wr, out_rx));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (control_tx, mut control_rx) = mpsc::unbounded_channel();
    // createConnectionEntry: the server's keep-alive until CONNECT says.
    let ttl = match settings.server_keep_alive {
        -1 | 0 => -1,
        k => i64::from(k) * util::KEEP_ALIVE_ADJUSTMENT,
    };
    let mut conn = Conn {
        broker,
        manager,
        settings,
        id: NEXT_CONNECTION.fetch_add(1, Ordering::Relaxed),
        client,
        remote,
        out: Arc::new(Outbound { tx: out_tx }),
        events: events_tx,
        control: control_tx,
        decoder: codec::Decoder::new(),
        version: None,
        client_id: None,
        client_id_assigned: false,
        ttl,
        using_server_keep_alive: false,
        core: None,
        stopped: false,
        closing: false,
    };
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    loop {
        // The connection TTL check (`FailureCheckAndFlushThread`); a TTL of
        // 0 fails a connection idle for one check period.
        let ttl = match conn.ttl {
            t if t < 0 => Duration::from_secs(u64::MAX / 4),
            0 => Duration::from_millis(2000),
            t => Duration::from_millis(t as u64),
        };
        tokio::select! {
            // Management failed the connection: a failure, as if the
            // client had gone.
            () = failed.failed() => {
                conn.disconnect(true).await;
                break;
            }
            Some(event) = events_rx.recv() => conn.on_event(event).await,
            Some(control) = control_rx.recv() => match control {
                Control::TakeOver { done } => {
                    conn.taken_over().await;
                    let _ = done.send(());
                    break;
                }
            },
            read = tokio::time::timeout(ttl, rd.read_buf(&mut buf)), if !conn.closing => match read {
                Err(_) => {
                    tracing::warn!(
                        "AMQ222067: Connection failure has been detected: AMQ229014: Did not receive data from {} within the {}ms connection TTL. The connection will now be closed. [code=CONNECTION_TIMEDOUT]",
                        conn.remote, conn.ttl
                    );
                    conn.disconnect(true).await;
                    break;
                }
                // The client went without DISCONNECT: a failure.
                Ok(Ok(0)) | Ok(Err(_)) => {
                    conn.disconnect(true).await;
                    break;
                }
                Ok(Ok(_)) => conn.process(&mut buf).await,
            },
        }
        if conn.closing {
            break;
        }
    }
    if !conn.stopped {
        conn.disconnect(true).await;
    }
    conn.out.close();
    drop(conn);
    let _ = writer.await;
}

impl Conn {
    fn v5(&self) -> bool {
        self.version == Some(Version::V5)
    }

    fn encode_version(&self) -> Version {
        self.decoder.version.unwrap_or(Version::V3_1_1)
    }

    /// `sendToClient`.
    fn send(&mut self, bytes: Vec<u8>) {
        match &mut self.core {
            Some(core) => core.send(bytes),
            None => self.out.send(Completion::default(), Some(bytes)),
        }
    }

    fn send_disconnect(&mut self, code: u8) {
        let bytes = codec::disconnect(self.encode_version(), code);
        self.send(bytes);
    }

    fn send_connack(&mut self, code: u8, session_present: bool, properties: &Properties) {
        // [MQTT-3.2.2-4], [MQTT-3.2.2-6]
        let present = session_present && code == reason::SUCCESS;
        let bytes = codec::connack(self.encode_version(), present, code, properties);
        self.send(bytes);
    }

    /// Decodes and handles each whole packet.
    async fn process(&mut self, buf: &mut Vec<u8>) {
        while !self.closing {
            let Some(decoded) = self.decoder.decode(buf) else {
                return;
            };
            match decoded {
                Err(e) => {
                    tracing::debug!("Disconnecting client due to message decoding failure. {e}");
                    if self.v5() {
                        self.send_disconnect(reason::MALFORMED_PACKET);
                    }
                    self.disconnect(true).await;
                }
                Ok(Packet::PingReq) => self.send(codec::pingresp()),
                Ok(packet) => self.act(packet).await,
            }
        }
    }

    /// `act`.
    async fn act(&mut self, packet: Packet) {
        let name = packet.name();
        let result = match packet {
            Packet::Connect(c) => {
                self.handle_connect(*c).await;
                Ok(())
            }
            Packet::Disconnect { properties, .. } => {
                if let Some(v) = properties.int(prop::SESSION_EXPIRY_INTERVAL)
                    && let Some(core) = &self.core
                {
                    lock(&core.state).client_session_expiry_interval = v;
                }
                self.disconnect(false).await;
                Ok(())
            }
            // Enhanced authentication is not implemented.
            Packet::Auth { .. } => Ok(()),
            Packet::ConnAck
            | Packet::SubAck
            | Packet::UnsubAck
            | Packet::PingResp
            | Packet::PingReq => {
                self.disconnect(true).await;
                Ok(())
            }
            other => match self.core.is_some() {
                true => self.handle(other).await,
                // The session does not exist yet.
                false => Err(Failure::Error("session is null".into())),
            },
        };
        if let Some(core) = &mut self.core {
            core.flush();
        }
        match result {
            Ok(()) => {}
            Err(Failure::Disconnect(code)) => {
                if self.v5() {
                    self.send_disconnect(code);
                }
                self.disconnect(true).await;
            }
            Err(Failure::Error(e)) => {
                tracing::error!(
                    "AMQ834002: Error processing MQTT packet; client ID: {}; packet: {name}; {e}",
                    self.client_id.as_deref().unwrap_or("null")
                );
                if self.v5() {
                    self.send_disconnect(reason::IMPLEMENTATION_SPECIFIC_ERROR);
                }
                self.disconnect(true).await;
            }
        }
    }

    /// The packets of a connected session.
    async fn handle(&mut self, packet: Packet) -> Handled {
        let max_packet_size = self.settings.maximum_packet_size;
        let core = self.core.as_mut().expect("connected");
        let broker = core.broker();
        let prefix = core.prefix();
        match packet {
            Packet::Publish(p) => {
                if core.version == Version::V5
                    && max_packet_size != -1
                    && 1 + util::variable_byte_integer_size(i64::from(p.remaining_length))
                        + i64::from(p.remaining_length)
                        > i64::from(max_packet_size)
                {
                    return Err(Failure::Disconnect(reason::PACKET_TOO_LARGE));
                }
                if p.qos == 2
                    && lock(&core.state)
                        .publish_cache(&prefix)
                        .contains(&broker, p.packet_id)
                {
                    tracing::warn!(
                        "AMQ834009: Ignoring duplicate MQTT QoS2 PUBLISH; packet ID: {}; client ID: {}.",
                        p.packet_id,
                        core.client_id
                    );
                    core.send_pub_reply(5, p.packet_id, reason::SUCCESS);
                    return Ok(());
                }
                match core.send_to_queue(p, false).await {
                    Ok(()) => Ok(()),
                    Err(PublishError::Disconnect(code)) => Err(Failure::Disconnect(code)),
                    Err(PublishError::Other(e)) => Err(e.into()),
                }
            }
            Packet::PubAck { packet_id, .. } => {
                core.acknowledge_delivery(packet_id, false).await;
                Ok(())
            }
            Packet::PubRec { packet_id, reason } => {
                if reason >= 0x80 {
                    core.acknowledge_delivery(packet_id, false).await;
                } else if lock(&core.state)
                    .pubrec_cache(&prefix)
                    .contains(&broker, packet_id)
                {
                    core.send_pub_reply(6, packet_id, reason::SUCCESS);
                } else {
                    core.acknowledge_delivery(packet_id, true).await;
                }
                Ok(())
            }
            Packet::PubRel { packet_id, .. } => {
                let removed = lock(&core.state)
                    .publish_cache(&prefix)
                    .remove(&broker, packet_id);
                if !removed {
                    tracing::debug!(
                        "MQTT client {} sent PUBREL for packet {packet_id} but no corresponding PUBLISH was found in the cache",
                        core.client_id
                    );
                }
                core.send_pub_reply(7, packet_id, reason::SUCCESS);
                Ok(())
            }
            Packet::PubComp { packet_id, .. } => {
                lock(&core.state)
                    .pubrec_cache(&prefix)
                    .remove(&broker, packet_id);
                Ok(())
            }
            Packet::Subscribe {
                packet_id,
                properties,
                subscriptions,
            } => {
                let id = properties.int(prop::SUBSCRIPTION_IDENTIFIER);
                let codes = core.add_subscriptions(subscriptions, id).await?;
                let bytes = codec::suback(core.version, packet_id, &codes);
                core.send(bytes);
                Ok(())
            }
            Packet::Unsubscribe {
                packet_id,
                properties,
                topics,
            } => {
                let codes = core.remove_subscriptions(&topics, true).await;
                let bytes = codec::unsuback(core.version, packet_id, &properties, &codes);
                core.send(bytes);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn on_event(&mut self, event: Event) {
        let Some(core) = &mut self.core else { return };
        match event {
            Event::Ack { consumer, message } => {
                if let Err(e) = core
                    .session
                    .acknowledge_in(consumer, message, true, None)
                    .await
                {
                    tracing::debug!("{}", e.message);
                }
            }
            Event::Cancel { consumer, message } => {
                core.session.cancel_one(consumer, message, false)
            }
            Event::Discard { packet_id } => core.acknowledge_delivery(packet_id, false).await,
            Event::Disconnect { consumer } => core.session.close_consumer(consumer, false).await,
        }
        core.flush();
    }

    /// `handleConnect`.
    async fn handle_connect(&mut self, connect: codec::Connect) {
        if self.core.is_some() {
            // A second CONNECT: Artemis's link stealing takes the
            // connection's own session over.
            if self.v5() {
                self.send_disconnect(reason::SESSION_TAKEN_OVER);
            }
            self.disconnect(false).await;
            return;
        }
        self.version = Some(connect.version);
        let mut client_id = connect.client_id.clone();
        // validateClientID: [MQTT-3.1.3-6], [MQTT-3.1.3-7], [MQTT-3.1.3-8]
        if client_id.is_empty() {
            if connect.clean_start {
                client_id = uuid::Uuid::new_v4().to_string();
                self.client_id_assigned = true;
            } else {
                self.invalid_client_id().await;
                return;
            }
        }
        self.client_id = Some(client_id.clone());
        self.client.set_client_id(Some(client_id.clone()));
        let password = connect
            .password
            .as_ref()
            .map(|p| String::from_utf8_lossy(p).into_owned());
        let username = connect.username.clone();
        let authenticated = match self.validate_user(username.clone(), password.clone()).await {
            Ok(a) => a,
            Err(()) => return,
        };
        if !self.link_stealing(&client_id).await {
            return;
        }
        self.manager.add_connected_client(
            &client_id,
            ConnectedClient {
                connection: self.id,
                control: self.control.clone(),
            },
        );
        self.calculate_keep_alive(&connect);
        self.connect(connect, client_id, username, password, authenticated)
            .await;
    }

    async fn invalid_client_id(&mut self) {
        let code = if self.v5() {
            reason::CLIENT_IDENTIFIER_NOT_VALID
        } else {
            reason::IDENTIFIER_REJECTED_3
        };
        self.send_connack(code, false, &Properties::default());
        self.disconnect(true).await;
    }

    /// `validateUser`: a refused login gets its CONNACK, then the
    /// connection closes. Login modules may block, so they run off the
    /// task.
    async fn validate_user(
        &mut self,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Authenticated, ()> {
        let Some(store) = self.broker.security().cloned() else {
            return Ok(Authenticated::default());
        };
        let info = self.client.security_info();
        let result = tokio::task::spawn_blocking(move || {
            store.authenticate(username.as_deref(), password.as_deref(), &info)
        })
        .await;
        match result {
            Ok(Ok(a)) => Ok(a),
            _ => {
                let code = if self.v5() {
                    reason::BAD_USER_NAME_OR_PASSWORD
                } else {
                    reason::NOT_AUTHORIZED_3
                };
                self.send_connack(code, false, &Properties::default());
                self.disconnect(true).await;
                Err(())
            }
        }
    }

    /// `handleLinkStealing` ([MQTT-3.1.4-2]): false when this connection is
    /// refused.
    async fn link_stealing(&mut self, client_id: &str) -> bool {
        let Some(existing) = self.manager.connected_client(client_id) else {
            return true;
        };
        if self.settings.allow_link_stealing {
            let (done, wait) = oneshot::channel();
            if existing.control.send(Control::TakeOver { done }).is_ok() {
                let _ = tokio::time::timeout(Duration::from_secs(30), wait).await;
            }
            tracing::debug!(
                "Existing MQTT session closed due to incoming session from {} with the same client ID: {client_id}",
                self.remote
            );
            true
        } else {
            if self.v5() {
                self.send_disconnect(reason::UNSPECIFIED_ERROR);
            }
            tracing::debug!(
                "Incoming MQTT session from {} closed due to existing session with the same client ID: {client_id}",
                self.remote
            );
            // Stopped first, so closing does not touch the other session.
            self.stopped = true;
            self.closing = true;
            false
        }
    }

    /// Another connection took the client ID.
    async fn taken_over(&mut self) {
        if self.v5() {
            self.send_disconnect(reason::SESSION_TAKEN_OVER);
        }
        self.disconnect(false).await;
    }

    /// `calculateKeepAlive`: the client's keep-alive unless the server's
    /// is lower, one and a half times over ([MQTT-3.1.2-22]).
    fn calculate_keep_alive(&mut self, connect: &codec::Connect) {
        let server = self.settings.server_keep_alive;
        let client = i32::from(connect.keep_alive);
        if server == -1 || (client <= server && client != 0) {
            self.ttl = i64::from(client) * util::KEEP_ALIVE_ADJUSTMENT;
        } else {
            self.using_server_keep_alive = true;
        }
    }

    /// `MQTTConnectionManager.connect`.
    async fn connect(
        &mut self,
        connect: codec::Connect,
        client_id: String,
        username: Option<String>,
        password: Option<String>,
        authenticated: Authenticated,
    ) {
        let version = connect.version;
        if version == Version::V5 && connect.properties.has(prop::AUTHENTICATION_METHOD) {
            self.send_connack(
                reason::BAD_AUTHENTICATION_METHOD,
                false,
                &Properties::default(),
            );
            self.disconnect(true).await;
            return;
        }
        // The Netty codec's "clean session" is 5's "clean start" too.
        let clean_start = connect.clean_start;
        let session_present = self.manager.has_session_state(&client_id);
        let state = self.manager.session_state(&client_id);
        let previous = {
            let mut s = lock(&state);
            s.attached = true;
            s.disconnected_time = 0;
            s.failed = false;
            s.session.take()
        };
        drop(previous);
        let mut session = ServerSession::new(
            self.broker.clone(),
            self.client.clone(),
            SessionOptions {
                name: uuid::Uuid::new_v4().to_string(),
                username,
                password,
                xa: false,
                auto_commit_sends: false,
                auto_commit_acks: false,
                pre_acknowledge: false,
                default_address: None,
            },
            authenticated,
        );
        session.set_started(true).await;
        let props = &connect.properties;
        let receive_maximum = if version == Version::V5 {
            props.int(prop::RECEIVE_MAXIMUM).unwrap_or(-1)
        } else {
            -1
        };
        let shared = Arc::new(Shared {
            out: self.out.clone(),
            events: self.events.clone(),
            state: state.clone(),
            manager: self.manager.clone(),
            version,
            client_id: client_id.clone(),
            wildcard: *self.broker.post_office().wildcard(),
            consumer_qos: Mutex::default(),
            receive_maximum,
            default_max_in_flight: self.settings.default_maximum_in_flight,
            closed: AtomicBool::new(false),
        });
        let mut core = Core {
            session,
            state: state.clone(),
            manager: self.manager.clone(),
            version,
            clean: false,
            settings: self.settings.clone(),
            shared,
            subscriptions_started: false,
            client_id: client_id.clone(),
            sender: None,
        };
        if clean_start {
            // [MQTT-3.1.2-6] the previous session goes.
            core.clean(true).await;
            core.clean = true;
        }
        if let Some(will) = &connect.will {
            let mut s = lock(&state);
            s.is_will = true;
            s.will.message = Some(will.message.clone());
            s.will.qos = will.qos;
            s.will.retain = will.retain;
            s.will.topic = will.topic.clone();
            s.will_status = WillStatus::NotSent;
            if version == Version::V5 {
                if let Some(d) = will.properties.int(prop::WILL_DELAY_INTERVAL) {
                    s.will.delay_interval = i64::from(d);
                }
                let user = will.properties.user_properties();
                if !user.is_empty() {
                    s.will.user_properties = Some(user.to_vec());
                }
            }
        }
        let connack_properties = {
            let mut s = lock(&state);
            if version == Version::V5 {
                s.client_session_expiry_interval =
                    props.int(prop::SESSION_EXPIRY_INTERVAL).unwrap_or(0);
                s.client_max_packet_size = props.int(prop::MAXIMUM_PACKET_SIZE).unwrap_or(0);
                s.client_topic_alias_maximum = props.int(prop::TOPIC_ALIAS_MAXIMUM);
                self.connack_properties(&client_id)
            } else {
                s.client_session_expiry_interval = self.settings.default_session_expiry_interval;
                Properties::default()
            }
        };
        let expiry = lock(&state).client_session_expiry_interval;
        if (expiry == -1 || expiry > 0)
            && let Err(e) = core.store_durable_state()
        {
            tracing::warn!("{}", e.message);
        }
        self.core = Some(core);
        self.send_connack(
            reason::SUCCESS,
            session_present && !clean_start,
            &connack_properties,
        );
        // [MQTT-3.2.0-1] the session starts after its CONNACK.
        let core = self.core.as_mut().expect("just set");
        if let Err(e) = core.start_subscriptions().await {
            tracing::error!("AMQ834001: Error disconnecting client. {}", e.message);
            self.disconnect(true).await;
        }
    }

    /// `getConnackProperties`.
    fn connack_properties(&self, client_id: &str) -> Properties {
        let mut p = Properties::default();
        if self.client_id_assigned {
            p.add(
                prop::ASSIGNED_CLIENT_IDENTIFIER,
                Value::Str(client_id.to_owned()),
            );
        }
        if self.settings.topic_alias_maximum != -1 {
            p.add_int(prop::TOPIC_ALIAS_MAXIMUM, self.settings.topic_alias_maximum);
        }
        if self.using_server_keep_alive {
            p.add_int(prop::SERVER_KEEP_ALIVE, self.settings.server_keep_alive);
        }
        if self.settings.maximum_packet_size != -1 {
            p.add_int(prop::MAXIMUM_PACKET_SIZE, self.settings.maximum_packet_size);
        }
        p
    }

    /// `MQTTConnectionManager.disconnect`: the session stops, the
    /// connection closes, and the client is no longer connected.
    async fn disconnect(&mut self, failure: bool) {
        self.closing = true;
        if self.stopped {
            return;
        }
        self.stopped = true;
        if let Some(mut core) = self.core.take() {
            let state = core.state.clone();
            let kept = core.stop(failure).await;
            if kept {
                // The state keeps the session for a delayed will and expiry.
                lock(&state).session = Some(Box::new(core));
            }
        }
        if let Some(id) = &self.client_id {
            self.manager.remove_connected_client(id, self.id);
        }
    }
}

/// The scan found a disconnected client's will delay over.
pub async fn send_detached_will(state: &StateRef) {
    let core = lock(state).session.take();
    let Some(mut core) = core else {
        return;
    };
    core.send_will_message().await;
    state::wait(core.session.take_completion()).await;
    let mut s = lock(state);
    if s.session.is_none() && !s.attached {
        s.session = Some(core);
    }
}

/// The scan found a disconnected client's session expired: its will goes
/// if it failed, and its subscriptions go.
pub async fn expire_detached(manager: &StateManager, state: &StateRef) {
    let (core, send_will, client_id) = {
        let mut s = lock(state);
        (
            s.session.take(),
            s.is_will && !s.attached && s.failed,
            s.client_id().to_owned(),
        )
    };
    let _ = manager;
    let Some(mut core) = core else {
        tracing::error!(
            "AMQ834008: Failed to remove session state for client with ID: {client_id}"
        );
        return;
    };
    if send_will {
        core.send_will_message().await;
    }
    core.clean(false).await;
    state::wait(core.session.take_completion()).await;
}
