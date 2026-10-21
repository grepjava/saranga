//! Rust ports of the plugins Artemis ships, by the class names
//! `<broker-plugin>` gives them:
//!
//! - `LoggingActiveMQServerPlugin`: logs connections, sessions, consumers,
//!   sends, deliveries, acknowledgements and more (`LOG_ALL_EVENTS`,
//!   `LOG_CONNECTION_EVENTS`, ...), with its `AMQ841xxx` messages;
//! - `NotificationActiveMQServerPlugin`: sends `CONNECTION_CREATED`,
//!   `CONNECTION_DESTROYED`, `ADDRESS_ADDED`, `ADDRESS_REMOVED`,
//!   `MESSAGE_DELIVERED` and `MESSAGE_EXPIRED` notifications;
//! - `BrokerMessageAuthorizationPlugin`: a message with a `requiredRole`
//!   (`ROLE_PROPERTY`) goes only to consumers whose user has that role;
//! - `ConnectionPeriodicExpiryPlugin`: closes the connections of matching
//!   acceptors once they are `periodSeconds` old.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use saranga_buffer::PropertyValue;

use super::{BrokerPlugin, RoutingStatus};
use crate::Broker;
use crate::message::{MessageRef, ServerMessage};
use crate::notifications::{self, NotificationType, hdr};
use crate::postoffice::AddressInfo;
use crate::queue::{AckReason, Queue};
use crate::registry::{ConsumerEntry, SessionEntry};
use crate::session::ClientConnection;
use crate::transaction::Transaction;

const PACKAGE: &str = "org.apache.activemq.artemis.core.server.plugin.impl";

/// The plugin a class name names, made with its properties; `None` for a
/// class Saranga has no port of.
pub fn create(
    class_name: &str,
    properties: &HashMap<String, String>,
) -> Option<Result<Arc<dyn BrokerPlugin>, String>> {
    let simple = class_name
        .strip_prefix(PACKAGE)
        .and_then(|s| s.strip_prefix('.'))?;
    Some(match simple {
        "LoggingActiveMQServerPlugin" => {
            Ok(Arc::new(Logging::new(properties)) as Arc<dyn BrokerPlugin>)
        }
        "NotificationActiveMQServerPlugin" => Ok(Arc::new(Notification::new(properties)) as _),
        "BrokerMessageAuthorizationPlugin" => {
            Ok(Arc::new(MessageAuthorization::new(properties)) as _)
        }
        "ConnectionPeriodicExpiryPlugin" => {
            ConnectionPeriodicExpiry::new(properties).map(|p| Arc::new(p) as _)
        }
        _ => return None,
    })
}

/// `Boolean.parseBoolean(properties.getOrDefault(key, "false"))`.
fn flag(properties: &HashMap<String, String>, key: &str) -> bool {
    properties
        .get(key)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

// ---------------------------------------------------------------------------

/// `LoggingActiveMQServerPlugin`.
pub struct Logging {
    all: bool,
    connections: bool,
    sessions: bool,
    consumers: bool,
    delivering: bool,
    sending: bool,
    internal: bool,
    broker: Mutex<Weak<Broker>>,
}

impl Logging {
    pub fn new(properties: &HashMap<String, String>) -> Self {
        let p = |k| flag(properties, k);
        let plugin = Self {
            all: p("LOG_ALL_EVENTS"),
            connections: p("LOG_CONNECTION_EVENTS"),
            sessions: p("LOG_SESSION_EVENTS"),
            consumers: p("LOG_CONSUMER_EVENTS"),
            delivering: p("LOG_DELIVERING_EVENTS"),
            sending: p("LOG_SENDING_EVENTS"),
            internal: p("LOG_INTERNAL_EVENTS"),
            broker: Mutex::new(Weak::new()),
        };
        tracing::debug!(
            "LoggingPlugin logAll={} logConnectionEvents={} logSessionEvents={} logConsumerEvents={} logSendingEvents={} logDeliveringEvents={} logInternalEvents={}",
            plugin.all,
            plugin.connections,
            plugin.sessions,
            plugin.consumers,
            plugin.sending,
            plugin.delivering,
            plugin.internal
        );
        plugin
    }

    fn node_id(&self) -> String {
        self.broker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()
            .map_or_else(|| "null".to_owned(), |b| b.node_id().to_owned())
    }

    fn queue_string(&self, queue: &Queue) -> String {
        match self
            .broker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()
        {
            Some(b) => crate::audit::queue_display(&b, queue),
            None => format!("QueueImpl[name={}]", queue.name()),
        }
    }
}

impl BrokerPlugin for Logging {
    fn registered(&self, broker: &Arc<Broker>) {
        *self.broker.lock().unwrap_or_else(|e| e.into_inner()) = Arc::downgrade(broker);
    }

    fn after_create_connection(&self, c: &ClientConnection) {
        if self.all || self.connections {
            tracing::info!(
                "AMQ841000: created connection: {}",
                crate::audit::connection_string(c, &self.node_id())
            );
        }
    }

    fn after_destroy_connection(&self, c: &ClientConnection) {
        if self.all || self.connections {
            tracing::info!(
                "AMQ841001: destroyed connection: {}",
                crate::audit::connection_string(c, &self.node_id())
            );
        }
    }

    fn after_create_session(&self, s: &SessionEntry) {
        if self.all || self.sessions {
            let c = s.connection.upgrade();
            tracing::info!(
                "AMQ841002: created session name: {}, session connectionID: {}, remote address {}",
                s.name,
                c.as_ref().map_or("null", |c| c.id()),
                c.as_ref().map_or("null", |c| c.remote_address())
            );
        }
    }

    fn after_close_session(&self, s: &SessionEntry, failed: bool) {
        if self.all || self.sessions {
            let c = s.connection.upgrade();
            tracing::info!(
                "AMQ841003: closed session with session name: {}, failed: {failed}, RemoteAddress: {}",
                s.name,
                c.as_ref().map_or("null", |c| c.remote_address())
            );
        }
    }

    fn after_session_metadata_added(&self, session: &str, key: &str, value: &str) {
        if self.all || self.sessions {
            tracing::info!(
                "AMQ841004: added session metadata for session name : {session}, key: {key}, data: {value}"
            );
        }
    }

    fn before_create_consumer(
        &self,
        id: i64,
        queue: &str,
        filter: Option<&str>,
        browse_only: bool,
    ) -> Result<(), crate::error::BrokerError> {
        if self.all || self.consumers {
            tracing::debug!(
                "AMQ843004: beforeCreateConsumer called with ConsumerID: {id}, QueueBinding: {queue}, filterString: {}, browseOnly: {browse_only}, supportLargeMessage: true",
                filter.unwrap_or("null")
            );
        }
        Ok(())
    }

    fn after_create_consumer(&self, c: &ConsumerEntry) {
        if self.all || self.consumers {
            tracing::info!(
                "AMQ841005: created consumer with ID: {}, with session name: {}",
                c.id,
                c.session
            );
        }
    }

    fn after_close_consumer(&self, c: &ConsumerEntry, failed: bool) {
        if self.all || self.consumers {
            tracing::info!(
                "AMQ841006: closed consumer ID: {}, with  consumer Session: {}, failed: {failed}",
                c.id,
                c.session
            );
        }
    }

    fn after_create_queue(&self, queue: &Queue) {
        if self.all || self.internal {
            tracing::info!("AMQ841007: created queue: {}", self.queue_string(queue));
        }
    }

    fn after_deploy_bridge(&self, bridge: &crate::bridge::Bridge) {
        if self.all || self.internal {
            tracing::info!("AMQ841015: deployed bridge: {}", bridge.describe());
        }
    }

    fn after_destroy_queue(&self, queue: &Queue, check_consumers: bool, auto_delete_address: bool) {
        if self.all || self.internal {
            // The session that destroyed it is not known.
            tracing::info!(
                "AMQ841008: destroyed queue: {}, with args address: {}, session: null, checkConsumerCount: {check_consumers}, removeConsumers: false, autoDeleteAddress: {auto_delete_address}",
                self.queue_string(queue),
                queue.config().address
            );
        }
    }

    fn after_send(
        &self,
        _session: &SessionEntry,
        message: &ServerMessage,
        result: RoutingStatus,
        tx: Option<&Transaction>,
    ) {
        if self.all || self.delivering {
            tracing::info!(
                "AMQ841009: sent message with ID: {}, result: {}, transaction: {}",
                message.message_id(),
                result.name(),
                tx.map_or_else(
                    || "UNAVAILABLE".to_owned(),
                    crate::audit::transaction_string
                )
            );
        }
    }

    fn on_send_exception(
        &self,
        session: &SessionEntry,
        message: &saranga_message::CoreMessage,
        error: &crate::error::BrokerError,
    ) {
        if self.all || self.sending {
            let c = session.connection.upgrade();
            tracing::info!(
                "AMQ841017: error sending message with ID: {}, session name: {}, session connectionID: {}, exception: {}",
                message.message_id,
                session.name,
                c.as_ref().map_or("null", |c| c.id()),
                error
            );
        }
    }

    fn after_message_route(&self, message: &ServerMessage, result: RoutingStatus) {
        if self.all || self.sending {
            tracing::info!(
                "AMQ841010: routed message with ID: {}, result: {}",
                message.message_id(),
                result.name()
            );
        }
    }

    fn on_message_route_exception(
        &self,
        message: &ServerMessage,
        error: &crate::error::BrokerError,
    ) {
        if self.all || self.sending {
            tracing::info!(
                "AMQ841018: error routing message with ID: {}, exception: {error}",
                message.message_id()
            );
        }
    }

    fn after_deliver(&self, c: &ConsumerEntry, message: &ServerMessage) {
        if self.all || self.delivering {
            tracing::info!(
                "AMQ841012: delivered message with message ID: {}, to consumer on address: {}, queue: {}, consumer sessionID: {}, consumerID: {}",
                message.message_id(),
                c.queue.config().address,
                c.queue.name(),
                c.session,
                c.id
            );
        }
    }

    fn message_expired(
        &self,
        reference: &MessageRef,
        _queue: &Queue,
        expiry_address: Option<&str>,
    ) {
        if self.all || self.internal {
            tracing::info!(
                "AMQ841013: expired message: {}, messageExpiryAddress: {}",
                crate::audit::reference_string(reference),
                expiry_address.unwrap_or("null")
            );
        }
    }

    fn message_acknowledged(&self, reference: &MessageRef, _queue: &Queue, _reason: AckReason) {
        if self.all || self.delivering {
            // The transaction is not known where the reference leaves its
            // queue.
            tracing::info!(
                "AMQ841014: acknowledged message: {}, with transaction: null",
                crate::audit::reference_string(reference)
            );
        }
    }

    fn critical_failure(&self, component: &str) {
        if self.all || self.internal {
            tracing::info!("AMQ841016: criticalFailure called with criticalComponent: {component}");
        }
    }
}

// ---------------------------------------------------------------------------

/// `NotificationActiveMQServerPlugin`.
pub struct Notification {
    connections: bool,
    addresses: bool,
    delivered: bool,
    expired: bool,
    broker: Mutex<Weak<Broker>>,
}

impl Notification {
    pub fn new(properties: &HashMap<String, String>) -> Self {
        Self {
            connections: flag(properties, "SEND_CONNECTION_NOTIFICATIONS"),
            addresses: flag(properties, "SEND_ADDRESS_NOTIFICATIONS"),
            delivered: flag(properties, "SEND_DELIVERED_NOTIFICATIONS"),
            expired: flag(properties, "SEND_EXPIRED_NOTIFICATIONS"),
            broker: Mutex::new(Weak::new()),
        }
    }

    fn send(&self, kind: NotificationType, props: Vec<(&'static str, PropertyValue)>) {
        if let Some(b) = self
            .broker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()
        {
            b.post_office().notify(kind, |_| props);
        }
    }

    fn connection(&self, c: &ClientConnection, kind: NotificationType) {
        if !self.connections {
            return;
        }
        let dn = c.security_info().cert_subject_dn().to_owned();
        self.send(
            kind,
            vec![
                (hdr::CONNECTION_NAME, notifications::string(Some(c.id()))),
                (hdr::CERT_SUBJECT_DN, notifications::string(Some(&dn))),
                (
                    hdr::REMOTE_ADDRESS,
                    notifications::string(Some(c.remote_address())),
                ),
            ],
        );
    }

    fn address(&self, info: &AddressInfo, kind: NotificationType) {
        if !self.addresses {
            return;
        }
        self.send(
            kind,
            vec![
                (hdr::ADDRESS, notifications::string(Some(&info.name))),
                (
                    hdr::ROUTING_TYPE,
                    PropertyValue::Byte(routing_type_byte(info)),
                ),
            ],
        );
    }

    fn notification_address(&self) -> Option<String> {
        let b = self
            .broker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()?;
        Some(b.management().config().notification_address.clone())
    }
}

/// `AddressInfo.getRoutingType().getType()`: MULTICAST 0, ANYCAST 1.
fn routing_type_byte(info: &AddressInfo) -> i8 {
    if info.routing_types.contains(crate::RoutingType::Multicast) {
        0
    } else {
        1
    }
}

impl BrokerPlugin for Notification {
    fn registered(&self, broker: &Arc<Broker>) {
        *self.broker.lock().unwrap_or_else(|e| e.into_inner()) = Arc::downgrade(broker);
    }

    fn unregistered(&self, _broker: &Broker) {
        *self.broker.lock().unwrap_or_else(|e| e.into_inner()) = Weak::new();
    }

    fn after_create_connection(&self, c: &ClientConnection) {
        self.connection(c, NotificationType::ConnectionCreated);
    }

    fn after_destroy_connection(&self, c: &ClientConnection) {
        self.connection(c, NotificationType::ConnectionDestroyed);
    }

    fn after_add_address(&self, info: &AddressInfo, _reload: bool) {
        self.address(info, NotificationType::AddressAdded);
    }

    fn after_remove_address(&self, _address: &str, info: Option<&AddressInfo>) {
        if let Some(info) = info {
            self.address(info, NotificationType::AddressRemoved);
        }
    }

    fn after_deliver(&self, c: &ConsumerEntry, message: &ServerMessage) {
        if !self.delivered {
            return;
        }
        let address = c.queue.config().address.to_string();
        if Some(&address) == self.notification_address().as_ref() {
            return;
        }
        self.send(
            NotificationType::MessageDelivered,
            vec![
                (hdr::ADDRESS, notifications::string(Some(&address))),
                (
                    hdr::ROUTING_TYPE,
                    PropertyValue::Byte(c.queue.config().routing_type.to_byte()),
                ),
                (
                    hdr::ROUTING_NAME,
                    notifications::string(Some(c.queue.name())),
                ),
                (hdr::CONSUMER_NAME, PropertyValue::Long(c.id)),
                (hdr::MESSAGE_ID, PropertyValue::Long(message.message_id())),
            ],
        );
    }

    fn message_expired(
        &self,
        reference: &MessageRef,
        queue: &Queue,
        _expiry_address: Option<&str>,
    ) {
        if !self.expired {
            return;
        }
        let config = queue.config();
        let address = config.address.to_string();
        if Some(&address) == self.notification_address().as_ref() {
            return;
        }
        // Expired by the reaper, not a consumer: no _AMQ_ConsumerName.
        let props = vec![
            (hdr::ADDRESS, notifications::string(Some(&address))),
            (
                hdr::ROUTING_TYPE,
                PropertyValue::Byte(config.routing_type.to_byte()),
            ),
            (hdr::ROUTING_NAME, notifications::string(Some(queue.name()))),
            (hdr::MESSAGE_ID, PropertyValue::Long(reference.message_id())),
        ];
        self.send(NotificationType::MessageExpired, props);
    }
}

// ---------------------------------------------------------------------------

/// `BrokerMessageAuthorizationPlugin`.
pub struct MessageAuthorization {
    role_property: String,
    broker: Mutex<Weak<Broker>>,
}

impl MessageAuthorization {
    pub fn new(properties: &HashMap<String, String>) -> Self {
        Self {
            role_property: properties
                .get("ROLE_PROPERTY")
                .cloned()
                .unwrap_or_else(|| "requiredRole".to_owned()),
            broker: Mutex::new(Weak::new()),
        }
    }
}

impl BrokerPlugin for MessageAuthorization {
    fn registered(&self, broker: &Arc<Broker>) {
        *self.broker.lock().unwrap_or_else(|e| e.into_inner()) = Arc::downgrade(broker);
    }

    fn unregistered(&self, _broker: &Broker) {
        *self.broker.lock().unwrap_or_else(|e| e.into_inner()) = Weak::new();
    }

    fn can_accept(&self, consumer: &ConsumerEntry, message: &ServerMessage) -> bool {
        let Some(required) = crate::message::string_property(message.core(), &self.role_property)
        else {
            return true;
        };
        // The subject of the consumer's session (getSessionSubject).
        let subject = self
            .broker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()
            .and_then(|b| b.registry().session(&consumer.session))
            .and_then(|s| s.connection.upgrade())
            .and_then(|c| c.subject());
        let Some(subject) = subject else {
            tracing::debug!("Subject not found for consumer: {}", consumer.id);
            return false;
        };
        let permitted = subject.roles().any(|r| r == required);
        if !permitted {
            tracing::debug!(
                "Message consumer: {} does not have required role `{required}` needed to receive message: {}",
                consumer.id,
                message.message_id()
            );
        }
        permitted
    }
}

// ---------------------------------------------------------------------------

/// `RandomUtil.randomMax`: `Math.abs` of a random `int`, modulo `max`, 0
/// taken as `max`. `Math.abs(Integer.MIN_VALUE)` stays negative, which
/// the scheduler runs at once.
fn random_max(max: u64) -> u64 {
    let b = uuid::Uuid::new_v4();
    let b = b.as_bytes();
    let value = i32::from_be_bytes([b[0], b[1], b[2], b[3]]).wrapping_abs() % max as i32;
    match value {
        0 => max,
        v if v < 0 => 0,
        v => v as u64,
    }
}

/// `ConnectionPeriodicExpiryPlugin`.
pub struct ConnectionPeriodicExpiry {
    /// `TimeUnit.SECONDS.toMillis(periodSeconds)`.
    period_ms: i64,
    accuracy_window_seconds: u64,
    acceptor_match: regex::Regex,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl ConnectionPeriodicExpiry {
    /// `init`: `Long.parseLong` of `periodSeconds` and `Integer.parseInt`
    /// of `accuracyWindowSeconds`, then `sanityCheckConfig`.
    pub fn new(properties: &HashMap<String, String>) -> Result<Self, String> {
        let number_format = |v: &str| format!("For input string: \"{v}\"");
        let period_seconds: i64 = match properties.get("periodSeconds") {
            Some(v) => saranga_filter::java_parse_int(v).ok_or_else(|| number_format(v))?,
            None => 15 * 60,
        };
        let accuracy: i32 = match properties.get("accuracyWindowSeconds") {
            Some(v) => saranga_filter::java_parse_int(v).ok_or_else(|| number_format(v))?,
            None => 30,
        };
        // sanityCheckConfig
        if accuracy <= 0 {
            return Err("accuracyWindowSeconds must be > 0".to_owned());
        }
        let Some(pattern) = properties.get("acceptorMatchRegex") else {
            return Err("acceptorMatchRegex must be configured".to_owned());
        };
        let acceptor_match = regex::Regex::new(&format!("^(?:{pattern})$"))
            .map_err(|e| format!("acceptorMatchRegex: {e}"))?;
        Ok(Self {
            // `TimeUnit.SECONDS.toMillis`, which saturates.
            period_ms: period_seconds.saturating_mul(1000),
            accuracy_window_seconds: accuracy as u64,
            acceptor_match,
            task: Mutex::new(None),
        })
    }
}

impl BrokerPlugin for ConnectionPeriodicExpiry {
    fn registered(&self, broker: &Arc<Broker>) {
        let weak = Arc::downgrade(broker);
        let window = std::time::Duration::from_secs(self.accuracy_window_seconds);
        let period_ms = self.period_ms;
        let pattern = self.acceptor_match.clone();
        let accuracy = self.accuracy_window_seconds;
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(window).await;
                let Some(broker) = weak.upgrade() else { return };
                let now = crate::message::now_millis();
                for c in broker.registry().connections() {
                    // `currentTime > creationTime + periodMillis`, the sum
                    // wrapping as Java's does.
                    if !c.acceptor().is_some_and(|a| pattern.is_match(a))
                        || now <= c.created().wrapping_add(period_ms)
                    {
                        continue;
                    }
                    // `RandomUtil.randomMax(accuracyWindowSeconds)` seconds,
                    // 1 to the window, so the clients do not all come back
                    // at once.
                    let delay = std::time::Duration::from_secs(random_max(accuracy));
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        tracing::debug!(
                            "connection {} terminated by session expiry plugin",
                            c.id()
                        );
                        c.fail();
                    });
                }
            }
        });
        *self.task.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
    }

    fn unregistered(&self, _broker: &Broker) {
        if let Some(t) = self.task.lock().unwrap_or_else(|e| e.into_inner()).take() {
            t.abort();
        }
    }
}
