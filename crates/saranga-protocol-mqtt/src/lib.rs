//! MQTT 3.1, 3.1.1 and 5 (`artemis-mqtt-protocol`).

pub mod codec;
pub mod connection;
pub mod state;
pub mod util;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use saranga_server::protocol::{Connection, ProtocolManager, ServeFuture};
use saranga_server::{Acceptor, Broker};

use crate::state::StateManager;

/// The broker settings MQTT reads (`Configuration`).
#[derive(Debug, Clone)]
pub struct MqttConfig {
    /// `mqtt-session-scan-interval`, in milliseconds.
    pub session_scan_interval: i64,
    /// `mqtt-subscription-persistence-enabled`.
    pub subscription_persistence_enabled: bool,
    /// `internal-naming-prefix`.
    pub internal_naming_prefix: String,
    /// `management-address`.
    pub management_address: String,
    /// `management-notification-address`.
    pub management_notification_address: String,
    /// Whether an acceptor serves MQTT: Artemis makes the session store
    /// only when one does.
    pub served: bool,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            session_scan_interval: 5000,
            subscription_persistence_enabled: true,
            internal_naming_prefix: "$.artemis.internal.".to_owned(),
            management_address: "activemq.management".to_owned(),
            management_notification_address: "activemq.notifications".to_owned(),
            served: true,
        }
    }
}

/// The settings an acceptor's URI gives MQTT (`MQTTProtocolManager`'s bean
/// properties).
#[derive(Debug, Clone)]
pub struct Settings {
    /// `defaultMqttSessionExpiryInterval`: for 3.x clients; -1 for never.
    pub default_session_expiry_interval: i32,
    /// `topicAliasMaximum`; -1 to say nothing.
    pub topic_alias_maximum: i32,
    /// `receiveMaximum`.
    pub receive_maximum: i32,
    /// `serverKeepAlive`, in seconds; -1 or 0 to use the client's.
    pub server_keep_alive: i32,
    /// `maximumPacketSize`; -1 for none.
    pub maximum_packet_size: i32,
    /// `closeMqttConnectionOnPublishAuthorizationFailure`.
    pub close_on_publish_authorization_failure: bool,
    /// `allowLinkStealing`.
    pub allow_link_stealing: bool,
    /// `defaultMaximumInFlightPublishMessages`.
    pub default_maximum_in_flight: i32,
    /// The subscription queues' filter: no management messages.
    pub message_filter: String,
    /// The same for filters starting with a wildcard: no `$` topics either
    /// ([MQTT-4.7.2-1]).
    pub message_filter_no_dollar: String,
}

impl Settings {
    pub fn from(acceptor: &Acceptor, config: &MqttConfig) -> Self {
        let p = |k: &str| acceptor.params.get(k).map(|v| v.trim());
        let int =
            |k: &str, default: i32| p(k).and_then(|v| v.parse::<i32>().ok()).unwrap_or(default);
        let flag = |k: &str, default: bool| {
            p(k).map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(default)
        };
        // getBaseFilter
        let base = format!(
            "NOT ((AMQAddress = '{}') OR (AMQAddress = '{}')",
            config.management_address, config.management_notification_address
        );
        Self {
            default_session_expiry_interval: int("defaultMqttSessionExpiryInterval", -1),
            topic_alias_maximum: int("topicAliasMaximum", util::DEFAULT_TOPIC_ALIAS_MAX),
            receive_maximum: int("receiveMaximum", util::DEFAULT_RECEIVE_MAXIMUM),
            server_keep_alive: int("serverKeepAlive", util::DEFAULT_SERVER_KEEP_ALIVE),
            maximum_packet_size: int("maximumPacketSize", util::DEFAULT_MAXIMUM_PACKET_SIZE),
            close_on_publish_authorization_failure: flag(
                "closeMqttConnectionOnPublishAuthorizationFailure",
                true,
            ),
            allow_link_stealing: flag("allowLinkStealing", true),
            default_maximum_in_flight: int(
                "defaultMaximumInFlightPublishMessages",
                util::DEFAULT_MAXIMUM_IN_FLIGHT_PUBLISH_MESSAGES,
            ),
            message_filter: format!("{base})"),
            message_filter_no_dollar: format!("{base} OR (AMQAddress LIKE '$%'))"),
        }
    }
}

/// The MQTT protocol (`MQTTProtocolManagerFactory` and
/// `MQTTProtocolManager`).
pub struct MqttProtocol {
    config: MqttConfig,
    state: OnceLock<Arc<StateManager>>,
}

impl MqttProtocol {
    pub fn new(config: MqttConfig) -> Self {
        Self {
            config,
            state: OnceLock::new(),
        }
    }
}

impl Default for MqttProtocol {
    fn default() -> Self {
        Self::new(MqttConfig::default())
    }
}

impl ProtocolManager for MqttProtocol {
    fn name(&self) -> &'static str {
        "MQTT"
    }

    /// `isProtocol`: a CONNECT with no flags, a valid remaining length, and
    /// a protocol name of `MQ...` when its length is 4 or 6.
    fn is_protocol(&self, header: &[u8]) -> bool {
        let mut bytes = header.iter().copied();
        let mut next = || bytes.next();
        if next() != Some(16) {
            return false;
        }
        let mut valid_length = false;
        for _ in 0..4 {
            match next() {
                Some(b) if b & 0x80 == 0 => {
                    valid_length = true;
                    break;
                }
                Some(_) => {}
                None => return false,
            }
        }
        if !valid_length || next() != Some(0) {
            return false;
        }
        match next() {
            Some(4 | 6) => next() == Some(b'M') && next() == Some(b'Q'),
            Some(_) => true,
            None => false,
        }
    }

    fn serve(&self, broker: Arc<Broker>, connection: Connection) -> ServeFuture {
        let settings = Settings::from(&connection.acceptor, &self.config);
        let state = self.state.get().cloned();
        Box::pin(async move {
            match state {
                Some(state) => connection::serve(broker, connection, state, settings).await,
                None => tracing::error!("MQTT connection before the broker started"),
            }
        })
    }

    /// `loadProtocolServices` and the protocol manager's construction: the
    /// packet-ID records are reloaded, the session store loaded, and the
    /// session scan started.
    fn start(&self, broker: &Arc<Broker>) -> ServeFuture {
        let manager = Arc::new(StateManager::new(
            broker,
            self.config.subscription_persistence_enabled,
            self.config.internal_naming_prefix.clone(),
        ));
        for (_, data) in broker.take_records(state::MQTT_PACKET_ID_CORRELATION) {
            if let Err(e) = manager.correlations.reload(&data) {
                tracing::warn!("invalid MQTT packet ID record: {e}");
            }
        }
        if self.state.set(manager.clone()).is_err() {
            return Box::pin(async {});
        }
        let served = self.config.served;
        let period = Duration::from_millis(self.config.session_scan_interval.max(1) as u64);
        Box::pin(async move {
            if served {
                manager.init().await;
            }
            // MQTTPeriodicTasks: it stops with the broker.
            let weak = Arc::downgrade(&manager);
            drop(manager);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(period).await;
                    let Some(m) = weak.upgrade() else { break };
                    if m.broker().is_none() {
                        break;
                    }
                    m.scan_sessions().await;
                }
            });
        })
    }
}
