//! AMQP 1.0 (`artemis-amqp-protocol`, and the parts of Proton-J it relies
//! on).
//!
//! - [`types`] and [`codec`]: the type system and its encoding, choosing
//!   the same constructors as Proton-J.
//! - [`composite`]: described list types.
//! - [`transport`], [`messaging`], [`transaction`] and [`sasl`]: the
//!   specification's types.
//! - [`frame`]: frames and protocol headers.
//! - [`message`]: AMQP messages kept in their own encoding, and
//!   [`native`], how the broker holds them.
//! - [`convert`]: conversion to and from Core.

pub mod bridge;
pub mod broker_connection;
pub mod codec;
pub mod composite;
pub mod connection;
pub mod convert;
pub mod describe;
pub mod federation;
pub mod frame;
pub mod javamap;
pub mod link;
pub mod message;
pub mod messaging;
pub mod native;
pub mod sasl;
pub mod transaction;
pub mod transport;
pub mod types;

pub use codec::{CodecError, Decoder};
pub use composite::{Composite, Field, Fields, Timestamp};
pub use types::{Array, ArrayType, Descriptor, Symbol, Value};

use std::sync::Arc;

use saranga_server::message::NativeDecoder;
use saranga_server::protocol::{Connection, ProtocolManager, ServeFuture};
use saranga_server::{Acceptor, Broker};

/// The settings an acceptor's URI gives AMQP (`ProtonProtocolManager`'s
/// bean properties), with Artemis's defaults.
#[derive(Debug, Clone)]
pub struct Settings {
    /// `amqpCredits`: the credit a producer's link is given.
    pub credits: i32,
    /// `amqpLowCredits`: below this the credit is topped up.
    pub low_credits: i32,
    /// `amqpMinLargeMessageSize`.
    pub min_large_message_size: usize,
    /// `amqpDuplicateDetection`.
    pub duplicate_detection: bool,
    /// `amqpUseModifiedForTransientDeliveryErrors`.
    pub use_modified_for_transient_delivery_errors: bool,
    /// `amqpDrainOnTransientDeliveryErrors`.
    pub drain_on_transient_delivery_errors: bool,
    /// `amqpTreatRejectAsUnmodifiedDeliveryFailed`.
    pub treat_reject_as_unmodified_delivery_failed: bool,
    /// `maxFrameSize`.
    pub max_frame_size: u32,
    /// `initialRemoteMaxFrameSize`: the largest frame read before the
    /// client's open says otherwise.
    pub initial_remote_max_frame_size: u32,
    /// `amqpIdleTimeout`, in milliseconds; `None` for the connection TTL.
    pub idle_timeout: Option<i64>,
    /// `saslMechanisms`; empty for the defaults.
    pub sasl_mechanisms: Vec<String>,
    /// `directDeliver`.
    pub direct_deliver: bool,
}

impl Settings {
    pub fn from(acceptor: &Acceptor) -> Self {
        Self::from_params(&acceptor.params)
    }

    /// The settings of a URI's parameters: an acceptor's, or a broker
    /// connection's (`createProtocolManager(server, extraParams)`).
    pub fn from_params(params: &std::collections::HashMap<String, String>) -> Self {
        let p = |k: &str| params.get(k).map(|v| v.trim());
        let int =
            |k: &str, default: i64| p(k).and_then(|v| v.parse::<i64>().ok()).unwrap_or(default);
        let flag = |k: &str, default: bool| {
            p(k).map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(default)
        };
        Self {
            credits: int("amqpCredits", 1000) as i32,
            low_credits: int("amqpLowCredits", 300) as i32,
            min_large_message_size: int("amqpMinLargeMessageSize", 100 * 1024).max(0) as usize,
            duplicate_detection: flag("amqpDuplicateDetection", true),
            use_modified_for_transient_delivery_errors: flag(
                "amqpUseModifiedForTransientDeliveryErrors",
                false,
            ),
            drain_on_transient_delivery_errors: flag("amqpDrainOnTransientDeliveryErrors", true),
            treat_reject_as_unmodified_delivery_failed: flag(
                "amqpTreatRejectAsUnmodifiedDeliveryFailed",
                false,
            ),
            max_frame_size: int("maxFrameSize", 128 * 1024) as u32,
            initial_remote_max_frame_size: int("initialRemoteMaxFrameSize", 4 * 1024) as u32,
            idle_timeout: p("amqpIdleTimeout").and_then(|v| v.parse().ok()),
            sasl_mechanisms: p("saslMechanisms")
                .map(|v| {
                    v.split(',')
                        .map(|m| m.trim().to_owned())
                        .filter(|m| !m.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            direct_deliver: flag("directDeliver", true),
        }
    }
}

/// The broker settings AMQP reads (`Configuration`).
#[derive(Debug, Clone, Default)]
pub struct AmqpConfig {
    /// `amqp-use-core-subscription-naming`.
    pub use_core_subscription_naming: bool,
    /// `<broker-connections>`.
    pub broker_connections: Vec<saranga_config::AmqpBrokerConnectConfig>,
}

/// The AMQP protocol (`ProtonProtocolManagerFactory` and
/// `ProtonProtocolManager`).
#[derive(Default)]
pub struct AmqpProtocol {
    config: AmqpConfig,
    /// The container IDs of the open connections.
    containers: Arc<connection::ContainerIds>,
    /// The broker connections, once started.
    broker_connections: broker_connection::BrokerConnectionManager,
}

impl AmqpProtocol {
    pub fn new(config: AmqpConfig) -> Self {
        Self {
            config,
            containers: Arc::default(),
            broker_connections: Default::default(),
        }
    }
}

impl ProtocolManager for AmqpProtocol {
    fn name(&self) -> &'static str {
        "AMQP"
    }

    /// `isProtocol`: the first four bytes are `AMQP`.
    fn is_protocol(&self, header: &[u8]) -> bool {
        header.starts_with(b"AMQP")
    }

    fn websocket_subprotocols(&self) -> &'static [&'static str] {
        &["amqp"]
    }

    fn serve(&self, broker: Arc<Broker>, connection: Connection) -> ServeFuture {
        let settings = Settings::from(&connection.acceptor);
        let config = self.config.clone();
        let containers = self.containers.clone();
        Box::pin(connection::serve(
            broker, connection, settings, config, containers,
        ))
    }

    /// AMQP messages are kept in their own encoding:
    /// `AMQPMessagePersister`, `V2` and `V3` records.
    fn message_decoder(&self) -> Option<Arc<NativeDecoder>> {
        Some(native::decoder())
    }

    /// `loadProtocolServices`: the broker connections start.
    fn start_services(&self, broker: &Arc<Broker>) {
        self.broker_connections.start(
            broker,
            &self.config.broker_connections,
            &self.config,
            &self.containers,
        );
    }

    fn stop_services(&self, broker: &Broker) {
        self.broker_connections.stop(broker);
    }
}
