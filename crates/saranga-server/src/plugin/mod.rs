//! Broker plugins: Artemis's `ActiveMQServerPlugin` as a Rust trait.
//!
//! A plugin is told of connections, sessions, consumers, addresses, queues
//! and bindings as they come and go, of messages as they are sent, routed,
//! delivered, acknowledged and expired, and of critical failures; its
//! `before_*` hooks can refuse what is about to happen, and `can_accept`
//! can keep a message from a consumer. Every hook does nothing unless the
//! plugin says otherwise.
//!
//! An embedding application registers its plugins in
//! `BrokerConfig::plugins` or with [`Plugins::register`]. `broker.xml`'s
//! `<broker-plugins>` name Java classes: those of Artemis's own plugins
//! get their Rust ports ([`builtin`]); others are reported and left out.

pub mod builtin;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use saranga_message::CoreMessage;

use crate::error::BrokerError;
use crate::message::{MessageRef, ServerMessage};
use crate::postoffice::{AddressInfo, QueueSpec};
use crate::queue::{AckReason, Queue};
use crate::registry::{ConsumerEntry, SessionEntry};
use crate::session::ClientConnection;
use crate::{Broker, transaction::Transaction};

/// `RoutingStatus`: what routing a message came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingStatus {
    Ok,
    NoBindings,
    DuplicatedId,
}

impl RoutingStatus {
    /// What routing came to: no queues is `NO_BINDINGS`.
    pub fn of(routed: &crate::postoffice::Routed) -> Self {
        if routed.iter().all(|(_, queues)| queues.is_empty()) {
            Self::NoBindings
        } else {
            Self::Ok
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Ok => "OK",
            Self::NoBindings => "NO_BINDINGS",
            Self::DuplicatedId => "DUPLICATED_ID",
        }
    }
}

/// The `ActiveMQServerPlugin` hooks.
#[allow(unused_variables)]
pub trait BrokerPlugin: Send + Sync {
    /// The plugin is installed in a broker (`registered`).
    fn registered(&self, broker: &Arc<Broker>) {}
    /// The broker is stopping, or the plugin was removed (`unregistered`).
    fn unregistered(&self, broker: &Broker) {}

    fn after_create_connection(&self, connection: &ClientConnection) {}
    fn after_destroy_connection(&self, connection: &ClientConnection) {}

    fn after_create_session(&self, session: &SessionEntry) {}
    fn before_close_session(&self, session: &SessionEntry, failed: bool) {}
    fn after_close_session(&self, session: &SessionEntry, failed: bool) {}
    fn after_session_metadata_added(&self, session: &str, key: &str, value: &str) {}

    fn before_create_consumer(
        &self,
        id: i64,
        queue: &str,
        filter: Option<&str>,
        browse_only: bool,
    ) -> Result<(), BrokerError> {
        Ok(())
    }
    fn after_create_consumer(&self, consumer: &ConsumerEntry) {}
    fn before_close_consumer(&self, consumer: &ConsumerEntry, failed: bool) {}
    fn after_close_consumer(&self, consumer: &ConsumerEntry, failed: bool) {}

    fn before_add_address(&self, info: &AddressInfo, reload: bool) -> Result<(), BrokerError> {
        Ok(())
    }
    fn after_add_address(&self, info: &AddressInfo, reload: bool) {}
    fn after_remove_address(&self, address: &str, info: Option<&AddressInfo>) {}

    fn before_create_queue(&self, spec: &QueueSpec) -> Result<(), BrokerError> {
        Ok(())
    }
    fn after_create_queue(&self, queue: &Queue) {}
    fn before_destroy_queue(&self, name: &str) -> Result<(), BrokerError> {
        Ok(())
    }
    /// `check_consumers` and `auto_delete_address` as the queue was
    /// destroyed with.
    fn after_destroy_queue(&self, queue: &Queue, check_consumers: bool, auto_delete_address: bool) {
    }

    /// A queue or a divert was bound to its address.
    fn after_add_binding(&self, name: &str, address: &str) {}
    /// `beforeRemoveBinding`: a queue's or a divert's binding is about to go.
    fn before_remove_binding(&self, name: &str, address: &str) {}
    fn after_remove_binding(&self, name: &str, address: &str) {}

    /// A session is about to send a message.
    fn before_send(
        &self,
        session: &SessionEntry,
        message: &CoreMessage,
        transacted: bool,
    ) -> Result<(), BrokerError> {
        Ok(())
    }
    fn after_send(
        &self,
        session: &SessionEntry,
        message: &ServerMessage,
        result: RoutingStatus,
        tx: Option<&Transaction>,
    ) {
    }
    fn on_send_exception(
        &self,
        session: &SessionEntry,
        message: &CoreMessage,
        error: &BrokerError,
    ) {
    }

    fn before_message_route(&self, message: &ServerMessage) -> Result<(), BrokerError> {
        Ok(())
    }
    fn after_message_route(&self, message: &ServerMessage, result: RoutingStatus) {}
    fn on_message_route_exception(&self, message: &ServerMessage, error: &BrokerError) {}

    /// Whether `consumer` may be given `message`; a message no consumer
    /// accepts stays on its queue.
    fn can_accept(&self, consumer: &ConsumerEntry, message: &ServerMessage) -> bool {
        true
    }
    fn before_deliver(&self, consumer: &ConsumerEntry, message: &ServerMessage) {}
    fn after_deliver(&self, consumer: &ConsumerEntry, message: &ServerMessage) {}

    /// A message expired: moved to `expiry_address`, or dropped (`None`).
    fn message_expired(&self, reference: &MessageRef, queue: &Queue, expiry_address: Option<&str>) {
    }
    /// A message left its queue: acknowledged, killed, expired or replaced.
    fn message_acknowledged(&self, reference: &MessageRef, queue: &Queue, reason: AckReason) {}

    /// The critical analyzer found `component` unresponsive.
    fn critical_failure(&self, component: &str) {}

    /// `ActiveMQServerBridgePlugin`: a bridge is about to be deployed, or
    /// was; it is about to send the message with this ID, or has; the other
    /// broker has the message, now acknowledged.
    fn before_deploy_bridge(
        &self,
        config: &saranga_config::BridgeConfig,
    ) -> Result<(), BrokerError> {
        Ok(())
    }
    fn after_deploy_bridge(&self, bridge: &crate::bridge::Bridge) {}
    fn before_deliver_bridge(&self, bridge: &str, message_id: i64) {}
    fn after_deliver_bridge(&self, bridge: &str, message_id: i64) {}
    fn after_acknowledge_bridge(&self, bridge: &str, message_id: i64) {}
}

/// A broker's plugins (`getBrokerPlugins`).
#[derive(Default)]
pub struct Plugins {
    list: RwLock<Vec<Arc<dyn BrokerPlugin>>>,
    /// There is at least one: the hooks cost nothing otherwise.
    any: AtomicBool,
}

impl Plugins {
    /// `registerBrokerPlugin`.
    pub fn register(&self, plugin: Arc<dyn BrokerPlugin>) {
        let mut list = self.list.write().unwrap_or_else(|e| e.into_inner());
        list.push(plugin);
        self.any.store(true, Ordering::Release);
    }

    /// `unRegisterBrokerPlugin`: removes a plugin, by identity.
    pub fn unregister(&self, plugin: &Arc<dyn BrokerPlugin>) -> bool {
        let mut list = self.list.write().unwrap_or_else(|e| e.into_inner());
        let before = list.len();
        list.retain(|p| !Arc::ptr_eq(p, plugin));
        self.any.store(!list.is_empty(), Ordering::Release);
        list.len() != before
    }

    pub fn is_empty(&self) -> bool {
        !self.any.load(Ordering::Acquire)
    }

    pub fn all(&self) -> Vec<Arc<dyn BrokerPlugin>> {
        if self.is_empty() {
            return Vec::new();
        }
        self.list.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Calls a hook on every plugin.
    pub fn each(&self, f: impl Fn(&dyn BrokerPlugin)) {
        if self.is_empty() {
            return;
        }
        for p in self.all() {
            f(&*p);
        }
    }

    /// Calls a `before_*` hook on every plugin; the first refusal stops it.
    pub fn check(
        &self,
        f: impl Fn(&dyn BrokerPlugin) -> Result<(), BrokerError>,
    ) -> Result<(), BrokerError> {
        if self.is_empty() {
            return Ok(());
        }
        for p in self.all() {
            f(&*p)?;
        }
        Ok(())
    }

    /// Whether every plugin accepts the message for the consumer.
    pub fn can_accept(&self, consumer: &ConsumerEntry, message: &ServerMessage) -> bool {
        self.is_empty() || self.all().iter().all(|p| p.can_accept(consumer, message))
    }
}

/// A `<broker-plugin>`: its class name and properties.
pub fn from_config(
    class_name: &str,
    properties: &HashMap<String, String>,
) -> Option<Result<Arc<dyn BrokerPlugin>, String>> {
    builtin::create(class_name, properties)
}
