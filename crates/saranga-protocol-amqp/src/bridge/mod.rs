//! AMQP bridges (`org.apache.activemq.artemis.protocol.amqp.connect.bridge`).
//!
//! A broker connection's `<bridge>` (`AMQPBridgeManager`) moves messages
//! between this broker and its peer, which need be no more than an AMQP
//! endpoint: its from policies attach receivers taking the peer's
//! messages into local addresses and queues (`AMQPBridgeFromPolicyManager`),
//! while there is demand for them here or always, and its to policies
//! attach senders sending local addresses' and queues' messages to the
//! peer (`AMQPBridgeToPolicyManager`) for as long as they exist. A link the
//! peer closes is attached again after a delay (link recovery).
//!
//! The connection's task does the links (see `connection.rs`), the policy
//! managers ([`from`], [`to`]) decide which.

pub mod config;
mod from;
pub mod policy;
mod to;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use saranga_config::AmqpBridgeElement;
use saranga_server::{Broker, RoutingType};

use crate::Settings;
use crate::connection::{ConnectionHandle, ManagedReceiver};
use crate::federation::{lock, wildcards_of};
use crate::transport::Error;
use crate::types::Value;
use config::{Configuration, LinkConfiguration};
use from::{FromAddressManager, FromQueueManager};
use policy::{AddressPolicy, QueuePolicy};
use to::{ToAddressManager, ToQueueManager};

/// `AmqpSupport.RECEIVER_PRIORITY`: a receiver's priority, in its
/// properties.
pub const RECEIVER_PRIORITY: &str = "priority";
/// `ActiveMQDefaultConfiguration.getDefaultConsumerPriority`.
const DEFAULT_CONSUMER_PRIORITY: i32 = 0;

/// What a bridge knows of its current connection.
#[derive(Clone)]
pub struct Connection {
    pub handle: ConnectionHandle,
    pub configuration: Configuration,
    /// The peer offered `SHARED-SUBS`.
    pub shared_subs: bool,
}

/// A bridge link of this end: what the connection makes of the peer's
/// answer.
#[derive(Clone)]
pub enum BridgeLink {
    /// A from policy's receiver (`AMQPBridgeReceiver`).
    Receiver(Arc<Receiver>),
    /// A to policy's sender (`AMQPBridgeSender`).
    Sender(Arc<Sender>),
}

impl std::fmt::Debug for BridgeLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Receiver(r) => write!(f, "Receiver({})", r.link_name),
            Self::Sender(s) => write!(f, "Sender({})", s.link_name),
        }
    }
}

/// What a link tells the entry that made it.
trait Owner: Send + Sync {
    fn opened(&self, key: &str, link: &str);
    fn remote_closed(&self, key: &str, link: &str);
    fn stopped(&self, key: &str, link: &str, ok: bool);
}

/// The link's identity in its manager, and how far its attach got.
struct LinkState {
    owner: Weak<dyn Owner>,
    key: String,
    attached: AtomicBool,
    closed: AtomicBool,
}

impl LinkState {
    fn new(owner: Weak<dyn Owner>, key: String) -> Self {
        Self {
            owner,
            key,
            attached: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }
    }

    fn attached(&self, link: &str, opened: bool) {
        self.attached.store(true, Ordering::SeqCst);
        if opened && let Some(o) = self.owner.upgrade() {
            o.opened(&self.key, link);
        }
    }

    /// The remote close handler, then the interceptor's answer.
    fn remote_closed(&self, link: &str, error: Option<&Error>) -> bool {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(o) = self.owner.upgrade() {
            o.remote_closed(&self.key, link);
        }
        crate::federation::manager::is_intercepted(error)
    }
}

/// A from policy's receiver (`AMQPBridgeFromAddressReceiver`,
/// `AMQPBridgeFromQueueReceiver`).
pub struct Receiver {
    pub link_name: String,
    /// The local address, or the local queue's FQQN.
    pub destination: String,
    /// The local queue, for a queue receiver.
    pub queue: Option<String>,
    pub credits: i32,
    pub credits_low: i32,
    pub use_modified: bool,
    pub large_threshold: i32,
    state: LinkState,
}

impl Receiver {
    /// `close`: the link is detached, unless the peer did it already.
    fn close(&self, c: &Connection) {
        if !self.state.closed.swap(true, Ordering::SeqCst) {
            c.handle
                .link_op(self.link_name.clone(), crate::connection::LinkOp::Close);
        }
    }
}

impl ManagedReceiver for Receiver {
    fn link_name(&self) -> &str {
        &self.link_name
    }

    fn destination(&self) -> &str {
        &self.destination
    }

    fn credits(&self) -> i32 {
        self.credits
    }

    fn credits_low(&self) -> i32 {
        self.credits_low
    }

    fn use_modified(&self) -> bool {
        self.use_modified
    }

    fn large_threshold(&self) -> i32 {
        self.large_threshold
    }

    fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::SeqCst)
    }

    fn attached(&self, opened: bool) {
        self.state.attached(&self.link_name, opened);
    }

    fn remote_closed(&self, error: Option<&Error>) -> bool {
        self.state.remote_closed(&self.link_name, error)
    }

    fn stopped(&self, ok: bool) {
        if let Some(o) = self.state.owner.upgrade() {
            o.stopped(&self.state.key, &self.link_name, ok);
        }
    }
}

/// What a to policy's sender consumes.
#[derive(Debug, Clone)]
pub enum SenderKind {
    /// A subscription queue on a local address
    /// (`AMQPBridgeToAddressSenderController`): durable and kept, or
    /// temporary and removed with the link.
    Address {
        address: String,
        queue: String,
        routing: RoutingType,
        durable: bool,
        filter: Option<String>,
        auto_delete: bool,
        auto_delete_delay: i64,
        auto_delete_message_count: i64,
    },
    /// A local queue (`AMQPBridgeToQueueSenderController`).
    Queue {
        address: String,
        queue: String,
        filter: Option<String>,
    },
}

/// A to policy's sender (`AMQPBridgeToAddressSender`,
/// `AMQPBridgeToQueueSender`).
pub struct Sender {
    pub link_name: String,
    pub kind: SenderKind,
    pub priority: i32,
    pub presettled: bool,
    state: LinkState,
}

impl Sender {
    pub fn is_closed(&self) -> bool {
        self.state.closed.load(Ordering::SeqCst)
    }

    pub fn attached(&self, opened: bool) {
        self.state.attached(&self.link_name, opened);
    }

    /// The peer closed the link: whether for a reason the connection
    /// survives.
    pub fn remote_closed(&self, error: Option<&Error>) -> bool {
        self.state.remote_closed(&self.link_name, error)
    }

    fn close(&self, c: &Connection) {
        if !self.state.closed.swap(true, Ordering::SeqCst) {
            c.handle
                .link_op(self.link_name.clone(), crate::connection::LinkOp::Close);
        }
    }
}

/// `brokerConnectionTimeout`: an attach the peer has not answered in
/// `attach-timeout` seconds fails the connection
/// (`signalResourceCreateError`).
fn watch_attach(c: &Connection, seconds: i32, state: Weak<dyn AttachWatch>) {
    if seconds <= 0 {
        return;
    }
    let handle = c.handle.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(seconds as u64)).await;
        if state.upgrade().is_some_and(|s| s.unanswered()) {
            handle.link_failed(
                "AMQ119020: The Broker Connection Open Callback Has Timed Out.".into(),
            );
        }
    });
}

trait AttachWatch: Send + Sync {
    fn unanswered(&self) -> bool;
}

impl AttachWatch for Receiver {
    fn unanswered(&self) -> bool {
        !self.state.attached.load(Ordering::SeqCst) && !self.state.closed.load(Ordering::SeqCst)
    }
}

impl AttachWatch for Sender {
    fn unanswered(&self) -> bool {
        !self.state.attached.load(Ordering::SeqCst) && !self.state.closed.load(Ordering::SeqCst)
    }
}

/// A link's recovery after the peer closed it
/// (`AMQPBridgeReceiverRecoveryHandler`, `AMQPBridgeSenderRecoveryHandler`):
/// attempts counted against `maxLinkRecoveryAttempts`, the first after
/// `linkRecoveryInitialDelay`, the rest after `linkRecoveryDelay`.
struct Recovery {
    attempts: i32,
    next_delay: i64,
    /// Bumped to cancel the scheduled attempt.
    generation: u64,
}

impl Recovery {
    fn new(c: &LinkConfiguration) -> Self {
        let initial = c.link_recovery_initial_delay();
        Self {
            attempts: 0,
            next_delay: if initial > 0 { initial } else { 1 },
            generation: 0,
        }
    }

    /// `tryScheduleNextRecovery`: the delay of the next attempt, if one
    /// is allowed.
    fn next(&mut self, c: &LinkConfiguration) -> Option<(u64, i64)> {
        let max = c.max_link_recovery_attempts();
        if max < 0 || self.attempts < max {
            self.generation += 1;
            Some((self.generation, self.next_delay))
        } else {
            None
        }
    }

    /// `handleReconnectionAttempt`: counted, and the delay is the later
    /// one from now on.
    fn attempted(&mut self, c: &LinkConfiguration) {
        if c.max_link_recovery_attempts() > 0 {
            self.attempts += 1;
        }
        let delay = c.link_recovery_delay();
        self.next_delay = if delay > 0 { delay } else { 1 };
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagerState {
    Uninitialized,
    Stopped,
    Started,
    Shutdown,
}

/// One `<bridge>` (`AMQPBridgeManager`).
pub struct BridgeManager {
    pub name: String,
    broker: Weak<Broker>,
    node_id: String,
    /// The bridge's `<property>`s.
    properties: Vec<(String, Value)>,
    from_address: Vec<Arc<FromAddressManager>>,
    from_queue: Vec<Arc<FromQueueManager>>,
    to_address: Vec<Arc<ToAddressManager>>,
    to_queue: Vec<Arc<ToQueueManager>>,
    state: Mutex<ManagerState>,
}

impl BridgeManager {
    /// `AMQPBridgeSupport.createManager`, then `initialize`: each policy
    /// manager registers its broker plugin.
    pub fn new(broker: &Arc<Broker>, e: &AmqpBridgeElement) -> Arc<Self> {
        let wildcards = wildcards_of(broker);
        let b = Arc::new_cyclic(|me: &Weak<BridgeManager>| Self {
            name: e.name.clone(),
            broker: Arc::downgrade(broker),
            node_id: broker.node_id().to_owned(),
            properties: e
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), Value::string(v.clone())))
                .collect(),
            from_address: e
                .from_address_policies
                .iter()
                .map(|p| FromAddressManager::new(me, AddressPolicy::from_config(p, &wildcards)))
                .collect(),
            from_queue: e
                .from_queue_policies
                .iter()
                .map(|p| FromQueueManager::new(me, QueuePolicy::from_config(p, &wildcards)))
                .collect(),
            to_address: e
                .to_address_policies
                .iter()
                .map(|p| ToAddressManager::new(me, AddressPolicy::from_config(p, &wildcards)))
                .collect(),
            to_queue: e
                .to_queue_policies
                .iter()
                .map(|p| ToQueueManager::new(me, QueuePolicy::from_config(p, &wildcards)))
                .collect(),
            state: Mutex::new(ManagerState::Uninitialized),
        });
        b.initialize();
        b
    }

    fn broker(&self) -> Option<Arc<Broker>> {
        self.broker.upgrade()
    }

    fn initialize(&self) {
        {
            let mut st = lock(&self.state);
            if *st != ManagerState::Uninitialized {
                return;
            }
            *st = ManagerState::Stopped;
        }
        self.each(|m| m.initialize());
    }

    fn each(&self, f: impl Fn(&dyn PolicyManager)) {
        for m in &self.from_address {
            f(m.as_ref());
        }
        for m in &self.from_queue {
            f(m.as_ref());
        }
        for m in &self.to_address {
            f(m.as_ref());
        }
        for m in &self.to_queue {
            f(m.as_ref());
        }
    }

    pub fn start(&self) {
        {
            let mut st = lock(&self.state);
            if *st != ManagerState::Stopped {
                return;
            }
            *st = ManagerState::Started;
        }
        self.each(|m| m.start());
    }

    /// `stop`: the links go; the policy managers wait for a start.
    pub fn stop(&self) {
        {
            let mut st = lock(&self.state);
            if *st != ManagerState::Started {
                return;
            }
            *st = ManagerState::Stopped;
        }
        self.each(|m| m.stop());
    }

    pub fn shutdown(&self) {
        {
            let mut st = lock(&self.state);
            if *st == ManagerState::Shutdown {
                return;
            }
            *st = ManagerState::Shutdown;
        }
        self.each(|m| m.shutdown());
    }

    /// `connectionRestored`, once the peer's Open is in: this connection's
    /// settings, then each policy's links.
    pub fn connection_restored(
        &self,
        handle: ConnectionHandle,
        settings: Settings,
        shared_subs: bool,
    ) {
        let c = Connection {
            handle,
            configuration: Configuration::new(self.properties.clone(), settings),
            shared_subs,
        };
        self.each(|m| m.connection_restored(&c));
    }

    /// `connectionInterrupted`: the links are gone with the connection.
    pub fn connection_interrupted(&self) {
        self.each(|m| m.connection_interrupted());
    }
}

/// What the bridge asks of each policy manager (`AMQPBridgePolicyManager`).
trait PolicyManager {
    fn initialize(&self);
    fn start(&self);
    fn stop(&self);
    fn shutdown(&self);
    fn connection_restored(&self, c: &Connection);
    fn connection_interrupted(&self);
}
