//! The local policy managers (`AMQPFederationAddressPolicyManager`,
//! `AMQPFederationQueuePolicyManager`): broker plugins that see demand
//! here and keep a federation receiver attached to the peer for it, as
//! `AMQPFederationConsumerManager` does: created on the first demand,
//! stopped (its credit drained) when the last goes, started again when
//! demand comes back, and closed after the receiver's idle timeout.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use saranga_server::RoutingType;
use saranga_server::plugin::BrokerPlugin;
use saranga_server::queue::ConsumerId;
use saranga_server::registry::ConsumerEntry;

use super::config::{ConsumerConfiguration, DEFAULT_LINK_QUIESCE_TIMEOUT};
use super::constants::*;
use super::policy::{AddressPolicy, QueuePolicy, address_filter, java_hash_order};
use super::{Connection, FedLink, Federation, lock};
use crate::composite::Fields;
use crate::connection::{LinkOp, LocalLink, LocalLinkKind, ManagedReceiver};
use crate::messaging::{
    DeliveryState, Modified, Source, Target, TargetKind, TerminusDurability, TerminusExpiryPolicy,
};
use crate::transport::{Attach, Error, ReceiverSettleMode, Role, SenderSettleMode};
use crate::types::{Descriptor, Symbol, Value};

/// `LINK_SEQUENCE_ID`: keeps receivers that would have the same name
/// apart.
static LINK_SEQUENCE_ID: AtomicU64 = AtomicU64::new(0);

/// Conditions a receiver's remote close may carry without the connection
/// being at fault (`remoteLinkClosedInterceptor`).
const RESOURCE_DELETED: &str = "amqp:resource-deleted";
const NOT_FOUND: &str = "amqp:not-found";
const DETACH_FORCED: &str = "amqp:link:detach-forced";

/// `ActiveMQDefaultConfiguration.getDefaultConsumerPriority`.
const DEFAULT_CONSUMER_PRIORITY: i32 = 0;

/// What a federation receiver takes messages into.
#[derive(Debug, Clone)]
pub enum ConsumerKind {
    /// An address: each message gets one more hop.
    Address { target: String },
    /// A queue, by its FQQN.
    Queue { fqqn: String },
}

/// The events a receiver's link tells its manager of.
trait Owner: Send + Sync {
    fn opened(&self, key: &str, link: &str);
    fn remote_closed(&self, key: &str, link: &str);
    fn stopped(&self, key: &str, link: &str, ok: bool);
    fn start_failed(&self, key: &str, link: &str);
}

/// A federation receiver (`AMQPFederationConsumer`).
pub struct Consumer {
    pub link_name: String,
    pub kind: ConsumerKind,
    pub credits: i32,
    pub credits_low: i32,
    pub use_modified: bool,
    pub drain_on_errors: bool,
    pub quiesce_timeout: i32,
    pub stop_timeout: i32,
    pub large_threshold: i32,
    federation: Weak<Federation>,
    owner: Weak<dyn Owner>,
    key: String,
    attached: AtomicBool,
    closed: AtomicBool,
}

impl ManagedReceiver for Consumer {
    fn link_name(&self) -> &str {
        &self.link_name
    }

    fn destination(&self) -> &str {
        match &self.kind {
            ConsumerKind::Address { target } => target,
            ConsumerKind::Queue { fqqn } => fqqn,
        }
    }

    fn adds_hop(&self) -> bool {
        matches!(self.kind, ConsumerKind::Address { .. })
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

    fn drain_on_transient_delivery_errors(&self) -> bool {
        self.drain_on_errors
    }

    fn link_quiesce_timeout(&self) -> i32 {
        self.quiesce_timeout
    }

    fn stop_timeout(&self) -> i32 {
        self.stop_timeout
    }

    fn large_threshold(&self) -> i32 {
        self.large_threshold
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn attached(&self, opened: bool) {
        self.attached.store(true, Ordering::SeqCst);
        if opened && let Some(o) = self.owner.upgrade() {
            o.opened(&self.key, &self.link_name);
        }
    }

    /// The manager forgets the receiver either way.
    fn remote_closed(&self, error: Option<&Error>) -> bool {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(o) = self.owner.upgrade() {
            o.remote_closed(&self.key, &self.link_name);
        }
        is_intercepted(error)
    }

    fn stopped(&self, ok: bool) {
        if let Some(o) = self.owner.upgrade() {
            o.stopped(&self.key, &self.link_name, ok);
        }
    }
}

/// `remoteLinkClosedInterceptor`: a remote close the connection survives.
pub(crate) fn is_intercepted(error: Option<&Error>) -> bool {
    error.is_some_and(|e| {
        matches!(
            e.condition.as_str(),
            RESOURCE_DELETED | NOT_FOUND | DETACH_FORCED
        )
    })
}

impl Consumer {
    /// A start asked for found the link gone.
    pub fn start_failed(&self) {
        if let Some(o) = self.owner.upgrade() {
            o.start_failed(&self.key, &self.link_name);
        }
    }

    pub fn federation(&self) -> Option<Arc<Federation>> {
        self.federation.upgrade()
    }

    /// `close`: the link is detached, unless the peer did it already.
    fn close(&self, c: &Connection) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            c.handle.link_op(self.link_name.clone(), LinkOp::Close);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryState {
    Ready,
    Starting,
    Started,
    Stopping,
    Stopped,
    Closed,
}

/// One receiver's demand and state (`AMQPFederationConsumerManager`).
struct Entry<D> {
    demand: HashSet<D>,
    state: EntryState,
    consumer: Option<Arc<Consumer>>,
    /// Bumped to cancel a pending idle timeout.
    idle: u64,
}

impl<D> Default for Entry<D> {
    fn default() -> Self {
        Self {
            demand: HashSet::new(),
            state: EntryState::Ready,
            consumer: None,
            idle: 0,
        }
    }
}

/// A timer an entry asks for.
enum Timer {
    Idle {
        link: String,
        generation: u64,
        ms: i32,
    },
    Stop {
        link: String,
        ms: i32,
    },
    Attach {
        consumer: Arc<Consumer>,
        seconds: i32,
    },
}

/// What an entry's transitions need from its manager.
trait Ops {
    fn connection(&self) -> &Connection;
    fn create(&self) -> (Arc<Consumer>, LocalLink);
    /// The receiver's idle timeout, in milliseconds.
    fn idle_timeout(&self) -> i32;
    fn quiesce_timeout(&self) -> i32;
    fn attach_timeout(&self) -> i32;
    fn schedule(&self, timer: Timer);
}

impl<D: Eq + Hash> Entry<D> {
    fn add_demand(&mut self, d: D, ops: &dyn Ops) {
        if self.state == EntryState::Closed {
            return;
        }
        self.demand.insert(d);
        match self.state {
            EntryState::Stopped => self.restart(ops),
            EntryState::Ready => self.create(ops),
            _ => {}
        }
    }

    fn remove_demand(&mut self, d: &D, ops: &dyn Ops) {
        if self.state == EntryState::Closed {
            return;
        }
        self.demand.remove(d);
        if !self.demand.is_empty() || self.state == EntryState::Ready {
            return;
        }
        if self.state != EntryState::Stopping {
            self.try_stop(ops);
        }
    }

    fn recover(&mut self, ops: &dyn Ops) {
        if self.demand.is_empty() {
            return;
        }
        match self.state {
            EntryState::Stopped => self.restart(ops),
            EntryState::Ready => self.create(ops),
            _ => {}
        }
    }

    /// `tryCreateFederationConsumer`.
    fn create(&mut self, ops: &dyn Ops) {
        self.state = EntryState::Starting;
        let (consumer, link) = ops.create();
        tracing::debug!("AMQP Federation creating receiver {}", consumer.link_name);
        self.consumer = Some(consumer.clone());
        ops.connection().handle.open(link);
        let seconds = ops.attach_timeout();
        if seconds > 0 {
            ops.schedule(Timer::Attach { consumer, seconds });
        }
    }

    /// `tryRestartFederationConsumer`.
    fn restart(&mut self, ops: &dyn Ops) {
        self.state = EntryState::Starting;
        self.idle += 1;
        if let Some(c) = &self.consumer {
            ops.connection()
                .handle
                .link_op(c.link_name.clone(), LinkOp::Start);
        }
    }

    /// `tryStopFederationConsumer`.
    fn try_stop(&mut self, ops: &dyn Ops) {
        let Some(c) = &self.consumer else { return };
        if self.state == EntryState::Stopping {
            return;
        }
        if self.state != EntryState::Closed {
            self.state = EntryState::Stopping;
        }
        ops.connection()
            .handle
            .link_op(c.link_name.clone(), LinkOp::Stop);
        let ms = ops.quiesce_timeout();
        if ms > 0 {
            ops.schedule(Timer::Stop {
                link: c.link_name.clone(),
                ms,
            });
        }
    }

    fn is_current(&self, link: &str) -> bool {
        self.consumer.as_ref().is_some_and(|c| c.link_name == link)
    }

    fn opened(&mut self, link: &str) {
        if self.is_current(link) && self.state == EntryState::Starting {
            self.state = EntryState::Started;
        }
    }

    /// The remote close handler: the receiver is forgotten; later demand
    /// or an event makes another.
    fn remote_closed(&mut self, link: &str) {
        if self.is_current(link) {
            self.consumer = None;
            if self.state != EntryState::Closed {
                self.state = EntryState::Ready;
            }
            self.idle += 1;
        }
    }

    /// `handleFederationConsumerStopped`.
    fn stopped(&mut self, link: &str, ok: bool, ops: &dyn Ops) {
        if !self.is_current(link) {
            return;
        }
        if self.state == EntryState::Closed {
            self.close_current(ops.connection());
            return;
        }
        if self.state != EntryState::Stopping {
            return;
        }
        if ok {
            self.state = EntryState::Stopped;
        } else {
            self.close_current(ops.connection());
        }
        match self.state {
            EntryState::Ready if !self.demand.is_empty() => self.create(ops),
            EntryState::Stopped => {
                if !self.demand.is_empty() {
                    self.restart(ops);
                } else if ops.idle_timeout() > 0 {
                    ops.schedule(Timer::Idle {
                        link: link.to_owned(),
                        generation: self.idle,
                        ms: ops.idle_timeout(),
                    });
                } else {
                    self.close_current(ops.connection());
                }
            }
            _ => {}
        }
    }

    fn start_failed(&mut self, link: &str, ops: &dyn Ops) {
        if !self.is_current(link) {
            return;
        }
        self.close_current(ops.connection());
        if self.state == EntryState::Ready && !self.demand.is_empty() {
            self.create(ops);
        }
    }

    fn idle_expired(&mut self, link: &str, generation: u64, c: &Connection) {
        if self.state == EntryState::Stopped && self.is_current(link) && self.idle == generation {
            self.close_current(c);
        }
    }

    /// `safeCloseCurrentFederationConsumer`.
    fn close_current(&mut self, c: &Connection) {
        if let Some(consumer) = self.consumer.take() {
            consumer.close(c);
            if self.state != EntryState::Closed {
                self.state = EntryState::Ready;
            }
            self.idle += 1;
        }
    }

    /// `shutdown` (stopping first) and `shutdownNow`; without a
    /// connection its links are gone already.
    fn shutdown(&mut self, stop_first: bool, ops: Option<&dyn Ops>) {
        if self.state == EntryState::Closed {
            return;
        }
        let was = self.state;
        self.state = EntryState::Closed;
        self.demand.clear();
        self.idle += 1;
        let Some(ops) = ops else {
            self.consumer = None;
            return;
        };
        match &self.consumer {
            None => {}
            Some(c) if stop_first && was != EntryState::Stopped && !c.is_closed() => {
                // Closed once its stop completes (`stopped`).
                ops.connection()
                    .handle
                    .link_op(c.link_name.clone(), LinkOp::Stop);
            }
            Some(_) => self.close_current(ops.connection()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagerState {
    Uninitialized,
    Stopped,
    Started,
    Shutdown,
}

/// What both local policy managers keep (`AMQPFederationPolicyManager`,
/// `AMQPFederationLocalPolicyManager`).
struct Base<K, D> {
    state: ManagerState,
    connection: Option<Connection>,
    configuration: Option<ConsumerConfiguration>,
    tracking: HashMap<K, Entry<D>>,
    plugin: Option<Arc<dyn BrokerPlugin>>,
}

impl<K, D> Default for Base<K, D> {
    fn default() -> Self {
        Self {
            state: ManagerState::Uninitialized,
            connection: None,
            configuration: None,
            tracking: HashMap::new(),
            plugin: None,
        }
    }
}

impl<K, D> Base<K, D> {
    fn is_active(&self) -> bool {
        self.connection.is_some() && self.state == ManagerState::Started
    }
}

/// Runs a timer an entry asked for, then the manager's handler.
fn spawn_timer<M: Send + Sync + 'static>(manager: Weak<M>, timer: Timer, handle: fn(&M, Timer)) {
    let delay = match &timer {
        Timer::Idle { ms, .. } | Timer::Stop { ms, .. } => Duration::from_millis(*ms as u64),
        Timer::Attach { seconds, .. } => Duration::from_secs(*seconds as u64),
    };
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        if let Some(m) = manager.upgrade() {
            handle(&m, timer);
        }
    });
}

/// An attach the peer never answered (`brokerConnectionTimeout`).
fn attach_timed_out(consumer: &Consumer) {
    if !consumer.attached.load(Ordering::SeqCst)
        && !consumer.is_closed()
        && let Some(f) = consumer.federation()
    {
        f.signal_error("AMQ119020: The Broker Connection Open Callback Has Timed Out.".into());
    }
}

/// The receiver's source: what the peer sends from, settled by its
/// outcome (`OUTCOMES`, `DEFAULT_OUTCOME`).
fn receiver_source(address: String, routing: RoutingType, filter: Option<Fields>) -> Source {
    let capability = match routing {
        RoutingType::Anycast => crate::link::capability::QUEUE,
        RoutingType::Multicast => crate::link::capability::TOPIC,
    };
    Source {
        address: Some(address),
        durable: TerminusDurability::None,
        expiry_policy: TerminusExpiryPolicy::LinkDetach,
        filter,
        default_outcome: Some(DeliveryState::Modified(Modified {
            delivery_failed: Some(true),
            ..Default::default()
        })),
        outcomes: Some(
            [
                "amqp:accepted:list",
                "amqp:rejected:list",
                "amqp:released:list",
                "amqp:modified:list",
            ]
            .iter()
            .map(|s| Symbol::from(*s))
            .collect(),
        ),
        capabilities: Some(vec![Symbol::from(capability)]),
        ..Default::default()
    }
}

fn selector(filter: &str) -> Value {
    Value::described(
        Descriptor::Code(crate::link::JMS_SELECTOR_CODE),
        Value::String(filter.to_owned()),
    )
}

/// `CompositeAddress.toFullyQualified`.
fn fqqn(address: &str, queue: &str) -> String {
    format!("{address}::{queue}")
}

fn fields(entries: Vec<(String, Value)>) -> Fields {
    let mut f = Fields::default();
    for (k, v) in java_hash_order(entries) {
        f.insert(k, v);
    }
    f
}

/// Whether a queue is the peer's own federation binding here
/// (`OPPOSING_PEER_MATCHING_TEMPLATE`,
/// `^federation\..+?\.policy\..+?\.address\..+?\.node\.<peer>$`).
fn is_opposing_federation_binding(queue: &str, peer: &str) -> bool {
    let Some(rest) = queue.strip_prefix("federation.") else {
        return false;
    };
    let Some(middle) = rest.strip_suffix(&format!(".node.{peer}")) else {
        return false;
    };
    middle.match_indices(".policy.").any(|(i, _)| {
        i >= 1
            && middle[i + 8..]
                .match_indices(".address.")
                .any(|(j, _)| j >= 1 && i + 8 + j + 9 < middle.len())
    })
}

// ------------------------------------------------------------ addresses

/// `AMQPFederationAddressPolicyManager`: demand is a queue bound to a
/// matching multicast address; one receiver per address (the conduit
/// consumer).
pub struct AddressManager {
    federation: Weak<Federation>,
    policy: AddressPolicy,
    /// The hops filter (`baseConsumerFilter`).
    filter: Option<String>,
    me: Weak<AddressManager>,
    base: Mutex<Base<String, String>>,
}

/// An address manager's [`Ops`] for one address.
struct AddressOps<'a> {
    manager: &'a AddressManager,
    connection: Connection,
    config: Option<ConsumerConfiguration>,
    address: String,
}

impl Ops for AddressOps<'_> {
    fn connection(&self) -> &Connection {
        &self.connection
    }

    fn create(&self) -> (Arc<Consumer>, LocalLink) {
        self.manager
            .new_receiver(&self.connection, self.config.as_ref(), &self.address)
    }

    fn idle_timeout(&self) -> i32 {
        self.config
            .as_ref()
            .map_or(0, |c| c.address_receiver_idle_timeout())
    }

    fn quiesce_timeout(&self) -> i32 {
        self.config
            .as_ref()
            .map_or(0, |c| c.receiver_quiesce_timeout())
    }

    fn attach_timeout(&self) -> i32 {
        self.config.as_ref().map_or(0, |c| c.link_attach_timeout())
    }

    fn schedule(&self, timer: Timer) {
        spawn_timer(self.manager.me.clone(), timer, AddressManager::on_timer);
    }
}

impl AddressManager {
    pub fn new(federation: &Arc<Federation>, policy: AddressPolicy) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            federation: Arc::downgrade(federation),
            filter: address_filter(policy.max_hops),
            policy,
            me: me.clone(),
            base: Mutex::default(),
        })
    }

    pub fn policy_name(&self) -> &str {
        &self.policy.name
    }

    fn ops(&self, b: &Base<String, String>, address: &str) -> Option<AddressOps<'_>> {
        Some(AddressOps {
            manager: self,
            connection: b.connection.clone()?,
            config: b.configuration.clone(),
            address: address.to_owned(),
        })
    }

    /// `handleManagerInitialized`: the plugin is registered.
    pub fn initialize(&self) {
        let Some(broker) = self.federation.upgrade().and_then(|f| f.broker()) else {
            return;
        };
        let plugin: Arc<dyn BrokerPlugin> = Arc::new(AddressPlugin(self.me.clone()));
        {
            let mut b = lock(&self.base);
            if b.state != ManagerState::Uninitialized {
                return;
            }
            b.state = ManagerState::Stopped;
            b.plugin = Some(plugin.clone());
        }
        broker.register_plugin(plugin);
    }

    pub fn start(&self) {
        let scan = {
            let mut b = lock(&self.base);
            if b.state != ManagerState::Stopped {
                return;
            }
            b.state = ManagerState::Started;
            b.is_active()
        };
        if scan {
            self.scan_all_bindings();
        }
    }

    pub fn stop(&self) {
        let mut b = lock(&self.base);
        if b.state != ManagerState::Started {
            return;
        }
        b.state = ManagerState::Stopped;
        self.cleanup(&mut b, false);
    }

    pub fn shutdown(&self) {
        let plugin = {
            let mut b = lock(&self.base);
            if b.state == ManagerState::Shutdown {
                return;
            }
            b.state = ManagerState::Shutdown;
            self.cleanup(&mut b, false);
            b.plugin.take()
        };
        if let (Some(p), Some(broker)) =
            (plugin, self.federation.upgrade().and_then(|f| f.broker()))
        {
            broker.unregister_plugin(&p);
        }
    }

    pub fn connection_interrupted(&self) {
        let mut b = lock(&self.base);
        self.cleanup(&mut b, true);
        b.connection = None;
    }

    /// `connectionRestored`: this connection's settings, then a scan.
    pub fn connection_restored(&self, c: &Connection) {
        let scan = {
            let mut b = lock(&self.base);
            b.configuration = Some(ConsumerConfiguration::new(
                c.configuration.clone(),
                self.policy.properties.clone(),
            ));
            b.connection = Some(c.clone());
            b.is_active()
        };
        if scan {
            self.scan_all_bindings();
        }
    }

    /// `safeCleanupManagerResources`.
    fn cleanup(&self, b: &mut Base<String, String>, force: bool) {
        let tracking = std::mem::take(&mut b.tracking);
        for (address, mut e) in tracking {
            match self.ops(b, &address).filter(|_| !force) {
                Some(ops) => e.shutdown(true, Some(&ops)),
                None => e.shutdown(false, None),
            }
        }
    }

    /// `scanAllBindings`: every queue bound now.
    fn scan_all_bindings(&self) {
        let Some(broker) = self.federation.upgrade().and_then(|f| f.broker()) else {
            return;
        };
        for q in broker.post_office().queues() {
            let c = q.config();
            self.binding_added(&c.name, &c.address);
        }
    }

    /// `afterAddBinding` of a queue binding (`checkBindingForMatch`).
    fn binding_added(&self, name: &str, address: &str) {
        let Some(broker) = self.federation.upgrade().and_then(|f| f.broker()) else {
            return;
        };
        if broker.post_office().queue(name).is_none() {
            return;
        }
        let Some(info) = broker.post_office().address_info(address) else {
            return;
        };
        let routing = if info.routing_types.multicast {
            RoutingType::Multicast
        } else {
            RoutingType::Anycast
        };
        let mut b = lock(&self.base);
        if !b.is_active() || !self.policy.test(address, routing) {
            return;
        }
        // Address receivers cannot pull: no match without credit.
        if b.configuration
            .as_ref()
            .is_none_or(|c| c.receiver_credits() <= 0)
        {
            return;
        }
        let Some(ops) = self.ops(&b, address) else {
            return;
        };
        // The peer's own federation bindings are not demand.
        if is_opposing_federation_binding(name, &ops.connection.remote_container) {
            return;
        }
        b.tracking
            .entry(address.to_owned())
            .or_default()
            .add_demand(name.to_owned(), &ops);
    }

    /// `afterRemoveBinding`: the binding's demand goes.
    fn binding_removed(&self, name: &str, address: &str) {
        let mut b = lock(&self.base);
        if !b.is_active() {
            return;
        }
        let Some(ops) = self.ops(&b, address) else {
            return;
        };
        if let Some(e) = b.tracking.get_mut(address) {
            e.remove_demand(&name.to_owned(), &ops);
        }
    }

    /// `afterRemoveAddress`: its receiver closes at once.
    fn address_removed(&self, address: &str) {
        let mut b = lock(&self.base);
        if !b.is_active() {
            return;
        }
        let Some(ops) = self.ops(&b, address) else {
            return;
        };
        if let Some(mut e) = b.tracking.remove(address) {
            e.shutdown(false, Some(&ops));
        }
    }

    /// `afterRemoteAddressAdded`: an address the peer lacked is there now.
    pub fn remote_address_added(&self, address: &str) {
        let mut b = lock(&self.base);
        if !b.is_active() || !self.policy.test(address, RoutingType::Multicast) {
            return;
        }
        let Some(ops) = self.ops(&b, address) else {
            return;
        };
        if let Some(e) = b.tracking.get_mut(address) {
            e.recover(&ops);
        }
    }

    /// The receiver for an address (`createConsumerInfo`,
    /// `AMQPFederationAddressConsumer.doCreateReceiver`).
    fn new_receiver(
        &self,
        c: &Connection,
        config: Option<&ConsumerConfiguration>,
        address: &str,
    ) -> (Arc<Consumer>, LocalLink) {
        let (fed_name, node_id) = self
            .federation
            .upgrade()
            .map(|f| (f.name.clone(), f.node_id().to_owned()))
            .unwrap_or_default();
        let queue_name = format!(
            "federation.{fed_name}.policy.{}.address.{address}.node.{node_id}",
            self.policy.name
        );
        let v2 = c.fqqn_subscriptions();
        let link_name = if v2 {
            format!(
                "federation-{fed_name}-policy-{}-address-receiver-{address}-{node_id}-{}",
                self.policy.name,
                LINK_SEQUENCE_ID.fetch_add(1, Ordering::SeqCst) + 1
            )
        } else {
            format!("federation-{fed_name}-address-receiver-{address}-{node_id}")
        };
        let mut filters = vec![(
            crate::link::NO_LOCAL_NAME.to_owned(),
            crate::link::no_local_value(),
        )];
        if let Some(f) = &self.filter {
            filters.push(("jms-selector".to_owned(), selector(f)));
        }
        let source_address = if v2 {
            fqqn(address, &queue_name)
        } else {
            address.to_owned()
        };
        let source_properties: Vec<(Value, Value)> = java_hash_order(vec![
            (
                ADDRESS_AUTO_DELETE.to_owned(),
                Value::Boolean(self.policy.auto_delete),
            ),
            (
                ADDRESS_AUTO_DELETE_DELAY.to_owned(),
                Value::Long(self.policy.auto_delete_delay),
            ),
            (
                ADDRESS_AUTO_DELETE_MSG_COUNT.to_owned(),
                Value::Long(self.policy.auto_delete_message_count),
            ),
        ])
        .into_iter()
        .map(|(k, v)| (Value::string(k), v))
        .collect();
        let attach = Attach {
            name: link_name.clone(),
            handle: 0,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(receiver_source(
                source_address,
                RoutingType::Multicast,
                Some(fields(filters)),
            )),
            target: Some(TargetKind::Target(Target {
                address: Some(address.to_owned()),
                ..Default::default()
            })),
            desired_capabilities: Some(vec![Symbol::from(FEDERATION_ADDRESS_RECEIVER)]),
            properties: Some(fields(vec![
                (
                    FEDERATED_ADDRESS_SOURCE_PROPERTIES.to_owned(),
                    Value::Map(source_properties),
                ),
                (
                    FEDERATION_POLICY_NAME.to_owned(),
                    Value::string(self.policy.name.clone()),
                ),
            ])),
            ..Default::default()
        };
        let owner: Weak<dyn Owner> = self.me.clone();
        let consumer = Arc::new(Consumer {
            link_name,
            kind: ConsumerKind::Address {
                target: address.to_owned(),
            },
            credits: config.map_or(1000, |c| c.receiver_credits()),
            credits_low: config.map_or(300, |c| c.receiver_credits_low()),
            use_modified: config.is_some_and(|c| c.use_modified_for_transient_delivery_errors()),
            drain_on_errors: config.is_none_or(|c| c.drain_on_transient_delivery_errors()),
            quiesce_timeout: config
                .map_or(DEFAULT_LINK_QUIESCE_TIMEOUT, |c| c.link_quiesce_timeout()),
            stop_timeout: config.map_or(0, |c| c.receiver_quiesce_timeout()),
            large_threshold: config.map_or(100 * 1024, |c| c.large_message_threshold()),
            federation: self.federation.clone(),
            owner,
            key: address.to_owned(),
            attached: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        });
        let link = LocalLink {
            attach,
            kind: LocalLinkKind::Federation(FedLink::Consumer(consumer.clone())),
        };
        (consumer, link)
    }

    fn on_timer(&self, timer: Timer) {
        let key = match &timer {
            Timer::Attach { consumer, .. } => {
                attach_timed_out(consumer);
                return;
            }
            Timer::Idle { link, .. } | Timer::Stop { link, .. } => self.key_of(link),
        };
        let Some(key) = key else { return };
        let mut b = lock(&self.base);
        let Some(ops) = self.ops(&b, &key) else {
            return;
        };
        let Some(e) = b.tracking.get_mut(&key) else {
            return;
        };
        match timer {
            Timer::Idle {
                link, generation, ..
            } => e.idle_expired(&link, generation, &ops.connection),
            Timer::Stop { link, .. } => {
                if e.state == EntryState::Stopping {
                    e.stopped(&link, false, &ops);
                }
            }
            Timer::Attach { .. } => {}
        }
    }

    /// The address whose receiver a link is.
    fn key_of(&self, link: &str) -> Option<String> {
        lock(&self.base)
            .tracking
            .iter()
            .find(|(_, e)| e.is_current(link))
            .map(|(k, _)| k.clone())
    }
}

impl Owner for AddressManager {
    fn opened(&self, key: &str, link: &str) {
        if let Some(e) = lock(&self.base).tracking.get_mut(key) {
            e.opened(link);
        }
    }

    fn remote_closed(&self, key: &str, link: &str) {
        if let Some(e) = lock(&self.base).tracking.get_mut(key) {
            e.remote_closed(link);
        }
    }

    fn stopped(&self, key: &str, link: &str, ok: bool) {
        let mut b = lock(&self.base);
        let Some(ops) = self.ops(&b, key) else { return };
        match b.tracking.get_mut(key) {
            Some(e) if e.is_current(link) => e.stopped(link, ok, &ops),
            // A receiver given up while stopping is closed now.
            _ => ops
                .connection
                .handle
                .link_op(link.to_owned(), LinkOp::Close),
        }
    }

    fn start_failed(&self, key: &str, link: &str) {
        let mut b = lock(&self.base);
        let Some(ops) = self.ops(&b, key) else { return };
        if let Some(e) = b.tracking.get_mut(key) {
            e.start_failed(link, &ops);
        }
    }
}

/// The address manager's broker hooks.
struct AddressPlugin(Weak<AddressManager>);

impl BrokerPlugin for AddressPlugin {
    fn after_add_binding(&self, name: &str, address: &str) {
        if let Some(m) = self.0.upgrade() {
            m.binding_added(name, address);
        }
    }

    fn after_remove_binding(&self, name: &str, address: &str) {
        if let Some(m) = self.0.upgrade() {
            m.binding_removed(name, address);
        }
    }

    fn after_remove_address(
        &self,
        address: &str,
        _info: Option<&saranga_server::postoffice::AddressInfo>,
    ) {
        if let Some(m) = self.0.upgrade() {
            m.address_removed(address);
        }
    }
}

// ------------------------------------------------------------ queues

/// A queue receiver's identity (`AMQPFederationGenericConsumerInfo`'s
/// equality): consumers alike share one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct QueueKey {
    address: String,
    queue: String,
    routing: RoutingType,
    filter: Option<String>,
    priority: i32,
}

impl QueueKey {
    fn text(&self) -> String {
        format!(
            "{}::{}|{:?}|{}|{}",
            self.address,
            self.queue,
            self.routing,
            self.filter.as_deref().unwrap_or(""),
            self.priority
        )
    }
}

/// `AMQPFederationQueuePolicyManager`: demand is a consumer of a matching
/// queue; one receiver per queue, filter and priority.
pub struct QueueManager {
    federation: Weak<Federation>,
    policy: QueuePolicy,
    me: Weak<QueueManager>,
    base: Mutex<Base<String, ConsumerId>>,
    /// The key of each receiver, by its text, and each consumer's.
    keys: Mutex<(HashMap<String, QueueKey>, HashMap<ConsumerId, String>)>,
}

struct QueueOps<'a> {
    manager: &'a QueueManager,
    connection: Connection,
    config: Option<ConsumerConfiguration>,
    key: Option<QueueKey>,
}

impl Ops for QueueOps<'_> {
    fn connection(&self) -> &Connection {
        &self.connection
    }

    fn create(&self) -> (Arc<Consumer>, LocalLink) {
        let key = self.key.as_ref().expect("a queue receiver's key");
        self.manager
            .new_receiver(&self.connection, self.config.as_ref(), key)
    }

    fn idle_timeout(&self) -> i32 {
        self.config
            .as_ref()
            .map_or(0, |c| c.queue_receiver_idle_timeout())
    }

    fn quiesce_timeout(&self) -> i32 {
        self.config
            .as_ref()
            .map_or(0, |c| c.receiver_quiesce_timeout())
    }

    fn attach_timeout(&self) -> i32 {
        self.config.as_ref().map_or(0, |c| c.link_attach_timeout())
    }

    fn schedule(&self, timer: Timer) {
        spawn_timer(self.manager.me.clone(), timer, QueueManager::on_timer);
    }
}

impl QueueManager {
    pub fn new(federation: &Arc<Federation>, policy: QueuePolicy) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            federation: Arc::downgrade(federation),
            policy,
            me: me.clone(),
            base: Mutex::default(),
            keys: Mutex::default(),
        })
    }

    pub fn policy_name(&self) -> &str {
        &self.policy.name
    }

    fn ops(&self, b: &Base<String, ConsumerId>, key: &str) -> Option<QueueOps<'_>> {
        Some(QueueOps {
            manager: self,
            connection: b.connection.clone()?,
            config: b.configuration.clone(),
            key: lock(&self.keys).0.get(key).cloned(),
        })
    }

    pub fn initialize(&self) {
        let Some(broker) = self.federation.upgrade().and_then(|f| f.broker()) else {
            return;
        };
        let plugin: Arc<dyn BrokerPlugin> = Arc::new(QueuePlugin(self.me.clone()));
        {
            let mut b = lock(&self.base);
            if b.state != ManagerState::Uninitialized {
                return;
            }
            b.state = ManagerState::Stopped;
            b.plugin = Some(plugin.clone());
        }
        broker.register_plugin(plugin);
    }

    pub fn start(&self) {
        let scan = {
            let mut b = lock(&self.base);
            if b.state != ManagerState::Stopped {
                return;
            }
            b.state = ManagerState::Started;
            b.is_active()
        };
        if scan {
            self.scan_all_bindings();
        }
    }

    pub fn stop(&self) {
        let mut b = lock(&self.base);
        if b.state != ManagerState::Started {
            return;
        }
        b.state = ManagerState::Stopped;
        self.cleanup(&mut b, false);
    }

    pub fn shutdown(&self) {
        let plugin = {
            let mut b = lock(&self.base);
            if b.state == ManagerState::Shutdown {
                return;
            }
            b.state = ManagerState::Shutdown;
            self.cleanup(&mut b, false);
            b.plugin.take()
        };
        if let (Some(p), Some(broker)) =
            (plugin, self.federation.upgrade().and_then(|f| f.broker()))
        {
            broker.unregister_plugin(&p);
        }
    }

    pub fn connection_interrupted(&self) {
        let mut b = lock(&self.base);
        self.cleanup(&mut b, true);
        b.connection = None;
    }

    pub fn connection_restored(&self, c: &Connection) {
        let scan = {
            let mut b = lock(&self.base);
            b.configuration = Some(ConsumerConfiguration::new(
                c.configuration.clone(),
                self.policy.properties.clone(),
            ));
            b.connection = Some(c.clone());
            b.is_active()
        };
        if scan {
            self.scan_all_bindings();
        }
    }

    fn cleanup(&self, b: &mut Base<String, ConsumerId>, force: bool) {
        let tracking = std::mem::take(&mut b.tracking);
        for (key, mut e) in tracking {
            match self.ops(b, &key).filter(|_| !force) {
                Some(ops) => e.shutdown(true, Some(&ops)),
                None => e.shutdown(false, None),
            }
        }
        let mut keys = lock(&self.keys);
        keys.0.clear();
        keys.1.clear();
    }

    /// `scanAllBindings`: the consumers of every queue.
    fn scan_all_bindings(&self) {
        let Some(broker) = self.federation.upgrade().and_then(|f| f.broker()) else {
            return;
        };
        for session in broker.registry().sessions() {
            for c in session.consumers() {
                self.consumer_created(&c);
            }
        }
    }

    /// `reactIfConsumerMatchesPolicy`.
    fn consumer_created(&self, consumer: &ConsumerEntry) {
        if consumer.browse_only {
            return;
        }
        let Some(broker) = self.federation.upgrade().and_then(|f| f.broker()) else {
            return;
        };
        let c = consumer.queue.config();
        let mut b = lock(&self.base);
        if !b.is_active() || !self.policy.test(&c.address, &c.name) {
            return;
        }
        // A federation's own consumers are not demand, unless asked.
        let federated = broker
            .session_metadata(&consumer.session, FEDERATION_NAME_METADATA)
            .is_some();
        if federated && !self.policy.include_federated {
            return;
        }
        let config = b.configuration.clone();
        let ignore_filters = config
            .as_ref()
            .is_some_and(|c| c.ignore_subscription_filters());
        let ignore_priorities = config
            .as_ref()
            .is_none_or(|c| c.ignore_subscription_priorities());
        // selectFilter, selectPriority
        let filter = consumer
            .filter
            .clone()
            .filter(|_| !ignore_filters)
            .or_else(|| c.filter_string.clone());
        let priority = if federated {
            consumer.priority
        } else if ignore_priorities {
            DEFAULT_CONSUMER_PRIORITY + self.policy.priority_adjustment
        } else {
            consumer.priority + self.policy.priority_adjustment
        };
        let key = QueueKey {
            address: c.address.to_string(),
            queue: c.name.to_string(),
            routing: c.routing_type,
            filter,
            priority,
        };
        let text = key.text();
        {
            let mut keys = lock(&self.keys);
            keys.0.insert(text.clone(), key);
            keys.1.insert(consumer.sequential_id, text.clone());
        }
        let Some(ops) = self.ops(&b, &text) else {
            return;
        };
        b.tracking
            .entry(text)
            .or_default()
            .add_demand(consumer.sequential_id, &ops);
    }

    /// `afterCloseConsumer`.
    fn consumer_closed(&self, consumer: &ConsumerEntry) {
        let Some(text) = lock(&self.keys).1.remove(&consumer.sequential_id) else {
            return;
        };
        let mut b = lock(&self.base);
        if !b.is_active() {
            return;
        }
        let Some(ops) = self.ops(&b, &text) else {
            return;
        };
        if let Some(e) = b.tracking.get_mut(&text) {
            e.remove_demand(&consumer.sequential_id, &ops);
        }
    }

    /// `afterRemoveBinding`: receivers for the queue close at once.
    fn binding_removed(&self, name: &str) {
        let mut b = lock(&self.base);
        let keys: Vec<String> = lock(&self.keys)
            .0
            .iter()
            .filter(|(_, k)| k.queue == name)
            .map(|(t, _)| t.clone())
            .collect();
        for text in keys {
            let ops = self.ops(&b, &text);
            if let Some(mut e) = b.tracking.remove(&text) {
                match &ops {
                    Some(o) => e.shutdown(false, Some(o)),
                    None => e.shutdown(false, None),
                }
            }
            lock(&self.keys).0.remove(&text);
        }
    }

    /// `afterRemoteQueueAdded`: a queue the peer lacked is there now.
    pub fn remote_queue_added(&self, _address: &str, queue: &str) {
        let Some(broker) = self.federation.upgrade().and_then(|f| f.broker()) else {
            return;
        };
        let mut b = lock(&self.base);
        if !b.is_active() || !self.policy.test_queue(queue) {
            return;
        }
        if broker.post_office().queue(queue).is_none() {
            return;
        }
        let keys: Vec<String> = lock(&self.keys)
            .0
            .iter()
            .filter(|(_, k)| k.queue == queue)
            .map(|(t, _)| t.clone())
            .collect();
        for text in keys {
            let Some(ops) = self.ops(&b, &text) else {
                continue;
            };
            if let Some(e) = b.tracking.get_mut(&text) {
                e.recover(&ops);
            }
        }
    }

    /// `AMQPFederationQueueConsumer.doCreateReceiver`.
    fn new_receiver(
        &self,
        _c: &Connection,
        config: Option<&ConsumerConfiguration>,
        key: &QueueKey,
    ) -> (Arc<Consumer>, LocalLink) {
        let (fed_name, node_id) = self
            .federation
            .upgrade()
            .map(|f| (f.name.clone(), f.node_id().to_owned()))
            .unwrap_or_default();
        let target = fqqn(&key.address, &key.queue);
        let link_name = format!(
            "federation-{fed_name}-policy-{}-queue-receiver-{target}-{node_id}:{}",
            self.policy.name,
            LINK_SEQUENCE_ID.fetch_add(1, Ordering::SeqCst)
        );
        let filter = key
            .filter
            .as_deref()
            .filter(|f| !f.is_empty())
            .map(|f| fields(vec![("jms-selector".to_owned(), selector(f))]));
        let attach = Attach {
            name: link_name.clone(),
            handle: 0,
            role: Role::Receiver,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(receiver_source(target.clone(), key.routing, filter)),
            target: Some(TargetKind::Target(Target {
                address: Some(target.clone()),
                ..Default::default()
            })),
            desired_capabilities: Some(vec![Symbol::from(FEDERATION_QUEUE_RECEIVER)]),
            properties: Some(fields(vec![
                (
                    FEDERATION_RECEIVER_PRIORITY.to_owned(),
                    Value::Int(key.priority),
                ),
                (
                    FEDERATION_POLICY_NAME.to_owned(),
                    Value::string(self.policy.name.clone()),
                ),
            ])),
            ..Default::default()
        };
        let owner: Weak<dyn Owner> = self.me.clone();
        let credits = config.map_or(1000, |c| c.receiver_credits());
        let consumer = Arc::new(Consumer {
            link_name,
            kind: ConsumerKind::Queue { fqqn: target },
            // A pulling receiver (no credit) is given batches instead.
            credits: if credits > 0 { credits } else { 100 },
            credits_low: config.map_or(300, |c| c.receiver_credits_low()).max(0),
            use_modified: config.is_some_and(|c| c.use_modified_for_transient_delivery_errors()),
            drain_on_errors: config.is_none_or(|c| c.drain_on_transient_delivery_errors()),
            quiesce_timeout: config
                .map_or(DEFAULT_LINK_QUIESCE_TIMEOUT, |c| c.link_quiesce_timeout()),
            stop_timeout: config.map_or(0, |c| c.receiver_quiesce_timeout()),
            large_threshold: config.map_or(100 * 1024, |c| c.large_message_threshold()),
            federation: self.federation.clone(),
            owner,
            key: key.text(),
            attached: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        });
        let link = LocalLink {
            attach,
            kind: LocalLinkKind::Federation(FedLink::Consumer(consumer.clone())),
        };
        (consumer, link)
    }

    fn on_timer(&self, timer: Timer) {
        let link = match &timer {
            Timer::Attach { consumer, .. } => {
                attach_timed_out(consumer);
                return;
            }
            Timer::Idle { link, .. } | Timer::Stop { link, .. } => link.clone(),
        };
        let mut b = lock(&self.base);
        let Some(key) = b
            .tracking
            .iter()
            .find(|(_, e)| e.is_current(&link))
            .map(|(k, _)| k.clone())
        else {
            return;
        };
        let Some(ops) = self.ops(&b, &key) else {
            return;
        };
        let Some(e) = b.tracking.get_mut(&key) else {
            return;
        };
        match timer {
            Timer::Idle { generation, .. } => e.idle_expired(&link, generation, &ops.connection),
            Timer::Stop { .. } => {
                if e.state == EntryState::Stopping {
                    e.stopped(&link, false, &ops);
                }
            }
            Timer::Attach { .. } => {}
        }
    }
}

impl Owner for QueueManager {
    fn opened(&self, key: &str, link: &str) {
        if let Some(e) = lock(&self.base).tracking.get_mut(key) {
            e.opened(link);
        }
    }

    fn remote_closed(&self, key: &str, link: &str) {
        if let Some(e) = lock(&self.base).tracking.get_mut(key) {
            e.remote_closed(link);
        }
    }

    fn stopped(&self, key: &str, link: &str, ok: bool) {
        let mut b = lock(&self.base);
        let Some(ops) = self.ops(&b, key) else { return };
        match b.tracking.get_mut(key) {
            Some(e) if e.is_current(link) => e.stopped(link, ok, &ops),
            _ => ops
                .connection
                .handle
                .link_op(link.to_owned(), LinkOp::Close),
        }
    }

    fn start_failed(&self, key: &str, link: &str) {
        let mut b = lock(&self.base);
        let Some(ops) = self.ops(&b, key) else { return };
        if let Some(e) = b.tracking.get_mut(key) {
            e.start_failed(link, &ops);
        }
    }
}

/// The queue manager's broker hooks.
struct QueuePlugin(Weak<QueueManager>);

impl BrokerPlugin for QueuePlugin {
    fn after_create_consumer(&self, consumer: &ConsumerEntry) {
        if let Some(m) = self.0.upgrade() {
            m.consumer_created(consumer);
        }
    }

    fn after_close_consumer(&self, consumer: &ConsumerEntry, _failed: bool) {
        if let Some(m) = self.0.upgrade() {
            m.consumer_closed(consumer);
        }
    }

    fn after_remove_binding(&self, name: &str, _address: &str) {
        if let Some(m) = self.0.upgrade() {
            m.binding_removed(name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_opposing_federation_binding as opposing;

    #[test]
    fn opposing_binding() {
        assert!(opposing("federation.f.policy.p.address.news.node.N1", "N1"));
        assert!(opposing(
            "federation.f.g.policy.p.address.a.b.node.N1",
            "N1"
        ));
        assert!(!opposing(
            "federation.f.policy.p.address.news.node.N2",
            "N1"
        ));
        assert!(!opposing("federation..policy.p.address.news.node.N1", "N1"));
        assert!(!opposing("federation.f.policy.p.address..node.N1", "N1"));
        assert!(!opposing("news", "N1"));
    }
}
