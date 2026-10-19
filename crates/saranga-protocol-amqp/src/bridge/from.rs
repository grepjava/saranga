//! The from policy managers (`AMQPBridgeFromAddressPolicyManager`,
//! `AMQPBridgeFromQueuePolicyManager`): broker plugins that see demand
//! here (a queue bound to a matching address, a consumer of a matching
//! queue), or take every match as demand when demand tracking is
//! disabled, and keep a receiver attached to the peer for it as
//! `AMQPBridgeReceiverManager` does: created on the first demand, stopped
//! (its credit drained) when the last goes, started again when demand
//! returns, closed after the receiver's idle timeout, and attached again
//! after a delay when the peer closes it while there is demand.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use saranga_server::RoutingType;
use saranga_server::plugin::BrokerPlugin;
use saranga_server::postoffice::AddressInfo;
use saranga_server::queue::{ConsumerId, Queue};
use saranga_server::registry::ConsumerEntry;

use super::config::LinkConfiguration;
use super::policy::{AddressPolicy, QueuePolicy};
use super::{
    BridgeManager, Connection, DEFAULT_CONSUMER_PRIORITY, LinkState, Owner, PolicyManager,
    RECEIVER_PRIORITY, Receiver, Recovery, watch_attach,
};
use crate::composite::Fields;
use crate::connection::{LinkOp, LocalLink, LocalLinkKind};
use crate::federation::lock;
use crate::federation::policy::java_hash_order;
use crate::messaging::{
    DeliveryState, Modified, Source, Target, TargetKind, TerminusDurability, TerminusExpiryPolicy,
};
use crate::transport::{Attach, ReceiverSettleMode, Role, SenderSettleMode};
use crate::types::{Descriptor, Symbol, Value};

/// `AMQPBridgeReceiver.LINK_SEQUENCE_ID`: keeps receivers that would have
/// the same name apart.
static LINK_SEQUENCE_ID: AtomicU64 = AtomicU64::new(0);

/// `AmqpSupport.COPY`: a durable subscription's distribution mode.
const COPY: &str = "copy";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryState {
    Ready,
    Starting,
    Started,
    Stopping,
    Stopped,
    Closed,
}

/// One receiver's demand and state (`AMQPBridgeReceiverManager`).
struct Entry<D> {
    demand: HashSet<D>,
    forced: bool,
    state: EntryState,
    receiver: Option<Arc<Receiver>>,
    /// Bumped to cancel a pending idle timeout.
    idle: u64,
    recovery: Option<Recovery>,
}

impl<D> Default for Entry<D> {
    fn default() -> Self {
        Self {
            demand: HashSet::new(),
            forced: false,
            state: EntryState::Ready,
            receiver: None,
            idle: 0,
            recovery: None,
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
    Recover {
        key: String,
        generation: u64,
        ms: i64,
    },
}

/// What an entry's transitions need from its manager.
trait Ops {
    fn connection(&self) -> &Connection;
    fn config(&self) -> &LinkConfiguration;
    fn key(&self) -> &str;
    fn create(&self) -> (Arc<Receiver>, LocalLink);
    /// The receiver's idle timeout, in milliseconds.
    fn idle_timeout(&self) -> i32;
    fn schedule(&self, timer: Timer);
}

impl<D: Eq + Hash> Entry<D> {
    fn has_demand(&self) -> bool {
        !self.demand.is_empty() || self.forced
    }

    fn force_demand(&mut self, ops: &dyn Ops) {
        if self.forced || self.state == EntryState::Closed {
            return;
        }
        self.forced = true;
        match self.state {
            EntryState::Stopped => self.restart(ops),
            EntryState::Ready => self.create(ops),
            _ => {}
        }
    }

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
        if self.has_demand() || self.state == EntryState::Ready {
            return;
        }
        if self.state != EntryState::Stopping {
            self.try_stop(ops);
        }
    }

    /// `tryCreateBridgeReceiver`.
    fn create(&mut self, ops: &dyn Ops) {
        self.state = EntryState::Starting;
        let (receiver, link) = ops.create();
        tracing::debug!("AMQP Bridge creating receiver {}", receiver.link_name);
        self.receiver = Some(receiver.clone());
        ops.connection().handle.open(link);
        let watch: Weak<dyn super::AttachWatch> = Arc::downgrade(&receiver) as _;
        watch_attach(ops.connection(), ops.config().link_attach_timeout(), watch);
    }

    /// `tryRestartBridgeReceiver`.
    fn restart(&mut self, ops: &dyn Ops) {
        self.state = EntryState::Started;
        self.idle += 1;
        if let Some(r) = &self.receiver {
            ops.connection()
                .handle
                .link_op(r.link_name.clone(), LinkOp::Start);
        }
    }

    /// `tryStopBridgeReceiver`.
    fn try_stop(&mut self, ops: &dyn Ops) {
        let Some(r) = &self.receiver else { return };
        if self.state == EntryState::Stopping {
            return;
        }
        if self.state != EntryState::Closed {
            self.state = EntryState::Stopping;
        }
        ops.connection()
            .handle
            .link_op(r.link_name.clone(), LinkOp::Stop);
        let ms = ops.config().receiver_quiesce_timeout();
        if ms > 0 {
            ops.schedule(Timer::Stop {
                link: r.link_name.clone(),
                ms,
            });
        }
    }

    fn is_current(&self, link: &str) -> bool {
        self.receiver.as_ref().is_some_and(|r| r.link_name == link)
    }

    /// The remote open handler: started, and no more recovery.
    fn opened(&mut self, link: &str) {
        if !self.is_current(link) {
            return;
        }
        if self.state == EntryState::Starting {
            self.state = EntryState::Started;
        }
        self.recovery = None;
    }

    /// The remote close handler: the receiver is forgotten, and attached
    /// again later while there is demand and recovery allows.
    fn remote_closed(&mut self, link: &str, ops: &dyn Ops) {
        if !self.is_current(link) {
            return;
        }
        self.close_current(ops.connection());
        let config = ops.config();
        if self.has_demand() && config.is_link_recovery_enabled() && self.state == EntryState::Ready
        {
            let recovery = self.recovery.get_or_insert_with(|| Recovery::new(config));
            match recovery.next(config) {
                Some((generation, ms)) => ops.schedule(Timer::Recover {
                    key: ops.key().to_owned(),
                    generation,
                    ms,
                }),
                None => self.recovery = None,
            }
        }
    }

    /// `tryRecoverReceiver`.
    fn recover(&mut self, generation: u64, ops: &dyn Ops) {
        let Some(r) = &mut self.recovery else { return };
        if r.generation != generation {
            return;
        }
        r.attempted(ops.config());
        if self.state == EntryState::Ready && self.has_demand() {
            self.create(ops);
        } else {
            self.recovery = None;
        }
    }

    /// `handleBridgeReceiverStopped`.
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
            EntryState::Ready if self.has_demand() => self.create(ops),
            EntryState::Stopped => {
                if self.has_demand() {
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

    fn idle_expired(&mut self, link: &str, generation: u64, c: &Connection) {
        if self.state == EntryState::Stopped && self.is_current(link) && self.idle == generation {
            self.close_current(c);
        }
    }

    /// `safeCloseCurrentBridgeReceiver`.
    fn close_current(&mut self, c: &Connection) {
        if let Some(r) = self.receiver.take() {
            r.close(c);
        }
        if self.state != EntryState::Closed {
            self.state = EntryState::Ready;
        }
        self.idle += 1;
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
        self.recovery = None;
        let Some(ops) = ops else {
            self.receiver = None;
            return;
        };
        match &self.receiver {
            None => {}
            Some(r)
                if stop_first
                    && was != EntryState::Stopped
                    && !crate::connection::ManagedReceiver::is_closed(r.as_ref()) =>
            {
                // Closed once its stop completes (`stopped`).
                ops.connection()
                    .handle
                    .link_op(r.link_name.clone(), LinkOp::Stop);
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

/// What both from policy managers keep (`AMQPBridgePolicyManager`,
/// `AMQPBridgeFromPolicyManager`).
struct Base<K, D> {
    state: ManagerState,
    connection: Option<Connection>,
    configuration: Option<LinkConfiguration>,
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
    /// `isActive`: connected and started.
    fn is_active(&self) -> bool {
        self.connection.is_some() && self.state == ManagerState::Started
    }

    fn tracking_disabled(&self) -> bool {
        self.configuration
            .as_ref()
            .is_some_and(|c| c.is_receiver_demand_tracking_disabled())
    }
}

/// Runs a timer an entry asked for, then the manager's handler.
fn spawn_timer<M: Send + Sync + 'static>(manager: Weak<M>, timer: Timer, handle: fn(&M, Timer)) {
    let delay = match &timer {
        Timer::Idle { ms, .. } | Timer::Stop { ms, .. } => Duration::from_millis(*ms as u64),
        Timer::Recover { ms, .. } => Duration::from_millis(*ms as u64),
    };
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        if let Some(m) = manager.upgrade() {
            handle(&m, timer);
        }
    });
}

/// `OUTCOMES` and `DEFAULT_OUTCOME`.
fn with_outcomes(mut source: Source) -> Source {
    source.default_outcome = Some(DeliveryState::Modified(Modified {
        delivery_failed: Some(true),
        ..Default::default()
    }));
    source.outcomes = Some(
        [
            "amqp:accepted:list",
            "amqp:rejected:list",
            "amqp:released:list",
            "amqp:modified:list",
        ]
        .iter()
        .map(|s| Symbol::from(*s))
        .collect(),
    );
    source
}

/// A JMS selector filter, unless the filter is blank.
fn selector_filter(filter: Option<&str>) -> Option<Fields> {
    let f = filter.filter(|f| !f.trim().is_empty())?;
    let mut fields = Fields::default();
    fields.insert(
        "jms-selector",
        Value::described(
            Descriptor::Code(crate::link::JMS_SELECTOR_CODE),
            Value::String(f.to_owned()),
        ),
    );
    Some(fields)
}

fn priority_properties(priority: Option<i32>) -> Option<Fields> {
    let p = priority?;
    let mut fields = Fields::default();
    for (k, v) in java_hash_order(vec![(RECEIVER_PRIORITY.to_owned(), Value::Int(p))]) {
        fields.insert(k, v);
    }
    Some(fields)
}

fn settle_mode(config: &LinkConfiguration) -> SenderSettleMode {
    if config.is_using_presettled_senders() {
        SenderSettleMode::Settled
    } else {
        SenderSettleMode::Unsettled
    }
}

/// An address's routing type as the policy tests it.
fn routing_of(info: &AddressInfo) -> RoutingType {
    if info.routing_types.multicast {
        RoutingType::Multicast
    } else {
        RoutingType::Anycast
    }
}

// ------------------------------------------------------------ addresses

/// `AMQPBridgeFromAddressPolicyManager`: demand is a queue bound to a
/// matching address; one receiver per address.
pub struct FromAddressManager {
    bridge: Weak<BridgeManager>,
    policy: AddressPolicy,
    me: Weak<FromAddressManager>,
    base: Mutex<Base<String, String>>,
}

struct AddressOps<'a> {
    manager: &'a FromAddressManager,
    connection: Connection,
    config: LinkConfiguration,
    address: String,
}

impl Ops for AddressOps<'_> {
    fn connection(&self) -> &Connection {
        &self.connection
    }

    fn config(&self) -> &LinkConfiguration {
        &self.config
    }

    fn key(&self) -> &str {
        &self.address
    }

    fn create(&self) -> (Arc<Receiver>, LocalLink) {
        self.manager
            .new_receiver(&self.connection, &self.config, &self.address)
    }

    fn idle_timeout(&self) -> i32 {
        self.config.address_receiver_idle_timeout()
    }

    fn schedule(&self, timer: Timer) {
        spawn_timer(self.manager.me.clone(), timer, FromAddressManager::on_timer);
    }
}

impl FromAddressManager {
    pub fn new(bridge: &Weak<BridgeManager>, policy: AddressPolicy) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            bridge: bridge.clone(),
            policy,
            me: me.clone(),
            base: Mutex::default(),
        })
    }

    fn ops(&self, b: &Base<String, String>, address: &str) -> Option<AddressOps<'_>> {
        Some(AddressOps {
            manager: self,
            connection: b.connection.clone()?,
            config: b.configuration.clone()?,
            address: address.to_owned(),
        })
    }

    fn broker(&self) -> Option<Arc<saranga_server::Broker>> {
        self.bridge.upgrade().and_then(|b| b.broker())
    }

    /// `safeCleanupManagerResources`: each receiver stopped first while
    /// connected, unless forced.
    fn cleanup(&self, b: &mut Base<String, String>, force: bool) {
        let tracking = std::mem::take(&mut b.tracking);
        for (address, mut e) in tracking {
            match self.ops(b, &address).filter(|_| !force) {
                Some(ops) => e.shutdown(true, Some(&ops)),
                None => e.shutdown(false, None),
            }
        }
    }

    /// `scanManagedResources`.
    fn scan(&self) {
        let Some(broker) = self.broker() else { return };
        if lock(&self.base).tracking_disabled() {
            // scanAllAddresses
            for info in broker.post_office().addresses() {
                self.address_added(&info);
            }
        } else {
            // scanAllBindings
            for q in broker.post_office().queues() {
                let c = q.config();
                self.binding_added(&c.name, &c.address);
            }
        }
    }

    /// `testIfAddressMatchesPolicy`: address receivers cannot pull, so
    /// nothing matches without credit.
    fn matches(&self, b: &Base<String, String>, address: &str, routing: RoutingType) -> bool {
        self.policy.test(address, routing)
            && b.configuration
                .as_ref()
                .is_some_and(|c| c.receiver_credits() > 0)
    }

    /// `afterAddAddress`: with demand tracking disabled, a matching address
    /// is demand of its own.
    fn address_added(&self, info: &AddressInfo) {
        let address = info.name.to_string();
        let mut b = lock(&self.base);
        if !b.is_active() || !b.tracking_disabled() || !self.matches(&b, &address, routing_of(info))
        {
            return;
        }
        let Some(ops) = self.ops(&b, &address) else {
            return;
        };
        b.tracking
            .entry(address.clone())
            .or_default()
            .force_demand(&ops);
    }

    /// `afterAddBinding` of a queue binding (`checkBindingForMatch`).
    fn binding_added(&self, name: &str, address: &str) {
        let Some(broker) = self.broker() else { return };
        if broker.post_office().queue(name).is_none() {
            return;
        }
        let Some(info) = broker.post_office().address_info(address) else {
            return;
        };
        let mut b = lock(&self.base);
        if !b.is_active() || b.tracking_disabled() || !self.matches(&b, address, routing_of(&info))
        {
            return;
        }
        let Some(ops) = self.ops(&b, address) else {
            return;
        };
        b.tracking
            .entry(address.to_owned())
            .or_default()
            .add_demand(name.to_owned(), &ops);
    }

    /// `afterRemoveBinding`: the binding's demand goes.
    fn binding_removed(&self, name: &str, address: &str) {
        let mut b = lock(&self.base);
        if !b.is_active() || b.tracking_disabled() {
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
        let ops = self.ops(&b, address);
        if let Some(mut e) = b.tracking.remove(address) {
            match &ops {
                Some(o) => e.shutdown(false, Some(o)),
                None => e.shutdown(false, None),
            }
        }
    }

    /// `AMQPBridgeFromAddressReceiver.doCreateReceiver`.
    fn new_receiver(
        &self,
        c: &Connection,
        config: &LinkConfiguration,
        address: &str,
    ) -> (Arc<Receiver>, LocalLink) {
        let (bridge_name, node_id) = self
            .bridge
            .upgrade()
            .map(|b| (b.name.clone(), b.node_id.clone()))
            .unwrap_or_default();
        let common = &self.policy.common;
        let remote = common.remote_address_for(address);
        let durable = self.policy.use_durable_subscriptions;
        // isUseSharedDurableSubscriptions
        let shared = durable && config.is_prefer_shared_durable_subscriptions() && c.shared_subs;
        let prefix = format!(
            "amqp-bridge-{bridge_name}-policy-{}-address-receiver-{remote}-{node_id}",
            common.name
        );
        let link_name = if shared {
            // A shared durable subscription for reconnects, a unique name
            // against stealing.
            format!(
                "{prefix}|{}",
                LINK_SEQUENCE_ID.fetch_add(1, Ordering::SeqCst) + 1
            )
        } else if durable {
            // A stable durable subscription name.
            prefix
        } else {
            format!(
                "{prefix}-{}",
                LINK_SEQUENCE_ID.fetch_add(1, Ordering::SeqCst) + 1
            )
        };
        let mut source = with_outcomes(Source {
            address: Some(remote),
            capabilities: common.terminus_capabilities(),
            filter: selector_filter(common.filter.as_deref()),
            ..Default::default()
        });
        if durable {
            source.durable = TerminusDurability::UnsettledState;
            source.expiry_policy = TerminusExpiryPolicy::Never;
            source.distribution_mode = Some(Symbol::from(COPY));
            if shared {
                source.capabilities = Some(vec![Symbol::from(crate::link::capability::SHARED)]);
            }
        } else {
            source.durable = TerminusDurability::None;
            source.expiry_policy = TerminusExpiryPolicy::LinkDetach;
        }
        let priority = (!config.is_receiver_priority_disabled())
            .then_some(common.priority)
            .flatten();
        let attach = Attach {
            name: link_name.clone(),
            handle: 0,
            role: Role::Receiver,
            snd_settle_mode: settle_mode(config),
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(source),
            target: Some(TargetKind::Target(Target {
                address: Some(address.to_owned()),
                ..Default::default()
            })),
            properties: priority_properties(priority),
            ..Default::default()
        };
        let owner: Weak<dyn Owner> = self.me.clone();
        let receiver = Arc::new(Receiver {
            link_name,
            destination: address.to_owned(),
            queue: None,
            credits: config.receiver_credits(),
            credits_low: config.receiver_credits_low(),
            use_modified: config.is_use_modified_for_transient_delivery_errors(),
            drain_on_errors: config.is_drain_on_transient_delivery_errors(),
            quiesce_timeout: config.link_quiesce_timeout(),
            stop_timeout: config.receiver_quiesce_timeout(),
            large_threshold: config.large_message_threshold(),
            state: LinkState::new(owner, address.to_owned()),
        });
        let link = LocalLink {
            attach,
            kind: LocalLinkKind::Bridge(super::BridgeLink::Receiver(receiver.clone())),
        };
        (receiver, link)
    }

    fn on_timer(&self, timer: Timer) {
        let key = match &timer {
            Timer::Recover { key, .. } => Some(key.clone()),
            Timer::Idle { link, .. } | Timer::Stop { link, .. } => lock(&self.base)
                .tracking
                .iter()
                .find(|(_, e)| e.is_current(link))
                .map(|(k, _)| k.clone()),
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
            Timer::Recover { generation, .. } => e.recover(generation, &ops),
        }
    }
}

impl PolicyManager for FromAddressManager {
    /// `handleManagerInitialized`: the plugin is registered.
    fn initialize(&self) {
        let Some(broker) = self.broker() else { return };
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

    fn start(&self) {
        let scan = {
            let mut b = lock(&self.base);
            if b.state != ManagerState::Stopped {
                return;
            }
            b.state = ManagerState::Started;
            b.is_active()
        };
        if scan {
            self.scan();
        }
    }

    fn stop(&self) {
        let mut b = lock(&self.base);
        if b.state != ManagerState::Started {
            return;
        }
        b.state = ManagerState::Stopped;
        self.cleanup(&mut b, false);
    }

    fn shutdown(&self) {
        let plugin = {
            let mut b = lock(&self.base);
            if b.state == ManagerState::Shutdown {
                return;
            }
            b.state = ManagerState::Shutdown;
            self.cleanup(&mut b, false);
            b.plugin.take()
        };
        if let (Some(p), Some(broker)) = (plugin, self.broker()) {
            broker.unregister_plugin(&p);
        }
    }

    fn connection_restored(&self, c: &Connection) {
        let scan = {
            let mut b = lock(&self.base);
            b.configuration = Some(LinkConfiguration::new(
                c.configuration.clone(),
                self.policy.common.properties.clone(),
            ));
            b.connection = Some(c.clone());
            b.is_active()
        };
        if scan {
            self.scan();
        }
    }

    fn connection_interrupted(&self) {
        let mut b = lock(&self.base);
        b.connection = None;
        self.cleanup(&mut b, true);
    }
}

impl Owner for FromAddressManager {
    fn opened(&self, key: &str, link: &str) {
        if let Some(e) = lock(&self.base).tracking.get_mut(key) {
            e.opened(link);
        }
    }

    fn remote_closed(&self, key: &str, link: &str) {
        let mut b = lock(&self.base);
        let Some(ops) = self.ops(&b, key) else { return };
        if let Some(e) = b.tracking.get_mut(key) {
            e.remote_closed(link, &ops);
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
}

/// The address manager's broker hooks.
struct AddressPlugin(Weak<FromAddressManager>);

impl BrokerPlugin for AddressPlugin {
    fn after_add_address(&self, info: &AddressInfo, _reload: bool) {
        if let Some(m) = self.0.upgrade() {
            m.address_added(info);
        }
    }

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

    fn after_remove_address(&self, address: &str, _info: Option<&AddressInfo>) {
        if let Some(m) = self.0.upgrade() {
            m.address_removed(address);
        }
    }
}

// ------------------------------------------------------------ queues

/// A queue receiver's identity (`AMQPBridgeReceiverInfo`'s equality):
/// consumers alike share one. Its priority is not part of it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct QueueKey {
    address: String,
    queue: String,
    routing: RoutingType,
    remote: String,
    filter: Option<String>,
}

impl QueueKey {
    fn text(&self) -> String {
        format!(
            "{}::{}|{:?}|{}|{}",
            self.address,
            self.queue,
            self.routing,
            self.remote,
            self.filter.as_deref().unwrap_or("")
        )
    }
}

/// Each receiver's key and priority, by the key's text, and each consumer's
/// key text.
type Keys = (
    HashMap<String, (QueueKey, Option<i32>)>,
    HashMap<ConsumerId, String>,
);

/// `AMQPBridgeFromQueuePolicyManager`: demand is a consumer of a matching
/// queue; one receiver per queue and filter.
pub struct FromQueueManager {
    bridge: Weak<BridgeManager>,
    policy: QueuePolicy,
    me: Weak<FromQueueManager>,
    base: Mutex<Base<String, Option<ConsumerId>>>,
    /// The key of each receiver, by its text, and each consumer's.
    keys: Mutex<Keys>,
}

struct QueueOps<'a> {
    manager: &'a FromQueueManager,
    connection: Connection,
    config: LinkConfiguration,
    text: String,
    key: Option<(QueueKey, Option<i32>)>,
}

impl Ops for QueueOps<'_> {
    fn connection(&self) -> &Connection {
        &self.connection
    }

    fn config(&self) -> &LinkConfiguration {
        &self.config
    }

    fn key(&self) -> &str {
        &self.text
    }

    fn create(&self) -> (Arc<Receiver>, LocalLink) {
        let (key, priority) = self.key.as_ref().expect("a queue receiver's key");
        self.manager
            .new_receiver(&self.config, key, *priority, &self.text)
    }

    fn idle_timeout(&self) -> i32 {
        self.config.queue_receiver_idle_timeout()
    }

    fn schedule(&self, timer: Timer) {
        spawn_timer(self.manager.me.clone(), timer, FromQueueManager::on_timer);
    }
}

impl FromQueueManager {
    pub fn new(bridge: &Weak<BridgeManager>, policy: QueuePolicy) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            bridge: bridge.clone(),
            policy,
            me: me.clone(),
            base: Mutex::default(),
            keys: Mutex::default(),
        })
    }

    fn broker(&self) -> Option<Arc<saranga_server::Broker>> {
        self.bridge.upgrade().and_then(|b| b.broker())
    }

    fn ops(&self, b: &Base<String, Option<ConsumerId>>, text: &str) -> Option<QueueOps<'_>> {
        Some(QueueOps {
            manager: self,
            connection: b.connection.clone()?,
            config: b.configuration.clone()?,
            text: text.to_owned(),
            key: lock(&self.keys).0.get(text).cloned(),
        })
    }

    fn cleanup(&self, b: &mut Base<String, Option<ConsumerId>>, force: bool) {
        let tracking = std::mem::take(&mut b.tracking);
        for (text, mut e) in tracking {
            match self.ops(b, &text).filter(|_| !force) {
                Some(ops) => e.shutdown(true, Some(&ops)),
                None => e.shutdown(false, None),
            }
        }
        let mut keys = lock(&self.keys);
        keys.0.clear();
        keys.1.clear();
    }

    /// `scanManagedResources`: every queue, or the consumers of each.
    fn scan(&self) {
        let Some(broker) = self.broker() else { return };
        if lock(&self.base).tracking_disabled() {
            for q in broker.post_office().queues() {
                self.demand(&q, None);
            }
        } else {
            for session in broker.registry().sessions() {
                for c in session.consumers() {
                    self.demand(&c.queue.clone(), Some(&c));
                }
            }
        }
    }

    /// `reactIfDemandMatchesPolicy`: a consumer, or with demand tracking
    /// disabled the queue itself.
    fn demand(&self, queue: &Queue, consumer: Option<&ConsumerEntry>) {
        let c = queue.config();
        let (address, name) = (c.address.to_string(), c.name.to_string());
        let mut b = lock(&self.base);
        if !b.is_active() || !self.policy.test(&address, &name) {
            return;
        }
        let Some(config) = b.configuration.clone() else {
            return;
        };
        // selectFilter: the policy's, else the consumer's, else the queue's.
        let common = &self.policy.common;
        let filter = match &common.filter {
            Some(f) if !f.trim().is_empty() => Some(f.clone()),
            _ => consumer
                .filter(|_| !config.is_ignore_subscription_filters())
                .and_then(|c| c.filter.clone())
                .or_else(|| {
                    c.filter_string
                        .clone()
                        .filter(|_| !config.is_ignore_queue_filters())
                }),
        };
        // selectPriority
        let priority = if config.is_receiver_priority_disabled() {
            None
        } else {
            Some(
                common
                    .priority
                    .unwrap_or(DEFAULT_CONSUMER_PRIORITY + self.policy.priority_adjustment),
            )
        };
        let key = QueueKey {
            remote: common.remote_address_for(&name),
            address,
            queue: name,
            routing: c.routing_type,
            filter,
        };
        let text = key.text();
        {
            let mut keys = lock(&self.keys);
            keys.0.entry(text.clone()).or_insert((key, priority));
            if let Some(c) = consumer {
                keys.1.insert(c.sequential_id, text.clone());
            }
        }
        let Some(ops) = self.ops(&b, &text) else {
            return;
        };
        let e = b.tracking.entry(text).or_default();
        match consumer {
            Some(c) => e.add_demand(Some(c.sequential_id), &ops),
            None => e.force_demand(&ops),
        }
    }

    /// `afterCloseConsumer`.
    fn consumer_closed(&self, consumer: &ConsumerEntry) {
        let Some(text) = lock(&self.keys).1.remove(&consumer.sequential_id) else {
            return;
        };
        let mut b = lock(&self.base);
        if !b.is_active() || b.tracking_disabled() {
            return;
        }
        let Some(ops) = self.ops(&b, &text) else {
            return;
        };
        if let Some(e) = b.tracking.get_mut(&text) {
            e.remove_demand(&Some(consumer.sequential_id), &ops);
        }
    }

    /// `afterRemoveBinding`: receivers for the queue close at once.
    fn binding_removed(&self, name: &str) {
        let mut b = lock(&self.base);
        if !b.is_active() {
            return;
        }
        let texts: Vec<String> = lock(&self.keys)
            .0
            .iter()
            .filter(|(_, (k, _))| k.queue == name)
            .map(|(t, _)| t.clone())
            .collect();
        for text in texts {
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

    /// `AMQPBridgeFromQueueReceiver.doCreateReceiver`.
    fn new_receiver(
        &self,
        config: &LinkConfiguration,
        key: &QueueKey,
        priority: Option<i32>,
        text: &str,
    ) -> (Arc<Receiver>, LocalLink) {
        let (bridge_name, node_id) = self
            .bridge
            .upgrade()
            .map(|b| (b.name.clone(), b.node_id.clone()))
            .unwrap_or_default();
        let common = &self.policy.common;
        let fqqn = format!("{}::{}", key.address, key.queue);
        let link_name = format!(
            "amqp-bridge-{bridge_name}-policy-{}-queue-receiver-{}-{node_id}:{}",
            common.name,
            key.remote,
            LINK_SEQUENCE_ID.fetch_add(1, Ordering::SeqCst)
        );
        let source = with_outcomes(Source {
            address: Some(key.remote.clone()),
            durable: TerminusDurability::None,
            expiry_policy: TerminusExpiryPolicy::LinkDetach,
            capabilities: common.terminus_capabilities(),
            filter: selector_filter(key.filter.as_deref()),
            ..Default::default()
        });
        let attach = Attach {
            name: link_name.clone(),
            handle: 0,
            role: Role::Receiver,
            snd_settle_mode: settle_mode(config),
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(source),
            target: Some(TargetKind::Target(Target {
                address: Some(fqqn.clone()),
                ..Default::default()
            })),
            properties: priority_properties(priority),
            ..Default::default()
        };
        // A pulling receiver (no credit) is given batches as it runs out.
        let (credits, credits_low) = if config.receiver_credits() > 0 {
            (config.receiver_credits(), config.receiver_credits_low())
        } else {
            (config.pull_receiver_batch_size(), 0)
        };
        let owner: Weak<dyn Owner> = self.me.clone();
        let receiver = Arc::new(Receiver {
            link_name,
            destination: fqqn,
            queue: Some(key.queue.clone()),
            credits,
            credits_low,
            use_modified: config.is_use_modified_for_transient_delivery_errors(),
            drain_on_errors: config.is_drain_on_transient_delivery_errors(),
            quiesce_timeout: config.link_quiesce_timeout(),
            stop_timeout: config.receiver_quiesce_timeout(),
            large_threshold: config.large_message_threshold(),
            state: LinkState::new(owner, text.to_owned()),
        });
        let link = LocalLink {
            attach,
            kind: LocalLinkKind::Bridge(super::BridgeLink::Receiver(receiver.clone())),
        };
        (receiver, link)
    }

    fn on_timer(&self, timer: Timer) {
        let key = match &timer {
            Timer::Recover { key, .. } => Some(key.clone()),
            Timer::Idle { link, .. } | Timer::Stop { link, .. } => lock(&self.base)
                .tracking
                .iter()
                .find(|(_, e)| e.is_current(link))
                .map(|(k, _)| k.clone()),
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
            Timer::Recover { generation, .. } => e.recover(generation, &ops),
        }
    }
}

impl PolicyManager for FromQueueManager {
    fn initialize(&self) {
        let Some(broker) = self.broker() else { return };
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

    fn start(&self) {
        let scan = {
            let mut b = lock(&self.base);
            if b.state != ManagerState::Stopped {
                return;
            }
            b.state = ManagerState::Started;
            b.is_active()
        };
        if scan {
            self.scan();
        }
    }

    fn stop(&self) {
        let mut b = lock(&self.base);
        if b.state != ManagerState::Started {
            return;
        }
        b.state = ManagerState::Stopped;
        self.cleanup(&mut b, false);
    }

    fn shutdown(&self) {
        let plugin = {
            let mut b = lock(&self.base);
            if b.state == ManagerState::Shutdown {
                return;
            }
            b.state = ManagerState::Shutdown;
            self.cleanup(&mut b, false);
            b.plugin.take()
        };
        if let (Some(p), Some(broker)) = (plugin, self.broker()) {
            broker.unregister_plugin(&p);
        }
    }

    fn connection_restored(&self, c: &Connection) {
        let scan = {
            let mut b = lock(&self.base);
            b.configuration = Some(LinkConfiguration::new(
                c.configuration.clone(),
                self.policy.common.properties.clone(),
            ));
            b.connection = Some(c.clone());
            b.is_active()
        };
        if scan {
            self.scan();
        }
    }

    fn connection_interrupted(&self) {
        let mut b = lock(&self.base);
        b.connection = None;
        self.cleanup(&mut b, true);
    }
}

impl Owner for FromQueueManager {
    fn opened(&self, key: &str, link: &str) {
        if let Some(e) = lock(&self.base).tracking.get_mut(key) {
            e.opened(link);
        }
    }

    fn remote_closed(&self, key: &str, link: &str) {
        let mut b = lock(&self.base);
        let Some(ops) = self.ops(&b, key) else { return };
        if let Some(e) = b.tracking.get_mut(key) {
            e.remote_closed(link, &ops);
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
}

/// The queue manager's broker hooks.
struct QueuePlugin(Weak<FromQueueManager>);

impl BrokerPlugin for QueuePlugin {
    fn after_create_consumer(&self, consumer: &ConsumerEntry) {
        if let Some(m) = self.0.upgrade()
            && !lock(&m.base).tracking_disabled()
        {
            m.demand(&consumer.queue, Some(consumer));
        }
    }

    fn after_close_consumer(&self, consumer: &ConsumerEntry, _failed: bool) {
        if let Some(m) = self.0.upgrade() {
            m.consumer_closed(consumer);
        }
    }

    fn after_add_binding(&self, name: &str, _address: &str) {
        let Some(m) = self.0.upgrade() else { return };
        if !lock(&m.base).tracking_disabled() {
            return;
        }
        if let Some(q) = m.broker().and_then(|b| b.post_office().queue(name)) {
            m.demand(&q, None);
        }
    }

    fn after_remove_binding(&self, name: &str, _address: &str) {
        if let Some(m) = self.0.upgrade() {
            m.binding_removed(name);
        }
    }
}
