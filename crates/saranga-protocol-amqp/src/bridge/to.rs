//! The to policy managers (`AMQPBridgeToAddressPolicyManager`,
//! `AMQPBridgeToQueuePolicyManager`): broker plugins that keep a sender
//! attached to the peer for each matching address and queue, for as long
//! as it exists (`AMQPBridgeSenderManager`), attached again after a delay
//! when the peer closes it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use saranga_server::RoutingType;
use saranga_server::plugin::BrokerPlugin;
use saranga_server::postoffice::AddressInfo;
use saranga_server::queue::Queue;

use super::config::LinkConfiguration;
use super::policy::{AddressPolicy, QueuePolicy};
use super::{
    BridgeLink, BridgeManager, Connection, DEFAULT_CONSUMER_PRIORITY, LinkState, Owner,
    PolicyManager, Recovery, Sender, SenderKind, watch_attach,
};
use crate::connection::{LocalLink, LocalLinkKind};
use crate::federation::lock;
use crate::messaging::{Source, Target, TargetKind, TerminusDurability, TerminusExpiryPolicy};
use crate::transport::{Attach, ReceiverSettleMode, Role, SenderSettleMode};

/// `AMQPBridgeSender.LINK_SEQUENCE_ID`.
static LINK_SEQUENCE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryState {
    Ready,
    Starting,
    Started,
    Closed,
}

/// One sender and its state (`AMQPBridgeSenderManager`).
struct Entry {
    state: EntryState,
    sender: Option<Arc<Sender>>,
    recovery: Option<Recovery>,
}

impl Default for Entry {
    fn default() -> Self {
        Self {
            state: EntryState::Ready,
            sender: None,
            recovery: None,
        }
    }
}

/// What an entry's transitions need from its manager.
trait Ops {
    fn connection(&self) -> &Connection;
    fn config(&self) -> &LinkConfiguration;
    fn key(&self) -> &str;
    fn create(&self) -> (Arc<Sender>, LocalLink);
    /// Schedules a recovery attempt.
    fn schedule(&self, key: String, generation: u64, ms: i64);
}

impl Entry {
    /// `start`.
    fn start(&mut self, ops: &dyn Ops) {
        if self.state == EntryState::Ready {
            self.create(ops);
        }
    }

    /// `tryCreateBridgeSender`.
    fn create(&mut self, ops: &dyn Ops) {
        self.state = EntryState::Starting;
        let (sender, link) = ops.create();
        tracing::debug!("AMQP Bridge creating sender {}", sender.link_name);
        self.sender = Some(sender.clone());
        ops.connection().handle.open(link);
        let watch: Weak<dyn super::AttachWatch> = Arc::downgrade(&sender) as _;
        watch_attach(ops.connection(), ops.config().link_attach_timeout(), watch);
    }

    fn is_current(&self, link: &str) -> bool {
        self.sender.as_ref().is_some_and(|s| s.link_name == link)
    }

    /// The remote open handler: started, and no more recovery. (Artemis's
    /// `setRemoteOpenHandler` sets the close handler instead, so its
    /// recovery handler lives on, issue 16.)
    fn opened(&mut self, link: &str) {
        if !self.is_current(link) {
            return;
        }
        if self.state == EntryState::Starting {
            self.state = EntryState::Started;
        }
        self.recovery = None;
    }

    /// The remote close handler: attached again later if recovery allows.
    fn remote_closed(&mut self, link: &str, ops: &dyn Ops) {
        if !self.is_current(link) {
            return;
        }
        self.close_current(ops.connection());
        let config = ops.config();
        if config.is_link_recovery_enabled() && self.state == EntryState::Ready {
            let recovery = self.recovery.get_or_insert_with(|| Recovery::new(config));
            match recovery.next(config) {
                Some((generation, ms)) => ops.schedule(ops.key().to_owned(), generation, ms),
                None => self.recovery = None,
            }
        }
    }

    /// `tryRecoverSender`.
    fn recover(&mut self, generation: u64, ops: &dyn Ops) {
        let Some(r) = &mut self.recovery else { return };
        if r.generation != generation {
            return;
        }
        r.attempted(ops.config());
        if self.state == EntryState::Ready {
            self.create(ops);
        } else {
            self.recovery = None;
        }
    }

    /// `safeCloseCurrentBridgeSender`.
    fn close_current(&mut self, c: &Connection) {
        if let Some(s) = self.sender.take() {
            s.close(c);
        }
        if self.state != EntryState::Closed {
            self.state = EntryState::Ready;
        }
    }

    /// `shutdownNow`; without a connection its link is gone already.
    fn shutdown_now(&mut self, c: Option<&Connection>) {
        if self.state == EntryState::Closed {
            return;
        }
        self.state = EntryState::Closed;
        self.recovery = None;
        match c {
            Some(c) => self.close_current(c),
            None => self.sender = None,
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

/// What both to policy managers keep (`AMQPBridgeToPolicyManager`).
struct Base<V> {
    state: ManagerState,
    connection: Option<Connection>,
    configuration: Option<LinkConfiguration>,
    /// Each entry, with what it sends.
    tracking: HashMap<String, (Entry, V)>,
    plugin: Option<Arc<dyn BrokerPlugin>>,
}

impl<V> Default for Base<V> {
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

impl<V> Base<V> {
    fn is_active(&self) -> bool {
        self.connection.is_some() && self.state == ManagerState::Started
    }

    /// `safeCleanupManagerResources`: every sender closes now.
    fn cleanup(&mut self) {
        let c = self.connection.clone();
        for (_, (mut e, _)) in std::mem::take(&mut self.tracking) {
            e.shutdown_now(c.as_ref());
        }
    }
}

/// A recovery timer, then the manager's handler.
fn spawn_recovery<M: Send + Sync + 'static>(
    manager: Weak<M>,
    key: String,
    generation: u64,
    ms: i64,
    handle: fn(&M, &str, u64),
) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(ms.max(0) as u64)).await;
        if let Some(m) = manager.upgrade() {
            handle(&m, &key, generation);
        }
    });
}

/// A sender's attach: from a local address or FQQN to the peer's address.
fn sender_attach(
    name: String,
    config: &LinkConfiguration,
    local: String,
    remote: String,
    capabilities: Option<Vec<crate::types::Symbol>>,
) -> Attach {
    Attach {
        name,
        handle: 0,
        role: Role::Sender,
        snd_settle_mode: if config.is_using_presettled_senders() {
            SenderSettleMode::Settled
        } else {
            SenderSettleMode::Unsettled
        },
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(Source {
            address: Some(local),
            ..Default::default()
        }),
        target: Some(TargetKind::Target(Target {
            address: Some(remote),
            durable: TerminusDurability::None,
            expiry_policy: TerminusExpiryPolicy::LinkDetach,
            capabilities,
            ..Default::default()
        })),
        initial_delivery_count: Some(0),
        ..Default::default()
    }
}

macro_rules! policy_manager {
    ($manager:ident, $plugin:ident, $value:ty) => {
        impl PolicyManager for $manager {
            fn initialize(&self) {
                let Some(broker) = self.broker() else { return };
                let plugin: Arc<dyn BrokerPlugin> = Arc::new($plugin(self.me.clone()));
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
                b.cleanup();
            }

            fn shutdown(&self) {
                let plugin = {
                    let mut b = lock(&self.base);
                    if b.state == ManagerState::Shutdown {
                        return;
                    }
                    b.state = ManagerState::Shutdown;
                    b.cleanup();
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
                b.cleanup();
            }
        }

        impl Owner for $manager {
            fn opened(&self, key: &str, link: &str) {
                if let Some((e, _)) = lock(&self.base).tracking.get_mut(key) {
                    e.opened(link);
                }
            }

            fn remote_closed(&self, key: &str, link: &str) {
                let mut b = lock(&self.base);
                let Some(ops) = self.ops(&b, key) else { return };
                if let Some((e, _)) = b.tracking.get_mut(key) {
                    e.remote_closed(link, &ops);
                }
            }

            fn stopped(&self, _key: &str, _link: &str, _ok: bool) {}
        }

        impl $manager {
            fn broker(&self) -> Option<Arc<saranga_server::Broker>> {
                self.bridge.upgrade().and_then(|b| b.broker())
            }

            fn on_recover(&self, key: &str, generation: u64) {
                let mut b = lock(&self.base);
                let Some(ops) = self.ops(&b, key) else { return };
                if let Some((e, _)) = b.tracking.get_mut(key) {
                    e.recover(generation, &ops);
                }
            }

            /// Starts the sender of a new match.
            fn track(&self, key: String, value: $value) {
                let mut b = lock(&self.base);
                if !b.is_active() || b.tracking.contains_key(&key) {
                    return;
                }
                b.tracking.insert(key.clone(), (Entry::default(), value));
                let Some(ops) = self.ops(&b, &key) else {
                    return;
                };
                if let Some((e, _)) = b.tracking.get_mut(&key) {
                    e.start(&ops);
                }
            }

            /// The resource is gone: its sender closes now.
            fn untrack(&self, key: &str) {
                let mut b = lock(&self.base);
                if !b.is_active() {
                    return;
                }
                let c = b.connection.clone();
                if let Some((mut e, _)) = b.tracking.remove(key) {
                    e.shutdown_now(c.as_ref());
                }
            }
        }
    };
}

fn bridge_names(bridge: &Weak<BridgeManager>) -> (String, String) {
    bridge
        .upgrade()
        .map(|b| (b.name.clone(), b.node_id.clone()))
        .unwrap_or_default()
}

// ------------------------------------------------------------ addresses

/// `AMQPBridgeToAddressPolicyManager`: a sender for each matching address,
/// consuming a subscription queue of its own.
pub struct ToAddressManager {
    bridge: Weak<BridgeManager>,
    policy: AddressPolicy,
    me: Weak<ToAddressManager>,
    base: Mutex<Base<RoutingType>>,
}

struct AddressOps<'a> {
    manager: &'a ToAddressManager,
    connection: Connection,
    config: LinkConfiguration,
    address: String,
    routing: RoutingType,
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

    fn create(&self) -> (Arc<Sender>, LocalLink) {
        self.manager
            .new_sender(&self.config, &self.address, self.routing)
    }

    fn schedule(&self, key: String, generation: u64, ms: i64) {
        spawn_recovery(
            self.manager.me.clone(),
            key,
            generation,
            ms,
            ToAddressManager::on_recover,
        );
    }
}

impl ToAddressManager {
    pub fn new(bridge: &Weak<BridgeManager>, policy: AddressPolicy) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            bridge: bridge.clone(),
            policy,
            me: me.clone(),
            base: Mutex::default(),
        })
    }

    fn ops(&self, b: &Base<RoutingType>, address: &str) -> Option<AddressOps<'_>> {
        Some(AddressOps {
            manager: self,
            connection: b.connection.clone()?,
            config: b.configuration.clone()?,
            address: address.to_owned(),
            routing: b.tracking.get(address).map(|(_, r)| *r)?,
        })
    }

    /// `scanManagedResources`: every address.
    fn scan(&self) {
        let Some(broker) = self.broker() else { return };
        for info in broker.post_office().addresses() {
            self.address_added(&info);
        }
    }

    /// `afterAddAddress`.
    fn address_added(&self, info: &AddressInfo) {
        let routing = if info.routing_types.multicast {
            RoutingType::Multicast
        } else {
            RoutingType::Anycast
        };
        let name = info.name.to_string();
        if self.policy.test(&name, routing) {
            self.track(name, routing);
        }
    }

    /// `AMQPBridgeToAddressSender.doCreateSender`, with the subscription
    /// queue `createSenderInfo` names.
    fn new_sender(
        &self,
        config: &LinkConfiguration,
        address: &str,
        routing: RoutingType,
    ) -> (Arc<Sender>, LocalLink) {
        let (bridge_name, node_id) = bridge_names(&self.bridge);
        let common = &self.policy.common;
        let remote = common.remote_address_for(address);
        let durable = self.policy.use_durable_subscriptions;
        let prefix = format!(
            "amqp-bridge-{bridge_name}-policy-{}-address-sender-to-{remote}",
            common.name
        );
        let queue = if durable {
            format!("{prefix}-{node_id}")
        } else {
            format!("{prefix}-{}", uuid::Uuid::new_v4())
        };
        let link_name = format!(
            "amqp-bridge-{bridge_name}-policy-{}-address-sender-{remote}-{node_id}-{}",
            common.name,
            LINK_SEQUENCE_ID.fetch_add(1, Ordering::SeqCst) + 1
        );
        let attach = sender_attach(
            link_name.clone(),
            config,
            address.to_owned(),
            remote,
            common.terminus_capabilities(),
        );
        let owner: Weak<dyn Owner> = self.me.clone();
        let sender = Arc::new(Sender {
            link_name,
            kind: SenderKind::Address {
                address: address.to_owned(),
                queue,
                routing,
                durable,
                filter: common.filter.clone(),
                auto_delete: config.is_auto_delete_durable_subscriptions(),
                auto_delete_delay: config.auto_delete_durable_subscription_delay(),
                auto_delete_message_count: config.auto_delete_durable_subscription_msg_count(),
            },
            priority: common.priority.unwrap_or(DEFAULT_CONSUMER_PRIORITY),
            presettled: config.is_using_presettled_senders(),
            state: LinkState::new(owner, address.to_owned()),
        });
        let link = LocalLink {
            attach,
            kind: LocalLinkKind::Bridge(BridgeLink::Sender(sender.clone())),
        };
        (sender, link)
    }
}

policy_manager!(ToAddressManager, AddressPlugin, RoutingType);

struct AddressPlugin(Weak<ToAddressManager>);

impl BrokerPlugin for AddressPlugin {
    fn after_add_address(&self, info: &AddressInfo, _reload: bool) {
        if let Some(m) = self.0.upgrade() {
            m.address_added(info);
        }
    }

    fn after_remove_address(&self, address: &str, _info: Option<&AddressInfo>) {
        if let Some(m) = self.0.upgrade() {
            m.untrack(address);
        }
    }
}

// ------------------------------------------------------------ queues

/// `AMQPBridgeToQueuePolicyManager`: a sender for each matching queue,
/// consuming it.
pub struct ToQueueManager {
    bridge: Weak<BridgeManager>,
    policy: QueuePolicy,
    me: Weak<ToQueueManager>,
    /// By FQQN, with the queue's address and name.
    base: Mutex<Base<(String, String)>>,
}

struct QueueOps<'a> {
    manager: &'a ToQueueManager,
    connection: Connection,
    config: LinkConfiguration,
    fqqn: String,
    queue: (String, String),
}

impl Ops for QueueOps<'_> {
    fn connection(&self) -> &Connection {
        &self.connection
    }

    fn config(&self) -> &LinkConfiguration {
        &self.config
    }

    fn key(&self) -> &str {
        &self.fqqn
    }

    fn create(&self) -> (Arc<Sender>, LocalLink) {
        self.manager
            .new_sender(&self.config, &self.fqqn, &self.queue.0, &self.queue.1)
    }

    fn schedule(&self, key: String, generation: u64, ms: i64) {
        spawn_recovery(
            self.manager.me.clone(),
            key,
            generation,
            ms,
            ToQueueManager::on_recover,
        );
    }
}

impl ToQueueManager {
    pub fn new(bridge: &Weak<BridgeManager>, policy: QueuePolicy) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            bridge: bridge.clone(),
            policy,
            me: me.clone(),
            base: Mutex::default(),
        })
    }

    fn ops(&self, b: &Base<(String, String)>, fqqn: &str) -> Option<QueueOps<'_>> {
        Some(QueueOps {
            manager: self,
            connection: b.connection.clone()?,
            config: b.configuration.clone()?,
            fqqn: fqqn.to_owned(),
            queue: b.tracking.get(fqqn).map(|(_, q)| q.clone())?,
        })
    }

    /// `scanManagedResources`: every queue.
    fn scan(&self) {
        let Some(broker) = self.broker() else { return };
        for q in broker.post_office().queues() {
            self.queue_added(&q);
        }
    }

    /// `checkQueueForMatch`.
    fn queue_added(&self, queue: &Queue) {
        let c = queue.config();
        let (address, name) = (c.address.to_string(), c.name.to_string());
        if self.policy.test(&address, &name) {
            self.track(format!("{address}::{name}"), (address, name));
        }
    }

    /// `AMQPBridgeToQueueSender.doCreateSender`.
    fn new_sender(
        &self,
        config: &LinkConfiguration,
        fqqn: &str,
        address: &str,
        queue: &str,
    ) -> (Arc<Sender>, LocalLink) {
        let (bridge_name, node_id) = bridge_names(&self.bridge);
        let common = &self.policy.common;
        let remote = common.remote_address_for(queue);
        let link_name = format!(
            "amqp-bridge-{bridge_name}-policy-{}-queue-sender-{remote}-{node_id}-{}",
            common.name,
            LINK_SEQUENCE_ID.fetch_add(1, Ordering::SeqCst) + 1
        );
        let attach = sender_attach(
            link_name.clone(),
            config,
            fqqn.to_owned(),
            remote,
            common.terminus_capabilities(),
        );
        let owner: Weak<dyn Owner> = self.me.clone();
        let sender = Arc::new(Sender {
            link_name,
            kind: SenderKind::Queue {
                address: address.to_owned(),
                queue: queue.to_owned(),
                filter: common.filter.clone(),
            },
            priority: common
                .priority
                .unwrap_or(DEFAULT_CONSUMER_PRIORITY + self.policy.priority_adjustment),
            presettled: config.is_using_presettled_senders(),
            state: LinkState::new(owner, fqqn.to_owned()),
        });
        let link = LocalLink {
            attach,
            kind: LocalLinkKind::Bridge(BridgeLink::Sender(sender.clone())),
        };
        (sender, link)
    }
}

policy_manager!(ToQueueManager, QueuePlugin, (String, String));

struct QueuePlugin(Weak<ToQueueManager>);

impl BrokerPlugin for QueuePlugin {
    fn after_create_queue(&self, queue: &Queue) {
        if let Some(m) = self.0.upgrade() {
            m.queue_added(queue);
        }
    }

    fn after_destroy_queue(&self, queue: &Queue, _check_consumers: bool, _auto_delete: bool) {
        if let Some(m) = self.0.upgrade() {
            let c = queue.config();
            m.untrack(&format!("{}::{}", c.address, c.name));
        }
    }
}
