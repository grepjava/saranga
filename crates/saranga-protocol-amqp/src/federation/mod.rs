//! AMQP federation (`org.apache.activemq.artemis.protocol.amqp.connect.federation`).
//!
//! A broker connection's `<federation>` is the source
//! (`AMQPFederationSource`): over its connection it opens a control link,
//! sends the remote policies on it, and opens events links; its local
//! policies watch for demand here (bindings of matching addresses,
//! consumers of matching queues) and attach federation receivers to the
//! peer, which sends the messages of its address or queue on them. The
//! peer, given the control link, becomes the target
//! (`AMQPFederationTarget`): the policies the source sent are local
//! policies there, attaching receivers back over the same connection.
//!
//! Both ends share [`Federation`]; the connection's task does the links
//! (see `connection.rs`), the policy managers ([`manager`]) decide which.

pub mod config;
pub mod constants;
pub mod manager;
pub mod policy;

use std::collections::HashSet;
use std::sync::{Arc, Mutex, Weak};

use saranga_config::{AmqpFederationElement, WildcardConfiguration};
use saranga_server::Broker;
use saranga_server::plugin::BrokerPlugin;
use saranga_server::postoffice::AddressInfo;

use crate::Settings;
use crate::composite::Fields;
use crate::connection::{ConnectionHandle, LocalLink, LocalLinkKind};
use crate::messaging::{Source, Target, TargetKind, TerminusDurability, TerminusExpiryPolicy};
use crate::transport::{Attach, ReceiverSettleMode, Role, SenderSettleMode};
use crate::types::{Descriptor, Symbol, Value};
use config::Configuration;
use constants::*;
use manager::{AddressManager, Consumer, QueueManager};
use policy::{AddressPolicy, QueuePolicy, Received};

pub(crate) fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The broker's wildcard settings, as the configuration's matcher takes
/// them.
pub(crate) fn wildcards_of(broker: &Broker) -> WildcardConfiguration {
    let w = broker.post_office().wildcard();
    WildcardConfiguration {
        routing_enabled: w.routing_enabled,
        delimiter: w.delimiter,
        any_words: w.any_words,
        single_word: w.single_word,
    }
}

/// A federation link of this end: what the connection makes of the peer's
/// answer.
#[derive(Clone)]
pub enum FedLink {
    /// The source's control link (`asyncCreateControlLink`).
    Control(Arc<Federation>),
    /// The source's events sender (`asyncCreateTargetEventsSender`).
    EventsSender(Arc<Federation>),
    /// The source's events receiver (`asnycCreateTargetEventsReceiver`).
    EventsReceiver(Arc<Federation>),
    /// A federation receiver of a local policy (`AMQPFederationConsumer`).
    Consumer(Arc<Consumer>),
}

impl std::fmt::Debug for FedLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(_) => write!(f, "Control"),
            Self::EventsSender(_) => write!(f, "EventsSender"),
            Self::EventsReceiver(_) => write!(f, "EventsReceiver"),
            Self::Consumer(c) => write!(f, "Consumer({})", c.link_name),
        }
    }
}

/// What the federation knows of its current connection.
#[derive(Clone)]
pub struct Connection {
    pub handle: ConnectionHandle,
    pub remote_container: String,
    pub configuration: Configuration,
    /// `AMQPFederationCapabilities`: the peer's federation version.
    pub remote_version: i32,
}

impl Connection {
    /// `isUseFQQNAddressSubscriptions`: a version 2 peer.
    pub fn fqqn_subscriptions(&self) -> bool {
        self.remote_version >= FEDERATION_V2
    }
}

#[derive(Default)]
struct State {
    started: bool,
    shut_down: bool,
    /// The connection, once the control link is up (`connected`).
    connection: Option<Connection>,
    /// The handle of a connection whose control link is not up yet.
    pending: Option<(ConnectionHandle, Settings)>,
    /// The control link's queue, which commands are routed to.
    control_queue: Option<String>,
    local_address: Vec<Arc<AddressManager>>,
    local_queue: Vec<Arc<QueueManager>>,
    /// The policies the source sends the target.
    remote_address: Vec<AddressPolicy>,
    remote_queue: Vec<QueuePolicy>,
    /// The events sender's queue (`AMQPFederationEventDispatcher`), and
    /// what it watches for.
    events_address: Option<String>,
    events_plugin: Option<Arc<dyn BrokerPlugin>>,
    address_watches: HashSet<String>,
    queue_watches: HashSet<String>,
}

/// One AMQP federation, either end (`AMQPFederation`).
pub struct Federation {
    pub name: String,
    source: bool,
    broker: Weak<Broker>,
    node_id: String,
    pub wildcards: WildcardConfiguration,
    /// The source's `<property>`s.
    properties: Vec<(String, Value)>,
    me: Weak<Federation>,
    state: Mutex<State>,
}

impl Federation {
    /// `installFederation`: the source of a broker connection's
    /// `<federation>`, its policies initialized.
    pub fn source(broker: &Arc<Broker>, e: &AmqpFederationElement) -> Arc<Self> {
        let wildcards = wildcards_of(broker);
        let f = Arc::new_cyclic(|me| Self {
            name: e.name.clone(),
            source: true,
            broker: Arc::downgrade(broker),
            node_id: broker.node_id().to_owned(),
            properties: e
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), Value::string(v.clone())))
                .collect(),
            me: me.clone(),
            state: Mutex::new(State::default()),
            wildcards,
        });
        for p in &e.local_address_policies {
            f.add_address_policy(AddressPolicy::from_config(p, &wildcards));
        }
        for p in &e.local_queue_policies {
            f.add_queue_policy(QueuePolicy::from_config(p, &wildcards));
        }
        {
            let mut st = lock(&f.state);
            for p in &e.remote_address_policies {
                if !st.remote_address.iter().any(|x| x.name == p.name) {
                    st.remote_address
                        .push(AddressPolicy::from_config(p, &wildcards));
                }
            }
            for p in &e.remote_queue_policies {
                if !st.remote_queue.iter().any(|x| x.name == p.name) {
                    st.remote_queue
                        .push(QueuePolicy::from_config(p, &wildcards));
                }
            }
        }
        f
    }

    /// The target the peer's control link makes (`AMQPFederationTarget`),
    /// connected and started.
    pub fn target(broker: &Arc<Broker>, name: String, connection: Connection) -> Arc<Self> {
        let f = Arc::new_cyclic(|me| Self {
            name,
            source: false,
            broker: Arc::downgrade(broker),
            node_id: broker.node_id().to_owned(),
            properties: Vec::new(),
            me: me.clone(),
            state: Mutex::new(State::default()),
            wildcards: wildcards_of(broker),
        });
        {
            let mut st = lock(&f.state);
            st.connection = Some(connection);
            st.started = true;
        }
        f
    }

    pub fn is_source(&self) -> bool {
        self.source
    }

    pub fn broker(&self) -> Option<Arc<Broker>> {
        self.broker.upgrade()
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    fn me(&self) -> Option<Arc<Self>> {
        self.me.upgrade()
    }

    pub fn is_started(&self) -> bool {
        lock(&self.state).started
    }

    pub fn connection(&self) -> Option<Connection> {
        lock(&self.state).connection.clone()
    }

    /// `addAddressMatchPolicy`: a local address policy, initialized, and
    /// started if the federation is.
    pub fn add_address_policy(&self, policy: AddressPolicy) {
        let Some(me) = self.me() else { return };
        let m = AddressManager::new(&me, policy);
        let (connected, started) = {
            let mut st = lock(&self.state);
            st.local_address
                .retain(|x| x.policy_name() != m.policy_name());
            st.local_address.push(m.clone());
            (st.connection.clone(), st.started)
        };
        m.initialize();
        if let Some(c) = connected {
            m.connection_restored(&c);
        }
        if started {
            // A heavy operation: off the connection's task.
            tokio::spawn(async move { m.start() });
        }
    }

    /// `addQueueMatchPolicy`.
    pub fn add_queue_policy(&self, policy: QueuePolicy) {
        let Some(me) = self.me() else { return };
        let m = QueueManager::new(&me, policy);
        let (connected, started) = {
            let mut st = lock(&self.state);
            st.local_queue
                .retain(|x| x.policy_name() != m.policy_name());
            st.local_queue.push(m.clone());
            (st.connection.clone(), st.started)
        };
        m.initialize();
        if let Some(c) = connected {
            m.connection_restored(&c);
        }
        if started {
            tokio::spawn(async move { m.start() });
        }
    }

    fn managers(&self) -> (Vec<Arc<AddressManager>>, Vec<Arc<QueueManager>>) {
        let st = lock(&self.state);
        (st.local_address.clone(), st.local_queue.clone())
    }

    /// `start`: the policy managers start.
    pub fn start(&self) {
        {
            let mut st = lock(&self.state);
            if st.started || st.shut_down {
                return;
            }
            st.started = true;
        }
        let (a, q) = self.managers();
        for m in a {
            m.start();
        }
        for m in q {
            m.start();
        }
    }

    /// `stop`: the policy managers stop, closing their receivers.
    pub fn stop(&self) {
        {
            let mut st = lock(&self.state);
            if !st.started {
                return;
            }
            st.started = false;
        }
        let (a, q) = self.managers();
        for m in a {
            m.stop();
        }
        for m in q {
            m.stop();
        }
    }

    /// `shutdown`: terminal; the policy managers go.
    pub fn shutdown(&self) {
        let (a, q, plugin) = {
            let mut st = lock(&self.state);
            if st.shut_down {
                return;
            }
            st.shut_down = true;
            st.started = false;
            st.events_address = None;
            (
                std::mem::take(&mut st.local_address),
                std::mem::take(&mut st.local_queue),
                st.events_plugin.take(),
            )
        };
        for m in a {
            m.shutdown();
        }
        for m in q {
            m.shutdown();
        }
        if let (Some(p), Some(b)) = (plugin, self.broker()) {
            b.unregister_plugin(&p);
        }
    }

    // -------------------------------------------------------- source

    /// `connectionRestored`: on a new connection, the control link.
    pub fn connection_restored(&self, handle: ConnectionHandle, settings: Settings) {
        let Some(me) = self.me() else { return };
        let configuration = Configuration::new(self.properties.clone(), settings.clone());
        lock(&self.state).pending = Some((handle.clone(), settings));
        let mut properties = Fields::default();
        let entries = policy::java_hash_order(vec![
            (FEDERATION_VERSION.to_owned(), Value::Int(FEDERATION_V2)),
            (FEDERATION_CONFIGURATION.to_owned(), configuration.to_map()),
            (FEDERATION_NAME.to_owned(), Value::string(self.name.clone())),
        ]);
        for (k, v) in entries {
            properties.insert(k, v);
        }
        let attach = Attach {
            name: format!(
                "federation-control-link:{}:{}:{}",
                self.name,
                self.node_id,
                uuid::Uuid::new_v4()
            ),
            handle: 0,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Unsettled,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(Source::default()),
            target: Some(TargetKind::Target(dynamic_target(Some(vec![
                Symbol::from(crate::link::capability::TEMP_TOPIC),
            ])))),
            initial_delivery_count: Some(0),
            desired_capabilities: Some(vec![Symbol::from(FEDERATION_CONTROL_LINK)]),
            properties: Some(properties),
            ..Default::default()
        };
        handle.open(LocalLink {
            attach,
            kind: LocalLinkKind::Federation(FedLink::Control(me)),
        });
    }

    /// The control link is up: the federation is connected
    /// (`AMQP_LINK_INITIALIZER_KEY` of the control link). Then the events
    /// links: a sender if there are remote policies, a receiver if there
    /// are local ones.
    pub fn control_attached(&self, remote_container: String, remote_version: i32, queue: String) {
        tracing::debug!(
            "AMQP Federation {} control link attached to {remote_container}, version {remote_version}",
            self.name
        );
        let Some(me) = self.me() else { return };
        let (handle, has_remote, has_local) = {
            let mut st = lock(&self.state);
            let Some((handle, settings)) = st.pending.take() else {
                return;
            };
            st.connection = Some(Connection {
                handle: handle.clone(),
                remote_container,
                configuration: Configuration::new(self.properties.clone(), settings),
                remote_version,
            });
            st.control_queue = Some(queue);
            (
                handle,
                !st.remote_address.is_empty() || !st.remote_queue.is_empty(),
                !st.local_address.is_empty() || !st.local_queue.is_empty(),
            )
        };
        if !self.is_started() {
            return;
        }
        if has_remote {
            handle.open(LocalLink {
                attach: self.events_attach(Role::Sender),
                kind: LocalLinkKind::Federation(FedLink::EventsSender(me.clone())),
            });
        }
        if has_local {
            handle.open(LocalLink {
                attach: self.events_attach(Role::Receiver),
                kind: LocalLinkKind::Federation(FedLink::EventsReceiver(me)),
            });
        }
    }

    /// An events link: a sender to, or a receiver from, a dynamic node.
    fn events_attach(&self, role: Role) -> Attach {
        let which = match role {
            Role::Sender => "sender",
            Role::Receiver => "receiver",
        };
        let node = dynamic_target(Some(vec![Symbol::from(
            crate::link::capability::TEMP_TOPIC,
        )]));
        let (source, target) = match role {
            Role::Sender => (Source::default(), node),
            Role::Receiver => (
                Source {
                    dynamic: true,
                    capabilities: node.capabilities.clone(),
                    durable: TerminusDurability::None,
                    expiry_policy: TerminusExpiryPolicy::LinkDetach,
                    dynamic_node_properties: node.dynamic_node_properties.clone(),
                    ..Default::default()
                },
                Target::default(),
            ),
        };
        Attach {
            name: format!(
                "federation-events-{which}:{}:{}:{}",
                self.name,
                self.node_id,
                uuid::Uuid::new_v4()
            ),
            handle: 0,
            role,
            snd_settle_mode: SenderSettleMode::Settled,
            rcv_settle_mode: ReceiverSettleMode::First,
            source: Some(source),
            target: Some(TargetKind::Target(target)),
            initial_delivery_count: (role == Role::Sender).then_some(0),
            desired_capabilities: Some(vec![Symbol::from(FEDERATION_EVENT_LINK)]),
            ..Default::default()
        }
    }

    /// The events sender's outcome is known (its queue, if the peer took
    /// it): the remote policies go to the peer.
    pub fn events_sender_attached(&self, queue: Option<String>) {
        tracing::debug!(
            "AMQP Federation {} events sender attached: {queue:?}",
            self.name
        );
        if let Some(q) = queue {
            self.register_events_sender(q);
        }
        let (control, handle, address, queues) = {
            let st = lock(&self.state);
            (
                st.control_queue.clone(),
                st.connection.as_ref().map(|c| c.handle.clone()),
                st.remote_address.clone(),
                st.remote_queue.clone(),
            )
        };
        let (Some(control), Some(handle)) = (control, handle) else {
            return;
        };
        for p in &queues {
            handle.route(control.clone(), policy::encode_queue_policy(p));
        }
        for p in &address {
            handle.route(control.clone(), policy::encode_address_policy(p));
        }
    }

    /// The events receiver's outcome is known: the local policies start
    /// looking for demand.
    pub fn events_receiver_attached(&self) {
        tracing::debug!("AMQP Federation {} events receiver attached", self.name);
        let connection = {
            let st = lock(&self.state);
            if !st.started {
                return;
            }
            st.connection.clone()
        };
        let Some(c) = connection else { return };
        let (a, q) = self.managers();
        tokio::spawn(async move {
            for m in a {
                m.connection_restored(&c);
            }
            for m in q {
                m.connection_restored(&c);
            }
        });
    }

    /// `connectionInterrupted`: the connection is gone, and with it every
    /// link.
    pub fn connection_interrupted(&self) {
        let plugin = {
            let mut st = lock(&self.state);
            st.connection = None;
            st.pending = None;
            st.control_queue = None;
            st.events_address = None;
            st.address_watches.clear();
            st.queue_watches.clear();
            st.events_plugin.take()
        };
        if let (Some(p), Some(b)) = (plugin, self.broker()) {
            b.unregister_plugin(&p);
        }
        let (a, q) = self.managers();
        for m in a {
            m.connection_interrupted();
        }
        for m in q {
            m.connection_interrupted();
        }
    }

    // -------------------------------------------------------- events

    /// `registerEventSender`: events go to this queue, and the broker is
    /// watched for what the peer is waiting for.
    pub fn register_events_sender(&self, queue: String) {
        let Some(broker) = self.broker() else { return };
        let plugin: Arc<dyn BrokerPlugin> = Arc::new(Watcher(self.me.clone()));
        {
            let mut st = lock(&self.state);
            st.events_address = Some(queue);
            if st.events_plugin.is_some() {
                return;
            }
            st.events_plugin = Some(plugin.clone());
        }
        broker.register_plugin(plugin);
    }

    /// `registerMissingAddress`.
    pub fn register_missing_address(&self, address: &str) {
        let mut st = lock(&self.state);
        if st.events_address.is_some() {
            st.address_watches.insert(address.to_owned());
        }
    }

    /// `registerMissingQueue`.
    pub fn register_missing_queue(&self, queue: &str) {
        let mut st = lock(&self.state);
        if st.events_address.is_some() {
            st.queue_watches.insert(queue.to_owned());
        }
    }

    fn send_event(&self, payload: Vec<u8>) {
        let st = lock(&self.state);
        if let (Some(a), Some(c)) = (&st.events_address, &st.connection) {
            c.handle.route(a.clone(), payload);
        }
    }

    /// A control or events message from the peer
    /// (`AMQPFederationCommandProcessor`, `AMQPFederationEventProcessor`).
    pub fn received(&self, r: Received) {
        tracing::debug!("AMQP Federation {} received {r:?}", self.name);
        match r {
            Received::AddQueuePolicy(p) => self.add_queue_policy(p),
            Received::AddAddressPolicy(p) => self.add_address_policy(p),
            Received::QueueAdded { address, queue } => {
                let (_, q) = self.managers();
                for m in q {
                    m.remote_queue_added(&address, &queue);
                }
            }
            Received::AddressAdded { address } => {
                let (a, _) = self.managers();
                for m in a {
                    m.remote_address_added(&address);
                }
            }
        }
    }

    /// `signalResourceCreateError`, `signalError`: the source's
    /// connection is made again; the target's closes.
    pub fn signal_error(&self, message: String) {
        let handle = {
            let st = lock(&self.state);
            st.connection
                .as_ref()
                .map(|c| c.handle.clone())
                .or_else(|| st.pending.as_ref().map(|(h, _)| h.clone()))
        };
        if let Some(h) = handle {
            h.link_failed(message);
        }
    }

    /// `prefixControlLinkQueueName`.
    pub fn control_queue_name(&self, suffix: &str) -> String {
        let d = self.wildcards.delimiter;
        format!(
            "{FEDERATION_BASE_VALIDATION_ADDRESS}{d}{FEDERATION_CONTROL_LINK_PREFIX}{d}{suffix}"
        )
    }

    /// `prefixEventsLinkQueueName`.
    pub fn events_queue_name(&self, suffix: &str) -> String {
        let d = self.wildcards.delimiter;
        format!("{FEDERATION_BASE_VALIDATION_ADDRESS}{d}{FEDERATION_EVENTS_LINK_PREFIX}{d}{suffix}")
    }
}

/// A dynamic node deleted when its link closes, as the control and events
/// links ask for.
fn dynamic_target(capabilities: Option<Vec<Symbol>>) -> Target {
    let mut props = Fields::default();
    props.insert(
        LIFETIME_POLICY,
        Value::described(
            Descriptor::Code(crate::messaging::lifetime::DELETE_ON_CLOSE),
            Value::List(Vec::new()),
        ),
    );
    Target {
        dynamic: true,
        capabilities,
        durable: TerminusDurability::None,
        expiry_policy: TerminusExpiryPolicy::LinkDetach,
        dynamic_node_properties: Some(props),
        ..Default::default()
    }
}

/// `AMQPFederationEventDispatcher` as a plugin: an address or queue the
/// peer was waiting for is added, and it is told, once.
struct Watcher(Weak<Federation>);

impl BrokerPlugin for Watcher {
    fn after_add_address(&self, info: &AddressInfo, _reload: bool) {
        let Some(f) = self.0.upgrade() else { return };
        let watched = lock(&f.state).address_watches.remove(&*info.name);
        if watched {
            f.send_event(policy::encode_address_added(&info.name));
        }
    }

    fn after_add_binding(&self, name: &str, address: &str) {
        let Some(f) = self.0.upgrade() else { return };
        let watched = lock(&f.state).queue_watches.remove(name);
        if watched {
            f.send_event(policy::encode_queue_added(address, name));
        }
    }
}
