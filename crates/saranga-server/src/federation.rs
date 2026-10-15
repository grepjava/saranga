//! Core federation: `FederationManager`, `Federation`, `FederationUpstream`,
//! `FederationDownstream`, `FederationConnection`, `FederatedAddress`,
//! `FederatedQueue` and `FederatedQueueConsumerImpl`.
//!
//! An upstream is a broker this one consumes from. Its address policies
//! make, for each multicast address here with queues (or with a divert
//! that feeds one), a queue on the upstream broker
//! (`federated.<federation>.<upstream>.<address>.multicast`) and a consumer
//! of it whose messages are routed here with one more hop (`_AMQ_Hops`).
//! Its queue policies make, for each consumer of a queue here, a consumer
//! of the queue of that name upstream, at the local consumer's priority
//! plus the adjustment, whose messages go to the local queue. A downstream
//! asks another broker (`FederationDownstreamConnectMessage`) to make this
//! broker its upstream.
//!
//! Address and queue federations are broker plugins, as in Artemis: they
//! hear of bindings and consumers through the plugin hooks.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use saranga_buffer::JString;
use saranga_buffer::{PropertyValue, SimpleString};
use saranga_client::discovery::DiscoveryConfig;
use saranga_client::{
    Connection, ConnectionOptions, Connector, Consumer, ConsumerOptions, CoreMessage, Delivery,
    Error as ClientError, QueueConfiguration, RoutingType as ClientRoutingType, Session,
    SessionOptions,
};
use saranga_config::{
    FederationAddressPolicyConfig, FederationConfig, FederationConnectionConfig,
    FederationDownstreamConfig, FederationPolicyConfig, FederationQueuePolicyConfig,
    FederationStreamConfig, Match, WildcardConfiguration,
};
use saranga_core_protocol::Packet;
use saranga_core_protocol::packets::cluster_types as wire;
use saranga_core_protocol::transport::{ParamValue, TransportConfiguration};
use tokio::runtime::Handle;
use tokio::sync::{Notify, watch};

use crate::Broker;
use crate::RoutingType;
use crate::plugin::BrokerPlugin;
use crate::postoffice::Transformer;
use crate::queue::Queue;
use crate::registry::ConsumerEntry;

/// `FederatedQueueConsumer.FEDERATION_NAME`: session metadata naming the
/// federation a session consumes for.
pub const FEDERATION_NAME: &str = "federation-name";
/// `FederatedQueueConsumer.FEDERATION_UPSTREAM_NAME`.
pub const FEDERATION_UPSTREAM_NAME: &str = "federation-upstream-name";
/// `FederationDownstream.FEDERATION_DOWNSTREAM_NAME`.
pub const FEDERATION_DOWNSTREAM_NAME: &str = "federation-downstream-name";
/// `FederationDownstreamConnectMessage.UPSTREAM_SUFFIX`.
pub const UPSTREAM_SUFFIX: &str = "-upstream";
/// `FederatedAddress.HDR_HOPS`: how many federations a message crossed,
/// a 4-byte big-endian int.
pub const HDR_HOPS: &str = "_AMQ_Hops";
/// `ChannelImpl.CHANNEL_ID.FEDERATION`.
pub const CHANNEL_FEDERATION: i64 = 4;

/// `ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE`.
const DEFAULT_CONSUMER_WINDOW_SIZE: i32 = 1024 * 1024;
/// The policy-set nesting `deployPolicyRefs` follows.
const MAX_POLICY_DEPTH: usize = 10;
/// `intialConnectDelayMultiplier` and `intialConnectDelayMax` (seconds).
const DELAY_MULTIPLIER: u64 = 2;
const DELAY_MAX: u64 = 30;

/// `FederatedQueueConsumer.getNextDelay`, in seconds.
fn next_delay(delay: u64) -> u64 {
    if delay == 0 {
        1
    } else {
        (delay * DELAY_MULTIPLIER).min(DELAY_MAX)
    }
}

async fn stopped(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|s| *s).await;
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// `Match.getPattern().asPredicate()`: the pattern is anchored at the end
/// only, and `find` takes any match, so a name matches when one of its
/// suffixes does.
pub fn pattern_finds(m: &Match, name: &str) -> bool {
    name.char_indices()
        .map(|(i, _)| i)
        .chain(std::iter::once(name.len()))
        .any(|i| m.matches(&name[i..]))
}

// ---------------------------------------------------------------------------
// The manager
// ---------------------------------------------------------------------------

/// `FederationManager`: the federations of a broker, by name.
#[derive(Default)]
pub struct FederationManager {
    broker: Mutex<Weak<Broker>>,
    federations: Mutex<HashMap<String, Arc<Federation>>>,
    started: AtomicBool,
    /// `federationDownstreamAuthorization`.
    downstream_authorization: Mutex<Vec<String>>,
}

impl FederationManager {
    /// `deploy` and `start`: the configured federations, once the broker
    /// is active.
    pub fn start(
        &self,
        broker: &Arc<Broker>,
        configs: &[FederationConfig],
        authorization: &[String],
    ) {
        *lock(&self.broker) = Arc::downgrade(broker);
        *lock(&self.downstream_authorization) = authorization.to_vec();
        for c in configs {
            self.deploy(c.clone());
        }
        self.started.store(true, Ordering::SeqCst);
        let federations: Vec<_> = lock(&self.federations).values().cloned().collect();
        for f in federations {
            f.start();
        }
    }

    /// `stop`.
    pub fn stop(&self) {
        self.started.store(false, Ordering::SeqCst);
        let federations: Vec<_> = lock(&self.federations).drain().map(|(_, f)| f).collect();
        for f in federations {
            f.stop();
        }
    }

    /// `deploy(FederationConfiguration)`: a federation of a name already
    /// deployed is kept unless its credentials changed.
    pub fn deploy(&self, config: FederationConfig) {
        let Some(broker) = lock(&self.broker).upgrade() else {
            return;
        };
        let existing = lock(&self.federations).get(&config.name).cloned();
        let federation = match existing {
            Some(f)
                if f.shared.config.user == config.user
                    && f.shared.config.password == config.password =>
            {
                f
            }
            Some(_) => {
                self.undeploy(&config.name);
                self.new_federation(&broker, config)
            }
            None => self.new_federation(&broker, config),
        };
        federation.deploy();
    }

    fn new_federation(&self, broker: &Arc<Broker>, config: FederationConfig) -> Arc<Federation> {
        let federation = Arc::new(Federation {
            shared: Arc::new(Shared {
                config,
                broker: Arc::downgrade(broker),
                runtime: Handle::current(),
                wildcards: {
                    let w = broker.post_office().wildcard();
                    WildcardConfiguration {
                        routing_enabled: w.routing_enabled,
                        delimiter: w.delimiter,
                        any_words: w.any_words,
                        single_word: w.single_word,
                    }
                },
            }),
            upstreams: Mutex::default(),
            downstreams: Mutex::default(),
            started: AtomicBool::new(false),
        });
        lock(&self.federations).insert(federation.name().to_owned(), federation.clone());
        if self.started.load(Ordering::SeqCst) {
            federation.start();
        }
        federation
    }

    /// `undeploy`: stops the federation of that name. False if none.
    pub fn undeploy(&self, name: &str) -> bool {
        let removed = lock(&self.federations).remove(name);
        match removed {
            Some(f) => {
                f.stop();
                true
            }
            None => false,
        }
    }

    pub fn get(&self, name: &str) -> Option<Arc<Federation>> {
        lock(&self.federations).get(name).cloned()
    }

    /// `authorizeDownstreamDeployment`: with security on, the subject must
    /// have one of the `downstream-authorization` roles.
    pub fn authorize_downstream(
        &self,
        security_enabled: bool,
        subject: Option<&saranga_security::jaas::Subject>,
    ) -> bool {
        if !security_enabled {
            return true;
        }
        let roles = lock(&self.downstream_authorization);
        subject.is_some_and(|s| s.roles().any(|r| roles.iter().any(|a| a == r)))
    }
}

/// What a federation's parts share.
struct Shared {
    config: FederationConfig,
    broker: Weak<Broker>,
    runtime: Handle,
    wildcards: WildcardConfiguration,
}

impl Shared {
    fn name(&self) -> &str {
        &self.config.name
    }

    /// `Federation.getFederationUser`.
    fn user(&self) -> Option<String> {
        self.config.user.clone()
    }

    fn password(&self) -> Option<String> {
        self.config.password.clone()
    }

    /// `FederatedAbstract.getTransformer`: the transformer a policy names,
    /// made from the broker's transformer of its class.
    fn transformer(&self, transformer_ref: Option<&str>) -> Option<Arc<dyn Transformer>> {
        let t = self.config.transformer(transformer_ref?)?;
        let broker = self.broker.upgrade()?;
        match broker.transformers().get(&t.class_name) {
            Some(make) => Some(make(&t.properties.iter().cloned().collect())),
            None => {
                tracing::warn!(
                    "federation {}: no transformer is registered as {}",
                    self.name(),
                    t.class_name
                );
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Federation
// ---------------------------------------------------------------------------

/// `Federation`: its upstreams and downstreams.
pub struct Federation {
    shared: Arc<Shared>,
    upstreams: Mutex<HashMap<String, Arc<Upstream>>>,
    downstreams: Mutex<HashMap<String, Arc<Downstream>>>,
    started: AtomicBool,
}

impl Federation {
    pub fn name(&self) -> &str {
        self.shared.name()
    }

    pub fn config(&self) -> &FederationConfig {
        &self.shared.config
    }

    pub fn upstream(&self, name: &str) -> Option<Arc<Upstream>> {
        lock(&self.upstreams).get(name).cloned()
    }

    fn start(&self) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        self.deploy();
        let upstreams: Vec<_> = lock(&self.upstreams).values().cloned().collect();
        for u in upstreams {
            u.start();
        }
        let downstreams: Vec<_> = lock(&self.downstreams).values().cloned().collect();
        for d in downstreams {
            d.start();
        }
    }

    fn stop(&self) {
        self.started.store(false, Ordering::SeqCst);
        let upstreams: Vec<_> = lock(&self.upstreams).drain().map(|(_, u)| u).collect();
        for u in upstreams {
            u.stop();
        }
        let downstreams: Vec<_> = lock(&self.downstreams).drain().map(|(_, d)| d).collect();
        for d in downstreams {
            d.stop();
        }
    }

    /// `deploy()`: each upstream and downstream of the configuration; one
    /// already deployed with the same connection keeps it.
    fn deploy(&self) {
        let config = &self.shared.config;
        for u in &config.upstreams {
            self.deploy_upstream(u);
        }
        for d in &config.downstreams {
            self.deploy_downstream(d);
        }
    }

    fn deploy_upstream(&self, config: &FederationStreamConfig) {
        let existing = self.upstream(&config.name);
        let upstream = match existing {
            Some(u) if u.connection.config == config.connection => u,
            _ => {
                self.undeploy(&config.name);
                let upstream = Arc::new(Upstream {
                    federation: self.shared.clone(),
                    config: config.clone(),
                    connection: Arc::new(FederationConnection::new(
                        &self.shared,
                        &config.name,
                        config.connection.clone(),
                    )),
                    addresses: Mutex::default(),
                    queues: Mutex::default(),
                });
                lock(&self.upstreams).insert(config.name.clone(), upstream.clone());
                if self.started.load(Ordering::SeqCst) {
                    upstream.start();
                }
                upstream
            }
        };
        upstream.deploy_policy_refs(&config.policy_refs, 0);
    }

    fn deploy_downstream(&self, config: &FederationDownstreamConfig) {
        let name = &config.stream.name;
        let existing = lock(&self.downstreams).get(name).cloned();
        let downstream = match existing {
            Some(d) if d.connection.config == config.stream.connection => d,
            _ => {
                self.undeploy(name);
                // A matching upstream's connection is shared.
                let shared = config
                    .stream
                    .connection
                    .share_connection
                    .then(|| {
                        lock(&self.upstreams)
                            .values()
                            .find(|u| u.config.connection == config.stream.connection)
                            .map(|u| u.connection.clone())
                    })
                    .flatten();
                let connection = match shared {
                    Some(c) => {
                        c.shared.store(true, Ordering::SeqCst);
                        c
                    }
                    None => Arc::new(FederationConnection::new(
                        &self.shared,
                        name,
                        config.stream.connection.clone(),
                    )),
                };
                let downstream = Arc::new(Downstream {
                    federation: self.shared.clone(),
                    config: config.clone(),
                    connection,
                    started: AtomicBool::new(false),
                    run: Mutex::default(),
                });
                lock(&self.downstreams).insert(name.clone(), downstream.clone());
                if self.started.load(Ordering::SeqCst) {
                    downstream.start();
                }
                downstream
            }
        };
        downstream.deploy();
    }

    /// `undeploy(name)`: the upstream and the downstream of that name.
    fn undeploy(&self, name: &str) {
        let upstream = lock(&self.upstreams).remove(name);
        if let Some(u) = upstream {
            u.stop();
        }
        let downstream = lock(&self.downstreams).remove(name);
        if let Some(d) = downstream {
            d.undeploy();
            d.stop();
        }
    }
}

// ---------------------------------------------------------------------------
// The connection
// ---------------------------------------------------------------------------

enum Target {
    Static(Vec<Connector>),
    Discovery(DiscoveryConfig),
}

/// `FederationConnection`: the `ClientSessionFactory` a stream's sessions
/// share, made when first needed, with its circuit breaker.
pub struct FederationConnection {
    config: FederationConnectionConfig,
    target: Option<Target>,
    /// `isPull`: the first connector says `consumerWindowSize=0`.
    pull: bool,
    /// `consumerWindowSize` from the first connector, if given.
    consumer_window_size: Option<i32>,
    started: AtomicBool,
    shared: AtomicBool,
    factory: tokio::sync::Mutex<Option<Connection>>,
    sessions: AtomicI32,
    circuit: Mutex<Option<(ClientError, Instant)>>,
}

impl FederationConnection {
    fn new(federation: &Shared, name: &str, config: FederationConnectionConfig) -> Self {
        let targets = federation
            .broker
            .upgrade()
            .map(|b| b.bridges().targets())
            .unwrap_or_default();
        let mut first: Option<TransportConfiguration> = None;
        let target = match (&config.discovery_group, &config.static_connectors) {
            (Some(group), _) if config.static_connectors.is_none() => {
                match targets.discovery_groups.iter().find(|d| d.name == *group) {
                    Some(d) => Some(Target::Discovery(d.clone())),
                    None => {
                        tracing::warn!(
                            "AMQ222126: No discovery group found with name {group} bridge will not be deployed."
                        );
                        None
                    }
                }
            }
            (_, Some(names)) => {
                let tcs: Vec<TransportConfiguration> = names
                    .iter()
                    .filter_map(|n| {
                        let found = targets
                            .connectors
                            .iter()
                            .find(|c| c.name.to_string_lossy() == *n)
                            .cloned();
                        if found.is_none() {
                            tracing::warn!(
                                "AMQ222134: No connector defined with name {n}. The bridge will not be deployed."
                            );
                        }
                        found
                    })
                    .collect();
                first = tcs.first().cloned();
                Some(Target::Static(
                    tcs.iter().filter_map(Connector::from_transport).collect(),
                ))
            }
            _ => {
                tracing::warn!("AMQ222175: Bridge {name} could not find configured connectors");
                None
            }
        };
        let consumer_window_size = first
            .as_ref()
            .and_then(|tc| transport_param(tc, "consumerWindowSize").and_then(|v| v.parse().ok()));
        Self {
            target,
            pull: consumer_window_size == Some(0),
            consumer_window_size,
            config,
            started: AtomicBool::new(false),
            shared: AtomicBool::new(false),
            factory: tokio::sync::Mutex::new(None),
            sessions: AtomicI32::new(0),
            circuit: Mutex::new(None),
        }
    }

    fn start(&self) {
        self.started.store(true, Ordering::SeqCst);
    }

    /// `stop`: the factory is closed.
    fn stop(&self, runtime: &Handle) {
        self.started.store(false, Ordering::SeqCst);
        if let Ok(mut slot) = self.factory.try_lock()
            && let Some(c) = slot.take()
        {
            runtime.spawn(async move { c.close().await });
        }
    }

    fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// The locator settings: no reconnection of the client's own; the
    /// federation's consumers connect again themselves.
    fn options(&self) -> ConnectionOptions {
        let c = &self.config;
        let millis = |v: i64| (v >= 0).then(|| Duration::from_millis(v as u64));
        ConnectionOptions {
            client_failure_check_period: millis(c.client_failure_check_period),
            connection_ttl: millis(c.connection_ttl),
            call_timeout: Duration::from_millis(c.call_timeout.max(0) as u64),
            call_failover_timeout: millis(c.call_failover_timeout),
            user: c.username.clone(),
            password: c.password.clone(),
            ha: c.ha,
            reconnect_attempts: 0,
            initial_connect_attempts: c.initial_connect_attempts,
            retry_interval: Duration::from_millis(c.retry_interval.max(0) as u64),
            retry_interval_multiplier: c.retry_interval_multiplier,
            max_retry_interval: Duration::from_millis(c.max_retry_interval.max(0) as u64),
            ..ConnectionOptions::default()
        }
    }

    /// `clientSessionFactory`: the open factory, or a new one, unless the
    /// last attempt failed less than `circuit-breaker-timeout` ago.
    async fn session_factory(&self) -> Result<Connection, ClientError> {
        if !self.is_started() {
            return Err(ClientError::InvalidArgument(
                "AMQ219034: Session creation failed".to_owned(),
            ));
        }
        let mut slot = self.factory.lock().await;
        if let Some(c) = slot.as_ref()
            && c.failure().is_none()
        {
            return Ok(c.clone());
        }
        let timeout = self.config.circuit_breaker_timeout;
        if timeout >= 0
            && let Some((e, until)) = lock(&self.circuit).clone()
            && Instant::now() <= until
        {
            return Err(e);
        }
        let connected = match &self.target {
            Some(Target::Static(connectors)) => {
                Connection::connect_to(connectors.clone(), self.options()).await
            }
            Some(Target::Discovery(d)) => {
                Connection::connect_discovery(d.clone(), self.options()).await
            }
            None => Err(ClientError::InvalidArgument(
                "no connectors to federate from".to_owned(),
            )),
        };
        match connected {
            Ok(c) => {
                *lock(&self.circuit) = None;
                *slot = Some(c.clone());
                Ok(c)
            }
            Err(e) => {
                *lock(&self.circuit) = Some((
                    e.clone(),
                    Instant::now() + Duration::from_millis(timeout.max(0) as u64),
                ));
                Err(e)
            }
        }
    }

    /// A session of the factory was closed: with none left, an unshared
    /// factory is closed too.
    async fn release(&self, conn: &Connection) {
        if self.sessions.fetch_sub(1, Ordering::SeqCst) > 1 || self.shared.load(Ordering::SeqCst) {
            return;
        }
        let mut slot = self.factory.lock().await;
        if self.sessions.load(Ordering::SeqCst) == 0 {
            if slot.as_ref().is_some_and(|c| same_connection(c, conn)) {
                slot.take();
            }
            conn.close().await;
        }
    }

    /// The session options of the factory's sessions: the stream's user,
    /// the locator's window.
    fn session_options(&self, user: Option<String>, password: Option<String>) -> SessionOptions {
        let mut options = SessionOptions {
            username: user,
            password,
            ..SessionOptions::default()
        };
        if let Some(w) = self.consumer_window_size {
            options.consumer_window_size = w;
        }
        options
    }
}

fn same_connection(a: &Connection, b: &Connection) -> bool {
    a.connector() == b.connector() && a.failure().is_some() == b.failure().is_some()
}

/// A connector parameter, with or without the extra-property prefix.
fn transport_param(tc: &TransportConfiguration, name: &str) -> Option<String> {
    let extra = format!("activemq.extraprops.{name}");
    tc.params.iter().find_map(|(k, v)| {
        let k = k.to_string_lossy();
        (k == name || k == extra).then(|| match v {
            ParamValue::String(s) => s.to_string_lossy(),
            ParamValue::Int(i) => i.to_string(),
            ParamValue::Long(l) => l.to_string(),
            ParamValue::Bool(b) => b.to_string(),
        })
    })
}

// ---------------------------------------------------------------------------
// Upstream
// ---------------------------------------------------------------------------

/// `FederationUpstream`: a broker this one consumes from, and the address
/// and queue federations its policies make.
pub struct Upstream {
    federation: Arc<Shared>,
    config: FederationStreamConfig,
    connection: Arc<FederationConnection>,
    addresses: Mutex<HashMap<String, Registered<FederatedAddress>>>,
    queues: Mutex<HashMap<String, Registered<FederatedQueue>>>,
}

/// A federation registered as a broker plugin.
struct Registered<T> {
    federated: Arc<T>,
    plugin: Arc<dyn BrokerPlugin>,
}

impl Upstream {
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// `getUser`: the stream's, or else the federation's.
    fn user(&self) -> Option<String> {
        match &self.config.connection.username {
            Some(u) if !u.is_empty() => Some(u.clone()),
            _ => self.federation.user(),
        }
    }

    fn password(&self) -> Option<String> {
        match &self.config.connection.password {
            Some(p) if !p.is_empty() => Some(p.clone()),
            _ => self.federation.password(),
        }
    }

    /// `getPriorityAdjustment`.
    fn priority_adjustment(&self) -> i32 {
        self.config.connection.priority_adjustment
    }

    /// The names of the queues and addresses federated, for tests and
    /// management.
    pub fn federated_consumers(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for r in lock(&self.addresses).values() {
            out.extend(r.federated.common.keys().into_iter().map(|k| k.queue_name));
        }
        for r in lock(&self.queues).values() {
            out.extend(r.federated.common.keys().into_iter().map(|k| k.queue_name));
        }
        out.sort();
        out
    }

    fn start(&self) {
        self.connection.start();
        let queues: Vec<_> = lock(&self.queues)
            .values()
            .map(|r| r.federated.clone())
            .collect();
        for q in queues {
            q.start();
        }
        let addresses: Vec<_> = lock(&self.addresses)
            .values()
            .map(|r| r.federated.clone())
            .collect();
        for a in addresses {
            a.start();
        }
    }

    fn stop(&self) {
        let addresses: Vec<_> = lock(&self.addresses).drain().map(|(_, r)| r).collect();
        let queues: Vec<_> = lock(&self.queues).drain().map(|(_, r)| r).collect();
        let broker = self.federation.broker.upgrade();
        for r in addresses {
            r.federated.stop();
            if let Some(b) = &broker {
                b.post_office().plugins().unregister(&r.plugin);
            }
        }
        for r in queues {
            r.federated.stop();
            if let Some(b) = &broker {
                b.post_office().plugins().unregister(&r.plugin);
            }
        }
        self.connection.stop(&self.federation.runtime);
    }

    /// `deployPolicyRefs`: policy sets are followed ten deep.
    fn deploy_policy_refs(self: &Arc<Self>, refs: &[String], depth: usize) {
        let mut depth = depth;
        for r in refs {
            match self.federation.config.policy(r) {
                Some(FederationPolicyConfig::Set { policy_refs, .. }) => {
                    if depth < MAX_POLICY_DEPTH {
                        depth += 1;
                        self.deploy_policy_refs(policy_refs, depth);
                    } else {
                        tracing::warn!(
                            "AMQ222281: Core federation upstream {} policy ref {r} are too self referential, avoiding stack overflow",
                            self.name()
                        );
                    }
                }
                Some(FederationPolicyConfig::Queue(p)) => self.deploy_queue(p),
                Some(FederationPolicyConfig::Address(p)) => self.deploy_address(p),
                None => tracing::warn!(
                    "AMQ222279: Core federation upstream {} policy ref {r} could not be resolved in configuration",
                    self.name()
                ),
            }
        }
    }

    fn deploy_queue(self: &Arc<Self>, config: &FederationQueuePolicyConfig) {
        if lock(&self.queues)
            .get(&config.name)
            .is_some_and(|r| r.federated.config == *config)
        {
            return;
        }
        self.undeploy_queue(&config.name);
        let federated = Arc::new(FederatedQueue::new(self, config.clone()));
        let plugin: Arc<dyn BrokerPlugin> = federated.clone();
        lock(&self.queues).insert(
            config.name.clone(),
            Registered {
                federated: federated.clone(),
                plugin: plugin.clone(),
            },
        );
        // federation.register: registered, so started.
        if let Some(b) = self.federation.broker.upgrade() {
            b.register_plugin(plugin);
        }
    }

    fn deploy_address(self: &Arc<Self>, config: &FederationAddressPolicyConfig) {
        if lock(&self.addresses)
            .get(&config.name)
            .is_some_and(|r| r.federated.config == *config)
        {
            return;
        }
        self.undeploy_address(&config.name);
        let federated = Arc::new(FederatedAddress::new(self, config.clone()));
        let plugin: Arc<dyn BrokerPlugin> = federated.clone();
        lock(&self.addresses).insert(
            config.name.clone(),
            Registered {
                federated: federated.clone(),
                plugin: plugin.clone(),
            },
        );
        if let Some(b) = self.federation.broker.upgrade() {
            b.register_plugin(plugin);
        }
    }

    fn undeploy_queue(&self, name: &str) {
        let removed = lock(&self.queues).remove(name);
        if let Some(r) = removed {
            r.federated.stop();
            if let Some(b) = self.federation.broker.upgrade() {
                b.unregister_plugin(&r.plugin);
            }
        }
    }

    fn undeploy_address(&self, name: &str) {
        let removed = lock(&self.addresses).remove(name);
        if let Some(r) = removed {
            r.federated.stop();
            if let Some(b) = self.federation.broker.upgrade() {
                b.unregister_plugin(&r.plugin);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Federated consumers
// ---------------------------------------------------------------------------

/// `FederatedConsumerKey`: what a remote consumer consumes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumerKey {
    pub queue_name: String,
    pub address: String,
    pub routing_type: RoutingType,
    /// The filter of the remote consumer.
    pub filter: Option<String>,
    /// The filter of the queue: the remote one made, or the local one's.
    pub queue_filter: Option<String>,
    pub priority: i32,
}

impl ConsumerKey {
    /// `CompositeAddress.toFullyQualified`.
    fn fqqn(&self) -> String {
        format!("{}::{}", self.address, self.queue_name)
    }
}

/// How a remote consumer's messages are made local.
#[derive(Clone)]
enum Arrival {
    /// `FederatedAddress.addHop`.
    AddHop,
    /// The FQQN of the local queue.
    ToQueue(String),
}

/// What `createRemoteConsumer` is given.
#[derive(Clone)]
struct ConsumerSpec {
    key: ConsumerKey,
    arrival: Arrival,
    transformer: Option<Arc<dyn Transformer>>,
    /// The remote queue an address federation makes, if missing.
    create_queue: Option<QueueConfiguration>,
}

/// `FederatedAbstract`: the remote consumers, counted by demand.
#[derive(Default)]
struct Common {
    state: Mutex<CommonState>,
}

#[derive(Default)]
struct CommonState {
    started: bool,
    consumers: HashMap<ConsumerKey, (RemoteConsumer, i32)>,
}

impl Common {
    fn keys(&self) -> Vec<ConsumerKey> {
        lock(&self.state).consumers.keys().cloned().collect()
    }

    fn start(&self) -> bool {
        let mut st = lock(&self.state);
        !std::mem::replace(&mut st.started, true)
    }

    fn stop(&self) {
        let consumers: Vec<_> = {
            let mut st = lock(&self.state);
            st.started = false;
            st.consumers.drain().map(|(_, (c, _))| c).collect()
        };
        for c in consumers {
            c.close();
        }
    }

    /// `createRemoteConsumer`: one more demand for the key; the first
    /// starts the consumer.
    fn create(&self, upstream: &Arc<Upstream>, spec: ConsumerSpec) {
        let mut st = lock(&self.state);
        if !st.started {
            return;
        }
        let entry = st
            .consumers
            .entry(spec.key.clone())
            .or_insert_with(|| (RemoteConsumer::start(upstream, spec), 0));
        entry.1 += 1;
    }

    /// `removeRemoteConsumer`: one less; none left closes the consumer.
    fn remove(&self, key: &ConsumerKey) {
        let mut st = lock(&self.state);
        let Some(entry) = st.consumers.get_mut(key) else {
            return;
        };
        entry.1 -= 1;
        if entry.1 <= 0
            && let Some((c, _)) = st.consumers.remove(key)
        {
            c.close();
        }
    }
}

/// `FederatedQueueConsumerImpl`: a consumer on the upstream broker, on a
/// task of its own that connects, consumes and connects again.
struct RemoteConsumer {
    stop: watch::Sender<bool>,
}

impl RemoteConsumer {
    fn start(upstream: &Arc<Upstream>, spec: ConsumerSpec) -> Self {
        let (stop, rx) = watch::channel(false);
        let upstream = upstream.clone();
        let runtime = upstream.federation.runtime.clone();
        runtime.spawn(run_consumer(upstream, spec, rx));
        Self { stop }
    }

    /// `close`: the consumer, its session and the factory if unused are
    /// closed.
    fn close(self) {
        let _ = self.stop.send(true);
    }
}

/// A connected remote consumer.
struct Link {
    conn: Connection,
    session: Session,
    consumer: Arc<Consumer>,
    /// Pull mode (`consumerWindowSize=0`): credits given while the local
    /// queue is empty.
    pull: Option<Arc<Pull>>,
}

/// `scheduleCreditOnEmpty`'s state.
struct Pull {
    queue: Queue,
    window: i32,
    pending: AtomicI32,
    again: Notify,
}

enum Ended {
    Stop,
    Failed,
}

/// `start`, `scheduleConnect` and `connectionFailed`: connects at once,
/// then after 1, 2, 4... up to 30 seconds while it fails, and at once again
/// after a connection fails.
async fn run_consumer(
    upstream: Arc<Upstream>,
    spec: ConsumerSpec,
    mut stop: watch::Receiver<bool>,
) {
    let Some(broker) = upstream.federation.broker.upgrade() else {
        return;
    };
    let session_name = format!(
        "federation:{}:{}:{}",
        upstream.federation.name(),
        upstream.name(),
        spec.key.queue_name
    );
    let mut local = crate::session::ServerSession::internal(broker.clone(), &session_name);
    drop(broker);
    let mut delay = 0u64;
    loop {
        if delay > 0 {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(delay)) => {}
                _ = stopped(&mut stop) => return,
            }
        }
        let connected = tokio::select! {
            r = connect(&upstream, &spec) => r,
            _ = stopped(&mut stop) => return,
        };
        let link = match connected {
            Ok(link) => link,
            Err(e) => {
                delay = next_delay(delay);
                tracing::trace!(
                    "federated consumer of {} failed to connect. Scheduling reconnect in {delay} seconds: {e}",
                    spec.key.queue_name
                );
                continue;
            }
        };
        let credits = link.pull.clone().map(|p| {
            let consumer = link.consumer.clone();
            let mut stop = stop.clone();
            upstream.federation.runtime.spawn(async move {
                tokio::select! {
                    _ = credit_on_empty(&consumer, &p) => {}
                    _ = stopped(&mut stop) => {}
                }
            })
        });
        let ended = consume(&upstream, &spec, &link, &mut local, &mut stop).await;
        if let Some(c) = credits {
            c.abort();
        }
        disconnect(&upstream, link).await;
        match ended {
            Ended::Stop => return,
            Ended::Failed => delay = 0,
        }
    }
}

/// `connect`: a session of the upstream's factory, with the federation's
/// metadata, the queue (made first by an address federation) and the
/// consumer.
async fn connect(upstream: &Arc<Upstream>, spec: &ConsumerSpec) -> Result<Link, ClientError> {
    let connection = &upstream.connection;
    let conn = connection.session_factory().await?;
    let session = conn
        .create_session(connection.session_options(upstream.user(), upstream.password()))
        .await?;
    connection.sessions.fetch_add(1, Ordering::SeqCst);
    let setup = async {
        session
            .add_metadata(FEDERATION_NAME, upstream.federation.name())
            .await?;
        session
            .add_metadata(FEDERATION_UPSTREAM_NAME, upstream.name())
            .await?;
        session.start().await?;
        if let Some(q) = &spec.create_queue
            && !session.queue_query(&q.name).await?.exists
        {
            session.create_queue(q.clone()).await?;
        }
        let query = session.queue_query(&spec.key.queue_name).await?;
        if !query.exists {
            return Err(ClientError::ActiveMq {
                code: saranga_client::ErrorType::QueueDoesNotExist as i32,
                message: format!("Queue {} does not exist on remote", spec.key.queue_name),
            });
        }
        let consumer = session
            .create_consumer_with(
                &spec.key.queue_name,
                ConsumerOptions {
                    filter: spec.key.filter.clone(),
                    priority: spec.key.priority,
                    ..ConsumerOptions::default()
                },
            )
            .await?;
        let pull = if consumer.client_window_size() == 0 {
            consumer.set_manual_flow();
            let local = upstream.federation.broker.upgrade().and_then(|b| {
                b.post_office()
                    .queue(query.name.as_deref().unwrap_or(&spec.key.queue_name))
            });
            local.map(|queue| {
                let window = match query.default_consumer_window_size {
                    Some(w) if w > 0 => w,
                    _ => DEFAULT_CONSUMER_WINDOW_SIZE,
                };
                Arc::new(Pull {
                    queue,
                    window,
                    pending: AtomicI32::new(0),
                    again: Notify::new(),
                })
            })
        } else {
            None
        };
        Ok((Arc::new(consumer), pull))
    };
    match setup.await {
        Ok((consumer, pull)) => Ok(Link {
            conn,
            session,
            consumer,
            pull,
        }),
        Err(e) => {
            session.close().await;
            connection.release(&conn).await;
            Err(e)
        }
    }
}

/// `disconnect`: the consumer and session, then the factory if no session
/// of it is left and it is not shared.
async fn disconnect(upstream: &Upstream, link: Link) {
    link.consumer.close().await;
    link.session.close().await;
    upstream.connection.release(&link.conn).await;
}

/// `scheduleCreditOnEmpty`: credits for a window once the local queue is
/// empty, looked at again after 1, 2, 4... up to 30 seconds.
async fn credit_on_empty(consumer: &Consumer, pull: &Pull) {
    loop {
        let mut delay = 0u64;
        loop {
            if delay > 0 {
                tokio::time::sleep(Duration::from_secs(delay)).await;
            }
            if pull.queue.message_count() == 0 {
                consumer.flow_control(pull.window);
                pull.pending.store(pull.window, Ordering::SeqCst);
                break;
            }
            if delay == 0 {
                consumer.reset_if_slow_consumer();
                pull.pending.store(0, Ordering::SeqCst);
            }
            delay = next_delay(delay);
        }
        pull.again.notified().await;
    }
}

/// The consumer's `onMessage` while connected: each message copied with a
/// new ID, transformed, routed here and acknowledged.
async fn consume(
    upstream: &Upstream,
    spec: &ConsumerSpec,
    link: &Link,
    local: &mut crate::session::ServerSession,
    stop: &mut watch::Receiver<bool>,
) -> Ended {
    loop {
        let received = tokio::select! {
            biased;
            _ = stopped(stop) => return Ended::Stop,
            _ = link.conn.failed() => return Ended::Failed,
            r = link.consumer.receive(Duration::ZERO) => r,
        };
        let delivery = match received {
            Ok(Some(d)) => d,
            Ok(None) | Err(_) => return Ended::Failed,
        };
        let text = message_string(&delivery.message);
        match federate(upstream, spec, local, &delivery).await {
            Ok(()) => {
                if link.consumer.acknowledge(&delivery).await.is_err() {
                    return Ended::Failed;
                }
            }
            Err(e) => {
                tracing::warn!("AMQ222305: Error during Core federation of message: {text}. {e}");
                if link.session.rollback(false).await.is_err() {
                    return Ended::Failed;
                }
                continue;
            }
        }
        if let Some(p) = &link.pull
            && p.pending.load(Ordering::SeqCst) > 0
            && p.pending
                .fetch_sub(delivery.flow_control_size(), Ordering::SeqCst)
                - delivery.flow_control_size()
                < 0
        {
            p.again.notify_one();
        }
    }
}

fn message_string(m: &CoreMessage) -> String {
    format!(
        "CoreMessage[messageID={}, durable={}, address={}]",
        m.message_id,
        m.durable,
        m.address
            .as_ref()
            .map(SimpleString::to_string_lossy)
            .unwrap_or_default()
    )
}

/// `message.copy(newID)`, the transformers (the policy's, then the hop or
/// the queue's address) and `postOffice.route(message, true)`.
async fn federate(
    upstream: &Upstream,
    spec: &ConsumerSpec,
    local: &mut crate::session::ServerSession,
    delivery: &Delivery,
) -> Result<(), crate::BrokerError> {
    let Some(broker) = upstream.federation.broker.upgrade() else {
        return Err(crate::BrokerError::internal("the broker is stopping"));
    };
    let mut message = delivery.message.clone();
    message.message_id = broker.post_office().ids().next();
    if let Some(t) = &spec.transformer {
        message = t.transform(message);
    }
    match &spec.arrival {
        Arrival::AddHop => add_hop(&mut message),
        Arrival::ToQueue(fqqn) => message.address = Some(SimpleString::from(fqqn.as_str())),
    }
    let large = if delivery.is_large() {
        let io = |e: std::io::Error| crate::BrokerError::internal(e.to_string());
        let mut writer = broker
            .large_messages()
            .create(message.message_id, message.durable)
            .map_err(io)?;
        for chunk in message.body.chunks(crate::large::CHUNK_SIZE) {
            writer.append(chunk).map_err(io)?;
        }
        let size = message.body.len() as i64;
        message.body = Vec::new();
        let key = SimpleString::from(crate::message::HDR_LARGE_BODY_SIZE);
        if message.properties.get(&key).is_none() {
            message.properties.insert(key, PropertyValue::Long(size));
        }
        Some(writer.finish().map_err(io)?)
    } else {
        None
    };
    local.route_direct(message, large).await?;
    local
        .take_completion()
        .wait()
        .await
        .map_err(|e| crate::BrokerError::internal(e.to_string()))
}

/// `FederatedAddress.addHop`.
fn add_hop(message: &mut CoreMessage) {
    let key = SimpleString::from(HDR_HOPS);
    let hops = match message.properties.get(&key) {
        Some(PropertyValue::Bytes(b)) if b.len() == 4 => {
            i32::from_be_bytes([b[0], b[1], b[2], b[3]])
        }
        _ => 0,
    };
    message
        .properties
        .insert(key, PropertyValue::Bytes((hops + 1).to_be_bytes().to_vec()));
}

// ---------------------------------------------------------------------------
// Address federation
// ---------------------------------------------------------------------------

/// `FederatedAddress`: the multicast addresses with queues here.
pub struct FederatedAddress {
    upstream: Weak<Upstream>,
    config: FederationAddressPolicyConfig,
    common: Common,
    /// The hops filter of the remote queues, none for any.
    filter: Option<String>,
    includes: Vec<Match>,
    excludes: Vec<Match>,
    pull: bool,
    /// `matchingDiverts`: the queues each divert is federating for.
    diverts: Mutex<HashMap<String, HashSet<String>>>,
}

impl FederatedAddress {
    fn new(upstream: &Arc<Upstream>, config: FederationAddressPolicyConfig) -> Self {
        let wildcards = &upstream.federation.wildcards;
        let filter = (config.max_hops != -1)
            .then(|| format!("{HDR_HOPS} IS NULL OR {HDR_HOPS}<{}", config.max_hops));
        Self {
            upstream: Arc::downgrade(upstream),
            includes: config
                .includes
                .iter()
                .map(|m| Match::new(m, wildcards))
                .collect(),
            excludes: config
                .excludes
                .iter()
                .map(|m| Match::new(m, wildcards))
                .collect(),
            pull: upstream.connection.pull,
            filter,
            config,
            common: Common::default(),
            diverts: Mutex::default(),
        }
    }

    /// `start`: every queue and divert binding there is.
    fn start(&self) {
        if !self.common.start() {
            return;
        }
        let Some(broker) = self.broker() else { return };
        for q in broker.post_office().queues() {
            self.queue_bound(&broker, &q);
        }
        if self.config.enable_divert_bindings {
            for d in broker.post_office().diverts() {
                self.divert_bound(&broker, &d.name);
            }
        }
    }

    fn stop(&self) {
        self.common.stop();
        lock(&self.diverts).clear();
    }

    fn broker(&self) -> Option<Arc<Broker>> {
        self.upstream.upgrade()?.federation.broker.upgrade()
    }

    /// `match(address, routingType)`.
    fn matches(&self, address: &str, routing_type: RoutingType) -> bool {
        if routing_type == RoutingType::Anycast {
            tracing::debug!("ignoring unsupported ANYCAST address {address}");
            return false;
        }
        if self.pull {
            tracing::debug!(
                "ignoring MULTICAST address {address} on unsupported pull connection, consumerWindowSize=0"
            );
            return false;
        }
        if self.excludes.iter().any(|m| pattern_finds(m, address)) {
            return false;
        }
        self.includes.is_empty() || self.includes.iter().any(|m| pattern_finds(m, address))
    }

    /// `match(AddressInfo)`: the address's first routing type.
    fn matches_address(&self, broker: &Broker, address: &str) -> Option<RoutingType> {
        let info = broker.post_office().address_info(address)?;
        let routing_type = if info.routing_types.multicast {
            RoutingType::Multicast
        } else {
            RoutingType::Anycast
        };
        self.matches(address, routing_type).then_some(routing_type)
    }

    fn key(&self, upstream: &Upstream, address: &str, routing_type: RoutingType) -> ConsumerKey {
        let route = match routing_type {
            RoutingType::Multicast => "multicast",
            RoutingType::Anycast => "anycast",
        };
        ConsumerKey {
            queue_name: format!(
                "federated.{}.{}.{address}.{route}",
                upstream.federation.name(),
                upstream.name()
            ),
            address: address.to_owned(),
            routing_type,
            filter: None,
            queue_filter: self.filter.clone(),
            priority: 0,
        }
    }

    /// `createRemoteConsumer` for an address: the remote queue is made
    /// first if missing, and messages come with one more hop.
    fn create(&self, address: &str, routing_type: RoutingType) {
        let Some(upstream) = self.upstream.upgrade() else {
            return;
        };
        let key = self.key(&upstream, address, routing_type);
        let mut queue = QueueConfiguration::new(key.queue_name.clone())
            .address(address)
            .routing_type(match routing_type {
                RoutingType::Multicast => ClientRoutingType::Multicast,
                RoutingType::Anycast => ClientRoutingType::Anycast,
            })
            .durable(true)
            .auto_created(false);
        queue.filter_string = key.queue_filter.clone();
        queue.auto_delete = Some(self.config.auto_delete.unwrap_or(true));
        queue.auto_delete_delay = Some(self.config.auto_delete_delay.unwrap_or(3_600_000));
        queue.auto_delete_message_count = Some(self.config.auto_delete_message_count.unwrap_or(-1));
        queue.max_consumers = Some(-1);
        queue.purge_on_no_consumers = Some(false);
        let spec = ConsumerSpec {
            key,
            arrival: Arrival::AddHop,
            transformer: upstream
                .federation
                .transformer(self.config.transformer_ref.as_deref()),
            create_queue: Some(queue),
        };
        self.common.create(&upstream, spec);
    }

    fn remove(&self, address: &str, routing_type: RoutingType) {
        let Some(upstream) = self.upstream.upgrade() else {
            return;
        };
        self.common
            .remove(&self.key(&upstream, address, routing_type));
    }

    /// `afterAddBinding` of a queue binding.
    fn queue_bound(&self, broker: &Broker, queue: &Queue) {
        let c = queue.config();
        if self.matches(&c.address, c.routing_type) {
            self.create(&c.address, c.routing_type);
        }
        if self.config.enable_divert_bindings {
            let diverts: Vec<String> = lock(&self.diverts).keys().cloned().collect();
            for d in diverts {
                self.divert_queue(broker, &d, queue);
            }
        }
    }

    /// `createRemoteConsumer(divertBinding, matchingQueues, queueBinding)`:
    /// a queue the divert forwards to makes demand for the divert's
    /// address, once.
    fn divert_queue(&self, broker: &Broker, divert: &str, queue: &Queue) {
        let Some(d) = broker.post_office().divert(divert) else {
            return;
        };
        let Some(routing_type) = self.matches_address(broker, &d.address) else {
            return;
        };
        let c = queue.config();
        if *c.address != *d.deployment.forwarding_address {
            return;
        }
        let added = lock(&self.diverts)
            .get_mut(divert)
            .is_some_and(|queues| queues.insert(c.name.to_string()));
        if added {
            self.create(&d.address, routing_type);
        }
    }

    /// `afterAddBinding` of a divert binding.
    fn divert_bound(&self, broker: &Broker, divert: &str) {
        let Some(d) = broker.post_office().divert(divert) else {
            return;
        };
        if self.matches_address(broker, &d.address).is_none() {
            return;
        }
        {
            let mut diverts = lock(&self.diverts);
            if diverts.contains_key(divert) {
                return;
            }
            diverts.insert(divert.to_owned(), HashSet::new());
        }
        let forward = d.deployment.forwarding_address.clone();
        for q in broker.post_office().queues() {
            if *q.config().address == *forward {
                self.divert_queue(broker, divert, &q);
            }
        }
    }
}

impl BrokerPlugin for FederatedAddress {
    fn registered(&self, _broker: &Arc<Broker>) {
        self.start();
    }

    fn unregistered(&self, _broker: &Broker) {
        self.stop();
    }

    /// `afterAddAddress`: diverts may come before the address they divert.
    fn after_add_address(&self, info: &crate::postoffice::AddressInfo, _reload: bool) {
        if !self.config.enable_divert_bindings {
            return;
        }
        let Some(broker) = self.broker() else { return };
        if self.matches_address(&broker, &info.name).is_none() {
            return;
        }
        for d in broker.post_office().diverts() {
            if *d.address == *info.name {
                self.divert_bound(&broker, &d.name);
            }
        }
    }

    fn after_add_binding(&self, name: &str, _address: &str) {
        if !lock(&self.common.state).started {
            return;
        }
        let Some(broker) = self.broker() else { return };
        if let Some(q) = broker.post_office().queue(name) {
            self.queue_bound(&broker, &q);
        } else if self.config.enable_divert_bindings {
            self.divert_bound(&broker, name);
        }
    }

    fn before_remove_binding(&self, name: &str, _address: &str) {
        let Some(broker) = self.broker() else { return };
        if let Some(q) = broker.post_office().queue(name) {
            let c = q.config();
            self.remove(&c.address, c.routing_type);
            if self.config.enable_divert_bindings {
                // Each divert forwarding to the queue's address that was
                // federating for the queue gives up its demand. (Artemis
                // 2.57 looks the queue up by its address, so it misses.)
                let diverts: Vec<String> = lock(&self.diverts).keys().cloned().collect();
                for divert in diverts {
                    let Some(d) = broker.post_office().divert(&divert) else {
                        continue;
                    };
                    if *d.deployment.forwarding_address != *c.address {
                        continue;
                    }
                    let removed = lock(&self.diverts)
                        .get_mut(&divert)
                        .is_some_and(|queues| queues.remove(&*c.name));
                    if removed && let Some(t) = self.matches_address(&broker, &d.address) {
                        self.remove(&d.address, t);
                    }
                }
            }
        } else if self.config.enable_divert_bindings {
            let Some(queues) = lock(&self.diverts).remove(name) else {
                return;
            };
            let Some(d) = broker.post_office().divert(name) else {
                return;
            };
            let Some(t) = self.matches_address(&broker, &d.address) else {
                return;
            };
            for q in broker.post_office().queues() {
                let c = q.config();
                if *c.address == *d.deployment.forwarding_address && queues.contains(&*c.name) {
                    self.remove(&d.address, t);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Queue federation
// ---------------------------------------------------------------------------

/// `FederatedQueue.Matcher`.
struct QueueMatcher {
    queue: Option<Match>,
    address: Option<Match>,
}

impl QueueMatcher {
    fn test(&self, queue: &str, address: &str) -> bool {
        self.queue.as_ref().is_none_or(|m| pattern_finds(m, queue))
            && self
                .address
                .as_ref()
                .is_none_or(|m| pattern_finds(m, address))
    }
}

/// `FederatedQueue`: the queues with consumers here.
pub struct FederatedQueue {
    upstream: Weak<Upstream>,
    config: FederationQueuePolicyConfig,
    common: Common,
    includes: Vec<QueueMatcher>,
    excludes: Vec<QueueMatcher>,
    /// The upstream's adjustment plus the policy's (-1 when unset).
    priority_adjustment: i32,
}

impl FederatedQueue {
    fn new(upstream: &Arc<Upstream>, config: FederationQueuePolicyConfig) -> Self {
        let wildcards = &upstream.federation.wildcards;
        let matcher = |m: &saranga_config::FederationQueueMatcher| {
            let compile = |s: &Option<String>| {
                s.as_deref()
                    .filter(|s| !s.is_empty())
                    .map(|s| Match::new(s, wildcards))
            };
            QueueMatcher {
                queue: compile(&m.queue_match),
                address: compile(&m.address_match),
            }
        };
        Self {
            upstream: Arc::downgrade(upstream),
            includes: config.includes.iter().map(matcher).collect(),
            excludes: config.excludes.iter().map(matcher).collect(),
            priority_adjustment: upstream.priority_adjustment()
                + config.priority_adjustment.unwrap_or(-1),
            config,
            common: Common::default(),
        }
    }

    fn broker(&self) -> Option<Arc<Broker>> {
        self.upstream.upgrade()?.federation.broker.upgrade()
    }

    /// `start`: the consumers of every queue.
    fn start(&self) {
        self.common.start();
        let Some(broker) = self.broker() else { return };
        for session in broker.registry().sessions() {
            for c in session.consumers() {
                if !c.browse_only {
                    self.consumer_created(&broker, &c);
                }
            }
        }
    }

    fn stop(&self) {
        self.common.stop();
    }

    fn key(&self, consumer: &ConsumerEntry) -> ConsumerKey {
        let c = consumer.queue.config();
        ConsumerKey {
            queue_name: c.name.to_string(),
            address: c.address.to_string(),
            routing_type: c.routing_type,
            filter: consumer.filter.clone(),
            queue_filter: c.filter_string.clone(),
            priority: consumer.priority + self.priority_adjustment,
        }
    }

    fn matches(&self, queue: &str, address: &str) -> bool {
        if self.excludes.iter().any(|m| m.test(queue, address)) {
            return false;
        }
        self.includes.is_empty() || self.includes.iter().any(|m| m.test(queue, address))
    }

    /// `createRemoteConsumer(ServerConsumer)`: not for a consumer of a
    /// federation's own session, unless `include-federated`.
    fn consumer_created(&self, broker: &Broker, consumer: &ConsumerEntry) {
        if !self.config.include_federated
            && broker
                .session_metadata(&consumer.session, FEDERATION_NAME)
                .is_some()
        {
            return;
        }
        let c = consumer.queue.config();
        if !self.matches(&c.name, &c.address) {
            return;
        }
        let Some(upstream) = self.upstream.upgrade() else {
            return;
        };
        let key = self.key(consumer);
        let spec = ConsumerSpec {
            arrival: Arrival::ToQueue(key.fqqn()),
            transformer: upstream
                .federation
                .transformer(self.config.transformer_ref.as_deref()),
            create_queue: None,
            key,
        };
        self.common.create(&upstream, spec);
    }
}

impl BrokerPlugin for FederatedQueue {
    fn registered(&self, _broker: &Arc<Broker>) {
        self.start();
    }

    fn unregistered(&self, _broker: &Broker) {
        self.stop();
    }

    fn after_create_consumer(&self, consumer: &ConsumerEntry) {
        if let Some(broker) = self.broker() {
            self.consumer_created(&broker, consumer);
        }
    }

    /// `beforeCloseConsumer`.
    fn before_close_consumer(&self, consumer: &ConsumerEntry, _failed: bool) {
        self.common.remove(&self.key(consumer));
    }
}

// ---------------------------------------------------------------------------
// Downstream
// ---------------------------------------------------------------------------

/// `FederationDownstream`: asks another broker to federate from this one
/// (`FederationDownstreamConnectMessage`), and asks again when its
/// connection fails.
pub struct Downstream {
    federation: Arc<Shared>,
    config: FederationDownstreamConfig,
    connection: Arc<FederationConnection>,
    started: AtomicBool,
    run: Mutex<Option<watch::Sender<bool>>>,
}

impl Downstream {
    fn name(&self) -> &str {
        &self.config.stream.name
    }

    fn start(self: &Arc<Self>) {
        self.connection.start();
        self.deploy();
    }

    fn stop(&self) {
        if let Some(stop) = lock(&self.run).take() {
            let _ = stop.send(true);
        }
        self.connection.stop(&self.federation.runtime);
    }

    /// `deploy`: once connected, the message is sent, until undeployed.
    fn deploy(self: &Arc<Self>) {
        if !self.connection.is_started() || self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let message = match self.connect_message() {
            Some(m) => m,
            None => {
                self.started.store(false, Ordering::SeqCst);
                return;
            }
        };
        let (stop, rx) = watch::channel(false);
        if let Some(old) = lock(&self.run).replace(stop) {
            let _ = old.send(true);
        }
        self.federation.runtime.spawn(self.clone().run(message, rx));
        tracing::info!(
            "AMQ222283: Core federation downstream {} has been deployed",
            self.name()
        );
    }

    /// `undeploy`.
    fn undeploy(&self) {
        if self.started.swap(false, Ordering::SeqCst) {
            if let Some(stop) = lock(&self.run).take() {
                let _ = stop.send(true);
            }
            tracing::info!(
                "AMQ222284: Core federation downstream {} has been undeployed",
                self.name()
            );
        }
    }

    /// The `FederationDownstreamConnectMessage`: the federation's name,
    /// credentials, policies and transformers, and this stream with the
    /// connector to connect back with.
    fn connect_message(&self) -> Option<Packet> {
        let config = &self.federation.config;
        let broker = self.federation.broker.upgrade()?;
        let connector = broker
            .bridges()
            .targets()
            .connectors
            .into_iter()
            .find(|c| c.name.to_string_lossy() == self.config.upstream_connector_ref);
        let Some(mut connector) = connector else {
            tracing::warn!(
                "AMQ222282: Core federation downstream {} upstream transport configuration ref {} could not be resolved in configuration",
                self.name(),
                self.config.upstream_connector_ref
            );
            return None;
        };
        // setUpstreamConfiguration: the TLS settings are the other
        // broker's own.
        connector.params.retain(|(k, _)| {
            let k = k.to_string_lossy();
            let ignored = STRIPPED_PARAMS.contains(&k.as_str());
            if ignored {
                tracing::warn!(
                    "AMQ212079: The upstream connector from the downstream federation will ignore url parameter {k}"
                );
            }
            !ignored
        });
        Some(Packet::FederationDownstreamConnectMessage(
            saranga_core_protocol::packets::cluster::FederationDownstreamConnectMessage(
                wire::FederationStreamConnect {
                    name: config.name.as_str().into(),
                    credentials: saranga_core_protocol::Flagged(Some(wire::Credentials {
                        user: config.user.as_deref().map(JString::from),
                        password: config.password.as_deref().map(JString::from),
                    })),

                    federation_policies: config.policies.iter().map(policy_to_wire).collect(),
                    transformer_configurations: config
                        .transformers
                        .iter()
                        .map(|(name, t)| wire::FederationTransformerConfiguration {
                            name: name.as_str().into(),
                            class_name: t.class_name.as_str().into(),
                            properties: t
                                .properties
                                .iter()
                                .map(|(k, v)| (k.as_str().into(), v.as_str().into()))
                                .collect(),
                        })
                        .collect(),
                    stream_configuration: wire::FederationDownstreamConfiguration {
                        stream: stream_to_wire(&self.config.stream),
                        upstream_configuration: connector,
                    },
                },
            ),
        ))
    }

    /// `scheduleConnect`: connects (at once, then after 1, 2, 4... up to
    /// 30 seconds) and sends the message once; when the connection fails,
    /// `connectionFailed` deploys it again.
    async fn run(self: Arc<Self>, message: Packet, mut stop: watch::Receiver<bool>) {
        let mut delay = 0u64;
        loop {
            if delay > 0 {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(delay)) => {}
                    _ = stopped(&mut stop) => return,
                }
            }
            let connected = tokio::select! {
                r = self.connect(&message) => r,
                _ = stopped(&mut stop) => return,
            };
            let (conn, session) = match connected {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!("federation downstream {}: {e}", self.name());
                    delay = next_delay(delay);
                    continue;
                }
            };
            tokio::select! {
                _ = conn.failed() => {}
                _ = stopped(&mut stop) => {
                    session.close().await;
                    self.connection.release(&conn).await;
                    return;
                }
            }
            // connectionFailed: started again, the message sent again.
            session.close().await;
            self.connection.release(&conn).await;
            delay = 0;
        }
    }

    async fn connect(&self, message: &Packet) -> Result<(Connection, Session), ClientError> {
        let conn = self.connection.session_factory().await?;
        let user = match &self.config.stream.connection.username {
            Some(u) if !u.is_empty() => Some(u.clone()),
            _ => self.federation.user(),
        };
        let password = match &self.config.stream.connection.password {
            Some(p) if !p.is_empty() => Some(p.clone()),
            _ => self.federation.password(),
        };
        let session = conn
            .create_session(self.connection.session_options(user, password))
            .await?;
        self.connection.sessions.fetch_add(1, Ordering::SeqCst);
        let setup = async {
            session
                .add_metadata(FEDERATION_NAME, self.federation.name())
                .await?;
            session
                .add_metadata(FEDERATION_DOWNSTREAM_NAME, self.name())
                .await?;
            session.start().await?;
            conn.send_packet(CHANNEL_FEDERATION, message.clone())
        };
        match setup.await {
            Ok(()) => Ok((conn, session)),
            Err(e) => {
                session.close().await;
                self.connection.release(&conn).await;
                Err(e)
            }
        }
    }
}

/// The parameters `setUpstreamConfiguration` takes off the connector.
const STRIPPED_PARAMS: &[&str] = &[
    "sslEnabled",
    "sslProvider",
    "handshake-timeout",
    "keyStorePath",
    "keyStorePassword",
    "keyStoreProvider",
    "keyStoreType",
    "trustStorePath",
    "trustStorePassword",
    "trustStoreProvider",
    "trustStoreType",
];

fn connection_to_wire(c: &FederationConnectionConfig) -> wire::FederationConnectionConfiguration {
    wire::FederationConnectionConfiguration {
        username: c.username.as_deref().map(JString::from),
        password: c.password.as_deref().map(JString::from),
        share_connection: c.share_connection,
        priority_adjustment: c.priority_adjustment,
        client_failure_check_period: c.client_failure_check_period,
        connection_ttl: c.connection_ttl,
        retry_interval: c.retry_interval,
        retry_interval_multiplier: c.retry_interval_multiplier,
        // As Artemis 2.57 encodes it (ARTEMIS-6306): the retry interval in
        // the maximum's place.
        max_retry_interval: c.retry_interval,
        initial_connect_attempts: c.initial_connect_attempts,
        reconnect_attempts: c.reconnect_attempts,
        call_timeout: c.call_timeout,
        call_failover_timeout: c.call_failover_timeout,
    }
}

fn stream_to_wire(s: &FederationStreamConfig) -> wire::FederationStreamConfiguration {
    wire::FederationStreamConfiguration {
        name: s.name.as_str().into(),
        connection_configuration: connection_to_wire(&s.connection),
        policy_refs: s.policy_refs.iter().map(|r| r.as_str().into()).collect(),
    }
}

fn policy_to_wire(p: &FederationPolicyConfig) -> wire::FederationPolicy {
    let opt = |s: &Option<String>| s.as_deref().map(JString::from);
    match p {
        FederationPolicyConfig::Address(a) => {
            wire::FederationPolicy::Address(wire::FederationAddressPolicyConfiguration {
                name: a.name.as_str().into(),
                auto_delete: a.auto_delete,
                auto_delete_delay: a.auto_delete_delay,
                auto_delete_message_count: a.auto_delete_message_count,
                max_hops: a.max_hops,
                transformer_ref: opt(&a.transformer_ref),
                includes: a.includes.iter().map(|m| m.as_str().into()).collect(),
                excludes: a.excludes.iter().map(|m| m.as_str().into()).collect(),
                enable_divert_bindings: a.enable_divert_bindings,
            })
        }
        FederationPolicyConfig::Queue(q) => {
            let matcher = |m: &saranga_config::FederationQueueMatcher| wire::QueueMatcher {
                address_match: opt(&m.address_match),
                queue_match: opt(&m.queue_match),
            };
            wire::FederationPolicy::Queue(wire::FederationQueuePolicyConfiguration {
                name: q.name.as_str().into(),
                include_federated: q.include_federated,
                priority_adjustment: q.priority_adjustment,
                transformer_ref: opt(&q.transformer_ref),
                includes: q.includes.iter().map(matcher).collect(),
                excludes: q.excludes.iter().map(matcher).collect(),
            })
        }
        FederationPolicyConfig::Set { name, policy_refs } => {
            wire::FederationPolicy::PolicySet(wire::FederationPolicySet {
                name: name.as_str().into(),
                policy_refs: policy_refs.iter().map(|r| r.as_str().into()).collect(),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// A downstream from another broker
// ---------------------------------------------------------------------------

fn text(s: &JString) -> String {
    s.to_string_lossy()
}

fn opt_text(s: &Option<JString>) -> Option<String> {
    s.as_ref().map(text)
}

fn policy_from_wire(p: &wire::FederationPolicy) -> FederationPolicyConfig {
    match p {
        wire::FederationPolicy::Address(a) => {
            FederationPolicyConfig::Address(FederationAddressPolicyConfig {
                name: text(&a.name),
                includes: a.includes.iter().map(text).collect(),
                excludes: a.excludes.iter().map(text).collect(),
                auto_delete: a.auto_delete,
                auto_delete_delay: a.auto_delete_delay,
                auto_delete_message_count: a.auto_delete_message_count,
                max_hops: a.max_hops,
                transformer_ref: opt_text(&a.transformer_ref),
                enable_divert_bindings: a.enable_divert_bindings,
            })
        }
        wire::FederationPolicy::Queue(q) => {
            let matcher = |m: &wire::QueueMatcher| saranga_config::FederationQueueMatcher {
                queue_match: opt_text(&m.queue_match),
                address_match: opt_text(&m.address_match),
            };
            FederationPolicyConfig::Queue(FederationQueuePolicyConfig {
                name: text(&q.name),
                include_federated: q.include_federated,
                priority_adjustment: q.priority_adjustment,
                transformer_ref: opt_text(&q.transformer_ref),
                includes: q.includes.iter().map(matcher).collect(),
                excludes: q.excludes.iter().map(matcher).collect(),
            })
        }
        wire::FederationPolicy::PolicySet(s) => FederationPolicyConfig::Set {
            name: text(&s.name),
            policy_refs: s.policy_refs.iter().map(text).collect(),
        },
    }
}

/// `FederationChannelHandler.handlePacket`: the federation configuration
/// a downstream asks for, an upstream back to the broker that sent it.
/// `acceptor` is the host and port of the acceptor the request came in
/// on, used where the connector it names leaves them out. Returns the
/// configuration and the connector its upstream names, which the broker
/// must know (`addConnectorConfiguration`).
pub fn downstream_request(
    message: &wire::FederationStreamConnect<wire::FederationDownstreamConfiguration>,
    acceptor: (Option<String>, Option<u16>),
) -> (FederationConfig, TransportConfiguration) {
    let downstream = &message.stream_configuration;
    let credentials = message.credentials.0.as_ref();
    let mut config = FederationConfig {
        name: format!("{}{UPSTREAM_SUFFIX}", text(&message.name)),
        user: credentials.and_then(|c| opt_text(&c.user)),
        password: credentials.and_then(|c| opt_text(&c.password)),
        ..FederationConfig::default()
    };
    for p in &message.federation_policies {
        config.add_policy(policy_from_wire(p));
    }
    for t in &message.transformer_configurations {
        config.add_transformer(
            text(&t.name),
            saranga_config::TransformerConfig {
                class_name: text(&t.class_name),
                properties: t
                    .properties
                    .iter()
                    .map(|(k, v)| (text(k), text(v)))
                    .collect(),
            },
        );
    }
    // The connector back: the acceptor's settings, then the connector's.
    let tc = &downstream.upstream_configuration;
    let mut params: Vec<(JString, ParamValue)> = Vec::new();
    if let Some(h) = &acceptor.0 {
        params.push(("host".into(), ParamValue::String(h.as_str().into())));
    }
    if let Some(p) = acceptor.1 {
        params.push((
            "port".into(),
            ParamValue::String(p.to_string().as_str().into()),
        ));
    }
    for (k, v) in &tc.params {
        match params.iter_mut().find(|(pk, _)| pk == k) {
            Some(slot) => slot.1 = v.clone(),
            None => params.push((k.clone(), v.clone())),
        }
    }
    let connector_name = format!("{}{UPSTREAM_SUFFIX}{UPSTREAM_SUFFIX}", text(&tc.name));
    let wc = &downstream.stream.connection_configuration;
    let connection = FederationConnectionConfig {
        static_connectors: Some(vec![connector_name.clone()]),
        username: opt_text(&wc.username),
        password: opt_text(&wc.password),
        share_connection: wc.share_connection,
        priority_adjustment: wc.priority_adjustment,
        client_failure_check_period: wc.client_failure_check_period,
        connection_ttl: wc.connection_ttl,
        retry_interval: wc.retry_interval,
        retry_interval_multiplier: wc.retry_interval_multiplier,
        max_retry_interval: wc.max_retry_interval,
        initial_connect_attempts: wc.initial_connect_attempts,
        reconnect_attempts: wc.reconnect_attempts,
        call_timeout: wc.call_timeout,
        call_failover_timeout: wc.call_failover_timeout,
        ..FederationConnectionConfig::default()
    };
    let mut stream = FederationStreamConfig {
        name: format!("{}{UPSTREAM_SUFFIX}", text(&downstream.stream.name)),
        connection,
        policy_refs: Vec::new(),
    };
    for r in &downstream.stream.policy_refs {
        stream.add_policy_ref(text(r));
    }
    config.upstreams.push(stream);
    let connector = TransportConfiguration {
        name: connector_name.as_str().into(),
        factory_class_name: tc.factory_class_name.clone(),
        params,
    };
    (config, connector)
}
