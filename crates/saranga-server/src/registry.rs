//! The broker's connections and their sessions, consumers and producers, as
//! management lists and closes them (`RemotingService.getConnections`,
//! `ActiveMQServer.getSessions`, `ServerSession.getServerConsumers` and
//! `getServerProducers`).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use saranga_security::audit;

use crate::message::now_millis;
use crate::queue::{ConsumerId, ConsumerMetrics, DeliverySink, Queue};
use crate::session::ClientConnection;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Every connection the broker serves, and the running totals.
#[derive(Default)]
pub struct Registry {
    connections: Mutex<BTreeMap<u64, Weak<ClientConnection>>>,
    next_key: AtomicI64,
    total_connections: AtomicI64,
    total_sessions: AtomicI64,
    total_consumers: AtomicI64,
    next_producer_id: AtomicI64,
    plugins: Arc<crate::plugin::Plugins>,
}

/// Keeps a connection listed until dropped.
pub struct Registration {
    registry: Weak<Registry>,
    key: u64,
    connection: Arc<ClientConnection>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(r) = self.registry.upgrade() {
            lock(&r.connections).remove(&self.key);
            r.plugins
                .each(|p| p.after_destroy_connection(&self.connection));
        }
        if audit::resource_enabled() {
            let c = &self.connection;
            let subject = c.subject();
            audit::resource(audit::fill(
                "AMQ601768: {} connection {} for user {} destroyed",
                &[
                    c.protocol(),
                    c.id(),
                    &audit::caller(subject.as_deref(), Some(c.remote_address())),
                ],
            ));
        }
    }
}

impl Registry {
    /// A registry whose connections the plugins are told of.
    pub fn new(plugins: Arc<crate::plugin::Plugins>) -> Self {
        Self {
            plugins,
            ..Self::default()
        }
    }

    pub fn plugins(&self) -> &Arc<crate::plugin::Plugins> {
        &self.plugins
    }

    /// Lists `connection` until the returned registration is dropped
    /// (`RemotingServiceImpl.connectionCreated` / `removeConnection`).
    pub fn add(self: &Arc<Self>, connection: &Arc<ClientConnection>) -> Registration {
        let key = self.next_key.fetch_add(1, Ordering::Relaxed) as u64;
        lock(&self.connections).insert(key, Arc::downgrade(connection));
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        if audit::resource_enabled() {
            audit::resource(audit::fill(
                "AMQ601767: {} connection {} from {} created",
                &[
                    connection.protocol(),
                    connection.id(),
                    connection.remote_address(),
                ],
            ));
        }
        self.plugins.each(|p| p.after_create_connection(connection));
        Registration {
            registry: Arc::downgrade(self),
            key,
            connection: connection.clone(),
        }
    }

    /// The connections, oldest first.
    pub fn connections(&self) -> Vec<Arc<ClientConnection>> {
        lock(&self.connections)
            .values()
            .filter_map(Weak::upgrade)
            .filter(|c| !c.is_failed())
            .collect()
    }

    /// Whether every connection is gone, failed ones included (their
    /// registrations dropped).
    pub fn all_closed(&self) -> bool {
        lock(&self.connections).is_empty()
    }

    /// How many connections are registered, failed ones included.
    pub fn registered(&self) -> usize {
        lock(&self.connections).len()
    }

    pub fn connection(&self, id: &str) -> Option<Arc<ClientConnection>> {
        self.connections().into_iter().find(|c| c.id() == id)
    }

    /// Every session of every connection.
    pub fn sessions(&self) -> Vec<Arc<SessionEntry>> {
        self.connections()
            .iter()
            .flat_map(|c| c.sessions())
            .collect()
    }

    pub fn session(&self, name: &str) -> Option<Arc<SessionEntry>> {
        self.sessions().into_iter().find(|s| s.name == name)
    }

    /// `getTotalConnectionCount`: connections since the broker started.
    pub fn total_connections(&self) -> i64 {
        self.total_connections.load(Ordering::Relaxed)
    }

    pub fn total_sessions(&self) -> i64 {
        self.total_sessions.load(Ordering::Relaxed)
    }

    pub fn total_consumers(&self) -> i64 {
        self.total_consumers.load(Ordering::Relaxed)
    }

    pub(crate) fn session_created(&self) {
        self.total_sessions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn consumer_created(&self) {
        self.total_consumers.fetch_add(1, Ordering::Relaxed);
    }

    /// `ServerProducerImpl.PRODUCER_ID_GENERATOR`.
    pub(crate) fn next_producer_id(&self) -> i64 {
        self.next_producer_id.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// Closes a session from outside its protocol (`ServerSession.close`
/// called by management): `force` as `closeSessionWithID` passes it.
pub type SessionCloser = Box<dyn Fn(bool) + Send + Sync>;

/// A session as management sees it.
pub struct SessionEntry {
    pub name: String,
    pub username: Option<String>,
    /// `getValidatedUser`: the validated user, or the username given.
    pub validated_user: Option<String>,
    pub created: i64,
    pub connection: Weak<ClientConnection>,
    default_address: Mutex<Option<String>>,
    consumers: Mutex<BTreeMap<i64, Arc<ConsumerEntry>>>,
    producers: Mutex<BTreeMap<String, Arc<ProducerEntry>>>,
    /// A Core client older than 2.28 names no producers: they are tracked
    /// by address (`ServerLegacyProducersImpl`).
    legacy_producers: AtomicBool,
    closer: Mutex<Option<SessionCloser>>,
    /// A send or an acknowledgement under way, for the critical analyzer.
    pub critical: crate::critical::Measure,
}

/// `ServerLegacyProducersImpl.MAX_PRODUCER_METRIC_SIZE`.
const MAX_LEGACY_PRODUCERS: usize = 100;

/// `ServerProducer.ANONYMOUS`.
pub const ANONYMOUS: &str = "ANONYMOUS";

impl SessionEntry {
    pub(crate) fn new(
        name: String,
        username: Option<String>,
        validated_user: Option<String>,
        connection: &Arc<ClientConnection>,
        default_address: Option<String>,
    ) -> Self {
        Self {
            name,
            validated_user: validated_user.or_else(|| username.clone()),
            username,
            created: now_millis(),
            connection: Arc::downgrade(connection),
            default_address: Mutex::new(default_address),
            consumers: Mutex::default(),
            producers: Mutex::default(),
            legacy_producers: AtomicBool::new(false),
            closer: Mutex::default(),
            critical: Default::default(),
        }
    }

    pub fn connection(&self) -> Option<Arc<ClientConnection>> {
        self.connection.upgrade()
    }

    pub fn default_address(&self) -> Option<String> {
        lock(&self.default_address).clone()
    }

    pub(crate) fn set_default_address(&self, address: &str) {
        lock(&self.default_address).get_or_insert_with(|| address.to_owned());
    }

    pub fn consumers(&self) -> Vec<Arc<ConsumerEntry>> {
        lock(&self.consumers).values().cloned().collect()
    }

    pub fn producers(&self) -> Vec<Arc<ProducerEntry>> {
        lock(&self.producers).values().cloned().collect()
    }

    pub(crate) fn add_consumer(&self, consumer: Arc<ConsumerEntry>) {
        lock(&self.consumers).insert(consumer.id, consumer);
    }

    pub(crate) fn remove_consumer(&self, id: i64) -> Option<Arc<ConsumerEntry>> {
        lock(&self.consumers).remove(&id)
    }

    /// Producers are tracked by address, as a Core client before 2.28
    /// needs.
    pub fn set_legacy_producers(&self) {
        self.legacy_producers.store(true, Ordering::Relaxed);
    }

    /// `addProducer`.
    pub(crate) fn add_producer(&self, id: i64, name: &str, protocol: &str, address: Option<&str>) {
        if self.legacy_producers.load(Ordering::Relaxed) {
            return;
        }
        lock(&self.producers).insert(
            name.to_owned(),
            Arc::new(ProducerEntry::new(
                id,
                name,
                protocol,
                address.unwrap_or(ANONYMOUS),
            )),
        );
    }

    /// `removeProducer`.
    pub(crate) fn remove_producer(&self, name: &str) {
        if !self.legacy_producers.load(Ordering::Relaxed) {
            lock(&self.producers).remove(name);
        }
    }

    pub(crate) fn clear_producers(&self) {
        lock(&self.producers).clear();
    }

    /// `updateProducerMetrics`: the producer named `sender` sent a message,
    /// or for a legacy client, the session's producer for its address.
    pub(crate) fn producer_sent(
        &self,
        registry: &Registry,
        sender: Option<&str>,
        address: &str,
        user_id: Option<String>,
        size: i64,
    ) {
        let producer = if self.legacy_producers.load(Ordering::Relaxed) {
            let name = format!("{}:{address}", self.name);
            let mut all = lock(&self.producers);
            if !all.contains_key(&name) && all.len() >= MAX_LEGACY_PRODUCERS {
                // MaxSizeMap: the eldest goes.
                let eldest = all.iter().min_by_key(|(_, p)| p.id).map(|(k, _)| k.clone());
                if let Some(k) = eldest {
                    all.remove(&k);
                }
            }
            all.entry(name.clone())
                .or_insert_with(|| {
                    Arc::new(ProducerEntry::new(
                        registry.next_producer_id(),
                        &name,
                        "CORE",
                        address,
                    ))
                })
                .clone()
        } else {
            let Some(p) = sender.and_then(|s| lock(&self.producers).get(s).cloned()) else {
                return;
            };
            p
        };
        producer.sent.fetch_add(1, Ordering::Relaxed);
        producer.sent_size.fetch_add(size, Ordering::Relaxed);
        *lock(&producer.last_message_id) = user_id;
    }

    /// How management closes this session; none for a protocol that has
    /// no way to.
    pub fn set_closer(&self, closer: SessionCloser) {
        *lock(&self.closer) = Some(closer);
    }

    /// `closeSessionWithID`: false when the session's protocol cannot close
    /// it from outside.
    pub fn close(&self, force: bool) -> bool {
        match &*lock(&self.closer) {
            Some(close) => {
                close(force);
                true
            }
            None => false,
        }
    }
}

/// A consumer as management sees it (`ServerConsumer`).
pub struct ConsumerEntry {
    /// The protocol's own consumer ID (`getID`).
    pub id: i64,
    /// The broker-wide ID (`getSequentialID`).
    pub sequential_id: ConsumerId,
    pub session: String,
    pub queue: Queue,
    pub filter: Option<String>,
    pub browse_only: bool,
    pub created: i64,
    pub metrics: Arc<ConsumerMetrics>,
    pub(crate) sink: Arc<dyn DeliverySink>,
}

impl ConsumerEntry {
    /// `ServerConsumer.disconnect`: the client is told its consumer is
    /// gone.
    pub fn disconnect(&self) {
        let reason = format!("Queue deleted: {}", self.queue.name());
        self.sink.disconnect(self.id, &reason);
    }
}

/// A producer as management sees it (`ServerProducerImpl`).
pub struct ProducerEntry {
    pub id: i64,
    pub name: String,
    pub protocol: String,
    pub address: String,
    pub created: i64,
    sent: AtomicI64,
    sent_size: AtomicI64,
    last_message_id: Mutex<Option<String>>,
}

impl ProducerEntry {
    fn new(id: i64, name: &str, protocol: &str, address: &str) -> Self {
        Self {
            id,
            name: name.to_owned(),
            protocol: protocol.to_owned(),
            address: address.to_owned(),
            created: now_millis(),
            sent: AtomicI64::new(0),
            sent_size: AtomicI64::new(0),
            last_message_id: Mutex::default(),
        }
    }

    pub fn sent(&self) -> i64 {
        self.sent.load(Ordering::Relaxed)
    }

    pub fn sent_size(&self) -> i64 {
        self.sent_size.load(Ordering::Relaxed)
    }

    /// `getLastProducedMessageID`: the last message's user ID.
    pub fn last_message_id(&self) -> Option<String> {
        lock(&self.last_message_id).clone()
    }
}
