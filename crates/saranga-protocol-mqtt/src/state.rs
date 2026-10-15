//! What MQTT keeps between connections: `MQTTStateManager`,
//! `MQTTSessionState`, `SubscriptionItem`, `PacketIdCache` and the
//! journalled packet-ID map (`JournalHashMap` of `PacketIdCorrelationKey`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use saranga_buffer::{JString, PropertyValue, Reader, SimpleString, Writer};
use saranga_message::CoreMessage;
use saranga_server::duplicate::CacheRef;
use saranga_server::message::now_millis;
use saranga_server::queue::Queue;
use saranga_server::session::{Completion, ServerSession};
use saranga_server::storage::Pending;
use saranga_server::transaction::TxRef;
use saranga_server::{Broker, BrokerError};
use tokio::sync::{mpsc, oneshot};

use crate::codec::SubscriptionOption;
use crate::util::{self, MQTT_SESSION_STORE, MQTT_WILDCARD, TWO_BYTE_INT_MAX};

/// `JournalRecordIds.MQTT_PACKET_ID_CORRELATION`.
pub const MQTT_PACKET_ID_CORRELATION: u8 = 54;

pub fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// `MqttTopicSubscription`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSubscription {
    pub filter: String,
    pub option: SubscriptionOption,
}

/// `SubscriptionItem`: a subscription, its identifier and the consumer
/// reading its queue while the client is connected.
#[derive(Debug, Clone)]
pub struct SubscriptionItem {
    pub subscription: TopicSubscription,
    pub id: Option<i32>,
    /// The topic filter, without any share, that identifier matching uses;
    /// set only once the subscription has an identifier.
    address: Option<String>,
    pub consumer: Option<i64>,
}

impl SubscriptionItem {
    pub fn new(subscription: TopicSubscription, id: Option<i32>, consumer: Option<i64>) -> Self {
        let mut item = Self {
            subscription: subscription.clone(),
            id: None,
            address: None,
            consumer,
        };
        item.set(subscription, id, true);
        item
    }

    /// `update`.
    pub fn update(&mut self, subscription: TopicSubscription, id: Option<i32>) {
        self.set(subscription, id, false);
    }

    fn set(&mut self, subscription: TopicSubscription, id: Option<i32>, first: bool) {
        if let Some(new_id) = id
            && Some(new_id) != self.id
            && (self.address.is_none() || first || self.subscription.filter != subscription.filter)
        {
            self.address = Some(util::decompose_shared(&subscription.filter).1.to_owned());
        }
        self.subscription = subscription;
        self.id = id;
    }

    /// `getMatchingId`: the identifier, if the topic matches the filter.
    pub fn matching_id(&self, topic: &str) -> Option<i32> {
        let id = self.id?;
        let address = self.address.as_deref()?;
        MQTT_WILDCARD.matches(topic, address).then_some(id)
    }
}

/// `WillStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WillStatus {
    NotSent,
    Sent,
    Sending,
}

/// A will message, as CONNECT gave it.
#[derive(Debug, Clone, Default)]
pub struct Will {
    pub message: Option<Vec<u8>>,
    pub topic: Option<String>,
    pub qos: u8,
    pub retain: bool,
    pub delay_interval: i64,
    pub user_properties: Option<Vec<(String, String)>>,
}

/// `CoreDeliveryInfo`: the consumer an in-flight packet ID came from and
/// the message it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreDeliveryInfo {
    pub consumer_id: i64,
    pub key: CorrelationKey,
}

/// `PacketIdCorrelationKey`: a message as delivered through one address.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CorrelationKey {
    pub message_id: i64,
    pub address: SimpleString,
}

/// `PacketIdCache`: QoS 2 packet IDs in a duplicate-ID cache named
/// `<internal prefix>mqtt.qos.<publish|pubrec>.<client>`, so they survive a
/// restart.
#[derive(Clone)]
pub struct PacketIdCache {
    name: String,
    cache: Option<CacheRef>,
}

impl PacketIdCache {
    fn new(prefix: &str, client_id: &str, kind: &str) -> Self {
        Self {
            name: format!("{prefix}mqtt.qos.{kind}.{client_id}"),
            cache: None,
        }
    }

    /// `check`: the cache, if it exists; looking it up must not create it.
    fn check(&mut self, broker: &Broker) -> Option<&CacheRef> {
        let po = broker.post_office();
        if self.cache.is_none() && po.duplicate_cache_exists(&self.name) {
            self.cache = Some(po.named_duplicate_cache(&self.name, TWO_BYTE_INT_MAX));
        }
        self.cache.as_ref()
    }

    /// `add`, in `tx`.
    pub fn add(
        &mut self,
        session: &ServerSession,
        packet_id: i32,
        tx: &TxRef,
    ) -> Result<(), BrokerError> {
        let cache = self.cache.get_or_insert_with(|| {
            session
                .broker()
                .post_office()
                .named_duplicate_cache(&self.name, TWO_BYTE_INT_MAX)
        });
        session.add_duplicate_in(tx, cache, util::int_to_bytes(packet_id))
    }

    pub fn contains(&mut self, broker: &Broker, packet_id: i32) -> bool {
        self.check(broker).is_some_and(|c| {
            saranga_server::duplicate::lock(c).contains(&util::int_to_bytes(packet_id))
        })
    }

    /// `remove`: whether it was there.
    pub fn remove(&mut self, broker: &Broker, packet_id: i32) -> bool {
        self.check(broker).is_some_and(|c| {
            saranga_server::duplicate::lock(c).remove(&util::int_to_bytes(packet_id))
        })
    }

    /// `getPacketIds`, oldest first.
    pub fn packet_ids(&mut self, broker: &Broker) -> Vec<i32> {
        self.check(broker)
            .map(|c| {
                saranga_server::duplicate::lock(c)
                    .entries()
                    .into_iter()
                    .map(|(id, _)| util::bytes_to_int(&id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `clear`: the cache and its records go.
    fn clear(&mut self, broker: &Broker) {
        broker.post_office().delete_duplicate_cache(&self.name);
        self.cache = None;
    }
}

/// `MQTTSessionState`: one client ID's session, connected or not.
pub struct SessionState {
    client_id: String,
    pub subscriptions: HashMap<String, SubscriptionItem>,
    pub publish_cache: Option<PacketIdCache>,
    pub pubrec_cache: Option<PacketIdCache>,
    pub attached: bool,
    pub disconnected_time: i64,
    /// The last packet ID handed out (`PacketIdGenerator`).
    current_packet_id: i32,
    pub client_session_expiry_interval: i32,
    pub is_will: bool,
    pub will: Will,
    pub will_status: WillStatus,
    pub failed: bool,
    pub client_max_packet_size: i32,
    client_topic_aliases: HashMap<i32, String>,
    pub client_topic_alias_maximum: Option<i32>,
    server_topic_aliases: HashMap<String, i32>,
    core_delivery_infos: HashMap<i32, CoreDeliveryInfo>,
    pub send_quota: i32,
    /// The session of the client's last connection (`getSession`), kept
    /// once it disconnects to send a delayed will and to clean up when the
    /// session expires.
    pub session: Option<Box<crate::connection::Core>>,
}

impl SessionState {
    pub fn new(client_id: &str) -> Self {
        Self {
            client_id: client_id.to_owned(),
            subscriptions: HashMap::new(),
            publish_cache: None,
            pubrec_cache: None,
            attached: false,
            disconnected_time: 0,
            current_packet_id: 0,
            client_session_expiry_interval: 0,
            is_will: false,
            will: Will::default(),
            will_status: WillStatus::NotSent,
            failed: false,
            client_max_packet_size: 0,
            client_topic_aliases: HashMap::new(),
            client_topic_alias_maximum: None,
            server_topic_aliases: HashMap::new(),
            core_delivery_infos: HashMap::new(),
            send_quota: 0,
            session: None,
        }
    }

    /// The constructor that reads a state message of the session store:
    /// a version byte, the subscriptions, and (since 2.31) the session
    /// expiry interval.
    pub fn decode(message: &CoreMessage) -> Result<Self, String> {
        let client_id = message
            .properties
            .get(&SimpleString::from(util::HDR_LAST_VALUE_NAME))
            .map(property_string)
            .unwrap_or_default();
        let mut state = Self::new(&client_id);
        let mut r = Reader::new(&message.body);
        let e = |e: saranga_buffer::DecodeError| e.to_string();
        let _version = r.u8().map_err(e)?;
        let count = r.i32().map_err(e)?;
        for _ in 0..count {
            let filter = r.string().map_err(e)?.to_string_lossy();
            let qos = r.i32().map_err(e)?;
            let no_local = r.bool().map_err(e)?;
            let retain_as_published = r.bool().map_err(e)?;
            let retain_handling = r.i32().map_err(e)?;
            let id = r.nullable_i32().map_err(e)?;
            if !(0..=2).contains(&qos) {
                return Err(format!("invalid QoS: {qos}"));
            }
            if !(0..=2).contains(&retain_handling) {
                return Err(format!("invalid RetainedHandlingPolicy: {retain_handling}"));
            }
            let subscription = TopicSubscription {
                filter: filter.clone(),
                option: SubscriptionOption {
                    qos: qos as u8,
                    no_local,
                    retain_as_published,
                    retain_handling: retain_handling as u8,
                },
            };
            state
                .subscriptions
                .insert(filter, SubscriptionItem::new(subscription, id, None));
        }
        if !r.is_empty() {
            state.client_session_expiry_interval = r.i32().map_err(e)?;
            state.disconnected_time = now_millis();
        }
        Ok(state)
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// `clear`.
    pub fn clear(&mut self, broker: &Broker) {
        self.subscriptions.clear();
        if let Some(c) = &mut self.publish_cache {
            c.clear(broker);
        }
        if let Some(c) = &mut self.pubrec_cache {
            c.clear(broker);
        }
        self.current_packet_id = 0;
        self.disconnected_time = 0;
        self.will.message = None;
        self.will_status = WillStatus::NotSent;
        self.failed = false;
        self.will.delay_interval = 0;
        self.will.retain = false;
        self.will.topic = None;
        self.client_max_packet_size = 0;
        self.clear_topic_aliases();
        self.client_topic_alias_maximum = Some(0);
    }

    /// `getPublishCache`: inbound QoS 2 packet IDs awaiting PUBREL.
    pub fn publish_cache(&mut self, prefix: &str) -> &mut PacketIdCache {
        let id = self.client_id.clone();
        self.publish_cache
            .get_or_insert_with(|| PacketIdCache::new(prefix, &id, "publish"))
    }

    /// `getPubRecCache`: outbound QoS 2 packet IDs awaiting PUBCOMP.
    pub fn pubrec_cache(&mut self, prefix: &str) -> &mut PacketIdCache {
        let id = self.client_id.clone();
        self.pubrec_cache
            .get_or_insert_with(|| PacketIdCache::new(prefix, &id, "pubrec"))
    }

    pub fn put_client_topic_alias(&mut self, alias: i32, topic: String) {
        self.client_topic_aliases.insert(alias, topic);
    }

    pub fn client_topic_alias(&self, alias: i32) -> Option<&String> {
        self.client_topic_aliases.get(&alias)
    }

    /// `addServerTopicAlias`: the next alias, if the client takes that
    /// many.
    pub fn add_server_topic_alias(&mut self, topic: &str) -> Option<i32> {
        let alias = self.server_topic_aliases.len() as i32 + 1;
        if alias <= self.client_topic_alias_maximum.unwrap_or(0) {
            self.server_topic_aliases.insert(topic.to_owned(), alias);
            Some(alias)
        } else {
            None
        }
    }

    pub fn server_topic_alias(&self, topic: &str) -> Option<i32> {
        self.server_topic_aliases.get(topic).copied()
    }

    pub fn clear_topic_aliases(&mut self) {
        self.client_topic_aliases.clear();
        self.server_topic_aliases.clear();
    }

    pub fn core_delivery_info(&self, packet_id: i32) -> Option<&CoreDeliveryInfo> {
        self.core_delivery_infos.get(&packet_id)
    }

    pub fn put_core_delivery_info(&mut self, packet_id: i32, info: CoreDeliveryInfo) {
        self.core_delivery_infos.insert(packet_id, info);
    }

    pub fn remove_core_delivery_info(&mut self, packet_id: i32) -> Option<CoreDeliveryInfo> {
        self.core_delivery_infos.remove(&packet_id)
    }

    pub fn core_delivery_info_exists(&self, packet_id: i32) -> bool {
        self.core_delivery_infos.contains_key(&packet_id)
    }

    pub fn clear_core_delivery_info(&mut self) {
        self.core_delivery_infos.clear();
    }

    /// `getMatchingSubscriptionIdentifiers` for an MQTT topic.
    pub fn matching_subscription_identifiers(&self, topic: &str) -> Vec<i32> {
        self.subscriptions
            .values()
            .filter_map(|item| item.matching_id(topic))
            .collect()
    }

    /// `PacketIdGenerator.generatePacketId`: the next ID from 1 to 65535
    /// not in flight, wrapping around.
    pub fn generate_packet_id(
        &mut self,
        broker: &Broker,
        correlations: &Correlations,
        prefix: &str,
    ) -> Result<i32, String> {
        let start = self.current_packet_id;
        loop {
            self.current_packet_id += 1;
            if self.current_packet_id > TWO_BYTE_INT_MAX {
                self.current_packet_id = 0;
            }
            let id = self.current_packet_id;
            if start == id {
                if id != 0 && !self.packet_id_in_use(broker, correlations, prefix, id) {
                    return Ok(id);
                }
                return Err("AMQ850001: Unable to generate MQTT packet ID. All valid values between 1 and 65535 are in use. IDs will become available as messages are acknowledged by the client that has received them.".into());
            }
            if id != 0 && !self.packet_id_in_use(broker, correlations, prefix, id) {
                return Ok(id);
            }
        }
    }

    /// `packetIdInUse`: in flight at QoS 1 or 2, or waiting for PUBCOMP.
    fn packet_id_in_use(
        &mut self,
        broker: &Broker,
        correlations: &Correlations,
        prefix: &str,
        id: i32,
    ) -> bool {
        self.core_delivery_info_exists(id)
            || correlations.contains_value(&self.client_id, id)
            || self
                .pubrec_cache
                .as_mut()
                .is_some_and(|c| c.contains(broker, id))
            || {
                let _ = prefix;
                false
            }
    }

    /// `serializeState`: the session-store message for this state.
    pub fn encode(&self, message_id: i64) -> CoreMessage {
        let mut message = CoreMessage {
            message_id,
            address: Some(MQTT_SESSION_STORE.into()),
            durable: true,
            ..CoreMessage::default()
        };
        message.properties.insert(
            util::HDR_LAST_VALUE_NAME.into(),
            PropertyValue::String(self.client_id.as_str().into()),
        );
        let mut w = Writer::new();
        w.u8(0);
        w.i32(self.subscriptions.len() as i32);
        for item in self.subscriptions_in_hash_map_order() {
            let sub = &item.subscription;
            w.string(&JString::from(sub.filter.as_str()));
            w.i32(i32::from(sub.option.qos));
            w.bool(sub.option.no_local);
            w.bool(sub.option.retain_as_published);
            w.i32(i32::from(sub.option.retain_handling));
            w.nullable_i32(item.id);
        }
        w.i32(self.client_session_expiry_interval);
        message.body = w.into_vec();
        message
    }

    /// The subscriptions in the order `new HashMap<>(subscriptionItems)`
    /// iterates them.
    fn subscriptions_in_hash_map_order(&self) -> Vec<&SubscriptionItem> {
        let mut items: Vec<&SubscriptionItem> = self.subscriptions.values().collect();
        items.sort_by(|a, b| a.subscription.filter.cmp(&b.subscription.filter));
        let hashes: Vec<i32> = items
            .iter()
            .map(|i| {
                let units: Vec<u16> = i.subscription.filter.encode_utf16().collect();
                saranga_storage::java::string_hash(&units)
            })
            .collect();
        let n = items.len();
        let capacity = (n as f32 / 0.75 + 1.0) as usize;
        saranga_storage::java::hash_map_order(&hashes, Some((capacity, 0.75)))
            .into_iter()
            .map(|i| items[i])
            .collect()
    }
}

fn property_string(v: &PropertyValue) -> String {
    match v {
        PropertyValue::String(s) => s.to_string_lossy(),
        other => format!("{other:?}"),
    }
}

pub type StateRef = Arc<Mutex<SessionState>>;

/// One record of the packet-ID map (`MapRecord`).
#[derive(Debug, Clone)]
struct MapRecord {
    id: i64,
    packet_id: i32,
}

/// `JournalHashMapProvider` of packet-ID correlations: per client, which
/// packet ID each in-flight message went out with, journalled so a
/// redelivery after a restart reuses it.
#[derive(Default)]
pub struct Correlations {
    maps: Mutex<HashMap<String, HashMap<CorrelationKey, MapRecord>>>,
}

impl Correlations {
    /// `AbstractHashMapPersister.encode` with `PacketIdCorrelationKey`'s
    /// persister.
    fn encode(id: i64, client_id: &str, key: &CorrelationKey, packet_id: i32) -> Vec<u8> {
        let mut w = Writer::new();
        w.i64(id)
            .u8(0)
            .string(&JString::from(client_id))
            .i64(key.message_id)
            .simple_string(&key.address)
            .i32(packet_id);
        w.into_vec()
    }

    /// `reload`: a record from the journal.
    pub fn reload(&self, data: &[u8]) -> Result<(), String> {
        let mut r = Reader::new(data);
        let e = |e: saranga_buffer::DecodeError| e.to_string();
        let id = r.i64().map_err(e)?;
        let _version = r.u8().map_err(e)?;
        let client_id = r.string().map_err(e)?.to_string_lossy();
        let message_id = r.i64().map_err(e)?;
        let address = r.simple_string().map_err(e)?;
        let packet_id = r.i32().map_err(e)?;
        lock(&self.maps).entry(client_id).or_default().insert(
            CorrelationKey {
                message_id,
                address,
            },
            MapRecord { id, packet_id },
        );
        Ok(())
    }

    pub fn get(&self, client_id: &str, key: &CorrelationKey) -> Option<i32> {
        lock(&self.maps)
            .get(client_id)?
            .get(key)
            .map(|r| r.packet_id)
    }

    /// `put`: journals the new record (synced; the delivery waits for it)
    /// and deletes the one it replaces.
    pub fn put(
        &self,
        broker: &Broker,
        client_id: &str,
        key: CorrelationKey,
        packet_id: i32,
    ) -> Option<Pending> {
        let id = broker.post_office().ids().next();
        let mut pending = None;
        if let Some(s) = broker.storage() {
            match s.add_record(
                id,
                MQTT_PACKET_ID_CORRELATION,
                Self::encode(id, client_id, &key, packet_id),
                true,
            ) {
                Ok(p) => pending = Some(p),
                Err(e) => tracing::warn!("{e}"),
            }
        }
        let old = lock(&self.maps)
            .entry(client_id.to_owned())
            .or_default()
            .insert(key, MapRecord { id, packet_id });
        if let (Some(old), Some(s)) = (old, broker.storage())
            && let Err(e) = s.delete_record(old.id)
        {
            tracing::warn!("{e}");
        }
        pending
    }

    /// `remove(key, txID)`: gone from the map at once; the record's delete
    /// is part of `tx`.
    pub fn remove_in(
        &self,
        session: &ServerSession,
        client_id: &str,
        key: &CorrelationKey,
        tx: &TxRef,
    ) -> Result<Option<i32>, BrokerError> {
        let removed = lock(&self.maps)
            .get_mut(client_id)
            .and_then(|m| m.remove(key));
        match removed {
            Some(r) => {
                session.delete_record_in(tx, r.id)?;
                Ok(Some(r.packet_id))
            }
            None => Ok(None),
        }
    }

    /// `clear`: the client's map is emptied and its records deleted.
    /// Artemis 2.57 leaves the records in the journal (see COMPAT.md).
    pub fn clear(&self, broker: &Broker, client_id: &str) {
        let removed = lock(&self.maps)
            .get_mut(client_id)
            .map(std::mem::take)
            .unwrap_or_default();
        if let Some(s) = broker.storage() {
            for r in removed.into_values() {
                if let Err(e) = s.delete_record(r.id) {
                    tracing::warn!("{e}");
                }
            }
        }
    }

    /// `containsValue`.
    pub fn contains_value(&self, client_id: &str, packet_id: i32) -> bool {
        lock(&self.maps)
            .get(client_id)
            .is_some_and(|m| m.values().any(|r| r.packet_id == packet_id))
    }
}

/// What another connection can ask of a connected client's connection.
pub enum Control {
    /// Another connection took the client ID (`handleLinkStealing`): the
    /// connection stops its session, then answers.
    TakeOver { done: oneshot::Sender<()> },
    /// A client with the same ID connected to another node of the cluster
    /// (`MQTTConnection.destroy`): the connection is closed as if it
    /// failed.
    Destroy,
}

/// A connected client, as `connectedClients` holds it.
#[derive(Clone)]
pub struct ConnectedClient {
    /// Which connection: `isClientConnected(clientId, connection)`.
    pub connection: u64,
    pub control: mpsc::UnboundedSender<Control>,
}

/// `MQTTStateManager`: the broker's MQTT session states and connected
/// clients.
pub struct StateManager {
    broker: Weak<Broker>,
    states: Mutex<HashMap<String, StateRef>>,
    connected: Mutex<HashMap<String, ConnectedClient>>,
    store: Mutex<Option<Queue>>,
    persistence_enabled: bool,
    pub correlations: Correlations,
    /// `getInternalNamingPrefix`.
    pub internal_prefix: String,
}

impl StateManager {
    pub fn new(broker: &Arc<Broker>, persistence_enabled: bool, internal_prefix: String) -> Self {
        Self {
            broker: Arc::downgrade(broker),
            states: Mutex::default(),
            connected: Mutex::default(),
            store: Mutex::default(),
            persistence_enabled,
            correlations: Correlations::default(),
            internal_prefix,
        }
    }

    pub fn broker(&self) -> Option<Arc<Broker>> {
        self.broker.upgrade()
    }

    /// `init`: creates the session store and loads the states in it.
    pub async fn init(&self) {
        if !self.persistence_enabled {
            return;
        }
        let Some(broker) = self.broker() else { return };
        let mut spec =
            saranga_server::postoffice::QueueSpec::new(MQTT_SESSION_STORE, MQTT_SESSION_STORE);
        spec.routing_type = Some(saranga_server::RoutingType::Anycast);
        spec.last_value = Some(true);
        spec.durable = true;
        spec.internal = true;
        spec.auto_create_address = Some(true);
        let queue = match broker.post_office().create_queue(spec, true) {
            Ok(q) => q,
            Err(e) => {
                tracing::error!("unable to create {MQTT_SESSION_STORE}: {e}");
                return;
            }
        };
        for r in queue.browse().await {
            let message = r.message.core();
            let client_id = message
                .properties
                .get(&SimpleString::from(util::HDR_LAST_VALUE_NAME))
                .map(property_string);
            let Some(client_id) = client_id.filter(|c| !c.is_empty()) else {
                tracing::warn!(
                    "AMQ834012: Invalid MQTT session state message. Client ID is empty or null. Will not load this state into memory."
                );
                continue;
            };
            match SessionState::decode(message) {
                Ok(state) => {
                    lock(&self.states).insert(client_id, Arc::new(Mutex::new(state)));
                }
                Err(e) => tracing::warn!(
                    "AMQ834013: Invalid MQTT session state message. Will not load this state into memory. {e}"
                ),
            }
        }
        *lock(&self.store) = Some(queue);
    }

    /// `getSessionState`: the client's state, made if it has none
    /// ([MQTT-3.1.2-4]).
    pub fn session_state(&self, client_id: &str) -> StateRef {
        lock(&self.states)
            .entry(client_id.to_owned())
            .or_insert_with(|| {
                tracing::debug!("Adding MQTT session state for: {client_id}");
                Arc::new(Mutex::new(SessionState::new(client_id)))
            })
            .clone()
    }

    pub fn has_session_state(&self, client_id: &str) -> bool {
        lock(&self.states).contains_key(client_id)
    }

    pub fn remove_session_state(&self, client_id: &str) -> Option<StateRef> {
        tracing::debug!("Removing MQTT session state for: {client_id}");
        lock(&self.states).remove(client_id)
    }

    pub fn session_states(&self) -> Vec<(String, StateRef)> {
        lock(&self.states)
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// `storeDurableState`: the state goes to the session store, replacing
    /// the client's last one, in its own transaction.
    pub fn store_durable_state(
        &self,
        session: &mut ServerSession,
        state: &SessionState,
    ) -> Result<(), BrokerError> {
        if !self.persistence_enabled {
            return Ok(());
        }
        let Some(queue) = lock(&self.store).clone() else {
            return Ok(());
        };
        tracing::debug!("Adding durable MQTT record for: {}", state.client_id);
        let message = state.encode(session.broker().post_office().ids().next());
        let tx = session.new_transaction();
        session.send_to_queue(message, None, &queue, Some(tx.clone()))?;
        session.commit_tx(&tx)
    }

    /// `removeDurableSubscriptionState`.
    pub async fn remove_durable_state(&self, client_id: &str) {
        if !self.persistence_enabled {
            return;
        }
        let Some(queue) = lock(&self.store).clone() else {
            return;
        };
        let Some(broker) = self.broker() else { return };
        let filter = format!(
            "{} = '{}'",
            util::HDR_LAST_VALUE_NAME,
            client_id.replace('\'', "''")
        );
        match broker.post_office().filters().create(Some(&filter)) {
            Ok(filter) => {
                let n = queue.delete_matching(filter).await;
                tracing::debug!("Removed {n} durable MQTT subscription record(s) for: {client_id}");
            }
            Err(e) => tracing::warn!("{e}"),
        }
    }

    pub fn connected_client(&self, client_id: &str) -> Option<ConnectedClient> {
        lock(&self.connected).get(client_id).cloned()
    }

    pub fn add_connected_client(&self, client_id: &str, client: ConnectedClient) {
        lock(&self.connected).insert(client_id.to_owned(), client);
    }

    /// Removes the client if `connection` is still the one connected with
    /// its ID; one that stole the ID stays.
    pub fn remove_connected_client(&self, client_id: &str, connection: u64) {
        let mut connected = lock(&self.connected);
        if connected
            .get(client_id)
            .is_some_and(|c| c.connection == connection)
        {
            connected.remove(client_id);
        }
    }

    /// `scanSessions`: expired sessions go; delayed wills go out.
    pub async fn scan_sessions(&self) {
        let now = now_millis();
        let mut to_remove = Vec::new();
        let mut wills = Vec::new();
        for (client_id, state) in self.session_states() {
            let s = lock(&state);
            let expiry = i64::from(s.client_session_expiry_interval);
            if !s.attached && expiry > 0 && s.disconnected_time + expiry * 1000 < now {
                to_remove.push(client_id.clone());
            }
            if s.is_will
                && !s.attached
                && s.failed
                && s.will.delay_interval > 0
                && s.disconnected_time + s.will.delay_interval * 1000 < now
            {
                wills.push(state.clone());
            }
        }
        for state in wills {
            crate::connection::send_detached_will(&state).await;
        }
        for client_id in to_remove {
            let Some(state) = self.remove_session_state(&client_id) else {
                continue;
            };
            crate::connection::expire_detached(self, &state).await;
        }
    }
}

/// Waits for what a detached session lined up.
pub async fn wait(completion: Completion) {
    if let Err(e) = completion.wait().await {
        tracing::error!("AMQ834016: Storage operation failed. Error code: 0; message: {e}");
    }
}
