//! Messages as the broker holds them: routed once, shared by every queue
//! and consumer through `Arc`, with the encoded form cached.

use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, OnceLock};

use saranga_buffer::{PropertyValue, SimpleString, Writer};
use saranga_message::{Bytes, CoreMessage};
use saranga_storage::message::{CoreMessageRecord, PersistedMessage};

use crate::RoutingType;
use crate::large::LargeBody;
use crate::settings::AddressSettings;

/// `Message.HDR_VALIDATED_USER`.
pub const HDR_VALIDATED_USER: &str = "_AMQ_VALIDATED_USER";

/// `Message.HDR_ROUTING_TYPE`.
pub const HDR_ROUTING_TYPE: &str = "_AMQ_ROUTING_TYPE";
/// `Message.HDR_ORIG_ROUTING_TYPE`.
pub const HDR_ORIG_ROUTING_TYPE: &str = "_AMQ_ORIG_ROUTING_TYPE";
/// `Message.HDR_ORIGINAL_ADDRESS`.
pub const HDR_ORIGINAL_ADDRESS: &str = "_AMQ_ORIG_ADDRESS";
/// `Message.HDR_ORIGINAL_QUEUE`.
pub const HDR_ORIGINAL_QUEUE: &str = "_AMQ_ORIG_QUEUE";
/// `Message.HDR_ORIG_MESSAGE_ID`.
pub const HDR_ORIG_MESSAGE_ID: &str = "_AMQ_ORIG_MESSAGE_ID";
/// `Message.HDR_ACTUAL_EXPIRY_TIME`.
pub const HDR_ACTUAL_EXPIRY_TIME: &str = "_AMQ_ACTUAL_EXPIRY";
/// `Message.HDR_SCHEDULED_DELIVERY_TIME`.
pub const HDR_SCHEDULED_DELIVERY_TIME: &str = "_AMQ_SCHED_DELIVERY";
/// `Message.HDR_DUPLICATE_DETECTION_ID`.
pub const HDR_DUPLICATE_DETECTION_ID: &str = "_AMQ_DUPL_ID";
/// `Message.HDR_BRIDGE_DUPLICATE_ID`.
pub const HDR_BRIDGE_DUPLICATE_ID: &str = "_AMQ_BRIDGE_DUP";
/// `Message.HDR_LAST_VALUE_NAME`.
pub const HDR_LAST_VALUE_NAME: &str = "_AMQ_LVQ_NAME";
/// `Message.HDR_GROUP_ID`.
pub const HDR_GROUP_ID: &str = "_AMQ_GROUP_ID";
/// `Message.HDR_GROUP_SEQUENCE`.
pub const HDR_GROUP_SEQUENCE: &str = "_AMQ_GROUP_SEQUENCE";
/// `Message.HDR_LARGE_BODY_SIZE`.
pub const HDR_LARGE_BODY_SIZE: &str = "_AMQ_LARGE_SIZE";

/// `System.currentTimeMillis`.
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn property<'a>(message: &'a CoreMessage, name: &str) -> Option<&'a PropertyValue> {
    message.properties.get_str(name)
}

/// A numeric property as a long, as `getLongProperty` would read one
/// written as any integer type.
pub(crate) fn long_property(message: &CoreMessage, name: &str) -> Option<i64> {
    match property(message, name)? {
        PropertyValue::Byte(v) => Some(i64::from(*v)),
        PropertyValue::Short(v) => Some(i64::from(*v)),
        PropertyValue::Int(v) => Some(i64::from(*v)),
        PropertyValue::Long(v) => Some(*v),
        _ => None,
    }
}

/// `getSimpleStringProperty`: any value as text.
pub fn string_property(message: &CoreMessage, name: &str) -> Option<String> {
    Some(match property(message, name)? {
        PropertyValue::Null => return None,
        PropertyValue::String(s) => s.to_string_lossy(),
        PropertyValue::Bool(v) => v.to_string(),
        PropertyValue::Byte(v) => v.to_string(),
        PropertyValue::Short(v) => v.to_string(),
        PropertyValue::Int(v) => v.to_string(),
        PropertyValue::Long(v) => v.to_string(),
        PropertyValue::Float(v) => v.to_string(),
        PropertyValue::Double(v) => v.to_string(),
        PropertyValue::Char(v) => String::from_utf16_lossy(&[*v]),
        PropertyValue::Bytes(b) => SimpleString::from_bytes(b.clone()).to_string_lossy(),
    })
}

/// `Message.getGroupID`: `_AMQ_GROUP_ID` as `SimpleString` bytes.
pub fn group_id(message: &CoreMessage) -> Option<Vec<u8>> {
    match property(message, HDR_GROUP_ID)? {
        PropertyValue::String(s) => Some(s.as_bytes().to_vec()),
        PropertyValue::Null => None,
        _ => string_property(message, HDR_GROUP_ID)
            .map(|s| SimpleString::from(s.as_str()).as_bytes().to_vec()),
    }
}

/// `Message.getGroupSequence`: 0 when unset.
pub fn group_sequence(message: &CoreMessage) -> i64 {
    long_property(message, HDR_GROUP_SEQUENCE).unwrap_or(0)
}

/// `_AMQ_ORIG_MESSAGE_ID`, when set.
pub fn orig_message_id(message: &CoreMessage) -> Option<i64> {
    long_property(message, HDR_ORIG_MESSAGE_ID)
}

/// `CoreMessage.getScheduledDeliveryTime`: 0 when unset.
pub fn scheduled_delivery_time(message: &CoreMessage) -> i64 {
    long_property(message, HDR_SCHEDULED_DELIVERY_TIME).unwrap_or(0)
}

/// `Message.isExpired`.
pub fn is_expired(message: &CoreMessage, now: i64) -> bool {
    message.expiration != 0 && now - message.expiration >= 0
}

/// `PostOfficeImpl.applyExpiryDelay`, reading the clock only when `s`
/// adjusts expiration.
pub fn apply_expiry_delay_now(message: &mut CoreMessage, s: &AddressSettings) {
    let unset = AddressSettings::NO_EXPIRY_DELAY;
    if !s.no_expiry
        && s.expiry_delay < 0
        && s.min_expiry_delay == unset
        && s.max_expiry_delay == unset
    {
        return;
    }
    apply_expiry_delay(message, s, now_millis());
}

/// `PostOfficeImpl.applyExpiryDelay`: the address's expiry settings applied
/// to a message being routed.
pub fn apply_expiry_delay(message: &mut CoreMessage, s: &AddressSettings, now: i64) {
    if s.no_expiry {
        message.expiration = 0;
    } else if s.expiry_delay >= 0 {
        if message.expiration == 0 {
            message.expiration = now + s.expiry_delay;
        }
    } else {
        let (min, max) = (s.min_expiry_delay, s.max_expiry_delay);
        let unset = AddressSettings::NO_EXPIRY_DELAY;
        if message.expiration == 0 {
            if max != unset {
                message.expiration = now + max;
            } else if min != unset {
                message.expiration = now + min;
            }
        } else if max != unset && message.expiration > now + max {
            message.expiration = now + max;
        } else if min != unset && message.expiration < now + min {
            message.expiration = now + min;
        }
    }
}

/// `QueueImpl.makeCopy` with `referenceOriginalMessage`: the copy a message
/// becomes when moved to a dead-letter or expiry address, then the routing
/// type `move` gives it.
pub fn make_copy(
    original: &CoreMessage,
    new_id: i64,
    new_address: &str,
    queue: &str,
    expiry: bool,
    now: i64,
) -> CoreMessage {
    let mut copy = original.clone();
    copy.message_id = new_id;
    copy.address = Some(SimpleString::from(new_address));
    let p = &mut copy.properties;
    p.insert(
        HDR_ORIGINAL_QUEUE.into(),
        PropertyValue::String(SimpleString::from(queue)),
    );
    p.insert(
        HDR_ORIGINAL_ADDRESS.into(),
        PropertyValue::String(original.address.clone().unwrap_or_default()),
    );
    p.insert(
        HDR_ORIG_MESSAGE_ID.into(),
        PropertyValue::Long(original.message_id),
    );
    if let Some(t) = routing_type(original) {
        p.insert(
            HDR_ORIG_ROUTING_TYPE.into(),
            PropertyValue::Byte(t.to_byte()),
        );
    }
    copy.expiration = 0;
    p.remove(&SimpleString::from(HDR_ROUTING_TYPE));
    if expiry {
        p.insert(HDR_ACTUAL_EXPIRY_TIME.into(), PropertyValue::Long(now));
    }
    if let Some(PropertyValue::Byte(t)) = property(original, HDR_ORIG_ROUTING_TYPE) {
        let t = *t;
        copy.properties
            .insert(HDR_ROUTING_TYPE.into(), PropertyValue::Byte(t));
    }
    copy
}

/// What `write` writes, in a buffer of its own size: a writer's spare
/// capacity would otherwise stay with the message as long as it is held.
fn exact(size: usize, write: impl FnOnce(&mut Writer)) -> Bytes {
    let mut w = Writer::with_capacity(size);
    write(&mut w);
    let v = w.into_vec();
    if v.len() == v.capacity() {
        v.into()
    } else {
        // A copy, as an allocator may shrink a block in place and keep its
        // size.
        Bytes::copy_from_slice(&v)
    }
}

/// A message kept in its own protocol's encoding (`AMQPMessage`): the
/// broker works on its [`view`](Self::view), and changes the broker makes
/// to the view are carried over to the native form by
/// [`updated`](Self::updated).
pub trait NativeMessage: Send + Sync + fmt::Debug {
    /// What the broker reads: the ID, address, durability, priority,
    /// expiration and timestamp, and as properties the ones the broker
    /// looks at (routing type, duplicate ID, groups, last value, scheduled
    /// delivery, and the broker's own).
    fn view(&self) -> CoreMessage;

    /// `Message.toCore`: the message as a client of another protocol
    /// receives it.
    fn to_core(&self) -> CoreMessage;

    /// The record its persister writes to the journal and to pages.
    fn persisted(&self) -> PersistedMessage;

    /// `getEncodeSize`.
    fn encode_size(&self) -> usize;

    /// `getMemoryEstimate`: what the message adds to its address's size
    /// while referenced.
    fn memory_estimate(&self) -> i64;

    /// `getObjectPropertyForFilter` and the header fields a filter reads.
    fn filter_value(&self, identifier: &str) -> Option<saranga_filter::Value>;

    /// The message after the broker changed its view from `before` to
    /// `after` (`setAddress`, `setBrokerProperty`, ... then `reencode`).
    fn updated(&self, before: &CoreMessage, after: &CoreMessage) -> Arc<dyn NativeMessage>;

    /// A large message whose body is kept in a file rather than in memory,
    /// rebuilt with `body` (its file read whole) in place, for delivery or
    /// conversion that needs the whole encoding. A message that carries its
    /// whole encoding already returns a copy of itself.
    fn reconstitute(&self, body: Vec<u8>) -> Arc<dyn NativeMessage>;

    /// `getStringBody`, for an `XPATH` selector: the body as text, `None`
    /// when the protocol's body is not text.
    fn string_body(&self) -> Option<String> {
        None
    }

    /// Whether `body`, a large message's file put together again (by a
    /// replay, from the retained journal), is this message's whole
    /// encoding rather than what is left of it once its first part was
    /// removed. A protocol whose record keeps nothing to check it by says
    /// yes.
    fn holds_large_body(&self, body: &crate::large::LargeBody) -> bool {
        let _ = body;
        true
    }

    fn as_any(&self) -> &dyn Any;
}

/// Decodes the records of one protocol's persisters, with the contents of
/// a large message's file; `None` for records of other persisters.
pub type NativeDecoder = dyn Fn(&PersistedMessage, Option<Vec<u8>>) -> Option<Result<Arc<dyn NativeMessage>, String>>
    + Send
    + Sync;

/// Turns persister records into messages: each protocol that persists its
/// own messages adds a decoder.
#[derive(Default, Clone)]
pub struct MessageDecoders(Vec<Arc<NativeDecoder>>);

impl fmt::Debug for MessageDecoders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MessageDecoders({})", self.0.len())
    }
}

impl MessageDecoders {
    pub fn new(decoders: Vec<Arc<NativeDecoder>>) -> Self {
        Self(decoders)
    }

    /// The message a regular (not large) record holds.
    pub fn decode(&self, record: &PersistedMessage) -> Result<Arc<ServerMessage>, String> {
        if let PersistedMessage::Core(c) = record {
            return Ok(ServerMessage::new(c.to_message()));
        }
        self.native(record, None).map(ServerMessage::new_native)
    }

    /// A large message of another protocol, with its file. The body stays in
    /// the file: the message keeps only its headers in memory and is read
    /// back whole when it is delivered.
    pub fn decode_large(
        &self,
        record: &PersistedMessage,
        body: Arc<LargeBody>,
    ) -> Result<Arc<ServerMessage>, String> {
        self.native(record, Some(Vec::new()))
            .map(|n| ServerMessage::new_native_large(n, body))
    }

    fn native(
        &self,
        record: &PersistedMessage,
        data: Option<Vec<u8>>,
    ) -> Result<Arc<dyn NativeMessage>, String> {
        for d in &self.0 {
            if let Some(r) = d(record, data.clone()) {
                return r;
            }
        }
        Err(format!(
            "couldn't find factory for persister {}",
            record.persister_id()
        ))
    }
}

/// A message after the broker has assigned its ID and address.
/// A message's `RefCountMessage` accounting: what Artemis keeps on the one
/// message object a journal ID has. Saranga's messages are immutable, so an
/// in-place change (a priority change, a redelivery's failure cause) rebuilds
/// the message; the rebuilt one shares this, so references and durable records
/// are never split across objects that stand for the same journal ID (which
/// would delete a multicast message's record while another queue still holds
/// it, or leak the memory a discarded message charged).
#[derive(Debug, Default)]
struct RefCounts {
    /// References on durable queues not yet acknowledged
    /// (`RefCountMessage.durableUp/Down`); the record is deleted at zero.
    durable_refs: AtomicI32,
    /// References in queues (`RefCountMessage.refUp/refDown`).
    refs: AtomicI32,
    /// The paging store of the message's address, which counts the memory
    /// it takes while referenced (`owner`).
    owner: OnceLock<Arc<crate::paging::PagingStore>>,
    /// `getMemoryEstimate`, fixed when first asked as in Artemis, so that
    /// what a reference adds its removal takes away.
    estimate: OnceLock<i64>,
}

#[derive(Debug)]
pub struct ServerMessage {
    /// The message, or for a native message the broker's view of it.
    core: CoreMessage,
    /// A message kept in another protocol's encoding.
    native: Option<Arc<dyn NativeMessage>>,
    /// A native message as a Core message, and its encoding, once a client
    /// of another protocol needed it.
    converted: OnceLock<Box<(CoreMessage, Vec<u8>)>>,
    /// `core` encoded as `CoreMessage.encode` writes it, the form carried by
    /// `SESS_RECEIVE_MSG`. For a large message, only its headers and
    /// properties (`encodeHeadersAndProperties`), the form `ADD_LARGE_MESSAGE`
    /// and `SESS_RECEIVE_LARGE_MSG` carry.
    encoded: Bytes,
    /// A large message's body (`LargeServerMessageImpl`); `core.body` is
    /// then empty.
    large: Option<Arc<LargeBody>>,
    /// The `RefCountMessage` accounting, shared with any in-place replacement
    /// of this message (see [`ServerMessage::with_core_keeping_refs`]).
    counts: Arc<RefCounts>,
}

impl ServerMessage {
    /// Assembles a message from its parts and its (fresh or shared) accounting.
    fn from_parts(
        core: CoreMessage,
        native: Option<Arc<dyn NativeMessage>>,
        encoded: Bytes,
        large: Option<Arc<LargeBody>>,
        counts: Arc<RefCounts>,
    ) -> Arc<Self> {
        Arc::new(Self {
            core,
            native,
            converted: OnceLock::new(),
            encoded,
            large,
            counts,
        })
    }

    pub fn new(core: CoreMessage) -> Arc<Self> {
        Self::new_counted(core, Arc::default())
    }

    fn new_counted(mut core: CoreMessage, counts: Arc<RefCounts>) -> Arc<Self> {
        let encoded = exact(core.encode_size(), |w| core.encode(w));
        // The body is held once, in the encoding (after its i32 end-of-body),
        // and so are the address and the properties' names and strings:
        // slices of it rather than copies.
        core.share(&encoded);
        Self::from_parts(core, None, encoded, None, counts)
    }

    /// A message kept in its own protocol's encoding.
    pub fn new_native(native: Arc<dyn NativeMessage>) -> Arc<Self> {
        Self::native_counted(native, None, Arc::default())
    }

    fn native_counted(
        native: Arc<dyn NativeMessage>,
        large: Option<Arc<LargeBody>>,
        counts: Arc<RefCounts>,
    ) -> Arc<Self> {
        Self::from_parts(native.view(), Some(native), Bytes::new(), large, counts)
    }

    /// A native message whose view is already made, with its large body if
    /// it has one.
    pub fn with_view(
        native: Arc<dyn NativeMessage>,
        view: CoreMessage,
        large: Option<Arc<LargeBody>>,
    ) -> Arc<Self> {
        Self::from_parts(view, Some(native), Bytes::new(), large, Arc::default())
    }

    /// A large message kept in its own protocol's encoding, whose file is
    /// `body`.
    pub fn new_native_large(native: Arc<dyn NativeMessage>, body: Arc<LargeBody>) -> Arc<Self> {
        Self::native_counted(native, Some(body), Arc::default())
    }

    /// A large message: `core` holds the headers and properties, `body` the
    /// body.
    pub fn new_large(core: CoreMessage, body: Arc<LargeBody>) -> Arc<Self> {
        Self::large_counted(core, body, Arc::default())
    }

    fn large_counted(core: CoreMessage, body: Arc<LargeBody>, counts: Arc<RefCounts>) -> Arc<Self> {
        let encoded = exact(core.headers_and_properties_size(), |w| {
            core.encode_headers_and_properties(w)
        });
        Self::from_parts(core, None, encoded, Some(body), counts)
    }

    /// The same message with other headers or properties, sharing a large
    /// body: a fresh, separately accounted message for a reroute (a divert, a
    /// dead-letter or expiry copy, a replay), which is counted into its queues
    /// anew.
    pub fn with_core(&self, core: CoreMessage) -> Arc<Self> {
        self.with_core_counted(core, Arc::default())
    }

    /// The same message rebuilt in place for a reference that stays in its
    /// queue (a priority change, a redelivery's failure cause): it shares this
    /// message's reference and journal accounting, so the change neither leaks
    /// the memory the reference charged nor, for a multicast message, lets one
    /// queue delete a journal record another still holds.
    pub fn with_core_keeping_refs(&self, core: CoreMessage) -> Arc<Self> {
        self.with_core_counted(core, self.counts.clone())
    }

    fn with_core_counted(&self, core: CoreMessage, counts: Arc<RefCounts>) -> Arc<Self> {
        if let Some(n) = &self.native {
            let n = n.updated(&self.core, &core);
            return Self::native_counted(n, self.large.clone(), counts);
        }
        match &self.large {
            Some(body) => Self::large_counted(core, body.clone(), counts),
            None => Self::new_counted(core, counts),
        }
    }

    /// A copy with its own message ID (`Message.copy(newID)`): a large
    /// message's body is copied to a file of its own. `None` when that copy
    /// fails (`LargeServerMessageImpl.copy` returns null): the new ID has no
    /// body of its own, so the caller must not route a copy that cannot be
    /// recovered rather than share the original's body under a new ID.
    pub fn copy(&self, core: CoreMessage) -> Option<Arc<Self>> {
        if let Some(n) = &self.native {
            let n = n.updated(&self.core, &core);
            return match &self.large {
                None => Some(Self::new_native(n)),
                Some(body) => match body.copy(core.message_id, core.durable) {
                    Ok(copy) => Some(Self::new_native_large(n, copy)),
                    Err(e) => {
                        tracing::warn!(
                            "AMQ222181: Unable to copy large message {}: {e}",
                            self.message_id()
                        );
                        None
                    }
                },
            };
        }
        match &self.large {
            None => Some(Self::new(core)),
            Some(body) => match body.copy(core.message_id, core.durable) {
                Ok(copy) => Some(Self::new_large(core, copy)),
                Err(e) => {
                    tracing::warn!(
                        "AMQ222181: Unable to copy large message {}: {e}",
                        self.message_id()
                    );
                    None
                }
            },
        }
    }

    pub fn large(&self) -> Option<&Arc<LargeBody>> {
        self.large.as_ref()
    }

    pub fn is_large(&self) -> bool {
        self.large.is_some()
    }

    /// The message, or for a native message the broker's view of it.
    pub fn core(&self) -> &CoreMessage {
        &self.core
    }

    /// The message kept in another protocol's encoding, if it is one.
    pub fn native(&self) -> Option<&Arc<dyn NativeMessage>> {
        self.native.as_ref()
    }

    fn converted(&self) -> Option<&(CoreMessage, Vec<u8>)> {
        let n = self.native.as_ref()?;
        Some(self.converted.get_or_init(|| {
            let core = n.to_core();
            let mut w = Writer::new();
            core.encode(&mut w);
            Box::new((core, w.into_vec()))
        }))
    }

    /// The message as a Core message, for delivery to a client of another
    /// protocol (`Message.toCore`).
    pub fn delivery_core(&self) -> &CoreMessage {
        self.converted().map_or(&self.core, |c| &c.0)
    }

    /// [`delivery_core`](Self::delivery_core) as `CoreMessage.encode` writes
    /// it, or for a large message its headers and properties.
    pub fn encoded(&self) -> &[u8] {
        self.converted().map_or(&self.encoded, |c| &c.1)
    }

    /// The record the message's persister writes to the journal and to
    /// pages (`ADD_MESSAGE_PROTOCOL`).
    pub fn persisted(&self) -> PersistedMessage {
        match &self.native {
            Some(n) => n.persisted(),
            None => PersistedMessage::Core(CoreMessageRecord::new(self.core.clone())),
        }
    }

    pub fn message_id(&self) -> i64 {
        self.core.message_id
    }

    pub fn is_expired(&self, now: i64) -> bool {
        is_expired(&self.core, now)
    }

    /// The priority as a list index. Out-of-range values clamp, as
    /// `PriorityLinkedListImpl` does.
    pub fn priority_level(&self) -> usize {
        usize::from(self.core.priority.min(9))
    }

    /// `isDurable`: persistent messages are journaled on durable queues.
    pub fn is_durable(&self) -> bool {
        self.core.durable
    }

    /// `durableUp`: returns the new count.
    pub fn durable_up(&self) -> i32 {
        self.counts.durable_refs.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// `durableDown`: returns the new count.
    pub fn durable_down(&self) -> i32 {
        self.counts.durable_refs.fetch_sub(1, Ordering::SeqCst) - 1
    }

    /// `Message.getEncodeSize`, the size counted for memory accounting.
    pub fn encode_size(&self) -> usize {
        match &self.native {
            Some(n) => n.encode_size(),
            None => self.encoded.len(),
        }
    }

    /// `Message.getMemoryEstimate`: what the message adds to its address's
    /// size while referenced. A Core message counts `Message.memoryOffset`,
    /// its buffer and its properties' (`TypedProperties.getMemoryOffset`);
    /// a native message counts what its protocol's message does, with no
    /// conversion.
    pub fn memory_estimate(&self) -> i64 {
        *self.counts.estimate.get_or_init(|| match &self.native {
            Some(n) => n.memory_estimate(),
            None => {
                let props = &self.core.properties;
                352 + self.encoded.len() as i64
                    + props.encode_size() as i64
                    + 8 * props.entries().len() as i64
            }
        })
    }

    /// `refUp`: returns the new count.
    pub fn ref_up(&self) -> i32 {
        self.counts.refs.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// `refDown`: returns the new count.
    pub fn ref_down(&self) -> i32 {
        self.counts.refs.fetch_sub(1, Ordering::SeqCst) - 1
    }

    /// The store accounting the message's memory, set with its first
    /// reference.
    pub fn owner(
        &self,
        store: impl FnOnce() -> Option<Arc<crate::paging::PagingStore>>,
    ) -> Option<&Arc<crate::paging::PagingStore>> {
        if self.counts.owner.get().is_none()
            && let Some(s) = store()
        {
            let _ = self.counts.owner.set(s);
        }
        self.counts.owner.get()
    }
}

/// Filters read a native message as its protocol defines
/// (`getObjectPropertyForFilter`), and any other message as Core.
impl saranga_filter::Filterable for ServerMessage {
    fn get(&self, identifier: &str) -> Option<saranga_filter::Value> {
        match &self.native {
            Some(n) => n.filter_value(identifier),
            None => self.core.get(identifier),
        }
    }

    fn try_get(
        &self,
        identifier: &str,
    ) -> Result<Option<saranga_filter::Value>, saranga_filter::PropertyError> {
        match &self.native {
            Some(n) => Ok(n.filter_value(identifier)),
            None => self.core.try_get(identifier),
        }
    }

    /// `FilterableServerMessage.getBodyAs(String.class)`: none for a large
    /// message.
    fn string_body(&self) -> Option<String> {
        if self.is_large() {
            return None;
        }
        match &self.native {
            Some(n) => n.string_body(),
            None => saranga_filter::Filterable::string_body(&self.core),
        }
    }
}

/// Where a paged message is (`PagePosition` of a `PagedReference`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PagePosition {
    pub page: i64,
    pub message_nr: i32,
    /// The transaction that paged it; -1 outside one.
    pub tx_id: i64,
    /// What it counts as in its queue's page counter.
    pub size: i64,
    /// Its `ACKNOWLEDGE_CURSOR` record once journaled; -1 before.
    pub ack_record: i64,
}

/// `Message.getRoutingType`: the `_AMQ_ROUTING_TYPE` byte property.
pub fn routing_type(message: &CoreMessage) -> Option<RoutingType> {
    match message.properties.get_str(HDR_ROUTING_TYPE) {
        Some(PropertyValue::Byte(b)) => RoutingType::from_byte(*b),
        _ => None,
    }
}

/// A message on one queue: `MessageReference`.
#[derive(Debug, Clone)]
pub struct MessageRef {
    pub message: Arc<ServerMessage>,
    pub delivery_count: i32,
    /// The delivery count last written to the journal (`persistedCount`).
    pub persisted_count: i32,
    /// When the reference may be delivered, in epoch milliseconds; 0 for
    /// now (`scheduledDeliveryTime`).
    pub scheduled_time: i64,
    /// Set for a message read from a page: it is not in the journal.
    pub paged: Option<PagePosition>,
    /// In the store whatever its durability: read from the store's pages.
    pub stored: bool,
    /// Consumers the message is not given to again
    /// (`Message.rejectConsumer`).
    pub rejected_consumers: Vec<crate::queue::ConsumerId>,
}

impl MessageRef {
    pub fn new(message: Arc<ServerMessage>) -> Self {
        Self {
            message,
            delivery_count: 0,
            persisted_count: 0,
            scheduled_time: 0,
            paged: None,
            stored: false,
            rejected_consumers: Vec::new(),
        }
    }

    pub fn message_id(&self) -> i64 {
        self.message.message_id()
    }
}
