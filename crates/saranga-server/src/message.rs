//! Messages as the broker holds them: routed once, shared by every queue
//! and consumer through `Arc`, with the encoded form cached.

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use saranga_buffer::{PropertyValue, SimpleString, Writer};
use saranga_message::CoreMessage;

use crate::RoutingType;
use crate::settings::AddressSettings;

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

/// `System.currentTimeMillis`.
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn property<'a>(message: &'a CoreMessage, name: &str) -> Option<&'a PropertyValue> {
    message.properties.get(&SimpleString::from(name))
}

/// A numeric property as a long, as `getLongProperty` would read one
/// written as any integer type.
fn long_property(message: &CoreMessage, name: &str) -> Option<i64> {
    match property(message, name)? {
        PropertyValue::Byte(v) => Some(i64::from(*v)),
        PropertyValue::Short(v) => Some(i64::from(*v)),
        PropertyValue::Int(v) => Some(i64::from(*v)),
        PropertyValue::Long(v) => Some(*v),
        _ => None,
    }
}

/// `CoreMessage.getScheduledDeliveryTime`: 0 when unset.
pub fn scheduled_delivery_time(message: &CoreMessage) -> i64 {
    long_property(message, HDR_SCHEDULED_DELIVERY_TIME).unwrap_or(0)
}

/// `Message.isExpired`.
pub fn is_expired(message: &CoreMessage, now: i64) -> bool {
    message.expiration != 0 && now - message.expiration >= 0
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

/// A message after the broker has assigned its ID and address.
#[derive(Debug)]
pub struct ServerMessage {
    core: CoreMessage,
    /// `core` encoded as `CoreMessage.encode` writes it, the form carried by
    /// `SESS_RECEIVE_MSG`.
    encoded: Vec<u8>,
    /// References on durable queues not yet acknowledged
    /// (`RefCountMessage.durableUp/Down`); the record is deleted at zero.
    durable_refs: AtomicI32,
}

impl ServerMessage {
    pub fn new(core: CoreMessage) -> Arc<Self> {
        let mut w = Writer::new();
        core.encode(&mut w);
        Arc::new(Self {
            core,
            encoded: w.into_vec(),
            durable_refs: AtomicI32::new(0),
        })
    }

    pub fn core(&self) -> &CoreMessage {
        &self.core
    }

    pub fn encoded(&self) -> &[u8] {
        &self.encoded
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
        self.durable_refs.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// `durableDown`: returns the new count.
    pub fn durable_down(&self) -> i32 {
        self.durable_refs.fetch_sub(1, Ordering::SeqCst) - 1
    }

    /// `Message.getEncodeSize`, the size counted for memory accounting.
    pub fn encode_size(&self) -> usize {
        self.encoded.len()
    }
}

/// `Message.getRoutingType`: the `_AMQ_ROUTING_TYPE` byte property.
pub fn routing_type(message: &CoreMessage) -> Option<RoutingType> {
    match message
        .properties
        .get(&SimpleString::from(HDR_ROUTING_TYPE))
    {
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
}

impl MessageRef {
    pub fn new(message: Arc<ServerMessage>) -> Self {
        Self {
            message,
            delivery_count: 0,
            persisted_count: 0,
            scheduled_time: 0,
        }
    }

    pub fn message_id(&self) -> i64 {
        self.message.message_id()
    }
}
