//! Messages as the broker holds them: routed once, shared by every queue
//! and consumer through `Arc`, with the encoded form cached.

use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString, Writer};
use saranga_message::CoreMessage;

use crate::RoutingType;

/// `Message.HDR_ROUTING_TYPE`.
pub const HDR_ROUTING_TYPE: &str = "_AMQ_ROUTING_TYPE";

/// A message after the broker has assigned its ID and address.
#[derive(Debug)]
pub struct ServerMessage {
    core: CoreMessage,
    /// `core` encoded as `CoreMessage.encode` writes it, the form carried by
    /// `SESS_RECEIVE_MSG`.
    encoded: Vec<u8>,
}

impl ServerMessage {
    pub fn new(core: CoreMessage) -> Arc<Self> {
        let mut w = Writer::new();
        core.encode(&mut w);
        Arc::new(Self {
            core,
            encoded: w.into_vec(),
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

    /// The priority as a list index. Out-of-range values clamp, as
    /// `PriorityLinkedListImpl` does.
    pub fn priority_level(&self) -> usize {
        usize::from(self.core.priority.min(9))
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
}

impl MessageRef {
    pub fn new(message: Arc<ServerMessage>) -> Self {
        Self {
            message,
            delivery_count: 0,
        }
    }

    pub fn message_id(&self) -> i64 {
        self.message.message_id()
    }
}
