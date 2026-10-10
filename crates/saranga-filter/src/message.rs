//! Filtering Core messages. Ported from `FilterImpl.FilterableServerMessage`
//! and `FilterImpl.getHeaderFieldValue`.

use saranga_buffer::{PropertyValue, SimpleString, Writer};
use saranga_message::CoreMessage;

use crate::{Filterable, PropertyError, Value};

/// `FilterConstants.ACTIVEMQ_PREFIX`: identifiers starting with this are
/// tried as message headers first.
const ACTIVEMQ_PREFIX: &str = "AMQ";
/// `FederatedAddress.HDR_HOPS`.
const HDR_HOPS: &str = "_AMQ_Hops";
/// `Message.HDR_GROUP_ID`.
const HDR_GROUP_ID: &str = "_AMQ_GROUP_ID";
/// `FilterConstants.NATIVE_MESSAGE_ID`.
const NATIVE_MESSAGE_ID: &str = "NATIVE_MESSAGE_ID";

impl Filterable for CoreMessage {
    fn get(&self, identifier: &str) -> Option<Value> {
        self.try_get(identifier).ok().flatten()
    }

    /// Header identifiers (`AMQUserID`, `AMQPriority`, `AMQTimestamp`,
    /// `AMQDurable`, `AMQExpiration`, `AMQSize`, `AMQAddress`, `AMQGroupID`)
    /// read the message headers. Anything else, including a header that is
    /// null, falls back to the message property of the same name.
    fn try_get(&self, identifier: &str) -> Result<Option<Value>, PropertyError> {
        let mut result = None;
        if identifier.starts_with(ACTIVEMQ_PREFIX) {
            result = header(self, identifier)?;
        }
        // Any identifier that merely starts with `_AMQ_Hops` reads the hops
        // count, as in Artemis.
        if identifier.starts_with(HDR_HOPS) {
            result = hops(self)?;
        }
        if result.is_none() {
            result = self
                .properties
                .get(&SimpleString::from(identifier))
                .and_then(Value::from_property);
        }
        Ok(result)
    }
}

fn header(msg: &CoreMessage, id: &str) -> Result<Option<Value>, PropertyError> {
    Ok(match id {
        "AMQUserID" => match &msg.user_id {
            // Core-to-AMQP conversion may keep an arbitrary message ID here.
            None => string_property(msg, NATIVE_MESSAGE_ID)?,
            Some(uuid) => Some(format!("ID:{}", uuid_string(uuid))),
        }
        .map(Value::String),
        // `(int) msg.getPriority()`: the priority byte is signed.
        "AMQPriority" => Some(Value::Int(i32::from(msg.priority as i8))),
        "AMQTimestamp" => Some(Value::Long(msg.timestamp)),
        "AMQDurable" => Some(Value::String(
            if msg.durable {
                "DURABLE"
            } else {
                "NON_DURABLE"
            }
            .into(),
        )),
        "AMQExpiration" => Some(Value::Long(msg.expiration)),
        "AMQSize" => Some(Value::Int(encode_size(msg))),
        "AMQAddress" => msg
            .address
            .as_ref()
            .map(|a| Value::String(a.to_string_lossy())),
        "AMQGroupID" => string_property(msg, HDR_GROUP_ID)?.map(Value::String),
        _ => None,
    })
}

/// `CoreMessage.getEncodeSize()`: the length of the full message buffer as
/// `CoreMessage::encode` writes it (end-of-body position, body, headers and
/// properties), which is what the Java broker holds for a message it
/// received.
pub(crate) fn encode_size(msg: &CoreMessage) -> i32 {
    let mut w = Writer::new();
    msg.encode(&mut w);
    w.len() as i32
}

/// `ByteUtil.bytesToInt(message.getExtraBytesProperty(HDR_HOPS))`.
fn hops(msg: &CoreMessage) -> Result<Option<Value>, PropertyError> {
    let error = |reason: &str| PropertyError {
        identifier: HDR_HOPS.into(),
        reason: reason.into(),
    };
    match msg.properties.get(&SimpleString::from(HDR_HOPS)) {
        None | Some(PropertyValue::Null) => Ok(None),
        Some(PropertyValue::Bytes(b)) => match b.first_chunk::<4>() {
            Some(bytes) => Ok(Some(Value::Int(i32::from_be_bytes(*bytes)))),
            None => Err(error("fewer than 4 bytes")),
        },
        Some(_) => Err(error("Invalid conversion")),
    }
}

/// `CoreMessage.getStringProperty`: scalar properties are converted with
/// Java's `toString`, and `byte[]` cannot be converted.
fn string_property(msg: &CoreMessage, key: &str) -> Result<Option<String>, PropertyError> {
    use crate::java::{double_to_string, float_to_string};
    let Some(value) = msg.properties.get(&SimpleString::from(key)) else {
        return Ok(None);
    };
    Ok(Some(match value {
        PropertyValue::Null => return Ok(None),
        PropertyValue::String(s) => s.to_string_lossy(),
        PropertyValue::Bool(v) => v.to_string(),
        PropertyValue::Char(c) => String::from_utf16_lossy(&[*c]),
        PropertyValue::Byte(v) => v.to_string(),
        PropertyValue::Short(v) => v.to_string(),
        PropertyValue::Int(v) => v.to_string(),
        PropertyValue::Long(v) => v.to_string(),
        PropertyValue::Float(v) => float_to_string(*v),
        PropertyValue::Double(v) => double_to_string(*v),
        PropertyValue::Bytes(_) => {
            return Err(PropertyError {
                identifier: key.into(),
                reason: "Invalid conversion".into(),
            });
        }
    }))
}

/// `org.apache.activemq.artemis.utils.UUID.toString`.
fn uuid_string(id: &[u8; 16]) -> String {
    let mut s = String::with_capacity(36);
    for (i, b) in id.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_format() {
        let id: [u8; 16] = std::array::from_fn(|i| (i * 17) as u8);
        assert_eq!(uuid_string(&id), "00112233-4455-6677-8899-aabbccddeeff");
    }
}
