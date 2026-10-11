//! Messages as management shows them (`Message.toMap`), and the messages
//! management sends (`AbstractControl.sendMessage`).

use std::borrow::Cow;
use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString, Writer};
use saranga_message::{CoreMessage, message_type};

use super::json::Json;
use crate::Broker;
use crate::message::{MessageRef, ServerMessage};

/// The message as a client of another protocol would read it: a native
/// message's whole content, not the view the broker routes by.
fn full_core(m: &ServerMessage) -> Cow<'_, CoreMessage> {
    match m.native() {
        Some(n) => Cow::Owned(n.to_core()),
        None => Cow::Borrowed(m.core()),
    }
}

/// `JsonUtil.truncateString`.
fn truncate(s: String, limit: i32) -> String {
    let units: Vec<u16> = s.encode_utf16().collect();
    if limit >= 0 && units.len() > limit as usize {
        let head = String::from_utf16_lossy(&units[..limit as usize]);
        format!("{head}, + {} more", units.len() - limit as usize)
    } else {
        s
    }
}

/// A property as `getObjectProperty` then `JsonUtil.truncate` and
/// `addToObject` give it.
fn property_json(v: &PropertyValue, limit: i32) -> Json {
    match v {
        PropertyValue::Null => Json::from(""),
        PropertyValue::Bool(b) => Json::from(*b),
        PropertyValue::Byte(b) => Json::from(i32::from(*b)),
        PropertyValue::Short(s) => Json::from(i32::from(*s)),
        PropertyValue::Int(i) => Json::from(*i),
        PropertyValue::Long(l) => Json::from(*l),
        PropertyValue::Float(f) => Json::from(f64::from(*f)),
        PropertyValue::Double(d) => Json::from(*d),
        PropertyValue::Char(c) => Json::from(String::from_utf16_lossy(&[*c])),
        PropertyValue::String(s) => Json::from(truncate(s.to_string_lossy(), limit)),
        PropertyValue::Bytes(b) => {
            let n = if limit >= 0 {
                b.len().min(limit as usize)
            } else {
                b.len()
            };
            Json::Array(
                b[..n]
                    .iter()
                    .map(|x| Json::from(i32::from(*x as i8)))
                    .collect(),
            )
        }
    }
}

/// `Message.toMap(valueSizeLimit)`: the properties, then the headers.
pub(super) fn to_map(r: &MessageRef, limit: i32) -> Json {
    let core = full_core(&r.message);
    let mut o = Json::object();
    for (k, v) in core.properties.entries() {
        o.put(&k.to_string_lossy(), property_json(v, limit));
    }
    o.put("messageID", core.message_id);
    if let Some(u) = core.user_id {
        o.put("userID", format!("ID:{}", uuid::Uuid::from_bytes(u)));
    }
    o.put(
        "address",
        core.address
            .as_ref()
            .map(|a| a.to_string_lossy())
            .unwrap_or_default(),
    );
    o.put("durable", core.durable);
    o.put("expiration", core.expiration);
    o.put("timestamp", core.timestamp);
    o.put("priority", i32::from(core.priority));
    o
}

/// `Objects.toString(getObjectPropertyForFilter(name), null)`.
pub(super) fn filter_value_string(m: &ServerMessage, name: &str) -> Option<String> {
    use saranga_filter::{Filterable, Value};
    Some(match m.get(name)? {
        Value::Bool(b) => b.to_string(),
        Value::Byte(b) => b.to_string(),
        Value::Short(s) => s.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Long(l) => l.to_string(),
        Value::Float(f) => super::json::java_double(f64::from(f)).replace(".0E", "E"),
        Value::Double(d) => super::json::java_double(d),
        Value::Char(c) => String::from_utf16_lossy(&[c]),
        Value::String(s) => s,
        other => format!("{other:?}"),
    })
}

/// `AbstractControl.sendMessage`: a message made of `headers` (as string
/// properties), `type` and `body` (text, or base64 for the other types)
/// is sent to `address` in a session of `user`. Returns its ID.
#[allow(clippy::too_many_arguments)]
pub(super) async fn send(
    broker: &Arc<Broker>,
    address: &str,
    headers: &[(String, String)],
    kind: i32,
    body: Option<String>,
    durable: bool,
    user: Option<String>,
    password: Option<String>,
    create_message_id: bool,
) -> Result<String, String> {
    let info = Arc::new(saranga_security::ConnectionInfo {
        remote_address: "management".to_owned(),
        certificates: None,
        security_domain: None,
    });
    let authenticated = match broker.security() {
        Some(s) => s
            .authenticate(user.as_deref(), password.as_deref(), &info)
            .map_err(|e| e.0)?,
        None => Default::default(),
    };
    let conn = Arc::new(crate::session::ClientConnection::new(
        crate::session::Transport::internal("management"),
        info,
        Vec::new(),
    ));
    let mut session = crate::session::ServerSession::new(
        broker.clone(),
        conn,
        crate::session::SessionOptions {
            name: format!("management::{}", uuid::Uuid::new_v4()),
            username: user,
            password,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: Some(address.to_owned()),
        },
        authenticated,
    );
    let id = broker.post_office().ids().next();
    let mut m = CoreMessage {
        message_id: id,
        address: Some(SimpleString::from(address)),
        message_type: kind as u8,
        durable,
        timestamp: crate::message::now_millis(),
        priority: 4,
        ..Default::default()
    };
    for (k, v) in headers {
        m.properties.insert(
            SimpleString::from(k.as_str()),
            PropertyValue::String(SimpleString::from(v.as_str())),
        );
    }
    if let Some(body) = body {
        if kind as u8 == message_type::TEXT {
            let mut w = Writer::new();
            w.nullable_simple_string(Some(&SimpleString::from(body.as_str())));
            m.body = w.into_vec();
        } else {
            m.body = base64_decode(&body).ok_or("Invalid base64 body")?;
        }
    }
    if create_message_id {
        m.user_id = Some(*uuid::Uuid::new_v4().as_bytes());
    }
    let sent = session.send_with_id(m).await;
    session.close(false).await;
    // The routed message reaches its queues once journaled.
    let written = session.take_completion().wait().await;
    sent.map_err(|e| e.message)?;
    written.map_err(|e| e.to_string())?;
    Ok(id.to_string())
}

/// `Base64.decode` (standard alphabet, padding optional, whitespace
/// ignored).
pub(super) fn base64_decode(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0;
    for c in text.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            b'=' => break,
            b' ' | b'\n' | b'\r' | b'\t' => continue,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}
