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

/// `Float.toString`.
fn java_float(f: f32) -> String {
    let abs = f.abs();
    if f.is_finite() && f != 0.0 && (1e-3..1e7).contains(&abs) {
        let s = format!("{f}");
        if s.contains('.') { s } else { format!("{s}.0") }
    } else if f.is_finite() && f != 0.0 {
        let s = format!("{f:e}");
        let (mantissa, exp) = s.split_once('e').unwrap_or((&s, "0"));
        let mantissa = if mantissa.contains('.') {
            mantissa.to_owned()
        } else {
            format!("{mantissa}.0")
        };
        format!("{mantissa}E{exp}")
    } else {
        super::json::java_double(f64::from(f))
    }
}

/// A property value's `toString`, as `"" + map` writes it.
fn property_text(v: &PropertyValue, limit: i32) -> String {
    match v {
        PropertyValue::Null => String::new(),
        PropertyValue::Bool(b) => b.to_string(),
        PropertyValue::Byte(b) => b.to_string(),
        PropertyValue::Short(s) => s.to_string(),
        PropertyValue::Int(i) => i.to_string(),
        PropertyValue::Long(l) => l.to_string(),
        PropertyValue::Float(f) => java_float(*f),
        PropertyValue::Double(d) => super::json::java_double(*d),
        PropertyValue::Char(c) => String::from_utf16_lossy(&[*c]),
        PropertyValue::String(s) => truncate(s.to_string_lossy(), limit),
        // An array's identity hash: no two runs agree on it.
        PropertyValue::Bytes(b) => format!("[B@{:x}", b.as_ptr() as usize & 0x7fff_ffff),
    }
}

/// `Message.toCompositeData(valueSizeLimit, deliveryCount)` as Jolokia
/// writes the composite: its items sorted by name, the typed property
/// tables as maps (null when empty), and `text` for a text message or
/// `BodyPreview` for any other.
pub(super) fn to_composite(r: &MessageRef, limit: i32) -> Json {
    let m = &r.message;
    let core = full_core(m);
    composite(
        &core,
        &Composed {
            protocol: Some(if m.native().is_some() { "AMQP" } else { "CORE" }),
            redelivered: r.delivery_count > 1,
            large: m.is_large(),
            persistent_size: m.encode_size() as i64,
        },
        limit,
    )
}

/// What a composite says of a message beside its content.
pub struct Composed {
    /// Its protocol, when known: a message a client received is Core
    /// whatever it was sent as.
    pub protocol: Option<&'static str>,
    pub redelivered: bool,
    pub large: bool,
    pub persistent_size: i64,
}

/// A Core message as [`to_composite`] writes a queue's: for messages a
/// client browsed, as the console browses another node's queue.
pub fn composite(core: &CoreMessage, about: &Composed, limit: i32) -> Json {
    let names: Vec<String> = core
        .properties
        .entries()
        .iter()
        .map(|(k, _)| k.to_string_lossy())
        .collect();
    let order = super::security::java_hash_order(names, false);
    let props: Vec<(String, &PropertyValue)> = order
        .into_iter()
        .filter_map(|n| {
            core.properties
                .entries()
                .iter()
                .find(|(k, _)| k.to_string_lossy() == n)
                .map(|(_, v)| (n, v))
        })
        .collect();
    let text = props
        .iter()
        .map(|(k, v)| format!("{k}={}", property_text(v, limit)))
        .collect::<Vec<_>>()
        .join(", ");
    let typed = |pick: &dyn Fn(&PropertyValue) -> Option<Json>| {
        let mut o = Json::object();
        for (k, v) in &props {
            if let Some(j) = pick(v) {
                o.put(k, j);
            }
        }
        match &o {
            Json::Object(items) if items.is_empty() => Json::Null,
            _ => o,
        }
    };
    let mut items: Vec<(&str, Json)> = vec![
        ("messageID", Json::from(core.message_id.to_string())),
        ("protocol", about.protocol.map_or(Json::Null, Json::from)),
        (
            "userID",
            Json::from(
                core.user_id
                    .map(|u| format!("ID:{}", uuid::Uuid::from_bytes(u)))
                    .unwrap_or_default(),
            ),
        ),
        (
            "address",
            Json::from(
                core.address
                    .as_ref()
                    .map(|a| a.to_string_lossy())
                    .unwrap_or_default(),
            ),
        ),
        ("durable", Json::from(core.durable)),
        ("expiration", Json::from(core.expiration)),
        ("timestamp", Json::from(core.timestamp)),
        ("priority", Json::from(i32::from(core.priority))),
        ("redelivered", Json::from(about.redelivered)),
        ("largeMessage", Json::from(about.large)),
        ("persistentSize", Json::from(about.persistent_size)),
        (
            "PropertiesText",
            Json::from(truncate(format!("{{{text}}}"), limit)),
        ),
        (
            "StringProperties",
            typed(&|v| match v {
                PropertyValue::String(s) => Some(Json::from(truncate(s.to_string_lossy(), limit))),
                PropertyValue::Null => Some(Json::from("")),
                _ => None,
            }),
        ),
        (
            "BooleanProperties",
            typed(&|v| match v {
                PropertyValue::Bool(b) => Some(Json::from(*b)),
                _ => None,
            }),
        ),
        (
            "ByteProperties",
            typed(&|v| match v {
                PropertyValue::Byte(b) => Some(Json::from(i32::from(*b))),
                _ => None,
            }),
        ),
        (
            "ShortProperties",
            typed(&|v| match v {
                PropertyValue::Short(s) => Some(Json::from(i32::from(*s))),
                _ => None,
            }),
        ),
        (
            "IntProperties",
            typed(&|v| match v {
                PropertyValue::Int(i) => Some(Json::from(*i)),
                _ => None,
            }),
        ),
        (
            "LongProperties",
            typed(&|v| match v {
                PropertyValue::Long(l) => Some(Json::from(*l)),
                _ => None,
            }),
        ),
        (
            "FloatProperties",
            typed(&|v| match v {
                PropertyValue::Float(f) => {
                    Some(Json::from(java_float(*f).parse::<f64>().unwrap_or(0.0)))
                }
                _ => None,
            }),
        ),
        (
            "DoubleProperties",
            typed(&|v| match v {
                PropertyValue::Double(d) => Some(Json::from(*d)),
                _ => None,
            }),
        ),
        ("type", Json::from(i32::from(core.message_type))),
    ];
    if core.message_type == message_type::TEXT {
        let body = if about.large {
            "[large message]".to_owned()
        } else if core
            .properties
            .entries()
            .iter()
            .any(|(k, _)| k.to_string_lossy() == "_AMQ_LARGE_COMPRESSED")
        {
            "[compressed]".to_owned()
        } else {
            let text = saranga_buffer::Reader::new(&core.body)
                .nullable_simple_string()
                .ok()
                .flatten();
            text.map(|t| truncate(t.to_string_lossy(), limit))
                .unwrap_or_default()
        };
        items.push(("text", Json::from(body)));
    } else {
        let body: &[u8] = if about.large { &[] } else { &core.body };
        let n = if limit >= 0 {
            body.len().min(limit as usize)
        } else {
            body.len()
        };
        items.push((
            "BodyPreview",
            Json::Array(
                body[..n]
                    .iter()
                    .map(|x| Json::from(i32::from(*x as i8)))
                    .collect(),
            ),
        ));
    }
    items.sort_by(|a, b| a.0.cmp(b.0));
    let mut o = Json::object();
    for (k, v) in items {
        o.put(k, v);
    }
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
    let mut m = CoreMessage {
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
            m.body = w.into_vec().into();
        } else {
            m.body = base64_decode(&body).ok_or("Invalid base64 body")?.into();
        }
    }
    if create_message_id {
        m.user_id = Some(*uuid::Uuid::new_v4().as_bytes());
    }
    send_as(broker, address, m, user, password).await
}

/// A message sent to `address` in a session of `user`, or of the call's
/// own subject when no user and no password are given, with a new ID,
/// which is returned.
async fn send_as(
    broker: &Arc<Broker>,
    address: &str,
    mut m: CoreMessage,
    user: Option<String>,
    password: Option<String>,
) -> Result<String, String> {
    let info = Arc::new(saranga_security::ConnectionInfo {
        remote_address: "management".to_owned(),
        certificates: None,
        security_domain: None,
        connection_id: None,
    });
    // With no user and no password, the call's own subject
    // (`SecurityStoreImpl.authenticate` on a `ManagementRemotingConnection`).
    let authenticated = match (broker.security(), super::current_subject()) {
        (Some(s), Some(subject)) if user.is_none() && password.is_none() => {
            s.authenticate_as(subject, &info)
        }
        (Some(s), _) => s
            .authenticate(user.as_deref(), password.as_deref(), &info)
            .map_err(|e| e.0)?,
        (None, _) => Default::default(),
    };
    let conn = Arc::new(crate::session::ClientConnection::new(
        crate::session::Transport::management(),
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
    session.on_management_connection();
    let id = broker.post_office().ids().next();
    m.message_id = id;
    m.address = Some(SimpleString::from(address));
    let sent = session.send_with_id(m).await;
    session.close(false).await;
    // The routed message reaches its queues once journaled.
    let written = session.take_completion().wait().await;
    sent.map_err(|e| e.message)?;
    written.map_err(|e| e.to_string())?;
    Ok(id.to_string())
}

/// The console's import: each message (as [`super::export`] writes them)
/// sent to `address` as the call's own subject, in order. Returns how many
/// were sent, and why the next one was not.
pub async fn import(
    broker: &Arc<Broker>,
    address: &str,
    messages: Vec<CoreMessage>,
) -> (usize, Option<String>) {
    let mut sent = 0;
    for m in messages {
        if let Err(e) = send_as(broker, address, m, None, None).await {
            return (sent, Some(e));
        }
        sent += 1;
    }
    (sent, None)
}

/// The console's export of a queue: up to `max` of its messages that
/// `filter` selects, whole, as [`super::export`] writes them, a large
/// message's body read from its file.
pub async fn export(
    broker: &Broker,
    queue: &str,
    filter: Option<String>,
    max: usize,
) -> Result<Vec<Json>, String> {
    let q = broker
        .post_office()
        .queue(queue)
        .ok_or_else(|| format!("AMQ229017: Queue {queue} does not exist"))?;
    let f = super::queue::filter(broker, filter)?;
    let mut out = Vec::new();
    for r in q.browse().await.iter() {
        if out.len() >= max {
            break;
        }
        if f.as_ref().is_some_and(|f| !f.matches(&*r.message)) {
            continue;
        }
        let mut core = full_core(&r.message).into_owned();
        if let Some(large) = r.message.large() {
            core.body = large.read_all().map_err(|e| e.to_string())?.into();
        }
        out.push(super::export::message_json(&core));
    }
    Ok(out)
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
