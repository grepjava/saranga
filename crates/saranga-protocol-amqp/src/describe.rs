//! `artemis data print` for AMQP messages: `AMQPStandardMessage.toString`
//! and `getUserID`, with Java's and Proton-J's `toString` of what they
//! print.

use saranga_storage::describe::properties_to_string;
use saranga_storage::java;
use saranga_storage::message::PersistedMessage;

use crate::convert::uuid_string;
use crate::message::AmqpMessage;
use crate::messaging::Properties;
use crate::types::Value;

/// Lets `data print` describe AMQP messages.
pub fn register() {
    saranga_storage::describe::set_amqp_describer(describe);
}

/// `getUserID` and `toString` of a message read back from a record.
pub fn describe(record: &PersistedMessage) -> Option<(Option<String>, String)> {
    // The time only matters to a TTL, which toString does not show.
    let (decoded, class, address) = match record {
        PersistedMessage::Amqp(r) => (
            AmqpMessage::from_record(r, 0),
            "AMQPStandardMessage",
            r.address.clone(),
        ),
        // The file is not read: what is printed is in the record.
        PersistedMessage::AmqpLarge(r) => (
            AmqpMessage::from_large_record(r, Vec::new(), 0),
            "AMQPLargeMessage",
            r.address.clone(),
        ),
        PersistedMessage::Core(_) => return None,
    };
    let Ok(m) = decoded else {
        return Some((
            None,
            format!(
                "{class}(<undecodable>, address={})",
                java::nullable(address.as_ref())
            ),
        ));
    };
    let user_id = m
        .properties()
        .and_then(|p| p.message_id.as_ref())
        .map(to_string);
    let app = m.application_properties();
    let application_properties = map_string(app.iter().map(|(k, v)| (k.clone(), to_string(v))));
    let message_annotations = match m.message_annotations() {
        Some(ma) => map_string(ma.iter().map(|(k, v)| (k.0.clone(), to_string(v)))),
        None => "{}".into(),
    };
    let properties = m
        .properties()
        .map_or_else(|| "null".into(), properties_string);
    let extra = m
        .record()
        .extra_properties
        .as_ref()
        .map_or_else(|| "null".into(), properties_to_string);
    let (durable, size) = match record {
        // The file's durability; getEncodeSize is 0.
        PersistedMessage::AmqpLarge(r) => (r.durable, 0),
        // Data print scans the message (getUserID) before toString: a
        // header without durable is then not durable.
        _ => (
            m.header().and_then(|h| h.durable).unwrap_or(false),
            m.encode_size(),
        ),
    };
    let text = format!(
        "{class}( [durable={durable}, messageID={}, address={}, size={size}, scanningStatus=SCANNED, applicationProperties={application_properties}, messageAnnotations={message_annotations}, properties={properties}, extraProperties = {extra}]",
        m.message_id(),
        java::nullable(m.address().as_ref()),
    );
    Some((user_id, text))
}

/// `AbstractMap.toString`.
fn map_string(entries: impl Iterator<Item = (String, String)>) -> String {
    let parts: Vec<String> = entries.map(|(k, v)| format!("{k}={v}")).collect();
    format!("{{{}}}", parts.join(", "))
}

/// `Object.toString` of a decoded AMQP value, as Proton-J gives it.
pub fn to_string(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Boolean(b) => b.to_string(),
        Value::Ubyte(n) => n.to_string(),
        Value::Ushort(n) => n.to_string(),
        Value::Uint(n) => n.to_string(),
        Value::Ulong(n) => n.to_string(),
        Value::Byte(n) => n.to_string(),
        Value::Short(n) => n.to_string(),
        Value::Int(n) => n.to_string(),
        Value::Long(n) => n.to_string(),
        Value::Float(f) => java::float(*f),
        Value::Double(d) => java::double(*d),
        Value::Char(c) => char::from_u32(*c).map_or_else(String::new, |c| c.to_string()),
        Value::Timestamp(t) => java::date(*t),
        Value::Uuid(u) => uuid_string(u),
        // Binary.toString
        Value::Binary(b) => {
            let mut s = String::from("\"");
            for &c in b {
                if c > 31 && c < 127 && c != b'\\' {
                    s.push(c as char);
                } else {
                    s.push_str(&format!("\\x{c:02x}"));
                }
            }
            s.push('"');
            s
        }
        Value::String(s) => s.clone(),
        Value::Symbol(s) => s.0.clone(),
        Value::List(items) => format!(
            "[{}]",
            items.iter().map(to_string).collect::<Vec<_>>().join(", ")
        ),
        Value::Map(entries) => {
            map_string(entries.iter().map(|(k, v)| (to_string(k), to_string(v))))
        }
        Value::Array(a) => format!(
            "[{}]",
            a.items.iter().map(to_string).collect::<Vec<_>>().join(", ")
        ),
        Value::Decimal32(_) | Value::Decimal64(_) | Value::Decimal128(_) => format!("{v:?}"),
        Value::Described(d, body) => format!("{d:?} {}", to_string(body)),
    }
}

/// Proton-J's `Properties.toString`.
fn properties_string(p: &Properties) -> String {
    let opt = |v: &Option<Value>| v.as_ref().map_or_else(|| "null".into(), to_string);
    let quoted = |s: &Option<String>| format!("'{}'", s.as_deref().unwrap_or("null"));
    let sym = |s: &Option<crate::Symbol>| s.as_ref().map_or_else(|| "null".into(), |s| s.0.clone());
    let time = |t: &Option<crate::Timestamp>| t.map_or_else(|| "null".into(), |t| java::date(t.0));
    format!(
        "Properties{{messageId={}, userId={}, to={}, subject={}, replyTo={}, correlationId={}, contentType={}, contentEncoding={}, absoluteExpiryTime={}, creationTime={}, groupId={}, groupSequence={}, replyToGroupId={}}}",
        opt(&p.message_id),
        p.user_id
            .as_ref()
            .map_or_else(|| "null".into(), |u| to_string(&Value::Binary(u.clone()))),
        quoted(&p.to),
        quoted(&p.subject),
        quoted(&p.reply_to),
        opt(&p.correlation_id),
        sym(&p.content_type),
        sym(&p.content_encoding),
        time(&p.absolute_expiry_time),
        time(&p.creation_time),
        quoted(&p.group_id),
        p.group_sequence
            .map_or_else(|| "null".into(), |n| n.to_string()),
        quoted(&p.reply_to_group_id),
    )
}
