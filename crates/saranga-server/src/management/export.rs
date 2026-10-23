//! Messages written out as JSON and read back, for the console's export
//! and import of a queue's messages. Unlike `browse`, which shows a message
//! (its bodies and properties cut at `management-message-attribute-size-limit`,
//! byte and char properties left out), this keeps all of it: every property
//! with its type, and the body whole, so a message imported is the message
//! exported, but for its ID.
//!
//! A message is an object:
//!
//! ```json
//! {"messageID": "12", "address": "orders", "type": 3, "durable": true,
//!  "priority": 4, "timestamp": 1792717353807, "expiration": 0,
//!  "userID": "ID:4d0c...", "properties": [{"name": "color", "type": "string",
//!  "value": "red"}], "text": "hello"}
//! ```
//!
//! A text message's body is its `text` (absent for a null text); any other
//! body is `body`, its bytes in base64. A property's `type` is `null`,
//! `boolean`, `byte`, `short`, `int`, `long`, `float`, `double`, `char`,
//! `bytes` or `string`; a long's value is text, which JavaScript cannot
//! hold as a number, a char's a one-character string, bytes' base64.

use saranga_buffer::{PropertyValue, Reader, SimpleString, Writer};
use saranga_message::{CoreMessage, message_type};

use super::json::Json;

/// A message as the export writes it.
pub fn message_json(m: &CoreMessage) -> Json {
    let properties: Vec<Json> = m
        .properties
        .entries()
        .iter()
        .map(|(k, v)| {
            let (kind, value) = property(v);
            Json::object()
                .with("name", k.to_string_lossy())
                .with("type", kind)
                .with("value", value)
        })
        .collect();
    let mut o = Json::object()
        .with("messageID", m.message_id.to_string())
        .with(
            "address",
            m.address
                .as_ref()
                .map_or(Json::Null, |a| Json::from(a.to_string_lossy())),
        )
        .with("type", i32::from(m.message_type))
        .with("durable", m.durable)
        .with("priority", i32::from(m.priority))
        .with("timestamp", m.timestamp)
        .with("expiration", m.expiration)
        .with(
            "userID",
            m.user_id.map_or(Json::Null, |u| {
                Json::from(format!("ID:{}", uuid::Uuid::from_bytes(u)))
            }),
        )
        .with("properties", Json::Array(properties));
    let text = (m.message_type == message_type::TEXT)
        .then(|| Reader::new(&m.body).nullable_simple_string().ok())
        .flatten();
    match text {
        Some(Some(t)) => o.put("text", t.to_string_lossy()),
        Some(None) => {}
        None => o.put("body", base64(&m.body)),
    }
    o
}

fn property(v: &PropertyValue) -> (&'static str, Json) {
    match v {
        PropertyValue::Null => ("null", Json::Null),
        PropertyValue::Bool(b) => ("boolean", Json::from(*b)),
        PropertyValue::Byte(b) => ("byte", Json::from(i32::from(*b))),
        PropertyValue::Short(s) => ("short", Json::from(i32::from(*s))),
        PropertyValue::Int(i) => ("int", Json::from(*i)),
        PropertyValue::Long(l) => ("long", Json::from(l.to_string())),
        PropertyValue::Float(f) => ("float", Json::from(f64::from(*f))),
        PropertyValue::Double(d) => ("double", Json::from(*d)),
        PropertyValue::Char(c) => ("char", Json::from(String::from_utf16_lossy(&[*c]))),
        PropertyValue::Bytes(b) => ("bytes", Json::from(base64(b))),
        PropertyValue::String(s) => ("string", Json::from(s.to_string_lossy())),
    }
}

/// A message as [`message_json`] wrote it, without its ID or its address:
/// the import gives it those.
pub fn message_from_json(j: &Json) -> Result<CoreMessage, String> {
    let Json::Object(_) = j else {
        return Err("a message must be a JSON object".to_owned());
    };
    let int = |k: &str| j.get(k).and_then(Json::as_i64);
    let message_type = match int("type") {
        Some(t) => u8::try_from(t).map_err(|_| format!("no message type {t}"))?,
        None => message_type::DEFAULT,
    };
    let mut m = CoreMessage {
        message_type,
        durable: j.get("durable").and_then(Json::as_bool).unwrap_or(true),
        priority: int("priority").map(|p| p.clamp(0, 9) as u8).unwrap_or(4),
        timestamp: int("timestamp").unwrap_or(0),
        expiration: int("expiration").unwrap_or(0),
        ..CoreMessage::default()
    };
    if let Some(u) = j.get("userID").and_then(Json::as_str) {
        let id = uuid::Uuid::parse_str(u.strip_prefix("ID:").unwrap_or(u))
            .map_err(|_| format!("userID {u} is not a UUID"))?;
        m.user_id = Some(*id.as_bytes());
    }
    if let Some(Json::Array(properties)) = j.get("properties") {
        for p in properties {
            let name = p
                .get("name")
                .and_then(Json::as_str)
                .ok_or("a property needs a name")?;
            let kind = p.get("type").and_then(Json::as_str).unwrap_or("string");
            let value = property_from(kind, p.get("value").unwrap_or(&Json::Null))
                .map_err(|e| format!("property {name}: {e}"))?;
            m.properties.insert(SimpleString::from(name), value);
        }
    }
    if let Some(text) = j.get("text") {
        let text = text.as_str().ok_or("text must be a string")?;
        let mut w = Writer::new();
        w.nullable_simple_string(Some(&SimpleString::from(text)));
        m.body = w.into_vec().into();
    } else if let Some(body) = j.get("body") {
        let body = body.as_str().ok_or("body must be base64 text")?;
        m.body = super::messages::base64_decode(body)
            .ok_or("body is not base64")?
            .into();
    } else if message_type == message_type::TEXT {
        let mut w = Writer::new();
        w.nullable_simple_string(None);
        m.body = w.into_vec().into();
    }
    Ok(m)
}

fn property_from(kind: &str, v: &Json) -> Result<PropertyValue, String> {
    let int = |lo: i64, hi: i64| -> Result<i64, String> {
        let n = match v {
            Json::String(s) => s
                .parse::<i64>()
                .map_err(|_| format!("{s} is not a number"))?,
            other => other.as_i64().ok_or("not a whole number")?,
        };
        if (lo..=hi).contains(&n) {
            Ok(n)
        } else {
            Err(format!("{n} is out of range for a {kind}"))
        }
    };
    let float = || -> Result<f64, String> {
        match v {
            Json::String(s) => s.parse::<f64>().map_err(|_| format!("{s} is not a number")),
            Json::Double(d) => Ok(*d),
            Json::Long(l) => Ok(*l as f64),
            _ => Err("not a number".to_owned()),
        }
    };
    let text = || v.as_str().ok_or_else(|| "not a string".to_owned());
    Ok(match kind {
        "null" => PropertyValue::Null,
        "boolean" => PropertyValue::Bool(v.as_bool().ok_or("not a boolean")?),
        "byte" => PropertyValue::Byte(int(i8::MIN.into(), i8::MAX.into())? as i8),
        "short" => PropertyValue::Short(int(i16::MIN.into(), i16::MAX.into())? as i16),
        "int" => PropertyValue::Int(int(i32::MIN.into(), i32::MAX.into())? as i32),
        "long" => PropertyValue::Long(int(i64::MIN, i64::MAX)?),
        "float" => PropertyValue::Float(float()? as f32),
        "double" => PropertyValue::Double(float()?),
        "char" => {
            let units: Vec<u16> = text()?.encode_utf16().collect();
            match units.as_slice() {
                [c] => PropertyValue::Char(*c),
                _ => return Err("a char is one character".to_owned()),
            }
        }
        "bytes" => {
            PropertyValue::Bytes(super::messages::base64_decode(text()?).ok_or("not base64")?)
        }
        "string" => PropertyValue::String(SimpleString::from(text()?)),
        other => return Err(format!("no property type {other}")),
    })
}

/// `Base64.getEncoder()`: the standard alphabet, padded.
fn base64(data: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let n = chunk.len();
        let v = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for i in 0..4 {
            out.push(if i <= n {
                A[((v >> (18 - 6 * i)) & 63) as usize] as char
            } else {
                '='
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_message_comes_back_as_it_was_written() {
        let mut m = CoreMessage {
            message_id: 12,
            address: Some(SimpleString::from("orders")),
            message_type: message_type::BYTES,
            durable: false,
            priority: 7,
            timestamp: 1_792_717_353_807,
            expiration: 1_792_717_999_000,
            user_id: Some([7; 16]),
            body: vec![0, 1, 2, 254, 255].into(),
            ..CoreMessage::default()
        };
        for (k, v) in [
            ("n", PropertyValue::Null),
            ("b", PropertyValue::Bool(true)),
            ("y", PropertyValue::Byte(-3)),
            ("s", PropertyValue::Short(-300)),
            ("i", PropertyValue::Int(70_000)),
            ("l", PropertyValue::Long(i64::MAX)),
            ("f", PropertyValue::Float(1.5)),
            ("d", PropertyValue::Double(-2.25)),
            ("c", PropertyValue::Char(u16::from(b'x'))),
            ("bytes", PropertyValue::Bytes(vec![9, 8, 7])),
            ("str", PropertyValue::String(SimpleString::from("red"))),
        ] {
            m.properties.insert(SimpleString::from(k), v);
        }
        let text = message_json(&m).to_string();
        assert!(text.contains("\"9223372036854775807\""), "{text}");
        let back = message_from_json(&Json::parse(&text).unwrap()).unwrap();
        assert_eq!(back.message_type, m.message_type);
        assert_eq!(back.durable, m.durable);
        assert_eq!(back.priority, m.priority);
        assert_eq!(back.timestamp, m.timestamp);
        assert_eq!(back.expiration, m.expiration);
        assert_eq!(back.user_id, m.user_id);
        assert_eq!(back.body, m.body);
        assert_eq!(back.properties.entries(), m.properties.entries());
    }

    #[test]
    fn a_text_message_keeps_its_text_and_a_null_text() {
        for text in [Some("héllo"), None] {
            let mut w = Writer::new();
            w.nullable_simple_string(text.map(SimpleString::from).as_ref());
            let m = CoreMessage {
                message_type: message_type::TEXT,
                body: w.into_vec().into(),
                ..CoreMessage::default()
            };
            let j = message_json(&m);
            assert_eq!(j.get("text").and_then(Json::as_str), text);
            assert_eq!(message_from_json(&j).unwrap().body, m.body);
        }
    }

    #[test]
    fn what_cannot_be_read_back_says_why() {
        let bad = |t: &str| message_from_json(&Json::parse(t).unwrap()).unwrap_err();
        assert!(bad("[]").contains("object"));
        assert!(bad(r#"{"properties":[{"name":"x","type":"byte","value":300}]}"#).contains("x"));
        assert!(bad(r#"{"body":"%%"}"#).contains("base64"));
        assert!(bad(r#"{"properties":[{"name":"x","type":"char","value":"ab"}]}"#).contains("one"));
    }

    #[test]
    fn base64_is_the_standard_alphabet_padded() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(&[0xfb, 0xff]), "+/8=");
    }
}
