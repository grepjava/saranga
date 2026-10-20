//! How Qpid JMS sets a message property: `JmsMessagePropertyIntercepter`
//! (the JMS headers and JMSX names a property can set),
//! `AmqpJmsMessagePropertyIntercepter` (the `JMS_AMQP_*` names),
//! `JmsMessagePropertySupport`'s name and value checks, and the
//! conversions of `TypeConversionSupport`.

use saranga_client::PropertyValue;
use saranga_protocol_amqp::types::Value;

use super::jms::JmsError;
use super::message::AmqpJmsMessage;
use crate::client::java_chars::{is_java_identifier_part, is_java_identifier_start};

/// The Java class of a property value.
pub fn class_name(v: &PropertyValue) -> &'static str {
    match v {
        PropertyValue::Null => "null",
        PropertyValue::Bool(_) => "java.lang.Boolean",
        PropertyValue::Byte(_) => "java.lang.Byte",
        PropertyValue::Short(_) => "java.lang.Short",
        PropertyValue::Int(_) => "java.lang.Integer",
        PropertyValue::Long(_) => "java.lang.Long",
        PropertyValue::Float(_) => "java.lang.Float",
        PropertyValue::Double(_) => "java.lang.Double",
        PropertyValue::Char(_) => "java.lang.Character",
        PropertyValue::String(_) => "java.lang.String",
        PropertyValue::Bytes(_) => "[B",
    }
}

/// `String.valueOf` of a primitive wrapper.
pub fn java_to_string(v: &PropertyValue) -> Option<String> {
    Some(match v {
        PropertyValue::Bool(b) => b.to_string(),
        PropertyValue::Byte(b) => b.to_string(),
        PropertyValue::Short(s) => s.to_string(),
        PropertyValue::Int(i) => i.to_string(),
        PropertyValue::Long(l) => l.to_string(),
        PropertyValue::Float(f) => saranga_filter::java_float_to_string(*f),
        PropertyValue::Double(d) => saranga_filter::java_double_to_string(*d),
        PropertyValue::Char(c) => char::from_u32(u32::from(*c))
            .map_or_else(|| String::from_utf16_lossy(&[*c]), String::from),
        PropertyValue::String(s) => s.to_string_lossy(),
        PropertyValue::Null | PropertyValue::Bytes(_) => return None,
    })
}

/// `NumberFormatException.forInputString`.
fn number_format(s: &str) -> JmsError {
    JmsError {
        class: "java.lang.NumberFormatException",
        message: format!("For input string: \"{s}\""),
    }
}

/// `Byte.valueOf` / `Short.valueOf`: `Integer.parseInt` within range.
fn narrow(s: &str, min: i64, max: i64) -> Result<i64, JmsError> {
    let v = saranga_filter::java_parse_int::<i32>(s).ok_or_else(|| number_format(s))?;
    let v = i64::from(v);
    if v < min || v > max {
        return Err(JmsError {
            class: "java.lang.NumberFormatException",
            message: format!("Value out of range. Value:\"{s}\" Radix:10"),
        });
    }
    Ok(v)
}

/// `Float.valueOf` / `Double.valueOf`'s failure.
fn float_format(s: &str) -> JmsError {
    let trimmed = s.trim_matches(|c: char| c <= ' ');
    JmsError {
        class: "java.lang.NumberFormatException",
        message: if trimmed.is_empty() {
            "empty String".to_owned()
        } else {
            format!("For input string: \"{s}\"")
        },
    }
}

/// The classes `TypeConversionSupport` converts to.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum To {
    Boolean,
    Integer,
    Long,
    String,
}

/// `TypeConversionSupport.convert`: the value as `to`, `None` where no
/// converter is registered, or the parse's failure.
pub fn convert(v: &PropertyValue, to: To) -> Result<Option<PropertyValue>, JmsError> {
    use PropertyValue as P;
    Ok(match (v, to) {
        (P::Bool(_), To::Boolean)
        | (P::Int(_), To::Integer)
        | (P::Long(_), To::Long)
        | (P::String(_), To::String) => Some(v.clone()),
        (
            P::Bool(_)
            | P::Byte(_)
            | P::Short(_)
            | P::Int(_)
            | P::Long(_)
            | P::Float(_)
            | P::Double(_),
            To::String,
        ) => java_to_string(v).map(|s| P::String(s.as_str().into())),
        (P::String(s), _) => {
            let s = s.to_string_lossy();
            Some(match to {
                To::Boolean => P::Bool(s.eq_ignore_ascii_case("true")),
                To::Integer => {
                    P::Int(saranga_filter::java_parse_int(&s).ok_or_else(|| number_format(&s))?)
                }
                To::Long => {
                    P::Long(saranga_filter::java_parse_int(&s).ok_or_else(|| number_format(&s))?)
                }
                To::String => unreachable!(),
            })
        }
        (P::Byte(b), To::Long) => Some(P::Long(i64::from(*b))),
        (P::Short(x), To::Long) => Some(P::Long(i64::from(*x))),
        (P::Int(x), To::Long) => Some(P::Long(i64::from(*x))),
        (P::Byte(b), To::Integer) => Some(P::Int(i32::from(*b))),
        (P::Short(x), To::Integer) => Some(P::Int(i32::from(*x))),
        _ => None,
    })
}

/// `Float.valueOf` and `Double.valueOf` of a string, as `TypeConversionSupport`
/// reads them for a float or double target.
pub fn parse_float(s: &str) -> Result<f32, JmsError> {
    saranga_filter::java_parse_float(s).ok_or_else(|| float_format(s))
}

pub fn parse_double(s: &str) -> Result<f64, JmsError> {
    saranga_filter::java_parse_double(s).ok_or_else(|| float_format(s))
}

/// `Byte.valueOf` of a string.
pub fn parse_byte(s: &str) -> Result<i8, JmsError> {
    Ok(narrow(s, i64::from(i8::MIN), i64::from(i8::MAX))? as i8)
}

/// `Short.valueOf` of a string.
pub fn parse_short(s: &str) -> Result<i16, JmsError> {
    Ok(narrow(s, i64::from(i16::MIN), i64::from(i16::MAX))? as i16)
}

fn illegal_argument(message: String) -> JmsError {
    JmsError {
        class: "java.lang.IllegalArgumentException",
        message,
    }
}

/// `checkPropertyNameIsValid(name, true)`.
pub fn check_name(name: &str) -> Result<(), JmsError> {
    let units: Vec<u16> = name.encode_utf16().collect();
    let Some(first) = units.first() else {
        return Err(illegal_argument(
            "Property name must not be the empty string".to_owned(),
        ));
    };
    if !is_java_identifier_start(*first) {
        return Err(illegal_argument(format!(
            "Identifier does not begin with a valid JMS identifier start character: '{name}' "
        )));
    }
    if let Some(bad) = units[1..].iter().find(|u| !is_java_identifier_part(**u)) {
        return Err(illegal_argument(format!(
            "Identifier contains invalid JMS identifier character '{}': '{name}' ",
            String::from_utf16_lossy(&[*bad])
        )));
    }
    if matches!(
        name,
        "NULL"
            | "TRUE"
            | "FALSE"
            | "NOT"
            | "AND"
            | "OR"
            | "BETWEEN"
            | "LIKE"
            | "IN"
            | "IS"
            | "ESCAPE"
    ) {
        return Err(illegal_argument(format!(
            "Identifier not allowed in JMS: '{name}'"
        )));
    }
    Ok(())
}

/// `checkValidObject`: primitive wrappers, strings and null.
fn check_value(v: &PropertyValue) -> Result<(), JmsError> {
    if let PropertyValue::Bytes(_) = v {
        // The array prints with its identity hash.
        return Err(JmsError {
            class: "javax.jms.MessageFormatException",
            message: format!(
                "Only objectified primitive objects and String types are allowed but was: [B@{:x} type: class [B",
                super::message::identity()
            ),
        });
    }
    Ok(())
}

fn cannot_set(name: &str, v: &PropertyValue) -> JmsError {
    JmsError::jms(format!(
        "Property {name} cannot be set from a {}.",
        class_name(v)
    ))
}

/// The conversion, or the interceptor's failure when there is none.
fn converted(name: &str, v: &PropertyValue, to: To) -> Result<PropertyValue, JmsError> {
    convert(v, to)?.ok_or_else(|| cannot_set(name, v))
}

fn as_string(v: PropertyValue) -> String {
    match v {
        PropertyValue::String(s) => s.to_string_lossy(),
        _ => unreachable!(),
    }
}

fn as_i64(v: &PropertyValue) -> i64 {
    match v {
        PropertyValue::Short(x) => i64::from(*x),
        PropertyValue::Int(x) => i64::from(*x),
        PropertyValue::Long(x) => *x,
        _ => unreachable!(),
    }
}

/// An application property's AMQP value.
pub fn amqp_value(v: &PropertyValue) -> Value {
    match v {
        PropertyValue::Null => Value::Null,
        PropertyValue::Bool(b) => Value::Boolean(*b),
        PropertyValue::Byte(b) => Value::Byte(*b),
        PropertyValue::Bytes(b) => Value::Binary(b.clone()),
        PropertyValue::Short(s) => Value::Short(*s),
        PropertyValue::Int(i) => Value::Int(*i),
        PropertyValue::Long(l) => Value::Long(*l),
        PropertyValue::Float(f) => Value::Float(*f),
        PropertyValue::Double(d) => Value::Double(*d),
        PropertyValue::Char(c) => Value::Char(u32::from(*c)),
        PropertyValue::String(s) => Value::String(s.to_string_lossy()),
    }
}

/// `JmsMessage.setObjectProperty` (and the typed setters, which box their
/// value): the name and value checked, then the interceptor for the name
/// or an application property.
pub fn set_property(m: &mut AmqpJmsMessage, name: &str, v: &PropertyValue) -> Result<(), JmsError> {
    check_name(name)?;
    check_value(v)?;
    let id_error = |e: String| {
        let (class, message) = e.split_once(": ").unwrap_or(("javax.jms.JMSException", &e));
        JmsError {
            class: if class == "org.apache.qpid.jms.exceptions.IdConversionException" {
                "org.apache.qpid.jms.exceptions.IdConversionException"
            } else {
                "javax.jms.JMSException"
            },
            message: message.to_owned(),
        }
    };
    match name {
        "JMSDestination" => Err(JmsError::jms(
            "Cannot set JMS Destination as a property, use setJMSDestination() instead",
        )),
        "JMSReplyTo" => Err(JmsError::jms(
            "Cannot set JMS ReplyTo as a property, use setJMSReplTo() instead",
        )),
        "JMSType" => {
            m.set_type(Some(as_string(converted(name, v, To::String)?)));
            Ok(())
        }
        "JMSDeliveryMode" => {
            let rc = match convert(v, To::Integer) {
                Ok(rc) => rc.map(|i| as_i64(&i) as i32),
                Err(nfe) => match v {
                    PropertyValue::String(s)
                        if s.to_string_lossy().eq_ignore_ascii_case("PERSISTENT") =>
                    {
                        Some(2)
                    }
                    PropertyValue::String(s)
                        if s.to_string_lossy().eq_ignore_ascii_case("NON_PERSISTENT") =>
                    {
                        Some(1)
                    }
                    _ => return Err(nfe),
                },
            };
            match rc {
                Some(rc) => m.set_persistent(rc == 2),
                None => match convert(v, To::Boolean)? {
                    Some(PropertyValue::Bool(b)) => m.set_persistent(b),
                    _ => return Err(cannot_set(name, v)),
                },
            }
            Ok(())
        }
        "JMSPriority" => {
            let p = as_i64(&converted(name, v, To::Integer)?) as i32;
            // `rc.byteValue()`, then the header's clamp.
            m.set_priority(i32::from(p as i8));
            Ok(())
        }
        "JMSMessageID" => m
            .set_message_id(Some(&as_string(converted(name, v, To::String)?)))
            .map_err(id_error),
        "JMSTimestamp" => {
            m.set_timestamp(as_i64(&converted(name, v, To::Long)?));
            Ok(())
        }
        "JMSCorrelationID" => m
            .set_correlation_id(Some(&as_string(converted(name, v, To::String)?)))
            .map_err(id_error),
        "JMSExpiration" => {
            m.set_expiration(as_i64(&converted(name, v, To::Long)?));
            Ok(())
        }
        "JMSRedelivered" => match converted(name, v, To::Boolean)? {
            PropertyValue::Bool(b) => {
                m.set_redelivered(b);
                Ok(())
            }
            _ => unreachable!(),
        },
        "JMSXDeliveryCount" => {
            m.set_delivery_count(as_i64(&converted(name, v, To::Integer)?) as i32);
            Ok(())
        }
        "JMSXGroupID" => {
            m.set_group_id(Some(as_string(converted(name, v, To::String)?)));
            Ok(())
        }
        "JMSXGroupSeq" => {
            m.set_group_sequence(as_i64(&converted(name, v, To::Integer)?) as i32);
            Ok(())
        }
        "JMSXUserID" => match v {
            PropertyValue::String(s) => {
                m.set_user_id(Some(&s.to_string_lossy()));
                Ok(())
            }
            PropertyValue::Null => {
                m.set_user_id(None);
                Ok(())
            }
            _ => Err(cannot_set(name, v)),
        },
        // A message this client sends has no acknowledgement callback.
        "JMS_AMQP_ACK_TYPE" => Err(JmsError::jms(
            "Session Acknowledgement Mode does not allow setting: JMS_AMQP_ACK_TYPE",
        )),
        "JMSDeliveryTime" => {
            m.set_delivery_time(as_i64(&converted(name, v, To::Long)?));
            Ok(())
        }
        "JMS_AMQP_TTL" => {
            let ttl = as_i64(&converted(name, v, To::Long)?);
            if !(0..=0xffff_ffff).contains(&ttl) {
                return Err(JmsError {
                    class: "javax.jms.MessageFormatException",
                    message: "JMS_AMQP_TTL must be a long with value in range 0 to 2^32 - 1"
                        .to_owned(),
                });
            }
            m.set_ttl_override(Some(ttl as u32));
            Ok(())
        }
        "JMS_AMQP_REPLY_TO_GROUP_ID" => {
            m.set_reply_to_group_id(Some(as_string(converted(name, v, To::String)?)));
            Ok(())
        }
        "JMS_AMQP_TYPED_ENCODING" => {
            let PropertyValue::Bool(typed) = converted(name, v, To::Boolean)? else {
                unreachable!()
            };
            if m.kind != super::message::kind::OBJECT {
                return Err(JmsError {
                    class: "javax.jms.MessageFormatException",
                    message: "JMS_AMQP_TYPED_ENCODING is only applicable to ObjectMessage"
                        .to_owned(),
                });
            }
            m.set_typed_object(typed).map_err(|e| {
                let (_, message) = e.split_once(": ").unwrap_or(("", &e));
                JmsError::jms(message)
            })
        }
        _ => {
            m.set_application_property(name, amqp_value(v));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_checked_as_qpid_checks_them() {
        assert!(check_name("count").is_ok());
        assert_eq!(
            check_name("").unwrap_err().to_java_string(),
            "java.lang.IllegalArgumentException: Property name must not be the empty string"
        );
        assert_eq!(
            check_name("a b").unwrap_err().to_java_string(),
            "java.lang.IllegalArgumentException: Identifier contains invalid JMS identifier character ' ': 'a b' "
        );
        assert_eq!(
            check_name("1a").unwrap_err().message,
            "Identifier does not begin with a valid JMS identifier start character: '1a' "
        );
        assert_eq!(
            check_name("LIKE").unwrap_err().message,
            "Identifier not allowed in JMS: 'LIKE'"
        );
    }

    #[test]
    fn interceptors_set_the_headers() {
        let mut m = AmqpJmsMessage::new_text(Some("x"));
        set_property(&mut m, "JMSXGroupSeq", &PropertyValue::String("3".into())).unwrap();
        assert_eq!(m.group_sequence(), 3);
        let e =
            set_property(&mut m, "JMSXGroupSeq", &PropertyValue::String("v".into())).unwrap_err();
        assert_eq!(
            e.to_java_string(),
            "java.lang.NumberFormatException: For input string: \"v\""
        );
        set_property(
            &mut m,
            "JMSCorrelationID",
            &PropertyValue::String("ID:AMQP_ULONG:7".into()),
        )
        .unwrap();
        assert_eq!(m.correlation_id().as_deref(), Some("ID:AMQP_ULONG:7"));
        let e = set_property(&mut m, "JMSXUserID", &PropertyValue::Int(1)).unwrap_err();
        assert_eq!(
            e.to_java_string(),
            "javax.jms.JMSException: Property JMSXUserID cannot be set from a java.lang.Integer."
        );
        set_property(&mut m, "k", &PropertyValue::Long(5)).unwrap();
        assert_eq!(m.property("k"), Some(&Value::Long(5)));
    }
}
