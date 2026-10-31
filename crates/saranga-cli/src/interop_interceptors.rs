//! The interceptors of the interceptors suite (tests/interop/interceptors),
//! under the class names of its Java ones, which Artemis loads from the
//! instance's lib/. Built only with the `interop-interceptors` feature.
//!
//! For each protocol an incoming interceptor refuses what carries `refuse`
//! and marks the rest with `<protocol>_in: seen`; an outgoing one refuses
//! a delivery that carries `hide` and marks the rest with
//! `<protocol>_out: seen`. MQTT's go by topic: `refuse` and `hide`.

use std::sync::Arc;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_core_protocol::Packet;
use saranga_protocol_amqp::message::AmqpMessage;
use saranga_protocol_amqp::types::Value;
use saranga_protocol_mqtt::codec::Packet as MqttPacket;
use saranga_protocol_openwire::commands::{DataStructure, Message};
use saranga_protocol_openwire::convert::{message_properties, set_message_properties};
use saranga_protocol_openwire::primitive::Primitive;
use saranga_protocol_stomp::frame::Frame;
use saranga_server::BrokerConfig;
use saranga_server::interceptor::{Interceptor, InterceptorError, InterceptorFactory};
use saranga_server::session::ClientConnection;

const PREFIX: &str = "org.saranga.interop.Interceptors$";

type Result = std::result::Result<bool, InterceptorError>;

pub fn register(config: &mut BrokerConfig) {
    let mut add = |name: &str, make: fn(String) -> Interceptor| {
        let name = format!("{PREFIX}{name}");
        let factory: InterceptorFactory = Arc::new({
            let name = name.clone();
            move || make(name.clone())
        });
        config.interceptors.insert(name, factory);
    };
    add("CoreIn", |n| Interceptor::new(n).on(core_in));
    add("CoreOut", |n| Interceptor::new(n).on(core_out));
    add("StompIn", |n| Interceptor::new(n).on(stomp_in));
    add("StompOut", |n| Interceptor::new(n).on(stomp_out));
    add("MqttIn", |n| {
        Interceptor::new(n).on(|p: &mut MqttPacket, _: &ClientConnection| mqtt(p, "refuse"))
    });
    add("MqttOut", |n| {
        Interceptor::new(n).on(|p: &mut MqttPacket, _: &ClientConnection| mqtt(p, "hide"))
    });
    add("OpenWireIn", |n| Interceptor::new(n).on(openwire_in));
    add("OpenWireOut", |n| Interceptor::new(n).on(openwire_out));
    add("AmqpIn", |n| {
        Interceptor::new(n)
            .on(|m: &mut AmqpMessage, _: &ClientConnection| amqp(m, "refuse", "amqp_in"))
    });
    add("AmqpOut", |n| {
        Interceptor::new(n)
            .on(|m: &mut AmqpMessage, _: &ClientConnection| amqp(m, "hide", "amqp_out"))
    });
}

fn mark_core(m: &mut saranga_message::CoreMessage, stop: &str, mark: &str) -> bool {
    if m.properties.get_str(stop).is_some() {
        return false;
    }
    m.properties.insert(
        SimpleString::from(mark),
        PropertyValue::String(SimpleString::from("seen")),
    );
    true
}

fn core_in(p: &mut Packet, _: &ClientConnection) -> Result {
    Ok(match p {
        Packet::SessionSendMessage(s) => mark_core(&mut s.message, "refuse", "core_in"),
        Packet::SessionSendMessageV2(s) => mark_core(&mut s.message, "refuse", "core_in"),
        Packet::SessionSendMessageV3(s) => mark_core(&mut s.message, "refuse", "core_in"),
        _ => true,
    })
}

fn core_out(p: &mut Packet, _: &ClientConnection) -> Result {
    Ok(match p {
        Packet::SessionReceiveMessage(r) => mark_core(&mut r.message, "hide", "core_out"),
        _ => true,
    })
}

fn stomp_in(f: &mut Frame, _: &ClientConnection) -> Result {
    if f.command != "SEND" {
        return Ok(true);
    }
    if f.has_header("refuse") {
        return Ok(false);
    }
    f.add_header("stomp_in", "seen");
    Ok(true)
}

fn stomp_out(f: &mut Frame, _: &ClientConnection) -> Result {
    if f.command == "MESSAGE" && f.has_header("hide") {
        return Ok(false);
    }
    // Every frame; a MESSAGE goes out as it was encoded before.
    f.add_header("stomp_out", "seen");
    Ok(true)
}

fn mqtt(p: &mut MqttPacket, topic: &str) -> Result {
    Ok(!matches!(p, MqttPacket::Publish(p) if p.topic == topic))
}

fn mark_openwire(m: &mut Message, stop: &str, mark: &str) -> Result {
    let mut properties = message_properties(m).map_err(|e| e.to_string())?;
    if properties.get(stop).is_some() {
        return Ok(false);
    }
    properties.insert(mark.to_owned(), Primitive::String("seen".to_owned()));
    set_message_properties(m, &properties).map_err(|e| e.to_string())?;
    Ok(true)
}

fn openwire_in(c: &mut DataStructure, _: &ClientConnection) -> Result {
    match c {
        DataStructure::Message(m) => mark_openwire(m, "refuse", "ow_in"),
        _ => Ok(true),
    }
}

fn openwire_out(c: &mut DataStructure, _: &ClientConnection) -> Result {
    match c {
        DataStructure::MessageDispatch(d) => match d.message.as_deref_mut() {
            Some(m) => mark_openwire(m, "hide", "ow_out"),
            None => Ok(true),
        },
        _ => Ok(true),
    }
}

fn amqp(m: &mut AmqpMessage, stop: &str, mark: &str) -> Result {
    if m.application_property(stop).is_some() {
        return Ok(false);
    }
    m.put_property(mark, Value::String("seen".to_owned()));
    Ok(true)
}
