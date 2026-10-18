//! The HORNETQ protocol (`HornetQProtocolManager`): Core for HornetQ 2.x
//! clients, whose property names and filter identifiers are converted as
//! the packets pass (`HQPropertiesConversionInterceptor` and
//! `HQFilterConversionInterceptor`).

use saranga_buffer::{SimpleString, TypedProperties};
use saranga_core_protocol::Packet;

/// The handshake a HornetQ client sends first, when it sends one.
pub(crate) const HANDSHAKE: &[u8] = b"HORNETQ";

/// `HQPropertiesConverter`: HornetQ's names for Artemis's properties.
const PROPERTIES: [(&str, &str); 10] = [
    ("_HQ_ACTUAL_EXPIRY", "_AMQ_ACTUAL_EXPIRY"),
    ("_HQ_ORIG_ADDRESS", "_AMQ_ORIG_ADDRESS"),
    ("_HQ_ORIG_QUEUE", "_AMQ_ORIG_QUEUE"),
    ("_HQ_ORIG_MESSAGE_ID", "_AMQ_ORIG_MESSAGE_ID"),
    ("_HQ_GROUP_ID", "_AMQ_GROUP_ID"),
    ("_HQ_LARGE_COMPRESSED", "_AMQ_LARGE_COMPRESSED"),
    ("_HQ_LARGE_SIZE", "_AMQ_LARGE_SIZE"),
    ("_HQ_SCHED_DELIVERY", "_AMQ_SCHED_DELIVERY"),
    ("_HQ_DUPL_ID", "_AMQ_DUPL_ID"),
    ("_HQ_LVQ_NAME", "_AMQ_LVQ_NAME"),
];

/// `HQPropertiesConverter.replaceHQProperties` (`to_artemis`) and
/// `replaceAMQProperties`: renames the properties, a renamed one replacing
/// any already under the new name.
pub(crate) fn rename(properties: &mut TypedProperties, to_artemis: bool) {
    let found: Vec<(SimpleString, &str)> = properties
        .entries()
        .iter()
        .filter_map(|(k, _)| {
            PROPERTIES.iter().find_map(|&(hq, amq)| {
                let (from, to) = if to_artemis { (hq, amq) } else { (amq, hq) };
                k.eq_str(from).then(|| (k.clone(), to))
            })
        })
        .collect();
    for (from, to) in found {
        if let Some(v) = properties.remove(&from) {
            properties.insert(SimpleString::from(to), v);
        }
    }
}

/// `HQFilterConversionInterceptor.convertHQToActiveMQFilterString`.
pub(crate) fn convert_filter(filter: &str) -> String {
    use saranga_filter::translate::parse;
    let f = parse(filter, "HQDurable", "AMQDurable");
    let f = parse(&f, "HQPriority", "AMQPriority");
    let f = parse(&f, "HQTimestamp", "AMQTimestamp");
    let f = parse(&f, "HQUserID", "AMQUserID");
    parse(&f, "HQExpiration", "AMQExpiration")
}

fn filter(filter: &mut Option<SimpleString>) {
    if let Some(f) = filter {
        *f = SimpleString::from(convert_filter(&f.to_string_lossy()).as_str());
    }
}

/// The incoming interceptors: sends get Artemis's property names, and
/// consumers and queues Artemis's filter identifiers.
pub(crate) fn incoming(packet: &mut Packet) {
    match packet {
        Packet::SessionSendMessage(p) => rename(&mut p.message.properties, true),
        Packet::SessionSendMessageV2(p) => rename(&mut p.message.properties, true),
        Packet::SessionSendMessageV3(p) => rename(&mut p.message.properties, true),
        Packet::SessionSendMessage1X(p) => rename(&mut p.message.properties, true),
        Packet::SessionSendLargeMessage(p) => rename(&mut p.message.properties, true),
        Packet::SessionCreateConsumerMessage(p) => filter(&mut p.filter_string),
        Packet::CreateQueueMessage(p) => filter(&mut p.filter_string),
        Packet::CreateQueueMessageV2(p) => filter(&mut p.filter_string),
        Packet::CreateSharedQueueMessage(p) => filter(&mut p.filter_string),
        Packet::CreateSharedQueueMessageV2(p) => filter(&mut p.filter_string),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use saranga_buffer::PropertyValue;

    #[test]
    fn renames_both_ways() {
        let mut p = TypedProperties::new();
        p.insert("_HQ_DUPL_ID".into(), PropertyValue::Int(1));
        p.insert("color".into(), PropertyValue::Int(2));
        p.insert("_AMQ_DUPL_ID".into(), PropertyValue::Int(3));
        rename(&mut p, true);
        assert_eq!(p.get_str("_AMQ_DUPL_ID"), Some(&PropertyValue::Int(1)));
        assert_eq!(p.get_str("_HQ_DUPL_ID"), None);
        assert_eq!(p.get_str("color"), Some(&PropertyValue::Int(2)));
        rename(&mut p, false);
        assert_eq!(p.get_str("_HQ_DUPL_ID"), Some(&PropertyValue::Int(1)));
        assert_eq!(p.get_str("_AMQ_DUPL_ID"), None);
    }

    #[test]
    fn converts_filters() {
        assert_eq!(
            convert_filter("HQPriority > 4 AND HQDurable = 'DURABLE' AND x = 'HQUserID'"),
            "AMQPriority > 4 AND AMQDurable = 'DURABLE' AND x = 'HQUserID'"
        );
    }
}
