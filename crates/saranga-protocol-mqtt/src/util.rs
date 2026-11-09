//! `MQTTUtil`, `MQTTReasonCodes` and `WildcardConfiguration.convert`:
//! topic names to core addresses and back, and the names of the broker's
//! MQTT resources and message properties.

use saranga_server::wildcard::{ESCAPE, WildcardConfig};

pub const DOLLAR: char = '$';
pub const SLASH: char = '/';

/// The internal last-value queue holding each client's session state.
pub const MQTT_SESSION_STORE: &str = "$sys.mqtt.sessions";
/// Each topic's retained message is in a queue of this prefix.
pub const MQTT_RETAIN_ADDRESS_PREFIX: &str = "$sys.mqtt.retain.";
pub const MQTT_QOS_LEVEL_KEY: &str = "mqtt.qos.level";
pub const MQTT_MESSAGE_RETAIN_KEY: &str = "mqtt.message.retain";
pub const MQTT_MESSAGE_RETAIN_INITIAL_DISTRIBUTION_KEY: &str =
    "mqtt.message.retain.initial.distribution";
pub const MQTT_PAYLOAD_FORMAT_INDICATOR_KEY: &str = "mqtt.payload.format.indicator";
pub const MQTT_RESPONSE_TOPIC_KEY: &str = "mqtt.response.topic";
pub const MQTT_CORRELATION_DATA_KEY: &str = "mqtt.correlation.data";
pub const MQTT_USER_PROPERTY_EXISTS_KEY: &str = "mqtt.user.property.exists";
pub const MQTT_USER_PROPERTY_KEY_PREFIX: &str = "mqtt.ordered.user.property.";
pub const MQTT_CONTENT_TYPE_KEY: &str = "mqtt.content.type";
pub const SHARED_SUBSCRIPTION_PREFIX: &str = "$share/";
/// `MessageUtil.CONNECTION_ID_PROPERTY_NAME`: MQTT puts the client ID there.
pub const CONNECTION_ID_PROPERTY: &str = "__AMQ_CID";
/// `Message.HDR_LAST_VALUE_NAME`.
pub const HDR_LAST_VALUE_NAME: &str = "_AMQ_LVQ_NAME";

pub const TWO_BYTE_INT_MAX: i32 = 0xFFFF;
pub const VARIABLE_BYTE_INT_MAX: i32 = 268_435_455;
pub const MAX_PACKET_SIZE: i32 = VARIABLE_BYTE_INT_MAX;
/// MQTT keep-alives are in seconds and last one and a half times as long
/// ([MQTT-3.1.2-22]).
pub const KEEP_ALIVE_ADJUSTMENT: i64 = 1500;
pub const DEFAULT_SERVER_KEEP_ALIVE: i32 = 60;
pub const DEFAULT_TOPIC_ALIAS_MAX: i32 = TWO_BYTE_INT_MAX;
pub const DEFAULT_RECEIVE_MAXIMUM: i32 = TWO_BYTE_INT_MAX;
pub const DEFAULT_MAXIMUM_PACKET_SIZE: i32 = MAX_PACKET_SIZE;
pub const DEFAULT_MAXIMUM_IN_FLIGHT_PUBLISH_MESSAGES: i32 = TWO_BYTE_INT_MAX;

/// `MQTT_WILDCARD`: `/` between words, `+` for one and `#` for any.
pub const MQTT_WILDCARD: WildcardConfig = WildcardConfig {
    routing_enabled: true,
    delimiter: SLASH,
    any_words: '#',
    single_word: '+',
    // An MQTT topic has no escape character: `\` is a literal.
    uses_escape: false,
};

/// `MQTTReasonCodes`.
pub mod reason {
    pub const UNACCEPTABLE_PROTOCOL_VERSION_3: u8 = 0x01;
    pub const IDENTIFIER_REJECTED_3: u8 = 0x02;
    pub const SERVER_UNAVAILABLE_3: u8 = 0x03;
    pub const BAD_USER_NAME_OR_PASSWORD_3: u8 = 0x04;
    pub const NOT_AUTHORIZED_3: u8 = 0x05;
    pub const SUCCESS: u8 = 0x00;
    /// A DISCONNECT asking the server to publish the will (5.0 3.14.2.1).
    pub const DISCONNECT_WITH_WILL: u8 = 0x04;
    pub const NO_SUBSCRIPTION_EXISTED: u8 = 0x11;
    pub const UNSPECIFIED_ERROR: u8 = 0x80;
    pub const MALFORMED_PACKET: u8 = 0x81;
    pub const IMPLEMENTATION_SPECIFIC_ERROR: u8 = 0x83;
    pub const UNSUPPORTED_PROTOCOL_VERSION: u8 = 0x84;
    pub const CLIENT_IDENTIFIER_NOT_VALID: u8 = 0x85;
    pub const BAD_USER_NAME_OR_PASSWORD: u8 = 0x86;
    pub const NOT_AUTHORIZED: u8 = 0x87;
    pub const SERVER_UNAVAILABLE: u8 = 0x88;
    pub const USE_ANOTHER_SERVER: u8 = 0x9C;
    pub const BAD_AUTHENTICATION_METHOD: u8 = 0x8C;
    pub const SESSION_TAKEN_OVER: u8 = 0x8E;
    pub const PACKET_IDENTIFIER_NOT_FOUND: u8 = 0x92;
    pub const TOPIC_ALIAS_INVALID: u8 = 0x94;
    pub const PACKET_TOO_LARGE: u8 = 0x95;
}

/// `WildcardConfiguration.convert`: `input` in `from`'s syntax, in `to`'s.
/// A delimiter or wildcard of `from` becomes the matching one of `to`; any
/// other character is a literal, escaped with `\` if `to` escapes and it would
/// otherwise be special there. In `from`'s syntax a `\` is an escape only if
/// `from` escapes (an MQTT topic has none, so its `\` is a literal); otherwise
/// a `\` before a delimiter or wildcard introduces that character as a literal.
/// This is injective and round-trips, where `AddressImpl`'s heuristic reads a
/// literal `\` before a delimiter as an escape and collapses distinct topics
/// (`a\/b` and `a\\/b` both to `a/b`).
pub fn convert(input: &str, from: &WildcardConfig, to: &WildcardConfig) -> String {
    if from == to {
        return input.to_owned();
    }
    let from_special = |c: char| {
        c == from.delimiter || c == from.single_word || c == from.any_words || c == ESCAPE
    };
    let emit_literal = |out: &mut String, c: char| {
        if to.uses_escape
            && (c == ESCAPE || c == to.delimiter || c == to.single_word || c == to.any_words)
        {
            out.push(ESCAPE);
        }
        out.push(c);
    };
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        if from.uses_escape && c == ESCAPE && chars.peek().is_some_and(|&n| from_special(n)) {
            // A `\` before a special character introduces it as a literal.
            emit_literal(&mut out, chars.next().unwrap());
        } else if c == from.delimiter {
            out.push(to.delimiter);
        } else if c == from.single_word {
            out.push(to.single_word);
        } else if c == from.any_words {
            out.push(to.any_words);
        } else {
            emit_literal(&mut out, c);
        }
    }
    out
}

/// `getCoreAddressFromMqttTopic`.
pub fn core_address(topic_filter: &str, wildcard: &WildcardConfig) -> String {
    convert(topic_filter, &MQTT_WILDCARD, wildcard)
}

/// `getCoreRetainAddressFromMqttTopic`.
pub fn core_retain_address(topic_filter: &str, wildcard: &WildcardConfig) -> String {
    format!(
        "{MQTT_RETAIN_ADDRESS_PREFIX}{}",
        core_address(topic_filter, wildcard)
    )
}

/// `getMqttTopicFromCoreAddress`.
pub fn mqtt_topic(address: &str, wildcard: &WildcardConfig) -> String {
    let address = address
        .strip_prefix(MQTT_RETAIN_ADDRESS_PREFIX)
        .unwrap_or(address);
    convert(address, wildcard, &MQTT_WILDCARD)
}

/// `getCoreQueueFromMqttTopic`: `<share>.<address>` for a shared
/// subscription, `<client>.<address>` otherwise.
pub fn core_queue(topic_filter: &str, client_id: &str, wildcard: &WildcardConfig) -> String {
    match decompose_shared(topic_filter) {
        (Some(share), filter) => format!("{share}.{}", core_address(filter, wildcard)),
        (None, filter) => format!("{client_id}.{}", core_address(filter, wildcard)),
    }
}

/// `isSharedSubscription`.
pub fn is_shared(topic_filter: &str) -> bool {
    topic_filter.starts_with(SHARED_SUBSCRIPTION_PREFIX)
}

/// `decomposeSharedSubscriptionTopicFilter`: the share name and the topic
/// filter of `$share/<share>/<filter>`; no share name otherwise. A shared
/// filter with no `/` after the share name is taken whole as the share
/// name, with an empty filter.
pub fn decompose_shared(topic_filter: &str) -> (Option<&str>, &str) {
    match topic_filter.strip_prefix(SHARED_SUBSCRIPTION_PREFIX) {
        Some(rest) => match rest.find(SLASH) {
            Some(i) => (Some(&rest[..i]), &rest[i + 1..]),
            // `substring(prefix, -1)` throws in Java; see
            // `connection::Connection::subscribe`.
            None => (Some(rest), ""),
        },
        None => (None, topic_filter),
    }
}

/// `CompositeAddress.extractAddressName`: the address of `address::queue`.
pub fn extract_address_name(name: &str) -> &str {
    saranga_server::postoffice::split_fqqn(name).0
}

/// `CompositeAddress.isFullyQualified`.
pub fn is_fully_qualified(name: &str) -> bool {
    saranga_server::postoffice::split_fqqn(name).1.is_some()
}

/// The size of a variable byte integer (`calculateVariableByteIntegerSize`).
pub fn variable_byte_integer_size(mut v: i64) -> i64 {
    let mut count = 0;
    loop {
        v /= 128;
        count += 1;
        if v <= 0 {
            return count;
        }
    }
}

/// `ByteUtil.intToBytes`.
pub fn int_to_bytes(v: i32) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

/// `ByteUtil.bytesToInt`.
pub fn bytes_to_int(b: &[u8]) -> i32 {
    let mut a = [0u8; 4];
    let n = b.len().min(4);
    a[4 - n..].copy_from_slice(&b[b.len() - n..]);
    i32::from_be_bytes(a)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CORE: WildcardConfig = WildcardConfig {
        routing_enabled: true,
        delimiter: '.',
        any_words: '#',
        single_word: '*',
        uses_escape: true,
    };

    #[test]
    fn topics_convert_both_ways() {
        assert_eq!(core_address("a/b/c", &CORE), "a.b.c");
        assert_eq!(core_address("a/+/#", &CORE), "a.*.#");
        // Characters special to core addresses are escaped.
        assert_eq!(core_address("a.b/c*", &CORE), "a\\.b.c\\*");
        assert_eq!(mqtt_topic("a\\.b.c\\*", &CORE), "a.b/c*");
        assert_eq!(mqtt_topic("a.b.c", &CORE), "a/b/c");
        assert_eq!(mqtt_topic("$sys.mqtt.retain.a.b", &CORE), "a/b");
        assert_eq!(core_retain_address("a/#", &CORE), "$sys.mqtt.retain.a.#");
    }

    #[test]
    fn literal_backslashes_do_not_collide() {
        // Distinct MQTT topics map to distinct core addresses and round-trip,
        // where the old heuristic collapsed `a\/b` and `a\\/b` both to `a/b`.
        assert_eq!(core_address("a\\/b", &CORE), "a\\\\.b");
        assert_eq!(core_address("a\\\\/b", &CORE), "a\\\\\\\\.b");
        assert_ne!(core_address("a\\/b", &CORE), core_address("a\\\\/b", &CORE));
        assert_eq!(mqtt_topic(&core_address("a\\/b", &CORE), &CORE), "a\\/b");
        assert_eq!(
            mqtt_topic(&core_address("a\\\\/b", &CORE), &CORE),
            "a\\\\/b"
        );
    }

    #[test]
    fn queue_names() {
        assert_eq!(core_queue("a/b", "client", &CORE), "client.a.b");
        assert_eq!(core_queue("$share/g/a/+", "client", &CORE), "g.a.*");
        assert_eq!(decompose_shared("$share/g/a/b"), (Some("g"), "a/b"));
        assert_eq!(decompose_shared("a/b"), (None, "a/b"));
    }

    #[test]
    fn packet_id_bytes() {
        assert_eq!(int_to_bytes(258), vec![0, 0, 1, 2]);
        assert_eq!(bytes_to_int(&[0, 0, 1, 2]), 258);
        assert_eq!(variable_byte_integer_size(0), 1);
        assert_eq!(variable_byte_integer_size(127), 1);
        assert_eq!(variable_byte_integer_size(128), 2);
        assert_eq!(variable_byte_integer_size(16_384), 3);
    }
}
