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
};

/// `MQTTReasonCodes`.
pub mod reason {
    pub const UNACCEPTABLE_PROTOCOL_VERSION_3: u8 = 0x01;
    pub const IDENTIFIER_REJECTED_3: u8 = 0x02;
    pub const SERVER_UNAVAILABLE_3: u8 = 0x03;
    pub const BAD_USER_NAME_OR_PASSWORD_3: u8 = 0x04;
    pub const NOT_AUTHORIZED_3: u8 = 0x05;
    pub const SUCCESS: u8 = 0x00;
    pub const NO_SUBSCRIPTION_EXISTED: u8 = 0x11;
    pub const UNSPECIFIED_ERROR: u8 = 0x80;
    pub const MALFORMED_PACKET: u8 = 0x81;
    pub const IMPLEMENTATION_SPECIFIC_ERROR: u8 = 0x83;
    pub const UNSUPPORTED_PROTOCOL_VERSION: u8 = 0x84;
    pub const CLIENT_IDENTIFIER_NOT_VALID: u8 = 0x85;
    pub const BAD_USER_NAME_OR_PASSWORD: u8 = 0x86;
    pub const NOT_AUTHORIZED: u8 = 0x87;
    pub const SERVER_UNAVAILABLE: u8 = 0x88;
    pub const BAD_AUTHENTICATION_METHOD: u8 = 0x8C;
    pub const SESSION_TAKEN_OVER: u8 = 0x8E;
    pub const PACKET_IDENTIFIER_NOT_FOUND: u8 = 0x92;
    pub const TOPIC_ALIAS_INVALID: u8 = 0x94;
    pub const PACKET_TOO_LARGE: u8 = 0x95;
}

/// `WildcardConfiguration.convert`: `input` in `from`'s syntax, in `to`'s.
/// Characters that are wildcards or delimiters in one syntax but not the
/// other are escaped with `\` so the conversion can be undone.
pub fn convert(input: &str, from: &WildcardConfig, to: &WildcardConfig) -> String {
    if from == to {
        return input.to_owned();
    }
    let escaped = is_escaped(input, from);
    let mut result: Vec<char> = if escaped {
        input.chars().collect()
    } else {
        escape(input, to, from).chars().collect()
    };
    replace_char(&mut result, from.delimiter, to.delimiter);
    replace_char(&mut result, from.single_word, to.single_word);
    replace_char(&mut result, from.any_words, to.any_words);
    let result: String = result.into_iter().collect();
    if escaped {
        unescape(&result, from)
    } else {
        result
    }
}

/// `escape`, called on the target syntax `this`.
fn escape(input: &str, this: &WildcardConfig, from: &WildcardConfig) -> String {
    let esc = ESCAPE.to_string();
    let mut result = input.replace(&esc, &format!("{ESCAPE}{ESCAPE}"));
    for (mine, theirs) in [
        (this.delimiter, from.delimiter),
        (this.single_word, from.single_word),
        (this.any_words, from.any_words),
    ] {
        if mine != theirs {
            result = result.replace(mine, &format!("{ESCAPE}{mine}"));
        }
    }
    result
}

/// `unescape`, called on the source syntax.
fn unescape(input: &str, this: &WildcardConfig) -> String {
    input
        .replace(&format!("{ESCAPE}{ESCAPE}"), &ESCAPE.to_string())
        .replace(
            &format!("{ESCAPE}{}", this.delimiter),
            &this.delimiter.to_string(),
        )
        .replace(
            &format!("{ESCAPE}{}", this.single_word),
            &this.single_word.to_string(),
        )
        .replace(
            &format!("{ESCAPE}{}", this.any_words),
            &this.any_words.to_string(),
        )
}

/// `isEscaped`: an escape before a delimiter or wildcard of `this`.
fn is_escaped(input: &str, this: &WildcardConfig) -> bool {
    let chars: Vec<char> = input.chars().collect();
    chars.windows(2).any(|w| {
        w[0] == ESCAPE
            && (w[1] == this.delimiter || w[1] == this.single_word || w[1] == this.any_words)
    })
}

/// `replaceChar`: every unescaped `from` becomes `to`.
fn replace_char(result: &mut [char], from: char, to: char) {
    if from == to {
        return;
    }
    for i in 0..result.len() {
        if result[i] == from && (i == 0 || result[i - 1] != ESCAPE) {
            result[i] = to;
        }
    }
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
