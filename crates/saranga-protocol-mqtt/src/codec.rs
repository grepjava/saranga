//! MQTT packets as Netty's `MqttDecoder` and `MqttEncoder` (4.1) read and
//! write them, which is what Artemis's clients see: the same checks, the
//! same error texts, the same byte layout.

use std::fmt;

/// `MQTTVersion`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    V3_1,
    V3_1_1,
    V5,
}

impl Version {
    /// `protocolLevel`.
    pub fn level(self) -> u8 {
        match self {
            Self::V3_1 => 3,
            Self::V3_1_1 => 4,
            Self::V5 => 5,
        }
    }

    fn protocol_name(self) -> &'static str {
        match self {
            Self::V3_1 => "MQIsdp",
            _ => "MQTT",
        }
    }

    /// `MqttVersion.fromProtocolNameAndLevel`.
    fn from_name_and_level(name: &str, level: u8) -> Result<Self, String> {
        let version = match level {
            3 => Self::V3_1,
            4 => Self::V3_1_1,
            5 => Self::V5,
            _ => return Err(format!("{name} is an unknown protocol name")),
        };
        if version.protocol_name() == name {
            Ok(version)
        } else {
            Err(format!("{name} and {level} don't match"))
        }
    }
}

/// MQTT 5 property identifiers (`MqttPropertyType`).
pub mod prop {
    pub const PAYLOAD_FORMAT_INDICATOR: u8 = 0x01;
    pub const MESSAGE_EXPIRY_INTERVAL: u8 = 0x02;
    pub const CONTENT_TYPE: u8 = 0x03;
    pub const RESPONSE_TOPIC: u8 = 0x08;
    pub const CORRELATION_DATA: u8 = 0x09;
    pub const SUBSCRIPTION_IDENTIFIER: u8 = 0x0B;
    pub const SESSION_EXPIRY_INTERVAL: u8 = 0x11;
    pub const ASSIGNED_CLIENT_IDENTIFIER: u8 = 0x12;
    pub const SERVER_KEEP_ALIVE: u8 = 0x13;
    pub const AUTHENTICATION_METHOD: u8 = 0x15;
    pub const AUTHENTICATION_DATA: u8 = 0x16;
    pub const REQUEST_PROBLEM_INFORMATION: u8 = 0x17;
    pub const WILL_DELAY_INTERVAL: u8 = 0x18;
    pub const REQUEST_RESPONSE_INFORMATION: u8 = 0x19;
    pub const RESPONSE_INFORMATION: u8 = 0x1A;
    pub const SERVER_REFERENCE: u8 = 0x1C;
    pub const REASON_STRING: u8 = 0x1F;
    pub const RECEIVE_MAXIMUM: u8 = 0x21;
    pub const TOPIC_ALIAS_MAXIMUM: u8 = 0x22;
    pub const TOPIC_ALIAS: u8 = 0x23;
    pub const MAXIMUM_QOS: u8 = 0x24;
    pub const RETAIN_AVAILABLE: u8 = 0x25;
    pub const USER_PROPERTY: u8 = 0x26;
    pub const MAXIMUM_PACKET_SIZE: u8 = 0x27;
    pub const WILDCARD_SUBSCRIPTION_AVAILABLE: u8 = 0x28;
    pub const SUBSCRIPTION_IDENTIFIER_AVAILABLE: u8 = 0x29;
    pub const SHARED_SUBSCRIPTION_AVAILABLE: u8 = 0x2A;
}

/// How a property's value is carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Byte,
    TwoByte,
    FourByte,
    VarInt,
    Str,
    Pair,
    Binary,
}

fn kind(id: u32) -> Result<Kind, String> {
    use prop::*;
    let Ok(id8) = u8::try_from(id) else {
        return Err(format!("unknown property type: {id}"));
    };
    Ok(match id8 {
        PAYLOAD_FORMAT_INDICATOR
        | REQUEST_PROBLEM_INFORMATION
        | REQUEST_RESPONSE_INFORMATION
        | MAXIMUM_QOS
        | RETAIN_AVAILABLE
        | WILDCARD_SUBSCRIPTION_AVAILABLE
        | SUBSCRIPTION_IDENTIFIER_AVAILABLE
        | SHARED_SUBSCRIPTION_AVAILABLE => Kind::Byte,
        SERVER_KEEP_ALIVE | RECEIVE_MAXIMUM | TOPIC_ALIAS_MAXIMUM | TOPIC_ALIAS => Kind::TwoByte,
        MESSAGE_EXPIRY_INTERVAL
        | SESSION_EXPIRY_INTERVAL
        | WILL_DELAY_INTERVAL
        | MAXIMUM_PACKET_SIZE => Kind::FourByte,
        SUBSCRIPTION_IDENTIFIER => Kind::VarInt,
        CONTENT_TYPE
        | RESPONSE_TOPIC
        | ASSIGNED_CLIENT_IDENTIFIER
        | AUTHENTICATION_METHOD
        | RESPONSE_INFORMATION
        | SERVER_REFERENCE
        | REASON_STRING => Kind::Str,
        USER_PROPERTY => Kind::Pair,
        CORRELATION_DATA | AUTHENTICATION_DATA => Kind::Binary,
        _ => return Err(format!("unknown property type: {id}")),
    })
}

/// A property value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i32),
    Str(String),
    Binary(Vec<u8>),
}

/// `MqttProperties`: single-valued properties by ID (a repeat replaces),
/// subscription identifiers and user properties in order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Properties {
    /// In insertion order; [`Properties::encode`] orders them as Netty's
    /// `IntObjectHashMap` iterates.
    single: Vec<(u8, Value)>,
    subscription_ids: Vec<i32>,
    user: Vec<(String, String)>,
}

impl Properties {
    pub fn is_empty(&self) -> bool {
        self.single.is_empty() && self.subscription_ids.is_empty() && self.user.is_empty()
    }

    /// `add`.
    pub fn add(&mut self, id: u8, value: Value) {
        if id == prop::SUBSCRIPTION_IDENTIFIER {
            if let Value::Int(v) = value {
                self.subscription_ids.push(v);
            }
            return;
        }
        match self.single.iter_mut().find(|(i, _)| *i == id) {
            Some(slot) => slot.1 = value,
            None => self.single.push((id, value)),
        }
    }

    pub fn add_int(&mut self, id: u8, v: i32) {
        self.add(id, Value::Int(v));
    }

    pub fn add_user(&mut self, key: String, value: String) {
        self.user.push((key, value));
    }

    fn get(&self, id: u8) -> Option<&Value> {
        self.single.iter().find(|(i, _)| *i == id).map(|(_, v)| v)
    }

    /// An integer property; for the subscription identifier, the first.
    pub fn int(&self, id: u8) -> Option<i32> {
        if id == prop::SUBSCRIPTION_IDENTIFIER {
            return self.subscription_ids.first().copied();
        }
        match self.get(id)? {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn string(&self, id: u8) -> Option<&str> {
        match self.get(id)? {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn binary(&self, id: u8) -> Option<&[u8]> {
        match self.get(id)? {
            Value::Binary(b) => Some(b),
            _ => None,
        }
    }

    pub fn has(&self, id: u8) -> bool {
        match id {
            prop::USER_PROPERTY => !self.user.is_empty(),
            prop::SUBSCRIPTION_IDENTIFIER => !self.subscription_ids.is_empty(),
            _ => self.get(id).is_some(),
        }
    }

    pub fn subscription_ids(&self) -> &[i32] {
        &self.subscription_ids
    }

    pub fn user_properties(&self) -> &[(String, String)] {
        &self.user
    }

    /// `encodeProperties`: the length, then the single-valued properties in
    /// `IntObjectHashMap` order, the subscription identifiers, and the user
    /// properties.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let mut body = Vec::new();
        let ids: Vec<u8> = self.single.iter().map(|(id, _)| *id).collect();
        for i in int_object_hash_map_order(&ids) {
            let (id, value) = &self.single[i];
            write_var_int(&mut body, u32::from(*id));
            match (kind(u32::from(*id)), value) {
                (Ok(Kind::Byte), Value::Int(v)) => body.push(*v as u8),
                (Ok(Kind::TwoByte), Value::Int(v)) => {
                    body.extend_from_slice(&(*v as u16).to_be_bytes())
                }
                (Ok(Kind::FourByte), Value::Int(v)) => body.extend_from_slice(&v.to_be_bytes()),
                (Ok(Kind::VarInt), Value::Int(v)) => write_var_int(&mut body, *v as u32),
                (_, Value::Str(s)) => write_string(&mut body, s),
                (_, Value::Binary(b)) => {
                    body.extend_from_slice(&(b.len() as u16).to_be_bytes());
                    body.extend_from_slice(b);
                }
                (_, Value::Int(v)) => body.extend_from_slice(&v.to_be_bytes()),
            }
        }
        for id in &self.subscription_ids {
            write_var_int(&mut body, u32::from(prop::SUBSCRIPTION_IDENTIFIER));
            write_var_int(&mut body, *id as u32);
        }
        for (k, v) in &self.user {
            write_var_int(&mut body, u32::from(prop::USER_PROPERTY));
            write_string(&mut body, k);
            write_string(&mut body, v);
        }
        write_var_int(out, body.len() as u32);
        out.extend_from_slice(&body);
    }
}

/// The order Netty's `IntObjectHashMap` (capacity 8, load factor 0.5,
/// linear probing, identity hash) iterates keys inserted in this order.
fn int_object_hash_map_order(keys: &[u8]) -> Vec<usize> {
    fn insert(table: &mut [Option<usize>], keys: &[u8], i: usize) {
        let mask = table.len() - 1;
        let mut at = usize::from(keys[i]) & mask;
        while table[at].is_some() {
            at = (at + 1) & mask;
        }
        table[at] = Some(i);
    }
    let mut table: Vec<Option<usize>> = vec![None; 8];
    let mut size = 0;
    for i in 0..keys.len() {
        insert(&mut table, keys, i);
        size += 1;
        let capacity = table.len();
        let max_size = (capacity - 1).min(capacity / 2);
        if size > max_size {
            let old = std::mem::replace(&mut table, vec![None; capacity * 2]);
            for k in old.into_iter().flatten() {
                insert(&mut table, keys, k);
            }
        }
    }
    table.into_iter().flatten().collect()
}

/// `MqttSubscriptionOption`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionOption {
    pub qos: u8,
    pub no_local: bool,
    pub retain_as_published: bool,
    /// 0 send at subscribe, 1 only for a new subscription, 2 never.
    pub retain_handling: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Will {
    pub properties: Properties,
    /// `None` for a topic longer than 32767 bytes, which Netty skips.
    pub topic: Option<String>,
    pub message: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    pub version: Version,
    pub clean_start: bool,
    pub keep_alive: u16,
    pub properties: Properties,
    pub client_id: String,
    pub will: Option<Will>,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
    /// The remaining length of the fixed header.
    pub remaining_length: i32,
    pub topic: String,
    /// -1 at QoS 0.
    pub packet_id: i32,
    pub properties: Properties,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    pub filter: String,
    pub option: SubscriptionOption,
}

/// A packet a client sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Connect(Box<Connect>),
    ConnAck,
    Publish(Publish),
    PubAck {
        packet_id: i32,
        reason: u8,
    },
    PubRec {
        packet_id: i32,
        reason: u8,
    },
    PubRel {
        packet_id: i32,
        reason: u8,
    },
    PubComp {
        packet_id: i32,
        reason: u8,
    },
    Subscribe {
        packet_id: i32,
        properties: Properties,
        subscriptions: Vec<Subscription>,
    },
    SubAck,
    Unsubscribe {
        packet_id: i32,
        properties: Properties,
        topics: Vec<String>,
    },
    UnsubAck,
    PingReq,
    PingResp,
    Disconnect {
        reason: u8,
        properties: Properties,
    },
    Auth {
        reason: u8,
        properties: Properties,
    },
}

impl Packet {
    /// `MqttMessageType`, for logs.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Connect(_) => "CONNECT",
            Self::ConnAck => "CONNACK",
            Self::Publish(_) => "PUBLISH",
            Self::PubAck { .. } => "PUBACK",
            Self::PubRec { .. } => "PUBREC",
            Self::PubRel { .. } => "PUBREL",
            Self::PubComp { .. } => "PUBCOMP",
            Self::Subscribe { .. } => "SUBSCRIBE",
            Self::SubAck => "SUBACK",
            Self::Unsubscribe { .. } => "UNSUBSCRIBE",
            Self::UnsubAck => "UNSUBACK",
            Self::PingReq => "PINGREQ",
            Self::PingResp => "PINGRESP",
            Self::Disconnect { .. } => "DISCONNECT",
            Self::Auth { .. } => "AUTH",
        }
    }
}

/// Why a packet could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError(pub String);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Not enough bytes yet, or the packet is bad.
enum Stop {
    More,
    Bad(String),
}

impl From<String> for Stop {
    fn from(s: String) -> Self {
        Self::Bad(s)
    }
}

const TYPE_NAMES: [&str; 16] = [
    "",
    "CONNECT",
    "CONNACK",
    "PUBLISH",
    "PUBACK",
    "PUBREC",
    "PUBREL",
    "PUBCOMP",
    "SUBSCRIBE",
    "SUBACK",
    "UNSUBSCRIBE",
    "UNSUBACK",
    "PINGREQ",
    "PINGRESP",
    "DISCONNECT",
    "AUTH",
];

/// Reads from the stream, not the packet: like Netty's replaying decoder,
/// a field may run on into the bytes after the packet's remaining length.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Cursor<'_> {
    fn u8(&mut self) -> Result<u8, Stop> {
        let b = *self.buf.get(self.pos).ok_or(Stop::More)?;
        self.pos += 1;
        Ok(b)
    }

    fn bytes(&mut self, n: usize) -> Result<&[u8], Stop> {
        if self.buf.len() - self.pos < n {
            return Err(Stop::More);
        }
        let b = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(b)
    }

    fn msb_lsb(&mut self) -> Result<i32, Stop> {
        let msb = self.u8()?;
        let lsb = self.u8()?;
        Ok(i32::from(msb) << 8 | i32::from(lsb))
    }

    fn i32(&mut self) -> Result<i32, Stop> {
        let b = self.bytes(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// `decodeVariableByteInteger`: value and bytes read.
    fn var_int(&mut self) -> Result<(i32, i32), Stop> {
        let (mut value, mut multiplier, mut loops) = (0i32, 1i32, 0);
        loop {
            let digit = self.u8()?;
            value = value.wrapping_add(i32::from(digit & 127).wrapping_mul(multiplier));
            multiplier = multiplier.wrapping_mul(128);
            loops += 1;
            if digit & 128 == 0 {
                return Ok((value, loops));
            }
            if loops == 4 {
                return Err(Stop::Bad(
                    "MQTT protocol limits Remaining Length to 4 bytes".into(),
                ));
            }
        }
    }

    /// `decodeMessageId`: anything but 0.
    fn message_id(&mut self) -> Result<i32, Stop> {
        let id = self.msb_lsb()?;
        if id == 0 {
            return Err(Stop::Bad(format!("invalid messageId: {id}")));
        }
        Ok(id)
    }

    /// `decodeString`: a strict UTF-8 string with no U+0000, and the bytes
    /// read. Longer than `max` bytes, it is skipped and comes back `None`.
    fn string_max(&mut self, max: i32) -> Result<(Option<String>, i32), Stop> {
        let size = self.msb_lsb()?;
        let bytes = self.bytes(size as usize)?;
        if size > max {
            return Ok((None, size + 2));
        }
        let s = std::str::from_utf8(bytes)
            .map_err(|_| Stop::Bad("invalid UTF-8 string in MQTT packet".into()))?;
        if s.contains('\0') {
            return Err(Stop::Bad(
                "MQTT UTF-8 Encoded String must not contain U+0000".into(),
            ));
        }
        Ok((Some(s.to_owned()), size + 2))
    }

    fn string(&mut self) -> Result<(String, i32), Stop> {
        let (s, n) = self.string_max(i32::MAX)?;
        Ok((s.unwrap_or_default(), n))
    }

    fn byte_array(&mut self) -> Result<Vec<u8>, Stop> {
        let size = self.msb_lsb()?;
        Ok(self.bytes(size as usize)?.to_vec())
    }

    /// `decodeProperties`. Like Netty, the loop counts the length's own
    /// bytes as property bytes.
    fn properties(&mut self) -> Result<(Properties, i32), Stop> {
        let (total, mut consumed) = self.var_int()?;
        if total > 0 && self.buf.len() - self.pos < total as usize {
            return Err(Stop::More);
        }
        let mut props = Properties::default();
        while consumed < total {
            let (id, n) = self.var_int()?;
            consumed += n;
            let id_u = id as u32;
            match kind(id_u)? {
                Kind::Byte => {
                    props.add_int(id as u8, i32::from(self.u8()?));
                    consumed += 1;
                }
                Kind::TwoByte => {
                    props.add_int(id as u8, self.msb_lsb()?);
                    consumed += 2;
                }
                Kind::FourByte => {
                    props.add_int(id as u8, self.i32()?);
                    consumed += 4;
                }
                Kind::VarInt => {
                    let (v, n) = self.var_int()?;
                    consumed += n;
                    props.add_int(id as u8, v);
                }
                Kind::Str => {
                    let (s, n) = self.string()?;
                    consumed += n;
                    props.add(id as u8, Value::Str(s));
                }
                Kind::Pair => {
                    let (k, n1) = self.string()?;
                    let (v, n2) = self.string()?;
                    consumed += n1 + n2;
                    props.add_user(k, v);
                }
                Kind::Binary => {
                    let b = self.byte_array()?;
                    consumed += b.len() as i32 + 2;
                    props.add(id as u8, Value::Binary(b));
                }
            }
        }
        Ok((props, consumed))
    }
}

/// `MqttDecoder`, with the MQTT version a CONNECT set (`MQTT_VERSION_KEY`).
#[derive(Debug)]
pub struct Decoder {
    /// `None` until a CONNECT names a version; 3.1.1 rules apply until then.
    pub version: Option<Version>,
    /// A bad packet was seen: every later byte is dropped (`BAD_MESSAGE`).
    bad: bool,
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder {
    pub fn new() -> Self {
        Self {
            version: None,
            bad: false,
        }
    }

    fn current(&self) -> Version {
        self.version.unwrap_or(Version::V3_1_1)
    }

    /// The next packet in `buf`, which loses its bytes; `None` until a whole
    /// one is there. After an error nothing more is decoded.
    pub fn decode(&mut self, buf: &mut Vec<u8>) -> Option<Result<Packet, DecodeError>> {
        if self.bad {
            buf.clear();
            return None;
        }
        let mut c = Cursor { buf, pos: 0 };
        match self.packet(&mut c) {
            Ok(p) => {
                let used = c.pos;
                buf.drain(..used);
                Some(Ok(p))
            }
            Err(Stop::More) => None,
            Err(Stop::Bad(e)) => {
                self.bad = true;
                buf.clear();
                Some(Err(DecodeError(e)))
            }
        }
    }

    fn packet(&mut self, c: &mut Cursor<'_>) -> Result<Packet, Stop> {
        // decodeFixedHeader
        let b1 = c.u8()?;
        let kind = b1 >> 4;
        if kind == 0 {
            return Err(Stop::Bad(format!("unknown message type: {kind}")));
        }
        let name = TYPE_NAMES[usize::from(kind)];
        let dup = b1 & 0x08 != 0;
        let qos = (b1 & 0x06) >> 1;
        let retain = b1 & 0x01 != 0;
        match kind {
            3 => {
                if qos == 3 {
                    return Err(Stop::Bad(format!(
                        "Illegal QOS Level in fixed header of PUBLISH message ({qos})"
                    )));
                }
            }
            6 | 8 | 10 => {
                if dup {
                    return Err(Stop::Bad(format!(
                        "Illegal BIT 3 in fixed header of {name} message, must be 0, found 1"
                    )));
                }
                if qos != 1 {
                    return Err(Stop::Bad(format!(
                        "Illegal QOS Level in fixed header of {name} message, must be 1, found {qos}"
                    )));
                }
                if retain {
                    return Err(Stop::Bad(format!(
                        "Illegal BIT 0 in fixed header of {name} message, must be 0, found 1"
                    )));
                }
            }
            _ => {
                if dup {
                    return Err(Stop::Bad(format!(
                        "Illegal BIT 3 in fixed header of {name} message, must be 0, found 1"
                    )));
                }
                if qos != 0 {
                    return Err(Stop::Bad(format!(
                        "Illegal BIT 2 or 1 in fixed header of {name} message, must be 0, found {qos}"
                    )));
                }
                if retain {
                    return Err(Stop::Bad(format!(
                        "Illegal BIT 0 in fixed header of {name} message, must be 0, found 1"
                    )));
                }
            }
        }
        let (mut remaining, mut multiplier, mut loops) = (0i32, 1i32, 0);
        loop {
            let digit = c.u8()?;
            remaining += i32::from(digit & 127) * multiplier;
            multiplier = multiplier.wrapping_mul(128);
            loops += 1;
            if digit & 128 == 0 {
                break;
            }
            if loops == 4 {
                return Err(Stop::Bad(format!(
                    "remaining length exceeds 4 digits ({name})"
                )));
            }
        }
        if kind == 15 && self.current() != Version::V5 {
            return Err(Stop::Bad("AUTH message requires at least MQTT 5".into()));
        }
        let remaining_length = remaining;
        let v5 = |d: &Self| d.current() == Version::V5;
        // validateNoBytesRemain
        let no_bytes_remain = |remaining: i32, consumed: i32| -> Result<(), Stop> {
            let left = remaining - consumed;
            if left != 0 {
                return Err(Stop::Bad(format!(
                    "non-zero remaining payload bytes: {left} ({name})"
                )));
            }
            Ok(())
        };
        match kind {
            1 => {
                let (proto, n) = c.string()?;
                remaining -= n;
                let level = c.u8()?;
                let version = Version::from_name_and_level(&proto, level)?;
                self.version = Some(version);
                let flags = c.u8()?;
                let keep_alive = c.msb_lsb()? as u16;
                remaining -= 4;
                let has_user = flags & 0x80 != 0;
                let has_password = flags & 0x40 != 0;
                let will_retain = flags & 0x20 != 0;
                let will_qos = (flags & 0x18) >> 3;
                let will_flag = flags & 0x04 != 0;
                let clean_start = flags & 0x02 != 0;
                if version != Version::V3_1 && flags & 0x01 != 0 {
                    return Err(Stop::Bad("non-zero reserved flag".into()));
                }
                let properties = if version == Version::V5 {
                    let (p, n) = c.properties()?;
                    remaining -= n;
                    p
                } else {
                    Properties::default()
                };
                // decodeConnectionPayload
                let (client_id, mut consumed) = c.string()?;
                // isValidClientId: 1 to 23 UTF-16 units in 3.1, anything
                // else in 3.1.1 and 5.
                if version == Version::V3_1 {
                    let units = client_id.encode_utf16().count();
                    if !(1..=23).contains(&units) {
                        return Err(Stop::Bad("invalid clientIdentifier".into()));
                    }
                }
                let will = if will_flag {
                    let properties = if version == Version::V5 {
                        let (p, n) = c.properties()?;
                        consumed += n;
                        p
                    } else {
                        Properties::default()
                    };
                    let (topic, n) = c.string_max(32767)?;
                    consumed += n;
                    let message = c.byte_array()?;
                    consumed += message.len() as i32 + 2;
                    Some(Will {
                        properties,
                        topic,
                        message,
                        qos: will_qos,
                        retain: will_retain,
                    })
                } else {
                    None
                };
                let username = if has_user {
                    let (u, n) = c.string()?;
                    consumed += n;
                    Some(u)
                } else {
                    None
                };
                let password = if has_password {
                    let p = c.byte_array()?;
                    consumed += p.len() as i32 + 2;
                    Some(p)
                } else {
                    None
                };
                no_bytes_remain(remaining, consumed)?;
                Ok(Packet::Connect(Box::new(Connect {
                    version,
                    clean_start,
                    keep_alive,
                    properties,
                    client_id,
                    will,
                    username,
                    password,
                })))
            }
            2 => {
                c.u8()?;
                c.u8()?;
                remaining -= 2;
                if v5(self) {
                    remaining -= c.properties()?.1;
                }
                no_bytes_remain(remaining, 0)?;
                Ok(Packet::ConnAck)
            }
            3 => {
                let (topic, mut n) = c.string()?;
                if topic.contains(['#', '+', '\0']) {
                    return Err(Stop::Bad(format!(
                        "invalid publish topic name: {topic} (contains wildcards)"
                    )));
                }
                let mut packet_id = -1;
                if qos > 0 {
                    packet_id = c.message_id()?;
                    n += 2;
                }
                let properties = if v5(self) {
                    let (p, pn) = c.properties()?;
                    n += pn;
                    p
                } else {
                    Properties::default()
                };
                remaining -= n;
                if remaining < 0 {
                    return Err(Stop::Bad(format!(
                        "minimumReadableBytes : {remaining} (expected: >= 0)"
                    )));
                }
                let payload = c.bytes(remaining as usize)?.to_vec();
                Ok(Packet::Publish(Publish {
                    dup,
                    qos,
                    retain,
                    remaining_length,
                    topic,
                    packet_id,
                    properties,
                    payload,
                }))
            }
            4..=7 => {
                // decodePubReplyMessage: the remaining length alone says
                // what follows the packet ID.
                let packet_id = c.message_id()?;
                let reason;
                if remaining > 3 {
                    reason = c.u8()?;
                    remaining -= 3 + c.properties()?.1;
                } else if remaining > 2 {
                    reason = c.u8()?;
                    remaining -= 3;
                } else {
                    reason = 0;
                    remaining -= 2;
                }
                no_bytes_remain(remaining, 0)?;
                Ok(match kind {
                    4 => Packet::PubAck { packet_id, reason },
                    5 => Packet::PubRec { packet_id, reason },
                    6 => Packet::PubRel { packet_id, reason },
                    _ => Packet::PubComp { packet_id, reason },
                })
            }
            8..=11 => {
                let packet_id = c.message_id()?;
                let properties = if v5(self) {
                    let (p, n) = c.properties()?;
                    remaining -= 2 + n;
                    p
                } else {
                    remaining -= 2;
                    Properties::default()
                };
                let mut consumed = 0;
                match kind {
                    8 => {
                        let mut subscriptions = Vec::new();
                        while consumed < remaining {
                            let (filter, n) = c.string()?;
                            consumed += n;
                            let option = c.u8()?;
                            let qos = option & 0x03;
                            if qos == 3 {
                                return Err(Stop::Bad(format!("invalid QoS: {qos}")));
                            }
                            let retain_handling = (option & 0x30) >> 4;
                            if retain_handling == 3 {
                                return Err(Stop::Bad(format!(
                                    "invalid RetainedHandlingPolicy: {retain_handling}"
                                )));
                            }
                            consumed += 1;
                            subscriptions.push(Subscription {
                                filter,
                                option: SubscriptionOption {
                                    qos,
                                    no_local: option & 0x04 != 0,
                                    retain_as_published: option & 0x08 != 0,
                                    retain_handling,
                                },
                            });
                        }
                        no_bytes_remain(remaining, consumed)?;
                        Ok(Packet::Subscribe {
                            packet_id,
                            properties,
                            subscriptions,
                        })
                    }
                    10 => {
                        let mut topics = Vec::new();
                        while consumed < remaining {
                            let (t, n) = c.string()?;
                            consumed += n;
                            topics.push(t);
                        }
                        no_bytes_remain(remaining, consumed)?;
                        Ok(Packet::Unsubscribe {
                            packet_id,
                            properties,
                            topics,
                        })
                    }
                    _ => {
                        while consumed < remaining {
                            c.u8()?;
                            consumed += 1;
                        }
                        no_bytes_remain(remaining, consumed)?;
                        Ok(if kind == 9 {
                            Packet::SubAck
                        } else {
                            Packet::UnsubAck
                        })
                    }
                }
            }
            12 | 13 => {
                no_bytes_remain(remaining, 0)?;
                Ok(if kind == 12 {
                    Packet::PingReq
                } else {
                    Packet::PingResp
                })
            }
            _ => {
                // DISCONNECT and AUTH
                let (reason, properties) = if remaining > 1 {
                    let reason = c.u8()?;
                    let (p, n) = c.properties()?;
                    remaining -= 1 + n;
                    (reason, p)
                } else if remaining > 0 {
                    remaining -= 1;
                    (c.u8()?, Properties::default())
                } else {
                    (0, Properties::default())
                };
                no_bytes_remain(remaining, 0)?;
                Ok(if kind == 14 {
                    Packet::Disconnect { reason, properties }
                } else {
                    Packet::Auth { reason, properties }
                })
            }
        }
    }
}

/// `writeVariableLengthInt`.
pub fn write_var_int(out: &mut Vec<u8>, mut num: u32) {
    loop {
        let mut digit = (num % 128) as u8;
        num /= 128;
        if num > 0 {
            digit |= 0x80;
        }
        out.push(digit);
        if num == 0 {
            return;
        }
    }
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn frame(byte1: u8, body: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(byte1);
    write_var_int(&mut out, body.len() as u32);
    out.extend_from_slice(&body);
    out
}

/// The properties, in MQTT 5 only (`encodePropertiesIfNeeded`).
fn properties_if_needed(version: Version, properties: &Properties, out: &mut Vec<u8>) {
    if version == Version::V5 {
        properties.encode(out);
    }
}

/// CONNACK.
pub fn connack(
    version: Version,
    session_present: bool,
    code: u8,
    properties: &Properties,
) -> Vec<u8> {
    let mut body = vec![u8::from(session_present), code];
    properties_if_needed(version, properties, &mut body);
    frame(0x20, body)
}

/// A PUBLISH; `None` for a topic Netty refuses to write (`invalid
/// topicName`).
#[allow(clippy::too_many_arguments)]
pub fn publish(
    version: Version,
    dup: bool,
    qos: u8,
    retain: bool,
    topic: &str,
    packet_id: i32,
    properties: &Properties,
    payload: &[u8],
) -> Option<Vec<u8>> {
    if topic.contains(['#', '+', '\0']) {
        return None;
    }
    let mut body = Vec::with_capacity(topic.len() + payload.len() + 16);
    write_string(&mut body, topic);
    if qos > 0 {
        body.extend_from_slice(&(packet_id as u16).to_be_bytes());
    }
    properties_if_needed(version, properties, &mut body);
    body.extend_from_slice(payload);
    let byte1 = 0x30 | (u8::from(dup) << 3) | (qos << 1) | u8::from(retain);
    Some(frame(byte1, body))
}

/// PUBACK (4), PUBREC (5), PUBREL (6) or PUBCOMP (7). The reason code goes
/// only to an MQTT 5 client, and only when it is not success.
pub fn pub_reply(version: Version, kind: u8, packet_id: i32, reason: u8) -> Vec<u8> {
    let mut body = (packet_id as u16).to_be_bytes().to_vec();
    if version == Version::V5 && reason != 0 {
        body.push(reason);
        Properties::default().encode(&mut body);
    }
    // [MQTT-3.6.1-1] PUBREL has QoS 1 in its fixed header.
    let byte1 = kind << 4 | if kind == 6 { 0x02 } else { 0 };
    frame(byte1, body)
}

/// SUBACK: each code as given.
pub fn suback(version: Version, packet_id: i32, codes: &[u8]) -> Vec<u8> {
    let mut body = (packet_id as u16).to_be_bytes().to_vec();
    properties_if_needed(version, &Properties::default(), &mut body);
    body.extend_from_slice(codes);
    frame(0x90, body)
}

/// UNSUBACK: the properties and reason codes in MQTT 5 only.
pub fn unsuback(
    version: Version,
    packet_id: i32,
    properties: &Properties,
    codes: &[u8],
) -> Vec<u8> {
    let mut body = (packet_id as u16).to_be_bytes().to_vec();
    properties_if_needed(version, properties, &mut body);
    if version == Version::V5 {
        body.extend_from_slice(codes);
    }
    frame(0xB0, body)
}

pub fn pingresp() -> Vec<u8> {
    vec![0xD0, 0x00]
}

/// DISCONNECT with a reason code, which only MQTT 5 carries.
pub fn disconnect(version: Version, reason: u8) -> Vec<u8> {
    let mut body = Vec::new();
    if version == Version::V5 && reason != 0 {
        body.push(reason);
        Properties::default().encode(&mut body);
    }
    frame(0xE0, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(bytes: &[u8]) -> (Decoder, Vec<Result<Packet, DecodeError>>) {
        let mut d = Decoder::new();
        let mut buf = bytes.to_vec();
        let mut out = Vec::new();
        while let Some(p) = d.decode(&mut buf) {
            out.push(p);
        }
        (d, out)
    }

    fn connect_v311(client_id: &str) -> Vec<u8> {
        let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 4, 0x02, 0, 60];
        write_string(&mut body, client_id);
        frame(0x10, body)
    }

    #[test]
    fn connect_and_publish() {
        let mut bytes = connect_v311("c1");
        bytes.extend(
            publish(
                Version::V3_1_1,
                false,
                1,
                false,
                "a/b",
                7,
                &Properties::default(),
                b"hi",
            )
            .unwrap(),
        );
        let (d, packets) = decode_all(&bytes);
        assert_eq!(d.version, Some(Version::V3_1_1));
        let Ok(Packet::Connect(c)) = &packets[0] else {
            panic!()
        };
        assert_eq!(c.client_id, "c1");
        assert!(c.clean_start);
        let Ok(Packet::Publish(p)) = &packets[1] else {
            panic!()
        };
        assert_eq!(
            (p.topic.as_str(), p.packet_id, p.payload.as_slice()),
            ("a/b", 7, &b"hi"[..])
        );
    }

    #[test]
    fn partial_packets_wait() {
        let bytes = connect_v311("c1");
        let mut d = Decoder::new();
        let mut buf = bytes[..5].to_vec();
        assert!(d.decode(&mut buf).is_none());
        buf.extend_from_slice(&bytes[5..]);
        assert!(matches!(d.decode(&mut buf), Some(Ok(Packet::Connect(_)))));
        assert!(buf.is_empty());
    }

    #[test]
    fn bad_packets() {
        let (_, p) = decode_all(&[0x00, 0x00]);
        assert_eq!(p, vec![Err(DecodeError("unknown message type: 0".into()))]);
        let (_, p) = decode_all(&[0x80, 0x02, 0x00, 0x01]);
        assert_eq!(
            p,
            vec![Err(DecodeError(
                "Illegal QOS Level in fixed header of SUBSCRIBE message, must be 1, found 0".into()
            ))]
        );
        let mut bytes = connect_v311("c1");
        bytes.extend([0x40, 0x02, 0x00, 0x00, 0xC0, 0x00]);
        let (_, p) = decode_all(&bytes);
        assert_eq!(p.len(), 2);
        assert_eq!(p[1], Err(DecodeError("invalid messageId: 0".into())));
        // Wildcards in a published topic.
        let mut body = Vec::new();
        write_string(&mut body, "a/#");
        let (_, p) = decode_all(&frame(0x30, body));
        assert_eq!(
            p,
            vec![Err(DecodeError(
                "invalid publish topic name: a/# (contains wildcards)".into()
            ))]
        );
        // An unknown protocol level.
        let (_, p) = decode_all(&frame(
            0x10,
            vec![0, 4, b'M', b'Q', b'T', b'T', 6, 0, 0, 0, 0, 0],
        ));
        assert_eq!(
            p,
            vec![Err(DecodeError("MQTT is an unknown protocol name".into()))]
        );
    }

    #[test]
    fn mqtt5_properties() {
        let mut props = Properties::default();
        props.add(prop::ASSIGNED_CLIENT_IDENTIFIER, Value::Str("x".into()));
        props.add_int(prop::TOPIC_ALIAS_MAXIMUM, 10);
        props.add_int(prop::SERVER_KEEP_ALIVE, 60);
        props.add_int(prop::MAXIMUM_PACKET_SIZE, 1000);
        let bytes = connack(Version::V5, false, 0, &props);
        // IntObjectHashMap order: 18 and 34 share slot 2, 19 then probes
        // past them to slot 4, 39 is in slot 7.
        assert_eq!(
            bytes,
            vec![
                0x20, 18, 0, 0, 15, 0x12, 0, 1, b'x', 0x22, 0, 10, 0x13, 0, 60, 0x27, 0, 0, 3, 0xE8
            ]
        );
        assert_eq!(
            connack(Version::V3_1_1, true, 5, &props),
            vec![0x20, 2, 1, 5]
        );
    }

    #[test]
    fn replies() {
        assert_eq!(pub_reply(Version::V5, 4, 1, 0), vec![0x40, 2, 0, 1]);
        assert_eq!(
            pub_reply(Version::V5, 5, 1, 0x92),
            vec![0x50, 4, 0, 1, 0x92, 0]
        );
        assert_eq!(pub_reply(Version::V3_1_1, 6, 1, 0x92), vec![0x62, 2, 0, 1]);
        assert_eq!(disconnect(Version::V5, 0x8E), vec![0xE0, 2, 0x8E, 0]);
        assert_eq!(
            suback(Version::V3_1_1, 3, &[1, 0x80]),
            vec![0x90, 4, 0, 3, 1, 0x80]
        );
        assert_eq!(suback(Version::V5, 3, &[1]), vec![0x90, 4, 0, 3, 0, 1]);
        assert_eq!(
            unsuback(Version::V3_1_1, 3, &Properties::default(), &[0]),
            vec![0xB0, 2, 0, 3]
        );
    }

    #[test]
    fn netty_hash_order() {
        assert_eq!(
            int_object_hash_map_order(&[18, 34, 19, 39]),
            vec![0, 1, 2, 3]
        );
        // A fifth key grows the table to 16 slots.
        assert_eq!(
            int_object_hash_map_order(&[1, 8, 9, 3, 2]),
            vec![0, 4, 3, 1, 2]
        );
    }
}
