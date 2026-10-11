//! AMQP messages against Artemis 2.57.0's own classes
//! (`tests/golden/amqp/messages.txt`, written by
//! `tests/golden-gen/amqp/MessageFixtures.java`): persisting, the changes
//! the broker makes to a message moved to a dead-letter address or routed
//! by a divert, redelivery, and conversion to and from Core.

use std::collections::BTreeMap;
use std::path::Path;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use saranga_protocol_amqp::convert;
use saranga_protocol_amqp::message::AmqpMessage;
use saranga_protocol_amqp::native::AmqpNative;
use saranga_server::message::{self as m, NativeMessage};
use saranga_storage::RecordCodec;
use saranga_storage::message::PersistedMessage;

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The fixtures by case, then kind.
fn fixtures() -> BTreeMap<String, BTreeMap<String, Vec<u8>>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/golden/amqp/messages.txt");
    let mut out: BTreeMap<String, BTreeMap<String, Vec<u8>>> = BTreeMap::new();
    for line in std::fs::read_to_string(path).unwrap().lines() {
        let mut parts = line.split(' ');
        let (case, kind, h) = (
            parts.next().unwrap(),
            parts.next().unwrap(),
            parts.next().unwrap(),
        );
        out.entry(case.into())
            .or_default()
            .insert(kind.into(), hex(h));
    }
    out
}

const NOW: i64 = 1_750_000_000_000;

/// A received message after `setMessageID(100)` and `setAddress("addr")`.
fn received(input: &[u8]) -> AmqpMessage {
    let mut msg = AmqpMessage::new(0, input.to_vec(), NOW).unwrap();
    msg.set_message_id(100);
    msg.set_address("addr".into());
    msg
}

fn persisted(n: &dyn NativeMessage) -> Vec<u8> {
    n.persisted().encode()
}

/// The Java-encoded and Saranga-encoded records differ: says where.
fn diff(case: &str, kind: &str, java: &[u8], ours: &[u8]) -> Option<String> {
    (java != ours).then(|| {
        format!(
            "{case} {kind}\n  artemis {}\n  saranga {}",
            to_hex(java),
            to_hex(ours)
        )
    })
}

#[test]
fn persisting_and_broker_changes_match_artemis() {
    let mut failures = Vec::new();
    for (case, kinds) in fixtures() {
        let Some(input) = kinds.get("input") else {
            continue;
        };
        let native = AmqpNative::new(received(input));
        failures.extend(diff(
            &case,
            "persist",
            &kinds["persist"],
            &persisted(&native),
        ));

        // QueueImpl.makeCopy to a dead-letter address.
        let before = native.view();
        let after = m::make_copy(&before, 200, "DLA", "q", true, 1234);
        let dla = native.updated(&before, &after);
        failures.extend(diff(&case, "dla", &kinds["dla"], &persisted(&*dla)));

        // DivertImpl: what Divert::copy does with a multicast divert.
        let mut after = before.clone();
        after.message_id = 300;
        let p = &mut after.properties;
        p.insert(
            m::HDR_ORIGINAL_QUEUE.into(),
            PropertyValue::String("div".into()),
        );
        p.insert(
            m::HDR_ORIGINAL_ADDRESS.into(),
            PropertyValue::String(before.address.clone().unwrap()),
        );
        p.insert(m::HDR_ORIG_MESSAGE_ID.into(), PropertyValue::Long(100));
        if let Some(t) = m::routing_type(&before) {
            p.insert(
                m::HDR_ORIG_ROUTING_TYPE.into(),
                PropertyValue::Byte(t.to_byte()),
            );
        }
        after.address = Some("other".into());
        after
            .properties
            .insert(m::HDR_ROUTING_TYPE.into(), PropertyValue::Byte(0));
        let divert = native.updated(&before, &after);
        failures.extend(diff(
            &case,
            "divert",
            &kinds["divert"],
            &persisted(&*divert),
        ));

        // A property the broker adds.
        let mut after = before.clone();
        after
            .properties
            .insert("added".into(), PropertyValue::String("x".into()));
        let prop = native.updated(&before, &after);
        failures.extend(diff(&case, "prop", &kinds["prop"], &persisted(&*prop)));

        failures.extend(diff(
            &case,
            "send2",
            &kinds["send2"],
            &native.message.send_buffer(2),
        ));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A Core message as `CoreMessagePersister` wrote it.
fn core_record(bytes: &[u8]) -> CoreMessage {
    match PersistedMessage::decode(bytes).unwrap() {
        PersistedMessage::Core(c) => c.to_message(),
        other => panic!("not a Core record: {other:?}"),
    }
}

/// A Core message with its properties sorted: Artemis keeps them in a
/// `HashMap`, whose order a Core client does not see.
fn normalized(mut c: CoreMessage) -> (CoreMessage, Vec<(String, PropertyValue)>) {
    let mut props: Vec<(String, PropertyValue)> = c
        .properties
        .entries()
        .iter()
        .map(|(k, v)| (k.to_string_lossy(), v.clone()))
        .collect();
    props.sort_by(|a, b| a.0.cmp(&b.0));
    c.properties = Default::default();
    (c, props)
}

#[test]
fn conversion_to_core_matches_artemis() {
    let mut failures = Vec::new();
    for (case, kinds) in fixtures() {
        let (Some(input), Some(java)) = (kinds.get("input"), kinds.get("core")) else {
            continue;
        };
        // Artemis copies an encoded amqp-value from offset 0 of a pooled
        // buffer's array, which is not where the buffer starts: the body
        // is whatever was there.
        if case == "value-int" {
            continue;
        }
        let java = core_record(java);
        let ours = convert::to_core(&received(input), NOW);
        let (mut java, java_props) = normalized(java);
        let (mut ours, our_props) = normalized(ours);
        // A message ID that is not a UUID becomes a random one.
        if !case.starts_with("ids-uuid") {
            assert_eq!(java.user_id.is_some(), ours.user_id.is_some(), "{case}");
            java.user_id = None;
            ours.user_id = None;
        }
        if java != ours || java_props != our_props {
            failures.push(format!(
                "{case}\n  artemis {java:?}\n          {java_props:?}\n  saranga {ours:?}\n          {our_props:?}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn conversion_from_core_matches_artemis() {
    let mut failures = Vec::new();
    let mut count = 0;
    for (case, kinds) in fixtures() {
        let (Some(core), Some(java)) = (kinds.get("core-in"), kinds.get("amqp")) else {
            continue;
        };
        let ours = convert::from_core(&core_record(core), NOW).send_buffer(1);
        failures.extend(diff(&case, "amqp", java, &ours));
        count += 1;
    }
    assert!(count >= 13, "only {count} cases");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn records_reload() {
    for (case, kinds) in fixtures() {
        let Some(record) = kinds.get("persist") else {
            continue;
        };
        let PersistedMessage::Amqp(r) = PersistedMessage::decode(record).unwrap() else {
            panic!("{case}: not an AMQP record");
        };
        let msg = AmqpMessage::from_record(&r, NOW).unwrap();
        assert_eq!(msg.address(), Some(SimpleString::from("addr")), "{case}");
        assert_eq!(msg.record().encode_record(), r.encode_record(), "{case}");
    }
}

trait EncodeRecord {
    fn encode_record(&self) -> Vec<u8>;
}

impl EncodeRecord for saranga_storage::message::AmqpMessageRecord {
    fn encode_record(&self) -> Vec<u8> {
        PersistedMessage::Amqp(self.clone()).encode()
    }
}
