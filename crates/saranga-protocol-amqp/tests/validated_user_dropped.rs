//! With `populate-validated-user` and no validated user (security off), the
//! session drops the user a producer claimed: an AMQP message's `user-id`,
//! `JMSXUserID` and `_AMQ_VALIDATED_USER` go with it.

use saranga_buffer::SimpleString;
use saranga_protocol_amqp::message::AmqpMessage;
use saranga_protocol_amqp::native::AmqpNative;
use saranga_server::message::NativeMessage;

fn str8(s: &str) -> Vec<u8> {
    let mut b = vec![0xa1, s.len() as u8];
    b.extend_from_slice(s.as_bytes());
    b
}

/// A message whose `user-id` and properties name `mallory`.
fn claimed() -> AmqpMessage {
    let mut data = vec![0x00, 0x53, 0x73];
    let fields = [vec![0x40], vec![0xa0, 7], b"mallory".to_vec()].concat();
    data.extend_from_slice(&[0xc0, (fields.len() + 1) as u8, 2]);
    data.extend_from_slice(&fields);
    let entries = [
        str8("JMSXUserID"),
        str8("mallory"),
        str8("_AMQ_VALIDATED_USER"),
        str8("mallory"),
    ]
    .concat();
    data.extend_from_slice(&[0x00, 0x53, 0x74, 0xc1, (entries.len() + 1) as u8, 4]);
    data.extend_from_slice(&entries);
    data.extend_from_slice(&[0x00, 0x53, 0x75, 0xa0, 1, 42]);
    AmqpMessage::new(0, data, 0).unwrap()
}

#[test]
fn a_dropped_user_leaves_no_trace() {
    let native = AmqpNative::new(claimed());
    let before = native.view();
    assert!(
        before
            .properties
            .get(&SimpleString::from("_AMQ_VALIDATED_USER"))
            .is_some()
    );
    let mut after = before.clone();
    after
        .properties
        .remove(&SimpleString::from("_AMQ_VALIDATED_USER"));
    let sent = native.updated(&before, &after);
    for (what, m) in [
        ("view", sent.view()),
        ("core", sent.to_core()),
        (
            "stored",
            saranga_protocol_amqp::native::decoder()(&sent.persisted(), None)
                .unwrap()
                .unwrap()
                .to_core(),
        ),
    ] {
        for key in ["_AMQ_VALIDATED_USER", "JMSXUserID"] {
            assert!(
                m.properties.get(&SimpleString::from(key)).is_none(),
                "{what}: {key} kept"
            );
        }
    }
}
