//! A slim (large) AMQP message keeps only its headers in memory, its body in a
//! file. Two things must still hold: its `encode_size` does not underflow when
//! the headers no longer cover a delivery-annotation length the record still
//! carries, and a section changed after it was spilled is rebuilt over the
//! file's body when it is delivered rather than being dropped.

use saranga_buffer::SimpleString;
use saranga_protocol_amqp::message::AmqpMessage;
use saranga_protocol_amqp::{Descriptor, Symbol, Value, codec};

const NOW: i64 = 1_750_000_000_000;

/// An AMQP Data-section body, as a client sends it.
fn data_message(body: &[u8]) -> Vec<u8> {
    let mut v = vec![0x00, 0x53, 0x75, 0xB0];
    v.extend_from_slice(&(body.len() as u32).to_be_bytes());
    v.extend_from_slice(body);
    v
}

/// A run of `len` equal bytes is in `haystack`.
fn has_run(haystack: &[u8], byte: u8, len: usize) -> bool {
    haystack.windows(len).any(|w| w.iter().all(|&b| b == byte))
}

fn annotation_text(m: &AmqpMessage, name: &str) -> Option<String> {
    m.annotation(name)
        .and_then(|v| v.as_str().map(str::to_owned))
}

/// A large message that carried delivery annotations is slimmed to its headers
/// with an empty body; `encode_size` encodes as 0 (as `AMQPLargeMessage` does)
/// rather than underflowing `data.len() - encoded_delivery_annotations_size`.
#[test]
fn encode_size_of_a_slim_message_with_delivery_annotations_is_zero() {
    let body = vec![0xABu8; 300_000];
    // Delivery annotations (descriptor 0x71), then the body.
    let da = Value::described(
        Descriptor::Code(0x71),
        Value::Map(vec![(
            Value::Symbol(Symbol::from("x-opt-hop")),
            Value::String("edge".to_owned()),
        )]),
    );
    let mut wire = Vec::new();
    codec::encode(&da, &mut wire);
    wire.extend_from_slice(&data_message(&body));

    let mut full = AmqpMessage::new(0, wire, NOW).unwrap();
    full.set_message_id(100);
    full.make_large();

    // The slim form holds no body but its record still carries the
    // delivery-annotation length.
    let slim = AmqpMessage::from_large_record(&full.large_record(), Vec::new(), NOW).unwrap();
    assert!(slim.data().is_empty(), "the slim form holds no body");
    // Without clamping, `0 - encoded_delivery_annotations_size` underflows and
    // panics here in a debug build.
    assert_eq!(
        slim.encode_size(),
        0,
        "a large message encodes as 0, like AMQPLargeMessage"
    );
}

/// A message spilled to a file as it arrived, then given a new annotation
/// before delivery (a federation hop, a broker property), delivers with that
/// annotation: the spill keeps the body offset the delivery encoder needs to
/// rebuild the changed sections over the file's body.
#[test]
fn a_section_changed_after_a_spill_survives_delivery() {
    let body = vec![0xABu8; 300_000];
    // A message with a header section (a message annotation) before its body,
    // so the headers the spill keeps are non-empty.
    let mut full = AmqpMessage::new(0, data_message(&body), NOW).unwrap();
    full.set_annotation("first", Value::String("one".to_owned()));
    full.reencode(NOW);
    let file_bytes = full.data().to_vec();
    let body_start = full.body_position().expect("a body section");

    // Spill: the receiver keeps only the headers in memory (the file holds the
    // whole encoding) and makes the message large.
    let mut slim = AmqpMessage::new(0, file_bytes[..body_start].to_vec(), NOW).unwrap();
    slim.set_message_id(100);
    slim.set_address(SimpleString::from("addr"));
    slim.make_large();

    // Reconstituted for delivery with the file's whole encoding, then a broker
    // adds an annotation and re-encodes.
    let mut whole = AmqpMessage::from_large_record(&slim.large_record(), file_bytes, NOW).unwrap();
    whole.set_annotation("hop", Value::String("added".to_owned()));
    whole.reencode(NOW);

    let delivered = AmqpMessage::new(0, whole.send_buffer(1), NOW).unwrap();
    assert_eq!(
        annotation_text(&delivered, "hop").as_deref(),
        Some("added"),
        "the annotation added after the spill reaches delivery"
    );
    assert_eq!(
        annotation_text(&delivered, "first").as_deref(),
        Some("one"),
        "the original annotation is still there"
    );
    assert!(
        has_run(delivered.data(), 0xAB, body.len()),
        "the whole body is delivered"
    );
}
