//! A large AMQP message keeps only its headers in memory: its body lives in a
//! file and the whole encoding is rebuilt from the file's bytes for delivery.
//! The slim form routes and filters like the whole message, and the rebuilt
//! form is byte-for-byte the original.

use saranga_buffer::SimpleString;
use saranga_protocol_amqp::message::AmqpMessage;

const NOW: i64 = 1_750_000_000_000;

/// An AMQP message that is just a Data section body, as a client sends it:
/// `0x00 0x53 0x75` (described, Data descriptor) then a `vbin32` of the body.
fn data_message(body: &[u8]) -> Vec<u8> {
    let mut v = vec![0x00, 0x53, 0x75, 0xB0];
    v.extend_from_slice(&(body.len() as u32).to_be_bytes());
    v.extend_from_slice(body);
    v
}

fn large_message(body: &[u8]) -> AmqpMessage {
    let mut m = AmqpMessage::new(0, data_message(body), NOW).unwrap();
    m.set_message_id(100);
    m.set_address(SimpleString::from("addr"));
    m.make_large();
    m
}

/// A run of `len` equal bytes is in `haystack`.
fn has_run(haystack: &[u8], byte: u8, len: usize) -> bool {
    haystack.windows(len).any(|w| w.iter().all(|&b| b == byte))
}

#[test]
fn slim_keeps_headers_and_rebuilds_the_whole_message() {
    let body = vec![0xABu8; 300_000];
    let full = large_message(&body);
    let full_wire = full.send_buffer(1);
    // What `server_send` writes to the large-message file: the message's own
    // encoding.
    let file_bytes = full.data().to_vec();
    assert!(
        has_run(&full_wire, 0xAB, body.len()),
        "the whole message carries its body"
    );

    // Slim: only the headers are held in memory, not the body.
    let slim = AmqpMessage::from_large_record(&full.large_record(), Vec::new(), NOW).unwrap();
    assert!(
        slim.data().len() < 4096,
        "the slim form keeps only headers, not the {}-byte body (data = {} bytes)",
        body.len(),
        slim.data().len()
    );
    // Routing and filtering read the slim form's fields, which survive.
    assert_eq!(slim.message_id(), 100);
    assert_eq!(
        slim.address().map(|a| a.to_string_lossy()),
        Some("addr".to_owned())
    );
    assert!(
        !has_run(&slim.send_buffer(1), 0xAB, body.len()),
        "the slim form does not carry the body"
    );

    // Reconstituted from the file's bytes: byte-for-byte the whole message.
    let whole = AmqpMessage::from_large_record(&slim.large_record(), file_bytes, NOW).unwrap();
    assert_eq!(
        whole.send_buffer(1),
        full_wire,
        "the reconstituted wire form matches the original"
    );
}
