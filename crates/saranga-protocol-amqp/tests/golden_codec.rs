//! Values encoded by Proton-J 0.35.0, the encoder Artemis uses
//! (`tests/golden/amqp/codec.txt`, written by
//! `tests/golden-gen/amqp/CodecFixtures.java`). Each must decode, as its own
//! type where it is one, and re-encode to the same bytes.

use std::path::Path;

use saranga_protocol_amqp::codec::{self, Decoder};
use saranga_protocol_amqp::composite::Composite;
use saranga_protocol_amqp::messaging::{
    Header, Modified, Properties, Received, Rejected, Source, Target,
};
use saranga_protocol_amqp::sasl::SaslFrame;
use saranga_protocol_amqp::transaction::{Declare, Declared, Discharge, TransactionalState};
use saranga_protocol_amqp::transport::Performative;
use saranga_protocol_amqp::{Descriptor, Value};

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn reencode<T: Composite>(bytes: &[u8]) -> Vec<u8> {
    T::decode(bytes).unwrap().to_bytes()
}

/// Re-encodes through the type the descriptor names, or as a plain value.
fn roundtrip(bytes: &[u8]) -> Vec<u8> {
    let value = codec::from_slice(bytes).unwrap();
    let code = match &value {
        Value::Described(d, _) => match **d {
            Descriptor::Code(c) => Some(c),
            Descriptor::Name(_) => None,
        },
        _ => None,
    };
    let mut out = Vec::new();
    match code {
        Some(0x10..=0x18) => Performative::from_value(value).unwrap().encode(&mut out),
        Some(0x40..=0x44) => SaslFrame::from_value(value).unwrap().encode(&mut out),
        Some(0x23) => out = reencode::<Received>(bytes),
        Some(0x25) => out = reencode::<Rejected>(bytes),
        Some(0x27) => out = reencode::<Modified>(bytes),
        Some(0x28) => out = reencode::<Source>(bytes),
        Some(0x29) => out = reencode::<Target>(bytes),
        Some(0x31) => out = reencode::<Declare>(bytes),
        Some(0x32) => out = reencode::<Discharge>(bytes),
        Some(0x33) => out = reencode::<Declared>(bytes),
        Some(0x34) => out = reencode::<TransactionalState>(bytes),
        Some(0x70) => out = reencode::<Header>(bytes),
        Some(0x73) => out = reencode::<Properties>(bytes),
        _ => codec::encode(&value, &mut out),
    }
    out
}

#[test]
fn proton_j_encodings_round_trip() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/golden/amqp/codec.txt");
    let text = std::fs::read_to_string(path).unwrap();
    let mut failures = Vec::new();
    let mut count = 0;
    for line in text.lines().filter(|l| !l.is_empty()) {
        let (name, h) = line.split_once(' ').unwrap();
        // Proton-J writes a disposition with a new Accepted instance as
        // list32 and with the shared one as list8: an object identity the
        // bytes cannot carry. Artemis only uses the shared one.
        if name == "disposition-new-accepted" {
            continue;
        }
        let bytes = hex(h);
        let out = std::panic::catch_unwind(|| roundtrip(&bytes));
        match out {
            Ok(out) if out == bytes => {}
            Ok(out) => failures.push(format!(
                "{name}\n  proton-j {h}\n  saranga  {}",
                out.iter().map(|b| format!("{b:02x}")).collect::<String>()
            )),
            Err(_) => failures.push(format!("{name}: panicked")),
        }
        // Skipping a value covers exactly its bytes.
        assert_eq!(
            Decoder::new(&bytes).skip().unwrap().len(),
            bytes.len(),
            "{name}"
        );
        count += 1;
    }
    assert!(count > 150, "only {count} fixtures");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
