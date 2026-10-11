//! Message conversion against Artemis's OpenWireMessageConverter
//! (tests/golden/openwire/messages.txt, from
//! tests/golden-gen/openwire/MessageFixtures.java).

use std::collections::BTreeMap;

use saranga_buffer::PropertyValue;
use saranga_message::CoreMessage;
use saranga_protocol_openwire::codec::dump;
use saranga_protocol_openwire::commands::{ConsumerId, Destination, Message};
use saranga_protocol_openwire::convert::{self, DispatchTarget};
use saranga_protocol_openwire::{DataStructure, WireFormat};
use saranga_storage::RecordCodec;
use saranga_storage::message::PersistedMessage;

const NOW: i64 = 1_700_000_000_000;
const NODE: &str = "11223344-5566-11ee-8001-020304050607";

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn fixtures() -> Vec<(String, BTreeMap<String, Vec<u8>>)> {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/golden/openwire/messages.txt"
    ))
    .unwrap();
    let mut cases: Vec<(String, BTreeMap<String, Vec<u8>>)> = Vec::new();
    for line in text.lines() {
        let mut parts = line.splitn(3, ' ');
        let (case, kind, hex) = (
            parts.next().unwrap(),
            parts.next().unwrap(),
            parts.next().unwrap_or(""),
        );
        if cases.last().is_none_or(|(c, _)| c != case) {
            cases.push((case.to_owned(), BTreeMap::new()));
        }
        cases
            .last_mut()
            .unwrap()
            .1
            .insert(kind.to_owned(), unhex(hex));
    }
    cases
}

fn format() -> WireFormat {
    let mut wf = WireFormat::new(12);
    wf.tight_encoding_enabled = true;
    wf
}

fn core_record(bytes: &[u8]) -> CoreMessage {
    match PersistedMessage::decode(bytes).unwrap() {
        PersistedMessage::Core(c) => c.to_message(),
        other => panic!("not a Core record: {other:?}"),
    }
}

/// The message without its properties, and its properties.
fn normalized(mut c: CoreMessage) -> (CoreMessage, Vec<(String, PropertyValue)>) {
    let props: Vec<(String, PropertyValue)> = c
        .properties
        .entries()
        .iter()
        .map(|(k, v)| (k.to_string_lossy(), v.clone()))
        .collect();
    // In the order Artemis encodes them (a new HashMap's), which
    // Saranga keeps.
    c.properties = Default::default();
    (c, props)
}

#[test]
fn inbound_matches_artemis() {
    let mut failures = Vec::new();
    let mut count = 0;
    for (case, kinds) in fixtures() {
        let Some(input) = kinds.get("input") else {
            continue;
        };
        let Some(DataStructure::Message(msg)) = format().unmarshal(input).unwrap() else {
            panic!("{case}: not a message");
        };
        count += 1;
        let ours = convert::inbound(&msg, &mut format(), NOW);
        if let Some(e) = kinds.get("error") {
            let java = String::from_utf8_lossy(e);
            match ours {
                Err(err) if java.ends_with(&err.0) => {}
                other => failures.push(format!("{case}: want {java}, got {other:?}")),
            }
            continue;
        }
        let ours = match ours {
            Ok(c) => c,
            Err(e) => {
                failures.push(format!("{case}: {e}"));
                continue;
            }
        };
        let (mut java, mut java_props) = normalized(core_record(&kinds["core"]));
        let (mut ours, mut our_props) = normalized(ours);
        assert!(java.user_id.is_some() && ours.user_id.is_some(), "{case}");
        java.user_id = None;
        ours.user_id = None;
        java.message_id = 0;
        // Scheduled: now + delay, at two different nows.
        for props in [&mut java_props, &mut our_props] {
            for (k, v) in props.iter_mut() {
                if k == "_AMQ_SCHED_DELIVERY" {
                    *v = PropertyValue::Long(0);
                }
            }
        }
        if java != ours || java_props != our_props {
            failures.push(format!(
                "{case}\n  artemis {java:?}\n          {java_props:?}\n  saranga {ours:?}\n          {our_props:?}"
            ));
        }
    }
    assert!(count > 15, "{count}");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Inflates what there is, finished or not (Artemis sends some bodies
/// sync-flushed but never finished).
fn inflate(b: &[u8]) -> Vec<u8> {
    let mut d = flate2::Decompress::new(true);
    let mut out = Vec::with_capacity(b.len() * 8 + 1024);
    loop {
        let before = d.total_out();
        let consumed = d.total_in() as usize;
        match d.decompress_vec(&b[consumed..], &mut out, flate2::FlushDecompress::Sync) {
            Ok(flate2::Status::StreamEnd) => break,
            Ok(_) if d.total_out() == before && d.total_in() as usize == consumed => break,
            Ok(_) => out.reserve(4096),
            Err(e) => panic!("{e}"),
        }
    }
    out
}

/// The dispatch with a compressed body inflated: Java's zlib and Saranga's
/// compress the same bytes differently.
fn comparable(ds: DataStructure) -> String {
    let DataStructure::MessageDispatch(mut md) = ds else {
        panic!("not a dispatch");
    };
    if let Some(m) = md.message.as_deref_mut()
        && m.compressed
    {
        uncompress(m);
    }
    dump(&DataStructure::MessageDispatch(md), 12, false)
}

fn uncompress(m: &mut Message) {
    let Some(c) = m.content.take() else { return };
    m.content = Some(if m.data_type == 24 {
        let mut out = c[..4].to_vec();
        out.extend(inflate(&c[4..]));
        out
    } else {
        inflate(&c)
    });
}

#[test]
fn dispatch_matches_artemis() {
    let mut failures = Vec::new();
    let mut count = 0;
    for (case, kinds) in fixtures() {
        let Some(core_in) = kinds.get("core-in") else {
            continue;
        };
        let params = String::from_utf8(kinds["params"].clone()).unwrap();
        let (delivery_count, destination) = params.split_once(' ').unwrap();
        let destination = Destination::create(destination, 1).unwrap();
        let consumer_id = ConsumerId {
            connection_id: Some("ID:consumer-conn".into()),
            session_id: 3,
            value: 4,
        };
        let target = DispatchTarget {
            consumer_id: &consumer_id,
            destination: &destination,
            server_node_id: NODE,
            decoded_properties: false,
        };
        let core = core_record(core_in);
        let ours = convert::message_dispatch(
            &core,
            delivery_count.parse().unwrap(),
            &target,
            &mut format(),
            55,
        );
        count += 1;
        if let Some(e) = kinds.get("error") {
            let java = String::from_utf8_lossy(e);
            if !matches!(&ours, Err(err) if java.ends_with(&err.0)) {
                failures.push(format!("{case}: want {java}, got {ours:?}"));
            }
            continue;
        }
        let ours = match ours {
            Ok(md) => md,
            Err(e) => {
                failures.push(format!("{case}: {e}"));
                continue;
            }
        };
        let java = &kinds["dispatch"];
        let ours = DataStructure::MessageDispatch(ours);
        let bytes = format().marshal(Some(&ours)).unwrap();
        if &bytes != java {
            let want = comparable(format().unmarshal(java).unwrap().unwrap());
            let got = comparable(ours);
            if want != got {
                let at = want
                    .bytes()
                    .zip(got.bytes())
                    .position(|(a, b)| a != b)
                    .unwrap_or(want.len().min(got.len()));
                let from = at.saturating_sub(60);
                failures.push(format!(
                    "{case}\n  artemis ...{}\n  saranga ...{}",
                    &want[from..(at + 300).min(want.len())],
                    &got[from..(at + 300).min(got.len())]
                ));
            }
        }
    }
    assert!(count > 25, "{count}");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
