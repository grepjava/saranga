//! Replays every AMQP connection recorded between Qpid JMS and the real
//! Artemis broker (`tests/golden/*/wire/conn-*` starting with an AMQP
//! header). Every frame must decode, and re-encode to the same bytes.

use std::path::{Path, PathBuf};

use saranga_protocol_amqp::frame::{self, FrameBody};
use saranga_protocol_amqp::messaging::DeliveryState;
use saranga_protocol_amqp::sasl::SaslFrame;
use saranga_protocol_amqp::transport::Performative;

fn connections() -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/golden");
    let mut out = Vec::new();
    for scenario in std::fs::read_dir(root).unwrap().flatten() {
        if let Ok(conns) = std::fs::read_dir(scenario.path().join("wire")) {
            out.extend(conns.flatten().map(|c| c.path()).filter(|c| {
                std::fs::read(c.join("c2s.bin")).is_ok_and(|b| b.starts_with(b"AMQP"))
            }));
        }
    }
    out.sort();
    out
}

/// Re-encodes one frame from its decoded form.
fn reencode(f: &frame::Frame, original: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    match &f.body {
        FrameBody::Amqp(p, payload) => frame::encode_amqp(&mut out, f.channel, p, payload),
        FrameBody::Sasl(s) => frame::encode_sasl(&mut out, s),
        FrameBody::Empty => out.extend(&original[..8]),
    }
    out
}

/// Every frame of one direction; returns the performatives seen.
fn replay(path: &Path, file: &str) -> Vec<String> {
    let data = std::fs::read(path.join(file)).unwrap();
    let mut buf = data.as_slice();
    let mut seen = Vec::new();
    while !buf.is_empty() {
        if buf.starts_with(b"AMQP") {
            buf = &buf[8..];
            seen.push("header".into());
            continue;
        }
        let size = frame::frame_size(buf).unwrap() as usize;
        let original = &buf[..size];
        let f = frame::decode(original).unwrap_or_else(|e| {
            panic!("{}/{file} @{}: {e}", path.display(), data.len() - buf.len())
        });
        let mut again = reencode(&f, original);
        // An Accepted outcome Artemis made with `new Accepted()`.
        if again != original
            && let FrameBody::Amqp(Performative::Disposition(d), payload) = &f.body
            && d.state == Some(DeliveryState::Accepted)
        {
            let mut d = d.clone();
            d.state = Some(DeliveryState::AcceptedNew);
            let fresh = frame::Frame {
                channel: f.channel,
                body: FrameBody::Amqp(Performative::Disposition(d), payload.clone()),
            };
            again = reencode(&fresh, original);
        }
        assert_eq!(
            again,
            original,
            "{}/{file} @{}: {:?}",
            path.display(),
            data.len() - buf.len(),
            f.body
        );
        seen.push(match &f.body {
            FrameBody::Amqp(p, _) => p.name().into(),
            FrameBody::Sasl(SaslFrame::Mechanisms(_)) => "sasl-mechanisms".into(),
            FrameBody::Sasl(_) => "sasl".into(),
            FrameBody::Empty => "empty".into(),
        });
        buf = &buf[size..];
    }
    seen
}

#[test]
fn qpid_jms_and_artemis_frames_round_trip() {
    let conns = connections();
    assert!(
        !conns.is_empty(),
        "no AMQP connections in the golden corpus"
    );
    let mut kinds = std::collections::BTreeSet::new();
    for c in &conns {
        kinds.extend(replay(c, "c2s.bin"));
        kinds.extend(replay(c, "s2c.bin"));
    }
    // Qpid JMS closes a connection without ending its sessions.
    for k in [
        "open",
        "begin",
        "attach",
        "flow",
        "transfer",
        "disposition",
        "detach",
        "close",
    ] {
        assert!(kinds.contains(k), "no {k} frame seen: {kinds:?}");
    }
}
