//! Replays every Core connection recorded from the real Artemis broker
//! (`tests/golden/*/wire/conn-*`). Every frame must decode and re-encode to
//! the identical bytes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use saranga_core_protocol::{Context, Direction, HANDSHAKE, Packet, read_frame};

fn golden_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/golden")
}

fn connections() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for scenario in std::fs::read_dir(golden_root()).unwrap().flatten() {
        let wire = scenario.path().join("wire");
        if let Ok(conns) = std::fs::read_dir(&wire) {
            // Connections of other protocols (AMQP starts with its own
            // header) have tests of their own.
            out.extend(conns.flatten().map(|c| c.path()).filter(|c| {
                std::fs::read(c.join("c2s.bin")).is_ok_and(|b| !b.starts_with(b"AMQP"))
            }));
        }
    }
    out.sort();
    assert!(!out.is_empty(), "no golden connections found");
    out
}

/// Decodes and re-encodes every frame of one direction of a connection.
/// Returns how many frames of each still-unported type were seen.
fn replay(path: &Path, direction: Direction) -> BTreeMap<i8, usize> {
    let file = match direction {
        Direction::ToServer => "c2s.bin",
        Direction::ToClient => "s2c.bin",
    };
    let data = std::fs::read(path.join(file)).unwrap();
    let mut buf = data.as_slice();
    if direction == Direction::ToServer {
        assert!(
            buf.starts_with(HANDSHAKE),
            "{}: missing handshake",
            path.display()
        );
        buf = &buf[HANDSHAKE.len()..];
    }

    let mut ctx = Context::new(direction, 0);
    let mut unknown = BTreeMap::new();
    let mut offset = data.len() - buf.len();
    while !buf.is_empty() {
        let (frame, used) = read_frame(buf)
            .unwrap_or_else(|e| panic!("{}/{file} @{offset}: {e}", path.display()))
            .unwrap_or_else(|| panic!("{}/{file} @{offset}: truncated frame", path.display()));
        let original = &buf[..used];
        let packet = Packet::decode(&frame, &ctx).unwrap_or_else(|e| {
            panic!(
                "{}/{file} @{offset}: type {} decode failed: {e}",
                path.display(),
                frame.packet_type
            )
        });
        let encoded = packet.encode_frame(frame.channel_id, &ctx);
        assert_eq!(
            encoded,
            original,
            "{}/{file} @{offset}: {} did not re-encode identically",
            path.display(),
            packet.name()
        );

        // Track the negotiated version the way each side does.
        match &packet {
            Packet::CreateSessionMessage(p) => ctx.version = p.version,
            Packet::CreateSessionMessageV2(p) => ctx.version = p.version,
            Packet::CreateSessionResponseMessage(p) => ctx.version = p.server_version,
            Packet::Unknown { packet_type, .. } => *unknown.entry(*packet_type).or_default() += 1,
            _ => {}
        }
        offset += used;
        buf = &buf[used..];
    }
    unknown
}

#[test]
fn golden_frames_roundtrip() {
    let mut unknown: BTreeMap<(Direction, i8), usize> = BTreeMap::new();
    for conn in connections() {
        for dir in [Direction::ToServer, Direction::ToClient] {
            for (ty, n) in replay(&conn, dir) {
                *unknown.entry((dir, ty)).or_default() += n;
            }
        }
    }
    // Every packet in real traffic must decode to a typed struct, not `Unknown`.
    assert!(
        unknown.is_empty(),
        "packets decoded as Unknown, (direction, type) => count: {unknown:?}"
    );
}
