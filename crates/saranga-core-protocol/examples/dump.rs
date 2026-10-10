//! Prints every packet of a captured connection, both directions interleaved
//! by file order (not by time).
//!   cargo run -p saranga-core-protocol --example dump -- tests/golden/core-basic/wire/conn-0

use saranga_core_protocol::{Context, Direction, HANDSHAKE, Packet, read_frame};

fn main() {
    let dir = std::path::PathBuf::from(std::env::args().nth(1).expect("connection dir"));
    for (file, direction) in [
        ("c2s.bin", Direction::ToServer),
        ("s2c.bin", Direction::ToClient),
    ] {
        println!("=== {file}");
        let data = std::fs::read(dir.join(file)).unwrap();
        let mut buf = data.strip_prefix(HANDSHAKE).unwrap_or(&data);
        let mut ctx = Context::new(direction, 0);
        while let Some((frame, used)) = read_frame(buf).unwrap() {
            let packet = Packet::decode(&frame, &ctx).unwrap();
            match &packet {
                Packet::CreateSessionMessage(p) => ctx.version = p.version,
                Packet::CreateSessionMessageV2(p) => ctx.version = p.version,
                Packet::CreateSessionResponseMessage(p) => ctx.version = p.server_version,
                _ => {}
            }
            let text = format!("{packet:?}");
            let text = if text.len()
                > std::env::var("DUMP_WIDTH")
                    .ok()
                    .and_then(|w| w.parse().ok())
                    .unwrap_or(300)
            {
                format!("{}…", &text[..text.floor_char_boundary(300)])
            } else {
                text
            };
            println!("ch{:<3} {text}", frame.channel_id);
            buf = &buf[used..];
        }
    }
}
