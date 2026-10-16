//! Writes each fuzz target's seeds into `fuzz/corpus/<target>/` (or the
//! directory given), cut from the golden corpus: Core and AMQP frames from
//! the recorded connections, the Core packet fixtures, the messages and
//! properties they carry, OpenWire frames for every version and format,
//! journal and page files and their records from the data directories, and
//! the repository's `broker.xml` files. MQTT, STOMP, WebSocket and filter
//! seeds, which the golden corpus has none of, are written here by hand.
//! Each seed is named by a hash of its bytes, so running it twice changes
//! nothing.
//!
//!     cargo run -p saranga-fuzz --bin fuzz-seeds [DIR]

use std::collections::{BTreeMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};

use saranga_buffer::Writer;
use saranga_core_protocol::{Context, Direction, HANDSHAKE, Packet, read_frame};
use saranga_journal::control::ControlFile;
use saranga_journal::format::{self, Record};
use saranga_protocol_amqp::frame::{self, FrameBody};
use saranga_protocol_amqp::transport::Performative;

/// The largest seed: a journal or page file is cut at a record before this.
/// Larger seeds slow every run of a target down for little more coverage.
const MAX_SEED: usize = 32 * 1024;

/// The largest run of OpenWire frames written with the cache on.
const MAX_CACHED_RUN: usize = 8 * 1024;

struct Seeds {
    dir: PathBuf,
    written: BTreeMap<&'static str, usize>,
    seen: HashSet<(&'static str, u64)>,
}

impl Seeds {
    fn add(&mut self, target: &'static str, bytes: &[u8]) {
        // FNV-1a.
        let hash = bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
            (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
        });
        if !self.seen.insert((target, hash)) {
            return;
        }
        let dir = self.dir.join(target);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("seed-{hash:016x}")), bytes).unwrap();
        *self.written.entry(target).or_default() += 1;
    }

    fn add_with(&mut self, target: &'static str, first: u8, rest: &[u8]) {
        let mut v = Vec::with_capacity(rest.len() + 1);
        v.push(first);
        v.extend_from_slice(rest);
        self.add(target, &v);
    }
}

fn golden() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/golden")
}

fn sorted(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|d| d.flatten().map(|e| e.path()).collect())
        .unwrap_or_default();
    v.sort();
    v
}

fn main() {
    let dir = std::env::args().nth(1).map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus"),
        PathBuf::from,
    );
    let mut seeds = Seeds {
        dir,
        written: BTreeMap::new(),
        seen: HashSet::new(),
    };
    for scenario in sorted(&golden()) {
        for conn in sorted(&scenario.join("wire")) {
            let c2s = std::fs::read(conn.join("c2s.bin")).unwrap_or_default();
            let s2c = std::fs::read(conn.join("s2c.bin")).unwrap_or_default();
            if c2s.starts_with(b"AMQP") {
                amqp(&mut seeds, &c2s);
                amqp(&mut seeds, &s2c);
            } else if c2s.starts_with(HANDSHAKE) {
                core(&mut seeds, &c2s[HANDSHAKE.len()..], Direction::ToServer);
                core(&mut seeds, &s2c, Direction::ToClient);
            }
        }
        if let Ok(gz) = std::fs::File::open(scenario.join("data.tar.gz")) {
            data_dir(&mut seeds, gz);
        }
    }
    packet_fixtures(&mut seeds);
    openwire(&mut seeds);
    by_hand(&mut seeds);
    config(&mut seeds);
    for (target, n) in &seeds.written {
        println!("{target}: {n}");
    }
}

/// Every frame of one direction of a Core connection, in the context it was
/// sent in, and the messages they carry.
fn core(seeds: &mut Seeds, mut buf: &[u8], direction: Direction) {
    let mut ctx = Context::new(direction, 0);
    let mut per_type: BTreeMap<(i8, i32), usize> = BTreeMap::new();
    while let Ok(Some((frame, used))) = read_frame(buf) {
        let original = &buf[..used];
        buf = &buf[used..];
        let n = per_type
            .entry((frame.packet_type, ctx.version))
            .or_default();
        *n += 1;
        if *n <= 8 {
            seeds.add_with("core_packet", saranga_fuzz::core_selector(&ctx), original);
        }
        let Ok(packet) = Packet::decode(&frame, &ctx) else {
            continue;
        };
        match &packet {
            Packet::CreateSessionMessage(p) => ctx.version = p.version,
            Packet::CreateSessionMessageV2(p) => ctx.version = p.version,
            Packet::CreateSessionResponseMessage(p) => ctx.version = p.server_version,
            Packet::SessionSendMessage(p) if *n <= 8 => message(seeds, &p.message),
            Packet::SessionReceiveMessage(p) if *n <= 8 => message(seeds, &p.message),
            _ => {}
        }
    }
}

fn message(seeds: &mut Seeds, m: &saranga_message::CoreMessage) {
    let mut w = Writer::new();
    m.encode(&mut w);
    seeds.add_with("core_message", 0, &w.into_vec());
    let mut w = Writer::new();
    m.encode_headers_and_properties(&mut w);
    seeds.add_with("core_message", 1, &w.into_vec());
    let mut w = Writer::new();
    m.properties.encode(&mut w);
    seeds.add_with("buffer", 0, &w.into_vec());
    if let Some(address) = &m.address {
        let mut w = Writer::new();
        saranga_buffer::Codec::write(address, &mut w);
        seeds.add_with("buffer", 1, &w.into_vec());
    }
}

/// `tests/golden/packets/<group>/<Name>[~case]@<s|c>-<version>.bin`.
fn packet_fixtures(seeds: &mut Seeds) {
    for group in sorted(&golden().join("packets")) {
        for path in sorted(&group) {
            let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
            let Some((_, ctx)) = stem.split_once('@') else {
                continue;
            };
            let Some((dir, version)) = ctx.split_once('-') else {
                continue;
            };
            let direction = if dir == "c" {
                Direction::ToClient
            } else {
                Direction::ToServer
            };
            let ctx = Context::new(direction, version.parse().unwrap_or(0));
            let bytes = std::fs::read(&path).unwrap();
            seeds.add_with("core_packet", saranga_fuzz::core_selector(&ctx), &bytes);
        }
    }
}

/// Every AMQP frame of one direction (a few of each performative), its
/// performative alone, and the payloads of its transfers.
fn amqp(seeds: &mut Seeds, data: &[u8]) {
    let mut buf = data;
    let mut per_kind: BTreeMap<String, usize> = BTreeMap::new();
    while !buf.is_empty() {
        if buf.starts_with(b"AMQP") {
            seeds.add("amqp_frame", &buf[..8.min(buf.len())]);
            buf = &buf[8.min(buf.len())..];
            continue;
        }
        let Some(size) = frame::frame_size(buf) else {
            return;
        };
        let size = size as usize;
        if size < 8 || size > buf.len() {
            return;
        }
        let original = &buf[..size];
        buf = &buf[size..];
        let Ok(f) = frame::decode(original) else {
            continue;
        };
        let kind = match &f.body {
            FrameBody::Amqp(p, _) => p.name().to_owned(),
            FrameBody::Sasl(_) => "sasl".to_owned(),
            FrameBody::Empty => "empty".to_owned(),
        };
        let n = per_kind.entry(kind).or_default();
        *n += 1;
        if *n > 12 {
            continue;
        }
        seeds.add("amqp_frame", original);
        match &f.body {
            FrameBody::Amqp(p, payload) => {
                let mut v = Vec::new();
                p.encode(&mut v);
                seeds.add("amqp_value", &v);
                if matches!(p, Performative::Transfer(_)) && !payload.is_empty() {
                    seeds.add("amqp_message", payload);
                }
            }
            FrameBody::Sasl(s) => {
                let mut v = Vec::new();
                s.encode(&mut v);
                seeds.add("amqp_value", &v);
            }
            FrameBody::Empty => {}
        }
    }
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

/// `tests/golden/openwire/codec/v<N>.txt.gz`: every frame, under the first
/// byte naming its version and format. A frame written with the cache on
/// may need the frames before it, so those runs go in as one seed each.
fn openwire(seeds: &mut Seeds) {
    let dir = golden().join("openwire/codec");
    for version in 1..=12u32 {
        let Ok(gz) = std::fs::File::open(dir.join(format!("v{version}.txt.gz"))) else {
            continue;
        };
        let mut text = String::new();
        flate2::read::GzDecoder::new(gz)
            .read_to_string(&mut text)
            .unwrap();
        let mut selector = 0u8;
        let mut cached: Vec<u8> = Vec::new();
        let flush = |seeds: &mut Seeds, selector: u8, cached: &mut Vec<u8>| {
            if !cached.is_empty() {
                seeds.add_with("openwire", selector, cached);
                cached.clear();
            }
        };
        for line in text.lines() {
            if let Some(cfg) = line.strip_prefix("config ") {
                flush(seeds, selector, &mut cached);
                let f: Vec<bool> = cfg.split(' ').map(|x| x == "true").collect();
                selector = (version - 1) as u8
                    | u8::from(f[0]) << 4
                    | u8::from(f[1]) << 5
                    | u8::from(f[2]) << 6;
            } else if let Some(hex) = line.strip_prefix("frame ") {
                let frame = unhex(hex);
                if selector & 0x20 != 0 {
                    if cached.len() + frame.len() > MAX_CACHED_RUN {
                        flush(seeds, selector, &mut cached);
                    }
                    cached.extend_from_slice(&frame);
                } else {
                    seeds.add_with("openwire", selector, &frame);
                }
            }
        }
        flush(seeds, selector, &mut cached);
    }
}

/// The journal, bindings and page files of a golden data directory.
fn data_dir(seeds: &mut Seeds, gz: std::fs::File) {
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(gz));
    let mut per_type: BTreeMap<(u8, u8), usize> = BTreeMap::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        let kind = if path.ends_with(".bindings") {
            0u8
        } else if path.ends_with(".amq") {
            1
        } else if path.ends_with(".page") {
            2
        } else {
            continue;
        };
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).unwrap();
        if kind == 2 {
            page(seeds, &bytes);
        } else {
            journal(seeds, kind, &bytes, &mut per_type);
        }
    }
}

fn journal(seeds: &mut Seeds, kind: u8, file: &[u8], per_type: &mut BTreeMap<(u8, u8), usize>) {
    if file.len() < format::SIZE_HEADER {
        return;
    }
    let version = i32::from_be_bytes(file[0..4].try_into().unwrap());
    let file_id = i64::from_be_bytes(file[8..16].try_into().unwrap()) as i32;
    let Some(scan) = format::scan(file, file_id, version, false) else {
        return;
    };
    if scan.records.is_empty() {
        return;
    }
    // The file up to its last record, or a record before MAX_SEED.
    let end = scan
        .records
        .iter()
        .map(|r| r.position)
        .chain([scan.last_data_pos])
        .filter(|&p| p <= MAX_SEED)
        .max()
        .unwrap_or(format::SIZE_HEADER);
    seeds.add_with("journal", 0, &file[..end]);
    for scanned in &scan.records {
        if let Record::Add {
            user_type, data, ..
        }
        | Record::Update {
            user_type, data, ..
        }
        | Record::AddTx {
            user_type, data, ..
        }
        | Record::UpdateTx {
            user_type, data, ..
        } = &scanned.record
        {
            let n = per_type.entry((kind, *user_type)).or_default();
            *n += 1;
            if *n <= 6 {
                let mut v = vec![kind, *user_type];
                v.extend_from_slice(data);
                seeds.add("storage_record", &v);
            }
        }
    }
}

fn page(seeds: &mut Seeds, file: &[u8]) {
    let read = saranga_paging::read_page(file);
    let mut seed = Vec::new();
    for (i, m) in read.messages.iter().enumerate() {
        let record = m.encode_record();
        if seed.len() + record.len() > MAX_SEED {
            break;
        }
        seed.extend_from_slice(&record);
        if i < 4 {
            seeds.add("page", &record);
        }
    }
    seeds.add("page", &seed);
}

/// A masked client frame.
fn ws_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let mask = [0x37, 0xfa, 0x21, 0x3d];
    let mut v = vec![0x80 | opcode];
    if payload.len() < 126 {
        v.push(0x80 | payload.len() as u8);
    } else {
        v.push(0x80 | 126);
        v.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    }
    v.extend_from_slice(&mask);
    v.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    v
}

/// The protocols and texts the golden corpus has nothing of.
fn by_hand(seeds: &mut Seeds) {
    // MQTT: 3.1.1 and 5 sessions.
    let mqtt311: &[&[u8]] = &[
        b"\x10\x0c\x00\x04MQTT\x04\x02\x00\x3c\x00\x00",
        b"\x10\x1a\x00\x04MQTT\x04\xc6\x00\x3c\x00\x01c\x00\x03w/t\x00\x01x\x00\x01u\x00\x01p",
        b"\x30\x07\x00\x03a/bhi",
        b"\x32\x09\x00\x03a/b\x00\x01hi",
        b"\x34\x09\x00\x03a/b\x00\x02hi",
        b"\x40\x02\x00\x01",
        b"\x50\x02\x00\x02",
        b"\x62\x02\x00\x02",
        b"\x70\x02\x00\x02",
        b"\x82\x08\x00\x01\x00\x03a/#\x01",
        b"\xa2\x07\x00\x02\x00\x03a/+",
        b"\xc0\x00",
        b"\xe0\x00",
    ];
    let mqtt5: &[&[u8]] = &[
        b"\x10\x10\x00\x04MQTT\x05\x02\x00\x3c\x05\x11\x00\x00\x00\x0a\x00\x00",
        b"\x30\x0b\x00\x03a/b\x04\x01\x01\x23\x00hi"[..13].as_ref(),
        b"\x32\x0a\x00\x03a/b\x00\x01\x00hi",
        b"\x82\x09\x00\x01\x00\x00\x03a/b\x01",
        b"\xa2\x08\x00\x02\x00\x00\x03a/b",
        b"\x40\x03\x00\x01\x00",
        b"\xe0\x02\x00\x00",
        b"\xf0\x00",
    ];
    for (i, p) in mqtt311.iter().enumerate() {
        seeds.add_with("mqtt", 0, p);
        if i > 0 {
            let mut session = mqtt311[0].to_vec();
            session.extend_from_slice(p);
            seeds.add_with("mqtt", 0, &session);
        }
        seeds.add_with("mqtt", 2, p);
    }
    seeds.add_with("mqtt", 0, &mqtt311.concat());
    for p in mqtt5 {
        seeds.add_with("mqtt", 1, p);
    }
    seeds.add_with("mqtt", 0, &mqtt5.concat());

    // STOMP.
    let stomp: &[&[u8]] = &[
        b"CONNECT\naccept-version:1.0,1.1,1.2\nhost:localhost\nlogin:u\npasscode:p\nheart-beat:1000,1000\n\n\0",
        b"STOMP\naccept-version:1.2\nhost:localhost\n\n\0",
        b"SEND\ndestination:/queue/a\ncontent-type:text/plain\n\nhello\0",
        b"SEND\ndestination:a\ncontent-length:5\nreceipt:r1\n\nhe\0lo\0",
        b"SEND\ndestination:a\\cb\nkey:x\\ny\\\\z\\r\n\nbody\0\n\n",
        b"SUBSCRIBE\r\nid:1\r\ndestination:/topic/t\r\nack:client-individual\r\nselector:color='red'\r\n\r\n\0",
        b"ACK\nid:1\nmessage-id:2\ntransaction:t\n\n\0",
        b"NACK\nid:1\n\n\0",
        b"BEGIN\ntransaction:t\n\n\0COMMIT\ntransaction:t\n\n\0ABORT\ntransaction:t\n\n\0",
        b"UNSUBSCRIBE\nid:1\n\n\0",
        b"\n\n\nDISCONNECT\nreceipt:77\n\n\0",
    ];
    for rules in 0..3 {
        for f in stomp {
            seeds.add_with("stomp", rules, f);
        }
        seeds.add_with("stomp", rules, &stomp.concat());
    }

    // WebSocket: handshakes, then frames.
    for request in [
        "GET /ws HTTP/1.1\r\nHost: localhost:61616\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: v12.stomp, mqtt\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: WebSocket\r\nConnection: keep-alive, Upgrade\r\nSec-WebSocket-Key: AQIDBAUGBwgJCgsMDQ4PEA==\r\nSec-WebSocket-Version: 13\r\n\r\n",
        "POST /x HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbody",
        "OPTIONS * HTTP/1.0\r\n\r\n",
    ] {
        seeds.add_with("websocket", 0, request.as_bytes());
    }
    let frames = [
        ws_frame(0x2, b"ARTEMIS"),
        ws_frame(0x1, b"CONNECT\naccept-version:1.2\n\n\0"),
        ws_frame(0x9, b"ping"),
        ws_frame(0xA, b""),
        ws_frame(0x2, &[0x42; 300]),
        ws_frame(0x8, b"\x03\xe8bye"),
    ];
    for f in &frames {
        seeds.add_with("websocket", 1, f);
    }
    seeds.add_with("websocket", 1, &frames.concat());
    seeds.add_with("websocket", 3, &frames.concat());
    let mut fragmented = vec![0x02, 0x83, 0, 0, 0, 0];
    fragmented.extend_from_slice(b"abc");
    fragmented.extend(ws_frame(0x0, b"def"));
    seeds.add_with("websocket", 1, &fragmented);

    // Filters, as JMS selectors and Artemis's own identifiers write them.
    for f in [
        "color = 'red'",
        "color <> 'red' AND size > 5",
        "size BETWEEN 1 AND 10 OR price < 3.5e0",
        "name LIKE 'a%b\\_c' ESCAPE '\\'",
        "color IN ('red', 'green', 'blue')",
        "color NOT IN ('x')",
        "flag IS NULL OR flag IS NOT NULL",
        "NOT (size >= 3) AND -size < 0 AND size * 2 / 1 + 1 - 1 = 20",
        "AMQPriority > 3 AND AMQDurable = 'DURABLE'",
        "AMQUserID = 'ID:1' OR AMQTimestamp > 0 OR AMQExpiration = 0 OR AMQSize > 0",
        "JMSPriority = 4 AND JMSType = 't'",
        "hyphenated.prop = 0x1F OR octal = 017 OR big = 9223372036854775807",
        "TRUE",
        "flag = FALSE",
        "\"quoted id\" = 'v'",
        "s = 'it''s'",
        "x LIKE '%'",
        "a.b.c = 1.5E+10 AND d = 2L",
        "XPATH '//root/a[@b=1]'",
        "(((size > 1)))",
        "color = 'red' AND (size > 1 OR (price < 2 AND NOT flag = TRUE))",
    ] {
        seeds.add("filter", f.as_bytes());
    }

    // Journal control files.
    let control = ControlFile {
        data_files: vec!["activemq-data-1.amq".into(), "activemq-data-2.amq".into()],
        new_files: vec!["activemq-data-3.cmp".into()],
        renames: vec![("activemq-data-3.cmp".into(), "activemq-data-3.amq".into())],
    };
    seeds.add_with("journal", 1, &control.encode_file());
    seeds.add_with("journal", 2, &control.encode_body());
    seeds.add_with("journal", 2, &ControlFile::default().encode_body());

    // JSON, as address settings are persisted.
    for j in [
        r#"{"maxSizeBytes":10485760,"addressFullMessagePolicy":"PAGE","deadLetterAddress":"DLA","expiryDelay":-1,"autoCreateQueues":true}"#,
        r#"[1,-2.5e3,"a\"b\\cé",true,false,null,{"k":[]}]"#,
    ] {
        seeds.add_with("buffer", 4, j.as_bytes());
    }
    // Strings in each of writeString's forms.
    for s in ["", "short", "a string of more than nine chars", "é∑"] {
        let mut w = Writer::new();
        saranga_buffer::Codec::write(&saranga_buffer::JString::from(s), &mut w);
        seeds.add_with("buffer", 2, &w.into_vec());
        let mut w = Writer::new();
        saranga_buffer::Codec::write(&Some(saranga_buffer::SimpleString::from(s)), &mut w);
        seeds.add_with("buffer", 3, &w.into_vec());
    }
}

/// Every `broker.xml` and `bootstrap.xml` in the repository's tests and
/// templates, and acceptor URIs.
fn config(seeds: &mut Seeds) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut dirs = vec![
        root.join("tests/golden/etc"),
        root.join("crates/saranga-cli/templates/etc"),
        root.join("tests/interop"),
    ];
    while let Some(dir) = dirs.pop() {
        for path in sorted(&dir) {
            if path.is_dir() {
                dirs.push(path);
            } else if path
                .extension()
                .is_some_and(|e| e == "xml" || e == "properties")
            {
                let bytes = std::fs::read(&path).unwrap();
                if bytes.len() <= MAX_SEED {
                    seeds.add("config", &bytes);
                }
            }
        }
    }
    for uri in [
        "tcp://0.0.0.0:61616?tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;amqpMinLargeMessageSize=102400;protocols=CORE,AMQP,STOMP,HORNETQ,MQTT,OPENWIRE;useEpoll=true;amqpCredits=1000;amqpLowCredits=300",
        "tcp://localhost:5671?sslEnabled=true;keyStorePath=k.p12;keyStorePassword=ENC(3a34fd21b82bf2a822fa49a8d8fa115d);needClientAuth=true",
        "vm://0",
        "ws://[::1]:8080?protocols=STOMP;webSocketEncoderType=text",
    ] {
        seeds.add("config", uri.as_bytes());
    }
}
