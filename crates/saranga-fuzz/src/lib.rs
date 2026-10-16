//! The bodies of the cargo-fuzz targets in `fuzz/`, one function per codec.
//!
//! Each takes arbitrary bytes and must return without panicking, whatever
//! they hold. Where a codec can also encode, what it decodes is encoded,
//! decoded again and encoded again, and the two encodings must be the same
//! bytes: they are compared as bytes, not as values, so that a NaN is equal
//! to itself. Where an input needs a setting the bytes cannot carry (a
//! direction, a protocol version, an encoding), its first byte chooses it.
//!
//! Every input that once failed is kept in `fuzz/regressions/<target>/`, and
//! `tests/regressions.rs` runs each through its target. `fuzz-seeds` cuts
//! the golden corpus into each target's seeds.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::{Pin, pin};
use std::task::{Context as TaskContext, Poll, Waker};

use saranga_buffer::json::Json;
use saranga_buffer::{Codec, JString, Reader, SimpleString, TypedProperties, Writer};
use saranga_core_protocol::{Context, Direction, Packet, read_frame};
use saranga_journal::control::ControlFile;
use saranga_journal::format::{self, FileHeader};
use saranga_message::CoreMessage;
use saranga_protocol_amqp::codec;
use saranga_protocol_amqp::frame::{self, FrameBody};
use saranga_protocol_amqp::message::AmqpMessage;
use saranga_protocol_amqp::sasl::SaslFrame;
use saranga_protocol_amqp::transport::Performative;
use saranga_protocol_mqtt::codec as mqtt;
use saranga_protocol_openwire::WireFormat;
use saranga_protocol_stomp::decoder::{self as stomp, Rules};
use saranga_storage::{JournalKind, decode_record};
use saranga_transport::websocket::{self, EncoderType, WebSocketStream};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

/// A target: it takes any input, and panics only on a bug.
pub type Target = fn(&[u8]);

/// Every target, by the name of its `fuzz/fuzz_targets/<name>.rs`.
pub const TARGETS: &[(&str, Target)] = &[
    ("buffer", buffer),
    ("core_message", core_message),
    ("core_packet", core_packet),
    ("journal", journal),
    ("storage_record", storage_record),
    ("page", page),
    ("amqp_frame", amqp_frame),
    ("amqp_value", amqp_value),
    ("amqp_message", amqp_message),
    ("mqtt", mqtt),
    ("stomp", stomp),
    ("openwire", openwire),
    ("websocket", websocket),
    ("filter", filter),
    ("config", config),
];

/// The first byte, and the rest.
fn split(data: &[u8]) -> Option<(u8, &[u8])> {
    data.split_first().map(|(b, rest)| (*b, rest))
}

/// Decodes `data`; if that works, the value must encode to bytes that
/// decode completely to a value encoding to the same bytes.
fn round_trip<T>(
    data: &[u8],
    read: impl Fn(&mut Reader<'_>) -> saranga_buffer::Result<T>,
    write: impl Fn(&T, &mut Writer),
) {
    let Ok(value) = read(&mut Reader::new(data)) else {
        return;
    };
    let encode = |v: &T| {
        let mut w = Writer::new();
        write(v, &mut w);
        w.into_vec()
    };
    let first = encode(&value);
    let mut r = Reader::new(&first);
    let again = read(&mut r).unwrap_or_else(|e| panic!("its own encoding does not decode: {e}"));
    assert!(
        r.is_empty(),
        "{} bytes left after its own encoding",
        r.remaining()
    );
    assert_eq!(encode(&again), first, "the encoding changed");
}

/// `saranga-buffer`: 0 `TypedProperties`, 1 a `SimpleString`, 2 a `JString`
/// in `writeString` form, 3 nullable strings, 4 JSON.
pub fn buffer(data: &[u8]) {
    let Some((selector, rest)) = split(data) else {
        return;
    };
    match selector % 5 {
        0 => round_trip(rest, TypedProperties::decode, TypedProperties::encode),
        1 => round_trip(rest, SimpleString::read, SimpleString::write),
        2 => round_trip(rest, JString::read, JString::write),
        3 => round_trip(rest, Option::<SimpleString>::read, Option::write),
        _ => {
            let Ok(text) = std::str::from_utf8(rest) else {
                return;
            };
            if let Ok(json) = Json::parse(text) {
                let first = json.to_string();
                let again = Json::parse(&first)
                    .unwrap_or_else(|e| panic!("its own text does not parse: {e}: {first}"));
                assert_eq!(again.to_string(), first, "the text changed");
            }
        }
    }
}

/// A Core message: even, the full buffer of `SESS_SEND`; odd, the headers
/// and properties alone, as a journal record holds them.
pub fn core_message(data: &[u8]) {
    let Some((selector, rest)) = split(data) else {
        return;
    };
    if selector % 2 == 0 {
        round_trip(rest, CoreMessage::decode, CoreMessage::encode);
    } else {
        round_trip(
            rest,
            CoreMessage::decode_headers_and_properties,
            CoreMessage::encode_headers_and_properties,
        );
    }
}

/// The Core context the first byte of a `core_packet` input names: bit 0 the
/// direction (set: to the client), the rest a version, 0 to 15 for 121 to
/// 136 and anything else for 0, a connection's version before its session.
pub fn core_context(selector: u8) -> Context {
    let direction = if selector & 1 == 0 {
        Direction::ToServer
    } else {
        Direction::ToClient
    };
    let v = i32::from(selector >> 1);
    Context::new(direction, if v < 16 { 121 + v } else { 0 })
}

/// The first byte of a `core_packet` input for a context.
pub fn core_selector(ctx: &Context) -> u8 {
    let v = if (121..=136).contains(&ctx.version) {
        (ctx.version - 121) as u8
    } else {
        16
    };
    v << 1 | u8::from(ctx.direction == Direction::ToClient)
}

/// Core frames, one after another, in the context of the first byte.
pub fn core_packet(data: &[u8]) {
    let Some((selector, mut buf)) = split(data) else {
        return;
    };
    let ctx = core_context(selector);
    while let Ok(Some((frame, used))) = read_frame(buf) {
        assert!(used > 0 && used <= buf.len(), "frame of {used} bytes");
        if let Ok(packet) = Packet::decode(&frame, &ctx) {
            let first = packet.encode_frame(frame.channel_id, &ctx);
            let (frame2, used2) = read_frame(&first)
                .unwrap_or_else(|e| panic!("{}: its own frame does not read: {e}", packet.name()))
                .unwrap_or_else(|| panic!("{}: its own frame is cut short", packet.name()));
            assert_eq!(
                used2,
                first.len(),
                "{}: bytes after its frame",
                packet.name()
            );
            let again = Packet::decode(&frame2, &ctx).unwrap_or_else(|e| {
                panic!("{}: its own encoding does not decode: {e}", packet.name())
            });
            assert_eq!(
                again.encode_frame(frame2.channel_id, &ctx),
                first,
                "{}: the encoding changed",
                packet.name()
            );
        }
        buf = &buf[used..];
    }
}

/// A journal file: 0 a data or bindings file, 1 a compaction control file,
/// 2 a control record's body.
pub fn journal(data: &[u8]) {
    let Some((selector, rest)) = split(data) else {
        return;
    };
    match selector % 3 {
        0 => journal_file(rest),
        1 => {
            if let Ok(Some(control)) = ControlFile::decode_file(rest) {
                let first = control.encode_file();
                let again = ControlFile::decode_file(&first)
                    .unwrap_or_else(|e| panic!("its own file does not decode: {e}"))
                    .expect("its own file holds a control record");
                assert_eq!(again, control, "the control file changed");
            }
        }
        _ => {
            if let Ok(control) = ControlFile::decode_body(rest) {
                let first = control.encode_body();
                let again = ControlFile::decode_body(&first)
                    .unwrap_or_else(|e| panic!("its own body does not decode: {e}"));
                assert_eq!(again.encode_body(), first, "the body changed");
            }
        }
    }
}

fn journal_file(buf: &[u8]) {
    if buf.len() < format::SIZE_HEADER {
        return;
    }
    let _ = FileHeader::decode(buf, i32::from_be_bytes(buf[4..8].try_into().expect("4")));
    let version = i32::from_be_bytes(buf[0..4].try_into().expect("4"));
    let file_id = i64::from_be_bytes(buf[8..16].try_into().expect("8")) as i32;
    for reclaimed in [false, true] {
        let Some(scan) = format::scan(buf, file_id, version, reclaimed) else {
            continue;
        };
        assert!(scan.last_data_pos <= buf.len(), "data ends past the file");
        for scanned in &scan.records {
            // Each record, alone in a new file, reads back as itself.
            let mut file = FileHeader::new(0, 1).encode().to_vec();
            scanned.record.encode(1, scanned.compact_count, &mut file);
            let again = format::scan(&file, 1, format::FORMAT_VERSION, false)
                .expect("a file with its header");
            assert_eq!(
                again.records.len(),
                1,
                "{:?} did not read back",
                scanned.record
            );
            assert_eq!(
                again.records[0].record, scanned.record,
                "the record changed"
            );
            if let format::Record::Add {
                user_type, data, ..
            }
            | format::Record::Update {
                user_type, data, ..
            }
            | format::Record::AddTx {
                user_type, data, ..
            }
            | format::Record::UpdateTx {
                user_type, data, ..
            } = &scanned.record
            {
                for kind in [0, 1] {
                    let mut input = vec![kind, *user_type];
                    input.extend_from_slice(data);
                    storage_record(&input);
                }
            }
        }
    }
}

/// A storage manager record: the first byte the journal (even bindings, odd
/// messages), the second its user record type, then its body.
pub fn storage_record(data: &[u8]) {
    let [kind, user_type, body @ ..] = data else {
        return;
    };
    let kind = if kind % 2 == 0 {
        JournalKind::Bindings
    } else {
        JournalKind::Messages
    };
    for safe in [false, true] {
        let _ = saranga_storage::describe::describe_body(*user_type, 1, false, body, safe);
    }
    let Ok(record) = decode_record(kind, *user_type, body) else {
        return;
    };
    let first = record.encode();
    let again = decode_record(kind, *user_type, &first)
        .unwrap_or_else(|e| panic!("{record:?}: its own encoding does not decode: {e}"));
    assert_eq!(again.encode(), first, "{record:?}: the encoding changed");
}

/// A page file.
pub fn page(data: &[u8]) {
    let read = saranga_paging::read_page(data);
    assert!(read.valid_size <= data.len(), "valid past the file");
    let _ = saranga_paging::count_messages(data);
    for message in &read.messages {
        let first = message.encode_record();
        let again = saranga_paging::read_page(&first);
        assert_eq!(again.messages.len(), 1, "{message:?} did not read back");
        assert_eq!(
            again.messages[0].encode_record(),
            first,
            "the record changed"
        );
    }
}

fn amqp_reencode(f: &frame::Frame) -> Vec<u8> {
    let mut out = Vec::new();
    match &f.body {
        FrameBody::Amqp(p, payload) => frame::encode_amqp(&mut out, f.channel, p, payload),
        FrameBody::Sasl(s) => frame::encode_sasl(&mut out, s),
        FrameBody::Empty => frame::encode_empty(&mut out),
    }
    out
}

/// AMQP frames, after an optional protocol header.
pub fn amqp_frame(data: &[u8]) {
    let mut buf = data;
    if buf.starts_with(b"AMQP") && buf.len() >= 8 {
        buf = &buf[8..];
    }
    while let Some(size) = frame::frame_size(buf) {
        let size = size as usize;
        if size < 8 || size > buf.len() {
            return;
        }
        let original = &buf[..size];
        if let Ok(f) = frame::decode(original) {
            let first = amqp_reencode(&f);
            let again = frame::decode(&first)
                .unwrap_or_else(|e| panic!("{:?}: its own frame does not decode: {e}", f.body));
            assert_eq!(
                amqp_reencode(&again),
                first,
                "{:?}: the encoding changed",
                f.body
            );
        }
        buf = &buf[size..];
    }
}

/// One AMQP value, also read as a performative and as a SASL frame.
pub fn amqp_value(data: &[u8]) {
    let mut decoder = codec::Decoder::new(data);
    while decoder.skip().is_ok_and(|s| !s.is_empty()) {}
    let Ok(value) = codec::from_slice(data) else {
        return;
    };
    let first = codec::to_vec(&value);
    let again = codec::from_slice(&first)
        .unwrap_or_else(|e| panic!("{value:?}: its own encoding does not decode: {e}"));
    assert_eq!(
        codec::to_vec(&again),
        first,
        "{value:?}: the encoding changed"
    );

    if let Ok(p) = Performative::from_value(value.clone()) {
        let mut first = Vec::new();
        p.encode(&mut first);
        let again = codec::from_slice(&first)
            .and_then(Performative::from_value)
            .unwrap_or_else(|e| panic!("{p:?}: its own encoding does not decode: {e}"));
        let mut second = Vec::new();
        again.encode(&mut second);
        assert_eq!(second, first, "{p:?}: the encoding changed");
    }
    if let Ok(s) = SaslFrame::from_value(value) {
        let mut first = Vec::new();
        s.encode(&mut first);
        let again = codec::from_slice(&first)
            .and_then(SaslFrame::from_value)
            .unwrap_or_else(|e| panic!("{s:?}: its own encoding does not decode: {e}"));
        let mut second = Vec::new();
        again.encode(&mut second);
        assert_eq!(second, first, "{s:?}: the encoding changed");
    }
}

/// An AMQP message's sections, as a transfer carries them. What the broker
/// re-encodes must scan again.
pub fn amqp_message(data: &[u8]) {
    let Ok(mut message) = AmqpMessage::new(0, data.to_vec(), 0) else {
        return;
    };
    message.reencode(0);
    let first = message.data().to_vec();
    if let Err(e) = AmqpMessage::new(0, first, 0) {
        panic!("its re-encoding does not scan: {e}");
    }
}

/// MQTT packets, the first byte choosing the version the decoder starts
/// with: 0 none (3.1.1 rules until a CONNECT), 1 5, 2 3.1.
pub fn mqtt(data: &[u8]) {
    let Some((selector, rest)) = split(data) else {
        return;
    };
    let mut decoder = mqtt::Decoder::new();
    decoder.version = match selector % 3 {
        0 => None,
        1 => Some(mqtt::Version::V5),
        _ => Some(mqtt::Version::V3_1),
    };
    let mut buf = rest.to_vec();
    loop {
        let before = buf.len();
        match decoder.decode(&mut buf) {
            None => return,
            Some(Ok(_)) => assert!(buf.len() < before, "a packet of no bytes"),
            Some(Err(_)) => {}
        }
        if buf.is_empty() {
            return;
        }
        if buf.len() == before {
            // An error that consumed nothing: the decoder drops what follows.
            assert!(
                decoder.decode(&mut buf).is_none(),
                "decoding after an error"
            );
            return;
        }
    }
}

/// STOMP frames under the rules of 1.0, 1.1 or 1.2.
pub fn stomp(data: &[u8]) {
    let Some((selector, mut buf)) = split(data) else {
        return;
    };
    let rules = match selector % 3 {
        0 => Rules::V10,
        1 => Rules::V11,
        _ => Rules::V12,
    };
    loop {
        buf = &buf[stomp::leading_eols(buf)..];
        match stomp::decode(rules, buf) {
            Ok(Some((_, used))) => {
                assert!(used > 0 && used <= buf.len(), "a frame of {used} bytes");
                buf = &buf[used..];
            }
            _ => return,
        }
    }
}

/// OpenWire frames. The first byte: the version (1 to 12) in its low four
/// bits, then tight encoding, the cache, stack traces and no size prefix.
pub fn openwire(data: &[u8]) {
    let Some((selector, mut buf)) = split(data) else {
        return;
    };
    let format = || {
        let mut wf = WireFormat::new(u32::from(selector & 0x0F) % 12 + 1);
        wf.tight_encoding_enabled = selector & 0x10 != 0;
        wf.set_cache_enabled(selector & 0x20 != 0);
        wf.stack_trace_enabled = selector & 0x40 != 0;
        wf.size_prefix_disabled = selector & 0x80 != 0;
        wf
    };
    // The reader of the input, then the writer and reader of each check,
    // each keeping its cache across the frames.
    let (mut reader, mut writer, mut checker, mut rewriter) =
        (format(), format(), format(), format());
    while !buf.is_empty() {
        let frame = if selector & 0x80 != 0 {
            std::mem::take(&mut buf)
        } else {
            let Some(size) = buf.get(..4) else { return };
            let size = u32::from_be_bytes(size.try_into().expect("4")) as usize;
            let Some(frame) = buf.get(..4 + size) else {
                return;
            };
            buf = &buf[4 + size..];
            frame
        };
        let Ok(Some(ds)) = reader.unmarshal(frame) else {
            return;
        };
        let Ok(first) = writer.marshal(Some(&ds)) else {
            return;
        };
        let again = checker
            .unmarshal(&first)
            .unwrap_or_else(|e| panic!("{ds:?}: its own encoding does not decode: {e}"))
            .unwrap_or_else(|| panic!("{ds:?}: its own encoding decodes to null"));
        let second = rewriter
            .marshal(Some(&again))
            .unwrap_or_else(|e| panic!("{again:?}: does not encode again: {e}"));
        assert_eq!(second, first, "{ds:?}: the encoding changed");
    }
}

/// Polls a future that never waits: everything it reads is in memory.
fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    let mut cx = TaskContext::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

/// A connection whose peer sends `input` and then closes, and swallows what
/// is written.
struct Wire<'a> {
    input: &'a [u8],
}

impl AsyncRead for Wire<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let n = self.input.len().min(buf.remaining());
        buf.put_slice(&self.input[..n]);
        self.input = &self.input[n..];
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Wire<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// WebSocket: even, an HTTP request and its handshake; odd, the frames a
/// client sends once upgraded (at most 64 KiB a frame).
pub fn websocket(data: &[u8]) {
    let Some((selector, rest)) = split(data) else {
        return;
    };
    if selector % 2 == 0 {
        let _ = websocket::is_http(rest);
        let mut wire = Wire { input: rest };
        let Ok((request, _)) = block_on(websocket::read_request(&mut wire)) else {
            return;
        };
        let _ = block_on(websocket::handshake(
            &mut wire,
            &request,
            &["v12.stomp", "mqtt", "amqp", "artemis"],
        ));
    } else {
        let encoder = if selector & 2 == 0 {
            EncoderType::Binary
        } else {
            EncoderType::Text
        };
        let mut stream = WebSocketStream::new(Wire { input: rest }, Vec::new(), 65536, encoder);
        let mut out = Vec::new();
        let _ = block_on(stream.read_to_end(&mut out));
        assert!(out.len() <= rest.len(), "more payload than was sent");
    }
}

/// A filter expression, matched against a message with a few properties.
pub fn filter(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let _ = saranga_filter::translate::convert_to_activemq_filter_string(text);
    let Ok(filter) = saranga_filter::Filter::parse(text) else {
        return;
    };
    let message: HashMap<String, String> = [
        ("color", "red"),
        ("size", "10"),
        ("price", "2.5"),
        ("flag", "true"),
        ("AMQUserID", "ID:1"),
        ("AMQPriority", "4"),
        ("AMQDurable", "DURABLE"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();
    let _ = filter.matches(&message);
    let _ = filter.matches(&HashMap::<String, String>::new());
}

/// Configuration text: `broker.xml`, `bootstrap.xml`, a properties file and
/// an acceptor URI.
pub fn config(data: &[u8]) {
    let _ = saranga_config::properties::parse(data);
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let _ = saranga_config::BrokerConfig::parse_with(text, |_| None);
    let _ = saranga_config::BootstrapConfig::parse(text, |_| None);
    let _ = saranga_config::AcceptorConfig::parse(None, text);
}
