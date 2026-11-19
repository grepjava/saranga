//! Frame decoding (`StompDecoder`, `StompDecoderV11`, `StompDecoderV12`).
//!
//! Every connection starts with the 1.0 rules; the version negotiated at
//! `CONNECT` picks the rules for the rest. A frame still arriving is read
//! on from where the last bytes left it ([`Progress`]), as Artemis's
//! incremental decoder keeps its position and header state.

use std::collections::HashMap;

use crate::frame::names;

/// The decoding rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rules {
    /// Headers are raw, leading whitespace in values is trimmed, and only
    /// `\n` ends a line.
    V10,
    /// Headers are escaped (`\n`, `\c`, `\\`).
    V11,
    /// As 1.1, plus `\r` escapes and `\r\n` line ends.
    V12,
}

/// A decoded frame: its command, headers (one value per name, as Artemis's
/// `HashMap` keeps them), and body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub command: &'static str,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Why bytes could not be decoded (`ActiveMQStompException` with its code).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// `INVALID_EOL_V10`: the command line did not end with `\n`; a client
    /// that has not negotiated a version is retried with the 1.2 rules.
    InvalidEolV10(u8),
    /// `INVALID_COMMAND`: the connection gets an ERROR and is closed.
    InvalidCommand(String),
    /// `UNDEFINED_ESCAPE`: as `InvalidCommand`.
    UndefinedEscape(String),
    /// `invalidTwoCRs`.
    TwoCrs,
    /// `badCRs`.
    BadCrs,
    /// A `content-length` that is not a number.
    ContentLength(String),
}

/// Parses the frame at the start of `buf`. Returns the frame and how many
/// bytes it took, or `None` when more bytes are needed. Leading line ends
/// (heart-beats) before a frame are skipped; `Ok(None)` with only those
/// in `buf` means nothing is pending.
pub fn decode(rules: Rules, buf: &[u8]) -> Result<Option<(Decoded, usize)>, DecodeError> {
    decode_lines(rules, buf, true, &mut Progress::default())
}

/// How far a frame still arriving has been read: kept by the caller between
/// reads of the same frame, and a new one (`Progress::default()`) for the
/// next frame or after an error. Each read then parses only the bytes that
/// came since, as `StompDecoder` keeps its `pos` and header state.
#[derive(Debug, Default)]
pub struct Progress {
    /// How far into the frame the body's NUL is known not to be.
    scanned: usize,
    /// The command and the headers read so far.
    headers: Option<HeaderState>,
}

impl Progress {
    /// How far the frame's bytes have been searched for the body's NUL.
    pub fn scanned(&self) -> usize {
        self.scanned
    }
}

/// [`decode`] of a frame still arriving, read on from `progress`.
pub fn decode_from(
    rules: Rules,
    buf: &[u8],
    progress: &mut Progress,
) -> Result<Option<(Decoded, usize)>, DecodeError> {
    decode_lines(rules, buf, true, progress)
}

/// [`decode_from`], taking plain header lines whole (`fast`) or not.
fn decode_lines(
    rules: Rules,
    buf: &[u8],
    fast: bool,
    progress: &mut Progress,
) -> Result<Option<(Decoded, usize)>, DecodeError> {
    let Progress { scanned, headers } = progress;
    let state = match headers {
        Some(h) if h.rules == rules => h,
        _ => {
            let Some((command, pos)) = parse_command(rules, buf)? else {
                return Ok(None);
            };
            headers.insert(HeaderState::new(rules, command, pos, fast))
        }
    };
    // (Rules change only between frames, with a new `Progress`.)
    if !state.done {
        let done = match rules {
            Rules::V10 => parse_headers_v10(buf, state)?,
            Rules::V11 | Rules::V12 => parse_headers_v11(rules, buf, state, fast)?,
        };
        if !done {
            return Ok(None);
        }
        state.done = true;
    }
    let mut pos = state.pos;
    let Some(body) = parse_body(rules, buf, &mut pos, state.length, scanned) else {
        return Ok(None);
    };
    // A newline after the NUL belongs to the frame.
    if buf.get(pos) == Some(&b'\n') {
        pos += 1;
    }
    let command = state.command;
    let headers = std::mem::take(&mut state.headers.list);
    *progress = Progress::default();
    Ok(Some((
        Decoded {
            command,
            headers,
            body,
        },
        pos,
    )))
}

/// How many leading bytes are line ends (heart-beats).
pub fn leading_eols(buf: &[u8]) -> usize {
    buf.iter()
        .take_while(|b| matches!(b, b'\n' | b'\r'))
        .count()
}

const COMMANDS: [&str; 15] = [
    names::ABORT,
    names::ACK,
    names::BEGIN,
    names::COMMIT,
    names::CONNECTED,
    names::CONNECT,
    names::DISCONNECT,
    names::RECEIPT,
    names::ERROR,
    names::MESSAGE,
    names::SEND,
    names::SUBSCRIBE,
    names::STOMP,
    names::UNSUBSCRIBE,
    names::NACK,
];

fn command(name: &str) -> &'static str {
    COMMANDS
        .iter()
        .find(|c| **c == name)
        .copied()
        .expect("a known command")
}

/// `parseCommand`: the command from its first one to three letters, and
/// the position after its line end.
fn parse_command(rules: Rules, buf: &[u8]) -> Result<Option<(&'static str, usize)>, DecodeError> {
    // Line ends before the command: heart-beats, or a newline after the
    // previous frame's NUL.
    let mut offset = 0;
    let mut cr = false;
    while offset < buf.len() {
        match buf[offset] {
            b'\n' => cr = false,
            b'\r' => {
                if cr {
                    return Err(DecodeError::TwoCrs);
                }
                cr = true;
            }
            _ => break,
        }
        offset += 1;
    }
    if offset == buf.len() {
        // Nothing but line ends so far; a lone `\r` may be the start of
        // `\r\n`.
        return Ok(None);
    }
    if cr {
        return Err(DecodeError::BadCrs);
    }
    if buf.len() < offset + 4 {
        return Ok(None);
    }
    // Bytes past what arrived read as 0, as Artemis's working buffer does.
    let at = |i: usize| buf.get(i).copied().unwrap_or(0);
    let name = match (rules, at(offset)) {
        (_, b'A') if at(offset + 1) == b'B' => names::ABORT,
        (_, b'A') => names::ACK,
        (_, b'B') => names::BEGIN,
        (_, b'C') if at(offset + 2) == b'M' => names::COMMIT,
        (_, b'C') if at(offset + 7) == b'E' => names::CONNECTED,
        (_, b'C') => names::CONNECT,
        (_, b'D') => names::DISCONNECT,
        (_, b'R') => names::RECEIPT,
        (_, b'E') => names::ERROR,
        (_, b'M') => names::MESSAGE,
        (Rules::V10, b'S') if at(offset + 1) == b'E' => names::SEND,
        (Rules::V10, b'S') if at(offset + 1) == b'T' => names::STOMP,
        (Rules::V10, b'S') => names::SUBSCRIBE,
        (_, b'S') if at(offset + 1) == b'E' => names::SEND,
        (_, b'S') if at(offset + 1) == b'U' => names::SUBSCRIBE,
        (_, b'S') => names::STOMP,
        (_, b'U') => names::UNSUBSCRIBE,
        (Rules::V11 | Rules::V12, b'N') => names::NACK,
        _ => return Err(DecodeError::InvalidCommand(dump(buf))),
    };
    let eol = if rules == Rules::V12 { 2 } else { 1 };
    // `tryIncrement`: a byte must follow the line end.
    let mut pos = offset + name.len() + eol;
    if pos >= buf.len() {
        return Ok(None);
    }
    match rules {
        Rules::V10 => {
            if buf[pos - 1] != b'\n' {
                return Err(DecodeError::InvalidEolV10(buf[pos - 1]));
            }
        }
        Rules::V11 => {
            if buf[pos - 1] != b'\n' {
                return Err(DecodeError::InvalidCommand(dump(buf)));
            }
        }
        Rules::V12 => {
            if buf[pos - 2] == b'\n' {
                pos -= 1;
            } else if buf[pos - 2] != b'\r' || buf[pos - 1] != b'\n' {
                return Err(DecodeError::InvalidCommand(dump(buf)));
            }
        }
    }
    Ok(Some((command(name), pos)))
}

/// The headers of a frame, one per name: looked up in a list while they
/// are few, in an index once they are many.
#[derive(Debug, Default)]
struct HeaderList {
    list: Vec<(String, String)>,
    index: HashMap<String, usize>,
}

/// The command, the headers read so far, and where the line being read
/// stands: what [`Progress`] keeps between reads.
#[derive(Debug)]
struct HeaderState {
    rules: Rules,
    command: &'static str,
    /// Where reading goes on.
    pos: usize,
    /// Whether the header block has ended.
    done: bool,
    headers: HeaderList,
    length: Option<usize>,
    // The line being read (1.1 and 1.2).
    holder: Vec<u8>,
    escaping: bool,
    in_name: bool,
    whitespace_only: bool,
    name: String,
    cr: bool,
    line_start: bool,
    // The line being read (1.0).
    start: usize,
    value_whitespace: bool,
    name_v10: Option<String>,
}

impl HeaderState {
    fn new(rules: Rules, command: &'static str, pos: usize, fast: bool) -> Self {
        HeaderState {
            rules,
            command,
            pos,
            done: false,
            headers: HeaderList {
                list: Vec::with_capacity(8),
                index: HashMap::new(),
            },
            length: None,
            holder: Vec::with_capacity(64),
            escaping: false,
            in_name: true,
            whitespace_only: true,
            name: String::new(),
            cr: false,
            line_start: fast,
            start: pos,
            value_whitespace: false,
            name_v10: None,
        }
    }
}

/// One header per name, the later replacing the earlier
/// (`HashMap.put`) or, with `first_wins`, the earlier kept.
fn put(headers: &mut HeaderList, name: String, value: String, first_wins: bool) {
    // Up to this many, the list is searched; past it, the index.
    const LISTED: usize = 16;
    let HeaderList { list, index } = headers;
    let at = if list.len() <= LISTED {
        list.iter().position(|(k, _)| *k == name)
    } else {
        if index.len() != list.len() {
            index.clear();
            index.extend(list.iter().enumerate().map(|(i, (k, _))| (k.clone(), i)));
        }
        index.get(&name).copied()
    };
    match at {
        Some(_) if first_wins => {}
        Some(i) => list[i].1 = value,
        None => {
            if !index.is_empty() {
                index.insert(name.clone(), list.len());
            }
            list.push((name, value));
        }
    }
}

fn content_length(value: &str) -> Result<usize, DecodeError> {
    // `Integer.parseInt`; a negative length reads as no length.
    match value.parse::<i32>() {
        Ok(n) if n >= 0 => Ok(n as usize),
        Ok(_) => Ok(usize::MAX),
        Err(_) => Err(DecodeError::ContentLength(value.to_owned())),
    }
}

/// `StompDecoder.parseHeaders`: raw names and values; spaces and tabs at
/// the start of a value are dropped. `false` while the header block has not
/// ended in `buf`: reading goes on from `s` once more has come.
fn parse_headers_v10(buf: &[u8], s: &mut HeaderState) -> Result<bool, DecodeError> {
    loop {
        let Some(&b) = buf.get(s.pos) else {
            return Ok(false);
        };
        s.pos += 1;
        match b {
            b':' => {
                if s.in_name {
                    s.name_v10 =
                        Some(String::from_utf8_lossy(&buf[s.start..s.pos - 1]).into_owned());
                    s.in_name = false;
                    s.start = s.pos;
                    s.value_whitespace = true;
                }
                s.whitespace_only = false;
            }
            b'\n' => {
                if s.whitespace_only {
                    break;
                }
                let value = String::from_utf8_lossy(&buf[s.start..s.pos - 1]).into_owned();
                if let Some(n) = &s.name_v10 {
                    if n == names::CONTENT_LENGTH {
                        s.length = Some(content_length(&value)?);
                    }
                    put(&mut s.headers, n.clone(), value, false);
                }
                s.whitespace_only = true;
                s.start = s.pos;
                s.in_name = true;
                s.value_whitespace = false;
            }
            b' ' | b'\t' => {
                if s.value_whitespace {
                    s.start += 1;
                }
            }
            _ => {
                s.whitespace_only = false;
                s.value_whitespace = false;
            }
        }
        if s.pos == buf.len() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `StompDecoderV11.parseHeaders` and `StompDecoderV12.parseHeaders`.
/// A `:` after the first in a line is dropped, as Artemis does. `false`
/// while the header block has not ended in `buf`: reading goes on from `s`
/// once more has come.
fn parse_headers_v11(
    rules: Rules,
    buf: &[u8],
    s: &mut HeaderState,
    fast: bool,
) -> Result<bool, DecodeError> {
    let v12 = rules == Rules::V12;
    let text = |h: &[u8]| String::from_utf8_lossy(h).into_owned();
    loop {
        // A whole line with one `:` and nothing escaped or carriage
        // returned but its end is taken at once; anything else, byte by
        // byte below.
        if s.line_start
            && !s.escaping
            && let Some(n) = buf[s.pos..].iter().position(|&b| b == b'\n')
        {
            let end = s.pos + n;
            let mut line = &buf[s.pos..end];
            if v12 && line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            let mut colons = line.iter().enumerate().filter(|(_, b)| **b == b':');
            if let (Some((c, _)), None) = (colons.next(), colons.next())
                && !line.iter().any(|&b| b == b'\\' || (v12 && b == b'\r'))
            {
                s.pos = end + 1;
                s.name = text(&line[..c]);
                let value = text(&line[c + 1..]);
                if s.name == names::CONTENT_LENGTH {
                    s.length = Some(content_length(&value)?);
                }
                put(&mut s.headers, s.name.clone(), value, v12);
                if s.pos == buf.len() {
                    return Ok(false);
                }
                continue;
            }
        }
        let Some(&b) = buf.get(s.pos) else {
            return Ok(false);
        };
        s.pos += 1;
        s.line_start = fast && b == b'\n';
        match b {
            b'\\' => {
                if s.escaping {
                    s.holder.push(b);
                    s.escaping = false;
                } else {
                    s.escaping = true;
                }
            }
            b':' => {
                if s.in_name {
                    s.name = text(&s.holder);
                    s.holder.clear();
                    s.in_name = false;
                }
                s.whitespace_only = false;
            }
            b'n' | b'c' | b'r' if s.escaping && (b != b'r' || v12) => {
                s.holder.push(match b {
                    b'n' => b'\n',
                    b'c' => b':',
                    _ => b'\r',
                });
                s.escaping = false;
            }
            b'\r' if v12 => {
                if s.cr {
                    return Err(DecodeError::TwoCrs);
                }
                s.cr = true;
            }
            b'\n' => {
                s.cr = false;
                if s.whitespace_only {
                    break;
                }
                let value = text(&s.holder);
                s.holder.clear();
                if s.name == names::CONTENT_LENGTH {
                    s.length = Some(content_length(&value)?);
                }
                put(&mut s.headers, s.name.clone(), value, v12);
                s.whitespace_only = true;
                s.in_name = true;
            }
            _ => {
                s.whitespace_only = false;
                if s.escaping {
                    return Err(DecodeError::UndefinedEscape(format!("\\{}", b as char)));
                }
                s.holder.push(b);
            }
        }
        if s.pos == buf.len() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The first NUL in `buf` from `from`, searching only past `scanned`; when
/// there is none, `scanned` becomes the end of `buf`.
fn find_nul(buf: &[u8], from: usize, scanned: &mut usize) -> Option<usize> {
    let start = from.max(*scanned).min(buf.len());
    match buf[start..].iter().position(|&b| b == 0) {
        Some(i) => Some(start + i),
        None => {
            *scanned = buf.len();
            None
        }
    }
}

/// `parseBody`: `content-length` bytes, or up to the NUL. Under 1.1 and
/// 1.2, the bytes after a length's worth up to the NUL are dropped.
fn parse_body(
    rules: Rules,
    buf: &[u8],
    pos: &mut usize,
    length: Option<usize>,
    scanned: &mut usize,
) -> Option<Vec<u8>> {
    match length {
        Some(len) => {
            let end = pos.checked_add(len)?;
            if end.checked_add(1)? > buf.len() {
                return None;
            }
            let content = buf[*pos..end].to_vec();
            match rules {
                Rules::V10 => *pos = end + 1,
                // The frame ends at the NUL, which may not be here yet:
                // Artemis takes the frame anyway and reads the rest of it
                // as the next one, so what it does depends on how the bytes
                // arrive.
                Rules::V11 | Rules::V12 => {
                    let nul = find_nul(buf, end, scanned)?;
                    *pos = nul + 1;
                }
            }
            Some(content)
        }
        None => {
            let nul = find_nul(buf, *pos, scanned)?;
            let content = buf[*pos..nul].to_vec();
            *pos = nul + 1;
            Some(content)
        }
    }
}

/// `dumpByteArray`: printable bytes as characters, others as numbers, each
/// followed by a comma.
fn dump(buf: &[u8]) -> String {
    let mut s = String::new();
    for &b in buf {
        // `(char) byte` sign-extends, so only 33 to 127 print as text.
        if (33..=127).contains(&b) {
            s.push(b as char);
        } else {
            s.push_str(&(b as i8).to_string());
        }
        s.push(',');
    }
    s
}

/// The headers as a map, for lookups that do not care about order.
pub fn header_map(headers: &[(String, String)]) -> HashMap<&str, &str> {
    headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(rules: Rules, bytes: &[u8]) -> (Decoded, usize) {
        decode(rules, bytes).unwrap().unwrap()
    }

    #[test]
    fn v10_trims_values_and_keeps_colons() {
        let (f, n) = one(
            Rules::V10,
            b"SEND\ndestination: \t/q:x\nfoo:1\nfoo:2\n\nhello\0\n",
        );
        assert_eq!(f.command, "SEND");
        assert_eq!(
            f.headers,
            vec![
                ("destination".into(), "/q:x".into()),
                ("foo".into(), "2".into())
            ]
        );
        assert_eq!(f.body, b"hello");
        assert_eq!(n, 44);
    }

    #[test]
    fn needs_more_bytes() {
        let full = b"SEND\ndestination:q\ncontent-length:3\n\na\0b\0";
        for i in 0..full.len() {
            assert_eq!(decode(Rules::V10, &full[..i]), Ok(None), "at {i}");
        }
        let (f, _) = one(Rules::V10, full);
        assert_eq!(f.body, b"a\0b");
    }

    #[test]
    fn v10_rejects_crlf_so_v12_is_tried() {
        assert_eq!(
            decode(Rules::V10, b"CONNECT\r\nhost:x\r\n\r\n\0"),
            Err(DecodeError::InvalidEolV10(b'\r'))
        );
        let (f, _) = one(
            Rules::V12,
            b"CONNECT\r\nhost:x\r\naccept-version:1.2\r\n\r\n\0",
        );
        assert_eq!(f.command, "CONNECT");
        assert_eq!(
            f.headers,
            vec![
                ("host".into(), "x".into()),
                ("accept-version".into(), "1.2".into())
            ]
        );
    }

    #[test]
    fn v11_escapes() {
        let (f, _) = one(Rules::V11, b"SEND\na\\cb:x\\ny\\\\z\nc:\\c: \n\n\0");
        assert_eq!(
            f.headers,
            vec![("a:b".into(), "x\ny\\z".into()), ("c".into(), ": ".into())]
        );
        assert!(matches!(
            decode(Rules::V11, b"SEND\na:\\t\n\n\0"),
            Err(DecodeError::UndefinedEscape(_))
        ));
        // `\r` is an escape only in 1.2.
        assert!(matches!(
            decode(Rules::V11, b"SEND\na:\\r\n\n\0"),
            Err(DecodeError::UndefinedEscape(_))
        ));
        let (f, _) = one(Rules::V12, b"SEND\na:\\r\na:2\n\n\0");
        assert_eq!(f.headers, vec![("a".into(), "\r".into())]);
    }

    #[test]
    fn heart_beats_before_a_frame() {
        assert_eq!(decode(Rules::V12, b"\n\r\n\n"), Ok(None));
        let (f, n) = one(Rules::V12, b"\n\r\nDISCONNECT\n\n\0\n");
        assert_eq!(f.command, "DISCONNECT");
        assert_eq!(n, 17);
        assert_eq!(decode(Rules::V12, b"\r\r\n"), Err(DecodeError::TwoCrs));
    }

    #[test]
    fn content_length_then_drain_to_nul() {
        let (f, n) = one(Rules::V11, b"SEND\ncontent-length:2\n\nabcd\0X");
        assert_eq!(f.body, b"ab");
        assert_eq!(n, 28);
    }

    /// The frame ends at its NUL however the bytes arrive: until it is
    /// there, there is no frame.
    #[test]
    fn content_length_waits_for_the_nul() {
        let whole = b"SEND\ncontent-length:2\n\nabcd\0SEND\n\n\0";
        for rules in [Rules::V11, Rules::V12] {
            for cut in 0..28 {
                assert_eq!(decode(rules, &whole[..cut]), Ok(None), "{rules:?} at {cut}");
            }
            let (f, n) = one(rules, whole);
            assert_eq!((f.body.as_slice(), n), (&b"ab"[..], 28));
            let (next, _) = one(rules, &whole[n..]);
            assert_eq!(next.command, "SEND");
        }
        // 1.0 takes the byte after the body as the NUL, as Artemis does.
        assert_eq!(one(Rules::V10, b"SEND\ncontent-length:2\n\nab\0").1, 26);
    }

    #[test]
    fn unknown_command() {
        let e = decode(Rules::V10, b"FOO\n\n\0").unwrap_err();
        assert_eq!(e, DecodeError::InvalidCommand("F,O,O,10,10,0,".into()));
        assert!(matches!(
            decode(Rules::V10, b"NACK\n\n\0"),
            Err(DecodeError::InvalidCommand(_))
        ));
        assert_eq!(one(Rules::V11, b"NACK\n\n\0").0.command, "NACK");
    }

    /// Taking plain header lines whole changes nothing: generated frames
    /// decode the same, or fail the same, both ways.
    #[test]
    fn whole_lines_decode_as_bytes_do() {
        let alphabet: &[u8] = b"ab:\\ncr\r\n\n\n\0";
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..20_000 {
            let mut frame = b"SEND\n".to_vec();
            let len = (next() % 40) as usize;
            for _ in 0..len {
                frame.push(alphabet[(next() % alphabet.len() as u64) as usize]);
            }
            if next() % 2 == 0 {
                frame.extend_from_slice(b"\n\nbody\0");
            }
            for rules in [Rules::V11, Rules::V12] {
                assert_eq!(
                    decode_lines(rules, &frame, true, &mut Progress::default()),
                    decode_lines(rules, &frame, false, &mut Progress::default()),
                    "{:?} {rules:?}",
                    String::from_utf8_lossy(&frame)
                );
            }
        }
    }

    /// A body arriving in pieces is searched for its NUL once: each read
    /// takes up where the last left off, rather than from the body's start.
    #[test]
    fn a_body_arriving_in_pieces_is_searched_once() {
        for (rules, length) in [
            (Rules::V12, None),
            (Rules::V12, Some(16usize)),
            (Rules::V10, None),
        ] {
            let mut buf = match length {
                Some(n) => format!("SEND\ndestination:q\ncontent-length:{n}\n\n").into_bytes(),
                None => b"SEND\ndestination:q\n\n".to_vec(),
            };
            let body = vec![b'x'; 64 * 1024];
            let mut progress = Progress::default();
            for chunk in body.chunks(4096) {
                buf.extend_from_slice(chunk);
                assert!(decode_from(rules, &buf, &mut progress).unwrap().is_none());
                assert_eq!(
                    progress.scanned(),
                    buf.len(),
                    "searched up to what has arrived"
                );
            }
            buf.push(0);
            let (d, used) = decode_from(rules, &buf, &mut progress).unwrap().unwrap();
            assert_eq!(used, buf.len());
            assert_eq!(d.body, &body[..length.unwrap_or(body.len())]);
        }
    }
}
