//! Frame decoding (`StompDecoder`, `StompDecoderV11`, `StompDecoderV12`).
//!
//! Every connection starts with the 1.0 rules; the version negotiated at
//! `CONNECT` picks the rules for the rest. Parsing starts over from the
//! frame's first byte whenever more bytes arrive, which reaches the same
//! result as Artemis's incremental decoder.

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
    let Some((command, mut pos)) = parse_command(rules, buf)? else {
        return Ok(None);
    };
    let Some((headers, content_length)) = parse_headers(rules, buf, &mut pos)? else {
        return Ok(None);
    };
    let Some(body) = parse_body(rules, buf, &mut pos, content_length) else {
        return Ok(None);
    };
    // A newline after the NUL belongs to the frame.
    if buf.get(pos) == Some(&b'\n') {
        pos += 1;
    }
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

type Headers = (Vec<(String, String)>, Option<usize>);

fn parse_headers(
    rules: Rules,
    buf: &[u8],
    pos: &mut usize,
) -> Result<Option<Headers>, DecodeError> {
    match rules {
        Rules::V10 => parse_headers_v10(buf, pos),
        Rules::V11 | Rules::V12 => parse_headers_v11(rules, buf, pos),
    }
}

/// One header per name, the later replacing the earlier
/// (`HashMap.put`) or, with `first_wins`, the earlier kept.
fn put(headers: &mut Vec<(String, String)>, name: String, value: String, first_wins: bool) {
    match headers.iter_mut().find(|(k, _)| *k == name) {
        Some(_) if first_wins => {}
        Some(slot) => slot.1 = value,
        None => headers.push((name, value)),
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
/// the start of a value are dropped.
fn parse_headers_v10(buf: &[u8], pos: &mut usize) -> Result<Option<Headers>, DecodeError> {
    let mut headers = Vec::with_capacity(8);
    let mut length = None;
    let mut start = *pos;
    let mut in_name = true;
    let mut value_whitespace = false;
    let mut whitespace_only = true;
    let mut name: Option<String> = None;
    loop {
        let Some(&b) = buf.get(*pos) else {
            return Ok(None);
        };
        *pos += 1;
        match b {
            b':' => {
                if in_name {
                    name = Some(String::from_utf8_lossy(&buf[start..*pos - 1]).into_owned());
                    in_name = false;
                    start = *pos;
                    value_whitespace = true;
                }
                whitespace_only = false;
            }
            b'\n' => {
                if whitespace_only {
                    break;
                }
                let value = String::from_utf8_lossy(&buf[start..*pos - 1]).into_owned();
                if let Some(n) = &name {
                    if n == names::CONTENT_LENGTH {
                        length = Some(content_length(&value)?);
                    }
                    put(&mut headers, n.clone(), value, false);
                }
                whitespace_only = true;
                start = *pos;
                in_name = true;
                value_whitespace = false;
            }
            b' ' | b'\t' => {
                if value_whitespace {
                    start += 1;
                }
            }
            _ => {
                whitespace_only = false;
                value_whitespace = false;
            }
        }
        if *pos == buf.len() {
            return Ok(None);
        }
    }
    Ok(Some((headers, length)))
}

/// `StompDecoderV11.parseHeaders` and `StompDecoderV12.parseHeaders`.
/// A `:` after the first in a line is dropped, as Artemis does.
fn parse_headers_v11(
    rules: Rules,
    buf: &[u8],
    pos: &mut usize,
) -> Result<Option<Headers>, DecodeError> {
    let v12 = rules == Rules::V12;
    let mut headers = Vec::with_capacity(8);
    let mut length = None;
    let mut holder: Vec<u8> = Vec::with_capacity(64);
    let mut escaping = false;
    let mut in_name = true;
    let mut whitespace_only = true;
    let mut name = String::new();
    let mut cr = false;
    let text = |h: &[u8]| String::from_utf8_lossy(h).into_owned();
    loop {
        let Some(&b) = buf.get(*pos) else {
            return Ok(None);
        };
        *pos += 1;
        match b {
            b'\\' => {
                if escaping {
                    holder.push(b);
                    escaping = false;
                } else {
                    escaping = true;
                }
            }
            b':' => {
                if in_name {
                    name = text(&holder);
                    holder.clear();
                    in_name = false;
                }
                whitespace_only = false;
            }
            b'n' | b'c' | b'r' if escaping && (b != b'r' || v12) => {
                holder.push(match b {
                    b'n' => b'\n',
                    b'c' => b':',
                    _ => b'\r',
                });
                escaping = false;
            }
            b'\r' if v12 => {
                if cr {
                    return Err(DecodeError::TwoCrs);
                }
                cr = true;
            }
            b'\n' => {
                cr = false;
                if whitespace_only {
                    break;
                }
                let value = text(&holder);
                holder.clear();
                if name == names::CONTENT_LENGTH {
                    length = Some(content_length(&value)?);
                }
                put(&mut headers, name.clone(), value, v12);
                whitespace_only = true;
                in_name = true;
            }
            _ => {
                whitespace_only = false;
                if escaping {
                    return Err(DecodeError::UndefinedEscape(format!("\\{}", b as char)));
                }
                holder.push(b);
            }
        }
        if *pos == buf.len() {
            return Ok(None);
        }
    }
    Ok(Some((headers, length)))
}

/// `parseBody`: `content-length` bytes, or up to the NUL. Under 1.1 and
/// 1.2, the bytes after a length's worth up to the NUL are dropped.
fn parse_body(rules: Rules, buf: &[u8], pos: &mut usize, length: Option<usize>) -> Option<Vec<u8>> {
    match length {
        Some(len) => {
            let end = pos.checked_add(len)?;
            if end.checked_add(1)? > buf.len() {
                return None;
            }
            let content = buf[*pos..end].to_vec();
            *pos = end;
            match rules {
                Rules::V10 => *pos += 1,
                Rules::V11 | Rules::V12 => {
                    while *pos < buf.len() {
                        *pos += 1;
                        if buf[*pos - 1] == 0 {
                            break;
                        }
                    }
                }
            }
            Some(content)
        }
        None => {
            let nul = buf[*pos..].iter().position(|&b| b == 0)?;
            let content = buf[*pos..*pos + nul].to_vec();
            *pos += nul + 1;
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
}
