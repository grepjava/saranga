//! Ported from `ByteUtil.convertTextBytes`.

use crate::{ConfigError, Result};

/// Parses a byte size such as `10M`, `20Mb`, `1GiB`, `512 k` or `-1`.
///
/// Mirrors the Java regexes `^\s*(\d+)\s*(k|m|g)?i?(b)?\s*$` (case
/// insensitive), multiplying by powers of 1024, and falls back to
/// `Long.parseLong` for anything else, which is how negative values such as
/// `-1` get through. As in Java, the multiplication wraps on overflow.
pub fn convert_text_bytes(text: &str) -> Result<i64> {
    let err = || ConfigError::invalid("byte size", text, "not a number of bytes");
    if let Some((digits, multiplier)) = match_suffixed(text) {
        let n: i64 = digits.parse().map_err(|_| err())?;
        return Ok(n.wrapping_mul(multiplier));
    }
    text.parse().map_err(|_| err())
}

fn is_java_space(c: u8) -> bool {
    // `\s` in java.util.regex: [ \t\n\x0B\f\r]
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// Returns the digit run and the multiplier when `text` matches one of the
/// four `ByteUtil` patterns.
fn match_suffixed(text: &str) -> Option<(&str, i64)> {
    let b = text.as_bytes();
    let mut i = 0;
    while i < b.len() && is_java_space(b[i]) {
        i += 1;
    }
    let start = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i == start {
        return None;
    }
    let digits = &text[start..i];
    while i < b.len() && is_java_space(b[i]) {
        i += 1;
    }
    let mut multiplier = 1i64;
    if i < b.len() {
        let unit = match b[i].to_ascii_lowercase() {
            b'k' => Some(1024),
            b'm' => Some(1024 * 1024),
            b'g' => Some(1024 * 1024 * 1024),
            _ => None,
        };
        if let Some(unit) = unit {
            multiplier = unit;
            i += 1;
            if i < b.len() && b[i].eq_ignore_ascii_case(&b'i') {
                i += 1;
            }
        }
    }
    if i < b.len() && b[i].eq_ignore_ascii_case(&b'b') {
        i += 1;
    }
    while i < b.len() && is_java_space(b[i]) {
        i += 1;
    }
    (i == b.len()).then_some((digits, multiplier))
}
