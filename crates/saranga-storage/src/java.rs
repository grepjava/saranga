//! Java library behaviour that `artemis data print` output depends on:
//! `HashMap` iteration order, `Double.toString`, `Date.toString`, Artemis's
//! `UUID`, `ByteUtil.bytesToHex` and URL-safe Base64.

use saranga_buffer::SimpleString;

/// Java text for UTF-16 code units, as a UTF-8 `PrintStream` writes it:
/// unpaired surrogates become `?`.
pub fn utf16(units: impl IntoIterator<Item = u16>) -> String {
    char::decode_utf16(units)
        .map(|r| r.unwrap_or('?'))
        .collect()
}

/// `SimpleString.toString()`.
pub fn simple_string(s: &SimpleString) -> String {
    s.decode_lossy('?')
}

/// `String.valueOf(simpleString)`: `null` for null.
pub fn nullable(s: Option<&SimpleString>) -> String {
    s.map_or_else(|| "null".to_owned(), simple_string)
}

/// `SimpleString.hashCode()`: `h = 31 * h + b` over the signed bytes.
pub fn simple_string_hash(s: &SimpleString) -> i32 {
    s.as_bytes().iter().fold(0i32, |h, &b| {
        h.wrapping_mul(31).wrapping_add(b as i8 as i32)
    })
}

/// `String.hashCode()`.
pub fn string_hash(units: &[u16]) -> i32 {
    units
        .iter()
        .fold(0i32, |h, &c| h.wrapping_mul(31).wrapping_add(c as i32))
}

/// `Long.hashCode(v)`.
pub fn long_hash(v: i64) -> i32 {
    (v ^ ((v as u64) >> 32) as i64) as i32
}

pub use saranga_buffer::java_hash::hash_map_order;

/// Keys of a Java `HashMap<Long, _>` in iteration order, given the keys in
/// insertion order (duplicates ignored).
pub fn long_map_order(keys: &[i64]) -> Vec<i64> {
    let hashes: Vec<i32> = keys.iter().map(|&k| long_hash(k)).collect();
    hash_map_order(&hashes, None)
        .into_iter()
        .map(|i| keys[i])
        .collect()
}

/// `Double.toString`.
pub fn double(v: f64) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    // Rust's `{:e}` prints the shortest digits that round-trip, as Java does
    // since JDK 19.
    java_decimal(
        &format!("{v:e}"),
        v.abs() >= 1e-3 && v.abs() < 1e7,
        v == 0.0,
    )
}

/// `Float.toString`.
pub fn float(v: f32) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    java_decimal(
        &format!("{v:e}"),
        v.abs() >= 1e-3 && v.abs() < 1e7,
        v == 0.0,
    )
}

/// Lays out Rust's `d.ddde-x` as Java does: plain notation in
/// `[10^-3, 10^7)`, `d.dddE-x` otherwise; always a fractional digit.
fn java_decimal(sci: &str, plain: bool, zero: bool) -> String {
    let (neg, sci) = match sci.strip_prefix('-') {
        Some(s) => (true, s),
        None => (false, sci),
    };
    let sign = if neg { "-" } else { "" };
    if zero {
        return format!("{sign}0.0");
    }
    let (mantissa, exp) = sci.split_once('e').expect("{:e} has an exponent");
    let exp: i32 = exp.parse().expect("integer exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    if !plain {
        let frac = if digits.len() > 1 { &digits[1..] } else { "0" };
        return format!("{sign}{}.{frac}E{exp}", &digits[..1]);
    }
    if exp >= 0 {
        let int_len = exp as usize + 1;
        let (int, frac) = if digits.len() > int_len {
            (digits[..int_len].to_owned(), digits[int_len..].to_owned())
        } else {
            (format!("{digits:0<int_len$}"), "0".to_owned())
        };
        format!("{sign}{int}.{frac}")
    } else {
        let zeros = "0".repeat((-exp - 1) as usize);
        format!("{sign}0.{zeros}{digits}")
    }
}

/// `new java.util.Date(millis).toString()` in the UTC time zone:
/// `EEE MMM dd HH:mm:ss zzz yyyy`. `artemis data print` uses the JVM's
/// default zone; the golden outputs were produced in UTC.
pub fn date(millis: i64) -> String {
    date_at_offset(millis, 0)
}

/// `new java.util.Date(millis).toString()` in a zone `offset` seconds
/// from UTC, named as Java names a zone without an abbreviation of its
/// own: `UTC`, else `GMT+08:00`.
pub fn date_at_offset(millis: i64, offset: i64) -> String {
    const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let secs = millis.div_euclid(1000) + offset;
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let zone = if offset == 0 {
        "UTC".to_owned()
    } else {
        let sign = if offset < 0 { '-' } else { '+' };
        let minutes = offset.abs() / 60;
        format!("GMT{sign}{:02}:{:02}", minutes / 60, minutes % 60)
    };
    format!(
        "{} {} {:02} {:02}:{:02}:{:02} {zone} {}",
        DAYS[days.rem_euclid(7) as usize],
        MONTHS[(m - 1) as usize],
        d,
        tod / 3600,
        tod % 3600 / 60,
        tod % 60,
        y
    )
}

/// Days since 1970-01-01 to a proleptic Gregorian (year, month, day).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// `new UUID(UUID.TYPE_TIME_BASED, bytes).toString()` (Artemis's `UUID`):
/// lower-case hex, `8-4-4-4-12`. The constructor overwrites the version
/// nibble with 1 and the variant bits with `10`, so bytes that are not a
/// time-based UUID print differently from what is stored.
pub fn uuid(bytes: &[u8; 16]) -> String {
    let mut bytes = *bytes;
    bytes[6] = (bytes[6] & 0x0f) | 0x10;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let mut s = String::with_capacity(36);
    for (i, b) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// `ByteUtil.bytesToHex(bytes, groupSize)`: upper-case hex, a space between
/// groups, `[]` when empty.
pub fn bytes_to_hex(bytes: &[u8], group_size: usize) -> String {
    if bytes.is_empty() {
        return "[]".into();
    }
    let mut s = String::new();
    for (j, b) in bytes.iter().enumerate() {
        if j > 0 && j % group_size == 0 {
            s.push(' ');
        }
        s.push_str(&format!("{b:02X}"));
    }
    s
}

/// `ByteUtil.maxString`.
pub fn max_string(value: &str, size: usize) -> String {
    let chars: Vec<char> = value.chars().collect();
    if chars.len() < size {
        value.to_owned()
    } else {
        let head: String = chars[..size / 2].iter().collect();
        let tail: String = chars[chars.len() - size / 2..].iter().collect();
        format!("{head} ... {tail}")
    }
}

/// `java.util.Base64.getUrlEncoder().encodeToString` (padded).
pub fn base64_url(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// `Base64.getUrlDecoder().decode`: URL-safe Base64, padding optional.
pub fn base64_url_decode(text: &str) -> Result<Vec<u8>, String> {
    let value = |c: u8| -> Result<u32, String> {
        Ok(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return Err(format!("Illegal base64 character {c:x}")),
        } as u32)
    };
    let data = text.trim_end_matches('=').as_bytes();
    if data.len() % 4 == 1 {
        return Err("Last unit does not have enough valid bits".to_owned());
    }
    let mut out = Vec::with_capacity(data.len() * 3 / 4);
    for chunk in data.chunks(4) {
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= value(c)? << (18 - 6 * i);
        }
        let bytes = n.to_be_bytes();
        out.extend_from_slice(&bytes[1..chunk.len()]);
    }
    Ok(out)
}

/// `new String(bytes, UTF_8)`: malformed input replaced as the JDK's
/// `String.decodeUTF8_UTF16` does it, which differs from Rust's lossy
/// decoding (an encoded surrogate is one U+FFFD, not three).
pub fn utf8_string(src: &[u8]) -> String {
    const REPL: char = '\u{fffd}';
    let cont = |b: u8| b & 0xc0 == 0x80;
    let malformed3 = |b1: u8, b2: u8| (b1 == 0xe0 && b2 & 0xe0 == 0x80) || !cont(b2);
    let mut out = String::with_capacity(src.len());
    let (mut sp, sl) = (0usize, src.len());
    while sp < sl {
        let b1 = src[sp];
        sp += 1;
        if b1 < 0x80 {
            out.push(b1 as char);
        } else if b1 >> 5 == 0b110 && b1 & 0x1e != 0 {
            if sp < sl {
                let b2 = src[sp];
                sp += 1;
                if !cont(b2) {
                    out.push(REPL);
                    sp -= 1;
                } else {
                    let c = (((b1 & 0x1f) as u32) << 6) | (b2 & 0x3f) as u32;
                    out.push(char::from_u32(c).unwrap_or(REPL));
                }
                continue;
            }
            out.push(REPL);
            break;
        } else if b1 >> 4 == 0b1110 {
            if sp + 1 < sl {
                let (b2, b3) = (src[sp], src[sp + 1]);
                sp += 2;
                if malformed3(b1, b2) || !cont(b3) {
                    out.push(REPL);
                    sp -= 3;
                    // malformed3(src, sp): one byte or two.
                    let n = if malformed3(src[sp], src[sp + 1]) {
                        1
                    } else {
                        2
                    };
                    sp += n;
                } else {
                    let c = (((b1 & 0x0f) as u32) << 12)
                        | (((b2 & 0x3f) as u32) << 6)
                        | (b3 & 0x3f) as u32;
                    // A surrogate is malformed.
                    out.push(char::from_u32(c).unwrap_or(REPL));
                }
                continue;
            }
            if sp < sl && malformed3(b1, src[sp]) {
                out.push(REPL);
                continue;
            }
            out.push(REPL);
            break;
        } else if b1 >> 3 == 0b11110 {
            let malformed4_2 = |b1: u8, b2: u8| {
                (b1 == 0xf0 && !(0x90..=0xbf).contains(&b2))
                    || (b1 == 0xf4 && b2 & 0xf0 != 0x80)
                    || !cont(b2)
            };
            if sp + 2 < sl {
                let (b2, b3, b4) = (src[sp], src[sp + 1], src[sp + 2]);
                sp += 3;
                let uc = (((b1 & 0x07) as u32) << 18)
                    | (((b2 & 0x3f) as u32) << 12)
                    | (((b3 & 0x3f) as u32) << 6)
                    | (b4 & 0x3f) as u32;
                if !cont(b2) || !cont(b3) || !cont(b4) || !(0x10000..=0x10ffff).contains(&uc) {
                    out.push(REPL);
                    sp -= 4;
                    // malformed4(src, sp): one, two or three bytes.
                    let (c1, c2) = (src[sp], src[sp + 1]);
                    sp += if c1 > 0xf4 || malformed4_2(c1, c2) {
                        1
                    } else if !cont(src[sp + 2]) {
                        2
                    } else {
                        3
                    };
                } else {
                    out.push(char::from_u32(uc).unwrap_or(REPL));
                }
                continue;
            }
            if b1 > 0xf4 || (sp < sl && malformed4_2(b1, src[sp])) {
                out.push(REPL);
                continue;
            }
            sp += 1;
            out.push(REPL);
            if sp < sl && !cont(src[sp]) {
                continue;
            }
            break;
        } else {
            out.push(REPL);
        }
    }
    out
}

/// `Arrays.hashCode(byte[])`, used here as a stand-in for Java identity
/// hash codes, which carry no meaning.
pub fn bytes_hash(data: &[u8]) -> i32 {
    data.iter().fold(1i32, |h, &b| {
        h.wrapping_mul(31).wrapping_add(b as i8 as i32)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_print_like_java() {
        for (v, s) in [
            (1.5, "1.5"),
            (-2.25, "-2.25"),
            (1.0e300, "1.0E300"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (100.0, "100.0"),
            (1.0e7, "1.0E7"),
            (9_999_999.0, "9999999.0"),
            (0.001, "0.001"),
            (0.0001, "1.0E-4"),
            (123.456, "123.456"),
            (1.0 / 3.0, "0.3333333333333333"),
            (f64::NAN, "NaN"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            assert_eq!(double(v), s, "{v}");
        }
        assert_eq!(float(-0.25), "-0.25");
        assert_eq!(float(1.5), "1.5");
        assert_eq!(float(0.1), "0.1");
        assert_eq!(float(3.4028235e38), "3.4028235E38");
    }

    #[test]
    fn dates_print_like_java_in_utc() {
        assert_eq!(date(0), "Thu Jan 01 00:00:00 UTC 1970");
        // core-basic's scheduled delivery time and persist-durable's
        // 2100-01-01 schedule.
        assert_eq!(date(1_791_630_307_820), "Sat Oct 10 11:05:07 UTC 2026");
        assert_eq!(date(4_102_444_800_000), "Fri Jan 01 00:00:00 UTC 2100");
        assert_eq!(date(-1), "Wed Dec 31 23:59:59 UTC 1969");
    }

    #[test]
    fn hash_map_order_matches_java() {
        // From persist-durable's data-print.txt: 15 queue IDs inserted in
        // this order iterate as below (table of 32 after one resize).
        let inserted = [
            19, 22, 27, 32, 37, 42, 47, 52, 57, 95, 102, 115, 144, 164, 179,
        ];
        assert_eq!(
            long_map_order(&inserted),
            [
                32, 164, 37, 102, 42, 47, 144, 19, 115, 179, 52, 22, 57, 27, 95
            ]
        );
        // Ordered by bucket, ties in insertion order.
        assert_eq!(long_map_order(&[17, 1, 33]), [17, 1, 33]);
        assert_eq!(long_map_order(&[21, 71, 43]), [21, 71, 43]);
    }

    #[test]
    fn hashes() {
        assert_eq!(
            string_hash(&"abc".encode_utf16().collect::<Vec<_>>()),
            96354
        );
        assert_eq!(long_hash(-1), 0);
        assert_eq!(long_hash(1 << 32), 1);
        assert_eq!(simple_string_hash(&"a".into()), 97 * 31);
    }

    #[test]
    fn formatting_helpers() {
        assert_eq!(
            uuid(&[
                0x10, 0x92, 0xe7, 0x7d, 0xc4, 0x95, 0x11, 0xf1, 0x9d, 0xd2, 0, 0x15, 0x5d, 0x1c,
                0x7a, 0x2e
            ]),
            "1092e77d-c495-11f1-9dd2-00155d1c7a2e"
        );
        // Version and variant bits are forced, as `new UUID(TYPE_TIME_BASED, ..)` does.
        assert_eq!(uuid(&[7; 16]), "07070707-0707-1707-8707-070707070707");
        assert_eq!(bytes_to_hex(&[0x67, 0, 0x6f, 0], 2), "6700 6F00");
        assert_eq!(bytes_to_hex(&[], 2), "[]");
        assert_eq!(base64_url(b"branch-1"), "YnJhbmNoLTE=");
        assert_eq!(base64_url(&[0xfb, 0xff]), "-_8=");
        assert_eq!(base64_url(b""), "");
        assert_eq!(max_string("abcdef", 4), "ab ... ef");
        assert_eq!(max_string("abc", 4), "abc");
        assert_eq!(utf16([0x61, 0xd800, 0x62]), "a?b");
    }
}
