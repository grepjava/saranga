//! The Java number parsing and formatting the selector depends on:
//! `Long.valueOf`, `Double.valueOf` / `Float.valueOf`, `Double.toString` /
//! `Float.toString`, and the `BigDecimal` Artemis falls back to for integer
//! literals that do not fit in a `long`.

use std::cmp::Ordering;
use std::fmt;

/// An integer literal too large for a `long`. Java keeps these as a
/// `BigDecimal` with scale 0; only its sign and decimal digits matter here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BigInt {
    negative: bool,
    /// Decimal magnitude without leading zeros.
    digits: String,
}

const LONG_MIN_MAGNITUDE: &str = "9223372036854775808";

impl BigInt {
    /// `new BigDecimal(text)` for an unsigned run of ASCII digits.
    pub(crate) fn from_digits(digits: &str) -> Self {
        let trimmed = digits.trim_start_matches('0');
        Self {
            negative: false,
            digits: if trimmed.is_empty() { "0" } else { trimmed }.to_owned(),
        }
    }

    pub(crate) fn negate(&self) -> Self {
        Self {
            negative: !self.negative && self.digits != "0",
            digits: self.digits.clone(),
        }
    }

    pub(crate) fn is_long_min(&self) -> bool {
        self.negative && self.digits == LONG_MIN_MAGNITUDE
    }

    /// `BigDecimal.longValue()`: the low-order 64 bits of the two's
    /// complement value.
    pub(crate) fn long_value(&self) -> i64 {
        let magnitude = self.digits.bytes().fold(0u64, |acc, d| {
            acc.wrapping_mul(10).wrapping_add(u64::from(d - b'0'))
        });
        let bits = if self.negative {
            magnitude.wrapping_neg()
        } else {
            magnitude
        };
        bits as i64
    }

    /// `BigDecimal.intValue()`: the low-order 32 bits.
    pub(crate) fn int_value(&self) -> i32 {
        self.long_value() as i32
    }

    pub(crate) fn double_value(&self) -> f64 {
        let v: f64 = self.digits.parse().unwrap_or(f64::INFINITY);
        if self.negative { -v } else { v }
    }

    /// `BigDecimal.compareTo`.
    pub(crate) fn compare(&self, other: &Self) -> Ordering {
        let magnitude = self
            .digits
            .len()
            .cmp(&other.digits.len())
            .then_with(|| self.digits.cmp(&other.digits));
        match (self.negative, other.negative) {
            (false, false) => magnitude,
            (true, true) => magnitude.reverse(),
            (false, true) => Ordering::Greater,
            (true, false) => Ordering::Less,
        }
    }
}

impl fmt::Display for BigInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.negative {
            f.write_str("-")?;
        }
        f.write_str(&self.digits)
    }
}

/// The first of each run of ten `char`s `Character.digit(c, 10)` reads as
/// 0 to 9 (JDK 25). A supplementary digit is two `char`s to Java, neither
/// of them a digit.
const DIGIT_ZEROS: [u32; 37] = [
    0x0030, 0x0660, 0x06F0, 0x07C0, 0x0966, 0x09E6, 0x0A66, 0x0AE6, 0x0B66, 0x0BE6, 0x0C66, 0x0CE6,
    0x0D66, 0x0DE6, 0x0E50, 0x0ED0, 0x0F20, 0x1040, 0x1090, 0x17E0, 0x1810, 0x1946, 0x19D0, 0x1A80,
    0x1A90, 0x1B50, 0x1BB0, 0x1C40, 0x1C50, 0xA620, 0xA8D0, 0xA900, 0xA9D0, 0xA9F0, 0xAA50, 0xABF0,
    0xFF10,
];

/// `Character.digit(c, 10)`.
fn java_digit(c: char) -> Option<u32> {
    let c = c as u32;
    DIGIT_ZEROS
        .iter()
        .find(|z| (**z..**z + 10).contains(&c))
        .map(|z| c - z)
}

/// `Long.parseLong(s, 10)`, and `Integer.parseInt`, `Short.parseShort`
/// and `Byte.parseByte` through the range of `T`: an optional sign, then
/// at least one digit as `Character.digit` reads them, with no spaces.
pub fn parse_int<T: TryFrom<i64>>(s: &str) -> Option<T> {
    let (negative, digits) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() {
        return None;
    }
    let mut v: i128 = 0;
    for c in digits.chars() {
        v = v * 10 + i128::from(java_digit(c)?);
        if v > i128::from(i64::MAX) + 1 {
            return None;
        }
    }
    let v = i64::try_from(if negative { -v } else { v }).ok()?;
    T::try_from(v).ok()
}

/// `Boolean.valueOf(String)`.
pub(crate) fn parse_boolean(s: &str) -> bool {
    s.eq_ignore_ascii_case("true")
}

/// The pieces of a Java floating-point string, validated against
/// `FloatingDecimal.readJavaFormatString`.
enum JavaFloat {
    NaN,
    Infinite {
        negative: bool,
    },
    /// A normalised decimal literal Rust's parser accepts.
    Finite(String),
    Hex(HexFloat),
}

fn read_java_float(s: &str) -> Option<JavaFloat> {
    // String.trim(): strips every char <= ' '.
    let s = s.trim_matches(|c: char| c <= ' ');
    let (negative, rest) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    if rest == "NaN" {
        return Some(JavaFloat::NaN);
    }
    if rest == "Infinity" {
        return Some(JavaFloat::Infinite { negative });
    }
    if rest.starts_with("0x") || rest.starts_with("0X") {
        return read_hex_float(negative, rest).map(JavaFloat::Hex);
    }
    let b = rest.as_bytes();
    let mut i = 0;
    let int_start = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    let int_digits = &rest[int_start..i];
    let mut frac_digits = "";
    if i < b.len() && b[i] == b'.' {
        i += 1;
        let frac_start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        frac_digits = &rest[frac_start..i];
    }
    if int_digits.is_empty() && frac_digits.is_empty() {
        return None;
    }
    let mut exponent = 0i64;
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        let mut exp_negative = false;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            exp_negative = b[i] == b'-';
            i += 1;
        }
        let exp_start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            exponent = (exponent * 10 + i64::from(b[i] - b'0')).min(10_000_000);
            i += 1;
        }
        if i == exp_start {
            return None;
        }
        if exp_negative {
            exponent = -exponent;
        }
    }
    // An optional type suffix, and nothing after it.
    if i < b.len() && (i != b.len() - 1 || !matches!(b[i], b'f' | b'F' | b'd' | b'D')) {
        return None;
    }
    let int_digits = if int_digits.is_empty() {
        "0"
    } else {
        int_digits
    };
    let frac_digits = if frac_digits.is_empty() {
        "0"
    } else {
        frac_digits
    };
    let sign = if negative { "-" } else { "" };
    Some(JavaFloat::Finite(format!(
        "{sign}{int_digits}.{frac_digits}e{exponent}"
    )))
}

/// A hexadecimal floating-point string's value: `mantissa * 2^exponent`,
/// the mantissa's first 120 significant bits and whether any nonzero bit
/// was dropped after them.
struct HexFloat {
    negative: bool,
    mantissa: u128,
    exponent: i64,
    sticky: bool,
}

/// `FloatingDecimal.parseHexString`: `0x`, hex digits with an optional
/// point, `p` and a decimal exponent, then an optional type suffix.
fn read_hex_float(negative: bool, rest: &str) -> Option<HexFloat> {
    let b = rest.as_bytes();
    let mut i = 2;
    let (mut mantissa, mut exponent, mut sticky) = (0u128, 0i64, false);
    let (mut digits, mut significant) = (0usize, 0usize);
    let mut point = false;
    while i < b.len() {
        let c = b[i];
        if c == b'.' && !point {
            point = true;
        } else if let Some(d) = (c as char).to_digit(16) {
            digits += 1;
            if point {
                exponent -= 4;
            }
            if significant > 0 || d != 0 {
                if significant < 30 {
                    mantissa = mantissa << 4 | u128::from(d);
                } else {
                    exponent += 4;
                    sticky |= d != 0;
                }
                significant += 1;
            }
        } else {
            break;
        }
        i += 1;
    }
    if digits == 0 || i >= b.len() || !matches!(b[i], b'p' | b'P') {
        return None;
    }
    i += 1;
    let mut exp_negative = false;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        exp_negative = b[i] == b'-';
        i += 1;
    }
    let exp_start = i;
    let mut binary = 0i64;
    while i < b.len() && b[i].is_ascii_digit() {
        binary = (binary * 10 + i64::from(b[i] - b'0')).min(1 << 40);
        i += 1;
    }
    if i == exp_start {
        return None;
    }
    if i < b.len() && (i != b.len() - 1 || !matches!(b[i], b'f' | b'F' | b'd' | b'D')) {
        return None;
    }
    exponent += if exp_negative { -binary } else { binary };
    Some(HexFloat {
        negative,
        mantissa,
        exponent,
        sticky,
    })
}

impl HexFloat {
    /// The bits of the nearest value with `precision` significand bits
    /// (the hidden one included), ties to even, as IEEE 754 lays them out
    /// with `exponent_bits` of exponent.
    fn round(&self, precision: u32, exponent_bits: u32) -> u64 {
        let bias = (1i64 << (exponent_bits - 1)) - 1;
        let sign = u64::from(self.negative) << (precision - 1 + exponent_bits);
        if self.mantissa == 0 {
            return sign;
        }
        let infinity = sign | (((1u64 << exponent_bits) - 1) << (precision - 1));
        let length = i64::from(128 - self.mantissa.leading_zeros());
        let top = length - 1 + self.exponent;
        if top > bias + 1 {
            return infinity;
        }
        // The position of the result's last bit: a normal number keeps
        // `precision` bits, a subnormal stops at the smallest one.
        let lowest = 1 - bias - i64::from(precision - 1);
        let last = (top - i64::from(precision - 1)).max(lowest);
        let shift = last - self.exponent;
        let mut kept = if shift <= 0 {
            self.mantissa << (-shift).min(127)
        } else if shift >= 128 {
            0
        } else {
            self.mantissa >> shift
        };
        if shift > 0 {
            let half = if shift > 128 {
                false
            } else {
                self.mantissa >> (shift - 1) & 1 == 1
            };
            let below = shift > 1
                && (shift > 129 || self.mantissa & ((1u128 << (shift - 1).min(127)) - 1) != 0);
            if half && (below || self.sticky || kept & 1 == 1) {
                kept += 1;
            }
        }
        if kept == 0 {
            return sign;
        }
        let mut last = last;
        if kept >> precision != 0 {
            kept >>= 1;
            last += 1;
        }
        let top = i64::from(127 - kept.leading_zeros() as i32) + last;
        if top > bias {
            return infinity;
        }
        let fraction = (kept as u64) & ((1u64 << (precision - 1)) - 1);
        if kept >> (precision - 1) == 0 {
            // Subnormal: no hidden bit, exponent field zero.
            return sign | fraction;
        }
        sign | (((top + bias) as u64) << (precision - 1)) | fraction
    }
}

/// `Double.valueOf(String)`.
pub fn parse_double(s: &str) -> Option<f64> {
    match read_java_float(s)? {
        JavaFloat::NaN => Some(f64::NAN),
        JavaFloat::Infinite { negative } => Some(if negative {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        }),
        JavaFloat::Finite(text) => text.parse().ok(),
        JavaFloat::Hex(h) => Some(f64::from_bits(h.round(53, 11))),
    }
}

/// `Float.valueOf(String)`.
pub fn parse_float(s: &str) -> Option<f32> {
    match read_java_float(s)? {
        JavaFloat::NaN => Some(f32::NAN),
        JavaFloat::Infinite { negative } => Some(if negative {
            f32::NEG_INFINITY
        } else {
            f32::INFINITY
        }),
        JavaFloat::Finite(text) => text.parse().ok(),
        JavaFloat::Hex(h) => Some(f32::from_bits(h.round(24, 8) as u32)),
    }
}

/// Java's `Double.compare` / `Float.compare` order: -0.0 sorts below 0.0,
/// and NaN equals itself and sorts above everything else.
pub(crate) fn compare_f64(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.total_cmp(&b),
    }
}

pub(crate) fn compare_f32(a: f32, b: f32) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.total_cmp(&b),
    }
}

/// `Double.toString(double)`.
pub fn double_to_string(v: f64) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0.0" } else { "0.0" }.into();
    }
    format_shortest(&format!("{v:e}"))
}

/// `Float.toString(float)`.
pub fn float_to_string(v: f32) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0.0" } else { "0.0" }.into();
    }
    format_shortest(&format!("{v:e}"))
}

/// Lays out Rust's shortest round-trip digits (`{:e}` output such as
/// `-1.2345e-5`) the way Java does: plain notation for magnitudes in
/// [1e-3, 1e7), otherwise `d.dddEn`. This matches the shortest-digit
/// algorithm Java has used since JDK 19.
fn format_shortest(sci: &str) -> String {
    let (negative, sci) = match sci.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, sci),
    };
    let (mantissa, exponent) = sci.split_once('e').unwrap_or((sci, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    if (-3..7).contains(&exponent) {
        if exponent >= 0 {
            let int_len = exponent as usize + 1;
            if digits.len() <= int_len {
                out.push_str(&digits);
                out.extend(std::iter::repeat_n('0', int_len - digits.len()));
                out.push_str(".0");
            } else {
                out.push_str(&digits[..int_len]);
                out.push('.');
                out.push_str(&digits[int_len..]);
            }
        } else {
            out.push_str("0.");
            out.extend(std::iter::repeat_n('0', (-exponent - 1) as usize));
            out.push_str(&digits);
        }
    } else {
        out.push_str(&digits[..1]);
        out.push('.');
        if digits.len() > 1 {
            out.push_str(&digits[1..]);
        } else {
            out.push('0');
        }
        out.push('E');
        out.push_str(&exponent.to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_double_strings() {
        assert_eq!(double_to_string(1.0), "1.0");
        assert_eq!(double_to_string(123.0), "123.0");
        assert_eq!(double_to_string(61.5), "61.5");
        assert_eq!(double_to_string(0.001), "0.001");
        assert_eq!(double_to_string(0.0001), "1.0E-4");
        assert_eq!(double_to_string(1e7), "1.0E7");
        assert_eq!(double_to_string(9999999.0), "9999999.0");
        assert_eq!(double_to_string(-1.25e10), "-1.25E10");
        assert_eq!(double_to_string(-0.0), "-0.0");
        assert_eq!(float_to_string(123.0), "123.0");
        assert_eq!(float_to_string(0.1), "0.1");
    }

    #[test]
    fn java_double_parsing() {
        assert_eq!(parse_double("5."), Some(5.0));
        assert_eq!(parse_double(".5"), Some(0.5));
        assert_eq!(parse_double("5.E1"), Some(50.0));
        assert_eq!(parse_double(" 1.5d "), Some(1.5));
        assert_eq!(parse_double("-Infinity"), Some(f64::NEG_INFINITY));
        assert!(parse_double("NaN").unwrap().is_nan());
        assert_eq!(parse_double("1e99999999999999"), Some(f64::INFINITY));
        assert_eq!(parse_double("1e-99999999999999"), Some(0.0));
        assert_eq!(parse_double("."), None);
        assert_eq!(parse_double("1e"), None);
        assert_eq!(parse_double("inf"), None);
        assert_eq!(parse_double("1.5x"), None);
        assert_eq!(parse_float("1.1"), Some(1.1f32));
    }

    /// Bits `Double.parseDouble` and `Float.parseFloat` give on JDK 25.
    #[test]
    fn java_hex_float_parsing() {
        let cases: [(&str, u64, u32); 28] = [
            ("0x1.8p1", 0x4008000000000000, 0x40400000),
            ("0X.8P-1074", 0x0000000000000000, 0x00000000),
            ("0x1.fffffep127", 0x47efffffe0000000, 0x7f7fffff),
            ("0x1.ffffffp127", 0x47effffff0000000, 0x7f800000),
            ("0x1p-1075", 0x0000000000000000, 0x00000000),
            (
                "0x1.000000000000000000001p-1075",
                0x0000000000000001,
                0x00000000,
            ),
            ("0x1.fffffffffffff8p1023", 0x7ff0000000000000, 0x7f800000),
            ("0x1.fffffffffffff7ffp1023", 0x7fefffffffffffff, 0x7f800000),
            ("-0x1p0d", 0xbff0000000000000, 0xbf800000),
            (" 0x10p0f ", 0x4030000000000000, 0x41800000),
            (
                "0x0.0000000000000000000000000001p120",
                0x4070000000000000,
                0x43800000,
            ),
            (
                "0x1.0000000000000800000000001p0",
                0x3ff0000000000001,
                0x3f800000,
            ),
            ("0x1.00000000000008p0", 0x3ff0000000000000, 0x3f800000),
            ("0x1.00000000000018p0", 0x3ff0000000000002, 0x3f800000),
            ("0x1p-1074", 0x0000000000000001, 0x00000000),
            ("0x1p-149", 0x36a0000000000000, 0x00000001),
            ("0x1.8p-150", 0x3698000000000000, 0x00000001),
            ("0x1p-150", 0x3690000000000000, 0x00000000),
            (
                "0xffffffffffffffffffffffffffffffffffffffffp0",
                0x49f0000000000000,
                0x7f800000,
            ),
            ("0x1p99999999999", 0x7ff0000000000000, 0x7f800000),
            ("0x1p-99999999999", 0x0000000000000000, 0x00000000),
            ("0x0p0", 0x0000000000000000, 0x00000000),
            ("-0x0p5", 0x8000000000000000, 0x80000000),
            ("0x1.p1", 0x4000000000000000, 0x40000000),
            ("0x.1p4", 0x3ff0000000000000, 0x3f800000),
            ("0x1p+3", 0x4020000000000000, 0x41000000),
            ("0x1P-3D", 0x3fc0000000000000, 0x3e000000),
            ("0x123abcp-7F", 0x40c23abc00000000, 0x4611d5e0),
        ];
        for (s, d, f) in cases {
            assert_eq!(parse_double(s).map(f64::to_bits), Some(d), "{s}");
            assert_eq!(parse_float(s).map(f32::to_bits), Some(f), "{s}");
        }
        for s in [
            "0x.p1", "0x1", "0x1.8", "0xp1", "0x1p", "0x1pf", "0x1p1x", "1x",
        ] {
            assert_eq!(parse_double(s), None, "{s}");
        }
    }

    #[test]
    fn java_integer_parsing() {
        assert_eq!(parse_int::<i64>("-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_int::<i64>("9223372036854775808"), None);
        assert_eq!(parse_int::<i32>("+12"), Some(12));
        assert_eq!(parse_int::<i8>("128"), None);
        assert_eq!(parse_int::<i8>("-128"), Some(-128));
        // Arabic-Indic and fullwidth digits, as Character.digit reads them.
        assert_eq!(parse_int::<i32>("\u{663}\u{FF14}"), Some(34));
        assert_eq!(parse_int::<i32>("\u{1D7CE}"), None);
        assert_eq!(parse_int::<i32>("-"), None);
        assert_eq!(parse_int::<i32>(" 1"), None);
    }

    #[test]
    fn big_int_wraps_like_big_decimal() {
        let big = BigInt::from_digits("9223372036854775808");
        assert_eq!(big.long_value(), i64::MIN);
        assert!(big.negate().is_long_min());
        let two_pow_64 = BigInt::from_digits("18446744073709551616");
        assert_eq!(two_pow_64.long_value(), 0);
        assert_eq!(two_pow_64.int_value(), 0);
        assert_eq!(big.compare(&two_pow_64), Ordering::Less);
        assert_eq!(big.negate().compare(&big), Ordering::Less);
    }
}
