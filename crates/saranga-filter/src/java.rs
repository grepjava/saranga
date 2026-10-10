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

/// `Long.parseLong(s, 10)` and friends. Rust's integer parsing accepts the
/// same syntax as Java's for ASCII input: an optional sign and at least one
/// digit, no whitespace. (Java also accepts non-ASCII Unicode digits; those
/// are rejected here.)
pub(crate) fn parse_int<T: std::str::FromStr>(s: &str) -> Option<T> {
    s.parse().ok()
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
        // Includes hexadecimal floating-point literals, which are not
        // supported here.
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

/// `Double.valueOf(String)`.
pub(crate) fn parse_double(s: &str) -> Option<f64> {
    match read_java_float(s)? {
        JavaFloat::NaN => Some(f64::NAN),
        JavaFloat::Infinite { negative } => Some(if negative {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        }),
        JavaFloat::Finite(text) => text.parse().ok(),
    }
}

/// `Float.valueOf(String)`.
pub(crate) fn parse_float(s: &str) -> Option<f32> {
    match read_java_float(s)? {
        JavaFloat::NaN => Some(f32::NAN),
        JavaFloat::Infinite { negative } => Some(if negative {
            f32::NEG_INFINITY
        } else {
            f32::INFINITY
        }),
        JavaFloat::Finite(text) => text.parse().ok(),
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
pub(crate) fn double_to_string(v: f64) -> String {
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
pub(crate) fn float_to_string(v: f32) -> String {
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
