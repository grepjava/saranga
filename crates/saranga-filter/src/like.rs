//! `LIKE` patterns. Ported from `ComparisonExpression.createLike` and
//! `ComparisonExpression.LikeExpression`.
//!
//! Artemis translates the pattern into a `java.util.regex` source string and
//! compiles it. We build the same source string, then read it back with a
//! parser for the handful of constructs the translation can emit (`\A`,
//! `\z`, `\xhh`, `.`, `.*?` and literal characters). Going through the
//! string keeps Artemis's quirks: `Integer.toHexString` does not pad, so an
//! escaped character outside `0x10..=0xff` yields a `\x` escape that is
//! either rejected or captures the wrong characters, exactly as in Java.

use std::sync::OnceLock;

/// `ComparisonExpression.MAX_WILDCARDS`, read once from
/// `ARTEMIS_SELECTOR_MAX_WILDCARDS` (default 5), as Artemis reads it from
/// that environment variable when the system property is not set.
fn max_wildcards() -> usize {
    static MAX: OnceLock<usize> = OnceLock::new();
    *MAX.get_or_init(|| {
        std::env::var("ARTEMIS_SELECTOR_MAX_WILDCARDS")
            .ok()
            .and_then(|v| v.trim().parse::<i32>().ok())
            .map_or(5, |v| v.max(0) as usize)
    })
}

/// The characters `LikeExpression` hex-escapes (`REGEXP_CONTROL_CHARS`).
const REGEXP_CONTROL_CHARS: &[char] = &[
    '.', '\\', '[', ']', '^', '$', '?', '*', '+', '{', '}', '|', '(', ')', ':', '&', '<', '>', '=',
    '!',
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Piece {
    Literal(char),
    /// `.` with `DOTALL`: any one character.
    Any,
    /// `.*?`: any run of characters.
    AnyRun,
}

/// A compiled `LIKE` pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LikePattern {
    pieces: Vec<Piece>,
}

impl LikePattern {
    /// `ComparisonExpression.createLike` plus the `LikeExpression`
    /// constructor. Errors are the exceptions Artemis throws at parse time.
    pub(crate) fn new(pattern: &str, escape: Option<&str>) -> Result<Self, String> {
        let escape = match escape {
            None => None,
            Some(e) => {
                let mut units = e.encode_utf16();
                match (units.next(), units.next()) {
                    (Some(u), None) => Some(u32::from(u)),
                    _ => {
                        return Err(format!(
                            "The ESCAPE string literal is invalid.  It can only be one character.  Literal used: {e}"
                        ));
                    }
                }
            }
        };
        let merged = merge_wildcards(pattern, escape)?;
        let regex = to_regex(&merged, escape);
        Ok(Self {
            pieces: compile(&regex)?,
        })
    }

    /// `Pattern.matcher(value).matches()`.
    pub(crate) fn matches(&self, value: &str) -> bool {
        let text: Vec<char> = value.chars().collect();
        let p = &self.pieces;
        let (mut ti, mut pi) = (0, 0);
        // Position of the last `.*?` and the text index it was tried at.
        let mut backtrack: Option<(usize, usize)> = None;
        while ti < text.len() {
            match p.get(pi) {
                Some(Piece::AnyRun) => {
                    backtrack = Some((pi, ti));
                    pi += 1;
                    continue;
                }
                Some(Piece::Any) => {
                    ti += 1;
                    pi += 1;
                    continue;
                }
                Some(Piece::Literal(c)) if *c == text[ti] => {
                    ti += 1;
                    pi += 1;
                    continue;
                }
                _ => {}
            }
            match backtrack {
                Some((star, start)) => {
                    pi = star + 1;
                    ti = start + 1;
                    backtrack = Some((star, start + 1));
                }
                None => return false,
            }
        }
        p[pi..].iter().all(|piece| *piece == Piece::AnyRun)
    }
}

/// Merges runs of unescaped `%` into one and enforces `MAX_WILDCARDS`.
fn merge_wildcards(pattern: &str, escape: Option<u32>) -> Result<Vec<char>, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut merged = Vec::with_capacity(chars.len());
    let mut last_was_wildcard = false;
    let mut wildcards = 0usize;
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        if escape == Some(u32::from(ch)) && i + 1 < chars.len() {
            merged.push(ch);
            i += 1;
            merged.push(chars[i]);
            last_was_wildcard = false;
        } else if ch == '%' {
            if !last_was_wildcard {
                wildcards += 1;
                merged.push(ch);
            }
            last_was_wildcard = true;
        } else {
            merged.push(ch);
            last_was_wildcard = false;
        }
        i += 1;
    }
    let max = max_wildcards();
    if wildcards > max {
        return Err(format!(
            "Invalid LIKE pattern: too many '%' wildcards (max {max}, found {wildcards})."
        ));
    }
    Ok(merged)
}

/// The regex source `LikeExpression` builds, without the `\A` / `\z`
/// anchors.
fn to_regex(like: &[char], escape: Option<u32>) -> Vec<char> {
    let mut regex = Vec::with_capacity(like.len() * 2);
    let push_hex = |regex: &mut Vec<char>, c: char| {
        regex.extend(['\\', 'x']);
        regex.extend(format!("{:x}", u32::from(c)).chars());
    };
    let mut i = 0;
    while i < like.len() {
        let c = like[i];
        let escape_next = escape == Some(u32::from(c))
            && like
                .get(i + 1)
                .is_some_and(|&n| n == '_' || n == '%' || n == c);
        if escape_next {
            i += 1;
            push_hex(&mut regex, like[i]);
        } else if c == '%' {
            regex.extend(['.', '*', '?']);
        } else if c == '_' {
            regex.push('.');
        } else if REGEXP_CONTROL_CHARS.contains(&c) {
            push_hex(&mut regex, c);
        } else {
            regex.push(c);
        }
        i += 1;
    }
    regex
}

/// Reads the regex source back the way `java.util.regex.Pattern` would.
fn compile(regex: &[char]) -> Result<Vec<Piece>, String> {
    let mut pieces = Vec::with_capacity(regex.len());
    let mut i = 0;
    // The trailing `\z` anchor follows the generated text, so a malformed
    // `\x` escape at the very end sees its backslash.
    let at = |j: usize| -> char { regex.get(j).copied().unwrap_or('\\') };
    while i < regex.len() {
        match regex[i] {
            '\\' => {
                // Only `\x` is ever generated here.
                let (h, l) = (at(i + 2), at(i + 3));
                match (h.to_digit(16), l.to_digit(16)) {
                    (Some(h), Some(l)) if i + 3 < regex.len() => {
                        pieces.push(Piece::Literal(
                            char::from_u32(h * 16 + l).expect("two hex digits are a valid char"),
                        ));
                        i += 4;
                    }
                    _ => return Err("Illegal hexadecimal escape sequence".into()),
                }
            }
            '.' if regex.get(i + 1) == Some(&'*') => {
                pieces.push(Piece::AnyRun);
                i += 3;
            }
            '.' => {
                pieces.push(Piece::Any);
                i += 1;
            }
            c => {
                pieces.push(Piece::Literal(c));
                i += 1;
            }
        }
    }
    Ok(pieces)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn like(pattern: &str, escape: Option<&str>, value: &str) -> bool {
        LikePattern::new(pattern, escape).unwrap().matches(value)
    }

    #[test]
    fn wildcards() {
        assert!(like("%", None, ""));
        assert!(like("a%b%c", None, "aXXbYYc"));
        assert!(!like("a%b%c", None, "aXXbYY"));
        assert!(like("_", None, "\u{1F600}"));
        assert!(like("%1234%", None, "hello1234\n"));
        assert!(like("a.c", None, "a.c"));
        assert!(!like("a.c", None, "abc"));
    }

    #[test]
    fn escapes() {
        assert!(like("!_%", Some("!"), "_foo"));
        assert!(!like("!_%", Some("!"), "foo"));
        // An escape before anything but `_`, `%` or itself is a literal.
        assert!(like("!a", Some("!"), "!a"));
        assert!(LikePattern::new("a", Some("ab")).is_err());
        assert!(LikePattern::new("a", Some("")).is_err());
    }

    #[test]
    fn unpadded_hex_quirk() {
        // Escaping a char above 0xff emits `\x17c`: Java reads `\x17` then `c`.
        assert!(like("\u{17c}\u{17c}", Some("\u{17c}"), "\u{17}c"));
        assert!(!like("\u{17c}\u{17c}", Some("\u{17c}"), "\u{17c}"));
        // Escaping a char below 0x10 emits a one-digit `\x9`, which Java
        // rejects unless a hex digit happens to follow.
        assert!(LikePattern::new("\t\t", Some("\t")).is_err());
        assert!(like("\t\ta", Some("\t"), "\u{9a}"));
    }
}
