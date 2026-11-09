//! Wildcard addresses: a queue bound to `news.#` receives what is sent to
//! `news.europe`. Ported from `AddressImpl.matches` and
//! `WildcardConfiguration`.

/// `WildcardConfiguration.ESCAPE`.
pub const ESCAPE: char = '\\';

/// `WildcardConfiguration`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WildcardConfig {
    pub routing_enabled: bool,
    pub delimiter: char,
    pub any_words: char,
    pub single_word: char,
    /// Whether this syntax escapes a literal delimiter or wildcard with `\`.
    /// Core addresses do; an MQTT topic has no escape, so a `\` in one is a
    /// literal character (converting between the two must not read it as an
    /// escape, or distinct topics collapse to one address).
    pub uses_escape: bool,
}

impl Default for WildcardConfig {
    fn default() -> Self {
        Self {
            routing_enabled: true,
            delimiter: '.',
            any_words: '#',
            single_word: '*',
            uses_escape: true,
        }
    }
}

impl WildcardConfig {
    /// `isWild`: an any-words or single-word character not escaped with
    /// `\`. Only a syntax that escapes (`uses_escape`) reads a `\` as an
    /// escape; in an MQTT topic a `\` is a literal, so `\#` is still wild.
    pub fn is_wild(&self, address: &str) -> bool {
        let mut previous = None;
        for c in address.chars() {
            let escaped = self.uses_escape && previous == Some(ESCAPE);
            if (c == self.any_words || c == self.single_word) && !escaped {
                return true;
            }
            previous = Some(c);
        }
        false
    }

    /// Splits an address into words on the delimiter. In a syntax that
    /// escapes (`uses_escape`), an escaped delimiter (`\.`) stays part of its
    /// word rather than a word boundary, so a literal delimiter in a word (as
    /// an MQTT topic level containing a `.` becomes) keeps the word whole.
    /// (`AddressImpl` splits on every delimiter, so `a\.b.c` is read as three
    /// words instead of two.) In a syntax that does not escape (an MQTT
    /// topic), a `\` is a literal and every delimiter splits, so the MQTT
    /// topic `a\/b/c` is three words as `AddressImpl` reads it, not two.
    fn words<'a>(&self, address: &'a str) -> Vec<&'a str> {
        let mut parts = Vec::new();
        let mut start = 0;
        let mut escaped = false;
        for (i, c) in address.char_indices() {
            if escaped {
                escaped = false;
            } else if self.uses_escape && c == ESCAPE {
                escaped = true;
            } else if c == self.delimiter {
                parts.push(&address[start..i]);
                start = i + c.len_utf8();
            }
        }
        parts.push(&address[start..]);
        parts
    }

    /// Whether `address` is matched by `pattern` (`AddressImpl.matches`, with
    /// `this` = `address` and `otherAddr` = `pattern`).
    ///
    /// The single-word wildcard matches one word; the any-words wildcard
    /// matches zero or more. A mismatch backtracks to let the last any-words
    /// wildcard take one more word, so a wildcard whose first candidate word
    /// leaves the rest unmatched tries the next and a wildcard after an
    /// any-words wildcard is matched as a wildcard (`AddressImpl` takes the
    /// first and never backtracks, and reads such a wildcard as a literal, so
    /// `a.b.b.c` misses `a.#.b.c` and `a.b.c` misses `a.#.*.c`). The two
    /// cursors use constant extra space, so a deep address cannot make a
    /// single match allocate a table quadratic in the word counts.
    pub fn matches(&self, address: &str, pattern: &str) -> bool {
        if address == pattern {
            return true;
        }
        let this = self.words(address);
        let other = self.words(pattern);
        let is = |part: &str, c: char| part.len() == c.len_utf8() && part.starts_with(c);
        let (n, m) = (this.len(), other.len());
        let (mut ti, mut oi) = (0usize, 0usize);
        // The last any-words wildcard and the address word it first stood at,
        // for backtracking: it takes one more word each time the rest fails.
        let mut star: Option<(usize, usize)> = None;
        while ti < n {
            if oi < m
                && (is(other[oi], self.single_word)
                    || (!is(other[oi], self.any_words) && this[ti] == other[oi]))
            {
                ti += 1;
                oi += 1;
            } else if oi < m && is(other[oi], self.any_words) {
                star = Some((oi, ti));
                oi += 1;
            } else if let Some((soi, sti)) = star {
                oi = soi + 1;
                star = Some((soi, sti + 1));
                ti = sti + 1;
            } else {
                return false;
            }
        }
        // A trailing any-words wildcard matches the empty remainder.
        while oi < m && is(other[oi], self.any_words) {
            oi += 1;
        }
        oi == m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `AddressImplTest`, case by case: (address, pattern, matches).
    #[test]
    fn address_impl_test() {
        let cases = [
            ("abcde", "abcde", true),
            ("a.b", "a.b", true),
            ("a.b.c", "a.b.c.d.e.f.g.h.i.j.k.l.m.n.*", false),
            ("a.b.c.d", "a.b.c.*", true),
            ("a.b.x.e", "a.b.c.*", false),
            ("a.b.c.d", "a.b.*.d", true),
            ("a.b.c.x", "a.b.*.d", false),
            ("a.b.c.d.e", "a.b.*.d.*", true),
            ("a.b.c.x.e", "a.b.*.d.*", false),
            ("a.b.c.d.e.f", "a.b.*.d.*.f", true),
            ("a.b.c.x.e.f", "a.b.*.d.*.f", false),
            ("a.b.c.d.e.f", "#", true),
            ("a.b.c.x.e.f", "#", true),
            ("a.b.c.d.e.f", "a.#", true),
            ("a.b.c.x.e.f", "a.#", true),
            ("a.b.c.d.e.f", "#.b.#", true),
            ("a.b.c.x.e.f", "#.b.#", true),
            ("a.b.c.d.e.f", "a.#.b.#", true),
            ("a.b.c.x.e.f", "a.#.b.#", true),
            ("a.b.c.d.e.f", "a.#.c.d.e.f", true),
            ("a.b.c.x.e.f", "a.#.c.d.e.f", false),
            ("a.b.c.d.e.f", "a.#.c.d.e.*", true),
            ("a.b.c.d.e.x", "a.#.c.d.e.*", true),
            ("a.b.c.d.e.f", "a.#.c.d.*.f", true),
            ("a.b.c.d.e.x", "a.#.c.d.*.f", false),
            ("a.b.c", "a.b.c.#", true),
            ("a.b.x.e", "a.b.c.#", false),
            ("usd.stock", "*.stock.#", true),
            ("a.b.x.e", "*.stock.#", false),
            ("a.b.c.d", "a.b.c.*", true),
            ("a.b.x.e", "a.b.c.*", false),
            ("a.b.c.d", "a.b.c#", false),
            ("a.b.c.d", "#a.b.c", false),
            ("a.b.c.d", "#*a.b.c", false),
            ("a.b.c.d", "a.b.c*", false),
            ("a.b.c.d", "*a.b.c", false),
            ("a.b.d", "a.b.#.d", true),
        ];
        let w = WildcardConfig::default();
        for (address, pattern, expected) in cases {
            assert_eq!(
                w.matches(address, pattern),
                expected,
                "{address} vs {pattern}"
            );
        }
    }

    /// An escaped delimiter keeps its word whole, and matching backtracks so a
    /// wildcard whose first candidate word leaves the rest unmatched tries the
    /// next. (`AddressImpl` splits every delimiter and never backtracks.)
    #[test]
    fn escaped_delimiters_and_backtracking() {
        let cases = [
            // An MQTT topic `a.b/c` becomes the address `a\.b.c` (two words),
            // and the subscription `+/c` becomes `*.c`: it matches.
            (r"a\.b.c", "*.c", true),
            (r"a\.b.c", r"a\.b.c", true),
            // `a\.b.c` is two words, so a three-word pattern does not match.
            (r"a\.b.c", "*.*.c", false),
            (r"a\.b.c", "a.b.c", false),
            // Any-words backtracks past its first candidate word.
            ("a.b.b.c", "a.#.b.c", true),
            // A wildcard after any-words is a wildcard, not a literal.
            ("a.b.c", "a.#.*.c", true),
            ("a.b.c.d", "a.#.*.d", true),
            ("x.y.z", "#.*", true),
            ("a.b.c", "a.#.x.c", false),
        ];
        let w = WildcardConfig::default();
        for (address, pattern, expected) in cases {
            assert_eq!(
                w.matches(address, pattern),
                expected,
                "{address} vs {pattern}"
            );
        }
    }

    /// A syntax without an escape (an MQTT topic, `uses_escape: false`) reads
    /// a `\` as an ordinary character: every delimiter splits and a wildcard
    /// after a `\` is still a wildcard, as `AddressImpl` (which never escapes)
    /// reads an MQTT topic. (The escape-aware `words` for core addresses would
    /// absorb the `\/` and miscount the words.)
    #[test]
    fn a_literal_backslash_is_not_an_escape_without_uses_escape() {
        const MQTT: WildcardConfig = WildcardConfig {
            routing_enabled: true,
            delimiter: '/',
            any_words: '#',
            single_word: '+',
            uses_escape: false,
        };
        // `a\/b/c` is three levels (`a\`, `b`, `c`): a `\` does not hide the
        // `/`, so the subscription `+/+/+` (three single words) matches it.
        assert!(MQTT.matches("a\\/b/c", "+/+/+"));
        assert!(MQTT.matches("a\\/b/c", "a\\/#"));
        // A different level count does not match.
        assert!(!MQTT.matches("a\\/b/c", "+/+"));
        // A `\` before a wildcard does not escape it.
        assert!(MQTT.is_wild("a/\\#"));
        assert!(MQTT.is_wild("\\+/b"));
        // With escaping on, the same `\#` is an escaped literal, not wild.
        let core = WildcardConfig::default();
        assert!(!core.is_wild("a.\\#"));
    }

    /// A deep address and pattern match in constant space: a client-controlled
    /// word count must not make one comparison allocate a table quadratic in
    /// the depths (about a gigabyte for two 65 KB strings).
    #[test]
    fn a_deep_match_does_not_allocate_quadratically() {
        let w = WildcardConfig::default();
        let deep = vec!["a"; 40_000].join(".");
        let stars = vec!["*"; 40_000].join(".");
        assert!(w.matches(&deep, "#"), "any-words matches the lot");
        assert!(
            w.matches(&deep, &stars),
            "one single-word per depth matches"
        );
        assert!(
            !w.matches(&deep, &format!("{deep}.b")),
            "a deeper pattern does not match"
        );
    }
}
