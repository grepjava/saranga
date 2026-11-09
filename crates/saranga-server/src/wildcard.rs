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
}

impl Default for WildcardConfig {
    fn default() -> Self {
        Self {
            routing_enabled: true,
            delimiter: '.',
            any_words: '#',
            single_word: '*',
        }
    }
}

impl WildcardConfig {
    /// `isWild`: an any-words or single-word character not escaped with
    /// `\`.
    pub fn is_wild(&self, address: &str) -> bool {
        let mut previous = None;
        for c in address.chars() {
            if (c == self.any_words || c == self.single_word) && previous != Some(ESCAPE) {
                return true;
            }
            previous = Some(c);
        }
        false
    }

    /// Splits an address into words on the delimiter, leaving an escaped
    /// delimiter (`\.`) part of its word rather than a word boundary, so a
    /// literal delimiter in a word (as an MQTT topic level containing a `.`
    /// becomes) keeps the word whole. (`AddressImpl` splits on every
    /// delimiter, so `a\.b.c` is read as three words instead of two.)
    fn words<'a>(&self, address: &'a str) -> Vec<&'a str> {
        let mut parts = Vec::new();
        let mut start = 0;
        let mut escaped = false;
        for (i, c) in address.char_indices() {
            if escaped {
                escaped = false;
            } else if c == ESCAPE {
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
    /// matches zero or more. Matching is a table over the two word lists so a
    /// wildcard whose first candidate word leaves the rest unmatched tries the
    /// next (`AddressImpl` takes the first and never backtracks, and reads a
    /// wildcard after an any-words wildcard as a literal, so `a.b.b.c` misses
    /// `a.#.b.c` and `a.b.c` misses `a.#.*.c`).
    pub fn matches(&self, address: &str, pattern: &str) -> bool {
        if address == pattern {
            return true;
        }
        let this = self.words(address);
        let other = self.words(pattern);
        let is = |part: &str, c: char| part.len() == c.len_utf8() && part.starts_with(c);
        let n = this.len();
        let m = other.len();
        // dp[ti][oi]: can this[ti..] be matched by other[oi..].
        let mut dp = vec![vec![false; m + 1]; n + 1];
        dp[n][m] = true;
        for oi in (0..m).rev() {
            let o = other[oi];
            for ti in (0..=n).rev() {
                dp[ti][oi] = if is(o, self.any_words) {
                    // Zero words (move past the wildcard) or one more word
                    // (consume this[ti] and stay on the wildcard).
                    dp[ti][oi + 1] || (ti < n && dp[ti + 1][oi])
                } else if is(o, self.single_word) {
                    ti < n && dp[ti + 1][oi + 1]
                } else {
                    ti < n && this[ti] == o && dp[ti + 1][oi + 1]
                };
            }
        }
        dp[0][0]
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
}
