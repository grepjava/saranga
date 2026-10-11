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

    /// Whether `address` is matched by `pattern` (`AddressImpl.matches`,
    /// with `this` = `address` and `otherAddr` = `pattern`).
    pub fn matches(&self, address: &str, pattern: &str) -> bool {
        if address == pattern {
            return true;
        }
        let this: Vec<&str> = address.split(self.delimiter).collect();
        let other: Vec<&str> = pattern.split(self.delimiter).collect();
        let is = |part: &str, c: char| part.len() == c.len_utf8() && part.starts_with(c);

        let (mut ti, mut oi) = (0, 0);
        while oi < other.len() {
            if ti >= this.len() {
                // Only a trailing any-words wildcard matches nothing more.
                return oi == other.len() - 1 && other[oi].starts_with(self.any_words);
            }
            let other_curr = other[oi];
            if is(other_curr, self.single_word) {
                ti += 1;
                oi += 1;
                continue;
            }
            if is(other_curr, self.any_words) {
                if oi == other.len() - 1 {
                    return true;
                }
                let other_next = other[oi + 1];
                // Skip parts of this address until one matches the part
                // after the wildcard.
                let mut this_curr = Some(this[ti]);
                let mut this_next = if ti < this.len() - 1 {
                    Some(this[ti + 1])
                } else {
                    this_curr
                };
                while let Some(curr) = this_curr {
                    if curr == other_next {
                        break;
                    }
                    ti += 1;
                    this_curr = this_next;
                    this_next = (this.len() - 1 > ti).then(|| this[ti + 1]);
                }
                if this_curr.is_none() {
                    return false;
                }
                oi += 1;
                continue;
            }
            if this[ti] != other_curr {
                return false;
            }
            ti += 1;
            oi += 1;
        }
        ti == this.len()
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
}
