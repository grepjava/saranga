//! Ported from `Match` and `HierarchicalObjectRepository.MatchComparator`.
//!
//! Artemis compiles each match string into a `java.util.regex.Pattern`:
//! the delimiter-plus-any-words suffix collapses to any-words, `.` and `$`
//! are escaped, single-word becomes `[^D]+` and any-words becomes `(D.+)*`
//! (where `D` is the delimiter). Saranga evaluates the same language with a
//! small backtracking matcher instead of a regex engine. The deliberate
//! differences: other regex metacharacters in a match (`+`, `?`, `(`, `[`,
//! `|`, ...) are matched literally here, while Java would interpret them;
//! and with `$` configured as the delimiter, Java leaves it unescaped (an
//! end-of-input anchor, so such matches never match), while Saranga treats
//! it as the delimiter.

use std::cmp::Ordering;

use crate::wildcard::WildcardConfiguration;
use crate::{ConfigError, Result, java_len, java_split};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token {
    Literal(char),
    /// `[^D]+`
    SingleWord,
    /// `(D.+)*`
    AnyWords,
    /// `.*`, used when the whole match is the any-words char.
    Everything,
}

/// A compiled match string such as `queues.#` or `a.*.c`.
#[derive(Debug, Clone)]
pub struct Match {
    text: String,
    tokens: Vec<Token>,
    delimiter: char,
    literal: bool,
}

impl PartialEq for Match {
    /// Two matches are equal when their match strings are, as in Java.
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text
    }
}

impl Eq for Match {}

impl Match {
    pub fn new(text: &str, wildcards: &WildcardConfiguration) -> Self {
        Self::with_literal(text, wildcards, false)
    }

    /// A literal match is still compiled as a pattern, but the repository
    /// only uses it for an exact lookup of the match string itself.
    pub fn with_literal(text: &str, wildcards: &WildcardConfiguration, literal: bool) -> Self {
        let any = wildcards.any_words;
        let tokens = if text.chars().eq(std::iter::once(any)) {
            vec![Token::Everything]
        } else {
            let collapsed =
                text.replace(&format!("{}{any}", wildcards.delimiter), &any.to_string());
            collapsed
                .chars()
                .map(|c| {
                    if c == wildcards.single_word {
                        Token::SingleWord
                    } else if c == any {
                        Token::AnyWords
                    } else {
                        Token::Literal(c)
                    }
                })
                .collect()
        };
        Self {
            text: text.to_owned(),
            tokens,
            delimiter: wildcards.delimiter,
            literal,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }

    pub fn is_literal(&self) -> bool {
        self.literal
    }

    /// True if `input` matches in full (`Matcher.matches()`).
    pub fn matches(&self, input: &str) -> bool {
        let input: Vec<char> = input.chars().collect();
        self.match_from(0, &input, 0)
    }

    fn match_from(&self, ti: usize, input: &[char], pos: usize) -> bool {
        let Some(&token) = self.tokens.get(ti) else {
            return pos == input.len();
        };
        match token {
            Token::Literal(c) => {
                input.get(pos) == Some(&c) && self.match_from(ti + 1, input, pos + 1)
            }
            Token::SingleWord => {
                let mut end = pos;
                while end < input.len() && input[end] != self.delimiter {
                    end += 1;
                    if self.match_from(ti + 1, input, end) {
                        return true;
                    }
                }
                false
            }
            Token::AnyWords => {
                // Zero repetitions, or a delimiter followed by at least one
                // char; `.+` may itself span delimiters, so one repetition
                // covers every longer case.
                if self.match_from(ti + 1, input, pos) {
                    return true;
                }
                if input.get(pos) != Some(&self.delimiter) {
                    return false;
                }
                let mut end = pos + 1;
                while end < input.len() && !is_line_terminator(input[end]) {
                    end += 1;
                    if self.match_from(ti + 1, input, end) {
                        return true;
                    }
                }
                false
            }
            Token::Everything => {
                let rest = &input[pos..];
                !rest.iter().any(|&c| is_line_terminator(c))
                    && self.match_from(ti + 1, input, input.len())
            }
        }
    }

    /// `Match.verify`: the any-words char may appear only as the last char.
    pub fn verify(text: &str, wildcards: &WildcardConfiguration) -> Result<()> {
        match text.chars().position(|c| c == wildcards.any_words) {
            Some(i) if i < text.chars().count() - 1 => {
                Err(ConfigError::InvalidMatch(text.to_owned()))
            }
            _ => Ok(()),
        }
    }

    /// `MatchComparator.compare`: orders two match strings from most to least
    /// specific. Matches without any-words come first; among those with it the
    /// longer one wins; single-word positions are compared word by word.
    pub fn compare_specificity(a: &str, b: &str, wildcards: &WildcardConfiguration) -> Ordering {
        let any = wildcards.any_words;
        let single = wildcards.single_word;
        let (a_any, b_any) = (a.contains(any), b.contains(any));
        let as_ordering = |n: isize| n.cmp(&0);
        if a_any && !b_any {
            return Ordering::Greater;
        }
        if !a_any && b_any {
            return Ordering::Less;
        }
        if a_any && b_any {
            return as_ordering(java_len(b) as isize - java_len(a) as isize);
        }
        let (a_single, b_single) = (a.contains(single), b.contains(single));
        if a_single && !b_single {
            return Ordering::Greater;
        }
        if !a_single && b_single {
            return Ordering::Less;
        }
        if a_single && b_single {
            let single_str = single.to_string();
            let left = java_split(a, wildcards.delimiter);
            let right = java_split(b, wildcards.delimiter);
            for (i, l) in left.iter().enumerate() {
                let Some(r) = right.get(i) else {
                    return Ordering::Less;
                };
                let (l_single, r_single) = (*l == single_str, *r == single_str);
                if l_single && !r_single {
                    return Ordering::Greater;
                }
                if !l_single && r_single {
                    return Ordering::Less;
                }
            }
        }
        as_ordering(java_len(a) as isize - java_len(b) as isize)
    }
}

/// Chars that `.` does not match in a Java regex without DOTALL.
fn is_line_terminator(c: char) -> bool {
    matches!(c, '\n' | '\r' | '\u{0085}' | '\u{2028}' | '\u{2029}')
}
