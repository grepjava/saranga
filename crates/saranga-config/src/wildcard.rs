//! Ported from `WildcardConfiguration`.

const ESCAPE: char = '\\';

/// The `<wildcard-addresses>` settings: whether wildcard routing is on and
/// which characters act as delimiter, single-word (`*`) and any-words (`#`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WildcardConfiguration {
    pub routing_enabled: bool,
    pub delimiter: char,
    pub any_words: char,
    pub single_word: char,
}

impl Default for WildcardConfiguration {
    fn default() -> Self {
        Self {
            routing_enabled: true,
            delimiter: '.',
            any_words: '#',
            single_word: '*',
        }
    }
}

impl WildcardConfiguration {
    /// True if `input` contains an unescaped single-word or any-words char.
    pub fn is_wild(&self, input: &str) -> bool {
        let mut prev: Option<char> = None;
        for c in input.chars() {
            if (c == self.single_word || c == self.any_words) && prev != Some(ESCAPE) {
                return true;
            }
            prev = Some(c);
        }
        false
    }

    /// True if `c` is the single-word, any-words or delimiter char.
    pub fn contains(&self, c: char) -> bool {
        c == self.single_word || c == self.any_words || c == self.delimiter
    }

    /// Rewrites `input` from this syntax into `target`'s. Characters that are
    /// special in `target` but not here are escaped with `\`; input that is
    /// already escaped is converted and then unescaped.
    pub fn convert(&self, input: &str, target: &WildcardConfiguration) -> String {
        if self == target {
            return input.to_owned();
        }
        let escaped = self.is_escaped(input);
        let mut result: Vec<char> = if escaped {
            input.chars().collect()
        } else {
            target.escape(input, self).chars().collect()
        };
        replace_char(&mut result, self.delimiter, target.delimiter);
        replace_char(&mut result, self.single_word, target.single_word);
        replace_char(&mut result, self.any_words, target.any_words);
        let result: String = result.into_iter().collect();
        if escaped {
            self.unescape(&result)
        } else {
            result
        }
    }

    fn escape(&self, input: &str, from: &WildcardConfiguration) -> String {
        let mut result = input.replace(ESCAPE, "\\\\");
        if self.delimiter != from.delimiter {
            result = result.replace(self.delimiter, &format!("{ESCAPE}{}", self.delimiter));
        }
        if self.single_word != from.single_word {
            result = result.replace(self.single_word, &format!("{ESCAPE}{}", self.single_word));
        }
        if self.any_words != from.any_words {
            result = result.replace(self.any_words, &format!("{ESCAPE}{}", self.any_words));
        }
        result
    }

    fn unescape(&self, input: &str) -> String {
        input
            .replace("\\\\", "\\")
            .replace(
                &format!("{ESCAPE}{}", self.delimiter),
                &self.delimiter.to_string(),
            )
            .replace(
                &format!("{ESCAPE}{}", self.single_word),
                &self.single_word.to_string(),
            )
            .replace(
                &format!("{ESCAPE}{}", self.any_words),
                &self.any_words.to_string(),
            )
    }

    fn is_escaped(&self, input: &str) -> bool {
        let chars: Vec<char> = input.chars().collect();
        chars
            .windows(2)
            .any(|w| w[0] == ESCAPE && self.contains(w[1]))
    }
}

/// Replaces `from` with `to` except where it is preceded by an escape.
fn replace_char(chars: &mut [char], from: char, to: char) {
    if from == to {
        return;
    }
    for i in 0..chars.len() {
        if chars[i] == from && (i == 0 || chars[i - 1] != ESCAPE) {
            chars[i] = to;
        }
    }
}
