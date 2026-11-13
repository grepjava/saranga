//! `java.util.regex.Pattern` syntax, as `JMXAccessControlList` compiles a
//! `management.xml` key, rewritten for `fancy_regex`. What Java spells
//! differently is rewritten to the same meaning: `\Q...\E` quoting, octal,
//! `\x`, `\u` and `\c` escapes, the ASCII-only `\d`, `\s`, `\w` and POSIX
//! `\p{...}` classes, `\h`, `\v`, `\R` and `\Z`, `.` not matching a line
//! terminator, and the ASCII word boundary. A construct with no equivalent
//! here (`\G`, `\X`, `\N{...}`, `\b{g}`, a Unicode block, a `java...`
//! property, the `d`, `s` and `U` flags) is refused rather than given another
//! meaning, as is anything `Pattern.compile` refuses that is noticed here.

/// Java's line terminators, which `.` does not match without `DOTALL`.
const NOT_LINE_TERMINATOR: &str = r"[^\x{0A}\x{0D}\x{85}\x{2028}\x{2029}]";

/// `\h`: horizontal whitespace.
const HORIZONTAL: &str =
    r"\x{20}\x{09}\x{A0}\x{1680}\x{180E}\x{2000}-\x{200A}\x{202F}\x{205F}\x{3000}";

/// `\v`: vertical whitespace.
const VERTICAL: &str = r"\x{0A}-\x{0D}\x{85}\x{2028}\x{2029}";

/// The pattern `java` written for `fancy_regex`, or why it cannot be.
pub(crate) fn translate(java: &str) -> Result<String, String> {
    let mut t = Translator {
        chars: java.chars().collect(),
        at: 0,
        out: String::with_capacity(java.len() * 2),
        class_depth: 0,
        groups: 0,
    };
    t.run()?;
    Ok(t.out)
}

struct Translator {
    chars: Vec<char>,
    at: usize,
    out: String,
    /// How deep inside `[...]` the scan is.
    class_depth: usize,
    /// Capturing groups opened so far, for Java's reading of `\nn`.
    groups: usize,
}

impl Translator {
    fn peek(&self, ahead: usize) -> Option<char> {
        self.chars.get(self.at + ahead).copied()
    }

    fn literal(&mut self, c: char) {
        self.out.push_str(&format!(r"\x{{{:X}}}", c as u32));
    }

    /// A class, written nested inside a class (which `fancy_regex` takes
    /// as a union) or on its own outside one.
    fn class(&mut self, items: &str, negated: bool) {
        self.out.push('[');
        if negated {
            self.out.push('^');
        }
        self.out.push_str(items);
        self.out.push(']');
    }

    fn run(&mut self) -> Result<(), String> {
        while let Some(c) = self.peek(0) {
            self.at += 1;
            match c {
                '\\' => self.escape()?,
                '[' => self.open_class(),
                ']' if self.class_depth > 0 => {
                    self.class_depth -= 1;
                    self.out.push(']');
                }
                // The regex crate reads `[:name:]` as a POSIX class, `--` as
                // difference and `~~` as symmetric difference; Java has
                // none of them. `fancy_regex` writes an escaped `:` back
                // bare, so a colon is doubled instead: the set is the same,
                // also as the end of a range, and `[::` names no class.
                ':' if self.class_depth > 0 => self.out.push_str("::"),
                '~' if self.class_depth > 0 => self.literal(c),
                '-' if self.class_depth > 0 && self.peek(0) == Some('-') => {
                    return Err("`--` in a character class".to_owned());
                }
                '.' if self.class_depth == 0 => self.out.push_str(NOT_LINE_TERMINATOR),
                '(' if self.class_depth == 0 => self.group()?,
                _ => self.out.push(c),
            }
        }
        Ok(())
    }

    fn open_class(&mut self) {
        self.class_depth += 1;
        self.out.push('[');
        if self.peek(0) == Some('^') {
            self.at += 1;
            self.out.push('^');
        }
        // A `]` first in a class is one of its characters.
        if self.peek(0) == Some(']') {
            self.at += 1;
            self.literal(']');
        }
    }

    fn group(&mut self) -> Result<(), String> {
        if self.peek(0) != Some('?') {
            self.groups += 1;
            self.out.push('(');
            return Ok(());
        }
        let rest: String = self.chars[self.at..].iter().take(3).collect();
        for kind in ["?:", "?=", "?!", "?>", "?<=", "?<!"] {
            if rest.starts_with(kind) {
                self.at += kind.len();
                self.out.push('(');
                self.out.push_str(kind);
                return Ok(());
            }
        }
        if rest.starts_with("?<") {
            // `(?<name>`.
            self.groups += 1;
            self.out.push('(');
            return Ok(());
        }
        // Inline flags, `(?flags)` or `(?flags:`.
        self.at += 1;
        let mut flags = String::new();
        loop {
            let Some(f) = self.peek(0) else {
                return Err("an unterminated flag group".to_owned());
            };
            self.at += 1;
            match f {
                ')' | ':' => {
                    // `u` (`UNICODE_CASE`) is always on here: case is folded
                    // beyond ASCII.
                    let flags = flags.replace('u', "");
                    let flags = flags.trim_end_matches('-');
                    match (flags.is_empty(), f) {
                        (true, ')') => {}
                        (true, _) => self.out.push_str("(?:"),
                        (false, _) => {
                            self.out.push_str("(?");
                            self.out.push_str(flags);
                            self.out.push(f);
                        }
                    }
                    return Ok(());
                }
                'i' | 'm' | 'x' | 'u' | '-' => flags.push(f),
                'd' | 's' | 'U' => return Err(format!("the `{f}` flag is not supported")),
                _ => return Err(format!("unknown inline flag `{f}`")),
            }
        }
    }

    fn escape(&mut self) -> Result<(), String> {
        let Some(c) = self.peek(0) else {
            return Err("a trailing `\\`".to_owned());
        };
        self.at += 1;
        let in_class = self.class_depth > 0;
        match c {
            '0' => {
                let digit = |t: &Self, i| t.peek(i).and_then(|d: char| d.to_digit(8));
                let Some(first) = digit(self, 0) else {
                    return Err("an illegal octal escape".to_owned());
                };
                let (value, len) = match (digit(self, 1), digit(self, 2)) {
                    (Some(b), Some(c)) if first <= 3 => (first * 64 + b * 8 + c, 3),
                    (Some(b), _) => (first * 8 + b, 2),
                    _ => (first, 1),
                };
                self.at += len;
                self.literal(char::from_u32(value).unwrap_or('\0'));
            }
            '1'..='9' if !in_class => {
                // `Pattern.ref`: further digits belong to the reference
                // while it still names a group opened before it.
                let mut number = c.to_digit(10).unwrap_or(0) as usize;
                while let Some(d) = self.peek(0).and_then(|d| d.to_digit(10)) {
                    let longer = number * 10 + d as usize;
                    if longer > self.groups {
                        break;
                    }
                    number = longer;
                    self.at += 1;
                }
                if number > self.groups {
                    // Java compiles a reference to a missing group; it
                    // never matches.
                    self.out.push_str(r"(?!)");
                } else {
                    self.out.push_str(&format!(r"(?:\{number})"));
                }
            }
            'x' => {
                let hex = if self.peek(0) == Some('{') {
                    let end = self.chars[self.at..]
                        .iter()
                        .position(|&e| e == '}')
                        .ok_or("an unterminated `\\x{`")?;
                    let hex: String = self.chars[self.at + 1..self.at + end].iter().collect();
                    self.at += end + 1;
                    hex
                } else {
                    let hex: String = self.chars[self.at..].iter().take(2).collect();
                    self.at += hex.chars().count();
                    hex
                };
                self.hex_literal(&hex)?;
            }
            'u' => {
                let high = self.hex4()?;
                let value = if (0xD800..0xDC00).contains(&high)
                    && self.peek(0) == Some('\\')
                    && self.peek(1) == Some('u')
                {
                    let mark = self.at;
                    self.at += 2;
                    match self.hex4()? {
                        low @ 0xDC00..0xE000 => 0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00),
                        _ => {
                            self.at = mark;
                            high
                        }
                    }
                } else {
                    high
                };
                let c = char::from_u32(value).ok_or("a lone surrogate escape")?;
                self.literal(c);
            }
            't' => self.literal('\t'),
            'n' => self.literal('\n'),
            'r' => self.literal('\r'),
            'f' => self.literal('\x0C'),
            'a' => self.literal('\x07'),
            'e' => self.literal('\x1B'),
            'c' => {
                let x = self.peek(0).ok_or("an illegal control escape")?;
                self.at += 1;
                let c = char::from_u32(x as u32 ^ 64).ok_or("an illegal control escape")?;
                self.literal(c);
            }
            'd' | 'D' => self.class("0-9", c == 'D'),
            's' | 'S' => self.class(r"\x{20}\x{09}-\x{0D}", c == 'S'),
            'w' | 'W' => self.class("a-zA-Z_0-9", c == 'W'),
            'h' | 'H' => self.class(HORIZONTAL, c == 'H'),
            'v' | 'V' => self.class(VERTICAL, c == 'V'),
            'p' | 'P' => self.property(c == 'P')?,
            'b' | 'B' if !in_class => {
                if self.peek(0) == Some('{') {
                    return Err("`\\b{...}` is not supported".to_owned());
                }
                // Since Java 19 `\b` agrees with the ASCII `\w`.
                self.out.push_str(if c == 'b' {
                    r"(?:(?<=[a-zA-Z_0-9])(?![a-zA-Z_0-9])|(?<![a-zA-Z_0-9])(?=[a-zA-Z_0-9]))"
                } else {
                    r"(?:(?<=[a-zA-Z_0-9])(?=[a-zA-Z_0-9])|(?<![a-zA-Z_0-9])(?![a-zA-Z_0-9]))"
                });
            }
            'A' | 'z' if !in_class => {
                self.out.push('\\');
                self.out.push(c);
            }
            'Z' if !in_class => self
                .out
                .push_str(r"(?=(?:\x{0D}\x{0A}|[\x{0A}\x{0D}\x{85}\x{2028}\x{2029}])?\z)"),
            'R' if !in_class => self
                .out
                .push_str(r"(?:\x{0D}\x{0A}|[\x{0A}-\x{0D}\x{85}\x{2028}\x{2029}])"),
            'k' if !in_class && self.peek(0) == Some('<') => self.out.push_str(r"\k"),
            'Q' => {
                while let Some(q) = self.peek(0) {
                    if q == '\\' && self.peek(1) == Some('E') {
                        self.at += 2;
                        return Ok(());
                    }
                    self.at += 1;
                    self.literal(q);
                }
            }
            c if c.is_ascii_alphanumeric() => {
                return Err(format!("the escape `\\{c}` is not supported"));
            }
            c => self.literal(c),
        }
        Ok(())
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let hex: String = self.chars[self.at..].iter().take(4).collect();
        if hex.len() != 4 {
            return Err("an illegal unicode escape".to_owned());
        }
        self.at += 4;
        u32::from_str_radix(&hex, 16).map_err(|_| "an illegal unicode escape".to_owned())
    }

    fn hex_literal(&mut self, hex: &str) -> Result<(), String> {
        let c = u32::from_str_radix(hex, 16)
            .ok()
            .filter(|_| !hex.is_empty())
            .and_then(char::from_u32)
            .ok_or_else(|| format!("an illegal hexadecimal escape `{hex}`"))?;
        self.literal(c);
        Ok(())
    }

    /// `\p{name}`, `\pL`: the POSIX names are ASCII classes, as in Java;
    /// categories, scripts and binary properties keep their meaning.
    fn property(&mut self, negated: bool) -> Result<(), String> {
        let name: String = if self.peek(0) == Some('{') {
            let end = self.chars[self.at..]
                .iter()
                .position(|&e| e == '}')
                .ok_or("an unterminated `\\p{`")?;
            let name = self.chars[self.at + 1..self.at + end].iter().collect();
            self.at += end + 1;
            name
        } else {
            let c = self.peek(0).ok_or("an incomplete `\\p`")?;
            self.at += 1;
            c.to_string()
        };
        let posix = match name.as_str() {
            "Lower" => Some("a-z"),
            "Upper" => Some("A-Z"),
            "ASCII" => Some(r"\x{00}-\x{7F}"),
            "Alpha" => Some("a-zA-Z"),
            "Digit" => Some("0-9"),
            "Alnum" => Some("a-zA-Z0-9"),
            "Punct" => Some(r"\x{21}-\x{2F}\x{3A}-\x{40}\x{5B}-\x{60}\x{7B}-\x{7E}"),
            "Graph" => Some(r"\x{21}-\x{7E}"),
            "Print" => Some(r"\x{20}-\x{7E}"),
            "Blank" => Some(r"\x{20}\x{09}"),
            "Cntrl" => Some(r"\x{00}-\x{1F}\x{7F}"),
            "XDigit" => Some("0-9a-fA-F"),
            "Space" => Some(r"\x{20}\x{09}-\x{0D}"),
            _ => None,
        };
        if let Some(items) = posix {
            self.class(items, negated);
            return Ok(());
        }
        let lower = name.to_ascii_lowercase();
        if name.starts_with("In") || lower.starts_with("block=") || lower.starts_with("blk=") {
            return Err(format!("the block `\\p{{{name}}}` is not supported"));
        }
        if name.starts_with("java") {
            return Err(format!("the property `\\p{{{name}}}` is not supported"));
        }
        let name = name.strip_prefix("Is").unwrap_or(&name);
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '=' | '-' | ' '))
        {
            return Err(format!("the property `\\p{{{name}}}` is not supported"));
        }
        self.out
            .push_str(&format!(r"\{}{{{name}}}", if negated { 'P' } else { 'p' }));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::translate;

    fn matches(java: &str, input: &str) -> bool {
        let regex = translate(java).unwrap_or_else(|e| panic!("{java}: {e}"));
        fancy_regex::Regex::new(&format!(r"\A(?:{regex})\z"))
            .unwrap_or_else(|e| panic!("{java} as {regex}: {e}"))
            .is_match(input)
            .unwrap()
    }

    #[test]
    fn java_spellings_keep_their_meaning() {
        // Each pair as `Pattern.compile(..).matcher(..).matches()` gives it.
        assert!(matches(r"\Qa.b\E", "a.b"));
        assert!(!matches(r"\Qa.b\E", "axb"));
        assert!(matches(r"\Qa.b", "a.b"));
        assert!(matches(r"[\Q]\E]", "]"));
        assert!(matches(r"\0101\x41\u0041\x{41}", "AAAA"));
        assert!(matches(r"\ud83d\ude00", "\u{1F600}"));
        assert!(matches(r"\cA", "\x01"));
        assert!(matches(r"a\-b\_", "a-b_"));
        assert!(!matches(r"\d", "\u{661}"));
        assert!(!matches(r"\w", "é"));
        assert!(!matches(r"\s", "\u{A0}"));
        assert!(matches(r"\h", "\u{A0}"));
        assert!(!matches(r"\h", "1"));
        assert!(matches(r"\v", "\n"));
        assert!(matches(r"a\R", "a\r\n"));
        assert!(!matches(r"\p{Lower}", "é"));
        assert!(matches(r"\p{Lower}", "e"));
        assert!(matches(r"\p{IsLatin}", "é"));
        assert!(matches(r"\p{IsLu}\p{Lu}\pL", "ÉÉé"));
        assert!(matches(r"\p{IsAlphabetic}", "é"));
        assert!(matches(r"\P{Digit}", "x"));
        assert!(!matches("a.b", "a\rb"));
        assert!(!matches("a.b", "a\nb"));
        assert!(matches("a.b", "a b"));
        assert!(matches("[[:alpha:]]", "l"));
        assert!(!matches("[[:alpha:]]", "b"));
        assert!(matches("[]a]", "]"));
        assert!(matches("[^]]", "a"));
        assert!(matches("[a&&[a-c]]", "a"));
        assert!(!matches("[a-c&&[^b]]", "b"));
        assert!(matches("(?x)a b", "ab"));
        assert!(matches("(?i)A", "a"));
        assert!(matches("(?iu:A)b", "ab"));
        assert!(matches("(a)\\1", "aa"));
        assert!(matches("(a)\\11", "aa1"));
        assert!(!matches("(a)\\2", "aa"));
        assert!(matches("(?<n>a)\\k<n>", "aa"));
        assert!(matches(r"x\b", "x"));
        assert!(!matches(r"x\b.", "xy"));
        assert!(matches(r"x\b.", "x-"));
        assert!(matches(r"x\b.", "xé"));
        assert!(matches(r"x\B.", "xy"));
        assert!(matches(r"a*+", "aaa"));
    }

    #[test]
    fn constructs_with_no_equivalent_are_refused() {
        for java in [
            r"\y",
            r"\0",
            r"\08",
            r"\c",
            r"\G",
            r"\X",
            r"\N{LATIN SMALL LETTER A}",
            r"\b{g}",
            r"\p{InGreek}",
            r"\p{javaLowerCase}",
            "(?s)a",
            "(?d)a",
            "(?U)a",
            "[a--]",
            r"\",
            r"\E",
            r"\x{110000}",
            r"\ud800",
        ] {
            assert!(translate(java).is_err(), "{java}");
        }
    }
}
