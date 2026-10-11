//! `java.util.Properties.load(InputStream)`: the format of
//! `artemis-users.properties` and `artemis-roles.properties`.
//!
//! The bytes are ISO-8859-1, as `load(InputStream)` reads them; other
//! characters must be written as `\uXXXX`. A logical line continues over
//! natural lines ending in an odd number of backslashes; lines whose first
//! non-blank character is `#` or `!` are comments. The key ends at the first
//! unescaped `=`, `:` or blank, and the value is the rest of the line after
//! the separator and any blanks around it.

/// The entries of a properties file, in file order. A key that appears
/// twice keeps its last value but its first position, as a `Hashtable` put
/// does for the value.
pub fn parse(bytes: &[u8]) -> Vec<(String, String)> {
    let text: Vec<char> = bytes.iter().map(|&b| b as char).collect();
    let mut out: Vec<(String, String)> = Vec::new();
    for line in logical_lines(&text) {
        let (key, value) = split_entry(&line);
        match out.iter_mut().find(|(k, _)| *k == key) {
            Some(entry) => entry.1 = value,
            None => out.push((key, value)),
        }
    }
    out
}

fn is_blank(c: char) -> bool {
    c == ' ' || c == '\t' || c == '\u{c}'
}

/// `Properties.LineReader.readLine`: logical lines with leading blanks,
/// comments, blank lines and continuation backslashes removed, escapes
/// still in place.
fn logical_lines(text: &[char]) -> Vec<Vec<char>> {
    let mut lines = Vec::new();
    let mut i = 0;
    let n = text.len();
    while i < n {
        // The start of a natural line that is not a continuation.
        while i < n && is_blank(text[i]) {
            i += 1;
        }
        if i >= n {
            break;
        }
        if text[i] == '\n' || text[i] == '\r' {
            i += 1;
            continue;
        }
        if text[i] == '#' || text[i] == '!' {
            while i < n && text[i] != '\n' && text[i] != '\r' {
                i += 1;
            }
            continue;
        }
        let mut line = Vec::new();
        loop {
            let start = i;
            while i < n && text[i] != '\n' && text[i] != '\r' {
                i += 1;
            }
            let natural = &text[start..i];
            let backslashes = natural.iter().rev().take_while(|&&c| c == '\\').count();
            if backslashes % 2 == 1 {
                line.extend_from_slice(&natural[..natural.len() - 1]);
                // Skip the line terminator, then the next line's leading
                // blanks.
                if i < n && text[i] == '\r' {
                    i += 1;
                }
                if i < n && text[i] == '\n' {
                    i += 1;
                }
                while i < n && is_blank(text[i]) {
                    i += 1;
                }
                if i >= n {
                    break;
                }
            } else {
                line.extend_from_slice(natural);
                break;
            }
        }
        lines.push(line);
    }
    lines
}

/// `Properties.load0`: splits a logical line into key and value.
fn split_entry(line: &[char]) -> (String, String) {
    let limit = line.len();
    let mut key_len = 0;
    let mut value_start = limit;
    let mut has_sep = false;
    let mut preceding_backslash = false;
    while key_len < limit {
        let c = line[key_len];
        if (c == '=' || c == ':') && !preceding_backslash {
            value_start = key_len + 1;
            has_sep = true;
            break;
        } else if is_blank(c) && !preceding_backslash {
            value_start = key_len + 1;
            break;
        }
        if c == '\\' {
            preceding_backslash = !preceding_backslash;
        } else {
            preceding_backslash = false;
        }
        key_len += 1;
    }
    while value_start < limit {
        let c = line[value_start];
        if !is_blank(c) {
            if !has_sep && (c == '=' || c == ':') {
                has_sep = true;
            } else {
                break;
            }
        }
        value_start += 1;
    }
    (
        load_convert(&line[..key_len]),
        load_convert(&line[value_start.min(limit)..]),
    )
}

/// `Properties.loadConvert`: resolves `\uXXXX`, `\t`, `\n`, `\r`, `\f`,
/// and `\x` for any other `x`.
fn load_convert(s: &[char]) -> String {
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        let c = s[i];
        i += 1;
        if c != '\\' || i >= s.len() {
            if c != '\\' {
                out.push(c);
            }
            continue;
        }
        let e = s[i];
        i += 1;
        match e {
            'u' => {
                let digits: String = s[i..(i + 4).min(s.len())].iter().collect();
                // Java throws on a malformed escape; keep the text instead.
                match (digits.len() == 4)
                    .then(|| u16::from_str_radix(&digits, 16).ok())
                    .flatten()
                {
                    Some(unit) => {
                        i += 4;
                        push_utf16(&mut out, unit, s, &mut i);
                    }
                    None => out.push('u'),
                }
            }
            't' => out.push('\t'),
            'r' => out.push('\r'),
            'n' => out.push('\n'),
            'f' => out.push('\u{c}'),
            other => out.push(other),
        }
    }
    out
}

/// Appends a UTF-16 unit from a `\u` escape, pairing a high surrogate with
/// a following `\uDCxx` escape.
fn push_utf16(out: &mut String, unit: u16, s: &[char], i: &mut usize) {
    if (0xD800..0xDC00).contains(&unit) && *i + 6 <= s.len() && s[*i] == '\\' && s[*i + 1] == 'u' {
        let digits: String = s[*i + 2..*i + 6].iter().collect();
        if let Ok(low) = u16::from_str_radix(&digits, 16)
            && (0xDC00..0xE000).contains(&low)
            && let Some(Ok(c)) = char::decode_utf16([unit, low]).next()
        {
            out.push(c);
            *i += 6;
            return;
        }
    }
    out.push(char::from_u32(unit as u32).unwrap_or('\u{fffd}'));
}

/// A properties file edited in place, as commons-configuration's
/// `PropertiesConfiguration` with its layout edits the users and roles
/// files for `addUser` and the like: comments, blank lines and the order of
/// the properties stay; a changed property keeps its place and separator,
/// a new one goes at the end.
#[derive(Debug, Clone, Default)]
pub struct Document {
    /// The natural lines, without their terminators.
    lines: Vec<String>,
    newline: &'static str,
}

/// A property of a [`Document`]: its key and value, and the natural lines
/// it spans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentEntry {
    pub key: String,
    pub value: String,
    first: usize,
    last: usize,
}

impl Document {
    /// Reads ISO-8859-1 bytes; the file's line terminator is kept.
    pub fn parse(bytes: &[u8]) -> Self {
        let text: String = bytes.iter().map(|&b| b as char).collect();
        let newline = if text.contains("\r\n") {
            "\r\n"
        } else if text.contains('\n') || !cfg!(windows) {
            "\n"
        } else {
            "\r\n"
        };
        let mut lines: Vec<String> = text
            .split('\n')
            .map(|l| l.strip_suffix('\r').unwrap_or(l).to_owned())
            .collect();
        if lines.last().is_some_and(String::is_empty) {
            lines.pop();
        }
        Self { lines, newline }
    }

    /// The properties, in file order, each occurrence of a repeated key
    /// on its own.
    pub fn entries(&self) -> Vec<DocumentEntry> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < self.lines.len() {
            let first = i;
            let trimmed: Vec<char> = self.lines[i].chars().skip_while(|&c| is_blank(c)).collect();
            if trimmed.is_empty() || trimmed[0] == '#' || trimmed[0] == '!' {
                i += 1;
                continue;
            }
            let mut logical: Vec<char> = Vec::new();
            let mut natural = trimmed;
            loop {
                let backslashes = natural.iter().rev().take_while(|&&c| c == '\\').count();
                if backslashes % 2 == 1 && i + 1 < self.lines.len() {
                    logical.extend_from_slice(&natural[..natural.len() - 1]);
                    i += 1;
                    natural = self.lines[i].chars().skip_while(|&c| is_blank(c)).collect();
                } else {
                    if backslashes % 2 == 1 {
                        natural.pop();
                    }
                    logical.extend_from_slice(&natural);
                    break;
                }
            }
            let (key, value) = split_entry(&logical);
            out.push(DocumentEntry {
                key,
                value,
                first,
                last: i,
            });
            i += 1;
        }
        out
    }

    pub fn is_empty(&self) -> bool {
        self.entries().is_empty()
    }

    /// `getKeys`: each key once, in file order.
    pub fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = Vec::new();
        for e in self.entries() {
            if !keys.contains(&e.key) {
                keys.push(e.key);
            }
        }
        keys
    }

    /// `getList`: every value of `key`.
    pub fn values(&self, key: &str) -> Vec<String> {
        self.entries()
            .into_iter()
            .filter(|e| e.key == key)
            .map(|e| e.value)
            .collect()
    }

    /// `getString`: the first value of `key`.
    pub fn get(&self, key: &str) -> Option<String> {
        self.values(key).into_iter().next()
    }

    /// `setProperty`: the first occurrence takes the value, keeping its
    /// key and separator as written; the others go. A new key is added.
    pub fn set(&mut self, key: &str, value: &str) {
        let entries: Vec<DocumentEntry> = self
            .entries()
            .into_iter()
            .filter(|e| e.key == key)
            .collect();
        let Some(first) = entries.first() else {
            self.add(key, value);
            return;
        };
        let prefix = separator_prefix(&self.lines[first.first]);
        let line = format!("{prefix}{}", escape_value(value));
        let (start, end) = (first.first, first.last);
        for e in entries.iter().skip(1).rev() {
            self.lines.drain(e.first..=e.last);
        }
        self.lines.splice(start..=end, [line]);
    }

    /// `addProperty`: another occurrence, at the end.
    pub fn add(&mut self, key: &str, value: &str) {
        self.lines
            .push(format!("{} = {}", escape_key(key), escape_value(value)));
    }

    /// `clearProperty`: every occurrence goes.
    pub fn clear(&mut self, key: &str) {
        for e in self.entries().into_iter().filter(|e| e.key == key).rev() {
            self.lines.drain(e.first..=e.last);
        }
    }

    /// `setHeader` on an emptied file: the header comment, then a blank
    /// line.
    pub fn set_header(&mut self, header: &str) {
        self.lines = header.lines().map(str::to_owned).collect();
        self.lines.push(String::new());
    }

    /// The file's bytes, ISO-8859-1 (other characters were escaped).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for l in &self.lines {
            out.extend(l.chars().map(|c| c as u32 as u8));
            out.extend_from_slice(self.newline.as_bytes());
        }
        out
    }
}

/// The text of a property line up to its value: indentation, key and
/// separator as written.
fn separator_prefix(line: &str) -> String {
    let chars: Vec<char> = line.chars().collect();
    let mut i = chars.iter().take_while(|&&c| is_blank(c)).count();
    let mut preceding_backslash = false;
    let mut has_sep = false;
    while i < chars.len() {
        let c = chars[i];
        if (c == '=' || c == ':') && !preceding_backslash {
            has_sep = true;
            i += 1;
            break;
        } else if is_blank(c) && !preceding_backslash {
            i += 1;
            break;
        }
        preceding_backslash = c == '\\' && !preceding_backslash;
        i += 1;
    }
    while i < chars.len() {
        let c = chars[i];
        if !is_blank(c) {
            if !has_sep && (c == '=' || c == ':') {
                has_sep = true;
            } else {
                break;
            }
        }
        i += 1;
    }
    let prefix: String = chars[..i].iter().collect();
    if has_sep || prefix.ends_with(is_blank) {
        prefix
    } else {
        format!("{prefix} = ")
    }
}

/// `PropertiesWriter.escapeKey`: separators, blanks and comment markers
/// get a backslash.
fn escape_key(key: &str) -> String {
    let mut out = String::new();
    for c in key.chars() {
        if matches!(c, '=' | ':' | ' ' | '\t' | '\u{c}' | '#' | '!' | '\\') {
            out.push('\\');
        }
        push_escaped(&mut out, c);
    }
    out
}

/// `PropertiesWriter.escapeValue`: backslashes doubled, control characters
/// as Java escapes, anything outside ASCII as `\uXXXX`.
fn escape_value(value: &str) -> String {
    let mut out = String::new();
    for c in value.chars() {
        if c == '\\' {
            out.push_str("\\\\");
        } else {
            push_escaped(&mut out, c);
        }
    }
    out
}

fn push_escaped(out: &mut String, c: char) {
    match c {
        '\t' => out.push_str("\\t"),
        '\n' => out.push_str("\\n"),
        '\r' => out.push_str("\\r"),
        '\u{c}' => out.push_str("\\f"),
        '\u{8}' => out.push_str("\\b"),
        c if (c as u32) < 32 || (c as u32) > 0x7f => {
            let mut units = [0u16; 2];
            for u in c.encode_utf16(&mut units) {
                out.push_str(&format!("\\u{u:04X}"));
            }
        }
        c => out.push(c),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Vec<(String, String)> {
        parse(s.as_bytes())
    }

    fn kv(k: &str, v: &str) -> (String, String) {
        (k.to_owned(), v.to_owned())
    }

    #[test]
    fn separators_and_blanks() {
        assert_eq!(
            p("a=1\nb = 2\nc:3\nd 4\n  e\t=\t5  \nf\ng=\n"),
            vec![
                kv("a", "1"),
                kv("b", "2"),
                kv("c", "3"),
                kv("d", "4"),
                kv("e", "5  "),
                kv("f", ""),
                kv("g", "")
            ]
        );
    }

    #[test]
    fn comments_and_continuations() {
        assert_eq!(
            p("# c\n! c\n\nkey = a, \\\n     b,\\\n c\nx=\\\\\n"),
            vec![kv("key", "a, b,c"), kv("x", "\\")]
        );
    }

    #[test]
    fn escapes() {
        assert_eq!(
            p("a\\=b=c\\:d\nu=\\u00e9\\t\\x\nCN\\=x,O\\=y=z\r\nlast=1"),
            vec![
                kv("a=b", "c:d"),
                kv("u", "\u{e9}\tx"),
                kv("CN=x,O=y", "z"),
                kv("last", "1")
            ]
        );
    }

    #[test]
    fn repeated_key_keeps_last_value() {
        assert_eq!(p("a=1\nb=2\na=3"), vec![kv("a", "3"), kv("b", "2")]);
    }

    #[test]
    fn latin1_bytes() {
        assert_eq!(parse(b"k=\xe9"), vec![kv("k", "\u{e9}")]);
    }

    #[test]
    fn document_edits_in_place() {
        let mut d = Document::parse(b"# users\n\nadmin=secret\nguest : x\n");
        assert_eq!(d.get("admin").as_deref(), Some("secret"));
        d.set("guest", "y");
        d.add("new", "ENC(1024:ab:cd)");
        d.set("admin", "a\\b");
        assert_eq!(
            String::from_utf8(d.to_bytes()).unwrap(),
            "# users\n\nadmin=a\\\\b\nguest : y\nnew = ENC(1024:ab:cd)\n"
        );
        d.clear("guest");
        assert_eq!(d.keys(), vec!["admin".to_owned(), "new".to_owned()]);
        let mut e = Document::parse(
            b"
",
        );
        assert!(e.is_empty());
        e.set_header("## h");
        e.add("r", "a,b");
        assert_eq!(e.to_bytes(), b"## h\n\nr = a,b\n".to_vec());
    }
}
