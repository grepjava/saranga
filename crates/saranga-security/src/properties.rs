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
}
