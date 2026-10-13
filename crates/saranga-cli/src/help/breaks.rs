//! Where Java's line `BreakIterator` (`BreakIterator.getLineInstance()`,
//! the JDK's own rules) lets a line break in ASCII text, which picocli
//! wraps descriptions at: after spaces, after closing punctuation before a
//! word, after a hyphen inside a word, before an opening bracket.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Space,
    /// Letters, digits and the symbols that join words (`/`, `=`, `<`...).
    Word,
    Hyphen,
    /// Closing punctuation: `.,:;!?)]}%` and a closing quote.
    Close,
    /// Opening punctuation, which never ends a line: `([{$'` and an
    /// opening quote.
    Open,
}

fn class(prev: Option<char>, c: char) -> Class {
    match c {
        ' ' => Class::Space,
        '-' => Class::Hyphen,
        '.' | ',' | ':' | ';' | '!' | '?' | ')' | ']' | '}' | '%' => Class::Close,
        '(' | '[' | '{' | '$' | '\'' => Class::Open,
        // A quote opens after a space or an opening and closes elsewhere.
        '"' => match prev {
            None | Some(' ' | '-' | '\'' | '(' | '[' | '{' | '$') => Class::Open,
            _ => Class::Close,
        },
        _ => Class::Word,
    }
}

/// Whether the line may break between `x` and `y`; `w` is the character
/// before `x`.
fn breaks(w: Option<char>, x: char, y: char) -> bool {
    let opening = matches!(y, '(' | '[' | '{');
    match class(w, x) {
        Class::Space => y != ' ',
        Class::Word => opening,
        Class::Open => false,
        Class::Close => {
            // A decimal point or a thousands separator inside a number.
            if matches!(x, '.' | ',') && w.is_some_and(|w| w.is_ascii_digit()) && y.is_ascii_digit()
            {
                return false;
            }
            matches!(class(Some(x), y), Class::Word | Class::Hyphen) || opening
        }
        // Java's iterator breaks after a hyphen inside a word; picocli's
        // wrapping never does.
        Class::Hyphen => opening,
    }
}

/// The byte offsets the line may break at, the end included: each word
/// runs from the previous offset, with the spaces after it.
pub fn line_breaks(text: &str) -> Vec<usize> {
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let mut out = Vec::new();
    for i in 1..chars.len() {
        let w = (i >= 2).then(|| chars[i - 2].1);
        if breaks(w, chars[i - 1].1, chars[i].1) {
            out.push(chars[i].0);
        }
    }
    if !text.is_empty() {
        out.push(text.len());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(text: &str) -> Vec<&str> {
        let mut start = 0;
        line_breaks(text)
            .into_iter()
            .map(|end| {
                let w = &text[start..end];
                start = end;
                w
            })
            .collect()
    }

    /// As `BreakIterator.getLineInstance()` splits them on JDK 21, but for
    /// the hyphens.
    #[test]
    fn as_java_breaks() {
        assert_eq!(
            words("xml:null/etc/bootstrap.xml."),
            ["xml:", "null/etc/bootstrap.", "xml."]
        );
        assert_eq!(
            words("tcp://localhost:61616 if"),
            ["tcp:", "//localhost:", "61616 ", "if"]
        );
        assert_eq!(
            words("(e.g. failover tests)."),
            ["(e.", "g. ", "failover ", "tests)."]
        );
        assert_eq!(
            words("Default=60000 --loop-time, a-b 'x' \"y\" 1.5 foo/bar"),
            [
                "Default=60000 ",
                "--loop-time, ",
                "a-b ",
                "'x' ",
                "\"y\" ",
                "1.5 ",
                "foo/bar"
            ]
        );
    }
}
