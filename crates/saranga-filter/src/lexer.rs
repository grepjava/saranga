//! The selector tokenizer. Ported from the token section of
//! `StrictParser.jj` and `HyphenatedParser.jj`.
//!
//! JavaCC picks the longest match among all token patterns, breaking ties
//! in declaration order (keywords, then numeric and string literals, then
//! identifiers). The functions below compute each pattern's longest match
//! so the same choice falls out.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Keyword {
    Not,
    And,
    Or,
    Between,
    Like,
    Escape,
    In,
    Is,
    True,
    False,
    Null,
    XPath,
    XQuery,
}

const KEYWORDS: &[(&str, Keyword)] = &[
    ("NOT", Keyword::Not),
    ("AND", Keyword::And),
    ("OR", Keyword::Or),
    ("BETWEEN", Keyword::Between),
    ("LIKE", Keyword::Like),
    ("ESCAPE", Keyword::Escape),
    ("IN", Keyword::In),
    ("IS", Keyword::Is),
    ("TRUE", Keyword::True),
    ("FALSE", Keyword::False),
    ("NULL", Keyword::Null),
    ("XPATH", Keyword::XPath),
    ("XQUERY", Keyword::XQuery),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Tok {
    Kw(Keyword),
    /// `DECIMAL_LITERAL`, including any `l`/`L` suffix.
    Decimal(String),
    /// `HEX_LITERAL`, including the `0x` prefix.
    Hex(String),
    Octal(String),
    Float(String),
    /// A decoded `STRING_LITERAL`.
    Str(String),
    /// An `ID`, or a decoded `QUOTED_ID`.
    Id(String),
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    LParen,
    RParen,
    Comma,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Eof,
}

#[derive(Debug, Clone)]
pub(crate) struct Token {
    pub tok: Tok,
    /// 1-based column (in chars) where the token starts.
    pub column: usize,
}

pub(crate) fn tokenize(text: &str, hyphenated: bool) -> Result<Vec<Token>, String> {
    let s: Vec<char> = text.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    loop {
        // SPECIAL_TOKEN whitespace and SKIP comments.
        while i < s.len() {
            if matches!(s[i], ' ' | '\t' | '\n' | '\r' | '\x0c') {
                i += 1;
            } else if let Some(len) = line_comment(&s, i).or_else(|| block_comment(&s, i)) {
                i += len;
            } else {
                break;
            }
        }
        if i >= s.len() {
            tokens.push(Token {
                tok: Tok::Eof,
                column: i + 1,
            });
            return Ok(tokens);
        }
        let (tok, len) = next_token(&s, i, hyphenated)
            .ok_or_else(|| format!("Lexical error at column {}. Encountered: {:?}", i + 1, s[i]))?;
        tokens.push(Token { tok, column: i + 1 });
        i += len;
    }
}

/// `"--" (~["\n","\r"])* ("\n"|"\r"|"\r\n")`. Without a line terminator
/// this does not match, and the dashes lex as minus signs.
fn line_comment(s: &[char], i: usize) -> Option<usize> {
    if s.get(i) != Some(&'-') || s.get(i + 1) != Some(&'-') {
        return None;
    }
    let mut j = i + 2;
    while j < s.len() && s[j] != '\n' && s[j] != '\r' {
        j += 1;
    }
    match s.get(j) {
        Some('\r') if s.get(j + 1) == Some(&'\n') => Some(j + 2 - i),
        Some(_) => Some(j + 1 - i),
        None => None,
    }
}

/// `/* ... */`, ending at the first `*/`.
fn block_comment(s: &[char], i: usize) -> Option<usize> {
    if s.get(i) != Some(&'/') || s.get(i + 1) != Some(&'*') {
        return None;
    }
    let mut j = i + 2;
    while j + 1 < s.len() {
        if s[j] == '*' && s[j + 1] == '/' {
            return Some(j + 2 - i);
        }
        j += 1;
    }
    None
}

fn next_token(s: &[char], i: usize, hyphenated: bool) -> Option<(Tok, usize)> {
    let c = s[i];
    let next = s.get(i + 1).copied();
    let punct = match (c, next) {
        ('<', Some('>')) => Some((Tok::Ne, 2)),
        ('<', Some('=')) => Some((Tok::Le, 2)),
        ('>', Some('=')) => Some((Tok::Ge, 2)),
        ('<', _) => Some((Tok::Lt, 1)),
        ('>', _) => Some((Tok::Gt, 1)),
        ('=', _) => Some((Tok::Eq, 1)),
        ('(', _) => Some((Tok::LParen, 1)),
        (')', _) => Some((Tok::RParen, 1)),
        (',', _) => Some((Tok::Comma, 1)),
        ('+', _) => Some((Tok::Plus, 1)),
        ('-', _) => Some((Tok::Minus, 1)),
        ('*', _) => Some((Tok::Star, 1)),
        ('/', _) => Some((Tok::Slash, 1)),
        ('%', _) => Some((Tok::Percent, 1)),
        _ => None,
    };
    if punct.is_some() {
        return punct;
    }
    if c.is_ascii_digit() || c == '.' {
        return number(s, i);
    }
    if c == '\'' {
        let len = quoted(s, i, '\'')?;
        return Some((Tok::Str(unquote(&s[i..i + len], '\'')), len));
    }
    if c == '"' && !hyphenated {
        let len = quoted(s, i, '"')?;
        return Some((Tok::Id(unquote(&s[i..i + len], '"')), len));
    }
    if c.is_ascii_alphabetic() || c == '_' || c == '$' {
        let mut j = i + 1;
        while j < s.len()
            && (s[j].is_ascii_alphanumeric()
                || s[j] == '_'
                || s[j] == '$'
                || (hyphenated && s[j] == '-'))
        {
            j += 1;
        }
        let word: String = s[i..j].iter().collect();
        let tok = KEYWORDS
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(&word))
            .map_or(Tok::Id(word), |(_, kw)| Tok::Kw(*kw));
        return Some((tok, j - i));
    }
    None
}

fn digits(s: &[char], mut i: usize, radix: u32) -> usize {
    let start = i;
    while i < s.len() && s[i].is_digit(radix) {
        i += 1;
    }
    i - start
}

/// The longest of `DECIMAL_LITERAL`, `HEX_LITERAL`, `OCTAL_LITERAL` and
/// `FLOATING_POINT_LITERAL`, ties going to the earlier one.
fn number(s: &[char], i: usize) -> Option<(Tok, usize)> {
    let text = |len: usize| -> String { s[i..i + len].iter().collect() };
    let mut best: Option<(u8, usize)> = None;
    let mut offer = |kind: u8, len: usize| {
        if len > 0 && best.is_none_or(|(_, l)| len > l) {
            best = Some((kind, len));
        }
    };

    if matches!(s[i], '1'..='9') {
        let mut len = 1 + digits(s, i + 1, 10);
        if matches!(s.get(i + len), Some('l' | 'L')) {
            len += 1;
        }
        offer(0, len);
    }
    if s[i] == '0' && matches!(s.get(i + 1), Some('x' | 'X')) {
        let hex = digits(s, i + 2, 16);
        if hex > 0 {
            offer(1, 2 + hex);
        }
    }
    if s[i] == '0' {
        offer(2, 1 + digits(s, i + 1, 8));
    }
    offer(3, float(s, i));

    let (kind, len) = best?;
    let t = text(len);
    let tok = match kind {
        0 => Tok::Decimal(t),
        1 => Tok::Hex(t),
        2 => Tok::Octal(t),
        _ => Tok::Float(t),
    };
    Some((tok, len))
}

/// Longest `FLOATING_POINT_LITERAL` match at `i`, or 0.
fn float(s: &[char], i: usize) -> usize {
    let exponent = |j: usize| -> usize {
        if !matches!(s.get(j), Some('e' | 'E')) {
            return 0;
        }
        let mut k = j + 1;
        if matches!(s.get(k), Some('+' | '-')) {
            k += 1;
        }
        let d = digits(s, k, 10);
        if d == 0 { 0 } else { k + d - j }
    };
    let int = digits(s, i, 10);
    if int > 0 {
        let j = i + int;
        if s.get(j) == Some(&'.') {
            let frac = digits(s, j + 1, 10);
            let k = j + 1 + frac;
            return k + exponent(k) - i;
        }
        let e = exponent(j);
        return if e > 0 { int + e } else { 0 };
    }
    if s[i] == '.' {
        let frac = digits(s, i + 1, 10);
        if frac > 0 {
            let k = i + 1 + frac;
            return k + exponent(k) - i;
        }
    }
    0
}

/// Longest match of `q ((q q) | ~[q])* q` at `i`.
fn quoted(s: &[char], i: usize, q: char) -> Option<usize> {
    let mut end = None;
    let mut j = i + 1;
    while j < s.len() {
        if s[j] != q {
            j += 1;
            continue;
        }
        let mut run = 0;
        while j + run < s.len() && s[j + run] == q {
            run += 1;
        }
        if run % 2 == 1 {
            // The last quote of an odd run closes the literal.
            end = Some(j + run);
            break;
        }
        // An even run may be doubled quotes or a close plus a new literal;
        // the longest match keeps going, remembering the latest close.
        end = Some(j + run - 1);
        j += run;
    }
    end.map(|e| e - i)
}

/// Strips the quotes and collapses doubled ones.
fn unquote(image: &[char], q: char) -> String {
    let mut out = String::with_capacity(image.len());
    let mut i = 1;
    while i < image.len() - 1 {
        let c = image[i];
        if c == q {
            i += 1;
        }
        out.push(c);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(text: &str) -> Vec<Tok> {
        tokenize(text, false)
            .unwrap()
            .into_iter()
            .map(|t| t.tok)
            .collect()
    }

    #[test]
    fn numbers() {
        assert_eq!(
            toks("1 10L 0x1F 017 0 1.5 .5 5. 5E10 1.e2"),
            vec![
                Tok::Decimal("1".into()),
                Tok::Decimal("10L".into()),
                Tok::Hex("0x1F".into()),
                Tok::Octal("017".into()),
                Tok::Octal("0".into()),
                Tok::Float("1.5".into()),
                Tok::Float(".5".into()),
                Tok::Float("5.".into()),
                Tok::Float("5E10".into()),
                Tok::Float("1.e2".into()),
                Tok::Eof,
            ]
        );
        // "08" is OCTAL "0" then DECIMAL "8"; "08.5" is one float.
        assert_eq!(
            toks("08 08.5"),
            vec![
                Tok::Octal("0".into()),
                Tok::Decimal("8".into()),
                Tok::Float("08.5".into()),
                Tok::Eof
            ]
        );
    }

    #[test]
    fn strings_and_ids() {
        assert_eq!(
            toks("'it''s' \"a.b\"\"c\" notX NoT"),
            vec![
                Tok::Str("it's".into()),
                Tok::Id("a.b\"c".into()),
                Tok::Id("notX".into()),
                Tok::Kw(Keyword::Not),
                Tok::Eof
            ]
        );
        // "'a''" is the literal 'a' followed by an unterminated quote.
        assert!(tokenize("'a''", false).is_err());
        assert_eq!(quoted(&['\'', 'a', '\'', '\''], 0, '\''), Some(3));
        // Quoted identifiers are not part of the hyphenated grammar.
        assert!(tokenize("\"a\"", true).is_err());
        assert_eq!(tokenize("a-b", true).unwrap()[0].tok, Tok::Id("a-b".into()));
    }

    #[test]
    fn comments() {
        assert_eq!(toks("a /* x */ -- y\n b"), toks("a b"));
        // An unterminated line comment is two minus signs.
        assert_eq!(
            toks("1 -- x"),
            vec![
                Tok::Decimal("1".into()),
                Tok::Minus,
                Tok::Minus,
                Tok::Id("x".into()),
                Tok::Eof
            ]
        );
    }
}
