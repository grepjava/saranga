//! A streaming XML reader for `data imp`, which reads files of any size
//! (`XMLStreamReader`'s events): elements with their attributes, text,
//! CDATA, the predefined entities and character references. Declarations,
//! comments, processing instructions and DOCTYPEs are skipped.

use std::io::BufRead;

use anyhow::{Context, bail};

#[derive(Debug, PartialEq)]
pub enum Event {
    /// `<name a="v">`, or `<name a="v"/>` (followed by its `End`).
    Start {
        name: String,
        attributes: Vec<(String, String)>,
    },
    End {
        name: String,
    },
    Text(String),
    CData(String),
    Eof,
}

pub struct Reader<R> {
    input: std::io::Bytes<R>,
    peeked: Option<u8>,
    /// An empty element's `End`, still to give.
    pending_end: Option<String>,
    line: usize,
    /// Characters read on the line.
    column: usize,
}

/// An `XMLStreamException` as the JDK's parser reports it.
#[derive(Debug)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "javax.xml.stream.XMLStreamException: {}", self.0)
    }
}

impl std::error::Error for ParseError {}

/// XML's white space (`isWhiteSpace`).
pub fn is_white_space(s: &str) -> bool {
    s.chars().all(|c| matches!(c, ' ' | '\t' | '\r' | '\n'))
}

impl<R: BufRead> Reader<R> {
    pub fn new(input: R) -> Self {
        Self {
            input: input.bytes(),
            peeked: None,
            pending_end: None,
            line: 1,
            column: 0,
        }
    }

    fn next_byte(&mut self) -> anyhow::Result<Option<u8>> {
        let b = match self.peeked.take() {
            Some(b) => Some(b),
            None => self.input.next().transpose().context("reading the input")?,
        };
        match b {
            Some(b'\n') => {
                self.line += 1;
                self.column = 0;
            }
            Some(b) if b & 0xc0 != 0x80 => self.column += 1,
            _ => {}
        }
        Ok(b)
    }

    /// A `ParseError` at the next character.
    fn parse_error(&self, message: &str) -> ParseError {
        ParseError(format!(
            "ParseError at [row,col]:[{},{}]\nMessage: {message}",
            self.line,
            self.column + 1
        ))
    }

    /// `XMLStreamReader.nextTag`: the next start or end tag, past white
    /// space, comments and processing instructions.
    pub fn next_tag(&mut self) -> anyhow::Result<Event> {
        loop {
            match self.next_event()? {
                Event::Text(t) if is_white_space(&t) => {}
                e @ (Event::Start { .. } | Event::End { .. }) => return Ok(e),
                Event::Text(_) | Event::CData(_) => {
                    return Err(self
                        .parse_error("found: CHARACTERS, expected START_ELEMENT or END_ELEMENT")
                        .into());
                }
                Event::Eof => {
                    return Err(self
                        .parse_error("found: END_DOCUMENT, expected START_ELEMENT or END_ELEMENT")
                        .into());
                }
            }
        }
    }

    fn peek(&mut self) -> anyhow::Result<Option<u8>> {
        if self.peeked.is_none() {
            self.peeked = self.input.next().transpose().context("reading the input")?;
        }
        Ok(self.peeked)
    }

    fn expect_byte(&mut self) -> anyhow::Result<u8> {
        match self.next_byte()? {
            Some(b) => Ok(b),
            None => bail!("unexpected end of the XML at line {}", self.line),
        }
    }

    /// Bytes up to and including `end`, without it.
    fn until(&mut self, end: &[u8]) -> anyhow::Result<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            out.push(self.expect_byte()?);
            if out.ends_with(end) {
                out.truncate(out.len() - end.len());
                return Ok(out);
            }
        }
    }

    pub fn next_event(&mut self) -> anyhow::Result<Event> {
        if let Some(name) = self.pending_end.take() {
            return Ok(Event::End { name });
        }
        loop {
            let Some(b) = self.peek()? else {
                return Ok(Event::Eof);
            };
            if b != b'<' {
                let mut text = Vec::new();
                while let Some(b) = self.peek()? {
                    if b == b'<' {
                        break;
                    }
                    text.push(self.expect_byte()?);
                    if text.ends_with(b"]]>") {
                        return Err(self
                            .parse_error(
                                "The character sequence \"]]>\" must not appear in content unless used to mark the end of a CDATA section.",
                            )
                            .into());
                    }
                }
                return Ok(Event::Text(unescape(&utf8(text)?)?));
            }
            self.next_byte()?;
            match self.peek()? {
                Some(b'?') => {
                    self.until(b"?>")?;
                }
                Some(b'!') => {
                    self.next_byte()?;
                    let mut head = Vec::new();
                    while head.len() < 7 && !(head.len() == 2 && head == b"--") {
                        head.push(self.expect_byte()?);
                    }
                    if head == b"--" {
                        self.until(b"-->")?;
                    } else if head == b"[CDATA[" {
                        return Ok(Event::CData(utf8(self.until(b"]]>")?)?));
                    } else {
                        // <!DOCTYPE ...> (no internal subset).
                        self.until(b">")?;
                    }
                }
                Some(b'/') => {
                    self.next_byte()?;
                    let name = utf8(self.until(b">")?)?.trim().to_owned();
                    return Ok(Event::End { name });
                }
                _ => return self.start_tag(),
            }
        }
    }

    /// After `<`: the name, the attributes, `>` or `/>`.
    fn start_tag(&mut self) -> anyhow::Result<Event> {
        let mut name = Vec::new();
        loop {
            match self.peek()? {
                Some(b) if b.is_ascii_whitespace() || b == b'>' || b == b'/' => break,
                Some(_) => name.push(self.expect_byte()?),
                None => bail!("unexpected end of the XML at line {}", self.line),
            }
        }
        let name = utf8(name)?;
        let mut attributes = Vec::new();
        loop {
            let b = self.expect_byte()?;
            match b {
                b'>' => return Ok(Event::Start { name, attributes }),
                b'/' => {
                    if self.expect_byte()? != b'>' {
                        bail!("malformed tag <{name}> at line {}", self.line);
                    }
                    self.pending_end = Some(name.clone());
                    return Ok(Event::Start { name, attributes });
                }
                b if b.is_ascii_whitespace() => {}
                b => {
                    let mut key = vec![b];
                    loop {
                        let c = self.expect_byte()?;
                        if c == b'=' {
                            break;
                        }
                        if !c.is_ascii_whitespace() {
                            key.push(c);
                        }
                    }
                    let quote = loop {
                        let c = self.expect_byte()?;
                        if c == b'"' || c == b'\'' {
                            break c;
                        }
                        if !c.is_ascii_whitespace() {
                            bail!("malformed attribute in <{name}> at line {}", self.line);
                        }
                    };
                    let value = self.until(&[quote])?;
                    attributes.push((utf8(key)?, unescape(&utf8(value)?)?));
                }
            }
        }
    }
}

fn utf8(bytes: Vec<u8>) -> anyhow::Result<String> {
    String::from_utf8(bytes).context("the XML is not UTF-8")
}

/// The predefined entities and character references.
fn unescape(s: &str) -> anyhow::Result<String> {
    if !s.contains('&') {
        return Ok(s.to_owned());
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        let Some(end) = rest[i..].find(';') else {
            bail!("unterminated entity in {s:?}");
        };
        let entity = &rest[i + 1..i + end];
        let c = match entity {
            "amp" => '&',
            "lt" => '<',
            "gt" => '>',
            "quot" => '"',
            "apos" => '\'',
            e if e.starts_with("#x") => char::from_u32(u32::from_str_radix(&e[2..], 16)?)
                .context("a bad character reference")?,
            e if e.starts_with('#') => {
                char::from_u32(e[1..].parse()?).context("a bad character reference")?
            }
            e => bail!("unknown entity &{e};"),
        };
        out.push(c);
        rest = &rest[i + end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events() {
        let xml = "<?xml version=\"1.0\"?>\n<a x=\"1 &amp; 2\"><!-- c --><b y='&#x41;'/>t&lt;<![CDATA[<raw>]]></a>";
        let mut r = Reader::new(xml.as_bytes());
        let mut events = Vec::new();
        loop {
            let e = r.next_event().unwrap();
            if e == Event::Eof {
                break;
            }
            events.push(e);
        }
        let start = |n: &str, a: &[(&str, &str)]| Event::Start {
            name: n.into(),
            attributes: a
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        };
        let end = |n: &str| Event::End { name: n.into() };
        assert_eq!(
            events,
            vec![
                Event::Text("\n".into()),
                start("a", &[("x", "1 & 2")]),
                start("b", &[("y", "A")]),
                end("b"),
                Event::Text("t<".into()),
                Event::CData("<raw>".into()),
                end("a"),
            ]
        );
    }

    #[test]
    fn a_cdata_end_in_content_is_the_jdks_parse_error() {
        let xml = "<a>\r\n   <![CDATA[x ]]> é ]]>\r\n</a>";
        let mut r = Reader::new(xml.as_bytes());
        let e = loop {
            match r.next_event() {
                Ok(Event::Eof) => panic!("no error"),
                Ok(_) => {}
                Err(e) => break e,
            }
        };
        assert_eq!(
            e.to_string(),
            "javax.xml.stream.XMLStreamException: ParseError at [row,col]:[2,24]\nMessage: The character sequence \"]]>\" must not appear in content unless used to mark the end of a CDATA section."
        );
    }

    #[test]
    fn next_tag_skips_white_space() {
        let mut r = Reader::new("<?xml version=\"1.0\"?>\r\n<m>\r\n <!-- c --> <n/>x".as_bytes());
        assert!(matches!(r.next_tag().unwrap(), Event::Start { name, .. } if name == "m"));
        assert!(matches!(r.next_tag().unwrap(), Event::Start { name, .. } if name == "n"));
        assert!(matches!(r.next_tag().unwrap(), Event::End { name } if name == "n"));
        assert!(
            r.next_tag()
                .unwrap_err()
                .to_string()
                .contains("found: CHARACTERS")
        );
    }
}
