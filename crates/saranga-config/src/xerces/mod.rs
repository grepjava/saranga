//! A port of the JDK's Xerces scanner (JDK 25), for the errors it reports
//! on XML that is not well formed: the message, line and column Artemis's
//! parsers report, and when they report it.
//!
//! Artemis reads `broker.xml` as `XMLUtil` reads it, through a DOM
//! `DocumentBuilder` given the file's text ([`Api::Dom`]), and
//! `bootstrap.xml` and `management.xml` through a StAX reader on the file's
//! bytes ([`Api::Stax`]). The two run the same scanner with different
//! settings and report what it finds differently; both are followed here.
//! roxmltree still builds the tree Saranga reads once the scanner has found
//! nothing wrong.

mod chars;
mod chars_table;
mod dtd;
mod encodings;
mod entity;
mod messages;
mod reader;
mod scanner;
mod xinclude;

use std::collections::HashMap;
use std::path::PathBuf;

use entity::ScannedEntity;

/// `XMLMessageFormatter.XML_DOMAIN` and `XMLNS_DOMAIN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Domain {
    Xml,
    Xmlns,
}

impl Domain {
    fn uri(self) -> &'static str {
        match self {
            Domain::Xml => "http://www.w3.org/TR/1998/REC-xml-19980210",
            Domain::Xmlns => "http://www.w3.org/TR/1999/REC-xml-names-19990114",
        }
    }
}

/// Which of Artemis's parsers reads the document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Api {
    /// `XMLUtil.readerToElement`: a namespace-aware, XInclude-aware
    /// `DocumentBuilder` parsing the text from a `StringReader`.
    Dom,
    /// `XmlProvider.createXMLStreamReader` on a `FileInputStream`, as
    /// `XmlUtil.decode` reads a DTO.
    Stax,
}

/// What the parser fails with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    /// A fatal error the scanner reported: a `SAXParseException` (DOM) or
    /// an `XMLStreamException` with its location (StAX).
    Parse(Box<ParseError>),
    /// An `IOException` out of the parser, `class: message` as Java prints
    /// it (a DTD or entity that cannot be read).
    Io {
        class: String,
        message: String,
        line: i32,
        column: i32,
    },
    /// A `RuntimeException` out of the parser.
    Runtime { class: String, message: String },
    /// An `XNIException` without a location: the DOM's `SAXException`,
    /// StAX's `XMLStreamException` at the reader's location.
    Xni {
        message: String,
        line: i32,
        column: i32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
    pub line: i32,
    pub column: i32,
    /// The entity's system id, when it has one (an external DTD or entity).
    pub system_id: Option<String>,
    /// What `DefaultErrorHandler` printed to the standard error when the
    /// error was reported (`[Fatal Error] :L:C: message`), if it printed.
    pub printed: Vec<String>,
    /// StAX: thrown while the reader was created, before any event.
    pub at_init: bool,
    /// StAX: the offset in the document's characters where the call to
    /// `next` that failed began; events before it were delivered.
    pub event_start: usize,
    /// StAX: the document's text as far as the reader read it, and the
    /// byte offset in it where the call to `next` that failed began.
    pub delivered: Option<(String, usize)>,
}

impl Failure {
    /// StAX: what the reader had read, and where in it the call that
    /// failed began.
    pub fn delivered(&self) -> Option<(String, usize)> {
        match self {
            Failure::Parse(p) => p.delivered.clone(),
            _ => None,
        }
    }
}

/// A document the scanner found well formed, with its characters as it
/// decoded them.
#[derive(Debug)]
pub struct Scanned {
    pub text: String,
}

/// XML 1.1 reads NEL and LINE SEPARATOR as line ends, and `\r` NEL as one;
/// roxmltree reads XML 1.0. The text with them as the line feeds the
/// scanner made of them, which keeps every position.
pub fn xml11_line_ends(text: &str) -> String {
    text.replace("\r\u{85}", "\r\n")
        .replace(['\u{85}', '\u{2028}'], "\n")
}

/// An argument of a message, formatted as `MessageFormat` formats it.
#[derive(Debug, Clone)]
pub(crate) enum Arg {
    Str(String),
    Int(i64),
    QName(QName),
}

impl Arg {
    /// `String.valueOf`, as an unformatted message joins its arguments.
    fn plain(&self) -> String {
        match self {
            Arg::Str(s) => s.clone(),
            Arg::Int(i) => i.to_string(),
            Arg::QName(q) => q.to_java_string(),
        }
    }

    /// `MessageFormat`'s rendering: integers grouped by thousands.
    fn formatted(&self) -> String {
        match self {
            Arg::Int(i) => group(*i),
            other => other.plain(),
        }
    }
}

fn group(i: i64) -> String {
    let digits = i.unsigned_abs().to_string();
    let mut out = String::new();
    for (k, c) in digits.chars().enumerate() {
        if k > 0 && (digits.len() - k).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    if i < 0 { format!("-{out}") } else { out }
}

/// `java.text.MessageFormat.format` for the patterns of
/// `XMLMessages.properties`: `''` is a quote, other quotes delimit literal
/// text, and `{n}` is an argument.
fn message_format(pattern: &str, args: &[Arg]) -> String {
    let mut out = String::new();
    let mut chars = pattern.chars().peekable();
    let mut quoted = false;
    while let Some(c) = chars.next() {
        match c {
            '\'' if chars.peek() == Some(&'\'') => {
                chars.next();
                out.push('\'');
            }
            '\'' => quoted = !quoted,
            '{' if !quoted => {
                let mut index = String::new();
                for d in chars.by_ref() {
                    if d == '}' {
                        break;
                    }
                    index.push(d);
                }
                match index.trim().parse::<usize>().ok().and_then(|i| args.get(i)) {
                    Some(arg) => out.push_str(&arg.formatted()),
                    None => {
                        out.push('{');
                        out.push_str(&index);
                        out.push('}');
                    }
                }
            }
            c => out.push(c),
        }
    }
    out
}

/// The message `XMLErrorReporter` gives an error: formatted when the
/// domain has a formatter, `domain#key?args` when not.
fn message(api: Api, domain: Domain, key: &str, args: &[Arg]) -> String {
    // StAX's StaxErrorReporter registers a formatter for the XML domain
    // only.
    if api == Api::Stax && domain == Domain::Xmlns {
        let mut s = format!("{}#{key}", domain.uri());
        if !args.is_empty() {
            s.push('?');
            s.push_str(&args.iter().map(Arg::plain).collect::<Vec<_>>().join("&"));
        }
        return s;
    }
    let template = messages::template(key)
        .unwrap_or("The error message corresponding to the message key can not be found.");
    if args.is_empty() {
        template.to_string()
    } else {
        message_format(template, args)
    }
}

/// Xerces's `QName`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct QName {
    pub prefix: Option<String>,
    pub localpart: Option<String>,
    pub rawname: Option<String>,
    pub uri: Option<String>,
}

impl QName {
    pub(crate) fn set(
        &mut self,
        prefix: Option<String>,
        localpart: Option<String>,
        rawname: Option<String>,
        uri: Option<String>,
    ) {
        *self = QName {
            prefix,
            localpart,
            rawname,
            uri,
        };
    }

    pub(crate) fn raw(&self) -> String {
        self.rawname.clone().unwrap_or_default()
    }

    /// `QName.toString`.
    fn to_java_string(&self) -> String {
        let mut parts = Vec::new();
        if let Some(p) = &self.prefix {
            parts.push(format!("prefix=\"{p}\""));
        }
        if let Some(l) = &self.localpart {
            parts.push(format!("localpart=\"{l}\""));
        }
        if let Some(r) = &self.rawname {
            parts.push(format!("rawname=\"{r}\""));
        }
        if let Some(u) = &self.uri {
            parts.push(format!("uri=\"{u}\""));
        }
        parts.join(",")
    }
}

/// `XMLScanner.NameType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NameType {
    Attribute,
    AttributeName,
    Comment,
    Doctype,
    ElementStart,
    ElementEnd,
    Entity,
    Notation,
    Pi,
    Reference,
}

/// What stops the scanner: an exception as Java throws it.
#[derive(Debug)]
pub(crate) enum Stop {
    /// A fatal error reported through `XMLErrorReporter`.
    Fatal {
        message: String,
        line: i32,
        column: i32,
        system_id: Option<String>,
    },
    /// `java.io.EOFException`.
    Eof,
    /// `MalformedByteSequenceException`.
    Malformed { key: &'static str, args: Vec<Arg> },
    /// Another `IOException`.
    Io {
        class: &'static str,
        message: String,
    },
    /// A `RuntimeException`.
    Runtime {
        class: &'static str,
        message: String,
    },
    /// An `XNIException` thrown without a location.
    Xni(String),
}

pub(crate) type R<T> = Result<T, Stop>;

/// The state literal the JDK 25 limits carry: they are set in
/// `conf/jaxp.properties`.
const STATE_LITERAL: &str = "jaxp.properties";

/// `XMLSecurityManager.Limit`, with JDK 25's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Limit {
    EntityExpansion,
    ElementAttribute,
    TotalEntitySize,
    GeneralEntitySize,
    ParameterEntitySize,
    MaxElementDepth,
    MaxName,
    EntityReplacement,
}

impl Limit {
    const ALL: usize = 8;

    fn index(self) -> usize {
        self as usize
    }

    pub(crate) fn max(self) -> i64 {
        match self {
            Limit::EntityExpansion => 2500,
            Limit::ElementAttribute => 200,
            Limit::TotalEntitySize => 100_000,
            Limit::GeneralEntitySize => 100_000,
            Limit::ParameterEntitySize => 15_000,
            Limit::MaxElementDepth => 100,
            Limit::MaxName => 1000,
            Limit::EntityReplacement => 100_000,
        }
    }

    pub(crate) fn key(self) -> &'static str {
        match self {
            Limit::EntityExpansion => "EntityExpansionLimit",
            Limit::ElementAttribute => "ElementAttributeLimit",
            Limit::TotalEntitySize => "TotalEntitySizeLimit",
            Limit::GeneralEntitySize => "MaxEntitySizeLimit",
            Limit::ParameterEntitySize => "MaxParameterEntitySizeLimit",
            Limit::MaxElementDepth => "MaxElementDepthLimit",
            Limit::MaxName => "MaxXMLNameLimit",
            Limit::EntityReplacement => "EntityReplacementLimit",
        }
    }

    pub(crate) fn system_property(self) -> &'static str {
        match self {
            Limit::EntityExpansion => "jdk.xml.entityExpansionLimit",
            Limit::ElementAttribute => "jdk.xml.elementAttributeLimit",
            Limit::TotalEntitySize => "jdk.xml.totalEntitySizeLimit",
            Limit::GeneralEntitySize => "jdk.xml.maxGeneralEntitySizeLimit",
            Limit::ParameterEntitySize => "jdk.xml.maxParameterEntitySizeLimit",
            Limit::MaxElementDepth => "jdk.xml.maxElementDepth",
            Limit::MaxName => "jdk.xml.maxXMLNameLimit",
            Limit::EntityReplacement => "jdk.xml.entityReplacementLimit",
        }
    }

    /// Limits `isOverLimit` compares by their total.
    fn by_total(self) -> bool {
        matches!(
            self,
            Limit::ElementAttribute
                | Limit::EntityExpansion
                | Limit::TotalEntitySize
                | Limit::EntityReplacement
                | Limit::MaxElementDepth
                | Limit::MaxName
        )
    }
}

/// `XMLLimitAnalyzer`.
#[derive(Debug, Default)]
pub(crate) struct Limits {
    values: [i64; Limit::ALL],
    totals: [i64; Limit::ALL],
    caches: [Option<HashMap<String, i64>>; Limit::ALL],
}

impl Limits {
    pub(crate) fn add(&mut self, limit: Limit, entity: &str, value: i64) {
        let i = limit.index();
        if matches!(
            limit,
            Limit::EntityExpansion
                | Limit::ElementAttribute
                | Limit::TotalEntitySize
                | Limit::EntityReplacement
        ) {
            self.totals[i] += value;
            return;
        }
        if matches!(limit, Limit::MaxElementDepth | Limit::MaxName) {
            self.values[i] = value;
            self.totals[i] = value;
            return;
        }
        let cache = self.caches[i].get_or_insert_with(HashMap::new);
        let accumulated = value + cache.get(entity).copied().unwrap_or(0);
        cache.insert(entity.to_string(), accumulated);
        if accumulated > self.values[i] {
            self.values[i] = accumulated;
        }
        if matches!(limit, Limit::GeneralEntitySize | Limit::ParameterEntitySize) {
            self.totals[Limit::TotalEntitySize.index()] += value;
        }
    }

    pub(crate) fn value(&self, limit: Limit) -> i64 {
        if limit == Limit::EntityReplacement {
            return self.totals[limit.index()];
        }
        self.values[limit.index()]
    }

    pub(crate) fn total(&self, limit: Limit) -> i64 {
        self.totals[limit.index()]
    }

    pub(crate) fn over(&self, limit: Limit) -> bool {
        if limit.by_total() {
            self.totals[limit.index()] > limit.max()
        } else {
            self.values[limit.index()] > limit.max()
        }
    }

    pub(crate) fn start_entity(&mut self, _name: &str) {}

    /// `XMLLimitAnalyzer.reset`.
    pub(crate) fn reset(&mut self, limit: Limit) {
        let i = limit.index();
        match limit {
            Limit::TotalEntitySize => self.totals[i] = 0,
            Limit::GeneralEntitySize => {
                self.values[i] = 0;
                self.caches[i] = None;
                self.totals[i] = 0;
            }
            _ => {}
        }
    }

    pub(crate) fn end_entity(&mut self, limit: Limit, name: &str) {
        if let Some(cache) = &mut self.caches[limit.index()] {
            cache.remove(name);
        }
    }
}

/// An entity the DTD declared.
#[derive(Debug, Clone)]
pub(crate) enum EntityDecl {
    Internal {
        text: String,
        in_external_subset: bool,
    },
    External {
        system_id: String,
        base_system_id: Option<String>,
        notation: Option<String>,
        in_external_subset: bool,
    },
}

impl EntityDecl {
    fn is_external(&self) -> bool {
        matches!(self, EntityDecl::External { .. })
    }

    fn is_unparsed(&self) -> bool {
        matches!(
            self,
            EntityDecl::External {
                notation: Some(_),
                ..
            }
        )
    }

    fn in_external_subset(&self) -> bool {
        match self {
            EntityDecl::Internal {
                in_external_subset, ..
            }
            | EntityDecl::External {
                in_external_subset, ..
            } => *in_external_subset,
        }
    }
}

/// Who `XMLEntityManager` tells of entities starting and ending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Handler {
    None,
    Document,
    Dtd,
}

/// The scanner, its entity scanner and entity manager in one: the state of
/// the JDK's `XMLNSDocumentScannerImpl` (or `XML11NSDocumentScannerImpl`),
/// `XMLDTDScannerImpl`, `XMLEntityScanner` and `XMLEntityManager` for one
/// parse.
pub(crate) struct Parser {
    api: Api,
    /// The directory a relative system id resolves against (`user.dir`).
    base_dir: PathBuf,
    /// `XMLEntityManager`'s current entity is the last.
    entities: Vec<ScannedEntity>,
    /// `XMLEntityScanner.isExternal`, kept from the last current entity.
    is_external: bool,
    /// The scanner reads XML 1.1.
    v11: bool,
    /// The document scanner is `XML11NSDocumentScannerImpl`.
    doc11: bool,
    /// StAX: the 1.0 scanner the switch to XML 1.1 replaced, which the
    /// entity manager still reports entities to.
    stale_document: Option<Box<scanner::State>>,
    /// `XMLVersionDetector` is reading the declaration.
    detecting_version: bool,
    /// The DOM's error reporter has the entity scanner as its locator.
    locator_ready: bool,
    handler: Handler,
    limits: Limits,
    entity_expansion_count: u32,
    /// The entities the DTD declared, in `XMLEntityStorage`.
    declared: HashMap<String, EntityDecl>,
    in_external_subset: bool,
    // XMLEntityScanner's fields.
    offset: usize,
    newlines: usize,
    counted: bool,
    white_space: Vec<usize>,
    white_space_len: usize,
    white_space_info_needed: bool,
    literal_start: usize,
    literal_length: usize,
    /// The scanner's own state.
    s: scanner::State,
    d: dtd::State,
    /// The document's characters as the scanner read them, for StAX.
    decoded: Vec<u16>,
    /// `XIncludeHandler`'s state, for the DOM.
    inc: xinclude::Includes,
    /// What the default error handler printed: warnings, and the errors of
    /// included documents.
    printed: Vec<String>,
}

impl Parser {
    fn new(api: Api, base_dir: PathBuf) -> Parser {
        Parser {
            api,
            base_dir,
            entities: Vec::new(),
            is_external: false,
            v11: false,
            doc11: false,
            stale_document: None,
            detecting_version: false,
            locator_ready: api == Api::Stax,
            handler: Handler::None,
            limits: Limits::default(),
            entity_expansion_count: 0,
            declared: HashMap::new(),
            in_external_subset: false,
            offset: 0,
            newlines: 0,
            counted: false,
            white_space: Vec::new(),
            white_space_len: 0,
            white_space_info_needed: true,
            literal_start: 0,
            literal_length: 0,
            s: scanner::State::default(),
            d: dtd::State::default(),
            decoded: Vec::new(),
            inc: xinclude::Includes::default(),
            printed: Vec::new(),
        }
    }

    /// `reportFatalError`: the error at the entity scanner's location.
    pub(crate) fn fatal(&self, key: &str, args: Vec<Arg>) -> Stop {
        self.stop(Domain::Xml, key, &args, true)
    }

    /// `XMLErrorReporter.reportError` without a locator: at the reporter's
    /// locator, which the DOM sets once the document starts.
    pub(crate) fn error(&self, domain: Domain, key: &str, args: Vec<Arg>) -> Stop {
        self.stop(domain, key, &args, self.locator_ready)
    }

    fn stop(&self, domain: Domain, key: &str, args: &[Arg], located: bool) -> Stop {
        let (line, column, system_id) = if located {
            (
                self.line(),
                self.column(),
                self.entities
                    .last()
                    .and_then(|e| e.expanded_system_id.clone()),
            )
        } else {
            (-1, -1, None)
        };
        Stop::Fatal {
            message: message(self.api, domain, key, args),
            line,
            column,
            system_id,
        }
    }

    /// The offset of the document entity's position in its characters.
    fn document_offset(&self) -> usize {
        match self.entities.first() {
            Some(e) => e.offset_of(e.position),
            None => self.decoded.len(),
        }
    }
}

/// Checks `text` as `XMLUtil.readerToElement` parses it: what the default
/// error handler printed on the way (`[Warning]` lines of includes that
/// fell back), or what the parse fails with.
pub fn check_dom(text: &str, base_dir: PathBuf) -> Result<Checked, Failure> {
    let mut parser = Parser::new(Api::Dom, base_dir);
    let chars: Vec<u16> = text.encode_utf16().collect();
    parser.parse_dom(chars)?;
    Ok(Checked {
        printed: parser.printed,
        xml11: parser.v11,
    })
}

/// A document the DOM reads.
#[derive(Debug)]
pub struct Checked {
    /// What the default error handler printed: `[Warning]` lines.
    pub printed: Vec<String>,
    /// The document is XML 1.1.
    pub xml11: bool,
}

/// Checks `bytes` as a StAX reader `XmlUtil.decode` creates reads them,
/// returning the characters it decoded.
pub fn check_stax(bytes: &[u8], base_dir: PathBuf) -> Result<Scanned, Failure> {
    let mut parser = Parser::new(Api::Stax, base_dir);
    parser.parse_stax(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_format_follows_java() {
        assert_eq!(
            message_format(
                "The '' = '' character must follow \"{0}\".",
                &[Arg::Str("version".into())]
            ),
            "The ' = ' character must follow \"version\"."
        );
        assert_eq!(
            message_format(
                "is \"{1}\" of \"{0}\"",
                &[Arg::Str("a".into()), Arg::Int(100064)]
            ),
            "is \"100,064\" of \"a\""
        );
        assert_eq!(group(1001), "1,001");
        assert_eq!(group(100), "100");
    }
}
