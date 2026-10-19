//! `XMLScanner`, `XMLDocumentFragmentScannerImpl`, `XMLDocumentScannerImpl`
//! and `XMLNSDocumentScannerImpl` (with their XML 1.1 counterparts): the
//! drivers that scan a document event by event, and the errors they
//! report.

use super::chars;
use super::encodings;
use super::entity::BUFFER_SIZE;
use super::reader::{Charset, JavaDecoder, Reader, Stream};
use super::{
    Api, Arg, Domain, Failure, Handler, Limit, NameType, ParseError, Parser, QName, R, Scanned,
    Stop,
};

// XMLEvent's constants.
const START_ELEMENT: i32 = 1;
const END_ELEMENT: i32 = 2;
const PROCESSING_INSTRUCTION: i32 = 3;
const CHARACTERS: i32 = 4;
const COMMENT: i32 = 5;
const START_DOCUMENT: i32 = 7;
const END_DOCUMENT: i32 = 8;
const ENTITY_REFERENCE: i32 = 9;
const DTD: i32 = 11;
const CDATA: i32 = 12;

// The scanner states.
pub(crate) const STATE_START_OF_MARKUP: i32 = 21;
pub(crate) const STATE_CONTENT: i32 = 22;
pub(crate) const STATE_PI: i32 = 23;
pub(crate) const STATE_DOCTYPE: i32 = 24;
pub(crate) const STATE_ROOT_ELEMENT: i32 = 26;
pub(crate) const STATE_COMMENT: i32 = 27;
pub(crate) const STATE_REFERENCE: i32 = 28;
pub(crate) const STATE_TERMINATED: i32 = 34;
pub(crate) const STATE_CDATA: i32 = 35;
pub(crate) const STATE_TEXT_DECL: i32 = 36;
pub(crate) const STATE_CHARACTER_DATA: i32 = 37;
pub(crate) const STATE_START_ELEMENT_TAG: i32 = 38;
pub(crate) const STATE_END_ELEMENT_TAG: i32 = 39;
pub(crate) const STATE_CHAR_REFERENCE: i32 = 40;
pub(crate) const STATE_BUILT_IN_REFS: i32 = 41;
pub(crate) const STATE_XML_DECL: i32 = 42;
pub(crate) const STATE_PROLOG: i32 = 43;
pub(crate) const STATE_TRAILING_MISC: i32 = 44;
pub(crate) const STATE_DTD_INTERNAL_DECLS: i32 = 45;
pub(crate) const STATE_DTD_EXTERNAL: i32 = 46;
pub(crate) const STATE_DTD_EXTERNAL_DECLS: i32 = 47;
pub(crate) const STATE_NO_SUCH_ELEMENT: i32 = 48;

const XML_URI: &str = "http://www.w3.org/XML/1998/namespace";
const XMLNS_URI: &str = "http://www.w3.org/2000/xmlns/";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Driver {
    #[default]
    XmlDecl,
    Prolog,
    Content,
    TrailingMisc,
}

/// `NamespaceSupport`.
#[derive(Debug, Clone)]
pub(crate) struct Namespaces {
    bindings: Vec<(String, Option<String>)>,
    contexts: Vec<usize>,
}

impl Default for Namespaces {
    fn default() -> Namespaces {
        Namespaces {
            bindings: vec![
                ("xml".into(), Some(XML_URI.into())),
                ("xmlns".into(), Some(XMLNS_URI.into())),
            ],
            contexts: vec![2],
        }
    }
}

impl Namespaces {
    pub(crate) fn push(&mut self) {
        self.contexts.push(self.bindings.len());
    }

    pub(crate) fn pop(&mut self) {
        if self.contexts.len() > 1 {
            let at = self.contexts.pop().unwrap();
            self.bindings.truncate(at);
        }
    }

    fn current(&self) -> usize {
        *self.contexts.last().unwrap()
    }

    pub(crate) fn declare(&mut self, prefix: &str, uri: Option<String>) -> bool {
        if prefix == "xml" || prefix == "xmlns" {
            return false;
        }
        let start = self.current();
        for b in self.bindings[start..].iter_mut().rev() {
            if b.0 == prefix {
                b.1 = uri;
                return true;
            }
        }
        self.bindings.push((prefix.to_string(), uri));
        true
    }

    pub(crate) fn uri(&self, prefix: &str) -> Option<String> {
        self.bindings
            .iter()
            .rev()
            .find(|b| b.0 == prefix)
            .and_then(|b| b.1.clone())
    }

    fn contains_in_current(&self, prefix: &str) -> bool {
        self.bindings[self.current()..]
            .iter()
            .any(|b| b.0 == prefix)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Attr {
    pub name: QName,
    pub value: String,
}

/// `XMLAttributesImpl`'s duplicate checks.
fn check_duplicates_ns(attrs: &[Attr]) -> Option<QName> {
    let same =
        |a: &Attr, b: &Attr| a.name.localpart == b.name.localpart && a.name.uri == b.name.uri;
    if attrs.len() <= 20 {
        for i in 0..attrs.len().saturating_sub(1) {
            for j in i + 1..attrs.len() {
                if same(&attrs[i], &attrs[j]) {
                    return Some(attrs[j].name.clone());
                }
            }
        }
        None
    } else {
        // The hashed view finds the first attribute an earlier one matches.
        (1..attrs.len())
            .find(|&j| (0..j).any(|i| same(&attrs[i], &attrs[j])))
            .map(|j| attrs[j].name.clone())
    }
}

#[derive(Debug, Default)]
pub(crate) struct State {
    pub state: i32,
    pub driver: Driver,
    pub markup_depth: i32,
    pub entity_depth: usize,
    pub entity_stack: Vec<i32>,
    pub elements: Vec<QName>,
    pub empty_element: bool,
    pub has_external_dtd: bool,
    pub standalone: bool,
    pub current_element: QName,
    pub element_qname: QName,
    pub attribute_qname: QName,
    pub attributes: Vec<Attr>,
    pub ns: Namespaces,
    pub bind_namespaces: bool,
    pub xmlns_declared: bool,
    pub doctype_name: Option<String>,
    pub doctype_public_id: Option<String>,
    pub doctype_system_id: Option<String>,
    pub seen_doctype_decl: bool,
    pub scanning_attribute: bool,
    pub found_builtin_refs: bool,
    pub builtin_handled: bool,
    pub last_was_end_element: bool,
    pub current_entity_name: Option<String>,
    /// The DOM's DTD validator has a grammar: it binds namespaces.
    pub has_grammar: bool,
    pub cdata_end: bool,
    /// StAX: where the document entity's root element ended.
    pub root_end: Option<usize>,
}

impl Parser {
    // ----- Entry points -----

    pub(crate) fn parse_dom(&mut self, chars: Vec<u16>) -> Result<(), Failure> {
        let result = {
            self.push_document_entity(Reader::chars(chars), None, None);
            self.run_dom()
        };
        result.map_err(|stop| self.failure(stop, false, 0))
    }

    /// The DOM on a file's bytes, as `XIncludeHandler` parses an included
    /// document: `uri` is its expanded system id.
    pub(crate) fn parse_dom_bytes(&mut self, bytes: Vec<u8>, uri: String) -> Result<(), Failure> {
        let result = (|| {
            let mut stream = Stream::new(bytes);
            let mut b4 = [0i32; 4];
            for b in &mut b4 {
                *b = stream.read_and_buffer();
            }
            let (encoding, reader_encoding, big_endian, bom) = encoding_info(&b4);
            stream.reset();
            if bom {
                if reader_encoding == "UTF-8" {
                    stream.skip(3);
                } else if reader_encoding == "UTF-16" {
                    stream.skip(2);
                }
            }
            let reader = self.manager_reader(reader_encoding, big_endian)?;
            let entity = super::entity::ScannedEntity::new(
                false,
                "[xml]",
                reader,
                Some(stream),
                Some(encoding.to_string()),
                false,
                false,
                true,
                Some(uri.clone()),
            );
            self.entities.push(entity);
            self.is_external = true;
            self.run_dom()
        })();
        result.map_err(|stop| self.failure(stop, false, 0))
    }

    fn run_dom(&mut self) -> R<()> {
        // XML11Configuration.parse: the version detector reads the
        // declaration of the document entity.
        let v11 = self.determine_doc_version()?;
        self.v11 = v11;
        self.doc11 = v11;
        self.s.state = STATE_XML_DECL;
        self.s.driver = Driver::XmlDecl;
        // startDocumentParsing.
        self.locator_ready = true;
        self.handler = Handler::Document;
        self.document_start_entity("[xml]", false)?;
        self.xinclude_start_document()?;
        // scanDocument(true).
        let mut event = self.next()?;
        loop {
            match event {
                CHARACTERS | ENTITY_REFERENCE | PROCESSING_INSTRUCTION | COMMENT | CDATA => {
                    self.check_node_count()?
                }
                START_DOCUMENT | START_ELEMENT | END_ELEMENT | DTD | 6 | 10 | 13 | 14 | 15 => {}
                _ => return Ok(()),
            }
            event = self.next()?;
            if event == END_DOCUMENT {
                return Ok(());
            }
        }
    }

    pub(crate) fn parse_stax(&mut self, bytes: Vec<u8>) -> Result<Scanned, Failure> {
        // createXMLStreamReader: the document entity, then its first event.
        if let Err(stop) = self
            .start_document_entity_stax(bytes)
            .and_then(|_| self.stax_next())
        {
            return Err(self.failure(stop, true, 0));
        }
        let mut start = 0;
        let result = (|| -> R<()> {
            // StAXStreamConnector.bridge: to the root, through it, and one
            // event past its end.
            let mut depth = 0usize;
            let mut event = START_DOCUMENT;
            while event != START_ELEMENT {
                start = self.document_offset();
                event = self.stax_next()?;
                if event == END_DOCUMENT {
                    return Ok(());
                }
            }
            loop {
                match event {
                    START_ELEMENT => depth += 1,
                    END_ELEMENT => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
                start = self.document_offset();
                event = self.stax_next()?;
            }
            self.s.root_end = Some(self.document_offset());
            if event != END_DOCUMENT {
                start = self.document_offset();
                self.stax_next()?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                let end = self.s.root_end.unwrap_or(self.decoded.len());
                let text = String::from_utf16_lossy(&self.decoded[..end.min(self.decoded.len())]);
                Ok(Scanned {
                    text: if self.v11 {
                        super::xml11_line_ends(&text)
                    } else {
                        text
                    },
                })
            }
            Err(stop) => Err(self.failure(stop, false, start)),
        }
    }

    /// XMLStreamReaderImpl.next.
    fn stax_next(&mut self) -> R<i32> {
        let event = self.next()?;
        if matches!(
            event,
            CHARACTERS | ENTITY_REFERENCE | PROCESSING_INSTRUCTION | COMMENT | CDATA
        ) {
            self.check_node_count()?;
        }
        Ok(event)
    }

    /// What a parse that stopped on `stop` fails with.
    fn failure(&self, stop: Stop, at_init: bool, event_start: usize) -> Failure {
        match stop {
            Stop::Fatal {
                message,
                line,
                column,
                system_id,
                ..
            } => {
                let mut printed = self.printed.clone();
                if self.api == Api::Dom {
                    let id = system_id
                        .as_deref()
                        .map(|s| s.rsplit('/').next().unwrap_or(s))
                        .unwrap_or("");
                    printed.push(format!("[Fatal Error] {id}:{line}:{column}: {message}"));
                }
                let (line, column) = if self.api == Api::Stax {
                    (self.line(), self.column())
                } else {
                    (line, column)
                };
                let delivered = self.delivered(event_start);
                Failure::Parse(Box::new(ParseError {
                    message,
                    line,
                    column,
                    system_id,
                    printed,
                    at_init,
                    event_start,
                    delivered,
                }))
            }
            Stop::Malformed { key, args } => {
                // The 5-argument reportError: printed by the default handler
                // without a locator in StAX.
                let message = super::message(self.api, Domain::Xml, key, &args);
                let mut printed = self.printed.clone();
                printed.push(format!("[Fatal Error] :-1:-1: {message}"));
                Failure::Parse(Box::new(ParseError {
                    message,
                    line: self.line(),
                    column: self.column(),
                    system_id: None,
                    printed,
                    at_init,
                    event_start,
                    delivered: self.delivered(event_start),
                }))
            }
            Stop::Io { class, message } => Failure::Io {
                class: class.to_string(),
                message,
                line: self.line(),
                column: self.column(),
            },
            Stop::Runtime { class, message } => Failure::Runtime {
                class: class.to_string(),
                message,
            },
            Stop::Xni(message) => Failure::Xni {
                message,
                line: self.line(),
                column: self.column(),
            },
            Stop::Eof => Failure::Runtime {
                class: "java.io.EOFException".into(),
                message: String::new(),
            },
        }
    }

    /// StAX: the document's characters the reader decoded, and the byte
    /// offset of character `at` in them.
    fn delivered(&self, at: usize) -> Option<(String, usize)> {
        if self.api != Api::Stax {
            return None;
        }
        let at = at.min(self.decoded.len());
        let cut = String::from_utf16_lossy(&self.decoded[..at]).len();
        Some((String::from_utf16_lossy(&self.decoded), cut))
    }

    fn push_document_entity(
        &mut self,
        reader: Reader,
        stream: Option<Stream>,
        encoding: Option<String>,
    ) {
        let entity = super::entity::ScannedEntity::new(
            false, "[xml]", reader, stream, encoding, false, false, true, None,
        );
        self.entities.push(entity);
        self.is_external = true;
        self.limits.start_entity("[xml]");
    }

    /// `XMLVersionDetector.determineDocVersion`: true for XML 1.1.
    fn determine_doc_version(&mut self) -> R<bool> {
        self.detecting_version = true;
        let result = self.detect_version();
        self.detecting_version = false;
        match result {
            Ok(v) => Ok(v),
            Err(Stop::Eof) => Err(self.error(Domain::Xml, "PrematureEOF", vec![])),
            Err(e) => Err(e),
        }
    }

    fn detect_version(&mut self) -> R<bool> {
        let mut expected: Vec<u16> = "<?xml version=     ".encode_utf16().collect();
        if !self.skip_string("<?xml")? {
            return Ok(false);
        }
        if !self.skip_decl_spaces()? {
            self.fixup_current_entity(&expected, 5);
            return Ok(false);
        }
        if !self.skip_string("version")? {
            self.fixup_current_entity(&expected, 6);
            return Ok(false);
        }
        self.skip_decl_spaces()?;
        if self.peek_char()? != '=' as i32 {
            self.fixup_current_entity(&expected, 13);
            return Ok(false);
        }
        self.scan_char(None)?;
        self.skip_decl_spaces()?;
        expected[14] = self.scan_char(None)? as u16;
        for k in 0..3 {
            expected[15 + k] = self.scan_char(None)? as u16;
        }
        expected[18] = self.scan_char(None)? as u16;
        self.fixup_current_entity(&expected, 19);
        Ok(expected[15..18] == ['1' as u16, '.' as u16, '1' as u16])
    }

    /// `XMLVersionDetector.fixupCurrentEntity`: puts what the detector
    /// read back as the declaration's expected start.
    fn fixup_current_entity(&mut self, scanned: &[u16], length: usize) {
        let e = self.cur_mut();
        if e.count - e.position + length > e.ch.len() {
            let mut ch = vec![0u16; length + e.count - e.position + 1];
            ch[..e.ch.len()].copy_from_slice(&e.ch);
            e.ch = ch;
        }
        if e.position < length {
            let (p, c) = (e.position, e.count);
            e.ch.copy_within(p..c, length);
            e.count += length - e.position;
        } else {
            for i in length..e.position {
                e.ch[i] = ' ' as u16;
            }
        }
        e.ch[..length].copy_from_slice(&scanned[..length]);
        e.position = 0;
        e.column = 1;
        e.line = 1;
    }

    /// `XMLDocumentScannerImpl.setInputSource` on a byte stream:
    /// `XMLEntityManager.startEntity` with `setupCurrentEntity`'s encoding
    /// detection.
    fn start_document_entity_stax(&mut self, bytes: Vec<u8>) -> R<()> {
        self.s.state = STATE_XML_DECL;
        self.s.driver = Driver::XmlDecl;
        self.handler = Handler::Document;
        let mut stream = Stream::new(bytes);
        let mut b4 = [0i32; 4];
        let mut count = 0;
        while count < 4 {
            b4[count] = stream.read_and_buffer();
            count += 1;
        }
        let (encoding, reader_encoding, big_endian, bom) = encoding_info(&b4);
        stream.reset();
        if bom {
            if reader_encoding == "UTF-8" {
                stream.skip(3);
            } else if reader_encoding == "UTF-16" {
                stream.skip(2);
            }
        }
        let reader = self.manager_reader(reader_encoding, big_endian)?;
        let entity = super::entity::ScannedEntity::new(
            false,
            "[xml]",
            reader,
            Some(stream),
            Some(encoding.to_string()),
            false,
            false,
            true,
            None,
        );
        self.entities.push(entity);
        self.is_external = true;
        self.limits.start_entity("[xml]");
        self.entity_expansion_count += 1;
        self.limits.add(Limit::EntityExpansion, "[xml]", 1);
        self.document_start_entity("[xml]", false)?;
        self.s.state = START_DOCUMENT;
        Ok(())
    }

    /// `XMLEntityManager.createReader`.
    fn manager_reader(&mut self, encoding: &str, big_endian: Option<bool>) -> R<Reader> {
        Ok(match (encoding, big_endian) {
            ("UTF-8", _) => Reader::utf8(BUFFER_SIZE),
            ("UTF-16", Some(be)) => Reader::utf16(BUFFER_SIZE, be),
            ("UTF-16BE", _) => Reader::utf16(BUFFER_SIZE, true),
            ("UTF-16LE", _) => Reader::utf16(BUFFER_SIZE, false),
            ("ISO-10646-UCS-4", Some(be)) => Reader::ucs(true, be),
            ("ISO-10646-UCS-4", None) => {
                return Err(self.fatal(
                    "EncodingByteOrderUnsupported",
                    vec![Arg::Str(encoding.into())],
                ));
            }
            ("ISO-10646-UCS-2", Some(be)) => Reader::ucs(false, be),
            ("ISO-10646-UCS-2", None) => {
                return Err(self.fatal(
                    "EncodingByteOrderUnsupported",
                    vec![Arg::Str(encoding.into())],
                ));
            }
            (other, _) => {
                if !chars::is_valid_iana_encoding(other) {
                    return Err(self.fatal("EncodingDeclInvalid", vec![Arg::Str(other.into())]));
                }
                match encodings::iana_to_java(&other.to_ascii_uppercase()) {
                    None => {
                        return Err(self.fatal("EncodingDeclInvalid", vec![Arg::Str(other.into())]));
                    }
                    Some(java) => java_reader(java)?,
                }
            }
        })
    }

    // ----- XMLEntityHandler -----

    pub(crate) fn handler_start_entity(
        &mut self,
        name: &str,
        _encoding: Option<&str>,
        skipped: bool,
    ) -> R<()> {
        match self.handler {
            Handler::Document => {
                self.with_document_handler(|p| p.document_start_entity(name, skipped))
            }
            Handler::Dtd => self.dtd_start_entity(name),
            Handler::None => Ok(()),
        }
    }

    pub(crate) fn handler_end_entity(&mut self, name: &str, last: bool) -> R<()> {
        match self.handler {
            Handler::Document => self.with_document_handler(|p| p.document_end_entity(name)),
            Handler::Dtd => self.dtd_end_entity(name, last),
            Handler::None => Ok(()),
        }
    }

    /// Runs `f` on the scanner the entity manager reports to: the one
    /// StAX replaced, if it switched to XML 1.1.
    fn with_document_handler(&mut self, f: impl FnOnce(&mut Parser) -> R<()>) -> R<()> {
        let Some(mut old) = self.stale_document.take() else {
            return f(self);
        };
        std::mem::swap(&mut self.s, &mut old);
        let result = f(self);
        std::mem::swap(&mut self.s, &mut old);
        self.stale_document = Some(old);
        result
    }

    /// `XMLDocumentScannerImpl.startEntity`.
    fn document_start_entity(&mut self, name: &str, skipped: bool) -> R<()> {
        let s = &mut self.s;
        if s.entity_stack.len() <= s.entity_depth {
            s.entity_stack.resize(s.entity_depth + 1, 0);
        }
        s.entity_stack[s.entity_depth] = s.markup_depth;
        s.entity_depth += 1;
        if self.s.standalone
            && self
                .declared
                .get(name)
                .is_some_and(|d| d.in_external_subset())
        {
            return Err(self.fatal(
                "MSG_REFERENCE_TO_EXTERNALLY_DECLARED_ENTITY_WHEN_STANDALONE",
                vec![Arg::Str(name.into())],
            ));
        }
        if name != "[xml]" && self.cur().external && !skipped {
            self.s.state = STATE_TEXT_DECL;
        }
        Ok(())
    }

    /// `XMLDocumentScannerImpl.endEntity`.
    fn document_end_entity(&mut self, name: &str) -> R<()> {
        if self.s.entity_depth > 0 {
            self.s.entity_depth -= 1;
        }
        if self.s.markup_depth
            != self
                .s
                .entity_stack
                .get(self.s.entity_depth)
                .copied()
                .unwrap_or(0)
        {
            return Err(self.fatal("MarkupEntityMismatch", vec![]));
        }
        if name == "[xml]" {
            if self.s.markup_depth == 0 && self.s.driver == Driver::TrailingMisc {
                self.s.state = STATE_TERMINATED;
            } else {
                return Err(Stop::Eof);
            }
        }
        Ok(())
    }

    /// An entity started on a reference, skipped as `startEntity` skips an
    /// undeclared one.
    fn skipped_entity(&mut self, name: &str) -> R<()> {
        self.handler_start_entity(name, None, true)?;
        self.handler_end_entity(name, false)
    }

    // ----- The drivers -----

    /// `XMLNSDocumentScannerImpl.next`.
    pub(crate) fn next(&mut self) -> R<i32> {
        if self.s.last_was_end_element && self.s.bind_namespaces {
            self.s.last_was_end_element = false;
            self.s.ns.pop();
        }
        let event = match self.s.driver {
            Driver::XmlDecl => self.xml_decl_driver(),
            Driver::Prolog => self.prolog_driver(),
            Driver::Content => self.content_driver(),
            Driver::TrailingMisc => self.trailing_misc_driver(),
        }?;
        self.s.last_was_end_element = event == END_ELEMENT;
        Ok(event)
    }

    /// The catch blocks every driver has: a malformed byte sequence and an
    /// end of file.
    fn caught(&mut self, result: R<i32>, eof: impl FnOnce(&mut Parser) -> R<i32>) -> R<i32> {
        match result {
            Err(Stop::Eof) => eof(self),
            other => other,
        }
    }

    fn xml_decl_driver(&mut self) -> R<i32> {
        self.s.state = STATE_PROLOG;
        self.s.driver = Driver::Prolog;
        let result = (|| -> R<i32> {
            if self.skip_string("<?xml")? {
                if chars::is_space(self.peek_char()?) {
                    self.s.markup_depth += 1;
                    self.scan_xml_decl_or_text_decl(false)?;
                } else {
                    self.cur_mut().position = 0;
                }
            }
            self.cur_mut().chunking.may_read_chunks = true;
            Ok(START_DOCUMENT)
        })();
        let result = self.caught(result, |p| Err(p.fatal("PrematureEOF", vec![])));
        if self.api == Api::Stax
            && result.is_ok()
            && !self.doc11
            && self.cur().version.as_deref() == Some("1.1")
        {
            // XMLStreamReaderImpl.switchToXML11Scanner: a new scanner, reset,
            // keeping the entity depth and namespaces, reads its own
            // START_DOCUMENT where the old one stopped.
            // The entity manager still reports entities to the old one.
            let mut old = std::mem::take(&mut self.s);
            self.s.entity_depth = old.entity_depth;
            self.s.entity_stack = vec![0; old.entity_depth.max(4)];
            self.s.ns = std::mem::take(&mut old.ns);
            self.stale_document = Some(Box::new(old));
            self.doc11 = true;
            self.cur_mut().chunking.may_read_chunks = true;
            return self.xml_decl_driver();
        }
        result
    }

    fn prolog_driver(&mut self) -> R<i32> {
        let result = self.prolog();
        self.caught(result, |p| Err(p.fatal("PrematureEOF", vec![])))
    }

    fn prolog(&mut self) -> R<i32> {
        loop {
            match self.s.state {
                STATE_PROLOG => {
                    self.skip_spaces()?;
                    if self.skip_char('<' as i32, None)? {
                        self.s.state = STATE_START_OF_MARKUP;
                    } else if self.skip_char('&' as i32, Some(NameType::Reference))? {
                        self.s.state = STATE_REFERENCE;
                    } else {
                        self.s.state = STATE_CONTENT;
                    }
                }
                STATE_START_OF_MARKUP => {
                    self.s.markup_depth += 1;
                    let c = self.peek_char()?;
                    if self.is_valid_name_start_char(c)
                        || self.is_valid_name_start_high_surrogate(c)
                    {
                        self.s.state = STATE_ROOT_ELEMENT;
                        self.s.driver = Driver::Content;
                        return self.content_driver();
                    } else if self.skip_char('!' as i32, None)? {
                        if self.skip_char('-' as i32, None)? {
                            if !self.skip_char('-' as i32, None)? {
                                return Err(self.fatal("InvalidCommentStart", vec![]));
                            }
                            self.s.state = STATE_COMMENT;
                        } else if self.skip_string("DOCTYPE")? {
                            self.s.state = STATE_DOCTYPE;
                        } else {
                            return Err(self.fatal("MarkupNotRecognizedInProlog", vec![]));
                        }
                    } else if self.skip_char('?' as i32, None)? {
                        self.s.state = STATE_PI;
                    } else {
                        return Err(self.fatal("MarkupNotRecognizedInProlog", vec![]));
                    }
                }
                _ => break,
            }
        }
        match self.s.state {
            STATE_COMMENT => {
                self.scan_comment_markup()?;
                self.s.state = STATE_PROLOG;
                Ok(COMMENT)
            }
            STATE_PI => {
                self.scan_pi()?;
                self.s.state = STATE_PROLOG;
                Ok(PROCESSING_INSTRUCTION)
            }
            STATE_DOCTYPE => {
                if self.s.seen_doctype_decl {
                    return Err(self.fatal("AlreadySeenDoctype", vec![]));
                }
                self.s.seen_doctype_decl = true;
                if self.scan_doctype_decl()? {
                    self.s.state = STATE_DTD_INTERNAL_DECLS;
                    self.s.driver = Driver::Content;
                    return self.dtd_driver();
                }
                if self.s.doctype_system_id.is_some() {
                    self.s.state = STATE_DTD_EXTERNAL;
                    self.s.driver = Driver::Content;
                    return self.dtd_driver();
                }
                self.s.state = STATE_PROLOG;
                Ok(DTD)
            }
            STATE_CONTENT => Err(self.fatal("ContentIllegalInProlog", vec![])),
            STATE_REFERENCE => Err(self.fatal("ReferenceIllegalInProlog", vec![])),
            _ => Ok(-1),
        }
    }

    /// `setEndDTDScanState`.
    pub(crate) fn end_dtd_scan_state(&mut self) {
        self.s.state = STATE_PROLOG;
        self.s.driver = Driver::Prolog;
        self.handler = Handler::Document;
    }

    fn trailing_misc_driver(&mut self) -> R<i32> {
        if self.s.empty_element {
            self.s.empty_element = false;
            return Ok(END_ELEMENT);
        }
        let result = self.trailing_misc();
        match result {
            Err(Stop::Eof) => {
                if self.s.markup_depth != 0 {
                    return Err(self.fatal("PrematureEOF", vec![]));
                }
                self.s.state = STATE_TERMINATED;
                Ok(END_DOCUMENT)
            }
            other => other,
        }
    }

    fn trailing_misc(&mut self) -> R<i32> {
        if self.s.state == STATE_TERMINATED {
            return Ok(END_DOCUMENT);
        }
        loop {
            match self.s.state {
                STATE_TRAILING_MISC => {
                    self.skip_spaces()?;
                    if self.s.state == STATE_TERMINATED {
                        return Ok(END_DOCUMENT);
                    }
                    if self.skip_char('<' as i32, None)? {
                        self.s.state = STATE_START_OF_MARKUP;
                    } else {
                        self.s.state = STATE_CONTENT;
                    }
                }
                STATE_START_OF_MARKUP => {
                    self.s.markup_depth += 1;
                    if self.skip_char('?' as i32, None)? {
                        self.s.state = STATE_PI;
                    } else if self.skip_char('!' as i32, None)? {
                        self.s.state = STATE_COMMENT;
                    } else {
                        // A '/' is consumed first, as the trailing-misc
                        // driver does, before the same error.
                        self.skip_char('/' as i32, None)?;
                        return Err(self.fatal("MarkupNotRecognizedInMisc", vec![]));
                    }
                }
                _ => break,
            }
        }
        match self.s.state {
            STATE_PI => {
                self.scan_pi()?;
                self.s.state = STATE_TRAILING_MISC;
                Ok(PROCESSING_INSTRUCTION)
            }
            STATE_COMMENT => {
                if !self.skip_string("--")? {
                    return Err(self.fatal("InvalidCommentStart", vec![]));
                }
                self.scan_comment_markup()?;
                self.s.state = STATE_TRAILING_MISC;
                Ok(COMMENT)
            }
            STATE_CONTENT => {
                if self.peek_char()? == -1 {
                    self.s.state = STATE_TERMINATED;
                    return Ok(END_DOCUMENT);
                }
                Err(self.fatal("ContentIllegalInTrailingMisc", vec![]))
            }
            STATE_TERMINATED => {
                self.s.state = STATE_NO_SUCH_ELEMENT;
                Ok(END_DOCUMENT)
            }
            STATE_NO_SUCH_ELEMENT => Err(Stop::Runtime {
                class: "java.util.NoSuchElementException",
                message: "No more events to be parsed".into(),
            }),
            state => Err(Stop::Xni(format!("Scanner State {state} not Recognized "))),
        }
    }

    /// The content driver (`FragmentContentDriver` with
    /// `XMLDocumentScannerImpl.ContentDriver`'s and the namespace scanner's
    /// hooks).
    fn content_driver(&mut self) -> R<i32> {
        let result = self.content();
        // ContentDriver.endOfFileHook.
        self.caught(result, |p| Err(p.fatal("PrematureEOF", vec![])))
    }

    fn start_of_markup(&mut self) -> R<()> {
        self.s.markup_depth += 1;
        let ch = self.peek_char()?;
        if self.is_valid_name_start_char(ch) || self.is_valid_name_start_high_surrogate(ch) {
            self.s.state = STATE_START_ELEMENT_TAG;
            return Ok(());
        }
        match ch {
            c if c == '?' as i32 => {
                self.s.state = STATE_PI;
                self.skip_char(c, None)?;
            }
            c if c == '!' as i32 => {
                self.skip_char(c, None)?;
                if self.skip_char('-' as i32, None)? {
                    if !self.skip_char('-' as i32, Some(NameType::Comment))? {
                        return Err(self.fatal("InvalidCommentStart", vec![]));
                    }
                    self.s.state = STATE_COMMENT;
                } else if self.skip_string("[CDATA[")? {
                    self.s.state = STATE_CDATA;
                } else if self.skip_string("DOCTYPE")? {
                    // ContentDriver.scanForDoctypeHook: a state nothing
                    // handles in content.
                    self.s.state = STATE_DOCTYPE;
                } else {
                    return Err(self.fatal("MarkupNotRecognizedInContent", vec![]));
                }
            }
            c if c == '/' as i32 => {
                self.s.state = STATE_END_ELEMENT_TAG;
                self.skip_char(c, Some(NameType::ElementEnd))?;
            }
            _ => return Err(self.fatal("MarkupNotRecognizedInContent", vec![])),
        }
        Ok(())
    }

    fn content(&mut self) -> R<i32> {
        loop {
            if self.s.state == STATE_CONTENT {
                let ch = self.peek_char()?;
                if ch == '<' as i32 {
                    self.scan_char(None)?;
                    self.s.state = STATE_START_OF_MARKUP;
                } else if ch == '&' as i32 {
                    self.scan_char(Some(NameType::Reference))?;
                    self.s.state = STATE_REFERENCE;
                } else {
                    self.s.state = STATE_CHARACTER_DATA;
                }
            }
            if self.s.state == STATE_START_OF_MARKUP {
                self.start_of_markup()?;
            }
            match self.s.state {
                STATE_START_ELEMENT_TAG => {
                    let empty = self.scan_start_element()?;
                    self.s.empty_element = empty;
                    self.s.state = if empty {
                        STATE_END_ELEMENT_TAG
                    } else {
                        STATE_CONTENT
                    };
                    return Ok(START_ELEMENT);
                }
                STATE_CHARACTER_DATA => return self.character_data(),
                STATE_END_ELEMENT_TAG => {
                    if self.s.empty_element {
                        self.s.empty_element = false;
                        self.s.state = STATE_CONTENT;
                        if self.s.markup_depth == 0 {
                            self.element_depth_is_zero();
                        }
                        return Ok(END_ELEMENT);
                    } else if self.scan_end_element()? == 0 {
                        self.element_depth_is_zero();
                        return Ok(END_ELEMENT);
                    }
                    self.s.state = STATE_CONTENT;
                    return Ok(END_ELEMENT);
                }
                STATE_COMMENT => {
                    self.scan_comment_markup()?;
                    self.s.state = STATE_CONTENT;
                    return Ok(COMMENT);
                }
                STATE_PI => {
                    self.scan_pi()?;
                    self.s.state = STATE_CONTENT;
                    return Ok(PROCESSING_INSTRUCTION);
                }
                STATE_CDATA => {
                    self.scan_cdata_section()?;
                    self.s.state = if !self.s.cdata_end {
                        STATE_CDATA
                    } else {
                        STATE_CONTENT
                    };
                    return Ok(if self.api == Api::Stax {
                        CDATA
                    } else {
                        CHARACTERS
                    });
                }
                STATE_REFERENCE => {
                    self.s.markup_depth += 1;
                    self.s.found_builtin_refs = false;
                    if self.skip_char('#' as i32, Some(NameType::Reference))? {
                        self.scan_char_reference_value(None)?;
                        self.s.markup_depth -= 1;
                        self.s.state = STATE_CONTENT;
                        return Ok(CHARACTERS);
                    }
                    self.scan_entity_reference()?;
                    if self.s.state == STATE_BUILT_IN_REFS {
                        self.s.state = STATE_CONTENT;
                        if self.s.builtin_handled {
                            self.s.builtin_handled = false;
                            return Ok(ENTITY_REFERENCE);
                        }
                        return Ok(CHARACTERS);
                    }
                    if self.s.state == STATE_TEXT_DECL {
                        continue;
                    }
                    if self.s.state == STATE_REFERENCE {
                        self.s.state = STATE_CONTENT;
                        if self
                            .s
                            .current_entity_name
                            .as_ref()
                            .is_some_and(|n| self.declared.contains_key(n))
                        {
                            continue;
                        }
                        return Ok(ENTITY_REFERENCE);
                    }
                    self.s.state = STATE_CONTENT;
                    continue;
                }
                STATE_TEXT_DECL => {
                    if self.skip_string("<?xml")? {
                        self.s.markup_depth += 1;
                        let res = {
                            let c = self.peek_char()?;
                            self.is_valid_name_char(c)
                        };
                        if res {
                            let mut target = String::from("xml");
                            loop {
                                let c = self.peek_char()?;
                                if !self.is_valid_ncname(c) {
                                    break;
                                }
                                target.push(
                                    char::from_u32(self.scan_char(None)? as u32)
                                        .unwrap_or('\u{FFFD}'),
                                );
                            }
                            self.scan_pi_data(&target)?;
                            self.s.markup_depth -= 1;
                        } else {
                            self.scan_xml_decl_or_text_decl(true)?;
                        }
                    }
                    self.cur_mut().chunking.may_read_chunks = true;
                    self.s.state = STATE_CONTENT;
                    continue;
                }
                STATE_ROOT_ELEMENT => {
                    // NSContentDriver.scanRootElementHook.
                    self.reconfigure_pipeline();
                    if self.scan_start_element()? {
                        self.s.state = STATE_TRAILING_MISC;
                        self.s.driver = Driver::TrailingMisc;
                        self.s.empty_element = true;
                        return Ok(START_ELEMENT);
                    }
                    self.s.state = STATE_CONTENT;
                    return Ok(START_ELEMENT);
                }
                STATE_CHAR_REFERENCE => {
                    self.scan_char_reference_value(None)?;
                    self.s.markup_depth -= 1;
                    self.s.state = STATE_CONTENT;
                    return Ok(CHARACTERS);
                }
                state => return Err(Stop::Xni(format!("Scanner State {state} not Recognized "))),
            }
        }
    }

    /// `elementDepthIsZeroHook`.
    fn element_depth_is_zero(&mut self) {
        self.s.state = STATE_TRAILING_MISC;
        self.s.driver = Driver::TrailingMisc;
        if self.api == Api::Stax && self.s.root_end.is_none() {
            self.s.root_end = Some(self.document_offset());
        }
    }

    /// `NSContentDriver.reconfigurePipeline`.
    fn reconfigure_pipeline(&mut self) {
        self.s.bind_namespaces = self.api == Api::Stax || !self.s.has_grammar;
    }

    fn character_data(&mut self) -> R<i32> {
        let mut c = self.scan_content()?;
        if self.skip_char('<' as i32, None)? {
            if self.skip_char('/' as i32, Some(NameType::ElementEnd))? {
                self.s.markup_depth += 1;
                self.s.state = STATE_END_ELEMENT_TAG;
            } else if chars::is_name_start(self.peek_char()?) {
                self.s.markup_depth += 1;
                self.s.state = STATE_START_ELEMENT_TAG;
            } else {
                self.s.state = STATE_START_OF_MARKUP;
            }
            return Ok(CHARACTERS);
        }
        if c == '\r' as i32 {
            self.scan_char(None)?;
            c = -1;
        } else if c == ']' as i32 {
            self.scan_char(None)?;
            if self.skip_char(']' as i32, None)? {
                while self.skip_char(']' as i32, None)? {}
                if self.skip_char('>' as i32, None)? {
                    return Err(self.fatal("CDEndInContent", vec![]));
                }
            }
            c = -1;
        }
        if c == '<' as i32 {
            self.scan_char(None)?;
            self.s.state = STATE_START_OF_MARKUP;
        } else if c == '&' as i32 {
            self.scan_char(Some(NameType::Reference))?;
            self.s.state = STATE_REFERENCE;
        } else if c != -1 && self.is_invalid_literal(c) {
            if chars::is_high_surrogate(c) {
                self.scan_surrogates()?;
                self.s.state = STATE_CONTENT;
            } else {
                return Err(self.fatal("InvalidCharInContent", vec![Arg::Str(format!("{c:x}"))]));
            }
        } else {
            self.scan_content_more()?;
            self.s.state = STATE_CONTENT;
        }
        Ok(CHARACTERS)
    }

    /// `XMLDocumentFragmentScannerImpl.scanContent(XMLStringBuffer)` (or
    /// XML 1.1's).
    fn scan_content_more(&mut self) -> R<i32> {
        let mut c = self.scan_content()?;
        let newline = if self.doc11 {
            c == '\r' as i32 || c == 0x85 || c == 0x2028
        } else {
            c == '\r' as i32
        };
        if newline {
            self.scan_char(None)?;
            c = -1;
            if !self.doc11 {
                return Ok(c);
            }
        }
        if c == ']' as i32 {
            self.scan_char(None)?;
            if self.skip_char(']' as i32, None)? {
                while self.skip_char(']' as i32, None)? {}
                if self.skip_char('>' as i32, None)? {
                    return Err(self.fatal("CDEndInContent", vec![]));
                }
            }
            c = -1;
        }
        Ok(c)
    }

    // ----- XMLScanner -----

    fn is_invalid(&self, c: i32) -> bool {
        if self.doc11 {
            chars::is_xml11_invalid(c)
        } else {
            chars::is_invalid(c)
        }
    }

    fn is_invalid_literal(&self, c: i32) -> bool {
        if self.doc11 {
            !chars::is_xml11_valid_literal(c)
        } else {
            chars::is_invalid(c)
        }
    }

    fn is_valid_name_char(&self, c: i32) -> bool {
        if self.doc11 {
            chars::is_xml11_name(c)
        } else {
            chars::is_name(c)
        }
    }

    fn is_valid_ncname(&self, c: i32) -> bool {
        if self.doc11 {
            chars::is_xml11_ncname(c)
        } else {
            chars::is_ncname(c)
        }
    }

    fn is_valid_name_start_char(&self, c: i32) -> bool {
        if self.doc11 {
            chars::is_xml11_name_start(c)
        } else {
            chars::is_name_start(c)
        }
    }

    fn is_valid_name_start_high_surrogate(&self, c: i32) -> bool {
        self.doc11 && chars::is_xml11_name_high_surrogate(c)
    }

    /// `XMLDocumentFragmentScannerImpl.scanXMLDeclOrTextDecl`.
    pub(crate) fn scan_xml_decl_or_text_decl(&mut self, text_decl: bool) -> R<()> {
        let (version, encoding, standalone) = self.scan_decl(text_decl)?;
        self.s.markup_depth -= 1;
        self.s.standalone = standalone.as_deref() == Some("yes");
        if let Some(v) = version {
            let e = self.cur_mut();
            e.version = Some(v.clone());
            e.xml_version = v;
        }
        if let Some(enc) = encoding
            && !self.cur().encoding_externally_specified
        {
            self.set_encoding(&enc)?;
        }
        Ok(())
    }

    /// `XMLScanner.scanXMLDeclOrTextDecl`: version, encoding, standalone.
    pub(crate) fn scan_decl(
        &mut self,
        text_decl: bool,
    ) -> R<(Option<String>, Option<String>, Option<String>)> {
        let mut version: Option<String> = None;
        let mut encoding: Option<String> = None;
        let mut standalone: Option<String> = None;
        let mut state = 0;
        let mut data_found = false;
        let mut saw_space = self.skip_spaces()?;
        let literal = self.cur().literal;
        self.cur_mut().literal = false;
        while self.peek_char()? != '?' as i32 {
            data_found = true;
            let (name, value) = self.scan_pseudo_attribute(text_decl)?;
            match state {
                0 => {
                    if name == "version" {
                        if !saw_space {
                            return Err(self.fatal(
                                if text_decl {
                                    "SpaceRequiredBeforeVersionInTextDecl"
                                } else {
                                    "SpaceRequiredBeforeVersionInXMLDecl"
                                },
                                vec![],
                            ));
                        }
                        state = 1;
                        if !(value == "1.0" || value == "1.1") {
                            return Err(self.fatal("VersionNotSupported", vec![Arg::Str(value)]));
                        }
                        if value == "1.1" {
                            let top_version = self
                                .entities
                                .first()
                                .filter(|_| self.entities.len() > 1)
                                .map(|e| e.version.clone());
                            if let Some(v) = top_version
                                && (v.is_none() || v.as_deref() == Some("1.0"))
                            {
                                return Err(self.fatal("VersionMismatch", vec![]));
                            }
                            self.v11 = true;
                        }
                        version = Some(value);
                    } else if name == "encoding" {
                        if !text_decl {
                            return Err(self.fatal("VersionInfoRequired", vec![]));
                        }
                        if !saw_space {
                            return Err(self.fatal("SpaceRequiredBeforeEncodingInTextDecl", vec![]));
                        }
                        encoding = Some(value);
                        state = 3;
                    } else if text_decl {
                        return Err(self.fatal("EncodingDeclRequired", vec![]));
                    } else {
                        return Err(self.fatal("VersionInfoRequired", vec![]));
                    }
                }
                1 => {
                    if name == "encoding" {
                        if !saw_space {
                            return Err(self.fatal(
                                if text_decl {
                                    "SpaceRequiredBeforeEncodingInTextDecl"
                                } else {
                                    "SpaceRequiredBeforeEncodingInXMLDecl"
                                },
                                vec![],
                            ));
                        }
                        encoding = Some(value);
                        state = if text_decl { 3 } else { 2 };
                    } else if !text_decl && name == "standalone" {
                        if !saw_space {
                            return Err(self.fatal("SpaceRequiredBeforeStandalone", vec![]));
                        }
                        state = 3;
                        if value != "yes" && value != "no" {
                            return Err(self.fatal("SDDeclInvalid", vec![Arg::Str(value)]));
                        }
                        standalone = Some(value);
                    } else {
                        return Err(self.fatal("EncodingDeclRequired", vec![]));
                    }
                }
                2 => {
                    if name == "standalone" {
                        if !saw_space {
                            return Err(self.fatal("SpaceRequiredBeforeStandalone", vec![]));
                        }
                        state = 3;
                        if value != "yes" && value != "no" {
                            return Err(self.fatal("SDDeclInvalid", vec![Arg::Str(value)]));
                        }
                        standalone = Some(value);
                    } else {
                        return Err(self.fatal("SDDeclNameInvalid", vec![]));
                    }
                }
                _ => return Err(self.fatal("NoMorePseudoAttributes", vec![])),
            }
            saw_space = self.skip_spaces()?;
        }
        if literal {
            self.cur_mut().literal = true;
        }
        if text_decl && state != 3 {
            return Err(self.fatal("MorePseudoAttributes", vec![]));
        }
        if text_decl {
            if !data_found && encoding.is_none() {
                return Err(self.fatal("EncodingDeclRequired", vec![]));
            }
        } else if !data_found && version.is_none() {
            return Err(self.fatal("VersionInfoRequired", vec![]));
        }
        if !self.skip_char('?' as i32, None)? {
            return Err(self.fatal("XMLDeclUnterminated", vec![]));
        }
        if !self.skip_char('>' as i32, None)? {
            return Err(self.fatal("XMLDeclUnterminated", vec![]));
        }
        Ok((version, encoding, standalone))
    }

    fn scan_pseudo_attribute(&mut self, text_decl: bool) -> R<(String, String)> {
        let name = self.scan_pseudo_attribute_name()?;
        let Some(name) = name else {
            return Err(self.fatal("PseudoAttrNameExpected", vec![]));
        };
        self.skip_spaces()?;
        if !self.skip_char('=' as i32, None)? {
            return Err(self.fatal(
                if text_decl {
                    "EqRequiredInTextDecl"
                } else {
                    "EqRequiredInXMLDecl"
                },
                vec![Arg::Str(name.into())],
            ));
        }
        self.skip_spaces()?;
        let quote = self.peek_char()?;
        if quote != '\'' as i32 && quote != '"' as i32 {
            return Err(self.fatal(
                if text_decl {
                    "QuoteRequiredInTextDecl"
                } else {
                    "QuoteRequiredInXMLDecl"
                },
                vec![Arg::Str(name.into())],
            ));
        }
        self.scan_char(Some(NameType::Attribute))?;
        let mut c = self.scan_literal(quote, false)?;
        let mut value = self.literal_raw();
        while c != quote {
            if c != -1 {
                if c == '&' as i32 || c == '%' as i32 || c == '<' as i32 || c == ']' as i32 {
                    let ch = self.scan_char(Some(NameType::Attribute))?;
                    value.push_str(&utf16_char(ch));
                } else if chars::is_high_surrogate(c) {
                    value.push_str(&self.scan_surrogates()?);
                } else if self.is_invalid_literal(c) {
                    let key = if text_decl {
                        "InvalidCharInTextDecl"
                    } else {
                        "InvalidCharInXMLDecl"
                    };
                    return Err(self.fatal(key, vec![Arg::Str(format!("{c:x}"))]));
                }
            }
            c = self.scan_literal(quote, false)?;
            value.push_str(&self.literal_raw());
        }
        if !self.skip_char(quote, None)? {
            return Err(self.fatal(
                if text_decl {
                    "CloseQuoteMissingInTextDecl"
                } else {
                    "CloseQuoteMissingInXMLDecl"
                },
                vec![Arg::Str(name.into())],
            ));
        }
        Ok((name.to_string(), value))
    }

    fn scan_pseudo_attribute_name(&mut self) -> R<Option<&'static str>> {
        let c = self.peek_char()?;
        for name in ["version", "encoding", "standalone"] {
            if c == name.as_bytes()[0] as i32 && self.skip_string(name)? {
                return Ok(Some(name));
            }
        }
        Ok(None)
    }

    /// The characters the last `scan_literal` scanned.
    fn literal_text(&self) -> String {
        let (start, len) = (self.literal_start, self.literal_length);
        let e = self.cur();
        let mut v: Vec<u16> = e.ch[start..start + len].to_vec();
        for &i in &self.white_space {
            if i >= start && i < start + len {
                v[i - start] = ' ' as u16;
            }
        }
        String::from_utf16_lossy(&v)
    }

    /// `XMLEntityScanner.setEncoding`, for the byte stream StAX reads.
    pub(crate) fn set_encoding(&mut self, encoding: &str) -> R<()> {
        let e = self.cur();
        if e.stream.is_none() {
            return Ok(());
        }
        if e.encoding.as_deref() == Some(encoding) {
            return Ok(());
        }
        let upper = encoding.to_ascii_uppercase();
        if e.encoding
            .as_deref()
            .is_some_and(|c| c.starts_with("UTF-16"))
        {
            if upper == "UTF-16" {
                return Ok(());
            }
            let be = e.encoding.as_deref() == Some("UTF-16BE");
            if upper == "ISO-10646-UCS-4" {
                self.cur_mut().reader = Reader::ucs(true, be);
                return Ok(());
            }
            if upper == "ISO-10646-UCS-2" {
                self.cur_mut().reader = Reader::ucs(false, be);
                return Ok(());
            }
        }
        let size = self.cur().buffer_size;
        let reader = match upper.as_str() {
            "UTF-8" => Reader::utf8(size),
            "US-ASCII" => Reader::ascii(size),
            "ISO-10646-UCS-4" | "ISO-10646-UCS-2" => {
                return Err(self.fatal(
                    "EncodingByteOrderUnsupported",
                    vec![Arg::Str(encoding.into())],
                ));
            }
            _ => {
                if !chars::is_valid_iana_encoding(encoding) {
                    return Err(self.fatal("EncodingDeclInvalid", vec![Arg::Str(encoding.into())]));
                }
                match encodings::iana_to_java(&upper) {
                    None => {
                        return Err(
                            self.fatal("EncodingDeclInvalid", vec![Arg::Str(encoding.into())])
                        );
                    }
                    Some("ASCII") => Reader::ascii(BUFFER_SIZE),
                    Some(java) => java_reader(java)?,
                }
            }
        };
        let e = self.cur_mut();
        e.reader = reader;
        e.encoding = Some(encoding.to_string());
        Ok(())
    }

    /// `scanPI`.
    fn scan_pi(&mut self) -> R<()> {
        let target = self.scan_name(NameType::Pi)?;
        let Some(target) = target else {
            return Err(self.fatal("PITargetRequired", vec![]));
        };
        self.scan_pi_data(&target)?;
        self.s.markup_depth -= 1;
        Ok(())
    }

    pub(crate) fn scan_pi_data(&mut self, target: &str) -> R<()> {
        if target.encode_utf16().count() == 3 && target.to_lowercase() == "xml" {
            return Err(self.fatal("ReservedPITarget", vec![]));
        }
        if !self.skip_spaces()? {
            if self.skip_string("?>")? {
                return Ok(());
            }
            return Err(self.fatal("SpaceRequiredInPI", vec![]));
        }
        if self.scan_data("?>", 0)? {
            loop {
                let c = self.peek_char()?;
                if c != -1 {
                    if chars::is_high_surrogate(c) {
                        self.scan_surrogates()?;
                    } else if self.is_invalid_literal(c) {
                        return Err(self.fatal("InvalidCharInPI", vec![Arg::Str(format!("{c:x}"))]));
                    }
                }
                if !self.scan_data("?>", 0)? {
                    break;
                }
            }
        }
        Ok(())
    }

    /// `scanComment` with the fragment scanner's `fMarkupDepth--`.
    fn scan_comment_markup(&mut self) -> R<()> {
        self.scan_comment()?;
        self.s.markup_depth -= 1;
        Ok(())
    }

    pub(crate) fn scan_comment(&mut self) -> R<()> {
        while self.scan_data("--", 0)? {
            let c = self.peek_char()?;
            if c != -1 {
                if chars::is_high_surrogate(c) {
                    self.scan_surrogates()?;
                } else if self.is_invalid_literal(c) {
                    return Err(
                        self.fatal("InvalidCharInComment", vec![Arg::Str(format!("{c:x}"))])
                    );
                }
            }
        }
        if !self.skip_char('>' as i32, Some(NameType::Comment))? {
            return Err(self.fatal("DashDashInComment", vec![]));
        }
        Ok(())
    }

    /// `scanSurrogates`: the pair, or the error for a lone surrogate.
    fn scan_surrogates(&mut self) -> R<String> {
        let high = self.scan_char(None)?;
        let low = self.peek_char()?;
        if !chars::is_low_surrogate(low) {
            return Err(self.fatal("InvalidCharInContent", vec![Arg::Str(format!("{high:x}"))]));
        }
        self.scan_char(None)?;
        let c = chars::supplemental(high as u16, low as u16);
        if self.is_invalid(c) {
            return Err(self.fatal("InvalidCharInContent", vec![Arg::Str(format!("{c:x}"))]));
        }
        Ok(String::from_utf16_lossy(&[high as u16, low as u16]))
    }

    /// `scanCharReferenceValue`, after `&#`: the character's value.
    pub(crate) fn scan_char_reference_value(&mut self, into: Option<&mut String>) -> R<i32> {
        let mut digits = String::new();
        let hex = self.skip_char('x' as i32, Some(NameType::Reference))?;
        let is_digit = |c: i32| {
            if hex {
                (c >= '0' as i32 && c <= '9' as i32)
                    || (c >= 'a' as i32 && c <= 'f' as i32)
                    || (c >= 'A' as i32 && c <= 'F' as i32)
            } else {
                c >= '0' as i32 && c <= '9' as i32
            }
        };
        let c = self.peek_char()?;
        if is_digit(c) {
            self.scan_char(Some(NameType::Reference))?;
            digits.push(c as u8 as char);
            loop {
                let c = self.peek_char()?;
                if !is_digit(c) {
                    break;
                }
                self.scan_char(Some(NameType::Reference))?;
                digits.push(c as u8 as char);
            }
        } else {
            return Err(self.fatal(
                if hex {
                    "HexdigitRequiredInCharRef"
                } else {
                    "DigitRequiredInCharRef"
                },
                vec![],
            ));
        }
        if !self.skip_char(';' as i32, Some(NameType::Reference))? {
            return Err(self.fatal("SemicolonRequiredInCharRef", vec![]));
        }
        let literal = if hex {
            format!("x{digits}")
        } else {
            digits.clone()
        };
        // Integer.parseInt: an int, or a NumberFormatException.
        let value = match i32::from_str_radix(&digits, if hex { 16 } else { 10 }) {
            Ok(v) if !self.is_invalid(v) => v,
            _ => return Err(self.fatal("InvalidCharRef", vec![Arg::Str(literal)])),
        };
        if let Some(buf) = into {
            if chars::is_supplemental(value) {
                buf.push_str(&String::from_utf16_lossy(&[
                    chars::high_surrogate(value),
                    chars::low_surrogate(value),
                ]));
            } else {
                buf.push_str(&String::from_utf16_lossy(&[value as u16]));
            }
        }
        if !self.entities.is_empty() && self.cur().is_ge {
            let n = if chars::is_supplemental(value) { 2 } else { 1 };
            let name = self.cur().name.clone();
            self.check_entity_size(false, &name, n)?;
        }
        Ok(value)
    }

    /// `XMLScanner.checkEntityLimit(boolean, String, int)`.
    pub(crate) fn check_entity_size(&mut self, pe: bool, name: &str, len: usize) -> R<()> {
        if pe {
            let name = format!("%{name}");
            self.limits
                .add(Limit::ParameterEntitySize, &name, len as i64);
            if self.limits.over(Limit::ParameterEntitySize) {
                let args = vec![
                    Arg::Str(name),
                    Arg::Int(self.limits.value(Limit::ParameterEntitySize)),
                    Arg::Int(Limit::ParameterEntitySize.max()),
                    Arg::Str(Limit::ParameterEntitySize.system_property().into()),
                ];
                return Err(self.fatal("MaxEntitySizeLimit", args));
            }
        } else {
            self.limits.add(Limit::GeneralEntitySize, name, len as i64);
            if self.limits.over(Limit::GeneralEntitySize) {
                let args = vec![
                    Arg::Str(name.into()),
                    Arg::Int(self.limits.value(Limit::GeneralEntitySize)),
                    Arg::Int(Limit::GeneralEntitySize.max()),
                    Arg::Str(Limit::GeneralEntitySize.system_property().into()),
                ];
                return Err(self.fatal("MaxEntitySizeLimit", args));
            }
        }
        if self.limits.over(Limit::TotalEntitySize) {
            let args = vec![
                Arg::Int(self.limits.total(Limit::TotalEntitySize)),
                Arg::Int(Limit::TotalEntitySize.max()),
                Arg::Str(super::STATE_LITERAL.into()),
            ];
            return Err(self.fatal("TotalEntitySizeLimit", args));
        }
        Ok(())
    }

    // ----- Elements -----

    /// `checkDepth`.
    fn check_depth(&mut self, name: &str) -> R<()> {
        let depth = self.s.elements.len() as i64;
        self.limits.add(Limit::MaxElementDepth, name, depth);
        if self.limits.over(Limit::MaxElementDepth) {
            let args = vec![
                Arg::Str(name.into()),
                Arg::Int(self.limits.total(Limit::MaxElementDepth)),
                Arg::Int(Limit::MaxElementDepth.max()),
                Arg::Str(Limit::MaxElementDepth.system_property().into()),
            ];
            return Err(self.fatal("MaxElementDepthLimit", args));
        }
        Ok(())
    }

    /// `XMLNSDocumentScannerImpl.scanStartElement` (or the XML 1.1 one):
    /// true for an empty element.
    fn scan_start_element(&mut self) -> R<bool> {
        if self.doc11 {
            return self.scan_start_element11();
        }
        let mut qname = QName::default();
        self.scan_qname(&mut qname, NameType::ElementStart)?;
        self.s.elements.push(qname.clone());
        self.s.element_qname = qname.clone();
        self.s.current_element = qname.clone();
        let rawname = qname.raw();
        self.check_depth(&rawname)?;
        if self.s.bind_namespaces {
            self.s.ns.push();
        }
        self.s.empty_element = false;
        self.s.attributes.clear();
        if !self.seek_close_of_start_tag()? {
            self.s.xmlns_declared = false;
            loop {
                self.scan_attribute()?;
                if self.s.attributes.len() as i64 > Limit::ElementAttribute.max() {
                    let args = vec![
                        Arg::Str(rawname.clone()),
                        Arg::Int(Limit::ElementAttribute.max()),
                        Arg::Str(Limit::ElementAttribute.system_property().into()),
                    ];
                    return Err(self.error(Domain::Xml, "ElementAttributeLimit", args));
                }
                if self.seek_close_of_start_tag()? {
                    break;
                }
            }
        }
        if self.s.bind_namespaces {
            self.bind_element()?;
        }
        if self.s.empty_element {
            self.s.markup_depth -= 1;
            if self.s.markup_depth < self.s.entity_stack[self.s.entity_depth - 1] {
                return Err(self.fatal("ElementEntityMismatch", vec![Arg::Str(rawname)]));
            }
            self.validator_element(true)?;
            self.xinclude_hook(true)?;
            self.s.elements.pop();
        } else {
            self.validator_element(false)?;
            self.xinclude_hook(false)?;
        }
        Ok(self.s.empty_element)
    }

    /// The DOM pipeline's `XMLNSDTDValidator`, when the document has a
    /// DOCTYPE: an element starting (and ending, when empty).
    fn validator_element(&mut self, empty: bool) -> R<()> {
        if self.api == Api::Dom && self.s.has_grammar {
            self.dtd_validator_start_element()?;
            if empty {
                self.s.ns.pop();
            }
        }
        Ok(())
    }

    /// The DOM's `XIncludeHandler`, after the element's namespaces are
    /// bound.
    fn xinclude_hook(&mut self, empty: bool) -> R<()> {
        if self.api == Api::Dom {
            self.xinclude_start(empty)?;
        }
        Ok(())
    }

    /// The namespace checks after the attributes, in both scanners.
    fn bind_element(&mut self) -> R<()> {
        let q = self.s.element_qname.clone();
        if q.prefix.as_deref() == Some("xmlns") {
            return Err(self.error(Domain::Xmlns, "ElementXMLNSPrefix", vec![Arg::Str(q.raw())]));
        }
        let prefix = q.prefix.clone().unwrap_or_default();
        let uri = self.s.ns.uri(&prefix);
        if q.prefix.is_some() && uri.is_none() {
            return Err(self.error(
                Domain::Xmlns,
                "ElementPrefixUnbound",
                vec![Arg::Str(prefix), Arg::Str(q.raw())],
            ));
        }
        if let Some(top) = self.s.elements.last_mut() {
            top.uri = uri.clone();
        }
        for i in 0..self.s.attributes.len() {
            let a = self.s.attributes[i].name.clone();
            let aprefix = a.prefix.clone().unwrap_or_default();
            let uri = self.s.ns.uri(&aprefix);
            if a.uri.is_some() && a.uri == uri {
                continue;
            }
            if !aprefix.is_empty() {
                if uri.is_none() {
                    return Err(self.error(
                        Domain::Xmlns,
                        "AttributePrefixUnbound",
                        vec![Arg::Str(q.raw()), Arg::Str(a.raw()), Arg::Str(aprefix)],
                    ));
                }
                self.s.attributes[i].name.uri = uri;
            }
        }
        if self.s.attributes.len() > 1
            && let Some(name) = check_duplicates_ns(&self.s.attributes)
        {
            return Err(match &name.uri {
                Some(uri) => self.error(
                    Domain::Xmlns,
                    "AttributeNSNotUnique",
                    vec![
                        Arg::Str(q.raw()),
                        Arg::Str(name.localpart.clone().unwrap_or_default()),
                        Arg::Str(uri.clone()),
                    ],
                ),
                None => self.error(
                    Domain::Xmlns,
                    "AttributeNotUnique",
                    vec![Arg::Str(q.raw()), Arg::Str(name.raw())],
                ),
            });
        }
        Ok(())
    }

    /// `seekCloseOfStartTag`.
    fn seek_close_of_start_tag(&mut self) -> R<bool> {
        let saw_space = self.skip_spaces()?;
        let c = self.peek_char()?;
        if c == '>' as i32 {
            self.scan_char(None)?;
            return Ok(true);
        }
        if c == '/' as i32 {
            self.scan_char(None)?;
            if !self.skip_char('>' as i32, Some(NameType::ElementEnd))? {
                return Err(self.fatal(
                    "ElementUnterminated",
                    vec![Arg::Str(self.s.element_qname.raw())],
                ));
            }
            self.s.empty_element = true;
            return Ok(true);
        }
        if (!self.is_valid_name_start_char(c) || !saw_space)
            && (!self.is_valid_name_start_high_surrogate(c) || !saw_space)
        {
            return Err(self.fatal(
                "ElementUnterminated",
                vec![Arg::Str(self.s.element_qname.raw())],
            ));
        }
        Ok(false)
    }

    /// `XMLNSDocumentScannerImpl.scanAttribute`.
    fn scan_attribute(&mut self) -> R<()> {
        let mut qname = QName::default();
        self.scan_qname(&mut qname, NameType::AttributeName)?;
        self.s.attribute_qname = qname.clone();
        self.skip_spaces()?;
        if !self.skip_char('=' as i32, Some(NameType::Attribute))? {
            return Err(self.fatal(
                "EqRequiredInAttribute",
                vec![
                    Arg::Str(self.s.current_element.raw()),
                    Arg::Str(qname.raw()),
                ],
            ));
        }
        self.skip_spaces()?;
        let localpart = qname.localpart.clone().unwrap_or_default();
        let prefix = qname.prefix.clone().unwrap_or_default();
        let is_ns_decl = self.s.bind_namespaces
            && (prefix == "xmlns" || (prefix.is_empty() && localpart == "xmlns"));
        let check_entities = self.s.has_external_dtd && !self.s.standalone;
        let element = self.s.current_element.raw();
        let value =
            self.scan_attribute_value(&qname.raw(), check_entities, &element, is_ns_decl)?;
        if self.s.bind_namespaces && is_ns_decl {
            if value.encode_utf16().count() > Limit::MaxName.max() as usize {
                let args = vec![
                    Arg::Str(value.clone()),
                    Arg::Int(value.encode_utf16().count() as i64),
                    Arg::Int(Limit::MaxName.max()),
                    Arg::Str(super::STATE_LITERAL.into()),
                ];
                return Err(self.error(Domain::Xml, "MaxXMLNameLimit", args));
            }
            let uri = value.clone();
            if prefix == "xmlns" && localpart == "xmlns" {
                return Err(self.error(
                    Domain::Xmlns,
                    "CantBindXMLNS",
                    vec![Arg::QName(qname.clone())],
                ));
            }
            if uri == XMLNS_URI {
                return Err(self.error(
                    Domain::Xmlns,
                    "CantBindXMLNS",
                    vec![Arg::QName(qname.clone())],
                ));
            }
            if localpart == "xml" {
                if uri != XML_URI {
                    return Err(self.error(
                        Domain::Xmlns,
                        "CantBindXML",
                        vec![Arg::QName(qname.clone())],
                    ));
                }
            } else if uri == XML_URI {
                return Err(self.error(
                    Domain::Xmlns,
                    "CantBindXML",
                    vec![Arg::QName(qname.clone())],
                ));
            }
            let bound = if localpart != "xmlns" {
                localpart.clone()
            } else {
                String::new()
            };
            if bound.is_empty() && localpart == "xmlns" {
                qname.prefix = Some("xmlns".into());
            }
            if uri.is_empty() && localpart != "xmlns" {
                return Err(self.error(
                    Domain::Xmlns,
                    "EmptyPrefixedAttName",
                    vec![Arg::QName(qname.clone())],
                ));
            }
            if self.s.ns.contains_in_current(&bound) {
                return Err(self.fatal(
                    "AttributeNotUnique",
                    vec![
                        Arg::Str(self.s.current_element.raw()),
                        Arg::Str(qname.raw()),
                    ],
                ));
            }
            let declared = self.s.ns.declare(
                &bound,
                if uri.is_empty() {
                    None
                } else {
                    Some(uri.clone())
                },
            );
            if !declared {
                if self.s.xmlns_declared {
                    return Err(self.fatal(
                        "AttributeNotUnique",
                        vec![
                            Arg::Str(self.s.current_element.raw()),
                            Arg::Str(qname.raw()),
                        ],
                    ));
                }
                self.s.xmlns_declared = true;
            }
            if self.api == Api::Stax {
                // fNotAddNSDeclAsAttribute.
                return Ok(());
            }
        }
        if self.s.bind_namespaces {
            if let Some(p) = &qname.prefix {
                qname.uri = self.s.ns.uri(p);
            }
            self.s.attributes.push(Attr { name: qname, value });
        } else {
            if self
                .s
                .attributes
                .iter()
                .any(|a| a.name.rawname == qname.rawname)
            {
                return Err(self.fatal(
                    "AttributeNotUnique",
                    vec![
                        Arg::Str(self.s.current_element.raw()),
                        Arg::Str(qname.raw()),
                    ],
                ));
            }
            if let Some(p) = &qname.prefix {
                qname.uri = self.s.ns.uri(p);
            }
            self.s.attributes.push(Attr { name: qname, value });
        }
        Ok(())
    }

    /// `XMLScanner.scanAttributeValue`: the value, normalized.
    /// The entity depth of the scanner the entity manager reports to.
    fn active_entity_depth(&self) -> usize {
        if self.handler == Handler::Dtd {
            self.d.entity_depth
        } else {
            self.s.entity_depth
        }
    }

    pub(crate) fn scan_attribute_value(
        &mut self,
        at_name: &str,
        check_entities: bool,
        ele_name: &str,
        is_nsuri: bool,
    ) -> R<String> {
        let quote = self.peek_char()?;
        if quote != '\'' as i32 && quote != '"' as i32 {
            return Err(self.fatal(
                "OpenQuoteExpected",
                vec![Arg::Str(ele_name.into()), Arg::Str(at_name.into())],
            ));
        }
        self.scan_char(Some(NameType::Attribute))?;
        let entity_depth = self.active_entity_depth();
        let mut c = self.scan_literal(quote, is_nsuri)?;
        let mut value = self.literal_text();
        if c != quote {
            self.s.scanning_attribute = true;
            loop {
                if c == '&' as i32 {
                    self.skip_char('&' as i32, Some(NameType::Reference))?;
                    if self.skip_char('#' as i32, Some(NameType::Reference))? {
                        self.scan_char_reference_value(Some(&mut value))?;
                    } else {
                        let name = self.scan_name(NameType::Entity)?;
                        if name.is_none() {
                            return Err(self.fatal("NameRequiredInReference", vec![]));
                        }
                        let name = name.unwrap();
                        if !self.skip_char(';' as i32, Some(NameType::Reference))? {
                            return Err(
                                self.fatal("SemicolonRequiredInReference", vec![Arg::Str(name)])
                            );
                        }
                        if let Some(ch) = builtin(&name) {
                            value.push(ch);
                            let entity = self.cur().name.clone();
                            self.check_entity_size(false, &entity, 1)?;
                        } else if self.declared.get(&name).is_some_and(|d| d.is_external()) {
                            return Err(
                                self.fatal("ReferenceToExternalEntity", vec![Arg::Str(name)])
                            );
                        } else {
                            if !self.declared.contains_key(&name) && !check_entities {
                                return Err(self.fatal("EntityNotDeclared", vec![Arg::Str(name)]));
                            }
                            self.start_entity(true, &name, true)?;
                        }
                    }
                } else if c == '<' as i32 {
                    return Err(self.fatal(
                        "LessthanInAttValue",
                        vec![Arg::Str(ele_name.into()), Arg::Str(at_name.into())],
                    ));
                } else if c == '%' as i32 || c == ']' as i32 {
                    self.scan_char(None)?;
                    value.push(c as u8 as char);
                } else if c != -1 && chars::is_high_surrogate(c) {
                    value.push_str(&self.scan_surrogates()?);
                } else if c != -1 && self.is_invalid_literal(c) {
                    return Err(self.fatal(
                        "InvalidCharInAttValue",
                        vec![
                            Arg::Str(ele_name.into()),
                            Arg::Str(at_name.into()),
                            Arg::Str(format!("{c:x}")),
                        ],
                    ));
                }
                c = self.scan_literal(quote, is_nsuri)?;
                value.push_str(&self.literal_text());
                if c == quote && entity_depth == self.active_entity_depth() {
                    break;
                }
            }
            self.s.scanning_attribute = false;
        }
        let cquote = self.scan_char(Some(NameType::Attribute))?;
        if cquote != quote {
            return Err(self.fatal(
                "CloseQuoteExpected",
                vec![Arg::Str(ele_name.into()), Arg::Str(at_name.into())],
            ));
        }
        Ok(value)
    }

    /// `XML11NSDocumentScannerImpl.scanStartElement`.
    fn scan_start_element11(&mut self) -> R<bool> {
        // A name that does not scan leaves the reused QName as it was.
        let mut qname = self.s.element_qname.clone();
        self.scan_qname(&mut qname, NameType::ElementStart)?;
        self.s.element_qname = qname.clone();
        let rawname = qname.raw();
        if self.s.bind_namespaces {
            self.s.ns.push();
        }
        self.s.elements.push(qname.clone());
        self.s.current_element = qname;
        let mut empty = false;
        self.s.attributes.clear();
        loop {
            let saw_space = self.skip_spaces()?;
            let c = self.peek_char()?;
            if c == '>' as i32 {
                self.scan_char(None)?;
                break;
            } else if c == '/' as i32 {
                self.scan_char(None)?;
                if !self.skip_char('>' as i32, None)? {
                    return Err(self.fatal("ElementUnterminated", vec![Arg::Str(rawname)]));
                }
                empty = true;
                break;
            } else if (!self.is_valid_name_start_char(c) || !saw_space)
                && (!self.is_valid_name_start_high_surrogate(c) || !saw_space)
            {
                return Err(self.fatal("ElementUnterminated", vec![Arg::Str(rawname)]));
            }
            self.scan_attribute11()?;
            if self.s.attributes.len() as i64 > Limit::ElementAttribute.max() {
                let args = vec![
                    Arg::Str(rawname.clone()),
                    Arg::Int(Limit::ElementAttribute.max()),
                    Arg::Str(Limit::ElementAttribute.system_property().into()),
                ];
                return Err(self.error(Domain::Xml, "ElementAttributeLimit", args));
            }
        }
        if self.s.bind_namespaces {
            self.bind_element()?;
        }
        if empty {
            self.s.markup_depth -= 1;
            if self.s.markup_depth < self.s.entity_stack[self.s.entity_depth - 1] {
                return Err(self.fatal(
                    "ElementEntityMismatch",
                    vec![Arg::Str(self.s.current_element.raw())],
                ));
            }
            self.validator_element(true)?;
            self.xinclude_hook(true)?;
            self.s.elements.pop();
        } else {
            self.validator_element(false)?;
            self.xinclude_hook(false)?;
        }
        Ok(empty)
    }

    /// `XML11NSDocumentScannerImpl.scanAttribute`.
    fn scan_attribute11(&mut self) -> R<()> {
        let mut qname = self.s.attribute_qname.clone();
        self.scan_qname(&mut qname, NameType::AttributeName)?;
        self.s.attribute_qname = qname.clone();
        self.skip_spaces()?;
        if !self.skip_char('=' as i32, Some(NameType::Attribute))? {
            return Err(self.fatal(
                "EqRequiredInAttribute",
                vec![
                    Arg::Str(self.s.current_element.raw()),
                    Arg::Str(qname.raw()),
                ],
            ));
        }
        self.skip_spaces()?;
        if !self.s.bind_namespaces
            && self
                .s
                .attributes
                .iter()
                .any(|a| a.name.rawname == qname.rawname)
        {
            return Err(self.fatal(
                "AttributeNotUnique",
                vec![
                    Arg::Str(self.s.current_element.raw()),
                    Arg::Str(qname.raw()),
                ],
            ));
        }
        let index = self.s.attributes.len();
        self.s.attributes.push(Attr {
            name: qname.clone(),
            value: String::new(),
        });
        let localpart = qname.localpart.clone().unwrap_or_default();
        let prefix = qname.prefix.clone().unwrap_or_default();
        let is_ns_decl = self.s.bind_namespaces
            && (prefix == "xmlns" || (prefix.is_empty() && localpart == "xmlns"));
        let check_entities = self.s.has_external_dtd && !self.s.standalone;
        let element = self.s.current_element.raw();
        let value =
            self.scan_attribute_value11(&qname.raw(), check_entities, &element, is_ns_decl)?;
        self.s.attributes[index].value = value.clone();
        if self.s.bind_namespaces {
            if is_ns_decl {
                if value.encode_utf16().count() > Limit::MaxName.max() as usize {
                    let args = vec![
                        Arg::Str(value.clone()),
                        Arg::Int(value.encode_utf16().count() as i64),
                        Arg::Int(Limit::MaxName.max()),
                        Arg::Str(super::STATE_LITERAL.into()),
                    ];
                    return Err(self.error(Domain::Xml, "MaxXMLNameLimit", args));
                }
                let uri = value;
                if prefix == "xmlns" && localpart == "xmlns" {
                    return Err(self.error(
                        Domain::Xmlns,
                        "CantBindXMLNS",
                        vec![Arg::QName(qname.clone())],
                    ));
                }
                if uri == XMLNS_URI {
                    return Err(self.error(
                        Domain::Xmlns,
                        "CantBindXMLNS",
                        vec![Arg::QName(qname.clone())],
                    ));
                }
                if localpart == "xml" {
                    if uri != XML_URI {
                        return Err(self.error(
                            Domain::Xmlns,
                            "CantBindXML",
                            vec![Arg::QName(qname.clone())],
                        ));
                    }
                } else if uri == XML_URI {
                    return Err(self.error(
                        Domain::Xmlns,
                        "CantBindXML",
                        vec![Arg::QName(qname.clone())],
                    ));
                }
                let bound = if localpart != "xmlns" {
                    localpart.clone()
                } else {
                    String::new()
                };
                self.s
                    .ns
                    .declare(&bound, if uri.is_empty() { None } else { Some(uri) });
                self.s.attributes[index].name.uri = self.s.ns.uri("xmlns");
            } else if let Some(p) = &qname.prefix {
                self.s.attributes[index].name.uri = self.s.ns.uri(p);
            }
        }
        Ok(())
    }

    /// `XML11DocumentScannerImpl.scanAttributeValue`.
    fn scan_attribute_value11(
        &mut self,
        at_name: &str,
        check_entities: bool,
        ele_name: &str,
        is_nsuri: bool,
    ) -> R<String> {
        let quote = self.peek_char()?;
        if quote != '\'' as i32 && quote != '"' as i32 {
            return Err(self.fatal(
                "OpenQuoteExpected",
                vec![Arg::Str(ele_name.into()), Arg::Str(at_name.into())],
            ));
        }
        self.scan_char(Some(NameType::Attribute))?;
        let entity_depth = self.s.entity_depth;
        let mut c = self.scan_literal(quote, is_nsuri)?;
        let space11 = |s: String| {
            s.chars()
                .map(|c| if chars::is_space(c as i32) { ' ' } else { c })
                .collect::<String>()
        };
        let raw = self.literal_raw();
        if c == quote && !raw.chars().any(|c| chars::is_space(c as i32)) {
            let cquote = self.scan_char(Some(NameType::Attribute))?;
            if cquote != quote {
                return Err(self.fatal(
                    "CloseQuoteExpected",
                    vec![Arg::Str(ele_name.into()), Arg::Str(at_name.into())],
                ));
            }
            return Ok(raw);
        }
        let mut value = space11(raw);
        if c != quote {
            self.s.scanning_attribute = true;
            loop {
                if c == '&' as i32 {
                    self.skip_char('&' as i32, Some(NameType::Reference))?;
                    if self.skip_char('#' as i32, Some(NameType::Reference))? {
                        self.scan_char_reference_value(Some(&mut value))?;
                    } else {
                        let name = self.scan_name(NameType::Reference)?;
                        let Some(name) = name else {
                            return Err(self.fatal("NameRequiredInReference", vec![]));
                        };
                        if !self.skip_char(';' as i32, Some(NameType::Reference))? {
                            return Err(
                                self.fatal("SemicolonRequiredInReference", vec![Arg::Str(name)])
                            );
                        }
                        if let Some(ch) = builtin(&name) {
                            value.push(ch);
                            let entity = self.cur().name.clone();
                            self.check_entity_size(false, &entity, 1)?;
                        } else if self.declared.get(&name).is_some_and(|d| d.is_external()) {
                            return Err(
                                self.fatal("ReferenceToExternalEntity", vec![Arg::Str(name)])
                            );
                        } else {
                            if !self.declared.contains_key(&name) && !check_entities {
                                return Err(self.fatal("EntityNotDeclared", vec![Arg::Str(name)]));
                            }
                            self.start_entity(true, &name, true)?;
                        }
                    }
                } else if c == '<' as i32 {
                    return Err(self.fatal(
                        "LessthanInAttValue",
                        vec![Arg::Str(ele_name.into()), Arg::Str(at_name.into())],
                    ));
                } else if c == '%' as i32 || c == ']' as i32 {
                    self.scan_char(None)?;
                    value.push(c as u8 as char);
                } else if c != -1 && chars::is_high_surrogate(c) {
                    value.push_str(&self.scan_surrogates()?);
                } else if c != -1 && self.is_invalid_literal(c) {
                    return Err(self.fatal(
                        "InvalidCharInAttValue",
                        vec![
                            Arg::Str(ele_name.into()),
                            Arg::Str(at_name.into()),
                            Arg::Str(format!("{c:x}")),
                        ],
                    ));
                }
                c = self.scan_literal(quote, is_nsuri)?;
                value.push_str(&space11(self.literal_raw()));
                if c == quote && entity_depth == self.s.entity_depth {
                    break;
                }
            }
            self.s.scanning_attribute = false;
        }
        let cquote = self.scan_char(None)?;
        if cquote != quote {
            return Err(self.fatal(
                "CloseQuoteExpected",
                vec![Arg::Str(ele_name.into()), Arg::Str(at_name.into())],
            ));
        }
        Ok(value)
    }

    /// The characters the last `scan_literal` scanned, as they are.
    fn literal_raw(&self) -> String {
        let (start, len) = (self.literal_start, self.literal_length);
        String::from_utf16_lossy(&self.cur().ch[start..start + len])
    }

    /// `scanEndElement`: the markup depth after it.
    fn scan_end_element(&mut self) -> R<i32> {
        let Some(name) = self.s.elements.pop() else {
            return Err(Stop::Runtime {
                class: "java.lang.ArrayIndexOutOfBoundsException",
                message: "Index -1 out of bounds for length 20".into(),
            });
        };
        let rawname = name.raw();
        if !self.skip_string(&rawname)? {
            return Err(self.fatal("ETagRequired", vec![Arg::Str(rawname)]));
        }
        self.skip_spaces()?;
        if !self.skip_char('>' as i32, Some(NameType::ElementEnd))? {
            return Err(self.fatal("ETagUnterminated", vec![Arg::Str(rawname)]));
        }
        self.s.markup_depth -= 2;
        if self.s.markup_depth < self.s.entity_stack[self.s.entity_depth - 1] {
            return Err(self.fatal("ElementEntityMismatch", vec![Arg::Str(rawname)]));
        }
        if self.api == Api::Dom && self.s.has_grammar {
            // XMLNSDTDValidator.endNamespaceScope.
            self.s.ns.pop();
        }
        if self.api == Api::Dom {
            self.xinclude_end(&name)?;
        }
        Ok(self.s.markup_depth)
    }

    /// `scanEntityReference`, after `&`.
    fn scan_entity_reference(&mut self) -> R<()> {
        let name = self.scan_name(NameType::Reference)?;
        let Some(name) = name else {
            return Err(self.fatal("NameRequiredInReference", vec![]));
        };
        if !self.skip_char(';' as i32, Some(NameType::Reference))? {
            return Err(self.fatal("SemicolonRequiredInReference", vec![Arg::Str(name)]));
        }
        if self.declared.get(&name).is_some_and(|d| d.is_unparsed()) {
            return Err(self.fatal("ReferenceToUnparsedEntity", vec![Arg::Str(name)]));
        }
        self.s.markup_depth -= 1;
        self.s.current_entity_name = Some(name.clone());
        if builtin(&name).is_some() {
            // handleCharacter.
            self.s.found_builtin_refs = true;
            let entity = self.cur().name.clone();
            self.check_entity_size(false, &entity, 1)?;
            self.s.builtin_handled = true;
            self.s.state = STATE_BUILT_IN_REFS;
            return Ok(());
        }
        if !self.declared.contains_key(&name) && !(self.s.has_external_dtd && !self.s.standalone) {
            return Err(self.fatal("EntityNotDeclared", vec![Arg::Str(name)]));
        }
        self.start_entity(true, &name, false)
    }

    /// `scanCDATASection`.
    fn scan_cdata_section(&mut self) -> R<()> {
        loop {
            if !self.scan_data("]]>", 0)? {
                self.s.cdata_end = true;
                self.s.markup_depth -= 1;
                return Ok(());
            }
            let c = self.peek_char()?;
            if c != -1 && self.is_invalid_literal(c) {
                if chars::is_high_surrogate(c) {
                    self.scan_surrogates()?;
                } else {
                    return Err(self.fatal("InvalidCharInCDSect", vec![Arg::Str(format!("{c:x}"))]));
                }
            } else {
                self.s.cdata_end = false;
                return Ok(());
            }
        }
    }

    /// `XMLEntityManager.startEntity(boolean, String, boolean)`: an entity
    /// a reference names.
    pub(crate) fn start_entity(&mut self, is_ge: bool, name: &str, literal: bool) -> R<()> {
        let Some(decl) = self.declared.get(name).cloned() else {
            return self.skipped_entity(name);
        };
        if decl.is_unparsed() {
            return self.skipped_entity(name);
        }
        // A reference to an entity being scanned.
        if let Some(i) = self.entities.iter().rposition(|e| e.name == name) {
            let mut path = name.to_string();
            let top = self.entities.len() - 1;
            for e in &self.entities[(i + 1).min(top)..top] {
                path.push_str(" -> ");
                path.push_str(&e.name);
            }
            path.push_str(" -> ");
            path.push_str(&self.cur().name);
            path.push_str(" -> ");
            path.push_str(name);
            return Err(self.fatal(
                "RecursiveReference",
                vec![Arg::Str(name.into()), Arg::Str(path)],
            ));
        }
        match decl {
            super::EntityDecl::Internal { text, .. } => {
                let chars: Vec<u16> = text.encode_utf16().collect();
                self.start_scanned_entity(
                    is_ge,
                    name,
                    Reader::chars(chars),
                    None,
                    None,
                    literal,
                    false,
                    None,
                )
            }
            super::EntityDecl::External {
                system_id,
                base_system_id,
                ..
            } => {
                let (bytes, expanded) =
                    self.open_external(&system_id, base_system_id.as_deref())?;
                self.start_external_entity(is_ge, name, bytes, expanded, literal)
            }
        }
    }

    /// An external entity: its bytes, read through encoding detection as
    /// the document's are.
    pub(crate) fn start_external_entity(
        &mut self,
        is_ge: bool,
        name: &str,
        bytes: Vec<u8>,
        expanded: String,
        literal: bool,
    ) -> R<()> {
        let mut stream = Stream::new(bytes);
        let mut b4 = [0i32; 4];
        for b in &mut b4 {
            *b = stream.read_and_buffer();
        }
        let (encoding, reader_encoding, big_endian, bom) = encoding_info(&b4);
        stream.reset();
        if bom {
            if reader_encoding == "UTF-8" {
                stream.skip(3);
            } else if reader_encoding == "UTF-16" {
                stream.skip(2);
            }
        }
        let reader = self.manager_reader(reader_encoding, big_endian)?;
        self.start_scanned_entity(
            is_ge,
            name,
            reader,
            Some(stream),
            Some(encoding.into()),
            literal,
            true,
            Some(expanded),
        )
    }

    /// Reads an external entity or DTD from the file system as
    /// `XMLEntityManager` opens it: relative to the entity that declares it,
    /// or to the working directory for the document itself.
    pub(crate) fn open_external(
        &self,
        system_id: &str,
        base: Option<&str>,
    ) -> R<(Vec<u8>, String)> {
        let path = if let Some(rest) = system_id.strip_prefix("file:") {
            PathBufExt::from_file_uri(rest)
        } else if system_id.contains("://") {
            return Err(Stop::Io {
                class: "java.io.IOException",
                message: format!(
                    "Saranga does not read external entities over the network: {system_id}"
                ),
            });
        } else {
            let dir = base
                .and_then(|b| b.strip_prefix("file:"))
                .map(PathBufExt::from_file_uri)
                .and_then(|p| p.parent().map(|d| d.to_path_buf()))
                .unwrap_or_else(|| self.base_dir.clone());
            dir.join(system_id)
        };
        match std::fs::read(&path) {
            Ok(bytes) => {
                let uri = format!("file:{}", path.to_string_lossy().replace('\\', "/"));
                Ok((
                    bytes,
                    if uri.starts_with("file:/") {
                        uri
                    } else {
                        uri.replacen("file:", "file:/", 1)
                    },
                ))
            }
            Err(e) => Err(Stop::Io {
                class: "java.io.FileNotFoundException",
                message: not_found(&path, &e),
            }),
        }
    }
}

struct PathBufExt;

impl PathBufExt {
    fn from_file_uri(rest: &str) -> std::path::PathBuf {
        let rest = rest.trim_start_matches("//");
        #[cfg(windows)]
        let rest = rest.trim_start_matches('/');
        std::path::PathBuf::from(rest)
    }
}

/// `FileNotFoundException`'s message: the path and the system's reason.
pub(crate) fn not_found(path: &std::path::Path, e: &std::io::Error) -> String {
    let reason = if e.kind() == std::io::ErrorKind::NotFound {
        if cfg!(windows) {
            "The system cannot find the file specified"
        } else {
            "No such file or directory"
        }
    } else if e.kind() == std::io::ErrorKind::PermissionDenied {
        if cfg!(windows) {
            "Access is denied"
        } else {
            "Permission denied"
        }
    } else {
        "Is a directory"
    };
    format!("{} ({reason})", path.display())
}

/// The character of a predefined entity.
fn builtin(name: &str) -> Option<char> {
    match name {
        "amp" => Some('&'),
        "lt" => Some('<'),
        "gt" => Some('>'),
        "quot" => Some('"'),
        "apos" => Some('\''),
        _ => None,
    }
}

fn utf16_char(c: i32) -> String {
    String::from_utf16_lossy(&[c as u16])
}

/// `new InputStreamReader(stream, java)`.
fn java_reader(java: &str) -> R<Reader> {
    let charset = if let Some(hex) = encodings::single_byte(java) {
        Charset::SingleByte(
            (0..256)
                .map(|i| u16::from_str_radix(&hex[i * 4..i * 4 + 4], 16).unwrap())
                .collect(),
        )
    } else {
        match java {
            "UTF8" => return Ok(Reader::utf8(BUFFER_SIZE)),
            "UTF-16" | "UnicodeBig" => Charset::Utf16 {
                big_endian: true,
                bom: true,
            },
            "UnicodeLittle" => Charset::Utf16 {
                big_endian: false,
                bom: true,
            },
            "UnicodeBigUnmarked" => Charset::Utf16 {
                big_endian: true,
                bom: false,
            },
            "UnicodeLittleUnmarked" => Charset::Utf16 {
                big_endian: false,
                bom: false,
            },
            other => {
                return Err(Stop::Io {
                    class: "java.io.UnsupportedEncodingException",
                    message: match encodings::multi_byte(other) {
                        // A charset Java has and Saranga does not decode.
                        Some(name) => format!("{other} (Saranga does not decode {name})"),
                        None => other.to_string(),
                    },
                });
            }
        }
    };
    Ok(Reader::Java(JavaDecoder {
        charset,
        pending: Vec::new(),
    }))
}

/// `XMLEntityManager.getEncodingInfo`: (encoding, reader encoding, big
/// endian, byte order mark).
fn encoding_info(b: &[i32; 4]) -> (&'static str, &'static str, Option<bool>, bool) {
    let present = b.iter().take_while(|&&x| x != -1).count();
    if present < 2 {
        return ("UTF-8", "UTF-8", None, false);
    }
    let (b0, b1) = (b[0], b[1]);
    if b0 == 0xFE && b1 == 0xFF {
        return ("UTF-16BE", "UTF-16", Some(true), true);
    }
    if b0 == 0xFF && b1 == 0xFE {
        return ("UTF-16LE", "UTF-16", Some(false), true);
    }
    if present < 3 {
        return ("UTF-8", "UTF-8", None, false);
    }
    let b2 = b[2];
    if b0 == 0xEF && b1 == 0xBB && b2 == 0xBF {
        return ("UTF-8", "UTF-8", None, true);
    }
    if present < 4 {
        return ("UTF-8", "UTF-8", None, false);
    }
    let b3 = b[3];
    match (b0, b1, b2, b3) {
        (0x00, 0x00, 0x00, 0x3C) => ("ISO-10646-UCS-4", "ISO-10646-UCS-4", Some(true), false),
        (0x3C, 0x00, 0x00, 0x00) => ("ISO-10646-UCS-4", "ISO-10646-UCS-4", Some(false), false),
        (0x00, 0x00, 0x3C, 0x00) | (0x00, 0x3C, 0x00, 0x00) => {
            ("ISO-10646-UCS-4", "ISO-10646-UCS-4", None, false)
        }
        (0x00, 0x3C, 0x00, 0x3F) => ("UTF-16BE", "UTF-16", Some(true), false),
        (0x3C, 0x00, 0x3F, 0x00) => ("UTF-16LE", "UTF-16", Some(false), false),
        (0x4C, 0x6F, 0xA7, 0x94) => ("CP037", "CP037", None, false),
        _ => ("UTF-8", "UTF-8", None, false),
    }
}
