//! `XMLDTDScannerImpl` and the DOCTYPE handling of `XMLDocumentScannerImpl`:
//! the internal subset, an external DTD read from the file system, entity
//! declarations the document then uses, and, for the DOM, the namespace
//! binding `XMLNSDTDValidator` does once a document has a DOCTYPE.

use std::collections::HashMap;

use super::scanner::{
    Attr, STATE_DTD_EXTERNAL, STATE_DTD_EXTERNAL_DECLS, STATE_DTD_INTERNAL_DECLS, STATE_PROLOG,
};
use super::{Api, Arg, Domain, EntityDecl, Handler, NameType, Parser, QName, R, Stop, chars};

const END_OF_INPUT: i32 = 0;
const TEXT_DECL: i32 = 1;
const MARKUP_DECL: i32 = 2;

const DTD_EVENT: i32 = 11;

/// An attribute an `ATTLIST` declares.
#[derive(Debug, Clone)]
pub(crate) struct AttDecl {
    pub name: String,
    pub cdata: bool,
    pub required: bool,
    pub default: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct State {
    pub state: i32,
    pub standalone: bool,
    pub seen_external_dtd: bool,
    pub seen_external_pe: bool,
    pub markup_depth: i32,
    pub ext_entity_depth: i32,
    pub include_sect_depth: i32,
    pub pe_stack: Vec<i32>,
    pub entity_depth: usize,
    /// The attributes declared for each element, in declaration order.
    pub attlists: HashMap<String, Vec<AttDecl>>,
}

impl Parser {
    /// `scanDoctypeDecl`, after `<!DOCTYPE`: true when an internal subset
    /// follows.
    pub(crate) fn scan_doctype_decl(&mut self) -> R<bool> {
        if !self.skip_spaces()? {
            return Err(self.fatal(
                "MSG_SPACE_REQUIRED_BEFORE_ROOT_ELEMENT_TYPE_IN_DOCTYPEDECL",
                vec![],
            ));
        }
        let name = self.scan_name(NameType::Doctype)?;
        let Some(name) = name else {
            return Err(self.fatal("MSG_ROOT_ELEMENT_TYPE_REQUIRED", vec![]));
        };
        self.s.doctype_name = Some(name.clone());
        if self.skip_spaces()? {
            let (system, public) = self.scan_external_id(false)?;
            self.s.doctype_system_id = system;
            self.s.doctype_public_id = public;
            self.skip_spaces()?;
        }
        self.s.has_external_dtd = self.s.doctype_system_id.is_some();
        // XMLDTDValidator.doctypeDecl: the DOM's validator has a grammar.
        self.s.has_grammar = true;
        if !self.skip_char('[' as i32, None)? {
            self.skip_spaces()?;
            if !self.skip_char('>' as i32, None)? {
                return Err(self.fatal("DoctypedeclUnterminated", vec![Arg::Str(name)]));
            }
            self.s.markup_depth -= 1;
            return Ok(false);
        }
        Ok(true)
    }

    /// `scanExternalID`: (system id, public id).
    pub(crate) fn scan_external_id(
        &mut self,
        optional_system_id: bool,
    ) -> R<(Option<String>, Option<String>)> {
        let mut public_id = None;
        if self.skip_string("PUBLIC")? {
            if !self.skip_spaces()? {
                return Err(self.fatal("SpaceRequiredAfterPUBLIC", vec![]));
            }
            public_id = Some(self.scan_pubid_literal()?);
            if !self.skip_spaces()? && !optional_system_id {
                return Err(self.fatal("SpaceRequiredBetweenPublicAndSystem", vec![]));
            }
        }
        if public_id.is_none() && !self.skip_string("SYSTEM")? {
            return Ok((None, None));
        }
        if public_id.is_none() && !self.skip_spaces()? {
            return Err(self.fatal("SpaceRequiredAfterSYSTEM", vec![]));
        }
        let quote = self.peek_char()?;
        if quote != '\'' as i32 && quote != '"' as i32 {
            if public_id.is_some() && optional_system_id {
                return Ok((None, public_id));
            }
            return Err(self.fatal("QuoteRequiredInSystemID", vec![]));
        }
        self.scan_char(None)?;
        let mut c = self.scan_literal(quote, false)?;
        let mut system = self.literal_text_raw();
        while c != quote {
            let p = self.peek_char()?;
            if chars::is_markup(p) || p == ']' as i32 {
                let ch = self.scan_char(None)?;
                system.push_str(&String::from_utf16_lossy(&[ch as u16]));
            } else if p != -1 && self.is_invalid_literal_dtd(p) {
                return Err(self.fatal("InvalidCharInSystemID", vec![Arg::Str(format!("{p:x}"))]));
            }
            c = self.scan_literal(quote, false)?;
            system.push_str(&self.literal_text_raw());
        }
        if !self.skip_char(quote, None)? {
            return Err(self.fatal("SystemIDUnterminated", vec![]));
        }
        Ok((Some(system), public_id))
    }

    fn literal_text_raw(&self) -> String {
        let (start, len) = (self.literal_start, self.literal_length);
        String::from_utf16_lossy(&self.cur().ch[start..start + len])
    }

    fn is_invalid_literal_dtd(&self, c: i32) -> bool {
        if self.v11 {
            !chars::is_xml11_valid_literal(c)
        } else {
            chars::is_invalid(c)
        }
    }

    /// `scanPubidLiteral`.
    fn scan_pubid_literal(&mut self) -> R<String> {
        let quote = self.scan_char(None)?;
        if quote != '\'' as i32 && quote != '"' as i32 {
            return Err(self.fatal("QuoteRequiredInPublicID", vec![]));
        }
        let mut out = String::new();
        let mut skip_space = true;
        loop {
            let c = self.scan_char(None)?;
            let space = c == ' ' as i32
                || c == '\n' as i32
                || c == '\r' as i32
                || (self.v11 && (c == 0x85 || c == 0x2028));
            if space {
                if !skip_space {
                    out.push(' ');
                    skip_space = true;
                }
            } else if c == quote {
                if skip_space {
                    out.pop();
                }
                return Ok(out);
            } else if chars::is_pubid(c) {
                out.push(c as u8 as char);
                skip_space = false;
            } else if c == -1 {
                return Err(self.fatal("PublicIDUnterminated", vec![]));
            } else {
                return Err(self.fatal("InvalidCharInPublicID", vec![Arg::Str(format!("{c:x}"))]));
            }
        }
    }

    /// `DTDDriver.next`.
    pub(crate) fn dtd_driver(&mut self) -> R<i32> {
        let result = self.dtd_dispatch();
        self.handler = Handler::Document;
        match result {
            Err(Stop::Eof) => Err(self.fatal("PrematureEOF", vec![])),
            Err(Stop::Io { class, message })
                if self.api == Api::Stax && self.s.state == STATE_DTD_EXTERNAL =>
            {
                // XMLStreamReaderImpl.next: an external DTD that cannot be
                // read is passed over when not validating.
                let _ = (class, message);
                self.s.state = STATE_PROLOG;
                self.s.driver = super::scanner::Driver::Prolog;
                Ok(DTD_EVENT)
            }
            Err(e) => Err(e),
            Ok(()) => Ok(DTD_EVENT),
        }
    }

    fn dtd_dispatch(&mut self) -> R<()> {
        self.handler = Handler::None;
        loop {
            match self.s.state {
                STATE_DTD_INTERNAL_DECLS => {
                    let has_external = self.s.has_external_dtd;
                    let more = self.scan_dtd_internal_subset(self.s.standalone, has_external)?;
                    if !more {
                        if !self.skip_char(']' as i32, None)? {
                            let name = self.s.doctype_name.clone().unwrap_or_default();
                            return Err(self.fatal("DoctypedeclNotClosed", vec![Arg::Str(name)]));
                        }
                        self.skip_spaces()?;
                        if !self.skip_char('>' as i32, None)? {
                            let name = self.s.doctype_name.clone().unwrap_or_default();
                            return Err(self.fatal("DoctypedeclUnterminated", vec![Arg::Str(name)]));
                        }
                        self.s.markup_depth -= 1;
                        if self.s.doctype_system_id.is_some() {
                            self.s.state = STATE_DTD_EXTERNAL;
                            continue;
                        }
                        self.end_dtd_scan_state();
                        return Ok(());
                    }
                }
                STATE_DTD_EXTERNAL => {
                    let system_id = self.s.doctype_system_id.clone().unwrap_or_default();
                    let base = self.cur().expanded_system_id.clone();
                    let (bytes, expanded) = self.open_external(&system_id, base.as_deref())?;
                    // setInputSource: the DTD scanner handles the entity.
                    self.handler = Handler::Dtd;
                    self.start_external_entity(false, "[dtd]", bytes, expanded, false)?;
                    self.s.state = STATE_DTD_EXTERNAL_DECLS;
                }
                STATE_DTD_EXTERNAL_DECLS => {
                    if !self.scan_dtd_external_subset()? {
                        self.end_dtd_scan_state();
                        return Ok(());
                    }
                }
                _ => {
                    self.end_dtd_scan_state();
                    return Ok(());
                }
            }
        }
    }

    fn dtd_reset_if_new(&mut self) {
        if self.d.state == 0 && !self.d.seen_external_dtd && self.d.markup_depth == 0 {
            self.d.state = TEXT_DECL;
        }
    }

    /// `scanDTDInternalSubset`.
    fn scan_dtd_internal_subset(&mut self, standalone: bool, _has_external: bool) -> R<bool> {
        self.dtd_reset_if_new();
        self.handler = Handler::Dtd;
        self.d.standalone = standalone;
        if self.d.state == TEXT_DECL {
            self.d.state = MARKUP_DECL;
        }
        loop {
            if !self.scan_decls(true)? {
                self.d.state = TEXT_DECL;
                self.limits.reset(super::Limit::GeneralEntitySize);
                self.limits.reset(super::Limit::TotalEntitySize);
                return Ok(false);
            }
        }
    }

    /// `scanDTDExternalSubset`.
    fn scan_dtd_external_subset(&mut self) -> R<bool> {
        self.handler = Handler::Dtd;
        if self.d.state == TEXT_DECL {
            self.d.seen_external_dtd = true;
            self.scan_text_decl()?;
            if self.d.state == END_OF_INPUT {
                return Ok(false);
            }
            self.d.state = MARKUP_DECL;
        }
        loop {
            if !self.scan_decls(true)? {
                return Ok(false);
            }
        }
    }

    /// The DTD scanner's `startEntity`.
    pub(crate) fn dtd_start_entity(&mut self, name: &str) -> R<()> {
        self.d.entity_depth += 1;
        if name == "[dtd]" {
            self.in_external_subset = true;
            self.d.ext_entity_depth += 1;
            self.d.state = TEXT_DECL;
        } else if name.starts_with('%') {
            self.d.pe_stack.push(self.d.markup_depth);
            if self.cur().external {
                self.d.ext_entity_depth += 1;
            }
        }
        Ok(())
    }

    /// The DTD scanner's `endEntity`.
    pub(crate) fn dtd_end_entity(&mut self, name: &str, last: bool) -> R<()> {
        if self.d.entity_depth > 0 {
            self.d.entity_depth -= 1;
        }
        if self.d.state == END_OF_INPUT {
            return Ok(());
        }
        if name.starts_with('%') {
            let start = self.d.pe_stack.pop().unwrap_or(0);
            if start == 0 && start < self.d.markup_depth {
                let current = self.cur().name.clone();
                return Err(self.error(
                    Domain::Xml,
                    "ILL_FORMED_PARAMETER_ENTITY_WHEN_USED_IN_DECL",
                    vec![Arg::Str(current)],
                ));
            }
            if self.cur().external {
                self.d.ext_entity_depth -= 1;
            }
        }
        if name == "[dtd]" {
            if self.d.include_sect_depth != 0 {
                return Err(self.fatal("IncludeSectUnterminated", vec![]));
            }
            self.d.state = END_OF_INPUT;
            self.in_external_subset = false;
            self.d.ext_entity_depth -= 1;
        }
        if last
            && (self.d.markup_depth != 0
                || self.d.ext_entity_depth != 0
                || self.d.include_sect_depth != 0)
        {
            return Err(self.error(Domain::Xml, "PrematureEOF", vec![Arg::Str(name.into())]));
        }
        Ok(())
    }

    fn scanning_internal_subset(&self) -> bool {
        self.d.ext_entity_depth == 0
    }

    /// `startPE`.
    fn start_pe(&mut self, name: &str, literal: bool) -> R<()> {
        let depth = self.d.pe_stack.len();
        self.start_entity(false, &format!("%{name}"), literal)?;
        if depth != self.d.pe_stack.len() && self.cur().external {
            self.scan_text_decl()?;
        }
        Ok(())
    }

    /// `scanTextDecl`: true when there was one.
    fn scan_text_decl(&mut self) -> R<bool> {
        let mut text_decl = false;
        if self.skip_string("<?xml")? {
            self.d.markup_depth += 1;
            let c = self.peek_char()?;
            if self.is_valid_name_char_dtd(c) {
                while {
                    let c = self.peek_char()?;
                    self.is_valid_name_char_dtd(c)
                } {
                    self.scan_char(None)?;
                }
                // XMLDTDScannerImpl.scanPIData(String, XMLString): the data
                // is not read.
                self.d.markup_depth -= 1;
            } else {
                let (_, encoding, _) = self.scan_decl(true)?;
                text_decl = true;
                self.d.markup_depth -= 1;
                if let Some(encoding) = encoding {
                    self.set_encoding(&encoding)?;
                }
            }
        }
        if !self.entities.is_empty() {
            self.cur_mut().chunking.may_read_chunks = true;
        }
        Ok(text_decl)
    }

    fn is_valid_name_char_dtd(&self, c: i32) -> bool {
        if self.v11 {
            chars::is_xml11_name(c)
        } else {
            chars::is_name(c)
        }
    }

    /// `skipSeparator`.
    fn skip_separator(&mut self, space_required: bool, look_for_pe_refs: bool) -> R<bool> {
        let depth = self.d.pe_stack.len();
        let saw_space = self.skip_spaces()?;
        if !look_for_pe_refs || !self.skip_char('%' as i32, Some(NameType::Reference))? {
            return Ok(!space_required || saw_space || depth != self.d.pe_stack.len());
        }
        loop {
            let name = self.scan_name(NameType::Entity)?;
            let Some(name) = name else {
                return Err(self.fatal("NameRequiredInPEReference", vec![]));
            };
            if !self.skip_char(';' as i32, Some(NameType::Reference))? {
                return Err(self.fatal("SemicolonRequiredInPEReference", vec![Arg::Str(name)]));
            }
            self.start_pe(&name, false)?;
            self.skip_spaces()?;
            if !self.skip_char('%' as i32, Some(NameType::Reference))? {
                return Ok(true);
            }
        }
    }

    /// `scanDecls`.
    fn scan_decls(&mut self, complete: bool) -> R<bool> {
        self.skip_separator(false, true)?;
        let mut again = true;
        while again && self.d.state == MARKUP_DECL {
            again = complete;
            let internal = !self.scanning_internal_subset();
            if self.skip_char('<' as i32, None)? {
                self.d.markup_depth += 1;
                if self.skip_char('?' as i32, None)? {
                    let target = self.scan_name(NameType::Pi)?;
                    let Some(target) = target else {
                        return Err(self.fatal("PITargetRequired", vec![]));
                    };
                    self.scan_pi_data(&target)?;
                    self.d.markup_depth -= 1;
                } else if self.skip_char('!' as i32, None)? {
                    if self.skip_char('-' as i32, None)? {
                        if !self.skip_char('-' as i32, None)? {
                            return Err(self.fatal("MSG_MARKUP_NOT_RECOGNIZED_IN_DTD", vec![]));
                        }
                        self.scan_comment()?;
                        self.d.markup_depth -= 1;
                    } else if self.skip_string("ELEMENT")? {
                        self.scan_element_decl()?;
                    } else if self.skip_string("ATTLIST")? {
                        self.scan_attlist_decl()?;
                    } else if self.skip_string("ENTITY")? {
                        self.scan_entity_decl()?;
                    } else if self.skip_string("NOTATION")? {
                        self.scan_notation_decl()?;
                    } else if self.skip_char('[' as i32, None)? && internal {
                        self.scan_conditional_sect()?;
                    } else {
                        self.d.markup_depth -= 1;
                        return Err(self.fatal("MSG_MARKUP_NOT_RECOGNIZED_IN_DTD", vec![]));
                    }
                } else {
                    self.d.markup_depth -= 1;
                    return Err(self.fatal("MSG_MARKUP_NOT_RECOGNIZED_IN_DTD", vec![]));
                }
            } else if self.d.include_sect_depth > 0 && self.skip_char(']' as i32, None)? {
                if !self.skip_char(']' as i32, None)? || !self.skip_char('>' as i32, None)? {
                    return Err(self.fatal("IncludeSectUnterminated", vec![]));
                }
                self.d.include_sect_depth -= 1;
                self.d.markup_depth -= 1;
            } else if self.scanning_internal_subset() && self.peek_char()? == ']' as i32 {
                return Ok(false);
            } else if self.skip_spaces()? {
            } else {
                return Err(self.fatal("MSG_MARKUP_NOT_RECOGNIZED_IN_DTD", vec![]));
            }
            self.skip_separator(false, true)?;
        }
        Ok(self.d.state != END_OF_INPUT)
    }

    fn scan_element_decl(&mut self) -> R<()> {
        let ext = !self.scanning_internal_subset();
        if !self.skip_separator(true, ext)? {
            return Err(self.fatal(
                "MSG_SPACE_REQUIRED_BEFORE_ELEMENT_TYPE_IN_ELEMENTDECL",
                vec![],
            ));
        }
        let name = self.scan_name(NameType::ElementStart)?;
        let Some(name) = name else {
            return Err(self.fatal("MSG_ELEMENT_TYPE_REQUIRED_IN_ELEMENTDECL", vec![]));
        };
        if !self.skip_separator(true, !self.scanning_internal_subset())? {
            return Err(self.fatal(
                "MSG_SPACE_REQUIRED_BEFORE_CONTENTSPEC_IN_ELEMENTDECL",
                vec![Arg::Str(name)],
            ));
        }
        if self.skip_string("EMPTY")? || self.skip_string("ANY")? {
        } else {
            if !self.skip_char('(' as i32, None)? {
                return Err(self.fatal(
                    "MSG_OPEN_PAREN_OR_ELEMENT_TYPE_REQUIRED_IN_CHILDREN",
                    vec![Arg::Str(name)],
                ));
            }
            self.d.markup_depth += 1;
            self.skip_separator(false, !self.scanning_internal_subset())?;
            if self.skip_string("#PCDATA")? {
                self.scan_mixed(&name)?;
            } else {
                self.scan_children(&name)?;
            }
        }
        self.skip_separator(false, !self.scanning_internal_subset())?;
        if !self.skip_char('>' as i32, None)? {
            return Err(self.fatal("ElementDeclUnterminated", vec![Arg::Str(name)]));
        }
        self.d.markup_depth -= 1;
        Ok(())
    }

    fn scan_mixed(&mut self, el: &str) -> R<()> {
        let mut child = None;
        self.skip_separator(false, !self.scanning_internal_subset())?;
        while self.skip_char('|' as i32, None)? {
            self.skip_separator(false, !self.scanning_internal_subset())?;
            child = self.scan_name(NameType::Entity)?;
            if child.is_none() {
                return Err(self.fatal(
                    "MSG_ELEMENT_TYPE_REQUIRED_IN_MIXED_CONTENT",
                    vec![Arg::Str(el.into())],
                ));
            }
            self.skip_separator(false, !self.scanning_internal_subset())?;
        }
        if self.skip_string(")*")? {
        } else if child.is_some() {
            return Err(self.fatal("MixedContentUnterminated", vec![Arg::Str(el.into())]));
        } else if self.skip_char(')' as i32, None)? {
        } else {
            return Err(self.fatal(
                "MSG_CLOSE_PAREN_REQUIRED_IN_CHILDREN",
                vec![Arg::Str(el.into())],
            ));
        }
        self.d.markup_depth -= 1;
        Ok(())
    }

    fn scan_children(&mut self, el: &str) -> R<()> {
        let mut stack: Vec<i32> = vec![0];
        let mut current_op = 0;
        loop {
            if self.skip_char('(' as i32, None)? {
                self.d.markup_depth += 1;
                stack.push(current_op);
                current_op = 0;
                self.skip_separator(false, !self.scanning_internal_subset())?;
                continue;
            }
            self.skip_separator(false, !self.scanning_internal_subset())?;
            let child = self.scan_name(NameType::ElementStart)?;
            if child.is_none() {
                return Err(self.fatal(
                    "MSG_OPEN_PAREN_OR_ELEMENT_TYPE_REQUIRED_IN_CHILDREN",
                    vec![Arg::Str(el.into())],
                ));
            }
            let c = self.peek_char()?;
            if c == '?' as i32 || c == '*' as i32 || c == '+' as i32 {
                self.scan_char(None)?;
            }
            loop {
                self.skip_separator(false, !self.scanning_internal_subset())?;
                let c = self.peek_char()?;
                if (c == ',' as i32 && current_op != '|' as i32)
                    || (c == '|' as i32 && current_op != ',' as i32)
                {
                    current_op = c;
                    self.scan_char(None)?;
                    break;
                } else if c != ')' as i32 {
                    return Err(self.fatal(
                        "MSG_CLOSE_PAREN_REQUIRED_IN_CHILDREN",
                        vec![Arg::Str(el.into())],
                    ));
                }
                current_op = stack.pop().unwrap_or(0);
                if self.skip_string(")?")? || self.skip_string(")+")? || self.skip_string(")*")? {
                } else {
                    self.scan_char(None)?;
                }
                self.d.markup_depth -= 1;
                if stack.is_empty() {
                    return Ok(());
                }
            }
            self.skip_separator(false, !self.scanning_internal_subset())?;
        }
    }

    fn scan_attlist_decl(&mut self) -> R<()> {
        if !self.skip_separator(true, !self.scanning_internal_subset())? {
            return Err(self.fatal(
                "MSG_SPACE_REQUIRED_BEFORE_ELEMENT_TYPE_IN_ATTLISTDECL",
                vec![],
            ));
        }
        let el = self.scan_name(NameType::ElementStart)?;
        let Some(el) = el else {
            return Err(self.fatal("MSG_ELEMENT_TYPE_REQUIRED_IN_ATTLISTDECL", vec![]));
        };
        if !self.skip_separator(true, !self.scanning_internal_subset())? {
            if self.skip_char('>' as i32, None)? {
                self.d.markup_depth -= 1;
                return Ok(());
            }
            return Err(self.fatal(
                "MSG_SPACE_REQUIRED_BEFORE_ATTRIBUTE_NAME_IN_ATTDEF",
                vec![Arg::Str(el)],
            ));
        }
        while !self.skip_char('>' as i32, None)? {
            let name = self.scan_name(NameType::AttributeName)?;
            let Some(name) = name else {
                return Err(self.fatal("AttNameRequiredInAttDef", vec![Arg::Str(el)]));
            };
            if !self.skip_separator(true, !self.scanning_internal_subset())? {
                return Err(self.fatal(
                    "MSG_SPACE_REQUIRED_BEFORE_ATTTYPE_IN_ATTDEF",
                    vec![Arg::Str(el), Arg::Str(name)],
                ));
            }
            let cdata = self.scan_att_type(&el, &name)?;
            if !self.skip_separator(true, !self.scanning_internal_subset())? {
                return Err(self.fatal(
                    "MSG_SPACE_REQUIRED_BEFORE_DEFAULTDECL_IN_ATTDEF",
                    vec![Arg::Str(el), Arg::Str(name)],
                ));
            }
            let (required, default) = self.scan_att_default_decl(&el, &name)?;
            let list = self.d.attlists.entry(el.clone()).or_default();
            if !list.iter().any(|a| a.name == name) {
                list.push(AttDecl {
                    name,
                    cdata,
                    required,
                    default,
                });
            }
            self.skip_separator(false, !self.scanning_internal_subset())?;
        }
        self.d.markup_depth -= 1;
        Ok(())
    }

    /// `scanAttType`: true for CDATA.
    fn scan_att_type(&mut self, el: &str, at: &str) -> R<bool> {
        let args = || vec![Arg::Str(el.into()), Arg::Str(at.into())];
        if self.skip_string("CDATA")? {
            return Ok(true);
        }
        for t in [
            "IDREFS", "IDREF", "ID", "ENTITY", "ENTITIES", "NMTOKENS", "NMTOKEN",
        ] {
            if self.skip_string(t)? {
                return Ok(false);
            }
        }
        if self.skip_string("NOTATION")? {
            if !self.skip_separator(true, !self.scanning_internal_subset())? {
                return Err(self.fatal("MSG_SPACE_REQUIRED_AFTER_NOTATION_IN_NOTATIONTYPE", args()));
            }
            if self.scan_char(None)? != '(' as i32 {
                return Err(self.fatal("MSG_OPEN_PAREN_REQUIRED_IN_NOTATIONTYPE", args()));
            }
            self.d.markup_depth += 1;
            loop {
                self.skip_separator(false, !self.scanning_internal_subset())?;
                if self.scan_name(NameType::AttributeName)?.is_none() {
                    return Err(self.fatal("MSG_NAME_REQUIRED_IN_NOTATIONTYPE", args()));
                }
                self.skip_separator(false, !self.scanning_internal_subset())?;
                let c = self.scan_char(None)?;
                if c != '|' as i32 {
                    if c != ')' as i32 {
                        return Err(self.fatal("NotationTypeUnterminated", args()));
                    }
                    break;
                }
            }
            self.d.markup_depth -= 1;
            return Ok(false);
        }
        if self.scan_char(None)? != '(' as i32 {
            return Err(self.fatal("AttTypeRequiredInAttDef", args()));
        }
        self.d.markup_depth += 1;
        loop {
            self.skip_separator(false, !self.scanning_internal_subset())?;
            if self.scan_nmtoken()?.is_none() {
                return Err(self.fatal("MSG_NMTOKEN_REQUIRED_IN_ENUMERATION", args()));
            }
            self.skip_separator(false, !self.scanning_internal_subset())?;
            let c = self.scan_char(None)?;
            if c != '|' as i32 {
                if c != ')' as i32 {
                    return Err(self.fatal("EnumerationUnterminated", args()));
                }
                break;
            }
        }
        self.d.markup_depth -= 1;
        Ok(false)
    }

    /// `scanAttDefaultDecl`: (required, default value).
    fn scan_att_default_decl(&mut self, el: &str, at: &str) -> R<(bool, Option<String>)> {
        if self.skip_string("#REQUIRED")? {
            return Ok((true, None));
        }
        if self.skip_string("#IMPLIED")? {
            return Ok((false, None));
        }
        if self.skip_string("#FIXED")?
            && !self.skip_separator(true, !self.scanning_internal_subset())?
        {
            return Err(self.fatal(
                "MSG_SPACE_REQUIRED_AFTER_FIXED_IN_DEFAULTDECL",
                vec![Arg::Str(el.into()), Arg::Str(at.into())],
            ));
        }
        let check_entities =
            !self.d.standalone && (self.d.seen_external_dtd || self.d.seen_external_pe);
        let value = self.scan_attribute_value_dtd(at, check_entities, el)?;
        Ok((false, Some(value)))
    }

    /// `XMLScanner.scanAttributeValue` as the DTD scanner runs it, with its
    /// own entity depth.
    fn scan_attribute_value_dtd(&mut self, at: &str, check_entities: bool, el: &str) -> R<String> {
        self.scan_attribute_value(at, check_entities, el, false)
    }

    fn scan_entity_decl(&mut self) -> R<()> {
        let mut is_pe = false;
        let mut saw_pe_ref = false;
        if self.skip_spaces()? {
            if !self.skip_char('%' as i32, Some(NameType::Reference))? {
                is_pe = false;
            } else if self.skip_separator(true, !self.scanning_internal_subset())? {
                is_pe = true;
            } else if self.scanning_internal_subset() {
                return Err(self.fatal(
                    "MSG_SPACE_REQUIRED_BEFORE_ENTITY_NAME_IN_ENTITYDECL",
                    vec![],
                ));
            } else if self.peek_char()? == '%' as i32 {
                self.skip_separator(false, !self.scanning_internal_subset())?;
                is_pe = true;
            } else {
                saw_pe_ref = true;
            }
        } else if self.scanning_internal_subset()
            || !self.skip_char('%' as i32, Some(NameType::Reference))?
        {
            return Err(self.fatal(
                "MSG_SPACE_REQUIRED_BEFORE_ENTITY_NAME_IN_ENTITYDECL",
                vec![],
            ));
        } else if self.skip_spaces()? {
            return Err(self.fatal("MSG_SPACE_REQUIRED_BEFORE_PERCENT_IN_PEDECL", vec![]));
        } else {
            saw_pe_ref = true;
        }
        if saw_pe_ref {
            loop {
                let pe = self.scan_name(NameType::Reference)?;
                let Some(pe) = pe else {
                    return Err(self.fatal("NameRequiredInPEReference", vec![]));
                };
                if !self.skip_char(';' as i32, Some(NameType::Reference))? {
                    return Err(self.fatal("SemicolonRequiredInPEReference", vec![Arg::Str(pe)]));
                }
                self.start_pe(&pe, false)?;
                self.skip_spaces()?;
                if !self.skip_char('%' as i32, Some(NameType::Reference))? {
                    break;
                }
                if !is_pe {
                    if self.skip_separator(true, !self.scanning_internal_subset())? {
                        is_pe = true;
                        break;
                    }
                    is_pe = self.skip_char('%' as i32, Some(NameType::Reference))?;
                }
            }
        }
        let name = self.scan_name(NameType::Entity)?;
        let Some(name) = name else {
            return Err(self.fatal("MSG_ENTITY_NAME_REQUIRED_IN_ENTITYDECL", vec![]));
        };
        if !self.skip_separator(true, !self.scanning_internal_subset())? {
            return Err(self.fatal(
                "MSG_SPACE_REQUIRED_AFTER_ENTITY_NAME_IN_ENTITYDECL",
                vec![Arg::Str(name)],
            ));
        }
        let (system_id, _) = self.scan_external_id(false)?;
        if is_pe && system_id.is_some() {
            self.d.seen_external_pe = true;
        }
        let mut notation = None;
        let saw_space = self.skip_separator(true, !self.scanning_internal_subset())?;
        if !is_pe && self.skip_string("NDATA")? {
            if !saw_space {
                return Err(self.fatal(
                    "MSG_SPACE_REQUIRED_BEFORE_NDATA_IN_UNPARSED_ENTITYDECL",
                    vec![Arg::Str(name)],
                ));
            }
            if !self.skip_separator(true, !self.scanning_internal_subset())? {
                return Err(self.fatal(
                    "MSG_SPACE_REQUIRED_BEFORE_NOTATION_NAME_IN_UNPARSED_ENTITYDECL",
                    vec![Arg::Str(name)],
                ));
            }
            notation = self.scan_name(NameType::Notation)?;
            if notation.is_none() {
                return Err(self.fatal(
                    "MSG_NOTATION_NAME_REQUIRED_FOR_UNPARSED_ENTITYDECL",
                    vec![Arg::Str(name)],
                ));
            }
        }
        let mut text = String::new();
        if system_id.is_none() {
            text = self.scan_entity_value(&name, is_pe)?;
        }
        self.skip_separator(false, !self.scanning_internal_subset())?;
        if !self.skip_char('>' as i32, None)? {
            return Err(self.fatal("EntityDeclUnterminated", vec![Arg::Str(name)]));
        }
        self.d.markup_depth -= 1;
        let key = if is_pe { format!("%{name}") } else { name };
        let in_external_subset = self.in_external_subset;
        if !self.declared.contains_key(&key) {
            let decl = match system_id {
                Some(system_id) => EntityDecl::External {
                    system_id,
                    base_system_id: self.cur().expanded_system_id.clone(),
                    notation,
                    in_external_subset,
                },
                None => EntityDecl::Internal {
                    text,
                    in_external_subset,
                },
            };
            self.declared.insert(key, decl);
        }
        Ok(())
    }

    /// `scanEntityValue`: the replacement text.
    fn scan_entity_value(&mut self, name: &str, is_pe: bool) -> R<String> {
        let quote = self.scan_char(None)?;
        if quote != '\'' as i32 && quote != '"' as i32 {
            return Err(self.fatal("OpenQuoteMissingInDecl", vec![]));
        }
        let entity_depth = self.d.entity_depth;
        let mut out = String::new();
        let mut c = self.scan_literal(quote, false)?;
        out.push_str(&self.literal_text_raw());
        if c != quote {
            loop {
                let mut count_char = 0;
                let before = out.encode_utf16().count();
                if self.skip_char('&' as i32, Some(NameType::Reference))? {
                    if self.skip_char('#' as i32, Some(NameType::Reference))? {
                        self.scan_char_reference_value(Some(&mut out))?;
                    } else {
                        out.push('&');
                        let e = self.scan_name(NameType::Reference)?;
                        let Some(e) = e else {
                            return Err(self.fatal("NameRequiredInReference", vec![]));
                        };
                        out.push_str(&e);
                        if !self.skip_char(';' as i32, Some(NameType::Reference))? {
                            return Err(
                                self.fatal("SemicolonRequiredInReference", vec![Arg::Str(e)])
                            );
                        }
                        out.push(';');
                    }
                } else if self.skip_char('%' as i32, Some(NameType::Reference))? {
                    loop {
                        let pe = self.scan_name(NameType::Reference)?;
                        let Some(pe) = pe else {
                            return Err(self.fatal("NameRequiredInPEReference", vec![]));
                        };
                        if !self.skip_char(';' as i32, Some(NameType::Reference))? {
                            return Err(
                                self.fatal("SemicolonRequiredInPEReference", vec![Arg::Str(pe)])
                            );
                        }
                        if self.scanning_internal_subset() {
                            return Err(self.fatal("PEReferenceWithinMarkup", vec![Arg::Str(pe)]));
                        }
                        self.start_pe(&pe, true)?;
                        self.skip_spaces()?;
                        if !self.skip_char('%' as i32, Some(NameType::Reference))? {
                            break;
                        }
                    }
                } else {
                    let p = self.peek_char()?;
                    if chars::is_high_surrogate(p) {
                        count_char += 1;
                        self.scan_surrogates_dtd()?;
                    } else if self.is_invalid_literal_dtd(p) {
                        return Err(
                            self.fatal("InvalidCharInLiteral", vec![Arg::Str(format!("{p:x}"))])
                        );
                    } else if p != quote || entity_depth != self.d.entity_depth {
                        out.push_str(&String::from_utf16_lossy(&[p as u16]));
                        self.scan_char(None)?;
                    }
                }
                let added = out.encode_utf16().count() - before + count_char;
                self.check_entity_size(is_pe, name, added)?;
                c = self.scan_literal(quote, false)?;
                if c == quote {
                    break;
                }
                out.push_str(&self.literal_text_raw());
            }
            let last = self.literal_text_raw();
            self.check_entity_size(is_pe, name, last.encode_utf16().count())?;
            out.push_str(&last);
        } else {
            let n = out.encode_utf16().count();
            self.check_entity_size(is_pe, name, n)?;
        }
        if is_pe {
            self.limits
                .end_entity(super::Limit::ParameterEntitySize, name);
        } else {
            self.limits
                .end_entity(super::Limit::GeneralEntitySize, name);
        }
        if !self.skip_char(quote, None)? {
            return Err(self.fatal("CloseQuoteMissingInDecl", vec![]));
        }
        Ok(out)
    }

    fn scan_surrogates_dtd(&mut self) -> R<()> {
        let high = self.scan_char(None)?;
        let low = self.peek_char()?;
        if !chars::is_low_surrogate(low) {
            return Err(self.fatal("InvalidCharInContent", vec![Arg::Str(format!("{high:x}"))]));
        }
        self.scan_char(None)?;
        let c = chars::supplemental(high as u16, low as u16);
        let invalid = if self.v11 {
            chars::is_xml11_invalid(c)
        } else {
            chars::is_invalid(c)
        };
        if invalid {
            return Err(self.fatal("InvalidCharInContent", vec![Arg::Str(format!("{c:x}"))]));
        }
        Ok(())
    }

    fn scan_notation_decl(&mut self) -> R<()> {
        if !self.skip_separator(true, !self.scanning_internal_subset())? {
            return Err(self.fatal(
                "MSG_SPACE_REQUIRED_BEFORE_NOTATION_NAME_IN_NOTATIONDECL",
                vec![],
            ));
        }
        let name = self.scan_name(NameType::Notation)?;
        let Some(name) = name else {
            return Err(self.fatal("MSG_NOTATION_NAME_REQUIRED_IN_NOTATIONDECL", vec![]));
        };
        if !self.skip_separator(true, !self.scanning_internal_subset())? {
            return Err(self.fatal(
                "MSG_SPACE_REQUIRED_AFTER_NOTATION_NAME_IN_NOTATIONDECL",
                vec![Arg::Str(name)],
            ));
        }
        let (system, public) = self.scan_external_id(true)?;
        if system.is_none() && public.is_none() {
            return Err(self.fatal("ExternalIDorPublicIDRequired", vec![Arg::Str(name)]));
        }
        self.skip_separator(false, !self.scanning_internal_subset())?;
        if !self.skip_char('>' as i32, None)? {
            return Err(self.fatal("NotationDeclUnterminated", vec![Arg::Str(name)]));
        }
        self.d.markup_depth -= 1;
        Ok(())
    }

    fn scan_conditional_sect(&mut self) -> R<()> {
        self.skip_separator(false, !self.scanning_internal_subset())?;
        if self.skip_string("INCLUDE")? {
            self.skip_separator(false, !self.scanning_internal_subset())?;
            if !self.skip_char('[' as i32, None)? {
                return Err(self.fatal("MSG_MARKUP_NOT_RECOGNIZED_IN_DTD", vec![]));
            }
            self.d.include_sect_depth += 1;
            return Ok(());
        }
        if self.skip_string("IGNORE")? {
            self.skip_separator(false, !self.scanning_internal_subset())?;
            if !self.skip_char('[' as i32, None)? {
                return Err(self.fatal("MSG_MARKUP_NOT_RECOGNIZED_IN_DTD", vec![]));
            }
            self.d.include_sect_depth += 1;
            let initial = self.d.include_sect_depth;
            loop {
                if self.skip_char('<' as i32, None)? {
                    if self.skip_char('!' as i32, None)? && self.skip_char('[' as i32, None)? {
                        self.d.include_sect_depth += 1;
                    }
                } else if self.skip_char(']' as i32, None)? {
                    if self.skip_char(']' as i32, None)? {
                        while self.skip_char(']' as i32, None)? {}
                        if self.skip_char('>' as i32, None)? {
                            let depth = self.d.include_sect_depth;
                            self.d.include_sect_depth -= 1;
                            if depth == initial {
                                self.d.markup_depth -= 1;
                                return Ok(());
                            }
                        }
                    }
                } else {
                    self.scan_char(None)?;
                    if self.d.state == END_OF_INPUT {
                        return Err(self.fatal("IgnoreSectUnterminated", vec![]));
                    }
                }
            }
        }
        Err(self.fatal("MSG_MARKUP_NOT_RECOGNIZED_IN_DTD", vec![]))
    }

    /// `XMLDTDValidator.addDTDDefaultAttrsAndValidate`'s defaults and
    /// `XMLNSDTDValidator.startNamespaceScope`: what the DOM does with an
    /// element once its document has a DOCTYPE.
    pub(crate) fn dtd_validator_start_element(&mut self) -> R<()> {
        let element = self.s.element_qname.clone();
        let raw = element.raw();
        if let Some(decls) = self.d.attlists.get(&raw).cloned() {
            for decl in decls {
                let specified = self
                    .s
                    .attributes
                    .iter()
                    .any(|a| a.name.rawname.as_deref() == Some(decl.name.as_str()));
                if specified || decl.required {
                    continue;
                }
                if let Some(value) = decl.default {
                    let (prefix, local) = match decl.name.split_once(':') {
                        Some((p, l)) => (Some(p.to_string()), l.to_string()),
                        None => (None, decl.name.clone()),
                    };
                    let mut name = QName::default();
                    name.set(prefix, Some(local), Some(decl.name.clone()), None);
                    let _ = decl.cdata;
                    self.s.attributes.push(Attr { name, value });
                }
            }
        }
        self.s.ns.push();
        if element.prefix.as_deref() == Some("xmlns") {
            return Err(self.error(Domain::Xmlns, "ElementXMLNSPrefix", vec![Arg::Str(raw)]));
        }
        for i in 0..self.s.attributes.len() {
            let a = self.s.attributes[i].clone();
            let local = a.name.localpart.clone().unwrap_or_default();
            let prefix = a.name.prefix.clone().unwrap_or_default();
            if prefix == "xmlns" || (prefix.is_empty() && local == "xmlns") {
                let uri = a.value.clone();
                let qname = a.name.raw();
                if prefix == "xmlns" && local == "xmlns" {
                    return Err(self.error(Domain::Xmlns, "CantBindXMLNS", vec![Arg::Str(qname)]));
                }
                if uri == "http://www.w3.org/2000/xmlns/" {
                    return Err(self.error(Domain::Xmlns, "CantBindXMLNS", vec![Arg::Str(qname)]));
                }
                if local == "xml" {
                    if uri != "http://www.w3.org/XML/1998/namespace" {
                        return Err(self.error(
                            Domain::Xmlns,
                            "CantBindXML",
                            vec![Arg::Str(qname)],
                        ));
                    }
                } else if uri == "http://www.w3.org/XML/1998/namespace" {
                    return Err(self.error(Domain::Xmlns, "CantBindXML", vec![Arg::Str(qname)]));
                }
                let bound = if local != "xmlns" {
                    local.clone()
                } else {
                    String::new()
                };
                if uri.is_empty() && local != "xmlns" {
                    return Err(self.error(
                        Domain::Xmlns,
                        "EmptyPrefixedAttName",
                        vec![Arg::Str(qname)],
                    ));
                }
                self.s
                    .ns
                    .declare(&bound, if uri.is_empty() { None } else { Some(uri) });
            }
        }
        let prefix = element.prefix.clone().unwrap_or_default();
        let uri = self.s.ns.uri(&prefix);
        if element.prefix.is_some() && uri.is_none() {
            return Err(self.error(
                Domain::Xmlns,
                "ElementPrefixUnbound",
                vec![Arg::Str(prefix), Arg::Str(raw)],
            ));
        }
        if let Some(top) = self.s.elements.last_mut() {
            top.uri = uri.clone();
        }
        for i in 0..self.s.attributes.len() {
            let a = self.s.attributes[i].name.clone();
            let aprefix = a.prefix.clone().unwrap_or_default();
            if a.rawname.as_deref() == Some("xmlns") {
                self.s.attributes[i].name.uri = self.s.ns.uri("xmlns");
            } else if !aprefix.is_empty() {
                let uri = self.s.ns.uri(&aprefix);
                if uri.is_none() {
                    return Err(self.error(
                        Domain::Xmlns,
                        "AttributePrefixUnbound",
                        vec![Arg::Str(raw.clone()), Arg::Str(a.raw()), Arg::Str(aprefix)],
                    ));
                }
                self.s.attributes[i].name.uri = uri;
            }
        }
        let n = self.s.attributes.len();
        for i in 0..n.saturating_sub(1) {
            let auri = self.s.attributes[i].name.uri.clone();
            let Some(auri) = auri.filter(|u| u != "http://www.w3.org/2000/xmlns/") else {
                continue;
            };
            let alocal = self.s.attributes[i].name.localpart.clone();
            for j in i + 1..n {
                if self.s.attributes[j].name.localpart == alocal
                    && self.s.attributes[j].name.uri.as_deref() == Some(auri.as_str())
                {
                    return Err(self.error(
                        Domain::Xmlns,
                        "AttributeNSNotUnique",
                        vec![
                            Arg::Str(raw.clone()),
                            Arg::Str(alocal.clone().unwrap_or_default()),
                            Arg::Str(auri.clone()),
                        ],
                    ));
                }
            }
        }
        Ok(())
    }
}
