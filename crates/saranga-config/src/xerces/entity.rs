//! `XMLEntityScanner` and `XML11EntityScanner`, with the parts of
//! `XMLEntityManager` that start and end entities: the scanning primitives,
//! with the buffer they read through and the line and column numbers they
//! keep, as the JDK has them.

use super::chars::{self, is_low_surrogate, supplemental};
use super::reader::{Chunking, Reader, Stream};
use super::{Arg, Domain, Limit, NameType, Parser, QName, R, Stop};

/// `XMLEntityManager.DEFAULT_BUFFER_SIZE`.
pub(crate) const BUFFER_SIZE: usize = 8192;
/// `XMLEntityManager.DEFAULT_XMLDECL_BUFFER_SIZE`: what `load` reads while
/// an entity may not read in chunks.
const XMLDECL_BUFFER_SIZE: usize = 64;
/// `Entity.ScannedEntity.DEFAULT_INTERNAL_BUFFER_SIZE`.
const INTERNAL_BUFFER_SIZE: usize = 1024;

/// `VALID_NAMES`: the ASCII name characters `XMLEntityScanner` tests without
/// `XMLChar`.
fn valid_name(c: u16) -> bool {
    matches!(c, 0x41..=0x5A | 0x61..=0x7A | 0x30..=0x39 | 45 | 46 | 58 | 95)
}

fn name_char(c: u16) -> bool {
    if c < 127 {
        valid_name(c)
    } else {
        chars::is_name(c as i32)
    }
}

#[derive(Debug)]
pub(crate) struct ScannedEntity {
    pub name: String,
    pub is_ge: bool,
    pub literal: bool,
    pub external: bool,
    pub chunking: Chunking,
    pub ch: Vec<u16>,
    pub buffer_size: usize,
    pub position: usize,
    pub count: usize,
    pub line: i32,
    pub column: i32,
    pub reader: Reader,
    pub stream: Option<Stream>,
    pub encoding: Option<String>,
    pub encoding_externally_specified: bool,
    pub xml_version: String,
    pub version: Option<String>,
    pub expanded_system_id: Option<String>,
    /// Characters the reader has handed over: `ch[i]` is the entity's
    /// character `read - (count - i)`.
    pub read: usize,
}

impl ScannedEntity {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        is_ge: bool,
        name: &str,
        reader: Reader,
        stream: Option<Stream>,
        encoding: Option<String>,
        literal: bool,
        encoding_externally_specified: bool,
        external: bool,
        expanded_system_id: Option<String>,
    ) -> ScannedEntity {
        let size = if external {
            BUFFER_SIZE
        } else {
            INTERNAL_BUFFER_SIZE
        };
        ScannedEntity {
            name: name.to_string(),
            is_ge,
            literal,
            external,
            chunking: Chunking {
                may_read_chunks: encoding_externally_specified,
                xml_decl_chunk_read: false,
            },
            ch: vec![0; size],
            buffer_size: BUFFER_SIZE,
            position: 0,
            count: 0,
            line: 1,
            column: 1,
            reader,
            stream,
            encoding,
            encoding_externally_specified,
            xml_version: "1.0".into(),
            version: None,
            expanded_system_id,
            read: 0,
        }
    }

    /// The entity's offset of `ch[i]`.
    pub(crate) fn offset_of(&self, i: usize) -> usize {
        (self.read + i).saturating_sub(self.count)
    }
}

impl Parser {
    pub(crate) fn cur(&self) -> &ScannedEntity {
        self.entities.last().expect("a current entity")
    }

    pub(crate) fn cur_mut(&mut self) -> &mut ScannedEntity {
        self.entities.last_mut().expect("a current entity")
    }

    /// `ch[i]`, as Java reads its array: an index past it is an
    /// `ArrayIndexOutOfBoundsException`.
    pub(crate) fn at(&self, i: usize) -> R<u16> {
        let e = self.cur();
        e.ch.get(i).copied().ok_or_else(|| Stop::Runtime {
            class: "java.lang.ArrayIndexOutOfBoundsException",
            message: format!("Index {i} out of bounds for length {}", e.ch.len()),
        })
    }

    pub(crate) fn line(&self) -> i32 {
        self.entities.last().map_or(-1, |e| e.line)
    }

    pub(crate) fn column(&self) -> i32 {
        self.entities.last().map_or(-1, |e| e.column)
    }

    fn external(&self) -> bool {
        self.is_external
    }

    /// `XMLEntityScanner.load`.
    pub(crate) fn load(&mut self, offset: usize, change_entity: bool) -> R<bool> {
        let e = self.cur_mut();
        let mut length = e.ch.len() - offset;
        if !e.chunking.may_read_chunks && length > XMLDECL_BUFFER_SIZE {
            length = XMLDECL_BUFFER_SIZE;
        }
        let ScannedEntity {
            reader,
            stream,
            chunking,
            ch,
            ..
        } = e;
        let count = reader.read(stream.as_mut(), chunking, ch, offset, length)?;
        let mut changed = false;
        if count != -1 {
            if count != 0 {
                e.count = count as usize + offset;
                e.position = offset;
                e.read += count as usize;
                if self.entities.len() == 1 && self.api == super::Api::Stax {
                    let e = self.entities.last().expect("the document");
                    self.decoded
                        .extend_from_slice(&e.ch[offset..offset + count as usize]);
                }
            }
        } else {
            e.count = offset;
            e.position = offset;
            changed = true;
            if change_entity {
                self.end_entity()?;
                if self.entities.is_empty() {
                    return Err(Stop::Eof);
                }
                let e = self.cur();
                if e.position == e.count {
                    self.load(0, true)?;
                }
            }
        }
        Ok(changed)
    }

    /// `XMLEntityManager.endEntity`.
    pub(crate) fn end_entity(&mut self) -> R<()> {
        let name = self.cur().name.clone();
        self.limits.end_entity(Limit::GeneralEntitySize, &name);
        let last = self.entities.len() == 1;
        self.handler_end_entity(&name, last)?;
        let document = name == "[xml]";
        self.entities.pop();
        if let Some(e) = self.entities.last() {
            self.is_external = e.external;
        }
        if self.entities.is_empty() && !document {
            return Err(Stop::Eof);
        }
        Ok(())
    }

    pub(crate) fn peek_char(&mut self) -> R<i32> {
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        let c = self.at(self.cur().position)? as i32;
        if self.v11 {
            if self.cur().external {
                return Ok(if c != '\r' as i32 && c != 0x85 && c != 0x2028 {
                    c
                } else {
                    '\n' as i32
                });
            }
            return Ok(c);
        }
        if self.external() {
            Ok(if c != '\r' as i32 { c } else { '\n' as i32 })
        } else {
            Ok(c)
        }
    }

    pub(crate) fn scan_char(&mut self, nt: Option<NameType>) -> R<i32> {
        if self.v11 {
            return self.scan_char11(nt);
        }
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        let mut offset = self.cur().position;
        let mut c = self.at(offset)? as i32;
        self.cur_mut().position += 1;
        let external = self.external();
        if c == '\n' as i32 || (c == '\r' as i32 && external) {
            let e = self.cur_mut();
            e.line += 1;
            e.column = 1;
            if e.position == e.count {
                e.ch[0] = c as u16;
                self.load(1, true)?;
                offset = 0;
            }
            let e = self.cur();
            if c == '\r' as i32 && external && e.position < e.count {
                let p = e.position;
                self.cur_mut().position += 1;
                if self.at(p)? != '\n' as u16 {
                    self.cur_mut().position -= 1;
                }
                c = '\n' as i32;
            }
        }
        self.cur_mut().column += 1;
        if !self.detecting_version {
            let len = self.cur().position - offset;
            self.check_entity_limit(nt, offset, len)?;
        }
        Ok(c)
    }

    fn scan_char11(&mut self, nt: Option<NameType>) -> R<i32> {
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        let mut offset = self.cur().position;
        let mut c = self.at(offset)? as i32;
        self.cur_mut().position += 1;
        let mut external = false;
        if c == '\n' as i32
            || ((c == '\r' as i32 || c == 0x85 || c == 0x2028) && {
                external = self.cur().external;
                external
            })
        {
            let e = self.cur_mut();
            e.line += 1;
            e.column = 1;
            if e.position == e.count {
                e.ch[0] = c as u16;
                self.load(1, true)?;
                offset = 0;
            }
            let e = self.cur();
            if c == '\r' as i32 && external && e.position < e.count {
                let p = e.position;
                self.cur_mut().position += 1;
                let cc = self.at(p)?;
                if cc != '\n' as u16 && cc != 0x85 {
                    self.cur_mut().position -= 1;
                }
            }
            c = '\n' as i32;
        }
        self.cur_mut().column += 1;
        if !self.detecting_version {
            let len = self.cur().position - offset;
            self.check_entity_limit(nt, offset, len)?;
        }
        Ok(c)
    }

    /// `checkBeforeLoad`: steps past a name character, moving the name to
    /// the front of the buffer when it reaches the end.
    fn check_before_load(&mut self, offset: usize, name_offset: Option<usize>) -> R<usize> {
        let e = self.cur_mut();
        e.position += 1;
        if e.position != e.count {
            return Ok(0);
        }
        let length = e.position - offset;
        let (name_off, name_len) = match name_offset {
            Some(n) => (n - offset, length - (n - offset)),
            None => (offset, length),
        };
        let _ = name_off;
        self.check_limit(Limit::MaxName, name_len)?;
        let e = self.cur_mut();
        if length == e.ch.len() {
            let mut tmp = vec![0u16; e.buffer_size * 2];
            tmp[..length].copy_from_slice(&e.ch[offset..offset + length]);
            e.ch = tmp;
            e.buffer_size *= 2;
        } else {
            e.ch.copy_within(offset..offset + length, 0);
        }
        Ok(length)
    }

    pub(crate) fn scan_name(&mut self, nt: NameType) -> R<Option<String>> {
        if self.v11 {
            return self.scan_name11(nt);
        }
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        let mut offset = self.cur().position;
        if chars::is_name_start(self.at(offset)? as i32) {
            let e = self.cur_mut();
            e.position += 1;
            if e.position == e.count {
                e.ch[0] = e.ch[offset];
                offset = 0;
                if self.load(1, false)? {
                    self.cur_mut().column += 1;
                    return Ok(Some(self.string(0, 1)));
                }
            }
            loop {
                let c = self.at(self.cur().position)?;
                if !name_char(c) {
                    break;
                }
                let length = self.check_before_load(offset, Some(offset))?;
                if length > 0 {
                    offset = 0;
                    if self.load(length, false)? {
                        break;
                    }
                }
            }
        }
        let length = self.cur().position - offset;
        self.cur_mut().column += length as i32;
        if length > 0 {
            self.check_limit(Limit::MaxName, length)?;
            self.check_entity_limit(Some(nt), offset, length)?;
            Ok(Some(self.string(offset, length)))
        } else {
            Ok(None)
        }
    }

    fn scan_name11(&mut self, nt: NameType) -> R<Option<String>> {
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        let mut offset = self.cur().position;
        let ch = self.at(offset)?;
        if chars::is_xml11_name_start(ch as i32) {
            let e = self.cur_mut();
            e.position += 1;
            if e.position == e.count {
                e.ch[0] = ch;
                offset = 0;
                if self.load(1, false)? {
                    self.cur_mut().column += 1;
                    return Ok(Some(self.string(0, 1)));
                }
            }
        } else if chars::is_xml11_name_high_surrogate(ch as i32) {
            let e = self.cur_mut();
            e.position += 1;
            if e.position == e.count {
                e.ch[0] = ch;
                offset = 0;
                if self.load(1, false)? {
                    self.cur_mut().position -= 1;
                    return Ok(None);
                }
            }
            let ch2 = self.at(self.cur().position)?;
            if !is_low_surrogate(ch2 as i32) || !chars::is_xml11_name_start(supplemental(ch, ch2)) {
                self.cur_mut().position -= 1;
                return Ok(None);
            }
            let e = self.cur_mut();
            e.position += 1;
            if e.position == e.count {
                e.ch[0] = ch;
                e.ch[1] = ch2;
                offset = 0;
                if self.load(2, false)? {
                    self.cur_mut().column += 2;
                    return Ok(Some(self.string(0, 2)));
                }
            }
        } else {
            return Ok(None);
        }
        loop {
            let ch = self.at(self.cur().position)?;
            if chars::is_xml11_name(ch as i32) {
                let length = self.check_before_load(offset, Some(offset))?;
                if length > 0 {
                    offset = 0;
                    if self.load(length, false)? {
                        break;
                    }
                }
            } else if chars::is_xml11_name_high_surrogate(ch as i32) {
                let length = self.check_before_load(offset, Some(offset))?;
                if length > 0 {
                    offset = 0;
                    if self.load(length, false)? {
                        self.cur_mut().position -= 1;
                        break;
                    }
                }
                let ch2 = self.at(self.cur().position)?;
                if !is_low_surrogate(ch2 as i32) || !chars::is_xml11_name(supplemental(ch, ch2)) {
                    self.cur_mut().position -= 1;
                    break;
                }
                let length = self.check_before_load(offset, Some(offset))?;
                if length > 0 {
                    offset = 0;
                    if self.load(length, false)? {
                        break;
                    }
                }
            } else {
                break;
            }
        }
        let length = self.cur().position - offset;
        self.cur_mut().column += length as i32;
        if length > 0 {
            self.check_limit(Limit::MaxName, length)?;
            self.check_entity_limit(Some(nt), offset, length)?;
            Ok(Some(self.string(offset, length)))
        } else {
            Ok(None)
        }
    }

    /// `scanNmtoken`, for the DTD scanner.
    pub(crate) fn scan_nmtoken(&mut self) -> R<Option<String>> {
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        let mut offset = self.cur().position;
        loop {
            let c = self.at(self.cur().position)?;
            let valid = if self.v11 {
                chars::is_xml11_name(c as i32)
            } else {
                name_char(c)
            };
            if !valid {
                if self.v11 && chars::is_xml11_name_high_surrogate(c as i32) {
                    // XML11EntityScanner.scanNmtoken's surrogate pair.
                    let e = self.cur_mut();
                    e.position += 1;
                    if e.position == e.count {
                        let length = self.grow_name(offset)?;
                        offset = 0;
                        if self.load(length, false)? {
                            self.cur_mut().position -= 1;
                            break;
                        }
                    }
                    let ch2 = self.at(self.cur().position)?;
                    if !is_low_surrogate(ch2 as i32) || !chars::is_xml11_name(supplemental(c, ch2))
                    {
                        self.cur_mut().position -= 1;
                        break;
                    }
                    let e = self.cur_mut();
                    e.position += 1;
                    if e.position == e.count {
                        let length = self.grow_name(offset)?;
                        offset = 0;
                        if self.load(length, false)? {
                            break;
                        }
                    }
                    continue;
                }
                break;
            }
            let e = self.cur_mut();
            e.position += 1;
            if e.position == e.count {
                let length = self.grow_name(offset)?;
                offset = 0;
                if self.load(length, false)? {
                    break;
                }
            }
        }
        let length = self.cur().position - offset;
        self.cur_mut().column += length as i32;
        Ok((length > 0).then(|| self.string(offset, length)))
    }

    /// Moves a token reaching the end of the buffer to its front, doubling
    /// the buffer when the token fills it.
    fn grow_name(&mut self, offset: usize) -> R<usize> {
        let e = self.cur_mut();
        let length = e.position - offset;
        if length == e.ch.len() {
            let mut tmp = vec![0u16; e.ch.len() * 2];
            tmp[..length].copy_from_slice(&e.ch[offset..offset + length]);
            e.ch = tmp;
            if !self.v11 {
                self.cur_mut().buffer_size *= 2;
            }
        } else {
            e.ch.copy_within(offset..offset + length, 0);
        }
        Ok(length)
    }

    /// `scanQName`: true when a name was scanned into `qname`.
    pub(crate) fn scan_qname(&mut self, qname: &mut QName, nt: NameType) -> R<bool> {
        if self.v11 {
            return self.scan_qname11(qname, nt);
        }
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        let mut offset = self.cur().position;
        if !chars::is_name_start(self.at(offset)? as i32) {
            return Ok(false);
        }
        let e = self.cur_mut();
        e.position += 1;
        if e.position == e.count {
            e.ch[0] = e.ch[offset];
            offset = 0;
            if self.load(1, false)? {
                self.cur_mut().column += 1;
                let name = self.string(0, 1);
                qname.set(None, Some(name.clone()), Some(name), None);
                self.check_entity_limit(Some(nt), 0, 1)?;
                return Ok(true);
            }
        }
        let mut index: Option<usize> = None;
        loop {
            let c = self.at(self.cur().position)?;
            if !name_char(c) {
                break;
            }
            if c == ':' as u16 {
                if index.is_some() {
                    break;
                }
                let i = self.cur().position;
                index = Some(i);
                self.check_limit(Limit::MaxName, i - offset)?;
            }
            let length = self.check_before_load(offset, index)?;
            if length > 0 {
                if let Some(i) = index {
                    index = Some(i - offset);
                }
                offset = 0;
                if self.load(length, false)? {
                    break;
                }
            }
        }
        let length = self.cur().position - offset;
        self.cur_mut().column += length as i32;
        if length == 0 {
            return Ok(false);
        }
        let rawname = self.string(offset, length);
        let (prefix, localpart) = match index {
            Some(index) => {
                let prefix_length = index - offset;
                self.check_limit(Limit::MaxName, prefix_length)?;
                let prefix = self.string(offset, prefix_length);
                let len = length - prefix_length - 1;
                let start_local = index + 1;
                if !chars::is_ncname_start(self.at(start_local)? as i32) {
                    return Err(self.error(
                        Domain::Xml,
                        "IllegalQName",
                        vec![Arg::Str(rawname.clone())],
                    ));
                }
                self.check_limit(Limit::MaxName, len)?;
                (Some(prefix), self.string(index + 1, len))
            }
            None => {
                self.check_limit(Limit::MaxName, length)?;
                (None, rawname.clone())
            }
        };
        qname.set(prefix, Some(localpart), Some(rawname), None);
        self.check_entity_limit(Some(nt), offset, length)?;
        Ok(true)
    }

    fn scan_qname11(&mut self, qname: &mut QName, nt: NameType) -> R<bool> {
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        let mut offset = self.cur().position;
        let ch = self.at(offset)?;
        if chars::is_xml11_ncname_start(ch as i32) {
            let e = self.cur_mut();
            e.position += 1;
            if e.position == e.count {
                e.ch[0] = ch;
                offset = 0;
                if self.load(1, false)? {
                    self.cur_mut().column += 1;
                    let name = self.string(0, 1);
                    qname.set(None, Some(name.clone()), Some(name), None);
                    self.check_entity_limit(Some(nt), 0, 1)?;
                    return Ok(true);
                }
            }
        } else if chars::is_xml11_name_high_surrogate(ch as i32) {
            let e = self.cur_mut();
            e.position += 1;
            if e.position == e.count {
                e.ch[0] = ch;
                offset = 0;
                if self.load(1, false)? {
                    self.cur_mut().position -= 1;
                    return Ok(false);
                }
            }
            let ch2 = self.at(self.cur().position)?;
            if !is_low_surrogate(ch2 as i32) || !chars::is_xml11_ncname_start(supplemental(ch, ch2))
            {
                self.cur_mut().position -= 1;
                return Ok(false);
            }
            let e = self.cur_mut();
            e.position += 1;
            if e.position == e.count {
                e.ch[0] = ch;
                e.ch[1] = ch2;
                offset = 0;
                if self.load(2, false)? {
                    self.cur_mut().column += 2;
                    let name = self.string(0, 2);
                    qname.set(None, Some(name.clone()), Some(name), None);
                    self.check_entity_limit(Some(nt), 0, 2)?;
                    return Ok(true);
                }
            }
        } else {
            return Ok(false);
        }
        let mut index: Option<usize> = None;
        let mut saw_incomplete_pair = false;
        loop {
            let ch = self.at(self.cur().position)?;
            if chars::is_xml11_name(ch as i32) {
                if ch == ':' as u16 {
                    if index.is_some() {
                        break;
                    }
                    let i = self.cur().position;
                    index = Some(i);
                    self.check_limit(Limit::MaxName, i - offset)?;
                }
                let length = self.check_before_load(offset, index)?;
                if length > 0 {
                    if let Some(i) = index {
                        index = Some(i - offset);
                    }
                    offset = 0;
                    if self.load(length, false)? {
                        break;
                    }
                }
            } else if chars::is_xml11_name_high_surrogate(ch as i32) {
                let length = self.check_before_load(offset, index)?;
                if length > 0 {
                    if let Some(i) = index {
                        index = Some(i - offset);
                    }
                    offset = 0;
                    if self.load(length, false)? {
                        saw_incomplete_pair = true;
                        self.cur_mut().position -= 1;
                        break;
                    }
                }
                let ch2 = self.at(self.cur().position)?;
                if !is_low_surrogate(ch2 as i32) || !chars::is_xml11_name(supplemental(ch, ch2)) {
                    saw_incomplete_pair = true;
                    self.cur_mut().position -= 1;
                    break;
                }
                let length = self.check_before_load(offset, index)?;
                if length > 0 {
                    if let Some(i) = index {
                        index = Some(i - offset);
                    }
                    offset = 0;
                    if self.load(length, false)? {
                        break;
                    }
                }
            } else {
                break;
            }
        }
        let length = self.cur().position - offset;
        self.cur_mut().column += length as i32;
        if length == 0 {
            return Ok(false);
        }
        let rawname = self.string(offset, length);
        let (prefix, localpart) = match index {
            Some(index) => {
                let prefix_length = index - offset;
                self.check_limit(Limit::MaxName, prefix_length)?;
                let prefix = self.string(offset, prefix_length);
                let len = length - prefix_length - 1;
                let start_local = self.at(index + 1)? as i32;
                if !chars::is_xml11_ncname_start(start_local)
                    && (!chars::is_xml11_name_high_surrogate(start_local) || saw_incomplete_pair)
                {
                    return Err(self.error(
                        Domain::Xml,
                        "IllegalQName",
                        vec![Arg::Str(rawname.clone())],
                    ));
                }
                self.check_limit(Limit::MaxName, len)?;
                (Some(prefix), self.string(index + 1, len))
            }
            None => {
                self.check_limit(Limit::MaxName, length)?;
                (None, rawname.clone())
            }
        };
        qname.set(prefix, Some(localpart), Some(rawname), None);
        self.check_entity_limit(Some(nt), offset, length)?;
        Ok(true)
    }

    /// The characters `ch[offset..offset + length]`.
    pub(crate) fn string(&self, offset: usize, length: usize) -> String {
        String::from_utf16_lossy(&self.cur().ch[offset..offset + length])
    }

    /// `scanContent`: the content up to markup or a newline that does not
    /// start it, and the character after it (-1 at the buffer's end).
    pub(crate) fn scan_content(&mut self) -> R<i32> {
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        } else if self.cur().position == self.cur().count - 1 {
            let e = self.cur_mut();
            e.ch[0] = e.ch[e.count - 1];
            self.load(1, false)?;
            self.cur_mut().position = 0;
        }
        if self.normalize_newlines(self.v11, false, None)? {
            return Ok(-1);
        }
        let external = self.cur().external;
        loop {
            let e = self.cur();
            if e.position >= e.count {
                break;
            }
            let c = self.at(e.position)? as i32;
            self.cur_mut().position += 1;
            let stop = if !self.v11 {
                !chars::is_content(c)
            } else if external {
                !chars::is_xml11_content(c) || c == 0x85 || c == 0x2028
            } else {
                !chars::is_xml11_internal_entity_content(c)
            };
            if stop {
                self.cur_mut().position -= 1;
                break;
            }
        }
        let length = self.cur().position - self.offset;
        let newlines = self.newlines as i32;
        self.cur_mut().column += length as i32 - newlines;
        if !self.counted {
            let offset = self.offset;
            self.check_entity_limit(None, offset, length)?;
        }
        let e = self.cur();
        if e.position != e.count {
            let c = self.at(e.position)? as i32;
            let newline = if self.v11 {
                (c == '\r' as i32 || c == 0x85 || c == 0x2028) && external
            } else {
                c == '\r' as i32 && self.external()
            };
            Ok(if newline { '\n' as i32 } else { c })
        } else {
            Ok(-1)
        }
    }

    /// `scanLiteral`: an attribute value or literal up to the quote or a
    /// character the caller handles, and that character (-1 at the buffer's
    /// end or a literal's quote).
    pub(crate) fn scan_literal(&mut self, quote: i32, is_nsuri: bool) -> R<i32> {
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        } else if self.cur().position == self.cur().count - 1 {
            let e = self.cur_mut();
            e.ch[0] = e.ch[e.count - 1];
            self.load(1, false)?;
            self.cur_mut().position = 0;
        }
        if self.white_space_info_needed {
            self.white_space_len = 0;
        }
        if self.normalize_newlines(self.v11, true, None)? {
            return Ok(-1);
        }
        let external = self.cur().external;
        if !self.v11 {
            let is_external = self.external();
            loop {
                let e = self.cur();
                if e.position >= e.count {
                    break;
                }
                let c = self.at(e.position)? as i32;
                if (c == quote && (!e.literal || is_external))
                    || c == '%' as i32
                    || !chars::is_content(c)
                    || (c == '\r' as i32 && !is_external)
                {
                    break;
                }
                if self.white_space_info_needed && c == '\t' as i32 {
                    let p = e.position;
                    self.white_space.push(p);
                    self.white_space_len = self.white_space.len();
                }
                self.cur_mut().position += 1;
            }
        } else if external {
            loop {
                let e = self.cur();
                if e.position >= e.count {
                    break;
                }
                let c = self.at(e.position)? as i32;
                self.cur_mut().position += 1;
                if c == quote
                    || c == '%' as i32
                    || !chars::is_xml11_content(c)
                    || c == 0x85
                    || c == 0x2028
                {
                    self.cur_mut().position -= 1;
                    break;
                }
            }
        } else {
            loop {
                let e = self.cur();
                if e.position >= e.count {
                    break;
                }
                let literal = e.literal;
                let c = self.at(e.position)? as i32;
                self.cur_mut().position += 1;
                if (c == quote && !literal)
                    || c == '%' as i32
                    || !chars::is_xml11_internal_entity_content(c)
                    || c == '\r' as i32
                {
                    self.cur_mut().position -= 1;
                    break;
                }
            }
        }
        let length = self.cur().position - self.offset;
        let newlines = self.newlines as i32;
        self.cur_mut().column += length as i32 - newlines;
        let offset = self.offset;
        self.check_entity_limit(None, offset, length)?;
        if is_nsuri {
            self.check_limit(Limit::MaxName, length)?;
        }
        self.literal_start = offset;
        self.literal_length = length;
        let e = self.cur();
        if e.position != e.count {
            let c = self.at(e.position)? as i32;
            Ok(if c == quote && e.literal { -1 } else { c })
        } else {
            Ok(-1)
        }
    }

    /// `scanData`: data up to `delimiter`, true when more is to come (a
    /// newline, an invalid character, or the buffer's end).
    pub(crate) fn scan_data(&mut self, delimiter: &str, chunk_limit: usize) -> R<bool> {
        let delim: Vec<u16> = delimiter.encode_utf16().collect();
        let delim_len = delim.len();
        let mut done = false;
        let external = self.cur().external;
        let mut buffered = 0usize;
        // XMLEntityScanner reloads while fewer than the delimiter remain,
        // XML11EntityScanner while no more than it does.
        let short = |p: &Parser| {
            let e = p.cur();
            if p.v11 {
                e.position as isize >= e.count as isize - delim_len as isize
            } else {
                e.position as isize > e.count as isize - delim_len as isize
            }
        };
        loop {
            if self.cur().position == self.cur().count {
                self.load(0, true)?;
            }
            let mut next_entity = false;
            while short(self) && !next_entity {
                let e = self.cur_mut();
                let (p, c) = (e.position, e.count);
                e.ch.copy_within(p..c, 0);
                next_entity = self.load(c - p, false)?;
                self.cur_mut().position = 0;
            }
            if short(self) {
                let (p, c) = (self.cur().position, self.cur().count);
                self.check_entity_limit(Some(NameType::Comment), p, c - p)?;
                let e = self.cur_mut();
                e.column += e.count as i32;
                e.position = e.count;
                self.load(0, true)?;
                return Ok(false);
            }
            if self.normalize_newlines(self.v11, false, Some(NameType::Comment))? {
                return Ok(true);
            }
            loop {
                let e = self.cur();
                if e.position >= e.count {
                    break;
                }
                let c = self.at(e.position)?;
                self.cur_mut().position += 1;
                if c == delim[0] {
                    let delim_offset = self.cur().position - 1;
                    let mut outer = false;
                    #[allow(clippy::needless_range_loop)]
                    for i in 1..delim_len {
                        if self.cur().position == self.cur().count {
                            self.cur_mut().position -= i;
                            outer = true;
                            break;
                        }
                        let c = self.at(self.cur().position)?;
                        self.cur_mut().position += 1;
                        if delim[i] != c {
                            // XMLEntityScanner steps back over what it
                            // matched; XML11EntityScanner over one character.
                            if self.v11 {
                                self.cur_mut().position -= 1;
                            } else {
                                self.cur_mut().position -= i;
                            }
                            break;
                        }
                    }
                    if outer {
                        break;
                    }
                    if self.cur().position == delim_offset + delim_len {
                        done = true;
                        break;
                    }
                } else if (!self.v11 && (c == '\n' as u16 || (self.external() && c == '\r' as u16)))
                    || (self.v11
                        && ((external && matches!(c, 0x0A | 0x0D | 0x85 | 0x2028))
                            || (!external && c == '\n' as u16)))
                {
                    self.cur_mut().position -= 1;
                    break;
                } else if (!self.v11 && chars::is_invalid(c as i32))
                    || (self.v11
                        && ((external && !chars::is_xml11_valid_literal(c as i32))
                            || (!external && !chars::is_xml11_valid(c as i32))))
                {
                    self.cur_mut().position -= 1;
                    let length = self.cur().position - self.offset;
                    let newlines = self.newlines as i32;
                    self.cur_mut().column += length as i32 - newlines;
                    let offset = self.offset;
                    self.check_entity_limit(Some(NameType::Comment), offset, length)?;
                    return Ok(true);
                }
                if chunk_limit > 0 && buffered + self.cur().position - self.offset >= chunk_limit {
                    break;
                }
            }
            let mut length = self.cur().position - self.offset;
            let newlines = self.newlines as i32;
            self.cur_mut().column += length as i32 - newlines;
            let offset = self.offset;
            self.check_entity_limit(Some(NameType::Comment), offset, length)?;
            if done {
                length -= delim_len;
            }
            buffered += length;
            if chunk_limit > 0 && buffered >= chunk_limit {
                break;
            }
            if done || chunk_limit != 0 {
                break;
            }
        }
        Ok(!done)
    }

    pub(crate) fn skip_char(&mut self, c: i32, nt: Option<NameType>) -> R<bool> {
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        let offset = self.cur().position;
        let cc = self.at(offset)? as i32;
        if cc == c {
            let e = self.cur_mut();
            e.position += 1;
            if c == '\n' as i32 {
                e.line += 1;
                e.column = 1;
            } else {
                e.column += 1;
            }
            let len = self.cur().position - offset;
            self.check_entity_limit(nt, offset, len)?;
            return Ok(true);
        }
        if self.v11 && c == '\n' as i32 && (cc == 0x2028 || cc == 0x85) && self.cur().external {
            let e = self.cur_mut();
            e.position += 1;
            e.line += 1;
            e.column = 1;
            let len = self.cur().position - offset;
            self.check_entity_limit(nt, offset, len)?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn skip_spaces(&mut self) -> R<bool> {
        if self.v11 {
            return self.skip_spaces11();
        }
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        if self.entities.is_empty() {
            return Ok(false);
        }
        let mut c = self.at(self.cur().position)? as i32;
        let mut offset = self.cur().position as isize - 1;
        if !chars::is_space(c) {
            return Ok(false);
        }
        loop {
            let mut entity_changed = false;
            let external = self.external();
            if c == '\n' as i32 || (external && c == '\r' as i32) {
                let e = self.cur_mut();
                e.line += 1;
                e.column = 1;
                if e.position == e.count - 1 {
                    e.ch[0] = c as u16;
                    entity_changed = self.load(1, true)?;
                    if !entity_changed {
                        self.cur_mut().position = 0;
                    } else if self.entities.is_empty() {
                        return Ok(true);
                    }
                }
                if c == '\r' as i32 && external {
                    let p = self.cur().position + 1;
                    self.cur_mut().position = p;
                    if self.at(p)? != '\n' as u16 {
                        self.cur_mut().position -= 1;
                    }
                }
            } else {
                self.cur_mut().column += 1;
            }
            let p = self.cur().position;
            self.check_entity_limit(
                None,
                offset.max(0) as usize,
                (p as isize - offset).max(0) as usize,
            )?;
            offset = p as isize;
            if !entity_changed {
                self.cur_mut().position += 1;
            }
            if self.cur().position == self.cur().count {
                self.load(0, true)?;
                if self.entities.is_empty() {
                    return Ok(true);
                }
            }
            c = self.at(self.cur().position)? as i32;
            if !chars::is_space(c) {
                return Ok(true);
            }
        }
    }

    fn skip_spaces11(&mut self) -> R<bool> {
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        if self.entities.is_empty() {
            return Ok(false);
        }
        let mut c = self.at(self.cur().position)? as i32;
        let mut offset = self.cur().position as isize - 1;
        let external = self.cur().external;
        let space = |c: i32| {
            if external {
                chars::is_xml11_space(c)
            } else {
                chars::is_space(c)
            }
        };
        if !space(c) {
            return Ok(false);
        }
        loop {
            let mut entity_changed = false;
            let newline = if external {
                matches!(c, 0x0A | 0x0D | 0x85 | 0x2028)
            } else {
                c == '\n' as i32
            };
            if newline {
                let e = self.cur_mut();
                e.line += 1;
                e.column = 1;
                if e.position == e.count - 1 {
                    e.ch[0] = c as u16;
                    entity_changed = self.load(1, true)?;
                    if !entity_changed {
                        self.cur_mut().position = 0;
                    } else if self.entities.is_empty() {
                        return Ok(true);
                    }
                }
                if external && c == '\r' as i32 {
                    let p = self.cur().position + 1;
                    self.cur_mut().position = p;
                    let cc = self.at(p)?;
                    if cc != '\n' as u16 && cc != 0x85 {
                        self.cur_mut().position -= 1;
                    }
                }
            } else {
                self.cur_mut().column += 1;
            }
            let p = self.cur().position;
            self.check_entity_limit(
                None,
                offset.max(0) as usize,
                (p as isize - offset).max(0) as usize,
            )?;
            offset = p as isize;
            if !entity_changed {
                self.cur_mut().position += 1;
            }
            if self.cur().position == self.cur().count {
                self.load(0, true)?;
                if self.entities.is_empty() {
                    return Ok(true);
                }
            }
            c = self.at(self.cur().position)? as i32;
            if !space(c) {
                return Ok(true);
            }
        }
    }

    /// `arrangeCapacity`: at least `length` characters in the buffer from
    /// the position, unless the entity ends first.
    pub(crate) fn arrange_capacity(&mut self, length: usize, change_entity: bool) -> R<bool> {
        let e = self.cur();
        if e.count - e.position >= length {
            return Ok(true);
        }
        while self.cur().count - self.cur().position < length {
            let e = self.cur_mut();
            if e.ch.len() - e.position < length {
                let (p, c) = (e.position, e.count);
                e.ch.copy_within(p..c, 0);
                e.count = c - p;
                e.position = 0;
            }
            let e = self.cur();
            if e.count - e.position < length {
                let (pos, count) = (e.position, e.count);
                let changed = self.load(count, change_entity)?;
                self.cur_mut().position = pos;
                if changed {
                    break;
                }
            }
        }
        let e = self.cur();
        Ok(e.count - e.position >= length)
    }

    /// `skipString`: true when the next characters are `s`.
    pub(crate) fn skip_string(&mut self, s: &str) -> R<bool> {
        let s: Vec<u16> = s.encode_utf16().collect();
        if self.v11 {
            return self.skip_string11(&s);
        }
        let length = s.len();
        if self.arrange_capacity(length, false)? {
            let before = self.cur().position;
            if self.cur().ch[before..before + length] == s[..] {
                let e = self.cur_mut();
                e.position += length;
                e.column += length as i32;
                if !self.detecting_version {
                    self.check_entity_limit(None, before, length)?;
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn skip_string11(&mut self, s: &[u16]) -> R<bool> {
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        let length = s.len();
        let before = self.cur().position;
        #[allow(clippy::needless_range_loop)]
        for i in 0..length {
            let c = self.at(self.cur().position)?;
            self.cur_mut().position += 1;
            if c != s[i] {
                self.cur_mut().position -= i + 1;
                return Ok(false);
            }
            if i < length - 1 && self.cur().position == self.cur().count {
                let e = self.cur_mut();
                let count = e.count;
                e.ch.copy_within(count - i - 1..count, 0);
                if self.load(i + 1, false)? {
                    self.cur_mut().position -= i + 1;
                    return Ok(false);
                }
            }
        }
        self.cur_mut().column += length as i32;
        if !self.detecting_version {
            self.check_entity_limit(None, before, length)?;
        }
        Ok(true)
    }

    /// `skipDeclSpaces`.
    pub(crate) fn skip_decl_spaces(&mut self) -> R<bool> {
        if self.cur().position == self.cur().count {
            self.load(0, true)?;
        }
        let mut c = self.at(self.cur().position)? as i32;
        if !chars::is_space(c) {
            return Ok(false);
        }
        let external = self.cur().external;
        loop {
            let mut entity_changed = false;
            if c == '\n' as i32 || (external && c == '\r' as i32) {
                let e = self.cur_mut();
                e.line += 1;
                e.column = 1;
                if e.position == e.count - 1 {
                    e.ch[0] = c as u16;
                    entity_changed = self.load(1, true)?;
                    if !entity_changed {
                        self.cur_mut().position = 0;
                    }
                }
                if c == '\r' as i32 && external {
                    let p = self.cur().position + 1;
                    self.cur_mut().position = p;
                    if self.at(p)? != '\n' as u16 {
                        self.cur_mut().position -= 1;
                    }
                }
            } else {
                self.cur_mut().column += 1;
            }
            if !entity_changed {
                self.cur_mut().position += 1;
            }
            if self.cur().position == self.cur().count {
                self.load(0, true)?;
            }
            c = self.at(self.cur().position)? as i32;
            if !chars::is_space(c) {
                return Ok(true);
            }
        }
    }

    /// `normalizeNewlines`: a run of newlines at the position, read as line
    /// feeds; true when it reaches the end of the buffer.
    pub(crate) fn normalize_newlines(
        &mut self,
        v11: bool,
        store_ws: bool,
        nt: Option<NameType>,
    ) -> R<bool> {
        self.offset = self.cur().position;
        let c = self.at(self.offset)? as i32;
        self.newlines = 0;
        self.counted = false;
        let external = self.external();
        let newline =
            |c: i32| c == '\n' as i32 || c == '\r' as i32 || (v11 && (c == 0x85 || c == 0x2028));
        if !(c == '\n' as i32
            || c == '\r' as i32
            || (v11 && (c == 0x85 || c == 0x2028) && external))
        {
            return Ok(false);
        }
        loop {
            let p = self.cur().position;
            let c = self.at(p)? as i32;
            self.cur_mut().position += 1;
            if !newline(c) {
                self.cur_mut().position -= 1;
                break;
            }
            self.newlines += 1;
            let e = self.cur_mut();
            e.line += 1;
            e.column = 1;
            if e.position == e.count {
                let offset = self.offset;
                let newlines = self.newlines;
                self.check_entity_limit(nt, offset, newlines)?;
                self.offset = 0;
                self.cur_mut().position = newlines;
                if self.load(newlines, false)? {
                    self.counted = true;
                    break;
                }
            }
            if c == '\r' as i32 && external {
                let cc = self.at(self.cur().position)? as i32;
                if cc == '\n' as i32 || (v11 && cc == 0x85) {
                    self.cur_mut().position += 1;
                    self.offset += 1;
                } else {
                    self.newlines += 1;
                }
            }
            let e = self.cur();
            if e.position as isize >= e.count as isize - 1 {
                break;
            }
        }
        let (offset, position) = (self.offset, self.cur().position);
        for i in offset..position {
            self.cur_mut().ch[i] = '\n' as u16;
            if store_ws {
                self.white_space.push(i);
                self.white_space_len = self.white_space.len();
            }
        }
        let length = position - offset;
        if position as isize == self.cur().count as isize - 1 {
            self.check_entity_limit(nt, offset, length)?;
            return Ok(true);
        }
        Ok(false)
    }

    /// `checkEntityLimit(NameType, ScannedEntity, int, int)`.
    pub(crate) fn check_entity_limit(
        &mut self,
        nt: Option<NameType>,
        _offset: usize,
        length: usize,
    ) -> R<()> {
        if self.entities.is_empty() || !self.cur().is_ge {
            return Ok(());
        }
        if nt != Some(NameType::Reference) {
            self.check_limit(Limit::GeneralEntitySize, length)?;
        }
        if matches!(
            nt,
            Some(NameType::ElementStart) | Some(NameType::AttributeName)
        ) {
            self.check_node_count()?;
        }
        Ok(())
    }

    pub(crate) fn check_node_count(&mut self) -> R<()> {
        if !self.entities.is_empty() && self.cur().is_ge {
            self.check_limit(Limit::EntityReplacement, 1)?;
        }
        Ok(())
    }

    /// `XMLEntityScanner.checkLimit`.
    pub(crate) fn check_limit(&mut self, limit: Limit, length: usize) -> R<()> {
        let name = self.cur().name.clone();
        self.limits.add(limit, &name, length as i64);
        if self.limits.over(limit) {
            let args = if limit == Limit::EntityReplacement {
                vec![
                    Arg::Int(self.limits.value(limit)),
                    Arg::Int(limit.max()),
                    Arg::Str(super::STATE_LITERAL.into()),
                ]
            } else {
                vec![
                    Arg::Str(name),
                    Arg::Int(self.limits.value(limit)),
                    Arg::Int(limit.max()),
                    Arg::Str(super::STATE_LITERAL.into()),
                ]
            };
            return Err(self.error(Domain::Xml, limit.key(), args));
        }
        if self.limits.over(Limit::TotalEntitySize) {
            let args = vec![
                Arg::Int(self.limits.total(Limit::TotalEntitySize)),
                Arg::Int(Limit::TotalEntitySize.max()),
                Arg::Str(Limit::TotalEntitySize.system_property().into()),
            ];
            return Err(self.error(Domain::Xml, "TotalEntitySizeLimit", args));
        }
        Ok(())
    }

    /// Starts an entity on `source`: `XMLEntityManager.startEntity` after
    /// `setupCurrentEntity`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start_scanned_entity(
        &mut self,
        is_ge: bool,
        name: &str,
        reader: Reader,
        stream: Option<Stream>,
        encoding: Option<String>,
        literal: bool,
        external: bool,
        system_id: Option<String>,
    ) -> R<()> {
        let entity = ScannedEntity::new(
            is_ge,
            name,
            reader,
            stream,
            encoding.clone(),
            literal,
            false,
            external,
            system_id,
        );
        self.entities.push(entity);
        self.is_external = external;
        self.limits.start_entity(name);
        self.entity_expansion_count += 1;
        self.limits.add(Limit::EntityExpansion, name, 1);
        if self.limits.over(Limit::EntityExpansion) {
            // getLimitValueByIndex: the limit as a string.
            let args = vec![
                Arg::Str(Limit::EntityExpansion.max().to_string()),
                Arg::Str(Limit::EntityExpansion.system_property().into()),
            ];
            return Err(self.error(Domain::Xml, "EntityExpansionLimit", args));
        }
        self.handler_start_entity(name, encoding.as_deref(), false)
    }
}
