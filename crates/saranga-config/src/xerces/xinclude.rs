//! The errors `XIncludeHandler` reports while the DOM parses a document
//! with `xi:include`s: the structure of includes and fallbacks, an `href`
//! it cannot use, a resource it cannot read (a warning, then the fallback),
//! and an included document that is not well formed, which its own parse
//! reports before the including one fails. What the includes are replaced
//! by is `crate::xinclude`'s business.

use std::path::{Path, PathBuf};

use super::{Arg, Failure, Parser, R, Stop};

const XINCLUDE_NS: &str = "http://www.w3.org/2001/XInclude";
const NORMAL: u8 = 1;
const IGNORE: u8 = 2;
const EXPECT_FALLBACK: u8 = 3;

/// `XIncludeMessages.properties`, the keys the handler reports.
fn template(key: &str) -> &'static str {
    match key {
        "FallbackChild" => {
            "Elements from namespace ''http://www.w3.org/2001/XInclude'', other than ''include'', are not allowed to be children of ''fallback'' elements.  However, ''{0}'' was found."
        }
        "FallbackParent" => {
            "A 'fallback' element was found that did not have 'include' as the parent."
        }
        "HrefFragmentIdentifierIllegal" => {
            "Fragment identifiers must not be used. The ''href'' attribute value ''{0}'' is not permitted."
        }
        "HrefSyntacticallyInvalid" => {
            "''href'' attribute value ''{0}'' is syntactically invalid. After applying escaping rules the value is neither a syntactically correct URI or IRI."
        }
        "IncludeChild" => {
            "Elements from namespace ''http://www.w3.org/2001/XInclude'', other than ''fallback'', are not allowed to be children of ''include'' elements.  However, ''{0}'' was found."
        }
        "InvalidParseValue" => {
            "Invalid value for ''parse'' attribute on ''include'' element: ''{0}''."
        }
        "MultipleFallbacks" => {
            "The [children] of an 'include' element cannot contain more than one 'fallback' element."
        }
        "NoFallback" => {
            "An 'include' with href ''{0}''failed, and no 'fallback' element was found."
        }
        "RecursiveInclude" => {
            "Recursive include detected.  Document ''{0}'' was already processed."
        }
        "TextResourceError" => {
            "Include operation failed, reverting to fallback. Resource error reading file as text (href=''{0}''). Reason: {1}"
        }
        "XMLParseError" => "Error attempting to parse XML file (href=''{0}''). Reason: {1}",
        "XMLResourceError" => {
            "Include operation failed, reverting to fallback. Resource error reading file as XML (href=''{0}''). Reason: {1}"
        }
        "XpointerMissing" => "xpointer attribute must be present when href attribute is absent.",
        "AcceptMalformed" => {
            "Characters outside the range #x20 through #x7E are not allowed in the value of the 'accept' attribute of an 'include' element."
        }
        "AcceptLanguageMalformed" => {
            "Characters outside the range #x20 through #x7E are not allowed in the value of the 'accept-language' attribute of an 'include' element."
        }
        _ => "The error message corresponding to the message key can not be found.",
    }
}

fn message(key: &str, args: &[&str]) -> String {
    if args.is_empty() {
        return template(key).to_string();
    }
    let args: Vec<Arg> = args.iter().map(|a| Arg::Str(a.to_string())).collect();
    super::message_format(template(key), &args)
}

/// The handler's state for one document.
#[derive(Debug, Default)]
pub(crate) struct Includes {
    /// The document's expanded system id: `None` for `broker.xml`, which
    /// `XMLUtil` parses from a string.
    pub system_id: Option<String>,
    /// The documents that included this one, nearest first.
    pub parents: Vec<String>,
    depth: usize,
    state: Vec<u8>,
    saw_include: Vec<bool>,
    saw_fallback: Vec<bool>,
    /// `xml:base` scopes: the depth and the base it sets.
    bases: Vec<(usize, String)>,
}

fn get<T: Copy + Default>(v: &[T], i: usize) -> T {
    v.get(i).copied().unwrap_or_default()
}

fn set<T: Copy + Default>(v: &mut Vec<T>, i: usize, x: T) {
    if v.len() <= i {
        v.resize(i + 1, T::default());
    }
    v[i] = x;
}

/// `XMLEntityManager.expandSystemId` for a file path.
pub(crate) fn file_uri(path: &Path) -> String {
    let p = path.to_string_lossy().replace('\\', "/");
    if p.starts_with('/') {
        format!("file://{p}")
    } else {
        format!("file:///{p}")
    }
}

fn path_of(uri: &str) -> PathBuf {
    let rest = uri.trim_start_matches("file:").trim_start_matches("//");
    let rest = if cfg!(windows) {
        rest.trim_start_matches('/')
    } else {
        rest
    };
    PathBuf::from(percent_decode(rest))
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && i + 2 < b.len()
            && let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `escapeHref`, for the characters a configuration's `href` has.
fn escape_href(href: &str) -> Option<String> {
    let mut out = String::new();
    for c in href.chars() {
        let u = c as u32;
        if u < 0x20 {
            return None;
        }
        if u > 0x7E
            || matches!(
                c,
                ' ' | '<' | '>' | '"' | '{' | '}' | '|' | '\\' | '^' | '`'
            )
        {
            let mut buf = [0u8; 4];
            for b in c.encode_utf8(&mut buf).bytes() {
                out.push_str(&format!("%{b:02X}"));
            }
        } else {
            out.push(c);
        }
    }
    Some(out)
}

/// Whether `URI(href, true)` takes the (escaped) href: its escapes are
/// well formed.
fn uri_ok(href: &str) -> bool {
    let b = href.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            if i + 2 >= b.len() || !b[i + 1].is_ascii_hexdigit() || !b[i + 2].is_ascii_hexdigit() {
                return false;
            }
            i += 3;
            continue;
        }
        i += 1;
    }
    !href
        .chars()
        .any(|c| (c as u32) < 0x20 || c == ' ' || (c as u32) > 0x7E)
}

impl Parser {
    fn xinclude_error(&self, key: &str, args: &[&str]) -> Stop {
        let message = message(key, args);
        Stop::Fatal {
            message,
            line: self.line(),
            column: self.column(),
            system_id: self.inc.system_id.clone(),
        }
    }

    /// `reportResourceError`: a warning the default handler prints.
    fn xinclude_warning(&mut self, key: &str, args: &[&str]) {
        let id = self
            .inc
            .system_id
            .as_deref()
            .map(|s| s.rsplit('/').next().unwrap_or(s))
            .unwrap_or("");
        let line = format!(
            "[Warning] {id}:{}:{}: {}",
            self.line(),
            self.column(),
            message(key, args)
        );
        self.printed.push(line);
    }

    fn current_base(&self) -> Option<String> {
        self.inc
            .bases
            .last()
            .map(|b| b.1.clone())
            .or_else(|| self.inc.system_id.clone())
    }

    /// Resolves `href` as `XMLEntityManager.expandSystemId` against the
    /// current base: the working directory for `broker.xml` itself.
    fn resolve_href(&self, href: &str) -> PathBuf {
        match self.current_base() {
            Some(base) => {
                let base = path_of(&base);
                let dir = if base.is_dir() {
                    base
                } else {
                    base.parent().map(Path::to_path_buf).unwrap_or_default()
                };
                normalize(&dir.join(percent_decode(href)))
            }
            None => normalize(&self.base_dir.join(percent_decode(href))),
        }
    }

    /// The handler's `startElement` or `emptyElement`, for the element the
    /// scanner has just read.
    pub(crate) fn xinclude_start(&mut self, empty: bool) -> R<()> {
        let element = self.s.elements.last().cloned().unwrap_or_default();
        let local = element.localpart.clone().unwrap_or_default();
        let in_ns = element.uri.as_deref() == Some(XINCLUDE_NS);
        let attr = |p: &Parser, name: &str| {
            p.s.attributes
                .iter()
                .find(|a| a.name.rawname.as_deref() == Some(name))
                .map(|a| a.value.clone())
        };
        let inc = &mut self.inc;
        inc.depth += 1;
        let d = inc.depth;
        let last = get(&inc.state, d - 1);
        let state =
            if last == EXPECT_FALLBACK && d >= 2 && get(&inc.state, d - 2) == EXPECT_FALLBACK {
                IGNORE
            } else {
                last
            };
        set(&mut inc.state, d, state);
        if let Some(base) = attr(self, "xml:base") {
            let expanded = match self.current_base() {
                Some(b) => file_uri(&normalize(
                    &path_of(&b).parent().unwrap_or(Path::new("")).join(&base),
                )),
                None => file_uri(&normalize(&self.base_dir.join(&base))),
            };
            let expanded = if base.ends_with('/') {
                format!("{expanded}/")
            } else {
                expanded
            };
            self.inc.bases.push((d, expanded));
        }
        if in_ns && local == "include" {
            let success = self.handle_include(&element)?;
            if success {
                set(&mut self.inc.state, d, IGNORE);
            } else if empty {
                let href = attr(self, "href").unwrap_or_default();
                return Err(self.xinclude_error("NoFallback", &[&href]));
            } else {
                set(&mut self.inc.state, d, EXPECT_FALLBACK);
            }
        } else if in_ns && local == "fallback" {
            if !get(&self.inc.saw_include, d - 1) {
                if get(&self.inc.state, d) != IGNORE {
                    return Err(self.xinclude_error("FallbackParent", &[]));
                }
            } else {
                set(&mut self.inc.saw_include, d, false);
                if get(&self.inc.saw_fallback, d) {
                    return Err(self.xinclude_error("MultipleFallbacks", &[]));
                }
                set(&mut self.inc.saw_fallback, d, true);
                if get(&self.inc.state, d) == EXPECT_FALLBACK {
                    set(&mut self.inc.state, d, NORMAL);
                }
            }
        } else if in_ns {
            let raw = element.raw();
            if get(&self.inc.saw_include, d - 1) {
                return Err(self.xinclude_error("IncludeChild", &[&raw]));
            }
            if get(&self.inc.saw_fallback, d - 1) {
                return Err(self.xinclude_error("FallbackChild", &[&raw]));
            }
        }
        if empty {
            self.xinclude_finish();
        }
        Ok(())
    }

    /// The handler's `endElement`.
    pub(crate) fn xinclude_end(&mut self, element: &super::QName) -> R<()> {
        let d = self.inc.depth;
        let in_ns = element.uri.as_deref() == Some(XINCLUDE_NS);
        let local = element.localpart.clone().unwrap_or_default();
        if in_ns
            && local == "include"
            && get(&self.inc.state, d) == EXPECT_FALLBACK
            && !get(&self.inc.saw_fallback, d + 1)
        {
            return Err(self.xinclude_error("NoFallback", &["unknown"]));
        }
        if in_ns && local == "fallback" && get(&self.inc.state, d) == NORMAL {
            set(&mut self.inc.state, d, IGNORE);
        }
        self.xinclude_finish();
        Ok(())
    }

    fn xinclude_finish(&mut self) {
        let d = self.inc.depth;
        set(&mut self.inc.saw_fallback, d + 1, false);
        set(&mut self.inc.saw_include, d, false);
        if self.inc.bases.last().is_some_and(|b| b.0 == d) {
            self.inc.bases.pop();
        }
        self.inc.depth -= 1;
    }

    /// `handleIncludeElement`: false when the include failed and its
    /// fallback is wanted.
    fn handle_include(&mut self, element: &super::QName) -> R<bool> {
        let d = self.inc.depth;
        let _ = element;
        if get(&self.inc.saw_include, d - 1) {
            return Err(self.xinclude_error("IncludeChild", &["include"]));
        }
        if get(&self.inc.state, d) == IGNORE {
            return Ok(true);
        }
        set(&mut self.inc.saw_include, d, true);
        let attr = |p: &Parser, name: &str| {
            p.s.attributes
                .iter()
                .find(|a| a.name.rawname.as_deref() == Some(name))
                .map(|a| a.value.clone())
        };
        let mut href = attr(self, "href").unwrap_or_default();
        let raw_href = href.clone();
        let parse = attr(self, "parse").unwrap_or_else(|| "xml".into());
        let xpointer = attr(self, "xpointer");
        if href.is_empty() && parse == "xml" {
            if xpointer.is_none() {
                return Err(self.xinclude_error("XpointerMissing", &[]));
            }
            let reason = "An xpointer was specified that points to a location in the source infoset. This location cannot be accessed due to the streaming nature of the processor.";
            self.xinclude_warning("XMLResourceError", &[&href, reason]);
            return Ok(false);
        }
        if !uri_ok(&href) {
            match escape_href(&href) {
                Some(escaped) if escaped != href => {
                    href = escaped;
                    if !uri_ok(&href) {
                        return Err(self.xinclude_error("HrefSyntacticallyInvalid", &[&href]));
                    }
                }
                _ => return Err(self.xinclude_error("HrefSyntacticallyInvalid", &[&href])),
            }
        }
        if href.contains('#') {
            return Err(self.xinclude_error("HrefFragmentIdentifierIllegal", &[&href]));
        }
        for (name, key) in [
            ("accept", "AcceptMalformed"),
            ("accept-language", "AcceptLanguageMalformed"),
        ] {
            if attr(self, name).is_some_and(|v| v.chars().any(|c| !(' '..='~').contains(&c))) {
                return Err(self.xinclude_error(key, &[]));
            }
        }
        if xpointer.is_some() {
            // Saranga does not evaluate XPointers; crate::xinclude refuses
            // them once the document is read.
            return Ok(true);
        }
        let path = self.resolve_href(&raw_href);
        match parse.as_str() {
            "xml" => {
                let bytes = match std::fs::read(&path) {
                    Ok(b) => b,
                    Err(e) => {
                        let reason = super::scanner::not_found(&path, &e);
                        self.xinclude_warning("XMLResourceError", &[&href, &reason]);
                        return Ok(false);
                    }
                };
                let uri = file_uri(&path);
                let mut parents = vec![self.inc.system_id.clone().unwrap_or_default()];
                parents.extend(self.inc.parents.iter().cloned());
                let mut child = Parser::new(super::Api::Dom, self.base_dir.clone());
                child.inc.system_id = Some(uri.clone());
                child.inc.parents = parents;
                let result = child.parse_dom_bytes(bytes, uri);
                if !matches!(result, Err(Failure::Parse(_))) {
                    self.printed.append(&mut child.printed);
                }
                match result {
                    Ok(()) => Ok(true),
                    Err(Failure::Parse(p)) => {
                        // What the included document printed, its own error last.
                        self.printed.extend(p.printed);
                        Err(self.xinclude_error("XMLParseError", &[&href, &p.message]))
                    }
                    Err(Failure::Io { message, .. }) => {
                        self.xinclude_warning("XMLResourceError", &[&href, &message]);
                        Ok(false)
                    }
                    Err(Failure::Xni { message, .. }) => {
                        Err(self.xinclude_error("XMLParseError", &[&href, &message]))
                    }
                    Err(Failure::Runtime { class, message }) => Err(Stop::Runtime {
                        class: Box::leak(class.into_boxed_str()),
                        message,
                    }),
                }
            }
            "text" => match std::fs::read(&path) {
                Ok(_) => Ok(true),
                Err(e) => {
                    let reason = super::scanner::not_found(&path, &e);
                    self.xinclude_warning("TextResourceError", &[&href, &reason]);
                    Ok(false)
                }
            },
            other => Err(self.xinclude_error("InvalidParseValue", &[other])),
        }
    }

    /// `startDocument` of an included document: is it one that includes it?
    pub(crate) fn xinclude_start_document(&mut self) -> R<()> {
        if let Some(id) = self.inc.system_id.clone()
            && self.inc.parents.contains(&id)
        {
            return Err(self.xinclude_error("RecursiveInclude", &[&id]));
        }
        Ok(())
    }
}

/// The path with `.` and `..` resolved, as a URI's are.
fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            std::path::Component::ParentDir => {
                out.pop();
            }
            std::path::Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hrefs_are_escaped_as_xerces_escapes_them() {
        assert_eq!(escape_href("a b.xml").as_deref(), Some("a%20b.xml"));
        assert!(!uri_ok("a%zz.xml"));
        assert!(uri_ok("a%20b.xml"));
    }
}
