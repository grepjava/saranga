//! XInclude in `broker.xml`, as the XInclude-aware parser `XMLUtil`
//! makes processes it: each `xi:include` is replaced by the document (or,
//! with `parse="text"`, the text) its `href` names, or by the children of
//! its `xi:fallback` when that cannot be read; with neither, the
//! configuration cannot be read.
//!
//! Artemis parses `broker.xml` from a string, without a location, so a
//! relative `href` in it is resolved against the working directory; one in
//! an included file, against that file's. Included documents have their
//! `${...}` replaced too (`replaceSystemPropsInXml`), and includes of their
//! own. `xpointer` is not supported.
//!
//! Xerces fixes up what it includes (`XIncludeHandler.processAttributes`):
//! the top-level elements of an included document get an `xml:base`, as do
//! the children of a fallback taken in `broker.xml` itself (an empty one,
//! that document having no base), and an `xml:lang` when their language is
//! not their include parent's. The schema then decides where those may
//! appear, as it does for Artemis.

use std::path::{Path, PathBuf};

use roxmltree::{Document, Node, ParsingOptions};

use crate::{ConfigError, Result};

const XINCLUDE_NS: &str = "http://www.w3.org/2001/XInclude";
const XML_NS: &str = "http://www.w3.org/XML/1998/namespace";

/// `text` with its includes replaced. `base` is the directory relative
/// hrefs are resolved against: `None` for the working directory.
pub(crate) fn expand(
    text: &str,
    base: Option<&Path>,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<String> {
    let scope = Scope {
        relative_base: None,
        language: String::new(),
    };
    expand_in(text, base, lookup, &mut Vec::new(), &scope)
}

/// The document a piece of text belongs to.
struct Scope {
    /// `getRelativeBaseURI` of the document: `None` for `broker.xml`.
    relative_base: Option<String>,
    /// The `[language]` in effect where the text is: `""` at the start of
    /// a document, as `XIncludeHandler` starts every document.
    language: String,
}

fn options<'a>() -> ParsingOptions<'a> {
    ParsingOptions {
        allow_dtd: true,
        ..ParsingOptions::default()
    }
}

fn is_xi(n: &Node<'_, '_>, name: &str) -> bool {
    n.is_element() && n.tag_name().namespace() == Some(XINCLUDE_NS) && n.tag_name().name() == name
}

fn expand_in(
    text: &str,
    base: Option<&Path>,
    lookup: &dyn Fn(&str) -> Option<String>,
    stack: &mut Vec<PathBuf>,
    scope: &Scope,
) -> Result<String> {
    if !text.contains(XINCLUDE_NS) {
        return Ok(text.to_owned());
    }
    let doc = Document::parse_with_options(text, options())?;
    // The outermost includes: those inside a fallback are expanded only if
    // the fallback is used.
    let includes: Vec<Node<'_, '_>> = doc
        .descendants()
        .filter(|n| is_xi(n, "include"))
        .filter(|n| !n.ancestors().skip(1).any(|a| is_xi(&a, "include")))
        .collect();
    let mut out = String::with_capacity(text.len());
    let mut at = 0;
    for include in includes {
        let range = include.range();
        out.push_str(&text[at..range.start]);
        out.push_str(&include_one(text, include, base, lookup, stack, scope)?);
        at = range.end;
    }
    out.push_str(&text[at..]);
    Ok(out)
}

/// The `[language]` at `n`: its nearest `xml:lang`, else the scope's.
fn language(n: Node<'_, '_>, scope: &Scope) -> String {
    n.ancestors()
        .find_map(|a| a.attribute((XML_NS, "lang")))
        .map_or_else(|| scope.language.clone(), str::to_owned)
}

/// What one `xi:include` is replaced by.
fn include_one(
    text: &str,
    include: Node<'_, '_>,
    base: Option<&Path>,
    lookup: &dyn Fn(&str) -> Option<String>,
    stack: &mut Vec<PathBuf>,
    scope: &Scope,
) -> Result<String> {
    let href = include.attribute("href").unwrap_or_default();
    if include.attribute("xpointer").is_some() {
        return Err(ConfigError::XInclude(format!(
            "xi:include of {href:?}: xpointer is not supported"
        )));
    }
    let parse = include.attribute("parse").unwrap_or("xml");
    if parse != "xml" && parse != "text" {
        return Err(include_error(format!(
            "Include operation failed: parse=\"{parse}\" is neither xml nor text"
        )));
    }
    if href.is_empty() {
        return Err(ConfigError::XInclude(
            "xi:include without href: including a part of the document itself needs xpointer, which is not supported".into(),
        ));
    }
    let path = resolve(href, base);
    // A resource that cannot be read takes the fallback; one that is not
    // well-formed fails the whole document, as with Xerces.
    let e = match read(&path, include.attribute("encoding")) {
        Ok(content) if parse == "text" => return Ok(escape(&content)),
        Ok(content) => {
            let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
            if stack.contains(&canonical) {
                return Err(include_error(format!(
                    "Include operation failed, detected a recursive include of {href:?}"
                )));
            }
            let relative_base = match &scope.relative_base {
                None => href.to_owned(),
                Some(parent) => relative_base_uri(parent, href, &path),
            };
            let parent_language = language(include, scope);
            stack.push(canonical);
            let included = included_document(
                &content,
                &path,
                lookup,
                stack,
                relative_base,
                &parent_language,
            );
            stack.pop();
            return included;
        }
        Err(e) => e,
    };
    match include.children().find(|c| is_xi(c, "fallback")) {
        Some(fallback) => {
            tracing::warn!(
                "Include operation failed, reverting to fallback. Resource error reading file as XML (href='{href}'). Reason: {e}"
            );
            expand_fallback(text, include, fallback, base, lookup, stack, scope)
        }
        None => Err(include_error(format!(
            "An include with href '{href}'failed, and no fallback element was found. ({e})"
        ))),
    }
}

fn include_error(message: String) -> ConfigError {
    ConfigError::XInclude(message)
}

/// A fallback's content, its includes expanded: parsed inside an element
/// declaring the XInclude namespace as the document does. In `broker.xml`
/// itself its top-level elements get an empty `xml:base`, the document's
/// base being none.
fn expand_fallback(
    text: &str,
    include: Node<'_, '_>,
    fallback: Node<'_, '_>,
    base: Option<&Path>,
    lookup: &dyn Fn(&str) -> Option<String>,
    stack: &mut Vec<PathBuf>,
    scope: &Scope,
) -> Result<String> {
    let (Some(first), Some(last)) = (fallback.first_child(), fallback.last_child()) else {
        return Ok(String::new());
    };
    let mut inner = String::new();
    let mut at = first.range().start;
    if scope.relative_base.is_none() {
        for child in fallback
            .children()
            .filter(|c| c.is_element() && !is_xi(c, "include"))
        {
            inner.push_str(&text[at..child.range().start]);
            inner.push_str(&fixed_up(text, child, &[("base", "")], false));
            at = child.range().end;
        }
    }
    inner.push_str(&text[at..last.range().end]);
    if !inner.contains("include") {
        return Ok(inner);
    }
    let prefix = include.tag_name().namespace().and_then(|ns| {
        include
            .namespaces()
            .find(|n| n.uri() == ns)
            .and_then(|n| n.name())
    });
    let open = match prefix {
        Some(p) => format!("<saranga-fallback xmlns:{p}=\"{XINCLUDE_NS}\">"),
        None => format!("<saranga-fallback xmlns=\"{XINCLUDE_NS}\">"),
    };
    let wrapped = format!("{open}{inner}</saranga-fallback>");
    let fragment_scope = Scope {
        relative_base: scope.relative_base.clone(),
        language: language(include, scope),
    };
    let expanded = expand_in(&wrapped, base, lookup, stack, &fragment_scope)?;
    Ok(expanded[open.len()..expanded.len() - "</saranga-fallback>".len()].to_owned())
}

/// The element `n` of `text` with the `xml:` attributes `set` given those
/// values (replacing any it has, as `XMLAttributesImpl.addAttribute`
/// does), and with `xmlns=""` when `undeclare_default`.
fn fixed_up(text: &str, n: Node<'_, '_>, set: &[(&str, &str)], undeclare_default: bool) -> String {
    let range = n.range();
    let name_start = range.start + 1;
    let name_end = name_start
        + text[name_start..]
            .find(|c: char| c.is_whitespace() || c == '/' || c == '>')
            .unwrap_or(0);
    let mut out = String::with_capacity(range.len() + 64);
    out.push_str(&text[range.start..name_end]);
    if undeclare_default {
        out.push_str(" xmlns=\"\"");
    }
    for (name, value) in set {
        let escaped = value
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('"', "&quot;");
        out.push_str(&format!(" xml:{name}=\"{escaped}\""));
    }
    let mut replaced: Vec<_> = n
        .attributes()
        .filter(|a| a.namespace() == Some(XML_NS) && set.iter().any(|(s, _)| *s == a.name()))
        .map(|a| a.range())
        .collect();
    replaced.sort_by_key(|r| r.start);
    let mut at = name_end;
    for r in replaced {
        out.push_str(&text[at..r.start]);
        at = r.end;
    }
    out.push_str(&text[at..range.end]);
    out
}

/// An included XML document: its `${...}` replaced, its own includes
/// expanded against its directory, and its root element (with what
/// surrounds it but the prolog) put in place of the include, fixed up as
/// a top-level included element.
fn included_document(
    content: &str,
    path: &Path,
    lookup: &dyn Fn(&str) -> Option<String>,
    stack: &mut Vec<PathBuf>,
    relative_base: String,
    parent_language: &str,
) -> Result<String> {
    let content = crate::substitute::replace_system_props(content, lookup);
    let dir = path.parent().map(Path::to_path_buf);
    let scope = Scope {
        relative_base: Some(relative_base.clone()),
        language: String::new(),
    };
    let expanded = expand_in(&content, dir.as_deref(), lookup, stack, &scope)?;
    let doc = Document::parse_with_options(&expanded, options())?;
    let root = doc.root_element();
    let own_language = root.attribute((XML_NS, "lang")).unwrap_or("").to_owned();
    let mut set = vec![("base", relative_base.as_str())];
    if !parent_language.eq_ignore_ascii_case(&own_language) {
        set.push(("lang", own_language.as_str()));
    }
    // Its names keep their namespaces: no default namespace stays none
    // inside an element that has one.
    let undeclare = root.default_namespace().is_none() && !declares_default(&expanded, root);
    Ok(fixed_up(&expanded, root, &set, undeclare))
}

/// Whether the element's start tag declares a default namespace (`xmlns`
/// or `xmlns=""`).
fn declares_default(text: &str, n: Node<'_, '_>) -> bool {
    let start = n.range().start;
    let end = text[start..].find('>').map_or(text.len(), |i| start + i);
    let tag = &text[start..end];
    tag.split(|c: char| c.is_whitespace())
        .any(|part| part.starts_with("xmlns=") || part == "xmlns")
}

/// `getRelativeBaseURI` of a document included from one whose relative
/// base is `parent`: `href` against it, as the path when the scheme and
/// authority are the parent's, else as Xerces gives it.
fn relative_base_uri(parent: &str, href: &str, path: &Path) -> String {
    if parent.is_empty() {
        return href.to_owned();
    }
    let (Some(base), Some(rel)) = (Uri::parse(parent), Uri::parse(href)) else {
        // MalformedURIException: the included document's expanded system id.
        return file_uri(path);
    };
    let uri = base.resolve(&rel);
    if base.scheme != uri.scheme {
        return href.to_owned();
    }
    if base.authority != uri.authority {
        return uri.scheme_specific_part();
    }
    let mut out = uri.path.clone();
    if uri.query.is_some() || uri.fragment.is_some() {
        if let Some(q) = &uri.query {
            out.push('?');
            out.push_str(q);
        }
        if let Some(f) = &uri.fragment {
            out.push('#');
            out.push_str(f);
        }
    }
    out
}

fn file_uri(path: &Path) -> String {
    let p = path.to_string_lossy().replace('\\', "/");
    if p.starts_with('/') {
        format!("file://{p}")
    } else {
        format!("file:///{p}")
    }
}

/// The parts of a URI reference, as Xerces's `util.URI` keeps them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Uri {
    scheme: Option<String>,
    authority: Option<String>,
    path: String,
    query: Option<String>,
    fragment: Option<String>,
}

impl Uri {
    /// A URI reference; `None` where `util.URI` would throw.
    fn parse(s: &str) -> Option<Uri> {
        let mut rest = s;
        let mut scheme = None;
        if let Some(colon) = s.find(':') {
            let before = &s[..colon];
            if colon > 0 && !before.contains(['/', '?', '#']) {
                if !before.starts_with(|c: char| c.is_ascii_alphabetic())
                    || !before
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c))
                {
                    return None;
                }
                scheme = Some(before.to_ascii_lowercase());
                rest = &s[colon + 1..];
            }
        }
        let mut authority = None;
        if let Some(after) = rest.strip_prefix("//") {
            let end = after.find(['/', '?', '#']).unwrap_or(after.len());
            authority = Some(after[..end].to_owned());
            rest = &after[end..];
        }
        let (rest, fragment) = match rest.split_once('#') {
            Some((r, f)) => (r, Some(f.to_owned())),
            None => (rest, None),
        };
        let (path, query) = match rest.split_once('?') {
            Some((p, q)) => (p, Some(q.to_owned())),
            None => (rest, None),
        };
        let ok = |c: char| c.is_ascii_alphanumeric() || "-_.!~*'();/:@&=+$,%".contains(c);
        if !path.chars().all(ok) {
            return None;
        }
        Some(Uri {
            scheme,
            authority,
            path: path.to_owned(),
            query,
            fragment,
        })
    }

    /// `absolutize`: this URI as the base of `rel`.
    fn resolve(&self, rel: &Uri) -> Uri {
        if rel.scheme.is_some() {
            return rel.clone();
        }
        if rel.authority.is_some() {
            return Uri {
                scheme: self.scheme.clone(),
                ..rel.clone()
            };
        }
        if rel.path.is_empty() {
            return Uri {
                query: rel.query.clone().or_else(|| self.query.clone()),
                fragment: rel.fragment.clone(),
                ..self.clone()
            };
        }
        let path = if rel.path.starts_with('/') {
            rel.path.clone()
        } else {
            let dir = self.path.rfind('/').map_or("", |i| &self.path[..=i]);
            remove_dots(&format!("{dir}{}", rel.path))
        };
        Uri {
            scheme: self.scheme.clone(),
            authority: self.authority.clone(),
            path,
            query: rel.query.clone(),
            fragment: rel.fragment.clone(),
        }
    }

    fn scheme_specific_part(&self) -> String {
        let mut out = String::new();
        if let Some(a) = &self.authority {
            out.push_str("//");
            out.push_str(a);
        }
        out.push_str(&self.path);
        if let Some(q) = &self.query {
            out.push('?');
            out.push_str(q);
        }
        out
    }
}

/// RFC 2396's `./` and `../` removal, as `absolutize` does it.
fn remove_dots(path: &str) -> String {
    let mut p = path.to_owned();
    while let Some(i) = p.find("/./") {
        p.replace_range(i..i + 2, "");
    }
    if p.ends_with("/.") {
        p.truncate(p.len() - 1);
    }
    if p.starts_with("./") {
        p.replace_range(..2, "");
    }
    while let Some(i) = p.find("/../") {
        let segment_start = p[..i].rfind('/').map_or(0, |j| j + 1);
        if &p[segment_start..i] == ".." || i == 0 {
            break;
        }
        p.replace_range(segment_start..i + 4, "");
    }
    if let Some(stripped) = p.strip_suffix("/..") {
        let segment_start = stripped.rfind('/').map_or(0, |j| j + 1);
        if &stripped[segment_start..] != ".." && !stripped.is_empty() {
            p.truncate(segment_start);
        }
    }
    p
}

fn resolve(href: &str, base: Option<&Path>) -> PathBuf {
    let path = crate::file_uri_path(href).unwrap_or_else(|| PathBuf::from(href));
    if path.is_absolute() {
        return path;
    }
    match base {
        Some(b) => b.join(path),
        None => std::env::current_dir().map_or(path.clone(), |d| d.join(path)),
    }
}

fn read(path: &Path, encoding: Option<&str>) -> std::result::Result<String, String> {
    if let Some(e) = encoding
        && !e.eq_ignore_ascii_case("UTF-8")
        && !e.eq_ignore_ascii_case("UTF8")
    {
        return Err(format!("the encoding {e} is not supported"));
    }
    let bytes = std::fs::read(path).map_err(|e| format!("{} ({e})", path.display()))?;
    let bytes = bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(&bytes);
    String::from_utf8(bytes.to_vec()).map_err(|e| format!("{}: {e}", path.display()))
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn none(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn includes_are_spliced_in() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.xml"),
            "<?xml version=\"1.0\"?>\n<!-- c --><address name=\"${n}\" xmlns=\"urn:activemq:core\"><xi:include xmlns:xi=\"http://www.w3.org/2001/XInclude\" href=\"b.xml\"/></address>",
        )
        .unwrap();
        std::fs::write(dir.path().join("b.xml"), "<anycast/>").unwrap();
        std::fs::write(dir.path().join("t.txt"), "a < b & c").unwrap();
        let d = dir.path().display().to_string().replace('\\', "/");
        let doc = format!(
            r#"<configuration xmlns:xi="http://www.w3.org/2001/XInclude"><x><xi:include href="{d}/a.xml"/></x><y><xi:include href="{d}/t.txt" parse="text"/></y></configuration>"#,
        );
        let lookup = |k: &str| (k == "n").then(|| "q".to_owned());
        let out = expand(&doc, None, &lookup).unwrap();
        // A nested include's base is its path when the scheme (here the
        // drive, on Windows) is its parent's.
        let path = d.split_once(':').map_or(d.as_str(), |(_, p)| p);
        assert_eq!(
            out,
            format!(
                r#"<configuration xmlns:xi="http://www.w3.org/2001/XInclude"><x><address xml:base="{d}/a.xml" name="q" xmlns="urn:activemq:core"><anycast xmlns="" xml:base="{path}/b.xml"/></address></x><y>a &lt; b &amp; c</y></configuration>"#
            )
        );
    }

    #[test]
    fn relative_bases_follow_xerces() {
        let p = Path::new("x");
        assert_eq!(
            relative_base_uri("sub/one.xml", "sub/two.xml", p),
            "sub/sub/two.xml"
        );
        assert_eq!(
            relative_base_uri("D:/tmp/xsdt/sub/one.xml", "sub/two.xml", p),
            "/tmp/xsdt/sub/sub/two.xml"
        );
        assert_eq!(
            relative_base_uri("sub/one.xml", "D:/x/y.xml", p),
            "D:/x/y.xml"
        );
        assert_eq!(relative_base_uri("a/b/c.xml", "../d.xml", p), "a/d.xml");
        assert_eq!(relative_base_uri("", "d.xml", p), "d.xml");
    }

    #[test]
    fn a_missing_file_takes_the_fallback_or_fails() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("f.xml"), "<f/>").unwrap();
        let doc = format!(
            r#"<c xmlns:xi="http://www.w3.org/2001/XInclude"><xi:include href="missing.xml"><xi:fallback><a/><xi:include href="{}/f.xml"/></xi:fallback></xi:include></c>"#,
            dir.path().display()
        );
        assert_eq!(
            expand(&doc, Some(dir.path()), &none).unwrap(),
            format!(
                "<c xmlns:xi=\"http://www.w3.org/2001/XInclude\"><a xml:base=\"\"/><f xmlns=\"\" xml:base=\"{}/f.xml\"/></c>",
                dir.path().display()
            )
        );
        let doc =
            r#"<c xmlns:xi="http://www.w3.org/2001/XInclude"><xi:include href="missing.xml"/></c>"#;
        let e = expand(doc, Some(dir.path()), &none)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains(
                "An include with href 'missing.xml'failed, and no fallback element was found"
            ),
            "{e}"
        );
    }

    #[test]
    fn relative_hrefs_and_loops() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).unwrap();
        // A nested include is relative to the file that has it.
        std::fs::write(
            sub.join("a.xml"),
            r#"<a xmlns:xi="http://www.w3.org/2001/XInclude"><xi:include href="b.xml"/></a>"#,
        )
        .unwrap();
        std::fs::write(sub.join("b.xml"), "<b/>").unwrap();
        let doc =
            r#"<c xmlns:xi="http://www.w3.org/2001/XInclude"><xi:include href="sub/a.xml"/></c>"#;
        assert_eq!(
            expand(doc, Some(dir.path()), &none).unwrap(),
            r#"<c xmlns:xi="http://www.w3.org/2001/XInclude"><a xmlns="" xml:base="sub/a.xml" xmlns:xi="http://www.w3.org/2001/XInclude"><b xmlns="" xml:base="sub/b.xml"/></a></c>"#
        );
        std::fs::write(
            sub.join("b.xml"),
            r#"<b xmlns:xi="http://www.w3.org/2001/XInclude"><xi:include href="a.xml"/></b>"#,
        )
        .unwrap();
        assert!(expand(doc, Some(dir.path()), &none).is_err());
    }
}
