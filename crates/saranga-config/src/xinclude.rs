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

use std::path::{Path, PathBuf};

use roxmltree::{Document, Node, ParsingOptions};

use crate::{ConfigError, Result};

const XINCLUDE_NS: &str = "http://www.w3.org/2001/XInclude";

/// `text` with its includes replaced. `base` is the directory relative
/// hrefs are resolved against: `None` for the working directory.
pub(crate) fn expand(
    text: &str,
    base: Option<&Path>,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<String> {
    expand_in(text, base, lookup, &mut Vec::new())
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
        out.push_str(&include_one(text, include, base, lookup, stack)?);
        at = range.end;
    }
    out.push_str(&text[at..]);
    Ok(out)
}

/// What one `xi:include` is replaced by.
fn include_one(
    text: &str,
    include: Node<'_, '_>,
    base: Option<&Path>,
    lookup: &dyn Fn(&str) -> Option<String>,
    stack: &mut Vec<PathBuf>,
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
            stack.push(canonical);
            let included = included_document(&content, &path, lookup, stack);
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
            let inner = match (fallback.first_child(), fallback.last_child()) {
                (Some(first), Some(last)) => &text[first.range().start..last.range().end],
                _ => "",
            };
            // The fallback's own includes, wrapped so that it parses with
            // the prefixes the document declares.
            expand_fragment(inner, include, base, lookup, stack)
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
/// declaring the XInclude namespace as the document does.
fn expand_fragment(
    inner: &str,
    include: Node<'_, '_>,
    base: Option<&Path>,
    lookup: &dyn Fn(&str) -> Option<String>,
    stack: &mut Vec<PathBuf>,
) -> Result<String> {
    if !inner.contains("include") {
        return Ok(inner.to_owned());
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
    let expanded = expand_in(&wrapped, base, lookup, stack)?;
    Ok(expanded[open.len()..expanded.len() - "</saranga-fallback>".len()].to_owned())
}

/// An included XML document: its `${...}` replaced, its own includes
/// expanded against its directory, and its root element (with what
/// surrounds it but the prolog) put in place of the include.
fn included_document(
    content: &str,
    path: &Path,
    lookup: &dyn Fn(&str) -> Option<String>,
    stack: &mut Vec<PathBuf>,
) -> Result<String> {
    let content = crate::substitute::replace_system_props(content, lookup);
    let dir = path.parent().map(Path::to_path_buf);
    let expanded = expand_in(&content, dir.as_deref(), lookup, stack)?;
    let doc = Document::parse_with_options(&expanded, options())?;
    let root = doc.root_element().range();
    Ok(expanded[root].to_owned())
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
        let doc = format!(
            r#"<configuration xmlns:xi="http://www.w3.org/2001/XInclude"><x><xi:include href="{0}/a.xml"/></x><y><xi:include href="{0}/t.txt" parse="text"/></y></configuration>"#,
            dir.path().display()
        );
        let lookup = |k: &str| (k == "n").then(|| "q".to_owned());
        let out = expand(&doc, None, &lookup).unwrap();
        assert_eq!(
            out,
            r#"<configuration xmlns:xi="http://www.w3.org/2001/XInclude"><x><address name="q" xmlns="urn:activemq:core"><anycast/></address></x><y>a &lt; b &amp; c</y></configuration>"#
        );
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
            "<c xmlns:xi=\"http://www.w3.org/2001/XInclude\"><a/><f/></c>"
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
            r#"<c xmlns:xi="http://www.w3.org/2001/XInclude"><a xmlns:xi="http://www.w3.org/2001/XInclude"><b/></a></c>"#
        );
        std::fs::write(
            sub.join("b.xml"),
            r#"<b xmlns:xi="http://www.w3.org/2001/XInclude"><xi:include href="a.xml"/></b>"#,
        )
        .unwrap();
        assert!(expand(doc, Some(dir.path()), &none).is_err());
    }
}
