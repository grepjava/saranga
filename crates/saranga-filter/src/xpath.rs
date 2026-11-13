//! `XPathExpression` with `JAXPXPathEvaluator`: an `XPATH` selector is an
//! XPath 1.0 expression, compiled when the selector is parsed and evaluated
//! as a boolean against the message body read as text
//! ([`Filterable::string_body`](crate::Filterable::string_body), Artemis's
//! `getBodyAs(String.class)`). The body is parsed as `XPathExpression`'s
//! document builder parses it: namespace aware, without comments, and with no
//! DOCTYPE (`XmlProvider` disallows one). A body that is not text, or is not
//! such a document, matches nothing.
//!
//! A document nested deeper than [`MAX_DEPTH`] elements matches nothing
//! either, and is not parsed: the parser's work grows with the square of the
//! depth, evaluating an expression walks the document recursively, and the
//! body comes from whoever sent the message.

use sxd_document::dom::{ChildOfElement, ChildOfRoot, Element};

/// The deepest element nesting a body is evaluated at.
pub const MAX_DEPTH: usize = 1024;

/// `JAXPXPathEvaluator`'s `xpath.compile`: an expression that does not
/// compile makes the selector invalid.
pub(crate) fn compile(expression: &str) -> Result<sxd_xpath::XPath, String> {
    match sxd_xpath::Factory::new().build(expression) {
        Ok(Some(xpath)) => Ok(xpath),
        Ok(None) => Err("an empty XPath expression".to_owned()),
        Err(e) => Err(format!("invalid XPath expression {expression:?}: {e}")),
    }
}

/// `JAXPXPathEvaluator.evaluate`: the expression's boolean value over the
/// body, false when there is no body or it does not parse.
pub(crate) fn matches(expression: &str, body: Option<&str>) -> bool {
    let Some(body) = body else {
        return false;
    };
    if has_doctype(body) || !within_depth(body) {
        return false;
    }
    let Ok(package) = sxd_document::parser::parse(body) else {
        return false;
    };
    let document = package.as_document();
    let root = document.root();
    let mut elements = Vec::new();
    for child in root.children() {
        match child {
            ChildOfRoot::Comment(c) => c.remove_from_parent(),
            ChildOfRoot::Element(e) => elements.push((e, 1)),
            ChildOfRoot::ProcessingInstruction(_) => {}
        }
    }
    if !drop_comments_within_depth(elements) {
        return false;
    }
    let Ok(xpath) = compile(expression) else {
        return false;
    };
    xpath
        .evaluate(&sxd_xpath::Context::new(), root)
        .is_ok_and(|v| v.boolean())
}

/// Removes the comments under `elements` (`IGNORE_COMMENTS`), walking the
/// tree without recursion; false if it is nested deeper than [`MAX_DEPTH`].
fn drop_comments_within_depth(mut elements: Vec<(Element<'_>, usize)>) -> bool {
    while let Some((element, depth)) = elements.pop() {
        if depth > MAX_DEPTH {
            return false;
        }
        for child in element.children() {
            match child {
                ChildOfElement::Comment(c) => c.remove_from_parent(),
                ChildOfElement::Element(e) => elements.push((e, depth + 1)),
                _ => {}
            }
        }
    }
    true
}

/// Whether the body's elements nest no deeper than [`MAX_DEPTH`], from a
/// scan of its markup before it is parsed. Markup the scan cannot follow is
/// left to the parser to refuse.
fn within_depth(body: &str) -> bool {
    let bytes = body.as_bytes();
    let mut depth = 0usize;
    let mut at = 0;
    while let Some(lt) = memchr(b'<', &bytes[at..]) {
        let tag = &body[at + lt..];
        let skip = |end: &str| tag.find(end).map(|e| at + lt + e + end.len());
        let next = if tag.starts_with("<!--") {
            skip("-->")
        } else if tag.starts_with("<![CDATA[") {
            skip("]]>")
        } else if tag.starts_with("<?") {
            skip("?>")
        } else if tag.starts_with("<!") {
            skip(">")
        } else if tag.starts_with("</") {
            depth = depth.saturating_sub(1);
            skip(">")
        } else {
            // A start tag: its end is the first `>` outside a quoted
            // attribute value.
            let mut quote = None;
            let mut end = None;
            for (i, c) in tag.bytes().enumerate().skip(1) {
                match (quote, c) {
                    (Some(q), c) if c == q => quote = None,
                    (Some(_), _) => {}
                    (None, b'"' | b'\'') => quote = Some(c),
                    (None, b'>') => {
                        end = Some(i);
                        break;
                    }
                    _ => {}
                }
            }
            end.map(|e| {
                if tag.as_bytes()[e - 1] != b'/' {
                    depth += 1;
                }
                at + lt + e + 1
            })
        };
        if depth > MAX_DEPTH {
            return false;
        }
        match next {
            Some(n) => at = n,
            None => return true,
        }
    }
    true
}

fn memchr(needle: u8, haystack: &[u8]) -> Option<usize> {
    haystack.iter().position(|&b| b == needle)
}

/// Whether the prolog, before the root element, declares a DOCTYPE
/// (`disallow-doctype-decl`: such a document does not parse).
fn has_doctype(body: &str) -> bool {
    let mut rest = body;
    while let Some(at) = rest.find('<') {
        let tag = &rest[at..];
        if tag.starts_with("<!DOCTYPE") {
            return true;
        }
        if tag.starts_with("<?") || tag.starts_with("<!--") {
            // A processing instruction, the XML declaration, or a comment,
            // still in the prolog.
            let end = if tag.starts_with("<?") { "?>" } else { "-->" };
            match tag.find(end) {
                Some(e) => rest = &tag[e + end.len()..],
                None => return false,
            }
        } else {
            // The root element: the prolog has ended.
            return false;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_expression_is_a_boolean_over_the_body() {
        let body = Some("<root><order id='7'/></root>");
        assert!(matches("//order", body));
        assert!(matches("/root/order/@id = 7", body));
        assert!(!matches("//invoice", body));
        assert!(!matches("//order", None));
        assert!(!matches("//order", Some("not xml")));
    }

    #[test]
    fn comments_are_not_in_the_document() {
        let body = Some("<!-- a --><root><!-- b --><x/></root>");
        assert!(matches("//x", body));
        assert!(!matches("//comment()", body));
    }

    #[test]
    fn a_doctype_is_refused() {
        assert!(!matches(
            "//x",
            Some("<?xml version='1.0'?><!DOCTYPE root [<!ENTITY e 'v'>]><root><x/></root>")
        ));
        // Not in the prolog: text of the document, not a declaration.
        assert!(matches(
            "//x",
            Some("<root><x/><![CDATA[<!DOCTYPE no>]]></root>")
        ));
    }

    #[test]
    fn a_document_nested_too_deeply_matches_nothing() {
        let nested = |n: usize| "<a>".repeat(n) + &"</a>".repeat(n);
        assert!(matches("string(/) = ''", Some(&nested(MAX_DEPTH))));
        assert!(!matches("string(/) = ''", Some(&nested(MAX_DEPTH + 1))));
        assert!(!matches("//a", Some(&nested(200_000))));
    }

    #[test]
    fn an_expression_that_does_not_compile_is_refused() {
        assert!(compile("//order").is_ok());
        assert!(compile("//order[").is_err());
        assert!(compile("").is_err());
    }
}
