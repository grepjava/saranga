//! `${name}` and `${name:default}` substitution. Ported from
//! `XMLUtil.replaceSystemPropsInString`.
//!
//! Artemis resolves a name against Java system properties first and then
//! against environment variables. Rust has no system properties, so
//! [`replace_system_props_from_env`] (what `BrokerConfig::parse` uses)
//! consults environment variables only. A caller that wants a `-D`-style
//! override layer passes its own lookup to [`replace_system_props`].

/// Bound on the number of substitutions. Java loops forever when a value
/// expands to itself (directly or through a cycle); Saranga stops instead.
const MAX_SUBSTITUTIONS: usize = 10_000;

/// Replaces every `${...}` placeholder in `input`, exactly as Artemis does:
///
/// - The placeholder runs from `${` to the next `}`. A `${` with no `}` after
///   it stops processing, leaving that and any later placeholders as they are.
/// - The name and the default (after the first `:`) are trimmed.
/// - An unresolved name without a default becomes the empty string.
/// - Every occurrence of the same placeholder text is replaced at once, and
///   a replacement that itself contains `${` is expanded again.
///
/// Artemis applies this to the raw file text before XML parsing, so it also
/// reaches comments and attribute values.
pub fn replace_system_props(input: &str, lookup: impl Fn(&str) -> Option<String>) -> String {
    let mut text = input.to_owned();
    for _ in 0..MAX_SUBSTITUTIONS {
        let Some(start) = text.find("${") else {
            break;
        };
        let Some(close) = text[start..].find('}') else {
            break;
        };
        let placeholder = text[start..start + close + 1].to_owned();
        let inner = crate::java_trim(&placeholder[2..placeholder.len() - 1]);
        let (name, default) = match inner.split_once(':') {
            Some((name, default)) => (crate::java_trim(name), crate::java_trim(default)),
            None => (inner, ""),
        };
        let value = lookup(name).unwrap_or_else(|| default.to_owned());
        text = text.replace(&placeholder, &value);
        if value.contains(&placeholder) {
            break;
        }
    }
    text
}

/// `XMLUtil.replaceSystemPropsInXml`: once the document is parsed, with
/// its includes, `${...}` is replaced in every element's attribute values
/// and text, the value taken as it is: what it holds is escaped in the
/// text returned, so `a&b<c` stays a value and adds no markup. Artemis
/// runs this after [`replace_system_props`] on the raw text of the file,
/// which an included file does not get; an entity-escaped `&#36;{X}` is
/// replaced here too. (Artemis replaces in comments as well, which carry
/// no configuration.)
pub fn replace_in_document(
    text: &str,
    lookup: impl Fn(&str) -> Option<String>,
) -> std::result::Result<String, roxmltree::Error> {
    if !text.contains("${") && !text.contains("&#36;") && !text.contains("&#x24;") {
        return Ok(text.to_owned());
    }
    let doc = roxmltree::Document::parse_with_options(
        text,
        roxmltree::ParsingOptions {
            allow_dtd: true,
            ..roxmltree::ParsingOptions::default()
        },
    )?;
    let mut edits: Vec<(std::ops::Range<usize>, String)> = Vec::new();
    for n in doc.descendants() {
        if n.is_element() {
            for a in n.attributes() {
                let v = a.value();
                if v.contains("${") {
                    let r = replace_system_props(v, &lookup);
                    if r != v {
                        edits.push((a.range_value(), escape(&r, true)));
                    }
                }
            }
        } else if n.is_text()
            && let Some(v) = n.text()
            && v.contains("${")
        {
            let r = replace_system_props(v, &lookup);
            if r != v {
                edits.push((n.range(), escape(&r, false)));
            }
        }
    }
    edits.sort_by_key(|(r, _)| r.start);
    let mut out = String::with_capacity(text.len());
    let mut at = 0;
    for (range, value) in edits {
        out.push_str(&text[at..range.start]);
        out.push_str(&value);
        at = range.end;
    }
    out.push_str(&text[at..]);
    Ok(out)
}

/// A value as XML text, or as an attribute value (quoted either way, its
/// white space kept as character references).
fn escape(v: &str, attribute: bool) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' if attribute => out.push_str("&quot;"),
            '\'' if attribute => out.push_str("&apos;"),
            '\n' if attribute => out.push_str("&#10;"),
            '\r' => out.push_str("&#13;"),
            '\t' if attribute => out.push_str("&#9;"),
            c => out.push(c),
        }
    }
    out
}

/// [`replace_system_props`] with environment variables as the only source.
pub fn replace_system_props_from_env(input: &str) -> String {
    replace_system_props(input, env_lookup)
}

pub(crate) fn env_lookup(name: &str) -> Option<String> {
    if name.is_empty() || name.contains(['=', '\0']) {
        return None;
    }
    std::env::var(name).ok()
}
