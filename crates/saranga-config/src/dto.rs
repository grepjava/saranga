//! `bootstrap.xml` and `management.xml` read as Artemis's `XmlUtil.decode`
//! reads them: a StAX reader whose attribute values go through
//! `PropertiesFilter`, unmarshalled by JAXB with `activemq.xsd` as its
//! schema, the first error ending it, then cast to the class asked for.

use roxmltree::{Document, ParsingOptions};

use crate::schema::{Xsd, validate_stream};

/// The namespace of `activemq.xsd`.
pub const DTO_NS: &str = "http://activemq.apache.org/schema";

const PACKAGE: &str = "org.apache.activemq.artemis.dto";

/// The class a file is decoded as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dto {
    /// `BrokerDTO`: `bootstrap.xml`.
    Broker,
    /// `ManagementContextDTO`: `management.xml`.
    ManagementContext,
}

impl Dto {
    fn class(self) -> &'static str {
        match self {
            Dto::Broker => "BrokerDTO",
            Dto::ManagementContext => "ManagementContextDTO",
        }
    }
}

/// The class JAXB unmarshals a root element of `activemq.xsd` as (its
/// `@XmlRootElement`).
fn root_class(local: &str) -> Option<&'static str> {
    Some(match local {
        "access" => "AccessDTO",
        "allowlist" => "AllowListDTO",
        "app" => "AppDTO",
        "authorisation" => "AuthorisationDTO",
        "binding" => "BindingDTO",
        "broker" => "BrokerDTO",
        "component" => "ComponentDTO",
        "connector" => "JMXConnectorDTO",
        "default-access" => "DefaultAccessDTO",
        "entry" => "EntryDTO",
        "jaas-security" => "JaasSecurityDTO",
        "management-context" => "ManagementContextDTO",
        "match" => "MatchDTO",
        "property" => "PropertyDTO",
        "request-log" => "RequestLogDTO",
        "role-access" => "RoleAccessDTO",
        "security-manager" => "SecurityManagerDTO",
        "server" => "ServerDTO",
        "web" => "WebServerDTO",
        "whitelist" => "WhiteListDTO",
        _ => return None,
    })
}

/// What `XmlUtil.decode` threw, as `printStackTrace` prints it without
/// the frames: the exception, then its cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    /// `getMessage`: none for an `UnmarshalException` with only a linked
    /// exception.
    pub message: Option<String>,
    /// The exception and its cause.
    pub trace: String,
    /// A `RuntimeException`, which the CLI reports with its usage, rather
    /// than the checked `UnmarshalException`.
    pub runtime: bool,
}

impl DecodeError {
    /// An `UnmarshalException` linked to `linked` (its `toString`).
    fn unmarshal(linked: String) -> Self {
        Self {
            message: None,
            trace: format!(
                "javax.xml.bind.UnmarshalException\n - with linked exception:\n[{linked}]\nCaused by: {linked}"
            ),
            runtime: false,
        }
    }

    fn runtime(class: &str, message: String) -> Self {
        Self {
            trace: format!("{class}: {message}"),
            message: Some(message),
            runtime: true,
        }
    }
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.trace)
    }
}

impl std::error::Error for DecodeError {}

/// `PropertiesFilter.filter`: each `${name}` with a value replaced, but by
/// `Matcher.replaceFirst`, which replaces the first `${...}` of the value,
/// so a placeholder without a value before one with a value takes that
/// value. A value a replacement made shorter than where the search was
/// throws, as `Matcher.find` does, with its message.
pub fn try_filter(
    value: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<String, &'static str> {
    let mut s = value.to_owned();
    let mut start = 0;
    // Java loops for ever on a value that brings its own placeholder back;
    // this stops after as many replacements as it could need otherwise.
    let mut budget = 10_000;
    loop {
        if start > s.len() {
            return Err("Illegal start index");
        }
        let Some((_, end, name)) = find_placeholder(&s, start) else {
            return Ok(s);
        };
        match lookup(&name) {
            Some(property) if budget > 0 => {
                budget -= 1;
                let (first, first_end, _) = find_placeholder(&s, 0).expect("a placeholder");
                s.replace_range(first..first_end, &property);
            }
            Some(_) => return Ok(s),
            None => start = end,
        }
    }
}

/// [`try_filter`], a value it throws on left as it is.
pub fn filter(value: &str, lookup: &dyn Fn(&str) -> Option<String>) -> String {
    try_filter(value, lookup).unwrap_or_else(|_| value.to_owned())
}

/// `\$\{([^\}]+)\}` found from `start`: where it is and the name.
fn find_placeholder(s: &str, start: usize) -> Option<(usize, usize, String)> {
    let bytes = s.as_bytes();
    let mut i = start;
    while i + 1 < bytes.len() {
        if bytes[i] == b'$' && bytes[i + 1] == b'{' {
            let name_start = i + 2;
            if let Some(len) = s[name_start..].find('}')
                && len > 0
            {
                let end = name_start + len + 1;
                return Some((i, end, s[name_start..name_start + len].to_owned()));
            }
        }
        i += 1;
    }
    None
}

/// `XmlUtil.decode`: whether the file's `bytes` unmarshal as `dto`, with
/// `lookup` the properties its attributes' `${...}` are replaced from. The
/// text as the reader decoded it, up to the end of the root element, when
/// they do.
pub fn decode(
    dto: Dto,
    bytes: &[u8],
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<String, DecodeError> {
    let options = || ParsingOptions {
        allow_dtd: true,
        ..ParsingOptions::default()
    };
    let base = std::env::current_dir().unwrap_or_default();
    let xml = match crate::xerces::check_stax(bytes, base) {
        Ok(scanned) => scanned.text,
        Err(failure) => return Err(stream_failure(&failure, lookup)),
    };
    let doc = match Document::parse_with_options(&xml, options()) {
        Ok(doc) => doc,
        // Xerces read it; roxmltree does not (a name it does not take).
        Err(e) => {
            let pos = e.pos();
            return Err(DecodeError::unmarshal(format!(
                "javax.xml.stream.XMLStreamException: ParseError at [row,col]:[{},{}]\nMessage: {e}",
                pos.row, pos.col
            )));
        }
    };
    let root = doc.root_element();
    // The reader filters an element's attributes as the connector reads
    // them, before the validator sees the element: a filter that throws
    // there ends the unmarshalling unless an error came before.
    let thrown = root.descendants().filter(|n| n.is_element()).find_map(|n| {
        n.attributes()
            .find_map(|a| try_filter(a.value(), lookup).err())
            .map(|e| (n.range().start, e))
    });
    let attribute_filter = |v: &str| filter(v, lookup);
    let invalid = validate_stream(Xsd::Dto, &xml, root, &attribute_filter).err();
    match (invalid, thrown) {
        (Some(f), thrown)
            if thrown.is_none_or(|(element, _)| f.offset().is_some_and(|o| o <= element)) =>
        {
            let (line, column) = f.location(&xml);
            return Err(DecodeError::unmarshal(format!(
                "org.xml.sax.SAXParseException; lineNumber: {line}; columnNumber: {column}; {}",
                f.message()
            )));
        }
        (_, Some((_, message))) => {
            return Err(DecodeError::runtime(
                "java.lang.IndexOutOfBoundsException",
                message.to_owned(),
            ));
        }
        _ => {}
    }
    // The root is one of the schema's elements; `Class.cast` then refuses
    // another class than the one asked for.
    let class = root_class(root.tag_name().name()).unwrap_or("Object");
    if class != dto.class() {
        return Err(DecodeError::runtime(
            "java.lang.ClassCastException",
            format!("Cannot cast {PACKAGE}.{class} to {PACKAGE}.{}", dto.class()),
        ));
    }
    Ok(xml)
}

/// The exception a reader that stopped on `failure` ends the decoding with:
/// a validation error on what it delivered before, else its own.
fn stream_failure(
    failure: &crate::xerces::Failure,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> DecodeError {
    use crate::xerces::Failure;
    let options = || ParsingOptions {
        allow_dtd: true,
        ..ParsingOptions::default()
    };
    let (message, line, column, at_init, printed) = match failure {
        Failure::Parse(p) => (
            p.message.clone(),
            p.line,
            p.column,
            p.at_init,
            p.printed.clone(),
        ),
        Failure::Io {
            message,
            line,
            column,
            ..
        }
        | Failure::Xni {
            message,
            line,
            column,
        } => (message.clone(), *line, *column, false, Vec::new()),
        Failure::Runtime { class, message } => {
            return DecodeError::runtime(class, message.clone());
        }
    };
    let stream = format!("ParseError at [row,col]:[{line},{column}]\nMessage: {message}");
    if at_init {
        // createXMLStreamReader threw it, before JAXB.
        print_errors(&printed);
        return DecodeError {
            trace: format!("javax.xml.stream.XMLStreamException: {stream}"),
            message: Some(stream),
            runtime: false,
        };
    }
    // The validator has seen the events the reader delivered: it stops at
    // an error there first.
    if let Some((text, cut)) = failure.delivered()
        && let Some((cut, prefix)) = readable_prefix(&text, cut)
        && let Ok(doc) = Document::parse_with_options(&prefix, options())
    {
        let attribute_filter = |v: &str| filter(v, lookup);
        if let Err(f) = validate_stream(Xsd::Dto, &prefix, doc.root_element(), &attribute_filter)
            && f.offset().is_some_and(|o| o <= cut)
        {
            let (line, column) = f.location(&prefix);
            return DecodeError::unmarshal(format!(
                "org.xml.sax.SAXParseException; lineNumber: {line}; columnNumber: {column}; {}",
                f.message()
            ));
        }
    }
    print_errors(&printed);
    DecodeError::unmarshal(format!("javax.xml.stream.XMLStreamException: {stream}"))
}

impl DecodeError {
    /// What `DocumentBuilder.parse` throws for a document the DOM refuses,
    /// as `XMLUtil` reads `broker.xml`: the `SAXParseException` (or the
    /// `IOException`, or the `SAXException` without a location).
    pub fn from_dom(failure: &crate::xerces::Failure) -> Self {
        use crate::xerces::Failure;
        match failure {
            Failure::Parse(p) => {
                // SAXParseException.toString.
                let mut trace = "org.xml.sax.SAXParseException".to_owned();
                if let Some(id) = &p.system_id {
                    trace.push_str(&format!("; systemId: {id}"));
                }
                if p.line != -1 {
                    trace.push_str(&format!("; lineNumber: {}", p.line));
                }
                if p.column != -1 {
                    trace.push_str(&format!("; columnNumber: {}", p.column));
                }
                trace.push_str(&format!("; {}", p.message));
                Self {
                    message: Some(p.message.clone()),
                    trace,
                    runtime: false,
                }
            }
            Failure::Io { class, message, .. } => Self {
                message: Some(message.clone()),
                trace: format!("{class}: {message}"),
                runtime: false,
            },
            Failure::Xni { message, .. } => Self {
                message: Some(message.clone()),
                trace: format!("org.xml.sax.SAXException: {message}"),
                runtime: false,
            },
            Failure::Runtime { class, message } => {
                let trace = if message.is_empty() {
                    class.clone()
                } else {
                    format!("{class}: {message}")
                };
                Self {
                    message: (!message.is_empty()).then(|| message.clone()),
                    trace,
                    runtime: true,
                }
            }
        }
    }
}

/// What Xerces's default error handler prints to the standard error.
pub(crate) fn print_errors(printed: &[String]) {
    for line in printed {
        eprintln!("{line}");
    }
}

/// Whether a decoded `management.xml`'s `<authorisation>` has a
/// `<whitelist>` and an `<allowlist>`.
pub fn management_lists(xml: &str) -> (bool, bool) {
    let options = ParsingOptions {
        allow_dtd: true,
        ..ParsingOptions::default()
    };
    let Ok(doc) = Document::parse_with_options(xml, options) else {
        return (false, false);
    };
    let has = |name: &str| {
        doc.root_element()
            .children()
            .filter(|c| c.is_element() && c.tag_name().name() == "authorisation")
            .flat_map(|a| a.children())
            .any(|c| c.is_element() && c.tag_name().name() == name)
    };
    (has("whitelist"), has("allowlist"))
}

/// What a streaming reader reads of `text` before failing at `at`: the
/// text up to the markup `at` is in, and the elements open there closed
/// after it, so that only what comes after the cut is made up. `None`
/// before the root element.
fn readable_prefix(text: &str, at: usize) -> Option<(usize, String)> {
    #[derive(Clone, Copy, PartialEq)]
    enum At {
        Text,
        Tag,
        Value(u8),
        Other(&'static [u8]),
    }
    let bytes = text.as_bytes();
    let mut open: Vec<&str> = Vec::new();
    let mut seen_root = false;
    let mut state = At::Text;
    // Where the markup being read began.
    let mut markup = 0;
    let mut i = 0;
    while i < at.min(bytes.len()) {
        let rest = &bytes[i..];
        match state {
            At::Text => {
                markup = i;
                if rest.starts_with(b"<!--") {
                    state = At::Other(b"-->");
                } else if rest.starts_with(b"<![CDATA[") {
                    state = At::Other(b"]]>");
                } else if rest.starts_with(b"<?") {
                    state = At::Other(b"?>");
                } else if rest.starts_with(b"<!") {
                    state = At::Other(b">");
                } else if rest.starts_with(b"<") {
                    state = At::Tag;
                }
                i += 1;
            }
            At::Tag => {
                match bytes[i] {
                    q @ (b'"' | b'\'') => state = At::Value(q),
                    b'>' => {
                        let tag = &text[markup + 1..i];
                        if let Some(name) = tag.strip_prefix('/') {
                            let name = name.trim_end();
                            if open.last() == Some(&name) {
                                open.pop();
                            }
                        } else if !tag.ends_with('/') {
                            let name = tag
                                .split(|c: char| c.is_whitespace() || c == '/')
                                .next()
                                .unwrap_or_default();
                            open.push(name);
                            seen_root = true;
                        } else {
                            seen_root = true;
                        }
                        state = At::Text;
                    }
                    _ => {}
                }
                i += 1;
            }
            At::Value(q) => {
                if bytes[i] == q {
                    state = At::Tag;
                }
                i += 1;
            }
            At::Other(close) => {
                if rest.starts_with(close) {
                    i += close.len();
                    state = At::Text;
                } else {
                    i += 1;
                }
            }
        }
    }
    let cut = if state == At::Text {
        at.min(bytes.len())
    } else {
        markup
    };
    if !seen_root {
        return None;
    }
    let mut prefix = text[..cut].to_owned();
    for name in open.iter().rev() {
        prefix.push_str("</");
        prefix.push_str(name);
        prefix.push('>');
    }
    Some((cut, prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(name: &str) -> Option<String> {
        match name {
            "a" => Some("A".to_owned()),
            "b" => Some("B".to_owned()),
            "self" => Some("${self}".to_owned()),
            _ => None,
        }
    }

    #[test]
    fn the_filter_replaces_as_replace_first_does() {
        assert_eq!(filter("x${a}y${b}", &props), "xAyB");
        assert_eq!(filter("${nope}", &props), "${nope}");
        // The value of the second goes where the first, unset, was.
        assert_eq!(filter("${nope}-x${a}", &props), "A-x${a}");
        assert_eq!(filter("${}${a}", &props), "${}A");
        assert_eq!(filter("${a", &props), "${a");
        // The search goes on past the end of what is left.
        assert_eq!(
            try_filter("${nope}-${a}", &props),
            Err("Illegal start index")
        );
        // Artemis loops for ever on this one.
        assert_eq!(filter("${self}", &props), "${self}");
    }
}
