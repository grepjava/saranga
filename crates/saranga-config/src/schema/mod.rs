//! `broker.xml` validated against Artemis's schemas, as Artemis validates
//! it: `FileDeploymentManager.readConfiguration` validates `<core>` against
//! `artemis-configuration.xsd` and `<jms>` against `artemis-jms.xsd` and
//! fails on the first error (`XMLUtil.validate`), and
//! `FileConfigurationParser.parseMainConfig`, which reloads the
//! configuration, validates the whole document against `artemis-server.xsd`
//! and only logs it.
//!
//! The validator is a port of the JDK's Xerces (`XMLSchemaValidator`, its
//! content models and datatypes) for the part of XML Schema the schemas
//! use, with its messages, so that an invalid configuration is reported as
//! Artemis reports it.

mod cm;
mod datatype;
mod model;
mod ncname;
mod uri;
mod validate;

use std::sync::OnceLock;

use roxmltree::Node;

use model::Schema;

const CONFIGURATION: (&str, &str) = (
    "artemis-configuration.xsd",
    include_str!("../../schema/artemis-configuration.xsd"),
);
const JMS: (&str, &str) = (
    "artemis-jms.xsd",
    include_str!("../../schema/artemis-jms.xsd"),
);
const SERVER: (&str, &str) = (
    "artemis-server.xsd",
    include_str!("../../schema/artemis-server.xsd"),
);
const XML: (&str, &str) = ("xml.xsd", include_str!("../../schema/xml.xsd"));
const DTO: (&str, &str) = ("activemq.xsd", include_str!("../../schema/activemq.xsd"));

/// The schemas in the `schema/` of the artemis-server jar. An import is
/// resolved next to the schema importing it, so `artemis-server.xsd`'s
/// import of `artemis-jms.xsd`, which is in the artemis-jms-server jar, is
/// not found, and Xerces goes on without it.
const SERVER_JAR: &[(&str, &str)] = &[CONFIGURATION, SERVER, XML];
const JMS_JAR: &[(&str, &str)] = &[JMS];
/// `org.apache.activemq/dto/activemq.xsd` of the artemis-dto jar.
const DTO_JAR: &[(&str, &str)] = &[DTO];

/// The schema an element is validated against.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Xsd {
    /// `schema/artemis-configuration.xsd`, for `<core>`.
    Configuration,
    /// `schema/artemis-jms.xsd`, for `<jms>`.
    Jms,
    /// `schema/artemis-server.xsd`, for the whole document.
    Server,
    /// `activemq.xsd`, for `bootstrap.xml` and `management.xml`.
    Dto,
}

impl Xsd {
    fn schema(self) -> &'static Schema {
        static CONFIGURATION_SCHEMA: OnceLock<Schema> = OnceLock::new();
        static JMS_SCHEMA: OnceLock<Schema> = OnceLock::new();
        static SERVER_SCHEMA: OnceLock<Schema> = OnceLock::new();
        static DTO_SCHEMA: OnceLock<Schema> = OnceLock::new();
        let (cell, files, root) = match self {
            Xsd::Configuration => (
                &CONFIGURATION_SCHEMA,
                SERVER_JAR,
                "artemis-configuration.xsd",
            ),
            Xsd::Jms => (&JMS_SCHEMA, JMS_JAR, "artemis-jms.xsd"),
            Xsd::Server => (&SERVER_SCHEMA, SERVER_JAR, "artemis-server.xsd"),
            Xsd::Dto => (&DTO_SCHEMA, DTO_JAR, "activemq.xsd"),
        };
        cell.get_or_init(|| Schema::load(files, root))
    }
}

/// An error a validation stopped at: a key of Xerces's
/// `XMLSchemaMessages` and its arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Fail {
    key: &'static str,
    args: Vec<String>,
    /// Where in the text a StAX reader is when the error is reported.
    offset: Option<usize>,
}

impl Fail {
    /// Where in the text the error was reported.
    pub(crate) fn offset(&self) -> Option<usize> {
        self.offset
    }

    /// The error, reported at `offset` unless it already has a place.
    pub(super) fn at(mut self, offset: usize) -> Self {
        self.offset.get_or_insert(offset);
        self
    }

    /// The line and column a StAX reader gives where the error was
    /// reported: see [`stax_location`].
    pub(crate) fn location(&self, text: &str) -> (i64, i64) {
        stax_location(text, self.offset.unwrap_or(text.len()))
    }

    /// The message, as the `SAXParseException` has it.
    pub(crate) fn message(&self) -> String {
        let template = template(self.key);
        let mut out = String::with_capacity(template.len() + 32);
        let mut rest = template;
        // `{n}` is an argument; other braces, as in `{nillable}`, are text.
        while let Some(i) = rest.find('{') {
            out.push_str(&rest[..i]);
            let close = rest[i..].find('}').map(|j| i + j);
            match close.and_then(|c| rest[i + 1..c].parse::<usize>().ok().map(|n| (c, n))) {
                Some((c, n)) => {
                    out.push_str(self.args.get(n).map_or("null", String::as_str));
                    rest = &rest[c + 1..];
                }
                None => {
                    out.push('{');
                    rest = &rest[i + 1..];
                }
            }
        }
        out.push_str(rest);
        out
    }
}

/// Where the JDK's StAX reader (`XMLEntityScanner`) says it is at `offset`
/// of `text`: lines end at `\r\n`, `\r` or `\n`, columns count UTF-16
/// units from 1. Newlines in text, attribute values, comments, processing
/// instructions and CDATA go through `normalizeNewlines`, which counts a
/// `\r` without a `\n` twice, so each takes one from the column of the
/// line its run of newlines starts; those between the markup are counted
/// right (`skipSpaces`).
pub(crate) fn stax_location(text: &str, offset: usize) -> (i64, i64) {
    #[derive(Clone, Copy, PartialEq)]
    enum At {
        /// Before or after the root element: white space and markup.
        Outside,
        Content,
        /// Inside a tag, outside its attribute values.
        Tag,
        Value(u8),
        Comment,
        Pi,
        Cdata,
        Doctype,
        /// The XML declaration, read as a tag's attributes are.
        Declaration,
    }
    let end = offset.min(text.len());
    // A byte order mark is not read as a character.
    let (text, end) = match text.strip_prefix('\u{feff}') {
        Some(t) => (t, end.saturating_sub('\u{feff}'.len_utf8())),
        None => (text, end),
    };
    let bytes = text.as_bytes();
    // A document starting with `<?xml` that is not its declaration (a PI
    // whose target begins with xml): XMLDeclDriver puts the reader back at
    // the start without taking back the 5 columns it counted.
    let column_start = if text.starts_with("<?xml")
        && !matches!(bytes.get(5), Some(b' ' | b'\t' | b'\r' | b'\n'))
    {
        6
    } else {
        1
    };
    let (mut line, mut column) = (1i64, column_start);
    let mut at = At::Outside;
    let mut depth = 0usize;
    // Whether the tag being read closes an element or is empty.
    let (mut closing, mut empty) = (false, false);
    // The doctype's `[`...`]` nesting.
    let mut subset = 0usize;
    let mut i = 0;
    while i < end {
        let rest = &bytes[i..];
        let b = bytes[i];
        if b == b'\r' || b == b'\n' {
            let normalized = matches!(
                at,
                At::Content | At::Value(_) | At::Comment | At::Pi | At::Cdata
            );
            let mut lone = 0;
            while i < end && (bytes[i] == b'\r' || bytes[i] == b'\n') {
                if bytes[i] == b'\r' && bytes.get(i + 1) == Some(&b'\n') {
                    i += 1;
                } else if bytes[i] == b'\r' {
                    lone += 1;
                }
                i += 1;
                line += 1;
                column = 1;
            }
            if normalized {
                column -= lone;
            }
            continue;
        }
        let n = match at {
            At::Outside | At::Content => {
                if rest.starts_with(b"<!--") {
                    at = At::Comment;
                    4
                } else if rest.starts_with(b"<![CDATA[") {
                    at = At::Cdata;
                    9
                } else if rest.starts_with(b"<!") {
                    at = At::Doctype;
                    2
                } else if i == 0
                    && rest.starts_with(b"<?xml")
                    && rest.get(5).is_some_and(u8::is_ascii_whitespace)
                {
                    at = At::Declaration;
                    5
                } else if rest.starts_with(b"<?") {
                    at = At::Pi;
                    2
                } else if b == b'<' {
                    at = At::Tag;
                    closing = rest.get(1) == Some(&b'/');
                    empty = false;
                    1
                } else {
                    utf8_len(b)
                }
            }
            At::Tag => match b {
                b'"' | b'\'' => {
                    at = At::Value(b);
                    1
                }
                b'/' => {
                    empty = !closing;
                    1
                }
                b'>' => {
                    if closing {
                        depth = depth.saturating_sub(1);
                    } else if !empty {
                        depth += 1;
                    }
                    at = if depth > 0 { At::Content } else { At::Outside };
                    1
                }
                _ => utf8_len(b),
            },
            At::Value(q) => {
                if b == q {
                    at = At::Tag;
                }
                utf8_len(b)
            }
            At::Comment | At::Pi | At::Cdata | At::Declaration => {
                let close: &[u8] = match at {
                    At::Comment => b"-->",
                    At::Pi | At::Declaration => b"?>",
                    _ => b"]]>",
                };
                if rest.starts_with(close) {
                    at = if depth > 0 { At::Content } else { At::Outside };
                    close.len()
                } else {
                    utf8_len(b)
                }
            }
            At::Doctype => {
                match b {
                    b'[' => subset += 1,
                    b']' => subset = subset.saturating_sub(1),
                    b'>' if subset == 0 => at = At::Outside,
                    _ => {}
                }
                utf8_len(b)
            }
        };
        let n = n.min(end - i);
        column += text[i..i + n]
            .chars()
            .map(|c| c.len_utf16() as i64)
            .sum::<i64>();
        i += n;
    }
    (line, column)
}

/// The length of the UTF-8 sequence `b` starts.
fn utf8_len(b: u8) -> usize {
    match b {
        0xf0.. => 4,
        0xe0.. => 3,
        0xc0.. => 2,
        _ => 1,
    }
}

/// `XMLSchemaMessages.properties`, with `MessageFormat`'s quoting undone.
fn template(key: &str) -> &'static str {
    match key {
        "cvc-attribute.3" => {
            "cvc-attribute.3: The value '{2}' of attribute '{1}' on element '{0}' is not valid with respect to its type, '{3}'."
        }
        "cvc-attribute.4" => {
            "cvc-attribute.4: The value '{2}' of attribute '{1}' on element '{0}' is not valid with respect to its fixed {value constraint}. The attribute must have a value of '{3}'."
        }
        "cvc-complex-type.2.1" => {
            "cvc-complex-type.2.1: Element '{0}' must have no character or element information item [children], because the type's content type is empty."
        }
        "cvc-complex-type.2.2" => {
            "cvc-complex-type.2.2: Element '{0}' must have no element [children], and the value must be valid."
        }
        "cvc-complex-type.2.3" => {
            "cvc-complex-type.2.3: Element '{0}' cannot have character [children], because the type's content type is element-only."
        }
        "cvc-complex-type.2.4.a" => {
            "cvc-complex-type.2.4.a: Invalid content was found starting with element '{0}'. One of '{1}' is expected."
        }
        "cvc-complex-type.2.4.b" => {
            "cvc-complex-type.2.4.b: The content of element '{0}' is not complete. One of '{1}' is expected."
        }
        "cvc-complex-type.2.4.c" => {
            "cvc-complex-type.2.4.c: The matching wildcard is strict, but no declaration can be found for element '{0}'."
        }
        "cvc-complex-type.2.4.d" => {
            "cvc-complex-type.2.4.d: Invalid content was found starting with element '{0}'. No child element is expected at this point."
        }
        "cvc-complex-type.3.1" => {
            "cvc-complex-type.3.1: Value '{2}' of attribute '{1}' of element '{0}' is not valid with respect to the corresponding attribute use. Attribute '{1}' has a fixed value of '{3}'."
        }
        "cvc-complex-type.3.2.2" => {
            "cvc-complex-type.3.2.2: Attribute '{1}' is not allowed to appear in element '{0}'."
        }
        "cvc-complex-type.4" => "cvc-complex-type.4: Attribute '{1}' must appear on element '{0}'.",
        "cvc-complex-type.4_ns" => {
            "cvc-complex-type.4: Attribute '{1}' belonging to namespace '{2}', must appear on element '{0}'."
        }
        "cvc-complex-type.5.1" => {
            "cvc-complex-type.5.1: In element '{0}', attribute '{1}' is a Wild ID. But there is already a Wild ID '{2}'. There can be only one."
        }
        "cvc-complex-type.5.2" => {
            "cvc-complex-type.5.2: In element '{0}', attribute '{1}' is a Wild ID. But there is already an attribute '{2}' derived from ID among the {attribute uses}."
        }
        "cvc-datatype-valid.1.2.1" => {
            "cvc-datatype-valid.1.2.1: '{0}' is not a valid value for '{1}'."
        }
        "cvc-datatype-valid.1.2.3" => {
            "cvc-datatype-valid.1.2.3: '{0}' is not a valid value of union type '{1}'."
        }
        "cvc-elt.1.a" => "cvc-elt.1.a: Cannot find the declaration of element '{0}'.",
        "cvc-elt.2" => {
            "cvc-elt.2: The value of {abstract} in the element declaration for '{0}' must be false."
        }
        "cvc-elt.3.1" => {
            "cvc-elt.3.1: Attribute '{1}' must not appear on element '{0}', because the {nillable} property of '{0}' is false."
        }
        "cvc-elt.3.2.1" => {
            "cvc-elt.3.2.1: Element '{0}' cannot have character or element information [children], because '{1}' is specified."
        }
        "cvc-elt.3.2.2" => {
            "cvc-elt.3.2.2: There must be no fixed {value constraint} for element '{0}', because '{1}' is specified."
        }
        "cvc-elt.4.2" => {
            "cvc-elt.4.2: Cannot resolve '{1}' to a type definition for element '{0}'."
        }
        "cvc-elt.4.3" => {
            "cvc-elt.4.3: Type '{1}' is not validly derived from the type definition, '{2}', of element '{0}'."
        }
        "cvc-elt.5.1.1" => {
            "cvc-elt.5.1.1: {value constraint} '{2}' of element '{0}' is not a valid default value for type '{1}'."
        }
        "cvc-elt.5.2.2.1" => {
            "cvc-elt.5.2.2.1: Element '{0}' must have no element information item [children]."
        }
        "cvc-elt.5.2.2.2.1" => {
            "cvc-elt.5.2.2.2.1: The value '{1}' of element '{0}' does not match the fixed {value constraint} value '{2}'."
        }
        "cvc-elt.5.2.2.2.2" => {
            "cvc-elt.5.2.2.2.2: The value '{1}' of element '{0}' does not match the {value constraint} value '{2}'."
        }
        "cvc-enumeration-valid" => {
            "cvc-enumeration-valid: Value '{0}' is not facet-valid with respect to enumeration '{1}'. It must be a value from the enumeration."
        }
        "cvc-id.1" => "cvc-id.1: There is no ID/IDREF binding for IDREF '{0}'.",
        "cvc-id.2" => "cvc-id.2: There are multiple occurrences of ID value '{0}'.",
        "cvc-maxInclusive-valid" => {
            "cvc-maxInclusive-valid: Value '{0}' is not facet-valid with respect to maxInclusive '{1}' for type '{2}'."
        }
        "cvc-minInclusive-valid" => {
            "cvc-minInclusive-valid: Value '{0}' is not facet-valid with respect to minInclusive '{1}' for type '{2}'."
        }
        "cvc-pattern-valid" => {
            "cvc-pattern-valid: Value '{0}' is not facet-valid with respect to pattern '{1}' for type '{2}'."
        }
        "cvc-type.2" => "cvc-type.2: The type definition cannot be abstract for element {0}.",
        "cvc-type.3.1.1" => {
            "cvc-type.3.1.1: Element '{0}' is a simple type, so it cannot have attributes, excepting those whose namespace name is identical to 'http://www.w3.org/2001/XMLSchema-instance' and whose [local name] is one of 'type', 'nil', 'schemaLocation' or 'noNamespaceSchemaLocation'. However, the attribute, '{1}' was found."
        }
        "cvc-type.3.1.2" => {
            "cvc-type.3.1.2: Element '{0}' is a simple type, so it must have no element information item [children]."
        }
        "UndeclaredPrefix" => {
            "UndeclaredPrefix: Cannot resolve '{0}' as a QName: the prefix '{1}' is not declared."
        }
        other => unreachable!("no message {other}"),
    }
}

/// Validates `node`, an element of the document parsed from `text`,
/// against `xsd`: the first error, as Xerces reports it.
pub(crate) fn validate(xsd: Xsd, text: &str, node: Node<'_, '_>) -> Result<(), Fail> {
    validate::Validator::new(xsd.schema(), text).root(node)
}

/// Validates `node` as JAXB's `ValidatingUnmarshaller` validates what a
/// StAX reader reads of it: attributes in the document's order, their
/// values through `filter`, each error placed where the reader is.
pub(crate) fn validate_stream(
    xsd: Xsd,
    text: &str,
    node: Node<'_, '_>,
    filter: &dyn Fn(&str) -> String,
) -> Result<(), Fail> {
    validate::Validator::streaming(xsd.schema(), text, filter).root(node)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(xsd: Xsd, xml: &str) -> Result<(), String> {
        let doc = roxmltree::Document::parse(xml).unwrap();
        validate(xsd, xml, doc.root_element()).map_err(|f| f.message())
    }

    #[test]
    fn the_schemas_load() {
        for xsd in [Xsd::Configuration, Xsd::Jms, Xsd::Server, Xsd::Dto] {
            let s = xsd.schema();
            assert!(!s.global_elems.is_empty(), "{xsd:?}");
        }
    }

    #[test]
    fn messages_follow_xerces() {
        let core = |body: &str| format!(r#"<core xmlns="urn:activemq:core">{body}</core>"#);
        assert_eq!(check(Xsd::Configuration, &core("")), Ok(()));
        assert_eq!(
            check(Xsd::Configuration, "<core/>"),
            Err("cvc-elt.1.a: Cannot find the declaration of element 'core'.".into())
        );
        assert_eq!(
            check(
                Xsd::Configuration,
                &core(
                    "<address-settings><address-setting match=\"#\"><max-size-bytes-reject-threshold>100K</max-size-bytes-reject-threshold></address-setting></address-settings>"
                )
            ),
            Err("cvc-datatype-valid.1.2.1: '100K' is not a valid value for 'integer'.".into())
        );
        assert_eq!(
            check(Xsd::Jms, r#"<jms xmlns="urn:activemq:jms"><topic name="t"> </topic></jms>"#),
            Err("cvc-complex-type.2.1: Element 'topic' must have no character or element information item [children], because the type's content type is empty.".into())
        );
        assert_eq!(
            check(
                Xsd::Configuration,
                &core(
                    "<bridges><bridge name=\"b\"><queue-name>q</queue-name><forwarding-address>f</forwarding-address><discovery-group-ref discovery-group-name=\"nope\"/></bridge></bridges>"
                )
            ),
            Err("cvc-id.1: There is no ID/IDREF binding for IDREF 'nope'.".into())
        );
        assert_eq!(
            check(Xsd::Configuration, &core("<acceptors><acceptor name=\"a\" xml:base=\"x.xml\">tcp://h:1</acceptor></acceptors>")),
            Err("cvc-complex-type.3.2.2: Attribute 'xml:base' is not allowed to appear in element 'acceptor'.".into())
        );
        let e = check(Xsd::Configuration, &core("<name>a</name><bogus/>")).unwrap_err();
        assert!(
            e.starts_with("cvc-complex-type.2.4.a: Invalid content was found starting with element '{\"urn:activemq:core\":bogus}'. One of '{\"urn:activemq:core\":"),
            "{e}"
        );
    }
}
