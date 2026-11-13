//! The functions `XPathFactory`'s compiler knows (Xalan's `FunctionTable`):
//! XPath 1.0's library and the XSLT functions Xalan keeps in it. An
//! unprefixed call to any other name, or with an argument count its
//! function does not take, does not compile; `sxd_xpath` would compile it
//! and fail every evaluation, so a selector Artemis refuses would be taken
//! and match nothing.

use sxd_xpath::context::Evaluation;
use sxd_xpath::function::{Args, Error, Function};
use sxd_xpath::nodeset::{Node, Nodeset};
use sxd_xpath::{Context, Value};

/// A function's name and the argument counts it takes (`None`: no most).
pub(super) const TABLE: &[(&str, usize, Option<usize>)] = &[
    ("current", 0, Some(0)),
    ("last", 0, Some(0)),
    ("position", 0, Some(0)),
    ("count", 1, Some(1)),
    ("id", 1, Some(1)),
    ("local-name", 0, Some(1)),
    ("namespace-uri", 0, Some(1)),
    ("name", 0, Some(1)),
    ("generate-id", 0, Some(1)),
    ("not", 1, Some(1)),
    ("true", 0, Some(0)),
    ("false", 0, Some(0)),
    ("boolean", 1, Some(1)),
    ("lang", 1, Some(1)),
    ("number", 0, Some(1)),
    ("floor", 1, Some(1)),
    ("ceiling", 1, Some(1)),
    ("round", 1, Some(1)),
    ("sum", 1, Some(1)),
    ("string", 0, Some(1)),
    ("starts-with", 2, Some(2)),
    ("contains", 2, Some(2)),
    ("substring-before", 2, Some(2)),
    ("substring-after", 2, Some(2)),
    ("normalize-space", 0, Some(1)),
    ("translate", 3, Some(3)),
    ("concat", 2, None),
    ("system-property", 1, Some(1)),
    ("function-available", 1, Some(1)),
    ("element-available", 1, Some(1)),
    ("substring", 2, Some(3)),
    ("string-length", 0, Some(1)),
    ("unparsed-entity-uri", 1, Some(1)),
    ("document-location", 0, Some(1)),
    ("here", 0, Some(0)),
];

/// The names `function-available` answers true for: the table's, and
/// `key`, which is named in it without an implementation (a call to it
/// does not compile).
fn available(name: &str) -> bool {
    name == "key" || TABLE.iter().any(|(n, ..)| *n == name)
}

/// A context with XPath's library and the table's functions it lacks.
pub(super) fn context<'d>() -> Context<'d> {
    let mut context = Context::new();
    context.set_function("current", Current);
    context.set_function("id", Id);
    context.set_function("lang", Lang);
    context.set_function("generate-id", GenerateId);
    context.set_function("system-property", SystemProperty);
    context.set_function("function-available", FunctionAvailable);
    context.set_function("element-available", ElementAvailable);
    context.set_function("unparsed-entity-uri", Empty);
    context.set_function("document-location", Empty);
    context.set_function("here", Here);
    context
}

/// A name with a prefix, which Java resolves against a namespace context
/// that `XPathExpression` never sets: the evaluation fails.
fn unprefixed(name: String) -> Result<String, Error> {
    if name.contains(':') {
        Err(Error::Other(format!(
            "no namespace for the prefix of {name}"
        )))
    } else {
        Ok(name)
    }
}

/// `current()`: the node evaluation started from, which is the document.
struct Current;

impl Function for Current {
    fn evaluate<'c, 'd>(
        &self,
        context: &Evaluation<'c, 'd>,
        args: Vec<Value<'d>>,
    ) -> Result<Value<'d>, Error> {
        Args(args).exactly(0)?;
        let mut nodes = Nodeset::new();
        nodes.add(context.node.document().root());
        Ok(Value::Nodeset(nodes))
    }
}

/// `id(...)`: elements by an ID-typed attribute, which only a DTD makes;
/// a body with one does not parse, so there are none.
struct Id;

impl Function for Id {
    fn evaluate<'c, 'd>(
        &self,
        _: &Evaluation<'c, 'd>,
        args: Vec<Value<'d>>,
    ) -> Result<Value<'d>, Error> {
        Args(args).exactly(1)?;
        Ok(Value::Nodeset(Nodeset::new()))
    }
}

/// `FuncLang`: the nearest `xml:lang` of the context node or an ancestor,
/// compared without case, matching the argument or it and a `-` suffix.
struct Lang;

impl Function for Lang {
    fn evaluate<'c, 'd>(
        &self,
        context: &Evaluation<'c, 'd>,
        args: Vec<Value<'d>>,
    ) -> Result<Value<'d>, Error> {
        let mut args = Args(args);
        args.exactly(1)?;
        let lang = args.pop_string()?.to_lowercase();
        let mut node = Some(context.node);
        while let Some(n) = node {
            if let Node::Element(e) = n
                && let Some(value) = e
                    .attribute(("http://www.w3.org/XML/1998/namespace", "lang"))
                    .map(|a| a.value().to_lowercase())
            {
                let matches = value
                    .strip_prefix(lang.as_str())
                    .is_some_and(|rest| rest.is_empty() || rest.starts_with('-'));
                return Ok(Value::Boolean(matches));
            }
            node = n.parent();
        }
        Ok(Value::Boolean(false))
    }
}

/// `generate-id(...)`: a name for the first node of the argument (the
/// context node without one) that no other node of the document has, or
/// `""` for no node.
struct GenerateId;

impl Function for GenerateId {
    fn evaluate<'c, 'd>(
        &self,
        context: &Evaluation<'c, 'd>,
        args: Vec<Value<'d>>,
    ) -> Result<Value<'d>, Error> {
        let mut args = Args(args);
        args.at_most(1)?;
        let node = if args.is_empty() {
            Some(context.node)
        } else {
            args.pop_nodeset()?.document_order_first()
        };
        let Some(node) = node else {
            return Ok(Value::String(String::new()));
        };
        // The node's place in a walk of the document, attributes and
        // namespaces after their element.
        let mut at = 0usize;
        let mut pending: Vec<Node<'d>> = vec![node.document().root().into()];
        while let Some(n) = pending.pop() {
            if n == node {
                return Ok(Value::String(format!("N{at}")));
            }
            at += 1;
            if let Node::Element(e) = n {
                pending.extend(e.attributes().into_iter().map(Node::from));
            }
            pending.extend(n.children().into_iter().rev());
        }
        // A namespace node, which the walk does not visit: named after its
        // element and prefix.
        Ok(Value::String(format!(
            "N{}{}",
            at,
            node.prefixed_name().unwrap_or_default()
        )))
    }
}

/// `system-property(name)`: Java answers a bare name from the JVM's
/// properties, which this broker does not have.
struct SystemProperty;

impl Function for SystemProperty {
    fn evaluate<'c, 'd>(
        &self,
        _: &Evaluation<'c, 'd>,
        args: Vec<Value<'d>>,
    ) -> Result<Value<'d>, Error> {
        let mut args = Args(args);
        args.exactly(1)?;
        unprefixed(args.pop_string()?)?;
        Ok(Value::String(String::new()))
    }
}

/// `function-available(name)`: whether the table names it.
struct FunctionAvailable;

impl Function for FunctionAvailable {
    fn evaluate<'c, 'd>(
        &self,
        _: &Evaluation<'c, 'd>,
        args: Vec<Value<'d>>,
    ) -> Result<Value<'d>, Error> {
        let mut args = Args(args);
        args.exactly(1)?;
        let name = unprefixed(args.pop_string()?)?;
        Ok(Value::Boolean(available(&name)))
    }
}

/// `element-available(name)`: there are no XSLT elements outside a
/// stylesheet.
struct ElementAvailable;

impl Function for ElementAvailable {
    fn evaluate<'c, 'd>(
        &self,
        _: &Evaluation<'c, 'd>,
        args: Vec<Value<'d>>,
    ) -> Result<Value<'d>, Error> {
        let mut args = Args(args);
        args.exactly(1)?;
        unprefixed(args.pop_string()?)?;
        Ok(Value::Boolean(false))
    }
}

/// `unparsed-entity-uri(...)` and `document-location(...)`: a body has no
/// DTD to declare an entity and no location it was read from.
struct Empty;

impl Function for Empty {
    fn evaluate<'c, 'd>(
        &self,
        _: &Evaluation<'c, 'd>,
        args: Vec<Value<'d>>,
    ) -> Result<Value<'d>, Error> {
        Args(args).at_most(1)?;
        Ok(Value::String(String::new()))
    }
}

/// `here()`: the node holding the expression, which only a stylesheet
/// has; Java's evaluation fails.
struct Here;

impl Function for Here {
    fn evaluate<'c, 'd>(
        &self,
        _: &Evaluation<'c, 'd>,
        _: Vec<Value<'d>>,
    ) -> Result<Value<'d>, Error> {
        Err(Error::Other("here() outside a stylesheet".to_owned()))
    }
}
