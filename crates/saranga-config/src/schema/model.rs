//! The schema components Xerces's traversers build from the XSDs, for the
//! part of XML Schema that Artemis's schemas use. A construct outside that
//! part is a bug in this loader, not in a broker configuration: it panics,
//! and the tests load every schema.

use std::collections::HashMap;
use std::sync::OnceLock;

use regex::Regex;
use roxmltree::{Document, Node};

use super::cm::ContentModel;
use super::datatype::{Decimal, Dv, SimpleType, Special, Types, Variety, Ws, XSD_NS};

pub(super) const XSI_NS: &str = "http://www.w3.org/2001/XMLSchema-instance";
pub(super) const XML_NS: &str = "http://www.w3.org/XML/1998/namespace";

/// A namespace and local name.
pub(super) type QName = (Option<String>, String);

/// Attribute uses as they are gathered: the declaration, whether it is
/// required, and the use's value constraint.
type Uses = Vec<(usize, bool, Option<Constraint>)>;

pub(super) enum Type {
    Simple(SimpleType),
    Complex(ComplexType),
}

pub(super) struct ComplexType {
    pub name: String,
    /// The type this one derives from: `None` only for `anyType`.
    pub base: Option<usize>,
    pub content: Content,
    pub attr_uses: Vec<AttrUse>,
    pub attr_wildcard: Option<Wildcard>,
    /// `fIDAttrName`: an attribute use whose type is an ID.
    pub id_attr: Option<String>,
    pub abstract_: bool,
    pub cm: OnceLock<Option<ContentModel>>,
}

pub(super) enum Content {
    Empty,
    Simple(usize),
    Element(Particle),
    Mixed(Particle),
}

pub(super) struct AttrUse {
    pub decl: usize,
    pub required: bool,
    pub constraint: Option<Constraint>,
}

pub(super) struct AttrDecl {
    pub name: String,
    pub namespace: Option<String>,
    pub ty: usize,
    pub constraint: Option<Constraint>,
}

/// A `default` or `fixed` value.
#[derive(Clone)]
pub(super) struct Constraint {
    pub fixed: bool,
    pub value: String,
}

pub(super) struct ElemDecl {
    pub name: String,
    pub namespace: Option<String>,
    pub ty: usize,
    pub global: bool,
    pub abstract_: bool,
    pub nillable: bool,
    pub constraint: Option<Constraint>,
    /// `substitutionGroup`: the head this element may stand for.
    pub head: Option<usize>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) enum NsConstraint {
    Any,
    /// `##other`: neither of these.
    Not(Vec<Option<String>>),
    List(Vec<Option<String>>),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Process {
    Strict,
    Lax,
    Skip,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Wildcard {
    pub ns: NsConstraint,
    pub process: Process,
}

impl Wildcard {
    /// `allowNamespace`.
    pub(super) fn allows(&self, ns: Option<&str>) -> bool {
        match &self.ns {
            NsConstraint::Any => true,
            NsConstraint::Not(list) => !list.iter().any(|n| n.as_deref() == ns),
            NsConstraint::List(list) => list.iter().any(|n| n.as_deref() == ns),
        }
    }
}

impl std::fmt::Display for Wildcard {
    /// `XSWildcardDecl.toString`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WC[")?;
        match &self.ns {
            NsConstraint::Any => f.write_str("##any")?,
            NsConstraint::Not(list) => {
                write!(f, "##other:\"{}\"", list[0].as_deref().unwrap_or(""))?;
            }
            NsConstraint::List(list) => {
                for (i, ns) in list.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "\"{}\"", ns.as_deref().unwrap_or(""))?;
                }
            }
        }
        f.write_str("]")
    }
}

pub(super) struct Particle {
    pub min: u32,
    /// `None` for `unbounded`.
    pub max: Option<u32>,
    pub term: Term,
}

pub(super) enum Term {
    Element(usize),
    Wildcard(Wildcard),
    Group(Compositor, Vec<Particle>),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Compositor {
    Sequence,
    Choice,
    All,
}

/// The grammars a validator has: the components of a schema and of the
/// schemas it imports, with the built-in ones.
pub(super) struct Schema {
    pub types: Vec<Type>,
    pub elems: Vec<ElemDecl>,
    pub attrs: Vec<AttrDecl>,
    pub global_elems: HashMap<QName, usize>,
    pub global_types: HashMap<QName, usize>,
    pub global_attrs: HashMap<QName, usize>,
    /// The namespaces there is a grammar for.
    pub grammars: Vec<Option<String>>,
    pub any_type: usize,
    pub any_simple: usize,
}

impl Types for Schema {
    fn simple(&self, id: usize) -> &SimpleType {
        match &self.types[id] {
            Type::Simple(s) => s,
            Type::Complex(c) => panic!("{} is not a simple type", c.name),
        }
    }
}

impl Schema {
    pub(super) fn complex(&self, id: usize) -> Option<&ComplexType> {
        match &self.types[id] {
            Type::Complex(c) => Some(c),
            Type::Simple(_) => None,
        }
    }

    pub(super) fn type_name(&self, id: usize) -> &str {
        match &self.types[id] {
            Type::Simple(s) => &s.name,
            Type::Complex(c) => &c.name,
        }
    }

    pub(super) fn has_grammar(&self, ns: Option<&str>) -> bool {
        self.grammars.iter().any(|g| g.as_deref() == ns)
    }

    pub(super) fn global_elem(&self, ns: Option<&str>, name: &str) -> Option<usize> {
        self.global_elems
            .get(&(ns.map(str::to_owned), name.to_owned()))
            .copied()
    }

    pub(super) fn global_attr(&self, ns: Option<&str>, name: &str) -> Option<usize> {
        self.global_attrs
            .get(&(ns.map(str::to_owned), name.to_owned()))
            .copied()
    }

    pub(super) fn global_type(&self, ns: Option<&str>, name: &str) -> Option<usize> {
        self.global_types
            .get(&(ns.map(str::to_owned), name.to_owned()))
            .copied()
    }

    /// The schema in `files` named `root` and every schema it imports.
    pub(super) fn load(files: &[(&str, &'static str)], root: &str) -> Schema {
        let mut schema = Schema {
            types: Vec::new(),
            elems: Vec::new(),
            attrs: Vec::new(),
            global_elems: HashMap::new(),
            global_types: HashMap::new(),
            global_attrs: HashMap::new(),
            grammars: vec![Some(XSD_NS.to_owned()), Some(XSI_NS.to_owned())],
            any_type: 0,
            any_simple: 0,
        };
        schema.builtins();

        let mut order = vec![root.to_owned()];
        let mut docs = Vec::new();
        let mut i = 0;
        while i < order.len() {
            let text = files
                .iter()
                .find(|(n, _)| *n == order[i])
                .unwrap_or_else(|| panic!("no schema {}", order[i]))
                .1;
            let doc = Document::parse(text).unwrap_or_else(|e| panic!("{}: {e}", order[i]));
            for import in doc
                .root_element()
                .children()
                .filter(|c| is_xsd(c, "import"))
            {
                let location = import.attribute("schemaLocation").expect("import location");
                // schema_reference.4: an import that cannot be read is a
                // warning, and the schema is used without it.
                let readable = files.iter().any(|(n, _)| *n == location);
                if readable && !order.iter().any(|o| o == location) {
                    order.push(location.to_owned());
                }
            }
            docs.push(doc);
            i += 1;
        }
        let mut loader = Loader {
            schema,
            globals: HashMap::new(),
            done: HashMap::new(),
            groups: HashMap::new(),
        };
        for doc in &docs {
            loader.register(doc.root_element());
        }
        let keys: Vec<(Kind, QName)> = loader.globals.keys().cloned().collect();
        for key in keys {
            loader.global(&key.0, &key.1);
        }
        loader.schema
    }

    fn push(&mut self, t: Type) -> usize {
        self.types.push(t);
        self.types.len() - 1
    }

    fn builtin(&mut self, name: &str, t: SimpleType) -> usize {
        let id = self.push(Type::Simple(t));
        self.global_types
            .insert((Some(XSD_NS.to_owned()), name.to_owned()), id);
        id
    }

    /// `SchemaGrammar.fAnyType` and the built-in simple types the schemas
    /// and the `xsi:` attributes use, with their bases.
    fn builtins(&mut self) {
        let any_type = self.push(Type::Complex(ComplexType {
            name: "anyType".into(),
            base: None,
            content: Content::Mixed(Particle {
                min: 1,
                max: Some(1),
                term: Term::Group(
                    Compositor::Sequence,
                    vec![Particle {
                        min: 0,
                        max: None,
                        term: Term::Wildcard(Wildcard {
                            ns: NsConstraint::Any,
                            process: Process::Lax,
                        }),
                    }],
                ),
            }),
            attr_uses: Vec::new(),
            attr_wildcard: Some(Wildcard {
                ns: NsConstraint::Any,
                process: Process::Lax,
            }),
            id_attr: None,
            abstract_: false,
            cm: OnceLock::new(),
        }));
        self.global_types
            .insert((Some(XSD_NS.to_owned()), "anyType".to_owned()), any_type);
        self.any_type = any_type;
        let any_simple = self.builtin(
            "anySimpleType",
            SimpleType::atomic("anySimpleType", None, Dv::AnySimple, Ws::Preserve),
        );
        self.any_simple = any_simple;
        let string = self.builtin(
            "string",
            SimpleType::atomic("string", Some(any_simple), Dv::String, Ws::Preserve),
        );
        let normalized = self.builtin(
            "normalizedString",
            SimpleType::atomic("normalizedString", Some(string), Dv::String, Ws::Replace),
        );
        let token = self.builtin(
            "token",
            SimpleType::atomic("token", Some(normalized), Dv::String, Ws::Collapse),
        );
        let mut language = SimpleType::atomic("language", Some(token), Dv::String, Ws::Collapse);
        let pattern = "([a-zA-Z]{1,8})(-[a-zA-Z0-9]{1,8})*";
        language.patterns = vec![(pattern.to_owned(), xsd_regex(pattern))];
        self.builtin("language", language);
        let mut ncname = SimpleType::atomic("NCName", Some(token), Dv::String, Ws::Collapse);
        ncname.special = Special::NcName;
        let ncname = self.builtin("NCName", ncname);
        let mut id = SimpleType::atomic("ID", Some(ncname), Dv::Id, Ws::Collapse);
        id.special = Special::NcName;
        self.builtin("ID", id);
        let mut idref = SimpleType::atomic("IDREF", Some(ncname), Dv::IdRef, Ws::Collapse);
        idref.special = Special::NcName;
        self.builtin("IDREF", idref);
        self.builtin(
            "boolean",
            SimpleType::atomic("boolean", Some(any_simple), Dv::Boolean, Ws::Collapse),
        );
        let decimal = self.builtin(
            "decimal",
            SimpleType::atomic("decimal", Some(any_simple), Dv::Decimal, Ws::Collapse),
        );
        let integer = self.builtin(
            "integer",
            SimpleType::atomic("integer", Some(decimal), Dv::Integer, Ws::Collapse),
        );
        let bounded = |name: &str, base: usize, min: &str, max: &str| {
            let mut t = SimpleType::atomic(name, Some(base), Dv::Integer, Ws::Collapse);
            t.min_inclusive = Some((Decimal::parse(min, true).unwrap(), min.to_owned()));
            t.max_inclusive = Some((Decimal::parse(max, true).unwrap(), max.to_owned()));
            t
        };
        let long = self.builtin(
            "long",
            bounded(
                "long",
                integer,
                "-9223372036854775808",
                "9223372036854775807",
            ),
        );
        self.builtin("int", bounded("int", long, "-2147483648", "2147483647"));
        self.builtin(
            "double",
            SimpleType::atomic("double", Some(any_simple), Dv::Double, Ws::Collapse),
        );
        let any_uri = self.builtin(
            "anyURI",
            SimpleType::atomic("anyURI", Some(any_simple), Dv::AnyUri, Ws::Collapse),
        );
        let qname = self.builtin(
            "QName",
            SimpleType::atomic("QName", Some(any_simple), Dv::QName, Ws::Collapse),
        );

        // SchemaGrammar.SG_XSI's attributes.
        let boolean = self.global_type(Some(XSD_NS), "boolean").unwrap();
        let mut locations = SimpleType::atomic(
            "#AnonType_schemaLocation",
            Some(any_simple),
            Dv::AnySimple,
            Ws::Collapse,
        );
        locations.variety = Variety::List(any_uri);
        locations.namespace = Some(XSI_NS.to_owned());
        let locations = self.push(Type::Simple(locations));
        for (name, ty) in [
            ("type", qname),
            ("nil", boolean),
            ("schemaLocation", locations),
            ("noNamespaceSchemaLocation", any_uri),
        ] {
            self.attrs.push(AttrDecl {
                name: name.to_owned(),
                namespace: Some(XSI_NS.to_owned()),
                ty,
                constraint: None,
            });
            self.global_attrs.insert(
                (Some(XSI_NS.to_owned()), name.to_owned()),
                self.attrs.len() - 1,
            );
        }
    }
}

/// An XML Schema regular expression, matched whole.
fn xsd_regex(pattern: &str) -> Regex {
    Regex::new(&format!("^(?:{pattern})$")).expect("schema pattern")
}

fn is_xsd(n: &Node<'_, '_>, name: &str) -> bool {
    n.is_element() && n.tag_name().namespace() == Some(XSD_NS) && n.tag_name().name() == name
}

/// The schema elements under `n`, without annotations.
fn xsd_children<'a, 'i>(n: Node<'a, 'i>) -> impl Iterator<Item = Node<'a, 'i>> {
    n.children()
        .filter(|c| c.is_element() && !is_xsd(c, "annotation"))
        .inspect(|c| assert_eq!(c.tag_name().namespace(), Some(XSD_NS), "non-schema element"))
}

/// Panics on any attribute of `n` not in `allowed`.
fn only(n: Node<'_, '_>, allowed: &[&str]) {
    for a in n.attributes() {
        assert!(
            a.namespace().is_some() || allowed.contains(&a.name()),
            "unsupported attribute {} on xsd:{}",
            a.name(),
            n.tag_name().name()
        );
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Kind {
    Type,
    Element,
    Attribute,
    AttributeGroup,
}

struct Loader<'a, 'i> {
    schema: Schema,
    /// Every global component's declaration.
    globals: HashMap<(Kind, QName), Node<'a, 'i>>,
    /// The components built so far.
    done: HashMap<(Kind, QName), usize>,
    /// Attribute groups: their uses.
    groups: HashMap<QName, Uses>,
}

impl<'a, 'i> Loader<'a, 'i> {
    fn register(&mut self, root: Node<'a, 'i>) {
        only(
            root,
            &[
                "targetNamespace",
                "attributeFormDefault",
                "elementFormDefault",
                "version",
            ],
        );
        let tns = root.attribute("targetNamespace").map(str::to_owned);
        if !self.schema.grammars.contains(&tns) {
            self.schema.grammars.push(tns.clone());
        }
        for c in xsd_children(root) {
            let kind = match c.tag_name().name() {
                "import" => continue,
                "complexType" | "simpleType" => Kind::Type,
                "element" => Kind::Element,
                "attribute" => Kind::Attribute,
                "attributeGroup" => Kind::AttributeGroup,
                other => panic!("unsupported global xsd:{other}"),
            };
            let name = c.attribute("name").expect("global name").to_owned();
            self.globals.insert((kind, (tns.clone(), name)), c);
        }
    }

    fn target_ns(n: Node<'_, '_>) -> Option<String> {
        let root = n
            .ancestors()
            .last()
            .and_then(|d| d.first_element_child())
            .expect("schema");
        root.attribute("targetNamespace").map(str::to_owned)
    }

    /// A QName in a schema: its prefix's namespace, or the default one.
    fn qname(n: Node<'_, '_>, value: &str) -> QName {
        let (prefix, local) = match value.split_once(':') {
            Some((p, l)) => (Some(p), l),
            None => (None, value),
        };
        let ns = match prefix {
            Some("xml") => Some(XML_NS.to_owned()),
            _ => n.lookup_namespace_uri(prefix).map(str::to_owned),
        };
        assert!(
            prefix.is_none() || ns.is_some(),
            "unbound prefix in {value}"
        );
        (ns, local.to_owned())
    }

    fn global(&mut self, kind: &Kind, qn: &QName) -> usize {
        if let Some(&id) = self.done.get(&(*kind, qn.clone())) {
            return id;
        }
        let node = *self
            .globals
            .get(&(*kind, qn.clone()))
            .unwrap_or_else(|| panic!("no {kind:?} {qn:?}"));
        match kind {
            Kind::Type => {
                if is_xsd(&node, "complexType") {
                    self.complex_type(node, Some(qn))
                } else {
                    self.simple_type(node, Some(qn))
                }
            }
            Kind::Element => self.element(node, true),
            Kind::Attribute => self.attribute_decl(node, true),
            Kind::AttributeGroup => {
                only(node, &["name"]);
                let mut uses = Vec::new();
                for c in xsd_children(node) {
                    match c.tag_name().name() {
                        "attribute" => uses.push(self.attribute_use(c)),
                        "attributeGroup" => {
                            let r = Self::qname(c, c.attribute("ref").expect("ref"));
                            self.global(&Kind::AttributeGroup, &r);
                            uses.extend(self.groups[&r].iter().cloned());
                        }
                        other => panic!("unsupported xsd:{other} in an attribute group"),
                    }
                }
                self.groups.insert(qn.clone(), uses);
                self.done.insert((*kind, qn.clone()), 0);
                0
            }
        }
    }

    fn type_ref(&mut self, n: Node<'_, '_>, value: &str) -> usize {
        let qn = Self::qname(n, value);
        if let Some(id) = self.schema.global_types.get(&qn) {
            return *id;
        }
        assert_ne!(
            qn.0.as_deref(),
            Some(XSD_NS),
            "unsupported built-in {value}"
        );
        self.global(&Kind::Type, &qn)
    }

    /// `genAnonTypeName`.
    fn anon_name(n: Node<'_, '_>) -> String {
        let mut name = String::from("#AnonType_");
        for a in n.ancestors().skip(1) {
            if a.parent().is_none_or(|p| p.is_root()) {
                break;
            }
            name.push_str(a.attribute("name").unwrap_or(""));
        }
        name
    }

    fn simple_type(&mut self, n: Node<'a, 'i>, global: Option<&QName>) -> usize {
        only(n, &["name"]);
        let name = global.map_or_else(|| Self::anon_name(n), |q| q.1.clone());
        let ns = Self::target_ns(n);
        let child = xsd_children(n).next().expect("simple type content");
        let t = match child.tag_name().name() {
            "restriction" => {
                only(child, &["base"]);
                let mut facets = Vec::new();
                let mut base = child.attribute("base").map(|b| self.type_ref(child, b));
                for f in xsd_children(child) {
                    if is_xsd(&f, "simpleType") {
                        base = Some(self.simple_type(f, None));
                    } else {
                        only(f, &["value"]);
                        facets.push((
                            f.tag_name().name(),
                            f.attribute("value").expect("facet value"),
                        ));
                    }
                }
                let base = base.expect("restriction base");
                let mut t = self.schema.simple(base).clone();
                t.name = name;
                t.namespace = ns;
                t.base = Some(base);
                let mut enumeration = Vec::new();
                let mut patterns = Vec::new();
                for (facet, value) in facets {
                    match facet {
                        "enumeration" => {
                            let v = self.schema.simple(base).facet_value(&self.schema, value);
                            enumeration.push(
                                v.unwrap_or_else(|e| panic!("enumeration {value}: {}", e.key)),
                            );
                        }
                        "pattern" => patterns.push(value),
                        other => panic!("unsupported facet {other}"),
                    }
                }
                if !enumeration.is_empty() {
                    t.enumeration = Some(enumeration);
                }
                if !patterns.is_empty() {
                    let joined = patterns.join("|");
                    let mut all = vec![(joined.clone(), xsd_regex(&joined))];
                    all.extend(t.patterns.iter().rev().cloned());
                    t.patterns = all;
                }
                t
            }
            "union" => {
                only(child, &["memberTypes"]);
                let mut members = Vec::new();
                for m in child
                    .attribute("memberTypes")
                    .unwrap_or("")
                    .split_whitespace()
                {
                    members.push(self.type_ref(child, m));
                }
                for m in xsd_children(child) {
                    assert!(is_xsd(&m, "simpleType"), "union member");
                    members.push(self.simple_type(m, None));
                }
                let mut t = SimpleType::atomic(
                    &name,
                    Some(self.schema.any_simple),
                    Dv::AnySimple,
                    Ws::Collapse,
                );
                t.namespace = ns;
                t.variety = Variety::Union(members);
                t
            }
            other => panic!("unsupported simple type xsd:{other}"),
        };
        let id = self.schema.push(Type::Simple(t));
        if let Some(q) = global {
            self.schema.global_types.insert(q.clone(), id);
            self.done.insert((Kind::Type, q.clone()), id);
        }
        id
    }

    fn complex_type(&mut self, n: Node<'a, 'i>, global: Option<&QName>) -> usize {
        only(n, &["name", "abstract", "mixed"]);
        let name = global.map_or_else(|| Self::anon_name(n), |q| q.1.clone());
        // The id is taken first: the type's own particles may refer to it.
        let id = self.schema.push(Type::Complex(ComplexType {
            name: name.clone(),
            base: Some(self.schema.any_type),
            content: Content::Empty,
            attr_uses: Vec::new(),
            attr_wildcard: None,
            id_attr: None,
            abstract_: n.attribute("abstract") == Some("true"),
            cm: OnceLock::new(),
        }));
        if let Some(q) = global {
            self.schema.global_types.insert(q.clone(), id);
            self.done.insert((Kind::Type, q.clone()), id);
        }
        let mixed = n.attribute("mixed") == Some("true");
        let first = xsd_children(n).next();
        let (base, content, mut uses) = match first.map(|f| f.tag_name().name()) {
            Some("simpleContent") => {
                let ext = Self::derivation(first.unwrap());
                only(ext, &["base"]);
                let base = self.type_ref(ext, ext.attribute("base").expect("base"));
                let (content, mut base_uses) = match &self.schema.types[base] {
                    Type::Simple(_) => (Content::Simple(base), Vec::new()),
                    Type::Complex(c) => match c.content {
                        Content::Simple(s) => (Content::Simple(s), self.uses_of(base)),
                        _ => panic!("simple content extending complex content"),
                    },
                };
                let mut uses = self.attributes(xsd_children(ext));
                merge(&self.schema, &mut uses, &mut base_uses);
                (base, content, uses)
            }
            Some("complexContent") => {
                let cc = first.unwrap();
                only(cc, &["mixed"]);
                let mixed = mixed || cc.attribute("mixed") == Some("true");
                let ext = Self::derivation(cc);
                only(ext, &["base"]);
                let base = self.type_ref(ext, ext.attribute("base").expect("base"));
                let (particle, uses) = self.content(ext);
                let base_type = self.schema.complex(base).expect("complex base");
                let content = match (particle, &base_type.content) {
                    (None, Content::Empty) => Content::Empty,
                    (None, Content::Simple(s)) => Content::Simple(*s),
                    (None, Content::Element(b)) => Content::Element(clone_particle(b)),
                    (None, Content::Mixed(b)) => Content::Mixed(clone_particle(b)),
                    (Some(p), Content::Empty) => wrap(p, mixed),
                    (Some(p), Content::Element(b)) if !mixed => {
                        let b = clone_particle(b);
                        Content::Element(Particle {
                            min: 1,
                            max: Some(1),
                            term: Term::Group(Compositor::Sequence, vec![b, p]),
                        })
                    }
                    _ => panic!("unsupported complex content extension"),
                };
                let mut uses = uses;
                let mut base_uses = self.uses_of(base);
                merge(&self.schema, &mut uses, &mut base_uses);
                (base, content, uses)
            }
            _ => {
                let (particle, uses) = self.content(n);
                let content = match particle {
                    Some(p) => wrap(p, mixed),
                    None if mixed => Content::Mixed(Particle {
                        min: 1,
                        max: Some(1),
                        term: Term::Group(Compositor::Sequence, Vec::new()),
                    }),
                    None => Content::Empty,
                };
                (self.schema.any_type, content, uses)
            }
        };
        let id_attr = uses
            .iter()
            .find(|u| self.schema.simple(self.schema.attrs[u.0].ty).is_id())
            .map(|u| self.schema.attrs[u.0].name.clone());
        let Type::Complex(t) = &mut self.schema.types[id] else {
            unreachable!()
        };
        t.base = Some(base);
        t.content = content;
        t.id_attr = id_attr;
        t.attr_uses = uses
            .drain(..)
            .map(|(decl, required, constraint)| AttrUse {
                decl,
                required,
                constraint,
            })
            .collect();
        id
    }

    fn uses_of(&self, ty: usize) -> Uses {
        self.schema
            .complex(ty)
            .map(|c| {
                c.attr_uses
                    .iter()
                    .map(|u| (u.decl, u.required, u.constraint.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The `xsd:extension` of simple or complex content.
    fn derivation<'b, 'j>(n: Node<'b, 'j>) -> Node<'b, 'j> {
        only(n, &[]);
        let d = xsd_children(n).next().expect("derivation");
        assert!(
            is_xsd(&d, "extension"),
            "unsupported xsd:{}",
            d.tag_name().name()
        );
        d
    }

    /// `processComplexContent`: the particle (`None` for empty content) and
    /// the attribute uses of what `n` holds.
    fn content(&mut self, n: Node<'a, 'i>) -> (Option<Particle>, Uses) {
        let mut children = xsd_children(n).peekable();
        let mut particle = None;
        if let Some(c) = children.peek().copied()
            && matches!(c.tag_name().name(), "sequence" | "choice" | "all" | "group")
        {
            children.next();
            let empty_group = xsd_children(c).next().is_none();
            let p = self.particle(c);
            // An empty sequence or all, or an optional empty choice, is no
            // particle.
            let empty = empty_group
                && (c.tag_name().name() != "choice" || p.as_ref().is_some_and(|p| p.min == 0));
            particle = if empty { None } else { p };
        }
        let uses = self.attributes(children);
        (particle, uses)
    }

    /// `traverseAttrsAndAttrGrps`: attributes and attribute groups, in order.
    fn attributes(&mut self, nodes: impl Iterator<Item = Node<'a, 'i>>) -> Uses {
        let mut uses: Uses = Vec::new();
        for c in nodes {
            match c.tag_name().name() {
                "attribute" => {
                    let u = self.attribute_use(c);
                    if !uses.iter().any(|x| x.0 == u.0) {
                        uses.push(u);
                    }
                }
                "attributeGroup" => {
                    only(c, &["ref"]);
                    let r = Self::qname(c, c.attribute("ref").expect("ref"));
                    self.global(&Kind::AttributeGroup, &r);
                    for u in self.groups[&r].clone() {
                        if !uses.iter().any(|x| x.0 == u.0) {
                            uses.push(u);
                        }
                    }
                }
                other => panic!("unsupported xsd:{other} among attributes"),
            }
        }
        uses
    }

    fn constraint(n: Node<'_, '_>) -> Option<Constraint> {
        match (n.attribute("default"), n.attribute("fixed")) {
            (Some(v), None) => Some(Constraint {
                fixed: false,
                value: v.to_owned(),
            }),
            (None, Some(v)) => Some(Constraint {
                fixed: true,
                value: v.to_owned(),
            }),
            (None, None) => None,
            _ => panic!("both default and fixed"),
        }
    }

    fn attribute_use(&mut self, n: Node<'a, 'i>) -> (usize, bool, Option<Constraint>) {
        only(n, &["name", "ref", "type", "use", "default", "fixed"]);
        let required = match n.attribute("use").unwrap_or("optional") {
            "optional" => false,
            "required" => true,
            other => panic!("unsupported use {other}"),
        };
        let decl = match n.attribute("ref") {
            Some(r) => {
                let qn = Self::qname(n, r);
                self.global(&Kind::Attribute, &qn)
            }
            None => self.attribute_decl(n, false),
        };
        let constraint = if n.attribute("ref").is_some() {
            Self::constraint(n)
        } else {
            None
        };
        (decl, required, constraint)
    }

    fn attribute_decl(&mut self, n: Node<'a, 'i>, global: bool) -> usize {
        let name = n.attribute("name").expect("attribute name").to_owned();
        let namespace = if global { Self::target_ns(n) } else { None };
        let ty = match n.attribute("type") {
            Some(t) => self.type_ref(n, t),
            None => match xsd_children(n).next() {
                Some(st) => self.simple_type(st, None),
                None => self.schema.any_simple,
            },
        };
        self.schema.attrs.push(AttrDecl {
            name: name.clone(),
            namespace: namespace.clone(),
            ty,
            constraint: Self::constraint(n),
        });
        let id = self.schema.attrs.len() - 1;
        if global {
            let qn = (namespace, name);
            self.schema.global_attrs.insert(qn.clone(), id);
            self.done.insert((Kind::Attribute, qn), id);
        }
        id
    }

    fn element(&mut self, n: Node<'a, 'i>, global: bool) -> usize {
        only(
            n,
            &[
                "name",
                "type",
                "minOccurs",
                "maxOccurs",
                "default",
                "fixed",
                "abstract",
                "nillable",
                "substitutionGroup",
            ],
        );
        let name = n.attribute("name").expect("element name").to_owned();
        let tns = Self::target_ns(n);
        let qualified = global
            || n.ancestors()
                .find(|a| is_xsd(a, "schema"))
                .and_then(|s| s.attribute("elementFormDefault"))
                == Some("qualified");
        let namespace = if qualified { tns } else { None };
        self.schema.elems.push(ElemDecl {
            name: name.clone(),
            namespace: namespace.clone(),
            ty: self.schema.any_type,
            global,
            abstract_: n.attribute("abstract") == Some("true"),
            nillable: n.attribute("nillable") == Some("true"),
            constraint: Self::constraint(n),
            head: None,
        });
        let id = self.schema.elems.len() - 1;
        if global {
            let qn = (namespace, name);
            self.schema.global_elems.insert(qn.clone(), id);
            self.done.insert((Kind::Element, qn), id);
        }
        let head = n.attribute("substitutionGroup").map(|h| {
            let qn = Self::qname(n, h);
            self.global(&Kind::Element, &qn)
        });
        let ty = match (n.attribute("type"), xsd_children(n).next()) {
            (Some(t), _) => self.type_ref(n, t),
            (None, Some(c)) if is_xsd(&c, "complexType") => self.complex_type(c, None),
            (None, Some(c)) if is_xsd(&c, "simpleType") => self.simple_type(c, None),
            (None, Some(c)) => panic!("unsupported xsd:{} in an element", c.tag_name().name()),
            (None, None) => head.map_or(self.schema.any_type, |h| self.schema.elems[h].ty),
        };
        let e = &mut self.schema.elems[id];
        e.ty = ty;
        e.head = head;
        id
    }

    fn occurs(n: Node<'_, '_>) -> (u32, Option<u32>) {
        let min = n
            .attribute("minOccurs")
            .map_or(1, |v| v.parse().expect("minOccurs"));
        let max = match n.attribute("maxOccurs") {
            None => Some(1),
            Some("unbounded") => None,
            Some(v) => Some(v.parse().expect("maxOccurs")),
        };
        // Xerces counts other bounds with its own messages; the schemas
        // have none.
        assert!(
            min <= 1 && matches!(max, None | Some(1)),
            "unsupported occurrence"
        );
        (min, max)
    }

    fn particle(&mut self, n: Node<'a, 'i>) -> Option<Particle> {
        let (min, max) = Self::occurs(n);
        let term = match n.tag_name().name() {
            "element" => match n.attribute("ref") {
                Some(r) => {
                    only(n, &["ref", "minOccurs", "maxOccurs"]);
                    let qn = Self::qname(n, r);
                    Term::Element(self.global(&Kind::Element, &qn))
                }
                None => Term::Element(self.element(n, false)),
            },
            "any" => {
                only(
                    n,
                    &["namespace", "processContents", "minOccurs", "maxOccurs"],
                );
                let tns = Self::target_ns(n);
                let ns = match n.attribute("namespace").unwrap_or("##any") {
                    "##any" => NsConstraint::Any,
                    "##other" => NsConstraint::Not(vec![tns, None]),
                    list => NsConstraint::List(
                        list.split_whitespace()
                            .map(|v| match v {
                                "##targetNamespace" => tns.clone(),
                                "##local" => None,
                                uri => Some(uri.to_owned()),
                            })
                            .collect(),
                    ),
                };
                let process = match n.attribute("processContents").unwrap_or("strict") {
                    "strict" => Process::Strict,
                    "lax" => Process::Lax,
                    "skip" => Process::Skip,
                    other => panic!("processContents {other}"),
                };
                Term::Wildcard(Wildcard { ns, process })
            }
            group @ ("sequence" | "choice" | "all") => {
                only(n, &["minOccurs", "maxOccurs"]);
                let compositor = match group {
                    "sequence" => Compositor::Sequence,
                    "choice" => Compositor::Choice,
                    _ => Compositor::All,
                };
                let children: Vec<Node<'a, 'i>> = xsd_children(n).collect();
                let particles = children
                    .into_iter()
                    .filter_map(|c| self.particle(c))
                    .collect();
                Term::Group(compositor, particles)
            }
            other => panic!("unsupported particle xsd:{other}"),
        };
        Some(Particle { min, max, term })
    }
}

fn wrap(p: Particle, mixed: bool) -> Content {
    if mixed {
        Content::Mixed(p)
    } else {
        Content::Element(p)
    }
}

fn clone_particle(p: &Particle) -> Particle {
    Particle {
        min: p.min,
        max: p.max,
        term: match &p.term {
            Term::Element(e) => Term::Element(*e),
            Term::Wildcard(w) => Term::Wildcard(w.clone()),
            Term::Group(c, ps) => Term::Group(*c, ps.iter().map(clone_particle).collect()),
        },
    }
}

/// `mergeAttributes`: the base's uses the derived type does not have,
/// after the derived type's own.
fn merge(schema: &Schema, uses: &mut Uses, base: &mut Uses) {
    for b in base.drain(..) {
        let decl = &schema.attrs[b.0];
        let overridden = uses.iter().any(|u| {
            let d = &schema.attrs[u.0];
            d.name == decl.name && d.namespace == decl.namespace
        });
        if !overridden {
            uses.push(b);
        }
    }
}
