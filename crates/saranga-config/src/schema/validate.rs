//! An element validated as Xerces's `XMLSchemaValidator` validates the
//! events `DOMValidatorHelper` sends it for a DOM node, up to the first
//! error, where the JAXP validator's default error handler throws; or, for
//! JAXB's `ValidatingUnmarshaller`, the events its `StAXStreamConnector`
//! sends: the attributes in the document's order, as a filter on the
//! reader gives their values, and each error where the reader is.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::HashSet;

use roxmltree::{Attribute, Node};

use super::Fail;
use super::cm::{ContentModel, Matched, State};
use super::datatype::{Context, Dv, Types, Value, Variety, fail};
use super::model::{Content, Process, Schema, Type, Wildcard, XSI_NS};

const XSI_NIL: &str = "http://www.w3.org/2001/XMLSchema-instance,nil";

impl Schema {
    /// `SubstitutionGroupHandler.getMatchingElemDecl`: the declaration of
    /// the element named `ns`/`local` where `exemplar` is expected.
    pub(super) fn matching_decl(
        &self,
        ns: Option<&str>,
        local: &str,
        exemplar: usize,
    ) -> Option<usize> {
        let e = &self.elems[exemplar];
        if e.name == local && e.namespace.as_deref() == ns {
            return Some(exemplar);
        }
        if !e.global || !self.has_grammar(ns) {
            return None;
        }
        let decl = self.global_elem(ns, local)?;
        self.substitutes(decl, exemplar).then_some(decl)
    }

    /// `substitutionGroupOK`.
    fn substitutes(&self, elem: usize, exemplar: usize) -> bool {
        if elem == exemplar {
            return true;
        }
        let Some(head) = self.elems[elem].head else {
            return false;
        };
        self.derives(self.elems[elem].ty, self.elems[exemplar].ty)
            && self.substitutes(head, exemplar)
    }

    /// `checkTypeDerivationOk` without blocking: whether `derived` is
    /// `base` or derived from it.
    pub(super) fn derives(&self, derived: usize, base: usize) -> bool {
        if base == self.any_type {
            return true;
        }
        let mut t = Some(derived);
        while let Some(id) = t {
            if id == base {
                return true;
            }
            t = match &self.types[id] {
                Type::Complex(c) => c.base,
                Type::Simple(s) => s.base.or((id == self.any_simple).then_some(self.any_type)),
            };
        }
        // A union's members (Type Derivation OK (Simple) 2.2.4).
        if let Type::Simple(b) = &self.types[base]
            && let Variety::Union(members) = &b.variety
        {
            return members.iter().any(|&m| self.derives(derived, m));
        }
        false
    }

    /// `XSElementDecl.toString`: `"namespace":name`.
    pub(super) fn describe_elem(&self, e: usize) -> String {
        let e = &self.elems[e];
        match &e.namespace {
            Some(ns) => format!("\"{ns}\":{}", e.name),
            None => e.name.clone(),
        }
    }
}

pub(super) struct Validator<'s, 't> {
    schema: &'s Schema,
    text: &'t str,
    ids: HashSet<String>,
    idrefs: Vec<String>,
    /// The events of a StAX reader, with this filter on attribute values,
    /// rather than of a DOM.
    stream: Option<&'t dyn Fn(&str) -> String>,
}

/// What `handleStartElement` makes of an element.
struct Opened<'s> {
    raw: String,
    decl: Option<usize>,
    ty: usize,
    complex: Option<&'s super::model::ComplexType>,
    cm: Option<&'s ContentModel>,
    state: Option<State>,
    nil: bool,
}

/// What an element's children were seen to hold.
struct Seen {
    text: bool,
    characters: bool,
    elements: bool,
    buffer: String,
}

impl<'s, 't> Validator<'s, 't> {
    pub(super) fn new(schema: &'s Schema, text: &'t str) -> Self {
        Self {
            schema,
            text,
            ids: HashSet::new(),
            idrefs: Vec::new(),
            stream: None,
        }
    }

    /// A validator of a StAX reader's events, `filter` giving the values of
    /// attributes.
    pub(super) fn streaming(
        schema: &'s Schema,
        text: &'t str,
        filter: &'t dyn Fn(&str) -> String,
    ) -> Self {
        Self {
            stream: Some(filter),
            ..Self::new(schema, text)
        }
    }

    /// An attribute's value as the validator gets it.
    fn value<'a>(&self, a: &Attribute<'a, '_>) -> Cow<'a, str> {
        match self.stream {
            Some(filter) => Cow::Owned(filter(a.value())),
            None => Cow::Borrowed(a.value()),
        }
    }

    /// The value of an attribute of `node`, as [`value`](Self::value).
    fn attribute<'a>(&self, node: Node<'a, '_>, name: (&str, &str)) -> Option<Cow<'a, str>> {
        node.attributes()
            .find(|a| a.namespace() == Some(name.0) && a.name() == name.1)
            .map(|a| self.value(&a))
    }

    /// The validation root, then Validation Root Valid (ID/IDREF).
    pub(super) fn root(&mut self, node: Node<'_, '_>) -> Result<(), Fail> {
        self.element(node, None, None, 0)?;
        match first_missing(&self.idrefs, &self.ids) {
            Some(missing) => Err(fail("cvc-id.1", vec![missing]).at(node.range().end)),
            None => Ok(()),
        }
    }

    /// An element's qualified name as written.
    fn raw_name(&self, node: Node<'_, '_>) -> String {
        let start = node.range().start + 1;
        let rest = &self.text[start..];
        let end = rest
            .find(|c: char| c.is_whitespace() || c == '/' || c == '>')
            .unwrap_or(rest.len());
        rest[..end].to_owned()
    }

    fn attr_raw_name(&self, a: &Attribute<'_, '_>) -> String {
        self.text[a.range_qname()].to_owned()
    }

    fn context<'c>(
        ids: &'c mut HashSet<String>,
        idrefs: &'c mut Vec<String>,
        resolve: &'c dyn Fn(Option<&str>) -> Option<String>,
    ) -> Context<'c> {
        Context {
            ids,
            idrefs,
            resolve,
        }
    }

    fn validate_simple(
        &mut self,
        node: Node<'_, '_>,
        ty: usize,
        content: &str,
    ) -> Result<Value, Fail> {
        let resolve = |p: Option<&str>| node.lookup_namespace_uri(p).map(str::to_owned);
        let schema = self.schema;
        let mut ctx = Self::context(&mut self.ids, &mut self.idrefs, &resolve);
        schema.simple(ty).validate(schema, content, &mut ctx)
    }

    /// `handleStartElement`, the children, then `handleEndElement`, an
    /// error at the end of the start tag or of the element, where a StAX
    /// reader is when it reports it.
    fn element(
        &mut self,
        node: Node<'_, '_>,
        decl: Option<usize>,
        wildcard: Option<&Wildcard>,
        depth: usize,
    ) -> Result<(), Fail> {
        let start = start_tag_end(self.text, node.range().start);
        let Opened {
            raw,
            decl,
            ty,
            complex,
            cm,
            mut state,
            nil,
        } = self
            .open(node, decl, wildcard, depth)
            .map_err(|f| f.at(start))?;
        let schema = self.schema;
        let fixed =
            decl.is_some_and(|d| schema.elems[d].constraint.as_ref().is_some_and(|c| c.fixed));
        let append = fixed
            || match complex {
                None => true,
                Some(c) => matches!(c.content, Content::Simple(_)),
            };
        let element_only = complex.is_some_and(|c| matches!(c.content, Content::Element(_)));
        let mut seen = Seen {
            text: false,
            characters: false,
            elements: false,
            buffer: String::new(),
        };
        for child in node.children() {
            if child.is_text() {
                let t = child.text().unwrap_or("");
                seen.text |= !t.is_empty();
                if append {
                    seen.buffer.push_str(t);
                }
                if element_only && t.chars().any(|c| !matches!(c, ' ' | '\t' | '\n' | '\r')) {
                    seen.characters = true;
                }
            } else if child.is_element() {
                seen.elements = true;
                let (child_decl, child_wildcard) = match (cm, state.as_mut()) {
                    (Some(m), Some(s)) => self
                        .transition(m, s, child)
                        .map_err(|f| f.at(start_tag_end(self.text, child.range().start)))?,
                    _ => (None, None),
                };
                if child_wildcard
                    .as_ref()
                    .is_some_and(|w| w.process == Process::Skip)
                {
                    continue;
                }
                self.element(child, child_decl, child_wildcard.as_ref(), depth + 1)?;
            }
        }
        self.element_content(node, &raw, decl, ty, nil, &seen, cm, state.as_ref())
            .map_err(|f| f.at(node.range().end))
    }

    /// `handleStartElement` up to the element's children.
    fn open(
        &mut self,
        node: Node<'_, '_>,
        decl: Option<usize>,
        wildcard: Option<&Wildcard>,
        depth: usize,
    ) -> Result<Opened<'s>, Fail> {
        let schema = self.schema;
        let raw = self.raw_name(node);
        let ns = element_ns(node);
        let local = node.tag_name().name();
        let mut decl = decl;
        if decl.is_none() && schema.has_grammar(ns) {
            decl = schema.global_elem(ns, local);
        }
        let mut ty = decl.map(|d| schema.elems[d].ty);
        let xsi_type = self.attribute(node, (XSI_NS, "type"));
        match xsi_type {
            None if ty.is_none() => {
                if depth == 0 {
                    return Err(fail("cvc-elt.1.a", vec![raw]));
                }
                if wildcard.is_some_and(|w| w.process == Process::Strict) {
                    return Err(fail("cvc-complex-type.2.4.c", vec![raw]));
                }
                ty = Some(schema.any_type);
            }
            None => {}
            Some(x) => ty = Some(self.xsi_type(node, &raw, &x, ty)?),
        }
        let ty = ty.expect("a type");
        if decl.is_some_and(|d| schema.elems[d].abstract_) {
            return Err(fail("cvc-elt.2", vec![raw]));
        }
        let complex = schema.complex(ty);
        if complex.is_some_and(|c| c.abstract_) {
            return Err(fail("cvc-type.2", vec![raw]));
        }
        let cm = complex.and_then(|c| {
            c.cm.get_or_init(|| match &c.content {
                Content::Element(p) | Content::Mixed(p) => Some(ContentModel::build(p)),
                _ => None,
            })
            .as_ref()
        });
        let state = cm.map(ContentModel::start);
        let mut nil = false;
        if let (Some(v), Some(d)) = (self.attribute(node, (XSI_NS, "nil")), decl) {
            nil = self.xsi_nil(&raw, &v, d)?;
        }
        self.attributes(node, &raw, ty)?;
        Ok(Opened {
            raw,
            decl,
            ty,
            complex,
            cm,
            state,
            nil,
        })
    }

    /// A child's transition in its parent's content model, with the errors
    /// `handleStartElement` reports for it.
    fn transition(
        &self,
        cm: &ContentModel,
        state: &mut State,
        child: Node<'_, '_>,
    ) -> Result<(Option<usize>, Option<Wildcard>), Fail> {
        let ns = element_ns(child);
        let local = child.tag_name().name();
        match cm.transition(self.schema, state, ns, local) {
            Some(Matched::Element(d)) => Ok((Some(d), None)),
            Some(Matched::Wildcard(w)) => Ok((None, Some(w))),
            None => Err(match cm.expected(self.schema, state) {
                Some(expected) => {
                    let name = match ns {
                        Some(uri) => format!("{{\"{uri}\":{local}}}"),
                        None => local.to_owned(),
                    };
                    fail("cvc-complex-type.2.4.a", vec![name, expected])
                }
                None => fail("cvc-complex-type.2.4.d", vec![self.raw_name(child)]),
            }),
        }
    }

    /// `getAndCheckXsiType`.
    fn xsi_type(
        &mut self,
        node: Node<'_, '_>,
        raw: &str,
        value: &str,
        current: Option<usize>,
    ) -> Result<usize, Fail> {
        let schema = self.schema;
        let qname = schema
            .global_type(Some(super::datatype::XSD_NS), "QName")
            .expect("QName");
        let Value::QName(uri, local) = self.validate_simple(node, qname, value)? else {
            unreachable!("a QName's value")
        };
        let mut ty = None;
        if uri.as_deref() == Some(super::datatype::XSD_NS) {
            ty = schema.global_type(uri.as_deref(), &local);
        }
        if ty.is_none() && schema.has_grammar(uri.as_deref()) {
            ty = schema.global_type(uri.as_deref(), &local);
        }
        let Some(ty) = ty else {
            return Err(fail("cvc-elt.4.2", vec![raw.to_owned(), value.to_owned()]));
        };
        if let Some(cur) = current
            && !schema.derives(ty, cur)
        {
            return Err(fail(
                "cvc-elt.4.3",
                vec![
                    raw.to_owned(),
                    value.to_owned(),
                    schema.type_name(cur).to_owned(),
                ],
            ));
        }
        Ok(ty)
    }

    /// `getXsiNil`.
    fn xsi_nil(&self, raw: &str, value: &str, decl: usize) -> Result<bool, Fail> {
        let d = &self.schema.elems[decl];
        if !d.nillable {
            return Err(fail(
                "cvc-elt.3.1",
                vec![raw.to_owned(), XSI_NIL.to_owned()],
            ));
        }
        let v = value.trim_matches(|c| matches!(c, ' ' | '\t' | '\n' | '\r'));
        if v == "true" || v == "1" {
            if d.constraint.as_ref().is_some_and(|c| c.fixed) {
                return Err(fail(
                    "cvc-elt.3.2.2",
                    vec![raw.to_owned(), XSI_NIL.to_owned()],
                ));
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// `processAttributes` and the required attributes of
    /// `addDefaultAttributes`, the attributes in the order of the DOM's
    /// attribute map, sorted by name, or a reader's, as written.
    fn attributes(&mut self, node: Node<'_, '_>, raw: &str, ty: usize) -> Result<(), Fail> {
        let schema = self.schema;
        let complex = schema.complex(ty);
        let mut attrs: Vec<(String, Attribute<'_, '_>)> = node
            .attributes()
            .map(|a| (self.attr_raw_name(&a), a))
            .collect();
        if self.stream.is_none() {
            attrs.sort_by(|a, b| java_compare(&a.0, &b.0));
        }
        let mut wild_id: Option<String> = None;
        for (araw, a) in &attrs {
            let ans = a.namespace();
            let alocal = a.name();
            if ans == Some(XSI_NS)
                && let Some(decl) = schema.global_attr(Some(XSI_NS), alocal)
            {
                let v = self.value(a);
                self.one_attribute(node, raw, araw, &v, decl, None)?;
                continue;
            }
            let Some(c) = complex else {
                return Err(fail("cvc-type.3.1.1", vec![raw.to_owned(), araw.clone()]));
            };
            let found = c.attr_uses.iter().position(|u| {
                let d = &schema.attrs[u.decl];
                d.name == alocal && d.namespace.as_deref() == ans
            });
            let decl = match found {
                Some(i) => c.attr_uses[i].decl,
                None => {
                    let not_allowed =
                        || fail("cvc-complex-type.3.2.2", vec![raw.to_owned(), araw.clone()]);
                    let w = match &c.attr_wildcard {
                        Some(w) if w.allows(ans) => w,
                        _ => return Err(not_allowed()),
                    };
                    if w.process == Process::Skip {
                        continue;
                    }
                    let d = if schema.has_grammar(ans) {
                        schema.global_attr(ans, alocal)
                    } else {
                        None
                    };
                    let Some(d) = d else {
                        if w.process == Process::Strict {
                            return Err(not_allowed());
                        }
                        continue;
                    };
                    let decl = &schema.attrs[d];
                    if schema.simple(decl.ty).dv() == Some(Dv::Id) {
                        if let Some(prev) = &wild_id {
                            return Err(fail(
                                "cvc-complex-type.5.1",
                                vec![raw.to_owned(), decl.name.clone(), prev.clone()],
                            ));
                        }
                        wild_id = Some(decl.name.clone());
                    }
                    d
                }
            };
            let v = self.value(a);
            self.one_attribute(node, raw, araw, &v, decl, found.map(|i| &c.attr_uses[i]))?;
        }
        if let (Some(c), Some(w)) = (complex, &wild_id)
            && let Some(id) = &c.id_attr
        {
            return Err(fail(
                "cvc-complex-type.5.2",
                vec![raw.to_owned(), w.clone(), id.clone()],
            ));
        }
        if let Some(c) = complex {
            for u in c.attr_uses.iter().filter(|u| u.required) {
                let d = &schema.attrs[u.decl];
                let specified = node
                    .attributes()
                    .any(|a| a.name() == d.name && a.namespace() == d.namespace.as_deref());
                if !specified {
                    return Err(match &d.namespace {
                        Some(ns) => fail(
                            "cvc-complex-type.4_ns",
                            vec![raw.to_owned(), d.name.clone(), ns.clone()],
                        ),
                        None => fail("cvc-complex-type.4", vec![raw.to_owned(), d.name.clone()]),
                    });
                }
            }
        }
        Ok(())
    }

    /// `processOneAttribute`.
    fn one_attribute(
        &mut self,
        node: Node<'_, '_>,
        raw: &str,
        araw: &str,
        value: &str,
        decl: usize,
        attr_use: Option<&super::model::AttrUse>,
    ) -> Result<(), Fail> {
        let schema = self.schema;
        let d = &schema.attrs[decl];
        let actual = self.validate_simple(node, d.ty, value)?;
        let fixed_of = |c: &Option<super::model::Constraint>| {
            c.as_ref().filter(|c| c.fixed).map(|c| c.value.clone())
        };
        if let Some(fixed) = fixed_of(&d.constraint)
            && schema
                .simple(d.ty)
                .facet_value(schema, &fixed)
                .ok()
                .map(|v| v.0)
                != Some(actual.clone())
        {
            return Err(fail(
                "cvc-attribute.4",
                vec![raw.to_owned(), araw.to_owned(), value.to_owned(), fixed],
            ));
        }
        if let Some(fixed) = attr_use.and_then(|u| fixed_of(&u.constraint))
            && schema
                .simple(d.ty)
                .facet_value(schema, &fixed)
                .ok()
                .map(|v| v.0)
                != Some(actual)
        {
            return Err(fail(
                "cvc-complex-type.3.1",
                vec![raw.to_owned(), araw.to_owned(), value.to_owned(), fixed],
            ));
        }
        Ok(())
    }

    /// `processElementContent`.
    #[allow(clippy::too_many_arguments)]
    fn element_content(
        &mut self,
        node: Node<'_, '_>,
        raw: &str,
        decl: Option<usize>,
        ty: usize,
        nil: bool,
        seen: &Seen,
        cm: Option<&ContentModel>,
        state: Option<&State>,
    ) -> Result<(), Fail> {
        let schema = self.schema;
        if nil && (seen.elements || seen.text) {
            return Err(fail(
                "cvc-elt.3.2.1",
                vec![raw.to_owned(), XSI_NIL.to_owned()],
            ));
        }
        let constraint = decl.and_then(|d| schema.elems[d].constraint.clone());
        match constraint {
            Some(c) if !seen.elements && !seen.text && !nil => {
                let d = decl.expect("a declaration");
                if ty != schema.elems[d].ty && !self.default_valid(ty, &c.value) {
                    return Err(fail(
                        "cvc-elt.5.1.1",
                        vec![
                            raw.to_owned(),
                            schema.type_name(ty).to_owned(),
                            c.value.clone(),
                        ],
                    ));
                }
                self.locally_valid_type(node, raw, ty, nil, seen, &c.value, cm, state)?;
            }
            constraint => {
                let actual =
                    self.locally_valid_type(node, raw, ty, nil, seen, &seen.buffer, cm, state)?;
                if let Some(c) = constraint.filter(|c| c.fixed && !nil) {
                    if seen.elements {
                        return Err(fail("cvc-elt.5.2.2.1", vec![raw.to_owned()]));
                    }
                    let content_type = schema.complex(ty).map(|t| &t.content);
                    if let Some(Content::Mixed(_)) = content_type {
                        if c.value != seen.buffer {
                            return Err(fail(
                                "cvc-elt.5.2.2.2.1",
                                vec![raw.to_owned(), seen.buffer.clone(), c.value],
                            ));
                        }
                    } else if let Some(actual) = actual {
                        let simple = match content_type {
                            Some(Content::Simple(s)) => Some(*s),
                            None => Some(ty),
                            _ => None,
                        };
                        if let Some(s) = simple
                            && schema
                                .simple(s)
                                .facet_value(schema, &c.value)
                                .ok()
                                .map(|v| v.0)
                                != Some(actual)
                        {
                            return Err(fail(
                                "cvc-elt.5.2.2.2.2",
                                vec![raw.to_owned(), seen.buffer.clone(), c.value],
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// `XSConstraints.ElementDefaultValidImmediate`.
    fn default_valid(&self, ty: usize, value: &str) -> bool {
        let simple = match self.schema.complex(ty) {
            None => Some(ty),
            Some(c) => match &c.content {
                Content::Simple(s) => Some(*s),
                Content::Mixed(_) => return true,
                _ => return false,
            },
        };
        let schema = self.schema;
        simple.is_some_and(|s| schema.simple(s).facet_value(schema, value).is_ok())
    }

    /// `elementLocallyValidType`: the content's value, when it is simple.
    #[allow(clippy::too_many_arguments)]
    fn locally_valid_type(
        &mut self,
        node: Node<'_, '_>,
        raw: &str,
        ty: usize,
        nil: bool,
        seen: &Seen,
        content: &str,
        cm: Option<&ContentModel>,
        state: Option<&State>,
    ) -> Result<Option<Value>, Fail> {
        let schema = self.schema;
        let Some(c) = schema.complex(ty) else {
            if seen.elements {
                return Err(fail("cvc-type.3.1.2", vec![raw.to_owned()]));
            }
            if nil {
                return Ok(None);
            }
            return self.validate_simple(node, ty, content).map(Some);
        };
        if nil {
            return Ok(None);
        }
        match &c.content {
            Content::Empty => {
                if seen.elements || seen.text {
                    return Err(fail("cvc-complex-type.2.1", vec![raw.to_owned()]));
                }
                Ok(None)
            }
            Content::Simple(s) => {
                if seen.elements {
                    return Err(fail("cvc-complex-type.2.2", vec![raw.to_owned()]));
                }
                self.validate_simple(node, *s, content).map(Some)
            }
            Content::Element(_) | Content::Mixed(_) => {
                if matches!(c.content, Content::Element(_)) && seen.characters {
                    return Err(fail("cvc-complex-type.2.3", vec![raw.to_owned()]));
                }
                if let (Some(cm), Some(state)) = (cm, state)
                    && !cm.can_end(state)
                {
                    let expected = cm
                        .expected(schema, state)
                        .unwrap_or_else(|| "{}".to_owned());
                    return Err(fail(
                        "cvc-complex-type.2.4.b",
                        vec![raw.to_owned(), expected],
                    ));
                }
                Ok(None)
            }
        }
    }
}

/// Where a start tag beginning at `start` ends: after its `>`.
fn start_tag_end(text: &str, start: usize) -> usize {
    let mut quote = None;
    for (i, &b) in text.as_bytes()[start..].iter().enumerate() {
        match (quote, b) {
            (None, b'"' | b'\'') => quote = Some(b),
            (Some(q), b) if q == b => quote = None,
            (None, b'>') => return start + i + 1,
            _ => {}
        }
    }
    text.len()
}

/// An element's namespace, `xmlns=""` being none.
fn element_ns<'a>(n: Node<'a, '_>) -> Option<&'a str> {
    n.tag_name().namespace().filter(|ns| !ns.is_empty())
}

/// `String.compareTo`: UTF-16 code units in order.
fn java_compare(a: &str, b: &str) -> Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

/// `String.hashCode`.
fn java_hash(s: &str) -> i32 {
    s.encode_utf16()
        .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(i32::from(c)))
}

/// `ValidationState.checkIDRefID`'s first IDREF without its ID: the first
/// of the `HashSet` it gathers them in, in a `HashSet`'s iteration order
/// (by bucket, then as added).
fn first_missing(idrefs: &[String], ids: &HashSet<String>) -> Option<String> {
    let mut missing: Vec<&String> = Vec::new();
    for r in idrefs {
        if !ids.contains(r) && !missing.contains(&r) {
            missing.push(r);
        }
    }
    let mut capacity = 16usize;
    for size in 1..=missing.len() {
        if size > capacity * 3 / 4 {
            capacity *= 2;
        }
    }
    let bucket = |s: &str| {
        let h = java_hash(s);
        let spread = h ^ ((h as u32) >> 16) as i32;
        (spread as u32 as usize) & (capacity - 1)
    };
    missing
        .iter()
        .enumerate()
        .min_by_key(|(i, s)| (bucket(s), *i))
        .map(|(_, s)| (*s).clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_hash_matches() {
        assert_eq!(java_hash(""), 0);
        assert_eq!(java_hash("a"), 97);
        assert_eq!(java_hash("hello"), 99162322);
        assert_eq!(java_hash("polygenelubricants"), i32::MIN);
    }

    #[test]
    fn first_missing_follows_hash_order() {
        let ids = HashSet::new();
        // "b" (98) hashes to bucket 2 and "a" (97) to 1.
        let refs = vec!["b".to_owned(), "a".to_owned()];
        assert_eq!(first_missing(&refs, &ids).as_deref(), Some("a"));
        assert_eq!(first_missing(&[], &ids), None);
    }
}
