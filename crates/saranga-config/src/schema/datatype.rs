//! Simple types as Xerces's `XSSimpleTypeDecl` validates them: the
//! built-ins the schemas use, and restrictions, lists and unions of them.

use std::cmp::Ordering;
use std::collections::HashSet;

use regex::Regex;

use super::Fail;
use super::ncname::{NCNAME_CHAR, NCNAME_START};

pub(super) const XSD_NS: &str = "http://www.w3.org/2001/XMLSchema";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Ws {
    Preserve,
    Replace,
    Collapse,
}

/// The `TypeValidator` a type validates its lexical form with.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Dv {
    AnySimple,
    String,
    Boolean,
    Decimal,
    Integer,
    Double,
    AnyUri,
    QName,
    Id,
    IdRef,
}

impl Dv {
    /// The primitive kind values of this validator compare as
    /// (`convertToPrimitiveKind`).
    fn primitive(self) -> u8 {
        match self {
            Dv::AnySimple | Dv::String | Dv::Id | Dv::IdRef => 0,
            Dv::Boolean => 1,
            Dv::Decimal | Dv::Integer => 2,
            Dv::Double => 3,
            Dv::AnyUri => 4,
            Dv::QName => 5,
        }
    }
}

/// `fPatternType`: the patterns of the name types, checked by hand.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Special {
    None,
    NcName,
}

#[derive(Clone, Debug)]
pub(super) enum Variety {
    Atomic(Dv),
    List(usize),
    Union(Vec<usize>),
}

/// An actual value.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Value {
    Str(String),
    Bool(bool),
    Dec(Decimal),
    Double(f64),
    QName(Option<String>, String),
    List(Vec<Value>),
}

#[derive(Clone, Debug)]
pub(super) struct SimpleType {
    /// `fTypeName`: the name, or `#AnonType_...` for an anonymous one.
    pub name: String,
    pub namespace: Option<String>,
    /// The base type: `None` only for `anySimpleType`, whose base is
    /// `anyType`.
    pub base: Option<usize>,
    pub variety: Variety,
    pub whitespace: Ws,
    pub special: Special,
    /// `pattern` facets, as written and compiled; the last is checked
    /// first, as Xerces does.
    pub patterns: Vec<(String, Regex)>,
    /// `enumeration`: the actual values, with the primitive kind of each.
    pub enumeration: Option<Vec<(Value, u8)>>,
    pub max_inclusive: Option<(Decimal, String)>,
    pub min_inclusive: Option<(Decimal, String)>,
}

/// What `ValidationState` keeps of a validation: the IDs and IDREFs seen,
/// and the namespaces in scope for a QName.
pub(super) struct Context<'a> {
    pub ids: &'a mut HashSet<String>,
    pub idrefs: &'a mut Vec<String>,
    pub resolve: &'a dyn Fn(Option<&str>) -> Option<String>,
}

pub(super) fn fail(key: &'static str, args: Vec<String>) -> Fail {
    Fail { key, args }
}

/// The schema-wide table of simple types the validator indexes into.
pub(super) trait Types {
    fn simple(&self, id: usize) -> &SimpleType;
}

impl SimpleType {
    pub(super) fn atomic(name: &str, base: Option<usize>, dv: Dv, ws: Ws) -> Self {
        Self {
            name: name.to_owned(),
            namespace: Some(XSD_NS.to_owned()),
            base,
            variety: Variety::Atomic(dv),
            whitespace: ws,
            special: Special::None,
            patterns: Vec::new(),
            enumeration: None,
            max_inclusive: None,
            min_inclusive: None,
        }
    }

    /// The validator of an atomic type (`fValidationDV`).
    pub(super) fn dv(&self) -> Option<Dv> {
        match self.variety {
            Variety::Atomic(dv) => Some(dv),
            _ => None,
        }
    }

    pub(super) fn is_id(&self) -> bool {
        self.dv() == Some(Dv::Id)
    }

    fn has_facets(&self) -> bool {
        self.enumeration.is_some() || self.max_inclusive.is_some() || self.min_inclusive.is_some()
    }

    /// `validate(String, ValidationContext, ValidatedInfo)`: the actual
    /// value, its facets and the ID rules.
    pub(super) fn validate(
        &self,
        types: &dyn Types,
        content: &str,
        ctx: &mut Context<'_>,
    ) -> Result<Value, Fail> {
        let (value, normalized, kind) = self.actual_value(types, content, ctx)?;
        if self.has_facets() {
            self.check_facets(&value, &normalized, kind)?;
        }
        self.check_extra_rules(types, &value, ctx)?;
        Ok(value)
    }

    /// `getActualValue`: the normalized value through the patterns and
    /// the type's validator, with the primitive kind of the value.
    fn actual_value(
        &self,
        types: &dyn Types,
        content: &str,
        ctx: &mut Context<'_>,
    ) -> Result<(Value, String, u8), Fail> {
        let nvalue = normalize(content, self.whitespace);
        for (source, regex) in self.patterns.iter().rev() {
            if !regex.is_match(&nvalue) {
                return Err(fail(
                    "cvc-pattern-valid",
                    vec![content.to_owned(), source.clone(), self.name.clone()],
                ));
            }
        }
        match &self.variety {
            Variety::Atomic(dv) => {
                if self.special == Special::NcName && !is_ncname(&nvalue) {
                    return Err(fail(
                        "cvc-datatype-valid.1.2.1",
                        vec![nvalue, "NCName".to_owned()],
                    ));
                }
                let value = dv_value(*dv, &nvalue, ctx)?;
                Ok((value, nvalue, dv.primitive()))
            }
            Variety::List(item) => {
                let item = types.simple(*item);
                let mut values = Vec::new();
                for token in nvalue.split(' ').filter(|t| !t.is_empty()) {
                    let (v, n, kind) = item.actual_value(types, token, ctx)?;
                    if item.has_facets() {
                        item.check_facets(&v, &n, kind)?;
                    }
                    values.push(v);
                }
                Ok((Value::List(values), nvalue, 6))
            }
            Variety::Union(members) => {
                for &m in members {
                    let member = types.simple(m);
                    if let Ok((v, n, kind)) = member.actual_value(types, content, ctx)
                        && (!member.has_facets() || member.check_facets(&v, &n, kind).is_ok())
                    {
                        return Ok((v, n, kind));
                    }
                }
                let mut names = String::new();
                for (i, &m) in members.iter().enumerate() {
                    let member = types.simple(m);
                    if i != 0 {
                        names.push_str(" | ");
                    }
                    if let Some(ns) = &member.namespace {
                        names.push('{');
                        names.push_str(ns);
                        names.push('}');
                    }
                    names.push_str(&member.name);
                    if let Some(e) = &member.enumeration {
                        names.push_str(" : ");
                        names.push_str(&enum_string(e));
                    }
                }
                Err(fail(
                    "cvc-datatype-valid.1.2.3",
                    vec![content.to_owned(), self.name.clone(), names],
                ))
            }
        }
    }

    /// `checkFacets`: enumeration, then the inclusive bounds.
    fn check_facets(&self, value: &Value, normalized: &str, kind: u8) -> Result<(), Fail> {
        if let Some(e) = &self.enumeration {
            let present = e.iter().any(|(v, k)| *k == kind && v == value);
            if !present {
                return Err(fail(
                    "cvc-enumeration-valid",
                    vec![normalized.to_owned(), enum_string(e)],
                ));
            }
        }
        if let (Some((max, lexical)), Value::Dec(d)) = (&self.max_inclusive, value)
            && d.cmp(max) == Ordering::Greater
        {
            return Err(fail(
                "cvc-maxInclusive-valid",
                vec![normalized.to_owned(), lexical.clone(), self.name.clone()],
            ));
        }
        if let (Some((min, lexical)), Value::Dec(d)) = (&self.min_inclusive, value)
            && d.cmp(min) == Ordering::Less
        {
            return Err(fail(
                "cvc-minInclusive-valid",
                vec![normalized.to_owned(), lexical.clone(), self.name.clone()],
            ));
        }
        Ok(())
    }

    /// `checkExtraRules`: IDs must be unique, IDREFs are kept for the end.
    fn check_extra_rules(
        &self,
        types: &dyn Types,
        value: &Value,
        ctx: &mut Context<'_>,
    ) -> Result<(), Fail> {
        match (&self.variety, value) {
            (Variety::Atomic(Dv::Id), Value::Str(s)) => {
                if !ctx.ids.insert(s.clone()) {
                    return Err(fail("cvc-id.2", vec![s.clone()]));
                }
            }
            (Variety::Atomic(Dv::IdRef), Value::Str(s)) => ctx.idrefs.push(s.clone()),
            (Variety::List(item), Value::List(values)) => {
                let item = types.simple(*item);
                for v in values.iter().rev() {
                    item.check_extra_rules(types, v, ctx)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// The actual value of a facet's lexical form, for `enumeration`
    /// (`getActualEnumValue`: the base type's value).
    pub(super) fn facet_value(
        &self,
        types: &dyn Types,
        lexical: &str,
    ) -> Result<(Value, u8), Fail> {
        let mut ids = HashSet::new();
        let mut idrefs = Vec::new();
        let resolve = |_: Option<&str>| None;
        let mut ctx = Context {
            ids: &mut ids,
            idrefs: &mut idrefs,
            resolve: &resolve,
        };
        let (v, n, kind) = self.actual_value(types, lexical, &mut ctx)?;
        if self.has_facets() {
            self.check_facets(&v, &n, kind)?;
        }
        Ok((v, kind))
    }
}

/// `appendEnumString`: `[a, b, c]`.
fn enum_string(e: &[(Value, u8)]) -> String {
    let items: Vec<String> = e.iter().map(|(v, _)| v.to_string()).collect();
    format!("[{}]", items.join(", "))
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Str(s) => f.write_str(s),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Dec(d) => write!(f, "{d}"),
            Value::Double(d) => write!(f, "{}", java_double(*d)),
            Value::QName(_, l) => f.write_str(l),
            Value::List(items) => {
                let items: Vec<String> = items.iter().map(|v| v.to_string()).collect();
                f.write_str(&items.join(" "))
            }
        }
    }
}

/// `Double.toString` for the values a schema's facets hold.
fn java_double(d: f64) -> String {
    if d.is_nan() {
        "NaN".into()
    } else if d.is_infinite() {
        if d > 0.0 { "INF".into() } else { "-INF".into() }
    } else if d == d.trunc() && d.abs() < 1e7 {
        format!("{d:.1}")
    } else {
        format!("{d:E}")
    }
}

/// `XSSimpleTypeDecl.normalize`.
pub(super) fn normalize(content: &str, ws: Ws) -> String {
    let space = |c: char| matches!(c, ' ' | '\t' | '\n' | '\r');
    match ws {
        Ws::Preserve => content.to_owned(),
        Ws::Replace => content
            .chars()
            .map(|c| if space(c) { ' ' } else { c })
            .collect(),
        Ws::Collapse => {
            let mut out = String::with_capacity(content.len());
            for word in content.split(space).filter(|w| !w.is_empty()) {
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push_str(word);
            }
            out
        }
    }
}

/// The `TypeValidator`s' `getActualValue`.
fn dv_value(dv: Dv, content: &str, ctx: &mut Context<'_>) -> Result<Value, Fail> {
    let invalid = |name: &str| {
        fail(
            "cvc-datatype-valid.1.2.1",
            vec![content.to_owned(), name.to_owned()],
        )
    };
    match dv {
        Dv::AnySimple | Dv::String => Ok(Value::Str(content.to_owned())),
        Dv::Boolean => match content {
            "false" | "0" => Ok(Value::Bool(false)),
            "true" | "1" => Ok(Value::Bool(true)),
            _ => Err(invalid("boolean")),
        },
        Dv::Decimal => Decimal::parse(content, false)
            .map(Value::Dec)
            .ok_or_else(|| invalid("decimal")),
        Dv::Integer => Decimal::parse(content, true)
            .map(Value::Dec)
            .ok_or_else(|| invalid("integer")),
        Dv::Double => parse_double(content)
            .map(Value::Double)
            .ok_or_else(|| invalid("double")),
        Dv::AnyUri => {
            if super::uri::is_valid(content) {
                Ok(Value::Str(content.to_owned()))
            } else {
                Err(invalid("anyURI"))
            }
        }
        Dv::Id | Dv::IdRef => {
            if is_ncname(content) {
                Ok(Value::Str(content.to_owned()))
            } else {
                Err(invalid("NCName"))
            }
        }
        Dv::QName => {
            let (prefix, local) = match content.find(':') {
                Some(i) if i > 0 => (&content[..i], &content[i + 1..]),
                _ => ("", content),
            };
            if (!prefix.is_empty() && !is_ncname(prefix)) || !is_ncname(local) {
                return Err(invalid("QName"));
            }
            let uri = (ctx.resolve)((!prefix.is_empty()).then_some(prefix));
            if !prefix.is_empty() && uri.is_none() {
                return Err(fail(
                    "UndeclaredPrefix",
                    vec![content.to_owned(), prefix.to_owned()],
                ));
            }
            Ok(Value::QName(uri, local.to_owned()))
        }
    }
}

/// `XDouble`: Java's `Double.parseDouble` for strings of digits, signs,
/// points and exponents, else `INF`, `-INF` or `NaN`.
fn parse_double(s: &str) -> Option<f64> {
    match s {
        "INF" => Some(f64::INFINITY),
        "-INF" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        _ if !s.is_empty()
            && s.bytes()
                .all(|c| c.is_ascii_digit() || matches!(c, b'.' | b'-' | b'+' | b'E' | b'e')) =>
        {
            s.parse().ok()
        }
        _ => None,
    }
}

/// `XMLChar.isValidNCName`.
pub(super) fn is_ncname(s: &str) -> bool {
    let mut units = s.encode_utf16();
    let in_table = |table: &[(u16, u16)], c: u16| {
        table
            .binary_search_by(|&(lo, hi)| {
                if hi < c {
                    Ordering::Less
                } else if lo > c {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            })
            .is_ok()
    };
    match units.next() {
        Some(first) if in_table(NCNAME_START, first) => units.all(|c| in_table(NCNAME_CHAR, c)),
        _ => false,
    }
}

/// `DecimalDV.XDecimal`: a sign, the integer digits without leading zeros
/// and the fraction digits without trailing zeros.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Decimal {
    sign: i8,
    int: String,
    frac: String,
}

impl Decimal {
    /// `XDecimal(content, integer)`: `initI` for an integer, else `initD`.
    pub(super) fn parse(content: &str, integer: bool) -> Option<Self> {
        let bytes = content.as_bytes();
        if bytes.is_empty() {
            return None;
        }
        let mut sign = 1;
        let mut start = 0;
        match bytes[0] {
            b'+' => start = 1,
            b'-' => {
                start = 1;
                sign = -1;
            }
            _ => {}
        }
        let mut int_start = start;
        while int_start < bytes.len() && bytes[int_start] == b'0' {
            int_start += 1;
        }
        let mut int_end = int_start;
        while int_end < bytes.len() && bytes[int_end].is_ascii_digit() {
            int_end += 1;
        }
        let mut frac = String::new();
        if int_end < bytes.len() {
            if integer || bytes[int_end] != b'.' {
                return None;
            }
            let frac_start = int_end + 1;
            if start == int_start && int_start == int_end && frac_start == bytes.len() {
                // Only a point: no digits at all.
                return None;
            }
            let mut frac_end = bytes.len();
            if !bytes[frac_start..].iter().all(u8::is_ascii_digit) {
                return None;
            }
            while frac_end > frac_start && bytes[frac_end - 1] == b'0' {
                frac_end -= 1;
            }
            frac = content[frac_start..frac_end].to_owned();
        } else if start == int_end {
            // A sign alone, or nothing.
            return None;
        }
        let int = content[int_start..int_end].to_owned();
        if int.is_empty() && frac.is_empty() {
            sign = 0;
        }
        Some(Self { sign, int, frac })
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.sign != other.sign {
            return self.sign.cmp(&other.sign);
        }
        let magnitude = self
            .int
            .len()
            .cmp(&other.int.len())
            .then_with(|| self.int.cmp(&other.int))
            .then_with(|| self.frac.cmp(&other.frac));
        if self.sign < 0 {
            magnitude.reverse()
        } else {
            magnitude
        }
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.sign < 0 {
            f.write_str("-")?;
        }
        f.write_str(if self.int.is_empty() { "0" } else { &self.int })?;
        if !self.frac.is_empty() {
            write!(f, ".{}", self.frac)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals() {
        for (s, int) in [("0", true), ("-0", true), ("+12", true), ("007", true)] {
            assert!(Decimal::parse(s, int).is_some(), "{s}");
        }
        for s in ["", "+", "-", "1.5", "1e3", "100K", " 1", "1.", ".5"] {
            assert!(Decimal::parse(s, true).is_none(), "{s}");
        }
        for s in ["1.", ".5", "-0.0", "1.50"] {
            assert!(Decimal::parse(s, false).is_some(), "{s}");
        }
        assert!(Decimal::parse(".", false).is_none());
        let max = Decimal::parse("9223372036854775807", true).unwrap();
        let over = Decimal::parse("9223372036854775808", true).unwrap();
        let neg = Decimal::parse("-9223372036854775809", true).unwrap();
        let min = Decimal::parse("-9223372036854775808", true).unwrap();
        assert!(over > max);
        assert!(neg < min);
        assert_eq!(Decimal::parse("-0", true), Decimal::parse("0", true));
        assert_eq!(Decimal::parse("1.50", false).unwrap().to_string(), "1.5");
    }

    #[test]
    fn names_and_whitespace() {
        assert!(is_ncname("abc-1._x"));
        assert!(!is_ncname("1abc"));
        assert!(!is_ncname("a:b"));
        assert!(!is_ncname(""));
        assert_eq!(normalize("  a \n\t b  ", Ws::Collapse), "a b");
        assert_eq!(normalize(" a\tb ", Ws::Replace), " a b ");
    }
}
