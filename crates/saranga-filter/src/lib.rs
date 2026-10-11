//! Artemis message filters: the JMS selector language used for queue and
//! consumer filters.
//!
//! Ported from the `artemis-selector` module (the `StrictParser.jj` and
//! `HyphenatedParser.jj` grammars, `SelectorParser`, and the
//! `org.apache.activemq.artemis.selector.filter` expression classes) and
//! from `org.apache.activemq.artemis.core.filter.impl.FilterImpl`.
//!
//! Evaluation follows the Java code rather than the JMS specification where
//! the two differ: SQL three-valued logic, Java numeric promotion, and
//! Artemis's lopsided handling of mixed types (for example `shortProp = 1`
//! is TRUE for a short property of 1, while `1 = shortProp` is FALSE).
//!
//! Not supported: evaluating `XPATH` against the message body (`XPATH`
//! parses but always evaluates to FALSE, and its expression is not
//! validated as Artemis does at parse time); hexadecimal floating-point
//! strings and non-ASCII digits in `convert_string_expressions:` conversions.

mod expr;
mod java;
mod lexer;
mod like;
mod message;
mod parser;
pub mod translate;

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};

/// A property value as the selector sees it: one variant per Java class a
/// message property can have. A missing or null property is `None` in
/// [`Filterable::get`].
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    /// A Java `char` (one UTF-16 code unit).
    Char(u16),
    String(String),
    /// A `byte[]`. Never equal to anything, and an error in `<`, `>`, `<=`
    /// and `>=` comparisons (Java compares arrays by identity and they are
    /// not `Comparable`).
    Bytes(Vec<u8>),
}

impl Value {
    /// Converts a `TypedProperties` value the way `FilterImpl` sees it:
    /// `SimpleString` becomes a `String` (lossily, for unpaired surrogates)
    /// and the null encoding is `None`.
    pub fn from_property(value: &saranga_buffer::PropertyValue) -> Option<Self> {
        use saranga_buffer::PropertyValue as P;
        Some(match value {
            P::Null => return None,
            P::Bool(v) => Self::Bool(*v),
            P::Byte(v) => Self::Byte(*v),
            P::Bytes(v) => Self::Bytes(v.clone()),
            P::Short(v) => Self::Short(*v),
            P::Int(v) => Self::Int(*v),
            P::Long(v) => Self::Long(*v),
            P::Float(v) => Self::Float(*v),
            P::Double(v) => Self::Double(*v),
            P::Char(v) => Self::Char(*v),
            P::String(v) => Self::String(v.to_string_lossy()),
        })
    }
}

/// Failure to read an identifier's value (Java throws from
/// `Filterable.getProperty`). The filter then does not match.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot read {identifier}: {reason}")]
pub struct PropertyError {
    pub identifier: String,
    pub reason: String,
}

/// What a filter is evaluated against. Ported from `Filterable`.
pub trait Filterable {
    /// The value of an identifier, or `None` if it is missing or null.
    fn get(&self, identifier: &str) -> Option<Value>;

    /// Like [`get`](Self::get), but may fail the way Java's `getProperty`
    /// can throw. The evaluator calls this; the default never fails.
    fn try_get(&self, identifier: &str) -> Result<Option<Value>, PropertyError> {
        Ok(self.get(identifier))
    }
}

/// `FilterImpl.FilterableMap`: every value is a string.
impl<S: BuildHasher> Filterable for HashMap<String, String, S> {
    fn get(&self, identifier: &str) -> Option<Value> {
        HashMap::get(self, identifier).map(|v| Value::String(v.clone()))
    }
}

/// A filter string that does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Invalid filter: {filter}: {reason}")]
pub struct FilterError {
    pub filter: String,
    pub reason: String,
}

const CONVERT_STRING_EXPRESSIONS_PREFIX: &str = "convert_string_expressions:";
const HYPHENATED_PROPS_PREFIX: &str = "hyphenated_props:";
const NO_CONVERT_STRING_EXPRESSIONS_PREFIX: &str = "no_convert_string_expressions:";
const NO_HYPHENATED_PROPS_PREFIX: &str = "no_hyphenated_props:";

/// A parsed filter. Ported from `FilterImpl`; equality and hashing use the
/// filter string, as in Java.
#[derive(Debug, Clone)]
pub struct Filter {
    text: String,
    expr: expr::Expr,
    convert_strings: bool,
}

impl Filter {
    /// `SelectorParser.parse`. The text may start with any sequence of the
    /// Artemis option prefixes `convert_string_expressions:`,
    /// `hyphenated_props:`, `no_convert_string_expressions:` and
    /// `no_hyphenated_props:`.
    pub fn parse(text: &str) -> Result<Filter, FilterError> {
        let mut actual = text;
        let mut convert_strings = false;
        let mut hyphenated = false;
        loop {
            if let Some(rest) = actual.strip_prefix(CONVERT_STRING_EXPRESSIONS_PREFIX) {
                convert_strings = true;
                actual = rest;
            } else if let Some(rest) = actual.strip_prefix(HYPHENATED_PROPS_PREFIX) {
                hyphenated = true;
                actual = rest;
            } else if let Some(rest) = actual.strip_prefix(NO_CONVERT_STRING_EXPRESSIONS_PREFIX) {
                convert_strings = false;
                actual = rest;
            } else if let Some(rest) = actual.strip_prefix(NO_HYPHENATED_PROPS_PREFIX) {
                hyphenated = false;
                actual = rest;
            } else {
                break;
            }
        }
        let expr = parser::parse(actual, hyphenated).map_err(|reason| FilterError {
            filter: text.to_owned(),
            reason,
        })?;
        Ok(Filter {
            text: text.to_owned(),
            expr,
            convert_strings,
        })
    }

    /// `FilterImpl.createFilter(String)`: the text is trimmed (Java's
    /// `String.trim`, which strips every char up to U+0020), and a null or
    /// blank filter is no filter.
    pub fn create(text: Option<&str>) -> Result<Option<Filter>, FilterError> {
        match text.map(|t| t.trim_matches(|c: char| c <= ' ')) {
            None | Some("") => Ok(None),
            Some(t) => Filter::parse(t).map(Some),
        }
    }

    /// The filter string, including any option prefixes.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// `FilterImpl.match`: true if the selector evaluates to TRUE. Unknown,
    /// FALSE and evaluation errors are all "no match".
    pub fn matches(&self, target: &(impl Filterable + ?Sized)) -> bool {
        let ctx = expr::Ctx {
            target,
            convert_strings: self.convert_strings,
        };
        self.expr.matches(&ctx).unwrap_or(false)
    }
}

impl PartialEq for Filter {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text
    }
}

impl Eq for Filter {}

impl Hash for Filter {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.text.hash(state);
    }
}

/// `FilterImpl.toString`.
impl std::fmt::Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FilterImpl [sfilterString={}]", self.text)
    }
}
