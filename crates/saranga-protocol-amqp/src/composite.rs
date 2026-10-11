//! Described list types (composites) and the conversion of their fields.
//!
//! A composite is written the way Proton-J writes it: fields up to the last
//! one that is set and not its default, with defaults written out when a
//! later field is set, as `list0` when no field is set, and otherwise as
//! `list8` or `list32` by size unless the type's fast-path encoder forces
//! `list32` (see each type's [`Composite::form`]).

use crate::codec::{self, CodecError, Result, code, invalid};
use crate::types::{Array, ArrayType, Descriptor, Symbol, Value};

/// How a composite's list is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListForm {
    /// `list8` while the fields take at most 253 bytes.
    Sized,
    /// Always `list32` (unless empty).
    Wide,
}

/// A described list type.
pub trait Composite: Sized {
    const CODE: u64;
    const NAME: &'static str;

    /// Builds the value from its list fields (missing trailing fields are
    /// null).
    fn from_fields(fields: Vec<Value>) -> Result<Self>;

    /// The fields to write, already cut after the last one that is set.
    fn to_fields(&self) -> Vec<Value>;

    fn form(&self) -> ListForm {
        ListForm::Sized
    }

    /// Writes the described list.
    fn encode(&self, out: &mut Vec<u8>) {
        out.push(code::DESCRIBED);
        codec::encode_descriptor(&Descriptor::Code(Self::CODE), out);
        let fields = self.to_fields();
        if fields.is_empty() {
            out.push(code::LIST0);
            return;
        }
        let mut body = Vec::new();
        for f in &fields {
            codec::encode(f, &mut body);
        }
        if self.form() == ListForm::Sized && body.len() <= 253 {
            out.extend([code::LIST8, (body.len() + 1) as u8, fields.len() as u8]);
        } else {
            out.push(code::LIST32);
            out.extend(((body.len() + 4) as u32).to_be_bytes());
            out.extend((fields.len() as u32).to_be_bytes());
        }
        out.extend(body);
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }

    /// Whether `d` names this type.
    fn is(d: &Descriptor) -> bool {
        d.is(Self::CODE, Self::NAME)
    }

    /// The type from the body of a described value with its descriptor.
    fn from_described(d: &Descriptor, body: Value) -> Result<Self> {
        if !Self::is(d) {
            return invalid(format!("expected {}, got {d:?}", Self::NAME));
        }
        match body {
            Value::List(fields) => Self::from_fields(fields),
            Value::Null => Self::from_fields(Vec::new()),
            other => invalid(format!("{} body {other:?}", Self::NAME)),
        }
    }

    /// Decodes the type from a complete described value.
    fn decode(bytes: &[u8]) -> Result<Self> {
        Self::from_value(codec::from_slice(bytes)?)
    }

    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Described(d, body) => Self::from_described(&d, *body),
            other => invalid(format!("expected {}, got {other:?}", Self::NAME)),
        }
    }

    /// The described value, for nesting in another value.
    fn to_value(&self) -> Value {
        // Re-decoding keeps the fast-path list form out of generic values,
        // which only nest inside other composites.
        Value::described(Descriptor::Code(Self::CODE), Value::List(self.to_fields()))
    }
}

/// Conversion of a composite's field to and from its value.
pub trait Field: Sized {
    fn from_value(v: Value) -> Result<Self>;
    fn to_value(&self) -> Value;
}

fn wrong<T>(what: &str, v: &Value) -> Result<T> {
    invalid(format!("expected {what}, got {v:?}"))
}

impl Field for Value {
    fn from_value(v: Value) -> Result<Self> {
        Ok(v)
    }
    fn to_value(&self) -> Value {
        self.clone()
    }
}

impl Field for bool {
    fn from_value(v: Value) -> Result<Self> {
        v.as_bool().map_or_else(|| wrong("boolean", &v), Ok)
    }
    fn to_value(&self) -> Value {
        Value::Boolean(*self)
    }
}

impl Field for u8 {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Ubyte(b) => Ok(b),
            ref other => wrong("ubyte", other),
        }
    }
    fn to_value(&self) -> Value {
        Value::Ubyte(*self)
    }
}

impl Field for u16 {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Ushort(b) => Ok(b),
            ref other => wrong("ushort", other),
        }
    }
    fn to_value(&self) -> Value {
        Value::Ushort(*self)
    }
}

impl Field for u32 {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Uint(b) => Ok(b),
            ref other => wrong("uint", other),
        }
    }
    fn to_value(&self) -> Value {
        Value::Uint(*self)
    }
}

impl Field for u64 {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Ulong(b) => Ok(b),
            ref other => wrong("ulong", other),
        }
    }
    fn to_value(&self) -> Value {
        Value::Ulong(*self)
    }
}

impl Field for String {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::String(s) => Ok(s),
            ref other => wrong("string", other),
        }
    }
    fn to_value(&self) -> Value {
        Value::String(self.clone())
    }
}

impl Field for Symbol {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Symbol(s) => Ok(s),
            ref other => wrong("symbol", other),
        }
    }
    fn to_value(&self) -> Value {
        Value::Symbol(self.clone())
    }
}

/// `binary`.
impl Field for Vec<u8> {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Binary(b) => Ok(b),
            ref other => wrong("binary", other),
        }
    }
    fn to_value(&self) -> Value {
        Value::Binary(self.clone())
    }
}

/// A timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp(pub i64);

impl Field for Timestamp {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Timestamp(t) => Ok(Timestamp(t)),
            ref other => wrong("timestamp", other),
        }
    }
    fn to_value(&self) -> Value {
        Value::Timestamp(self.0)
    }
}

/// `symbol` with `multiple="true"`: a single symbol or an array of them,
/// written as an array (`Symbol[]` in Proton-J).
impl Field for Vec<Symbol> {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Symbol(s) => Ok(vec![s]),
            Value::Array(a) => a
                .items
                .into_iter()
                .map(<Symbol as Field>::from_value)
                .collect(),
            ref other => wrong("symbols", other),
        }
    }
    fn to_value(&self) -> Value {
        Value::Array(Array {
            element: ArrayType::Primitive(code::SYM32),
            items: self.iter().cloned().map(Value::Symbol).collect(),
        })
    }
}

/// `fields` and the other symbol-keyed maps (filter sets, annotations).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Fields(pub Vec<(Symbol, Value)>);

impl Fields {
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.iter().find(|(k, _)| k.0 == key).map(|(_, v)| v)
    }

    pub fn contains(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn insert(&mut self, key: impl Into<Symbol>, value: Value) {
        let key = key.into();
        match self.0.iter_mut().find(|(k, _)| *k == key) {
            Some(e) => e.1 = value,
            None => self.0.push((key, value)),
        }
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        let i = self.0.iter().position(|(k, _)| k.0 == key)?;
        Some(self.0.remove(i).1)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Field for Fields {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::Map(entries) => entries
                .into_iter()
                .map(|(k, v)| Ok((<Symbol as Field>::from_value(k)?, v)))
                .collect::<Result<_>>()
                .map(Fields),
            ref other => wrong("map", other),
        }
    }
    fn to_value(&self) -> Value {
        Value::Map(
            self.0
                .iter()
                .map(|(k, v)| (Value::Symbol(k.clone()), v.clone()))
                .collect(),
        )
    }
}

/// A composite nested in another.
impl<T: Composite> Field for T {
    fn from_value(v: Value) -> Result<Self> {
        <T as Composite>::from_value(v)
    }
    fn to_value(&self) -> Value {
        <T as Composite>::to_value(self)
    }
}

/// Reads field `i` of a composite.
pub(crate) fn take(fields: &mut [Value], i: usize) -> Value {
    fields.get_mut(i).map(std::mem::take).unwrap_or(Value::Null)
}

pub(crate) fn opt<T: Field>(v: Value, name: &str) -> Result<Option<T>> {
    if v.is_null() {
        return Ok(None);
    }
    T::from_value(v)
        .map(Some)
        .map_err(|e| CodecError::Invalid(format!("{name}: {e}")))
}

pub(crate) fn req<T: Field>(v: Value, name: &str) -> Result<T> {
    if v.is_null() {
        return invalid(format!("mandatory field {name} is null"));
    }
    T::from_value(v).map_err(|e| CodecError::Invalid(format!("{name}: {e}")))
}

/// Cuts trailing fields that are unset (null) or at their default.
pub(crate) fn trim(mut fields: Vec<(Value, bool)>) -> Vec<Value> {
    while fields
        .last()
        .is_some_and(|(v, default)| *default || v.is_null())
    {
        fields.pop();
    }
    fields.into_iter().map(|(v, _)| v).collect()
}

/// Declares a composite. Each field is `opt` (nullable), `req` (mandatory)
/// or `def` (with a default, `= expr`).
macro_rules! composite {
    (
        $(#[$doc:meta])*
        pub struct $name:ident ($code:expr, $sym:expr) {
            $( $(#[$fdoc:meta])* $kind:ident $field:ident : $ty:ty $(= $def:expr)? ),* $(,)?
        }
        $(form($s:ident) $form:block)?
    ) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq)]
        pub struct $name {
            $( $(#[$fdoc])* pub $field: composite!(@ty $kind $ty) ),*
        }

        impl Default for $name {
            fn default() -> Self {
                Self { $( $field: composite!(@default $kind $ty $(, $def)?) ),* }
            }
        }

        impl $crate::composite::Composite for $name {
            const CODE: u64 = $code;
            const NAME: &'static str = $sym;

            #[allow(unused_mut, unused_variables, unused_assignments)]
            fn from_fields(mut fields: Vec<$crate::types::Value>) -> $crate::codec::Result<Self> {
                let mut i = 0;
                Ok(Self {
                    $( $field: {
                        let v = $crate::composite::take(&mut fields, i);
                        i += 1;
                        composite!(@read $kind $field v $(, $def)?)
                    } ),*
                })
            }

            fn to_fields(&self) -> Vec<$crate::types::Value> {
                $crate::composite::trim(vec![
                    $( composite!(@write $kind self.$field $(, $def)?) ),*
                ])
            }

            $(
                fn form(&self) -> $crate::composite::ListForm {
                    let $s = self;
                    $form
                }
            )?
        }
    };
    (@ty opt $ty:ty) => { Option<$ty> };
    (@ty req $ty:ty) => { $ty };
    (@ty def $ty:ty) => { $ty };
    (@default opt $ty:ty) => { None };
    (@default req $ty:ty) => { <$ty>::default() };
    (@default def $ty:ty, $def:expr) => { $def };
    (@read opt $field:ident $v:ident) => {
        $crate::composite::opt($v, stringify!($field))?
    };
    (@read req $field:ident $v:ident) => {
        $crate::composite::req($v, stringify!($field))?
    };
    (@read def $field:ident $v:ident, $def:expr) => {
        $crate::composite::opt($v, stringify!($field))?.unwrap_or($def)
    };
    (@write opt $f:expr) => {
        match &$f {
            Some(v) => ($crate::composite::Field::to_value(v), false),
            None => ($crate::types::Value::Null, true),
        }
    };
    (@write req $f:expr) => {
        ($crate::composite::Field::to_value(&$f), false)
    };
    (@write def $f:expr, $def:expr) => {
        ($crate::composite::Field::to_value(&$f), $f == $def)
    };
}

pub(crate) use composite;
