//! The AMQP 1.0 type system (part 1 of the specification): primitive
//! values, described values and arrays.

use std::fmt;

/// An AMQP symbol: ASCII text naming something (`org.apache.qpid.proton.amqp.Symbol`).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Symbol(pub String);

impl Symbol {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, ":{}", self.0)
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for Symbol {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl PartialEq<str> for Symbol {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for Symbol {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

/// A descriptor: a numeric code or a symbolic name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Descriptor {
    Code(u64),
    Name(Symbol),
}

impl Descriptor {
    /// Whether this is the descriptor `code` or its symbolic `name`.
    pub fn is(&self, code: u64, name: &str) -> bool {
        match self {
            Self::Code(c) => *c == code,
            Self::Name(n) => n.0 == name,
        }
    }
}

/// The element type of an array: the widest constructor of a primitive
/// type, or a described type over one.
#[derive(Debug, Clone, PartialEq)]
pub enum ArrayType {
    /// A primitive type, named by its widest format code (`0x56` boolean,
    /// `0x70` uint, `0xb1` string, `0xd0` list, ...).
    Primitive(u8),
    Described(Box<Descriptor>, u8),
}

/// An AMQP array: values of one type.
#[derive(Debug, Clone, PartialEq)]
pub struct Array {
    pub element: ArrayType,
    /// The elements; for a described element type, the values without
    /// their descriptor.
    pub items: Vec<Value>,
}

/// Any AMQP value.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Value {
    #[default]
    Null,
    Boolean(bool),
    Ubyte(u8),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Decimal32([u8; 4]),
    Decimal64([u8; 8]),
    Decimal128([u8; 16]),
    /// A UTF-32 code point.
    Char(u32),
    /// Milliseconds since the Unix epoch.
    Timestamp(i64),
    Uuid([u8; 16]),
    Binary(Vec<u8>),
    String(String),
    Symbol(Symbol),
    List(Vec<Value>),
    /// Entries in encoded order.
    Map(Vec<(Value, Value)>),
    Array(Array),
    Described(Box<Descriptor>, Box<Value>),
}

impl Value {
    pub fn described(descriptor: Descriptor, value: Value) -> Self {
        Self::Described(Box::new(descriptor), Box::new(value))
    }

    pub fn symbol(s: impl Into<String>) -> Self {
        Self::Symbol(Symbol(s.into()))
    }

    pub fn string(s: impl Into<String>) -> Self {
        Self::String(s.into())
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_symbol(&self) -> Option<&Symbol> {
        match self {
            Self::Symbol(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// An unsigned or signed integer of any width that fits `u64`.
    pub fn as_u64(&self) -> Option<u64> {
        match *self {
            Self::Ubyte(v) => Some(v.into()),
            Self::Ushort(v) => Some(v.into()),
            Self::Uint(v) => Some(v.into()),
            Self::Ulong(v) => Some(v),
            Self::Byte(v) => u64::try_from(v).ok(),
            Self::Short(v) => u64::try_from(v).ok(),
            Self::Int(v) => u64::try_from(v).ok(),
            Self::Long(v) => u64::try_from(v).ok(),
            _ => None,
        }
    }

    /// Any integer as a Java `Number.longValue()` would give it.
    pub fn as_i64(&self) -> Option<i64> {
        match *self {
            Self::Ubyte(v) => Some(v.into()),
            Self::Ushort(v) => Some(v.into()),
            Self::Uint(v) => Some(v.into()),
            Self::Ulong(v) => Some(v as i64),
            Self::Byte(v) => Some(v.into()),
            Self::Short(v) => Some(v.into()),
            Self::Int(v) => Some(v.into()),
            Self::Long(v) => Some(v),
            Self::Timestamp(v) => Some(v),
            Self::Float(v) => Some(v as i64),
            Self::Double(v) => Some(v as i64),
            _ => None,
        }
    }

    pub fn as_u32(&self) -> Option<u32> {
        self.as_u64().and_then(|v| u32::try_from(v).ok())
    }

    pub fn as_binary(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Self::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&[(Value, Value)]> {
        match self {
            Self::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Looks a key up in a map value.
    pub fn map_get(&self, key: &Value) -> Option<&Value> {
        self.as_map()?
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Self::Uint(v)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Self::Ulong(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Self::Int(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::Long(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self::String(v.to_owned())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<Symbol> for Value {
    fn from(v: Symbol) -> Self {
        Self::Symbol(v)
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Self::Binary(v)
    }
}
