use crate::{DecodeError, Reader, Result, SimpleString, Writer, constants as c};

/// One value in a [`TypedProperties`] map. Floats are compared bitwise so a
/// decoded value always re-encodes to the same bytes.
#[derive(Debug, Clone)]
pub enum PropertyValue {
    Null,
    Bool(bool),
    Byte(i8),
    Bytes(Vec<u8>),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Char(u16),
    String(SimpleString),
}

impl PartialEq for PropertyValue {
    fn eq(&self, other: &Self) -> bool {
        use PropertyValue::*;
        match (self, other) {
            (Null, Null) => true,
            (Bool(a), Bool(b)) => a == b,
            (Byte(a), Byte(b)) => a == b,
            (Bytes(a), Bytes(b)) => a == b,
            (Short(a), Short(b)) => a == b,
            (Int(a), Int(b)) => a == b,
            (Long(a), Long(b)) => a == b,
            (Float(a), Float(b)) => a.to_bits() == b.to_bits(),
            (Double(a), Double(b)) => a.to_bits() == b.to_bits(),
            (Char(a), Char(b)) => a == b,
            (String(a), String(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for PropertyValue {}

impl PropertyValue {
    fn decode(r: &mut Reader<'_>) -> Result<Self> {
        Ok(match r.u8()? {
            c::NULL => Self::Null,
            c::BOOLEAN => Self::Bool(r.bool()?),
            c::BYTE => Self::Byte(r.i8()?),
            c::BYTES => Self::Bytes(r.byte_array()?),
            c::SHORT => Self::Short(r.i16()?),
            c::INT => Self::Int(r.i32()?),
            c::LONG => Self::Long(r.i64()?),
            c::FLOAT => Self::Float(r.f32()?),
            c::DOUBLE => Self::Double(r.f64()?),
            c::CHAR => Self::Char(r.u16()?),
            c::STRING => Self::String(r.simple_string()?),
            t => return Err(DecodeError::Invalid(format!("invalid property type {t}"))),
        })
    }

    fn encode(&self, w: &mut Writer) {
        match self {
            Self::Null => w.u8(c::NULL),
            Self::Bool(v) => w.u8(c::BOOLEAN).byte_buf_bool(*v),
            Self::Byte(v) => w.u8(c::BYTE).i8(*v),
            Self::Bytes(v) => w.u8(c::BYTES).byte_array(v),
            Self::Short(v) => w.u8(c::SHORT).i16(*v),
            Self::Int(v) => w.u8(c::INT).i32(*v),
            Self::Long(v) => w.u8(c::LONG).i64(*v),
            Self::Float(v) => w.u8(c::FLOAT).f32(*v),
            Self::Double(v) => w.u8(c::DOUBLE).f64(*v),
            Self::Char(v) => w.u8(c::CHAR).u16(*v),
            Self::String(v) => w.u8(c::STRING).simple_string(v),
        };
    }
}

/// Artemis `TypedProperties`: message properties keyed by `SimpleString`.
///
/// Entries keep their decode order. Artemis iterates a `HashMap`, so
/// preserving the order we read is what makes re-encoding byte-exact.
/// `None` is the NULL encoding; `Some(vec![])` is NOT_NULL with zero entries.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TypedProperties(Option<Vec<(SimpleString, PropertyValue)>>);

impl TypedProperties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entries(&self) -> &[(SimpleString, PropertyValue)] {
        self.0.as_deref().unwrap_or_default()
    }

    pub fn get(&self, key: &SimpleString) -> Option<&PropertyValue> {
        self.entries()
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    /// Inserts or replaces a property, keeping the position of an existing key.
    pub fn insert(&mut self, key: SimpleString, value: PropertyValue) {
        let entries = self.0.get_or_insert_with(Vec::new);
        match entries.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => entries.push((key, value)),
        }
    }

    pub fn remove(&mut self, key: &SimpleString) -> Option<PropertyValue> {
        let entries = self.0.as_mut()?;
        let i = entries.iter().position(|(k, _)| k == key)?;
        Some(entries.remove(i).1)
    }

    pub fn decode(r: &mut Reader<'_>) -> Result<Self> {
        if !r.not_null()? {
            return Ok(Self(None));
        }
        let count = r.len("properties")?;
        let mut entries = Vec::with_capacity(count.min(r.remaining()));
        for _ in 0..count {
            let key = r.simple_string()?;
            entries.push((key, PropertyValue::decode(r)?));
        }
        Ok(Self(Some(entries)))
    }

    pub fn encode(&self, w: &mut Writer) {
        match &self.0 {
            None => {
                w.u8(c::NULL);
            }
            Some(entries) => {
                w.u8(c::NOT_NULL).i32(entries.len() as i32);
                for (k, v) in entries {
                    w.simple_string(k);
                    v.encode(w);
                }
            }
        }
    }
}
