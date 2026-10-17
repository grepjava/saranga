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

    /// The length of [`encode`](Self::encode)'s output.
    fn encode_size(&self) -> usize {
        1 + match self {
            Self::Null => 0,
            Self::Bool(_) | Self::Byte(_) => 1,
            Self::Short(_) | Self::Char(_) => 2,
            Self::Int(_) | Self::Float(_) => 4,
            Self::Long(_) | Self::Double(_) => 8,
            Self::Bytes(v) => 4 + v.len(),
            Self::String(v) => 4 + v.as_bytes().len(),
        }
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

    /// [`get`](Self::get) by a name, without making a key of it.
    pub fn get_str(&self, key: &str) -> Option<&PropertyValue> {
        // Names are nearly always ASCII: then a match is the key's bytes,
        // each followed by a zero, and anything of another length is not
        // one. The check is made once rather than for each entry.
        if key.is_ascii() {
            let len = key.len() * 2;
            return self
                .entries()
                .iter()
                .find(|(k, _)| {
                    let b = k.as_bytes();
                    b.len() == len
                        && b.as_chunks::<2>()
                            .0
                            .iter()
                            .zip(key.as_bytes())
                            .all(|(&[lo, hi], &a)| lo == a && hi == 0)
                })
                .map(|(_, v)| v);
        }
        self.entries()
            .iter()
            .find(|(k, _)| k.eq_str(key))
            .map(|(_, v)| v)
    }

    /// Room for `n` more entries and no more; none for none, which leaves
    /// a NULL encoding NULL.
    pub fn reserve_exact(&mut self, n: usize) {
        if n > 0 {
            self.0.get_or_insert_with(Vec::new).reserve_exact(n);
        }
    }

    /// NULL again if room was reserved and nothing was put.
    pub fn unreserve(&mut self) {
        if matches!(&self.0, Some(v) if v.is_empty() && v.capacity() > 0) {
            self.0 = None;
        }
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

    /// Puts the entries, taken as put in this order, in the order Java's
    /// `TypedProperties` keeps them: a `new HashMap<>()` of those puts, as
    /// the Java client encodes a message it made.
    pub fn sort_as_java_hash_map(&mut self) {
        let Some(entries) = self.0.as_mut() else {
            return;
        };
        let hashes: Vec<i32> = entries
            .iter()
            .map(|(k, _)| {
                k.as_bytes().iter().fold(0i32, |h, &b| {
                    h.wrapping_mul(31).wrapping_add(i32::from(b as i8))
                })
            })
            .collect();
        let order = crate::java_hash::hash_map_order(&hashes, None);
        let mut taken: Vec<Option<(SimpleString, PropertyValue)>> =
            std::mem::take(entries).into_iter().map(Some).collect();
        *entries = order.into_iter().filter_map(|i| taken[i].take()).collect();
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

    /// Makes the names and string values slices of `encoded`, where
    /// [`encode`](Self::encode) wrote these properties from `at`, in place
    /// of copies of their own.
    pub fn share(&mut self, encoded: &bytes::Bytes, mut at: usize) {
        let Some(entries) = &mut self.0 else {
            return;
        };
        let share = |s: &mut SimpleString, at: usize| {
            let end = at + s.as_bytes().len();
            debug_assert_eq!(&encoded[at..end], s.as_bytes());
            *s = SimpleString::from_shared(encoded.slice(at..end));
            end
        };
        at += 5;
        for (k, v) in entries {
            at = share(k, at + 4);
            at = match v {
                PropertyValue::String(s) => share(s, at + 5),
                other => at + other.encode_size(),
            };
        }
    }

    /// The length of [`encode`](Self::encode)'s output, without encoding.
    pub fn encode_size(&self) -> usize {
        match &self.0 {
            None => 1,
            Some(entries) => {
                5 + entries
                    .iter()
                    .map(|(k, v)| 4 + k.as_bytes().len() + v.encode_size())
                    .sum::<usize>()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_str_finds_ascii_and_other_names() {
        let mut p = TypedProperties::default();
        for name in ["a", "ab", "_AMQ_ROUTING_TYPE", "héllo", "日本"] {
            p.insert(SimpleString::from(name), PropertyValue::String(name.into()));
        }
        for name in ["a", "ab", "_AMQ_ROUTING_TYPE", "héllo", "日本"] {
            assert_eq!(
                p.get_str(name),
                Some(&PropertyValue::String(name.into())),
                "{name}"
            );
        }
        for name in ["", "b", "abc", "_AMQ_ROUTING_TYPF", "hello", "日"] {
            assert_eq!(p.get_str(name), None, "{name}");
        }
    }

    #[test]
    fn encode_size_is_what_encode_writes() {
        let mut p = TypedProperties::default();
        let size = |p: &TypedProperties| {
            let mut w = Writer::new();
            p.encode(&mut w);
            (w.len(), p.encode_size())
        };
        let (written, sized) = size(&p);
        assert_eq!(written, sized);
        let values = [
            PropertyValue::Null,
            PropertyValue::Bool(true),
            PropertyValue::Byte(-1),
            PropertyValue::Bytes(vec![1, 2, 3]),
            PropertyValue::Short(7),
            PropertyValue::Int(7),
            PropertyValue::Long(7),
            PropertyValue::Float(1.5),
            PropertyValue::Double(1.5),
            PropertyValue::Char(65),
            PropertyValue::String("héllo".into()),
        ];
        for (i, v) in values.into_iter().enumerate() {
            p.insert(SimpleString::from(format!("k{i}").as_str()), v);
            let (written, sized) = size(&p);
            assert_eq!(written, sized);
        }
    }
}
