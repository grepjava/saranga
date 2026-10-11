//! `org.apache.activemq.util.MarshallingSupport`: the typed values of
//! message properties, map message bodies and stream message bodies.

use std::fmt::Write as _;

use saranga_storage::java;

use crate::codec::{CodecError, Input, decode_modified_utf8, write_utf};

pub const NULL: u8 = 0;
pub const BOOLEAN_TYPE: u8 = 1;
pub const BYTE_TYPE: u8 = 2;
pub const CHAR_TYPE: u8 = 3;
pub const SHORT_TYPE: u8 = 4;
pub const INTEGER_TYPE: u8 = 5;
pub const LONG_TYPE: u8 = 6;
pub const DOUBLE_TYPE: u8 = 7;
pub const FLOAT_TYPE: u8 = 8;
pub const STRING_TYPE: u8 = 9;
pub const BYTE_ARRAY_TYPE: u8 = 10;
pub const MAP_TYPE: u8 = 11;
pub const LIST_TYPE: u8 = 12;
pub const BIG_STRING_TYPE: u8 = 13;

type R<T> = Result<T, CodecError>;

/// A value of a primitive map or list.
#[derive(Debug, Clone, PartialEq)]
pub enum Primitive {
    Null,
    Bool(bool),
    Byte(i8),
    /// A UTF-16 code unit.
    Char(u16),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    Map(PrimitiveMap),
    List(Vec<Primitive>),
}

impl Primitive {
    /// `toString()`, as a property that cannot hold the value (a map or a
    /// list) gets it. A byte array prints its identity in Java; Saranga
    /// prints `[B@0`.
    pub fn java_string(&self) -> String {
        match self {
            Primitive::Null => "null".into(),
            Primitive::Bool(b) => b.to_string(),
            Primitive::Byte(v) => v.to_string(),
            Primitive::Char(c) => java::utf16([*c]),
            Primitive::Short(v) => v.to_string(),
            Primitive::Int(v) => v.to_string(),
            Primitive::Long(v) => v.to_string(),
            Primitive::Float(v) => java::float(*v),
            Primitive::Double(v) => java::double(*v),
            Primitive::String(s) => s.clone(),
            Primitive::Bytes(_) => "[B@0".into(),
            Primitive::Map(m) => {
                let mut s = String::from("{");
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        s.push_str(", ");
                    }
                    let _ = write!(s, "{k}={}", v.java_string());
                }
                s.push('}');
                s
            }
            Primitive::List(l) => {
                let items: Vec<String> = l.iter().map(Primitive::java_string).collect();
                format!("[{}]", items.join(", "))
            }
        }
    }
}

/// A `java.util.HashMap<String, Object>`: entries in insertion order,
/// iterated in the map's bucket order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PrimitiveMap {
    entries: Vec<(String, Primitive)>,
    /// The constructor's initial capacity (`None`: the default).
    capacity: Option<usize>,
}

impl PrimitiveMap {
    /// `new HashMap<>()`.
    pub fn new() -> Self {
        Self::default()
    }

    /// `new HashMap<>(capacity)`.
    pub fn with_capacity(capacity: usize) -> Self {
        PrimitiveMap {
            entries: Vec::new(),
            capacity: Some(capacity),
        }
    }

    pub fn insert(&mut self, key: String, value: Primitive) {
        match self.entries.iter_mut().find(|(k, _)| *k == key) {
            Some(e) => e.1 = value,
            None => self.entries.push((key, value)),
        }
    }

    pub fn get(&self, key: &str) -> Option<&Primitive> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn remove(&mut self, key: &str) -> Option<Primitive> {
        let i = self.entries.iter().position(|(k, _)| k == key)?;
        Some(self.entries.remove(i).1)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The entries in Java's iteration order.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Primitive)> {
        let hashes: Vec<i32> = self
            .entries
            .iter()
            .map(|(k, _)| java::string_hash(&k.encode_utf16().collect::<Vec<_>>()))
            .collect();
        java::hash_map_order(&hashes, self.capacity.map(|c| (c, 0.75)))
            .into_iter()
            .map(move |i| (&self.entries[i].0, &self.entries[i].1))
    }
}

// ---------------------------------------------------------------- reading

/// `UTF8Buffer.toString()`: the bytes as standard UTF-8, malformed
/// sequences (modified UTF-8's NUL and surrogates) replaced as Java does.
fn utf8_buffer(b: &[u8]) -> String {
    java::utf8_string(b)
}

fn read_bytes<'a>(input: &mut Input<'a>, len: i32) -> R<&'a [u8]> {
    if len < 0 {
        return Err(CodecError::Io("NegativeArraySizeException".into()));
    }
    input.take(len as usize)
}

/// `unmarshalPrimitiveMap`: `None` for a null map.
pub fn read_map(input: &mut Input<'_>) -> R<Option<PrimitiveMap>> {
    read_map_at(input, false, 0)
}

fn read_map_at(input: &mut Input<'_>, force: bool, depth: u32) -> R<Option<PrimitiveMap>> {
    if depth > 127 {
        return Err(CodecError::Io("Exceeded maximum depth".into()));
    }
    let size = input.i32()?;
    if size < 0 {
        return Ok(None);
    }
    let mut map = PrimitiveMap::with_capacity((size as usize).min(128));
    for _ in 0..size {
        let len = input.i16()? as u16 as i32;
        let name = utf8_buffer(read_bytes(input, len)?);
        let value = read_primitive(input, force, depth + 1)?;
        map.insert(name, value);
    }
    Ok(Some(map))
}

fn read_list(input: &mut Input<'_>, force: bool, depth: u32) -> R<Vec<Primitive>> {
    if depth > 127 {
        return Err(CodecError::Io("Exceeded maximum depth".into()));
    }
    let size = input.i32()?;
    let mut list = Vec::new();
    for _ in 0..size.max(0) {
        list.push(read_primitive(input, force, depth + 1)?);
    }
    Ok(list)
}

/// `unmarshalPrimitive`.
pub fn read_primitive(input: &mut Input<'_>, force: bool, depth: u32) -> R<Primitive> {
    Ok(match input.u8()? {
        NULL => Primitive::Null,
        BOOLEAN_TYPE => Primitive::Bool(input.u8()? != 0),
        BYTE_TYPE => Primitive::Byte(input.u8()? as i8),
        CHAR_TYPE => Primitive::Char(input.i16()? as u16),
        SHORT_TYPE => Primitive::Short(input.i16()?),
        INTEGER_TYPE => Primitive::Int(input.i32()?),
        LONG_TYPE => Primitive::Long(input.i64()?),
        DOUBLE_TYPE => Primitive::Double(f64::from_bits(input.i64()? as u64)),
        FLOAT_TYPE => Primitive::Float(f32::from_bits(input.i32()? as u32)),
        STRING_TYPE => {
            let len = input.i16()? as u16 as i32;
            Primitive::String(utf8_buffer(read_bytes(input, len)?))
        }
        BYTE_ARRAY_TYPE => {
            let len = input.i32()?;
            Primitive::Bytes(read_bytes(input, len)?.to_vec())
        }
        MAP_TYPE => match read_map_at(input, true, depth)? {
            Some(m) => Primitive::Map(m),
            None => Primitive::Null,
        },
        LIST_TYPE => Primitive::List(read_list(input, true, depth)?),
        BIG_STRING_TYPE => {
            if force {
                match read_utf8(input)? {
                    Some(s) => Primitive::String(s),
                    None => Primitive::Null,
                }
            } else {
                let len = input.i32()?;
                Primitive::String(utf8_buffer(read_bytes(input, len)?))
            }
        }
        t => return Err(CodecError::Io(format!("Unknown primitive type: {t}"))),
    })
}

/// `readUTF8`: an int length, then modified UTF-8; `None` for -1.
pub fn read_utf8(input: &mut Input<'_>) -> R<Option<String>> {
    let len = input.i32()?;
    if len < 0 {
        return Ok(None);
    }
    decode_modified_utf8(input.take(len as usize)?).map(Some)
}

// ---------------------------------------------------------------- writing

/// `marshalPrimitiveMap`.
pub fn write_map(out: &mut Vec<u8>, map: Option<&PrimitiveMap>) -> R<()> {
    let Some(map) = map else {
        out.extend_from_slice(&(-1i32).to_be_bytes());
        return Ok(());
    };
    out.extend_from_slice(&(map.len() as i32).to_be_bytes());
    for (k, v) in map.iter() {
        write_utf(out, k)?;
        write_primitive(out, v)?;
    }
    Ok(())
}

/// `marshalPrimitive`.
pub fn write_primitive(out: &mut Vec<u8>, v: &Primitive) -> R<()> {
    match v {
        Primitive::Null => out.push(NULL),
        Primitive::Bool(b) => out.extend_from_slice(&[BOOLEAN_TYPE, *b as u8]),
        Primitive::Byte(b) => out.extend_from_slice(&[BYTE_TYPE, *b as u8]),
        Primitive::Char(c) => {
            out.push(CHAR_TYPE);
            out.extend_from_slice(&c.to_be_bytes());
        }
        Primitive::Short(s) => {
            out.push(SHORT_TYPE);
            out.extend_from_slice(&s.to_be_bytes());
        }
        Primitive::Int(i) => {
            out.push(INTEGER_TYPE);
            out.extend_from_slice(&i.to_be_bytes());
        }
        Primitive::Long(l) => {
            out.push(LONG_TYPE);
            out.extend_from_slice(&l.to_be_bytes());
        }
        Primitive::Float(f) => {
            out.push(FLOAT_TYPE);
            out.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        Primitive::Double(d) => {
            out.push(DOUBLE_TYPE);
            out.extend_from_slice(&d.to_bits().to_be_bytes());
        }
        Primitive::Bytes(b) => {
            out.push(BYTE_ARRAY_TYPE);
            out.extend_from_slice(&(b.len() as i32).to_be_bytes());
            out.extend_from_slice(b);
        }
        Primitive::String(s) => write_string(out, s)?,
        Primitive::Map(m) => {
            out.push(MAP_TYPE);
            write_map(out, Some(m))?;
        }
        Primitive::List(l) => {
            out.push(LIST_TYPE);
            out.extend_from_slice(&(l.len() as i32).to_be_bytes());
            for v in l {
                write_primitive(out, v)?;
            }
        }
    }
    Ok(())
}

/// `marshalString`: `writeUTF` under 8191 chars, `writeUTF8` above.
pub fn write_string(out: &mut Vec<u8>, s: &str) -> R<()> {
    if s.encode_utf16().count() < 8191 {
        out.push(STRING_TYPE);
        write_utf(out, s)
    } else {
        out.push(BIG_STRING_TYPE);
        write_utf8(out, Some(s));
        Ok(())
    }
}

/// `writeUTF8`: an int length, then modified UTF-8 (no length limit);
/// -1 for null.
pub fn write_utf8(out: &mut Vec<u8>, s: Option<&str>) {
    let Some(s) = s else {
        out.extend_from_slice(&(-1i32).to_be_bytes());
        return;
    };
    let at = out.len();
    out.extend_from_slice(&[0; 4]);
    for c in s.encode_utf16() {
        match c {
            1..=0x7f => out.push(c as u8),
            0 | 0x80..=0x7ff => {
                out.push(0xc0 | ((c >> 6) & 0x1f) as u8);
                out.push(0x80 | (c & 0x3f) as u8);
            }
            _ => {
                out.push(0xe0 | ((c >> 12) & 0x0f) as u8);
                out.push(0x80 | ((c >> 6) & 0x3f) as u8);
                out.push(0x80 | (c & 0x3f) as u8);
            }
        }
    }
    let len = (out.len() - at - 4) as i32;
    out[at..at + 4].copy_from_slice(&len.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_round_trip_in_hash_order() {
        let mut m = PrimitiveMap::new();
        m.insert("b".into(), Primitive::Int(1));
        m.insert("a".into(), Primitive::String("x\u{0}y".into()));
        let mut out = Vec::new();
        write_map(&mut out, Some(&m)).unwrap();
        // "a" (hash 97) iterates before "b" (98).
        assert_eq!(&out[4..7], &[0, 1, b'a']);
        let back = read_map(&mut Input::new(&out)).unwrap().unwrap();
        // UTF8Buffer reads modified UTF-8's NUL as two malformed bytes.
        assert_eq!(
            back.get("a"),
            Some(&Primitive::String("x\u{fffd}\u{fffd}y".into()))
        );
        assert_eq!(back.get("b"), Some(&Primitive::Int(1)));
        // ... and each half of an encoded surrogate pair as one.
        let mut out = Vec::new();
        write_primitive(&mut out, &Primitive::String("\u{1f600}".into())).unwrap();
        let v = read_primitive(&mut Input::new(&out), false, 0).unwrap();
        assert_eq!(v, Primitive::String("\u{fffd}\u{fffd}".into()));
    }
}
