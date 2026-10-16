//! Encoding and decoding of AMQP values (part 1.6 of the specification).
//!
//! The encoder picks the same constructors as Proton-J's `EncoderImpl`,
//! which Artemis uses, so that a section the broker re-encodes is byte for
//! byte what Artemis writes: the smallest fixed-width form of a number,
//! `str8`/`sym8`/`vbin8` up to 255 bytes, and `list8`/`map8` while the
//! encoded size and count fit.

use crate::types::{Array, ArrayType, Descriptor, Symbol, Value};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("unexpected end of AMQP data")]
    Eof,
    #[error("invalid AMQP data: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, CodecError>;

pub(crate) fn invalid<T>(msg: impl Into<String>) -> Result<T> {
    Err(CodecError::Invalid(msg.into()))
}

/// Format codes.
pub mod code {
    pub const DESCRIBED: u8 = 0x00;
    pub const NULL: u8 = 0x40;
    pub const BOOLEAN: u8 = 0x56;
    pub const TRUE: u8 = 0x41;
    pub const FALSE: u8 = 0x42;
    pub const UBYTE: u8 = 0x50;
    pub const USHORT: u8 = 0x60;
    pub const UINT: u8 = 0x70;
    pub const SMALL_UINT: u8 = 0x52;
    pub const UINT0: u8 = 0x43;
    pub const ULONG: u8 = 0x80;
    pub const SMALL_ULONG: u8 = 0x53;
    pub const ULONG0: u8 = 0x44;
    pub const BYTE: u8 = 0x51;
    pub const SHORT: u8 = 0x61;
    pub const INT: u8 = 0x71;
    pub const SMALL_INT: u8 = 0x54;
    pub const LONG: u8 = 0x81;
    pub const SMALL_LONG: u8 = 0x55;
    pub const FLOAT: u8 = 0x72;
    pub const DOUBLE: u8 = 0x82;
    pub const DECIMAL32: u8 = 0x74;
    pub const DECIMAL64: u8 = 0x84;
    pub const DECIMAL128: u8 = 0x94;
    pub const CHAR: u8 = 0x73;
    pub const TIMESTAMP: u8 = 0x83;
    pub const UUID: u8 = 0x98;
    pub const VBIN8: u8 = 0xa0;
    pub const VBIN32: u8 = 0xb0;
    pub const STR8: u8 = 0xa1;
    pub const STR32: u8 = 0xb1;
    pub const SYM8: u8 = 0xa3;
    pub const SYM32: u8 = 0xb3;
    pub const LIST0: u8 = 0x45;
    pub const LIST8: u8 = 0xc0;
    pub const LIST32: u8 = 0xd0;
    pub const MAP8: u8 = 0xc1;
    pub const MAP32: u8 = 0xd1;
    pub const ARRAY8: u8 = 0xe0;
    pub const ARRAY32: u8 = 0xf0;
}

// ---------------------------------------------------------------- encoding

/// Encodes `value` onto `out`.
pub fn encode(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.push(code::NULL),
        Value::Boolean(b) => out.push(if *b { code::TRUE } else { code::FALSE }),
        Value::Ubyte(v) => out.extend([code::UBYTE, *v]),
        Value::Ushort(v) => {
            out.push(code::USHORT);
            out.extend(v.to_be_bytes());
        }
        Value::Uint(v) => encode_uint(*v, out),
        Value::Ulong(v) => encode_ulong(*v, out),
        Value::Byte(v) => out.extend([code::BYTE, *v as u8]),
        Value::Short(v) => {
            out.push(code::SHORT);
            out.extend(v.to_be_bytes());
        }
        Value::Int(v) => {
            if let Ok(b) = i8::try_from(*v) {
                out.extend([code::SMALL_INT, b as u8]);
            } else {
                out.push(code::INT);
                out.extend(v.to_be_bytes());
            }
        }
        Value::Long(v) => {
            if let Ok(b) = i8::try_from(*v) {
                out.extend([code::SMALL_LONG, b as u8]);
            } else {
                out.push(code::LONG);
                out.extend(v.to_be_bytes());
            }
        }
        Value::Float(v) => {
            out.push(code::FLOAT);
            out.extend(v.to_bits().to_be_bytes());
        }
        Value::Double(v) => {
            out.push(code::DOUBLE);
            out.extend(v.to_bits().to_be_bytes());
        }
        Value::Decimal32(v) => {
            out.push(code::DECIMAL32);
            out.extend(v);
        }
        Value::Decimal64(v) => {
            out.push(code::DECIMAL64);
            out.extend(v);
        }
        Value::Decimal128(v) => {
            out.push(code::DECIMAL128);
            out.extend(v);
        }
        Value::Char(v) => {
            out.push(code::CHAR);
            out.extend(v.to_be_bytes());
        }
        Value::Timestamp(v) => {
            out.push(code::TIMESTAMP);
            out.extend(v.to_be_bytes());
        }
        Value::Uuid(v) => {
            out.push(code::UUID);
            out.extend(v);
        }
        Value::Binary(b) => variable(code::VBIN8, code::VBIN32, b, out),
        Value::String(s) => variable(code::STR8, code::STR32, s.as_bytes(), out),
        Value::Symbol(s) => variable(code::SYM8, code::SYM32, s.0.as_bytes(), out),
        Value::List(items) => encode_list(items, out),
        Value::Map(entries) => encode_map(entries, out),
        Value::Array(a) => encode_array(a, out),
        Value::Described(d, v) => {
            out.push(code::DESCRIBED);
            encode_descriptor(d, out);
            encode(v, out);
        }
    }
}

/// Encodes `value` into a new buffer.
pub fn to_vec(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode(value, &mut out);
    out
}

pub fn encode_descriptor(d: &Descriptor, out: &mut Vec<u8>) {
    match d {
        Descriptor::Code(c) => encode_ulong(*c, out),
        Descriptor::Name(n) => encode(&Value::Symbol(n.clone()), out),
    }
}

fn encode_uint(v: u32, out: &mut Vec<u8>) {
    if v == 0 {
        out.push(code::UINT0);
    } else if let Ok(b) = u8::try_from(v) {
        out.extend([code::SMALL_UINT, b]);
    } else {
        out.push(code::UINT);
        out.extend(v.to_be_bytes());
    }
}

fn encode_ulong(v: u64, out: &mut Vec<u8>) {
    if v == 0 {
        out.push(code::ULONG0);
    } else if let Ok(b) = u8::try_from(v) {
        out.extend([code::SMALL_ULONG, b]);
    } else {
        out.push(code::ULONG);
        out.extend(v.to_be_bytes());
    }
}

pub(crate) fn variable(short: u8, long: u8, bytes: &[u8], out: &mut Vec<u8>) {
    if let Ok(n) = u8::try_from(bytes.len()) {
        out.extend([short, n]);
    } else {
        out.push(long);
        out.extend((bytes.len() as u32).to_be_bytes());
    }
    out.extend(bytes);
}

/// Writes a compound (list or map) of `count` elements whose encodings are
/// `body`.
fn compound(short: u8, long: u8, count: usize, body: &[u8], out: &mut Vec<u8>) {
    // ListType/MapType: the short form while the encoded elements take at
    // most 253 bytes.
    if count <= 255 && body.len() <= 253 {
        out.extend([short, (body.len() + 1) as u8, count as u8]);
    } else {
        out.push(long);
        out.extend(((body.len() + 4) as u32).to_be_bytes());
        out.extend((count as u32).to_be_bytes());
    }
    out.extend(body);
}

fn encode_list(items: &[Value], out: &mut Vec<u8>) {
    if items.is_empty() {
        out.push(code::LIST0);
        return;
    }
    let mut body = Vec::new();
    for v in items {
        encode(v, &mut body);
    }
    compound(code::LIST8, code::LIST32, items.len(), &body, out);
}

fn encode_map(entries: &[(Value, Value)], out: &mut Vec<u8>) {
    let mut body = Vec::new();
    for (k, v) in entries {
        encode(k, &mut body);
        encode(v, &mut body);
    }
    compound(code::MAP8, code::MAP32, entries.len() * 2, &body, out);
}

/// Writes one array element without its constructor.
fn encode_element(cons: u8, v: &Value, out: &mut Vec<u8>) {
    let mut tmp = Vec::new();
    match (cons, v) {
        (code::BOOLEAN, Value::Boolean(b)) => out.push(u8::from(*b)),
        (code::UINT, v) => out.extend(v.as_u32().unwrap_or(0).to_be_bytes()),
        (code::ULONG, v) => out.extend(v.as_u64().unwrap_or(0).to_be_bytes()),
        (code::INT, v) => out.extend((v.as_i64().unwrap_or(0) as i32).to_be_bytes()),
        (code::LONG, v) => out.extend(v.as_i64().unwrap_or(0).to_be_bytes()),
        (code::STR32 | code::SYM32 | code::VBIN32, v) => {
            let bytes: &[u8] = match v {
                Value::String(s) => s.as_bytes(),
                Value::Symbol(s) => s.0.as_bytes(),
                Value::Binary(b) => b,
                _ => &[],
            };
            out.extend((bytes.len() as u32).to_be_bytes());
            out.extend(bytes);
        }
        (code::LIST32, Value::List(items)) => {
            for i in items {
                encode(i, &mut tmp);
            }
            out.extend(((tmp.len() + 4) as u32).to_be_bytes());
            out.extend((items.len() as u32).to_be_bytes());
            out.extend(tmp);
        }
        (code::MAP32, Value::Map(entries)) => {
            for (k, v) in entries {
                encode(k, &mut tmp);
                encode(v, &mut tmp);
            }
            out.extend(((tmp.len() + 4) as u32).to_be_bytes());
            out.extend(((entries.len() * 2) as u32).to_be_bytes());
            out.extend(tmp);
        }
        (code::ARRAY32, Value::Array(a)) => {
            encode_array_body(a, true, &mut tmp);
            out.extend(tmp);
        }
        (_, v) => {
            // Fixed-width types: the value's own encoding minus its code.
            encode(v, &mut tmp);
            out.extend(&tmp[1..]);
        }
    }
}

fn encode_array(a: &Array, out: &mut Vec<u8>) {
    let mut body = Vec::new();
    encode_array_body(a, false, &mut body);
    out.extend(body);
}

/// An array's encoding; `wide` forces `array32` (an array inside an array).
fn encode_array_body(a: &Array, wide: bool, out: &mut Vec<u8>) {
    let (descriptor, prim) = match &a.element {
        ArrayType::Primitive(p) => (None, *p),
        ArrayType::Described(d, p) => (Some(d), *p),
    };
    // ArrayType writes every element with the type's widest constructor.
    let cons = canonical(prim);
    let mut ctor = Vec::new();
    if let Some(d) = descriptor {
        ctor.push(code::DESCRIBED);
        encode_descriptor(d, &mut ctor);
    }
    ctor.push(cons);
    let mut elems = Vec::new();
    for v in &a.items {
        encode_element(cons, v, &mut elems);
    }
    let size = ctor.len() + elems.len();
    // The short form while the constructor and elements take at most 254
    // bytes.
    if !wide && a.items.len() <= 255 && size <= 254 {
        out.extend([code::ARRAY8, (size + 1) as u8, a.items.len() as u8]);
    } else {
        if !wide {
            out.push(code::ARRAY32);
        }
        out.extend(((size + 4) as u32).to_be_bytes());
        out.extend((a.items.len() as u32).to_be_bytes());
    }
    out.extend(ctor);
    out.extend(elems);
}

// ---------------------------------------------------------------- decoding

/// How deep described values and compounds may nest. Each level is a
/// recursion when a value is decoded, encoded and dropped, so a peer must
/// not choose it: a few kilobytes of nesting would overflow a stack.
pub const MAX_DEPTH: usize = 100;

/// Reads AMQP values from a byte slice.
#[derive(Debug, Clone)]
pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
    /// The described values and compounds this decoder is inside.
    depth: usize,
}

impl<'a> Decoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            depth: 0,
        }
    }

    /// Goes one level deeper, failing past [`MAX_DEPTH`].
    fn deeper(&self) -> Result<usize> {
        if self.depth >= MAX_DEPTH {
            return invalid(format!("values nested more than {MAX_DEPTH} deep"));
        }
        Ok(self.depth + 1)
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn set_position(&mut self, pos: usize) {
        self.pos = pos.min(self.buf.len());
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn has_remaining(&self) -> bool {
        self.pos < self.buf.len()
    }

    pub fn rest(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(CodecError::Eof);
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("length checked"))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    /// Peeks at the next format code without consuming it.
    pub fn peek(&self) -> Result<u8> {
        self.buf.get(self.pos).copied().ok_or(CodecError::Eof)
    }

    /// Reads one value.
    pub fn read(&mut self) -> Result<Value> {
        let c = self.u8()?;
        if c == code::DESCRIBED {
            let d = self.read_descriptor()?;
            let depth = self.depth;
            self.depth = self.deeper()?;
            let v = self.read();
            self.depth = depth;
            return Ok(Value::Described(Box::new(d), Box::new(v?)));
        }
        self.read_primitive(c)
    }

    /// Reads a descriptor (after the `0x00`): a ulong or a symbol, never
    /// itself described.
    pub fn read_descriptor(&mut self) -> Result<Descriptor> {
        let c = self.u8()?;
        if c == code::DESCRIBED {
            return invalid("described descriptor");
        }
        match self.read_primitive(c)? {
            Value::Ulong(c) => Ok(Descriptor::Code(c)),
            Value::Symbol(s) => Ok(Descriptor::Name(s)),
            other => invalid(format!("descriptor {other:?}")),
        }
    }

    /// Reads the `0x00` and descriptor of a described value, leaving the
    /// value next; `None`, reading nothing, when the next value is not
    /// described.
    pub fn read_described_head(&mut self) -> Result<Option<Descriptor>> {
        if self.peek()? != code::DESCRIBED {
            return Ok(None);
        }
        self.pos += 1;
        self.read_descriptor().map(Some)
    }

    /// Reads a list, giving the bytes of its first fields, as many as `out`
    /// holds, without decoding them; fields the list does not have are
    /// `None`.
    pub fn list_fields(&mut self, out: &mut [Option<&'a [u8]>]) -> Result<()> {
        out.fill(None);
        let c = self.u8()?;
        let (count, mut d) = match c {
            code::LIST0 => return Ok(()),
            code::LIST8 | code::LIST32 => self.compound(c == code::LIST32)?,
            other => return invalid(format!("list expected, found {other:#04x}")),
        };
        for i in 0..count {
            let field = d.skip()?;
            if let Some(slot) = out.get_mut(i) {
                *slot = Some(field);
            }
        }
        Ok(())
    }

    /// Skips one value, returning its bytes.
    pub fn skip(&mut self) -> Result<&'a [u8]> {
        let start = self.pos;
        self.skip_value()?;
        Ok(&self.buf[start..self.pos])
    }

    fn skip_value(&mut self) -> Result<()> {
        // Each descriptor, then the value they describe.
        let mut c = self.u8()?;
        while c == code::DESCRIBED {
            let d = self.u8()?;
            if d == code::DESCRIBED {
                return invalid("described descriptor");
            }
            self.skip_primitive(d)?;
            c = self.u8()?;
        }
        self.skip_primitive(c)
    }

    fn skip_primitive(&mut self, c: u8) -> Result<()> {
        let n = match c >> 4 {
            0x4 => 0,
            0x5 => 1,
            0x6 => 2,
            0x7 => 4,
            0x8 => 8,
            0x9 => 16,
            0xa | 0xc | 0xe => self.u8()? as usize,
            0xb | 0xd | 0xf => self.u32()? as usize,
            _ => return invalid(format!("format code 0x{c:02x}")),
        };
        self.take(n)?;
        Ok(())
    }

    fn read_primitive(&mut self, c: u8) -> Result<Value> {
        Ok(match c {
            code::NULL => Value::Null,
            code::TRUE => Value::Boolean(true),
            code::FALSE => Value::Boolean(false),
            code::BOOLEAN => match self.u8()? {
                0 => Value::Boolean(false),
                1 => Value::Boolean(true),
                b => return invalid(format!("boolean {b}")),
            },
            code::UBYTE => Value::Ubyte(self.u8()?),
            code::USHORT => Value::Ushort(u16::from_be_bytes(self.array()?)),
            code::UINT => Value::Uint(self.u32()?),
            code::SMALL_UINT => Value::Uint(self.u8()?.into()),
            code::UINT0 => Value::Uint(0),
            code::ULONG => Value::Ulong(u64::from_be_bytes(self.array()?)),
            code::SMALL_ULONG => Value::Ulong(self.u8()?.into()),
            code::ULONG0 => Value::Ulong(0),
            code::BYTE => Value::Byte(self.u8()? as i8),
            code::SHORT => Value::Short(i16::from_be_bytes(self.array()?)),
            code::INT => Value::Int(i32::from_be_bytes(self.array()?)),
            code::SMALL_INT => Value::Int((self.u8()? as i8).into()),
            code::LONG => Value::Long(i64::from_be_bytes(self.array()?)),
            code::SMALL_LONG => Value::Long((self.u8()? as i8).into()),
            code::FLOAT => Value::Float(f32::from_bits(self.u32()?)),
            code::DOUBLE => Value::Double(f64::from_bits(u64::from_be_bytes(self.array()?))),
            code::DECIMAL32 => Value::Decimal32(self.array()?),
            code::DECIMAL64 => Value::Decimal64(self.array()?),
            code::DECIMAL128 => Value::Decimal128(self.array()?),
            code::CHAR => Value::Char(self.u32()?),
            code::TIMESTAMP => Value::Timestamp(i64::from_be_bytes(self.array()?)),
            code::UUID => Value::Uuid(self.array()?),
            code::VBIN8 | code::VBIN32 => Value::Binary(self.sized(c == code::VBIN32)?.to_vec()),
            code::STR8 | code::STR32 => Value::String(
                std::str::from_utf8(self.sized(c == code::STR32)?)
                    .map_err(|e| CodecError::Invalid(format!("string: {e}")))?
                    .to_owned(),
            ),
            code::SYM8 | code::SYM32 => Value::Symbol(Symbol(
                String::from_utf8_lossy(self.sized(c == code::SYM32)?).into_owned(),
            )),
            code::LIST0 => Value::List(Vec::new()),
            code::LIST8 | code::LIST32 => {
                let (count, mut d) = self.compound(c == code::LIST32)?;
                let mut items = Vec::with_capacity(count.min(d.remaining()));
                for _ in 0..count {
                    items.push(d.read()?);
                }
                Value::List(items)
            }
            code::MAP8 | code::MAP32 => {
                let (count, mut d) = self.compound(c == code::MAP32)?;
                if count % 2 != 0 {
                    return invalid(format!("map with {count} elements"));
                }
                let mut entries = Vec::with_capacity((count / 2).min(d.remaining()));
                for _ in 0..count / 2 {
                    let k = d.read()?;
                    let v = d.read()?;
                    entries.push((k, v));
                }
                Value::Map(entries)
            }
            code::ARRAY8 | code::ARRAY32 => {
                let (count, mut d) = self.compound(c == code::ARRAY32)?;
                Value::Array(d.array_body(count)?)
            }
            other => return invalid(format!("format code 0x{other:02x}")),
        })
    }

    fn sized(&mut self, wide: bool) -> Result<&'a [u8]> {
        let n = if wide {
            self.u32()? as usize
        } else {
            self.u8()? as usize
        };
        self.take(n)
    }

    /// A compound's count and a decoder over its elements.
    fn compound(&mut self, wide: bool) -> Result<(usize, Decoder<'a>)> {
        let (size, count) = if wide {
            let size = self.u32()? as usize;
            if size < 4 {
                return invalid("compound size");
            }
            (size - 4, self.u32()? as usize)
        } else {
            let size = self.u8()? as usize;
            if size < 1 {
                return invalid("compound size");
            }
            (size - 1, self.u8()? as usize)
        };
        let depth = self.deeper()?;
        Ok((
            count,
            Decoder {
                depth,
                ..Decoder::new(self.take(size)?)
            },
        ))
    }

    fn array_body(&mut self, count: usize) -> Result<Array> {
        let mut c = self.u8()?;
        let element = if c == code::DESCRIBED {
            let d = self.read_descriptor()?;
            c = self.u8()?;
            ArrayType::Described(Box::new(d), canonical(c))
        } else {
            ArrayType::Primitive(canonical(c))
        };
        // As proton-j's ArrayType (its zero-width element limit is 0 by
        // default): a zero-width element costs no bytes, so its count would
        // be bounded by nothing; every other element takes at least a byte.
        let zero_width = matches!(
            c,
            code::NULL | code::TRUE | code::FALSE | code::UINT0 | code::ULONG0 | code::LIST0
        );
        if count > 0 && zero_width {
            return invalid(format!("array of {count} zero-width elements"));
        }
        if count > self.remaining() {
            return invalid(format!(
                "array element count {count} greater than the {} bytes left",
                self.remaining()
            ));
        }
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(self.read_primitive(c)?);
        }
        Ok(Array { element, items })
    }
}

/// The widest format code of a primitive type, naming the type.
fn canonical(c: u8) -> u8 {
    match c {
        code::TRUE | code::FALSE => code::BOOLEAN,
        code::SMALL_UINT | code::UINT0 => code::UINT,
        code::SMALL_ULONG | code::ULONG0 => code::ULONG,
        code::SMALL_INT => code::INT,
        code::SMALL_LONG => code::LONG,
        code::VBIN8 => code::VBIN32,
        code::STR8 => code::STR32,
        code::SYM8 => code::SYM32,
        code::LIST0 | code::LIST8 => code::LIST32,
        code::MAP8 => code::MAP32,
        code::ARRAY8 => code::ARRAY32,
        other => other,
    }
}

/// Decodes exactly one value filling `bytes`.
pub fn from_slice(bytes: &[u8]) -> Result<Value> {
    let mut d = Decoder::new(bytes);
    let v = d.read()?;
    if d.has_remaining() {
        return invalid("trailing bytes after value");
    }
    Ok(v)
}

/// The text of exactly one encoded `string` filling `bytes`, read in
/// place; `None` for anything else.
pub fn utf8_string(bytes: &[u8]) -> Option<&str> {
    let text = match *bytes.first()? {
        code::STR8 => {
            let (&len, rest) = bytes[1..].split_first()?;
            (rest.len() == usize::from(len)).then_some(rest)?
        }
        code::STR32 => {
            let len = u32::from_be_bytes(bytes.get(1..5)?.try_into().ok()?);
            let rest = &bytes[5..];
            (rest.len() == len as usize).then_some(rest)?
        }
        _ => return None,
    };
    std::str::from_utf8(text).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_string_in_place() {
        for s in ["", "queue", &"x".repeat(300)] {
            let mut out = Vec::new();
            encode(&Value::String(s.to_owned()), &mut out);
            assert_eq!(utf8_string(&out), Some(s));
        }
        assert_eq!(utf8_string(&[code::STR8, 2, b'a']), None);
        assert_eq!(utf8_string(&[code::NULL]), None);
    }

    fn rt(v: Value) -> Vec<u8> {
        let bytes = to_vec(&v);
        assert_eq!(from_slice(&bytes).unwrap(), v, "{bytes:02x?}");
        bytes
    }

    #[test]
    fn smallest_forms() {
        assert_eq!(rt(Value::Uint(0)), [0x43]);
        assert_eq!(rt(Value::Uint(7)), [0x52, 7]);
        assert_eq!(rt(Value::Uint(256)), [0x70, 0, 0, 1, 0]);
        assert_eq!(rt(Value::Ulong(0x10)), [0x53, 0x10]);
        assert_eq!(rt(Value::Int(-1)), [0x54, 0xff]);
        assert_eq!(rt(Value::Long(200)), [0x81, 0, 0, 0, 0, 0, 0, 0, 200]);
        assert_eq!(rt(Value::Boolean(true)), [0x41]);
        assert_eq!(rt(Value::List(vec![])), [0x45]);
        assert_eq!(rt(Value::string("ab")), [0xa1, 2, b'a', b'b']);
    }

    #[test]
    fn compounds() {
        let v = Value::Map(vec![(Value::symbol("k"), Value::Int(1))]);
        assert_eq!(rt(v), [0xc1, 6, 2, 0xa3, 1, b'k', 0x54, 1]);
        let long = Value::List(vec![Value::Binary(vec![0; 300])]);
        assert_eq!(rt(long)[0], 0xd0);
        let arr = Value::Array(Array {
            element: ArrayType::Primitive(code::SYM32),
            items: vec![Value::symbol("a"), Value::symbol("bc")],
        });
        assert_eq!(
            rt(arr),
            [
                0xe0, 0x0d, 2, 0xb3, 0, 0, 0, 1, b'a', 0, 0, 0, 2, b'b', b'c'
            ]
        );
        let described = Value::described(Descriptor::Code(0x70), Value::List(vec![Value::Null]));
        assert_eq!(rt(described), [0x00, 0x53, 0x70, 0xc0, 2, 1, 0x40]);
    }

    #[test]
    fn skip_returns_the_value_bytes() {
        let bytes = to_vec(&Value::List(vec![Value::Int(1), Value::string("x")]));
        let mut d = Decoder::new(&bytes);
        assert_eq!(d.skip().unwrap(), &bytes[..]);
        assert!(!d.has_remaining());
    }

    #[test]
    fn array_counts_are_bounded_by_their_bytes() {
        // A billion nulls in two bytes, as proton-j refuses.
        assert!(from_slice(&[0xf0, 0, 0, 0, 5, 0x3b, 0x9a, 0xca, 0x00, 0x40]).is_err());
        assert!(from_slice(&[0xe0, 2, 3, 0x41]).is_err());
        assert!(from_slice(&[0xe0, 3, 3, 0x50, 1]).is_err());
        // No elements is fine whatever their type.
        let empty = from_slice(&[0xe0, 2, 0, 0x40]).unwrap();
        assert_eq!(
            empty,
            Value::Array(Array {
                element: ArrayType::Primitive(code::NULL),
                items: vec![]
            })
        );
    }
}
