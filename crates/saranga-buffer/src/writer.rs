use crate::{JString, SimpleString, constants};

/// Growable output buffer. Mirrors the write side of `ActiveMQBuffer`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Writer {
    buf: Vec<u8>,
}

macro_rules! write_be {
    ($name:ident, $ty:ty) => {
        pub fn $name(&mut self, v: $ty) -> &mut Self {
            self.buf.extend_from_slice(&v.to_be_bytes());
            self
        }
    };
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            buf: Vec::with_capacity(n),
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }

    pub fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(b);
        self
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }

    pub fn i8(&mut self, v: i8) -> &mut Self {
        self.u8(v as u8)
    }

    /// `ActiveMQBuffer.writeBoolean`: true is written as `0xFF` (-1).
    /// Used by packets and journal records.
    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.u8(if v { 0xff } else { 0 })
    }

    /// Netty `ByteBuf.writeBoolean`: true is written as `0x01`. Used where
    /// Artemis writes to the raw `ByteBuf` (`TypedProperties`, `CoreMessage`
    /// headers).
    pub fn byte_buf_bool(&mut self, v: bool) -> &mut Self {
        self.u8(v as u8)
    }

    write_be!(i16, i16);
    write_be!(u16, u16);
    write_be!(i32, i32);
    write_be!(i64, i64);

    pub fn f32(&mut self, v: f32) -> &mut Self {
        self.bytes(&v.to_bits().to_be_bytes())
    }

    pub fn f64(&mut self, v: f64) -> &mut Self {
        self.bytes(&v.to_bits().to_be_bytes())
    }

    /// Overwrites a previously written i32, e.g. a length placeholder.
    pub fn set_i32(&mut self, at: usize, v: i32) {
        self.buf[at..at + 4].copy_from_slice(&v.to_be_bytes());
    }

    pub fn simple_string(&mut self, s: &SimpleString) -> &mut Self {
        self.byte_array(s.as_bytes())
    }

    pub fn nullable_simple_string(&mut self, s: Option<&SimpleString>) -> &mut Self {
        self.nullable(s, Self::simple_string)
    }

    /// `ActiveMQBuffer.writeString`.
    pub fn string(&mut self, s: &JString) -> &mut Self {
        s.encode(self);
        self
    }

    pub fn nullable_string(&mut self, s: Option<&JString>) -> &mut Self {
        self.nullable(s, Self::string)
    }

    /// `ActiveMQBuffer.writeUTF`.
    pub fn utf(&mut self, s: &JString) -> &mut Self {
        s.encode_utf(self);
        self
    }

    pub fn nullable_bool(&mut self, v: Option<bool>) -> &mut Self {
        self.nullable(v.as_ref(), |w, v| w.bool(*v))
    }

    pub fn nullable_i32(&mut self, v: Option<i32>) -> &mut Self {
        self.nullable(v.as_ref(), |w, v| w.i32(*v))
    }

    pub fn nullable_i64(&mut self, v: Option<i64>) -> &mut Self {
        self.nullable(v.as_ref(), |w, v| w.i64(*v))
    }

    pub fn nullable<T: ?Sized>(
        &mut self,
        v: Option<&T>,
        f: impl for<'w> FnOnce(&'w mut Self, &T) -> &'w mut Self,
    ) -> &mut Self {
        match v {
            None => self.u8(constants::NULL),
            Some(v) => f(self.u8(constants::NOT_NULL), v),
        }
    }

    /// An i32-length-prefixed byte array.
    pub fn byte_array(&mut self, b: &[u8]) -> &mut Self {
        self.i32(b.len() as i32).bytes(b)
    }
}
