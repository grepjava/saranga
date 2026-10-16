use crate::{DecodeError, JString, Result, SimpleString, constants};

/// Cursor over an encoded buffer. Mirrors the read side of `ActiveMQBuffer`.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    /// The buffer as shared bytes, when strings are to be slices of it
    /// rather than copies.
    shared: Option<&'a bytes::Bytes>,
}

macro_rules! read_be {
    ($name:ident, $ty:ty) => {
        pub fn $name(&mut self) -> Result<$ty> {
            Ok(<$ty>::from_be_bytes(self.array()?))
        }
    };
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            shared: None,
        }
    }

    /// A reader whose strings are slices of `buf`, sharing it.
    pub fn shared(buf: &'a bytes::Bytes) -> Self {
        Self {
            buf,
            pos: 0,
            shared: Some(buf),
        }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// The unread part of the buffer.
    pub fn rest(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(DecodeError::UnexpectedEof {
                needed: n,
                remaining: self.remaining(),
            });
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    pub fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.bytes(N)?.try_into().expect("length checked"))
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    pub fn i8(&mut self) -> Result<i8> {
        Ok(self.u8()? as i8)
    }

    /// Any non-zero byte is true (both `ActiveMQBuffer` and Netty).
    pub fn bool(&mut self) -> Result<bool> {
        Ok(self.u8()? != 0)
    }

    read_be!(i16, i16);
    read_be!(u16, u16);
    read_be!(i32, i32);
    read_be!(i64, i64);

    pub fn f32(&mut self) -> Result<f32> {
        Ok(f32::from_bits(u32::from_be_bytes(self.array()?)))
    }

    pub fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(u64::from_be_bytes(self.array()?)))
    }

    /// Reads an i32 length that must be non-negative.
    pub fn len(&mut self, what: &str) -> Result<usize> {
        let len = self.i32()?;
        usize::try_from(len)
            .map_err(|_| DecodeError::Invalid(format!("negative {what} length {len}")))
    }

    /// Reads a nullable marker byte; `true` means a value follows.
    pub fn not_null(&mut self) -> Result<bool> {
        Ok(self.u8()? != constants::NULL)
    }

    pub fn simple_string(&mut self) -> Result<SimpleString> {
        let len = self.len("SimpleString")?;
        let data = self.bytes(len)?;
        Ok(match self.shared {
            Some(shared) => SimpleString::from_shared(shared.slice_ref(data)),
            None => SimpleString::from_bytes(data.to_vec()),
        })
    }

    pub fn nullable_simple_string(&mut self) -> Result<Option<SimpleString>> {
        self.nullable(Self::simple_string)
    }

    /// `ActiveMQBuffer.readString`.
    pub fn string(&mut self) -> Result<JString> {
        JString::decode(self)
    }

    pub fn nullable_string(&mut self) -> Result<Option<JString>> {
        self.nullable(Self::string)
    }

    /// `ActiveMQBuffer.readUTF`: u16 byte length then Java modified UTF-8.
    pub fn utf(&mut self) -> Result<JString> {
        JString::decode_utf(self)
    }

    pub fn nullable_bool(&mut self) -> Result<Option<bool>> {
        self.nullable(Self::bool)
    }

    pub fn nullable_i32(&mut self) -> Result<Option<i32>> {
        self.nullable(Self::i32)
    }

    pub fn nullable_i64(&mut self) -> Result<Option<i64>> {
        self.nullable(Self::i64)
    }

    pub fn nullable<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<Option<T>> {
        if self.not_null()? {
            f(self).map(Some)
        } else {
            Ok(None)
        }
    }

    /// An i32-length-prefixed byte array.
    pub fn byte_array(&mut self) -> Result<Vec<u8>> {
        let len = self.len("byte array")?;
        Ok(self.bytes(len)?.to_vec())
    }
}
