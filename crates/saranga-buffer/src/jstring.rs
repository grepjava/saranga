use std::fmt;

use crate::{DecodeError, Reader, Result, SimpleString, Writer};

/// A Java `String`: a sequence of UTF-16 code units, which may contain
/// unpaired surrogates. Used for fields Artemis writes with `writeString` /
/// `writeUTF` (as opposed to `SimpleString`).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct JString(Vec<u16>);

/// Strings shorter than this are written as raw chars (`UTF8Util.writeString`).
const SHORT_LIMIT: usize = 9;
/// Strings shorter than this (and not short) are written as modified UTF-8.
const UTF_LIMIT: usize = 0xfff;

impl JString {
    pub fn from_code_units(units: Vec<u16>) -> Self {
        Self(units)
    }

    pub fn code_units(&self) -> &[u16] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn to_string_lossy(&self) -> String {
        String::from_utf16_lossy(&self.0)
    }

    /// `UTF8Util.writeString`.
    pub(crate) fn encode(&self, w: &mut Writer) {
        let len = self.0.len();
        w.i32(len as i32);
        if len < SHORT_LIMIT {
            for &c in &self.0 {
                w.u16(c);
            }
        } else if len < UTF_LIMIT {
            self.encode_utf(w);
        } else {
            w.simple_string(&self.to_simple_string());
        }
    }

    /// `ChannelBufferWrapper.readStringInternal`.
    pub(crate) fn decode(r: &mut Reader<'_>) -> Result<Self> {
        let len = r.len("string")?;
        if len < SHORT_LIMIT {
            (0..len).map(|_| r.u16()).collect::<Result<_>>().map(Self)
        } else if len < UTF_LIMIT {
            Self::decode_utf(r)
        } else {
            let s = r.simple_string()?;
            Ok(Self(s.code_units().collect()))
        }
    }

    /// Byte length of the modified UTF-8 form.
    pub fn utf_len(&self) -> usize {
        self.0
            .iter()
            .map(|&c| match c {
                0..=0x7f => 1,
                0x80..=0x7ff => 2,
                _ => 3,
            })
            .sum()
    }

    /// `UTF8Util.saveUTF`: u16 byte length then Java modified UTF-8 (each
    /// code unit encoded independently, NUL as a single 0 byte).
    ///
    /// # Panics
    /// If the encoded form exceeds 0xffff bytes, as Artemis throws.
    pub(crate) fn encode_utf(&self, w: &mut Writer) {
        let len = self.utf_len();
        assert!(len <= 0xffff, "string too long for writeUTF: {len} bytes");
        w.u16(len as u16);
        for &c in &self.0 {
            match c {
                0..=0x7f => {
                    w.u8(c as u8);
                }
                0x80..=0x7ff => {
                    w.u8(0xc0 | (c >> 6) as u8 & 0x1f).u8(0x80 | c as u8 & 0x3f);
                }
                _ => {
                    w.u8(0xe0 | (c >> 12) as u8 & 0x0f)
                        .u8(0x80 | (c >> 6) as u8 & 0x3f)
                        .u8(0x80 | c as u8 & 0x3f);
                }
            }
        }
    }

    /// `UTF8Util.readUTF`.
    pub(crate) fn decode_utf(r: &mut Reader<'_>) -> Result<Self> {
        let size = r.u16()? as usize;
        let bytes = r.bytes(size)?;
        let mut out = Vec::with_capacity(size);
        let mut i = 0;
        let byte = |i: usize| {
            bytes
                .get(i)
                .map(|&b| b as u16)
                .ok_or_else(|| DecodeError::Invalid("truncated modified UTF-8".into()))
        };
        while i < size {
            let c = byte(i)?;
            match c >> 4 {
                0x0..=0x7 => {
                    out.push(c);
                    i += 1;
                }
                0xc | 0xd => {
                    out.push((c & 0x1f) << 6 | byte(i + 1)? & 0x3f);
                    i += 2;
                }
                0xe => {
                    out.push((c & 0x0f) << 12 | (byte(i + 1)? & 0x3f) << 6 | byte(i + 2)? & 0x3f);
                    i += 3;
                }
                _ => return Err(DecodeError::Invalid(format!("unhandled utf8 byte {c:#x}"))),
            }
        }
        Ok(Self(out))
    }

    pub fn to_simple_string(&self) -> SimpleString {
        SimpleString::from_bytes(self.0.iter().flat_map(|c| c.to_le_bytes()).collect())
    }
}

impl From<&str> for JString {
    fn from(s: &str) -> Self {
        Self(s.encode_utf16().collect())
    }
}

impl From<String> for JString {
    fn from(s: String) -> Self {
        s.as_str().into()
    }
}

impl fmt::Display for JString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_string_lossy())
    }
}

impl fmt::Debug for JString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.to_string_lossy())
    }
}
