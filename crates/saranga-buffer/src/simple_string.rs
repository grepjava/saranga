use std::fmt;

/// Artemis `SimpleString`: a string stored as raw UTF-16LE bytes, encoded on
/// the wire as an i32 byte length followed by those bytes.
///
/// The bytes are kept verbatim so that any value read off the wire or the
/// journal re-encodes identically, even if it is not valid UTF-16.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct SimpleString(Vec<u8>);

impl SimpleString {
    pub fn from_bytes(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// UTF-16 code units (`char`s in Java terms).
    pub fn code_units(&self) -> impl Iterator<Item = u16> + '_ {
        self.0
            .as_chunks::<2>()
            .0
            .iter()
            .map(|&c| u16::from_le_bytes(c))
    }

    /// Lossy conversion: unpaired surrogates become U+FFFD.
    pub fn to_string_lossy(&self) -> String {
        char::decode_utf16(self.code_units())
            .map(|r| r.unwrap_or(char::REPLACEMENT_CHARACTER))
            .collect()
    }
}

impl From<&str> for SimpleString {
    fn from(s: &str) -> Self {
        Self(s.encode_utf16().flat_map(u16::to_le_bytes).collect())
    }
}

impl From<String> for SimpleString {
    fn from(s: String) -> Self {
        s.as_str().into()
    }
}

impl fmt::Display for SimpleString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_string_lossy())
    }
}

impl fmt::Debug for SimpleString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.to_string_lossy())
    }
}
