use std::fmt;

use bytes::Bytes;

/// Artemis `SimpleString`: a string stored as raw UTF-16LE bytes, encoded on
/// the wire as an i32 byte length followed by those bytes.
///
/// The bytes are kept verbatim so that any value read off the wire or the
/// journal re-encodes identically, even if it is not valid UTF-16. They are
/// shared: cloning one, as messages, queues and keys do, copies nothing.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct SimpleString(Bytes);

impl SimpleString {
    pub fn from_bytes(data: Vec<u8>) -> Self {
        Self(Bytes::from(data))
    }

    /// One sharing `data`, as a slice of a message's encoding is.
    pub fn from_shared(data: Bytes) -> Self {
        Self(data)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether this is `s`, compared without making a `SimpleString` of it.
    pub fn eq_str(&self, s: &str) -> bool {
        // UTF-16 never takes more code units than UTF-8 takes bytes.
        if self.0.len() > s.len() * 2 {
            return false;
        }
        if s.is_ascii() {
            return self.0.len() == s.len() * 2
                && self
                    .0
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .zip(s.as_bytes())
                    .all(|(c, &b)| *c == [b, 0]);
        }
        self.code_units().eq(s.encode_utf16())
    }

    /// Whether this starts with `prefix`, compared as [`eq_str`](Self::eq_str).
    pub fn starts_with_str(&self, prefix: &str) -> bool {
        let mut units = self.code_units();
        prefix.encode_utf16().all(|u| units.next() == Some(u))
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
        self.decode_lossy(char::REPLACEMENT_CHARACTER)
    }

    /// The text, with `replacement` for each unpaired surrogate. Names
    /// are nearly always ASCII, which is copied byte by byte.
    pub fn decode_lossy(&self, replacement: char) -> String {
        let units = self.0.as_chunks::<2>().0;
        if units.iter().all(|&[lo, hi]| hi == 0 && lo < 0x80) {
            let ascii: Vec<u8> = units.iter().map(|&[lo, _]| lo).collect();
            return String::from_utf8(ascii).unwrap_or_default();
        }
        let mut s = String::with_capacity(units.len());
        s.extend(char::decode_utf16(self.code_units()).map(|r| r.unwrap_or(replacement)));
        s
    }
}

impl From<&str> for SimpleString {
    fn from(s: &str) -> Self {
        // UTF-16LE. Names are nearly always ASCII: each byte, then a zero.
        if s.is_ascii() {
            let mut v = Vec::with_capacity(s.len() * 2);
            for &b in s.as_bytes() {
                v.extend_from_slice(&[b, 0]);
            }
            return Self(Bytes::from(v));
        }
        let mut v = Vec::with_capacity(s.len() * 2);
        for u in s.encode_utf16() {
            v.extend_from_slice(&u.to_le_bytes());
        }
        Self(Bytes::from(v))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf16_and_comparisons() {
        for s in ["", "_AMQ_ORIG_ADDRESS", "€uro", "a\u{1F600}b"] {
            let ss = SimpleString::from(s);
            let expected: Vec<u8> = s.encode_utf16().flat_map(u16::to_le_bytes).collect();
            assert_eq!(ss.as_bytes(), &expected[..], "{s}");
            assert_eq!(ss.to_string_lossy(), s);
            assert!(ss.eq_str(s));
            assert!(ss.starts_with_str(""));
            assert!(ss.starts_with_str(s));
        }
        let ss = SimpleString::from("_AMQ_ORIG_QUEUE");
        assert!(ss.starts_with_str("_AMQ_ORIG_"));
        assert!(!ss.starts_with_str("_AMQ_ORIG_QUEUEX"));
        assert!(!ss.eq_str("_AMQ_ORIG_"));
        assert!(!SimpleString::from("€").eq_str("e"));
    }
}
