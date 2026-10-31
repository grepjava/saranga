//! `StringPrep.prepAsQueryString`: SASLprep (RFC 4013) of a query string,
//! which may hold unassigned code points, over the tables of RFC 3454.

use unicode_normalization::UnicodeNormalization;

use super::tables::{B1, C12, D1, D2, PROHIBITED};

/// `StringPrepError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StringPrepError {
    #[error("String contains a prohibited character")]
    ProhibitedCharacter,
    /// `StringPrepRTLError`: both RandALCat and LCat characters, or a
    /// RandALCat one that neither starts nor ends the string.
    #[error("Invalid RTL string")]
    Rtl,
}

fn in_class(class: &[(u32, u32)], c: char) -> bool {
    let c = c as u32;
    match class.binary_search_by(|&(start, _)| start.cmp(&c)) {
        Ok(_) => true,
        Err(0) => false,
        Err(i) => c <= class[i - 1].1,
    }
}

/// `prepAsQueryString`: mapped (B.1 to nothing, C.1.2 to a space),
/// normalized to NFKC, then checked for prohibited characters and bidi.
pub fn prep_as_query_string(s: &str) -> Result<String, StringPrepError> {
    let mapped: String = s
        .chars()
        .filter(|&c| !in_class(B1, c))
        .map(|c| if in_class(C12, c) { ' ' } else { c })
        .collect();
    let s: String = mapped.nfkc().collect();
    if is_containing_prohibited_characters(&s) {
        return Err(StringPrepError::ProhibitedCharacter);
    }
    verify_rtl(&s)?;
    Ok(s)
}

/// `isContainingProhibitedCharacters`.
pub fn is_containing_prohibited_characters(s: &str) -> bool {
    s.chars().any(|c| in_class(PROHIBITED, c))
}

/// `verifyRTL`, RFC 3454 section 6. The last character is checked as
/// Java's `charAt` gives it, so one outside the BMP is a surrogate, never
/// RandALCat.
fn verify_rtl(s: &str) -> Result<(), StringPrepError> {
    if !s.chars().any(|c| in_class(D1, c)) {
        return Ok(());
    }
    if s.chars().any(|c| in_class(D2, c)) {
        return Err(StringPrepError::Rtl);
    }
    let first = s.chars().next().is_some_and(|c| in_class(D1, c));
    let last = s
        .chars()
        .next_back()
        .is_some_and(|c| (c as u32) <= 0xFFFF && in_class(D1, c));
    if first && last {
        Ok(())
    } else {
        Err(StringPrepError::Rtl)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sasl_prep_maps_normalizes_and_prohibits() {
        assert_eq!(prep_as_query_string("user").unwrap(), "user");
        // Soft hyphen to nothing, no-break space to a space.
        assert_eq!(prep_as_query_string("I\u{AD}X").unwrap(), "IX");
        assert_eq!(prep_as_query_string("a\u{A0}b").unwrap(), "a b");
        // NFKC: the Roman numeral IX, and a superscript.
        assert_eq!(prep_as_query_string("\u{2168}").unwrap(), "IX");
        assert_eq!(prep_as_query_string("\u{AA}").unwrap(), "a");
        // A control character is prohibited.
        assert_eq!(
            prep_as_query_string("a\u{7}"),
            Err(StringPrepError::ProhibitedCharacter)
        );
        // RFC 4013's examples of bidi: RandALCat with LCat, and RandALCat
        // not at both ends.
        assert_eq!(prep_as_query_string("\u{627}1"), Err(StringPrepError::Rtl));
        assert_eq!(
            prep_as_query_string("\u{627}1\u{628}").unwrap(),
            "\u{627}1\u{628}"
        );
        assert_eq!(
            prep_as_query_string("\u{627}a\u{628}"),
            Err(StringPrepError::Rtl)
        );
        // Unassigned code points are allowed in a query string.
        assert_eq!(prep_as_query_string("\u{0221}").unwrap(), "\u{0221}");
    }
}
