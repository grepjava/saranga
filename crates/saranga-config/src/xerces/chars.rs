//! `XMLChar` and `XML11Char`: the character classes the scanners test,
//! from the JDK's own tables.

use std::sync::OnceLock;

use super::chars_table::{XML10, XML11};

const VALID: u8 = 0x01;
const SPACE: u8 = 0x02;
const NAME_START: u8 = 0x04;
const NAME: u8 = 0x08;
const PUBID: u8 = 0x10;
const CONTENT: u8 = 0x20;
const NCNAME_START: u8 = 0x40;
const NCNAME: u8 = 0x80;
/// `MASK_XML11_CONTROL`, where XML 1.0's table has `MASK_PUBID`.
const XML11_CONTROL: u8 = 0x10;

fn table(runs: &'static [(u16, u16, u8)], cell: &'static OnceLock<Box<[u8]>>) -> &'static [u8] {
    cell.get_or_init(|| {
        let mut t = vec![0u8; 0x10000];
        for &(start, end, flags) in runs {
            for c in start..=end {
                t[c as usize] = flags;
            }
        }
        t.into_boxed_slice()
    })
}

fn chars() -> &'static [u8] {
    static T: OnceLock<Box<[u8]>> = OnceLock::new();
    table(XML10, &T)
}

fn chars11() -> &'static [u8] {
    static T: OnceLock<Box<[u8]>> = OnceLock::new();
    table(XML11, &T)
}

fn has(c: i32, mask: u8) -> bool {
    (0..0x10000).contains(&c) && chars()[c as usize] & mask != 0
}

fn has11(c: i32, mask: u8) -> bool {
    (0..0x10000).contains(&c) && chars11()[c as usize] & mask != 0
}

fn supplementary(c: i32) -> bool {
    (0x10000..=0x10FFFF).contains(&c)
}

pub(crate) fn is_supplemental(c: i32) -> bool {
    supplementary(c)
}

pub(crate) fn supplemental(high: u16, low: u16) -> i32 {
    (high as i32 - 0xD800) * 0x400 + (low as i32 - 0xDC00) + 0x10000
}

pub(crate) fn high_surrogate(c: i32) -> u16 {
    (((c - 0x10000) >> 10) + 0xD800) as u16
}

pub(crate) fn low_surrogate(c: i32) -> u16 {
    (((c - 0x10000) & 0x3FF) + 0xDC00) as u16
}

pub(crate) fn is_high_surrogate(c: i32) -> bool {
    (0xD800..=0xDBFF).contains(&c)
}

pub(crate) fn is_low_surrogate(c: i32) -> bool {
    (0xDC00..=0xDFFF).contains(&c)
}

pub(crate) fn is_valid(c: i32) -> bool {
    has(c, VALID) || supplementary(c)
}

pub(crate) fn is_invalid(c: i32) -> bool {
    !is_valid(c)
}

pub(crate) fn is_content(c: i32) -> bool {
    has(c, CONTENT) || supplementary(c)
}

pub(crate) fn is_markup(c: i32) -> bool {
    c == '<' as i32 || c == '&' as i32 || c == '%' as i32
}

/// `XMLChar.isSpace`, which indexes the table for any `c <= 0x20`: a
/// negative `c` is Java's `ArrayIndexOutOfBoundsException`, left to the
/// caller (no scanner passes one).
pub(crate) fn is_space(c: i32) -> bool {
    c <= 0x20 && has(c, SPACE)
}

pub(crate) fn is_name_start(c: i32) -> bool {
    has(c, NAME_START)
}

pub(crate) fn is_name(c: i32) -> bool {
    has(c, NAME)
}

pub(crate) fn is_ncname_start(c: i32) -> bool {
    has(c, NCNAME_START)
}

pub(crate) fn is_ncname(c: i32) -> bool {
    has(c, NCNAME)
}

pub(crate) fn is_pubid(c: i32) -> bool {
    has(c, PUBID)
}

pub(crate) fn is_valid_iana_encoding(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
        }
        _ => false,
    }
}

pub(crate) fn is_xml11_space(c: i32) -> bool {
    has11(c, SPACE)
}

pub(crate) fn is_xml11_valid(c: i32) -> bool {
    has11(c, VALID) || supplementary(c)
}

pub(crate) fn is_xml11_invalid(c: i32) -> bool {
    !is_xml11_valid(c)
}

pub(crate) fn is_xml11_valid_literal(c: i32) -> bool {
    ((0..0x10000).contains(&c) && {
        let f = chars11()[c as usize];
        f & VALID != 0 && f & XML11_CONTROL == 0
    }) || supplementary(c)
}

pub(crate) fn is_xml11_content(c: i32) -> bool {
    has11(c, CONTENT) || supplementary(c)
}

pub(crate) fn is_xml11_internal_entity_content(c: i32) -> bool {
    has11(c, XML11_CONTROL | CONTENT) || supplementary(c)
}

fn xml11_astral(c: i32) -> bool {
    (0x10000..0xF0000).contains(&c)
}

pub(crate) fn is_xml11_name_start(c: i32) -> bool {
    has11(c, NAME_START) || xml11_astral(c)
}

pub(crate) fn is_xml11_name(c: i32) -> bool {
    has11(c, NAME) || xml11_astral(c)
}

pub(crate) fn is_xml11_ncname_start(c: i32) -> bool {
    has11(c, NCNAME_START) || xml11_astral(c)
}

pub(crate) fn is_xml11_ncname(c: i32) -> bool {
    has11(c, NCNAME) || xml11_astral(c)
}

pub(crate) fn is_xml11_name_high_surrogate(c: i32) -> bool {
    (0xD800..=0xDB7F).contains(&c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_follow_the_jdk_tables() {
        assert!(is_space(' ' as i32) && is_space('\r' as i32) && !is_space(0x85));
        assert!(is_xml11_space(0x85) && is_xml11_space(0x2028));
        assert!(is_name_start(':' as i32) && is_name(':' as i32) && !is_ncname(':' as i32));
        assert!(is_content('a' as i32) && !is_content('<' as i32) && !is_content(']' as i32));
        assert!(is_invalid(0) && is_invalid(0xFFFE) && is_valid(0x1F600));
        assert!(!is_xml11_valid_literal(1) && is_xml11_valid(1));
        assert!(is_pubid('\'' as i32) && !is_pubid('"' as i32));
    }
}
