//! Just enough DER to take apart the few structures keystores wrap keys
//! in.

pub const SEQUENCE: u8 = 0x30;
pub const OID: u8 = 0x06;
pub const OCTET_STRING: u8 = 0x04;

/// Reads one element with the given tag: its contents and what follows.
pub fn read(data: &[u8], tag: u8) -> Option<(&[u8], &[u8])> {
    let (&t, rest) = data.split_first()?;
    if t != tag {
        return None;
    }
    let (&first, mut rest) = rest.split_first()?;
    let len = if first < 0x80 {
        first as usize
    } else {
        let n = (first & 0x7f) as usize;
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let len = rest[..n]
            .iter()
            .fold(0usize, |acc, &b| (acc << 8) | b as usize);
        rest = &rest[n..];
        len
    };
    (rest.len() >= len).then(|| rest.split_at(len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_and_long_lengths() {
        assert_eq!(
            read(&[0x04, 0x02, 1, 2, 9], OCTET_STRING),
            Some((&[1u8, 2][..], &[9u8][..]))
        );
        let mut long = vec![0x30, 0x81, 0x80];
        long.extend(std::iter::repeat_n(7u8, 0x80));
        assert_eq!(
            read(&long, SEQUENCE).map(|(c, r)| (c.len(), r.len())),
            Some((0x80, 0))
        );
        assert_eq!(read(&[0x04, 0x05, 1], OCTET_STRING), None);
        assert_eq!(read(&[0x30, 0x00], OCTET_STRING), None);
    }
}
