//! Time-based (version 1) UUIDs, as `UUIDGenerator.generateUUID` makes for
//! session names and JMS message IDs (the `userID` header).

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// 100 ns intervals between the Gregorian epoch (1582-10-15) and the Unix epoch.
const GREGORIAN_OFFSET: u64 = 0x01B2_1DD2_1381_4000;

struct Generator {
    /// Random node ID with the multicast bit set (RFC 4122 4.5), and clock sequence.
    node: [u8; 6],
    clock_seq: u16,
    last: u64,
}

fn random_u64() -> u64 {
    let mut h = RandomState::new().build_hasher();
    h.write_u128(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    h.finish()
}

fn generator() -> &'static Mutex<Generator> {
    static GEN: OnceLock<Mutex<Generator>> = OnceLock::new();
    GEN.get_or_init(|| {
        let r = random_u64().to_be_bytes();
        let mut node = [r[0], r[1], r[2], r[3], r[4], r[5]];
        node[0] |= 0x01;
        Mutex::new(Generator {
            node,
            clock_seq: u16::from_be_bytes([r[6], r[7]]) & 0x3fff,
            last: 0,
        })
    })
}

/// A new time-based UUID. Timestamps are kept strictly increasing so UUIDs
/// made in the same 100 ns tick still differ.
pub fn time_based() -> [u8; 16] {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let ticks = now.as_nanos() as u64 / 100 + GREGORIAN_OFFSET;
    let mut g = generator().lock().expect("uuid generator");
    let ts = ticks.max(g.last + 1);
    g.last = ts;
    let mut b = [0u8; 16];
    b[0..4].copy_from_slice(&(ts as u32).to_be_bytes());
    b[4..6].copy_from_slice(&((ts >> 32) as u16).to_be_bytes());
    b[6..8].copy_from_slice(&(((ts >> 48) as u16 & 0x0fff) | 0x1000).to_be_bytes());
    b[8..10].copy_from_slice(&(g.clock_seq | 0x8000).to_be_bytes());
    b[10..16].copy_from_slice(&g.node);
    b
}

/// The canonical `8-4-4-4-12` form (`UUID.toString`).
pub fn to_string(b: &[u8; 16]) -> String {
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_variant_and_uniqueness() {
        let a = time_based();
        let b = time_based();
        assert_ne!(a, b);
        assert_eq!(a[6] >> 4, 1);
        assert_eq!(a[8] & 0xc0, 0x80);
        let s = to_string(&a);
        assert_eq!(s.len(), 36);
        assert_eq!(&s[14..15], "1");
    }

    #[test]
    fn formats_like_java() {
        // A user ID from the core-basic golden capture.
        let id = [
            18, 93, 195, 38, 196, 146, 17, 241, 143, 177, 0, 21, 93, 28, 122, 46,
        ];
        assert_eq!(to_string(&id), "125dc326-c492-11f1-8fb1-00155d1c7a2e");
    }
}
