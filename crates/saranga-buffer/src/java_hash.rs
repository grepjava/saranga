//! Java's `HashMap` iteration order, for what Saranga writes or prints
//! in the order a Java map of the same keys would give.

/// The order in which a `java.util.HashMap` iterates keys inserted in this
/// order (`hashes[i]` is the `hashCode()` of the i-th distinct key). Returns
/// indexes into `hashes`.
///
/// `initial_capacity` and `load_factor` are the constructor arguments;
/// `None` is the no-argument constructor. Iteration walks the table's
/// buckets in index order; within a bucket nodes stay in insertion order,
/// also across resizes. Treeified bins (8 or more keys in one bucket of a
/// table of at least 64) keep their insertion order here, which Java's tree
/// bins only approximately do.
pub fn hash_map_order(hashes: &[i32], capacity: Option<(usize, f32)>) -> Vec<usize> {
    const TREEIFY_THRESHOLD: usize = 8;
    const MIN_TREEIFY_CAPACITY: usize = 64;
    let (mut cap, load_factor) = match capacity {
        None => (16, 0.75f32),
        Some((n, lf)) => (table_size_for(n), lf),
    };
    let mut threshold = (cap as f32 * load_factor) as usize;
    let spread = |h: i32| (h ^ ((h as u32) >> 16) as i32) as u32 as usize;
    let mut size = 0;
    for i in 0..hashes.len() {
        // Only a small table grows for a full bin; counting the bin only
        // then keeps the whole walk linear.
        if cap < MIN_TREEIFY_CAPACITY && {
            let bin = spread(hashes[i]) & (cap - 1);
            hashes[..i]
                .iter()
                .filter(|&&h| spread(h) & (cap - 1) == bin)
                .count()
                >= TREEIFY_THRESHOLD
        } {
            cap *= 2;
            threshold = (cap as f32 * load_factor) as usize;
        }
        size += 1;
        if size > threshold {
            cap *= 2;
            threshold = (cap as f32 * load_factor) as usize;
        }
    }
    let mut order: Vec<usize> = (0..hashes.len()).collect();
    order.sort_by_key(|&i| spread(hashes[i]) & (cap - 1));
    order
}

/// `HashMap.tableSizeFor`.
fn table_size_for(n: usize) -> usize {
    n.max(1).next_power_of_two().min(1 << 30)
}

/// `String.hashCode`: over the UTF-16 code units.
pub fn string_hash(s: &str) -> i32 {
    s.encode_utf16()
        .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(i32::from(c)))
}

#[cfg(test)]
mod order_tests {
    use super::*;

    /// The order as it was first written: every key's bin counted against
    /// all the keys before it.
    fn counted_every_time(hashes: &[i32], capacity: Option<(usize, f32)>) -> Vec<usize> {
        let (mut cap, load_factor) = match capacity {
            None => (16, 0.75f32),
            Some((n, lf)) => (table_size_for(n), lf),
        };
        let mut threshold = (cap as f32 * load_factor) as usize;
        let spread = |h: i32| (h ^ ((h as u32) >> 16) as i32) as u32 as usize;
        for i in 0..hashes.len() {
            let bin = spread(hashes[i]) & (cap - 1);
            let in_bin = hashes[..i]
                .iter()
                .filter(|&&h| spread(h) & (cap - 1) == bin)
                .count();
            if in_bin >= 8 && cap < 64 {
                cap *= 2;
                threshold = (cap as f32 * load_factor) as usize;
            }
            if i + 1 > threshold {
                cap *= 2;
                threshold = (cap as f32 * load_factor) as usize;
            }
        }
        let mut order: Vec<usize> = (0..hashes.len()).collect();
        order.sort_by_key(|&i| spread(hashes[i]) & (cap - 1));
        order
    }

    #[test]
    fn the_order_is_what_counting_every_bin_gave() {
        let mut seed = 0x2545_f491_u32;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed as i32
        };
        for n in [0, 1, 7, 9, 13, 40, 100, 700] {
            // Spread keys, and keys crowded into few bins.
            let spread: Vec<i32> = (0..n).map(|_| next()).collect();
            let crowded: Vec<i32> = (0..n).map(|_| (next() & 3) << 4).collect();
            for hashes in [&spread, &crowded] {
                for capacity in [None, Some((1, 0.75)), Some((128, 0.75)), Some((3, 1.0))] {
                    assert_eq!(
                        hash_map_order(hashes, capacity),
                        counted_every_time(hashes, capacity),
                        "{n} {capacity:?}"
                    );
                }
            }
        }
    }
}
