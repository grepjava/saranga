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
        let bin = spread(hashes[i]) & (cap - 1);
        let in_bin = hashes[..i]
            .iter()
            .filter(|&&h| spread(h) & (cap - 1) == bin)
            .count();
        if in_bin >= TREEIFY_THRESHOLD && cap < MIN_TREEIFY_CAPACITY {
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
