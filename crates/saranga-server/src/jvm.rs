//! The JVM's maximum heap (`Runtime.maxMemory`), of which Artemis takes
//! `global-max-size-percent-of-jvm-max-memory` when `global-max-size` is
//! unset: the `-Xmx` (or `-XX:MaxHeapSize`) of `JAVA_ARGS` and
//! `JAVA_ARGS_APPEND`, which an instance's bin/artemis starts Java with,
//! the last one winning, else the JVM's default, `-XX:MaxRAMPercentage`
//! (25%) of the memory (`-XX:MaxRAM`, else the machine's or the
//! container's).

/// `Runtime.maxMemory` for the JVM an instance's bin/artemis would start.
/// Outside an instance (no `ARTEMIS_INSTANCE`), the JVM's default.
pub fn max_memory() -> i64 {
    let args = if std::env::var_os("ARTEMIS_INSTANCE").is_some() {
        ["JAVA_ARGS", "JAVA_ARGS_APPEND"]
            .iter()
            .filter_map(|name| std::env::var(name).ok())
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        String::new()
    };
    max_memory_of(&args, physical_memory)
}

/// The maximum heap the options give, `physical` the memory there is.
pub fn max_memory_of(args: &str, physical: impl FnOnce() -> u64) -> i64 {
    let mut max_heap = None;
    let mut max_ram = None;
    let mut percent = 25.0f64;
    for a in args.split_whitespace().map(|a| a.trim_matches('"')) {
        if let Some(v) = a.strip_prefix("-Xmx") {
            max_heap = size(v).or(max_heap);
        } else if let Some(v) = a.strip_prefix("-XX:MaxHeapSize=") {
            max_heap = size(v).or(max_heap);
        } else if let Some(v) = a.strip_prefix("-XX:MaxRAM=") {
            max_ram = size(v).or(max_ram);
        } else if let Some(v) = a.strip_prefix("-XX:MaxRAMPercentage=") {
            percent = v.parse().unwrap_or(percent);
        }
    }
    if let Some(h) = max_heap {
        return h;
    }
    let ram = max_ram.unwrap_or_else(|| physical() as i64);
    (ram as f64 * percent / 100.0) as i64
}

/// A JVM size option: bytes, or with `k`, `m`, `g` or `t`.
fn size(v: &str) -> Option<i64> {
    let (digits, unit) = match v.char_indices().last()? {
        (i, c) if c.is_ascii_alphabetic() => (&v[..i], c.to_ascii_lowercase()),
        _ => (v, 'b'),
    };
    let n: i64 = digits.parse().ok()?;
    let shift = match unit {
        'b' => 0,
        'k' => 10,
        'm' => 20,
        'g' => 30,
        't' => 40,
        _ => return None,
    };
    n.checked_mul(1 << shift)
}

/// The machine's memory, or the container's limit if lower.
fn physical_memory() -> u64 {
    use sysinfo::{MemoryRefreshKind, RefreshKind, System};
    let sys = System::new_with_specifics(
        RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
    );
    let total = sys.total_memory();
    match sys.cgroup_limits() {
        Some(c) if c.total_memory > 0 => total.min(c.total_memory),
        _ => total,
    }
}

/// `ActiveMQDefaultConfiguration.getDefaultMaxGlobalSizeAsPercentOfJvmMaxMemory`,
/// in Java's float arithmetic.
pub fn percent_of(max_memory: i64, percent: i32) -> i64 {
    (max_memory as f32 * (percent as f32 / 100.0f32)) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_options() {
        let none = || 8 << 30;
        assert_eq!(
            max_memory_of("-Xms512M -Xmx2G -Dhawtio.role=amq", none),
            2 << 30
        );
        assert_eq!(max_memory_of("-Xmx2G -Xmx4096m", none), 4 << 30);
        assert_eq!(max_memory_of("-XX:MaxHeapSize=1073741824", none), 1 << 30);
        assert_eq!(max_memory_of("\"-Xmx512k\"", none), 512 << 10);
        assert_eq!(max_memory_of("-Dx=y", none), 2 << 30);
        assert_eq!(max_memory_of("-XX:MaxRAMPercentage=50", none), 4 << 30);
        assert_eq!(max_memory_of("-XX:MaxRAM=1g", none), 256 << 20);
        assert_eq!(max_memory_of("-Xmx2Q", none), 2 << 30);
    }

    #[test]
    fn java_float_arithmetic() {
        assert_eq!(percent_of(2 << 30, 50), 1 << 30);
        // 0.3f and the product rounded to a float, as Java computes them.
        assert_eq!(percent_of(2 << 30, 30), 644_245_120);
    }
}
