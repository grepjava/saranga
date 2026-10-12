//! The parts of HdrHistogram 2.2.2 that `perf` uses: a `Histogram` of two
//! significant digits from 1 up to any value, its percentiles and mean,
//! the `SingleWriterRecorder` that swaps interval histograms, and the
//! `HistogramLogWriter` log format with V2 compressed histograms.

use std::io::Write;
use std::sync::Mutex;

use flate2::Compression;
use flate2::write::ZlibEncoder;

/// Two significant digits: 2 * 10^2 values have single unit resolution,
/// so a sub-bucket holds 256 values and its upper half 128.
const SIGNIFICANT_DIGITS: i32 = 2;
const SUB_BUCKET_HALF_COUNT_MAGNITUDE: u32 = 7;
const SUB_BUCKET_HALF_COUNT: usize = 1 << SUB_BUCKET_HALF_COUNT_MAGNITUDE;
const SUB_BUCKET_COUNT: i64 = 1 << (SUB_BUCKET_HALF_COUNT_MAGNITUDE + 1);
const SUB_BUCKET_MASK: i64 = SUB_BUCKET_COUNT - 1;
/// `64 - unitMagnitude - subBucketHalfCountMagnitude - 1`, unit magnitude 0.
const LEADING_ZERO_COUNT_BASE: u32 = 64 - SUB_BUCKET_HALF_COUNT_MAGNITUDE - 1;
/// `Histogram(2)`: auto-resizing, from a highest trackable value of 2.
const INITIAL_HIGHEST_TRACKABLE: i64 = 2;
const V2_ENCODING_COOKIE: i32 = 0x1c84_9303 | 0x10;
const V2_COMPRESSED_ENCODING_COOKIE: i32 = 0x1c84_9304 | 0x10;

fn bucket_index(value: i64) -> u32 {
    LEADING_ZERO_COUNT_BASE - (value | SUB_BUCKET_MASK).leading_zeros()
}

fn sub_bucket_index(value: i64, bucket: u32) -> i64 {
    value >> bucket
}

fn counts_index(value: i64) -> usize {
    let bucket = bucket_index(value);
    let sub = sub_bucket_index(value, bucket);
    (((bucket as usize) + 1) << SUB_BUCKET_HALF_COUNT_MAGNITUDE) + sub as usize
        - SUB_BUCKET_HALF_COUNT
}

fn value_from_index(index: usize) -> i64 {
    let mut bucket = (index >> SUB_BUCKET_HALF_COUNT_MAGNITUDE) as i64 - 1;
    let mut sub = ((index & (SUB_BUCKET_HALF_COUNT - 1)) + SUB_BUCKET_HALF_COUNT) as i64;
    if bucket < 0 {
        sub -= SUB_BUCKET_HALF_COUNT as i64;
        bucket = 0;
    }
    sub << bucket
}

fn size_of_equivalent_range(value: i64) -> i64 {
    let bucket = bucket_index(value);
    let sub = sub_bucket_index(value, bucket);
    1 << if sub >= SUB_BUCKET_COUNT {
        bucket + 1
    } else {
        bucket
    }
}

fn lowest_equivalent(value: i64) -> i64 {
    let bucket = bucket_index(value);
    sub_bucket_index(value, bucket) << bucket
}

fn highest_equivalent(value: i64) -> i64 {
    lowest_equivalent(value) + size_of_equivalent_range(value) - 1
}

fn median_equivalent(value: i64) -> i64 {
    lowest_equivalent(value) + (size_of_equivalent_range(value) >> 1)
}

/// `getLengthForNumberOfBuckets(getBucketsNeededToCoverValue(value))`.
fn array_length_for(value: i64) -> usize {
    let mut smallest_untrackable = SUB_BUCKET_COUNT;
    let mut buckets = 1usize;
    while smallest_untrackable <= value {
        if smallest_untrackable > i64::MAX / 2 {
            buckets += 1;
            break;
        }
        smallest_untrackable <<= 1;
        buckets += 1;
    }
    (buckets + 1) * SUB_BUCKET_HALF_COUNT
}

/// An auto-resizing `Histogram(2)`.
#[derive(Clone, Debug)]
pub struct Histogram {
    counts: Vec<i64>,
    total: i64,
    /// The largest value recorded, 0 for none.
    max: i64,
    highest_trackable: i64,
    pub start_ms: i64,
    pub end_ms: i64,
    pub tag: Option<String>,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            counts: vec![0; array_length_for(INITIAL_HIGHEST_TRACKABLE)],
            total: 0,
            max: 0,
            highest_trackable: INITIAL_HIGHEST_TRACKABLE,
            start_ms: 0,
            end_ms: 0,
            tag: None,
        }
    }
}

impl Histogram {
    /// `resize` after a value out of range: the counts cover it, and the
    /// highest trackable value is the last count's highest value.
    fn cover(&mut self, value: i64) {
        if value <= self.highest_trackable {
            return;
        }
        let len = array_length_for(value);
        if len > self.counts.len() {
            self.counts.resize(len, 0);
        }
        self.highest_trackable = highest_equivalent(value_from_index(self.counts.len() - 1));
    }

    /// `recordValue`; negative values are out of range and not recorded.
    pub fn record(&mut self, value: i64) {
        if value < 0 {
            return;
        }
        self.cover(value);
        self.counts[counts_index(value)] += 1;
        self.total += 1;
        self.max = self.max.max(value);
    }

    pub fn reset(&mut self) {
        self.counts.iter_mut().for_each(|c| *c = 0);
        self.total = 0;
        self.max = 0;
    }

    /// `add`: the other histogram's counts added to these.
    pub fn add(&mut self, other: &Histogram) {
        if other.total == 0 {
            return;
        }
        self.cover(other.max_value());
        for (i, c) in other.counts.iter().enumerate() {
            if *c != 0 {
                self.counts[i] += c;
            }
        }
        self.total += other.total;
        self.max = self.max.max(other.max_value());
    }

    pub fn total_count(&self) -> i64 {
        self.total
    }

    /// `getMaxValue`: the highest value equivalent to the largest recorded.
    pub fn max_value(&self) -> i64 {
        if self.max == 0 {
            0
        } else {
            highest_equivalent(self.max)
        }
    }

    /// `getMean`: each recorded value as the middle of its range.
    pub fn mean(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        let mut sum = 0.0;
        for (i, c) in self.counts.iter().enumerate() {
            if *c != 0 {
                sum += median_equivalent(value_from_index(i)) as f64 * *c as f64;
            }
        }
        sum / self.total as f64
    }

    /// `getValueAtPercentile`.
    pub fn value_at_percentile(&self, percentile: f64) -> i64 {
        let requested = next_down(percentile).clamp(0.0, 100.0);
        let fp_count = requested * self.total as f64 / 100.0;
        let count_at = (fp_count.ceil() as i64).max(1);
        let mut running = 0;
        for (i, c) in self.counts.iter().enumerate() {
            running += c;
            if running >= count_at {
                let v = value_from_index(i);
                return if percentile == 0.0 {
                    lowest_equivalent(v)
                } else {
                    highest_equivalent(v)
                };
            }
        }
        0
    }

    /// `encodeIntoByteBuffer`: the V2 encoding, its counts up to the
    /// largest value as ZigZag LEB128 numbers, a run of zeros as one
    /// negative count.
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&V2_ENCODING_COOKIE.to_be_bytes());
        out.extend_from_slice(&0i32.to_be_bytes());
        // normalizingIndexOffset
        out.extend_from_slice(&0i32.to_be_bytes());
        out.extend_from_slice(&SIGNIFICANT_DIGITS.to_be_bytes());
        // lowestDiscernibleValue
        out.extend_from_slice(&1i64.to_be_bytes());
        out.extend_from_slice(&self.highest_trackable.to_be_bytes());
        // integerToDoubleValueConversionRatio
        out.extend_from_slice(&1.0f64.to_be_bytes());
        let payload_start = out.len();
        let limit = counts_index(self.max_value()) + 1;
        let mut i = 0;
        while i < limit {
            let count = self.counts[i];
            i += 1;
            let mut zeros = 0i64;
            if count == 0 {
                zeros = 1;
                while i < limit && self.counts[i] == 0 {
                    zeros += 1;
                    i += 1;
                }
            }
            zig_zag(&mut out, if zeros > 1 { -zeros } else { count });
        }
        let payload = (out.len() - payload_start) as i32;
        out[4..8].copy_from_slice(&payload.to_be_bytes());
        out
    }

    /// `encodeIntoCompressedByteBuffer` at `Deflater.BEST_COMPRESSION`.
    pub fn encode_compressed(&self) -> Vec<u8> {
        let mut z = ZlibEncoder::new(Vec::new(), Compression::best());
        let _ = z.write_all(&self.encode());
        let compressed = z.finish().unwrap_or_default();
        let mut out = Vec::with_capacity(compressed.len() + 8);
        out.extend_from_slice(&V2_COMPRESSED_ENCODING_COOKIE.to_be_bytes());
        out.extend_from_slice(&(compressed.len() as i32).to_be_bytes());
        out.extend_from_slice(&compressed);
        out
    }
}

/// `Math.nextAfter(d, -Infinity)`.
fn next_down(d: f64) -> f64 {
    if d.is_nan() || d == f64::NEG_INFINITY {
        d
    } else if d == 0.0 {
        -f64::from_bits(1)
    } else if d > 0.0 {
        f64::from_bits(d.to_bits() - 1)
    } else {
        f64::from_bits(d.to_bits() + 1)
    }
}

/// `ZigZagEncoding.putLong`: LEB128 of the ZigZag value, at most 9 bytes,
/// the ninth holding a whole byte.
fn zig_zag(out: &mut Vec<u8>, value: i64) {
    let mut v = ((value << 1) ^ (value >> 63)) as u64;
    for _ in 0..8 {
        if v >> 7 == 0 {
            out.push(v as u8);
            return;
        }
        out.push((v & 0x7f) as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// `SingleWriterRecorder(2)`: values recorded by one writer, taken as
/// interval histograms.
#[derive(Default, Debug)]
pub struct Recorder {
    active: Mutex<Histogram>,
}

impl Recorder {
    pub fn record(&self, value: i64) {
        self.active.lock().expect("recorder").record(value);
    }

    /// `getIntervalHistogram(recycle)`: what was recorded since the last
    /// call, `recycle` reset to record the next interval.
    pub fn interval(&self, recycle: Option<Histogram>) -> Histogram {
        let mut next = recycle.unwrap_or_default();
        next.reset();
        let mut active = self.active.lock().expect("recorder");
        let now = super::stats::now_ms();
        next.start_ms = now;
        let mut taken = std::mem::replace(&mut *active, next);
        taken.end_ms = now;
        taken
    }
}

/// `HistogramLogWriter`.
pub struct LogWriter {
    out: std::io::BufWriter<std::fs::File>,
}

impl LogWriter {
    pub fn create(path: &str) -> std::io::Result<Self> {
        Ok(Self {
            out: std::io::BufWriter::new(std::fs::File::create(path)?),
        })
    }

    /// `outputLogFormatVersion` (`%n` is the platform line separator).
    pub fn output_log_format_version(&mut self) -> std::io::Result<()> {
        write!(
            self.out,
            "#[Histogram log format version 1.3]{}",
            super::stats::LINE_SEPARATOR
        )
    }

    /// `outputLegend` (`println`).
    pub fn output_legend(&mut self) -> std::io::Result<()> {
        write!(
            self.out,
            "\"StartTimestamp\",\"Interval_Length\",\"Interval_Max\",\"Interval_Compressed_Histogram\"{}",
            super::stats::LINE_SEPARATOR
        )
    }

    /// `outputIntervalHistogram(histogram)`: start and length in seconds,
    /// the maximum in units of a million. A tag the pattern `.[, \r\n].`
    /// matches (a whole tag of three characters) is refused.
    pub fn output_interval_histogram(&mut self, h: &Histogram) -> anyhow::Result<()> {
        let start = h.start_ms as f64 / 1000.0;
        let end = h.end_ms as f64 / 1000.0;
        let max = h.max_value() as f64 / 1_000_000.0;
        let encoded = base64(&h.encode_compressed());
        match &h.tag {
            None => writeln!(self.out, "{start:.3},{:.3},{max:.3},{encoded}", end - start)?,
            Some(tag) => {
                let chars: Vec<char> = tag.chars().collect();
                if let [a, b, c] = chars[..]
                    && !matches!(a, '\r' | '\n')
                    && matches!(b, ',' | ' ' | '\r' | '\n')
                    && !matches!(c, '\r' | '\n')
                {
                    anyhow::bail!("Tag string cannot contain commas, spaces, or line breaks");
                }
                writeln!(
                    self.out,
                    "Tag={tag},{start:.3},{:.3},{max:.3},{encoded}",
                    end - start
                )?
            }
        }
        Ok(())
    }

    pub fn close(mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

/// Standard Base64 with padding.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut s = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                s.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                s.push('=');
            }
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equivalent_ranges() {
        // Single unit resolution up to 255, then ranges of 2, 4, ...
        assert_eq!(highest_equivalent(200), 200);
        assert_eq!(lowest_equivalent(257), 256);
        assert_eq!(highest_equivalent(257), 257);
        assert_eq!(highest_equivalent(1000), 1003);
        assert_eq!(median_equivalent(1000), 1002);
        assert_eq!(value_from_index(counts_index(1000)), 1000);
        assert_eq!(counts_index(255), 255);
        assert_eq!(counts_index(256), 256);
        assert_eq!(counts_index(512), 384);
    }

    #[test]
    fn percentiles_and_mean() {
        let mut h = Histogram::default();
        assert_eq!(h.value_at_percentile(50.0), 0);
        assert_eq!(h.mean(), 0.0);
        for v in 1..=1000 {
            h.record(v);
        }
        assert_eq!(h.total_count(), 1000);
        assert_eq!(h.value_at_percentile(50.0), 501);
        assert_eq!(h.value_at_percentile(90.0), 903);
        assert_eq!(h.value_at_percentile(99.0), 991);
        assert_eq!(h.value_at_percentile(99.99), 1003);
        assert_eq!(h.value_at_percentile(0.0), 1);
        assert_eq!(h.max_value(), 1003);
        assert!((h.mean() - 500.874).abs() < 1e-9);
        // HdrHistogram 2.2.2's encodeIntoByteBuffer of the same histogram.
        let java = "HISTEwAAAfsAAAAAAAAAAgAAAAAAAAABAAAAAAAAA/8/8AAAAAAAAAACAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIAg==";
        assert_eq!(base64(&h.encode()), java);
        let mut sum = Histogram::default();
        sum.add(&h);
        sum.add(&h);
        assert_eq!(sum.total_count(), 2000);
        assert_eq!(sum.value_at_percentile(50.0), 501);
        assert!((sum.mean() - h.mean()).abs() < 1e-9);
    }

    #[test]
    fn zig_zag_encoding() {
        let mut out = Vec::new();
        zig_zag(&mut out, 1);
        zig_zag(&mut out, -3);
        zig_zag(&mut out, 64);
        assert_eq!(out, [2, 5, 0x80, 1]);
    }

    #[test]
    fn base64_padding() {
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
    }
}
