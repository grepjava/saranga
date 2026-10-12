//! `LiveStatistics` and `RateSampler`: per-second rates and latency
//! percentiles of the producers and listeners, printed, and written to the
//! `--json` report and the `--hdr` histogram log.

use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Instant;

use super::hdr::{Histogram, LogWriter, Recorder};

/// Java's `System.lineSeparator()`.
pub const LINE_SEPARATOR: &str = if cfg!(windows) { "\r\n" } else { "\n" };

/// `System.currentTimeMillis`.
pub fn now_ms() -> i64 {
    crate::client::jms::now_millis()
}

/// What a producer's load generator counts and records
/// (`AsyncJms2ProducerFacade`'s counters and the generator's recorders).
#[derive(Debug, Default)]
pub struct ProducerStats {
    pub sent: AtomicI64,
    pub completed: AtomicI64,
    pub not_available: AtomicI64,
    pub send_completed_latencies: Recorder,
    /// Only for a producer with a target rate.
    pub wait_latencies: Option<Recorder>,
    pub fatal: AtomicBool,
}

/// What a `RecordingMessageListener` counts and records.
#[derive(Debug)]
pub struct ListenerStats {
    pub received: AtomicI64,
    pub latencies: Recorder,
    /// Shared by all the listeners of a benchmark: a broken connection.
    pub fatal: Arc<AtomicBool>,
}

/// `RateSampler`: the change of a counter between two samples.
struct RateSampler {
    sampling: Box<dyn Fn() -> i64 + Send>,
    last_sample: i64,
    last_sample_time: Instant,
    rate: i64,
    time_span_ns: i64,
}

impl RateSampler {
    fn of(sampling: impl Fn() -> i64 + Send + 'static) -> Self {
        let last_sample = sampling();
        Self {
            sampling: Box::new(sampling),
            last_sample,
            last_sample_time: Instant::now(),
            rate: 0,
            time_span_ns: 0,
        }
    }

    fn run(&mut self) {
        let now = Instant::now();
        let sample = (self.sampling)();
        self.rate = sample - self.last_sample;
        self.time_span_ns = now.duration_since(self.last_sample_time).as_nanos() as i64;
        self.last_sample = sample;
        self.last_sample_time = now;
    }

    fn report_rate(&self, interval_ns: i64) -> i64 {
        if self.time_span_ns == 0 {
            return 0;
        }
        ((self.rate as i128 * interval_ns as i128) / self.time_span_ns as i128) as i64
    }
}

/// One kind of latency: the histograms taken from each recorder, their
/// sum over the interval and over the whole (not warming up) run.
struct Latencies {
    per_source: Vec<Option<Histogram>>,
    interval: Histogram,
    accumulated: Histogram,
}

impl Latencies {
    fn new(sources: usize) -> Self {
        Self {
            per_source: vec![None; sources],
            interval: Histogram::default(),
            accumulated: Histogram::default(),
        }
    }

    /// `getIntervalHistogram` of each recorder, then `aggregateLatencies`.
    fn sample<'a>(
        &mut self,
        recorders: impl Iterator<Item = &'a Recorder>,
        warmup: bool,
        last_sample_time: i64,
        sample_time: i64,
    ) {
        for (slot, recorder) in self.per_source.iter_mut().zip(recorders) {
            *slot = Some(recorder.interval(slot.take()));
        }
        self.interval.reset();
        self.interval.start_ms = last_sample_time;
        self.interval.end_ms = sample_time;
        for h in self.per_source.iter().flatten() {
            self.interval.add(h);
        }
        if !warmup {
            self.accumulated.add(&self.interval);
        }
    }
}

pub struct LiveStatistics {
    producers: Vec<Arc<ProducerStats>>,
    listeners: Option<Vec<Arc<ListenerStats>>>,
    wait: Option<Latencies>,
    sent_latencies: Option<Latencies>,
    end_to_end: Option<Latencies>,
    sent: Option<RateSampler>,
    blocked: Option<RateSampler>,
    completed: Option<RateSampler>,
    received: Option<RateSampler>,
    sample_time: i64,
    json: Option<std::io::BufWriter<std::fs::File>>,
    json_samples: u64,
    hdr: Option<LogWriter>,
}

/// `ReportInterval.sec`.
const SEC_NS: i64 = 1_000_000_000;

impl LiveStatistics {
    pub fn new(
        json: Option<&str>,
        hdr: Option<&str>,
        producers: Vec<Arc<ProducerStats>>,
        listeners: Option<Vec<Arc<ListenerStats>>>,
    ) -> anyhow::Result<Self> {
        let has_producers = !producers.is_empty();
        let sum = |f: fn(&ProducerStats) -> i64| {
            let p = producers.clone();
            move || p.iter().map(|s| f(s)).sum::<i64>()
        };
        let (sent, blocked, completed) = if has_producers {
            (
                Some(RateSampler::of(sum(|s| s.sent.load(Ordering::Relaxed)))),
                Some(RateSampler::of(sum(|s| {
                    s.not_available.load(Ordering::Relaxed)
                }))),
                Some(RateSampler::of(sum(|s| {
                    s.completed.load(Ordering::Relaxed)
                }))),
            )
        } else {
            (None, None, None)
        };
        let received = listeners.as_ref().map(|l| {
            let l = l.clone();
            RateSampler::of(move || l.iter().map(|s| s.received.load(Ordering::Relaxed)).sum())
        });
        let json = match json {
            Some(path) => {
                let mut w = std::io::BufWriter::new(std::fs::File::create(path)?);
                w.write_all(b"[\n")?;
                Some(w)
            }
            None => None,
        };
        let hdr = match hdr {
            Some(path) => {
                let mut w = LogWriter::create(path)?;
                w.output_log_format_version()?;
                w.output_legend()?;
                Some(w)
            }
            None => None,
        };
        Ok(Self {
            wait: (has_producers && producers[0].wait_latencies.is_some())
                .then(|| Latencies::new(producers.len())),
            sent_latencies: has_producers.then(|| Latencies::new(producers.len())),
            end_to_end: listeners.as_ref().map(|l| Latencies::new(l.len())),
            producers,
            listeners,
            sent,
            blocked,
            completed,
            received,
            sample_time: now_ms(),
            json,
            json_samples: 0,
            hdr,
        })
    }

    fn any_fatal_error(&self) -> bool {
        self.producers
            .iter()
            .any(|p| p.fatal.load(Ordering::Relaxed))
            || self
                .listeners
                .iter()
                .flatten()
                .any(|l| l.fatal.load(Ordering::Relaxed))
    }

    pub fn sample_metrics(&mut self, warmup: bool) {
        let last = self.sample_time;
        self.sample_time = now_ms();
        for r in [
            &mut self.received,
            &mut self.completed,
            &mut self.blocked,
            &mut self.sent,
        ]
        .into_iter()
        .flatten()
        {
            r.run();
        }
        let now = self.sample_time;
        if let (Some(l), Some(listeners)) = (&mut self.end_to_end, &self.listeners) {
            l.sample(listeners.iter().map(|s| &s.latencies), warmup, last, now);
        }
        if let Some(l) = &mut self.sent_latencies {
            l.sample(
                self.producers.iter().map(|p| &p.send_completed_latencies),
                warmup,
                last,
                now,
            );
        }
        if let Some(l) = &mut self.wait {
            l.sample(
                self.producers
                    .iter()
                    .flat_map(|p| p.wait_latencies.as_ref()),
                warmup,
                last,
                now,
            );
        }
    }

    /// `outAtInterval` at `ReportInterval.sec`.
    pub fn out_at_interval(
        &mut self,
        warmup: bool,
        out: &mut String,
        include_latencies: bool,
    ) -> anyhow::Result<()> {
        // The space after "true" keeps the length of "false".
        out.push_str("\n--- warmup ");
        out.push_str(if warmup { "true " } else { "false" });
        append_rate(out, "\n--- sent:     ", &self.sent);
        append_rate(out, "\n--- blocked:  ", &self.blocked);
        append_rate(out, "\n--- completed:", &self.completed);
        append_rate(out, "\n--- received: ", &self.received);
        if include_latencies {
            out_percentiles(
                out,
                "\n--- send delay time:",
                self.wait.as_ref().map(|l| &l.interval),
            );
            out_percentiles(
                out,
                "\n--- send ack time:  ",
                self.sent_latencies.as_ref().map(|l| &l.interval),
            );
            out_percentiles(
                out,
                "\n--- transfer time:  ",
                self.end_to_end.as_ref().map(|l| &l.interval),
            );
        }
        self.append_json_interval(warmup)?;
        self.append_tagged_hdr(warmup)?;
        Ok(())
    }

    fn append_json_interval(&mut self, warmup: bool) -> anyhow::Result<()> {
        if self.json.is_none() {
            return Ok(());
        }
        let mut j = JsonObject::default();
        j.string("sampleType", "interval");
        j.raw("warmup", &warmup.to_string());
        j.raw("time", &self.sample_time.to_string());
        for (name, r) in [
            ("sent", &self.sent),
            ("delayed", &self.blocked),
            ("completed", &self.completed),
            ("received", &self.received),
        ] {
            if let Some(r) = r {
                j.raw(name, &r.report_rate(SEC_NS).to_string());
            }
        }
        add_percentiles(
            &mut j,
            "delaySendTime",
            self.wait.as_ref().map(|l| &l.interval),
        );
        add_percentiles(
            &mut j,
            "sendTime",
            self.sent_latencies.as_ref().map(|l| &l.interval),
        );
        add_percentiles(
            &mut j,
            "transferTime",
            self.end_to_end.as_ref().map(|l| &l.interval),
        );
        self.write_json(j)
    }

    fn write_json(&mut self, j: JsonObject) -> anyhow::Result<()> {
        let Some(w) = &mut self.json else {
            return Ok(());
        };
        if self.json_samples > 0 {
            w.write_all(b",\n")?;
        }
        self.json_samples += 1;
        w.write_all(j.finish().as_bytes())?;
        w.flush()?;
        Ok(())
    }

    fn append_tagged_hdr(&mut self, warmup: bool) -> anyhow::Result<()> {
        let Some(w) = &mut self.hdr else {
            return Ok(());
        };
        for (l, tag) in [
            (&mut self.wait, "delay send"),
            (&mut self.sent_latencies, "send"),
            (&mut self.end_to_end, "transfer"),
        ] {
            if let Some(l) = l {
                l.interval.tag = Some(if warmup {
                    format!("warmup {tag}")
                } else {
                    tag.to_owned()
                });
                w.output_interval_histogram(&l.interval)?;
            }
        }
        Ok(())
    }

    /// `outSummary`.
    pub fn out_summary(&mut self, out: &mut String) -> anyhow::Result<()> {
        out.push_str("\n--- SUMMARY");
        let failed = self.any_fatal_error();
        out.push_str("\n--- result:         ");
        out.push_str(if failed {
            "        fail"
        } else {
            "     success"
        });
        for (label, r) in [
            ("\n--- total sent:     ", &self.sent),
            ("\n--- total blocked:  ", &self.blocked),
            ("\n--- total completed:", &self.completed),
            ("\n--- total received: ", &self.received),
        ] {
            if let Some(r) = r {
                out.push_str(label);
                out.push_str(&pad(r.last_sample.to_string(), 12));
            }
        }
        out_percentiles(
            out,
            "\n--- aggregated delay send time:",
            self.wait.as_ref().map(|l| &l.accumulated),
        );
        out_percentiles(
            out,
            "\n--- aggregated send time:      ",
            self.sent_latencies.as_ref().map(|l| &l.accumulated),
        );
        out_percentiles(
            out,
            "\n--- aggregated transfer time:  ",
            self.end_to_end.as_ref().map(|l| &l.accumulated),
        );
        if self.json.is_some() {
            let mut j = JsonObject::default();
            j.string("sampleType", "summary");
            j.raw("time", &self.sample_time.to_string());
            j.string("result", if failed { "fail" } else { "success" });
            for (name, r) in [
                ("totalSent", &self.sent),
                ("totalBlocked", &self.blocked),
                ("totalCompleted", &self.completed),
                ("totalReceived", &self.received),
            ] {
                if let Some(r) = r {
                    j.raw(name, &r.last_sample.to_string());
                }
            }
            add_percentiles(
                &mut j,
                "totalDelaySendTime",
                self.wait.as_ref().map(|l| &l.accumulated),
            );
            add_percentiles(
                &mut j,
                "totalSendTime",
                self.sent_latencies.as_ref().map(|l| &l.accumulated),
            );
            add_percentiles(
                &mut j,
                "totalTransferTime",
                self.end_to_end.as_ref().map(|l| &l.accumulated),
            );
            self.write_json(j)?;
        }
        Ok(())
    }

    pub fn close(mut self) -> anyhow::Result<()> {
        if let Some(mut w) = self.json.take() {
            w.write_all(b"\n]")?;
            w.flush()?;
        }
        if let Some(w) = self.hdr.take() {
            w.close()?;
        }
        Ok(())
    }
}

/// `appendRateOf`: ` <metric> <rate, 8 wide> msg/sec`.
fn append_rate(out: &mut String, metric: &str, sampler: &Option<RateSampler>) {
    if let Some(s) = sampler {
        let _ = write!(
            out,
            " {metric} {} msg/sec",
            pad(s.report_rate(SEC_NS).to_string(), 8)
        );
    }
}

/// `outPercentiles`.
fn out_percentiles(out: &mut String, metric: &str, h: Option<&Histogram>) {
    let Some(h) = h else {
        return;
    };
    let f = |v: f64| pad(decimal2(v), 9);
    let _ = write!(
        out,
        " {metric} mean: {} us - 50.00%: {} us - 90.00%: {} us - 99.00%: {} us - 99.90%: {} us - 99.99%: {} us - max:    {} us",
        f(h.mean()),
        f(h.value_at_percentile(50.0) as f64),
        f(h.value_at_percentile(90.0) as f64),
        f(h.value_at_percentile(99.0) as f64),
        f(h.value_at_percentile(99.9) as f64),
        f(h.value_at_percentile(99.99) as f64),
        f(h.max_value() as f64),
    );
}

/// `addPercentiles`: the mean as text, the percentiles as numbers.
fn add_percentiles(j: &mut JsonObject, metric: &str, h: Option<&Histogram>) {
    let Some(h) = h else {
        return;
    };
    let mut p = JsonObject::default();
    p.string("mean", &decimal2(h.mean()));
    for (name, percentile) in [
        ("50", 50.0),
        ("90", 90.0),
        ("99", 99.0),
        ("99.9", 99.9),
        ("99.99", 99.99),
    ] {
        p.raw(name, &h.value_at_percentile(percentile).to_string());
    }
    p.raw("max", &h.max_value().to_string());
    p.raw("count", &h.total_count().to_string());
    j.raw(metric, &p.finish());
}

/// `PaddingDecimalFormat`: spaces before, up to `width`.
fn pad(s: String, width: usize) -> String {
    format!("{s:>width$}")
}

/// `DecimalFormat("0.00")`: two decimals, half even on the exact value.
pub fn decimal2(v: f64) -> String {
    format!("{v:.2}")
}

/// A compact JSON object, its members in the order added.
#[derive(Default)]
struct JsonObject {
    s: String,
}

impl JsonObject {
    fn key(&mut self, k: &str) {
        self.s.push(if self.s.is_empty() { '{' } else { ',' });
        self.s.push('"');
        self.s.push_str(k);
        self.s.push_str("\":");
    }

    fn raw(&mut self, k: &str, v: &str) {
        self.key(k);
        self.s.push_str(v);
    }

    fn string(&mut self, k: &str, v: &str) {
        self.key(k);
        self.s.push('"');
        self.s.push_str(v);
        self.s.push('"');
    }

    fn finish(mut self) -> String {
        if self.s.is_empty() {
            self.s.push('{');
        }
        self.s.push('}');
        self.s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats() {
        assert_eq!(decimal2(0.125), "0.12");
        assert_eq!(decimal2(0.375), "0.38");
        assert_eq!(decimal2(12.0), "12.00");
        assert_eq!(pad("12".into(), 8), "      12");
        assert_eq!(pad("123456789".into(), 8), "123456789");
        let mut j = JsonObject::default();
        j.string("a", "b");
        j.raw("c", "1");
        assert_eq!(j.finish(), r#"{"a":"b","c":1}"#);
    }

    #[test]
    fn interval_report() {
        let p = Arc::new(ProducerStats::default());
        let mut s = LiveStatistics::new(None, None, vec![p.clone()], None).unwrap();
        p.sent.store(10, Ordering::Relaxed);
        p.send_completed_latencies.record(100);
        std::thread::sleep(std::time::Duration::from_millis(5));
        s.sample_metrics(false);
        let mut out = String::new();
        s.out_at_interval(false, &mut out, true).unwrap();
        let lines: Vec<&str> = out.split('\n').collect();
        assert_eq!(lines[0], "");
        assert_eq!(lines[1], "--- warmup false ");
        assert!(lines[2].starts_with("--- sent:      "));
        assert!(lines[2].ends_with(" msg/sec "));
        assert_eq!(
            lines[5],
            "--- send ack time:   mean:    100.00 us - 50.00%:    100.00 us - 90.00%:    100.00 us - 99.00%:    100.00 us - 99.90%:    100.00 us - 99.99%:    100.00 us - max:       100.00 us"
        );
        let mut out = String::new();
        s.out_summary(&mut out).unwrap();
        assert!(out.starts_with(
            "\n--- SUMMARY\n--- result:              success\n--- total sent:               10"
        ));
    }
}
