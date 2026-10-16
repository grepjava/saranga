//! Checks a history of queue operations, Jepsen style, against the model of
//! a queue: what is sent is received once.
//!
//! A history has a line for each operation a client process completed:
//!
//! ```text
//! PROCESS TYPE F VALUE INVOKE COMPLETE
//! 3 ok enqueue 17 120533 121904
//! ```
//!
//! F is `enqueue` (a send and its commit) or `dequeue` (a receive and its
//! commit); TYPE is `ok` (it took effect), `fail` (it did not: the
//! transaction was rolled back) or `info` (the client cannot tell); INVOKE
//! and COMPLETE are when it began and ended, in any unit as long as every
//! process uses the same clock. Each value is enqueued once. A process does
//! one operation at a time and none after an `info`, as in Jepsen, where a
//! process whose operation's outcome is unknown is replaced by a new one.
//! Lines starting with `#` are comments.
//!
//! The history must end with the queue drained. It is valid when:
//!
//! - every value whose enqueue was `ok` was dequeued, `ok` or `info`;
//! - no value was dequeued `ok` twice (a dequeue whose outcome is unknown may
//!   have been rolled back, and the value received again);
//! - no value was dequeued that was never enqueued, or whose enqueue failed;
//! - no value was dequeued before its enqueue began.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::str::FromStr;

use anyhow::{Context, Result, anyhow, bail};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Type {
    Ok,
    Fail,
    Info,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum F {
    Enqueue,
    Dequeue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Op {
    pub process: u64,
    pub kind: Type,
    pub f: F,
    pub value: u64,
    pub invoke: u64,
    pub complete: u64,
}

impl FromStr for Op {
    type Err = anyhow::Error;

    fn from_str(line: &str) -> Result<Op> {
        let words: Vec<&str> = line.split_whitespace().collect();
        let [process, kind, f, value, invoke, complete] = words[..] else {
            bail!("not PROCESS TYPE F VALUE INVOKE COMPLETE");
        };
        let number = |word: &str| {
            word.parse::<u64>()
                .map_err(|_| anyhow!("not a number: {word}"))
        };
        Ok(Op {
            process: number(process)?,
            kind: match kind {
                "ok" => Type::Ok,
                "fail" => Type::Fail,
                "info" => Type::Info,
                _ => bail!("unknown type {kind}"),
            },
            f: match f {
                "enqueue" => F::Enqueue,
                "dequeue" => F::Dequeue,
                _ => bail!("unknown operation {f}"),
            },
            value: number(value)?,
            invoke: number(invoke)?,
            complete: number(complete)?,
        })
    }
}

pub fn parse(text: &str) -> Result<Vec<Op>> {
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty() && !line.starts_with('#'))
        .map(|(n, line)| {
            line.parse()
                .with_context(|| format!("line {}: {line}", n + 1))
        })
        .collect()
}

/// What the check counted and found wrong.
#[derive(Debug, Default)]
pub struct Report {
    /// Operations by function and type.
    pub counts: BTreeMap<(&'static str, &'static str), usize>,
    /// Values whose enqueue's outcome was unknown and that were dequeued.
    pub recovered: usize,
    /// Values dequeued `ok` once and with an unknown outcome as well.
    pub unknown_twice: usize,
    /// What makes the history invalid, a line each.
    pub anomalies: Vec<String>,
    /// How many anomalies of each kind.
    pub kinds: BTreeMap<&'static str, usize>,
}

impl Report {
    pub fn valid(&self) -> bool {
        self.anomalies.is_empty()
    }

    fn anomaly(&mut self, kind: &'static str, text: String) {
        *self.kinds.entry(kind).or_default() += 1;
        self.anomalies.push(text);
    }
}

fn name(kind: Type) -> &'static str {
    match kind {
        Type::Ok => "ok",
        Type::Fail => "fail",
        Type::Info => "info",
    }
}

pub fn check(ops: &[Op]) -> Report {
    let mut report = Report::default();
    for op in ops {
        let f = match op.f {
            F::Enqueue => "enqueue",
            F::Dequeue => "dequeue",
        };
        *report.counts.entry((f, name(op.kind))).or_default() += 1;
    }

    // Each process one operation at a time, and none after an info.
    let mut processes: HashMap<u64, Vec<&Op>> = HashMap::new();
    for op in ops {
        if op.complete < op.invoke {
            report.anomaly(
                "malformed",
                format!("process {}: {:?} ends before it begins", op.process, op),
            );
        }
        processes.entry(op.process).or_default().push(op);
    }
    let mut ids: Vec<_> = processes.keys().copied().collect();
    ids.sort();
    for id in ids {
        let list = processes.get_mut(&id).unwrap();
        list.sort_by_key(|op| op.invoke);
        for pair in list.windows(2) {
            if pair[1].invoke < pair[0].complete {
                report.anomaly("malformed", format!("process {id}: two operations at once"));
            }
            if pair[0].kind == Type::Info {
                report.anomaly(
                    "malformed",
                    format!("process {id}: went on after an unknown outcome"),
                );
            }
        }
    }

    let mut enqueues: HashMap<u64, &Op> = HashMap::new();
    for op in ops.iter().filter(|op| op.f == F::Enqueue) {
        if enqueues.insert(op.value, op).is_some() {
            report.anomaly("malformed", format!("value {} enqueued twice", op.value));
        }
    }
    let mut dequeues: BTreeMap<u64, Vec<&Op>> = BTreeMap::new();
    for op in ops
        .iter()
        .filter(|op| op.f == F::Dequeue && op.kind != Type::Fail)
    {
        dequeues.entry(op.value).or_default().push(op);
    }

    let mut values: Vec<_> = enqueues.keys().copied().collect();
    values.sort();
    for value in values {
        let enqueue = enqueues[&value];
        let taken = dequeues.get(&value).map(Vec::as_slice).unwrap_or_default();
        match enqueue.kind {
            Type::Ok if taken.is_empty() => report.anomaly(
                "lost",
                format!("lost: {value}, enqueued ok and never dequeued"),
            ),
            Type::Info if !taken.is_empty() => report.recovered += 1,
            _ => {}
        }
    }
    for (&value, taken) in &dequeues {
        let oks = taken.iter().filter(|op| op.kind == Type::Ok).count();
        if oks > 1 {
            report.anomaly(
                "duplicated",
                format!("duplicated: {value}, dequeued ok {oks} times"),
            );
        } else if oks == 1 && taken.len() > 1 {
            report.unknown_twice += 1;
        }
        match enqueues.get(&value) {
            None => report.anomaly(
                "unexpected",
                format!("unexpected: {value}, dequeued and never enqueued"),
            ),
            Some(enqueue) if enqueue.kind == Type::Fail => report.anomaly(
                "unexpected",
                format!("unexpected: {value}, dequeued though its enqueue failed"),
            ),
            Some(enqueue) => {
                for op in taken.iter().filter(|op| op.complete < enqueue.invoke) {
                    report.anomaly(
                        "early",
                        format!(
                            "early: {value}, dequeued (process {}, ending at {}) before its enqueue began at {}",
                            op.process, op.complete, enqueue.invoke
                        ),
                    );
                }
            }
        }
    }
    report
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for function in ["enqueue", "dequeue"] {
            write!(f, "{function}:")?;
            for kind in ["ok", "fail", "info"] {
                write!(
                    f,
                    " {} {kind}",
                    self.counts.get(&(function, kind)).copied().unwrap_or(0)
                )?;
            }
            writeln!(f)?;
        }
        writeln!(
            f,
            "unknown enqueues dequeued: {}; values dequeued ok and unknown: {}",
            self.recovered, self.unknown_twice
        )?;
        for line in self.anomalies.iter().take(20) {
            writeln!(f, "{line}")?;
        }
        if self.anomalies.len() > 20 {
            writeln!(f, "... {} more", self.anomalies.len() - 20)?;
        }
        if self.valid() {
            write!(f, "valid")
        } else {
            let kinds: Vec<String> = self
                .kinds
                .iter()
                .map(|(kind, n)| format!("{n} {kind}"))
                .collect();
            write!(f, "invalid: {}", kinds.join(", "))
        }
    }
}
