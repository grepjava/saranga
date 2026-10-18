//! The broker's log as Artemis writes it, configured by the instance's
//! `etc/log4j2.properties` (or the file `-Dlog4j2.configurationFile`
//! names), so runbooks that read or tune Artemis's logs work unchanged.
//!
//! - **Loggers.** Each event gets the name of the Artemis logger that
//!   would have logged it: its target when that is an explicit dotted
//!   name (`org.apache.activemq.audit.base`), else the logger of the
//!   message bundle its `AMQ` code belongs to (`AMQ221007` is
//!   `org.apache.activemq.artemis.core.server`), else one for its crate.
//!   Levels, appender references (with their levels) and additivity apply
//!   down the dotted hierarchy as in log4j 2.
//! - **Appenders:** `Console` (`SYSTEM_OUT` or `SYSTEM_ERR`), `File`, and
//!   `RollingFile`/`RollingRandomAccessFile` with the time-based, cron,
//!   size-based and on-startup triggering policies and the default
//!   rollover strategy's `max`; `.gz` archives are compressed.
//! - **`PatternLayout`**, with the conversions Artemis's files use and
//!   the common others: `%d` (named and `SimpleDateFormat` patterns, in
//!   local time), `%p`/`%level`, `%c`/`%logger{n}`, `%m`/`%msg`, `%n`,
//!   `%t`/`%thread`, `%pid`, `%C`, `%F`, `%L`, `%highlight{..}` and
//!   `%style{..}` (without colours), and width and truncation modifiers.
//! - **`monitorInterval`:** the file is reread when it changes, as
//!   log4j's monitor rereads it; the audit loggers can be turned on so.
//!
//! `RUST_LOG`, when set, chooses which events are logged instead of the
//! levels (a logger set to `OFF` stays off); the appenders still apply.

use std::collections::HashMap;
use std::fmt::{self, Write as _};
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::field::{Field, Visit};
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

/// `${sys:name}`: a system property's value.
pub type Lookup = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// Artemis's console and file pattern.
pub const ARTEMIS_PATTERN: &str = "%d %-5level [%logger] %msg%n";
/// `DefaultConfiguration.DEFAULT_PATTERN`.
const LOG4J2_DEFAULT_PATTERN: &str = "%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n";

// log4j 2's level values.
const OFF: u32 = 0;
const ERROR: u32 = 200;
const WARN: u32 = 300;
const INFO: u32 = 400;
const DEBUG: u32 = 500;
const TRACE: u32 = 600;
const ALL: u32 = u32::MAX;

fn level_value(level: Level) -> u32 {
    match level {
        Level::ERROR => ERROR,
        Level::WARN => WARN,
        Level::INFO => INFO,
        Level::DEBUG => DEBUG,
        Level::TRACE => TRACE,
    }
}

fn parse_level(s: &str) -> Option<u32> {
    Some(match s.trim().to_ascii_uppercase().as_str() {
        "OFF" => OFF,
        "FATAL" => 100,
        "ERROR" => ERROR,
        "WARN" => WARN,
        "INFO" => INFO,
        "DEBUG" => DEBUG,
        "TRACE" => TRACE,
        "ALL" => ALL,
        _ => return None,
    })
}

fn level_name(level: Level) -> &'static str {
    match level {
        Level::ERROR => "ERROR",
        Level::WARN => "WARN",
        Level::INFO => "INFO",
        Level::DEBUG => "DEBUG",
        Level::TRACE => "TRACE",
    }
}

// ---------------------------------------------------------------------------
// Logger names

/// The logger of the message bundle an `AMQ` code belongs to, as each
/// bundle's `@LogBundle` range and `BundleFactory.newBundle` name it.
fn bundle_logger(code: u32) -> Option<&'static str> {
    let two = code / 10_000;
    let three = code / 1000;
    Some(match two {
        10 => "org.apache.activemq.artemis.integration.bootstrap",
        11 => "org.apache.activemq.artemis.protocol.amqp.logger",
        12 => "org.apache.activemq.artemis.jms.server",
        13 => "org.apache.activemq.artemis.jms.client",
        14 => "org.apache.activemq.artemis.journal",
        15 => "org.apache.activemq.artemis.ra",
        17 => "org.apache.activemq.artemis.service.extensions.xa.recovery",
        20 => "org.apache.activemq.artemis.logs",
        21 => "org.apache.activemq.artemis.core.client",
        22 => "org.apache.activemq.artemis.core.server",
        24 => "org.apache.activemq.artemis",
        33 => "org.apache.activemq.artemis.core.protocol.stomp",
        34 => "org.apache.activemq.artemis.jms.bridge",
        35 => "org.apache.activemq.artemis.service.extensions",
        60 => "org.apache.activemq.audit.base",
        83 | 85 => "org.apache.activemq.artemis.core.protocol.mqtt",
        84 => "org.apache.activemq.artemis.core.server.plugin.impl",
        _ if three == 119 => "org.apache.activemq.artemis.protocol.amqp.logger",
        _ => return None,
    })
}

/// The logger for a crate's events that carry no code.
fn crate_logger(krate: &str) -> Option<&'static str> {
    Some(match krate {
        "saranga" | "saranga_cli" => "org.apache.activemq.artemis.integration.bootstrap",
        "saranga_server" | "saranga_ha" => "org.apache.activemq.artemis.core.server",
        "saranga_cluster" => "org.apache.activemq.artemis.core.server.cluster",
        "saranga_journal" => "org.apache.activemq.artemis.journal",
        "saranga_storage" => "org.apache.activemq.artemis.core.persistence",
        "saranga_paging" => "org.apache.activemq.artemis.core.paging",
        "saranga_config" => "org.apache.activemq.artemis.core.config",
        "saranga_security" => "org.apache.activemq.artemis.spi.core.security",
        "saranga_transport" => "org.apache.activemq.artemis.core.remoting",
        "saranga_management" => "org.apache.activemq.artemis",
        "saranga_client" => "org.apache.activemq.artemis.core.client",
        "saranga_message" | "saranga_buffer" => "org.apache.activemq.artemis.core.message",
        "saranga_filter" => "org.apache.activemq.artemis.selector",
        "saranga_core_protocol" => "org.apache.activemq.artemis.core.protocol.core",
        "saranga_protocol_amqp" => "org.apache.activemq.artemis.protocol.amqp",
        "saranga_protocol_mqtt" => "org.apache.activemq.artemis.core.protocol.mqtt",
        "saranga_protocol_stomp" => "org.apache.activemq.artemis.core.protocol.stomp",
        "saranga_protocol_openwire" => "org.apache.activemq.artemis.core.protocol.openwire",
        _ => return None,
    })
}

/// The six digits of a message's leading `AMQnnnnnn` code.
fn amq_code(message: &str) -> Option<u32> {
    let digits = message.strip_prefix("AMQ")?.get(..6)?;
    if digits.bytes().all(|b| b.is_ascii_digit()) {
        digits.parse().ok()
    } else {
        None
    }
}

/// The Artemis logger an event is logged as.
pub fn logger_name(meta: &Metadata<'_>, message: &str) -> String {
    let target = meta.target();
    if meta.module_path() != Some(target) {
        return target.replace("::", ".");
    }
    if let Some(name) = amq_code(message).and_then(bundle_logger) {
        return name.to_owned();
    }
    let krate = target.split("::").next().unwrap_or(target);
    match crate_logger(krate) {
        Some(name) => name.to_owned(),
        None => target.replace("::", "."),
    }
}

// ---------------------------------------------------------------------------
// Time

/// The local time zone's offset from UTC at an instant, in seconds.
fn local_offset(secs: i64) -> i64 {
    saranga_server::localtime::offset(secs)
}

/// The calendar fields of an instant, in local time or UTC.
struct Civil {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    millis: i64,
    weekday: usize,
    offset: i64,
}

fn civil(millis: i64, utc: bool) -> Civil {
    let offset = if utc {
        0
    } else {
        local_offset(millis.div_euclid(1000))
    };
    let local = millis + offset * 1000;
    let secs = local.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    Civil {
        year: yoe + era * 400 + i64::from(month <= 2),
        month,
        day,
        hour: tod / 3600,
        minute: tod % 3600 / 60,
        second: tod % 60,
        millis: local.rem_euclid(1000),
        // 1970-01-01 was a Thursday.
        weekday: (days + 4).rem_euclid(7) as usize,
        offset,
    }
}

const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DAYS: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

#[derive(Clone, Debug)]
enum DateField {
    Text(String),
    /// A `SimpleDateFormat` letter and its count.
    Letter(char, usize),
    UnixSeconds,
    UnixMillis,
}

/// A `%d` format: a `SimpleDateFormat` pattern or one of log4j's names.
#[derive(Clone, Debug)]
struct DateFormat {
    fields: Vec<DateField>,
    utc: bool,
}

impl DateFormat {
    fn new(pattern: &str, zone: Option<&str>) -> Self {
        let pattern = match pattern {
            "" | "DEFAULT" => "yyyy-MM-dd HH:mm:ss,SSS",
            "ABSOLUTE" => "HH:mm:ss,SSS",
            "COMPACT" => "yyyyMMddHHmmssSSS",
            "DATE" => "dd MMM yyyy HH:mm:ss,SSS",
            "ISO8601" => "yyyy-MM-dd'T'HH:mm:ss,SSS",
            "ISO8601_BASIC" => "yyyyMMdd'T'HHmmss,SSS",
            "ISO8601_OFFSET_DATE_TIME_HHMM" => "yyyy-MM-dd'T'HH:mm:ss,SSSXX",
            "ISO8601_OFFSET_DATE_TIME_HHCMM" => "yyyy-MM-dd'T'HH:mm:ss,SSSXXX",
            "DEFAULT_MICROS" | "DEFAULT_NANOS" => "yyyy-MM-dd HH:mm:ss,SSS",
            "UNIX" => {
                return Self {
                    fields: vec![DateField::UnixSeconds],
                    utc: true,
                };
            }
            "UNIX_MILLIS" => {
                return Self {
                    fields: vec![DateField::UnixMillis],
                    utc: true,
                };
            }
            p => p,
        };
        let utc = zone.is_some_and(|z| {
            let z = z.trim();
            z.eq_ignore_ascii_case("UTC") || z.eq_ignore_ascii_case("GMT") || z == "Z"
        });
        let mut fields = Vec::new();
        let chars: Vec<char> = pattern.chars().collect();
        let mut i = 0;
        let mut text = String::new();
        while i < chars.len() {
            let c = chars[i];
            if c == '\'' {
                // A quoted run; '' is a quote.
                if chars.get(i + 1) == Some(&'\'') {
                    text.push('\'');
                    i += 2;
                    continue;
                }
                i += 1;
                while i < chars.len() {
                    if chars[i] == '\'' {
                        if chars.get(i + 1) == Some(&'\'') {
                            text.push('\'');
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    text.push(chars[i]);
                    i += 1;
                }
                i += 1;
            } else if c.is_ascii_alphabetic() {
                let mut n = 1;
                while chars.get(i + n) == Some(&c) {
                    n += 1;
                }
                if !text.is_empty() {
                    fields.push(DateField::Text(std::mem::take(&mut text)));
                }
                fields.push(DateField::Letter(c, n));
                i += n;
            } else {
                text.push(c);
                i += 1;
            }
        }
        if !text.is_empty() {
            fields.push(DateField::Text(text));
        }
        Self { fields, utc }
    }

    fn format(&self, millis: i64, out: &mut String) {
        let t = civil(millis, self.utc);
        for f in &self.fields {
            match f {
                DateField::Text(s) => out.push_str(s),
                DateField::UnixSeconds => {
                    let _ = write!(out, "{}", millis.div_euclid(1000));
                }
                DateField::UnixMillis => {
                    let _ = write!(out, "{millis}");
                }
                DateField::Letter(c, n) => {
                    let n = *n;
                    let num = |out: &mut String, v: i64| {
                        let _ = write!(out, "{v:0n$}");
                    };
                    match c {
                        'y' | 'u' if n == 2 => num(out, t.year.rem_euclid(100)),
                        'y' | 'u' => num(out, t.year),
                        'M' | 'L' if n >= 4 => out.push_str(MONTHS[(t.month - 1) as usize]),
                        'M' | 'L' if n == 3 => out.push_str(&MONTHS[(t.month - 1) as usize][..3]),
                        'M' | 'L' => num(out, t.month),
                        'd' => num(out, t.day),
                        'H' => num(out, t.hour),
                        'k' => num(out, if t.hour == 0 { 24 } else { t.hour }),
                        'h' => num(out, if t.hour % 12 == 0 { 12 } else { t.hour % 12 }),
                        'K' => num(out, t.hour % 12),
                        'm' => num(out, t.minute),
                        's' => num(out, t.second),
                        'S' => {
                            // Milliseconds, padded or cut to the count.
                            let ms = format!("{:03}", t.millis);
                            if n <= 3 {
                                out.push_str(&ms[..n]);
                            } else {
                                out.push_str(&ms);
                                out.extend(std::iter::repeat_n('0', n - 3));
                            }
                        }
                        'a' => out.push_str(if t.hour < 12 { "AM" } else { "PM" }),
                        'E' if n >= 4 => out.push_str(DAYS[t.weekday]),
                        'E' => out.push_str(&DAYS[t.weekday][..3]),
                        'Z' => offset(out, t.offset, false),
                        'X' if t.offset == 0 => out.push('Z'),
                        'X' => offset(out, t.offset, n >= 3),
                        'z' => {
                            if t.offset == 0 {
                                out.push_str("UTC");
                            } else {
                                out.push_str("GMT");
                                offset(out, t.offset, true);
                            }
                        }
                        other => out.extend(std::iter::repeat_n(*other, n)),
                    }
                }
            }
        }

        fn offset(out: &mut String, secs: i64, colon: bool) {
            let sign = if secs < 0 { '-' } else { '+' };
            let m = secs.abs() / 60;
            let _ = if colon {
                write!(out, "{sign}{:02}:{:02}", m / 60, m % 60)
            } else {
                write!(out, "{sign}{:02}{:02}", m / 60, m % 60)
            };
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

fn mtime_millis(path: &Path) -> Option<i64> {
    let modified = std::fs::metadata(path).ok()?.modified().ok()?;
    Some(modified.duration_since(UNIX_EPOCH).ok()?.as_millis() as i64)
}

// ---------------------------------------------------------------------------
// PatternLayout

/// One logged event, as a layout sees it.
pub struct Record<'a> {
    pub millis: i64,
    pub level: Level,
    pub logger: &'a str,
    pub message: &'a str,
    pub thread: &'a str,
    pub meta: &'a Metadata<'a>,
}

#[derive(Clone, Debug)]
enum Conversion {
    Date(DateFormat),
    Level,
    /// The logger name, cut to its last n (or without its first -n) parts.
    Logger(Option<i32>),
    Message,
    Newline,
    Thread,
    ThreadId,
    Pid,
    Class,
    File,
    Line,
    /// A conversion with nothing to show here (`%ex`, `%X`, `%M`).
    Empty,
    /// `%highlight{..}`, `%style{..}`: the pattern inside, uncoloured.
    Nested(Pattern),
}

#[derive(Clone, Debug)]
enum Piece {
    Text(String),
    Conv {
        conversion: Conversion,
        min: usize,
        max: Option<usize>,
        left: bool,
    },
}

/// A compiled `PatternLayout` pattern.
#[derive(Clone, Debug)]
pub struct Pattern {
    pieces: Vec<Piece>,
}

const NEWLINE: &str = if cfg!(windows) { "\r\n" } else { "\n" };

impl Pattern {
    /// The pattern, and the conversions it does not know.
    pub fn compile(pattern: &str) -> (Self, Vec<String>) {
        let mut unknown = Vec::new();
        let chars: Vec<char> = pattern.chars().collect();
        let mut i = 0;
        let p = Self::parse(&chars, &mut i, &mut unknown, false);
        (p, unknown)
    }

    fn parse(chars: &[char], i: &mut usize, unknown: &mut Vec<String>, nested: bool) -> Self {
        let mut pieces = Vec::new();
        let mut text = String::new();
        while *i < chars.len() {
            let c = chars[*i];
            if nested && c == '}' {
                break;
            }
            if c == '\\' && *i + 1 < chars.len() {
                // log4j's escapes in patterns.
                match chars[*i + 1] {
                    'n' => text.push('\n'),
                    'r' => text.push('\r'),
                    't' => text.push('\t'),
                    other => text.push(other),
                }
                *i += 2;
                continue;
            }
            if c != '%' {
                text.push(c);
                *i += 1;
                continue;
            }
            *i += 1;
            if chars.get(*i) == Some(&'%') {
                text.push('%');
                *i += 1;
                continue;
            }
            // The format modifier: -min.max
            let left = chars.get(*i) == Some(&'-');
            if left {
                *i += 1;
            }
            let mut min = 0usize;
            while let Some(d) = chars.get(*i).and_then(|c| c.to_digit(10)) {
                min = min * 10 + d as usize;
                *i += 1;
            }
            let mut max = None;
            if chars.get(*i) == Some(&'.') {
                *i += 1;
                let mut m = 0usize;
                while let Some(d) = chars.get(*i).and_then(|c| c.to_digit(10)) {
                    m = m * 10 + d as usize;
                    *i += 1;
                }
                max = Some(m);
            }
            let start = *i;
            while chars.get(*i).is_some_and(|c| c.is_ascii_alphabetic()) {
                *i += 1;
            }
            let name: String = chars[start..*i].iter().collect();
            // Options: {..}{..}; a nested pattern for the styling ones.
            let styled = matches!(
                name.as_str(),
                "highlight"
                    | "style"
                    | "equals"
                    | "equalsIgnoreCase"
                    | "notEmpty"
                    | "varsNotEmpty"
                    | "variablesNotEmpty"
                    | "maxLength"
                    | "maxLen"
                    | "encode"
                    | "enc"
                    | "replace"
            );
            let mut options = Vec::new();
            let mut inner = None;
            while chars.get(*i) == Some(&'{') {
                *i += 1;
                if styled && inner.is_none() {
                    inner = Some(Self::parse(chars, i, unknown, true));
                    *i += 1;
                    continue;
                }
                let mut depth = 1;
                let mut option = String::new();
                while let Some(&c) = chars.get(*i) {
                    *i += 1;
                    match c {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    option.push(c);
                }
                options.push(option);
            }
            if !text.is_empty() {
                pieces.push(Piece::Text(std::mem::take(&mut text)));
            }
            let option = |n: usize| options.get(n).map(String::as_str);
            let conversion = match name.as_str() {
                "d" | "date" => {
                    Conversion::Date(DateFormat::new(option(0).unwrap_or(""), option(1)))
                }
                "p" | "level" => Conversion::Level,
                "c" | "logger" => Conversion::Logger(option(0).and_then(|o| o.trim().parse().ok())),
                "m" | "msg" | "message" => Conversion::Message,
                "n" => Conversion::Newline,
                "t" | "thread" | "tn" | "threadName" => Conversion::Thread,
                "T" | "tid" | "threadId" => Conversion::ThreadId,
                "pid" | "processId" => Conversion::Pid,
                "C" | "class" => Conversion::Class,
                "F" | "file" => Conversion::File,
                "L" | "line" => Conversion::Line,
                "ex" | "exception" | "throwable" | "xEx" | "xException" | "xThrowable" | "rEx"
                | "rException" | "rThrowable" | "X" | "mdc" | "MDC" | "x" | "NDC" | "M"
                | "method" | "K" | "map" | "MAP" | "marker" | "markerSimpleName" | "u" | "uuid"
                | "sn" | "sequenceNumber" | "l" | "location" | "fqcn" | "endOfBatch" => {
                    Conversion::Empty
                }
                "r" | "relative" => Conversion::Empty,
                "highlight" | "style" => {
                    Conversion::Nested(inner.take().unwrap_or(Self { pieces: Vec::new() }))
                }
                _ => {
                    unknown.push(format!("%{name}"));
                    pieces.push(Piece::Text(format!("%{name}")));
                    continue;
                }
            };
            pieces.push(Piece::Conv {
                conversion,
                min,
                max,
                left,
            });
        }
        if !text.is_empty() {
            pieces.push(Piece::Text(text));
        }
        Self { pieces }
    }

    pub fn format(&self, r: &Record<'_>, out: &mut String) {
        for piece in &self.pieces {
            match piece {
                Piece::Text(t) => out.push_str(t),
                Piece::Conv {
                    conversion,
                    min,
                    max,
                    left,
                } => {
                    let mut s = String::new();
                    match conversion {
                        Conversion::Date(f) => f.format(r.millis, &mut s),
                        Conversion::Level => s.push_str(level_name(r.level)),
                        Conversion::Logger(None) => s.push_str(r.logger),
                        Conversion::Logger(Some(n)) => s.push_str(&abbreviate(r.logger, *n)),
                        Conversion::Message => s.push_str(r.message),
                        Conversion::Newline => s.push_str(NEWLINE),
                        Conversion::Thread => s.push_str(r.thread),
                        Conversion::ThreadId => {
                            let _ = write!(s, "{}", thread_number());
                        }
                        Conversion::Pid => {
                            let _ = write!(s, "{}", std::process::id());
                        }
                        Conversion::Class => {
                            s.push_str(&r.meta.module_path().unwrap_or("?").replace("::", "."))
                        }
                        Conversion::File => s.push_str(
                            r.meta
                                .file()
                                .map_or("?", |f| f.rsplit(['/', '\\']).next().unwrap_or(f)),
                        ),
                        Conversion::Line => {
                            let _ = write!(s, "{}", r.meta.line().unwrap_or(0));
                        }
                        Conversion::Empty => {}
                        Conversion::Nested(p) => p.format(r, &mut s),
                    }
                    let mut chars = s.chars().count();
                    if let Some(max) = max
                        && chars > *max
                    {
                        // log4j keeps the end of a truncated value.
                        s = s.chars().skip(chars - max).collect();
                        chars = *max;
                    }
                    let pad = min.saturating_sub(chars);
                    if *left {
                        out.push_str(&s);
                        out.extend(std::iter::repeat_n(' ', pad));
                    } else {
                        out.extend(std::iter::repeat_n(' ', pad));
                        out.push_str(&s);
                    }
                }
            }
        }
    }
}

/// `%c{n}`: the last n dotted parts, or without the first -n.
fn abbreviate(name: &str, n: i32) -> String {
    let parts: Vec<&str> = name.split('.').collect();
    let keep = if n >= 0 {
        parts.len().saturating_sub(n as usize)
    } else {
        (-n as usize).min(parts.len())
    };
    parts[keep..].join(".")
}

fn thread_number() -> u64 {
    // ThreadId's Debug is "ThreadId(n)".
    let id = format!("{:?}", std::thread::current().id());
    id.trim_start_matches("ThreadId(")
        .trim_end_matches(')')
        .parse()
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Appenders

enum FilePart {
    Text(String),
    Date(DateFormat),
    Index,
}

/// A rolling file's `filePattern`.
struct FilePattern {
    parts: Vec<FilePart>,
    gzip: bool,
}

impl FilePattern {
    fn new(pattern: &str) -> Self {
        let mut parts = Vec::new();
        let chars: Vec<char> = pattern.chars().collect();
        let mut i = 0;
        let mut text = String::new();
        while i < chars.len() {
            if chars[i] == '%' {
                let rest: String = chars[i + 1..].iter().collect();
                let date = rest.strip_prefix("date").or_else(|| rest.strip_prefix('d'));
                if let Some(after) = date {
                    let consumed = rest.len() - after.len();
                    let (format, used) = match after.strip_prefix('{') {
                        Some(a) => match a.find('}') {
                            Some(end) => (&a[..end], end + 2),
                            None => ("", 0),
                        },
                        None => ("yyyy-MM-dd", 0),
                    };
                    if !text.is_empty() {
                        parts.push(FilePart::Text(std::mem::take(&mut text)));
                    }
                    parts.push(FilePart::Date(DateFormat::new(format, None)));
                    i += 1 + rest[..consumed + used].chars().count();
                    continue;
                }
                if rest.starts_with('i') {
                    if !text.is_empty() {
                        parts.push(FilePart::Text(std::mem::take(&mut text)));
                    }
                    parts.push(FilePart::Index);
                    i += 2;
                    continue;
                }
            }
            text.push(chars[i]);
            i += 1;
        }
        if !text.is_empty() {
            parts.push(FilePart::Text(text));
        }
        let gzip = pattern.ends_with(".gz");
        Self { parts, gzip }
    }

    fn has_date(&self) -> bool {
        self.parts.iter().any(|p| matches!(p, FilePart::Date(_)))
    }

    fn has_index(&self) -> bool {
        self.parts.iter().any(|p| matches!(p, FilePart::Index))
    }

    /// The pattern's dates at a time: what changes when the period does.
    fn period(&self, millis: i64) -> String {
        let mut s = String::new();
        for p in &self.parts {
            if let FilePart::Date(f) = p {
                f.format(millis, &mut s);
                s.push('|');
            }
        }
        s
    }

    fn path(&self, millis: i64, index: u32) -> PathBuf {
        let mut s = String::new();
        for p in &self.parts {
            match p {
                FilePart::Text(t) => s.push_str(t),
                FilePart::Date(f) => f.format(millis, &mut s),
                FilePart::Index => {
                    let _ = write!(s, "{index}");
                }
            }
        }
        PathBuf::from(s)
    }
}

struct Rolling {
    pattern: FilePattern,
    /// Roll when the pattern's date changes (a time or cron policy).
    timed: bool,
    max_size: Option<u64>,
    on_startup: bool,
    max_index: u32,
}

struct FileState {
    file: Option<File>,
    size: u64,
    /// When the file was last written.
    last: i64,
}

struct FileSink {
    path: PathBuf,
    append: bool,
    rolling: Option<Rolling>,
    state: Mutex<FileState>,
}

impl FileSink {
    fn open(&self, state: &mut FileState, first: bool) -> std::io::Result<()> {
        if let Some(dir) = self.path.parent()
            && !dir.as_os_str().is_empty()
        {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(self.append || !first)
            .truncate(!self.append && first)
            .open(&self.path)?;
        state.size = file.metadata().map_or(0, |m| m.len());
        state.last = if state.size > 0 {
            mtime_millis(&self.path).unwrap_or_else(now_millis)
        } else {
            now_millis()
        };
        state.file = Some(file);
        Ok(())
    }

    fn write(&self, millis: i64, line: &str) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.file.is_none() {
            if let Err(e) = self.open(&mut state, true) {
                eprintln!("saranga: unable to open {}: {e}", self.path.display());
                return;
            }
            if let Some(r) = &self.rolling
                && r.on_startup
                && state.size > 0
            {
                self.roll(&mut state);
            }
        }
        if let Some(r) = &self.rolling {
            let due = (r.timed && r.pattern.period(state.last) != r.pattern.period(millis))
                || r.max_size
                    .is_some_and(|max| state.size > 0 && state.size + line.len() as u64 > max);
            if due {
                self.roll(&mut state);
            }
        }
        if let Some(f) = state.file.as_mut() {
            if let Err(e) = f.write_all(line.as_bytes()) {
                eprintln!("saranga: unable to write {}: {e}", self.path.display());
                return;
            }
            state.size += line.len() as u64;
            state.last = millis;
        }
    }

    /// Moves the file to its archive name and starts a new one.
    fn roll(&self, state: &mut FileState) {
        let Some(r) = &self.rolling else { return };
        state.file = None;
        let target = if r.pattern.has_index() {
            let max = r.max_index.max(1);
            let free = (1..=max).find(|&i| !self.archive(r, state.last, i).exists());
            match free {
                Some(i) => self.archive(r, state.last, i),
                None => {
                    // The oldest goes, the others move down one.
                    let _ = std::fs::remove_file(self.archive(r, state.last, 1));
                    for i in 2..=max {
                        let _ = std::fs::rename(
                            self.archive(r, state.last, i),
                            self.archive(r, state.last, i - 1),
                        );
                    }
                    self.archive(r, state.last, max)
                }
            }
        } else {
            self.archive(r, state.last, 0)
        };
        let moved = if r.pattern.gzip {
            gzip(&self.path, &target).and_then(|()| std::fs::remove_file(&self.path))
        } else {
            std::fs::rename(&self.path, &target)
        };
        if let Err(e) = moved {
            eprintln!(
                "saranga: unable to roll {} to {}: {e}",
                self.path.display(),
                target.display()
            );
        }
        if let Err(e) = self.open(state, false) {
            eprintln!("saranga: unable to open {}: {e}", self.path.display());
        }
    }

    /// The archive path (with `.gz` when it is compressed).
    fn archive(&self, r: &Rolling, millis: i64, index: u32) -> PathBuf {
        r.pattern.path(millis, index)
    }
}

fn gzip(from: &Path, to: &Path) -> std::io::Result<()> {
    if let Some(dir) = to.parent()
        && !dir.as_os_str().is_empty()
    {
        std::fs::create_dir_all(dir)?;
    }
    let mut input = File::open(from)?;
    let mut encoder =
        flate2::write::GzEncoder::new(File::create(to)?, flate2::Compression::default());
    std::io::copy(&mut input, &mut encoder)?;
    encoder.finish()?;
    Ok(())
}

enum Sink {
    Stdout,
    Stderr,
    File(FileSink),
}

struct Appender {
    layout: Pattern,
    /// A `ThresholdFilter`'s level.
    threshold: u32,
    sink: Sink,
}

impl Appender {
    fn append(&self, r: &Record<'_>) {
        if level_value(r.level) > self.threshold {
            return;
        }
        let mut line = String::new();
        self.layout.format(r, &mut line);
        match &self.sink {
            Sink::Stdout => {
                let _ = std::io::stdout().lock().write_all(line.as_bytes());
            }
            Sink::Stderr => {
                let _ = std::io::stderr().lock().write_all(line.as_bytes());
            }
            Sink::File(f) => f.write(r.millis, &line),
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration

struct LoggerConfig {
    name: String,
    level: u32,
    /// Appender indexes, with each reference's own level.
    refs: Vec<(usize, u32)>,
    additive: bool,
}

/// A parsed `log4j2.properties`.
pub struct Config {
    root: LoggerConfig,
    loggers: HashMap<String, LoggerConfig>,
    appenders: Vec<Appender>,
    /// The most verbose level any logger has: events above it are off.
    max_level: u32,
    /// Seconds between checks of the file; 0 for none.
    monitor_interval: u64,
}

impl Config {
    /// Artemis's default: INFO, to the console, in Artemis's pattern.
    pub fn console(level: u32, stderr: bool) -> Self {
        let (layout, _) = Pattern::compile(ARTEMIS_PATTERN);
        Self {
            root: LoggerConfig {
                name: String::new(),
                level,
                refs: vec![(0, ALL)],
                additive: false,
            },
            // The audit loggers are off, as Artemis's file has them.
            loggers: HashMap::from([(
                "org.apache.activemq.audit".to_owned(),
                LoggerConfig {
                    name: "org.apache.activemq.audit".to_owned(),
                    level: OFF,
                    refs: Vec::new(),
                    additive: false,
                },
            )]),
            appenders: vec![Appender {
                layout,
                threshold: ALL,
                sink: if stderr { Sink::Stderr } else { Sink::Stdout },
            }],
            max_level: level,
            monitor_interval: 0,
        }
    }

    /// log4j2's `DefaultConfiguration`, which a JVM with no configuration
    /// file logs with: ERROR, to standard output, in log4j2's default
    /// pattern.
    pub fn log4j2_default() -> Self {
        let (layout, _) = Pattern::compile(LOG4J2_DEFAULT_PATTERN);
        Self {
            root: LoggerConfig {
                name: String::new(),
                level: ERROR,
                refs: vec![(0, ALL)],
                additive: false,
            },
            loggers: HashMap::new(),
            appenders: vec![Appender {
                layout,
                threshold: ALL,
                sink: Sink::Stdout,
            }],
            max_level: ERROR,
            monitor_interval: 0,
        }
    }

    /// The configuration a `log4j2.properties` gives, and what in it was
    /// not understood. `sys` resolves `${sys:..}` lookups.
    pub fn parse(text: &[u8], sys: &dyn Fn(&str) -> Option<String>) -> (Self, Vec<String>) {
        let mut warnings = Vec::new();
        let entries = saranga_config::properties::parse(text);
        let mut vars = HashMap::new();
        for (k, v) in &entries {
            if let Some(name) = k.strip_prefix("property.") {
                vars.insert(name.to_owned(), v.trim().to_owned());
            }
        }
        let resolve = |v: &str| substitute(v.trim(), &vars, sys);
        let mut props: HashMap<String, String> = HashMap::new();
        for (k, v) in &entries {
            props.insert(k.trim().to_owned(), resolve(v));
        }
        let get = |k: &str| props.get(k).map(String::as_str);

        // Appenders, by their `name`.
        let mut appender_ids: Vec<&str> = entries
            .iter()
            .filter_map(|(k, _)| k.strip_prefix("appender."))
            .filter_map(|k| k.split('.').next())
            .collect();
        appender_ids.dedup();
        let mut seen = std::collections::HashSet::new();
        appender_ids.retain(|id| seen.insert(*id));
        let mut appenders = Vec::new();
        let mut by_name = HashMap::new();
        for id in appender_ids {
            let a = |k: &str| get(&format!("appender.{id}.{k}"));
            let name = a("name").unwrap_or(id).to_owned();
            let kind = a("type").unwrap_or("");
            let pattern = match a("layout.type") {
                None | Some("PatternLayout") => a("layout.pattern").unwrap_or("%m%n"),
                Some(other) => {
                    warnings.push(format!(
                        "appender {name}: layout {other} is not supported; using a PatternLayout"
                    ));
                    a("layout.pattern").unwrap_or("%m%n")
                }
            };
            let (layout, unknown) = Pattern::compile(pattern);
            for u in unknown {
                warnings.push(format!("appender {name}: unknown conversion {u}"));
            }
            let threshold = props
                .iter()
                .filter(|(k, v)| {
                    k.starts_with(&format!("appender.{id}.filter."))
                        && k.ends_with(".type")
                        && v.as_str() == "ThresholdFilter"
                })
                .find_map(|(k, _)| {
                    let base = k.trim_end_matches(".type");
                    get(&format!("{base}.level")).and_then(parse_level)
                })
                .unwrap_or(ALL);
            let sink = match kind {
                "Console" => match a("target").map(str::to_ascii_uppercase).as_deref() {
                    Some("SYSTEM_ERR") => Sink::Stderr,
                    _ => Sink::Stdout,
                },
                "File"
                | "RandomAccessFile"
                | "MemoryMappedFile"
                | "RollingFile"
                | "RollingRandomAccessFile" => {
                    let Some(file_name) = a("fileName") else {
                        warnings.push(format!("appender {name}: no fileName"));
                        continue;
                    };
                    let rolling = kind.starts_with("Rolling").then(|| {
                        let policies: Vec<(&str, &str)> = props
                            .iter()
                            .filter_map(|(k, v)| {
                                let rest = k.strip_prefix(&format!("appender.{id}.policies."))?;
                                let (pid, field) = rest.split_once('.')?;
                                (field == "type").then_some((pid, v.as_str()))
                            })
                            .collect();
                        let policy_field = |kind: &str, field: &str| {
                            policies
                                .iter()
                                .find(|(_, t)| *t == kind)
                                .and_then(|(pid, _)| {
                                    get(&format!("appender.{id}.policies.{pid}.{field}"))
                                })
                        };
                        let has = |kind: &str| policies.iter().any(|(_, t)| *t == kind);
                        let pattern = FilePattern::new(a("filePattern").unwrap_or(""));
                        let timed = (has("TimeBasedTriggeringPolicy")
                            || has("CronTriggeringPolicy"))
                            && pattern.has_date();
                        let max_size = has("SizeBasedTriggeringPolicy").then(|| {
                            policy_field("SizeBasedTriggeringPolicy", "size")
                                .and_then(parse_size)
                                .unwrap_or(10 * 1024 * 1024)
                        });
                        for (_, t) in &policies {
                            if !matches!(
                                *t,
                                "TimeBasedTriggeringPolicy"
                                    | "CronTriggeringPolicy"
                                    | "SizeBasedTriggeringPolicy"
                                    | "OnStartupTriggeringPolicy"
                                    | "Policies"
                            ) {
                                warnings
                                    .push(format!("appender {name}: policy {t} is not supported"));
                            }
                        }
                        Rolling {
                            timed,
                            max_size,
                            on_startup: has("OnStartupTriggeringPolicy"),
                            max_index: a("strategy.max").and_then(|m| m.parse().ok()).unwrap_or(7),
                            pattern,
                        }
                    });
                    Sink::File(FileSink {
                        path: PathBuf::from(file_name),
                        append: a("append").is_none_or(|v| !v.eq_ignore_ascii_case("false")),
                        rolling,
                        state: Mutex::new(FileState {
                            file: None,
                            size: 0,
                            last: 0,
                        }),
                    })
                }
                other => {
                    warnings.push(format!("appender {name}: type {other} is not supported"));
                    continue;
                }
            };
            by_name.insert(name, appenders.len());
            appenders.push(Appender {
                layout,
                threshold,
                sink,
            });
        }

        // Loggers.
        let refs_of = |prefix: &str, warnings: &mut Vec<String>| {
            let mut ids: Vec<&str> = props
                .keys()
                .filter_map(|k| k.strip_prefix(&format!("{prefix}.appenderRef.")))
                .filter_map(|k| k.split('.').next())
                .collect();
            ids.sort_unstable();
            ids.dedup();
            let mut refs = Vec::new();
            for rid in ids {
                let Some(target) = get(&format!("{prefix}.appenderRef.{rid}.ref")) else {
                    continue;
                };
                match by_name.get(target) {
                    Some(&i) => refs.push((
                        i,
                        get(&format!("{prefix}.appenderRef.{rid}.level"))
                            .and_then(parse_level)
                            .unwrap_or(ALL),
                    )),
                    None => warnings.push(format!("{prefix}: no appender {target}")),
                }
            }
            refs
        };
        // `rootLogger = LEVEL, ref, ...`, the short form.
        let short = |value: &str, warnings: &mut Vec<String>| {
            let mut parts = value.split(',').map(str::trim);
            let level = parts.next().and_then(parse_level);
            let refs: Vec<(usize, u32)> = parts
                .filter(|r| !r.is_empty())
                .filter_map(|r| match by_name.get(r) {
                    Some(&i) => Some((i, ALL)),
                    None => {
                        warnings.push(format!("no appender {r}"));
                        None
                    }
                })
                .collect();
            (level, refs)
        };
        let mut root = LoggerConfig {
            name: String::new(),
            level: get("rootLogger.level")
                .and_then(parse_level)
                .unwrap_or(ERROR),
            refs: refs_of("rootLogger", &mut warnings),
            additive: false,
        };
        if let Some(v) = get("rootLogger") {
            let (level, refs) = short(v, &mut warnings);
            if let Some(l) = level {
                root.level = l;
            }
            root.refs.extend(refs);
        }
        let mut logger_ids: Vec<&str> = entries
            .iter()
            .filter_map(|(k, _)| k.strip_prefix("logger."))
            .map(|k| k.split('.').next().unwrap_or(k))
            .collect();
        let mut seen = std::collections::HashSet::new();
        logger_ids.retain(|id| seen.insert(*id));
        let mut loggers = HashMap::new();
        for id in logger_ids {
            let prefix = format!("logger.{id}");
            let (name, level, mut refs) = match get(&format!("{prefix}.name")) {
                Some(name) => (
                    name.to_owned(),
                    get(&format!("{prefix}.level")).and_then(parse_level),
                    refs_of(&prefix, &mut warnings),
                ),
                None => match get(&prefix) {
                    // `logger.<name> = LEVEL, ref, ...`
                    Some(v) => {
                        let (level, refs) = short(v, &mut warnings);
                        (id.to_owned(), level, refs)
                    }
                    None => {
                        warnings.push(format!("{prefix}: no name"));
                        continue;
                    }
                },
            };
            refs.dedup();
            let additive = get(&format!("{prefix}.additivity"))
                .is_none_or(|v| !v.eq_ignore_ascii_case("false"));
            loggers.insert(
                name.clone(),
                LoggerConfig {
                    name,
                    // A logger without a level takes its parent's.
                    level: level.unwrap_or(u32::MAX - 1),
                    refs,
                    additive,
                },
            );
        }
        let mut config = Self {
            root,
            loggers,
            appenders,
            max_level: 0,
            monitor_interval: get("monitorInterval")
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0),
        };
        // Inherited levels, now that every logger is known.
        let names: Vec<String> = config.loggers.keys().cloned().collect();
        let inherited: Vec<(String, u32)> = names
            .iter()
            .filter(|n| config.loggers[*n].level == u32::MAX - 1)
            .map(|n| (n.clone(), config.inherited_level(n)))
            .collect();
        for (n, l) in inherited {
            if let Some(c) = config.loggers.get_mut(&n) {
                c.level = l;
            }
        }
        config.max_level = config
            .loggers
            .values()
            .map(|l| l.level)
            .chain([config.root.level])
            .max()
            .unwrap_or(OFF);
        (config, warnings)
    }

    fn inherited_level(&self, name: &str) -> u32 {
        let mut n = name;
        while let Some((parent, _)) = n.rsplit_once('.') {
            if let Some(l) = self.loggers.get(parent)
                && l.level != u32::MAX - 1
            {
                return l.level;
            }
            n = parent;
        }
        self.root.level
    }

    /// The configuration of the logger a name falls under.
    fn find(&self, name: &str) -> &LoggerConfig {
        let mut n = name;
        loop {
            if let Some(l) = self.loggers.get(n) {
                return l;
            }
            match n.rsplit_once('.') {
                Some((parent, _)) => n = parent,
                None => return &self.root,
            }
        }
    }

    fn parent(&self, logger: &LoggerConfig) -> Option<&LoggerConfig> {
        if logger.name.is_empty() {
            return None;
        }
        Some(match logger.name.rsplit_once('.') {
            Some((parent, _)) => self.find(parent),
            None => &self.root,
        })
    }

    /// Whether any logger logs at a level, before the event's logger is
    /// known.
    fn may_log(&self, level: u32) -> bool {
        level <= self.max_level
    }

    /// Logs an event to its logger's appenders and, while additive, its
    /// ancestors' (`LoggerConfig.log`).
    fn log(&self, r: &Record<'_>, levels: bool) {
        let level = level_value(r.level);
        let mut logger = self.find(r.logger);
        let allowed = if levels {
            level <= logger.level
        } else {
            logger.level != OFF
        };
        if !allowed {
            return;
        }
        loop {
            for &(appender, ref_level) in &logger.refs {
                if level <= ref_level {
                    self.appenders[appender].append(r);
                }
            }
            if !logger.additive {
                break;
            }
            match self.parent(logger) {
                Some(p) => logger = p,
                None => break,
            }
        }
    }
}

/// `10 MB`, `512 KB`, `1GB`: `FileSize.parse`.
fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (n, unit) = s.split_at(split);
    let n: f64 = n.trim().parse().ok()?;
    let mult = match unit.trim().to_ascii_uppercase().as_str() {
        "" | "B" | "BYTES" => 1.0,
        "K" | "KB" => 1024.0,
        "M" | "MB" => 1024.0 * 1024.0,
        "G" | "GB" => 1024.0 * 1024.0 * 1024.0,
        "T" | "TB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    Some((n * mult) as u64)
}

/// log4j's `${prefix:name:-default}` lookups: `sys`, `env`, and the
/// file's own `property.` values; `$${` is a literal `${`.
fn substitute(
    value: &str,
    vars: &HashMap<String, String>,
    sys: &dyn Fn(&str) -> Option<String>,
) -> String {
    let mut out = String::new();
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        if start > 0 && rest.as_bytes()[start - 1] == b'$' {
            out.push_str(&rest[..start - 1]);
            out.push_str("${");
            rest = &rest[start + 2..];
            continue;
        }
        out.push_str(&rest[..start]);
        let Some(end) = rest[start..].find('}') else {
            out.push_str(&rest[start..]);
            return out;
        };
        let expr = &rest[start + 2..start + end];
        let (key, default) = match expr.split_once(":-") {
            Some((k, d)) => (k, Some(d)),
            None => (expr, None),
        };
        let found = match key.split_once(':') {
            Some(("sys", name)) => sys(name),
            Some(("env", name)) => std::env::var(name).ok(),
            Some(("main" | "ctx" | "date" | "java" | "jndi" | "web", _)) => None,
            _ => vars.get(key).cloned().or_else(|| sys(key)),
        };
        match found.or_else(|| default.map(str::to_owned)) {
            Some(v) => out.push_str(&v),
            None => out.push_str(&rest[start..start + end + 1]),
        }
        rest = &rest[start + end + 1..];
    }
    out.push_str(rest);
    out
}

// ---------------------------------------------------------------------------
// The layer

#[derive(Default)]
struct Fields {
    message: String,
    extra: String,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else if !field.name().starts_with("log.") {
            let _ = write!(self.extra, " {}={value}", field.name());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else if !field.name().starts_with("log.") {
            let _ = write!(self.extra, " {}={value:?}", field.name());
        }
    }
}

struct Shared {
    config: RwLock<Arc<Config>>,
    /// Levels come from the configuration, not `RUST_LOG`.
    levels: bool,
}

static SHARED: OnceLock<Arc<Shared>> = OnceLock::new();

struct ArtemisLayer {
    shared: Arc<Shared>,
}

impl ArtemisLayer {
    /// Whether an event can be logged before its message is known: exactly
    /// for an explicit logger (the audit loggers), else whether any logger
    /// logs at its level; `None` when that is for `RUST_LOG` to say.
    fn enabled_for(&self, meta: &Metadata<'_>) -> Option<bool> {
        let config = self.config();
        let level = level_value(*meta.level());
        if meta.module_path() != Some(meta.target()) {
            let logger = config.find(&meta.target().replace("::", ".")).level;
            return if self.shared.levels {
                Some(level <= logger)
            } else if logger == OFF {
                Some(false)
            } else {
                None
            };
        }
        if self.shared.levels {
            Some(config.may_log(level))
        } else {
            None
        }
    }

    fn config(&self) -> Arc<Config> {
        self.shared
            .config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

impl<S: Subscriber> Layer<S> for ArtemisLayer {
    fn register_callsite(&self, meta: &'static Metadata<'static>) -> Interest {
        if meta.is_span() {
            return Interest::never();
        }
        // An explicit logger is known now: decided once, until the
        // configuration changes (which rebuilds the interest cache).
        match self.enabled_for(meta) {
            Some(true) if self.shared.levels => Interest::always(),
            Some(false) => Interest::never(),
            _ => Interest::sometimes(),
        }
    }

    fn enabled(&self, meta: &Metadata<'_>, _: Context<'_, S>) -> bool {
        self.enabled_for(meta).unwrap_or(true)
    }

    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let meta = event.metadata();
        let logger = logger_name(meta, &fields.message);
        if !fields.extra.is_empty() {
            fields.message.push_str(&fields.extra);
        }
        let thread = std::thread::current();
        let record = Record {
            millis: now_millis(),
            level: *meta.level(),
            logger: &logger,
            message: &fields.message,
            thread: thread.name().unwrap_or("main"),
            meta,
        };
        self.config().log(&record, self.shared.levels);
    }
}

/// Starts logging the broker's way: Artemis's console pattern at INFO
/// until [`configure`] reads the instance's file.
pub fn init_broker() {
    let env = EnvFilter::try_from_default_env().ok();
    let shared = Arc::new(Shared {
        config: RwLock::new(Arc::new(Config::console(INFO, false))),
        levels: env.is_none(),
    });
    let _ = SHARED.set(shared.clone());
    tracing_subscriber::registry()
        .with(env)
        .with(ArtemisLayer { shared })
        .init();
}

/// Starts logging as the utility commands do (`artemis-utility.profile`):
/// WARN to the console in Artemis's pattern, then the instance's
/// `log4j2-utility.properties`, found on the classpath through etc/. Run
/// from the home, whose `bin/artemis` names no configuration, it is
/// log4j2's default: errors only, so that what a command writes to
/// standard output (`data exp`'s XML) is all there is.
pub fn init_utility(etc: Option<&Path>) {
    let env = EnvFilter::try_from_default_env().ok();
    let config = match etc {
        Some(_) => Config::console(WARN, false),
        None => Config::log4j2_default(),
    };
    let shared = Arc::new(Shared {
        config: RwLock::new(Arc::new(config)),
        levels: env.is_none(),
    });
    let _ = SHARED.set(shared.clone());
    tracing_subscriber::registry()
        .with(env)
        .with(ArtemisLayer { shared })
        .init();
    if let Some(etc) = etc {
        configure(
            etc.join("log4j2-utility.properties"),
            Arc::new(|k| std::env::var(k).ok()),
        );
    }
}

/// Where `-Dlog4j2.configurationFile` points, else `etc/log4j2.properties`.
pub fn config_path(etc: &Path, configured: Option<String>) -> PathBuf {
    match configured {
        Some(c) => {
            let c = c.split(',').next().unwrap_or(&c).trim().to_owned();
            let path = c
                .strip_prefix("file://")
                .or_else(|| c.strip_prefix("file:"))
                .map_or(c.as_str(), |p| p);
            // file:///C:/x on Windows.
            let path = if cfg!(windows) && path.starts_with('/') && path.get(2..3) == Some(":") {
                &path[1..]
            } else {
                path
            };
            PathBuf::from(path)
        }
        None => etc.join("log4j2.properties"),
    }
}

/// Reads a broker's logging configuration, then watches it as
/// `monitorInterval` asks. Without the file, the default stays.
pub fn configure(path: PathBuf, sys: Lookup) {
    let Some(shared) = SHARED.get().cloned() else {
        return;
    };
    let Ok(text) = std::fs::read(&path) else {
        tracing::debug!("no logging configuration at {}", path.display());
        return;
    };
    let mut interval = apply(&shared, &path, &text, &*sys);
    let mut stamp = mtime_millis(&path);
    std::thread::Builder::new()
        .name("log4j2-monitor".to_owned())
        .spawn(move || {
            loop {
                if interval == 0 {
                    // Nothing to watch until a reload asks again.
                    std::thread::sleep(Duration::from_secs(5));
                    continue;
                }
                std::thread::sleep(Duration::from_secs(interval));
                let now = mtime_millis(&path);
                if now == stamp {
                    continue;
                }
                stamp = now;
                if let Ok(text) = std::fs::read(&path) {
                    interval = apply(&shared, &path, &text, &*sys);
                }
            }
        })
        .ok();
}

fn apply(shared: &Shared, path: &Path, text: &[u8], sys: &dyn Fn(&str) -> Option<String>) -> u64 {
    let (config, warnings) = Config::parse(text, sys);
    let interval = config.monitor_interval;
    *shared.config.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(config);
    tracing::callsite::rebuild_interest_cache();
    for w in warnings {
        tracing::warn!("{}: {w}", path.display());
    }
    interval
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record<'a>(meta: &'a Metadata<'a>, logger: &'a str, message: &'a str) -> Record<'a> {
        Record {
            // 2026-10-11 22:20:25.435 UTC
            millis: 1_791_757_225_435,
            level: Level::INFO,
            logger,
            message,
            thread: "main",
            meta,
        }
    }

    /// An event's metadata, for layouts that show its module or line.
    fn test_meta() -> &'static Metadata<'static> {
        struct Capture(Mutex<Option<&'static Metadata<'static>>>);
        impl Subscriber for Capture {
            fn enabled(&self, _: &Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
            fn event(&self, e: &Event<'_>) {
                *self.0.lock().unwrap() = Some(e.metadata());
            }
            fn enter(&self, _: &tracing::span::Id) {}
            fn exit(&self, _: &tracing::span::Id) {}
        }
        let capture = Arc::new(Capture(Mutex::new(None)));
        tracing::subscriber::with_default(capture.clone(), || tracing::info!("x"));
        capture.0.lock().unwrap().expect("an event")
    }

    #[test]
    fn artemis_pattern() {
        let meta = test_meta();
        let (p, unknown) =
            Pattern::compile("%d{yyyy-MM-dd HH:mm:ss,SSS}{UTC} %-5level [%logger] %msg%n");
        assert!(unknown.is_empty());
        let mut s = String::new();
        p.format(
            &record(
                meta,
                "org.apache.activemq.artemis.core.server",
                "AMQ221007: Server is now active",
            ),
            &mut s,
        );
        assert_eq!(
            s,
            format!(
                "2026-10-11 22:20:25,435 INFO  [org.apache.activemq.artemis.core.server] AMQ221007: Server is now active{NEWLINE}"
            )
        );
    }

    #[test]
    fn modifiers_and_abbreviation() {
        let meta = test_meta();
        let (p, _) = Pattern::compile("[%10.3c{2}] %-7p|%.4m|%highlight{%p}|%%");
        let mut s = String::new();
        p.format(&record(meta, "a.b.c.d", "abcdefg"), &mut s);
        assert_eq!(s, "[       c.d] INFO   |defg|INFO|%");
    }

    #[test]
    fn dates() {
        let mut s = String::new();
        DateFormat::new("ISO8601", Some("UTC")).format(1_791_757_225_435, &mut s);
        assert_eq!(s, "2026-10-11T22:20:25,435");
        s.clear();
        DateFormat::new("EEE dd MMM yy hh:mm a 'o''clock' Z", Some("GMT")).format(0, &mut s);
        assert_eq!(s, "Thu 01 Jan 70 12:00 AM o'clock +0000");
    }

    #[test]
    fn logger_names_from_codes() {
        assert_eq!(
            bundle_logger(221007),
            Some("org.apache.activemq.artemis.core.server")
        );
        assert_eq!(
            bundle_logger(142023),
            Some("org.apache.activemq.artemis.journal")
        );
        assert_eq!(
            bundle_logger(834003),
            Some("org.apache.activemq.artemis.core.protocol.mqtt")
        );
        assert_eq!(
            bundle_logger(119031),
            Some("org.apache.activemq.artemis.protocol.amqp.logger")
        );
        assert_eq!(bundle_logger(241001), Some("org.apache.activemq.artemis"));
        assert_eq!(amq_code("AMQ221007: x"), Some(221007));
        assert_eq!(amq_code("AMQ22100: x"), None);
    }

    const ARTEMIS: &str = include_str!("../templates/etc/log4j2.properties");

    #[test]
    fn artemis_file() {
        let sys = |n: &str| (n == "artemis.instance").then(|| "/i".to_owned());
        let (c, warnings) = Config::parse(ARTEMIS.as_bytes(), &sys);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(c.monitor_interval, 5);
        assert_eq!(c.root.level, INFO);
        assert_eq!(c.find("org.apache.activemq.audit.base").level, OFF);
        assert!(!c.find("org.apache.activemq.audit.base").additive);
        assert_eq!(c.find("org.eclipse.jetty.server").level, WARN);
        assert_eq!(
            c.find("org.apache.activemq.artemis.core.server.impl").level,
            INFO
        );
        assert_eq!(c.appenders.len(), 3);
        let Sink::File(f) = &c.appenders[1].sink else {
            panic!("not a file")
        };
        assert_eq!(f.path, PathBuf::from("/i/log/artemis.log"));
        let r = f.rolling.as_ref().expect("rolling");
        assert!(r.timed);
        // The date in the archive's name is local time.
        let mut expected = String::from("/i/log/artemis.log.");
        DateFormat::new("yyyy-MM-dd", None).format(1_791_757_225_435, &mut expected);
        assert_eq!(
            r.pattern.path(1_791_757_225_435, 0),
            PathBuf::from(expected)
        );
    }

    #[test]
    fn substitution() {
        let vars = HashMap::from([("dir".to_owned(), "/logs".to_owned())]);
        let sys = |n: &str| (n == "a").then(|| "A".to_owned());
        assert_eq!(
            substitute(
                "${dir}/x-${sys:a}-${sys:b:-B}-$${sys:a}-${sys:c}",
                &vars,
                &sys
            ),
            "/logs/x-A-B-${sys:a}-${sys:c}"
        );
    }

    #[test]
    fn additivity_and_refs() {
        let text = "\
rootLogger = INFO, out
logger.a.name = x.y
logger.a.level = DEBUG
logger.a.additivity = false
logger.a.appenderRef.e.ref = err
logger.a.appenderRef.e.level = WARN
logger.b.name = x.y.z
appender.o.type = Console
appender.o.name = out
appender.e.type = Console
appender.e.name = err
appender.e.target = SYSTEM_ERR
";
        let (c, warnings) = Config::parse(text.as_bytes(), &|_| None);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(c.root.refs, vec![(0, ALL)]);
        let a = c.find("x.y.q");
        assert_eq!(a.name, "x.y");
        assert_eq!(a.refs, vec![(1, WARN)]);
        // x.y.z has no level: it inherits x.y's, and is additive to it.
        let b = c.find("x.y.z.w");
        assert_eq!(b.level, DEBUG);
        assert!(b.additive);
        assert_eq!(c.parent(b).map(|p| p.name.as_str()), Some("x.y"));
        assert_eq!(c.max_level, DEBUG);
    }

    #[test]
    fn rolls_by_size_with_index() {
        let dir = std::env::temp_dir().join(format!("saranga-log-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let base = dir.to_string_lossy().replace('\\', "/");
        let text = format!(
            "\
rootLogger.level = INFO
rootLogger.appenderRef.f.ref = f
appender.f.type = RollingFile
appender.f.name = f
appender.f.fileName = {base}/a.log
appender.f.filePattern = {base}/a.log.%i.gz
appender.f.layout.type = PatternLayout
appender.f.layout.pattern = %m
appender.f.policies.type = Policies
appender.f.policies.size.type = SizeBasedTriggeringPolicy
appender.f.policies.size.size = 10 B
appender.f.strategy.type = DefaultRolloverStrategy
appender.f.strategy.max = 2
"
        );
        let (c, warnings) = Config::parse(text.as_bytes(), &|_| None);
        assert!(warnings.is_empty(), "{warnings:?}");
        let meta = test_meta();
        for m in ["0123456789", "abcdefghij", "ABCDEFGHIJ", "klmnopqrst"] {
            c.log(&record(meta, "x", m), true);
        }

        let read = |p: &str| std::fs::read_to_string(dir.join(p)).unwrap_or_default();
        let gunzip = |p: &str| {
            let mut s = String::new();
            let f = File::open(dir.join(p)).expect(p);
            std::io::Read::read_to_string(&mut flate2::read::GzDecoder::new(f), &mut s).expect(p);
            s
        };
        assert_eq!(read("a.log"), "klmnopqrst");
        // Three rolls, two kept: the oldest went.
        assert_eq!(gunzip("a.log.1.gz"), "abcdefghij");
        assert_eq!(gunzip("a.log.2.gz"), "ABCDEFGHIJ");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_size("10 MB"), Some(10 * 1024 * 1024));
        assert_eq!(parse_size("512KB"), Some(512 * 1024));
        assert_eq!(parse_size("7"), Some(7));
        assert_eq!(parse_size("x"), None);
    }
}
