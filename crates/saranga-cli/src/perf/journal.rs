//! `saranga perf-journal` (`PerfJournal`, `SyncCalculation`): how many
//! synced block writes the journal's disk takes per millisecond, and the
//! `journal-buffer-timeout` that follows. `create` tunes the journal with
//! the same test.

use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::time::Instant;

use anyhow::Context;
use clap::Args;
use saranga_config::JournalType;
use saranga_storage::java;

use crate::data::dirs::{CliLock, DataDirArgs};

#[derive(Args)]
pub struct PerfJournalArgs {
    /// The block size for each write. Default 4096.
    #[arg(long = "block-size", default_value_t = 4096)]
    size: usize,
    /// The number of writes to be performed. Default: 250.
    #[arg(long, default_value_t = 250)]
    writes: u64,
    /// The number of tries for the test. Default: 5.
    #[arg(long, default_value_t = 5)]
    tries: u32,
    /// Disable syncs.
    #[arg(long = "no-sync")]
    no_sync: bool,
    /// Enable syncs.
    #[arg(long)]
    sync: bool,
    /// Journal type to be used: Default: read from broker.xml.
    #[arg(long = "journal-type")]
    journal_type: Option<String>,
    /// Perform each write synchronously, e.g. if there was a single
    /// producer.
    #[arg(long = "sync-writes")]
    sync_writes: bool,
    /// The file name to be used. Default: test.tmp.
    #[arg(long, default_value = "test.tmp")]
    file: String,
    /// libaio.maxAIO to be used. Default: read from broker.xml.
    #[arg(long = "max-aio", default_value_t = 0)]
    max_aio: i32,
    /// This will allow certain tools like print-data to be performed
    /// ignoring any running servers. WARNING: Changing data concurrently
    /// with a running broker may damage your data. Be careful with this
    /// option.
    #[arg(long = "f")]
    ignore_lock: bool,
    #[command(flatten)]
    dirs: DataDirArgs,
}

fn type_name(t: JournalType) -> &'static str {
    match t {
        JournalType::Nio => "NIO",
        JournalType::Asyncio => "ASYNCIO",
        JournalType::Mapped => "MAPPED",
    }
}

pub fn run(args: &PerfJournalArgs) -> anyhow::Result<()> {
    let d = args.dirs.unchecked()?;
    let _lock = match (&d.instance, args.ignore_lock) {
        (Some(_), true) => None,
        (instance, _) => Some(CliLock::take(instance.as_deref())?),
    };
    let journal = d.config.as_ref().map(|c| &c.journal);
    let mut datasync = journal
        .and_then(|j| j.datasync)
        .unwrap_or(saranga_config::JournalConfig::DEFAULT_DATASYNC);
    if args.no_sync {
        datasync = false;
    } else if args.sync {
        datasync = true;
    }
    let journal_type = match &args.journal_type {
        Some(t) => t.parse().map_err(|_| {
            crate::client::UsageError(format!(
                "Invalid JournalType:{t} valid Types: NIO,ASYNCIO,MAPPED"
            ))
        })?,
        None => journal.map_or(saranga_config::JournalConfig::DEFAULT_JOURNAL_TYPE, |j| {
            j.journal_type
        }),
    };
    // libaio's maxAIO: Saranga writes as NIO does, one write at a time.
    let _ = args.max_aio;
    println!();
    println!("Auto tuning journal ...");
    println!(
        "Performing {} tests writing {} blocks of {} on each test, sync={datasync} with journalType = {}",
        args.tries,
        args.writes,
        args.size,
        type_name(journal_type)
    );
    std::fs::create_dir_all(&d.journal).ok();
    notices(args, journal_type);
    let time = sync_test(
        &d.journal,
        &args.file,
        args.size,
        args.writes,
        args.tries,
        datasync,
        args.dirs.verbose,
    )?;
    let nanos = to_nanos(time, args.writes, args.dirs.verbose);
    println!(
        "Your system can execute {} writes per millisecond",
        decimal_format(args.writes as f64 / time as f64)
    );
    println!("Your journal-buffer-timeout should be:{nanos}");
    println!("You should use this following configuration:");
    println!();
    println!("<journal-buffer-timeout>{nanos}</journal-buffer-timeout>");
    Ok(())
}

/// What `syncTest` prints before the runs: the notice on `--sync-writes`
/// with `ASYNCIO`, and with `--verbose` the file factory.
fn notices(args: &PerfJournalArgs, journal_type: JournalType) {
    if journal_type == JournalType::Asyncio && args.sync_writes {
        let stars = "*".repeat(91);
        println!();
        println!("{stars}");
        println!(
            "*** Notice: The recommendation for AsyncIO journal is to not use --sync-writes          ***"
        );
        println!(
            "***         The measures here will be useful to understand your device                  ***"
        );
        println!(
            "***         however the result here won't represent the best configuration option       ***"
        );
        println!("{stars}");
        println!();
    }
    if args.dirs.verbose {
        let factory = if journal_type == JournalType::Mapped {
            "org.apache.activemq.artemis.core.io.mapped.MappedSequentialFileFactory"
        } else {
            "org.apache.activemq.artemis.core.io.nio.NIOSequentialFileFactory"
        };
        println!("Using {factory} to calculate sync times, alignment=1");
    }
}

/// `syncTest`: `tries` runs of `blocks` writes of `block_size` bytes, each
/// synced when `fsync`, over a file `file_name` in `dir` filled first; the
/// fastest run's milliseconds. Every journal type writes through a file
/// here, as `NIO` does (and as Artemis does for `ASYNCIO` without libaio).
pub(crate) fn sync_test(
    dir: &Path,
    file_name: &str,
    block_size: usize,
    blocks: u64,
    tries: u32,
    fsync: bool,
    verbose: bool,
) -> anyhow::Result<u64> {
    let path = dir.join(file_name);
    let _ = std::fs::remove_file(&path);
    let block = vec![b't'; block_size];
    let result = (|| -> anyhow::Result<u64> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("creating {}", path.display()))?;
        let zeros = vec![0u8; 1024 * 1024];
        let mut left = block_size as u64 * blocks;
        while left > 0 {
            let n = left.min(zeros.len() as u64) as usize;
            file.write_all(&zeros[..n])?;
            left -= n as u64;
        }
        file.sync_all()?;
        let mut best = u64::MAX;
        for n in 0..tries {
            if verbose {
                println!("**************************************************");
                println!("{n} of {tries} calculation");
            }
            file.seek(SeekFrom::Start(0))?;
            let start = Instant::now();
            for _ in 0..blocks {
                file.write_all(&block)?;
                if fsync {
                    file.sync_data()?;
                }
            }
            let time = start.elapsed().as_millis() as u64;
            if verbose {
                println!("Time = {time} milliseconds");
                println!(
                    "Writes / millisecond = {}",
                    decimal_format(blocks as f64 / time as f64)
                );
                println!("bufferTimeout = {}", to_nanos(time, blocks, verbose));
                println!("**************************************************");
            }
            best = best.min(time);
        }
        Ok(best)
    })();
    let _ = std::fs::remove_file(&path);
    result
}

/// `SyncCalculation.toNanos`: the nanoseconds a block write takes.
pub(crate) fn to_nanos(time_ms: u64, blocks: u64, verbose: bool) -> i64 {
    let per_ms = blocks as f64 / time_ms as f64;
    if verbose {
        println!("Blocks per millisecond::{}", java::double(per_ms));
    }
    let wait = (1_000_000.0 / per_ms) as i64;
    if verbose {
        println!(
            "your system could make a sync every {wait} nanoseconds, and this will be your timeout"
        );
    }
    wait
}

/// `new DecimalFormat("###.##").format(d)`: at most two decimals, rounded
/// half-even, without trailing zeros or a leading zero.
pub(crate) fn decimal_format(d: f64) -> String {
    if d.is_infinite() {
        return "\u{221e}".to_owned();
    }
    if d.is_nan() {
        return "\u{fffd}".to_owned();
    }
    let scaled = d * 100.0;
    let floor = scaled.floor();
    let rest = scaled - floor;
    let cents = if (rest - 0.5).abs() < 1e-9 {
        if floor as i64 % 2 == 0 {
            floor
        } else {
            floor + 1.0
        }
    } else {
        scaled.round()
    } as i64;
    let (int, frac) = (cents / 100, cents % 100);
    let mut out = if int == 0 {
        String::new()
    } else {
        int.to_string()
    };
    if frac != 0 {
        let f = format!("{frac:02}");
        out.push('.');
        out.push_str(f.trim_end_matches('0'));
    }
    if out.is_empty() {
        out.push('0');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::decimal_format;

    #[test]
    fn decimal_formats() {
        assert_eq!(decimal_format(12.346), "12.35");
        assert_eq!(decimal_format(12.0), "12");
        assert_eq!(decimal_format(0.5), ".5");
        assert_eq!(decimal_format(0.0), "0");
        assert_eq!(decimal_format(f64::INFINITY), "\u{221e}");
    }
}
