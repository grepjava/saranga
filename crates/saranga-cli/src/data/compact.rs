//! `saranga data compact` (`CompactJournal`): compacts the message and
//! bindings journals of a broker that is not running.

use std::path::Path;

use anyhow::Context;
use saranga_journal::{Journal, JournalConfig};
use saranga_storage::ids;

use super::dirs::{CliLock, DataDirArgs};

pub fn run(args: &DataDirArgs) -> anyhow::Result<()> {
    let d = args.resolve()?;
    let _lock = CliLock::take(d.instance.as_deref())?;
    let journal = d.config.as_ref().map(|c| &c.journal);
    let mut messages = JournalConfig::messages(&d.journal);
    if let Some(j) = journal {
        if let Some(n) = j.min_files {
            messages.min_files = n.max(0) as usize;
        }
        if let Some(n) = j.pool_files {
            messages.pool_size = n;
        }
        if let Some(n) = j.file_size {
            messages.file_size = n.max(0) as usize;
        }
    }
    compact(
        messages,
        &[ids::UPDATE_DELIVERY_COUNT, ids::SET_SCHEDULED_DELIVERY_TIME],
    )?;
    println!(
        "Compactation succeeded for {}",
        absolute(&d.journal).display()
    );
    let mut bindings = JournalConfig::bindings(&d.bindings);
    bindings.min_files = 2;
    bindings.pool_size = 2;
    compact(bindings, &[])?;
    println!(
        "Compactation succeeded for {}",
        absolute(&d.bindings).display()
    );
    Ok(())
}

/// `compactJournal`: load, compact, stop.
fn compact(mut config: JournalConfig, replaceable: &[u8]) -> anyhow::Result<()> {
    config.remove_extra_files_on_load = true;
    let dir = config.directory.clone();
    let journal = Journal::open(config).with_context(|| format!("opening {}", dir.display()))?;
    for &r in replaceable {
        journal.replaceable_record(r);
    }
    journal
        .load_all()
        .with_context(|| format!("loading {}", dir.display()))?;
    journal
        .compact()
        .with_context(|| format!("compacting {}", dir.display()))?;
    journal.stop()?;
    Ok(())
}

fn absolute(p: &Path) -> std::path::PathBuf {
    std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf())
}
