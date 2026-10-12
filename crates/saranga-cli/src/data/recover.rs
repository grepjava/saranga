//! `saranga data recover` (`RecoverMessages`): every message record still
//! in the journal's files, deleted, acknowledged or rolled back or not,
//! copied to a new journal; large message bodies kept as events are
//! written back to their files.

use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::Args;
use saranga_journal::format::{self, FileHeader, Record};
use saranga_journal::{Journal, JournalConfig};
use saranga_storage::ids;

use super::dirs::{CliLock, DataDirArgs};

#[derive(Args)]
pub struct RecoverArgs {
    #[command(flatten)]
    dirs: DataDirArgs,
    /// Try to recover as many records as possible from reclaimed files.
    #[arg(long)]
    reclaimed: bool,
    /// Output folder container the new journal with all the generated
    /// messages.
    #[arg(long, required = true)]
    target: PathBuf,
}

/// The records `recover` copies.
const OF_INTEREST: [u8; 5] = [
    ids::ADD_LARGE_MESSAGE,
    ids::ADD_MESSAGE,
    ids::ADD_MESSAGE_PROTOCOL,
    ids::ADD_REF,
    ids::PAGE_TRANSACTION,
];

pub fn run(args: &RecoverArgs) -> anyhow::Result<()> {
    let _lock = CliLock::take(crate::client::instance_dir().as_deref())?;
    let d = super::parameter_dirs(&args.dirs)?;
    let target = &args.target;
    if !target.exists() {
        std::fs::create_dir_all(target)
            .map_err(|_| anyhow::anyhow!("It was not possible to create {}", target.display()))?;
    }
    if !target.is_dir() {
        bail!("{} is not a directory", target.display());
    }
    let file_size = d
        .config
        .as_ref()
        .and_then(|c| c.journal.file_size)
        .map_or(10 * 1024 * 1024, |n| n.max(0) as usize);
    let mut config = JournalConfig::messages(target);
    config.file_size = file_size;
    config.min_files = 2;
    config.pool_size = 2;
    config.compact_min_files = 0;
    config.compact_percentage = 0;
    config.datasync = false;
    let out = Journal::open(config).with_context(|| format!("opening {}", target.display()))?;
    out.set_auto_reclaim(false);
    out.load_all()?;

    let stdout = std::io::stdout();
    let mut print = stdout.lock();
    let mut added: HashSet<i64> = HashSet::new();
    let mut refs: HashSet<(i64, i64)> = HashSet::new();
    // The large message whose body is being written, and its file.
    let mut large: Option<(i64, std::fs::File)> = None;
    let mut files: Vec<(i64, String, Vec<u8>)> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&d.journal) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if !name.starts_with("activemq-data") || !name.ends_with(".amq") {
                continue;
            }
            let bytes = std::fs::read(e.path())?;
            if bytes.len() < format::SIZE_HEADER {
                continue;
            }
            let id = i64::from_be_bytes(bytes[8..16].try_into().unwrap_or_default());
            files.push((id, name, bytes));
        }
    }
    files.sort_by_key(|f| f.0);
    for (file_id, name, bytes) in files {
        let rid = format::record_id_of(file_id);
        writeln!(
            print,
            "Recovering messages from file JournalFileImpl: ({name} id = {file_id}, recordID = {rid})"
        )?;
        let Ok(header) = FileHeader::decode(&bytes, 0) else {
            continue;
        };
        let Some(scan) = format::scan(&bytes, rid, header.journal_version, args.reclaimed) else {
            continue;
        };
        let mut add =
            |id: i64, user_type: u8, data: Vec<u8>, print: &mut dyn Write| -> anyhow::Result<()> {
                if !OF_INTEREST.contains(&user_type) {
                    return Ok(());
                }
                if !added.insert(id) {
                    writeln!(print, "RecordID {id} would been duplicated, ignoring it")?;
                    return Ok(());
                }
                if let Err(e) = out.append_add(id, user_type, data, false, None) {
                    writeln!(print, "Cannot append record for {id}->{e}")?;
                }
                Ok(())
            };
        let mut update = |id: i64,
                          user_type: u8,
                          data: Vec<u8>,
                          print: &mut dyn Write|
         -> anyhow::Result<()> {
            if !OF_INTEREST.contains(&user_type) {
                return Ok(());
            }
            if user_type == ids::ADD_REF {
                let queue = data
                    .get(..8)
                    .map_or(0, |b| i64::from_be_bytes(b.try_into().unwrap_or_default()));
                if !refs.insert((id, queue)) {
                    writeln!(
                        print,
                        "AddReference on {id} / queue={queue} has already been recorded, ignoring it"
                    )?;
                    return Ok(());
                }
            }
            match out.append_update(id, user_type, data, true, false, None) {
                Ok(true) => {}
                Ok(false) => writeln!(
                    print,
                    "Cannot update record {id}-> Cannot find add info {id} on compactor or current records"
                )?,
                Err(e) => writeln!(print, "Cannot update record {id}-> {e}")?,
            }
            Ok(())
        };
        for r in scan.records {
            match r.record {
                Record::Event {
                    id,
                    user_type,
                    data,
                } => match user_type {
                    ids::ADD_REF => update(id, user_type, data, &mut print)?,
                    ids::ADD_MESSAGE_BODY => {
                        if large.as_ref().is_none_or(|(l, _)| *l != id) {
                            let path = d.large_messages.join(format!("{id}.msg"));
                            let f = std::fs::OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open(&path)
                                .with_context(|| format!("opening {}", path.display()))?;
                            large = Some((id, f));
                        }
                        if let Some((_, f)) = &mut large {
                            f.write_all(&data)?;
                        }
                    }
                    _ => add(id, user_type, data, &mut print)?,
                },
                Record::Add {
                    id,
                    user_type,
                    data,
                }
                | Record::AddTx {
                    id,
                    user_type,
                    data,
                    ..
                } => add(id, user_type, data, &mut print)?,
                Record::Update {
                    id,
                    user_type,
                    data,
                }
                | Record::UpdateTx {
                    id,
                    user_type,
                    data,
                    ..
                } => update(id, user_type, data, &mut print)?,
                _ => {}
            }
        }
        // done(): the file's large message body is closed.
        large = None;
    }
    out.sync()?;
    out.stop()?;
    Ok(())
}
