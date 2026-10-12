//! `saranga data encode` and `data decode` (`EncodeJournal`,
//! `DecodeJournal`): a journal's records as text, one per line, and a new
//! journal written from that text.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use clap::Args;
use saranga_journal::format::{self, FileHeader, Record};
use saranga_journal::{Journal, JournalConfig};
use saranga_storage::java;

use super::dirs::{CliLock, DataDirArgs};

#[derive(Args)]
pub struct JournalArgs {
    /// The journal folder. Default: read 'journal-directory' from
    /// broker.xml.
    #[arg(long)]
    directory: Option<PathBuf>,
    /// The journal prefix.
    #[arg(long, default_value = "activemq-data")]
    prefix: String,
    /// The journal suffix.
    #[arg(long, default_value = "amq")]
    suffix: String,
    /// The journal size.
    #[arg(long = "file-size", default_value_t = 10_485_760)]
    size: usize,
    #[command(flatten)]
    dirs: DataDirArgs,
}

#[derive(Args)]
pub struct DecodeArgs {
    #[command(flatten)]
    journal: JournalArgs,
    /// The input file name.
    #[arg(long, required = true)]
    input: PathBuf,
}

impl JournalArgs {
    /// The directory given, else broker.xml's journal directory, with the
    /// CLI lock of the instance.
    fn directory(&self) -> anyhow::Result<(PathBuf, CliLock)> {
        let instance = crate::client::instance_dir();
        let lock = CliLock::take(instance.as_deref())?;
        let dir = match &self.directory {
            Some(d) => d.clone(),
            None => self.dirs.unchecked()?.journal,
        };
        Ok((dir, lock))
    }
}

/// `encode`.
pub fn encode(args: &JournalArgs) -> anyhow::Result<()> {
    let (dir, _lock) = args.directory()?;
    let stdout = std::io::stdout();
    let mut out = std::io::BufWriter::new(stdout.lock());
    for (file_id, name, bytes) in order_files(&dir, &args.prefix, &args.suffix)? {
        let rid = format::record_id_of(file_id);
        // JournalFileImpl.toString
        writeln!(
            out,
            "#File,JournalFileImpl: ({name} id = {file_id}, recordID = {rid})"
        )?;
        let Ok(header) = FileHeader::decode(&bytes, 0) else {
            continue;
        };
        let Some(scan) = format::scan(&bytes, rid, header.journal_version, false) else {
            continue;
        };
        for r in scan.records {
            let cc = r.compact_count;
            let describe = |id: i64, user_type: u8, data: &[u8], is_update: bool| {
                format!(
                    "id@{id},userRecordType@{},length@{},isUpdate@{is_update},compactCount@{cc},data@{}",
                    user_type as i8,
                    data.len(),
                    java::base64_url(data)
                )
            };
            match r.record {
                Record::UpdateTx {
                    tx,
                    id,
                    user_type,
                    data,
                } => writeln!(
                    out,
                    "operation@UpdateTX,txID@{tx},{}",
                    describe(id, user_type, &data, true)
                )?,
                Record::Update {
                    id,
                    user_type,
                    data,
                } => writeln!(
                    out,
                    "operation@Update,{}",
                    describe(id, user_type, &data, true)
                )?,
                Record::Rollback { tx } => writeln!(out, "operation@Rollback,txID@{tx}")?,
                Record::Prepare {
                    tx,
                    num_records,
                    data,
                } => writeln!(
                    out,
                    "operation@Prepare,txID@{tx},numberOfRecords@{num_records},extraData@{}",
                    java::base64_url(&data)
                )?,
                Record::DeleteTx { tx, id, data } => writeln!(
                    out,
                    "operation@DeleteRecordTX,txID@{tx},{}",
                    describe(id, 0, &data, true)
                )?,
                Record::Delete { id } => writeln!(out, "operation@DeleteRecord,id@{id}")?,
                Record::Commit { tx, num_records } => writeln!(
                    out,
                    "operation@Commit,txID@{tx},numberOfRecords@{num_records}"
                )?,
                Record::AddTx {
                    tx,
                    id,
                    user_type,
                    data,
                } => writeln!(
                    out,
                    "operation@AddRecordTX,txID@{tx},{}",
                    describe(id, user_type, &data, false)
                )?,
                Record::Add {
                    id,
                    user_type,
                    data,
                } => writeln!(
                    out,
                    "operation@AddRecord,{}",
                    describe(id, user_type, &data, false)
                )?,
                // JournalReaderCallback has no event callback.
                Record::Event { .. } => {}
            }
        }
    }
    out.flush()?;
    Ok(())
}

/// `orderFiles`: the journal's files with at least a header, by file ID.
fn order_files(
    dir: &Path,
    prefix: &str,
    suffix: &str,
) -> anyhow::Result<Vec<(i64, String, Vec<u8>)>> {
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(files);
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if !name.starts_with(prefix) || !name.ends_with(&format!(".{suffix}")) {
            continue;
        }
        let bytes = std::fs::read(e.path()).with_context(|| format!("reading {name}"))?;
        if bytes.len() < format::SIZE_HEADER {
            continue;
        }
        let file_id = i64::from_be_bytes(bytes[8..16].try_into().unwrap_or_default());
        files.push((file_id, name, bytes));
    }
    files.sort_by_key(|f| f.0);
    Ok(files)
}

/// `decode`: a brand new journal from `encode`'s text. A transaction whose
/// commit or prepare counts more records than came before it is left out.
pub fn decode(args: &DecodeArgs) -> anyhow::Result<()> {
    let a = &args.journal;
    let (dir, _lock) = a.directory()?;
    if !dir.exists() && std::fs::create_dir_all(&dir).is_err() {
        eprintln!("Could not create directory {}", dir.display());
    }
    if !order_files(&dir, &a.prefix, &a.suffix)?.is_empty() {
        bail!("Import needs to create a brand new journal");
    }
    let mut config = JournalConfig::messages(&dir);
    config.file_prefix = a.prefix.clone();
    config.file_extension = a.suffix.clone();
    config.file_size = a.size;
    config.min_files = 2;
    config.pool_size = 2;
    config.compact_min_files = 0;
    config.compact_percentage = 0;
    let journal = Journal::open(config).with_context(|| format!("opening {}", dir.display()))?;
    journal.load_all()?;
    let input = std::fs::File::open(&args.input)
        .map_err(|e| anyhow::anyhow!(super::dirs::file_not_found(&args.input, &e)))?;
    let mut counters: HashMap<i64, i32> = HashMap::new();
    for (n, line) in std::io::BufReader::new(input).lines().enumerate() {
        let line_number = n + 1;
        let line = line?;
        let split: Vec<&str> = line.split(',').collect();
        if split.first() == Some(&"#File") {
            counters.clear();
            continue;
        }
        let p = parse_line(&split);
        let operation = p.get("operation").cloned();
        let result = (|| -> anyhow::Result<()> {
            let op = operation.as_deref().context("no operation")?;
            match op {
                "AddRecord" => {
                    let (id, t, data) = record(&p)?;
                    journal.append_add(id, t, data, false, None)?;
                }
                "AddRecordTX" => {
                    let tx = long("txID", &p)?;
                    *counters.entry(tx).or_default() += 1;
                    let (id, t, data) = record(&p)?;
                    journal.append_add_tx(tx, id, t, data)?;
                }
                "UpdateTX" => {
                    let tx = long("txID", &p)?;
                    *counters.entry(tx).or_default() += 1;
                    let (id, t, data) = record(&p)?;
                    journal.append_update_tx(tx, id, t, data)?;
                }
                "Update" => {
                    let (id, t, data) = record(&p)?;
                    journal.append_update(id, t, data, false, false, None)?;
                }
                "DeleteRecord" => {
                    // Not found: the record was reclaimed already.
                    let _ = journal.append_delete(long("id", &p)?, false, None);
                }
                "DeleteRecordTX" => {
                    let tx = long("txID", &p)?;
                    let id = long("id", &p)?;
                    *counters.entry(tx).or_default() += 1;
                    journal.append_delete_tx(tx, id, Vec::new())?;
                }
                "Prepare" | "Commit" => {
                    let tx = long("txID", &p)?;
                    let expected: i32 = string("numberOfRecords", &p)?.parse()?;
                    let had = *counters.entry(tx).or_default();
                    if had != expected {
                        eprintln!(
                            "Transaction {tx} at line {line_number} is incomplete. The {} record expected {expected} while the import only had {had}",
                            op.to_lowercase()
                        );
                    } else if op == "Prepare" {
                        journal.append_prepare(tx, encoded("extraData", &p)?, false, None)?;
                    } else {
                        journal.append_commit(tx, false, None)?;
                    }
                }
                "Rollback" => journal.append_rollback(long("txID", &p)?, false, None)?,
                other => eprintln!("Invalid operation {other} at line {line_number}"),
            }
            Ok(())
        })();
        if let Err(e) = result {
            eprintln!(
                "Error at line {line_number}, operation={} msg = {e}",
                operation.as_deref().unwrap_or("null")
            );
        }
    }
    journal.stop()?;
    Ok(())
}

/// `parseLine`: `name@value` pairs; a bare word is its own value.
fn parse_line<'a>(split: &[&'a str]) -> HashMap<&'a str, String> {
    split
        .iter()
        .map(|el| {
            // String.split drops trailing empty strings: "data@" is "data".
            let tuple: Vec<&str> = el.split('@').collect();
            let tuple: Vec<&str> = match tuple.iter().rposition(|t| !t.is_empty()) {
                Some(last) => tuple[..=last].to_vec(),
                None => Vec::new(),
            };
            match tuple.as_slice() {
                [k, v] => (*k, (*v).to_owned()),
                _ => (*el, (*el).to_owned()),
            }
        })
        .collect()
}

fn string<'a>(name: &str, p: &'a HashMap<&str, String>) -> anyhow::Result<&'a str> {
    p.get(name)
        .map(String::as_str)
        .with_context(|| format!("property {name} not found"))
}

fn long(name: &str, p: &HashMap<&str, String>) -> anyhow::Result<i64> {
    Ok(string(name, p)?.parse()?)
}

fn encoded(name: &str, p: &HashMap<&str, String>) -> anyhow::Result<Vec<u8>> {
    java::base64_url_decode(string(name, p)?).map_err(anyhow::Error::msg)
}

/// `parseRecord`: ID, user record type and data.
fn record(p: &HashMap<&str, String>) -> anyhow::Result<(i64, u8, Vec<u8>)> {
    let id = long("id", p)?;
    let user_type = string("userRecordType", p)?.parse::<i8>()? as u8;
    Ok((id, user_type, encoded("data", p)?))
}
