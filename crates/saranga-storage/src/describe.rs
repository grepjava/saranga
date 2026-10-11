//! `artemis data print`: a port of `DescribeJournal` and the CLI's
//! `PrintData`, producing the same text.
//!
//! Differences from the Java tool:
//! - The banner names Saranga instead of the Artemis ASCII art.
//! - Dates print in UTC (`Date.toString()` uses the JVM's default zone; the
//!   golden outputs were made in UTC).
//! - Java identity hash codes (`CoreMessage[...]@<n>`, `XidImpl (<n> ...`)
//!   have no meaning; Saranga prints `Arrays.hashCode` of the record body.
//! - Lines end with `\n` on every platform.
//! - Not rendered like Java yet: AMQP messages (their AMQP sections are not
//!   decoded), the `setting=` part of persisted address settings (Saranga
//!   does not model `AddressSettings` yet) and the paging section (page files
//!   are Phase 5). Each prints a clear placeholder instead.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::io::{self, Write};
use std::path::Path;

use saranga_buffer::{PropertyValue, SimpleString, TypedProperties};
use saranga_journal::format::{self, FileHeader, Record};
use saranga_journal::{
    Journal, JournalConfig, LoaderCallback, PreparedTransactionInfo, RecordInfo,
};
use saranga_message::CoreMessage;

use crate::codec::*;
use crate::ids;
use crate::java;
use crate::message::{LargeMessageRecord, PersistedMessage};

const BINDINGS_BANNER: &str = "B I N D I N G S  J O U R N A L";
const MESSAGES_BANNER: &str = "M E S S A G E S   J O U R N A L";

/// `PrintData`'s options.
#[derive(Debug, Clone, Default)]
pub struct PrintDataOptions {
    /// `--safe`: print the data structure without user data.
    pub safe: bool,
    /// `--reclaimed`: also print records of reclaimed files.
    pub reclaimed: bool,
    /// `--skip-bindings`.
    pub skip_bindings: bool,
    /// `--skip-journal`.
    pub skip_journal: bool,
    /// `--max-pages`: `None` is unlimited (-1); `Some(0)` skips paging.
    pub max_pages: Option<u32>,
    /// Prints the page files (`saranga_paging::print::print_pages`); without
    /// it each store folder is only listed.
    pub print_pages: Option<PagePrinter>,
}

/// `PrintData.printPages`: (out, paging directory, messages journal,
/// bindings journal, safe, max pages).
pub type PagePrinter = fn(
    &mut dyn Write,
    &Path,
    &JournalDescription,
    Option<&JournalDescription>,
    bool,
    Option<u32>,
) -> io::Result<()>;

/// What `DescribeJournal` returns: the surviving records of a journal.
#[derive(Debug, Clone, Default)]
pub struct JournalDescription {
    pub records: Vec<RecordInfo>,
    pub prepared: Vec<PreparedTransactionInfo>,
    /// The queue bindings among the records (bindings journal only):
    /// `(queue ID, binding)`. `None` when there are none.
    pub queue_bindings: Option<Vec<(i64, PersistentQueueBindingEncoding)>>,
}

/// Which journal to describe, with the `JournalImpl` settings
/// `DescribeJournal` uses for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Journals {
    Bindings,
    Messages,
}

impl Journals {
    fn config(self, dir: &Path) -> JournalConfig {
        let mut cfg = match self {
            // new JournalImpl(1024 * 1024, 2, 2, -1, 0, ..., "activemq-bindings", "bindings", 1)
            Journals::Bindings => {
                let mut c = JournalConfig::bindings(dir);
                c.pool_size = 2;
                c
            }
            // new JournalImpl(journalFileSize, journalMinFiles, journalPoolFiles, 0, 0, ..., "activemq-data", "amq", 1)
            Journals::Messages => JournalConfig::messages(dir),
        };
        cfg.compact_min_files = 0;
        cfg.compact_percentage = 0;
        cfg.remove_extra_files_on_load = false;
        cfg
    }
}

/// `PrintData.printData`: the bindings journal, the message journal and the
/// paging directory.
pub fn print_data(
    out: &mut dyn Write,
    bindings: &Path,
    journal: &Path,
    paging: &Path,
    opts: &PrintDataOptions,
) -> io::Result<()> {
    print_banner_header(out)?;

    let lock = journal.join("server.lock");
    if lock.is_file() {
        // FileLockNodeManager: the node ID is the 16 bytes after the 3-byte
        // state at the start of server.lock.
        let bytes = std::fs::read(&lock)?;
        if bytes.len() >= 19 {
            let id: [u8; 16] = bytes[3..19].try_into().expect("16 bytes");
            print_banner(out, &format!("Server's ID={}", java::uuid(&id)))?;
        }
    }

    print_banner(out, BINDINGS_BANNER)?;
    let bindings_describe = if opts.skip_bindings {
        writeln!(out, ".... skipping")?;
        writeln!(out)?;
        None
    } else {
        Some(describe_journal(
            out,
            Journals::Bindings,
            bindings,
            opts.safe,
            true,
            true,
            opts.reclaimed,
        )?)
    };

    print_banner(out, MESSAGES_BANNER)?;
    if opts.skip_journal {
        writeln!(out, ".... skipping")?;
        writeln!(out)?;
    }
    let messages = describe_journal(
        out,
        Journals::Messages,
        journal,
        opts.safe,
        !opts.skip_journal,
        !opts.skip_journal,
        opts.reclaimed,
    )?;

    print_banner(out, "P A G I N G")?;
    if opts.max_pages == Some(0) {
        writeln!(out, ".... skipping")?;
        writeln!(out)?;
    } else {
        match opts.print_pages {
            Some(print) => print(
                out,
                paging,
                &messages,
                bindings_describe.as_ref(),
                opts.safe,
                opts.max_pages,
            )?,
            None => print_pages(out, paging, &messages, bindings_describe.as_ref())?,
        }
    }
    Ok(())
}

/// `Artemis.printBanner`, with Saranga's name.
fn print_banner_header(out: &mut dyn Write) -> io::Result<()> {
    writeln!(out, " Saranga {}", env!("CARGO_PKG_VERSION"))?;
    writeln!(out)?;
    writeln!(out)
}

/// `PrintData.printBanner`.
fn print_banner(out: &mut dyn Write, text: &str) -> io::Result<()> {
    writeln!(out)?;
    writeln!(out, "********************************************")?;
    writeln!(out, "{text}")?;
    writeln!(out, "********************************************")
}

/// Without a page printer: each store folder is listed with a placeholder.
fn print_pages(
    out: &mut dyn Write,
    paging: &Path,
    _messages: &JournalDescription,
    _bindings: Option<&JournalDescription>,
) -> io::Result<()> {
    let Ok(entries) = std::fs::read_dir(paging) else {
        return Ok(());
    };
    let mut stores: Vec<String> = entries
        .flatten()
        .filter(|e| e.path().join("address.txt").is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    stores.sort();
    for store in stores {
        writeln!(
            out,
            "####################################################################################################"
        )?;
        writeln!(
            out,
            "Exploring store folder {store}: page files are not printed by saranga data print yet"
        )?;
    }
    Ok(())
}

/// `describeBindingsJournal` / `describeMessagesJournal`: every record of
/// every file in file order (when `print_records`), then the records that
/// survive a load (when `print_surviving`).
pub fn describe_journal(
    out: &mut dyn Write,
    which: Journals,
    dir: &Path,
    safe: bool,
    print_records: bool,
    print_surviving: bool,
    reclaimed: bool,
) -> io::Result<JournalDescription> {
    let cfg = which.config(dir);
    let mut sink = io::sink();
    let records_out: &mut dyn Write = if print_records { out } else { &mut sink };
    writeln!(records_out, "Journal path: {}", dir.display())?;

    let mut counters = Counters::default();
    for (file_id, name, bytes) in order_files(dir, &cfg.file_extension)? {
        writeln!(
            records_out,
            "#JournalFileImpl: ({name} id = {file_id}, recordID = {}) (size={})",
            format::record_id_of(file_id),
            bytes.len()
        )?;
        let header = FileHeader::decode(&bytes, cfg.user_version).map_err(io::Error::other)?;
        let rid = format::record_id_of(file_id);
        let Some(scan) = format::scan(&bytes, rid, header.journal_version, reclaimed) else {
            continue;
        };
        for r in scan.records {
            let cc = r.compact_count;
            let info = |id: i64, user_type: u8, data: Vec<u8>, is_update: bool| RecordInfo {
                id,
                user_record_type: user_type,
                data,
                is_update,
                replaceable_update: false,
                compact_count: cc,
            };
            match r.record {
                Record::Event {
                    id,
                    user_type,
                    data,
                } => {
                    let i = info(id, user_type, data, false);
                    writeln!(records_out, "operation@Event;{}", describe_record(&i, safe))?;
                }
                Record::UpdateTx {
                    tx,
                    id,
                    user_type,
                    data,
                } => {
                    let i = info(id, user_type, data, true);
                    writeln!(
                        records_out,
                        "operation@UpdateTX;txID={tx},{}",
                        describe_record(&i, safe)
                    )?;
                    counters.check_record_counter(records_out, &i)?;
                }
                Record::Update {
                    id,
                    user_type,
                    data,
                } => {
                    let i = info(id, user_type, data, true);
                    writeln!(
                        records_out,
                        "operation@Update;{}",
                        describe_record(&i, safe)
                    )?;
                    counters.check_record_counter(records_out, &i)?;
                }
                Record::Rollback { tx } => writeln!(records_out, "operation@Rollback;txID={tx}")?,
                Record::Prepare {
                    tx,
                    num_records,
                    data,
                } => writeln!(
                    records_out,
                    "operation@Prepare,txID={tx},numberOfRecords={num_records},extraData={}, xid={}",
                    java::base64_url(&data),
                    to_xid(&data)
                )?,
                Record::DeleteTx { tx, id, data } => {
                    let i = info(id, 0, data, true);
                    writeln!(
                        records_out,
                        "operation@DeleteRecordTX;txID={tx},{}",
                        describe_record(&i, safe)
                    )?;
                }
                Record::Delete { id } => {
                    writeln!(records_out, "operation@DeleteRecord;recordID={id}")?
                }
                Record::Commit { tx, num_records } => writeln!(
                    records_out,
                    "operation@Commit;txID={tx},numberOfRecords={num_records}"
                )?,
                Record::AddTx {
                    tx,
                    id,
                    user_type,
                    data,
                } => {
                    let i = info(id, user_type, data, false);
                    writeln!(
                        records_out,
                        "operation@AddRecordTX;txID={tx},{}",
                        describe_record(&i, safe)
                    )?;
                }
                Record::Add {
                    id,
                    user_type,
                    data,
                } => {
                    let i = info(id, user_type, data, false);
                    writeln!(
                        records_out,
                        "operation@AddRecord;{}",
                        describe_record(&i, safe)
                    )?;
                }
            }
        }
    }
    writeln!(records_out)?;
    if !counters.is_empty() {
        writeln!(records_out, "#Counters during initial load:")?;
        counters.print(records_out)?;
    }

    let surviving_out: &mut dyn Write = if print_surviving { out } else { &mut sink };
    print_surviving_records(surviving_out, which, dir, safe)
}

/// `orderFiles`: the files with the journal's extension that hold at least
/// a header, ordered by file ID.
fn order_files(dir: &Path, extension: &str) -> io::Result<Vec<(i64, String, Vec<u8>)>> {
    let suffix = format!(".{extension}");
    let mut files = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Ok(files);
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if !name.ends_with(&suffix) || !e.path().is_file() {
            continue;
        }
        let bytes = std::fs::read(e.path())?;
        if bytes.len() < format::SIZE_HEADER {
            continue;
        }
        let file_id = i64::from_be_bytes(bytes[8..16].try_into().expect("8 bytes"));
        files.push((file_id, name, bytes));
    }
    files.sort_by_key(|f| f.0);
    Ok(files)
}

/// `DescribeJournal.printSurvivingRecords`.
fn print_surviving_records(
    out: &mut dyn Write,
    which: Journals,
    dir: &Path,
    safe: bool,
) -> io::Result<JournalDescription> {
    writeln!(out, "### Surviving Records Summary ###")?;

    let loaded = load(which, dir, safe)?;
    let mut counters = Counters::default();
    let mut message_count = 0u64;
    let mut large_message_count = 0u64;
    let mut ref_counts = RefCounts::default();
    let mut prepared_message_count = 0u64;
    let mut prepared_large_message_count = 0u64;
    let mut prepared_ref_counts = RefCounts::default();
    let mut bindings: Option<Vec<(i64, PersistentQueueBindingEncoding)>> = None;

    for info in &loaded.records {
        let mut counter_queue = None;
        match info.user_record_type {
            ids::ADD_MESSAGE | ids::ADD_MESSAGE_PROTOCOL => message_count += 1,
            ids::ADD_LARGE_MESSAGE => large_message_count += 1,
            ids::ADD_REF => {
                if let Ok(r) = RefEncoding::decode(&info.data) {
                    ref_counts.add(r.queue_id);
                }
            }
            ids::ACKNOWLEDGE_REF => {
                if let Ok(r) = RefEncoding::decode(&info.data) {
                    ref_counts.ack(r.queue_id);
                }
            }
            ids::PAGE_CURSOR_COUNTER_VALUE => {
                if let Ok(c) = PageCountRecord::decode(&info.data) {
                    counters.load_value(c.queue_id, c.value, c.persistent_size.unwrap_or(0));
                    counter_queue = Some(c.queue_id);
                }
            }
            ids::PAGE_CURSOR_COUNTER_INC => {
                if let Ok(c) = PageCountRecordInc::decode(&info.data) {
                    counters.load_inc(c.queue_id);
                    counter_queue = Some(c.queue_id);
                }
            }
            ids::QUEUE_BINDING_RECORD => {
                if let Ok(b) = PersistentQueueBindingEncoding::decode(&info.data) {
                    bindings.get_or_insert_with(Vec::new).push((info.id, b));
                }
            }
            _ => {}
        }
        writeln!(out, "{}", describe_record(info, safe))?;
        if let Some(q) = counter_queue {
            writeln!(
                out,
                "##SubsCounter for queue={q}, value={}",
                counters.value(q)
            )?;
            writeln!(out)?;
        }
    }

    if !counters.is_empty() {
        writeln!(out, "### Page Counters")?;
        counters.print(out)?;
    }

    writeln!(out)?;
    writeln!(out, "### Prepared TX ###")?;
    for tx in &loaded.prepared {
        writeln!(out, "{}", tx.id)?;
        for info in &tx.records {
            writeln!(out, "- {}", describe_record(info, safe))?;
            match info.user_record_type {
                ids::ADD_MESSAGE | ids::ADD_MESSAGE_PROTOCOL => prepared_message_count += 1,
                ids::ADD_LARGE_MESSAGE => prepared_large_message_count += 1,
                ids::ADD_REF => {
                    if let Ok(r) = RefEncoding::decode(&info.data) {
                        prepared_ref_counts.add(r.queue_id);
                    }
                }
                _ => {}
            }
        }
        for info in &tx.records_to_delete {
            writeln!(out, "- {} <marked to delete>", describe_record(info, safe))?;
        }
    }

    if !loaded.failed.is_empty() {
        writeln!(out)?;
        writeln!(
            out,
            "### Failed Transactions (Missing commit/prepare/rollback record) ###"
        )?;
    }
    writeln!(out, "{}", loaded.failed)?;

    writeln!(out, "### Message Counts ###")?;
    writeln!(out, "message count={message_count}")?;
    writeln!(out, "large message count={large_message_count}")?;
    writeln!(out, "message reference count")?;
    ref_counts.print(out)?;
    writeln!(out, "prepared message count={prepared_message_count}")?;
    writeln!(
        out,
        "prepared large message count={prepared_large_message_count}"
    )?;
    writeln!(out, "prepared message reference count")?;
    prepared_ref_counts.print(out)?;

    Ok(JournalDescription {
        records: loaded.records,
        prepared: loaded.prepared,
        queue_bindings: bindings,
    })
}

struct Loaded {
    records: Vec<RecordInfo>,
    prepared: Vec<PreparedTransactionInfo>,
    /// The failed-transactions text, as `bufferFailingTransactions` builds it.
    failed: String,
}

/// `journal.start(); journal.load(records, preparedTransactions,
/// failureCallback, false)`.
fn load(which: Journals, dir: &Path, safe: bool) -> io::Result<Loaded> {
    struct Collect {
        safe: bool,
        records: Vec<RecordInfo>,
        deleted: HashSet<i64>,
        prepared: Vec<PreparedTransactionInfo>,
        failed: String,
    }
    impl LoaderCallback for Collect {
        fn add_record(&mut self, info: RecordInfo) {
            self.records.push(info);
        }
        fn update_record(&mut self, info: RecordInfo) {
            self.records.push(info);
        }
        fn delete_record(&mut self, id: i64) {
            self.deleted.insert(id);
        }
        fn add_prepared_transaction(&mut self, tx: PreparedTransactionInfo) {
            self.prepared.push(tx);
        }
        fn failed_transaction(
            &mut self,
            tx: i64,
            records: Vec<RecordInfo>,
            to_delete: Vec<RecordInfo>,
        ) {
            let _ = writeln!(self.failed, "Transaction {tx} failed with these records:");
            for info in &records {
                let _ = writeln!(self.failed, "- {}", describe_record(info, self.safe));
            }
            for info in &to_delete {
                let _ = writeln!(
                    self.failed,
                    "- {} <marked to delete>",
                    describe_record(info, self.safe)
                );
            }
        }
    }
    let mut c = Collect {
        safe,
        records: Vec::new(),
        deleted: HashSet::new(),
        prepared: Vec::new(),
        failed: String::new(),
    };
    if dir.is_dir() {
        let journal = Journal::open(which.config(dir)).map_err(io::Error::other)?;
        let res = journal.load(&mut c).map_err(io::Error::other);
        journal.stop().map_err(io::Error::other)?;
        res?;
    }
    let deleted = c.deleted;
    c.records.retain(|r| !deleted.contains(&r.id));
    Ok(Loaded {
        records: c.records,
        prepared: c.prepared,
        failed: c.failed,
    })
}

/// `PageSubscriptionCounterImpl` as `DescribeJournal` uses it: without a
/// storage manager, `loadInc` + `processReload` changes nothing (the reload
/// fails and only logs), so only `loadValue` sets the value.
#[derive(Default)]
struct Counters {
    /// Queue IDs in insertion order, and (value, persistent size).
    order: Vec<i64>,
    values: HashMap<i64, (i64, i64)>,
}

impl Counters {
    fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    fn lookup(&mut self, q: i64) -> &mut (i64, i64) {
        if !self.values.contains_key(&q) {
            self.order.push(q);
        }
        self.values.entry(q).or_insert((0, 0))
    }

    fn load_value(&mut self, q: i64, value: i64, size: i64) {
        *self.lookup(q) = (value, size);
    }

    fn load_inc(&mut self, q: i64) {
        self.lookup(q);
    }

    fn value(&self, q: i64) -> i64 {
        self.values.get(&q).map_or(0, |v| v.0)
    }

    /// `printCounters`.
    fn print(&self, out: &mut dyn Write) -> io::Result<()> {
        for q in java::long_map_order(&self.order) {
            writeln!(out, "Queue {q} value={}", self.value(q))?;
        }
        Ok(())
    }

    /// `checkRecordCounter`, for update records read from the files.
    fn check_record_counter(&mut self, out: &mut dyn Write, info: &RecordInfo) -> io::Result<()> {
        let (q, tail) = match info.user_record_type {
            ids::PAGE_CURSOR_COUNTER_VALUE => {
                let Ok(c) = PageCountRecord::decode(&info.data) else {
                    return Ok(());
                };
                let old = self.value(c.queue_id);
                if old != 0 && old != c.value {
                    writeln!(
                        out,
                        "####### Counter replace wrongly on queue {} oldValue={old} newValue={}",
                        c.queue_id, c.value
                    )?;
                }
                self.load_value(c.queue_id, c.value, c.persistent_size.unwrap_or(0));
                (c.queue_id, format!(", result={}", self.value(c.queue_id)))
            }
            ids::PAGE_CURSOR_COUNTER_INC => {
                let Ok(c) = PageCountRecordInc::decode(&info.data) else {
                    return Ok(());
                };
                self.load_inc(c.queue_id);
                (c.queue_id, format!(" increased by {}", c.value))
            }
            _ => return Ok(()),
        };
        let (value, size) = self.values[&q];
        write!(
            out,
            "#Counter queue {q} value={value} persistentSize={size}{tail}"
        )?;
        if value < 0 {
            writeln!(out, " #NegativeCounter!!!!")?;
        } else {
            writeln!(out)?;
        }
        writeln!(out)
    }
}

/// The per-queue message reference counts (`Map<Long, Count>`).
#[derive(Default)]
struct RefCounts {
    order: Vec<i64>,
    counts: HashMap<i64, i64>,
}

impl RefCounts {
    fn add(&mut self, q: i64) {
        if !self.counts.contains_key(&q) {
            self.order.push(q);
        }
        *self.counts.entry(q).or_insert(0) += 1;
    }

    /// An ack of a queue never seen creates its count at 0 without
    /// decrementing it.
    fn ack(&mut self, q: i64) {
        match self.counts.get_mut(&q) {
            Some(c) => *c -= 1,
            None => {
                self.order.push(q);
                self.counts.insert(q, 0);
            }
        }
    }

    fn print(&self, out: &mut dyn Write) -> io::Result<()> {
        for q in java::long_map_order(&self.order) {
            writeln!(out, "queue id {q},count={}", self.counts[&q])?;
        }
        Ok(())
    }
}

/// `DescribeJournal.describeRecord`.
pub fn describe_record(info: &RecordInfo, safe: bool) -> String {
    format!(
        "recordID={};userRecordType={};isUpdate={};compactCount={};{}",
        info.id,
        info.user_record_type as i8,
        info.is_update,
        info.compact_count,
        describe_body(
            info.user_record_type,
            info.id,
            info.is_update,
            &info.data,
            safe
        )
    )
}

/// `toString(newObjectEncoding(info), safe)`: the text for a record body.
pub fn describe_body(user_type: u8, id: i64, is_update: bool, data: &[u8], safe: bool) -> String {
    match describe_object(user_type, id, is_update, data) {
        Ok(Some(d)) => d.render(safe),
        Ok(None) => "** null **".into(),
        // Java would throw here and stop printing.
        Err(e) => format!("** failed to decode: {e} **"),
    }
}

/// A described record body. `unsafe_class` is set for the classes
/// `--safe` hides (`isSafe`).
struct Described {
    text: String,
    safe_text: Option<String>,
}

impl Described {
    fn plain(text: String) -> Self {
        Self {
            text,
            safe_text: None,
        }
    }

    fn hidden(text: String, class: &str) -> Self {
        Self {
            text,
            safe_text: Some(format!("{class}(safe data)")),
        }
    }

    fn render(self, safe: bool) -> String {
        match (safe, self.safe_text) {
            (true, Some(s)) => s,
            _ => self.text,
        }
    }
}

/// `newObjectEncoding` followed by `toString()`.
fn describe_object(
    user_type: u8,
    id: i64,
    is_update: bool,
    data: &[u8],
) -> saranga_buffer::Result<Option<Described>> {
    let plain = |s: String| Ok(Some(Described::plain(s)));
    match user_type {
        ids::DIVERT_RECORD => {
            let d = PersistedDivertConfiguration::decode(data)?;
            plain(format!(
                "PersistedDivertConfiguration{{storeId={id}, divertConfiguration={}}}",
                divert_to_string(&d)
            ))
        }
        ids::BRIDGE_RECORD => {
            let b = PersistedBridgeConfiguration::decode(data)?;
            plain(format!(
                "PersistedBridgeConfiguration{{storeId={id}, bridgeConfiguration=org.apache.activemq.artemis.core.config.BridgeConfiguration@{:x}}}",
                bridge_hash(&b)
            ))
        }
        ids::ADD_LARGE_MESSAGE_PENDING => {
            let p = PendingLargeMessageEncoding::decode(data)?;
            plain(format!(
                "PendingLargeMessageEncoding::MessageID={}",
                p.large_message_id
            ))
        }
        ids::ADD_LARGE_MESSAGE => {
            let m = LargeMessageRecord::decode(data)?.message;
            let text = format!(
                "LargeMessage(messageID={}{};msg={}",
                m.message_id,
                user_message_id(&m),
                large_message_to_string(&m, data)
            );
            Ok(Some(Described {
                text,
                safe_text: Some("LargeServerMessageImpl(safe data)".into()),
            }))
        }
        ids::ADD_MESSAGE => {
            plain("ADD-MESSAGE is not supported any longer, use export/import".into())
        }
        ids::ADD_MESSAGE_PROTOCOL => Ok(Some(describe_message(
            &PersistedMessage::decode(data)?,
            data,
        ))),
        ids::ADD_REF => plain(format!(
            "AddRef;QueueEncoding [queueID={}]",
            RefEncoding::decode(data)?.queue_id
        )),
        ids::ACKNOWLEDGE_REF => plain(format!(
            "ACK;QueueEncoding [queueID={}]",
            RefEncoding::decode(data)?.queue_id
        )),
        ids::UPDATE_DELIVERY_COUNT => {
            let d = DeliveryCountUpdateEncoding::decode(data)?;
            plain(format!(
                "DeliveryCountUpdateEncoding [queueID={}, count={}]",
                d.queue_id, d.count
            ))
        }
        ids::PAGE_TRANSACTION => {
            let p = PageTransactionEncoding::decode(data)?;
            if is_update {
                plain(format!(
                    "PageUpdateTXEncoding [pageTX={}, records={}]",
                    p.transaction_id, p.records
                ))
            } else {
                plain(format!(
                    "PageTransactionInfoImpl(transactionID={},id={id},numberOfMessages={})",
                    p.transaction_id, p.records
                ))
            }
        }
        ids::SET_SCHEDULED_DELIVERY_TIME => {
            let s = ScheduledDeliveryEncoding::decode(data)?;
            plain(format!(
                "ScheduledDeliveryEncoding [scheduledDeliveryTime={}, queueID={}]",
                s.scheduled_delivery_time, s.queue_id
            ))
        }
        ids::DUPLICATE_ID => plain(duplicate_id_to_string(&DuplicateIdEncoding::decode(data)?)),
        ids::HEURISTIC_COMPLETION => {
            let h = HeuristicCompletionEncoding::decode(data)?;
            plain(format!(
                "HeuristicCompletionEncoding [xid={}, isCommit={}]",
                xid_to_string(&h.xid),
                h.is_commit
            ))
        }
        ids::ACKNOWLEDGE_CURSOR => {
            let c = CursorAckRecordEncoding::decode(data)?;
            plain(format!(
                "CursorAckRecordEncoding [queueID={}, position={}]",
                c.queue_id,
                page_position(&c)
            ))
        }
        ids::PAGE_CURSOR_COUNTER_VALUE => {
            let c = PageCountRecord::decode(data)?;
            plain(format!(
                "PageCountRecord [queueID={}, value={}, persistentSize={}]",
                c.queue_id,
                c.value,
                c.persistent_size.unwrap_or(0)
            ))
        }
        ids::PAGE_CURSOR_COMPLETE => {
            let c = CursorAckRecordEncoding::decode(data)?;
            plain(format!(
                "PGComplete [queueID={}, position={}]",
                c.queue_id,
                page_position(&c)
            ))
        }
        ids::PAGE_CURSOR_COUNTER_INC => {
            let c = PageCountRecordInc::decode(data)?;
            plain(format!(
                "PageCountRecordInc [queueID={}, value={}, persistentSize={}]",
                c.queue_id,
                c.value,
                c.persistent_size.unwrap_or(0)
            ))
        }
        ids::PAGE_CURSOR_PENDING_COUNTER => {
            let c = PageCountPendingEncoding::decode(data)?;
            plain(format!(
                "PageCountPendingImpl [queueID={}, pageID={}]",
                c.queue_id, c.page_id
            ))
        }
        ids::QUEUE_STATUS_RECORD => {
            let s = QueueStatusEncoding::decode(data)?;
            plain(format!(
                "QueueStatusEncoding [id={id}, queueID={}, status={}]",
                s.queue_id,
                queue_status_name(s.status).unwrap_or("null")
            ))
        }
        ids::QUEUE_BINDING_RECORD => {
            let b = PersistentQueueBindingEncoding::decode(data)?;
            Ok(Some(Described::hidden(
                format!(
                    "PersistentQueueBindingEncoding [queueConfiguration={}]",
                    queue_configuration(id, &b)
                ),
                "PersistentQueueBindingEncoding",
            )))
        }
        ids::ID_COUNTER_RECORD => plain(format!(
            "IDCounterEncoding [id={}]",
            IdCounterEncoding::decode(data)?.id
        )),
        ids::GROUP_RECORD => {
            let g = GroupingEncoding::decode(data)?;
            plain(format!(
                "GroupingEncoding [id={id}, groupId={}, clusterName={}]",
                java::simple_string(&g.group_id),
                java::simple_string(&g.cluster_name)
            ))
        }
        // Artemis quirk: AbstractPersistedAddressSetting declares its own
        // `storeId` field, shadowing the one `setStoreId` sets, so both
        // address-setting records always print storeId=0.
        ids::ADDRESS_SETTING_RECORD => {
            let s = PersistedAddressSetting::decode(data)?;
            plain(format!(
                "PersistedAddressSetting [storeId=0, addressMatch={}, setting=<{} bytes of AddressSettings, not printed by saranga>]",
                java::simple_string(&s.address_match),
                s.settings.len()
            ))
        }
        ids::ADDRESS_SETTING_RECORD_JSON => {
            let s = PersistedAddressSettingJson::decode(data)?;
            plain(format!(
                "PersistedAddressSettingJSON{{jsonSetting={}, storeId=0, addressMatch={}, setting=<AddressSettings not printed by saranga>}}",
                java::simple_string(&s.json),
                java::simple_string(&s.address_match)
            ))
        }
        ids::SECURITY_SETTING_RECORD => plain(security_setting_to_string(
            id,
            &PersistedSecuritySetting::decode(data)?,
        )),
        ids::ADDRESS_BINDING_RECORD => {
            let a = PersistentAddressBindingEncoding::decode(data)?;
            Ok(Some(Described::hidden(
                address_binding_to_string(id, &a),
                "PersistentAddressBindingEncoding",
            )))
        }
        ids::ADDRESS_STATUS_RECORD => {
            let s = AddressStatusEncoding::decode(data)?;
            plain(format!(
                "AddressStatusEncoding{{status={}, id={}}}",
                queue_status_name(s.status).unwrap_or("null"),
                s.address_id
            ))
        }
        ids::USER_RECORD => {
            let u = PersistedUser::decode(data)?;
            plain(format!(
                "PersistedUser [storeId={id}, username={}, password=****]",
                java::utf16(u.username.code_units().iter().copied())
            ))
        }
        ids::ROLE_RECORD => {
            let r = PersistedRole::decode(data)?;
            let roles: Vec<String> = r
                .roles
                .iter()
                .map(|r| java::utf16(r.code_units().iter().copied()))
                .collect();
            plain(format!(
                "PersistedRole [storeId={id}, username={}, roles [{}]]",
                java::utf16(r.username.code_units().iter().copied()),
                roles.join(", ")
            ))
        }
        ids::ACK_RETRY => {
            let a = AckRetryRecord::decode(data)?;
            let node = if a.node_id.is_empty() {
                "null".to_owned()
            } else {
                // new String(bytes, US_ASCII): non-ASCII bytes decode to U+FFFD.
                a.node_id
                    .iter()
                    .map(|&b| if b < 0x80 { b as char } else { '\u{fffd}' })
                    .collect()
            };
            let reason = match a.reason {
                1 => "KILLED",
                2 => "EXPIRED",
                3 => "REPLACED",
                _ => "NORMAL",
            };
            let retry = format!(
                "AckRetry{{nodeID='{node}', messageID={}, reason={reason}, pageAttempts=0, queueAttempts=0, viewCount=0}}",
                a.message_id
            );
            plain(format!(
                "MapRecord{{collectionID={}, id={}, key={retry}, value={retry}}}",
                a.queue_id, a.id
            ))
        }
        // newObjectEncoding has no case for these (and unknown types).
        _ => Ok(None),
    }
}

/// `DescribeJournal.toXid`: `null` when the data is not an Xid.
fn to_xid(data: &[u8]) -> String {
    let mut r = saranga_buffer::Reader::new(data);
    match Xid::read(&mut r) {
        Ok(x) => xid_to_string(&x),
        Err(_) => "null".into(),
    }
}

/// `XidImpl.toString`. Java prints an identity hash code first; Saranga
/// prints `XidImpl.hashCode()` there.
fn xid_to_string(x: &Xid) -> String {
    let rep = |b: &[u8]| {
        b.iter()
            .map(|&b| (b as i8).to_string())
            .collect::<Vec<_>>()
            .join(".")
    };
    let bytes = xid_bytes(x);
    format!(
        "XidImpl ({} bq:{} formatID:{} gtxid:{} base64:{}",
        java::bytes_hash(&bytes),
        rep(&x.branch_qualifier),
        x.format_id,
        rep(&x.global_transaction_id),
        java::base64_url(&bytes)
    )
}

/// `XidImpl.toByteArray`: branch qualifier, global transaction ID, then the
/// format ID's bytes computed as `(byte) ((formatId >> i * 8) % 0xFF)`
/// (Java's signed remainder, not a mask).
fn xid_bytes(x: &Xid) -> Vec<u8> {
    let mut b = x.branch_qualifier.clone();
    b.extend_from_slice(&x.global_transaction_id);
    for i in 0..4 {
        b.push(((x.format_id >> (i * 8)) % 0xFF) as u8);
    }
    b
}

fn page_position(c: &CursorAckRecordEncoding) -> String {
    format!(
        "PagePositionImpl [pageNr={}, messageNr={}, recordID=-1]",
        c.page_nr, c.message_nr
    )
}

fn duplicate_id_to_string(d: &DuplicateIdEncoding) -> String {
    let address = java::simple_string(&d.address);
    let hex = java::bytes_to_hex(&d.duplicate_id, 2);
    if address.starts_with("BRIDGE") && d.duplicate_id.len() == 24 {
        let uuid: [u8; 16] = d.duplicate_id[..16].try_into().expect("16 bytes");
        let id = i64::from_be_bytes(d.duplicate_id[16..].try_into().expect("8 bytes"));
        format!(
            "DuplicateIDEncoding [address={address}, duplID={hex} / nodeUUID={} messageID={id}]",
            java::uuid(&uuid)
        )
    } else {
        format!("DuplicateIDEncoding [address={address}, duplID={hex}]")
    }
}

fn address_binding_to_string(id: i64, a: &PersistentAddressBindingEncoding) -> String {
    // An EnumSet iterates in declaration order: MULTICAST, ANYCAST.
    let mut types = Vec::new();
    for (b, name) in [(0u8, "MULTICAST"), (1, "ANYCAST")] {
        if a.routing_types.contains(&b) {
            types.push(name);
        }
    }
    format!(
        "PersistentAddressBindingEncoding [id={id}, name={}, routingTypes={{{}}}, autoCreated={}, internal={}]",
        java::simple_string(&a.name),
        types.join(","),
        a.auto_created,
        a.internal.unwrap_or(false)
    )
}

/// `Character.isWhitespace`.
fn java_is_whitespace(c: u16) -> bool {
    match c {
        0x09..=0x0d | 0x1c..=0x1f => true,
        0xa0 | 0x2007 | 0x202f => false,
        _ => char::from_u32(c as u32).is_some_and(|ch| ch.is_whitespace() && !matches!(c, 0x85)),
    }
}

/// `QueueConfiguration.toString()` after `PersistentQueueBindingEncoding
/// .decode`, `applyStaticDefaults` and `setId`.
fn queue_configuration(id: i64, b: &PersistentQueueBindingEncoding) -> String {
    let fqqn_split = |s: &str| {
        s.split_once("::")
            .map(|(a, q)| (a.to_owned(), q.to_owned()))
    };
    let raw_name = java::simple_string(&b.name);
    let raw_address = java::simple_string(&b.address);
    let mut fqqn = "null";
    // setName then setAddress: a fully qualified name sets the address too,
    // but setAddress always replaces it.
    let mut name = raw_name.clone();
    if let Some((_, q)) = fqqn_split(&raw_name) {
        name = q;
        fqqn = "true";
    }
    let address = match fqqn_split(&raw_address) {
        Some((a, q)) => {
            name = q;
            fqqn = "true";
            a
        }
        None => raw_address,
    };
    let filter = b
        .filter_string
        .as_ref()
        .filter(|f| !f.is_empty() && !f.code_units().all(java_is_whitespace));
    let opt_ss =
        |v: &Option<Option<SimpleString>>| java::nullable(v.as_ref().and_then(|v| v.as_ref()));
    let (max_consumers, purge, routing_type) = match b.consumers {
        Some((m, p, rt)) => (m, p, rt),
        None => (-1, false, 0),
    };
    let routing_type = if routing_type == 1 {
        "ANYCAST"
    } else {
        "MULTICAST"
    };
    let opt = |v: Option<bool>| v.map_or_else(|| "null".to_owned(), |v| v.to_string());
    format!(
        "QueueConfiguration [id={id}, name={name}, address={address}, routingType={routing_type}, filterString={}, durable=null, user={}, maxConsumers={max_consumers}, exclusive={}, groupRebalance={}, groupRebalancePauseDispatch={}, groupBuckets={}, groupFirstKey={}, lastValue={}, lastValueKey={}, nonDestructive={}, purgeOnNoConsumers={purge}, enabled={}, consumersBeforeDispatch={}, delayBeforeDispatch={}, consumerPriority=null, autoDelete={}, autoDeleteDelay={}, autoDeleteMessageCount={}, ringSize={}, configurationManaged={}, temporary=null, autoCreateAddress=null, internal={}, transient=null, autoCreated={}, fqqn={fqqn}]",
        java::nullable(filter),
        b.user().unwrap_or_else(|| "null".into()),
        b.exclusive.unwrap_or(false),
        b.group_rebalance.unwrap_or(false),
        b.group_rebalance_pause_dispatch.unwrap_or(false),
        b.group_buckets.unwrap_or(-1),
        opt_ss(&b.group_first_key),
        b.last_value.unwrap_or(false),
        opt_ss(&b.last_value_key),
        b.non_destructive.unwrap_or(false),
        b.enabled.unwrap_or(true),
        b.consumers_before_dispatch.unwrap_or(0),
        b.delay_before_dispatch.unwrap_or(-1),
        b.auto_delete.unwrap_or(b.auto_created),
        b.auto_delete_delay.unwrap_or(0),
        b.auto_delete_message_count.unwrap_or(0),
        b.ring_size.unwrap_or(-1),
        opt(b.configuration_managed),
        opt(b.internal),
        b.auto_created,
    )
}

fn security_setting_to_string(id: i64, s: &PersistedSecuritySetting) -> String {
    let n = |v: &Option<SimpleString>| java::nullable(v.as_ref());
    let nn = |v: &Option<Option<SimpleString>>| java::nullable(v.as_ref().and_then(|v| v.as_ref()));
    format!(
        "PersistedSecuritySetting [storeId={id}, addressMatch={}, sendRoles={}, consumeRoles={}, createDurableQueueRoles={}, deleteDurableQueueRoles={}, createNonDurableQueueRoles={}, deleteNonDurableQueueRoles={}, manageRoles={}, browseRoles={}, createAddressRoles={}, deleteAddressRoles={}, viewRoles={}, editRoles={}]",
        java::simple_string(&s.address_match),
        n(&s.send_roles),
        n(&s.consume_roles),
        n(&s.create_durable_queue_roles),
        n(&s.delete_durable_queue_roles),
        n(&s.create_non_durable_queue_roles),
        n(&s.delete_non_durable_queue_roles),
        n(&s.manage_roles),
        n(&s.browse_roles),
        n(&s.create_address_roles),
        n(&s.delete_address_roles),
        nn(&s.view_roles),
        nn(&s.edit_roles),
    )
}

fn jstr(s: &Option<saranga_buffer::JString>) -> String {
    s.as_ref().map_or_else(
        || "null".to_owned(),
        |s| java::utf16(s.code_units().iter().copied()),
    )
}

fn component_routing_type(b: u8) -> &'static str {
    match b {
        0 => "MULTICAST",
        1 => "ANYCAST",
        2 => "STRIP",
        3 => "PASS",
        _ => "null",
    }
}

/// `TransformerConfiguration.toString()`; the properties print as a Java
/// `HashMap<String, String>`.
fn transformer_to_string(t: &Option<TransformerConfiguration>) -> String {
    let Some(t) = t else {
        return "null".into();
    };
    let mut keys: Vec<&Option<saranga_buffer::JString>> = Vec::new();
    let mut values: HashMap<Option<Vec<u16>>, &Option<saranga_buffer::JString>> = HashMap::new();
    for (k, v) in &t.properties {
        let key = k.as_ref().map(|k| k.code_units().to_vec());
        if values.insert(key, v).is_none() {
            keys.push(k);
        }
    }
    let hashes: Vec<i32> = keys
        .iter()
        .map(|k| k.as_ref().map_or(0, |k| java::string_hash(k.code_units())))
        .collect();
    let entries: Vec<String> = java::hash_map_order(&hashes, None)
        .into_iter()
        .map(|i| {
            let k = keys[i];
            let v = values[&k.as_ref().map(|k| k.code_units().to_vec())];
            format!("{}={}", jstr(k), jstr(v))
        })
        .collect();
    format!(
        "TransformerConfiguration [className={}, properties={{{}}}]",
        java::utf16(t.class_name.code_units().iter().copied()),
        entries.join(", ")
    )
}

fn divert_to_string(d: &PersistedDivertConfiguration) -> String {
    format!(
        "DivertConfiguration [name={}, routingName={}, address={}, forwardingAddress={}, exclusive={}, filterString={}, routing-type={}, transformerConfiguration={}]",
        jstr(&d.name),
        jstr(&d.routing_name),
        jstr(&d.address),
        jstr(&d.forwarding_address),
        d.exclusive,
        jstr(&d.filter_string),
        component_routing_type(d.routing_type),
        transformer_to_string(&d.transformer)
    )
}

/// `BridgeConfiguration.hashCode()`, which `Object.toString()` prints in hex
/// (the class has no `toString`).
fn bridge_hash(b: &PersistedBridgeConfiguration) -> u32 {
    let s = |v: &Option<saranga_buffer::JString>| {
        v.as_ref().map_or(0, |s| java::string_hash(s.code_units()))
    };
    let long = |v: Option<i64>| v.map_or(0, java::long_hash);
    let int = |v: Option<i32>| v.unwrap_or(0);
    let boolean = |v: Option<bool>| match v {
        Some(true) => 1231,
        Some(false) => 1237,
        None => 0,
    };
    let double = |v: Option<f64>| v.map_or(0, |d| java::long_hash(d.to_bits() as i64));
    let connectors = if b.static_connectors.is_empty() {
        0
    } else {
        b.static_connectors
            .iter()
            .fold(1i32, |h, c| h.wrapping_mul(31).wrapping_add(s(c)))
    };
    let transformer = b.transformer.as_ref().map_or(0, |t| {
        let props = t
            .properties
            .iter()
            .fold(0i32, |h, (k, v)| h.wrapping_add(s(k) ^ s(v)));
        [java::string_hash(t.class_name.code_units()), props]
            .iter()
            .fold(1i32, |h, v| h.wrapping_mul(31).wrapping_add(*v))
    });
    let fields = [
        long(b.call_timeout),
        long(b.client_failure_check_period),
        int(b.confirmation_window_size),
        int(b.producer_window_size),
        long(b.connection_ttl),
        s(&b.discovery_group_name),
        s(&b.filter_string),
        s(&b.forwarding_address),
        boolean(b.ha),
        long(b.max_retry_interval),
        int(b.min_large_message_size),
        s(&b.name),
        s(&b.password),
        s(&b.queue_name),
        int(b.initial_connect_attempts),
        int(b.reconnect_attempts),
        long(b.retry_interval),
        double(b.retry_interval_multiplier),
        connectors,
        transformer,
        boolean(b.use_duplicate_detection),
        s(&b.user),
        int(b.concurrency),
        long(Some(
            b.pending_ack_timeout.unwrap_or(Some(60_000)).unwrap_or(0),
        )),
        boolean(b.configuration_managed),
        s(&b.client_id.clone().flatten()),
    ];
    fields
        .iter()
        .fold(1i32, |h, v| h.wrapping_mul(31).wrapping_add(*v)) as u32
}

/// `;userMessageID=<uuid>` when the message has a user ID.
fn user_message_id(m: &CoreMessage) -> String {
    m.user_id
        .map(|u| format!(";userMessageID={}", java::uuid(&u)))
        .unwrap_or_default()
}

/// Describes an AMQP message as `AMQPStandardMessage.toString` (or
/// `AMQPLargeMessage`'s) does, with its `getUserID` if it has one; `None`
/// for other records. The AMQP protocol, which knows the encoding, sets it
/// ([`set_amqp_describer`]).
pub type AmqpDescriber = fn(&PersistedMessage) -> Option<(Option<String>, String)>;

static AMQP_DESCRIBER: std::sync::OnceLock<AmqpDescriber> = std::sync::OnceLock::new();

/// Sets how AMQP messages are described; without it they print without
/// their sections.
pub fn set_amqp_describer(describe: AmqpDescriber) {
    let _ = AMQP_DESCRIBER.set(describe);
}

/// `MessageDescribe` for an `ADD_MESSAGE_PROTOCOL` record.
fn describe_message(m: &PersistedMessage, data: &[u8]) -> Described {
    match m {
        PersistedMessage::Core(c) => {
            let msg = c.to_message();
            let size = c.buffer_size();
            Described {
                text: format!(
                    "Message(messageID={}{};msg={}",
                    msg.message_id,
                    user_message_id(&msg),
                    core_message_to_string(&msg, size, data)
                ),
                safe_text: Some(format!("CoreMessage(safe data, size={size})")),
            }
        }
        PersistedMessage::Amqp(a)
            if let Some((user_id, text)) = AMQP_DESCRIBER.get().and_then(|d| d(m)) =>
        {
            Described {
                text: format!(
                    "Message(messageID={}{};msg={text}",
                    a.message_id,
                    user_id
                        .map(|u| format!(";userMessageID={u}"))
                        .unwrap_or_default()
                ),
                safe_text: Some(format!(
                    "AMQPStandardMessage(safe data, size={})",
                    a.data.len()
                )),
            }
        }
        PersistedMessage::Amqp(a) => Described {
            text: format!(
                "Message(messageID={};msg=AMQPStandardMessage(<AMQP sections not printed by saranga>, address={})@{}",
                a.message_id,
                java::nullable(a.address.as_ref()),
                java::bytes_hash(data)
            ),
            safe_text: Some(format!(
                "AMQPStandardMessage(safe data, size={})",
                a.data.len()
            )),
        },
        PersistedMessage::AmqpLarge(a)
            if let Some((user_id, text)) = AMQP_DESCRIBER.get().and_then(|d| d(m)) =>
        {
            Described {
                text: format!(
                    "LargeMessage(messageID={}{};msg={text}",
                    a.message_id,
                    user_id
                        .map(|u| format!(";userMessageID={u}"))
                        .unwrap_or_default()
                ),
                safe_text: Some("AMQPLargeMessage(safe data)".into()),
            }
        }
        PersistedMessage::AmqpLarge(a) => Described {
            text: format!(
                "LargeMessage(messageID={};msg=AMQPLargeMessage(<AMQP sections not printed by saranga>, address={})@{}",
                a.message_id,
                java::nullable(a.address.as_ref()),
                java::bytes_hash(data)
            ),
            safe_text: Some("AMQPLargeMessage(safe data)".into()),
        },
    }
}

/// `CoreMessage.toString()`.
pub fn core_message_to_string(m: &CoreMessage, size: usize, data: &[u8]) -> String {
    format!(
        "CoreMessage[messageID={}, durable={}, userID={}, priority={}, timestamp={}, expiration={}, durable={}, address={}, size={size}, properties={}]@{}",
        m.message_id,
        m.durable,
        m.user_id.map_or_else(|| "null".into(), |u| java::uuid(&u)),
        m.priority as i8,
        to_date(m.timestamp),
        to_date(m.expiration),
        m.durable,
        java::nullable(m.address.as_ref()),
        properties_to_string(&m.properties),
        java::bytes_hash(data)
    )
}

/// `LargeServerMessageImpl.toString()`.
fn large_message_to_string(m: &CoreMessage, data: &[u8]) -> String {
    format!(
        "LargeServerMessage[messageID={},durable={},userID={},priority={}, timestamp={},expiration={}, durable={}, address={}, properties={}]@{}",
        m.message_id,
        m.durable,
        m.user_id.map_or_else(|| "null".into(), |u| java::uuid(&u)),
        m.priority as i8,
        to_date(m.timestamp),
        to_date(m.expiration),
        m.durable,
        java::nullable(m.address.as_ref()),
        properties_to_string(&m.properties),
        java::bytes_hash(data)
    )
}

/// `CoreMessage.toDate`.
fn to_date(millis: i64) -> String {
    if millis == 0 {
        "0".into()
    } else {
        java::date(millis)
    }
}

/// `TypedProperties.toString()`. The decoded properties live in a
/// `HashMap<>(numHeaders, 1.0f)`, which fixes the iteration order.
pub fn properties_to_string(p: &TypedProperties) -> String {
    let entries = p.entries();
    // A repeated key keeps its first position and its last value.
    let mut keys: Vec<&SimpleString> = Vec::new();
    let mut values: HashMap<&SimpleString, &PropertyValue> = HashMap::new();
    for (k, v) in entries {
        if values.insert(k, v).is_none() {
            keys.push(k);
        }
    }
    let hashes: Vec<i32> = keys.iter().map(|k| java::simple_string_hash(k)).collect();
    let order = java::hash_map_order(&hashes, Some((entries.len(), 1.0)));
    let mut s = String::from("TypedProperties[");
    for (n, i) in order.iter().enumerate() {
        let key = java::simple_string(keys[*i]);
        let _ = write!(s, "{key}=");
        match values[keys[*i]] {
            PropertyValue::Null => s.push_str("NULL-value"),
            PropertyValue::Bytes(b) => {
                let _ = write!(s, "[{}]", java::max_string(&java::bytes_to_hex(b, 2), 150));
                if key.starts_with("_AMQ_ROUTE_TO") {
                    s.push_str(", bytesAsLongs[");
                    let mut chunks = b.chunks(8).peekable();
                    while let Some(c) = chunks.next() {
                        match <[u8; 8]>::try_from(c) {
                            Ok(c) => {
                                let _ = write!(s, "{}", i64::from_be_bytes(c));
                                if chunks.peek().is_some() {
                                    s.push_str(", ");
                                }
                            }
                            Err(_) => s.push_str("error-converting-longs=null"),
                        }
                    }
                    s.push(']');
                }
            }
            PropertyValue::Bool(v) => {
                let _ = write!(s, "{v}");
            }
            PropertyValue::Byte(v) => {
                let _ = write!(s, "{v}");
            }
            PropertyValue::Short(v) => {
                let _ = write!(s, "{v}");
            }
            PropertyValue::Int(v) => {
                let _ = write!(s, "{v}");
            }
            PropertyValue::Long(v) => {
                let _ = write!(s, "{v}");
            }
            PropertyValue::Float(v) => s.push_str(&java::float(*v)),
            PropertyValue::Double(v) => s.push_str(&java::double(*v)),
            PropertyValue::Char(c) => s.push_str(&java::utf16([*c])),
            PropertyValue::String(v) => s.push_str(&java::simple_string(v)),
        }
        if n + 1 < order.len() {
            s.push_str(", ");
        }
    }
    s.push(']');
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xid_rendering() {
        // From core-xa's data-print.txt (the identity hash differs).
        let x = Xid {
            format_id: 4_871_257,
            branch_qualifier: b"branch-1".to_vec(),
            global_transaction_id: b"global-transaction-1".to_vec(),
        };
        let s = xid_to_string(&x);
        assert!(
            s.ends_with(" bq:98.114.97.110.99.104.45.49 formatID:4871257 gtxid:103.108.111.98.97.108.45.116.114.97.110.115.97.99.116.105.111.110.45.49 base64:YnJhbmNoLTFnbG9iYWwtdHJhbnNhY3Rpb24tMfeeSgA="),
            "{s}"
        );
        assert_eq!(to_xid(&[]), "null");
    }

    #[test]
    fn properties_print_in_java_hash_map_order() {
        let mut p = TypedProperties::new();
        p.insert("__AMQ_CID".into(), PropertyValue::String("c".into()));
        p.insert("_AMQ_ROUTING_TYPE".into(), PropertyValue::Byte(1));
        p.insert("pDouble".into(), PropertyValue::Double(1e300));
        p.insert("pEmpty".into(), PropertyValue::String("".into()));
        p.insert("n".into(), PropertyValue::Null);
        p.insert(
            "_AMQ_ROUTE_TO".into(),
            PropertyValue::Bytes(vec![0, 0, 0, 0, 0, 0, 0, 7]),
        );
        let s = properties_to_string(&p);
        assert!(s.contains("pDouble=1.0E300"), "{s}");
        assert!(s.contains("pEmpty=,") || s.ends_with("pEmpty=]"), "{s}");
        assert!(s.contains("n=NULL-value"), "{s}");
        assert!(
            s.contains("_AMQ_ROUTE_TO=[0000 0000 0000 0007], bytesAsLongs[7]"),
            "{s}"
        );
        assert_eq!(
            properties_to_string(&TypedProperties::new()),
            "TypedProperties[]"
        );
    }

    #[test]
    fn queue_configuration_defaults_for_old_records() {
        let b = PersistentQueueBindingEncoding {
            name: "a::q".into(),
            address: "a".into(),
            filter_string: Some("  ".into()),
            metadata: Some("".into()),
            auto_created: true,
            ..Default::default()
        };
        let s = queue_configuration(5, &b);
        assert!(s.starts_with("QueueConfiguration [id=5, name=q, address=a, routingType=MULTICAST, filterString=null,"), "{s}");
        assert!(s.contains("autoDelete=true,"), "{s}");
        assert!(s.contains("configurationManaged=null,"), "{s}");
        assert!(s.ends_with("autoCreated=true, fqqn=true]"), "{s}");
    }

    #[test]
    fn unknown_and_null_bodies() {
        assert_eq!(describe_body(49, 1, false, &[1, 2], false), "** null **");
        assert_eq!(describe_body(99, 1, false, &[], false), "** null **");
        assert_eq!(
            describe_body(ids::ADD_REF, 1, true, &3i64.to_be_bytes(), false),
            "AddRef;QueueEncoding [queueID=3]"
        );
        assert_eq!(
            describe_body(
                ids::PAGE_TRANSACTION,
                9,
                false,
                &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2],
                false
            ),
            "PageTransactionInfoImpl(transactionID=1,id=9,numberOfMessages=2)"
        );
        assert_eq!(
            describe_body(
                ids::PAGE_TRANSACTION,
                9,
                true,
                &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2],
                false
            ),
            "PageUpdateTXEncoding [pageTX=1, records=2]"
        );
    }
}
