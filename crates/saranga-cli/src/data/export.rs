//! `saranga data exp` (`XmlDataExporter`): the addresses, queues and
//! messages of a broker's data directory as the XML `artemis data imp`
//! reads, written as Artemis writes it (the JDK's `XMLStreamWriter` behind
//! Artemis's `PrettyPrintHandler`), down to the `HashMap` orders.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;
use saranga_buffer::{PropertyValue, SimpleString, TypedProperties};
use saranga_journal::{Journal, JournalConfig, RecordInfo};
use saranga_message::CoreMessage;
use saranga_paging::{PagedBody, page_id_of, read_page};
use saranga_storage::RecordCodec;
use saranga_storage::codec::{
    CursorAckRecordEncoding, PageTransactionEncoding, PersistentAddressBindingEncoding,
    PersistentQueueBindingEncoding, RefEncoding,
};
use saranga_storage::message::{LargeMessageRecord, PersistedMessage};
use saranga_storage::{ids, java};

use super::dirs::{DataDirArgs, DbArgs};

#[derive(Args)]
pub struct ExportArgs {
    #[command(flatten)]
    dirs: DataDirArgs,
    #[command(flatten)]
    db: DbArgs,
    /// How often to print progress in the console. Set to <= 0 to disable
    /// it.
    #[arg(
        long = "log-interval",
        default_value_t = 10_000,
        allow_negative_numbers = true
    )]
    log_interval: i64,
    /// In case a queue does not exist, this will define the prefix to be
    /// used on the message export.
    #[arg(long = "undefined-prefix", default_value = "UndefinedQueue_")]
    undefined_prefix: String,
    /// Only export the specified queue(s). Repeatable. When omitted, all
    /// queues are exported.
    #[arg(long = "queue")]
    queues: Vec<String>,
}

pub fn run(args: ExportArgs) -> anyhow::Result<()> {
    let _lock = args.db.lock()?;
    let out = args.db.out()?;
    if let Some(store) = args.dirs.unchecked()?.database() {
        return from_tables(&args, out, &store);
    }
    let d = args.dirs.resolve()?;
    let file_size = d
        .config
        .as_ref()
        .and_then(|c| c.journal.file_size)
        .map(|n| n.max(0) as usize);
    let mut exporter = Exporter {
        args: &args,
        source: "journal",
        large_messages: d.large_messages.clone(),
        queue_bindings: HashMap::new(),
        queue_order: Vec::new(),
        address_bindings: HashMap::new(),
        address_order: Vec::new(),
        messages: BTreeMap::new(),
        refs: HashMap::new(),
        cursor_acks: HashMap::new(),
        page_completes: HashMap::new(),
        page_txs: HashSet::new(),
        xml: Xml::new(std::io::BufWriter::new(out)),
        amqp: saranga_protocol_amqp::native::decoder(),
    };
    exporter.bindings(&d.bindings)?;
    exporter.warn_unknown_queues();
    exporter.message_journal(&d.journal, file_size)?;
    exporter.print(&d.paging)?;
    Ok(())
}

/// `RelationalDatabaseXmlDataExporter`: the same document, from the tables.
/// Paged messages are rows like the others. Large bodies go through files, as
/// the printing reads them from there.
fn from_tables(
    args: &ExportArgs,
    out: Box<dyn Write + '_>,
    store: &saranga_server::storage::DatabaseConfig,
) -> anyhow::Result<()> {
    let tables = saranga_server::storage::read_tables(store)?;
    let bodies = tempfile::tempdir().context("a directory for large bodies")?;
    let mut exporter = Exporter {
        args,
        source: "database",
        large_messages: bodies.path().to_path_buf(),
        queue_bindings: HashMap::new(),
        queue_order: Vec::new(),
        address_bindings: HashMap::new(),
        address_order: Vec::new(),
        messages: BTreeMap::new(),
        refs: HashMap::new(),
        cursor_acks: HashMap::new(),
        page_completes: HashMap::new(),
        page_txs: HashSet::new(),
        xml: Xml::new(std::io::BufWriter::new(out)),
        amqp: saranga_protocol_amqp::native::decoder(),
    };
    exporter.tables(&tables)?;
    exporter.warn_unknown_queues();
    exporter.print(bodies.path())?;
    Ok(())
}

/// A queue binding: its name, address, filter and routing type.
struct QueueBinding {
    name: String,
    address: String,
    filter: String,
    routing_type: &'static str,
}

/// A message to export: Core, its body whole or in a large message file.
struct Exported {
    message: CoreMessage,
    /// The large message's body file.
    large: Option<PathBuf>,
    /// The `HashMap` its properties are in: decoded (`(n, 1.0)`) or built.
    decoded: bool,
}

struct Exporter<'a> {
    args: &'a ExportArgs,
    /// Where the messages come from, for the log.
    source: &'static str,
    large_messages: PathBuf,
    queue_bindings: HashMap<i64, QueueBinding>,
    /// The queue IDs as first put (for `HashMap<Long, _>` order).
    queue_order: Vec<i64>,
    address_bindings: HashMap<i64, (String, Vec<&'static str>)>,
    address_order: Vec<i64>,
    messages: BTreeMap<i64, Exported>,
    /// By message: the queue IDs referring to it, as first put.
    refs: HashMap<i64, Vec<i64>>,
    cursor_acks: HashMap<i64, HashSet<(i64, i32)>>,
    /// The pages each queue has consumed whole (`PAGE_CURSOR_COMPLETE`),
    /// whose individual cursor acknowledgements were deleted then.
    page_completes: HashMap<i64, HashSet<i64>>,
    page_txs: HashSet<i64>,
    xml: Xml<std::io::BufWriter<Box<dyn Write + 'a>>>,
    amqp: std::sync::Arc<saranga_server::message::NativeDecoder>,
}

/// The journal as `data exp` loads it: no compaction, nothing removed.
fn load(mut config: JournalConfig) -> anyhow::Result<Vec<RecordInfo>> {
    if !config.directory.is_dir() {
        return Ok(Vec::new());
    }
    config.compact_min_files = 0;
    config.compact_percentage = 0;
    config.remove_extra_files_on_load = false;
    let dir = config.directory.clone();
    let journal = Journal::open(config).with_context(|| format!("opening {}", dir.display()))?;
    let loaded = journal
        .load_all()
        .with_context(|| format!("loading {}", dir.display()))?;
    journal.stop()?;
    Ok(loaded.committed)
}

fn routing_type_name(t: u8) -> &'static str {
    if t == 1 { "ANYCAST" } else { "MULTICAST" }
}

impl Exporter<'_> {
    fn include(&self, queue: &str) -> bool {
        self.args.queues.is_empty() || self.args.queues.iter().any(|q| q == queue)
    }

    /// `getBindings`.
    fn bindings(&mut self, dir: &Path) -> anyhow::Result<()> {
        let mut config = JournalConfig::bindings(dir);
        config.pool_size = 2;
        for r in load(config)? {
            match r.user_record_type {
                ids::QUEUE_BINDING_RECORD => {
                    let b = PersistentQueueBindingEncoding::decode(&r.data)
                        .with_context(|| format!("queue binding {}", r.id))?;
                    if !self.queue_bindings.contains_key(&r.id) {
                        self.queue_order.push(r.id);
                    }
                    self.queue_bindings.insert(
                        r.id,
                        QueueBinding {
                            name: java::simple_string(&b.name),
                            address: java::simple_string(&b.address),
                            filter: b
                                .filter_string
                                .as_ref()
                                .map(java::simple_string)
                                .unwrap_or_default(),
                            routing_type: routing_type_name(b.consumers.map_or(0, |c| c.2)),
                        },
                    );
                }
                ids::ADDRESS_BINDING_RECORD => {
                    let b = PersistentAddressBindingEncoding::decode(&r.data)
                        .with_context(|| format!("address binding {}", r.id))?;
                    // An EnumSet: MULTICAST, then ANYCAST.
                    let mut types = Vec::new();
                    if b.routing_types.contains(&0) {
                        types.push("MULTICAST");
                    }
                    if b.routing_types.contains(&1) {
                        types.push("ANYCAST");
                    }
                    if !self.address_bindings.contains_key(&r.id) {
                        self.address_order.push(r.id);
                    }
                    self.address_bindings
                        .insert(r.id, (java::simple_string(&b.name), types));
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// What the tables hold. A transaction not committed exports nothing, as
    /// its records are not among a journal's committed ones.
    fn tables(&mut self, tables: &saranga_server::storage::Tables) -> anyhow::Result<()> {
        for a in &tables.addresses {
            // An EnumSet: MULTICAST, then ANYCAST.
            let mut types = Vec::new();
            if a.multicast {
                types.push("MULTICAST");
            }
            if a.anycast {
                types.push("ANYCAST");
            }
            self.address_order.push(a.id);
            self.address_bindings.insert(a.id, (a.name.clone(), types));
        }
        for q in &tables.queues {
            let address = self
                .address_bindings
                .get(&q.address)
                .map(|(name, _)| name.clone())
                .unwrap_or_default();
            self.queue_order.push(q.id);
            self.queue_bindings.insert(
                q.id,
                QueueBinding {
                    name: q.name.clone(),
                    address,
                    filter: q.filter.clone().unwrap_or_default(),
                    routing_type: routing_type_name(u8::from(!q.multicast)),
                },
            );
        }
        for m in tables.messages.iter().filter(|m| m.tx.is_none()) {
            if let Some(body) = &m.body {
                std::fs::write(self.large_file(m.id), body)
                    .with_context(|| format!("the body of message {}", m.id))?;
            }
            let exported = if m.large && m.core {
                self.large(m.id, &m.record)?
            } else if m.large {
                self.persisted(&m.record, Some(m.id))?
            } else {
                self.persisted(&m.record, None)?
            };
            self.messages.insert(m.id, exported);
        }
        for r in tables.references.iter().filter(|r| r.tx.is_none()) {
            self.refs.entry(r.message).or_default().push(r.queue);
        }
        Ok(())
    }

    /// `warnUnknownQueues`.
    fn warn_unknown_queues(&self) {
        for q in &self.args.queues {
            if !self.queue_bindings.values().any(|b| &b.name == q) {
                eprintln!("Queue '{q}' was not found; nothing will be exported for it.");
            }
        }
    }

    /// `processMessageJournal`.
    fn message_journal(&mut self, dir: &Path, file_size: Option<usize>) -> anyhow::Result<()> {
        let mut config = JournalConfig::messages(dir);
        if let Some(n) = file_size {
            config.file_size = n;
        }
        let mut acks = Vec::new();
        for r in load(config)? {
            match r.user_record_type {
                ids::ADD_MESSAGE_PROTOCOL => {
                    let m = self.persisted(&r.data, None)?;
                    self.messages.insert(r.id, m);
                }
                ids::ADD_LARGE_MESSAGE => {
                    let m = self.large(r.id, &r.data)?;
                    self.messages.insert(r.id, m);
                }
                ids::ADD_MESSAGE => {
                    eprintln!(
                        "Message {} is in the format before Artemis 2.0 (ADD_MESSAGE) and is not exported",
                        r.id
                    );
                }
                ids::ADD_REF => {
                    let q = RefEncoding::decode(&r.data)?.queue_id;
                    let refs = self.refs.entry(r.id).or_default();
                    if !refs.contains(&q) {
                        refs.push(q);
                    }
                }
                ids::ACKNOWLEDGE_REF => acks.push(r),
                ids::ACKNOWLEDGE_CURSOR => {
                    let e = CursorAckRecordEncoding::decode(&r.data)?;
                    self.cursor_acks
                        .entry(e.queue_id)
                        .or_default()
                        .insert((e.page_nr, e.message_nr));
                }
                ids::PAGE_CURSOR_COMPLETE => {
                    let e = CursorAckRecordEncoding::decode(&r.data)?;
                    self.page_completes
                        .entry(e.queue_id)
                        .or_default()
                        .insert(e.page_nr);
                }
                ids::PAGE_TRANSACTION => {
                    let e = PageTransactionEncoding::decode(&r.data)?;
                    self.page_txs.insert(e.transaction_id);
                }
                _ => {}
            }
        }
        // removeAcked
        for r in acks {
            let q = RefEncoding::decode(&r.data)?.queue_id;
            if let Some(refs) = self.refs.get_mut(&r.id) {
                refs.retain(|&x| x != q);
                if refs.is_empty() {
                    self.messages.remove(&r.id);
                    self.refs.remove(&r.id);
                }
            }
        }
        Ok(())
    }

    /// A message record (`ADD_MESSAGE_PROTOCOL`, or a paged one) as Core
    /// (`toCore`).
    fn persisted(&self, data: &[u8], large: Option<i64>) -> anyhow::Result<Exported> {
        let p = PersistedMessage::decode(data).context("decoding a message")?;
        self.native(p, large)
    }

    fn native(&self, p: PersistedMessage, large: Option<i64>) -> anyhow::Result<Exported> {
        if let PersistedMessage::Core(c) = &p {
            return Ok(Exported {
                message: c.to_message(),
                large: None,
                decoded: true,
            });
        }
        // A large message's body is in its file, whichever record named it.
        let large = large.or_else(|| p.is_large().then(|| p.message_id()));
        let body = match large {
            Some(id) => Some(Self::read_large(&self.large_file(id))?),
            None => None,
        };
        let native = (self.amqp)(&p, body)
            .ok_or_else(|| anyhow::anyhow!("no decoder for message {}", p.message_id()))?
            .map_err(|e| anyhow::anyhow!("message {}: {e}", p.message_id()))?;
        Ok(Exported {
            message: native.to_core(),
            large: None,
            decoded: false,
        })
    }

    /// An `ADD_LARGE_MESSAGE` record: a Core large message's headers, its
    /// body in `<id>.msg`; or another protocol's large message.
    fn large(&self, id: i64, data: &[u8]) -> anyhow::Result<Exported> {
        match LargeMessageRecord::decode(data) {
            Ok(r) => Ok(Exported {
                message: r.message,
                large: Some(self.large_file(id)),
                decoded: true,
            }),
            Err(_) => self.persisted(data, Some(id)),
        }
    }

    fn large_file(&self, id: i64) -> PathBuf {
        self.large_messages.join(format!("{id}.msg"))
    }

    /// A large message's body. A missing or unreadable file fails the
    /// export with exit status 1: Artemis writes an empty body (creating
    /// the missing file) and exits 0, which loses the body without a word.
    fn read_large(file: &Path) -> anyhow::Result<Vec<u8>> {
        std::fs::read(file).map_err(|e| unreadable("the large message body", file, e))
    }

    /// `printDataAsXML`.
    fn print(&mut self, paging: &Path) -> anyhow::Result<()> {
        self.xml.start("activemq-journal")?;
        self.print_bindings()?;
        self.print_messages(paging)?;
        self.xml.end()?;
        self.xml.flush()?;
        Ok(())
    }

    /// `printBindingsAsXML`, in the `HashMap`s' orders.
    fn print_bindings(&mut self) -> anyhow::Result<()> {
        let included: Option<HashSet<String>> = (!self.args.queues.is_empty()).then(|| {
            self.queue_bindings
                .values()
                .filter(|b| self.include(&b.name))
                .map(|b| b.address.clone())
                .collect()
        });
        self.xml.start("bindings")?;
        for id in java::long_map_order(&self.address_order) {
            let (name, types) = &self.address_bindings[&id];
            if included.as_ref().is_some_and(|i| !i.contains(name)) {
                continue;
            }
            self.xml.empty("address-binding")?;
            self.xml.attr("routing-types", &types.join(","))?;
            self.xml.attr("name", name)?;
            self.xml.attr("id", &id.to_string())?;
        }
        for id in java::long_map_order(&self.queue_order) {
            let b = &self.queue_bindings[&id];
            if !self.include(&b.name) {
                continue;
            }
            self.xml.empty("queue-binding")?;
            self.xml.attr("address", &b.address)?;
            self.xml.attr("filter-string", &b.filter)?;
            self.xml.attr("name", &b.name)?;
            self.xml.attr("id", &id.to_string())?;
            self.xml.attr("routing-type", b.routing_type)?;
        }
        self.xml.end()?;
        Ok(())
    }

    /// `printAllMessagesAsXML`: the journal's messages by ID, then the
    /// paged ones.
    fn print_messages(&mut self, paging: &Path) -> anyhow::Result<()> {
        self.xml.start("messages")?;
        let interval = self.args.log_interval;
        if interval > 0 {
            eprintln!("Processing {} messages", self.source);
        }
        let mut count = 0i64;
        let messages = std::mem::take(&mut self.messages);
        for (id, m) in &messages {
            let refs = self.refs.get(id).cloned().unwrap_or_default();
            let queues = self.queue_names(&refs);
            if queues.is_empty() {
                continue;
            }
            self.print_message(m, &queues)?;
            count += 1;
            if interval > 0 && count % interval == 0 {
                eprintln!("exported {count} messages from journal");
            }
        }
        self.print_paged(paging)?;
        self.xml.end()?;
        Ok(())
    }

    /// `extractQueueNames`: the message's queues in `HashMap<Long, _>`
    /// order; a queue without a binding is named with the undefined prefix.
    fn queue_names(&mut self, refs: &[i64]) -> Vec<String> {
        let mut out = Vec::new();
        for id in java::long_map_order(refs) {
            let name = match self.queue_bindings.get(&id) {
                Some(b) => b.name.clone(),
                None => {
                    if !self.args.queues.is_empty() {
                        continue;
                    }
                    let name = format!("{}{id}", self.args.undefined_prefix);
                    self.queue_order.push(id);
                    self.queue_bindings.insert(
                        id,
                        QueueBinding {
                            name: name.clone(),
                            address: name.clone(),
                            filter: String::new(),
                            routing_type: "MULTICAST",
                        },
                    );
                    eprintln!("Queue ID {id} not defined. Exporting it as {name}");
                    name
                }
            };
            if self.include(&name) {
                out.push(name);
            }
        }
        out
    }

    /// `printPagedMessagesAsXML`: each store's pages from the first, each
    /// message to its queues that have not acknowledged it, if its page
    /// transaction (if any) completed.
    fn print_paged(&mut self, paging: &Path) -> anyhow::Result<()> {
        let interval = self.args.log_interval;
        let mut count = 0i64;
        for (_, folder) in saranga_paging::print::store_folders(paging) {
            // An unreadable folder or page fails the export with exit status
            // 1: Artemis prints the exception, skips the rest of paging and
            // exits 0.
            let mut pages: Vec<i64> = std::fs::read_dir(&folder)
                .map_err(|e| unreadable("the page folder", &folder, e))?
                .flatten()
                .filter_map(|e| page_id_of(&e.file_name().to_string_lossy()))
                .collect();
            pages.sort_unstable();
            let Some(&first) = pages.first() else {
                continue;
            };
            for page_id in first..first + pages.len() as i64 {
                let path = folder.join(saranga_paging::page_file_name(page_id));
                // A page missing from the range is empty, as Artemis's
                // `newPageObject` reads it.
                let data = match std::fs::read(&path) {
                    Ok(data) => data,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(unreadable("the page", &path, e)),
                };
                for (nr, paged) in read_page(&data).messages.into_iter().enumerate() {
                    count += 1;
                    if interval > 0 && count % interval == 0 {
                        eprintln!("Exported {count} messages from paging");
                    }
                    let mut queues = Vec::new();
                    for q in &paged.queue_ids {
                        let acked = self
                            .cursor_acks
                            .get(q)
                            .is_some_and(|s| s.contains(&(page_id, nr as i32)))
                            || self
                                .page_completes
                                .get(q)
                                .is_some_and(|s| s.contains(&page_id));
                        if acked {
                            continue;
                        }
                        if let Some(b) = self.queue_bindings.get(q)
                            && self.include(&b.name)
                        {
                            queues.push(b.name.clone());
                        }
                    }
                    let tx = paged.transaction_id;
                    if queues.is_empty() || !(tx == -1 || self.page_txs.contains(&tx)) {
                        continue;
                    }
                    let m = match paged.body {
                        PagedBody::Message(p) => self.native(p, None)?,
                        PagedBody::CoreLarge(c) => Exported {
                            large: Some(self.large_file(c.message_id)),
                            message: c,
                            decoded: true,
                        },
                        PagedBody::OtherLarge(p) => {
                            let id = p.message_id();
                            self.native(p, Some(id))?
                        }
                    };
                    self.print_message(&m, &queues)?;
                }
            }
        }
        Ok(())
    }

    /// `XMLMessageExporter.printSingleMessageAsXML`.
    fn print_message(&mut self, m: &Exported, queues: &[String]) -> anyhow::Result<()> {
        let large = match &m.large {
            Some(file) => Some(Self::read_large(file)?),
            None => None,
        };
        let body = match &large {
            Some(b) => Body::Large(b),
            None => Body::Base64(&m.message.body),
        };
        print_message(&mut self.xml, &m.message, m.decoded, Some(queues), body)
    }
}

/// A file the export cannot read, as a `CLIException` (exit status 1) so
/// that the export does not go on without its contents.
fn unreadable(what: &str, path: &Path, e: std::io::Error) -> anyhow::Error {
    crate::client::CliError(format!("Cannot read {what} {}: {e}", path.display())).into()
}

/// A message body as `printMessageBody` writes it.
pub(super) enum Body<'a> {
    /// Base64 in one CDATA.
    Base64(&'a [u8]),
    /// `isLarge`, Base64 in CDATAs of 1000 bytes each.
    Large(&'a [u8]),
    /// A text message's text in a CDATA, as is (`encodeTextUTF8`); none
    /// fails as `readBodyText(...).toString()` does.
    Text(Option<String>),
}

/// `XMLMessageExporter.printSingleMessageAsXML`: the message's attributes,
/// its properties, its queues if any, its body.
pub(super) fn print_message<W: Write>(
    x: &mut Xml<W>,
    c: &CoreMessage,
    decoded: bool,
    queues: Option<&[String]>,
    body: Body,
) -> anyhow::Result<()> {
    x.start("message")?;
    x.attr("id", &c.message_id.to_string())?;
    x.attr("priority", &(c.priority as i8).to_string())?;
    x.attr("expiration", &c.expiration.to_string())?;
    x.attr("timestamp", &c.timestamp.to_string())?;
    x.attr("type", pretty_type(c.message_type))?;
    if let Some(u) = &c.user_id {
        x.attr("user-id", &java::uuid(u))?;
    }
    x.start("properties")?;
    for (key, value) in property_order(&c.properties, decoded) {
        x.empty("property")?;
        x.attr("name", &java::simple_string(key))?;
        x.attr("value", &property_text(value))?;
        if let Some(t) = property_type(value) {
            x.attr("type", t)?;
        }
    }
    x.end()?;
    if let Some(queues) = queues {
        x.start("queues")?;
        for q in queues {
            x.empty("queue")?;
            x.attr("name", q)?;
        }
        x.end()?;
    }
    x.start("body")?;
    match body {
        Body::Large(body) => {
            x.attr("isLarge", "true")?;
            // LARGE_MESSAGE_CHUNK_SIZE
            for chunk in body.chunks(1000) {
                x.cdata(&java::base64_url(chunk))?;
            }
        }
        Body::Base64(body) => x.cdata(&java::base64_url(body))?,
        Body::Text(Some(text)) => x.cdata(&text)?,
        Body::Text(None) => {
            anyhow::bail!("java.lang.RuntimeException: java.lang.NullPointerException")
        }
    }
    x.end()?;
    x.end()?;
    Ok(())
}

/// `getMessagePrettyType`.
fn pretty_type(t: u8) -> &'static str {
    match t {
        4 => "bytes",
        5 => "map",
        2 => "object",
        6 => "stream",
        3 => "text",
        _ => "default",
    }
}

/// `getPropertyNames` order: a decoded message's properties are in a
/// `HashMap(n, 1.0)`, a built one's in a default `HashMap`. A repeated key
/// keeps its first position and its last value.
fn property_order(p: &TypedProperties, decoded: bool) -> Vec<(&SimpleString, &PropertyValue)> {
    let entries = p.entries();
    let mut keys: Vec<&SimpleString> = Vec::new();
    let mut values: HashMap<&SimpleString, &PropertyValue> = HashMap::new();
    for (k, v) in entries {
        if values.insert(k, v).is_none() {
            keys.push(k);
        }
    }
    let hashes: Vec<i32> = keys.iter().map(|k| java::simple_string_hash(k)).collect();
    let capacity = decoded.then_some((entries.len(), 1.0));
    let map: Vec<usize> = java::hash_map_order(&hashes, capacity);
    // getPropertyNames: new HashSet<>(keySet()), filled in the map's order;
    // on JDK 19 and later its table is HashMap.newHashMap(max(n, 12)).
    let set_hashes: Vec<i32> = map.iter().map(|&i| hashes[i]).collect();
    let set_capacity = (keys.len().max(12) as f64 / 0.75).ceil() as usize;
    java::hash_map_order(&set_hashes, Some((set_capacity, 0.75)))
        .into_iter()
        .map(|j| (keys[map[j]], values[keys[map[j]]]))
        .collect()
}

/// `XmlDataExporterUtil.convertProperty`.
fn property_text(v: &PropertyValue) -> String {
    match v {
        PropertyValue::Null => "_AMQ_NULL".to_owned(),
        PropertyValue::Bool(b) => b.to_string(),
        PropertyValue::Byte(b) => b.to_string(),
        PropertyValue::Bytes(b) => java::base64_url(b),
        PropertyValue::Short(n) => n.to_string(),
        PropertyValue::Int(n) => n.to_string(),
        PropertyValue::Long(n) => n.to_string(),
        PropertyValue::Float(f) => java::float(*f),
        PropertyValue::Double(f) => java::double(*f),
        PropertyValue::Char(c) => java::utf16([*c]),
        PropertyValue::String(s) => java::simple_string(s),
    }
}

/// `XmlDataExporterUtil.getPropertyType`; none for a char.
fn property_type(v: &PropertyValue) -> Option<&'static str> {
    Some(match v {
        PropertyValue::Null | PropertyValue::Bytes(_) => "bytes",
        PropertyValue::Bool(_) => "boolean",
        PropertyValue::Byte(_) => "byte",
        PropertyValue::Short(_) => "short",
        PropertyValue::Int(_) => "integer",
        PropertyValue::Long(_) => "long",
        PropertyValue::Float(_) => "float",
        PropertyValue::Double(_) => "double",
        PropertyValue::String(_) => "simple-string",
        PropertyValue::Char(_) => return None,
    })
}

/// The JDK's `XMLStreamWriter` as Artemis's `PrettyPrintHandler` drives
/// it: each start, empty element and CDATA on a line of its own, indented
/// three spaces a level, each end element on a new line too. A start or
/// empty element's tag is closed when the next thing is written.
pub struct Xml<W: Write> {
    out: W,
    depth: usize,
    open: Vec<&'static str>,
    /// `>` or `/>` still to write.
    pending: Option<&'static str>,
}

/// `System.lineSeparator()`.
const LINE_SEPARATOR: &str = if cfg!(windows) { "\r\n" } else { "\n" };

impl<W: Write> Xml<W> {
    pub fn new(out: W) -> Self {
        Self {
            out,
            depth: 0,
            open: Vec::new(),
            pending: None,
        }
    }

    fn close_pending(&mut self) -> std::io::Result<()> {
        if let Some(p) = self.pending.take() {
            self.out.write_all(p.as_bytes())?;
        }
        Ok(())
    }

    fn line(&mut self) -> std::io::Result<()> {
        self.close_pending()?;
        write!(self.out, "{LINE_SEPARATOR}{}", " ".repeat(self.depth * 3))
    }

    pub fn start(&mut self, name: &'static str) -> std::io::Result<()> {
        self.line()?;
        self.depth += 1;
        self.open.push(name);
        write!(self.out, "<{name}")?;
        self.pending = Some(">");
        Ok(())
    }

    pub fn empty(&mut self, name: &'static str) -> std::io::Result<()> {
        self.line()?;
        write!(self.out, "<{name}")?;
        self.pending = Some("/>");
        Ok(())
    }

    pub fn attr(&mut self, name: &str, value: &str) -> std::io::Result<()> {
        write!(self.out, " {name}=\"")?;
        for c in value.chars() {
            match c {
                '&' => self.out.write_all(b"&amp;")?,
                '<' => self.out.write_all(b"&lt;")?,
                '>' => self.out.write_all(b"&gt;")?,
                '"' => self.out.write_all(b"&quot;")?,
                c => write!(self.out, "{c}")?,
            }
        }
        self.out.write_all(b"\"")
    }

    pub fn cdata(&mut self, text: &str) -> std::io::Result<()> {
        self.line()?;
        write!(self.out, "<![CDATA[{text}]]>")
    }

    pub fn end(&mut self) -> std::io::Result<()> {
        self.depth -= 1;
        self.line()?;
        let name = self.open.pop().unwrap_or_default();
        write!(self.out, "</{name}>")
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.close_pending()?;
        self.out.flush()
    }

    /// Text written as is, past the handler (`writeStartDocument`).
    pub fn raw(&mut self, text: &str) -> std::io::Result<()> {
        self.close_pending()?;
        self.out.write_all(text.as_bytes())
    }

    /// `writeEndDocument`: every element still open ended, with no line
    /// between them.
    pub fn end_document(&mut self) -> std::io::Result<()> {
        self.close_pending()?;
        while let Some(name) = self.open.pop() {
            self.depth -= 1;
            write!(self.out, "</{name}>")?;
        }
        Ok(())
    }

    /// The output, unflushed.
    pub fn into_inner(self) -> W {
        self.out
    }
}

#[cfg(test)]
mod page_complete_tests {
    use std::sync::Arc;

    use clap::Parser;
    use saranga_message::CoreMessage;
    use saranga_server::management::Json;
    use saranga_server::postoffice::{QueueSpec, SelectorFilters};
    use saranga_server::session::{ClientConnection, ServerSession, SessionOptions, Transport};
    use saranga_server::{Broker, BrokerConfig, RoutingType};

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: super::ExportArgs,
    }

    /// A queue that consumed whole pages has their cursor acknowledgements
    /// replaced by `PAGE_CURSOR_COMPLETE`: their messages are not exported
    /// for it, and another queue still holding them keeps them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn messages_of_a_completed_page_are_not_exported_for_the_queue() {
        let dir = tempfile::tempdir().unwrap();
        let file = saranga_config::BrokerConfig::parse(
            r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
                 <persistence-enabled>true</persistence-enabled>
                 <journal-file-size>102400</journal-file-size>
                 <address-settings>
                   <address-setting match="t">
                     <max-size-bytes>1</max-size-bytes>
                     <page-size-bytes>5000</page-size-bytes>
                     <address-full-policy>PAGE</address-full-policy>
                   </address-setting>
                 </address-settings>
               </core></configuration>"#,
        )
        .unwrap();
        let config =
            BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir.path()).unwrap();
        let broker = Broker::start(config).await.unwrap();
        let po = broker.post_office();
        for q in ["s1", "s2"] {
            let mut spec = QueueSpec::new(q, "t");
            spec.auto_create_address = Some(true);
            spec.routing_type = Some(RoutingType::Multicast);
            spec.durable = true;
            po.create_queue(spec, false).unwrap();
        }
        let conn = Arc::new(ClientConnection::new(
            Transport::internal("test"),
            Arc::new(Default::default()),
            Vec::new(),
        ));
        let mut s = ServerSession::new(
            broker.clone(),
            conn,
            SessionOptions {
                name: "s".into(),
                username: None,
                password: None,
                xa: false,
                auto_commit_sends: true,
                auto_commit_acks: true,
                pre_acknowledge: false,
                default_address: None,
            },
            Default::default(),
        );
        for _ in 0..40 {
            let m = CoreMessage {
                address: Some("t".into()),
                durable: true,
                body: vec![1u8; 1000].into(),
                ..Default::default()
            };
            s.send(m, None).await.unwrap();
            s.take_completion().wait().await.unwrap();
        }
        assert!(po.paging_store("t").is_paging());
        broker
            .management()
            .invoke(
                &broker,
                "queue.s1",
                "removeMessages",
                &[Json::from("")],
                Default::default(),
            )
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        s.close(false).await;
        broker.stop().unwrap();
        drop(broker);

        let data = dir.path().join("data");
        std::fs::create_dir_all(data.join("large-messages")).unwrap();
        let out = dir.path().join("out.xml");
        let cli = Cli::parse_from([
            "exp".as_ref(),
            "--f".as_ref(),
            "--bindings".as_ref(),
            data.join("bindings").as_os_str(),
            "--journal".as_ref(),
            data.join("journal").as_os_str(),
            "--paging".as_ref(),
            data.join("paging").as_os_str(),
            "--large-messages".as_ref(),
            data.join("large-messages").as_os_str(),
            "--output".as_ref(),
            out.as_os_str(),
        ]);
        super::run(cli.args).unwrap();
        let xml = std::fs::read_to_string(&out).unwrap();
        let count = |q: &str| xml.matches(&format!("<queue name=\"{q}\"/>")).count();
        assert_eq!(count("s1"), 0, "consumed messages exported");
        assert_eq!(count("s2"), 40);
    }

    /// A golden scenario's data directory (written by Artemis 2.57.0),
    /// unpacked into a temporary directory.
    fn golden(scenario: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let gz = std::fs::File::open(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../tests/golden")
                .join(scenario)
                .join("data.tar.gz"),
        )
        .unwrap();
        tar::Archive::new(flate2::read::GzDecoder::new(gz))
            .unpack(dir.path())
            .unwrap();
        dir
    }

    /// `data exp` of a data directory as the command runs it (with
    /// `treat_error`), and the XML it wrote.
    fn export(dir: &std::path::Path) -> (anyhow::Result<()>, String) {
        let data = dir.join("data");
        let out = dir.join("out.xml");
        let cli = Cli::parse_from([
            "exp".as_ref(),
            "--f".as_ref(),
            "--bindings".as_ref(),
            data.join("bindings").as_os_str(),
            "--journal".as_ref(),
            data.join("journal").as_os_str(),
            "--paging".as_ref(),
            data.join("paging").as_os_str(),
            "--large-messages".as_ref(),
            data.join("large-messages").as_os_str(),
            "--output".as_ref(),
            out.as_os_str(),
        ]);
        let result = super::super::treat_error(super::run(cli.args));
        (result, std::fs::read_to_string(&out).unwrap_or_default())
    }

    /// The golden scenarios export whole, with every body: the checks below
    /// fail only on what they remove.
    #[test]
    fn golden_data_with_large_messages_and_pages_exports() {
        for scenario in ["core-large", "amqp-durable", "persist-paging"] {
            let dir = golden(scenario);
            let (result, xml) = export(dir.path());
            result.unwrap_or_else(|e| panic!("{scenario}: {e:#}"));
            assert!(xml.contains("</activemq-journal>"), "{scenario}");
        }
    }

    /// The failure a missing or unreadable file must give: a `CLIException`
    /// (exit status 1) naming the file, not an empty body or a skipped page.
    fn assert_fails_naming(result: anyhow::Result<()>, file: &str) {
        let e = result.expect_err("the export went on without the file");
        assert!(
            e.downcast_ref::<crate::client::CliError>().is_some(),
            "not exit status 1: {e:#}"
        );
        assert!(e.to_string().contains(file), "{e:#}");
    }

    /// A Core large message whose body file is gone: Artemis 2.57.0 exports
    /// it with an empty body, creates the file empty and exits 0.
    #[test]
    fn a_missing_core_large_body_fails_the_export() {
        let dir = golden("core-large");
        let body = dir.path().join("data/large-messages/25.msg");
        std::fs::remove_file(&body).unwrap();
        let (result, _) = export(dir.path());
        assert_fails_naming(result, "25.msg");
        assert!(!body.exists(), "the export created the body file");
    }

    /// The same for another protocol's large message, which the export
    /// converts with its body.
    #[test]
    fn a_missing_amqp_large_body_fails_the_export() {
        let dir = golden("amqp-durable");
        std::fs::remove_file(dir.path().join("data/large-messages/44.msg")).unwrap();
        let (result, _) = export(dir.path());
        assert_fails_naming(result, "44.msg");
    }

    /// And for a paged large message.
    #[test]
    fn a_missing_paged_large_body_fails_the_export() {
        let dir = golden("persist-paging");
        std::fs::remove_file(dir.path().join("data/large-messages/1467.msg")).unwrap();
        let (result, _) = export(dir.path());
        assert_fails_naming(result, "1467.msg");
    }

    /// A page that is there but cannot be read (a directory in its place):
    /// Artemis prints the exception, skips the rest of paging and exits 0.
    #[test]
    fn an_unreadable_page_fails_the_export() {
        let dir = golden("persist-paging");
        let page = dir
            .path()
            .join("data/paging/72573e46-c4b9-11f1-a548-5613fc732e75/000000016.page");
        std::fs::remove_file(&page).unwrap();
        std::fs::create_dir(&page).unwrap();
        let (result, _) = export(dir.path());
        assert_fails_naming(result, "000000016.page");
    }
}
