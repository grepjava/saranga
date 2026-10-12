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

use super::dirs::{CliLock, DataDirArgs};

#[derive(Args)]
pub struct ExportArgs {
    #[command(flatten)]
    dirs: DataDirArgs,
    /// Output name for the file.
    #[arg(long)]
    output: Option<PathBuf>,
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
    let _lock = CliLock::take(crate::client::instance_dir().as_deref())?;
    let d = args.dirs.resolve()?;
    let file_size = d
        .config
        .as_ref()
        .and_then(|c| c.journal.file_size)
        .map(|n| n.max(0) as usize);
    let out: Box<dyn Write> = match &args.output {
        Some(p) => {
            Box::new(std::fs::File::create(p).with_context(|| format!("creating {}", p.display()))?)
        }
        None => Box::new(std::io::stdout().lock()),
    };
    let mut exporter = Exporter {
        args: &args,
        large_messages: d.large_messages.clone(),
        queue_bindings: HashMap::new(),
        queue_order: Vec::new(),
        address_bindings: HashMap::new(),
        address_order: Vec::new(),
        messages: BTreeMap::new(),
        refs: HashMap::new(),
        cursor_acks: HashMap::new(),
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
        let body = large.map(|id| std::fs::read(self.large_file(id)).unwrap_or_default());
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
            eprintln!("Processing journal messages");
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
            let mut pages: Vec<i64> = std::fs::read_dir(&folder)
                .map(|entries| {
                    entries
                        .flatten()
                        .filter_map(|e| page_id_of(&e.file_name().to_string_lossy()))
                        .collect()
                })
                .unwrap_or_default();
            pages.sort_unstable();
            let Some(&first) = pages.first() else {
                continue;
            };
            for page_id in first..first + pages.len() as i64 {
                let path = folder.join(saranga_paging::page_file_name(page_id));
                let Ok(data) = std::fs::read(&path) else {
                    continue;
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
                            .is_some_and(|s| s.contains(&(page_id, nr as i32)));
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
        let c = &m.message;
        let x = &mut self.xml;
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
        for (key, value) in property_order(&c.properties, m.decoded) {
            x.empty("property")?;
            x.attr("name", &java::simple_string(key))?;
            x.attr("value", &property_text(value))?;
            if let Some(t) = property_type(value) {
                x.attr("type", t)?;
            }
        }
        x.end()?;
        x.start("queues")?;
        for q in queues {
            x.empty("queue")?;
            x.attr("name", q)?;
        }
        x.end()?;
        x.start("body")?;
        match &m.large {
            Some(file) => {
                x.attr("isLarge", "true")?;
                let body = std::fs::read(file).unwrap_or_default();
                // LARGE_MESSAGE_CHUNK_SIZE
                for chunk in body.chunks(1000) {
                    x.cdata(&java::base64_url(chunk))?;
                }
            }
            None => x.cdata(&java::base64_url(&c.body))?,
        }
        x.end()?;
        x.end()?;
        Ok(())
    }
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
}
