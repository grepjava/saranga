//! The paging section of `data print` (`PrintData.printPages`): every
//! store's pages, each message with the queues it was routed to and what
//! the cursor records say about them.

use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use saranga_message::CoreMessage;
use saranga_storage::RecordCodec;
use saranga_storage::codec::{CursorAckRecordEncoding, PageTransactionEncoding};
use saranga_storage::describe::{JournalDescription, core_message_to_string, properties_to_string};
use saranga_storage::ids;
use saranga_storage::java;
use saranga_storage::message::PersistedMessage;

use crate::{PagedBody, PagedMessage, page_id_of, read_page};

/// `PageCursorsInfo`: the cursor records of the message journal.
#[derive(Default)]
struct CursorsInfo {
    /// Per queue: acknowledged (page, message number) positions.
    acks: HashMap<i64, HashSet<(i64, i32)>>,
    /// Per queue: completed pages.
    complete: HashMap<i64, HashSet<i64>>,
    /// Transactions with a `PAGE_TRANSACTION` record.
    page_txs: HashSet<i64>,
}

/// `calculateCursorsInfo`.
fn cursors_info(messages: &JournalDescription) -> CursorsInfo {
    let mut info = CursorsInfo::default();
    for r in &messages.records {
        match r.user_record_type {
            ids::ACKNOWLEDGE_CURSOR => {
                if let Ok(e) = CursorAckRecordEncoding::decode(&r.data) {
                    info.acks
                        .entry(e.queue_id)
                        .or_default()
                        .insert((e.page_nr, e.message_nr));
                }
            }
            ids::PAGE_CURSOR_COMPLETE => {
                if let Ok(e) = CursorAckRecordEncoding::decode(&r.data)
                    && !info
                        .complete
                        .entry(e.queue_id)
                        .or_default()
                        .insert(e.page_nr)
                {
                    eprintln!(
                        "Page {} has been already set as complete on queue {}",
                        e.page_nr, e.queue_id
                    );
                }
            }
            ids::PAGE_TRANSACTION => {
                if let Ok(e) = PageTransactionEncoding::decode(&r.data) {
                    info.page_txs.insert(e.transaction_id);
                }
            }
            _ => {}
        }
    }
    info
}

/// A store as `PagingManagerImpl.getStoreNames` lists it.
struct Store {
    address: String,
    folder: PathBuf,
}

/// `SimpleString.hashCode`: over the UTF-16LE bytes.
fn simple_string_hash(s: &str) -> i32 {
    s.encode_utf16()
        .flat_map(|c| [c as u8, (c >> 8) as u8])
        .fold(0i32, |h, b| {
            h.wrapping_mul(31).wrapping_add(i32::from(b as i8))
        })
}

/// The stores in `ConcurrentHashMap` iteration order: by bucket of the
/// spread hash. Stores sharing a bucket follow Java's directory listing
/// order, which Saranga cannot know; they are ordered by folder name.
fn stores(paging: &Path) -> Vec<Store> {
    let Ok(entries) = std::fs::read_dir(paging) else {
        return Vec::new();
    };
    let mut stores: Vec<Store> = entries
        .flatten()
        .filter_map(|e| {
            let folder = e.path();
            let text = std::fs::read_to_string(folder.join(crate::ADDRESS_FILE)).ok()?;
            let address = text.lines().next().filter(|a| !a.is_empty())?.to_owned();
            Some(Store { address, folder })
        })
        .collect();
    let mut n = 16usize;
    while stores.len() >= n - (n >> 2) {
        n *= 2;
    }
    let bucket = |s: &Store| {
        let h = simple_string_hash(&s.address);
        ((h ^ ((h as u32) >> 16) as i32) & 0x7fff_ffff) as usize & (n - 1)
    };
    stores.sort_by(|a, b| bucket(a).cmp(&bucket(b)).then(a.folder.cmp(&b.folder)));
    stores
}

/// `NullStorageLargeServerMessage.toString` (`data print` reads large
/// messages with a `NullStorageManager`).
fn null_storage_large_to_string(m: &CoreMessage) -> String {
    format!(
        "NullStorageLargeServerMessage[messageID={}, durable={}, address={},properties={}]",
        m.message_id,
        m.durable,
        java::nullable(m.address.as_ref()),
        properties_to_string(&m.properties)
    )
}

/// The message part of a page line: `,userMessageID=<id>, msg=<message>`,
/// or with `safe` its class and size only.
fn describe(m: &PagedMessage, record: &[u8], safe: bool) -> String {
    match &m.body {
        PagedBody::Message(PersistedMessage::Core(c)) => {
            let msg = c.to_message();
            let size = c.buffer_size();
            if safe {
                format!(", msg=CoreMessage(safe data, size={size})")
            } else {
                format!(
                    ",userMessageID={}, msg={}",
                    msg.user_id.map(|u| java::uuid(&u)).unwrap_or_default(),
                    core_message_to_string(&msg, size, record)
                )
            }
        }
        PagedBody::CoreLarge(msg) => {
            if safe {
                ", msg=NullStorageLargeServerMessage(safe data)".to_owned()
            } else {
                format!(
                    ",userMessageID={}, msg={}",
                    msg.user_id.map(|u| java::uuid(&u)).unwrap_or_default(),
                    null_storage_large_to_string(msg)
                )
            }
        }
        PagedBody::Message(other) | PagedBody::OtherLarge(other) => {
            let class = if other.is_large() {
                "AMQPLargeMessage"
            } else {
                "AMQPStandardMessage"
            };
            if safe {
                format!(", msg={class}(safe data)")
            } else {
                format!(
                    ",userMessageID=, msg={class}(<AMQP sections not printed by saranga>)@{}",
                    java::bytes_hash(record)
                )
            }
        }
    }
}

/// `PrintData.printPages`: `max_pages` stops after that many pages of a
/// store.
pub fn print_pages(
    out: &mut dyn Write,
    paging: &Path,
    messages: &JournalDescription,
    bindings: Option<&JournalDescription>,
    safe: bool,
    max_pages: Option<u32>,
) -> io::Result<()> {
    let info = cursors_info(messages);
    let existing: HashSet<i64> = bindings
        .and_then(|b| b.queue_bindings.as_ref())
        .map(|q| q.iter().map(|(id, _)| *id).collect())
        .unwrap_or_default();
    for store in stores(paging) {
        writeln!(
            out,
            "####################################################################################################"
        )?;
        writeln!(
            out,
            "Exploring store {} folder = {}",
            store.address,
            store.folder.display()
        )?;
        let mut pages: Vec<i64> = std::fs::read_dir(&store.folder)?
            .flatten()
            .filter_map(|e| e.file_name().to_str().and_then(page_id_of))
            .collect();
        pages.sort();
        let number = pages.len() as i64;
        let current = pages.last().copied().unwrap_or(0);
        let mut pgid = pages.first().copied().unwrap_or(i64::MAX);
        writeln!(
            out,
            "Number of pages ::{number}, Current writing page ::{current}"
        )?;
        for pg in 0..number {
            if let Some(max) = max_pages
                && pg > i64::from(max)
            {
                writeln!(
                    out,
                    "******* Giving up at Page {pgid}, System has a total of {number} pages"
                )?;
                break;
            }
            let path = |id: i64| store.folder.join(crate::page_file_name(id));
            while !path(pgid).exists() && pgid < current {
                pgid += 1;
            }
            writeln!(out, "*******   Page {pgid}")?;
            let data = std::fs::read(path(pgid)).unwrap_or_default();
            let page = read_page(&data);
            let mut at = 0usize;
            for (msg_id, m) in page.messages.iter().enumerate() {
                // The record bytes, for the identity hash Java prints.
                let size =
                    i32::from_be_bytes(data[at + 1..at + 5].try_into().expect("4 bytes")) as usize;
                let record = &data[at..at + size + crate::SIZE_RECORD];
                at += size + crate::SIZE_RECORD;
                let msg_id = msg_id as i32;
                write!(
                    out,
                    "pg={pgid}, msg={msg_id},pgTX={}{}",
                    m.transaction_id,
                    describe(m, record, safe)
                )?;
                write!(out, ",Queues = ")?;
                let mut ack_count = 0;
                for (i, q) in m.queue_ids.iter().enumerate() {
                    write!(out, "{q}")?;
                    let mut acked = info
                        .acks
                        .get(q)
                        .is_some_and(|a| a.contains(&(pgid, msg_id)));
                    if acked {
                        write!(out, " (ACK)")?;
                    }
                    if info.complete.get(q).is_some_and(|c| c.contains(&pgid)) {
                        acked = true;
                        write!(out, " (PG-COMPLETE)")?;
                    }
                    if !existing.contains(q) {
                        write!(out, " (N/A) ")?;
                        acked = true;
                    }
                    if acked {
                        ack_count += 1;
                    } else {
                        write!(out, " (OK) ")?;
                    }
                    if i + 1 < m.queue_ids.len() {
                        write!(out, ",")?;
                    }
                }
                if m.transaction_id >= 0 && !info.page_txs.contains(&m.transaction_id) {
                    write!(out, ", **PG_TX_NOT_FOUND**")?;
                }
                writeln!(out)?;
                if ack_count != m.queue_ids.len() {
                    writeln!(
                        out,
                        "^^^ Previous record has {ack_count} acked queues and {} queues routed",
                        m.queue_ids.len()
                    )?;
                    writeln!(out)?;
                }
            }
            pgid += 1;
        }
    }
    Ok(())
}
