//! `ReplayManager`: the messages once sent to an address, read back from
//! the journal retention directory and routed again, to the same address or
//! another.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use saranga_buffer::SimpleString;
use saranga_journal::format::{self, FileHeader, Record};
use saranga_storage::ids;

use crate::Broker;
use crate::large::LargeBody;
use crate::message::{ServerMessage, now_millis};
use crate::storage::{StoredMessage, decode_large_message, decode_message};

/// `ActiveMQServerControl.replay`: the messages sent to `address` that
/// `filter` matches, from the retention files dated between `start` and
/// `end` (`yyyyMMddHHmmss`, local time; both or neither), are routed to
/// `target` (`address` when empty) under new message IDs.
pub async fn replay(
    broker: &Broker,
    start: Option<i64>,
    end: Option<i64>,
    address: Option<String>,
    target: Option<String>,
    filter: Option<String>,
) -> Result<(), String> {
    let storage = broker.storage().cloned();
    let Some((storage, retention)) = storage.and_then(|s| {
        let r = s.retention()?.clone();
        Some((s, r))
    }) else {
        return Err("AMQ229239: There is no retention configured. In order to use the replay method you must specify journal-retention-directory element on the broker.xml".to_owned());
    };
    let Some(source) = address else {
        return Err("sourceAddress".to_owned());
    };
    let target = target
        .filter(|t| !t.trim().is_empty())
        .unwrap_or_else(|| source.clone());
    let filter = saranga_filter::Filter::create(filter.as_deref()).map_err(|_| {
        crate::error::BrokerError::invalid_filter(filter.as_deref().unwrap_or_default()).message
    })?;
    tracing::debug!("Replay start::sourceAddress={source}");

    storage.force_backup().map_err(|e| e.to_string())?;

    let mut replay = Replay {
        broker,
        source,
        target,
        filter,
        bodies: HashMap::new(),
    };
    for file in history_files(&retention.directory) {
        if start.is_some() || end.is_some() {
            let Some(t) =
                saranga_journal::history_file_millis(PREFIX, &file.name, retention.local_offset)
            else {
                continue;
            };
            if start.is_some_and(|s| t < s) || end.is_some_and(|e| t > e) {
                tracing::debug!("File {} being skipped", file.name);
                continue;
            }
        }
        tracing::debug!("Reading retention file {}", file.name);
        for record in file.records() {
            replay.read(&file, record).await;
        }
    }
    tracing::debug!("Replay done::sourceAddress={}", replay.source);
    Ok(())
}

/// The retention copies are named after the message journal's files.
const PREFIX: &str = "activemq-data";
const EXTENSION: &str = "amq";

/// A retention copy.
#[derive(Clone)]
struct HistoryFile {
    path: PathBuf,
    name: String,
    file_id: i64,
}

impl HistoryFile {
    /// `readJournalFile`: its records, events included.
    fn records(&self) -> Vec<Record> {
        let Ok(bytes) = std::fs::read(&self.path) else {
            return Vec::new();
        };
        let Ok(header) = FileHeader::decode(&bytes, 0) else {
            return Vec::new();
        };
        format::scan(
            &bytes,
            format::record_id_of(self.file_id),
            header.journal_version,
            false,
        )
        .map(|s| s.records.into_iter().map(|r| r.record).collect())
        .unwrap_or_default()
    }
}

/// `orderFiles` over the retention directory: by file ID.
fn history_files(dir: &Path) -> Vec<HistoryFile> {
    let mut files: Vec<HistoryFile> = std::fs::read_dir(dir)
        .map(|d| {
            d.flatten()
                .filter_map(|e| {
                    let name = e.file_name().into_string().ok()?;
                    if !name.starts_with(PREFIX) || !name.ends_with(&format!(".{EXTENSION}")) {
                        return None;
                    }
                    let mut h = [0u8; format::SIZE_HEADER];
                    let f = std::fs::File::open(e.path()).ok()?;
                    std::io::Read::read_exact(&mut &f, &mut h).ok()?;
                    let file_id = FileHeader::decode(&h, 0).ok()?.file_id;
                    Some(HistoryFile {
                        path: e.path(),
                        name,
                        file_id,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    files.sort_by_key(|f| f.file_id);
    files
}

struct Replay<'a> {
    broker: &'a Broker,
    source: String,
    target: String,
    filter: Option<saranga_filter::Filter>,
    /// The files holding each large message's body events, in the order
    /// they were read.
    bodies: HashMap<i64, BTreeMap<i64, HistoryFile>>,
}

impl Replay<'_> {
    async fn read(&mut self, file: &HistoryFile, record: Record) {
        let (id, user_type, data) = match record {
            Record::Event {
                id,
                user_type: ids::ADD_MESSAGE_BODY,
                ..
            } => {
                self.bodies
                    .entry(id)
                    .or_default()
                    .insert(file.file_id, file.clone());
                return;
            }
            Record::Event {
                id,
                user_type,
                data,
            }
            | Record::Add {
                id,
                user_type,
                data,
            }
            | Record::Update {
                id,
                user_type,
                data,
            }
            | Record::AddTx {
                id,
                user_type,
                data,
                ..
            }
            | Record::UpdateTx {
                id,
                user_type,
                data,
                ..
            } => (id, user_type, data),
            _ => return,
        };
        let stored = match user_type {
            ids::ADD_LARGE_MESSAGE => decode_large_message(&data).map(|m| (m, true)),
            ids::ADD_MESSAGE_PROTOCOL => match decode_message(&data) {
                Ok(StoredMessage::Core(m)) => Ok((m, false)),
                Ok(StoredMessage::Native(record)) => {
                    self.route_native(id, record).await;
                    return;
                }
                Err(e) => Err(e),
            },
            _ => return,
        };
        match stored {
            Ok((m, large)) => self.route_core(m, large).await,
            Err(e) => tracing::warn!("replay: message {id}: {e}"),
        }
    }

    /// `messageMatch`: sent to the source address, and matching the filter.
    fn matches(&self, message: &ServerMessage) -> bool {
        let address = message.core().address.as_ref().map(|a| a.to_string_lossy());
        address.as_deref() == Some(self.source.as_str())
            && self
                .filter
                .as_ref()
                .is_none_or(|f| f.matches(message as &dyn saranga_filter::Filterable))
    }

    async fn route_core(&mut self, core: saranga_message::CoreMessage, large: bool) {
        let original = core.message_id;
        let message = if large {
            // The filter sees the headers and properties; the body follows
            // only for a match.
            let view = ServerMessage::new(core.clone());
            if !self.matches(&view) {
                self.bodies.remove(&original);
                return;
            }
            let id = self.broker.post_office().ids().next();
            let Some(body) = self.body(original, id, core.durable) else {
                return;
            };
            ServerMessage::new_large(core, body)
        } else {
            let m = ServerMessage::new(core);
            if !self.matches(&m) {
                return;
            }
            m
        };
        self.send(message).await;
    }

    async fn route_native(
        &mut self,
        original: i64,
        record: saranga_storage::message::PersistedMessage,
    ) {
        let decoders = self.broker.post_office().paging().decoders();
        let message = if record.is_large() {
            let id = self.broker.post_office().ids().next();
            let Some(body) = self.body(original, id, true) else {
                return;
            };
            decoders.decode_large(&record, body)
        } else {
            decoders.decode(&record)
        };
        match message {
            Ok(m) if self.matches(&m) => self.send(m).await,
            Ok(_) => {
                self.bodies.remove(&original);
            }
            Err(e) => tracing::warn!("replay: message {original}: {e}"),
        }
    }

    /// `readLargeMessageBody`: the body events of `original`, written to a
    /// file for the message's new ID.
    fn body(&mut self, original: i64, id: i64, durable: bool) -> Option<Arc<LargeBody>> {
        let mut writer = match self.broker.large_messages().create(id, durable) {
            Ok(w) => w,
            Err(e) => {
                tracing::warn!("replay: large message {original}: {e}");
                return None;
            }
        };
        for file in self.bodies.remove(&original).unwrap_or_default().values() {
            for r in file.records() {
                if let Record::Event {
                    id,
                    user_type: ids::ADD_MESSAGE_BODY,
                    data,
                } = r
                    && id == original
                    && let Err(e) = writer.append(&data)
                {
                    tracing::warn!("replay: large message {original}: {e}");
                    return None;
                }
            }
        }
        match writer.finish() {
            Ok(b) => Some(b),
            Err(e) => {
                tracing::warn!("replay: large message {original}: {e}");
                None
            }
        }
    }

    /// `route`: under a new message ID, to the target address.
    async fn send(&self, message: Arc<ServerMessage>) {
        let po = self.broker.post_office();
        let mut core = message.core().clone();
        core.message_id = match message.large() {
            // The body's file is named after the ID it was given.
            Some(body) => body.id(),
            None => po.ids().next(),
        };
        core.address = Some(SimpleString::from(self.target.as_str()));
        crate::message::apply_expiry_delay(&mut core, &po.settings(&self.target), now_millis());
        let message = message.with_core(core);
        po.deliver_internal(message, &self.target).await;
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn dates_are_local() {
        assert_eq!(
            saranga_journal::local_date_millis("20261012103000", |_| 3600),
            Some(1_791_801_000_000 - 3_600_000)
        );
        assert_eq!(saranga_journal::local_date_millis("2026", |_| 0), None);
    }
}
