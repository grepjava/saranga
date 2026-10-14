//! The Artemis append-only journal, on-disk compatible with
//! `org.apache.activemq.artemis.core.journal.impl.JournalImpl`.
//!
//! [`format`] is the file and record layout. [`Journal`] is the journal:
//! loading with transaction recovery, appends with group commit, reclaim of
//! files whose records are all deleted, the file pool and compaction with
//! its rename-control file.

mod compact;
pub mod control;
pub mod format;
mod io;
mod journal;
mod retention;
mod sync;

pub use retention::{history_file_millis, local_date_millis};

use std::sync::Arc;

pub use io::Completion;
pub use journal::{
    Journal, JournalConfig, JournalFileInfo, LoadInfo, LoadResult, LoaderCallback,
    PreparedTransactionInfo, RecordInfo, Replicator, Retention,
};
pub use sync::SyncJournal;

#[derive(Debug, Clone, thiserror::Error)]
pub enum JournalError {
    #[error("I/O error: {0}")]
    Io(Arc<std::io::Error>),
    #[error("journal is corrupt: {0}")]
    Corrupt(String),
    #[error("{0}")]
    IllegalState(String),
    /// `recordLargerThanStoreMax`.
    #[error(
        "AMQ149005: Message of {size} bytes is bigger than the max record size of {max} bytes. You should try to move large application properties to the message body."
    )]
    RecordTooLarge { size: usize, max: usize },
    /// `ActiveMQShutdownException`: the journal is not loaded.
    #[error("Journal must be in state=LOADED, was [{0}]")]
    NotLoaded(&'static str),
}

impl JournalError {
    pub(crate) fn io(e: std::io::Error) -> Self {
        Self::Io(Arc::new(e))
    }
}

pub type Result<T> = std::result::Result<T, JournalError>;
