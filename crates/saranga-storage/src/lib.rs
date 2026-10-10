//! The storage manager's journal records, on-disk compatible with Artemis
//! (`JournalStorageManager`, `AbstractJournalStorageManager`,
//! `JournalRecordIds` and `core/persistence/impl/journal/codec/*`).
//!
//! - [`ids`]: the user record types and message persister IDs.
//! - [`codec`] and [`message`]: one codec per record body;
//!   [`decode_record`] dispatches on the user record type.
//! - [`BatchingIdGenerator`]: the ID generator and its checkpoint records.
//! - [`describe`]: `artemis data print` (`PrintData`, `DescribeJournal`).

pub mod codec;
pub mod describe;
pub mod id_generator;
pub mod ids;
pub mod java;
pub mod message;
mod record;

pub use codec::RecordCodec;
pub use id_generator::{
    BatchingIdGenerator, CHECKPOINT_BATCH_SIZE, IdCounterStore, IdGeneratorStopped,
};
pub use record::{JournalKind, StorageRecord, decode_record};
pub use saranga_buffer::DecodeError;
