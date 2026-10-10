//! The journal file format: file header, record layouts and the tolerant
//! file scanner. Ported from `JournalImpl` (constants, `readJournalFile`,
//! `writeHeader`, `readFileHeader`) and `impl/dataformat/*`.
//!
//! Every record starts with its type byte, the 31-bit record ID of the file
//! it was written to and a compact count, and ends with a check size equal to
//! the record's total length. The scanner skips anything that does not
//! validate, byte by byte, so stale records left by a previous use of a
//! reused file (which carry another file ID) and torn writes are ignored.

use crate::JournalError;

/// `JournalImpl.FORMAT_VERSION`.
pub const FORMAT_VERSION: i32 = 2;
/// `JournalImpl.COMPATIBLE_VERSIONS`: versions that load but are not written.
const COMPATIBLE_VERSIONS: [i32; 1] = [1];

/// `JournalImpl.MIN_FILE_SIZE`.
pub const MIN_FILE_SIZE: usize = 1024;
/// Journal version (int), user version (int), file ID (long).
pub const SIZE_HEADER: usize = 16;

const SIZE_BYTE: usize = 1;
const SIZE_INT: usize = 4;
const SIZE_LONG: usize = 8;
/// Record type, file ID and the trailing check size.
const BASIC_SIZE: usize = SIZE_BYTE + SIZE_INT + SIZE_INT;
pub const SIZE_ADD_RECORD: usize = BASIC_SIZE + SIZE_LONG + SIZE_BYTE + SIZE_INT;
pub const SIZE_ADD_RECORD_TX: usize = BASIC_SIZE + SIZE_LONG + SIZE_BYTE + SIZE_LONG + SIZE_INT;
pub const SIZE_DELETE_RECORD_TX: usize = BASIC_SIZE + SIZE_LONG + SIZE_LONG + SIZE_INT;
pub const SIZE_DELETE_RECORD: usize = BASIC_SIZE + SIZE_LONG;
pub const SIZE_COMPLETE_TRANSACTION_RECORD: usize = BASIC_SIZE + SIZE_LONG + SIZE_INT;
pub const SIZE_PREPARE_RECORD: usize = SIZE_COMPLETE_TRANSACTION_RECORD + SIZE_INT;
pub const SIZE_COMMIT_RECORD: usize = SIZE_COMPLETE_TRANSACTION_RECORD;
pub const SIZE_ROLLBACK_RECORD: usize = BASIC_SIZE + SIZE_LONG;

// Record markers.
pub const EVENT_RECORD: u8 = 10;
pub const ADD_RECORD: u8 = 11;
pub const UPDATE_RECORD: u8 = 12;
pub const ADD_RECORD_TX: u8 = 13;
pub const UPDATE_RECORD_TX: u8 = 14;
pub const DELETE_RECORD_TX: u8 = 15;
pub const DELETE_RECORD: u8 = 16;
pub const PREPARE_RECORD: u8 = 17;
pub const COMMIT_RECORD: u8 = 18;
pub const ROLLBACK_RECORD: u8 = 19;

/// The record ID a file's records carry: `(int) (fileID & Integer.MAX_VALUE)`.
pub fn record_id_of(file_id: i64) -> i32 {
    (file_id & i32::MAX as i64) as i32
}

/// A journal record, as written by `JournalAddRecord`, `JournalAddRecordTX`,
/// `JournalDeleteRecord`, `JournalDeleteRecordTX`, `JournalCompleteRecordTX`
/// and `JournalRollbackRecordTX`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    /// `appendAddEvent`: an add record that is never tracked as live.
    Event {
        id: i64,
        user_type: u8,
        data: Vec<u8>,
    },
    Add {
        id: i64,
        user_type: u8,
        data: Vec<u8>,
    },
    Update {
        id: i64,
        user_type: u8,
        data: Vec<u8>,
    },
    Delete {
        id: i64,
    },
    AddTx {
        tx: i64,
        id: i64,
        user_type: u8,
        data: Vec<u8>,
    },
    UpdateTx {
        tx: i64,
        id: i64,
        user_type: u8,
        data: Vec<u8>,
    },
    /// The optional body is the `EncodingSupport` passed to
    /// `appendDeleteRecordTransactional`; empty when there is none.
    DeleteTx {
        tx: i64,
        id: i64,
        data: Vec<u8>,
    },
    /// `num_records`: the transaction's records in the file holding this
    /// record (see `JournalCompleteRecordTX`). `data` is the prepare's extra
    /// data (the Xid for XA).
    Prepare {
        tx: i64,
        num_records: i32,
        data: Vec<u8>,
    },
    Commit {
        tx: i64,
        num_records: i32,
    },
    Rollback {
        tx: i64,
    },
}

impl Record {
    pub fn marker(&self) -> u8 {
        match self {
            Record::Event { .. } => EVENT_RECORD,
            Record::Add { .. } => ADD_RECORD,
            Record::Update { .. } => UPDATE_RECORD,
            Record::Delete { .. } => DELETE_RECORD,
            Record::AddTx { .. } => ADD_RECORD_TX,
            Record::UpdateTx { .. } => UPDATE_RECORD_TX,
            Record::DeleteTx { .. } => DELETE_RECORD_TX,
            Record::Prepare { .. } => PREPARE_RECORD,
            Record::Commit { .. } => COMMIT_RECORD,
            Record::Rollback { .. } => ROLLBACK_RECORD,
        }
    }

    /// `getEncodeSize()`: the record's full length, also its check size.
    pub fn encode_size(&self) -> usize {
        match self {
            Record::Event { data, .. } | Record::Add { data, .. } | Record::Update { data, .. } => {
                SIZE_ADD_RECORD + data.len() + 1
            }
            Record::AddTx { data, .. } | Record::UpdateTx { data, .. } => {
                SIZE_ADD_RECORD_TX + data.len() + 1
            }
            Record::DeleteTx { data, .. } => SIZE_DELETE_RECORD_TX + data.len() + 1,
            Record::Delete { .. } => SIZE_DELETE_RECORD + 1,
            Record::Prepare { data, .. } => SIZE_PREPARE_RECORD + data.len() + 1,
            Record::Commit { .. } => SIZE_COMMIT_RECORD + 1,
            Record::Rollback { .. } => SIZE_ROLLBACK_RECORD + 1,
        }
    }

    /// The transaction ID, for transactional records.
    pub fn tx(&self) -> Option<i64> {
        match self {
            Record::AddTx { tx, .. }
            | Record::UpdateTx { tx, .. }
            | Record::DeleteTx { tx, .. }
            | Record::Prepare { tx, .. }
            | Record::Commit { tx, .. }
            | Record::Rollback { tx } => Some(*tx),
            _ => None,
        }
    }

    /// Sets the per-file record count of a prepare or commit record
    /// (`setNumberOfRecords`); other records ignore it.
    pub fn set_num_records(&mut self, n: i32) {
        if let Record::Prepare { num_records, .. } | Record::Commit { num_records, .. } = self {
            *num_records = n;
        }
    }

    /// Appends the record to `out`, tagged with the record ID of the file it
    /// goes to. Compact counts above `Byte.MAX_VALUE` saturate, as
    /// `setCompactCount` does.
    pub fn encode(&self, file_record_id: i32, compact_count: i16, out: &mut Vec<u8>) {
        let start = out.len();
        out.push(self.marker());
        out.extend_from_slice(&file_record_id.to_be_bytes());
        out.push(compact_count.clamp(i16::MIN, i8::MAX as i16) as i8 as u8);
        match self {
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
            } => {
                out.extend_from_slice(&id.to_be_bytes());
                out.extend_from_slice(&(data.len() as i32).to_be_bytes());
                out.push(*user_type);
                out.extend_from_slice(data);
            }
            Record::AddTx {
                tx,
                id,
                user_type,
                data,
            }
            | Record::UpdateTx {
                tx,
                id,
                user_type,
                data,
            } => {
                out.extend_from_slice(&tx.to_be_bytes());
                out.extend_from_slice(&id.to_be_bytes());
                out.extend_from_slice(&(data.len() as i32).to_be_bytes());
                out.push(*user_type);
                out.extend_from_slice(data);
            }
            Record::DeleteTx { tx, id, data } => {
                out.extend_from_slice(&tx.to_be_bytes());
                out.extend_from_slice(&id.to_be_bytes());
                out.extend_from_slice(&(data.len() as i32).to_be_bytes());
                out.extend_from_slice(data);
            }
            Record::Delete { id } => out.extend_from_slice(&id.to_be_bytes()),
            Record::Prepare {
                tx,
                num_records,
                data,
            } => {
                out.extend_from_slice(&tx.to_be_bytes());
                out.extend_from_slice(&num_records.to_be_bytes());
                out.extend_from_slice(&(data.len() as i32).to_be_bytes());
                out.extend_from_slice(data);
            }
            Record::Commit { tx, num_records } => {
                out.extend_from_slice(&tx.to_be_bytes());
                out.extend_from_slice(&num_records.to_be_bytes());
            }
            Record::Rollback { tx } => out.extend_from_slice(&tx.to_be_bytes()),
        }
        out.extend_from_slice(&(self.encode_size() as i32).to_be_bytes());
        debug_assert_eq!(out.len() - start, self.encode_size());
    }
}

/// A file header: `writeHeader`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHeader {
    pub journal_version: i32,
    pub user_version: i32,
    pub file_id: i64,
}

impl FileHeader {
    pub fn new(user_version: i32, file_id: i64) -> Self {
        Self {
            journal_version: FORMAT_VERSION,
            user_version,
            file_id,
        }
    }

    pub fn encode(&self) -> [u8; SIZE_HEADER] {
        let mut b = [0u8; SIZE_HEADER];
        b[0..4].copy_from_slice(&self.journal_version.to_be_bytes());
        b[4..8].copy_from_slice(&self.user_version.to_be_bytes());
        b[8..16].copy_from_slice(&self.file_id.to_be_bytes());
        b
    }

    /// `readFileHeader`: rejects unknown journal versions
    /// (`journalFileMisMatch`, AMQ149002) and a user version other than the
    /// expected one (`journalDifferentVersion`, AMQ149001).
    pub fn decode(b: &[u8], expected_user_version: i32) -> Result<Self, JournalError> {
        if b.len() < SIZE_HEADER {
            return Err(JournalError::Corrupt("file shorter than its header".into()));
        }
        let journal_version = i32::from_be_bytes(b[0..4].try_into().unwrap());
        if journal_version != FORMAT_VERSION && !COMPATIBLE_VERSIONS.contains(&journal_version) {
            return Err(JournalError::Corrupt(
                "AMQ149002: Journal files version mismatch. You should export the data from the previous version and import it as explained on the user's manual".into(),
            ));
        }
        let user_version = i32::from_be_bytes(b[4..8].try_into().unwrap());
        if user_version != expected_user_version {
            return Err(JournalError::Corrupt(
                "AMQ149001: Journal data belong to a different version".into(),
            ));
        }
        Ok(Self {
            journal_version,
            user_version,
            file_id: i64::from_be_bytes(b[8..16].try_into().unwrap()),
        })
    }
}

/// A record found by [`scan`]: `RecordInfo` plus where it was.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedRecord {
    pub record: Record,
    pub compact_count: i16,
    /// Offset of the record in the file.
    pub position: usize,
}

/// What [`scan`] found in one file.
#[derive(Debug, Clone, Default)]
pub struct ScanResult {
    pub records: Vec<ScannedRecord>,
    /// The position after the last valid record (`readJournalFile`'s return
    /// value): where appends continue.
    pub last_data_pos: usize,
    /// `markAsDataFile`: the file holds something that looked like a record
    /// of this file but did not validate. The loader keeps such a file as a
    /// data file even without valid records.
    pub damaged: bool,
}

fn invalid_size(file_size: usize, pos: usize, size: i64) -> bool {
    size < 0 || pos as i64 + size > file_size as i64
}

fn be_i32(b: &[u8], at: usize) -> i32 {
    i32::from_be_bytes(b[at..at + 4].try_into().unwrap())
}

fn be_i64(b: &[u8], at: usize) -> i64 {
    i64::from_be_bytes(b[at..at + 8].try_into().unwrap())
}

/// `readJournalFile`: scans a whole file for valid records of the file with
/// record ID `file_record_id`. With `reclaimed` set, records of any file ID
/// are accepted (used for the compaction control file). Returns `None` for
/// a file shorter than its header.
pub fn scan(
    buf: &[u8],
    file_record_id: i32,
    journal_version: i32,
    reclaimed: bool,
) -> Option<ScanResult> {
    let file_size = buf.len();
    if file_size < SIZE_HEADER {
        return None;
    }
    let mut out = ScanResult {
        last_data_pos: SIZE_HEADER,
        ..Default::default()
    };
    let mut p = SIZE_HEADER;
    while p < file_size {
        let pos = p;
        let record_type = buf[p];
        p += 1;
        if !(EVENT_RECORD..=ROLLBACK_RECORD).contains(&record_type) {
            continue;
        }
        macro_rules! skip {
            (mark) => {{
                out.damaged = true;
                p = pos + 1;
                continue;
            }};
            () => {{
                p = pos + 1;
                continue;
            }};
        }
        if invalid_size(file_size, p, SIZE_INT as i64) {
            skip!(mark);
        }
        let read_file_id = be_i32(buf, p);
        p += SIZE_INT;
        if read_file_id != file_record_id && !reclaimed {
            skip!();
        }
        let mut compact_count: i16 = 0;
        if journal_version >= 2 {
            if invalid_size(file_size, p, SIZE_BYTE as i64) {
                skip!(mark);
            }
            compact_count = buf[p] as i8 as i16;
            p += 1;
        }
        let is_complete = matches!(
            record_type,
            PREPARE_RECORD | COMMIT_RECORD | ROLLBACK_RECORD
        );
        let is_tx = is_complete
            || matches!(
                record_type,
                ADD_RECORD_TX | UPDATE_RECORD_TX | DELETE_RECORD_TX
            );
        let mut tx = 0i64;
        if is_tx {
            if invalid_size(file_size, p, SIZE_LONG as i64) {
                skip!(mark);
            }
            tx = be_i64(buf, p);
            p += SIZE_LONG;
        }
        let mut id = 0i64;
        if !is_complete {
            if invalid_size(file_size, p, SIZE_LONG as i64) {
                skip!(mark);
            }
            id = be_i64(buf, p);
            p += SIZE_LONG;
        }
        let mut variable_size: i64 = 0;
        let mut prepare_extra: i64 = 0;
        let mut user_type = 0u8;
        let mut body: &[u8] = &[];
        if record_type <= DELETE_RECORD_TX {
            if invalid_size(file_size, p, SIZE_INT as i64) {
                skip!(mark);
            }
            variable_size = be_i32(buf, p) as i64;
            p += SIZE_INT;
            if record_type != DELETE_RECORD_TX {
                if invalid_size(file_size, p, 1) {
                    skip!();
                }
                user_type = buf[p];
                p += 1;
            }
            if invalid_size(file_size, p, variable_size) {
                skip!();
            }
            body = &buf[p..p + variable_size as usize];
            p += variable_size as usize;
        }
        let mut num_records = 0i32;
        if record_type == PREPARE_RECORD || record_type == COMMIT_RECORD {
            if invalid_size(file_size, p, SIZE_INT as i64) {
                skip!();
            }
            num_records = be_i32(buf, p);
            p += SIZE_INT;
            if record_type == PREPARE_RECORD {
                if invalid_size(file_size, p, SIZE_INT as i64) {
                    skip!();
                }
                prepare_extra = be_i32(buf, p) as i64;
                p += SIZE_INT;
            }
            variable_size = 0;
        }
        let record_size = record_size(record_type, journal_version) as i64;
        let total = record_size + variable_size + prepare_extra;
        if invalid_size(file_size, pos, total) {
            skip!(mark);
        }
        let check_at = pos + total as usize - SIZE_INT;
        if be_i32(buf, check_at) as i64 != total {
            skip!(mark);
        }
        let record = match record_type {
            EVENT_RECORD => Record::Event {
                id,
                user_type,
                data: body.to_vec(),
            },
            ADD_RECORD => Record::Add {
                id,
                user_type,
                data: body.to_vec(),
            },
            UPDATE_RECORD => Record::Update {
                id,
                user_type,
                data: body.to_vec(),
            },
            DELETE_RECORD => Record::Delete { id },
            ADD_RECORD_TX => Record::AddTx {
                tx,
                id,
                user_type,
                data: body.to_vec(),
            },
            UPDATE_RECORD_TX => Record::UpdateTx {
                tx,
                id,
                user_type,
                data: body.to_vec(),
            },
            DELETE_RECORD_TX => Record::DeleteTx {
                tx,
                id,
                data: body.to_vec(),
            },
            PREPARE_RECORD => Record::Prepare {
                tx,
                num_records,
                data: buf[p..p + prepare_extra as usize].to_vec(),
            },
            COMMIT_RECORD => Record::Commit { tx, num_records },
            _ => Record::Rollback { tx },
        };
        out.records.push(ScannedRecord {
            record,
            compact_count,
            position: pos,
        });
        p = pos + total as usize;
        out.last_data_pos = p;
    }
    Some(out)
}

/// `getRecordSize`: the fixed part of a record of this type.
fn record_size(record_type: u8, journal_version: i32) -> usize {
    let size = match record_type {
        ADD_RECORD | EVENT_RECORD | UPDATE_RECORD => SIZE_ADD_RECORD,
        ADD_RECORD_TX | UPDATE_RECORD_TX => SIZE_ADD_RECORD_TX,
        DELETE_RECORD => SIZE_DELETE_RECORD,
        DELETE_RECORD_TX => SIZE_DELETE_RECORD_TX,
        PREPARE_RECORD => SIZE_PREPARE_RECORD,
        COMMIT_RECORD => SIZE_COMMIT_RECORD,
        _ => SIZE_ROLLBACK_RECORD,
    };
    if journal_version >= 2 { size + 1 } else { size }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_with(records: &[(Record, i16)], file_id: i64, size: usize) -> Vec<u8> {
        let mut b = FileHeader::new(0, file_id).encode().to_vec();
        for (r, cc) in records {
            r.encode(record_id_of(file_id), *cc, &mut b);
        }
        b.resize(size, 0);
        b
    }

    fn all_kinds() -> Vec<Record> {
        vec![
            Record::Event {
                id: 1,
                user_type: 45,
                data: vec![1, 2],
            },
            Record::Add {
                id: 2,
                user_type: 21,
                data: vec![3; 10],
            },
            Record::Update {
                id: 2,
                user_type: 34,
                data: vec![],
            },
            Record::Delete { id: 2 },
            Record::AddTx {
                tx: 9,
                id: 3,
                user_type: 45,
                data: vec![7; 5],
            },
            Record::UpdateTx {
                tx: 9,
                id: 3,
                user_type: 33,
                data: vec![1],
            },
            Record::DeleteTx {
                tx: 9,
                id: 3,
                data: vec![],
            },
            Record::DeleteTx {
                tx: 9,
                id: 4,
                data: vec![5, 6],
            },
            Record::Prepare {
                tx: 9,
                num_records: 4,
                data: vec![1, 2, 3],
            },
            Record::Commit {
                tx: 9,
                num_records: 4,
            },
            Record::Rollback { tx: 10 },
        ]
    }

    #[test]
    fn roundtrip_every_record_type() {
        let recs: Vec<_> = all_kinds().into_iter().map(|r| (r, 0)).collect();
        let buf = file_with(&recs, 5, 4096);
        let res = scan(&buf, 5, FORMAT_VERSION, false).unwrap();
        let got: Vec<_> = res.records.iter().map(|r| r.record.clone()).collect();
        assert_eq!(got, all_kinds());
        assert!(!res.damaged);
        let len: usize = all_kinds().iter().map(Record::encode_size).sum();
        assert_eq!(res.last_data_pos, SIZE_HEADER + len);
    }

    #[test]
    fn records_of_another_file_id_are_skipped() {
        let mut buf = file_with(
            &[(
                Record::Add {
                    id: 1,
                    user_type: 1,
                    data: vec![1],
                },
                0,
            )],
            7,
            1024,
        );
        // A stale record from a previous use of the file, after the live one.
        let mut stale = Vec::new();
        Record::Add {
            id: 2,
            user_type: 1,
            data: vec![2],
        }
        .encode(3, 0, &mut stale);
        let at = SIZE_HEADER + 24;
        buf[at..at + stale.len()].copy_from_slice(&stale);
        let res = scan(&buf, 7, FORMAT_VERSION, false).unwrap();
        assert_eq!(res.records.len(), 1);
        assert_eq!(res.last_data_pos, at);
        assert_eq!(
            scan(&buf, 7, FORMAT_VERSION, true).unwrap().records.len(),
            2
        );
    }

    #[test]
    fn torn_record_is_ignored_and_marks_the_file() {
        let mut buf = file_with(
            &[
                (
                    Record::Add {
                        id: 1,
                        user_type: 1,
                        data: vec![1; 8],
                    },
                    0,
                ),
                (
                    Record::Add {
                        id: 2,
                        user_type: 1,
                        data: vec![2; 8],
                    },
                    0,
                ),
            ],
            1,
            1024,
        );
        let second = SIZE_HEADER + 31;
        buf[second + 31 - 1] ^= 0xff; // break the check size
        let res = scan(&buf, 1, FORMAT_VERSION, false).unwrap();
        assert_eq!(res.records.len(), 1);
        assert!(res.damaged);
        assert_eq!(res.last_data_pos, second);
    }

    #[test]
    fn compact_count_saturates() {
        let mut b = Vec::new();
        Record::Delete { id: 1 }.encode(1, 300, &mut b);
        assert_eq!(b[5], 127);
    }

    #[test]
    fn header_versions() {
        let h = FileHeader::new(3, 42).encode();
        assert_eq!(FileHeader::decode(&h, 3).unwrap().file_id, 42);
        assert!(FileHeader::decode(&h, 0).is_err());
        let mut v1 = h;
        v1[3] = 1;
        assert_eq!(FileHeader::decode(&v1, 3).unwrap().journal_version, 1);
        v1[3] = 3;
        assert!(FileHeader::decode(&v1, 3).is_err());
    }
}
