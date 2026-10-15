//! The compaction control file, `journal-rename-control.ctr`. Ported from
//! `AbstractJournalUpdateTask.writeControlFile` / `readControlFile`.
//!
//! Before compacted files replace the originals, the compactor writes the
//! names of the files being replaced, the new `.cmp` files and any pending
//! renames into this file. If the broker dies before the control file is
//! deleted, the next load finishes the job: it deletes the old files and
//! drops the `.cmp` extension of the new ones.

use std::path::Path;

use saranga_buffer::{JString, Reader, Writer};

use crate::format::{self, FileHeader, Record};
use crate::{JournalError, Result};

pub const FILE_COMPACT_CONTROL: &str = "journal-rename-control.ctr";

/// What a control file lists.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ControlFile {
    pub data_files: Vec<String>,
    pub new_files: Vec<String>,
    pub renames: Vec<(String, String)>,
}

impl ControlFile {
    /// The record body: three counted lists of `writeUTF` strings.
    pub fn encode_body(&self) -> Vec<u8> {
        let mut w = Writer::new();
        let utf = |w: &mut Writer, s: &str| {
            w.utf(&JString::from(s));
        };
        w.i32(self.data_files.len() as i32);
        self.data_files.iter().for_each(|f| utf(&mut w, f));
        w.i32(self.new_files.len() as i32);
        self.new_files.iter().for_each(|f| utf(&mut w, f));
        w.i32(self.renames.len() as i32);
        for (a, b) in &self.renames {
            utf(&mut w, a);
            utf(&mut w, b);
        }
        w.into_vec()
    }

    pub fn decode_body(body: &[u8]) -> Result<Self> {
        let bad =
            |e: saranga_buffer::DecodeError| JournalError::Corrupt(format!("control file: {e}"));
        let mut r = Reader::new(body);
        let list = |r: &mut Reader<'_>| -> Result<Vec<String>> {
            let n = r.i32().map_err(bad)?;
            (0..n.max(0))
                .map(|_| r.utf().map(|s| s.to_string_lossy()).map_err(bad))
                .collect()
        };
        let data_files = list(&mut r)?;
        let new_files = list(&mut r)?;
        let n = r.i32().map_err(bad)?;
        let mut renames = Vec::new();
        for _ in 0..n.max(0) {
            let a = r.utf().map_err(bad)?.to_string_lossy();
            let b = r.utf().map_err(bad)?.to_string_lossy();
            renames.push((a, b));
        }
        Ok(Self {
            data_files,
            new_files,
            renames,
        })
    }

    /// The file's bytes: a header with file ID 0 and user version 0, then
    /// one add record (ID 1, user type 0) of file record ID 0.
    pub fn encode_file(&self) -> Vec<u8> {
        let mut b = FileHeader::new(0, 0).encode().to_vec();
        Record::Add {
            id: 1,
            user_type: 0,
            data: self.encode_body(),
        }
        .encode(0, 0, &mut b);
        b
    }

    /// Reads a control file's bytes. `None` when it holds no valid record,
    /// in which case Artemis deletes it and ignores it.
    pub fn decode_file(bytes: &[u8]) -> Result<Option<Self>> {
        let Some(scan) = format::scan(bytes, 0, format::FORMAT_VERSION, false) else {
            return Ok(None);
        };
        match scan.records.into_iter().find_map(|r| match r.record {
            Record::Add { data, .. } => Some(data),
            _ => None,
        }) {
            Some(body) => Self::decode_body(&body).map(Some),
            None => Ok(None),
        }
    }

    /// Writes the control file into `dir`, synced.
    pub fn write(&self, dir: &Path) -> Result<()> {
        let path = dir.join(FILE_COMPACT_CONTROL);
        let f = crate::io::open(&path, true).map_err(JournalError::io)?;
        crate::io::write_at(&f, &path, &self.encode_file(), 0).map_err(JournalError::io)?;
        crate::io::sync_all(&f, &path).map_err(JournalError::io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let c = ControlFile {
            data_files: vec!["activemq-data-1.amq".into(), "activemq-data-2.amq".into()],
            new_files: vec!["activemq-data-7.amq.cmp".into()],
            renames: vec![("a.tmp".into(), "a".into())],
        };
        assert_eq!(ControlFile::decode_file(&c.encode_file()).unwrap(), Some(c));
        assert_eq!(ControlFile::decode_file(&[0; 40]).unwrap(), None);
    }
}
