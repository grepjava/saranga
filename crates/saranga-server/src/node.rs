//! The node ID and the primary lock in the journal directory. Ported from
//! `FileBasedNodeManager` and `FileLockNodeManager` (primary side only;
//! backups come with HA, Phase 9).
//!
//! `server.lock` holds a state byte (`0` before the first start, `L` while
//! active, `P` after a clean stop), two `0` bytes, then the node's 16-byte
//! UUID, created on first start and kept for good. `serverlock.1` is locked
//! while the broker runs, so two brokers never share a journal.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

pub const SERVER_LOCK_NAME: &str = "server.lock";
const FIRST_TIME_START: u8 = b'0';
const ACTIVE: u8 = b'L';
const PAUSED: u8 = b'P';

fn write_at(f: &File, data: &[u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        f.write_all_at(data, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < data.len() {
            done += f.seek_write(&data[done..], offset + done as u64)?;
        }
        Ok(())
    }
}

fn read_at(f: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        f.read_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        f.seek_read(buf, offset)
    }
}

/// The node's identity and its hold on the journal directory.
pub struct NodeLock {
    node_id: String,
    server_lock: File,
    _primary_lock: File,
}

impl NodeLock {
    /// Reads or creates the node ID and takes the primary lock, failing if
    /// another broker holds it.
    pub fn acquire(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(SERVER_LOCK_NAME);
        let created = !path.exists();
        let server_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        if created {
            write_at(&server_lock, &[FIRST_TIME_START; 3], 0)?;
            server_lock.sync_all()?;
        }
        // createNodeId
        let mut id = [0u8; 16];
        let read = read_at(&server_lock, &mut id, 3)?;
        if read != 16 {
            id = *uuid::Uuid::new_v4().as_bytes();
            write_at(&server_lock, &id, 3)?;
            server_lock.sync_all()?;
        }
        for i in 1..3 {
            let p = dir.join(format!("serverlock.{i}"));
            if !p.exists() {
                File::create(&p)?;
            }
        }
        let primary_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join("serverlock.1"))?;
        match primary_lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(io::Error::other(format!(
                    "another broker holds the primary lock in {}",
                    dir.display()
                )));
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(e),
        }
        let lock = Self {
            node_id: uuid::Uuid::from_bytes(id).to_string(),
            server_lock,
            _primary_lock: primary_lock,
        };
        lock.write_state(ACTIVE)?;
        Ok(lock)
    }

    /// The node ID, as `UUID.toString` prints it.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    fn write_state(&self, state: u8) -> io::Result<()> {
        write_at(&self.server_lock, &[state], 0)?;
        self.server_lock.sync_all()
    }

    /// `pausePrimaryServer`: a clean stop. The lock goes with `self`.
    pub fn pause(self) -> io::Result<()> {
        self.write_state(PAUSED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_survives_restart_and_lock_is_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let a = NodeLock::acquire(dir.path()).unwrap();
        let id = a.node_id().to_owned();
        assert!(NodeLock::acquire(dir.path()).is_err());
        a.pause().unwrap();
        let bytes = std::fs::read(dir.path().join(SERVER_LOCK_NAME)).unwrap();
        assert_eq!(bytes.len(), 19);
        assert_eq!(&bytes[..3], b"P00");
        let b = NodeLock::acquire(dir.path()).unwrap();
        assert_eq!(b.node_id(), id);
    }

    #[test]
    fn reads_an_artemis_node_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut f = b"P00".to_vec();
        f.extend_from_slice(&[
            0x0f, 0x0f, 0x34, 0xe1, 0xc4, 0x92, 0x11, 0xf1, 0x81, 0xb3, 0x3e, 0x90, 0xb6, 0xcb,
            0x8c, 0x8d,
        ]);
        std::fs::write(dir.path().join(SERVER_LOCK_NAME), f).unwrap();
        let lock = NodeLock::acquire(dir.path()).unwrap();
        assert_eq!(lock.node_id(), "0f0f34e1-c492-11f1-81b3-3e90b6cb8c8d");
    }
}
