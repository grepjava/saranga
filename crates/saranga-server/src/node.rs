//! The node ID and the locks of a journal directory shared by a primary and
//! its backups. Ported from `FileBasedNodeManager` and `FileLockNodeManager`.
//!
//! `server.lock` holds a state byte, two `0` bytes, then the node's 16-byte
//! UUID, created on first start and kept for good. The state is `0` before
//! the first start, `F` while a primary starts (a backup active on its data
//! gives way to it), `L` while a primary, or the backup that took over from
//! it, is active, and `P` after a clean stop that no backup should take
//! over from. It is read and written under a lock on `server.lock`.
//! `serverlock.1` is locked by the active broker, `serverlock.2` by the
//! backup waiting for it to fail. The locks are those of a Java broker
//! (see [`saranga_ha::filelock`]), so Java and Saranga brokers share a directory.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use saranga_ha::filelock::{Held, LockFile};

pub const SERVER_LOCK_NAME: &str = "server.lock";
const FIRST_TIME_START: u8 = b'0';
const ACTIVE: u8 = b'L';
const FAILINGBACK: u8 = b'F';
const PAUSED: u8 = b'P';
const NOT_STARTED: u8 = b'N';

const STATE_LOCK_POS: usize = 0;
const PRIMARY_LOCK_POS: usize = 1;
const BACKUP_LOCK_POS: usize = 2;

/// `LOCK_ACCESS_FAILURE_WAIT_TIME_NANOS`.
const LOCK_ACCESS_FAILURE_WAIT: Duration = Duration::from_secs(2);
/// Between attempts at a lock another broker holds.
const LOCK_RETRY: Duration = Duration::from_millis(500);
/// Between looks at the state while waiting on another broker.
const STATE_POLL: Duration = Duration::from_secs(2);

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

/// Writes `data` at `offset` of `server.lock` and syncs it.
fn write_synced(f: &File, path: &Path, data: &[u8], offset: u64) -> io::Result<()> {
    if let Err((n, e)) = saranga_fault::write(path, data.len()) {
        write_at(f, &data[..n], offset)?;
        return Err(e);
    }
    write_at(f, data, offset)?;
    saranga_fault::check(saranga_fault::Op::Sync, path)?;
    f.sync_all()
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

/// `ActiveMQLockAcquisitionTimeoutException`'s message.
fn timed_out(timeout: Duration) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "Timed out waiting for lock. Waited for {}",
            timeout.as_secs()
        ),
    )
}

fn interrupted() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "Lock was interrupted")
}

static WAITS_INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Every wait for a lock in the process gives up: the process is stopping
/// before its broker became active.
pub fn interrupt_waits() {
    WAITS_INTERRUPTED.store(true, Ordering::Release);
}

static LAST_NODE_ID: Mutex<Option<String>> = Mutex::new(None);

/// The node ID the process's node manager last read, which a broker
/// stopped while it waits for a lock reports (`ActiveMQServerImpl.stop`'s
/// `tempNodeID`).
pub fn last_node_id() -> Option<String> {
    LAST_NODE_ID
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

fn set_node_id(slot: &Mutex<String>, id: String) {
    *LAST_NODE_ID.lock().unwrap_or_else(|e| e.into_inner()) = Some(id.clone());
    *slot.lock().unwrap_or_else(|e| e.into_inner()) = id;
}

/// `setNodeID` as a replicating backup takes over: `server.lock` in `dir`
/// is given the node ID of the primary whose data it copied, which it is
/// from now on.
pub fn write_node_id(dir: &Path, node_id: &str) -> io::Result<()> {
    let id = node_id_bytes(node_id)?;
    let n = NodeManager::open(dir, None)?;
    write_synced(
        n.channels[STATE_LOCK_POS].file(),
        &n.server_lock_path,
        &id,
        3,
    )
}

/// `new UUID(TYPE_TIME_BASED, UUID.stringToBytes(nodeID))`, as
/// `setNodeID` stores a node ID: its hexadecimal pairs, dashes skipped
/// (any other character counting as -1, as `Character.digit` gives it),
/// with the version and variant bits forced. A `coordination-id` is not
/// hexadecimal, so what is stored does not read back as it.
pub fn node_id_bytes(node_id: &str) -> io::Result<[u8; 16]> {
    let digit = |c: char| c.to_digit(16).map_or(-1i32, |d| d as i32);
    let chars: Vec<char> = node_id.chars().collect();
    let mut data = [0u8; 16];
    let mut i = 0;
    let mut n = 0;
    while i < chars.len() {
        while i < chars.len() && chars[i] == '-' {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        let (Some(&c1), Some(&c2)) = (chars.get(i), chars.get(i + 1)) else {
            return Err(io::Error::other(format!("bad node ID {node_id}")));
        };
        i += 2;
        let slot = data
            .get_mut(n)
            .ok_or_else(|| io::Error::other(format!("bad node ID {node_id}")))?;
        *slot = ((digit(c1) << 4) + digit(c2)) as u8;
        n += 1;
    }
    data[6] = (data[6] & 0x0F) | 0x10;
    data[8] = (data[8] & 0x3F) | 0x80;
    Ok(data)
}

/// The HA policy and the node manager's settings (`ha-policy`,
/// `node-manager-lock-directory`, `journal-lock-*`).
#[derive(Debug, Clone)]
pub struct HaConfig {
    pub policy: saranga_config::HaPolicy,
    /// Where `server.lock` is; the journal directory when `None`.
    pub lock_directory: Option<PathBuf>,
    /// `journal-lock-acquisition-timeout`; `None` waits for ever.
    pub lock_acquisition_timeout: Option<Duration>,
    /// `journal-lock-monitor-timeout`.
    pub lock_monitor_period: Duration,
    /// `journal-lock-monitor-max-retries`.
    pub lock_monitor_max_retries: u32,
    /// Replication with a lock manager: what the activation holds.
    pub coordination: Option<std::sync::Arc<crate::replication::pluggable::Coordination>>,
}

impl Default for HaConfig {
    fn default() -> Self {
        Self {
            policy: saranga_config::HaPolicy::PrimaryOnly,
            lock_directory: None,
            lock_acquisition_timeout: None,
            lock_monitor_period: Duration::from_secs(2),
            lock_monitor_max_retries: 0,
            coordination: None,
        }
    }
}

impl HaConfig {
    /// From `broker.xml`, with a relative lock directory under `instance`.
    pub fn from_file(config: &saranga_config::BrokerConfig, instance: &Path) -> Self {
        Self {
            policy: config.ha_policy.clone(),
            lock_directory: config
                .node_manager_lock_directory
                .as_deref()
                .map(|d| instance.join(d)),
            lock_acquisition_timeout: u64::try_from(config.journal_lock_acquisition_timeout)
                .ok()
                .map(Duration::from_millis),
            lock_monitor_period: Duration::from_millis(
                config.journal_lock_monitor_timeout.max(1) as u64
            ),
            lock_monitor_max_retries: config.journal_lock_monitor_max_retries.max(0) as u32,
            coordination: None,
        }
    }

    /// `failover-on-shutdown`: a clean stop leaves the state active for a
    /// backup to take over.
    pub fn failover_on_shutdown(&self) -> bool {
        match self.policy {
            // A replicating primary never sends its backup `FAIL_OVER` on a
            // clean stop; the backup takes over as from a crash.
            saranga_config::HaPolicy::PrimaryOnly
            | saranga_config::HaPolicy::ReplicationPrimary(_)
            | saranga_config::HaPolicy::ReplicationBackup(_) => false,
            saranga_config::HaPolicy::SharedStorePrimary {
                failover_on_shutdown,
                ..
            }
            | saranga_config::HaPolicy::SharedStoreBackup {
                failover_on_shutdown,
                ..
            } => failover_on_shutdown,
        }
    }
}

/// The node's identity and its locks in the directory.
pub struct NodeManager {
    server_lock_path: PathBuf,
    /// `server.lock`, `serverlock.1` and `serverlock.2` (`lockChannels`).
    channels: [LockFile; 3],
    node_id: Mutex<String>,
    /// `journal-lock-acquisition-timeout`; `None` waits for ever.
    timeout: Option<Duration>,
    interrupted: AtomicBool,
    primary_lock: Mutex<Option<Held>>,
    backup_lock: Mutex<Option<Held>>,
    /// `serverLockLastModified`: when this broker last wrote the state.
    last_modified: Mutex<Option<SystemTime>>,
}

impl NodeManager {
    /// `start`: `server.lock` created if it is not there, with the node ID
    /// read or made (`setUpServerLockFile`, `createNodeId`), and the lock
    /// files opened.
    pub fn open(dir: &Path, timeout: Option<Duration>) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let server_lock_path = dir.join(SERVER_LOCK_NAME);
        let created = !server_lock_path.exists();
        let server_lock = LockFile::open(&server_lock_path)?;
        if created {
            write_synced(
                server_lock.file(),
                &server_lock_path,
                &[FIRST_TIME_START; 3],
                0,
            )?;
        }
        let mut id = [0u8; 16];
        if read_at(server_lock.file(), &mut id, 3)? != 16 {
            // `UUIDGenerator.generateUUID`: time-based, as Artemis makes it.
            id = saranga_client::uuid::time_based();
            write_synced(server_lock.file(), &server_lock_path, &id, 3)?;
        }
        let channels = [
            server_lock,
            LockFile::open(&dir.join("serverlock.1"))?,
            LockFile::open(&dir.join("serverlock.2"))?,
        ];
        let node_id = Mutex::new(String::new());
        // `new UUID(TYPE_TIME_BASED, bytes)`: the version and variant bits
        // are forced, so a Java broker on this directory reads the same ID.
        set_node_id(&node_id, saranga_storage::java::uuid(&id));
        Ok(Self {
            server_lock_path,
            channels,
            node_id,
            timeout,
            interrupted: AtomicBool::new(false),
            primary_lock: Mutex::new(None),
            backup_lock: Mutex::new(None),
            last_modified: Mutex::new(None),
        })
    }

    /// The node ID, as `UUID.toString` prints it.
    pub fn node_id(&self) -> String {
        self.node_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// `readNodeId`: the ID the primary wrote, which a backup takes on.
    fn read_node_id(&self) -> io::Result<()> {
        let mut id = [0u8; 16];
        if read_at(self.channels[STATE_LOCK_POS].file(), &mut id, 3)? != 16 {
            return Err(io::Error::other("primary server did not write id to file"));
        }
        set_node_id(&self.node_id, saranga_storage::java::uuid(&id));
        Ok(())
    }

    /// `interrupt`: a wait for a lock gives up.
    pub fn interrupt(&self) {
        self.interrupted.store(true, Ordering::Release);
    }

    fn is_interrupted(&self) -> bool {
        self.interrupted.load(Ordering::Acquire) || WAITS_INTERRUPTED.load(Ordering::Acquire)
    }

    /// Sleeps, or gives up once interrupted.
    fn pause_for(&self, d: Duration) -> io::Result<()> {
        let until = Instant::now() + d;
        while Instant::now() < until {
            if self.is_interrupted() {
                return Err(interrupted());
            }
            std::thread::sleep(Duration::from_millis(100).min(until - Instant::now()));
        }
        Ok(())
    }

    /// `lock`: tries until the lock is free, the acquisition timeout passes
    /// or the wait is interrupted. Failing to reach the file (an NFS volume
    /// gone for a while) is retried too.
    fn lock(&self, pos: usize) -> io::Result<Held> {
        let start = Instant::now();
        let mut recurring = false;
        loop {
            if self.is_interrupted() {
                return Err(interrupted());
            }
            match self.channels[pos].try_lock() {
                Ok(lock) => {
                    recurring = false;
                    if let Some(t) = self.timeout
                        && start.elapsed() > t
                    {
                        return Err(timed_out(t));
                    }
                    match lock {
                        Some(l) => return Ok(l),
                        None => self.pause_for(LOCK_RETRY)?,
                    }
                }
                Err(e) => {
                    if !recurring {
                        tracing::warn!("Failure when accessing a lock file: {e}");
                    }
                    recurring = true;
                    let mut wait = LOCK_ACCESS_FAILURE_WAIT;
                    if let Some(t) = self.timeout {
                        let Some(left) = t.checked_sub(start.elapsed()).filter(|l| !l.is_zero())
                        else {
                            return Err(timed_out(t));
                        };
                        wait = wait.min(left);
                    }
                    self.pause_for(wait)?;
                }
            }
        }
    }

    /// `getState`, under the state lock.
    fn state(&self) -> io::Result<u8> {
        let _lock = self.lock(STATE_LOCK_POS)?;
        let mut b = [0u8; 1];
        Ok(
            match read_at(self.channels[STATE_LOCK_POS].file(), &mut b, 0)? {
                0 => NOT_STARTED,
                _ => b[0],
            },
        )
    }

    /// `writeFileLockStatus`, under the state lock.
    fn write_state(&self, state: u8) -> io::Result<()> {
        {
            let _lock = self.lock(STATE_LOCK_POS)?;
            let f = self.channels[STATE_LOCK_POS].file();
            write_synced(f, &self.server_lock_path, &[state], 0)?;
        }
        *self.last_modified.lock().unwrap_or_else(|e| e.into_inner()) =
            std::fs::metadata(&self.server_lock_path)
                .and_then(|m| m.modified())
                .ok();
        Ok(())
    }

    fn timeout_text(&self) -> String {
        match self.timeout {
            None => "indefinitely".to_owned(),
            Some(t) => format!("{} milliseconds", t.as_millis()),
        }
    }

    /// `startPrimaryNode`: the state set to `F`, so that an active backup
    /// gives way, then the primary lock taken. [`Self::activation_complete`]
    /// follows once the broker is up.
    pub fn start_primary(&self) -> io::Result<()> {
        self.write_state(FAILINGBACK)?;
        tracing::info!(
            "AMQ221034: Waiting {} to obtain primary lock",
            self.timeout_text()
        );
        let lock = self.lock(PRIMARY_LOCK_POS)?;
        *self.primary_lock.lock().unwrap_or_else(|e| e.into_inner()) = Some(lock);
        tracing::info!("AMQ221035: Primary Server Obtained primary lock");
        Ok(())
    }

    /// The activation callback of `startPrimaryNode`: the state set to `L`.
    pub fn activation_complete(&self) -> io::Result<()> {
        self.write_state(ACTIVE)
    }

    /// `pausePrimaryServer`: a clean stop no backup takes over from.
    pub fn pause_primary(&self) -> io::Result<()> {
        let r = self.write_state(PAUSED);
        self.primary_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        r
    }

    /// `crashPrimaryServer`: the primary lock let go with the state left
    /// active, for a backup to take over.
    pub fn crash_primary(&self) {
        self.primary_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
    }

    /// `startBackup`: waits for the backup lock, then takes on the node ID
    /// the primary wrote.
    pub fn start_backup(&self) -> io::Result<()> {
        tracing::info!("AMQ221032: Waiting to become backup node");
        let lock = self.lock(BACKUP_LOCK_POS)?;
        *self.backup_lock.lock().unwrap_or_else(|e| e.into_inner()) = Some(lock);
        tracing::info!("AMQ221033: ** got backup lock");
        self.read_node_id()
    }

    /// `releaseBackup`.
    pub fn release_backup(&self) {
        self.backup_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
    }

    /// `awaitPrimaryNode`: waits for the primary to have started, then for
    /// its lock. The lock taken with the state still active means the
    /// primary died, or stopped for the backup to take over; paused or
    /// failing back, it is let go again.
    pub fn await_primary(&self) -> io::Result<()> {
        loop {
            let mut state = self.state()?;
            while state == NOT_STARTED || state == FIRST_TIME_START {
                self.pause_for(STATE_POLL)?;
                state = self.state()?;
            }
            let lock = self.lock(PRIMARY_LOCK_POS)?;
            match self.state()? {
                ACTIVE => {
                    // Marks the file, so that a primary that still thinks
                    // it holds the lock sees it lost.
                    let _ = self.channels[STATE_LOCK_POS]
                        .file()
                        .set_modified(SystemTime::now());
                    *self.primary_lock.lock().unwrap_or_else(|e| e.into_inner()) = Some(lock);
                    return Ok(());
                }
                _ => {
                    drop(lock);
                    self.pause_for(STATE_POLL)?;
                }
            }
        }
    }

    /// `isAwaitingFailback`: a primary is starting on the data.
    pub fn is_awaiting_failback(&self) -> io::Result<bool> {
        Ok(self.state()? == FAILINGBACK)
    }

    /// `awaitActiveStatus`.
    pub fn await_active_status(&self) -> io::Result<()> {
        while self.state()? != ACTIVE {
            self.pause_for(STATE_POLL)?;
        }
        Ok(())
    }

    /// `isBackupActive`: some other broker holds the primary lock.
    pub fn is_backup_active(&self) -> io::Result<bool> {
        Ok(self.channels[PRIMARY_LOCK_POS].try_lock()?.is_none())
    }

    pub fn holds_primary_lock(&self) -> bool {
        self.primary_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    /// `MonitorLock.run`, one check: the lock is lost when it is not held,
    /// the state cannot be read, or another broker marked `server.lock`
    /// while the state is active.
    pub fn primary_lock_lost(&self) -> bool {
        if !self.holds_primary_lock() {
            return true;
        }
        let state = match self.state() {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("{e}");
                return true;
            }
        };
        let recorded = *self.last_modified.lock().unwrap_or_else(|e| e.into_inner());
        match std::fs::metadata(&self.server_lock_path).and_then(|m| m.modified()) {
            Ok(now) => state == ACTIVE && recorded.is_some_and(|r| now > r),
            Err(_) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_ids_stored_as_set_node_id_stores_them() {
        let id = "7debb3d1-0d4b-11ec-9704-ae9213b68ac4";
        let b = node_id_bytes(id).unwrap();
        assert_eq!(saranga_storage::java::uuid(&b), id);
        // A coordination-id, which is not hexadecimal.
        let b = node_id_bytes("peer.journal.001").unwrap();
        assert_eq!(
            saranga_storage::java::uuid(&b),
            "fedfefef-ef9f-1001-8000-000000000000"
        );
    }

    #[test]
    fn node_id_survives_restart_and_one_primary_at_a_time() {
        let dir = tempfile::tempdir().unwrap();
        let a = NodeManager::open(dir.path(), None).unwrap();
        a.start_primary().unwrap();
        a.activation_complete().unwrap();
        let id = a.node_id();
        let b = NodeManager::open(dir.path(), Some(Duration::from_millis(600))).unwrap();
        assert_eq!(b.node_id(), id);
        let e = b.start_primary().unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::TimedOut);
        assert!(b.is_backup_active().unwrap());
        a.pause_primary().unwrap();
        let bytes = std::fs::read(dir.path().join(SERVER_LOCK_NAME)).unwrap();
        assert_eq!(bytes.len(), 19);
        assert_eq!(&bytes[..3], b"P00");
        b.start_primary().unwrap();
        assert!(b.holds_primary_lock());
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
        let n = NodeManager::open(dir.path(), None).unwrap();
        assert_eq!(n.node_id(), "0f0f34e1-c492-11f1-81b3-3e90b6cb8c8d");
    }

    #[test]
    fn a_backup_takes_over_from_a_crashed_primary_but_not_a_paused_one() {
        let dir = tempfile::tempdir().unwrap();
        let primary = NodeManager::open(dir.path(), None).unwrap();
        primary.start_primary().unwrap();
        primary.activation_complete().unwrap();
        let backup = std::sync::Arc::new(NodeManager::open(dir.path(), None).unwrap());
        backup.start_backup().unwrap();
        assert_eq!(backup.node_id(), primary.node_id());
        let waiting = {
            let b = backup.clone();
            std::thread::spawn(move || b.await_primary())
        };
        std::thread::sleep(Duration::from_millis(700));
        assert!(!waiting.is_finished());
        primary.crash_primary();
        waiting.join().unwrap().unwrap();
        assert!(backup.holds_primary_lock());
        // A primary restarting asks for failback.
        let restarted = std::sync::Arc::new(NodeManager::open(dir.path(), None).unwrap());
        let starting = {
            let r = restarted.clone();
            std::thread::spawn(move || r.start_primary())
        };
        while !backup.is_awaiting_failback().unwrap() {
            std::thread::sleep(Duration::from_millis(50));
        }
        backup.crash_primary();
        starting.join().unwrap().unwrap();
        restarted.activation_complete().unwrap();
        // Paused, a second backup does not take over; interrupted, it gives
        // up.
        restarted.pause_primary().unwrap();
        backup.release_backup();
        let second = std::sync::Arc::new(NodeManager::open(dir.path(), None).unwrap());
        second.start_backup().unwrap();
        let waiting = {
            let b = second.clone();
            std::thread::spawn(move || b.await_primary())
        };
        std::thread::sleep(Duration::from_millis(700));
        assert!(!waiting.is_finished());
        second.interrupt();
        let e = waiting.join().unwrap().unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::Interrupted);
    }

    #[test]
    fn the_monitor_sees_a_backup_take_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let primary = NodeManager::open(dir.path(), None).unwrap();
        primary.start_primary().unwrap();
        primary.activation_complete().unwrap();
        assert!(!primary.primary_lock_lost());
        // As a backup does once it has the lock (the file system's clock
        // may be coarse).
        std::fs::File::options()
            .write(true)
            .open(dir.path().join(SERVER_LOCK_NAME))
            .unwrap()
            .set_modified(SystemTime::now() + Duration::from_secs(5))
            .unwrap();
        assert!(primary.primary_lock_lost());
    }
}
