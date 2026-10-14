//! `DistributedLockManager`: the locks and longs replication with a
//! pluggable lock manager coordinates through, by the class names
//! `broker.xml` gives Artemis's implementations.
//!
//! - `CuratorDistributedLockManager` (and the deprecated
//!   `CuratorDistributedPrimitiveManager`): ZooKeeper, with the znodes
//!   Curator makes, so Java and Saranga brokers share an ensemble. A lock
//!   is `/<id>/locks`, a semaphore with one lease; a long is
//!   `/<id>/activation-sequence`.
//! - `FileBasedLockManager`: a lock is the file `<locks-folder>/<id>`,
//!   locked as Java locks it; a long is eight bytes in `ML:<id>`.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;

use crate::curator::{Curator, make_path};
use crate::filelock::{Held, LockFile};
use crate::zookeeper::{self, ConnectionState, ZooKeeper};

pub const CURATOR_CLASS: &str =
    "org.apache.activemq.artemis.lockmanager.zookeeper.CuratorDistributedLockManager";
pub const CURATOR_DEPRECATED_CLASS: &str =
    "org.apache.activemq.artemis.quorum.zookeeper.CuratorDistributedPrimitiveManager";
pub const FILE_CLASS: &str = "org.apache.activemq.artemis.lockmanager.file.FileBasedLockManager";

/// `ActiveMQDefaultConfiguration.getDefaultDistributedLockManagerClassName`.
pub const DEFAULT_CLASS: &str = CURATOR_CLASS;

/// `CuratorDistributedLockManager.VALID_PARAMS`, in the order its error
/// lists them.
const CURATOR_PARAMS: [&str; 7] = [
    "connect-string",
    "namespace",
    "session-ms",
    "session-percent",
    "connection-ms",
    "retries",
    "retries-ms",
];

/// `UnavailableStateException`: the coordination service cannot answer,
/// or the primitive was lost with the session.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct Unavailable(pub String);

impl From<zookeeper::Error> for Unavailable {
    fn from(e: zookeeper::Error) -> Self {
        Self(e.to_string())
    }
}

#[derive(Debug, Clone)]
struct CuratorSettings {
    connect_string: String,
    namespace: Option<String>,
    session_ms: u64,
    session_percent: u32,
    connection_ms: u64,
    retries: i32,
    retries_ms: u64,
}

#[derive(Debug, Clone)]
enum Kind {
    Curator(CuratorSettings),
    File(PathBuf),
}

#[derive(Clone)]
enum Backend {
    Curator(Curator),
    File(PathBuf),
}

/// The open locks, by ID.
type Locks = Arc<Mutex<HashMap<String, Arc<LockInner>>>>;

struct Started {
    backend: Backend,
    /// Turned true for good once the session is `LOST`.
    unavailable: watch::Sender<bool>,
    locks: Locks,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A manager, started and stopped as the activations need it.
pub struct LockManager {
    kind: Kind,
    started: Mutex<Option<Started>>,
    /// Serializes start and stop.
    lifecycle: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for LockManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockManager")
            .field("kind", &self.kind)
            .finish()
    }
}

fn parse<T: std::str::FromStr>(
    props: &BTreeMap<String, String>,
    k: &str,
    default: T,
) -> Result<T, String> {
    match props.get(k) {
        // `Integer.parseInt`.
        Some(v) => v
            .parse()
            .map_err(|_| format!("java.lang.NumberFormatException: For input string: \"{v}\"")),
        None => Ok(default),
    }
}

impl LockManager {
    /// `DistributedLockManager.newInstanceOf`: the implementation named,
    /// with its properties checked as its constructor checks them.
    pub fn new(
        class_name: &str,
        properties: &BTreeMap<String, String>,
    ) -> Result<Arc<Self>, String> {
        let kind = match class_name {
            CURATOR_CLASS | CURATOR_DEPRECATED_CLASS => {
                for k in properties.keys() {
                    if !CURATOR_PARAMS.contains(&k.as_str()) {
                        let mut accepted: Vec<&str> = CURATOR_PARAMS.to_vec();
                        accepted.sort_unstable();
                        return Err(format!(
                            "non existent parameter {k}: accepted list is {}",
                            accepted.join(",")
                        ));
                    }
                }
                let connect_string = properties
                    .get("connect-string")
                    .cloned()
                    .ok_or_else(|| "connectString cannot be null".to_owned())?;
                Kind::Curator(CuratorSettings {
                    connect_string,
                    namespace: properties.get("namespace").cloned(),
                    session_ms: parse(properties, "session-ms", 18_000)?,
                    session_percent: parse(properties, "session-percent", 33)?,
                    connection_ms: parse(properties, "connection-ms", 8_000)?,
                    retries: parse(properties, "retries", 1)?,
                    retries_ms: parse(properties, "retries-ms", 1_000)?,
                })
            }
            FILE_CLASS => {
                let folder =
                    PathBuf::from(properties.get("locks-folder").cloned().unwrap_or_default());
                if !folder.exists() {
                    return Err(format!(
                        "{} is supposed to already exists",
                        folder.display()
                    ));
                }
                if !folder.is_dir() {
                    return Err(format!(
                        "{} is supposed to be a directory",
                        folder.display()
                    ));
                }
                Kind::File(folder)
            }
            other => return Err(format!("java.lang.ClassNotFoundException: {other}")),
        };
        Ok(Arc::new(Self {
            kind,
            started: Mutex::new(None),
            lifecycle: tokio::sync::Mutex::new(()),
        }))
    }

    pub fn is_started(&self) -> bool {
        lock(&self.started).is_some()
    }

    /// `start(timeout)`: connected to the service within `timeout` (`None`
    /// waits for ever).
    pub async fn start(&self, timeout: Option<Duration>) -> bool {
        let _l = self.lifecycle.lock().await;
        if self.is_started() {
            return true;
        }
        let (unavailable, _) = watch::channel(false);
        let locks: Locks = Arc::default();
        let backend = match &self.kind {
            Kind::File(folder) => Backend::File(folder.clone()),
            Kind::Curator(s) => {
                let zk = match ZooKeeper::start(zookeeper::Config {
                    connect_string: s.connect_string.clone(),
                    session_timeout: Duration::from_millis(s.session_ms),
                    connection_timeout: Duration::from_millis(s.connection_ms),
                    session_percent: s.session_percent,
                }) {
                    Ok(zk) => zk,
                    Err(e) => {
                        tracing::warn!("{e}");
                        return false;
                    }
                };
                let mut events = zk.subscribe();
                if !zk.wait_connected(timeout).await {
                    zk.close().await;
                    return false;
                }
                let curator = Curator::new(
                    zk,
                    s.namespace.clone(),
                    s.retries,
                    Duration::from_millis(s.retries_ms),
                );
                // `stateChanged`.
                let tx = unavailable.clone();
                let held = locks.clone();
                tokio::spawn(async move {
                    loop {
                        match events.recv().await {
                            Ok(ConnectionState::Lost) => {
                                tx.send_replace(true);
                                for l in lock(&held).values() {
                                    l.on_lost();
                                }
                                return;
                            }
                            Ok(ConnectionState::Reconnected) => {
                                let ls: Vec<_> = lock(&held).values().cloned().collect();
                                for l in ls {
                                    l.on_reconnected().await;
                                }
                            }
                            Ok(_) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(_) => return,
                        }
                    }
                });
                Backend::Curator(curator)
            }
        };
        *lock(&self.started) = Some(Started {
            backend,
            unavailable,
            locks,
        });
        true
    }

    /// `stop`: every primitive closed with the session, which takes its
    /// leases.
    pub async fn stop(&self) {
        let _l = self.lifecycle.lock().await;
        let Some(s) = lock(&self.started).take() else {
            return;
        };
        let locks: Vec<_> = lock(&s.locks).drain().map(|(_, l)| l).collect();
        for l in locks {
            l.close_silently();
        }
        if let Backend::Curator(c) = &s.backend {
            c.zk.close().await;
        }
    }

    /// `UnavailableManagerListener`: true once the service is lost.
    pub fn unavailable(&self) -> Option<watch::Receiver<bool>> {
        lock(&self.started)
            .as_ref()
            .map(|s| s.unavailable.subscribe())
    }

    fn started(&self) -> Result<(Backend, watch::Receiver<bool>, Locks), Unavailable> {
        let s = lock(&self.started);
        let s = s
            .as_ref()
            .ok_or_else(|| Unavailable("manager isn't started yet!".into()))?;
        Ok((
            s.backend.clone(),
            s.unavailable.subscribe(),
            s.locks.clone(),
        ))
    }

    /// `getDistributedLock`: one per ID while it is open.
    pub fn distributed_lock(&self, id: &str) -> Result<DistributedLock, Unavailable> {
        let (backend, unavailable, locks) = self.started()?;
        let mut map = lock(&locks);
        if let Some(l) = map.get(id) {
            return Ok(DistributedLock(l.clone()));
        }
        let file = match &backend {
            Backend::File(folder) => {
                Some(LockFile::open(&folder.join(id)).map_err(|e| Unavailable(e.to_string()))?)
            }
            Backend::Curator(_) => None,
        };
        let (lost, _) = watch::channel(*unavailable.borrow());
        let inner = Arc::new(LockInner {
            id: id.to_owned(),
            backend,
            file,
            state: Mutex::new(LockState::default()),
            lost,
            unavailable,
            locks: locks.clone(),
        });
        map.insert(id.to_owned(), inner.clone());
        Ok(DistributedLock(inner))
    }

    /// `getMutableLong`.
    pub fn mutable_long(&self, id: &str) -> Result<MutableLong, Unavailable> {
        let (backend, unavailable, _) = self.started()?;
        Ok(MutableLong {
            id: id.to_owned(),
            backend,
            unavailable,
        })
    }
}

#[derive(Default)]
struct LockState {
    /// The lease's path and the data that identifies this holder.
    lease: Option<(String, Vec<u8>)>,
    file: Option<Held>,
    closed: bool,
}

struct LockInner {
    id: String,
    backend: Backend,
    file: Option<LockFile>,
    state: Mutex<LockState>,
    /// The `UnavailableLockListener`s' event.
    lost: watch::Sender<bool>,
    unavailable: watch::Receiver<bool>,
    locks: Locks,
}

impl LockInner {
    fn check(&self) -> Result<(), Unavailable> {
        if *self.lost.borrow() || *self.unavailable.borrow() {
            return Err(Unavailable(format!(
                "lock with id = {} isn't available",
                self.id
            )));
        }
        if lock(&self.state).closed {
            return Err(Unavailable("This lock is closed".into()));
        }
        Ok(())
    }

    fn on_lost(&self) {
        lock(&self.state).lease = None;
        self.lost.send_replace(true);
    }

    /// `handleReconnected`: the lease must still be this holder's.
    async fn on_reconnected(&self) {
        let lease = lock(&self.state).lease.clone();
        let (Some((path, version)), Backend::Curator(c)) = (lease, &self.backend) else {
            return;
        };
        match c.get_data(&path).await {
            Ok(d) if d == version => {}
            _ => self.on_lost(),
        }
    }

    fn close_silently(&self) {
        let mut s = lock(&self.state);
        s.closed = true;
        s.file = None;
        s.lease = None;
    }
}

/// `DistributedLock`.
#[derive(Clone)]
pub struct DistributedLock(Arc<LockInner>);

impl DistributedLock {
    pub fn id(&self) -> &str {
        &self.0.id
    }

    /// `tryLock()`.
    pub async fn try_lock(&self) -> Result<bool, Unavailable> {
        self.0.check()?;
        match &self.0.backend {
            Backend::File(_) => {
                let mut s = lock(&self.0.state);
                if s.file.is_some() {
                    return Err(Unavailable("unlock first".into()));
                }
                let held = self
                    .0
                    .file
                    .as_ref()
                    .expect("a file lock")
                    .try_lock()
                    .map_err(|e| Unavailable(e.to_string()))?;
                let got = held.is_some();
                s.file = held;
                Ok(got)
            }
            Backend::Curator(c) => {
                if lock(&self.0.state).lease.is_some() {
                    return Err(Unavailable("unlock first".into()));
                }
                let version = uuid::Uuid::new_v4().to_string().into_bytes();
                let base = make_path(&format!("/{}", self.0.id), "locks");
                match c.try_acquire_lease(&base, &version).await? {
                    Some(path) => {
                        lock(&self.0.state).lease = Some((path, version));
                        Ok(true)
                    }
                    None => Ok(false),
                }
            }
        }
    }

    /// `tryLock(timeout)`: tried every 250 milliseconds.
    pub async fn try_lock_for(&self, timeout: Duration) -> Result<bool, Unavailable> {
        if timeout.is_zero() {
            return self.try_lock().await;
        }
        let period = Duration::from_millis(250);
        let deadline = tokio::time::Instant::now() + timeout;
        let mut next = tokio::time::Instant::now();
        loop {
            tokio::time::sleep_until(next).await;
            if self.try_lock().await? {
                return Ok(true);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            next = if deadline - now < period {
                now
            } else {
                next + period
            };
        }
    }

    /// `isHeldByCaller`.
    pub async fn is_held_by_caller(&self) -> Result<bool, Unavailable> {
        self.0.check()?;
        match &self.0.backend {
            Backend::File(_) => Ok(lock(&self.0.state).file.is_some()),
            Backend::Curator(c) => {
                let Some((path, version)) = lock(&self.0.state).lease.clone() else {
                    return Ok(false);
                };
                Ok(c.get_data(&path).await? == version)
            }
        }
    }

    /// `unlock`.
    pub async fn unlock(&self) -> Result<(), Unavailable> {
        self.0.check()?;
        let (lease, _file) = {
            let mut s = lock(&self.0.state);
            (s.lease.take(), s.file.take())
        };
        if let (Some((path, _)), Backend::Curator(c)) = (lease, &self.0.backend) {
            c.delete(&path).await?;
        }
        Ok(())
    }

    /// `addListener`: true once the lock is lost.
    pub fn lost(&self) -> watch::Receiver<bool> {
        self.0.lost.subscribe()
    }

    /// `close`: a lease held is returned.
    pub async fn close(&self) {
        lock(&self.0.locks).remove(&self.0.id);
        let (lease, unavailable) = {
            let mut s = lock(&self.0.state);
            if s.closed {
                return;
            }
            s.closed = true;
            s.file = None;
            (
                s.lease.take(),
                *self.0.lost.borrow() || *self.0.unavailable.borrow(),
            )
        };
        if let (Some((path, _)), Backend::Curator(c), false) = (lease, &self.0.backend, unavailable)
        {
            let _ = c.delete(&path).await;
        }
    }
}

/// `MutableLong`.
pub struct MutableLong {
    id: String,
    backend: Backend,
    unavailable: watch::Receiver<bool>,
}

impl MutableLong {
    pub fn id(&self) -> &str {
        &self.id
    }

    fn check(&self) -> Result<(), Unavailable> {
        if *self.unavailable.borrow() {
            return Err(Unavailable(format!(
                "mutableLong with id = {} isn't available",
                self.id
            )));
        }
        Ok(())
    }

    fn file(folder: &std::path::Path, id: &str) -> std::io::Result<std::fs::File> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(folder.join(format!("ML:{id}")))
    }

    fn path(&self) -> String {
        format!("/{}/activation-sequence", self.id)
    }

    pub async fn get(&self) -> Result<i64, Unavailable> {
        self.check()?;
        match &self.backend {
            Backend::Curator(c) => Ok(c.get_long(&self.path()).await?),
            Backend::File(folder) => {
                use std::io::{Read, Seek};
                let mut f = Self::file(folder, &self.id).map_err(|e| Unavailable(e.to_string()))?;
                let mut b = [0u8; 8];
                f.seek(std::io::SeekFrom::Start(0))
                    .map_err(|e| Unavailable(e.to_string()))?;
                let mut n = 0;
                while n < 8 {
                    match f.read(&mut b[n..]) {
                        Ok(0) => break,
                        Ok(k) => n += k,
                        Err(e) => return Err(Unavailable(e.to_string())),
                    }
                }
                Ok(if n == 8 { i64::from_be_bytes(b) } else { 0 })
            }
        }
    }

    pub async fn set(&self, value: i64) -> Result<(), Unavailable> {
        self.check()?;
        match &self.backend {
            Backend::Curator(c) => Ok(c.force_set_long(&self.path(), value).await?),
            Backend::File(folder) => {
                use std::io::{Seek, Write};
                let mut f = Self::file(folder, &self.id).map_err(|e| Unavailable(e.to_string()))?;
                f.seek(std::io::SeekFrom::Start(0))
                    .and_then(|_| f.write_all(&value.to_be_bytes()))
                    .and_then(|_| f.sync_data())
                    .map_err(|e| Unavailable(e.to_string()))
            }
        }
    }

    /// `compareAndSet`, which is not atomic: a get, then a set.
    pub async fn compare_and_set(&self, expected: i64, value: i64) -> Result<bool, Unavailable> {
        if self.get().await? != expected {
            return Ok(false);
        }
        self.set(value).await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn curator_parameters_are_checked() {
        let e = LockManager::new(
            CURATOR_CLASS,
            &props(&[("connect-string", "x"), ("bogus", "1")]),
        )
        .unwrap_err();
        assert_eq!(
            e,
            "non existent parameter bogus: accepted list is connect-string,connection-ms,namespace,retries,retries-ms,session-ms,session-percent"
        );
        assert!(LockManager::new("org.foo.Nope", &props(&[])).is_err());
    }

    #[tokio::test]
    async fn file_locks_exclude_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let p = props(&[("locks-folder", dir.path().to_str().unwrap())]);
        let a = LockManager::new(FILE_CLASS, &p).unwrap();
        let b = LockManager::new(FILE_CLASS, &p).unwrap();
        assert!(a.start(None).await && b.start(None).await);
        let la = a.distributed_lock("node").unwrap();
        let lb = b.distributed_lock("node").unwrap();
        assert!(la.try_lock().await.unwrap());
        assert!(la.is_held_by_caller().await.unwrap());
        assert!(!lb.try_lock().await.unwrap());
        la.unlock().await.unwrap();
        assert!(lb.try_lock().await.unwrap());
        let n = a.mutable_long("node").unwrap();
        assert_eq!(n.get().await.unwrap(), 0);
        n.set(3).await.unwrap();
        assert_eq!(b.mutable_long("node").unwrap().get().await.unwrap(), 3);
        assert!(!n.compare_and_set(2, 4).await.unwrap());
        assert!(n.compare_and_set(3, -4).await.unwrap());
        assert_eq!(n.get().await.unwrap(), -4);
        a.stop().await;
        b.stop().await;
    }
}
