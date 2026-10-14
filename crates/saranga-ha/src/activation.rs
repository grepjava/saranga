//! Activation sequences (`ActivationSequenceStateMachine`): each node ID
//! has a coordinated sequence in the lock manager and a local one in
//! `server.activation.sequence`, beside `server.lock`. A broker may be
//! active on its data only with the node's lock and a local sequence the
//! coordinated one has not moved past; it claims the next sequence
//! (negated) before taking it locally, then commits it.

use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::lockmanager::{DistributedLock, LockManager, MutableLong, Unavailable};

/// `NodeManager.NULL_NODE_ACTIVATION_SEQUENCE`.
pub const NULL_SEQUENCE: i64 = -1;

/// `FileBasedNodeManager.SERVER_ACTIVATION_SEQUENCE_NAME`.
pub const SEQUENCE_FILE: &str = "server.activation.sequence";

const CHECK_ACTIVATION_SEQUENCE_WAIT: Duration = Duration::from_millis(200);
const PRIMARY_LOCK_ACQUIRE_TIMEOUT: Duration = Duration::from_millis(2000);

/// `readNodeActivationSequence`: [`NULL_SEQUENCE`] without the file or its
/// eight bytes.
pub fn read_sequence(dir: &Path) -> io::Result<i64> {
    let mut f = match OpenOptions::new().read(true).open(dir.join(SEQUENCE_FILE)) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(NULL_SEQUENCE),
        Err(e) => return Err(e),
    };
    let mut b = [0u8; 8];
    let mut n = 0;
    while n < 8 {
        match f.read(&mut b[n..])? {
            0 => return Ok(NULL_SEQUENCE),
            k => n += k,
        }
    }
    Ok(i64::from_be_bytes(b))
}

/// `writeNodeActivationSequence`: big-endian at the start, forced to disk.
pub fn write_sequence(dir: &Path, sequence: i64) -> io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join(SEQUENCE_FILE))?;
    f.seek(SeekFrom::Start(0))?;
    f.write_all(&sequence.to_be_bytes())?;
    f.sync_data()
}

/// The node manager's part: the node ID and its local sequence.
#[derive(Debug, Clone)]
pub struct LocalNode {
    pub dir: PathBuf,
    pub node_id: String,
    pub sequence: i64,
}

impl LocalNode {
    pub fn read(dir: &Path, node_id: &str) -> io::Result<Self> {
        Ok(Self {
            dir: dir.to_path_buf(),
            node_id: node_id.to_owned(),
            sequence: read_sequence(dir)?,
        })
    }

    pub fn write(&mut self, sequence: i64) -> io::Result<()> {
        write_sequence(&self.dir, sequence)?;
        self.sequence = sequence;
        Ok(())
    }
}

/// Why an activation step failed.
#[derive(Debug, thiserror::Error)]
pub enum ActivationError {
    /// The lock manager cannot answer: a broker can start it again and
    /// retry.
    #[error(transparent)]
    Unavailable(#[from] Unavailable),
    /// The local sequence could not be written (`NodeManagerException`).
    #[error("{0}")]
    Local(#[from] io::Error),
    /// The coordinated sequence moved (`ActiveMQException`).
    #[error("{0}")]
    Refused(String),
}

enum Validation {
    Stale,
    InSync,
    MaybeInSync,
    SelfRepair,
}

async fn validate(
    coordinated: &MutableLong,
    id: &str,
    local: i64,
) -> Result<Validation, Unavailable> {
    let current = coordinated.get().await?;
    if local == current {
        return Ok(Validation::InSync);
    }
    if current > 0 {
        tracing::info!(
            "Not a candidate for NodeID = {id} activation, local activation sequence {local} does not match coordinated activation sequence {current}"
        );
        return Ok(Validation::Stale);
    }
    let claimed = -current;
    match claimed - local {
        0 => Ok(Validation::SelfRepair),
        1 => Ok(Validation::MaybeInSync),
        _ => {
            tracing::info!(
                "Not a candidate for NodeID = {id} activation, local activation sequence {local} does not match coordinated activation sequence {claimed}"
            );
            Ok(Validation::Stale)
        }
    }
}

/// `tryActivate`: the node's lock, once this broker's data is the latest
/// (repairing an unfinished claim), or `None` when its data is stale. It
/// waits while the data may be valuable but the lock is held.
pub async fn try_activate(
    node: &mut LocalNode,
    manager: &LockManager,
) -> Result<Option<DistributedLock>, ActivationError> {
    let id = node.node_id.clone();
    let local = node.sequence;
    if local < 0 {
        return Err(ActivationError::Refused(
            "nodeActivationSequence must be > 0".into(),
        ));
    }
    let lock = manager.distributed_lock(&id)?;
    let coordinated = manager.mutable_long(&id)?;
    loop {
        let timeout = match validate(&coordinated, &id, local).await? {
            Validation::Stale => {
                lock.close().await;
                return Ok(None);
            }
            Validation::InSync => PRIMARY_LOCK_ACQUIRE_TIMEOUT,
            Validation::SelfRepair | Validation::MaybeInSync => Duration::ZERO,
        };
        if !lock.try_lock_for(timeout).await? {
            tracing::debug!(
                "Candidate for Node ID = {id}, with local activation sequence: {local}, cannot acquire primary lock within {}; retrying",
                timeout.as_millis()
            );
            if timeout.is_zero() {
                tokio::time::sleep(CHECK_ACTIVATION_SEQUENCE_WAIT).await;
            }
            continue;
        }
        match validate(&coordinated, &id, local).await? {
            Validation::Stale => {
                lock.close().await;
                return Ok(None);
            }
            Validation::InSync => {
                tracing::info!(
                    "Assuming active role for NodeID = {id}, local activation sequence {local} matches current coordinated activation sequence {local}"
                );
                return Ok(Some(lock));
            }
            Validation::SelfRepair => {
                tracing::info!(
                    "Assuming active role for NodeID = {id}: local activation sequence {local} matches claimed coordinated activation sequence {local}. Repairing sequence"
                );
                if let Err(e) = coordinated.set(local).await {
                    lock.close().await;
                    return Err(e.into());
                }
                return Ok(Some(lock));
            }
            Validation::MaybeInSync => {
                tracing::warn!(
                    "Assuming active role for NodeID = {id}: repairing claimed activation sequence"
                );
                let next = local + 1;
                let repaired = match node.write(next) {
                    Ok(()) => coordinated.set(next).await.map_err(ActivationError::from),
                    Err(e) => Err(e.into()),
                };
                if let Err(e) = repaired {
                    lock.close().await;
                    return Err(e);
                }
                return Ok(Some(lock));
            }
        }
    }
}

/// `awaitNextCommittedActivationSequence`: whether another broker
/// activated past `sequence` within `timeout`.
pub async fn await_next_committed(
    manager: &LockManager,
    id: &str,
    sequence: i64,
    timeout: Duration,
) -> Result<bool, Unavailable> {
    let coordinated = manager.mutable_long(id)?;
    let start = tokio::time::Instant::now();
    loop {
        let value = coordinated.get().await?;
        if value > sequence {
            tracing::info!(
                "Detected a new activation sequence with NodeID = {id}: and sequence: {value}"
            );
            return Ok(true);
        }
        if value < 0 {
            let claimed = -value;
            if claimed - sequence > 1 {
                tracing::info!(
                    "Detected furthers sequential server activations from sequence {sequence}, with NodeID = {id}: and claimed sequence: {claimed}"
                );
                return Ok(true);
            }
            tracing::debug!(
                "Detected claiming of activation sequence = {claimed} for NodeID = {id}"
            );
        }
        tokio::time::sleep(CHECK_ACTIVATION_SEQUENCE_WAIT).await;
        if start.elapsed() >= timeout {
            return Ok(false);
        }
    }
}

/// `ensureSequentialAccessToNodeData`: holding the node's lock, the next
/// sequence is claimed, taken locally and committed, so that no other
/// copy of the data can activate.
pub async fn ensure_sequential_access(
    server: &str,
    node: &mut LocalNode,
    manager: &LockManager,
) -> Result<(), ActivationError> {
    let id = node.node_id.clone();
    let local = node.sequence;
    if local < 0 {
        return Err(ActivationError::Refused(
            "nodeActivationSequence must be >= 0".into(),
        ));
    }
    let lock = manager.distributed_lock(&id)?;
    if !lock.is_held_by_caller().await? {
        let m = format!(
            "Server [{server}] primary lock for NodeID = {id} not held. Activation sequence cannot be safely changed"
        );
        tracing::info!("{m}");
        return Err(Unavailable(m).into());
    }
    let coordinated = manager.mutable_long(&id)?;
    let next = local + 1;
    if !coordinated.compare_and_set(local, -next).await? {
        let m = format!(
            "Server [{server}], cannot assume active role for NodeID = {id}, activation sequence claim failed, local activation sequence {local} no longer matches current coordinated sequence {}",
            coordinated.get().await?
        );
        tracing::info!("{m}");
        return Err(ActivationError::Refused(m));
    }
    if let Err(e) = node.write(next) {
        tracing::error!(
            "Server [{server}] failed to set local activation sequence to: {next} for NodeId ={id}. Cannot continue committing coordinated activation sequence: REQUIRES ADMIN INTERVENTION"
        );
        return Err(Unavailable(e.to_string()).into());
    }
    tracing::info!(
        "Server [{server}], incremented local activation sequence to: {next} for NodeId = {id}"
    );
    if !coordinated.compare_and_set(-next, next).await? {
        let m = format!(
            "Server [{server}], cannot assume active role for NodeID = {id}, activation sequence commit failed, local activation sequence {local} no longer matches current coordinated sequence {}",
            coordinated.get().await?
        );
        tracing::info!("{m}");
        return Err(ActivationError::Refused(m));
    }
    tracing::info!(
        "Server [{server}], incremented coordinated activation sequence to: {next} for NodeId = {id}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lockmanager::FILE_CLASS;
    use std::collections::BTreeMap;

    #[test]
    fn sequence_file() {
        let d = tempfile::tempdir().unwrap();
        assert_eq!(read_sequence(d.path()).unwrap(), NULL_SEQUENCE);
        write_sequence(d.path(), 7).unwrap();
        assert_eq!(read_sequence(d.path()).unwrap(), 7);
        assert_eq!(
            std::fs::read(d.path().join(SEQUENCE_FILE)).unwrap(),
            7i64.to_be_bytes()
        );
    }

    #[tokio::test]
    async fn a_primary_activates_and_its_stale_peer_does_not() {
        let locks = tempfile::tempdir().unwrap();
        let (a_dir, b_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let p: BTreeMap<_, _> = [(
            "locks-folder".to_owned(),
            locks.path().display().to_string(),
        )]
        .into();
        let ma = LockManager::new(FILE_CLASS, &p).unwrap();
        let mb = LockManager::new(FILE_CLASS, &p).unwrap();
        ma.start(None).await;
        mb.start(None).await;
        // A fresh primary: sequence 0, activates and moves to 1.
        let mut a = LocalNode::read(a_dir.path(), "n").unwrap();
        a.write(0).unwrap();
        let la = try_activate(&mut a, &ma).await.unwrap().expect("in sync");
        ensure_sequential_access("a", &mut a, &ma).await.unwrap();
        assert_eq!(a.sequence, 1);
        assert_eq!(read_sequence(a_dir.path()).unwrap(), 1);
        assert_eq!(ma.mutable_long("n").unwrap().get().await.unwrap(), 1);
        // A replica in sync at 1 waits for the lock; it gets it once the
        // primary goes, then a primary still at 0 is stale.
        let mut b = LocalNode::read(b_dir.path(), "n").unwrap();
        b.write(1).unwrap();
        la.close().await;
        let lb = try_activate(&mut b, &mb).await.unwrap().expect("in sync");
        ensure_sequential_access("b", &mut b, &mb).await.unwrap();
        assert_eq!(b.sequence, 2);
        lb.close().await;
        assert!(try_activate(&mut a, &ma).await.unwrap().is_none());
        // An unfinished claim one ahead is repaired.
        mb.mutable_long("n").unwrap().set(-3).await.unwrap();
        let lb = try_activate(&mut b, &mb).await.unwrap().expect("repaired");
        assert_eq!(b.sequence, 3);
        assert_eq!(mb.mutable_long("n").unwrap().get().await.unwrap(), 3);
        lb.close().await;
        assert!(
            await_next_committed(&ma, "n", 2, Duration::from_millis(100))
                .await
                .unwrap()
        );
    }
}
