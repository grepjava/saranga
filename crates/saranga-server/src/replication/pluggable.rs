//! Replication coordinated through a pluggable lock manager
//! (`ReplicationPrimaryActivation`, `ReplicationBackupActivation`): a
//! broker is active on a node's data only while it holds the node's lock
//! in the manager, with the latest activation sequence. Nothing is voted
//! and nothing is decided alone.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

use saranga_ha::activation::{
    self, ActivationError, LocalNode, NULL_SEQUENCE, ensure_sequential_access, try_activate,
};
use saranga_ha::lockmanager::{DistributedLock, LockManager};

use super::backup::{BackupConfig, Pluggable};

/// `ReplicationPrimaryActivation.FAILBACK_TIMEOUT_MILLIS`: how long a
/// broker that handed back waits for the other to activate.
const FAILBACK_TIMEOUT: Duration = Duration::from_millis(4_000);

/// What an active broker holds: the manager, the node's lock and the
/// node's local activation sequence.
pub struct Coordination {
    pub manager: Arc<LockManager>,
    pub lock: DistributedLock,
    node: tokio::sync::Mutex<LocalNode>,
    node_id: String,
    /// As the logs name the server.
    server: String,
}

impl std::fmt::Debug for Coordination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Coordination")
            .field("node_id", &self.node_id)
            .finish()
    }
}

impl Coordination {
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// The local activation sequence, from a blocking thread.
    pub fn sequence_blocking(&self) -> i64 {
        self.node.blocking_lock().sequence
    }

    pub async fn sequence(&self) -> i64 {
        self.node.lock().await.sequence
    }

    /// `ensureSequentialAccessToNodeData` while active, when the backup's
    /// connection goes: its copy is no longer the latest.
    pub async fn ensure_sequential_access(&self) -> Result<(), ActivationError> {
        let mut node = self.node.lock().await;
        ensure_sequential_access(&self.server, &mut node, &self.manager).await
    }

    /// `restartAsBackupAfterFailback`, once stopped: the manager started
    /// again until the broker handed back to has activated past this
    /// one's sequence, a few seconds at most.
    pub async fn await_failback(&self) {
        let sequence = self.sequence().await;
        if self.manager.start(None).await {
            match activation::await_next_committed(
                &self.manager,
                &self.node_id,
                sequence,
                FAILBACK_TIMEOUT,
            )
            .await
            {
                Ok(true) => {}
                Ok(false) => tracing::warn!(
                    "Timed out waiting for failback server activation with NodeID = {}: and sequence > {sequence}: after {}ms",
                    self.node_id,
                    FAILBACK_TIMEOUT.as_millis()
                ),
                Err(e) => tracing::debug!(
                    "Unavailable distributed manager while awaiting failback activation sequence: ignored: {e}"
                ),
            }
        }
        self.manager.stop().await;
    }

    /// `onUnavailableLockEvent`: once the lock is lost, the broker is
    /// asked to stop.
    pub(crate) fn watch(self: &Arc<Self>, broker: Weak<crate::Broker>) {
        let mut lost = self.lock.lost();
        let mut unavailable = self.manager.unavailable();
        tokio::spawn(async move {
            let unavailable_changed = async {
                match unavailable.as_mut() {
                    Some(u) => {
                        let _ = u.wait_for(|u| *u).await;
                    }
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                r = lost.wait_for(|l| *l) => if r.is_err() { return },
                _ = unavailable_changed => {}
            }
            if let Some(b) = broker.upgrade()
                && !b.is_stopped()
            {
                tracing::error!("Quorum UNAVAILABLE: async stopping broker.");
                b.request_shutdown();
            }
        });
    }
}

/// The node ID a broker coordinates on, with its local sequence:
/// `coordination-id` when the primary has one (`applyCoordinationId`,
/// written to `server.lock` as `setNodeID` writes it), or the node ID in
/// `server.lock`.
pub async fn local_node(dir: &Path, coordination_id: Option<&str>) -> std::io::Result<LocalNode> {
    let dir = dir.to_path_buf();
    let coordination_id = coordination_id.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        let node_id = match coordination_id {
            Some(id) => {
                tracing::info!("Applying shared peer NodeID={id} to enable coordinated activation");
                crate::node::write_node_id(&dir, &id)?;
                id
            }
            None => crate::node::NodeManager::open(&dir, None)?.node_id(),
        };
        LocalNode::read(&dir, &node_id)
    })
    .await
    .map_err(std::io::Error::other)?
}

/// How a broker starts: the policy it is active with once coordinated,
/// and whether it was a backup that became active.
pub struct Activated {
    pub coordination: Arc<Coordination>,
    /// It replicated another broker first, or activated as a backup
    /// (`AMQ221037`).
    pub from_backup: bool,
}

/// What [`activate`] needs.
pub struct Activation {
    /// The broker's name, as the logs name it.
    pub name: String,
    pub manager: Arc<LockManager>,
    /// The node manager's directory.
    pub lock_dir: PathBuf,
    /// The configured policy is a primary's.
    pub primary: bool,
    /// Starting again as a backup, after handing back.
    pub as_backup: bool,
    pub coordination_id: Option<String>,
    /// How to replicate, `pluggable` set here.
    pub backup: BackupConfig,
    pub version: String,
}

/// The manager started, again after it was lost, until it answers.
async fn start(manager: &LockManager) {
    while !manager.start(None).await {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// `tryActivate`, the manager started again while it cannot answer.
async fn activate_on(
    node: &mut LocalNode,
    manager: &LockManager,
) -> Result<Option<DistributedLock>, String> {
    loop {
        start(manager).await;
        match try_activate(node, manager).await {
            Ok(l) => return Ok(l),
            Err(ActivationError::Unavailable(_)) => manager.stop().await,
            Err(e) => {
                manager.stop().await;
                return Err(e.to_string());
            }
        }
    }
}

/// `ReplicationPrimaryActivation.run` and `ReplicationBackupActivation.run`
/// up to the activation: a primary activates if its data is the latest,
/// and replicates the broker with its node ID otherwise, asking it to
/// hand back; a backup activates on data it kept, or replicates a primary
/// until the primary goes, then competes for its lock.
pub async fn activate(a: Activation) -> Result<Activated, String> {
    let manager = a.manager.clone();
    let mut as_backup = !a.primary || a.as_backup;
    // A primary's backup policy fails back (`ReplicationBackupPolicy.failback`).
    let try_failback = a.primary;
    let server = format!("Saranga::name={}", a.name);
    let io = |e: std::io::Error| e.to_string();
    loop {
        let coordination_id = a.coordination_id.as_deref().filter(|_| a.primary);
        let mut node = local_node(&a.lock_dir, coordination_id).await.map_err(io)?;
        if !as_backup {
            if node.sequence == NULL_SEQUENCE {
                // Persists an initial activation sequence.
                node.write(0).map_err(io)?;
            }
            match activate_on(&mut node, &manager).await? {
                Some(lock) => return active(&server, node, lock, &manager, false).await,
                None => {
                    manager.stop().await;
                    tracing::info!(
                        "This broker cannot become primary with NodeID = {}: restarting as backup",
                        node.node_id
                    );
                    as_backup = true;
                    continue;
                }
            }
        }
        start(&manager).await;
        // Only a backup with a positive local sequence may have valuable
        // data.
        if node.sequence > 0 {
            match activate_on(&mut node, &manager).await? {
                Some(lock) => return active(&server, node, lock, &manager, true).await,
                None => start(&manager).await,
            }
        }
        let mut cfg = a.backup.clone();
        cfg.fail_back = try_failback;
        cfg.pluggable = Some(Pluggable {
            lock_dir: a.lock_dir.clone(),
            expected_node_id: try_failback.then(|| node.node_id.clone()),
        });
        let Some(mut unavailable) = manager.unavailable() else {
            continue;
        };
        let replicated = tokio::select! {
            r = super::backup::replicate(cfg, &a.version) => r.map_err(io)?,
            _ = async { let _ = unavailable.wait_for(|u| *u).await; } => {
                tracing::info!("Unavailable quorum service detected: try restart server");
                manager.stop().await;
                continue;
            }
        };
        // The copy is complete and the primary gone: the node ID and
        // sequence were persisted with the copy.
        let mut node = LocalNode::read(&a.lock_dir, &replicated).map_err(io)?;
        if node.sequence > 0 {
            let lock = match try_activate(&mut node, &manager).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::warn!("Errored while attempting failover: {e}");
                    None
                }
            };
            if let Some(lock) = lock {
                return active(&server, node, lock, &manager, true).await;
            }
        } else {
            tracing::error!(
                "Expected positive local activation sequence for NodeID = {} during fail-over, but was {}: restarting as backup",
                node.node_id,
                node.sequence
            );
        }
        tracing::info!("AMQ221070: Restarting as backup based on quorum vote results.");
        manager.stop().await;
        as_backup = true;
    }
}

/// `startAsPrimary` and the primary's own start: the next sequence taken
/// while holding the lock.
async fn active(
    server: &str,
    mut node: LocalNode,
    lock: DistributedLock,
    manager: &Arc<LockManager>,
    from_backup: bool,
) -> Result<Activated, String> {
    if let Err(e) = ensure_sequential_access(server, &mut node, manager).await {
        tracing::warn!("{e}");
        manager.stop().await;
        return Err(if from_backup {
            "This server cannot ensure sequential access to broker data: activation is failed"
                .to_owned()
        } else {
            e.to_string()
        });
    }
    Ok(Activated {
        coordination: Arc::new(Coordination {
            manager: manager.clone(),
            lock,
            node_id: node.node_id.clone(),
            node: tokio::sync::Mutex::new(node),
            server: server.to_owned(),
        }),
        from_backup,
    })
}
