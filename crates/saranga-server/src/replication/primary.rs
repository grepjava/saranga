//! The primary's side: a backup registers on the cluster channel, gets a
//! copy of the journals, pages and large messages as they are
//! (`JournalStorageManager.startReplication`), then everything written
//! after. Ported from `SharedNothingPrimaryActivation` and
//! `ReplicationManager`.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use saranga_buffer::SimpleString;
use saranga_core_protocol::packets::cluster::{
    BackupRegistrationMessage, BackupReplicationStartFailedMessage, backup_registration_problem,
};
use saranga_core_protocol::packets::replication::{
    JournalContent, PrimaryStopping, ReplicationPageEventMessage,
    ReplicationPrimaryIsStoppingMessage, ReplicationStartSyncMessage, ReplicationSyncFileMessage,
    SyncData, SyncDataType, SyncFileChunk, SyncFileType,
};

use super::{BINDINGS, JournalReplicator, MESSAGES, Replicator, lock};
use crate::Broker;
use crate::core_protocol::ConnShared;

/// `ChannelImpl.CHANNEL_ID.CLUSTER`.
const CHANNEL_CLUSTER: i64 = 3;

/// `sendLargeFile`'s chunks.
const CHUNK: usize = 32 * 1024;

/// Packets of the synchronization waiting for the backup at most: it
/// does not run ahead of the backup by more than a few megabytes.
const WINDOW: usize = 64;

impl Broker {
    /// `getActivationChannelHandler`, `BACKUP_REGISTRATION`: a backup asks
    /// for this primary's data. The replicator the connection hands the
    /// backup's responses to, or `None` when this broker does not
    /// replicate or already replicates to another backup.
    pub(crate) fn backup_registered(
        self: &Arc<Self>,
        conn: &Arc<ConnShared>,
        msg: BackupRegistrationMessage,
    ) -> Option<Arc<Replicator>> {
        let policy = self.replication_policy.clone()?;
        self.storage.as_ref()?;
        let r = {
            let mut current = lock(&self.replicating);
            if current.as_ref().is_some_and(|r| !r.is_stopped()) {
                drop(current);
                // `ActiveMQAlreadyReplicatingException`.
                conn.send(
                    CHANNEL_CLUSTER,
                    BackupReplicationStartFailedMessage {
                        problem: backup_registration_problem::ALREADY_REPLICATING,
                    },
                );
                return None;
            }
            let r = Replicator::new(conn.clone());
            *current = Some(r.clone());
            r
        };
        let broker = self.clone();
        let replicator = r.clone();
        let timeout = Duration::from_millis(policy.initial_replication_sync_timeout.max(1) as u64);
        // A primary restarted after its backup took over asks for its node
        // back; this broker hands back if its policy allows it.
        let fail_back = msg.backup_wants_fail_back && policy.allow_failback;
        tokio::spawn(async move {
            let b = broker.clone();
            let rr = replicator.clone();
            let synced =
                tokio::task::spawn_blocking(move || b.synchronize(&rr, timeout, fail_back)).await;
            match synced {
                Ok(Ok(())) => {
                    // `clusterConnection.nodeAnnounced`: the backup pairs
                    // with this primary in the topology clients get.
                    if let Some(cluster) = broker.cluster() {
                        cluster.node_announced(
                            super::now_millis(),
                            &broker.node_id,
                            policy.group_name.clone(),
                            None,
                            None,
                            Some(msg.connector),
                            true,
                        );
                        if fail_back {
                            broker.hand_back(cluster.clone(), replicator).await;
                        }
                    }
                }
                Ok(Err(e)) => {
                    if !replicator.is_stopped() {
                        tracing::warn!("AMQ222013: Error when trying to start replication: {e}");
                        conn_fail(&replicator);
                    }
                    broker.replication_stopped(&replicator);
                }
                Err(e) => {
                    tracing::warn!("AMQ222013: Error when trying to start replication: {e}");
                    broker.replication_stopped(&replicator);
                }
            }
        });
        Some(r)
    }

    /// `startReplication`'s synchronization, on a blocking thread.
    fn synchronize(
        &self,
        r: &Arc<Replicator>,
        timeout: Duration,
        fail_back: bool,
    ) -> std::io::Result<()> {
        let storage = self
            .storage
            .clone()
            .ok_or_else(|| std::io::Error::other("no journals"))?;
        let node_id = self.node_id.clone();
        let io = |e: saranga_journal::JournalError| std::io::Error::other(e.to_string());
        let start =
            |content: SyncDataType, ids: Vec<i64>, node_id: &str| ReplicationStartSyncMessage {
                synchronization_is_finished: false,
                // Only the journals' say whether this broker will hand back.
                allows_auto_fail_back: fail_back && content != SyncDataType::LargeMessages,
                node_id: node_id.into(),
                data: Some(SyncData {
                    data_type: content,
                    ids,
                }),
            };
        // prepareJournalForCopy, messages then bindings: each journal moves
        // to a new file, and the backup reserves the ones before it.
        let mut journals = Vec::new();
        for (content, id, journal) in [
            (
                SyncDataType::JournalMessages,
                MESSAGES,
                storage.messages_journal(),
            ),
            (
                SyncDataType::JournalBindings,
                BINDINGS,
                storage.bindings_journal(),
            ),
        ] {
            let replicator = Arc::new(JournalReplicator {
                journal: id,
                replicator: r.clone(),
            });
            let files = journal
                .start_replication(replicator, |files| {
                    r.send(
                        start(content, files.iter().map(|f| f.file_id).collect(), &node_id),
                        None,
                    )
                })
                .map_err(io)?;
            journals.push((content, journal.directory(), files));
        }
        let paging = self.post_office.paging();
        let pages = paging.start_replication(r.clone());
        let large = self.large_messages.start_replication(r.clone(), |ids| {
            r.send(start(SyncDataType::LargeMessages, ids.to_vec(), ""), None)
        })?;

        for (content, dir, files) in &journals {
            let content = match content {
                SyncDataType::JournalBindings => JournalContent::Bindings,
                _ => JournalContent::Messages,
            };
            for f in files {
                let path = dir.join(&f.name);
                tracing::info!(
                    "AMQ221025: Replication: sending NIOSequentialFile {} (size={}) to replica, origin=journal",
                    path.display(),
                    std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
                );
                send_file(
                    r,
                    SyncFileType::Journal(content),
                    f.file_id,
                    &path,
                    u64::MAX,
                    timeout,
                )?;
            }
        }
        for (id, path, size) in &large {
            send_file(r, SyncFileType::LargeMessage, *id, path, *size, timeout)?;
        }
        for (address, files) in &pages {
            let store = SimpleString::from(&**address);
            for (id, path) in files {
                tracing::info!(
                    "AMQ221025: Replication: sending NIOSequentialFile {} (size={}) to replica, origin={address}",
                    path.display(),
                    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
                );
                send_file(
                    r,
                    SyncFileType::Page(store.clone()),
                    *id,
                    path,
                    u64::MAX,
                    timeout,
                )?;
                r.send(
                    ReplicationPageEventMessage {
                        store_name: store.clone(),
                        page_number: *id,
                        is_delete: false,
                    },
                    None,
                );
            }
        }

        // sendSynchronizationDone: the backup is up to date once it answers.
        let (tx, rx) = std::sync::mpsc::channel();
        r.send(
            ReplicationStartSyncMessage {
                synchronization_is_finished: true,
                allows_auto_fail_back: false,
                node_id: node_id.as_str().into(),
                data: Some(SyncData {
                    data_type: SyncDataType::ActivationSequence,
                    ids: vec![0],
                }),
            },
            Some(Box::new(move |_| {
                let _ = tx.send(());
            })),
        );
        if rx.recv_timeout(timeout).is_err() || r.is_stopped() {
            return Err(std::io::Error::other(format!(
                "AMQ229114: Replication synchronization process timed out after waiting {} milliseconds",
                timeout.as_millis()
            )));
        }
        storage.messages_journal().sync_finished();
        storage.bindings_journal().sync_finished();
        paging.sync_finished();
        self.large_messages.sync_finished();
        r.synchronized
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// `startReplication` after a failback request: once the backup has
    /// announced itself with this node ID (`BackupTopologyListener`, a
    /// minute at most), this broker stops as if it failed, telling the
    /// backup to take over (`fail(true)`), and starts again as its backup.
    async fn hand_back(
        self: &Arc<Self>,
        cluster: Arc<crate::cluster::ClusterConnection>,
        r: Arc<Replicator>,
    ) {
        let listener =
            crate::cluster::FailbackListener::new(&self.node_id, cluster.connector().clone());
        let _subscription = crate::cluster::Subscription::new(&cluster, listener.clone());
        if !listener.wait(Duration::from_secs(60)).await {
            tracing::error!(
                "AMQ224057: Backup server that requested fail-back was not announced. Server will not stop for fail-back."
            );
            return;
        }
        if r.is_stopped() || self.is_stopped() {
            return;
        }
        // `sendPrimaryIsStopping`, the first thing `stop` does when
        // failing over.
        r.send(
            ReplicationPrimaryIsStoppingMessage {
                primary_stopping: PrimaryStopping::StopCalled,
            },
            None,
        );
        self.failing_back
            .store(true, std::sync::atomic::Ordering::Release);
        self.failback.notify_one();
    }

    /// The storage's stop with replication: when failing back, the backup
    /// is told to take over (`FAIL_OVER`, five seconds at most for its
    /// answer); then nothing more is replicated and its connection closes.
    pub(crate) fn stop_replicating(&self) {
        let Some(r) = lock(&self.replicating).clone() else {
            return;
        };
        if self.failing_back.load(std::sync::atomic::Ordering::Acquire) && !r.is_stopped() {
            let (tx, rx) = std::sync::mpsc::channel();
            r.send(
                ReplicationPrimaryIsStoppingMessage {
                    primary_stopping: PrimaryStopping::FailOver,
                },
                Some(Box::new(move |_| {
                    let _ = tx.send(());
                })),
            );
            let _ = rx.recv_timeout(Duration::from_secs(5));
        }
        self.replication_stopped(&r);
        conn_fail(&r);
    }

    /// The connection of the backup this broker replicates to, which
    /// `freeze` leaves open while the clients' close.
    pub fn is_backup_connection(&self, c: &Arc<crate::session::ClientConnection>) -> bool {
        lock(&self.replicating)
            .as_ref()
            .is_some_and(|r| Arc::ptr_eq(&r.conn.client, c))
    }

    /// `ReplicationFailureListener`: the backup's connection is gone. The
    /// primary goes on alone; with `vote-on-replication-failure` it first
    /// asks the cluster whether it is still the primary, and stops if not.
    pub(crate) fn backup_connection_lost(self: &Arc<Self>, r: &Arc<Replicator>, failed: bool) {
        let attached = lock(&self.replicating)
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, r));
        if !attached {
            return;
        }
        if failed {
            tracing::warn!(
                "AMQ222092: Connection to the backup node failed, removing replication now"
            );
        } else {
            tracing::warn!(
                "AMQ222091: The backup node has been shut-down, replication will now stop"
            );
        }
        self.replication_stopped(r);
        let vote = self
            .replication_policy
            .as_ref()
            .filter(|p| failed && p.vote_on_replication_failure)
            .cloned();
        if let Some(policy) = vote
            && !self.is_stopped()
        {
            let broker = self.clone();
            tokio::spawn(async move {
                if !super::quorum::is_still_active(&broker, &policy).await {
                    tracing::warn!(
                        "AMQ222200: Could not find a primary or backup in the cluster that agrees this server is active, stopping"
                    );
                    broker.request_shutdown();
                }
            });
        }
    }

    /// `stopReplication`: nothing more is replicated, and what waited for
    /// the backup is released.
    pub(crate) fn replication_stopped(&self, r: &Arc<Replicator>) {
        {
            let mut current = lock(&self.replicating);
            if current.as_ref().is_some_and(|c| Arc::ptr_eq(c, r)) {
                *current = None;
            } else {
                r.stop();
                return;
            }
        }
        if let Some(s) = &self.storage {
            s.messages_journal().stop_replication();
            s.bindings_journal().stop_replication();
        }
        self.post_office.paging().stop_replication();
        self.large_messages.stop_replication();
        r.stop();
    }

    /// A backup is attached and has its copy of this broker's data
    /// (`isReplicaSync`).
    pub fn is_replica_sync(&self) -> bool {
        lock(&self.replicating)
            .as_ref()
            .is_some_and(|r| !r.is_stopped() && r.is_synchronized())
    }
}

/// The synchronization failed: the backup's connection is closed, as
/// `stopReplication` closes it.
fn conn_fail(r: &Replicator) {
    r.conn.close();
}

/// `sendLargeFile`: a file in chunks. A journal or page file ends with a
/// packet without data, which closes the backup's copy; a large message's
/// copy stays open for what is appended to it afterwards.
fn send_file(
    r: &Replicator,
    file_type: SyncFileType,
    id: i64,
    path: &Path,
    max: u64,
    timeout: Duration,
) -> std::io::Result<()> {
    let mut f = File::open(path)?;
    let mut left = max;
    let mut buf = vec![0u8; CHUNK];
    loop {
        if !r.wait_window(WINDOW, timeout) {
            return Err(std::io::Error::other(if r.is_stopped() {
                "replication stopped".to_owned()
            } else {
                format!(
                    "AMQ229114: Replication synchronization process timed out after waiting {} milliseconds",
                    timeout.as_millis()
                )
            }));
        }
        let want = (left.min(CHUNK as u64)) as usize;
        let n = if want == 0 {
            0
        } else {
            f.read(&mut buf[..want])?
        };
        left -= n as u64;
        let last = n == 0 || left == 0;
        let data_size = if n == 0 { -1 } else { n as i32 };
        r.send(
            ReplicationSyncFileMessage {
                file_id: id,
                chunk: Some(SyncFileChunk {
                    file_type: file_type.clone(),
                    data_size,
                    data: buf[..n].to_vec(),
                    trailing: Vec::new(),
                }),
            },
            None,
        );
        if last {
            return Ok(());
        }
    }
}
