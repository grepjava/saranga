//! The backup's side: it moves its old data away, finds its primary
//! through the cluster connection, registers, and keeps a copy of
//! everything the primary writes until the primary fails. Ported from
//! `SharedNothingBackupActivation`, `SharedNothingBackupQuorum`,
//! `ReplicationEndpoint` and `FileMoveManager`.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use saranga_buffer::{Codec, Writer};
use saranga_client::{Connection, ConnectionOptions, Connector};
use saranga_core_protocol::Packet;
use saranga_core_protocol::packets::cluster::{
    BackupRegistrationMessage, backup_registration_problem,
};
use saranga_core_protocol::packets::replication::{
    AddOperationType, JournalContent, PrimaryStopping, ReplicationResponseMessage,
    ReplicationResponseMessageV2, ReplicationStartSyncMessage, ReplicationSyncFileMessage,
    SyncDataType, SyncFileType,
};
use saranga_journal::format::Record;
use saranga_journal::{Journal, JournalConfig, SyncJournal};

use super::CHANNEL_REPLICATION;
use crate::cluster::ClusterConfig;

/// `ChannelImpl.CHANNEL_ID.CLUSTER`.
const CHANNEL_CLUSTER: i64 = 3;

/// `SharedNothingBackupQuorum.WAIT_TIME_AFTER_FIRST_PRIMARY_STOPPING_MSG`.
const WAIT_AFTER_STOP_CALLED: Duration = Duration::from_secs(60);

/// What a replicating backup needs to know.
#[derive(Debug, Clone)]
pub struct BackupConfig {
    /// The broker's name, as the logs name it.
    pub name: String,
    pub policy: saranga_config::Replication,
    pub cluster: ClusterConfig,
    pub messages: JournalConfig,
    pub bindings: JournalConfig,
    pub paging: PathBuf,
    pub large_messages: PathBuf,
    /// `attemptFailBack`: a primary that found its node active elsewhere
    /// copies the active broker's data, asking it to hand back once done.
    pub fail_back: bool,
    /// Replication coordinated through a lock manager
    /// (`ReplicationBackupActivation`): no votes.
    pub pluggable: Option<Pluggable>,
}

/// What a backup coordinated through a lock manager needs.
#[derive(Debug, Clone)]
pub struct Pluggable {
    /// The node manager's directory, where the node ID and the activation
    /// sequence are written once the copy is complete.
    pub lock_dir: PathBuf,
    /// When failing back: the node ID this broker must replicate
    /// (`NamedNodeIdLocatorForReplication`).
    pub expected_node_id: Option<String>,
}

/// `SharedNothingBackupActivation.run` up to the activation: returns the
/// node ID of the primary that failed, whose copy this backup now has.
/// With a lock manager (`ReplicationBackupActivation.replicateAndFailover`)
/// it returns once the primary is gone with the copy complete, and fails
/// when the primary sends what this backup must not take.
pub async fn replicate(cfg: BackupConfig, version: &str) -> io::Result<String> {
    let mut logged = false;
    loop {
        let c = cfg.clone();
        tokio::task::spawn_blocking(move || move_server_data(&c))
            .await
            .map_err(io::Error::other)??;
        if !logged {
            if cfg.pluggable.is_some() {
                tracing::info!(
                    "Saranga Backup Server version {version} [Saranga::name={}] started, awaiting connection to a primary to start replication",
                    cfg.name
                );
            } else {
                tracing::info!(
                    "AMQ221109: Saranga Backup Server version {version} [null] started; waiting for primary to fail before activating"
                );
            }
            logged = true;
        }
        if let Some(id) = cfg
            .pluggable
            .as_ref()
            .and_then(|p| p.expected_node_id.as_ref())
        {
            tracing::info!("awaiting connecting to node with NodeID = {id}");
        }
        let (conn, primary) = locate(&cfg).await;
        match attempt(&cfg, conn, primary).await? {
            Signal::FailOver(node_id) => return Ok(node_id),
            Signal::Retry => {
                tokio::time::sleep(Duration::from_millis(
                    cfg.policy.retry_replication_wait.max(1) as u64,
                ))
                .await;
            }
        }
    }
}

enum Signal {
    FailOver(String),
    Retry,
}

fn options(cluster: &ClusterConfig) -> ConnectionOptions {
    ConnectionOptions {
        client_failure_check_period: Some(cluster.check_period),
        connection_ttl: Some(cluster.connection_ttl),
        call_timeout: cluster.call_timeout,
        user: Some(cluster.cluster_user.clone()),
        password: Some(cluster.cluster_password.clone()),
        cluster_connection: true,
        ..ConnectionOptions::default()
    }
}

/// `NamedNodeLocatorForReplication` / `AnyNodeLocatorForReplication`: a
/// primary from the topology of the cluster connection's brokers, of the
/// backup's group if it has one, connected to and authorized as the
/// cluster user. Tried until one answers.
async fn locate(cfg: &BackupConfig) -> (Connection, String) {
    let cluster = &cfg.cluster;
    let mut next = 0usize;
    loop {
        if !cluster.static_connectors.is_empty() {
            let target = &cluster.static_connectors[next % cluster.static_connectors.len()];
            next += 1;
            if let Some(found) = try_locate(cfg, target).await {
                return found;
            }
        }
        tokio::time::sleep(cluster.retry_interval.max(Duration::from_millis(100))).await;
    }
}

async fn try_locate(
    cfg: &BackupConfig,
    target: &saranga_core_protocol::transport::TransportConfiguration,
) -> Option<(Connection, String)> {
    let cluster = &cfg.cluster;
    let (host, port) = crate::cluster::socket_address(target)?;
    let conn = Connection::connect((host.as_str(), port), options(cluster))
        .await
        .ok()?;
    // Saranga's client keeps no backup group names, so `group-name` does
    // not choose among the primaries.
    // When failing back, only the broker with this node's ID.
    let expected = cfg
        .pluggable
        .as_ref()
        .and_then(|p| p.expected_node_id.as_deref());
    let found = conn
        .topology()
        .into_iter()
        .find(|(id, primary, _)| primary.is_some() && expected.is_none_or(|e| e == id));
    let Some((node_id, Some(primary), _)) = found else {
        conn.close().await;
        return None;
    };
    // The primary itself, if the connector reached another broker.
    let here = Connector::new(host.clone(), port);
    let conn = if primary.is_equivalent(&here) {
        conn
    } else {
        conn.close().await;
        Connection::connect((primary.host.as_str(), primary.port), options(cluster))
            .await
            .ok()?
    };
    match conn
        .cluster_connect(&cluster.cluster_user, &cluster.cluster_password)
        .await
    {
        Ok(true) => Some((conn, node_id)),
        _ => {
            tracing::warn!(
                "AMQ224105: Connecting to cluster failed: cluster user {} not accepted by {primary}",
                cluster.cluster_user
            );
            conn.close().await;
            None
        }
    }
}

/// `SharedNothingPrimaryActivation.isNodeIdUsed`: whether a broker of the
/// cluster is active with this node ID, as an active backup is after its
/// primary failed. The cluster connection's brokers are asked for their
/// topology, and the broker it names for the node must take a session.
pub async fn is_node_id_used(cluster: &ClusterConfig, node_id: &str) -> bool {
    let options = ConnectionOptions {
        reconnect_attempts: 0,
        ..options(cluster)
    };
    for target in &cluster.static_connectors {
        let Some((host, port)) = crate::cluster::socket_address(target) else {
            continue;
        };
        // `listener.latch.await(5, SECONDS)`.
        let connected = tokio::time::timeout(
            Duration::from_secs(5),
            Connection::connect((host.as_str(), port), options.clone()),
        )
        .await;
        let Ok(Ok(conn)) = connected else {
            continue;
        };
        let primary = conn
            .topology()
            .into_iter()
            .find(|(id, _, _)| id == node_id)
            .and_then(|(_, primary, _)| primary);
        conn.close().await;
        return match primary {
            Some(p) => is_active(cluster, &p).await,
            None => false,
        };
    }
    false
}

/// `NodeIdListener.isActive`: a session as the cluster user.
async fn is_active(cluster: &ClusterConfig, primary: &Connector) -> bool {
    let options = ConnectionOptions {
        reconnect_attempts: 0,
        user: Some(cluster.cluster_user.clone()),
        password: Some(cluster.cluster_password.clone()),
        ..ConnectionOptions::default()
    };
    let session = async {
        let conn = Connection::connect((primary.host.as_str(), primary.port), options)
            .await
            .ok()?;
        let session = conn
            .create_session(saranga_client::SessionOptions {
                username: Some(cluster.cluster_user.clone()),
                password: Some(cluster.cluster_password.clone()),
                ..saranga_client::SessionOptions::default()
            })
            .await;
        conn.close().await;
        session.ok()
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(5), session).await,
        Ok(Some(_))
    )
}

/// One replication from a primary: registration, then the copy, until
/// the connection goes or the primary says it stops.
async fn attempt(cfg: &BackupConfig, conn: Connection, primary_node: String) -> io::Result<Signal> {
    let packets = conn.channel_packets(CHANNEL_REPLICATION);
    let mut cluster_packets = conn.channel_packets(CHANNEL_CLUSTER);
    conn.send_packet(
        CHANNEL_CLUSTER,
        BackupRegistrationMessage {
            cluster_user: cfg.cluster.cluster_user.as_str().into(),
            cluster_password: cfg.cluster.cluster_password.as_str().into(),
            backup_wants_fail_back: cfg.fail_back,
            connector: cfg.cluster.connector.clone(),
        }
        .into(),
    )
    .map_err(|e| io::Error::other(e.to_string()))?;
    let sender = conn.clone();
    let c = cfg.clone();
    let (synced, mut synced_rx) = tokio::sync::watch::channel(None);
    // Dropped with this future when the broker stops while it is a
    // backup: the endpoint's thread then lets go of the connection.
    let (_running, stopped) = tokio::sync::watch::channel(());
    let incoming = Incoming {
        packets,
        stopped,
        handle: tokio::runtime::Handle::current(),
    };
    let mut endpoint = tokio::task::spawn_blocking(move || {
        let mut e = Endpoint::new(c, synced);
        let end = e.run(incoming, &sender);
        (e, end)
    });
    // `BackupManager.announceBackup`, once the copy is complete.
    let mut announcer = None;
    let (mut endpoint, end) = loop {
        tokio::select! {
            r = &mut endpoint => break r.map_err(io::Error::other)?,
            Ok(()) = synced_rx.changed(), if announcer.is_none() => {
                if let Some(id) = synced_rx.borrow().clone() {
                    announcer = Some(crate::cluster::BackupAnnouncer::start(cfg.cluster.clone(), id));
                }
            }
            Some(p) = cluster_packets.recv() => {
                if let Packet::BackupReplicationStartFailedMessage(m) = p {
                    tracing::warn!(
                        "AMQ222164: Error when trying to start replication {}",
                        match m.problem {
                            backup_registration_problem::ALREADY_REPLICATING => "ALREADY_REPLICATING",
                            backup_registration_problem::AUTHENTICATION => "AUTHENTICATION",
                            _ => "EXCEPTION",
                        }
                    );
                    conn.close().await;
                }
            }
        }
    };
    if let Some(a) = announcer.take() {
        a.close().await;
    }
    let up_to_date = endpoint.up_to_date;
    let refused = endpoint.refused.take();
    let node_id = endpoint.node_id.clone().unwrap_or(primary_node);
    let topology = conn.topology();
    conn.close().await;
    let c = cfg.clone();
    tokio::task::spawn_blocking(move || endpoint.close(&c))
        .await
        .map_err(io::Error::other)?;
    if let Some(e) = refused {
        // `WrongNodeId`, `WrongActivationSequence`: the broker stops.
        return Err(io::Error::other(e));
    }
    if !up_to_date {
        // It cannot take over with half a copy: it starts again.
        if cfg.pluggable.is_some() && !matches!(end, End::Stopped) {
            tracing::info!(
                "Replication failure while initial sync not yet completed: restart as backup"
            );
        }
        return Ok(Signal::Retry);
    }
    if cfg.pluggable.is_some() {
        // The lock manager decides, not a vote.
        return Ok(match end {
            End::Stopped => Signal::Retry,
            _ => Signal::FailOver(node_id),
        });
    }
    match end {
        End::PrimaryStopping(PrimaryStopping::FailOver) => Ok(Signal::FailOver(node_id)),
        End::Stopped => Ok(Signal::Retry),
        End::PrimaryStopping(PrimaryStopping::StopCalled) | End::Disconnected => {
            if matches!(end, End::Disconnected) && !primary_is_down(cfg, &node_id, topology).await {
                tracing::info!("AMQ221070: Restarting as backup based on quorum vote results.");
                return Ok(Signal::Retry);
            }
            if matches!(end, End::Disconnected) {
                tracing::info!("AMQ221071: Failing over based on quorum vote results.");
            }
            Ok(Signal::FailOver(node_id))
        }
    }
}

/// `SharedNothingBackupQuorum.isPrimaryDown`: up to `vote-retries` votes
/// of the other primaries on whether the primary is still there.
async fn primary_is_down(
    cfg: &BackupConfig,
    node_id: &str,
    topology: Vec<(String, Option<Connector>, Option<Connector>)>,
) -> bool {
    let size = if cfg.policy.quorum_size == -1 {
        topology.len() as i32
    } else {
        cfg.policy.quorum_size
    };
    let voters: Vec<_> = topology
        .iter()
        .filter(|(id, _, _)| id != node_id)
        .filter_map(|(_, p, _)| p.clone())
        .map(|p| connector_transport(&p))
        .collect();
    for attempt in 0..cfg.policy.vote_retries.max(1) {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_millis(
                cfg.policy.vote_retry_wait.max(1) as u64
            ))
            .await;
        }
        let down = super::quorum::vote(
            &cfg.cluster,
            voters.clone(),
            size,
            super::quorum::Question {
                target: node_id.to_owned(),
                stay_active: None,
            },
            Duration::from_secs(cfg.policy.quorum_vote_wait.max(1) as u64),
        )
        .await;
        if down {
            return true;
        }
    }
    false
}

/// A connector to vote through.
fn connector_transport(c: &Connector) -> saranga_core_protocol::transport::TransportConfiguration {
    use saranga_core_protocol::transport::{ParamValue, TransportConfiguration};
    TransportConfiguration {
        name: c.to_string().as_str().into(),
        factory_class_name: crate::cluster::NETTY_CONNECTOR_FACTORY.into(),
        params: vec![
            ("port".into(), ParamValue::Int(c.port as i32)),
            ("host".into(), ParamValue::String(c.host.as_str().into())),
        ],
    }
}

/// How the replication ended.
enum End {
    Disconnected,
    /// The broker stopped while it was a backup.
    Stopped,
    PrimaryStopping(PrimaryStopping),
}

/// `ReplicationEndpoint`: applies what the primary sends to this backup's
/// own directories, answering each packet in order.
struct Endpoint {
    cfg: BackupConfig,
    /// The journals while they are copied (`FileWrapperJournal`).
    syncing: [Option<SyncJournal>; 2],
    /// The journals once the copy is complete.
    journals: [Option<Journal>; 2],
    up_to_date: bool,
    node_id: Option<String>,
    /// Why the primary's copy cannot be taken (`ReplicationObserver`'s
    /// `WrongNodeId`, `WrongActivationSequence`).
    refused: Option<String>,
    pages: Pages,
    large: Large,
    /// Told the primary's node ID once the copy is complete.
    synced: tokio::sync::watch::Sender<Option<String>>,
}

impl Endpoint {
    fn new(cfg: BackupConfig, synced: tokio::sync::watch::Sender<Option<String>>) -> Self {
        Self {
            synced,
            pages: Pages::new(cfg.paging.clone()),
            large: Large::new(cfg.large_messages.clone()),
            cfg,
            syncing: [None, None],
            journals: [None, None],
            up_to_date: false,
            node_id: None,
            refused: None,
        }
    }

    fn journal_config(&self, journal: usize) -> JournalConfig {
        if journal == 0 {
            self.cfg.bindings.clone()
        } else {
            self.cfg.messages.clone()
        }
    }

    /// Handles packets until the connection goes, or the primary says it
    /// stops for good.
    fn run(&mut self, mut incoming: Incoming, conn: &Connection) -> End {
        let mut stop_called: Option<std::time::Instant> = None;
        loop {
            // STOP_CALLED: the backup fails over once the primary has
            // stopped, or after a minute at most.
            let left = stop_called.map(|at| WAIT_AFTER_STOP_CALLED.saturating_sub(at.elapsed()));
            let packet = match incoming.recv(left) {
                Recv::Packet(p) => *p,
                Recv::Stopped => return End::Stopped,
                Recv::Closed | Recv::TimedOut if stop_called.is_some() => {
                    return End::PrimaryStopping(PrimaryStopping::StopCalled);
                }
                Recv::Closed | Recv::TimedOut => return End::Disconnected,
            };
            let stopping = match &packet {
                Packet::ReplicationPrimaryIsStoppingMessage(m) => Some(m.primary_stopping),
                _ => None,
            };
            let response = self.handle(packet);
            if let Some(r) = response {
                let _ = conn.send_packet(CHANNEL_REPLICATION, r);
            }
            if self.refused.is_some() {
                return End::Disconnected;
            }
            match stopping {
                Some(PrimaryStopping::FailOver) => {
                    return End::PrimaryStopping(PrimaryStopping::FailOver);
                }
                Some(PrimaryStopping::StopCalled) if self.up_to_date => {
                    stop_called.get_or_insert_with(std::time::Instant::now);
                }
                _ => {}
            }
        }
    }

    /// `handlePacket`: the packet applied, and its response.
    fn handle(&mut self, packet: Packet) -> Option<Packet> {
        let ok = || Some(Packet::from(ReplicationResponseMessage {}));
        let result = match packet {
            Packet::ReplicationStartSyncMessage(m) => {
                return Some(self.start_sync(m).into());
            }
            Packet::ReplicationSyncFileMessage(m) => self.sync_file(m),
            Packet::ReplicationAddMessage(m) => {
                let user_type = m.journal_record_type as u8;
                let (id, data) = (m.id, m.record_data);
                self.journal_op(
                    m.journal_id,
                    match m.operation {
                        AddOperationType::Add => Record::Add {
                            id,
                            user_type,
                            data,
                        },
                        AddOperationType::Update => Record::Update {
                            id,
                            user_type,
                            data,
                        },
                        AddOperationType::Event => Record::Event {
                            id,
                            user_type,
                            data,
                        },
                    },
                )
            }
            Packet::ReplicationAddTXMessage(m) => {
                let user_type = m.record_type as u8;
                let (tx, id, data) = (m.tx_id, m.id, m.record_data);
                self.journal_op(
                    m.journal_id,
                    match m.operation {
                        AddOperationType::Update => Record::UpdateTx {
                            tx,
                            id,
                            user_type,
                            data,
                        },
                        _ => Record::AddTx {
                            tx,
                            id,
                            user_type,
                            data,
                        },
                    },
                )
            }
            Packet::ReplicationDeleteMessage(m) => {
                self.journal_op(m.journal_id, Record::Delete { id: m.id })
            }
            Packet::ReplicationDeleteTXMessage(m) => self.journal_op(
                m.journal_id,
                Record::DeleteTx {
                    tx: m.tx_id,
                    id: m.id,
                    data: m.record_data,
                },
            ),
            Packet::ReplicationPrepareMessage(m) => self.journal_op(
                m.journal_id,
                Record::Prepare {
                    tx: m.tx_id,
                    num_records: 0,
                    data: m.record_data,
                },
            ),
            Packet::ReplicationCommitMessage(m) => self.journal_op(
                m.journal_id,
                if m.rollback {
                    Record::Rollback { tx: m.tx_id }
                } else {
                    Record::Commit {
                        tx: m.tx_id,
                        num_records: 0,
                    }
                },
            ),
            Packet::ReplicationPageWriteMessage(m) => {
                let address = m
                    .address
                    .clone()
                    .flatten()
                    .map(|a| a.to_string_lossy())
                    .or_else(|| m.paged_message_address());
                match address {
                    Some(a) => {
                        let mut body = Writer::new();
                        m.paged_message.write(&mut body);
                        self.pages.write(&a, m.page_number, body.as_slice())
                    }
                    None => Err(io::Error::other("a page write without an address")),
                }
            }
            Packet::ReplicationPageEventMessage(m) => {
                let store = m.store_name.to_string_lossy();
                if m.is_delete {
                    self.pages.delete(&store, m.page_number)
                } else {
                    self.pages.close(&store, m.page_number);
                    Ok(())
                }
            }
            Packet::ReplicationLargeMessageBeginMessage(m) => self.large.begin(m.message_id),
            Packet::ReplicationLargeMessageWriteMessage(m) => {
                self.large.write(m.message_id, &m.body)
            }
            Packet::ReplicationLargeMessageEndMessage(m) => {
                if m.is_delete == Some(true) {
                    self.large.delete(m.message_id)
                } else {
                    self.large.close(m.message_id);
                    Ok(())
                }
            }
            Packet::ReplicationPrimaryIsStoppingMessage(_) => Ok(()),
            Packet::BackupReplicationStartFailedMessage(m) => {
                tracing::warn!(
                    "AMQ222164: Error when trying to start replication {}",
                    m.problem
                );
                Ok(())
            }
            other => {
                tracing::warn!(
                    "AMQ222182: Packet {} can't be processed by the ReplicationEndpoint",
                    other.name()
                );
                Ok(())
            }
        };
        if let Err(e) = result {
            tracing::warn!("AMQ222185: Error handling replication packet: {e}");
        }
        ok()
    }

    /// `handleStartReplicationSynchronization`.
    fn start_sync(&mut self, m: ReplicationStartSyncMessage) -> ReplicationResponseMessageV2 {
        if m.synchronization_is_finished {
            // The version of the data copied, which this backup activates
            // with.
            let sequence = match &m.data {
                Some(d) if d.ids.len() == 1 => d.ids[0],
                _ => 0,
            };
            if let Err(e) = self.finish_sync(m.node_id.to_string_lossy(), sequence) {
                tracing::warn!("AMQ222185: Error handling replication packet: {e}");
                return ReplicationResponseMessageV2 {
                    synchronization_is_finished_acknowledgement: false,
                };
            }
            return ReplicationResponseMessageV2 {
                synchronization_is_finished_acknowledgement: true,
            };
        }
        let Some(data) = m.data else {
            return ReplicationResponseMessageV2::default();
        };
        let result = match data.data_type {
            SyncDataType::LargeMessages => self.large.start_sync(&data.ids),
            SyncDataType::JournalBindings | SyncDataType::JournalMessages => {
                let j = if data.data_type == SyncDataType::JournalBindings {
                    0
                } else {
                    1
                };
                if self.cfg.fail_back && !m.allows_auto_fail_back {
                    tracing::error!("AMQ224056: Primary server will not fail-back automatically");
                }
                let node_id = m.node_id.to_string_lossy();
                if !node_id.is_empty() {
                    if let Some(e) = self.wrong_node_id(&node_id) {
                        self.refused = Some(e);
                        return ReplicationResponseMessageV2::default();
                    }
                    self.node_id = Some(node_id);
                }
                SyncJournal::start(self.journal_config(j), &data.ids)
                    .map(|s| self.syncing[j] = Some(s))
                    .map_err(|e| io::Error::other(e.to_string()))
            }
            SyncDataType::ActivationSequence => Ok(()),
        };
        if let Err(e) = result {
            tracing::warn!("AMQ222185: Error handling replication packet: {e}");
        }
        ReplicationResponseMessageV2::default()
    }

    /// `finishSynchronization`: the copies are complete; the journals are
    /// loaded to go on with what the primary appends.
    /// `ReplicationObserver.validateNodeId`: the primary first seen, or
    /// the one failed back to.
    fn wrong_node_id(&self, node_id: &str) -> Option<String> {
        let p = self.cfg.pluggable.as_ref()?;
        let ok = match (&self.node_id, &p.expected_node_id) {
            (Some(existing), _) => existing == node_id,
            (None, Some(expected)) => expected == node_id,
            (None, None) => true,
        };
        (!ok).then(|| {
            tracing::error!(
                "Stopping broker because of wrong node ID communication from primary: maybe a misbehaving primary?"
            );
            format!("wrong node ID {node_id} from the primary")
        })
    }

    fn finish_sync(&mut self, node_id: String, sequence: i64) -> io::Result<()> {
        if self.cfg.pluggable.is_some() {
            // `onRemoteBackupUpToDate`.
            if let Some(e) = self.wrong_node_id(&node_id) {
                self.refused = Some(e);
                return Ok(());
            }
            if sequence <= 0 {
                tracing::error!("Illegal activation sequence {sequence} from NodeID = {node_id}");
                tracing::error!(
                    "Stopping broker because of wrong activation sequence communication from primary: maybe a misbehaving primary?"
                );
                self.refused = Some(format!("illegal activation sequence {sequence}"));
                return Ok(());
            }
        }
        for j in 0..2 {
            if let Some(s) = self.syncing[j].take() {
                s.finish().map_err(|e| io::Error::other(e.to_string()))?;
            }
            let journal = Journal::open(self.journal_config(j))
                .map_err(|e| io::Error::other(e.to_string()))?;
            journal
                .load_sync_only()
                .map_err(|e| io::Error::other(e.to_string()))?;
            self.journals[j] = Some(journal);
        }
        self.large.finish_sync()?;
        if let Some(p) = &self.cfg.pluggable {
            // `nodeManager.stopBackup`: the node ID and its activation
            // sequence persisted.
            crate::node::write_node_id(&p.lock_dir, &node_id)?;
            saranga_ha::activation::write_sequence(&p.lock_dir, sequence)?;
        }
        self.node_id = Some(node_id.clone());
        self.up_to_date = true;
        self.synced.send_replace(Some(node_id.clone()));
        tracing::info!(
            "AMQ221024: Backup server Saranga::name={} is synchronized with primary server, nodeID={node_id}.",
            self.cfg.name
        );
        Ok(())
    }

    fn sync_file(&mut self, m: ReplicationSyncFileMessage) -> io::Result<()> {
        let Some(chunk) = m.chunk else {
            return Ok(());
        };
        let data = (chunk.data_size > 0).then_some(&chunk.data[..]);
        match chunk.file_type {
            SyncFileType::Journal(content) => {
                let j = match content {
                    JournalContent::Bindings => 0,
                    JournalContent::Messages => 1,
                };
                let s = self.syncing[j]
                    .as_mut()
                    .ok_or_else(|| io::Error::other("a journal file before its synchronization"))?;
                match data {
                    Some(d) => s.write_file(m.file_id, d),
                    None => s.close_file(m.file_id),
                }
                .map_err(|e| io::Error::other(e.to_string()))
            }
            SyncFileType::Page(store) => {
                let store = store.to_string_lossy();
                match data {
                    Some(d) => self.pages.write_raw(&store, m.file_id, d),
                    None => {
                        self.pages.close(&store, m.file_id);
                        Ok(())
                    }
                }
            }
            SyncFileType::LargeMessage => match data {
                Some(d) => self.large.write_synced(m.file_id, d),
                None => {
                    self.large.close(m.file_id);
                    Ok(())
                }
            },
        }
    }

    fn journal_op(&mut self, journal_id: i8, record: Record) -> io::Result<()> {
        let j = match journal_id {
            0 => 0,
            1 => 1,
            other => return Err(io::Error::other(format!("no journal {other}"))),
        };
        if let Some(s) = &mut self.syncing[j] {
            return s
                .append(record)
                .map_err(|e| io::Error::other(e.to_string()));
        }
        let Some(journal) = &self.journals[j] else {
            return Err(io::Error::other(
                "a journal record before the synchronization",
            ));
        };
        let r = match record {
            Record::Add {
                id,
                user_type,
                data,
            } => journal.append_add(id, user_type, data, false, None),
            Record::Event {
                id,
                user_type,
                data,
            } => journal.append_add_event(id, user_type, data, false, None),
            Record::Update {
                id,
                user_type,
                data,
            } => journal
                .append_update(id, user_type, data, false, false, None)
                .map(|_| ()),
            Record::Delete { id } => journal.append_delete(id, false, None).map(|_| ()),
            Record::AddTx {
                tx,
                id,
                user_type,
                data,
            } => journal.append_add_tx(tx, id, user_type, data),
            Record::UpdateTx {
                tx,
                id,
                user_type,
                data,
            } => journal.append_update_tx(tx, id, user_type, data),
            Record::DeleteTx { tx, id, data } => journal.append_delete_tx(tx, id, data),
            Record::Prepare { tx, data, .. } => journal.append_prepare(tx, data, false, None),
            Record::Commit { tx, .. } => journal.append_commit(tx, false, None),
            Record::Rollback { tx } => journal.append_rollback(tx, false, None),
        };
        r.map_err(|e| io::Error::other(e.to_string()))
    }

    /// `ActiveMQServerImpl.stopComponent(replicationEndpoint)`: everything
    /// written out and closed.
    fn close(mut self, _cfg: &BackupConfig) {
        for s in self.syncing.iter_mut() {
            if let Some(s) = s.take() {
                let _ = s.finish();
            }
        }
        for j in self.journals.iter_mut() {
            if let Some(j) = j.take() {
                let _ = j.stop();
            }
        }
        self.pages.close_all();
        self.large.close_all();
    }
}

enum Recv {
    Packet(Box<Packet>),
    Closed,
    TimedOut,
    Stopped,
}

/// The replication channel's packets, as the endpoint's thread waits for
/// them.
struct Incoming {
    packets: tokio::sync::mpsc::UnboundedReceiver<Packet>,
    /// Closed when the broker stops.
    stopped: tokio::sync::watch::Receiver<()>,
    handle: tokio::runtime::Handle,
}

impl Incoming {
    /// The next packet, within `timeout` if there is one.
    fn recv(&mut self, timeout: Option<Duration>) -> Recv {
        let Self {
            packets,
            stopped,
            handle,
        } = self;
        handle.block_on(async {
            let limit = async {
                match timeout {
                    Some(t) => tokio::time::sleep(t).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                p = packets.recv() => match p {
                    Some(p) => Recv::Packet(Box::new(p)),
                    None => Recv::Closed,
                },
                _ = stopped.changed() => Recv::Stopped,
                _ = limit => Recv::TimedOut,
            }
        })
    }
}

trait PagedAddress {
    fn paged_message_address(&self) -> Option<String>;
}

impl PagedAddress for saranga_core_protocol::packets::replication::ReplicationPageWriteMessage {
    /// Before 2.24 the address is the message's own.
    fn paged_message_address(&self) -> Option<String> {
        use saranga_core_protocol::packets::replication::{PagedMessageBody, PersistedMessage};
        match &self.paged_message.message {
            PagedMessageBody::Persisted {
                message:
                    PersistedMessage::Core {
                        address, message, ..
                    },
                ..
            } => address
                .clone()
                .or_else(|| message.address.clone())
                .map(|a| a.to_string_lossy()),
            PagedMessageBody::Persisted {
                message: PersistedMessage::Amqp(r),
                ..
            }
            | PagedMessageBody::Persisted {
                message: PersistedMessage::AmqpV2 { record: r, .. },
                ..
            }
            | PagedMessageBody::Persisted {
                message: PersistedMessage::AmqpV3 { record: r, .. },
                ..
            } => r.address.clone().map(|a| a.to_string_lossy()),
            _ => None,
        }
    }
}

/// The backup's page files, by address: each address gets a directory of
/// its own, as a paging store makes one.
struct Pages {
    dir: PathBuf,
    folders: HashMap<String, PathBuf>,
    open: HashMap<(String, i64), File>,
}

impl Pages {
    fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            folders: HashMap::new(),
            open: HashMap::new(),
        }
    }

    fn folder(&mut self, address: &str) -> io::Result<PathBuf> {
        if let Some(f) = self.folders.get(address) {
            return Ok(f.clone());
        }
        let folder = self.dir.join(uuid::Uuid::new_v4().to_string());
        fs::create_dir_all(&folder)?;
        fs::write(
            folder.join(saranga_paging::ADDRESS_FILE),
            format!("{address}\n"),
        )?;
        self.folders.insert(address.to_owned(), folder.clone());
        Ok(folder)
    }

    fn file(&mut self, address: &str, page: i64) -> io::Result<&mut File> {
        let key = (address.to_owned(), page);
        if !self.open.contains_key(&key) {
            let path = self
                .folder(address)?
                .join(saranga_paging::page_file_name(page));
            let f = OpenOptions::new().create(true).append(true).open(path)?;
            self.open.insert(key.clone(), f);
        }
        Ok(self.open.get_mut(&key).expect("opened"))
    }

    /// `Page.writeDirect`: a record around the paged message.
    fn write(&mut self, address: &str, page: i64, body: &[u8]) -> io::Result<()> {
        let mut record = Vec::with_capacity(body.len() + saranga_paging::SIZE_RECORD);
        record.push(b'{');
        record.extend_from_slice(&(body.len() as i32).to_be_bytes());
        record.extend_from_slice(body);
        record.push(b'}');
        self.file(address, page)?.write_all(&record)
    }

    /// A page file's bytes, as copied in the synchronization.
    fn write_raw(&mut self, address: &str, page: i64, data: &[u8]) -> io::Result<()> {
        self.file(address, page)?.write_all(data)
    }

    fn close(&mut self, address: &str, page: i64) {
        if let Some(f) = self.open.remove(&(address.to_owned(), page)) {
            let _ = f.sync_all();
        }
    }

    fn delete(&mut self, address: &str, page: i64) -> io::Result<()> {
        self.open.remove(&(address.to_owned(), page));
        let Some(folder) = self.folders.get(address) else {
            return Ok(());
        };
        match fs::remove_file(folder.join(saranga_paging::page_file_name(page))) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    fn close_all(&mut self) {
        for (_, f) in self.open.drain() {
            let _ = f.sync_all();
        }
    }
}

/// The backup's large-message files. A body being copied keeps what the
/// primary appends meanwhile apart until its copy is complete
/// (`LargeServerMessageInSync`).
struct Large {
    dir: PathBuf,
    open: HashMap<i64, File>,
    syncing: HashSet<i64>,
    appended: HashMap<i64, Vec<u8>>,
}

impl Large {
    fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            open: HashMap::new(),
            syncing: HashSet::new(),
            appended: HashMap::new(),
        }
    }

    fn path(&self, id: i64) -> PathBuf {
        self.dir.join(format!("{id}.msg"))
    }

    fn create(&mut self, id: i64) -> io::Result<()> {
        fs::create_dir_all(&self.dir)?;
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.path(id))?;
        self.open.insert(id, f);
        Ok(())
    }

    fn start_sync(&mut self, ids: &[i64]) -> io::Result<()> {
        for id in ids {
            self.create(*id)?;
            self.syncing.insert(*id);
        }
        Ok(())
    }

    fn begin(&mut self, id: i64) -> io::Result<()> {
        self.create(id)
    }

    fn file(&mut self, id: i64) -> io::Result<&mut File> {
        if !self.open.contains_key(&id) {
            fs::create_dir_all(&self.dir)?;
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.path(id))?;
            self.open.insert(id, f);
        }
        Ok(self.open.get_mut(&id).expect("opened"))
    }

    fn write(&mut self, id: i64, data: &[u8]) -> io::Result<()> {
        if self.syncing.contains(&id) {
            self.appended.entry(id).or_default().extend_from_slice(data);
            return Ok(());
        }
        self.file(id)?.write_all(data)
    }

    fn write_synced(&mut self, id: i64, data: &[u8]) -> io::Result<()> {
        self.file(id)?.write_all(data)
    }

    fn close(&mut self, id: i64) {
        if self.syncing.contains(&id) {
            return;
        }
        if let Some(f) = self.open.remove(&id) {
            let _ = f.sync_all();
        }
    }

    fn delete(&mut self, id: i64) -> io::Result<()> {
        self.open.remove(&id);
        self.syncing.remove(&id);
        self.appended.remove(&id);
        match fs::remove_file(self.path(id)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    /// `joinSyncedData`.
    fn finish_sync(&mut self) -> io::Result<()> {
        let ids: Vec<i64> = self.syncing.drain().collect();
        for id in ids {
            if let Some(data) = self.appended.remove(&id) {
                self.file(id)?.write_all(&data)?;
            }
        }
        Ok(())
    }

    fn close_all(&mut self) {
        for (_, f) in self.open.drain() {
            let _ = f.sync_all();
        }
    }
}

/// `moveServerData`: what is in the bindings, journal, paging and
/// large-messages directories goes to an `oldreplica.N` directory beside
/// it, at most `max-saved-replicated-journals-size` of them kept (all with
/// -1, none with 0).
fn move_server_data(cfg: &BackupConfig) -> io::Result<()> {
    let max = cfg.policy.max_saved_replicated_journals_size;
    // A primary failing back keeps its node ID (`preserveLockFiles`).
    let keep = cfg
        .pluggable
        .as_ref()
        .filter(|p| p.expected_node_id.is_some())
        .map(|p| p.lock_dir.clone());
    for dir in [
        &cfg.bindings.directory,
        &cfg.messages.directory,
        &cfg.paging,
        &cfg.large_messages,
    ] {
        let preserve: &[&str] = if keep.as_deref() == Some(dir.as_path()) {
            &["server.lock", "serverlock"]
        } else {
            &[]
        };
        move_data(dir, max, preserve)?;
    }
    Ok(())
}

const PREFIX: &str = "oldreplica.";

fn old_folder_id(name: &str) -> Option<i32> {
    name.strip_prefix(PREFIX)?.parse().ok()
}

/// `FileMoveManager.doMove`.
fn move_data(dir: &Path, max: i32, preserve: &[&str]) -> io::Result<()> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(());
    };
    let mut files = Vec::new();
    let mut folders = Vec::new();
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        match old_folder_id(&name) {
            Some(id) => folders.push(id),
            None if preserve.iter().any(|p| name.starts_with(p)) => {}
            None => files.push(name),
        }
    }
    if files.is_empty() {
        return Ok(());
    }
    folders.sort();
    // checkOldFolders: room for the one about to be made.
    let check_old = |creating: i32, folders: &mut Vec<i32>| {
        if max < 0 {
            return;
        }
        let excess = folders.len() as i32 + creating - max;
        for _ in 0..excess.max(0) {
            let id = folders.remove(0);
            let path = dir.join(format!("{PREFIX}{id}"));
            tracing::info!(
                "AMQ221055: There were too many old replicated folders upon startup, removing {}",
                path.display()
            );
            let _ = fs::remove_dir_all(&path);
        }
    };
    if max == 0 {
        check_old(0, &mut folders);
        tracing::info!(
            "AMQ221059: Deleting old data directory {} as the max folders is set to 0",
            dir.display()
        );
        for f in files {
            let path = dir.join(f);
            let _ = if path.is_dir() {
                fs::remove_dir_all(&path)
            } else {
                fs::remove_file(&path)
            };
        }
        return Ok(());
    }
    let next = folders.last().copied().unwrap_or(0) + 1;
    check_old(1, &mut folders);
    let to = dir.join(format!("{PREFIX}{next}"));
    fs::create_dir_all(&to)?;
    tracing::info!(
        "AMQ222162: Moving data directory {} to {}",
        dir.display(),
        to.display()
    );
    for f in files {
        fs::rename(dir.join(&f), to.join(&f))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_data_moves_aside_keeping_at_most_the_maximum() {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path();
        for round in 1..=3 {
            fs::write(dir.join("a.amq"), format!("{round}")).unwrap();
            move_data(dir, 2, &[]).unwrap();
        }
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, ["oldreplica.2", "oldreplica.3"]);
        assert_eq!(
            fs::read_to_string(dir.join("oldreplica.3/a.amq")).unwrap(),
            "3"
        );
        // With none to keep, the old copies go too.
        fs::write(dir.join("b.amq"), "x").unwrap();
        move_data(dir, 0, &[]).unwrap();
        assert_eq!(fs::read_dir(dir).unwrap().count(), 0);
    }
}
