//! `saranga activation`: the local and coordinated activation sequences of
//! a broker replicating with a lock manager (`ActivationSequenceList`,
//! `ActivationSequenceSet`), to repair them by hand after a disaster.

use std::time::Duration;

use clap::{Args, Subcommand};
use saranga_ha::activation::{self, NULL_SEQUENCE};
use saranga_ha::lockmanager::LockManager;

use crate::data::dirs::{DataDirArgs, DbArgs};

/// `MANAGER_START_TIMEOUT_SECONDS`.
const MANAGER_START_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Subcommand)]
pub enum ActivationCommand {
    /// List local and/or remote (i.e. coordinated) activation sequences.
    List(ListArgs),
    /// Set local and/or remote (i.e. coordinated) activation sequence.
    Set(SetArgs),
}

#[derive(Args)]
pub struct ListArgs {
    #[command(flatten)]
    dirs: DataDirArgs,
    /// This can be used just with --remote option. If not set, broker
    /// NodeID is used instead.
    #[arg(long = "node-id")]
    node_id: Option<String>,
    /// List just remote (i.e. coordinated) activation sequence.
    #[arg(long)]
    remote: bool,
    /// List just local activation sequence.
    #[arg(long)]
    local: bool,
}

#[derive(Args)]
pub struct SetArgs {
    #[command(flatten)]
    dirs: DataDirArgs,
    /// Target sequence for this UUID overwriting the NodeID of this broker
    /// too. If not set, broker NodeID is used instead.
    #[arg(long = "node-id")]
    node_id: Option<String>,
    /// Set just remote (i.e. coordinated) activation sequence.
    #[arg(long)]
    remote: bool,
    /// Set just local activation sequence.
    #[arg(long)]
    local: bool,
    /// The new activation sequence.
    #[arg(long, required = true, allow_negative_numbers = true)]
    to: i64,
}

/// The replication policy's manager and coordination-id, and where
/// `server.lock` is.
struct Policy {
    manager: saranga_config::LockManagerConfig,
    coordination_id: Option<String>,
    primary: bool,
    lock_dir: std::path::PathBuf,
}

fn policy(dirs: &DataDirArgs) -> anyhow::Result<Policy> {
    let d = dirs.unchecked()?;
    let config = d.config.ok_or_else(|| {
        anyhow::anyhow!("This command support just <primary> or <backup> replication configuration")
    })?;
    let (r, primary) = match &config.ha_policy {
        saranga_config::HaPolicy::ReplicationPrimary(r) => (r, true),
        saranga_config::HaPolicy::ReplicationBackup(r) => (r, false),
        _ => anyhow::bail!(
            "This command support just <primary> or <backup> replication configuration"
        ),
    };
    let manager = r
        .manager
        .clone()
        .ok_or_else(|| anyhow::anyhow!("java.lang.NullPointerException"))?;
    // `getNodeManagerLockLocation`.
    let lock_dir = match (&config.node_manager_lock_directory, &d.instance) {
        (Some(l), Some(i)) => i.join(l),
        (Some(l), None) => l.into(),
        (None, _) => d.journal.clone(),
    };
    Ok(Policy {
        manager,
        coordination_id: r.coordination_id.clone().filter(|_| primary),
        primary,
        lock_dir,
    })
}

/// The node ID, a coordination-id applied first.
async fn local_node(
    p: &Policy,
    coordination_id: Option<&str>,
) -> anyhow::Result<activation::LocalNode> {
    Ok(saranga_server::replication::pluggable::local_node(&p.lock_dir, coordination_id).await?)
}

async fn started(p: &Policy) -> anyhow::Result<std::sync::Arc<LockManager>> {
    let m = LockManager::new(&p.manager.class_name, &p.manager.properties)
        .map_err(|e| anyhow::anyhow!(e))?;
    if !m.start(Some(MANAGER_START_TIMEOUT)).await {
        anyhow::bail!(
            "distributed manager isn't started in {} seconds",
            MANAGER_START_TIMEOUT.as_secs()
        );
    }
    Ok(m)
}

pub async fn run(cmd: ActivationCommand) -> anyhow::Result<()> {
    let _lock = DbArgs::default().lock()?;
    match cmd {
        ActivationCommand::List(a) => list(a).await,
        ActivationCommand::Set(a) => set(a).await,
    }
}

async fn list(a: ListArgs) -> anyhow::Result<()> {
    if a.remote && a.local {
        anyhow::bail!(
            "--local and --remote cannot be both present: to list both sequences just drop both options"
        );
    }
    if a.node_id.is_some() && !a.remote {
        anyhow::bail!("--node-id must be used just with --remote");
    }
    let p = policy(&a.dirs)?;
    let (node_id, local) = match &a.node_id {
        Some(id) => (id.clone(), None),
        None => {
            let n = local_node(&p, p.coordination_id.as_deref()).await?;
            (n.node_id.clone(), Some(n.sequence))
        }
    };
    if !a.remote
        && let Some(sequence) = local
    {
        if sequence == NULL_SEQUENCE {
            println!("No local activation sequence for NodeID={node_id}");
        } else {
            println!("Local activation sequence for NodeID={node_id}: {sequence}");
        }
    }
    if !a.local {
        let m = started(&p).await?;
        let result = async {
            let l = m.distributed_lock(&node_id)?;
            if !l.try_lock().await? {
                anyhow::bail!(
                    "Cannot safely get the coordinated activation sequence for NodeID={node_id}: maybe the primary lock is still held."
                );
            }
            let sequence = m.mutable_long(&node_id)?.get().await?;
            println!("Coordinated activation sequence for NodeID={node_id}: {sequence}");
            l.close().await;
            Ok(())
        }
        .await;
        m.stop().await;
        result?;
    }
    Ok(())
}

async fn set(a: SetArgs) -> anyhow::Result<()> {
    if a.remote && a.local {
        anyhow::bail!(
            "Both --local and --remote cannot be present. To set both sequences just remove --local and --remote."
        );
    }
    if a.to < 0 {
        anyhow::bail!("--to must be >= 0");
    }
    let p = policy(&a.dirs)?;
    let mut coordination_id = a.node_id.clone();
    if p.primary && p.coordination_id.is_some() {
        if a.node_id.is_some() {
            anyhow::bail!(
                "Forcing NodeID with multi-primary is not supported! Try again without --node-id"
            );
        }
        coordination_id = p.coordination_id.clone();
    }
    let mut node = local_node(&p, coordination_id.as_deref()).await?;
    let node_id = node.node_id.clone();
    if !a.remote {
        let before = node.sequence;
        node.write(a.to)?;
        if before == NULL_SEQUENCE {
            println!(
                "Forced local activation sequence for NodeID={node_id} to {}",
                a.to
            );
        } else {
            println!(
                "Forced local activation sequence for NodeID={node_id} from {before} to {}",
                a.to
            );
        }
    }
    if !a.local {
        let m = started(&p).await?;
        let result = async {
            let l = m.distributed_lock(&node_id)?;
            if !l.try_lock().await? {
                anyhow::bail!(
                    "Cannot safely set coordinated activation sequence for NodeID={node_id}: primary lock is still held."
                );
            }
            let long = m.mutable_long(&node_id)?;
            let before = long.get().await?;
            long.set(a.to).await?;
            println!(
                "Forced coordinated activation sequence for NodeID={node_id} from {before} to {}",
                a.to
            );
            l.close().await;
            Ok(())
        }
        .await;
        m.stop().await;
        result?;
    }
    Ok(())
}
