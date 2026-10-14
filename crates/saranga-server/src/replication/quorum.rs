//! Quorum votes between the brokers of a cluster: a backup that lost its
//! primary asks the other primaries whether they still see it before it
//! becomes active, and a primary that lost its backup may ask whether it
//! should go on. Ported from `QuorumManager`, `QuorumVoteServerConnect` and
//! `ServerConnectVoteHandler`.

use std::sync::Arc;
use std::time::Duration;

use saranga_buffer::{JString, SimpleString};
use saranga_client::{Connection, ConnectionOptions};
use saranga_core_protocol::packets::cluster::{QuorumVoteMessage, QuorumVoteReplyMessage};
use saranga_core_protocol::packets::cluster_types::{ServerConnectVote, Vote};
use saranga_core_protocol::transport::{ParamValue, TransportConfiguration};
use saranga_core_protocol::{Packet, types};

use crate::Broker;
use crate::cluster::ClusterConfig;

/// `QuorumVoteServerConnect.PRIMARY_FAILOVER_VOTE`.
pub const PRIMARY_FAILOVER_VOTE: &str = "PrimaryFailoverQuorumVote";
/// `OLD_PRIMARY_FAILOVER_VOTE`, the name before 2.37.
pub const OLD_PRIMARY_FAILOVER_VOTE: &str = "LiveFailoverQuorumVote";

/// `ServerConnectVoteHandler.vote`: whether this broker's topology still
/// has a primary for the node voted on. The reply carries the handler name
/// the request used.
pub(crate) fn answer(broker: &Broker, msg: QuorumVoteMessage) -> Option<QuorumVoteReplyMessage> {
    let name = msg.handler.to_string_lossy();
    if name != PRIMARY_FAILOVER_VOTE && name != OLD_PRIMARY_FAILOVER_VOTE {
        tracing::debug!("no quorum vote handler {name}");
        return None;
    }
    let Vote::ServerConnect(request) = msg.vote else {
        return None;
    };
    let node_id = request.node_id.to_string_lossy();
    let primary = broker.cluster().and_then(|c| {
        c.members()
            .into_iter()
            .find(|m| m.node_id == node_id)
            .and_then(|m| m.primary)
    });
    let reply = match primary {
        Some(tc) => {
            tracing::info!("AMQ221064: Node {node_id} found in cluster topology");
            ServerConnectVote {
                vote: request.vote,
                node_id: request.node_id,
                transport_configuration: Some(transport_to_string(&tc).as_str().into()),
            }
        }
        None => {
            tracing::info!("AMQ221065: Node {node_id} not found in cluster topology");
            ServerConnectVote {
                vote: !request.vote,
                node_id: request.node_id,
                transport_configuration: None,
            }
        }
    };
    Some(QuorumVoteReplyMessage {
        handler: msg.handler,
        vote: Vote::ServerConnect(reply),
    })
}

/// `TransportConfiguration.toString`, which votes compare: the name, the
/// factory with its dots as dashes, then the parameters in the order a
/// Java `HashMap` of them iterates.
pub fn transport_to_string(tc: &TransportConfiguration) -> String {
    let dashes = |s: &str| s.replace('.', "-");
    let mut out = format!(
        "TransportConfiguration(name={}, factory={})",
        tc.name.to_string_lossy(),
        dashes(&tc.factory_class_name.to_string_lossy())
    );
    let keys: Vec<String> = tc.params.iter().map(|(k, _)| k.to_string_lossy()).collect();
    let hashes: Vec<i32> = keys.iter().map(|k| java_string_hash(k)).collect();
    for (n, i) in saranga_buffer::java_hash::hash_map_order(&hashes, None)
        .into_iter()
        .enumerate()
    {
        out.push(if n == 0 { '?' } else { '&' });
        let key = &keys[i];
        let value = if key.to_ascii_lowercase().contains("password") {
            "****".to_owned()
        } else {
            match &tc.params[i].1 {
                ParamValue::Bool(b) => b.to_string(),
                ParamValue::Int(v) => v.to_string(),
                ParamValue::Long(v) => v.to_string(),
                ParamValue::String(s) => s.to_string_lossy(),
            }
        };
        out.push_str(&dashes(key));
        out.push('=');
        out.push_str(&dashes(&value));
    }
    out
}

/// `String.hashCode`.
fn java_string_hash(s: &str) -> i32 {
    s.encode_utf16()
        .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(c as i32))
}

/// A vote's question: whether `target` still has an active primary
/// (`hasPrimary`, asked by a backup), or whether the one asking is still the
/// primary of it (`isStillActive`, its connector compared with what the
/// others see).
pub(crate) struct Question {
    pub target: String,
    pub stay_active: Option<String>,
}

/// `QuorumVoteServerConnect` over `QuorumManager.vote`: each broker in
/// `voters` is asked, and the decision is whether a majority of `size`
/// answered yes. With nothing to ask for (`size` of 1 or 2) it is yes at
/// once.
pub(crate) async fn vote(
    cluster: &ClusterConfig,
    voters: Vec<TransportConfiguration>,
    size: i32,
    question: Question,
    wait: Duration,
) -> bool {
    let size = size.max(0);
    let mut needed = if size <= 2 { size / 2 } else { size / 2 + 1 };
    tracing::info!("AMQ221066: Initiating quorum vote: {PRIMARY_FAILOVER_VOTE}");
    tracing::info!("AMQ221084: Requested {} quorum votes", voters.len());
    if needed == 0 {
        return true;
    }
    let question = Arc::new(question);
    let mut tasks = tokio::task::JoinSet::new();
    for tc in voters {
        let cluster = cluster.clone();
        let q = question.clone();
        tasks.spawn(async move { ask(&cluster, &tc, &q).await });
    }
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let next = tokio::time::timeout_at(deadline, tasks.join_next()).await;
        match next {
            Ok(Some(Ok(Some(reply)))) => {
                if !reply.vote {
                    continue;
                }
                let accepted = match &question.stay_active {
                    None => true,
                    Some(connector) => reply
                        .transport_configuration
                        .as_ref()
                        .is_some_and(|t| &t.to_string_lossy() == connector),
                };
                if accepted {
                    needed -= 1;
                    if needed == 0 {
                        break;
                    }
                }
            }
            Ok(Some(_)) => {}
            // All votes cast, or the wait is over.
            Ok(None) | Err(_) => break,
        }
    }
    tasks.abort_all();
    needed == 0
}

/// `VoteRunnable`: connects to one broker as the cluster user and asks.
async fn ask(
    cluster: &ClusterConfig,
    tc: &TransportConfiguration,
    question: &Question,
) -> Option<ServerConnectVote> {
    let (host, port) = crate::cluster::socket_address(tc)?;
    let options = ConnectionOptions {
        client_failure_check_period: Some(cluster.check_period),
        connection_ttl: Some(cluster.connection_ttl),
        call_timeout: cluster.call_timeout,
        user: Some(cluster.cluster_user.clone()),
        password: Some(cluster.cluster_password.clone()),
        cluster_connection: true,
        ..ConnectionOptions::default()
    };
    let conn = Connection::connect((host.as_str(), port), options)
        .await
        .ok()?;
    let reply = async {
        if !conn
            .cluster_connect(&cluster.cluster_user, &cluster.cluster_password)
            .await
            .ok()?
        {
            return None;
        }
        let request = QuorumVoteMessage {
            handler: SimpleString::from(PRIMARY_FAILOVER_VOTE),
            vote: Vote::ServerConnect(ServerConnectVote {
                vote: question.stay_active.is_some(),
                node_id: JString::from(question.target.as_str()),
                transport_configuration: None,
            }),
        };
        match conn
            .cluster_request(request.into(), types::QUORUM_VOTE_REPLY)
            .await
            .ok()?
        {
            Packet::QuorumVoteReplyMessage(QuorumVoteReplyMessage {
                vote: Vote::ServerConnect(v),
                ..
            }) => Some(v),
            _ => None,
        }
    }
    .await;
    conn.close().await;
    reply
}

/// `QuorumManager.isStillActive`: a primary that lost its backup asks the
/// other brokers whether they still see it as the primary of its node.
pub(crate) async fn is_still_active(
    broker: &Arc<Broker>,
    policy: &saranga_config::Replication,
) -> bool {
    let Some(cluster) = broker.cluster() else {
        return true;
    };
    let members = cluster.members();
    let size = if policy.quorum_size == -1 {
        members.len() as i32
    } else {
        policy.quorum_size
    };
    let voters = members
        .into_iter()
        .filter(|m| m.node_id != broker.node_id())
        .filter_map(|m| m.primary)
        .collect();
    vote(
        cluster.config(),
        voters,
        size,
        Question {
            target: broker.node_id().to_owned(),
            stay_active: Some(transport_to_string(cluster.connector())),
        },
        Duration::from_secs(5),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_configuration_as_java_prints_it() {
        let tc = TransportConfiguration {
            name: "artemis".into(),
            factory_class_name:
                "org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory".into(),
            params: vec![
                ("port".into(), ParamValue::String("61616".into())),
                ("host".into(), ParamValue::String("localhost".into())),
            ],
        };
        assert_eq!(
            transport_to_string(&tc),
            "TransportConfiguration(name=artemis, factory=org-apache-activemq-artemis-core-remoting-impl-netty-NettyConnectorFactory) ?port=61616&host=localhost"
                .replace(") ?", ")?")
        );
    }
}
