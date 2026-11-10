//! `is_node_id_used` must check every configured connector, not stop at the
//! first reachable peer: an unrelated broker reached first must not hide an
//! active node reachable through a later connector (which would let a
//! restarting primary activate alongside its promoted backup — split brain).

use std::sync::Arc;

use saranga_server::cluster::ClusterConfig;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Acceptor, Broker, BrokerConfig};
use tokio::net::TcpListener;

async fn cluster_node() -> (
    Arc<Broker>,
    ClusterConfig,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let file = saranga_config::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <security-enabled>false</security-enabled>
             <persistence-enabled>false</persistence-enabled>
             <connectors><connector name="self">tcp://127.0.0.1:{port}</connector></connectors>
             <cluster-connections><cluster-connection name="c">
               <connector-ref>self</connector-ref>
             </cluster-connection></cluster-connections>
           </core></configuration>"#
    ))
    .unwrap();
    let cc = ClusterConfig::all_from_file(&file).remove(0);
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.clusters.push(cc.clone());
    let b = Broker::start(config).await.unwrap();
    let t = tokio::spawn(b.clone().serve_acceptor(l, Acceptor::default()));
    (b, cc, t)
}

#[tokio::test]
async fn active_node_detection_checks_every_connector_regardless_of_order() {
    let (first, a, t1) = cluster_node().await;
    let (second, b, t2) = cluster_node().await;

    let mut query = a.clone();
    // The active node (`second`) is reachable through its own connector; the
    // unrelated broker (`first`) does not know it.
    query.static_connectors = vec![b.connector.clone(), a.connector.clone()];
    let target_first =
        saranga_server::replication::backup::is_node_id_used(&query, second.node_id()).await;

    query.static_connectors.reverse();
    let unrelated_first =
        saranga_server::replication::backup::is_node_id_used(&query, second.node_id()).await;

    t1.abort();
    t2.abort();
    first.stop().unwrap();
    second.stop().unwrap();

    assert!(
        target_first,
        "control: the active node is reachable through its connector"
    );
    assert!(
        unrelated_first,
        "an unrelated first peer must not hide the active node on a later connector"
    );
}
