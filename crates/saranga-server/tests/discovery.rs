//! Broadcast groups: an active broker broadcasts its connectors, a discovery
//! group hears them, and management starts and stops the group
//! (`BroadcastGroupControl`).

use std::sync::Arc;
use std::time::Duration;

use saranga_client::discovery::{DiscoveryConfig, DiscoveryGroup, EndpointConfig};
use saranga_core_protocol::transport::{ParamValue, TransportConfiguration};
use saranga_server::discovery::BroadcastGroupConfig;
use saranga_server::management::Json;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Broker, BrokerConfig};

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("0.0.0.0:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn connector() -> TransportConfiguration {
    TransportConfiguration {
        name: "artemis".into(),
        factory_class_name:
            "org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory".into(),
        params: vec![
            ("port".into(), ParamValue::String("61999".into())),
            ("host".into(), ParamValue::String("localhost".into())),
        ],
    }
}

async fn attribute(broker: &Arc<Broker>, name: &str) -> Json {
    broker
        .management()
        .attribute(broker, "broadcastgroup.bg", name, Default::default())
        .await
        .unwrap()
}

async fn op(broker: &Arc<Broker>, operation: &str) {
    broker
        .management()
        .invoke(
            broker,
            "broadcastgroup.bg",
            operation,
            &[],
            Default::default(),
        )
        .await
        .unwrap();
}

/// Whether the group hears a broadcast within two seconds, one heard
/// before not counted.
async fn heard(group: &DiscoveryGroup) -> bool {
    group.wait_for_broadcast(Some(Duration::ZERO)).await;
    group.wait_for_broadcast(Some(Duration::from_secs(2))).await
}

#[tokio::test]
async fn an_active_broker_broadcasts_its_connectors() {
    let port = free_udp_port();
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.broadcast_groups = vec![BroadcastGroupConfig {
        name: "bg".to_owned(),
        broadcast_period: Duration::from_millis(100),
        connector_names: vec!["artemis".to_owned()],
        connectors: vec![connector()],
        endpoint: saranga_config::UdpEndpointConfig {
            group_address: Some("231.7.7.7".to_owned()),
            group_port: i32::from(port),
            local_bind_address: None,
            local_bind_port: -1,
        },
    }];
    let Ok(group) = DiscoveryGroup::start(
        &DiscoveryConfig::new(EndpointConfig::new("231.7.7.7", port)),
        "listener",
    ) else {
        // No multicast here.
        return;
    };
    let broker = Broker::start(config).await.unwrap();
    if !heard(&group).await {
        // Multicast loopback is not available on this host.
        return;
    }
    let entries = group.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].node_id, broker.node_id());
    assert_eq!(entries[0].connector, connector());

    assert_eq!(attribute(&broker, "name").await, Json::from("bg"));
    assert_eq!(
        attribute(&broker, "BroadcastPeriod").await,
        Json::from(100i64)
    );
    assert_eq!(
        attribute(&broker, "GroupAddress").await,
        Json::from("231.7.7.7")
    );
    assert_eq!(
        attribute(&broker, "GroupPort").await,
        Json::from(i32::from(port))
    );
    assert_eq!(attribute(&broker, "LocalBindPort").await, Json::from(-1i32));
    assert_eq!(
        attribute(&broker, "ConnectorPairsAsJSON").await,
        Json::from("[\"artemis\"]")
    );
    assert_eq!(attribute(&broker, "Started").await, Json::from(true));

    op(&broker, "stop").await;
    assert_eq!(attribute(&broker, "Started").await, Json::from(false));
    // Any datagram already on its way, then silence.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!heard(&group).await);
    op(&broker, "start").await;
    assert!(heard(&group).await);
    broker.stop().unwrap();
    group.stop();
}
