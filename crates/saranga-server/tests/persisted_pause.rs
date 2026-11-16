//! A persisted address pause outlives a change of the address's routing
//! types, which rewrites the address under a new ID: the pause record
//! follows it, where recovery used to drop it as naming no address.

use std::path::Path;
use std::sync::Arc;

use saranga_server::postoffice::{RoutingTypes, SelectorFilters};
use saranga_server::{Broker, BrokerConfig, RoutingType};

async fn start(dir: &Path) -> Arc<Broker> {
    let file = saranga_config::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>65536</journal-file-size>
           </core></configuration>"#,
    )
    .unwrap();
    Broker::start(BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn a_paused_address_stays_paused_after_its_routing_types_change() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path()).await;
    let po = broker.post_office();
    let first = po
        .add_or_update_address("a", RoutingTypes::of(RoutingType::Anycast), false, false)
        .unwrap();
    assert!(po.pause_address("a", true).unwrap());
    let mut both = RoutingTypes::of(RoutingType::Anycast);
    both.add(RoutingType::Multicast);
    let updated = po.add_or_update_address("a", both, false, false).unwrap();
    assert_ne!(first.id, updated.id, "rewritten under a new ID");
    broker.stop().unwrap();
    drop(broker);

    let broker = start(dir.path()).await;
    let status = broker.post_office().address_status("a").unwrap();
    assert!(status.info.routing_types.contains(RoutingType::Multicast));
    assert!(status.paused && status.persisted_pause, "still paused");
    // Resumed, it stays resumed: the old record is gone too.
    assert!(broker.post_office().resume_address("a").unwrap());
    broker.stop().unwrap();
    drop(broker);
    let broker = start(dir.path()).await;
    assert!(!broker.post_office().address_status("a").unwrap().paused);
    broker.stop().unwrap();
}
