//! The address and queue reaper (`PostOfficeImpl.reapAddresses`).

use std::sync::Arc;

use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::{Broker, BrokerConfig, RoutingType};

async fn broker() -> Arc<Broker> {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.address_queue_scan_period = -1;
    Broker::start(config).await.unwrap()
}

fn auto_created(name: &str) -> QueueSpec {
    let mut spec = QueueSpec::new(name, name);
    spec.routing_type = Some(RoutingType::Anycast);
    spec.auto_created = true;
    spec.auto_create_address = Some(true);
    spec
}

#[tokio::test]
async fn unused_queues_stay() {
    let broker = broker().await;
    let po = broker.post_office();
    po.create_queue(auto_created("fresh"), false).unwrap();
    // Nothing ever consumed from it: not removed, however many passes.
    for _ in 0..3 {
        po.reap(false);
    }
    assert!(po.queue("fresh").is_some());
    assert!(po.address_info("fresh").is_some());
}

#[tokio::test]
async fn startup_pass_removes_empty_auto_created_queues() {
    let broker = broker().await;
    let po = broker.post_office();
    po.create_queue(auto_created("empty"), false).unwrap();
    let mut configured = QueueSpec::new("kept", "kept");
    configured.routing_type = Some(RoutingType::Anycast);
    configured.auto_create_address = Some(true);
    po.create_queue(configured, false).unwrap();
    po.reap(true);
    assert!(po.queue("empty").is_none(), "auto-created and empty");
    assert!(
        po.address_info("empty").is_none(),
        "its auto-created address too"
    );
    assert!(po.queue("kept").is_some(), "not auto-created");
}
