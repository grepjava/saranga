//! What deploying the configured addresses writes into their queues'
//! configurations, as Artemis's `deployQueuesFromListQueueConfiguration`
//! writes into the `QueueConfiguration`s `exportConfigAsProperties`
//! exports: a queue it makes gets its journal ID, one that is there after a
//! restart only the defaults.

use std::path::Path;
use std::sync::Arc;

use saranga_config as cfg;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::reload::DeployedQueue;
use saranga_server::{Broker, BrokerConfig, RoutingType};

async fn start(dir: &Path) -> Arc<Broker> {
    let mut file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <journal-file-size>65536</journal-file-size>
             <addresses>
               <address name="orders"><anycast><queue name="orders"/></anycast></address>
             </addresses>
           </core></configuration>"#,
    )
    .unwrap();
    // A queue broker properties put on an address given no routing type,
    // which Artemis fails to deploy: after its queue on a new broker,
    // before it after a restart.
    let entries = [(
        "addressConfigurations.events.queueConfigs.audit.routingType".to_owned(),
        "ANYCAST".to_owned(),
    )];
    let warnings = cfg::broker_properties::apply(&mut file, None, "test", &entries, &|_| None);
    assert!(warnings.is_empty(), "{warnings:?}");
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

fn deployed(broker: &Broker, name: &str) -> Option<DeployedQueue> {
    broker
        .post_office()
        .deployed_queues()
        .into_iter()
        .find(|d| *d.spec.name == *name)
}

#[tokio::test]
async fn deploying_writes_what_artemis_writes_into_the_queue_configurations() {
    let dir = std::env::temp_dir().join(format!("saranga-deploy-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let broker = start(&dir).await;
    let orders = deployed(&broker, "orders").unwrap();
    assert_eq!(orders.address, "orders");
    assert!(orders.id.is_some_and(|id| id > 0), "{orders:?}");
    let s = &orders.spec;
    assert!(s.configuration_managed && !s.temporary && !s.auto_created);
    assert_eq!(s.auto_create_address, Some(true));
    // The address settings' defaults, and no static ones.
    assert_eq!((s.max_consumers, s.ring_size), (Some(-1), Some(-1)));
    assert_eq!((s.auto_delete, s.enabled), (Some(false), None));
    let audit = deployed(&broker, "audit").unwrap();
    assert_eq!(audit.spec.routing_type, Some(RoutingType::Anycast));
    assert!(audit.id.is_some());
    broker.stop().unwrap();
    drop(broker);

    let broker = start(&dir).await;
    let orders = deployed(&broker, "orders").unwrap();
    assert_eq!(orders.id, None, "it was there");
    assert!(orders.spec.configuration_managed);
    assert_eq!(orders.spec.enabled, Some(true), "the static defaults");
    assert!(
        deployed(&broker, "audit").is_none(),
        "its address fails before its queues"
    );

    let mut config = cfg::configuration::Configuration::default();
    config
        .address_configurations
        .push(cfg::configuration::CoreAddressConfiguration {
            name: Some("orders".to_owned()),
            anycast: true,
            queue_configs: vec![cfg::configuration::QueueConfiguration {
                name: "orders".to_owned(),
                ..Default::default()
            }],
            ..Default::default()
        });
    DeployedQueue::write_all(&broker.post_office().deployed_queues(), &mut config);
    let q = &config.address_configurations[0].queue_configs[0];
    assert!(q.configuration_managed && q.id.is_none());
    assert_eq!(q.routing_type.as_deref(), Some("ANYCAST"));
    assert_eq!(q.enabled, Some(true));
    broker.stop().unwrap();
    drop(broker);
    let _ = std::fs::remove_dir_all(&dir);
}
