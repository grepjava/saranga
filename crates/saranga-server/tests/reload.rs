//! Configuration reload (`deployReloadableConfigFromConfiguration`).

use std::path::Path;
use std::sync::Arc;

use saranga_config as cfg;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::reload::Reload;
use saranga_server::{Broker, BrokerConfig};

fn xml(persistent: bool, body: &str) -> cfg::BrokerConfig {
    cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>{persistent}</persistence-enabled>
             <journal-pool-files>2</journal-pool-files>
             <journal-file-size>65536</journal-file-size>
             {body}
           </core></configuration>"#
    ))
    .unwrap()
}

async fn start(file: &cfg::BrokerConfig, instance: &Path) -> Arc<Broker> {
    let mut config =
        BrokerConfig::from_file_config(file, Arc::new(SelectorFilters), instance).unwrap();
    config.address_queue_scan_period = -1;
    Broker::start(config).await.unwrap()
}

const BEFORE: &str = r##"
  <address-settings>
    <address-setting match="#"><max-delivery-attempts>5</max-delivery-attempts></address-setting>
    <address-setting match="forced.#">
      <config-delete-addresses>FORCE</config-delete-addresses>
      <config-delete-queues>FORCE</config-delete-queues>
    </address-setting>
  </address-settings>
  <addresses>
    <address name="orders"><anycast><queue name="orders"/></anycast></address>
    <address name="forced.gone"><anycast><queue name="forced.gone"/></anycast></address>
    <address name="forced.keep"><multicast>
      <queue name="forced.keep.a"/><queue name="forced.keep.b"/>
    </multicast></address>
    <address name="kept"><anycast><queue name="kept"/></anycast></address>
  </addresses>
  <diverts>
    <divert name="d1"><address>orders</address><forwarding-address>kept</forwarding-address></divert>
    <divert name="d2"><address>orders</address><forwarding-address>kept</forwarding-address></divert>
  </diverts>"##;

const AFTER: &str = r##"
  <address-settings>
    <address-setting match="#"><max-delivery-attempts>7</max-delivery-attempts></address-setting>
    <address-setting match="forced.#">
      <config-delete-addresses>FORCE</config-delete-addresses>
      <config-delete-queues>FORCE</config-delete-queues>
    </address-setting>
  </address-settings>
  <addresses>
    <address name="orders"><anycast>
      <queue name="orders" max-consumers="1"><filter string="colour = 'red'"/></queue>
    </anycast></address>
    <address name="forced.keep"><multicast><queue name="forced.keep.a"/></multicast></address>
    <address name="added"><anycast><queue name="added"/></anycast></address>
  </addresses>
  <diverts>
    <divert name="d1"><address>orders</address><forwarding-address>added</forwarding-address>
      <exclusive>true</exclusive></divert>
    <divert name="d3"><address>orders</address><forwarding-address>kept</forwarding-address></divert>
  </diverts>"##;

#[tokio::test]
async fn reload_reconciles() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(&xml(false, BEFORE), dir.path()).await;
    let po = broker.post_office();
    assert_eq!(po.settings("orders").max_delivery_attempts, 5);

    broker
        .reload(Reload::from_file_config(&xml(false, AFTER)).unwrap())
        .await;

    assert_eq!(
        po.settings("orders").max_delivery_attempts,
        7,
        "settings swapped"
    );
    let orders = po.queue("orders").unwrap();
    assert_eq!(orders.config().max_consumers, 1, "queue updated");
    // The new filter routes; the configuration queue queries report keeps
    // the old one, as in Artemis.
    assert_eq!(orders.filter_string().as_deref(), Some("colour = 'red'"));
    assert!(orders.filter().is_some());
    assert_eq!(orders.config().filter_string, None);
    assert!(po.queue("added").is_some(), "new queue deployed");
    assert!(po.queue("forced.gone").is_none() && po.address_info("forced.gone").is_none());
    assert!(po.queue("forced.keep.a").is_some());
    assert!(
        po.queue("forced.keep.b").is_none(),
        "config-delete-queues FORCE"
    );
    assert!(
        po.queue("kept").is_some() && po.address_info("kept").is_some(),
        "config-delete-* OFF keeps what left the configuration"
    );
    let mut diverts: Vec<_> = po
        .diverts()
        .iter()
        .map(|d| (d.name.to_string(), d.exclusive, d.forwarding.join(",")))
        .collect();
    diverts.sort();
    assert_eq!(
        diverts,
        vec![
            ("d1".to_owned(), true, "added".to_owned()),
            ("d3".to_owned(), false, "kept".to_owned()),
        ]
    );
}

/// The queue update is journalled (`updateQueueBinding`): after a restart
/// whose configuration no longer lists the queue, it has the updated
/// settings, the filter included.
#[tokio::test]
async fn queue_update_is_recovered() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(&xml(true, BEFORE), dir.path()).await;
    broker
        .reload(Reload::from_file_config(&xml(true, AFTER)).unwrap())
        .await;
    broker.stop().unwrap();
    drop(broker);

    let broker = start(&xml(true, ""), dir.path()).await;
    let orders = broker.post_office().queue("orders").unwrap().config();
    assert_eq!(orders.max_consumers, 1);
    assert_eq!(orders.filter_string.as_deref(), Some("colour = 'red'"));
}
