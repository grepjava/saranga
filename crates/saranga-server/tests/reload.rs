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
        .await
        .unwrap();

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
        .await
        .unwrap();
    broker.stop().unwrap();
    drop(broker);

    let broker = start(&xml(true, ""), dir.path()).await;
    let orders = broker.post_office().queue("orders").unwrap().config();
    assert_eq!(orders.max_consumers, 1);
    assert_eq!(orders.filter_string.as_deref(), Some("colour = 'red'"));
}

/// The deprecated `<queues>` (`deployQueuesFromConfiguration`), at start
/// and on reload, and the legacy `<jms>` (`LegacyJMSConfiguration`), at
/// start only.
#[tokio::test]
async fn legacy_queues_and_jms() {
    use saranga_server::RoutingType;
    let dir = tempfile::tempdir().unwrap();
    let file = cfg::BrokerConfig::parse(
        r#"<configuration xmlns="urn:activemq">
             <core xmlns="urn:activemq:core">
               <persistence-enabled>false</persistence-enabled>
               <queues>
                 <queue name="q1"/>
                 <queue name="q2"><address>a2</address><filter string="x = 1"/></queue>
               </queues>
             </core>
             <jms xmlns="urn:activemq:jms">
               <queue name="jq"><selector string="y = 2"/></queue>
               <topic name="jt"/>
             </jms>
           </configuration>"#,
    )
    .unwrap();
    let broker = start(&file, dir.path()).await;
    let po = broker.post_office();
    let q1 = po.queue("q1").unwrap();
    assert_eq!(&*q1.config().address, "q1");
    assert!(
        po.address_info("q1")
            .unwrap()
            .routing_types
            .contains(RoutingType::Multicast)
    );
    let q2 = po.queue("q2").unwrap();
    assert_eq!(&*q2.config().address, "a2");
    assert_eq!(q2.filter_string().as_deref(), Some("x = 1"));
    assert!(
        po.address_info("a2")
            .unwrap()
            .routing_types
            .contains(RoutingType::Multicast)
    );
    let jq = po.queue("jq").unwrap();
    assert_eq!(&*jq.config().address, "jq");
    assert_eq!(jq.filter_string().as_deref(), Some("y = 2"));
    assert!(
        po.address_info("jq")
            .unwrap()
            .routing_types
            .contains(RoutingType::Anycast)
    );
    let jt = po.address_info("jt").unwrap();
    assert!(jt.routing_types.contains(RoutingType::Multicast));
    assert!(!jt.routing_types.contains(RoutingType::Anycast));

    broker
        .reload(
            Reload::from_file_config(&xml(
                false,
                "<queues><queue name=\"q3\"><address>a3</address></queue></queues>",
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(&*po.queue("q3").unwrap().config().address, "a3");
    // Nothing is undeployed without config-delete-* FORCE.
    assert!(po.queue("q1").is_some() && po.queue("jq").is_some());
}

/// A port nothing listens on.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn listens(port: u16) -> bool {
    std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
}

/// An acceptor plan for `acceptors` (name and URI), making each as `run`
/// does.
fn plan(acceptors: &[(&str, String)]) -> saranga_server::acceptors::AcceptorPlan {
    let configs: Vec<cfg::AcceptorConfig> = acceptors
        .iter()
        .map(|(name, uri)| cfg::AcceptorConfig::parse(Some(name), uri).unwrap())
        .collect();
    saranga_server::acceptors::AcceptorPlan {
        configured: configs
            .iter()
            .map(|a| (a.name.clone().unwrap(), a.transport_params()))
            .collect(),
        create: Box::new(move |name| {
            let a = configs
                .iter()
                .find(|a| a.name.as_deref() == Some(name))
                .unwrap();
            let port = a.port.unwrap();
            Ok(Some(saranga_server::acceptors::NewAcceptor {
                address: format!("127.0.0.1:{port}"),
                acceptor: saranga_server::Acceptor {
                    name: name.to_owned(),
                    host: "127.0.0.1".to_owned(),
                    port,
                    ..Default::default()
                },
                auto_start: a.param("autoStart").is_none_or(|v| v == "true"),
            }))
        }),
    }
}

/// `updateAcceptors`: a changed acceptor is made again, a removed one
/// stops, a new one starts unless `autoStart` is false, and one whose
/// parameters are only reordered is left alone.
#[tokio::test]
async fn acceptors_are_reconciled() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(&xml(false, ""), dir.path()).await;
    let [kept, changed, moved, removed, added, manual] = std::array::from_fn(|_| free_port());
    let uri = |port: u16, query: &str| format!("tcp://127.0.0.1:{port}?{query}");
    let before = [
        ("kept", uri(kept, "tcpNoDelay=true;protocols=CORE")),
        ("changed", uri(changed, "protocols=CORE")),
        ("removed", uri(removed, "protocols=CORE")),
    ];
    let p = plan(&before);
    broker.set_configured_acceptors(p.configured.clone());
    for (name, uri) in &before {
        let a = cfg::AcceptorConfig::parse(Some(name), uri).unwrap();
        let port = a.port.unwrap();
        broker
            .add_acceptor(
                name,
                &format!("127.0.0.1:{port}"),
                saranga_server::Acceptor {
                    name: name.to_string(),
                    host: "127.0.0.1".to_owned(),
                    port,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    assert!(listens(kept) && listens(changed) && listens(removed));

    let mut reload = Reload::from_file_config(&xml(false, "")).unwrap();
    reload.acceptors = Some(plan(&[
        ("kept", uri(kept, "protocols=CORE;tcpNoDelay=true")),
        ("changed", uri(moved, "protocols=CORE")),
        ("added", uri(added, "protocols=CORE")),
        ("manual", uri(manual, "protocols=CORE;autoStart=false")),
    ]));
    broker.reload(reload).await.unwrap();
    assert!(listens(kept), "kept");
    assert!(!listens(changed) && listens(moved), "changed");
    assert!(!listens(removed), "removed");
    assert!(listens(added), "added");
    assert!(!listens(manual), "autoStart=false");
    let mut names = broker.acceptor_names();
    names.sort();
    assert_eq!(names, ["added", "changed", "kept", "manual"]);
    assert_eq!(broker.acceptor("manual").map(|(_, started)| started), Some(false));
    broker.start_acceptor("manual").await.unwrap();
    assert!(listens(manual));

    // A new acceptor on a port in use fails the reload, after the others.
    let mut reload = Reload::from_file_config(&xml(false, "")).unwrap();
    reload.acceptors = Some(plan(&[
        ("kept", uri(kept, "protocols=CORE;tcpNoDelay=true")),
        ("changed", uri(moved, "protocols=CORE")),
        ("added", uri(added, "protocols=CORE")),
        ("manual", uri(manual, "protocols=CORE;autoStart=false")),
        ("clash", uri(kept, "protocols=CORE")),
    ]));
    let e = broker.reload(reload).await.unwrap_err();
    assert!(
        e.starts_with(&format!(
            "java.lang.IllegalStateException: AMQ229230: Failed to bind acceptor clash to 127.0.0.1:{kept}\nCaused by: java.net.BindException: Address already in use"
        )),
        "{e}"
    );
}
