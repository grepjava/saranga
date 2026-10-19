//! Unknown or unsupported elements produce warnings, never errors.

use saranga_config::{BrokerConfig, HaPolicy, Warning};

fn parse(xml: &str) -> BrokerConfig {
    BrokerConfig::parse_with(xml, |_| None).unwrap()
}

fn unknown(parent: &str, name: &str) -> Warning {
    Warning::UnknownElement {
        parent: parent.into(),
        name: name.into(),
    }
}

#[test]
fn unknown_core_element() {
    let c = parse(
        "<configuration><core><name>b</name><frobnicate>1</frobnicate></core></configuration>",
    );
    assert_eq!(c.name, "b");
    assert_eq!(c.warnings, [unknown("core", "frobnicate")]);
    assert_eq!(
        c.warnings[0].to_string(),
        "unknown element <frobnicate> in <core> was ignored"
    );
}

#[test]
fn features_not_acted_on_are_reported() {
    let c = parse(
        r##"<configuration><core>
              <security-settings><security-setting match="#"><permission type="send" roles="amq"/></security-setting></security-settings>
              <connection-routers/>
              <critical-analyzer>true</critical-analyzer>
              <remoting-incoming-interceptors><class-name>org.example.Check</class-name></remoting-incoming-interceptors>
              <connector-services><connector-service><factory-class>org.example.F</factory-class></connector-service></connector-services>
           </core></configuration>"##,
    );
    let names: Vec<String> = c
        .warnings
        .iter()
        .map(|w| match w {
            Warning::Unsupported { name, .. } => name.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    // The critical analyzer only watches the broker; nothing to report.
    assert_eq!(
        names,
        [
            "connection-routers",
            "remoting-incoming-interceptors",
            "connector-services"
        ]
    );
}

#[test]
fn colocated_backups_and_jdbc_are_refused() {
    for xml in [
        "<ha-policy><replication><colocated/></replication></ha-policy>",
        "<ha-policy><shared-store><colocated/></shared-store></ha-policy>",
        "<store><database-store><jdbc-connection-url>x</jdbc-connection-url></database-store></store>",
    ] {
        let e = BrokerConfig::parse_with(
            &format!("<configuration><core>{xml}</core></configuration>"),
            |_| None,
        )
        .unwrap_err();
        assert!(
            matches!(e, saranga_config::ConfigError::Unsupported { .. }),
            "{xml}: {e}"
        );
    }
    let c = parse(
        "<configuration><core><ha-policy><live-only/></ha-policy><store><file-store/></store></core></configuration>",
    );
    assert!(c.warnings.is_empty(), "{:?}", c.warnings);
}

#[test]
fn shared_store_policies() {
    let c = parse(
        "<configuration><core><ha-policy><shared-store><primary><failover-on-shutdown>true</failover-on-shutdown></primary></shared-store></ha-policy></core></configuration>",
    );
    assert_eq!(
        c.ha_policy,
        HaPolicy::SharedStorePrimary {
            failover_on_shutdown: true,
            wait_for_activation: true
        }
    );
    assert!(c.warnings.is_empty(), "{:?}", c.warnings);
    let c = parse(
        "<configuration><core><journal-lock-acquisition-timeout>5000</journal-lock-acquisition-timeout><ha-policy><shared-store><slave><allow-failback>false</allow-failback><scale-down/></slave></shared-store></ha-policy></core></configuration>",
    );
    assert_eq!(
        c.ha_policy,
        HaPolicy::SharedStoreBackup {
            allow_failback: false,
            failover_on_shutdown: false,
            restart_backup: true
        }
    );
    assert_eq!(c.journal_lock_acquisition_timeout, 5000);
    assert_eq!(
        c.warnings
            .iter()
            .map(|w| match w {
                Warning::Deprecated { name, .. } | Warning::Unsupported { name, .. } =>
                    name.as_str(),
                other => panic!("{other:?}"),
            })
            .collect::<Vec<_>>(),
        ["slave", "scale-down"]
    );
    assert_eq!(
        parse("<configuration><core/></configuration>").ha_policy,
        HaPolicy::PrimaryOnly
    );
}

#[test]
fn cluster_connections() {
    let c = parse(
        r##"<configuration><core>
              <connectors>
                 <connector name="artemis">tcp://localhost:61616</connector>
                 <connector name="node0">tcp://other:61716?sslEnabled=false</connector>
              </connectors>
              <cluster-connections>
                 <cluster-connection name="my-cluster">
                    <connector-ref>artemis</connector-ref>
                    <retry-interval>1000</retry-interval>
                    <message-load-balancing>OFF</message-load-balancing>
                    <max-hops>0</max-hops>
                    <static-connectors allow-direct-connections-only="true">
                       <connector-ref>node0</connector-ref>
                    </static-connectors>
                 </cluster-connection>
                 <cluster-connection name="lost"><connector-ref>nowhere</connector-ref></cluster-connection>
              </cluster-connections>
           </core></configuration>"##,
    );
    let names: Vec<_> = c.connectors.iter().map(|c| c.name.as_deref()).collect();
    assert_eq!(names, [Some("artemis"), Some("node0")]);
    assert_eq!(c.connectors[1].host.as_deref(), Some("other"));
    assert_eq!(c.cluster_connections.len(), 1);
    let cc = &c.cluster_connections[0];
    assert_eq!(cc.name, "my-cluster");
    assert_eq!(cc.connector_name, "artemis");
    assert_eq!(cc.retry_interval, 1000);
    assert_eq!(cc.max_hops, 0);
    assert_eq!(
        cc.message_load_balancing,
        saranga_config::MessageLoadBalancing::Off
    );
    assert_eq!(cc.static_connectors, ["node0"]);
    assert!(cc.allow_direct_connections_only);
    assert_eq!(cc.call_timeout, 30_000);
    assert_eq!(
        c.warnings
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        [
            "AMQ222217: Cannot find connector-ref nowhere. The cluster-connection lost will not be deployed."
        ]
    );
}

#[test]
fn unknown_nested_elements() {
    let c = parse(
        r##"<configuration><core>
              <address-settings>
                 <address-setting match="#">
                    <max-delivery-attempts>3</max-delivery-attempts>
                    <future-setting>x</future-setting>
                 </address-setting>
                 <stray/>
              </address-settings>
              <addresses>
                 <address name="a">
                    <anycast><queue name="q"><colour>red</colour></queue><pipe/></anycast>
                    <broadcast/>
                 </address>
              </addresses>
              <acceptors><listener/></acceptors>
              <wildcard-addresses><separator>/</separator></wildcard-addresses>
           </core></configuration>"##,
    );
    assert_eq!(c.address_settings[0].1.max_delivery_attempts(), 3);
    assert_eq!(c.addresses[0].queues.len(), 1);
    assert_eq!(
        c.warnings,
        [
            unknown("acceptors", "listener"),
            unknown("address-settings", "stray"),
            unknown("address-setting", "future-setting"),
            unknown("anycast", "pipe"),
            unknown("queue", "colour"),
            unknown("address", "broadcast"),
            unknown("wildcard-addresses", "separator"),
        ]
    );
}

#[test]
fn unknown_sibling_of_core() {
    let c = parse("<configuration><core/><jms/><other/></configuration>");
    assert_eq!(c.warnings, [unknown("configuration", "other")]);
}

#[test]
fn missing_core() {
    let c = parse("<configuration/>");
    assert_eq!(c.warnings, [Warning::NoCoreElement]);
    assert_eq!(c.name, "localhost");
    // A root `<core>` is not searched, as in Artemis.
    let c = parse("<core><name>x</name></core>");
    assert_eq!(c.warnings, [Warning::NoCoreElement]);
    assert_eq!(c.name, "localhost");
}

#[test]
fn deprecated_and_ignored_elements() {
    let c = parse(
        r#"<configuration><core>
              <queues><queue name="q"/></queues>
              <wild-card-routing-enabled>false</wild-card-routing-enabled>
           </core></configuration>"#,
    );
    assert!(c.wildcard_addresses.routing_enabled);
    assert_eq!(c.warnings.len(), 2);
    assert_eq!(c.warnings[0], Warning::QueuesDeprecated);
    assert_eq!(
        c.warnings[0].to_string(),
        "AMQ222307: The queues element is deprecated and replaced by the addresses element"
    );
    assert!(
        matches!(&c.warnings[1], Warning::Unsupported { name, .. } if name == "wild-card-routing-enabled")
    );
}

#[test]
fn prefixed_namespaces_match_on_local_name() {
    let c = parse(
        r#"<a:configuration xmlns:a="urn:activemq" xmlns:c="urn:activemq:core">
              <c:core><c:name>ns</c:name><c:persistence-enabled>false</c:persistence-enabled></c:core>
           </a:configuration>"#,
    );
    assert_eq!(c.name, "ns");
    assert!(!c.persistence_enabled);
    assert!(c.warnings.is_empty());
}

#[test]
fn replication_policies() {
    let c = parse(
        "<configuration><core><ha-policy><replication><primary>           <group-name>g</group-name><vote-on-replication-failure>true</vote-on-replication-failure>           <quorum-size>3</quorum-size><initial-replication-sync-timeout>9000</initial-replication-sync-timeout>         </primary></replication></ha-policy></core></configuration>",
    );
    let HaPolicy::ReplicationPrimary(r) = &c.ha_policy else {
        panic!("{:?}", c.ha_policy)
    };
    assert_eq!(r.group_name.as_deref(), Some("g"));
    assert!(r.vote_on_replication_failure);
    assert_eq!(
        (r.quorum_size, r.initial_replication_sync_timeout),
        (3, 9000)
    );
    // A primary hands back to a restarted primary; it checks for one
    // active with its node ID only when told to.
    assert!(r.allow_failback && !r.check_for_active_server);
    assert!(!c.ha_policy.is_backup());

    let c = parse(
        "<configuration><core><ha-policy><replication><backup></backup></replication></ha-policy></core></configuration>",
    );
    let HaPolicy::ReplicationBackup(r) = &c.ha_policy else {
        panic!("{:?}", c.ha_policy)
    };
    assert!(!r.allow_failback);

    let c = parse(
        "<configuration><core><ha-policy><replication><slave>           <allow-failback>true</allow-failback><max-saved-replicated-journals-size>0</max-saved-replicated-journals-size>         </slave></replication></ha-policy></core></configuration>",
    );
    let HaPolicy::ReplicationBackup(r) = &c.ha_policy else {
        panic!("{:?}", c.ha_policy)
    };
    assert!(r.allow_failback && r.restart_backup);
    assert_eq!(r.max_saved_replicated_journals_size, 0);
    assert_eq!(
        (r.vote_retries, r.vote_retry_wait, r.quorum_vote_wait),
        (12, 5000, 30)
    );
    assert!(c.ha_policy.is_backup());
}

#[test]
fn pluggable_replication_policies() {
    let c = parse(
        r#"<configuration><core><ha-policy><replication><primary>
             <manager>
               <properties>
                 <property key="connect-string" value="127.0.0.1:6666,127.0.0.1:6667"/>
                 <property key="session-ms" value="4000"/>
               </properties>
             </manager>
             <coordination-id>peer-journal-001</coordination-id>
           </primary></replication></ha-policy></core></configuration>"#,
    );
    let HaPolicy::ReplicationPrimary(r) = &c.ha_policy else {
        panic!("{:?}", c.ha_policy)
    };
    let m = r.manager.as_ref().expect("a manager");
    assert_eq!(
        m.class_name,
        "org.apache.activemq.artemis.lockmanager.zookeeper.CuratorDistributedLockManager"
    );
    assert_eq!(
        m.properties["connect-string"],
        "127.0.0.1:6666,127.0.0.1:6667"
    );
    assert_eq!(m.properties["session-ms"], "4000");
    // `setCoordinationId`: dashes made dots; a primary never hands back.
    assert_eq!(r.coordination_id.as_deref(), Some("peer.journal.001"));
    assert!(!r.allow_failback);
    assert_eq!(r.retry_replication_wait, 2000);

    let c = parse(
        r#"<configuration><core><ha-policy><replication><backup>
             <manager><class-name>org.apache.activemq.artemis.lockmanager.file.FileBasedLockManager</class-name>
               <properties><property key="locks-folder" value="/tmp/locks"/></properties></manager>
             <allow-failback>true</allow-failback>
           </backup></replication></ha-policy></core></configuration>"#,
    );
    let HaPolicy::ReplicationBackup(r) = &c.ha_policy else {
        panic!("{:?}", c.ha_policy)
    };
    assert!(r.allow_failback);
    assert_eq!(
        r.manager.as_ref().unwrap().properties["locks-folder"],
        "/tmp/locks"
    );

    let c = parse(
        "<configuration><core><ha-policy><replication><primary><manager/><coordination-id>abc</coordination-id></primary></replication></ha-policy></core></configuration>",
    );
    let HaPolicy::ReplicationPrimary(r) = &c.ha_policy else {
        panic!()
    };
    assert_eq!(r.coordination_id.as_deref(), Some("abc+"));
    assert!(r.manager.as_ref().unwrap().properties.is_empty());
}

#[test]
fn legacy_queues_are_deployed() {
    let c = parse(
        r#"<configuration><core>
              <queues>
                 <queue name="q1"/>
                 <queue name="q2" max-consumers="3" purge-on-no-consumers="true">
                    <address>a2</address>
                    <filter string="color='red'"/>
                    <durable>false</durable>
                    <user>bob</user>
                 </queue>
              </queues>
           </core></configuration>"#,
    );
    assert_eq!(c.queues.len(), 2);
    let (q1, q2) = (&c.queues[0], &c.queues[1]);
    assert_eq!((q1.name.as_str(), q1.address.as_str()), ("q1", "q1"));
    assert_eq!(q1.routing_type, saranga_config::RoutingType::Multicast);
    assert!(q1.durable);
    assert_eq!((q2.name.as_str(), q2.address.as_str()), ("q2", "a2"));
    assert_eq!(q2.filter.as_deref(), Some("color='red'"));
    assert!(!q2.durable);
    assert_eq!(q2.user.as_deref(), Some("bob"));
    assert_eq!(q2.max_consumers, Some(3));
    assert!(q2.purge_on_no_consumers);
    assert!(c.addresses.is_empty());
}

#[test]
fn legacy_jms_configuration() {
    let c = parse(
        r#"<configuration xmlns="urn:activemq">
              <core xmlns="urn:activemq:core"/>
              <jms xmlns="urn:activemq:jms">
                 <topic name="t1"/>
                 <queue name="jq1"><selector string="x > 1"/><durable>false</durable></queue>
                 <queue name="jq2"/>
                 <queue/>
              </jms>
           </configuration>"#,
    );
    let shown: Vec<_> = c
        .jms_addresses
        .iter()
        .map(|a| {
            (
                a.name.as_str(),
                a.routing_types.clone(),
                a.queues
                    .iter()
                    .map(|q| (q.name.clone(), q.filter.clone(), q.durable))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    use saranga_config::RoutingType::{Anycast, Multicast};
    assert_eq!(
        shown,
        [
            (
                "jq1",
                vec![Anycast],
                vec![("jq1".to_owned(), Some("x > 1".to_owned()), false)]
            ),
            ("jq2", vec![Anycast], vec![("jq2".to_owned(), None, true)]),
            ("t1", vec![Multicast], vec![]),
        ]
    );
    assert_eq!(
        c.warnings,
        [Warning::JmsKeyMissing {
            element: "queue".into()
        }]
    );
    assert!(c.addresses.is_empty());
}

#[test]
fn xinclude_brings_in_another_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("addresses.xml"),
        r#"<addresses xmlns="urn:activemq:core">
             <address name="${prefix}.q"><anycast><queue name="${prefix}.q"/></anycast></address>
           </addresses>"#,
    )
    .unwrap();
    let xml = format!(
        r#"<configuration xmlns="urn:activemq" xmlns:xi="http://www.w3.org/2001/XInclude">
              <core xmlns="urn:activemq:core">
                 <name>${{name}}</name>
                 <xi:include href="{}/addresses.xml"/>
              </core>
           </configuration>"#,
        dir.path().display()
    );
    let lookup = |k: &str| match k {
        "name" => Some("b1".to_owned()),
        "prefix" => Some("orders".to_owned()),
        _ => None,
    };
    let c = BrokerConfig::parse_with(&xml, lookup).unwrap();
    assert_eq!(c.name, "b1");
    assert_eq!(c.addresses.len(), 1);
    assert_eq!(c.addresses[0].name, "orders.q");
    assert!(c.warnings.is_empty(), "{:?}", c.warnings);

    let missing = r#"<configuration xmlns:xi="http://www.w3.org/2001/XInclude"><core>
           <xi:include href="no-such-file.xml"/></core></configuration>"#;
    let e = BrokerConfig::parse_with(missing, |_| None).unwrap_err();
    assert!(
        e.to_string().contains("no fallback element was found"),
        "{e}"
    );
}
