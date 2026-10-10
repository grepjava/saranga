//! Unknown or unsupported elements produce warnings, never errors.

use saranga_config::{BrokerConfig, Warning};

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
              <cluster-connections/>
              <critical-analyzer>true</critical-analyzer>
              <ha-policy><replication><primary/></replication></ha-policy>
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
    assert_eq!(names, ["ha-policy", "cluster-connections"]);
}

#[test]
fn backups_and_jdbc_are_refused() {
    for xml in [
        "<ha-policy><replication><backup/></replication></ha-policy>",
        "<ha-policy><shared-store><slave/></shared-store></ha-policy>",
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
    assert_eq!(c.warnings.len(), 2);
    assert!(matches!(&c.warnings[0], Warning::Unsupported { name, .. } if name == "jms"));
    assert_eq!(c.warnings[1], unknown("configuration", "other"));
}

#[test]
fn xinclude_is_reported() {
    let c = parse(
        r#"<configuration xmlns="urn:activemq" xmlns:xi="http://www.w3.org/2001/XInclude">
              <core xmlns="urn:activemq:core">
                 <xi:include href="addresses.xml"/>
              </core>
           </configuration>"#,
    );
    assert_eq!(
        c.warnings,
        [Warning::XIncludeUnsupported {
            href: Some("addresses.xml".into())
        }]
    );
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
    let names: Vec<_> = c
        .warnings
        .iter()
        .map(|w| match w {
            Warning::Unsupported { name, .. } => name.as_str(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(names, ["queues", "wild-card-routing-enabled"]);
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
