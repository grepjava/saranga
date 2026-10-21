//! `artemis properties` against Artemis's own output: each
//! `tests/golden/properties/<case>.xml` read and exported must give
//! `<case>.properties`, the file Artemis 2.57's `artemis properties` wrote
//! for it (`-Xmx1g`, on Windows), the lines before a failure included.

use std::path::{Path, PathBuf};

use saranga_config::Validation;
use saranga_config::configuration::{
    ExportContext, ExportError, export_as_properties, read_configuration,
};

fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/golden/properties")
}

/// The value of `key=` in the golden file: what Artemis took from where it
/// ran, which the export is given the same.
fn line_value(expected: &str, key: &str) -> Option<String> {
    expected
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{key}=")))
        .map(str::to_owned)
}

fn export(case: &str) -> (String, String, Option<ExportError>) {
    let dir = golden_dir();
    let xml = std::fs::read_to_string(dir.join(format!("{case}.xml"))).unwrap();
    let expected = std::fs::read_to_string(dir.join(format!("{case}.properties")))
        .unwrap()
        .replace("\r\n", "\n");
    let url = line_value(&expected, "configurationUrl");
    let config = read_configuration(
        &xml,
        |_| None,
        Validation::Deploy { jms: true },
        url.as_deref(),
    )
    .unwrap();
    let ctx = ExportContext {
        max_memory: 1 << 30,
        broker_instance: line_value(&expected, "brokerInstance").unwrap_or_default(),
        uuid: || "not-used".to_owned(),
        identity: |class| format!("{class}@0"),
    };
    let (text, failed) = export_as_properties(&config, &ctx);
    (expected, text.replace("\r\n", "\n"), failed)
}

fn assert_same(case: &str) {
    let (expected, actual, failed) = export(case);
    assert_eq!(failed, None, "{case}");
    assert_eq!(actual, expected, "{case}");
}

#[test]
fn defaults() {
    assert_same("empty");
}

#[test]
fn artemis_full_test_configuration() {
    assert_same("full-config");
}

#[test]
fn transports_of_every_schema() {
    assert_same("connectors");
}

#[test]
fn ha_policies() {
    for case in [
        "ha-replicated",
        "ha-repl-primary-mgr",
        "ha-repl-backup-mgr",
        "ha-shared-primary",
        "ha-shared-backup",
        "ha-colocated",
        "ha-repl-colocated",
    ] {
        assert_same(case);
    }
}

#[test]
fn stores_routers_and_the_rest() {
    for case in [
        "store-file",
        "store-db",
        "routers",
        "lock-coord",
        "security",
        "federation",
        "amqp-uris",
        "cluster-uri",
        "metrics-logging",
    ] {
        assert_same(case);
    }
}

#[test]
fn a_divert_without_routing_name_gets_a_new_uuid() {
    // Artemis's UUID is its own, made as it ran.
    let (expected, actual, failed) = export("misc");
    assert_eq!(failed, None);
    let routing = |text: &str| {
        text.lines()
            .map(
                |l| match l.strip_prefix("divertConfigurations.d1.routingName=") {
                    Some(_) => "divertConfigurations.d1.routingName=<uuid>".to_owned(),
                    None => l.to_owned(),
                },
            )
            .collect::<Vec<_>>()
    };
    assert_eq!(routing(&actual), routing(&expected));
    assert!(actual.contains("divertConfigurations.d1.routingName=not-used\n"));
}

#[test]
fn a_replica_stops_at_check_for_primary_server() {
    let (expected, actual, failed) = export("ha-replica");
    assert_eq!(
        failed,
        Some(ExportError::Runtime(
            "accessing: checkForPrimaryServer@[]".to_owned()
        ))
    );
    assert_eq!(actual, expected);
}

#[test]
fn terminus_capabilities_stop_the_export() {
    let (expected, actual, failed) = export("amqp-caps");
    assert!(matches!(failed, Some(ExportError::NullPointer(_))));
    assert_eq!(actual, expected);
}

/// The broker plugin lines of `<broker-plugins>` holding the logging plugin
/// and a `ConnectionPeriodicExpiryPlugin` with `properties`.
fn expiry_plugin(properties: &str) -> Result<Vec<String>, saranga_config::ConfigError> {
    let xml = format!(
        r#"<?xml version='1.0'?>
<configuration xmlns="urn:activemq" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="urn:activemq /schema/artemis-configuration.xsd">
   <core xmlns="urn:activemq:core">
      <broker-plugins>
         <broker-plugin class-name="org.apache.activemq.artemis.core.server.plugin.impl.LoggingActiveMQServerPlugin"/>
         <broker-plugin class-name="org.apache.activemq.artemis.core.server.plugin.impl.ConnectionPeriodicExpiryPlugin">{properties}</broker-plugin>
      </broker-plugins>
   </core>
</configuration>"#
    );
    let config = read_configuration(&xml, |_| None, Validation::Deploy { jms: true }, None)?;
    let ctx = ExportContext {
        max_memory: 1 << 30,
        broker_instance: String::new(),
        uuid: || "not-used".to_owned(),
        identity: |class| format!("{class}@0"),
    };
    let (text, failed) = export_as_properties(&config, &ctx);
    assert_eq!(failed, None);
    Ok(text
        .lines()
        .filter(|l| l.contains("Plugins."))
        .map(str::to_owned)
        .collect())
}

/// `ConnectionPeriodicExpiryPlugin` as Artemis's manual configures it,
/// which Artemis 2.57's parser refuses (docs/upstream/artemis-issues.md,
/// issue 20): exported as `exportAsProperties` exports the plugin made and
/// `init`ed so (scratchpad probe `CpeExport.java` on 2.57), in
/// `brokerPlugins` alone, as an `ActiveMQServerBasePlugin`.
#[test]
fn a_connection_expiry_plugin_as_the_manual_configures_it() {
    let regex = r#"<property key="acceptorMatchRegex" value="netty-client-acceptor"/><property key="periodSeconds" value="60"/>"#;
    assert_eq!(
        expiry_plugin(regex).unwrap(),
        [
            "brokerPlugins.name-attribute-must-be-set-for-properties-key-export.acceptorMatchRegex=netty-client-acceptor",
            "brokerPlugins.name-attribute-must-be-set-for-properties-key-export.accuracyWindowSeconds=30",
            "brokerPlugins.name-attribute-must-be-set-for-properties-key-export.periodSeconds=60",
        ]
    );
    assert_eq!(
        expiry_plugin(&format!(r#"{regex}<property key="name" value="exp"/>"#)).unwrap(),
        [
            "brokerPlugins.exp.acceptorMatchRegex=netty-client-acceptor",
            "brokerPlugins.exp.accuracyWindowSeconds=30",
            "brokerPlugins.exp.name=exp",
            "brokerPlugins.exp.periodSeconds=60",
        ]
    );
    // `init`'s exceptions.
    for (properties, message) in [
        (
            r#"<property key="acceptorMatchRegex" value="a"/><property key="accuracyWindowSeconds" value="0"/>"#,
            "accuracyWindowSeconds must be > 0",
        ),
        (
            r#"<property key="acceptorMatchRegex" value="a"/><property key="periodSeconds" value="x"/>"#,
            "For input string: \"x\"",
        ),
        (
            r#"<property key="acceptorMatchRegex" value="a"/><property key="accuracyWindowSeconds" value="y"/>"#,
            "For input string: \"y\"",
        ),
        ("", "acceptorMatchRegex must be configured"),
    ] {
        match expiry_plugin(properties) {
            Err(saranga_config::ConfigError::Runtime(m)) => assert_eq!(m, message),
            other => panic!("{properties}: {other:?}"),
        }
    }
}
