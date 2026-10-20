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
