//! `<database-store>` with `database-storage`: Artemis's relational store,
//! its settings as `createDatabaseStoreConfig` reads them.

use saranga_config::configuration::{DatabaseStorage, StoreConfiguration, read_configuration};
use saranga_config::{BrokerConfig, Validation};

const SETTINGS: &str = "<database-storage>true</database-storage>\
    <jdbc-connection-url>jdbc:postgresql://h/d</jdbc-connection-url>\
    <database-connections>12</database-connections>\
    <database-max-read-connections>3</database-max-read-connections>\
    <database-read-idle-timeout>7000</database-read-idle-timeout>\
    <database-max-retries>9</database-max-retries>\
    <database-retry-interval-millis>100</database-retry-interval-millis>\
    <database-flush-period-nanos>100000</database-flush-period-nanos>\
    <max-pending-writes>500</max-pending-writes>\
    <database-page-join-fetch>true</database-page-join-fetch>";

/// What the schema requires of every `<database-store>`.
const TABLES: &str = "<message-table-name>MESSAGES</message-table-name>    <bindings-table-name>BINDINGS</bindings-table-name>    <large-message-table-name>LARGE_MESSAGES</large-message-table-name>    <page-store-table-name>PAGE_STORE</page-store-table-name>";

fn document(store: &str) -> String {
    format!(
        r#"<configuration xmlns="urn:activemq" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><core xmlns="urn:activemq:core"><store><database-store>{TABLES}{store}</database-store></store></core></configuration>"#
    )
}

fn parsed(store: &str) -> DatabaseStorage {
    let validation = Validation::Deploy { jms: true };
    let c = read_configuration(&document(store), |_| None, validation, None).unwrap();
    match c.store_configuration {
        Some(StoreConfiguration::Database(d)) => *d,
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_settings_are_read() {
    let d = parsed(SETTINGS);
    assert!(d.database_storage);
    assert_eq!(d.database_connections, 12);
    assert_eq!(d.database_max_read_connections, 3);
    assert_eq!(d.database_read_idle_timeout, 7000);
    assert_eq!(d.database_max_retries, 9);
    assert_eq!(d.database_retry_interval_millis, 100);
    assert_eq!(d.database_flush_period_nanos, 100_000);
    assert_eq!(d.max_pending_writes, 500);
    assert!(d.page_join_fetch);
}

/// The defaults are `ActiveMQDefaultConfiguration`'s, which differ from
/// what the schema's documentation says of three of them.
#[test]
fn the_defaults_are_the_code_s() {
    let d = parsed("");
    assert!(!d.database_storage);
    assert_eq!(d.database_connections, 6);
    assert_eq!(d.database_max_read_connections, -1);
    assert_eq!(d.database_read_idle_timeout, 5000);
    assert_eq!(d.database_max_retries, 5);
    assert_eq!(d.database_retry_interval_millis, 5000);
    assert_eq!(d.database_flush_period_nanos, 3_333_333);
    assert_eq!(d.max_pending_writes, 100_000);
    assert!(!d.page_join_fetch);
}

#[test]
fn the_schema_takes_them() {
    BrokerConfig::validate(
        &document(SETTINGS),
        |_| None,
        Validation::Deploy { jms: true },
    )
    .unwrap();
}

/// `database-storage` is the store Saranga has; without it, the JDBC journal
/// it does not.
#[test]
fn only_the_jdbc_journal_is_refused() {
    let parse =
        |store: &str| BrokerConfig::parse_validated(&document(store), |_| None, Validation::Reload);
    assert!(matches!(
        parse("<jdbc-connection-url>x</jdbc-connection-url>"),
        Err(saranga_config::ConfigError::Unsupported { .. })
    ));
    let c = parse(SETTINGS).unwrap();
    let d = c.database_store.expect("the store's settings");
    assert_eq!(d.database_connections, 12);
    assert_eq!(
        d.jdbc_connection_url.as_deref(),
        Some("jdbc:postgresql://h/d")
    );
}
