//! Parses `tests/golden/etc/broker.xml`, the file `artemis create` generated
//! for the reference broker.

use std::path::Path;

use saranga_config::{AddressFullMessagePolicy, BrokerConfig, JournalType, RoutingType, Warning};

fn golden() -> BrokerConfig {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/golden/etc/broker.xml");
    BrokerConfig::from_file(path).unwrap()
}

#[test]
fn top_level_settings() {
    let c = golden();
    assert_eq!(c.name, "0.0.0.0");
    assert!(c.persistence_enabled);
    assert!(c.security_enabled);
    assert_eq!(c.journal_directory, "data/journal");
    assert_eq!(c.bindings_directory, "data/bindings");
    assert_eq!(c.paging_directory, "data/paging");
    assert_eq!(c.large_messages_directory, "data/large-messages");
    // Only in a comment, so the JVM-derived default applies.
    assert_eq!(c.global_max_size, None);
    assert_eq!(c.global_max_messages, -1);
    assert!(c.wildcard_addresses.routing_enabled);
    assert_eq!(c.wildcard_addresses.delimiter, '.');
    assert_eq!(c.literal_match_markers, None);
}

#[test]
fn journal_settings() {
    let j = golden().journal;
    assert_eq!(j.journal_type, JournalType::Asyncio);
    assert_eq!(j.datasync, Some(true));
    assert_eq!(j.min_files, Some(2));
    assert_eq!(j.pool_files, Some(10));
    assert_eq!(j.device_block_size, Some(4096));
    assert_eq!(j.file_size, Some(10 * 1024 * 1024));
    assert_eq!(j.buffer_timeout, Some(24000));
    assert_eq!(j.max_io, Some(4096));
    assert_eq!(j.max_redelivery_records, Some(1));
    assert_eq!(j.purge_page_folders, Some(false));
    assert_eq!(j.page_sync_timeout, Some(800000));
    assert_eq!(j.disk_scan_period, Some(5000));
    assert_eq!(j.max_disk_usage, Some(90));
    assert_eq!(j.buffer_size, None);
    assert_eq!(j.compact_min_files, None);
}

#[test]
fn acceptors() {
    let c = golden();
    let names: Vec<_> = c
        .acceptors
        .iter()
        .map(|a| a.name.as_deref().unwrap())
        .collect();
    assert_eq!(names, ["artemis", "amqp", "stomp", "hornetq", "mqtt"]);

    let artemis = &c.acceptors[0];
    assert_eq!(artemis.scheme, "tcp");
    assert_eq!(artemis.host.as_deref(), Some("0.0.0.0"));
    assert_eq!(artemis.port, Some(61616));
    assert_eq!(
        artemis.protocols(),
        ["CORE", "AMQP", "STOMP", "HORNETQ", "MQTT", "OPENWIRE"]
    );
    assert_eq!(artemis.param("tcpSendBufferSize"), Some("1048576"));
    assert_eq!(artemis.param("amqpCredits"), Some("1000"));
    assert_eq!(artemis.param("supportAdvisory"), Some("false"));
    assert_eq!(artemis.params.len(), 10);
    assert!(
        artemis
            .uri
            .starts_with("tcp://0.0.0.0:61616?tcpSendBufferSize=1048576;")
    );

    let ports: Vec<_> = c.acceptors.iter().map(|a| a.port.unwrap()).collect();
    assert_eq!(ports, [61616, 5672, 61613, 5445, 1883]);

    let hornetq = &c.acceptors[3];
    assert_eq!(hornetq.protocols(), ["HORNETQ", "STOMP"]);
    assert_eq!(hornetq.param("anycastPrefix"), Some("jms.queue."));
    assert_eq!(hornetq.param("multicastPrefix"), Some("jms.topic."));
    assert_eq!(c.acceptors[1].protocols(), ["AMQP"]);
    assert_eq!(c.acceptors[4].protocols(), ["MQTT"]);
}

#[test]
fn address_settings() {
    let c = golden();
    let matches: Vec<_> = c.address_settings.iter().map(|(m, _)| m.as_str()).collect();
    assert_eq!(matches, ["activemq.management.#", "#"]);

    let mgmt = &c.address_settings[0].1;
    assert_eq!(mgmt.dead_letter_address.as_deref(), Some("DLQ"));
    assert_eq!(mgmt.expiry_address.as_deref(), Some("ExpiryQueue"));
    assert_eq!(mgmt.redelivery_delay, Some(0));
    assert_eq!(mgmt.max_size_bytes, Some(-1));
    assert_eq!(mgmt.message_counter_history_day_limit, Some(10));
    assert_eq!(
        mgmt.address_full_message_policy,
        Some(AddressFullMessagePolicy::Page)
    );
    assert_eq!(mgmt.auto_create_queues, Some(true));
    assert_eq!(mgmt.auto_create_addresses, Some(true));
    assert_eq!(mgmt.auto_delete_queues, None);

    let all = &c.address_settings[1].1;
    assert_eq!(all.auto_delete_queues, Some(false));
    assert_eq!(all.auto_delete_addresses, Some(false));
    assert_eq!(all.page_size_bytes, Some(10 * 1024 * 1024));
    assert_eq!(all.max_size_messages, Some(-1));
    assert_eq!(all.max_read_page_messages, Some(-1));
    assert_eq!(all.max_read_page_bytes, Some(20 * 1024 * 1024));
    assert_eq!(all.page_limit_bytes, Some(-1));
    assert_eq!(all.page_limit_messages, Some(-1));
}

#[test]
fn address_settings_resolution() {
    let c = golden();
    let repo = c.address_settings_repository().unwrap();

    let orders = repo.get_match("orders").unwrap();
    assert_eq!(orders.dead_letter_address.as_deref(), Some("DLQ"));
    assert!(!orders.auto_delete_queues());
    assert_eq!(orders.max_read_page_bytes(), 20 * 1024 * 1024);
    assert_eq!(orders.max_delivery_attempts(), 10);

    // The management match inherits what it does not set from `#`.
    let mgmt = repo.get_match("activemq.management.abc").unwrap();
    assert_eq!(mgmt.max_size_bytes, Some(-1));
    assert_eq!(mgmt.auto_delete_queues, Some(false));
    assert_eq!(mgmt.page_size_bytes, Some(10 * 1024 * 1024));

    // A fully qualified name resolves against its address.
    let fqqn = repo.get_match("activemq.management.abc::q").unwrap();
    assert_eq!(fqqn, mgmt);
}

#[test]
fn addresses() {
    let c = golden();
    assert_eq!(c.addresses.len(), 2);
    for (address, name) in c.addresses.iter().zip(["DLQ", "ExpiryQueue"]) {
        assert_eq!(address.name, name);
        assert_eq!(address.routing_types, [RoutingType::Anycast]);
        assert_eq!(address.queues.len(), 1);
        let q = &address.queues[0];
        assert_eq!(q.name, name);
        assert_eq!(q.address, name);
        assert_eq!(q.routing_type, RoutingType::Anycast);
        assert!(q.durable);
        assert_eq!(q.filter, None);
        assert_eq!(q.max_consumers, None);
        assert!(!q.purge_on_no_consumers);
        assert_eq!(q.ring_size, -1);
    }
}

#[test]
fn no_warnings() {
    let warnings: Vec<Warning> = golden().warnings;
    assert!(warnings.is_empty(), "{warnings:?}");
}
