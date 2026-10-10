//! Writes a data directory with one record of (almost) every user record
//! type, for comparing `saranga data print` with `artemis data print` on
//! records the golden corpus does not have. Shared by
//! `examples/every_record.rs` and `tests/data_print.rs`.
//!
//! AMQP messages and the binary address settings are left out: their bodies
//! here would not be valid for Java's decoders.

use saranga_buffer::{PropertyValue, SimpleString, TypedProperties};
use saranga_journal::{Journal, JournalConfig};
use saranga_message::CoreMessage;
use saranga_storage::codec::*;
use saranga_storage::ids;
use saranga_storage::message::{CoreMessageRecord, LargeMessageRecord, PersistedMessage};

/// Writes `<dir>/bindings` and `<dir>/journal`.
pub fn write(dir: &std::path::Path) {
    let bindings = Journal::open(JournalConfig::bindings(dir.join("bindings"))).unwrap();
    bindings.load_all().unwrap();
    let messages = Journal::open(JournalConfig::messages(dir.join("journal"))).unwrap();
    messages.load_all().unwrap();

    let mut id = 100;
    let mut next = || {
        id += 1;
        id
    };
    let add = |j: &Journal, id: i64, t: u8, data: Vec<u8>| {
        j.append_add(id, t, data, true, None).unwrap();
    };

    // Bindings journal.
    add(
        &bindings,
        1,
        ids::ID_COUNTER_RECORD,
        IdCounterEncoding { id: 1000 }.encode(),
    );
    add(
        &bindings,
        next(),
        ids::GROUP_RECORD,
        GroupingEncoding {
            group_id: "group-1".into(),
            cluster_name: "cluster".into(),
        }
        .encode(),
    );
    let queue_id = next();
    add(
        &bindings,
        queue_id,
        ids::QUEUE_BINDING_RECORD,
        PersistentQueueBindingEncoding {
            name: "addr::q".into(),
            address: "addr".into(),
            filter_string: Some("color = 'red'".into()),
            metadata: Some(PersistentQueueBindingEncoding::metadata_for(Some("bob"))),
            auto_created: false,
            consumers: Some((10, true, 1)),
            exclusive: Some(true),
            last_value: Some(false),
            configuration_managed: None,
            ..Default::default()
        }
        .encode(),
    );
    add(
        &bindings,
        next(),
        ids::QUEUE_STATUS_RECORD,
        QueueStatusEncoding {
            queue_id,
            status: 0,
        }
        .encode(),
    );
    let address_id = next();
    add(
        &bindings,
        address_id,
        ids::ADDRESS_BINDING_RECORD,
        PersistentAddressBindingEncoding {
            name: "addr".into(),
            routing_types: vec![1, 0],
            auto_created: true,
            internal: None,
        }
        .encode(),
    );
    add(
        &bindings,
        next(),
        ids::ADDRESS_STATUS_RECORD,
        AddressStatusEncoding {
            address_id,
            status: 1,
        }
        .encode(),
    );
    add(
        &bindings,
        next(),
        ids::SECURITY_SETTING_RECORD,
        PersistedSecuritySetting {
            address_match: "secure.#".into(),
            send_roles: Some("a,b".into()),
            consume_roles: Some("c".into()),
            view_roles: Some(Some("v".into())),
            edit_roles: Some(None),
            ..Default::default()
        }
        .encode(),
    );
    add(
        &bindings,
        next(),
        ids::DIVERT_RECORD,
        PersistedDivertConfiguration {
            name: Some("divert-1".into()),
            address: Some("from".into()),
            forwarding_address: Some("to".into()),
            routing_name: Some("route".into()),
            exclusive: true,
            filter_string: None,
            routing_type: 2,
            transformer: Some(TransformerConfiguration {
                class_name: "org.example.Transformer".into(),
                properties: vec![
                    (Some("zeta".into()), Some("1".into())),
                    (Some("alpha".into()), Some("2".into())),
                    (Some("key-number-three".into()), None),
                ],
            }),
        }
        .encode(),
    );
    add(
        &bindings,
        next(),
        ids::BRIDGE_RECORD,
        PersistedBridgeConfiguration {
            name: Some("bridge-1".into()),
            parent_name: Some("bridge-1".into()),
            queue_name: Some("q".into()),
            forwarding_address: Some("fwd".into()),
            ha: Some(false),
            retry_interval: Some(2000),
            retry_interval_multiplier: Some(1.5),
            initial_connect_attempts: Some(-1),
            reconnect_attempts: Some(-1),
            reconnect_attempts_on_same_node: Some(10),
            use_duplicate_detection: Some(true),
            confirmation_window_size: Some(1024 * 1024),
            producer_window_size: Some(-1),
            client_failure_check_period: Some(30000),
            user: Some("u".into()),
            password: Some("p".into()),
            connection_ttl: Some(60000),
            max_retry_interval: Some(2000),
            min_large_message_size: Some(102400),
            call_timeout: Some(30000),
            concurrency: Some(1),
            configuration_managed: Some(false),
            routing_type: 3,
            transformer: None,
            static_connectors: vec![Some("connector-1".into())],
            pending_ack_timeout: Some(Some(60000)),
            client_id: Some(None),
            ..Default::default()
        }
        .encode(),
    );
    add(
        &bindings,
        next(),
        ids::USER_RECORD,
        PersistedUser {
            username: "alice".into(),
            password: "secret".into(),
        }
        .encode(),
    );
    add(
        &bindings,
        next(),
        ids::ROLE_RECORD,
        PersistedRole {
            username: "alice".into(),
            roles: vec!["admin".into(), "users".into()],
        }
        .encode(),
    );
    add(
        &bindings,
        next(),
        ids::KEY_VALUE_PAIR_RECORD,
        PersistedKeyValuePair {
            map_id: "map".into(),
            key: "k".into(),
            value: "v".into(),
        }
        .encode(),
    );
    add(
        &bindings,
        next(),
        ids::CONNECTOR_RECORD,
        PersistedConnector {
            name: "c".into(),
            url: "tcp://localhost:61616".into(),
        }
        .encode(),
    );
    add(
        &bindings,
        next(),
        ids::ADDRESS_SETTING_RECORD_JSON,
        PersistedAddressSettingJson {
            address_match: "#".into(),
            json: "{\"max-delivery-attempts\":3}".into(),
        }
        .encode(),
    );

    // Message journal.
    let mut props = TypedProperties::new();
    props.insert("pChar".into(), PropertyValue::Char(0x41));
    props.insert("pBytes".into(), PropertyValue::Bytes(vec![1, 2, 3]));
    props.insert("pNull".into(), PropertyValue::Null);
    props.insert("pFloat".into(), PropertyValue::Float(1.0e-5));
    props.insert(
        "_AMQ_ROUTE_TO".into(),
        PropertyValue::Bytes(vec![0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 1, 0]),
    );
    let msg_id = next();
    let message = CoreMessage {
        message_id: msg_id,
        address: Some("addr".into()),
        user_id: Some([7; 16]),
        message_type: 3,
        durable: true,
        expiration: 4_102_444_800_000,
        timestamp: 1_700_000_000_123,
        priority: 9,
        properties: props,
        body: vec![1, 0, 0, 0, 2, 0x68, 0],
    };
    add(
        &messages,
        msg_id,
        ids::ADD_MESSAGE_PROTOCOL,
        PersistedMessage::Core(CoreMessageRecord::new(message.clone())).encode(),
    );
    let upd = |j: &Journal, id: i64, t: u8, data: Vec<u8>| {
        j.append_update(id, t, data, true, false, None).unwrap();
    };
    upd(
        &messages,
        msg_id,
        ids::ADD_REF,
        RefEncoding { queue_id }.encode(),
    );
    upd(
        &messages,
        msg_id,
        ids::UPDATE_DELIVERY_COUNT,
        DeliveryCountUpdateEncoding { queue_id, count: 3 }.encode(),
    );
    upd(
        &messages,
        msg_id,
        ids::SET_SCHEDULED_DELIVERY_TIME,
        ScheduledDeliveryEncoding {
            queue_id,
            scheduled_delivery_time: 4_102_444_800_000,
        }
        .encode(),
    );
    let large_id = next();
    let mut large = message.clone();
    large.message_id = large_id;
    large.body.clear();
    large.user_id = None;
    add(
        &messages,
        large_id,
        ids::ADD_LARGE_MESSAGE,
        LargeMessageRecord { message: large }.encode(),
    );
    add(
        &messages,
        next(),
        ids::ADD_LARGE_MESSAGE_PENDING,
        PendingLargeMessageEncoding {
            large_message_id: large_id,
        }
        .encode(),
    );
    let mut dup = vec![9u8; 16];
    dup.extend_from_slice(&77i64.to_be_bytes());
    add(
        &messages,
        next(),
        ids::DUPLICATE_ID,
        DuplicateIdEncoding {
            address: "BRIDGE.addr".into(),
            duplicate_id: dup,
        }
        .encode(),
    );
    add(
        &messages,
        next(),
        ids::HEURISTIC_COMPLETION,
        HeuristicCompletionEncoding {
            xid: Xid {
                format_id: -3,
                branch_qualifier: vec![0xff, 1],
                global_transaction_id: vec![2],
            },
            is_commit: false,
        }
        .encode(),
    );
    let page_tx = next();
    add(
        &messages,
        page_tx,
        ids::PAGE_TRANSACTION,
        PageTransactionEncoding {
            transaction_id: 55,
            records: 4,
        }
        .encode(),
    );
    upd(
        &messages,
        page_tx,
        ids::PAGE_TRANSACTION,
        PageTransactionEncoding {
            transaction_id: 55,
            records: 1,
        }
        .encode(),
    );
    add(
        &messages,
        next(),
        ids::ACKNOWLEDGE_CURSOR,
        CursorAckRecordEncoding {
            queue_id,
            page_nr: 3,
            message_nr: 4,
        }
        .encode(),
    );
    add(
        &messages,
        next(),
        ids::PAGE_CURSOR_COMPLETE,
        CursorAckRecordEncoding {
            queue_id,
            page_nr: 2,
            message_nr: -1,
        }
        .encode(),
    );
    let counter = next();
    add(
        &messages,
        counter,
        ids::PAGE_CURSOR_COUNTER_VALUE,
        PageCountRecord {
            queue_id,
            value: 10,
            persistent_size: Some(1000),
        }
        .encode(),
    );
    upd(
        &messages,
        counter,
        ids::PAGE_CURSOR_COUNTER_VALUE,
        PageCountRecord {
            queue_id,
            value: 12,
            persistent_size: None,
        }
        .encode(),
    );
    let inc = next();
    add(
        &messages,
        inc,
        ids::PAGE_CURSOR_COUNTER_INC,
        PageCountRecordInc {
            queue_id,
            value: -20,
            persistent_size: Some(5),
        }
        .encode(),
    );
    upd(
        &messages,
        inc,
        ids::PAGE_CURSOR_COUNTER_INC,
        PageCountRecordInc {
            queue_id: 999,
            value: 1,
            persistent_size: None,
        }
        .encode(),
    );
    add(
        &messages,
        next(),
        ids::PAGE_CURSOR_PENDING_COUNTER,
        PageCountPendingEncoding {
            queue_id,
            page_id: 6,
        }
        .encode(),
    );
    add(
        &messages,
        next(),
        ids::ACK_RETRY,
        AckRetryRecord {
            id: 5,
            version: 0,
            queue_id,
            node_id: b"node-a".to_vec(),
            message_id: msg_id,
            reason: 2,
        }
        .encode(),
    );
    add(
        &messages,
        next(),
        ids::MQTT_PACKET_ID_CORRELATION,
        MqttPacketIdCorrelation {
            id: 6,
            version: 0,
            client_id: "mqtt-client".into(),
            core_message_id: msg_id,
            address: SimpleString::from("addr"),
            packet_id: 12,
        }
        .encode(),
    );
    add(&messages, next(), ids::ADD_MESSAGE_BODY, vec![1, 2, 3]);
    add(&messages, next(), ids::ADD_MESSAGE, vec![1, 2, 3]);
    // A transaction that is never completed, one prepared and one with a
    // delete.
    let tx = next();
    messages
        .append_add_tx(tx, next(), ids::ADD_REF, RefEncoding { queue_id }.encode())
        .unwrap();
    let ptx = next();
    messages
        .append_update_tx(
            ptx,
            msg_id,
            ids::ACKNOWLEDGE_REF,
            RefEncoding { queue_id }.encode(),
        )
        .unwrap();
    messages.append_delete_tx(ptx, page_tx, vec![]).unwrap();
    messages
        .append_prepare(
            ptx,
            Xid {
                format_id: 1,
                branch_qualifier: b"b".to_vec(),
                global_transaction_id: b"g".to_vec(),
            }
            .encode(),
            true,
            None,
        )
        .unwrap();
    bindings.stop().unwrap();
    messages.stop().unwrap();
}
