//! Journal user record types (`JournalRecordIds`) and the message persister
//! IDs (`PersisterIDs`).
//!
//! Record type IDs are global: the bindings journal and the message journal
//! never use the same ID for different things.

// Bindings journal.

/// `GROUP_RECORD`: a grouping-handler binding ([`crate::codec::GroupingEncoding`]).
pub const GROUP_RECORD: u8 = 20;
/// `QUEUE_BINDING_RECORD` ([`crate::codec::PersistentQueueBindingEncoding`]).
pub const QUEUE_BINDING_RECORD: u8 = 21;
/// `QUEUE_STATUS_RECORD` ([`crate::codec::QueueStatusEncoding`]).
pub const QUEUE_STATUS_RECORD: u8 = 22;
/// `ID_COUNTER_RECORD`: the `BatchingIDGenerator` checkpoint
/// ([`crate::codec::IdCounterEncoding`]).
pub const ID_COUNTER_RECORD: u8 = 24;
/// `ADDRESS_SETTING_RECORD`: the pre-JSON binary address settings
/// ([`crate::codec::PersistedAddressSetting`]). Only read, never written.
pub const ADDRESS_SETTING_RECORD: u8 = 25;
/// `SECURITY_SETTING_RECORD` ([`crate::codec::PersistedSecuritySetting`]).
pub const SECURITY_SETTING_RECORD: u8 = 26;
/// `DIVERT_RECORD` ([`crate::codec::PersistedDivertConfiguration`]).
pub const DIVERT_RECORD: u8 = 27;
/// `BRIDGE_RECORD` ([`crate::codec::PersistedBridgeConfiguration`]).
pub const BRIDGE_RECORD: u8 = 28;

// Message journal.

/// `ADD_LARGE_MESSAGE_PENDING`: no longer written; Artemis scans the page
/// files for pending large messages instead.
pub const ADD_LARGE_MESSAGE_PENDING: u8 = 29;
/// `ADD_LARGE_MESSAGE`: a Core large message's headers and properties.
pub const ADD_LARGE_MESSAGE: u8 = 30;
/// `ADD_MESSAGE`: the pre-2.0 message record. Artemis refuses to load it.
pub const ADD_MESSAGE: u8 = 31;
pub const ADD_REF: u8 = 32;
pub const ACKNOWLEDGE_REF: u8 = 33;
pub const UPDATE_DELIVERY_COUNT: u8 = 34;
pub const PAGE_TRANSACTION: u8 = 35;
pub const SET_SCHEDULED_DELIVERY_TIME: u8 = 36;
pub const DUPLICATE_ID: u8 = 37;
pub const HEURISTIC_COMPLETION: u8 = 38;
pub const ACKNOWLEDGE_CURSOR: u8 = 39;
pub const PAGE_CURSOR_COUNTER_VALUE: u8 = 40;
pub const PAGE_CURSOR_COUNTER_INC: u8 = 41;
pub const PAGE_CURSOR_COMPLETE: u8 = 42;
pub const PAGE_CURSOR_PENDING_COUNTER: u8 = 43;
/// `ADDRESS_BINDING_RECORD` (bindings journal).
pub const ADDRESS_BINDING_RECORD: u8 = 44;
/// `ADD_MESSAGE_PROTOCOL`: a message of any protocol, led by its persister ID.
pub const ADD_MESSAGE_PROTOCOL: u8 = 45;
/// `ADDRESS_STATUS_RECORD` (bindings journal).
pub const ADDRESS_STATUS_RECORD: u8 = 46;
/// `USER_RECORD` (bindings journal).
pub const USER_RECORD: u8 = 47;
/// `ROLE_RECORD` (bindings journal).
pub const ROLE_RECORD: u8 = 48;
/// `ADD_MESSAGE_BODY`: a large message's body chunk, recorded when journal
/// retention (history) is on.
pub const ADD_MESSAGE_BODY: u8 = 49;
/// `KEY_VALUE_PAIR_RECORD` (bindings journal).
pub const KEY_VALUE_PAIR_RECORD: u8 = 50;
/// `CONNECTOR_RECORD` (bindings journal).
pub const CONNECTOR_RECORD: u8 = 51;
/// `ADDRESS_SETTING_RECORD_JSON` (bindings journal).
pub const ADDRESS_SETTING_RECORD_JSON: u8 = 52;
/// `ACK_RETRY`: the AMQP mirror's pending acks (`AckRetry`).
pub const ACK_RETRY: u8 = 53;
/// `MQTT_PACKET_ID_CORRELATION` (`PacketIdCorrelationKey`).
pub const MQTT_PACKET_ID_CORRELATION: u8 = 54;

/// Every user record type Artemis 2.57.0 defines, with its Java name.
pub const ALL: [(u8, &str); 34] = [
    (GROUP_RECORD, "GROUP_RECORD"),
    (QUEUE_BINDING_RECORD, "QUEUE_BINDING_RECORD"),
    (QUEUE_STATUS_RECORD, "QUEUE_STATUS_RECORD"),
    (ID_COUNTER_RECORD, "ID_COUNTER_RECORD"),
    (ADDRESS_SETTING_RECORD, "ADDRESS_SETTING_RECORD"),
    (SECURITY_SETTING_RECORD, "SECURITY_SETTING_RECORD"),
    (DIVERT_RECORD, "DIVERT_RECORD"),
    (BRIDGE_RECORD, "BRIDGE_RECORD"),
    (ADD_LARGE_MESSAGE_PENDING, "ADD_LARGE_MESSAGE_PENDING"),
    (ADD_LARGE_MESSAGE, "ADD_LARGE_MESSAGE"),
    (ADD_MESSAGE, "ADD_MESSAGE"),
    (ADD_REF, "ADD_REF"),
    (ACKNOWLEDGE_REF, "ACKNOWLEDGE_REF"),
    (UPDATE_DELIVERY_COUNT, "UPDATE_DELIVERY_COUNT"),
    (PAGE_TRANSACTION, "PAGE_TRANSACTION"),
    (SET_SCHEDULED_DELIVERY_TIME, "SET_SCHEDULED_DELIVERY_TIME"),
    (DUPLICATE_ID, "DUPLICATE_ID"),
    (HEURISTIC_COMPLETION, "HEURISTIC_COMPLETION"),
    (ACKNOWLEDGE_CURSOR, "ACKNOWLEDGE_CURSOR"),
    (PAGE_CURSOR_COUNTER_VALUE, "PAGE_CURSOR_COUNTER_VALUE"),
    (PAGE_CURSOR_COUNTER_INC, "PAGE_CURSOR_COUNTER_INC"),
    (PAGE_CURSOR_COMPLETE, "PAGE_CURSOR_COMPLETE"),
    (PAGE_CURSOR_PENDING_COUNTER, "PAGE_CURSOR_PENDING_COUNTER"),
    (ADDRESS_BINDING_RECORD, "ADDRESS_BINDING_RECORD"),
    (ADD_MESSAGE_PROTOCOL, "ADD_MESSAGE_PROTOCOL"),
    (ADDRESS_STATUS_RECORD, "ADDRESS_STATUS_RECORD"),
    (USER_RECORD, "USER_RECORD"),
    (ROLE_RECORD, "ROLE_RECORD"),
    (ADD_MESSAGE_BODY, "ADD_MESSAGE_BODY"),
    (KEY_VALUE_PAIR_RECORD, "KEY_VALUE_PAIR_RECORD"),
    (CONNECTOR_RECORD, "CONNECTOR_RECORD"),
    (ADDRESS_SETTING_RECORD_JSON, "ADDRESS_SETTING_RECORD_JSON"),
    (ACK_RETRY, "ACK_RETRY"),
    (MQTT_PACKET_ID_CORRELATION, "MQTT_PACKET_ID_CORRELATION"),
];

/// The Java name of a user record type.
pub fn name(user_type: u8) -> Option<&'static str> {
    ALL.iter().find(|(t, _)| *t == user_type).map(|(_, n)| *n)
}

/// `PersisterIDs`: the first byte of an `ADD_MESSAGE_PROTOCOL` record says
/// which persister wrote the rest.
pub mod persister {
    /// `PersisterIDs.MAX_PERSISTERS`.
    pub const MAX_PERSISTERS: u8 = 5;
    /// `LargeMessagePersister`. It writes no ID byte: `ADD_LARGE_MESSAGE`
    /// records are always Core large messages.
    pub const CORE_LARGE_MESSAGE: u8 = 0;
    /// `CoreMessagePersister`.
    pub const CORE_MESSAGE: u8 = 1;
    /// `AMQPMessagePersister`.
    pub const AMQP_MESSAGE: u8 = 2;
    /// `AMQPMessagePersisterV2`: adds the extra properties.
    pub const AMQP_MESSAGE_V2: u8 = 3;
    /// `AMQPLargeMessagePersister`.
    pub const AMQP_LARGE_MESSAGE: u8 = 4;
    /// `AMQPMessagePersisterV3`: adds the expiration.
    pub const AMQP_MESSAGE_V3: u8 = 5;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_unique_and_named() {
        let mut seen = std::collections::HashSet::new();
        for (id, n) in ALL {
            assert!(seen.insert(id), "{n}");
            assert_eq!(name(id), Some(n));
        }
        assert_eq!(name(23), None);
    }
}
