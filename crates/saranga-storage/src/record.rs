//! [`StorageRecord`]: any user record body, decoded by its user record type.

use saranga_buffer::{Result, Writer};

use crate::codec::*;
use crate::ids;
use crate::message::{LargeMessageRecord, PersistedMessage};

/// Which journal a record was read from.
///
/// Artemis's record type IDs are global, so decoding does not depend on it;
/// it is kept so callers state where a record came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JournalKind {
    /// `activemq-bindings-N.bindings`.
    Bindings,
    /// `activemq-data-N.amq`.
    Messages,
}

/// A decoded user record body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageRecord {
    Group(GroupingEncoding),
    QueueBinding(PersistentQueueBindingEncoding),
    QueueStatus(QueueStatusEncoding),
    IdCounter(IdCounterEncoding),
    AddressSetting(PersistedAddressSetting),
    SecuritySetting(PersistedSecuritySetting),
    Divert(PersistedDivertConfiguration),
    Bridge(PersistedBridgeConfiguration),
    AddLargeMessagePending(PendingLargeMessageEncoding),
    AddLargeMessage(LargeMessageRecord),
    /// The pre-2.0 message record, kept as raw bytes.
    AddMessage(RawBody),
    AddRef(RefEncoding),
    AcknowledgeRef(RefEncoding),
    UpdateDeliveryCount(DeliveryCountUpdateEncoding),
    PageTransaction(PageTransactionEncoding),
    ScheduledDeliveryTime(ScheduledDeliveryEncoding),
    DuplicateId(DuplicateIdEncoding),
    HeuristicCompletion(HeuristicCompletionEncoding),
    AcknowledgeCursor(CursorAckRecordEncoding),
    PageCursorCounterValue(PageCountRecord),
    PageCursorCounterInc(PageCountRecordInc),
    PageCursorComplete(CursorAckRecordEncoding),
    PageCursorPendingCounter(PageCountPendingEncoding),
    AddressBinding(PersistentAddressBindingEncoding),
    AddMessageProtocol(PersistedMessage),
    AddressStatus(AddressStatusEncoding),
    User(PersistedUser),
    Role(PersistedRole),
    AddMessageBody(RawBody),
    KeyValuePair(PersistedKeyValuePair),
    Connector(PersistedConnector),
    AddressSettingJson(PersistedAddressSettingJson),
    AckRetry(AckRetryRecord),
    MqttPacketIdCorrelation(MqttPacketIdCorrelation),
    /// A user record type Artemis 2.57.0 does not define.
    Unknown {
        user_type: u8,
        data: Vec<u8>,
    },
}

macro_rules! dispatch {
    ($($id:path => $variant:ident($ty:ty)),* $(,)?) => {
        /// Decodes a user record body. Unknown record types decode to
        /// [`StorageRecord::Unknown`]; a known type with a malformed body is
        /// an error.
        pub fn decode_record(
            journal: JournalKind,
            user_type: u8,
            data: &[u8],
        ) -> Result<StorageRecord> {
            let _ = journal;
            Ok(match user_type {
                $($id => StorageRecord::$variant(<$ty>::decode(data)?),)*
                _ => StorageRecord::Unknown {
                    user_type,
                    data: data.to_vec(),
                },
            })
        }

        impl StorageRecord {
            /// The user record type this record is written with.
            pub fn user_type(&self) -> u8 {
                match self {
                    $(StorageRecord::$variant(_) => $id,)*
                    StorageRecord::Unknown { user_type, .. } => *user_type,
                }
            }

            /// Encodes the record body.
            pub fn encode(&self) -> Vec<u8> {
                match self {
                    $(StorageRecord::$variant(v) => v.encode(),)*
                    StorageRecord::Unknown { data, .. } => data.clone(),
                }
            }

            /// Appends the record body to `w`.
            pub fn write(&self, w: &mut Writer) {
                match self {
                    $(StorageRecord::$variant(v) => v.write(w),)*
                    StorageRecord::Unknown { data, .. } => {
                        w.bytes(data);
                    }
                }
            }
        }
    };
}

dispatch! {
    ids::GROUP_RECORD => Group(GroupingEncoding),
    ids::QUEUE_BINDING_RECORD => QueueBinding(PersistentQueueBindingEncoding),
    ids::QUEUE_STATUS_RECORD => QueueStatus(QueueStatusEncoding),
    ids::ID_COUNTER_RECORD => IdCounter(IdCounterEncoding),
    ids::ADDRESS_SETTING_RECORD => AddressSetting(PersistedAddressSetting),
    ids::SECURITY_SETTING_RECORD => SecuritySetting(PersistedSecuritySetting),
    ids::DIVERT_RECORD => Divert(PersistedDivertConfiguration),
    ids::BRIDGE_RECORD => Bridge(PersistedBridgeConfiguration),
    ids::ADD_LARGE_MESSAGE_PENDING => AddLargeMessagePending(PendingLargeMessageEncoding),
    ids::ADD_LARGE_MESSAGE => AddLargeMessage(LargeMessageRecord),
    ids::ADD_MESSAGE => AddMessage(RawBody),
    ids::ADD_REF => AddRef(RefEncoding),
    ids::ACKNOWLEDGE_REF => AcknowledgeRef(RefEncoding),
    ids::UPDATE_DELIVERY_COUNT => UpdateDeliveryCount(DeliveryCountUpdateEncoding),
    ids::PAGE_TRANSACTION => PageTransaction(PageTransactionEncoding),
    ids::SET_SCHEDULED_DELIVERY_TIME => ScheduledDeliveryTime(ScheduledDeliveryEncoding),
    ids::DUPLICATE_ID => DuplicateId(DuplicateIdEncoding),
    ids::HEURISTIC_COMPLETION => HeuristicCompletion(HeuristicCompletionEncoding),
    ids::ACKNOWLEDGE_CURSOR => AcknowledgeCursor(CursorAckRecordEncoding),
    ids::PAGE_CURSOR_COUNTER_VALUE => PageCursorCounterValue(PageCountRecord),
    ids::PAGE_CURSOR_COUNTER_INC => PageCursorCounterInc(PageCountRecordInc),
    ids::PAGE_CURSOR_COMPLETE => PageCursorComplete(CursorAckRecordEncoding),
    ids::PAGE_CURSOR_PENDING_COUNTER => PageCursorPendingCounter(PageCountPendingEncoding),
    ids::ADDRESS_BINDING_RECORD => AddressBinding(PersistentAddressBindingEncoding),
    ids::ADD_MESSAGE_PROTOCOL => AddMessageProtocol(PersistedMessage),
    ids::ADDRESS_STATUS_RECORD => AddressStatus(AddressStatusEncoding),
    ids::USER_RECORD => User(PersistedUser),
    ids::ROLE_RECORD => Role(PersistedRole),
    ids::ADD_MESSAGE_BODY => AddMessageBody(RawBody),
    ids::KEY_VALUE_PAIR_RECORD => KeyValuePair(PersistedKeyValuePair),
    ids::CONNECTOR_RECORD => Connector(PersistedConnector),
    ids::ADDRESS_SETTING_RECORD_JSON => AddressSettingJson(PersistedAddressSettingJson),
    ids::ACK_RETRY => AckRetry(AckRetryRecord),
    ids::MQTT_PACKET_ID_CORRELATION => MqttPacketIdCorrelation(MqttPacketIdCorrelation),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_defined_type_dispatches() {
        for (id, name) in ids::ALL {
            // Most bodies fail to decode from nothing; none may be Unknown.
            if let Ok(rec) = decode_record(JournalKind::Bindings, id, &[]) {
                assert!(!matches!(rec, StorageRecord::Unknown { .. }), "{name}");
                assert_eq!(rec.user_type(), id, "{name}");
            }
        }
        let rec = decode_record(JournalKind::Messages, ids::ADD_REF, &5i64.to_be_bytes()).unwrap();
        assert_eq!(rec, StorageRecord::AddRef(RefEncoding { queue_id: 5 }));
        assert_eq!(rec.encode(), 5i64.to_be_bytes());
        let unknown = decode_record(JournalKind::Messages, 99, &[1, 2]).unwrap();
        assert_eq!(unknown.user_type(), 99);
        assert_eq!(unknown.encode(), [1, 2]);
    }
}
