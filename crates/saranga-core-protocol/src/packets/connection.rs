//! Connection-level packets: ping, disconnect, errors, null responses,
//! confirmations and session creation.

use saranga_buffer::{JString, Reader, Result, SimpleString, Writer};

use crate::transport::TransportConfiguration;
use crate::{Context, Flagged, PacketCodec, types};

wire_packet! {
    /// `Ping`
    Ping = types::PING => {
        pub connection_ttl: i64,
    }
}

wire_packet! {
    /// `DisconnectMessage`
    DisconnectMessage = types::DISCONNECT => {
        pub node_id: Option<SimpleString>,
    }
}

wire_packet! {
    /// `DisconnectMessage_V2`
    DisconnectMessageV2 = types::DISCONNECT_V2 => {
        pub node_id: Option<SimpleString>,
        pub scale_down_node_id: Option<SimpleString>,
    }
}

wire_packet! {
    /// `DisconnectMessage_V3`. `reason` is the `DisconnectReason` type byte,
    /// -1 for none.
    DisconnectMessageV3 = types::DISCONNECT_V3 => {
        pub node_id: Option<SimpleString>,
        pub reason: i8,
        pub target_node_id: Option<SimpleString>,
        pub target_connector: Flagged<TransportConfiguration>,
    }
}

wire_packet! {
    /// `ActiveMQExceptionMessage`. `code` is the `ActiveMQExceptionType` code.
    ActiveMQExceptionMessage = types::EXCEPTION => {
        pub code: i32,
        pub message: Option<JString>,
    }
}

/// Reads a trailing i64 only if one is present, as the `_V2` response packets do.
fn read_trailing_i64(r: &mut Reader<'_>) -> Result<Option<i64>> {
    if r.remaining() >= 8 {
        r.i64().map(Some)
    } else {
        Ok(None)
    }
}

/// `ActiveMQExceptionMessage_V2`. The correlation ID is read only if present.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ActiveMQExceptionMessageV2 {
    pub code: i32,
    pub message: Option<JString>,
    pub correlation_id: Option<i64>,
}

impl PacketCodec for ActiveMQExceptionMessageV2 {
    const TYPE: i8 = types::EXCEPTION;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            code: r.i32()?,
            message: r.nullable_string()?,
            correlation_id: read_trailing_i64(r)?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.i32(self.code).nullable_string(self.message.as_ref());
        if let Some(id) = self.correlation_id {
            w.i64(id);
        }
    }
}

wire_packet! {
    /// `NullResponseMessage`
    NullResponseMessage = types::NULL_RESPONSE => {}
}

/// `NullResponseMessage_V2`. The correlation ID is read only if present
/// (Java then uses -1).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NullResponseMessageV2 {
    pub correlation_id: Option<i64>,
}

impl PacketCodec for NullResponseMessageV2 {
    const TYPE: i8 = types::NULL_RESPONSE;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            correlation_id: read_trailing_i64(r)?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        if let Some(id) = self.correlation_id {
            w.i64(id);
        }
    }
}

wire_packet! {
    /// `PacketsConfirmedMessage`
    PacketsConfirmedMessage = types::PACKETS_CONFIRMED => {
        pub command_id: i32,
    }
}

wire_packet! {
    /// `CreateSessionMessage`
    CreateSessionMessage = types::CREATESESSION => {
        pub name: JString,
        pub session_channel_id: i64,
        pub version: i32,
        pub username: Option<JString>,
        pub password: Option<JString>,
        pub min_large_message_size: i32,
        pub xa: bool,
        pub auto_commit_sends: bool,
        pub auto_commit_acks: bool,
        pub window_size: i32,
        pub pre_acknowledge: bool,
        pub default_address: Option<JString>,
    }
}

wire_packet! {
    /// `CreateSessionMessage_V2`: adds the client ID.
    CreateSessionMessageV2 = types::CREATESESSION_V2 => {
        pub name: JString,
        pub session_channel_id: i64,
        pub version: i32,
        pub username: Option<JString>,
        pub password: Option<JString>,
        pub min_large_message_size: i32,
        pub xa: bool,
        pub auto_commit_sends: bool,
        pub auto_commit_acks: bool,
        pub window_size: i32,
        pub pre_acknowledge: bool,
        pub default_address: Option<JString>,
        pub client_id: Option<JString>,
    }
}

wire_packet! {
    /// `CreateSessionResponseMessage`
    CreateSessionResponseMessage = types::CREATESESSION_RESP => {
        pub server_version: i32,
    }
}

wire_packet! {
    /// `PacketImpl(SESS_START)`
    SessionStart = types::SESS_START => {}
}

wire_packet! {
    /// `PacketImpl(SESS_STOP)`
    SessionStop = types::SESS_STOP => {}
}

wire_packet! {
    /// `PacketImpl(SESS_XA_SUSPEND)`
    SessionXASuspend = types::SESS_XA_SUSPEND => {}
}

wire_packet! {
    /// `PacketImpl(SESS_XA_INDOUBT_XIDS)`
    SessionXAInDoubtXids = types::SESS_XA_INDOUBT_XIDS => {}
}

wire_packet! {
    /// `PacketImpl(SESS_XA_GET_TIMEOUT)`
    SessionXAGetTimeout = types::SESS_XA_GET_TIMEOUT => {}
}
