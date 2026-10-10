use saranga_buffer::{DecodeError, Reader, Result, Writer};

#[allow(unused_imports)]
use crate::packets::cluster::*;
use crate::packets::connection::*;
#[allow(unused_imports)]
use crate::packets::large::*;
use crate::packets::message::*;
#[allow(unused_imports)]
use crate::packets::replication::*;
#[allow(unused_imports)]
use crate::packets::session::*;
#[allow(unused_imports)]
use crate::packets::xa::*;
use crate::{Context, Direction, PacketCodec, RawFrame, encode_frame, types};

/// Declares the [`Packet`] enum: one variant per packet struct, plus
/// `Unknown` for types not ported yet (kept as raw bytes so they still
/// round-trip).
macro_rules! packet_enum {
    ($($variant:ident),* $(,)?) => {
        /// A decoded Core packet body.
        #[derive(Debug, Clone, PartialEq)]
        pub enum Packet {
            $( $variant($variant), )*
            Unknown { packet_type: i8, body: Vec<u8> },
        }

        impl Packet {
            pub fn packet_type(&self) -> i8 {
                match self {
                    $( Self::$variant(_) => <$variant as PacketCodec>::TYPE, )*
                    Self::Unknown { packet_type, .. } => *packet_type,
                }
            }

            /// Encodes the body (everything after the channel ID).
            pub fn encode_body(&self, w: &mut Writer, ctx: &Context) {
                match self {
                    $( Self::$variant(p) => p.encode(w, ctx), )*
                    Self::Unknown { body, .. } => { w.bytes(body); }
                }
            }

            /// The packet's struct name, for diagnostics.
            pub fn name(&self) -> &'static str {
                match self {
                    $( Self::$variant(_) => stringify!($variant), )*
                    Self::Unknown { .. } => "Unknown",
                }
            }
        }

        $(
            impl From<$variant> for Packet {
                fn from(p: $variant) -> Self {
                    Self::$variant(p)
                }
            }
        )*
    };
}

packet_enum! {
    Ping,
    DisconnectMessage,
    DisconnectMessageV2,
    DisconnectMessageV3,
    ActiveMQExceptionMessage,
    ActiveMQExceptionMessageV2,
    NullResponseMessage,
    NullResponseMessageV2,
    PacketsConfirmedMessage,
    CreateSessionMessage,
    CreateSessionMessageV2,
    CreateSessionResponseMessage,
    SessionStart,
    SessionStop,
    SessionXASuspend,
    SessionXAInDoubtXids,
    SessionXAGetTimeout,
    SessionSendMessage,
    SessionSendMessageV2,
    SessionSendMessageV3,
    SessionReceiveMessage,
    // ---- session ----
    CreateAddressMessage,
    CreateProducerMessage,
    RemoveProducerMessage,
    CreateQueueMessage,
    CreateQueueMessageV2,
    CreateSharedQueueMessage,
    CreateSharedQueueMessageV2,
    SessionDeleteQueueMessage,
    SessionQueueQueryMessage,
    SessionQueueQueryResponseMessage,
    SessionQueueQueryResponseMessageV2,
    SessionQueueQueryResponseMessageV3,
    SessionBindingQueryMessage,
    SessionBindingQueryResponseMessage,
    SessionBindingQueryResponseMessageV2,
    SessionBindingQueryResponseMessageV3,
    SessionBindingQueryResponseMessageV4,
    SessionBindingQueryResponseMessageV5,
    SessionCreateConsumerMessage,
    SessionConsumerCloseMessage,
    SessionConsumerFlowCreditMessage,
    SessionForceConsumerDelivery,
    DisconnectConsumerMessage,
    DisconnectConsumerWithKillMessage,
    SessionAcknowledgeMessage,
    SessionIndividualAcknowledgeMessage,
    SessionExpireMessage,
    SessionCommitMessage,
    SessionCommitMessageV2,
    RollbackMessage,
    SessionCloseMessage,
    ReattachSessionMessage,
    ReattachSessionResponseMessage,
    SessionAddMetaDataMessage,
    SessionAddMetaDataMessageV2,
    SessionUniqueAddMetaDataMessage,
    SessionRequestProducerCreditsMessage,
    SessionProducerCreditsMessage,
    SessionProducerCreditsFailMessage,
    SessionSendMessage1X,
    SessionReceiveMessage1X,
    // ---- end session ----
    // ---- xa ----
    // ---- end xa ----
    // ---- large ----
    // ---- end large ----
    // ---- cluster ----
    // ---- end cluster ----
    // ---- replication ----
    // ---- end replication ----
}

fn body<P: PacketCodec + Into<Packet>>(r: &mut Reader<'_>, ctx: &Context) -> Result<Packet> {
    P::decode(r, ctx).map(Into::into)
}

impl Packet {
    /// Decodes a frame's body. The whole body must be consumed.
    pub fn decode(frame: &RawFrame<'_>, ctx: &Context) -> Result<Self> {
        let mut r = Reader::new(frame.body);
        let packet = Self::decode_body(frame.packet_type, &mut r, ctx)?;
        if !r.is_empty() {
            return Err(DecodeError::Invalid(format!(
                "{} trailing bytes after {} (type {})",
                r.remaining(),
                packet.name(),
                frame.packet_type
            )));
        }
        Ok(packet)
    }

    /// Picks the struct for a type byte the way `PacketDecoder`,
    /// `ServerPacketDecoder` and `ClientPacketDecoder` do.
    fn decode_body(ty: i8, r: &mut Reader<'_>, ctx: &Context) -> Result<Self> {
        let async_v1 = ctx.is_version_before_async_response_change();
        match ty {
            types::PING => body::<Ping>(r, ctx),
            types::DISCONNECT => body::<DisconnectMessage>(r, ctx),
            types::DISCONNECT_V2 => body::<DisconnectMessageV2>(r, ctx),
            types::DISCONNECT_V3 => body::<DisconnectMessageV3>(r, ctx),
            types::EXCEPTION if async_v1 => body::<ActiveMQExceptionMessage>(r, ctx),
            types::EXCEPTION => body::<ActiveMQExceptionMessageV2>(r, ctx),
            types::NULL_RESPONSE if async_v1 => body::<NullResponseMessage>(r, ctx),
            types::NULL_RESPONSE => body::<NullResponseMessageV2>(r, ctx),
            types::PACKETS_CONFIRMED => body::<PacketsConfirmedMessage>(r, ctx),
            types::CREATESESSION => body::<CreateSessionMessage>(r, ctx),
            types::CREATESESSION_V2 => body::<CreateSessionMessageV2>(r, ctx),
            types::CREATESESSION_RESP => body::<CreateSessionResponseMessage>(r, ctx),
            types::SESS_START => body::<SessionStart>(r, ctx),
            types::SESS_STOP => body::<SessionStop>(r, ctx),
            types::SESS_XA_SUSPEND => body::<SessionXASuspend>(r, ctx),
            types::SESS_XA_INDOUBT_XIDS => body::<SessionXAInDoubtXids>(r, ctx),
            types::SESS_XA_GET_TIMEOUT => body::<SessionXAGetTimeout>(r, ctx),
            types::SESS_SEND if ctx.direction == Direction::ToServer => {
                if ctx.is_version_before_address_change() {
                    body::<SessionSendMessage1X>(r, ctx)
                } else if async_v1 {
                    body::<SessionSendMessage>(r, ctx)
                } else if ctx.is_before_producer_metrics_changed() {
                    body::<SessionSendMessageV2>(r, ctx)
                } else {
                    body::<SessionSendMessageV3>(r, ctx)
                }
            }
            types::SESS_RECEIVE_MSG if ctx.direction == Direction::ToClient => {
                if ctx.is_version_before_address_change() {
                    body::<SessionReceiveMessage1X>(r, ctx)
                } else {
                    body::<SessionReceiveMessage>(r, ctx)
                }
            }
            // ---- session ----
            types::CREATE_ADDRESS => body::<CreateAddressMessage>(r, ctx),
            types::CREATE_PRODUCER if ctx.direction == Direction::ToServer => {
                body::<CreateProducerMessage>(r, ctx)
            }
            types::REMOVE_PRODUCER if ctx.direction == Direction::ToServer => {
                body::<RemoveProducerMessage>(r, ctx)
            }
            types::CREATE_QUEUE => body::<CreateQueueMessage>(r, ctx),
            types::CREATE_QUEUE_V2 => body::<CreateQueueMessageV2>(r, ctx),
            types::CREATE_SHARED_QUEUE => body::<CreateSharedQueueMessage>(r, ctx),
            types::CREATE_SHARED_QUEUE_V2 => body::<CreateSharedQueueMessageV2>(r, ctx),
            types::DELETE_QUEUE => body::<SessionDeleteQueueMessage>(r, ctx),
            types::SESS_QUEUEQUERY => body::<SessionQueueQueryMessage>(r, ctx),
            types::SESS_QUEUEQUERY_RESP => body::<SessionQueueQueryResponseMessage>(r, ctx),
            types::SESS_QUEUEQUERY_RESP_V2 => body::<SessionQueueQueryResponseMessageV2>(r, ctx),
            types::SESS_QUEUEQUERY_RESP_V3 => body::<SessionQueueQueryResponseMessageV3>(r, ctx),
            types::SESS_BINDINGQUERY => body::<SessionBindingQueryMessage>(r, ctx),
            types::SESS_BINDINGQUERY_RESP => body::<SessionBindingQueryResponseMessage>(r, ctx),
            types::SESS_BINDINGQUERY_RESP_V2 => {
                body::<SessionBindingQueryResponseMessageV2>(r, ctx)
            }
            types::SESS_BINDINGQUERY_RESP_V3 => {
                body::<SessionBindingQueryResponseMessageV3>(r, ctx)
            }
            types::SESS_BINDINGQUERY_RESP_V4 => {
                body::<SessionBindingQueryResponseMessageV4>(r, ctx)
            }
            types::SESS_BINDINGQUERY_RESP_V5 => {
                body::<SessionBindingQueryResponseMessageV5>(r, ctx)
            }
            types::SESS_CREATECONSUMER => body::<SessionCreateConsumerMessage>(r, ctx),
            types::SESS_CONSUMER_CLOSE => body::<SessionConsumerCloseMessage>(r, ctx),
            types::SESS_FLOWTOKEN => body::<SessionConsumerFlowCreditMessage>(r, ctx),
            types::SESS_FORCE_CONSUMER_DELIVERY => body::<SessionForceConsumerDelivery>(r, ctx),
            types::DISCONNECT_CONSUMER => body::<DisconnectConsumerMessage>(r, ctx),
            types::DISCONNECT_CONSUMER_KILL => body::<DisconnectConsumerWithKillMessage>(r, ctx),
            types::SESS_ACKNOWLEDGE => body::<SessionAcknowledgeMessage>(r, ctx),
            types::SESS_INDIVIDUAL_ACKNOWLEDGE => {
                body::<SessionIndividualAcknowledgeMessage>(r, ctx)
            }
            types::SESS_EXPIRED => body::<SessionExpireMessage>(r, ctx),
            types::SESS_COMMIT if !ctx.is_version_support_commit_v2() => {
                body::<SessionCommitMessage>(r, ctx)
            }
            types::SESS_COMMIT => body::<SessionCommitMessageV2>(r, ctx),
            types::SESS_ROLLBACK => body::<RollbackMessage>(r, ctx),
            types::SESS_CLOSE => body::<SessionCloseMessage>(r, ctx),
            types::REATTACH_SESSION => body::<ReattachSessionMessage>(r, ctx),
            types::REATTACH_SESSION_RESP => body::<ReattachSessionResponseMessage>(r, ctx),
            types::SESS_ADD_METADATA => body::<SessionAddMetaDataMessage>(r, ctx),
            types::SESS_ADD_METADATA2 => body::<SessionAddMetaDataMessageV2>(r, ctx),
            types::SESS_UNIQUE_ADD_METADATA => body::<SessionUniqueAddMetaDataMessage>(r, ctx),
            types::SESS_PRODUCER_REQUEST_CREDITS => {
                body::<SessionRequestProducerCreditsMessage>(r, ctx)
            }
            types::SESS_PRODUCER_CREDITS => body::<SessionProducerCreditsMessage>(r, ctx),
            types::SESS_PRODUCER_FAIL_CREDITS => body::<SessionProducerCreditsFailMessage>(r, ctx),
            // ---- end session ----
            // ---- xa ----
            // ---- end xa ----
            // ---- large ----
            // ---- end large ----
            // ---- cluster ----
            // ---- end cluster ----
            // ---- replication ----
            // ---- end replication ----
            _ => Ok(Self::unknown(ty, r)),
        }
    }

    fn unknown(packet_type: i8, r: &mut Reader<'_>) -> Self {
        let body = r.rest().to_vec();
        let _ = r.bytes(body.len());
        Self::Unknown { packet_type, body }
    }

    /// Encodes a complete frame.
    pub fn encode_frame(&self, channel_id: i64, ctx: &Context) -> Vec<u8> {
        encode_frame(self.packet_type(), channel_id, |w| self.encode_body(w, ctx))
    }
}
