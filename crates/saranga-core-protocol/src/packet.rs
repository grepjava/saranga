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
    // ---- end session ----
    // ---- xa ----
    // ---- end xa ----
    // ---- large ----
    // ---- end large ----
    // ---- cluster ----
    SubscribeClusterTopologyUpdatesMessage,
    SubscribeClusterTopologyUpdatesMessageV2,
    ClusterTopologyChangeMessage,
    ClusterTopologyChangeMessageV2,
    ClusterTopologyChangeMessageV3,
    ClusterTopologyChangeMessageV4,
    NodeAnnounceMessage,
    ClusterConnectMessage,
    ClusterConnectReplyMessage,
    BackupRegistrationMessage,
    BackupReplicationStartFailedMessage,
    BackupRequestMessage,
    BackupResponseMessage,
    QuorumVoteMessage,
    QuorumVoteReplyMessage,
    ScaleDownAnnounceMessage,
    ConnectMessage,
    ConnectResponseMessage,
    FederationDownstreamConnectMessage,
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
                    Ok(Self::unknown(ty, r)) // SessionSendMessage_1X: not ported yet
                } else if async_v1 {
                    body::<SessionSendMessage>(r, ctx)
                } else if ctx.is_before_producer_metrics_changed() {
                    body::<SessionSendMessageV2>(r, ctx)
                } else {
                    body::<SessionSendMessageV3>(r, ctx)
                }
            }
            types::SESS_RECEIVE_MSG
                if ctx.direction == Direction::ToClient
                    && !ctx.is_version_before_address_change() =>
            {
                body::<SessionReceiveMessage>(r, ctx)
            }
            // ---- session ----
            // ---- end session ----
            // ---- xa ----
            // ---- end xa ----
            // ---- large ----
            // ---- end large ----
            // ---- cluster ----
            // PacketDecoder (both sides):
            types::CLUSTER_TOPOLOGY => body::<ClusterTopologyChangeMessage>(r, ctx),
            types::CLUSTER_TOPOLOGY_V2 => body::<ClusterTopologyChangeMessageV2>(r, ctx),
            types::CLUSTER_TOPOLOGY_V3 => body::<ClusterTopologyChangeMessageV3>(r, ctx),
            types::CLUSTER_TOPOLOGY_V4 => body::<ClusterTopologyChangeMessageV4>(r, ctx),
            types::SUBSCRIBE_TOPOLOGY => body::<SubscribeClusterTopologyUpdatesMessage>(r, ctx),
            types::SUBSCRIBE_TOPOLOGY_V2 => {
                body::<SubscribeClusterTopologyUpdatesMessageV2>(r, ctx)
            }
            types::CONNECT => body::<ConnectMessage>(r, ctx),
            types::CONNECT_RESPONSE => body::<ConnectResponseMessage>(r, ctx),
            types::FEDERATION_DOWNSTREAM_CONNECT => {
                body::<FederationDownstreamConnectMessage>(r, ctx)
            }
            // ServerPacketDecoder only. Not guarded on direction: the
            // connecting side of cluster, backup and quorum connections is
            // itself a broker that decodes with ServerPacketDecoder
            // (ActiveMQServerSideProtocolManagerFactory), and no client
            // decoder maps these type bytes to anything else.
            types::BACKUP_REGISTRATION => body::<BackupRegistrationMessage>(r, ctx),
            types::BACKUP_REGISTRATION_FAILED => {
                body::<BackupReplicationStartFailedMessage>(r, ctx)
            }
            types::CLUSTER_CONNECT => body::<ClusterConnectMessage>(r, ctx),
            types::CLUSTER_CONNECT_REPLY => body::<ClusterConnectReplyMessage>(r, ctx),
            types::NODE_ANNOUNCE => body::<NodeAnnounceMessage>(r, ctx),
            types::BACKUP_REQUEST => body::<BackupRequestMessage>(r, ctx),
            types::BACKUP_REQUEST_RESPONSE => body::<BackupResponseMessage>(r, ctx),
            types::QUORUM_VOTE => body::<QuorumVoteMessage>(r, ctx),
            types::QUORUM_VOTE_REPLY => body::<QuorumVoteReplyMessage>(r, ctx),
            types::SCALEDOWN_ANNOUNCEMENT => body::<ScaleDownAnnounceMessage>(r, ctx),
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
