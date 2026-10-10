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
    SessionXAAfterFailedMessage,
    SessionXACommitMessage,
    SessionXAEndMessage,
    SessionXAForgetMessage,
    SessionXAGetInDoubtXidsResponseMessage,
    SessionXAGetTimeoutResponseMessage,
    SessionXAJoinMessage,
    SessionXAPrepareMessage,
    SessionXAResponseMessage,
    SessionXAResponseMessageV2,
    SessionXAResumeMessage,
    SessionXARollbackMessage,
    SessionXASetTimeoutMessage,
    SessionXASetTimeoutResponseMessage,
    SessionXAStartMessage,
    // ---- end xa ----
    // ---- large ----
    SessionSendContinuationMessage,
    SessionSendContinuationMessageV2,
    SessionSendContinuationMessageV3,
    SessionReceiveContinuationMessage,
    SessionSendLargeMessage,
    SessionReceiveLargeMessage,
    SessionReceiveClientLargeMessage,
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
    ReplicationResponseMessage,
    ReplicationResponseMessageV2,
    ReplicationAddMessage,
    ReplicationAddTXMessage,
    ReplicationDeleteMessage,
    ReplicationDeleteTXMessage,
    ReplicationPrepareMessage,
    ReplicationCommitMessage,
    ReplicationPageWriteMessage,
    ReplicationPageEventMessage,
    ReplicationLargeMessageBeginMessage,
    ReplicationLargeMessageEndMessage,
    ReplicationLargeMessageWriteMessage,
    ReplicationSyncFileMessage,
    ReplicationStartSyncMessage,
    ReplicationPrimaryIsStoppingMessage,
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
            types::SESS_XA_START => body::<SessionXAStartMessage>(r, ctx),
            types::SESS_XA_FAILED => body::<SessionXAAfterFailedMessage>(r, ctx),
            types::SESS_XA_END => body::<SessionXAEndMessage>(r, ctx),
            types::SESS_XA_COMMIT => body::<SessionXACommitMessage>(r, ctx),
            types::SESS_XA_PREPARE => body::<SessionXAPrepareMessage>(r, ctx),
            types::SESS_XA_RESP if async_v1 => body::<SessionXAResponseMessage>(r, ctx),
            types::SESS_XA_RESP => body::<SessionXAResponseMessageV2>(r, ctx),
            types::SESS_XA_ROLLBACK => body::<SessionXARollbackMessage>(r, ctx),
            types::SESS_XA_JOIN => body::<SessionXAJoinMessage>(r, ctx),
            types::SESS_XA_RESUME => body::<SessionXAResumeMessage>(r, ctx),
            types::SESS_XA_FORGET => body::<SessionXAForgetMessage>(r, ctx),
            types::SESS_XA_INDOUBT_XIDS_RESP => {
                body::<SessionXAGetInDoubtXidsResponseMessage>(r, ctx)
            }
            types::SESS_XA_SET_TIMEOUT => body::<SessionXASetTimeoutMessage>(r, ctx),
            types::SESS_XA_SET_TIMEOUT_RESP => body::<SessionXASetTimeoutResponseMessage>(r, ctx),
            types::SESS_XA_GET_TIMEOUT_RESP => body::<SessionXAGetTimeoutResponseMessage>(r, ctx),
            // ---- end xa ----
            // ---- large ----
            types::SESS_SEND_CONTINUATION if async_v1 => {
                body::<SessionSendContinuationMessage>(r, ctx)
            }
            types::SESS_SEND_CONTINUATION if ctx.is_before_producer_metrics_changed() => {
                body::<SessionSendContinuationMessageV2>(r, ctx)
            }
            types::SESS_SEND_CONTINUATION => body::<SessionSendContinuationMessageV3>(r, ctx),
            types::SESS_RECEIVE_CONTINUATION => body::<SessionReceiveContinuationMessage>(r, ctx),
            // Only ServerPacketDecoder knows SESS_SEND_LARGE.
            types::SESS_SEND_LARGE if ctx.direction == Direction::ToServer => {
                body::<SessionSendLargeMessage>(r, ctx)
            }
            // ClientPacketDecoder; SessionReceiveLargeMessage is encode-only.
            types::SESS_RECEIVE_LARGE_MSG if ctx.direction == Direction::ToClient => {
                body::<SessionReceiveClientLargeMessage>(r, ctx)
            }
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
            // ServerPacketDecoder only; both ends of a replication connection
            // use it, so these decode the same in either direction.
            types::REPLICATION_APPEND => body::<ReplicationAddMessage>(r, ctx),
            types::REPLICATION_APPEND_TX => body::<ReplicationAddTXMessage>(r, ctx),
            types::REPLICATION_DELETE => body::<ReplicationDeleteMessage>(r, ctx),
            types::REPLICATION_DELETE_TX => body::<ReplicationDeleteTXMessage>(r, ctx),
            types::REPLICATION_PREPARE => body::<ReplicationPrepareMessage>(r, ctx),
            types::REPLICATION_COMMIT_ROLLBACK => body::<ReplicationCommitMessage>(r, ctx),
            types::REPLICATION_RESPONSE => body::<ReplicationResponseMessage>(r, ctx),
            types::REPLICATION_RESPONSE_V2 => body::<ReplicationResponseMessageV2>(r, ctx),
            types::REPLICATION_PAGE_WRITE => body::<ReplicationPageWriteMessage>(r, ctx),
            types::REPLICATION_PAGE_EVENT => body::<ReplicationPageEventMessage>(r, ctx),
            types::REPLICATION_LARGE_MESSAGE_BEGIN => {
                body::<ReplicationLargeMessageBeginMessage>(r, ctx)
            }
            types::REPLICATION_LARGE_MESSAGE_END => {
                body::<ReplicationLargeMessageEndMessage>(r, ctx)
            }
            types::REPLICATION_LARGE_MESSAGE_WRITE => {
                body::<ReplicationLargeMessageWriteMessage>(r, ctx)
            }
            types::REPLICATION_START_FINISH_SYNC => body::<ReplicationStartSyncMessage>(r, ctx),
            types::REPLICATION_SYNC_FILE => body::<ReplicationSyncFileMessage>(r, ctx),
            types::REPLICATION_SCHEDULED_FAILOVER => {
                body::<ReplicationPrimaryIsStoppingMessage>(r, ctx)
            }
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
