//! Cluster, topology, HA (backup/quorum/scale-down), federation and failover-check packets.

use saranga_buffer::{Codec, JString, Reader, Result, SimpleString, Writer};

use super::cluster_types::{
    FederationDownstreamConfiguration, FederationStreamConnect, MapValue, PersistedMap, Vote,
};
use crate::transport::TransportConfiguration;
use crate::{Context, Flagged, PacketCodec, types};

// ---------------------------------------------------------------------------
// Topology
// ---------------------------------------------------------------------------

wire_packet! {
    /// `SubscribeClusterTopologyUpdatesMessage`
    SubscribeClusterTopologyUpdatesMessage = types::SUBSCRIBE_TOPOLOGY => {
        pub cluster_connection: bool,
    }
}

wire_packet! {
    /// `SubscribeClusterTopologyUpdatesMessageV2`: adds the client version.
    SubscribeClusterTopologyUpdatesMessageV2 = types::SUBSCRIBE_TOPOLOGY_V2 => {
        pub cluster_connection: bool,
        pub client_version: i32,
    }
}

/// The non-exit part of a topology change: the
/// `Pair<TransportConfiguration, TransportConfiguration>` (primary, backup),
/// each behind a boolean presence flag, then `last`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TopologyUpdate {
    pub primary: Flagged<TransportConfiguration>,
    pub backup: Flagged<TransportConfiguration>,
    pub last: bool,
}

impl Codec for TopologyUpdate {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            primary: Codec::read(r)?,
            backup: Codec::read(r)?,
            last: r.bool()?,
        })
    }

    fn write(&self, w: &mut Writer) {
        self.primary.write(w);
        self.backup.write(w);
        w.bool(self.last);
    }
}

/// Reads the `exit` flag; the update follows only when it is false.
fn read_update(r: &mut Reader<'_>, exit: bool) -> Result<Option<TopologyUpdate>> {
    if exit {
        Ok(None)
    } else {
        TopologyUpdate::read(r).map(Some)
    }
}

fn write_update(w: &mut Writer, update: &Option<TopologyUpdate>) {
    if let Some(u) = update {
        u.write(w);
    }
}

/// `ClusterTopologyChangeMessage`. `update` is `None` for a node exit
/// (`exit == true`), in which case only the node ID is encoded.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClusterTopologyChangeMessage {
    pub node_id: JString,
    pub update: Option<TopologyUpdate>,
}

impl PacketCodec for ClusterTopologyChangeMessage {
    const TYPE: i8 = types::CLUSTER_TOPOLOGY;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let exit = r.bool()?;
        Ok(Self {
            node_id: r.string()?,
            update: read_update(r, exit)?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.bool(self.update.is_none()).string(&self.node_id);
        write_update(w, &self.update);
    }
}

/// `ClusterTopologyChangeMessage_V2`: adds the unique event ID and the
/// backup group name. Java reads the group name only if bytes remain, so
/// the outer `Option` records whether it was on the wire at all.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClusterTopologyChangeMessageV2 {
    pub node_id: JString,
    pub unique_event_id: i64,
    pub update: Option<TopologyUpdate>,
    pub backup_group_name: Option<Option<JString>>,
}

impl PacketCodec for ClusterTopologyChangeMessageV2 {
    const TYPE: i8 = types::CLUSTER_TOPOLOGY_V2;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let exit = r.bool()?;
        Ok(Self {
            node_id: r.string()?,
            unique_event_id: r.i64()?,
            update: read_update(r, exit)?,
            backup_group_name: if r.is_empty() {
                None
            } else {
                Some(r.nullable_string()?)
            },
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.bool(self.update.is_none())
            .string(&self.node_id)
            .i64(self.unique_event_id);
        write_update(w, &self.update);
        if let Some(name) = &self.backup_group_name {
            w.nullable_string(name.as_ref());
        }
    }
}

/// Shared body of `ClusterTopologyChangeMessage_V3` and `_V4`. The backup
/// group name is always present here: V3 reads the scale-down group name
/// unconditionally after it.
fn decode_v3(r: &mut Reader<'_>) -> Result<ClusterTopologyChangeMessageV3> {
    let exit = r.bool()?;
    Ok(ClusterTopologyChangeMessageV3 {
        node_id: r.string()?,
        unique_event_id: r.i64()?,
        update: read_update(r, exit)?,
        backup_group_name: r.nullable_string()?,
        scale_down_group_name: r.nullable_string()?,
    })
}

fn encode_v3(
    w: &mut Writer,
    node_id: &JString,
    unique_event_id: i64,
    update: &Option<TopologyUpdate>,
    backup_group_name: &Option<JString>,
    scale_down_group_name: &Option<JString>,
) {
    w.bool(update.is_none())
        .string(node_id)
        .i64(unique_event_id);
    write_update(w, update);
    w.nullable_string(backup_group_name.as_ref())
        .nullable_string(scale_down_group_name.as_ref());
}

/// `ClusterTopologyChangeMessage_V3`: adds the scale-down group name.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClusterTopologyChangeMessageV3 {
    pub node_id: JString,
    pub unique_event_id: i64,
    pub update: Option<TopologyUpdate>,
    pub backup_group_name: Option<JString>,
    pub scale_down_group_name: Option<JString>,
}

impl PacketCodec for ClusterTopologyChangeMessageV3 {
    const TYPE: i8 = types::CLUSTER_TOPOLOGY_V3;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        decode_v3(r)
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        encode_v3(
            w,
            &self.node_id,
            self.unique_event_id,
            &self.update,
            &self.backup_group_name,
            &self.scale_down_group_name,
        );
    }
}

/// `ClusterTopologyChangeMessage_V4`: adds the sender's server version.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClusterTopologyChangeMessageV4 {
    pub node_id: JString,
    pub unique_event_id: i64,
    pub update: Option<TopologyUpdate>,
    pub backup_group_name: Option<JString>,
    pub scale_down_group_name: Option<JString>,
    pub server_version: i32,
}

impl PacketCodec for ClusterTopologyChangeMessageV4 {
    const TYPE: i8 = types::CLUSTER_TOPOLOGY_V4;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let v3 = decode_v3(r)?;
        Ok(Self {
            node_id: v3.node_id,
            unique_event_id: v3.unique_event_id,
            update: v3.update,
            backup_group_name: v3.backup_group_name,
            scale_down_group_name: v3.scale_down_group_name,
            server_version: r.i32()?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        encode_v3(
            w,
            &self.node_id,
            self.unique_event_id,
            &self.update,
            &self.backup_group_name,
            &self.scale_down_group_name,
        );
        w.i32(self.server_version);
    }
}

wire_packet! {
    /// `NodeAnnounceMessage`
    NodeAnnounceMessage = types::NODE_ANNOUNCE => {
        pub node_id: JString,
        pub backup_group_name: Option<JString>,
        pub backup: bool,
        pub current_event_id: i64,
        pub connector: Flagged<TransportConfiguration>,
        pub backup_connector: Flagged<TransportConfiguration>,
        pub scale_down_group_name: Option<JString>,
    }
}

// ---------------------------------------------------------------------------
// Cluster connections
// ---------------------------------------------------------------------------

wire_packet! {
    /// `ClusterConnectMessage`
    ClusterConnectMessage = types::CLUSTER_CONNECT => {
        pub cluster_user: JString,
        pub cluster_password: JString,
    }
}

wire_packet! {
    /// `ClusterConnectReplyMessage`
    ClusterConnectReplyMessage = types::CLUSTER_CONNECT_REPLY => {
        pub authorized: bool,
    }
}

// ---------------------------------------------------------------------------
// HA: backups, quorum, scale-down
// ---------------------------------------------------------------------------

wire_packet! {
    /// `BackupRegistrationMessage`
    BackupRegistrationMessage = types::BACKUP_REGISTRATION => {
        pub cluster_user: JString,
        pub cluster_password: JString,
        pub backup_wants_fail_back: bool,
        pub connector: TransportConfiguration,
    }
}

/// `BackupReplicationStartFailedMessage.BackupRegistrationProblem` codes.
pub mod backup_registration_problem {
    pub const EXCEPTION: i32 = 0;
    pub const AUTHENTICATION: i32 = 1;
    pub const ALREADY_REPLICATING: i32 = 2;
}

wire_packet! {
    /// `BackupReplicationStartFailedMessage`. `problem` is the
    /// [`backup_registration_problem`] code.
    BackupReplicationStartFailedMessage = types::BACKUP_REGISTRATION_FAILED => {
        pub problem: i32,
    }
}

wire_packet! {
    /// `BackupRequestMessage`
    BackupRequestMessage = types::BACKUP_REQUEST => {
        pub backup_size: i32,
        pub journal_directory: Option<JString>,
        pub bindings_directory: Option<JString>,
        pub large_messages_directory: Option<JString>,
        pub paging_directory: Option<JString>,
        pub node_id: Option<SimpleString>,
    }
}

wire_packet! {
    /// `BackupResponseMessage`
    BackupResponseMessage = types::BACKUP_REQUEST_RESPONSE => {
        pub backup_started: bool,
    }
}

/// `QuorumVoteMessage`: the handler name, then the vote filling the rest of
/// the body.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QuorumVoteMessage {
    pub handler: SimpleString,
    pub vote: Vote,
}

impl PacketCodec for QuorumVoteMessage {
    const TYPE: i8 = types::QUORUM_VOTE;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let handler = r.simple_string()?;
        let vote = Vote::decode(&handler, r)?;
        Ok(Self { handler, vote })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.simple_string(&self.handler);
        self.vote.encode(w);
    }
}

/// `QuorumVoteReplyMessage`: same layout as [`QuorumVoteMessage`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QuorumVoteReplyMessage {
    pub handler: SimpleString,
    pub vote: Vote,
}

impl PacketCodec for QuorumVoteReplyMessage {
    const TYPE: i8 = types::QUORUM_VOTE_REPLY;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        let handler = r.simple_string()?;
        let vote = Vote::decode(&handler, r)?;
        Ok(Self { handler, vote })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.simple_string(&self.handler);
        self.vote.encode(w);
    }
}

wire_packet! {
    /// `ScaleDownAnnounceMessage`
    ScaleDownAnnounceMessage = types::SCALEDOWN_ANNOUNCEMENT => {
        pub target_node_id: SimpleString,
        pub scaled_down_node_id: SimpleString,
    }
}

// ---------------------------------------------------------------------------
// Connect / failover check (CONNECT = CHECK_FOR_FAILOVER)
// ---------------------------------------------------------------------------

/// Reads a trailing `AbstractMapPersister` record only if bytes remain.
fn read_trailing_map(r: &mut Reader<'_>) -> Result<Option<PersistedMap>> {
    if r.is_empty() {
        Ok(None)
    } else {
        PersistedMap::read(r).map(Some)
    }
}

/// `ConnectMessage` (type `CONNECT`, also `CHECK_FOR_FAILOVER`). `fields`
/// is the `ConnectMapSerializer` record, absent from older senders.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ConnectMessage {
    pub node_id: Option<JString>,
    pub fields: Option<PersistedMap>,
}

impl ConnectMessage {
    pub const FIELD_CLIENT_VERSION: i16 = 1;
    pub const FIELD_AUTH_MECHANISM: i16 = 2;
    pub const FIELD_AUTH_DATA: i16 = 3;

    fn field(&self, key: i16) -> Option<&MapValue> {
        self.fields.as_ref().and_then(|m| m.get(key))
    }

    /// The client version, 0 when absent (as in Java).
    pub fn client_version(&self) -> i32 {
        match self.field(Self::FIELD_CLIENT_VERSION) {
            Some(MapValue::Integer(v)) => *v,
            _ => 0,
        }
    }

    pub fn auth_mechanism(&self) -> Option<&JString> {
        match self.field(Self::FIELD_AUTH_MECHANISM) {
            Some(MapValue::String(v)) => Some(v),
            _ => None,
        }
    }

    pub fn auth_data(&self) -> Option<&[u8]> {
        match self.field(Self::FIELD_AUTH_DATA) {
            Some(MapValue::ByteArray(v)) => Some(v),
            _ => None,
        }
    }
}

impl PacketCodec for ConnectMessage {
    const TYPE: i8 = types::CONNECT;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            node_id: r.nullable_string()?,
            fields: read_trailing_map(r)?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.nullable_string(self.node_id.as_ref());
        if let Some(m) = &self.fields {
            m.write(w);
        }
    }
}

/// `ConnectResponseMessage` (type `CONNECT_RESPONSE`, also
/// `CHECK_FOR_FAILOVER_REPLY`). `fields` is the
/// `ConnectResponseMapSerializer` record, absent from older senders.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ConnectResponseMessage {
    pub ok_to_failover: bool,
    pub fields: Option<PersistedMap>,
}

impl ConnectResponseMessage {
    pub const FIELD_SERVER_VERSION: i16 = 1;

    /// The server version, 0 when absent (as in Java).
    pub fn server_version(&self) -> i32 {
        match self
            .fields
            .as_ref()
            .and_then(|m| m.get(Self::FIELD_SERVER_VERSION))
        {
            Some(MapValue::Integer(v)) => *v,
            _ => 0,
        }
    }
}

impl PacketCodec for ConnectResponseMessage {
    const TYPE: i8 = types::CONNECT_RESPONSE;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Ok(Self {
            ok_to_failover: r.bool()?,
            fields: read_trailing_map(r)?,
        })
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        w.bool(self.ok_to_failover);
        if let Some(m) = &self.fields {
            m.write(w);
        }
    }
}

// ---------------------------------------------------------------------------
// Federation
// ---------------------------------------------------------------------------

/// `FederationDownstreamConnectMessage`: a `FederationStreamConnectMessage`
/// whose stream configuration is a `FederationDownstreamConfiguration`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FederationDownstreamConnectMessage(
    pub FederationStreamConnect<FederationDownstreamConfiguration>,
);

impl PacketCodec for FederationDownstreamConnectMessage {
    const TYPE: i8 = types::FEDERATION_DOWNSTREAM_CONNECT;

    fn decode(r: &mut Reader<'_>, _ctx: &Context) -> Result<Self> {
        Codec::read(r).map(Self)
    }

    fn encode(&self, w: &mut Writer, _ctx: &Context) {
        self.0.write(w);
    }
}

#[cfg(test)]
mod tests {
    use super::super::cluster_types::FederationPolicy;
    use super::*;
    use crate::{Direction, Packet, read_frame};

    fn fixture(name: &str) -> Packet {
        let path = format!(
            "{}/../../tests/golden/packets/cluster/{name}.bin",
            env!("CARGO_MANIFEST_DIR")
        );
        let bytes = std::fs::read(&path).unwrap();
        let (frame, _) = read_frame(&bytes).unwrap().unwrap();
        let dir = if name.contains("@s-") {
            Direction::ToServer
        } else {
            Direction::ToClient
        };
        Packet::decode(&frame, &Context::new(dir, 136)).unwrap()
    }

    #[test]
    fn votes_decode_by_handler() {
        let Packet::QuorumVoteMessage(m) = fixture("QuorumVoteMessage~oldname@s-136") else {
            panic!()
        };
        assert!(matches!(m.vote, Vote::ServerConnect(ref v) if !v.vote));
        let Packet::QuorumVoteReplyMessage(m) = fixture("QuorumVoteReplyMessage@c-136") else {
            panic!()
        };
        assert!(
            matches!(m.vote, Vote::ServerConnect(ref v) if v.transport_configuration.is_some())
        );
        let Packet::QuorumVoteReplyMessage(m) =
            fixture("QuorumVoteReplyMessage~requestbackup@c-136")
        else {
            panic!()
        };
        assert!(
            matches!(m.vote, Vote::RequestBackup(ref v) if v.backups_size == 3 && v.backup_available)
        );
        let Packet::QuorumVoteMessage(m) = fixture("QuorumVoteMessage~unknown@s-136") else {
            panic!()
        };
        assert_eq!(m.vote, Vote::Raw(vec![0xff]));
    }

    #[test]
    fn connect_fields() {
        let Packet::ConnectMessage(m) = fixture("ConnectMessage~plain@s-136") else {
            panic!()
        };
        assert_eq!(m.client_version(), 136);
        assert_eq!(m.auth_mechanism().unwrap().to_string_lossy(), "PLAIN");
        assert_eq!(m.auth_data().unwrap(), "\0user\0pässword".as_bytes());
        let Packet::ConnectResponseMessage(m) = fixture("ConnectResponseMessage@c-136") else {
            panic!()
        };
        assert!(m.ok_to_failover);
        assert_eq!(m.server_version(), 136);
    }

    #[test]
    fn federation_policies() {
        let Packet::FederationDownstreamConnectMessage(m) =
            fixture("FederationDownstreamConnectMessage@s-136")
        else {
            panic!()
        };
        let m = m.0;
        assert_eq!(m.federation_policies.len(), 3);
        assert!(
            matches!(&m.federation_policies[0], FederationPolicy::Address(p) if p.enable_divert_bindings && p.max_hops == 2)
        );
        assert!(
            matches!(&m.federation_policies[1], FederationPolicy::Queue(p) if p.priority_adjustment == Some(-1))
        );
        assert!(matches!(
            &m.federation_policies[2],
            FederationPolicy::PolicySet(_)
        ));
        let conn = &m.stream_configuration.stream.connection_configuration;
        // Java writes retryInterval where maxRetryInterval is read back.
        assert_eq!((conn.retry_interval, conn.max_retry_interval), (750, 750));
        assert_eq!(conn.retry_interval_multiplier, 1.5);
    }
}
