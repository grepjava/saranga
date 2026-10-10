//! Encodable objects embedded in the cluster, HA and federation packets:
//! quorum votes, the `AbstractMapPersister` record used by the
//! connect handshake, and the federation configurations.

use saranga_buffer::{Codec, DecodeError, JString, Reader, Result, SimpleString, Writer};

use crate::Flagged;
use crate::transport::TransportConfiguration;

/// Reads an i32 count followed by that many `T`s.
pub(crate) fn read_list<T: Codec>(r: &mut Reader<'_>) -> Result<Vec<T>> {
    let count = r.i32()?;
    let mut out = Vec::new();
    for _ in 0..count.max(0) {
        out.push(T::read(r)?);
    }
    Ok(out)
}

/// Writes an i32 count followed by the items.
pub(crate) fn write_list<T: Codec>(w: &mut Writer, items: &[T]) {
    w.i32(items.len() as i32);
    for item in items {
        item.write(w);
    }
}

// ---------------------------------------------------------------------------
// Quorum votes
// ---------------------------------------------------------------------------

/// `QuorumVoteServerConnect.PRIMARY_FAILOVER_VOTE`.
pub const PRIMARY_FAILOVER_VOTE: &str = "PrimaryFailoverQuorumVote";
/// `QuorumVoteServerConnect.OLD_PRIMARY_FAILOVER_VOTE` (before 2.36.0).
pub const OLD_PRIMARY_FAILOVER_VOTE: &str = "LiveFailoverQuorumVote";
/// `ColocatedActivation.REQUEST_BACKUP_QUORUM_VOTE`.
pub const REQUEST_BACKUP_QUORUM_VOTE: &str = "RequestBackupQuorumVote";

/// `BooleanVote`: a plain yes/no vote. Never sent on its own (it is the
/// "not connected" local vote) but is the base of [`ServerConnectVote`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BooleanVote {
    pub vote: bool,
}

impl Codec for BooleanVote {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self { vote: r.bool()? })
    }

    fn write(&self, w: &mut Writer) {
        w.bool(self.vote);
    }
}

/// `ServerConnectVote` (handler `PrimaryFailoverQuorumVote`, or
/// `LiveFailoverQuorumVote` from older brokers).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ServerConnectVote {
    pub vote: bool,
    pub node_id: JString,
    /// The primary connector's `toString()`, set on positive replies.
    pub transport_configuration: Option<JString>,
}

impl Codec for ServerConnectVote {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            vote: r.bool()?,
            node_id: r.string()?,
            transport_configuration: r.nullable_string()?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.bool(self.vote)
            .string(&self.node_id)
            .nullable_string(self.transport_configuration.as_ref());
    }
}

/// `ColocatedActivation.RequestBackupVote` (handler `RequestBackupQuorumVote`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestBackupVote {
    pub backups_size: i32,
    pub node_id: Option<JString>,
    pub backup_available: bool,
}

impl Codec for RequestBackupVote {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            backups_size: r.i32()?,
            node_id: r.nullable_string()?,
            backup_available: r.bool()?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.i32(self.backups_size)
            .nullable_string(self.node_id.as_ref())
            .bool(self.backup_available);
    }
}

/// The vote carried by `QuorumVoteMessage` / `QuorumVoteReplyMessage`.
/// Java keeps the vote as raw bytes until a `QuorumVoteHandler` registered
/// under the packet's handler name decodes it; here the known handlers are
/// decoded eagerly and anything else stays raw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Vote {
    ServerConnect(ServerConnectVote),
    RequestBackup(RequestBackupVote),
    /// A vote for an unknown handler, or one its handler could not decode
    /// exactly.
    Raw(Vec<u8>),
}

impl Default for Vote {
    fn default() -> Self {
        Self::Raw(Vec::new())
    }
}

impl Vote {
    /// Decodes the rest of the packet as the vote for `handler`.
    pub fn decode(handler: &SimpleString, r: &mut Reader<'_>) -> Result<Self> {
        let rest = r.bytes(r.remaining())?;
        let name = handler.to_string_lossy();
        let typed = |f: fn(&mut Reader<'_>) -> Result<Self>| {
            let mut vr = Reader::new(rest);
            match f(&mut vr) {
                Ok(v) if vr.is_empty() => Some(v),
                _ => None,
            }
        };
        let vote = match name.as_str() {
            PRIMARY_FAILOVER_VOTE | OLD_PRIMARY_FAILOVER_VOTE => {
                typed(|r| ServerConnectVote::read(r).map(Self::ServerConnect))
            }
            REQUEST_BACKUP_QUORUM_VOTE => {
                typed(|r| RequestBackupVote::read(r).map(Self::RequestBackup))
            }
            _ => None,
        };
        Ok(vote.unwrap_or_else(|| Self::Raw(rest.to_vec())))
    }

    pub fn encode(&self, w: &mut Writer) {
        match self {
            Self::ServerConnect(v) => v.write(w),
            Self::RequestBackup(v) => v.write(w),
            Self::Raw(b) => {
                w.bytes(b);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// AbstractMapPersister
// ---------------------------------------------------------------------------

/// A value in a [`PersistedMap`] (`AbstractMapPersister.Datatypes`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapValue {
    Boolean(bool),
    String(JString),
    Integer(i32),
    Long(i64),
    Byte(i8),
    ByteArray(Vec<u8>),
}

/// The record written by `AbstractMapPersister`: an i32 record size
/// (including itself), an unsigned-short entry count, then entries of
/// (i16 key, type byte, value). Entries keep their wire order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PersistedMap {
    pub entries: Vec<(i16, MapValue)>,
}

/// `getMaxAllowedElements()` of both connect persisters.
const MAX_MAP_ELEMENTS: usize = 10;
const HEADER_SIZE: usize = 6;

impl PersistedMap {
    pub fn get(&self, key: i16) -> Option<&MapValue> {
        self.entries.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
    }
}

impl Codec for PersistedMap {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let start = r.remaining();
        let size = r.i32()?;
        if size < HEADER_SIZE as i32 {
            return Err(DecodeError::Invalid(format!("invalid record size {size}")));
        }
        let size = size as usize;
        if size - 4 > r.remaining() {
            return Err(DecodeError::Invalid(format!(
                "record size {size} exceeds available bytes"
            )));
        }
        let count = r.u16()? as usize;
        if count > MAX_MAP_ELEMENTS {
            return Err(DecodeError::Invalid(format!(
                "{count} map entries, max {MAX_MAP_ELEMENTS}"
            )));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let key = r.i16()?;
            let value = match r.u8()? {
                0 => MapValue::Boolean(r.bool()?),
                1 => MapValue::String(r.string()?),
                2 => MapValue::Integer(r.i32()?),
                3 => MapValue::Long(r.i64()?),
                4 => MapValue::Byte(r.i8()?),
                5 => MapValue::ByteArray(r.byte_array()?),
                t => return Err(DecodeError::Invalid(format!("unknown datatype id {t}"))),
            };
            entries.push((key, value));
        }
        let used = start - r.remaining();
        if used != size {
            return Err(DecodeError::Invalid(format!(
                "map record size {size} but {used} bytes decoded"
            )));
        }
        Ok(Self { entries })
    }

    fn write(&self, w: &mut Writer) {
        let at = w.len();
        w.i32(0).u16(self.entries.len() as u16);
        for (key, value) in &self.entries {
            w.i16(*key);
            match value {
                MapValue::Boolean(v) => w.u8(0).bool(*v),
                MapValue::String(v) => w.u8(1).string(v),
                MapValue::Integer(v) => w.u8(2).i32(*v),
                MapValue::Long(v) => w.u8(3).i64(*v),
                MapValue::Byte(v) => w.u8(4).i8(*v),
                MapValue::ByteArray(v) => w.u8(5).byte_array(v),
            };
        }
        let size = (w.len() - at) as i32;
        w.set_i32(at, size);
    }
}

// ---------------------------------------------------------------------------
// Federation
// ---------------------------------------------------------------------------

/// `FederationConfiguration.Credentials`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Credentials {
    pub user: Option<JString>,
    pub password: Option<JString>,
}

impl Codec for Credentials {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            user: r.nullable_string()?,
            password: r.nullable_string()?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.nullable_string(self.user.as_ref())
            .nullable_string(self.password.as_ref());
    }
}

/// `FederationConnectionConfiguration`. Only the fields that go on the wire.
///
/// Java quirk: `encode` writes `retryInterval` in the slot that `decode`
/// reads as `maxRetryInterval`, so the wire `max_retry_interval` always
/// equals `retry_interval` for Java senders.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FederationConnectionConfiguration {
    pub username: Option<JString>,
    pub password: Option<JString>,
    pub share_connection: bool,
    pub priority_adjustment: i32,
    pub client_failure_check_period: i64,
    pub connection_ttl: i64,
    pub retry_interval: i64,
    pub retry_interval_multiplier: f64,
    pub max_retry_interval: i64,
    pub initial_connect_attempts: i32,
    pub reconnect_attempts: i32,
    pub call_timeout: i64,
    pub call_failover_timeout: i64,
}

impl Codec for FederationConnectionConfiguration {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            username: r.nullable_string()?,
            password: r.nullable_string()?,
            share_connection: r.bool()?,
            priority_adjustment: r.i32()?,
            client_failure_check_period: r.i64()?,
            connection_ttl: r.i64()?,
            retry_interval: r.i64()?,
            retry_interval_multiplier: r.f64()?,
            max_retry_interval: r.i64()?,
            initial_connect_attempts: r.i32()?,
            reconnect_attempts: r.i32()?,
            call_timeout: r.i64()?,
            call_failover_timeout: r.i64()?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.nullable_string(self.username.as_ref())
            .nullable_string(self.password.as_ref())
            .bool(self.share_connection)
            .i32(self.priority_adjustment)
            .i64(self.client_failure_check_period)
            .i64(self.connection_ttl)
            .i64(self.retry_interval)
            .f64(self.retry_interval_multiplier)
            .i64(self.max_retry_interval)
            .i32(self.initial_connect_attempts)
            .i32(self.reconnect_attempts)
            .i64(self.call_timeout)
            .i64(self.call_failover_timeout);
    }
}

/// `FederationStreamConfiguration`: the part shared by upstream and
/// downstream configurations. `policy_refs` keeps the wire (HashSet) order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FederationStreamConfiguration {
    pub name: JString,
    pub connection_configuration: FederationConnectionConfiguration,
    pub policy_refs: Vec<JString>,
}

impl Codec for FederationStreamConfiguration {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            name: r.string()?,
            connection_configuration: FederationConnectionConfiguration::read(r)?,
            policy_refs: read_list(r)?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.string(&self.name);
        self.connection_configuration.write(w);
        write_list(w, &self.policy_refs);
    }
}

/// `FederationDownstreamConfiguration`: the stream configuration plus the
/// connector the downstream broker should use to connect back upstream.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FederationDownstreamConfiguration {
    pub stream: FederationStreamConfiguration,
    pub upstream_configuration: TransportConfiguration,
}

impl Codec for FederationDownstreamConfiguration {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            stream: FederationStreamConfiguration::read(r)?,
            upstream_configuration: TransportConfiguration::read(r)?,
        })
    }

    fn write(&self, w: &mut Writer) {
        self.stream.write(w);
        self.upstream_configuration.write(w);
    }
}

/// `FederationTransformerConfiguration`. Properties keep wire order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FederationTransformerConfiguration {
    pub name: JString,
    pub class_name: JString,
    pub properties: Vec<(JString, JString)>,
}

impl Codec for FederationTransformerConfiguration {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let name = r.string()?;
        let class_name = r.string()?;
        let count = r.i32()?;
        let mut properties = Vec::new();
        for _ in 0..count.max(0) {
            properties.push((r.string()?, r.string()?));
        }
        Ok(Self {
            name,
            class_name,
            properties,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.string(&self.name)
            .string(&self.class_name)
            .i32(self.properties.len() as i32);
        for (k, v) in &self.properties {
            w.string(k).string(v);
        }
    }
}

/// `FederationAddressPolicyConfiguration`. Matchers are the `addressMatch`
/// strings, in wire order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FederationAddressPolicyConfiguration {
    pub name: JString,
    pub auto_delete: Option<bool>,
    pub auto_delete_delay: Option<i64>,
    pub auto_delete_message_count: Option<i64>,
    pub max_hops: i32,
    pub transformer_ref: Option<JString>,
    pub includes: Vec<JString>,
    pub excludes: Vec<JString>,
    /// Read only if bytes remain (Java `readableBytes() > 0`); inside a
    /// packet more data always follows, so it is always present there.
    pub enable_divert_bindings: bool,
}

impl Codec for FederationAddressPolicyConfiguration {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            name: r.string()?,
            auto_delete: r.nullable_bool()?,
            auto_delete_delay: r.nullable_i64()?,
            auto_delete_message_count: r.nullable_i64()?,
            max_hops: r.i32()?,
            transformer_ref: r.nullable_string()?,
            includes: read_list(r)?,
            excludes: read_list(r)?,
            enable_divert_bindings: !r.is_empty() && r.bool()?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.string(&self.name)
            .nullable_bool(self.auto_delete)
            .nullable_i64(self.auto_delete_delay)
            .nullable_i64(self.auto_delete_message_count)
            .i32(self.max_hops)
            .nullable_string(self.transformer_ref.as_ref());
        write_list(w, &self.includes);
        write_list(w, &self.excludes);
        w.bool(self.enable_divert_bindings);
    }
}

/// `FederationQueuePolicyConfiguration.Matcher`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueueMatcher {
    pub address_match: Option<JString>,
    pub queue_match: Option<JString>,
}

impl Codec for QueueMatcher {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            address_match: r.nullable_string()?,
            queue_match: r.nullable_string()?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.nullable_string(self.address_match.as_ref())
            .nullable_string(self.queue_match.as_ref());
    }
}

/// `FederationQueuePolicyConfiguration`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FederationQueuePolicyConfiguration {
    pub name: JString,
    pub include_federated: bool,
    pub priority_adjustment: Option<i32>,
    pub transformer_ref: Option<JString>,
    pub includes: Vec<QueueMatcher>,
    pub excludes: Vec<QueueMatcher>,
}

impl Codec for FederationQueuePolicyConfiguration {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            name: r.string()?,
            include_federated: r.bool()?,
            priority_adjustment: r.nullable_i32()?,
            transformer_ref: r.nullable_string()?,
            includes: read_list(r)?,
            excludes: read_list(r)?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.string(&self.name)
            .bool(self.include_federated)
            .nullable_i32(self.priority_adjustment)
            .nullable_string(self.transformer_ref.as_ref());
        write_list(w, &self.includes);
        write_list(w, &self.excludes);
    }
}

/// `FederationPolicySet`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FederationPolicySet {
    pub name: JString,
    pub policy_refs: Vec<JString>,
}

impl Codec for FederationPolicySet {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            name: r.string()?,
            policy_refs: read_list(r)?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.string(&self.name);
        write_list(w, &self.policy_refs);
    }
}

const ADDRESS_POLICY_CLASS: &str =
    "org.apache.activemq.artemis.core.config.federation.FederationAddressPolicyConfiguration";
const POLICY_SET_CLASS: &str =
    "org.apache.activemq.artemis.core.config.federation.FederationPolicySet";
const QUEUE_POLICY_CLASS: &str =
    "org.apache.activemq.artemis.core.config.federation.FederationQueuePolicyConfiguration";

/// `FederationPolicy`, as encoded in a federation connect packet: the Java
/// class name (`FederationPolicy.getFederationPolicy`) then the policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FederationPolicy {
    Address(FederationAddressPolicyConfiguration),
    PolicySet(FederationPolicySet),
    Queue(FederationQueuePolicyConfiguration),
}

impl FederationPolicy {
    pub fn name(&self) -> &JString {
        match self {
            Self::Address(p) => &p.name,
            Self::PolicySet(p) => &p.name,
            Self::Queue(p) => &p.name,
        }
    }

    fn class_name(&self) -> &'static str {
        match self {
            Self::Address(_) => ADDRESS_POLICY_CLASS,
            Self::PolicySet(_) => POLICY_SET_CLASS,
            Self::Queue(_) => QUEUE_POLICY_CLASS,
        }
    }
}

impl Codec for FederationPolicy {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        let class = r.string()?.to_string_lossy();
        Ok(match class.as_str() {
            ADDRESS_POLICY_CLASS => Self::Address(Codec::read(r)?),
            POLICY_SET_CLASS => Self::PolicySet(Codec::read(r)?),
            QUEUE_POLICY_CLASS => Self::Queue(Codec::read(r)?),
            _ => {
                return Err(DecodeError::Invalid(format!(
                    "unknown federation policy {class}"
                )));
            }
        })
    }

    fn write(&self, w: &mut Writer) {
        w.string(&JString::from(self.class_name()));
        match self {
            Self::Address(p) => p.write(w),
            Self::PolicySet(p) => p.write(w),
            Self::Queue(p) => p.write(w),
        }
    }
}

/// The body of `FederationStreamConnectMessage<T>`: everything the subclass
/// shares, generic over the stream configuration.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FederationStreamConnect<T> {
    pub name: JString,
    pub credentials: Flagged<Credentials>,
    /// `federationPolicyMap.values()` in wire order.
    pub federation_policies: Vec<FederationPolicy>,
    /// `transformerConfigurationMap.values()` in wire order.
    pub transformer_configurations: Vec<FederationTransformerConfiguration>,
    pub stream_configuration: T,
}

impl<T: Codec> Codec for FederationStreamConnect<T> {
    fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            name: r.string()?,
            credentials: Codec::read(r)?,
            federation_policies: read_list(r)?,
            transformer_configurations: read_list(r)?,
            stream_configuration: T::read(r)?,
        })
    }

    fn write(&self, w: &mut Writer) {
        w.string(&self.name);
        self.credentials.write(w);
        write_list(w, &self.federation_policies);
        write_list(w, &self.transformer_configurations);
        self.stream_configuration.write(w);
    }
}
