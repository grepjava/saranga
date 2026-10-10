/// Incrementing protocol versions (`PacketImpl` constants).
pub mod version {
    pub const ADDRESSING_CHANGE: i32 = 129;
    pub const ARTEMIS_2_7_0: i32 = 130;
    pub const ASYNC_RESPONSE_CHANGE: i32 = ARTEMIS_2_7_0;
    pub const CONSUMER_PRIORITY_CHANGE: i32 = ARTEMIS_2_7_0;
    pub const FQQN_CHANGE: i32 = ARTEMIS_2_7_0;
    pub const ARTEMIS_2_18_0: i32 = 131;
    pub const ARTEMIS_2_21_0: i32 = 132;
    pub const ARTEMIS_2_24_0: i32 = 133;
    pub const ARTEMIS_2_28_0: i32 = 134;
    pub const ARTEMIS_2_29_0: i32 = 135;
    pub const ARTEMIS_2_37_0: i32 = 136;

    /// The version this implementation speaks (Artemis 2.57.0).
    pub const CURRENT: i32 = ARTEMIS_2_37_0;
    /// `activemq.version.compatibleVersionList`.
    pub const COMPATIBLE: std::ops::RangeInclusive<i32> = 121..=136;
}

/// Which side is decoding. A few packet types decode differently on the
/// client (`ClientPacketDecoder`) and the server (`ServerPacketDecoder`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Direction {
    /// Packets travelling client to server, decoded by the server.
    ToServer,
    /// Packets travelling server to client, decoded by the client.
    ToClient,
}

/// Per-connection decoding state: direction and the negotiated channel
/// version (`CoreRemotingConnection.getChannelVersion`). The version starts
/// at 0 and is set from the client's version when a session is created.
///
/// The predicates mirror the `CoreRemotingConnection` default methods
/// exactly, including their treatment of version 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Context {
    pub direction: Direction,
    pub version: i32,
}

impl Context {
    pub fn new(direction: Direction, version: i32) -> Self {
        Self { direction, version }
    }

    pub fn is_version_before_address_change(&self) -> bool {
        self.version > 0 && self.version < version::ADDRESSING_CHANGE
    }

    pub fn is_version_before_async_response_change(&self) -> bool {
        self.version > 0 && self.version < version::ASYNC_RESPONSE_CHANGE
    }

    pub fn is_version_support_consumer_priority(&self) -> bool {
        self.version >= version::CONSUMER_PRIORITY_CHANGE
    }

    pub fn is_version_new_fqqn(&self) -> bool {
        self.version >= version::ARTEMIS_2_7_0
    }

    pub fn is_version_support_client_id(&self) -> bool {
        self.version >= version::ARTEMIS_2_18_0
    }

    pub fn is_version_support_routing(&self) -> bool {
        self.version >= version::ARTEMIS_2_18_0
    }

    pub fn is_version_support_commit_v2(&self) -> bool {
        self.version >= version::ARTEMIS_2_21_0
    }

    pub fn is_version_using_long_on_page_replication(&self) -> bool {
        self.version >= version::ARTEMIS_2_24_0
    }

    pub fn is_before_two_eighteen(&self) -> bool {
        self.version < version::ARTEMIS_2_18_0
    }

    pub fn is_before_producer_metrics_changed(&self) -> bool {
        self.version < version::ARTEMIS_2_28_0
    }
}
