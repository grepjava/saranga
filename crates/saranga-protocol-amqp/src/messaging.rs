//! Messaging types (part 3 of the specification): terminuses, delivery
//! states and the message sections.

use crate::codec::{Result, invalid};
use crate::composite::{Composite, Field, Fields, Timestamp, composite};
use crate::transaction::{Coordinator, Declared, TransactionalState};
use crate::transport::Error;
use crate::types::{Descriptor, Symbol, Value};

/// `terminus-durability`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TerminusDurability {
    #[default]
    None,
    Configuration,
    UnsettledState,
}

impl Field for TerminusDurability {
    fn from_value(v: Value) -> Result<Self> {
        match u32::from_value(v)? {
            0 => Ok(Self::None),
            1 => Ok(Self::Configuration),
            2 => Ok(Self::UnsettledState),
            n => invalid(format!("terminus-durability {n}")),
        }
    }
    fn to_value(&self) -> Value {
        Value::Uint(*self as u32)
    }
}

/// `terminus-expiry-policy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TerminusExpiryPolicy {
    LinkDetach,
    #[default]
    SessionEnd,
    ConnectionClose,
    Never,
}

impl TerminusExpiryPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LinkDetach => "link-detach",
            Self::SessionEnd => "session-end",
            Self::ConnectionClose => "connection-close",
            Self::Never => "never",
        }
    }
}

impl Field for TerminusExpiryPolicy {
    fn from_value(v: Value) -> Result<Self> {
        match Symbol::from_value(v)?.as_str() {
            "link-detach" => Ok(Self::LinkDetach),
            "session-end" => Ok(Self::SessionEnd),
            "connection-close" => Ok(Self::ConnectionClose),
            "never" => Ok(Self::Never),
            other => invalid(format!("terminus-expiry-policy {other}")),
        }
    }
    fn to_value(&self) -> Value {
        Value::symbol(self.as_str())
    }
}

composite! {
    /// `source`.
    pub struct Source(0x28, "amqp:source:list") {
        opt address: String,
        def durable: TerminusDurability = TerminusDurability::None,
        def expiry_policy: TerminusExpiryPolicy = TerminusExpiryPolicy::SessionEnd,
        def timeout: u32 = 0,
        def dynamic: bool = false,
        opt dynamic_node_properties: Fields,
        opt distribution_mode: Symbol,
        opt filter: Fields,
        opt default_outcome: DeliveryState,
        opt outcomes: Vec<Symbol>,
        opt capabilities: Vec<Symbol>,
    }
}

composite! {
    /// `target`.
    pub struct Target(0x29, "amqp:target:list") {
        opt address: String,
        def durable: TerminusDurability = TerminusDurability::None,
        def expiry_policy: TerminusExpiryPolicy = TerminusExpiryPolicy::SessionEnd,
        def timeout: u32 = 0,
        def dynamic: bool = false,
        opt dynamic_node_properties: Fields,
        opt capabilities: Vec<Symbol>,
    }
}

/// An attach's target: a node, or a transaction coordinator.
#[derive(Debug, Clone, PartialEq)]
pub enum TargetKind {
    Target(Target),
    Coordinator(Coordinator),
}

impl Field for TargetKind {
    fn from_value(v: Value) -> Result<Self> {
        match &v {
            Value::Described(d, _) if Coordinator::is(d) => Ok(Self::Coordinator(
                <Coordinator as Composite>::from_value(v)?,
            )),
            _ => Ok(Self::Target(<Target as Composite>::from_value(v)?)),
        }
    }
    fn to_value(&self) -> Value {
        match self {
            Self::Target(t) => Composite::to_value(t),
            Self::Coordinator(c) => Composite::to_value(c),
        }
    }
}

composite! {
    /// `received`.
    pub struct Received(0x23, "amqp:received:list") {
        req section_number: u32,
        req section_offset: u64,
    }
}

composite! {
    /// `accepted`.
    pub struct Accepted(0x24, "amqp:accepted:list") {}
}

composite! {
    /// `rejected`.
    pub struct Rejected(0x25, "amqp:rejected:list") {
        opt error: Error,
    }
}

composite! {
    /// `released`.
    pub struct Released(0x26, "amqp:released:list") {}
}

composite! {
    /// `modified`.
    pub struct Modified(0x27, "amqp:modified:list") {
        opt delivery_failed: bool,
        opt undeliverable_here: bool,
        opt message_annotations: Fields,
    }
}

/// A delivery state or outcome.
#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryState {
    Received(Received),
    Accepted,
    Rejected(Rejected),
    Released,
    Modified(Modified),
    Declared(Declared),
    Transactional(TransactionalState),
}

impl Field for DeliveryState {
    fn from_value(v: Value) -> Result<Self> {
        let Value::Described(d, _) = &v else {
            return invalid(format!("delivery state {v:?}"));
        };
        let d: &Descriptor = d;
        Ok(if Received::is(d) {
            Self::Received(<Received as Composite>::from_value(v)?)
        } else if Accepted::is(d) {
            Self::Accepted
        } else if Rejected::is(d) {
            Self::Rejected(<Rejected as Composite>::from_value(v)?)
        } else if Released::is(d) {
            Self::Released
        } else if Modified::is(d) {
            Self::Modified(<Modified as Composite>::from_value(v)?)
        } else if Declared::is(d) {
            Self::Declared(<Declared as Composite>::from_value(v)?)
        } else if TransactionalState::is(d) {
            Self::Transactional(<TransactionalState as Composite>::from_value(v)?)
        } else {
            return invalid(format!("delivery state {d:?}"));
        })
    }

    fn to_value(&self) -> Value {
        match self {
            Self::Received(s) => Composite::to_value(s),
            Self::Accepted => Composite::to_value(&Accepted {}),
            Self::Rejected(s) => Composite::to_value(s),
            Self::Released => Composite::to_value(&Released {}),
            Self::Modified(s) => Composite::to_value(s),
            Self::Declared(s) => Composite::to_value(s),
            Self::Transactional(s) => Composite::to_value(s),
        }
    }
}

composite! {
    /// `header`.
    pub struct Header(0x70, "amqp:header:list") {
        opt durable: bool,
        opt priority: u8,
        /// Milliseconds.
        opt ttl: u32,
        opt first_acquirer: bool,
        opt delivery_count: u32,
    }
}

composite! {
    /// `properties`.
    pub struct Properties(0x73, "amqp:properties:list") {
        /// ulong, uuid, binary or string.
        opt message_id: Value,
        opt user_id: Vec<u8>,
        opt to: String,
        opt subject: String,
        opt reply_to: String,
        opt correlation_id: Value,
        opt content_type: Symbol,
        opt content_encoding: Symbol,
        opt absolute_expiry_time: Timestamp,
        opt creation_time: Timestamp,
        opt group_id: String,
        opt group_sequence: u32,
        opt reply_to_group_id: String,
    }
    // FastPathPropertiesType always writes list32.
    form(_s) { crate::composite::ListForm::Wide }
}

/// Section descriptors.
pub mod section {
    pub const HEADER: u64 = 0x70;
    pub const DELIVERY_ANNOTATIONS: u64 = 0x71;
    pub const MESSAGE_ANNOTATIONS: u64 = 0x72;
    pub const PROPERTIES: u64 = 0x73;
    pub const APPLICATION_PROPERTIES: u64 = 0x74;
    pub const DATA: u64 = 0x75;
    pub const AMQP_SEQUENCE: u64 = 0x76;
    pub const AMQP_VALUE: u64 = 0x77;
    pub const FOOTER: u64 = 0x78;
}

/// Lifetime policies of dynamic nodes.
pub mod lifetime {
    pub const DELETE_ON_CLOSE: u64 = 0x2b;
    pub const DELETE_ON_NO_LINKS: u64 = 0x2c;
    pub const DELETE_ON_NO_MESSAGES: u64 = 0x2d;
    pub const DELETE_ON_NO_LINKS_OR_MESSAGES: u64 = 0x2e;
}
