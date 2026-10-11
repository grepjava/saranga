//! Transport performatives (part 2 of the specification).

use crate::codec::{Result, invalid};
use crate::composite::{Composite, Field, Fields, ListForm, composite};
use crate::messaging::{DeliveryState, Source, TargetKind};
use crate::types::{Symbol, Value};

/// `role`: a boolean, `true` for the receiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Role {
    #[default]
    Sender,
    Receiver,
}

impl Field for Role {
    fn from_value(v: Value) -> Result<Self> {
        Ok(if bool::from_value(v)? {
            Role::Receiver
        } else {
            Role::Sender
        })
    }
    fn to_value(&self) -> Value {
        Value::Boolean(*self == Role::Receiver)
    }
}

/// `sender-settle-mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SenderSettleMode {
    Unsettled,
    Settled,
    #[default]
    Mixed,
}

impl Field for SenderSettleMode {
    fn from_value(v: Value) -> Result<Self> {
        match u8::from_value(v)? {
            0 => Ok(Self::Unsettled),
            1 => Ok(Self::Settled),
            2 => Ok(Self::Mixed),
            n => invalid(format!("sender-settle-mode {n}")),
        }
    }
    fn to_value(&self) -> Value {
        Value::Ubyte(*self as u8)
    }
}

/// `receiver-settle-mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReceiverSettleMode {
    #[default]
    First,
    Second,
}

impl Field for ReceiverSettleMode {
    fn from_value(v: Value) -> Result<Self> {
        match u8::from_value(v)? {
            0 => Ok(Self::First),
            1 => Ok(Self::Second),
            n => invalid(format!("receiver-settle-mode {n}")),
        }
    }
    fn to_value(&self) -> Value {
        Value::Ubyte(*self as u8)
    }
}

composite! {
    /// `error`.
    pub struct Error(0x1d, "amqp:error:list") {
        req condition: Symbol,
        opt description: String,
        opt info: Fields,
    }
}

impl Error {
    pub fn new(condition: &str, description: impl Into<Option<String>>) -> Self {
        Self {
            condition: Symbol::from(condition),
            description: description.into(),
            info: None,
        }
    }
}

composite! {
    /// `open`.
    pub struct Open(0x10, "amqp:open:list") {
        req container_id: String,
        opt hostname: String,
        def max_frame_size: u32 = u32::MAX,
        def channel_max: u16 = u16::MAX,
        /// Milliseconds.
        opt idle_time_out: u32,
        opt outgoing_locales: Vec<Symbol>,
        opt incoming_locales: Vec<Symbol>,
        opt offered_capabilities: Vec<Symbol>,
        opt desired_capabilities: Vec<Symbol>,
        opt properties: Fields,
    }
}

composite! {
    /// `begin`.
    pub struct Begin(0x11, "amqp:begin:list") {
        opt remote_channel: u16,
        req next_outgoing_id: u32,
        req incoming_window: u32,
        req outgoing_window: u32,
        def handle_max: u32 = u32::MAX,
        opt offered_capabilities: Vec<Symbol>,
        opt desired_capabilities: Vec<Symbol>,
        opt properties: Fields,
    }
}

composite! {
    /// `attach`.
    pub struct Attach(0x12, "amqp:attach:list") {
        req name: String,
        req handle: u32,
        req role: Role,
        def snd_settle_mode: SenderSettleMode = SenderSettleMode::Mixed,
        def rcv_settle_mode: ReceiverSettleMode = ReceiverSettleMode::First,
        opt source: Source,
        opt target: TargetKind,
        opt unsettled: Value,
        def incomplete_unsettled: bool = false,
        opt initial_delivery_count: u32,
        opt max_message_size: u64,
        opt offered_capabilities: Vec<Symbol>,
        opt desired_capabilities: Vec<Symbol>,
        opt properties: Fields,
    }
}

composite! {
    /// `flow`.
    pub struct Flow(0x13, "amqp:flow:list") {
        opt next_incoming_id: u32,
        req incoming_window: u32,
        req next_outgoing_id: u32,
        req outgoing_window: u32,
        opt handle: u32,
        opt delivery_count: u32,
        opt link_credit: u32,
        opt available: u32,
        def drain: bool = false,
        def echo: bool = false,
        opt properties: Fields,
    }
    // FastPathFlowType
    form(s) { if s.properties.is_some() { ListForm::Wide } else { ListForm::Sized } }
}

composite! {
    /// `transfer`. The message bytes follow it in the frame.
    pub struct Transfer(0x14, "amqp:transfer:list") {
        req handle: u32,
        opt delivery_id: u32,
        opt delivery_tag: Vec<u8>,
        opt message_format: u32,
        opt settled: bool,
        def more: bool = false,
        opt rcv_settle_mode: ReceiverSettleMode,
        opt state: DeliveryState,
        def resume: bool = false,
        def aborted: bool = false,
        def batchable: bool = false,
    }
    // FastPathTransferType
    form(s) {
        if s.state.is_some() || s.delivery_tag.as_ref().is_some_and(|t| t.len() > 200) {
            ListForm::Wide
        } else {
            ListForm::Sized
        }
    }
}

composite! {
    /// `disposition`.
    pub struct Disposition(0x15, "amqp:disposition:list") {
        req role: Role,
        req first: u32,
        opt last: u32,
        def settled: bool = false,
        opt state: DeliveryState,
        def batchable: bool = false,
    }
    // FastPathDispositionType: the shared Accepted and Released instances
    // are written in the short form.
    form(s) {
        match s.state {
            None | Some(DeliveryState::Accepted) | Some(DeliveryState::Released) => ListForm::Sized,
            Some(_) => ListForm::Wide,
        }
    }
}

composite! {
    /// `detach`.
    pub struct Detach(0x16, "amqp:detach:list") {
        req handle: u32,
        def closed: bool = false,
        opt error: Error,
    }
}

composite! {
    /// `end`.
    pub struct End(0x17, "amqp:end:list") {
        opt error: Error,
    }
}

composite! {
    /// `close`.
    pub struct Close(0x18, "amqp:close:list") {
        opt error: Error,
    }
}

/// A frame's performative.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)] // one per frame, short-lived
pub enum Performative {
    Open(Open),
    Begin(Begin),
    Attach(Attach),
    Flow(Flow),
    Transfer(Transfer),
    Disposition(Disposition),
    Detach(Detach),
    End(End),
    Close(Close),
}

impl Performative {
    pub fn from_value(v: Value) -> Result<Self> {
        let Value::Described(d, body) = v else {
            return invalid(format!("performative {v:?}"));
        };
        let code = match &*d {
            crate::types::Descriptor::Code(c) => *c,
            crate::types::Descriptor::Name(n) => match n.as_str() {
                Open::NAME => Open::CODE,
                Begin::NAME => Begin::CODE,
                Attach::NAME => Attach::CODE,
                Flow::NAME => Flow::CODE,
                Transfer::NAME => Transfer::CODE,
                Disposition::NAME => Disposition::CODE,
                Detach::NAME => Detach::CODE,
                End::NAME => End::CODE,
                Close::NAME => Close::CODE,
                other => return invalid(format!("performative {other}")),
            },
        };
        let d = crate::types::Descriptor::Code(code);
        Ok(match code {
            Open::CODE => Self::Open(Open::from_described(&d, *body)?),
            Begin::CODE => Self::Begin(Begin::from_described(&d, *body)?),
            Attach::CODE => Self::Attach(Attach::from_described(&d, *body)?),
            Flow::CODE => Self::Flow(Flow::from_described(&d, *body)?),
            Transfer::CODE => Self::Transfer(Transfer::from_described(&d, *body)?),
            Disposition::CODE => Self::Disposition(Disposition::from_described(&d, *body)?),
            Detach::CODE => Self::Detach(Detach::from_described(&d, *body)?),
            End::CODE => Self::End(End::from_described(&d, *body)?),
            Close::CODE => Self::Close(Close::from_described(&d, *body)?),
            other => return invalid(format!("performative 0x{other:x}")),
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Open(p) => p.encode(out),
            Self::Begin(p) => p.encode(out),
            Self::Attach(p) => p.encode(out),
            Self::Flow(p) => p.encode(out),
            Self::Transfer(p) => p.encode(out),
            Self::Disposition(p) => p.encode(out),
            Self::Detach(p) => p.encode(out),
            Self::End(p) => p.encode(out),
            Self::Close(p) => p.encode(out),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Open(_) => "open",
            Self::Begin(_) => "begin",
            Self::Attach(_) => "attach",
            Self::Flow(_) => "flow",
            Self::Transfer(_) => "transfer",
            Self::Disposition(_) => "disposition",
            Self::Detach(_) => "detach",
            Self::End(_) => "end",
            Self::Close(_) => "close",
        }
    }
}

/// Error conditions (`AmqpError`, `ConnectionError`, `SessionError`,
/// `LinkError`).
pub mod condition {
    pub const INTERNAL_ERROR: &str = "amqp:internal-error";
    pub const NOT_FOUND: &str = "amqp:not-found";
    pub const UNAUTHORIZED_ACCESS: &str = "amqp:unauthorized-access";
    pub const DECODE_ERROR: &str = "amqp:decode-error";
    pub const RESOURCE_LIMIT_EXCEEDED: &str = "amqp:resource-limit-exceeded";
    pub const NOT_ALLOWED: &str = "amqp:not-allowed";
    pub const INVALID_FIELD: &str = "amqp:invalid-field";
    pub const NOT_IMPLEMENTED: &str = "amqp:not-implemented";
    pub const RESOURCE_LOCKED: &str = "amqp:resource-locked";
    pub const PRECONDITION_FAILED: &str = "amqp:precondition-failed";
    pub const RESOURCE_DELETED: &str = "amqp:resource-deleted";
    pub const ILLEGAL_STATE: &str = "amqp:illegal-state";
    pub const FRAME_SIZE_TOO_SMALL: &str = "amqp:frame-size-too-small";
    pub const CONNECTION_FORCED: &str = "amqp:connection:forced";
    pub const FRAMING_ERROR: &str = "amqp:connection:framing-error";
    pub const REDIRECT: &str = "amqp:connection:redirect";
    pub const WINDOW_VIOLATION: &str = "amqp:session:window-violation";
    pub const ERRANT_LINK: &str = "amqp:session:errant-link";
    pub const HANDLE_IN_USE: &str = "amqp:session:handle-in-use";
    pub const UNATTACHED_HANDLE: &str = "amqp:session:unattached-handle";
    pub const DETACH_FORCED: &str = "amqp:link:detach-forced";
    pub const TRANSFER_LIMIT_EXCEEDED: &str = "amqp:link:transfer-limit-exceeded";
    pub const MESSAGE_SIZE_EXCEEDED: &str = "amqp:link:message-size-exceeded";
    pub const LINK_REDIRECT: &str = "amqp:link:redirect";
    pub const STOLEN: &str = "amqp:link:stolen";
    pub const TRANSACTION_UNKNOWN_ID: &str = "amqp:transaction:unknown-id";
    pub const TRANSACTION_ROLLBACK: &str = "amqp:transaction:rollback";
    pub const TRANSACTION_TIMEOUT: &str = "amqp:transaction:timeout";
}
