//! Transactions (part 4 of the specification).

use crate::composite::composite;
use crate::messaging::DeliveryState;
use crate::types::{Symbol, Value};

composite! {
    /// `coordinator`: the target of a link that declares transactions.
    pub struct Coordinator(0x30, "amqp:coordinator:list") {
        opt capabilities: Vec<Symbol>,
    }
}

composite! {
    /// `declare`.
    pub struct Declare(0x31, "amqp:declare:list") {
        opt global_id: Value,
    }
}

composite! {
    /// `discharge`.
    pub struct Discharge(0x32, "amqp:discharge:list") {
        req txn_id: Vec<u8>,
        opt fail: bool,
    }
}

composite! {
    /// `declared`.
    pub struct Declared(0x33, "amqp:declared:list") {
        req txn_id: Vec<u8>,
    }
}

composite! {
    /// `transactional-state`.
    pub struct TransactionalState(0x34, "amqp:transactional-state:list") {
        req txn_id: Vec<u8>,
        opt outcome: Box<DeliveryState>,
    }
}

impl crate::composite::Field for Box<DeliveryState> {
    fn from_value(v: Value) -> crate::codec::Result<Self> {
        DeliveryState::from_value(v).map(Box::new)
    }
    fn to_value(&self) -> Value {
        crate::composite::Field::to_value(&**self)
    }
}

/// Transaction capabilities.
pub mod capability {
    pub const LOCAL: &str = "amqp:local-transactions";
    pub const DISTRIBUTED: &str = "amqp:distributed-transactions";
    pub const PROMOTABLE: &str = "amqp:promotable-transactions";
    pub const MULTI_TXNS_PER_SSN: &str = "amqp:multi-txns-per-ssn";
    pub const MULTI_SSNS_PER_TXN: &str = "amqp:multi-ssns-per-txn";
}
