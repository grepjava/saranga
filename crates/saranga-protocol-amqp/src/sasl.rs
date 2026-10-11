//! SASL frames (part 5.3 of the specification).

use crate::codec::{Result, invalid};
use crate::composite::{Composite, composite};
use crate::types::{Symbol, Value};

composite! {
    /// `sasl-mechanisms`.
    pub struct SaslMechanisms(0x40, "amqp:sasl-mechanisms:list") {
        req sasl_server_mechanisms: Vec<Symbol>,
    }
}

composite! {
    /// `sasl-init`.
    pub struct SaslInit(0x41, "amqp:sasl-init:list") {
        req mechanism: Symbol,
        opt initial_response: Vec<u8>,
        opt hostname: String,
    }
}

composite! {
    /// `sasl-challenge`.
    pub struct SaslChallenge(0x42, "amqp:sasl-challenge:list") {
        req challenge: Vec<u8>,
    }
}

composite! {
    /// `sasl-response`.
    pub struct SaslResponse(0x43, "amqp:sasl-response:list") {
        req response: Vec<u8>,
    }
}

composite! {
    /// `sasl-outcome`.
    pub struct SaslOutcome(0x44, "amqp:sasl-outcome:list") {
        req code: u8,
        opt additional_data: Vec<u8>,
    }
}

/// `sasl-code`.
pub mod code {
    pub const OK: u8 = 0;
    pub const AUTH: u8 = 1;
    pub const SYS: u8 = 2;
    pub const SYS_PERM: u8 = 3;
    pub const SYS_TEMP: u8 = 4;
}

/// A SASL frame's body.
#[derive(Debug, Clone, PartialEq)]
pub enum SaslFrame {
    Mechanisms(SaslMechanisms),
    Init(SaslInit),
    Challenge(SaslChallenge),
    Response(SaslResponse),
    Outcome(SaslOutcome),
}

impl SaslFrame {
    pub fn from_value(v: Value) -> Result<Self> {
        let Value::Described(d, _) = &v else {
            return invalid(format!("SASL frame {v:?}"));
        };
        Ok(if SaslMechanisms::is(d) {
            Self::Mechanisms(SaslMechanisms::from_value(v)?)
        } else if SaslInit::is(d) {
            Self::Init(SaslInit::from_value(v)?)
        } else if SaslChallenge::is(d) {
            Self::Challenge(SaslChallenge::from_value(v)?)
        } else if SaslResponse::is(d) {
            Self::Response(SaslResponse::from_value(v)?)
        } else if SaslOutcome::is(d) {
            Self::Outcome(SaslOutcome::from_value(v)?)
        } else {
            return invalid(format!("SASL frame {d:?}"));
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Mechanisms(f) => f.encode(out),
            Self::Init(f) => f.encode(out),
            Self::Challenge(f) => f.encode(out),
            Self::Response(f) => f.encode(out),
            Self::Outcome(f) => f.encode(out),
        }
    }
}
