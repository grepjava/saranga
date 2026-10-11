//! AMQP 1.0 (`artemis-amqp-protocol`, and the parts of Proton-J it relies
//! on).
//!
//! - [`types`] and [`codec`]: the type system and its encoding, choosing
//!   the same constructors as Proton-J.
//! - [`composite`]: described list types.
//! - [`transport`], [`messaging`], [`transaction`] and [`sasl`]: the
//!   specification's types.
//! - [`frame`]: frames and protocol headers.

pub mod codec;
pub mod composite;
pub mod frame;
pub mod messaging;
pub mod sasl;
pub mod transaction;
pub mod transport;
pub mod types;

pub use codec::{CodecError, Decoder};
pub use composite::{Composite, Field, Fields, Timestamp};
pub use types::{Array, ArrayType, Descriptor, Symbol, Value};
