//! Wire primitives shared by every Artemis encoding (Core protocol, journal,
//! paging). Ported from `artemis-commons`: `ChannelBufferWrapper`,
//! `SimpleString`, `UTF8Util`, `TypedProperties` and `DataConstants`.
//!
//! All multi-byte integers are big-endian, as in Netty's `ByteBuf`.

mod codec;
pub mod java_hash;
mod jstring;
mod properties;
mod reader;
mod simple_string;
mod writer;

pub use codec::Codec;
pub use jstring::JString;
pub use properties::{PropertyValue, TypedProperties};
pub use reader::Reader;
pub use simple_string::SimpleString;
pub use writer::Writer;

/// `DataConstants` from Artemis.
pub mod constants {
    pub const NULL: u8 = 0;
    pub const NOT_NULL: u8 = 1;
    pub const BOOLEAN: u8 = 2;
    pub const BYTE: u8 = 3;
    pub const BYTES: u8 = 4;
    pub const SHORT: u8 = 5;
    pub const INT: u8 = 6;
    pub const LONG: u8 = 7;
    pub const FLOAT: u8 = 8;
    pub const DOUBLE: u8 = 9;
    pub const STRING: u8 = 10;
    pub const CHAR: u8 = 11;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("unexpected end of buffer: needed {needed} bytes, {remaining} remaining")]
    UnexpectedEof { needed: usize, remaining: usize },
    #[error("invalid encoding: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, DecodeError>;
