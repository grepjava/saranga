//! Client errors. Ported from `ActiveMQException`, `ActiveMQExceptionType`
//! and the client-side messages of `ActiveMQClientMessageBundle`.

use std::sync::Arc;

use saranga_buffer::DecodeError;

/// `ActiveMQExceptionType`, shared with the broker (ADR 0003).
pub use saranga_core_protocol::ErrorType;

/// A client error. Broker exceptions and the client-side failures Artemis
/// reports as `ActiveMQException` keep their exception code.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An `ActiveMQException`: from an `ActiveMQExceptionMessage` sent by the
    /// broker, or raised by the client with the code Artemis uses.
    #[error("{message} (code {code})")]
    ActiveMq { code: i32, message: String },
    /// The transport failed to connect.
    #[error("I/O error: {0}")]
    Io(Arc<std::io::Error>),
    /// A frame from the broker did not decode.
    #[error("decode error: {0}")]
    Decode(#[from] DecodeError),
    /// The message would be sent as a large message, which this client does
    /// not support (see [`crate::Producer::send`]).
    #[error(
        "message body of {body_size} bytes exceeds min-large-message-size {min_large_message_size}; large messages are not supported"
    )]
    LargeMessage {
        body_size: usize,
        min_large_message_size: i32,
    },
    /// A broker answer of a type the call did not expect.
    #[error("unexpected response packet {0}")]
    UnexpectedResponse(&'static str),
    /// A caller-supplied value Artemis rejects with `IllegalArgumentException`.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

impl Error {
    pub(crate) fn amq(kind: ErrorType, message: impl Into<String>) -> Self {
        Self::ActiveMq {
            code: kind.code(),
            message: message.into(),
        }
    }

    /// The Artemis exception code, when this is an `ActiveMQException`.
    pub fn code(&self) -> Option<i32> {
        match self {
            Self::ActiveMq { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// The Artemis exception type (`ActiveMQExceptionType.getType`:
    /// unknown codes are `GenericException`).
    pub fn kind(&self) -> Option<ErrorType> {
        self.code().map(ErrorType::from_code)
    }

    // `ActiveMQClientMessageBundle` messages the client raises itself.

    pub(crate) fn channel_disconnected() -> Self {
        Self::amq(ErrorType::NotConnected, "AMQ219006: Channel disconnected")
    }

    pub(crate) fn connection_destroyed() -> Self {
        Self::amq(
            ErrorType::NotConnected,
            "AMQ219010: Connection is destroyed",
        )
    }

    pub(crate) fn connection_timed_out() -> Self {
        Self::amq(
            ErrorType::ConnectionTimedOut,
            "AMQ219011: Did not receive data from server",
        )
    }

    pub(crate) fn topology_timed_out() -> Self {
        Self::amq(
            ErrorType::ConnectionTimedOut,
            "AMQ219013: Timed out waiting to receive cluster topology",
        )
    }

    pub(crate) fn timed_out_sending_packet(timeout_ms: u128, packet_type: i8) -> Self {
        Self::amq(
            ErrorType::ConnectionTimedOut,
            format!(
                "AMQ219014: Timed out after waiting {timeout_ms} ms for response when sending packet {packet_type}"
            ),
        )
    }

    pub(crate) fn disconnected() -> Self {
        Self::amq(
            ErrorType::Disconnected,
            "AMQ219015: The connection was disconnected because of server shutdown",
        )
    }

    pub(crate) fn unblocking_a_call(cause: &Error) -> Self {
        Self::amq(
            ErrorType::Unblocked,
            format!(
                "AMQ219016: Connection failure detected. Unblocking a blocking call that will never get a response ({cause})"
            ),
        )
    }

    pub(crate) fn consumer_closed() -> Self {
        Self::amq(ErrorType::ObjectClosed, "AMQ219017: Consumer is closed")
    }

    pub(crate) fn producer_closed() -> Self {
        Self::amq(ErrorType::ObjectClosed, "AMQ219018: Producer is closed")
    }

    pub(crate) fn session_closed() -> Self {
        Self::amq(ErrorType::ObjectClosed, "AMQ219019: Session is closed")
    }

    pub(crate) fn address_is_full(address: &str, size: i32) -> Self {
        Self::amq(
            ErrorType::AddressFull,
            format!("AMQ219058: Address \"{address}\" is full. Message encode size = {size}B"),
        )
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(Arc::new(e))
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_roundtrip() {
        for code in [0, 5, 100, 113, 210, 223, 999] {
            assert_eq!(ErrorType::from_code(code).code(), code);
        }
        assert_eq!(ErrorType::from_code(207), ErrorType::GenericException);
        let e = Error::amq(ErrorType::QueueDoesNotExist, "x");
        assert_eq!(e.code(), Some(100));
        assert_eq!(e.kind(), Some(ErrorType::QueueDoesNotExist));
    }
}
