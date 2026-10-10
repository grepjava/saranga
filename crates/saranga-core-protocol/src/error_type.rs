/// `ActiveMQExceptionType`: the error codes carried by
/// `ActiveMQExceptionMessage`. Clients branch on these codes, so every
/// server error that reaches a client maps to exactly one of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ErrorType {
    InternalError = 0,
    UnsupportedPacket = 1,
    NotConnected = 2,
    ConnectionTimedOut = 3,
    Disconnected = 4,
    Unblocked = 5,
    IoError = 6,
    QueueDoesNotExist = 100,
    QueueExists = 101,
    ObjectClosed = 102,
    InvalidFilterExpression = 103,
    IllegalState = 104,
    SecurityException = 105,
    AddressDoesNotExist = 106,
    AddressExists = 107,
    IncompatibleClientServerVersions = 108,
    LargeMessageErrorBody = 110,
    TransactionRolledBack = 111,
    SessionCreationRejected = 112,
    DuplicateIdRejected = 113,
    DuplicateMetadata = 114,
    TransactionOutcomeUnknown = 115,
    AlreadyReplicating = 116,
    InterceptorRejectedPacket = 117,
    InvalidTransientQueueUse = 118,
    RemoteDisconnect = 119,
    TransactionTimeout = 120,
    NativeErrorInternal = 200,
    NativeErrorInvalidBuffer = 201,
    NativeErrorNotAligned = 202,
    NativeErrorCantInitializeAio = 203,
    NativeErrorCantReleaseAio = 204,
    NativeErrorCantOpenCloseFile = 205,
    NativeErrorCantAllocateQueue = 206,
    NativeErrorPreallocateFile = 208,
    NativeErrorAllocateMemory = 209,
    AddressFull = 210,
    LargeMessageInterrupted = 211,
    ClusterSecurityException = 212,
    NotImplemented = 213,
    MaxConsumerLimitExceeded = 214,
    UnexpectedRoutingTypeForAddress = 215,
    InvalidQueueConfiguration = 216,
    DeleteAddressError = 217,
    NullRef = 218,
    ShutdownError = 219,
    ReplicationTimeoutError = 220,
    DivertDoesNotExist = 221,
    RoutingException = 222,
    TimeoutException = 223,
    GenericException = 999,
}

impl ErrorType {
    const ALL: [Self; 51] = [
        Self::InternalError,
        Self::UnsupportedPacket,
        Self::NotConnected,
        Self::ConnectionTimedOut,
        Self::Disconnected,
        Self::Unblocked,
        Self::IoError,
        Self::QueueDoesNotExist,
        Self::QueueExists,
        Self::ObjectClosed,
        Self::InvalidFilterExpression,
        Self::IllegalState,
        Self::SecurityException,
        Self::AddressDoesNotExist,
        Self::AddressExists,
        Self::IncompatibleClientServerVersions,
        Self::LargeMessageErrorBody,
        Self::TransactionRolledBack,
        Self::SessionCreationRejected,
        Self::DuplicateIdRejected,
        Self::DuplicateMetadata,
        Self::TransactionOutcomeUnknown,
        Self::AlreadyReplicating,
        Self::InterceptorRejectedPacket,
        Self::InvalidTransientQueueUse,
        Self::RemoteDisconnect,
        Self::TransactionTimeout,
        Self::NativeErrorInternal,
        Self::NativeErrorInvalidBuffer,
        Self::NativeErrorNotAligned,
        Self::NativeErrorCantInitializeAio,
        Self::NativeErrorCantReleaseAio,
        Self::NativeErrorCantOpenCloseFile,
        Self::NativeErrorCantAllocateQueue,
        Self::NativeErrorPreallocateFile,
        Self::NativeErrorAllocateMemory,
        Self::AddressFull,
        Self::LargeMessageInterrupted,
        Self::ClusterSecurityException,
        Self::NotImplemented,
        Self::MaxConsumerLimitExceeded,
        Self::UnexpectedRoutingTypeForAddress,
        Self::InvalidQueueConfiguration,
        Self::DeleteAddressError,
        Self::NullRef,
        Self::ShutdownError,
        Self::ReplicationTimeoutError,
        Self::DivertDoesNotExist,
        Self::RoutingException,
        Self::TimeoutException,
        Self::GenericException,
    ];

    pub fn code(self) -> i32 {
        self as i32
    }

    /// `ActiveMQExceptionType.getType`: unknown codes map to
    /// `GenericException`.
    pub fn from_code(code: i32) -> Self {
        Self::ALL
            .into_iter()
            .find(|t| t.code() == code)
            .unwrap_or(Self::GenericException)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_roundtrip() {
        for t in ErrorType::ALL {
            assert_eq!(ErrorType::from_code(t.code()), t);
        }
        assert_eq!(ErrorType::from_code(7), ErrorType::GenericException);
        assert_eq!(ErrorType::QueueDoesNotExist.code(), 100);
    }
}
