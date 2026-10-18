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

    /// The constant's Java name, as `[code=...]` in log lines shows it
    /// (Artemis's own spelling, `NOT_IMPLEMTNED_EXCEPTION` included).
    pub fn name(self) -> &'static str {
        match self {
            Self::InternalError => "INTERNAL_ERROR",
            Self::UnsupportedPacket => "UNSUPPORTED_PACKET",
            Self::NotConnected => "NOT_CONNECTED",
            Self::ConnectionTimedOut => "CONNECTION_TIMEDOUT",
            Self::Disconnected => "DISCONNECTED",
            Self::Unblocked => "UNBLOCKED",
            Self::IoError => "IO_ERROR",
            Self::QueueDoesNotExist => "QUEUE_DOES_NOT_EXIST",
            Self::QueueExists => "QUEUE_EXISTS",
            Self::ObjectClosed => "OBJECT_CLOSED",
            Self::InvalidFilterExpression => "INVALID_FILTER_EXPRESSION",
            Self::IllegalState => "ILLEGAL_STATE",
            Self::SecurityException => "SECURITY_EXCEPTION",
            Self::AddressDoesNotExist => "ADDRESS_DOES_NOT_EXIST",
            Self::AddressExists => "ADDRESS_EXISTS",
            Self::IncompatibleClientServerVersions => "INCOMPATIBLE_CLIENT_SERVER_VERSIONS",
            Self::LargeMessageErrorBody => "LARGE_MESSAGE_ERROR_BODY",
            Self::TransactionRolledBack => "TRANSACTION_ROLLED_BACK",
            Self::SessionCreationRejected => "SESSION_CREATION_REJECTED",
            Self::DuplicateIdRejected => "DUPLICATE_ID_REJECTED",
            Self::DuplicateMetadata => "DUPLICATE_METADATA",
            Self::TransactionOutcomeUnknown => "TRANSACTION_OUTCOME_UNKNOWN",
            Self::AlreadyReplicating => "ALREADY_REPLICATING",
            Self::InterceptorRejectedPacket => "INTERCEPTOR_REJECTED_PACKET",
            Self::InvalidTransientQueueUse => "INVALID_TRANSIENT_QUEUE_USE",
            Self::RemoteDisconnect => "REMOTE_DISCONNECT",
            Self::TransactionTimeout => "TRANSACTION_TIMEOUT",
            Self::NativeErrorInternal => "NATIVE_ERROR_INTERNAL",
            Self::NativeErrorInvalidBuffer => "NATIVE_ERROR_INVALID_BUFFER",
            Self::NativeErrorNotAligned => "NATIVE_ERROR_NOT_ALIGNED",
            Self::NativeErrorCantInitializeAio => "NATIVE_ERROR_CANT_INITIALIZE_AIO",
            Self::NativeErrorCantReleaseAio => "NATIVE_ERROR_CANT_RELEASE_AIO",
            Self::NativeErrorCantOpenCloseFile => "NATIVE_ERROR_CANT_OPEN_CLOSE_FILE",
            Self::NativeErrorCantAllocateQueue => "NATIVE_ERROR_CANT_ALLOCATE_QUEUE",
            Self::NativeErrorPreallocateFile => "NATIVE_ERROR_PREALLOCATE_FILE",
            Self::NativeErrorAllocateMemory => "NATIVE_ERROR_ALLOCATE_MEMORY",
            Self::AddressFull => "ADDRESS_FULL",
            Self::LargeMessageInterrupted => "LARGE_MESSAGE_INTERRUPTED",
            Self::ClusterSecurityException => "CLUSTER_SECURITY_EXCEPTION",
            Self::NotImplemented => "NOT_IMPLEMTNED_EXCEPTION",
            Self::MaxConsumerLimitExceeded => "MAX_CONSUMER_LIMIT_EXCEEDED",
            Self::UnexpectedRoutingTypeForAddress => "UNEXPECTED_ROUTING_TYPE_FOR_ADDRESS",
            Self::InvalidQueueConfiguration => "INVALID_QUEUE_CONFIGURATION",
            Self::DeleteAddressError => "DELETE_ADDRESS_ERROR",
            Self::NullRef => "NULL_REF",
            Self::ShutdownError => "SHUTDOWN_ERROR",
            Self::ReplicationTimeoutError => "REPLICATION_TIMEOUT_ERROR",
            Self::DivertDoesNotExist => "DIVERT_DOES_NOT_EXIST",
            Self::RoutingException => "ROUTING_EXCEPTION",
            Self::TimeoutException => "TIMEOUT_EXCEPTION",
            Self::GenericException => "GENERIC_EXCEPTION",
        }
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
