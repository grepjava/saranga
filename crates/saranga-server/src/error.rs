//! Errors that reach clients. Each carries the `ActiveMQExceptionType` the
//! client branches on and the `ActiveMQMessageBundle` text, ID included,
//! that Artemis sends (ADR 0003).

use saranga_core_protocol::ErrorType;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct BrokerError {
    pub kind: ErrorType,
    pub message: String,
}

pub type Result<T, E = BrokerError> = std::result::Result<T, E>;

impl BrokerError {
    pub fn new(kind: ErrorType, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn bundle(kind: ErrorType, id: u32, text: String) -> Self {
        Self::new(kind, format!("AMQ{id}: {text}"))
    }

    /// `ActiveMQSecurityException`.
    pub fn security(e: saranga_security::SecurityError) -> Self {
        Self::new(ErrorType::SecurityException, e.0)
    }

    /// `rejectEmptyValidatedUser`.
    pub fn reject_empty_validated_user() -> Self {
        Self::bundle(
            ErrorType::IllegalState,
            229225,
            "Validated User is not set".to_owned(),
        )
    }

    /// `ActiveMQIllegalStateException` with a plain message.
    pub fn illegal_state(text: impl Into<String>) -> Self {
        Self::new(ErrorType::IllegalState, text)
    }

    pub fn internal(text: impl Into<String>) -> Self {
        Self::new(ErrorType::InternalError, text)
    }

    pub fn no_such_queue(name: &str) -> Self {
        Self::bundle(
            ErrorType::QueueDoesNotExist,
            229017,
            format!("Queue {name} does not exist"),
        )
    }

    pub fn queue_already_exists(name: &str, address: &str) -> Self {
        Self::bundle(
            ErrorType::QueueExists,
            229019,
            format!("Queue {name} already exists on address {address}"),
        )
    }

    pub fn invalid_filter(filter: &str) -> Self {
        Self::bundle(
            ErrorType::InvalidFilterExpression,
            229020,
            format!("Invalid filter: {filter}"),
        )
    }

    pub fn cannot_delete_queue_with_consumers(name: &str, consumers: i32) -> Self {
        Self::bundle(
            ErrorType::IllegalState,
            229025,
            format!(
                "Cannot delete queue {name} on binding {name} - it has consumers = {consumers}"
            ),
        )
    }

    pub fn consumer_no_reference(consumer: i64, message: i64, queue: &str) -> Self {
        Self::bundle(
            ErrorType::IllegalState,
            229027,
            format!(
                "Could not find reference on consumerId = {consumer}, messageId = {message} queue = {queue}"
            ),
        )
    }

    pub fn consumer_doesnt_exist(consumer: i64) -> Self {
        Self::bundle(
            ErrorType::IllegalState,
            229028,
            format!("Consumer {consumer} doesn't exist on the server"),
        )
    }

    pub fn no_address() -> Self {
        Self::bundle(
            ErrorType::IllegalState,
            229029,
            "No address configured on the Server's Session".into(),
        )
    }

    pub fn incompatible_client_server() -> Self {
        Self::bundle(
            ErrorType::IncompatibleClientServerVersions,
            229033,
            "Server and client versions incompatible".into(),
        )
    }

    /// `cannotCreateConsumerOnClosedSession`.
    pub fn consumer_on_closed_session(queue: &str) -> Self {
        Self::bundle(
            ErrorType::IllegalState,
            229232,
            format!("Cannot create consumer on {queue}. Session is closed."),
        )
    }

    pub fn duplicate_metadata(key: &str, data: &str) -> Self {
        Self::bundle(
            ErrorType::DuplicateMetadata,
            229035,
            format!("Metadata {key}={data} had been set already"),
        )
    }

    pub fn max_consumer_limit_reached(address: &str, queue: &str) -> Self {
        Self::bundle(
            ErrorType::MaxConsumerLimitExceeded,
            229200,
            format!("Maximum Consumer Limit Reached on Queue:(address={address},queue={queue})"),
        )
    }

    pub fn address_does_not_exist(address: &str) -> Self {
        Self::bundle(
            ErrorType::AddressDoesNotExist,
            229203,
            format!("Address Does Not Exist: {address}"),
        )
    }

    /// A plain `IllegalArgumentException` in Java, so it reaches the client
    /// as an internal error.
    pub fn invalid_routing_type_for_address(
        routing_type: &str,
        address: &str,
        supported: &str,
    ) -> Self {
        Self::bundle(
            ErrorType::InternalError,
            229207,
            format!(
                "Can not create queue with routing type: {routing_type}, Supported routing types for address: {address} are {supported}"
            ),
        )
    }

    /// A plain `IllegalStateException` in Java (see above).
    pub fn invalid_routing_type_delete(routing_type: &str, address: &str) -> Self {
        Self::bundle(
            ErrorType::InternalError,
            229209,
            format!(
                "Can't remove routing type {routing_type}, queues exists for address: {address}. Please delete queues before removing this routing type."
            ),
        )
    }

    /// A plain `IllegalStateException` in Java.
    pub fn invalid_max_consumers_update(queue: &str, max_consumers: i32, consumers: i32) -> Self {
        Self::bundle(
            ErrorType::InternalError,
            229210,
            format!(
                "Can't update queue {queue} with maxConsumers: {max_consumers}. Current consumers are {consumers}."
            ),
        )
    }

    /// A plain `IllegalStateException` in Java.
    pub fn invalid_routing_type_update(
        queue: &str,
        routing_type: &str,
        address: &str,
        supported: &str,
    ) -> Self {
        Self::bundle(
            ErrorType::InternalError,
            229211,
            format!(
                "Can't update queue {queue} with routing type: {routing_type}, Supported routing types for address: {address} are {supported}"
            ),
        )
    }

    pub fn invalid_queue_name(name: &str) -> Self {
        Self::bundle(
            ErrorType::IllegalState,
            229216,
            format!("Invalid queue name: {name}"),
        )
    }

    pub fn transaction_rolled_back() -> Self {
        Self::new(
            ErrorType::TransactionRolledBack,
            "Transaction has been rolled back",
        )
    }
}
