//! `AMQPFederationConstants` and the federation's names from
//! `AMQPFederationPolicySupport`.

pub const FEDERATION_VERSION: &str = "federation_version";
pub const FEDERATION_V1: i32 = 1;
pub const FEDERATION_V2: i32 = 2;
pub const FEDERATION_BASE_VALIDATION_ADDRESS: &str = "$ACTIVEMQ_ARTEMIS_FEDERATION";
pub const FEDERATION_CONTROL_LINK_PREFIX: &str = "control";
pub const FEDERATION_EVENTS_LINK_PREFIX: &str = "events";
pub const FEDERATION_CONTROL_LINK: &str = "AMQ_FEDERATION_CONTROL_LINK";
pub const FEDERATION_EVENT_LINK: &str = "AMQ_FEDERATION_EVENT_LINK";
pub const FEDERATION_CONFIGURATION: &str = "federation-configuration";
pub const FEDERATION_QUEUE_RECEIVER: &str = "AMQ_FEDERATION_QUEUE_RECEIVER";
pub const FEDERATION_ADDRESS_RECEIVER: &str = "AMQ_FEDERATION_ADDRESS_RECEIVER";
pub const FEDERATION_RECEIVER_PRIORITY: &str = "priority";
pub const FEDERATION_NAME: &str = "federationName";
pub const FEDERATION_POLICY_NAME: &str = "federationPolicyName";
/// `FederationConstants.FEDERATION_NAME`: the session metadata of a
/// federation's sessions.
pub const FEDERATION_NAME_METADATA: &str = "federation-name";

// Configuration keys.
pub const LINK_ATTACH_TIMEOUT: &str = "attach-timeout";
pub const RECEIVER_CREDITS: &str = "amqpCredits";
pub const RECEIVER_CREDITS_LOW: &str = "amqpLowCredits";
pub const PULL_RECEIVER_BATCH_SIZE: &str = "amqpPullConsumerCredits";
pub const LARGE_MESSAGE_THRESHOLD: &str = "minLargeMessageSize";
pub const IGNORE_ADDRESS_BINDING_FILTERS: &str = "ignoreAddressBindingFilters";
pub const IGNORE_QUEUE_CONSUMER_FILTERS: &str = "ignoreQueueConsumerFilters";
pub const IGNORE_QUEUE_CONSUMER_PRIORITIES: &str = "ignoreQueueConsumerPriorities";
pub const USE_MODIFIED_FOR_TRANSIENT_DELIVERY_ERRORS: &str =
    "amqpUseModifiedForTransientDeliveryErrors";
pub const RECEIVER_LINK_QUIESCE_TIMEOUT: &str = "amqpLinkQuiesceTimeout";
pub const RECEIVER_DRAIN_ON_TRANSIENT_DELIVERY_ERRORS: &str = "amqpDrainOnTransientDeliveryErrors";
pub const RECEIVER_QUIESCE_TIMEOUT: &str = "receiverQuiesceTimeout";
pub const ADDRESS_RECEIVER_IDLE_TIMEOUT: &str = "addressReceiverIdleTimeout";
pub const QUEUE_RECEIVER_IDLE_TIMEOUT: &str = "queueReceiverIdleTimeout";
/// `AmqpSupport.TUNNEL_CORE_MESSAGES`.
pub const TUNNEL_CORE_MESSAGES: &str = "tunnel-core-messages";

// Control messages.
pub const OPERATION_TYPE: &str = "x-opt-amq-federation-op-type";
pub const ADD_QUEUE_POLICY: &str = "ADD_QUEUE_POLICY";
pub const ADD_ADDRESS_POLICY: &str = "ADD_ADDRESS_POLICY";
pub const POLICY_NAME: &str = "policy-name";
pub const QUEUE_INCLUDES: &str = "queue-includes";
pub const QUEUE_EXCLUDES: &str = "queue-excludes";
pub const QUEUE_INCLUDE_FEDERATED: &str = "include-federated";
pub const QUEUE_PRIORITY_ADJUSTMENT: &str = "priority-adjustment";
pub const ADDRESS_INCLUDES: &str = "address-includes";
pub const ADDRESS_EXCLUDES: &str = "address-excludes";
pub const ADDRESS_AUTO_DELETE: &str = "auto-delete";
pub const ADDRESS_AUTO_DELETE_DELAY: &str = "auto-delete-delay";
pub const ADDRESS_AUTO_DELETE_MSG_COUNT: &str = "auto-delete-msg-count";
pub const ADDRESS_MAX_HOPS: &str = "max-hops";
pub const ADDRESS_ENABLE_DIVERT_BINDINGS: &str = "enable-divert-bindings";
pub const ADDRESS_ALLOW_WILDCARD_GROUPINGS: &str = "allow-wildcard-groupings";
pub const POLICY_PROPERTIES_MAP: &str = "policy-properties-map";
pub const TRANSFORMER_CLASS_NAME: &str = "transformer-class-name";
pub const TRANSFORMER_PROPERTIES_MAP: &str = "transformer-properties-map";

// Events.
pub const EVENT_TYPE: &str = "x-opt-amq-federation-ev-type";
pub const REQUESTED_QUEUE_ADDED: &str = "REQUESTED_QUEUE_ADDED_EVENT";
pub const REQUESTED_ADDRESS_ADDED: &str = "REQUESTED_ADDRESS_ADDED_EVENT";
pub const REQUESTED_QUEUE_NAME: &str = "REQUESTED_QUEUE_NAME";
pub const REQUESTED_ADDRESS_NAME: &str = "REQUESTED_ADDRESS_NAME";

// Federated messages.
pub const MESSAGE_HOPS_ANNOTATION: &str = "x-opt-amq-fed-hops";
pub const MESSAGE_HOPS_PROPERTY: &str = "_AMQ_Fed_Hops";
pub const FEDERATED_ADDRESS_SOURCE_PROPERTIES: &str = "federated-address-source-properties";

/// `AmqpSupport.LIFETIME_POLICY`.
pub const LIFETIME_POLICY: &str = "lifetime-policy";
