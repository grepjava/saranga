//! Client defaults. Ported from `ActiveMQClient` and the parts of
//! `ActiveMQDefaultConfiguration` the client reads (via `ServerLocatorImpl`).

use std::time::Duration;

/// `DEFAULT_CLIENT_FAILURE_CHECK_PERIOD` (30 s): the ping and failure-check period.
pub const CLIENT_FAILURE_CHECK_PERIOD: Duration = Duration::from_millis(30_000);
/// `DEFAULT_CONNECTION_TTL` (60 s), sent in every `Ping`.
pub const CONNECTION_TTL: Duration = Duration::from_millis(60_000);
/// `DEFAULT_CALL_TIMEOUT` (30 s): how long a blocking call waits.
pub const CALL_TIMEOUT: Duration = Duration::from_millis(30_000);
/// `DEFAULT_MIN_LARGE_MESSAGE_SIZE` (100 KiB).
pub const MIN_LARGE_MESSAGE_SIZE: i32 = 100 * 1024;
/// `DEFAULT_COMPRESS_LARGE_MESSAGES`.
pub const COMPRESS_LARGE_MESSAGES: bool = false;
/// `DEFAULT_COMPRESSION_LEVEL`: -1, `Deflater.DEFAULT_COMPRESSION`.
pub const COMPRESSION_LEVEL: i32 = -1;
/// `DEFAULT_CONSUMER_WINDOW_SIZE` (1 MiB).
pub const CONSUMER_WINDOW_SIZE: i32 = 1024 * 1024;
/// `DEFAULT_CONFIRMATION_WINDOW_SIZE`: -1, no resend cache.
pub const CONFIRMATION_WINDOW_SIZE: i32 = -1;
/// `DEFAULT_PRODUCER_WINDOW_SIZE` (64 KiB).
pub const PRODUCER_WINDOW_SIZE: i32 = 64 * 1024;
/// `DEFAULT_BLOCK_ON_ACKNOWLEDGE`.
pub const BLOCK_ON_ACKNOWLEDGE: bool = false;
/// `DEFAULT_BLOCK_ON_DURABLE_SEND`.
pub const BLOCK_ON_DURABLE_SEND: bool = true;
/// `DEFAULT_BLOCK_ON_NON_DURABLE_SEND`.
pub const BLOCK_ON_NON_DURABLE_SEND: bool = false;
/// `DEFAULT_ACK_BATCH_SIZE` (1 MiB).
pub const ACK_BATCH_SIZE: i32 = 1024 * 1024;
/// `DEFAULT_PRE_ACKNOWLEDGE`.
pub const PRE_ACKNOWLEDGE: bool = false;
/// `ActiveMQDefaultConfiguration.getDefaultMaxQueueConsumers()`.
pub const MAX_QUEUE_CONSUMERS: i32 = -1;
/// `ActiveMQDefaultConfiguration.getDefaultPurgeOnNoConsumers()`.
pub const PURGE_ON_NO_CONSUMERS: bool = false;
/// `ActiveMQDefaultConfiguration.getDefaultConsumerPriority()`.
pub const CONSUMER_PRIORITY: i32 = 0;
