//! An async (tokio) Artemis Core protocol client.
//!
//! Ported from the Artemis core client (`artemis-core-client`):
//! `ClientSessionFactoryImpl`, `RemotingConnectionImpl`, `ChannelImpl`,
//! `ActiveMQSessionContext`, `ClientSessionImpl`, `ClientProducerImpl`, the
//! producer credit classes and `ClientConsumerImpl`, with the defaults of
//! `ActiveMQClient` and `ServerLocatorImpl`. Behaviour follows the Java
//! client: the same packets, in the same order, with the same blocking,
//! correlation and flow-control rules.
//!
//! Large messages are sent in chunks and received whole: `receive` returns
//! them once the last chunk arrived, their body buffered in memory (as
//! `ClientLargeMessageImpl` does without an output stream), and inflated if
//! compressed. Streaming bodies to and from `InputStream`/`OutputStream`
//! is not supported.
//!
//! A connection reconnects, or fails over to the backup the broker's
//! topology pairs with its server and then to the other servers it was
//! given, when its options ask for it (`reconnectAttempts`,
//! `failoverAttempts`; see [`Connection::connect_to`]); its sessions are
//! recreated on the new connection with their consumers, as
//! `ClientSessionImpl.handleFailover` does.
//!
//! Not supported: XA, a confirmation window (resend cache, so sessions are
//! recreated rather than reattached), and message handlers (use
//! [`Consumer::receive`]).
//!
//! ```no_run
//! # async fn demo() -> saranga_client::Result<()> {
//! use std::time::Duration;
//! use saranga_client::*;
//!
//! let conn = Connection::connect("127.0.0.1:61616", ConnectionOptions::default()).await?;
//! let session = conn.create_session(SessionOptions::default()).await?;
//! session
//!     .create_queue(QueueConfiguration::new("q").routing_type(RoutingType::Anycast))
//!     .await?;
//! let producer = session.create_producer(Some("q")).await?;
//! producer.send(message::text_message("hello")).await?;
//! let consumer = session.create_consumer("q", None, false).await?;
//! session.start().await?;
//! if let Some(d) = consumer.receive(Duration::from_secs(5)).await? {
//!     consumer.acknowledge(&d).await?;
//! }
//! conn.close().await;
//! # Ok(())
//! # }
//! ```

mod channel;
mod connection;
mod consumer;
pub mod defaults;
mod error;
mod large;
pub mod message;
mod producer;
mod session;
pub mod uuid;

pub use connection::{Connection, ConnectionOptions, Connector, FailoverEvent};
pub use consumer::{Consumer, ConsumerOptions, Delivery, FORCED_DELIVERY_MESSAGE};
pub use error::{Error, ErrorType, Result};
pub use message::MessageExt;
pub use producer::Producer;
pub use saranga_buffer::{PropertyValue, SimpleString};
pub use saranga_message::{CoreMessage, message_type};
pub use session::{
    AddressQuery, QueueConfiguration, QueueQuery, RoutingType, Session, SessionOptions,
};
