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
//! Not supported: large messages (sends fail with [`Error::LargeMessage`];
//! received large messages are dropped after returning their credits), XA,
//! a confirmation window (resend cache), failover and reconnection, and
//! message handlers (use [`Consumer::receive`]).
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
pub mod message;
mod producer;
mod session;
mod uuid;

pub use connection::{Connection, ConnectionOptions};
pub use consumer::{Consumer, ConsumerOptions, Delivery, FORCED_DELIVERY_MESSAGE};
pub use error::{Error, ErrorType, Result};
pub use message::MessageExt;
pub use producer::Producer;
pub use saranga_buffer::{PropertyValue, SimpleString};
pub use saranga_message::{CoreMessage, message_type};
pub use session::{
    AddressQuery, QueueConfiguration, QueueQuery, RoutingType, Session, SessionOptions,
};
