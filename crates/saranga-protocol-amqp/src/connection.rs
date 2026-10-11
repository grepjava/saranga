//! An AMQP connection.

use std::sync::Arc;

use saranga_server::Broker;
use saranga_server::protocol::Connection as Accepted;

use crate::{AmqpConfig, Settings};

/// Serves one connection until it closes.
pub async fn serve(
    _broker: Arc<Broker>,
    _connection: Accepted,
    _settings: Settings,
    _config: AmqpConfig,
) {
    tracing::warn!("AMQP connections are not served yet");
}
