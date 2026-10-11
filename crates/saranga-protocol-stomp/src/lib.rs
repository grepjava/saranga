//! STOMP 1.0, 1.1 and 1.2 (`artemis-stomp-protocol`).

pub mod connection;
pub mod convert;
pub mod decoder;
pub mod frame;

use std::sync::Arc;

use saranga_server::Broker;
use saranga_server::protocol::{Connection, ProtocolManager, ServeFuture};

/// The STOMP protocol (`StompProtocolManager`).
pub struct StompProtocol;

impl ProtocolManager for StompProtocol {
    fn name(&self) -> &'static str {
        "STOMP"
    }

    /// A first frame of `CONNECT` or `STOMP`.
    fn is_protocol(&self, header: &[u8]) -> bool {
        header.starts_with(b"CONNECT") || header.starts_with(b"STOMP")
    }

    fn serve(&self, broker: Arc<Broker>, connection: Connection) -> ServeFuture {
        Box::pin(connection::serve(broker, connection))
    }
}
