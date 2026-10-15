//! Connections the broker makes to other brokers (`BrokerConnection`), as
//! management sees them. A protocol provides them: AMQP's broker
//! connections.

use std::sync::{Arc, Mutex};

/// `BrokerConnection`.
pub trait BrokerConnection: Send + Sync {
    fn name(&self) -> &str;
    /// The wire protocol: `AMQP`.
    fn protocol(&self) -> &'static str;
    /// The configuration's URI.
    fn uri(&self) -> &str;
    fn user(&self) -> Option<&str>;
    fn retry_interval(&self) -> i64;
    fn reconnect_attempts(&self) -> i32;
    fn is_started(&self) -> bool;
    fn is_connected(&self) -> bool;
    fn start(&self);
    fn stop(&self);
}

/// The broker's connections to others (`brokerConnectionMap`), in the
/// order they were registered.
#[derive(Default)]
pub struct BrokerConnections(Mutex<Vec<Arc<dyn BrokerConnection>>>);

impl BrokerConnections {
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Arc<dyn BrokerConnection>>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `registerBrokerConnection`: one of the same name is replaced.
    pub fn register(&self, connection: Arc<dyn BrokerConnection>) {
        let mut all = self.lock();
        match all.iter_mut().find(|c| c.name() == connection.name()) {
            Some(slot) => *slot = connection,
            None => all.push(connection),
        }
    }

    /// `unregisterBrokerConnection`.
    pub fn unregister(&self, name: &str) {
        self.lock().retain(|c| c.name() != name);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn BrokerConnection>> {
        self.lock().iter().find(|c| c.name() == name).cloned()
    }

    pub fn all(&self) -> Vec<Arc<dyn BrokerConnection>> {
        self.lock().clone()
    }
}
