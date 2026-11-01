//! A router's targets: the broker itself (`LocalTarget`) and the brokers
//! of its pool (`ActiveMQTarget`), each asked through its management.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use saranga_buffer::json::Json;
use saranga_client::{Connection, ConnectionOptions, Connector, SessionOptions};
use saranga_core_protocol::transport::{ParamValue, TransportConfiguration};

use crate::Broker;
use crate::management::requestor::Requestor;

/// `ResourceNames.BROKER`.
pub const BROKER: &str = "broker";

/// How long a management call on a target waits (3000 ms throughout).
pub const TIMEOUT: Duration = Duration::from_millis(3000);

/// What a target tells the monitor that watches it (`TargetListener`).
pub(crate) trait Listener: Send + Sync {
    fn disconnected(&self);
}

pub struct Target {
    kind: Kind,
    connector: Option<TransportConfiguration>,
    node_id: Mutex<Option<String>>,
    connected: AtomicBool,
    listener: Mutex<Option<Weak<dyn Listener>>>,
}

enum Kind {
    Local(Weak<Broker>),
    Remote {
        username: Option<String>,
        password: Option<String>,
        link: tokio::sync::Mutex<Option<Link>>,
    },
}

/// `ActiveMQTarget`'s session factory and its `ActiveMQManagementProxy`.
struct Link {
    connection: Connection,
    requestor: Requestor,
}

impl Target {
    /// `LocalTarget`: no connector, the node ID the broker's once
    /// connected.
    pub fn local(broker: &Arc<Broker>) -> Arc<Self> {
        Arc::new(Self {
            kind: Kind::Local(Arc::downgrade(broker)),
            connector: None,
            node_id: Mutex::new(None),
            connected: AtomicBool::new(true),
            listener: Mutex::new(None),
        })
    }

    /// `ActiveMQTargetFactory.createTarget`: a broker the pool's
    /// credentials log in to; its node ID, when not given, asked for once
    /// connected.
    pub fn remote(
        connector: TransportConfiguration,
        node_id: Option<String>,
        username: Option<String>,
        password: Option<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            kind: Kind::Remote {
                username,
                password,
                link: tokio::sync::Mutex::new(None),
            },
            connector: Some(connector),
            node_id: Mutex::new(node_id),
            connected: AtomicBool::new(false),
            listener: Mutex::new(None),
        })
    }

    pub fn is_local(&self) -> bool {
        matches!(self.kind, Kind::Local(_))
    }

    pub fn node_id(&self) -> Option<String> {
        self.node_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn set_node_id(&self, id: String) {
        *self.node_id.lock().unwrap_or_else(|e| e.into_inner()) = Some(id);
    }

    pub fn connector(&self) -> Option<&TransportConfiguration> {
        self.connector.as_ref()
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    pub(crate) fn set_listener(&self, listener: Option<Weak<dyn Listener>>) {
        *self.listener.lock().unwrap_or_else(|e| e.into_inner()) = listener;
    }

    fn fire_disconnected(&self) {
        let listener = self
            .listener
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(l) = listener.and_then(|l| l.upgrade()) {
            l.disconnected();
        }
    }

    /// The connector's `host` and `port` (`ConfigurationHelper`, with
    /// Netty's defaults), where the protocols redirect a client.
    pub fn host_port(&self) -> (String, i32) {
        let param = |name: &str| {
            self.connector.as_ref()?.params.iter().find_map(|(k, v)| {
                (k.to_string_lossy() == name).then(|| match v {
                    ParamValue::String(s) => s.to_string_lossy(),
                    ParamValue::Bool(b) => b.to_string(),
                    ParamValue::Int(i) => i.to_string(),
                    ParamValue::Long(l) => l.to_string(),
                })
            })
        };
        let host = param("host").unwrap_or_else(|| "localhost".to_owned());
        let port = param("port")
            .and_then(|p| p.trim().parse().ok())
            .unwrap_or(61616);
        (host, port)
    }

    /// `LocalTarget.connect`: the broker's node ID.
    pub fn connect_local(&self) {
        if let Kind::Local(broker) = &self.kind
            && self.node_id().is_none()
            && let Some(b) = broker.upgrade()
        {
            self.set_node_id(b.node_id().to_owned());
        }
    }

    /// `connect`: the local target takes the broker's node ID; a remote
    /// one logs in, opens its management session (with a client ID the
    /// routers of the other broker leave where it is) and learns the node
    /// ID if it had none.
    pub async fn connect(self: &Arc<Self>) -> Result<(), String> {
        match &self.kind {
            Kind::Local(broker) => {
                if self.node_id().is_none()
                    && let Some(b) = broker.upgrade()
                {
                    self.set_node_id(b.node_id().to_owned());
                }
                Ok(())
            }
            Kind::Remote {
                username,
                password,
                link,
            } => {
                let connector = self
                    .connector
                    .as_ref()
                    .and_then(Connector::from_transport)
                    .ok_or_else(|| format!("{self} has no host and port to connect to"))?;
                let options = ConnectionOptions {
                    user: username.clone(),
                    password: password.clone(),
                    ..ConnectionOptions::default()
                };
                let connection = Connection::connect_connector(connector, options)
                    .await
                    .map_err(|e| e.to_string())?;
                // `connectionFailed`: the target disconnects.
                let weak = Arc::downgrade(self);
                connection.set_failover_listener(move |event| {
                    if matches!(event, saranga_client::FailoverEvent::FailoverFailed)
                        && let Some(t) = weak.upgrade()
                    {
                        tokio::spawn(async move { t.disconnect().await });
                    }
                });
                let session = connection
                    .create_session(SessionOptions {
                        username: username.clone(),
                        password: password.clone(),
                        client_id: Some(format!(
                            "{}{}",
                            super::CLIENT_ID_PREFIX,
                            saranga_client::uuid::to_string(&saranga_client::uuid::time_based())
                        )),
                        ..SessionOptions::default()
                    })
                    .await;
                let requestor = match session {
                    Ok(s) => Requestor::on(s).await,
                    Err(e) => Err(e),
                };
                let requestor = match requestor {
                    Ok(r) => r,
                    Err(e) => {
                        connection.close().await;
                        return Err(e.to_string());
                    }
                };
                *link.lock().await = Some(Link {
                    connection,
                    requestor,
                });
                if self.node_id().is_none() {
                    match self.attribute(BROKER, "NodeID").await {
                        Ok(Json::String(id)) => self.set_node_id(id),
                        Ok(other) => return Err(format!("NodeID: {other}")),
                        Err(e) => return Err(e),
                    }
                }
                self.connected.store(true, Ordering::Release);
                Ok(())
            }
        }
    }

    /// `disconnect`: the management session and the connection close, and
    /// the monitor hears the target is no longer ready.
    pub async fn disconnect(&self) {
        let Kind::Remote { link, .. } = &self.kind else {
            return;
        };
        let link = link.lock().await.take();
        if self.connected.swap(false, Ordering::AcqRel) {
            if let Some(l) = link {
                l.requestor.close().await;
                l.connection.close().await;
            }
            self.fire_disconnected();
        } else if let Some(l) = link {
            // A connect that failed half-way.
            l.connection.close().await;
        }
    }

    /// `checkReadiness`: the local broker always is; a remote one is when
    /// it is active.
    pub async fn check_readiness(&self) -> bool {
        if self.is_local() {
            return true;
        }
        match self.attribute(BROKER, "Active").await {
            Ok(Json::Bool(active)) => active,
            Ok(other) => {
                tracing::warn!("Error on check readiness: Active is {other}");
                false
            }
            Err(e) => {
                tracing::warn!("Error on check readiness: {e}");
                false
            }
        }
    }

    /// `getAttribute` of a management resource.
    pub async fn attribute(&self, resource: &str, name: &str) -> Result<Json, String> {
        match &self.kind {
            Kind::Local(broker) => {
                let broker = broker.upgrade().ok_or("the broker is gone")?;
                broker
                    .management()
                    .attribute(&broker, resource, name, Default::default())
                    .await
            }
            Kind::Remote { link, .. } => {
                let link = link.lock().await;
                let link = link.as_ref().ok_or("not connected")?;
                let reply = link
                    .requestor
                    .attribute(resource, name, TIMEOUT)
                    .await
                    .map_err(|e| e.to_string())?;
                if reply.succeeded {
                    Ok(reply.result.unwrap_or(Json::Null))
                } else {
                    Err(format!(
                        "Failed to get {resource}.{name}. Reason: {}",
                        reply.result.as_ref().map_or_else(
                            || "null".to_owned(),
                            |r| match r {
                                Json::String(s) => s.clone(),
                                other => other.to_string(),
                            }
                        )
                    ))
                }
            }
        }
    }

    /// `TransportConfiguration.toJson`, for `getTargetAsJSON`.
    pub fn connector_json(&self) -> Json {
        let Some(c) = &self.connector else {
            return Json::Null;
        };
        let params = c
            .params
            .iter()
            .map(|(k, v)| {
                let v = match v {
                    ParamValue::String(s) => Json::from(s.to_string_lossy()),
                    ParamValue::Bool(b) => Json::from(*b),
                    ParamValue::Int(i) => Json::from(i64::from(*i)),
                    ParamValue::Long(l) => Json::from(*l),
                };
                (k.to_string_lossy(), v)
            })
            .collect();
        Json::object()
            .with("name", c.name.to_string_lossy())
            .with("factoryClassName", c.factory_class_name.to_string_lossy())
            .with("params", Json::Object(params))
            .with("extraProps", Json::object())
    }
}

/// `AbstractTarget.toString`.
impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let class = if self.is_local() {
            "LocalTarget"
        } else {
            "ActiveMQTarget"
        };
        let connector = self.connector.as_ref().map_or_else(
            || "null".to_owned(),
            crate::replication::quorum::transport_to_string,
        );
        let node_id = self.node_id().unwrap_or_else(|| "null".to_owned());
        write!(f, "{class} [connector={connector}, nodeID={node_id}]")
    }
}
