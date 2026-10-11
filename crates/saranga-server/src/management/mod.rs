//! Management (`ManagementServiceImpl`): the broker's resources, their
//! attributes and operations, reached through messages sent to the
//! management address from any protocol.
//!
//! A resource is named as `ResourceNames` names it: `broker`,
//! `address.<name>`, `queue.<name>` and `divert.<name>`. An attribute is
//! read as the control's getter would be, and an operation is matched to
//! one of the control's methods by name and parameter count, its
//! parameters converted from JSON as `invokeOperation` converts them.

mod address;
pub mod args;
mod connections;
pub mod json;
mod messages;
mod queue;
mod security;
mod server;
mod transactions;
mod views;

use std::sync::Arc;

use saranga_buffer::{PropertyValue, Reader, SimpleString, Writer};
use saranga_message::{CoreMessage, message_type};

use crate::Broker;
pub use json::Json;
pub(crate) use security::recovered as recovered_security_setting;

/// `ManagementHelper.HDR_RESOURCE_NAME`.
pub const HDR_RESOURCE_NAME: &str = "_AMQ_ResourceName";
/// `ManagementHelper.HDR_ATTRIBUTE`.
pub const HDR_ATTRIBUTE: &str = "_AMQ_Attribute";
/// `ManagementHelper.HDR_OPERATION_NAME`.
pub const HDR_OPERATION_NAME: &str = "_AMQ_OperationName";
/// `ManagementHelper.HDR_OPERATION_SUCCEEDED`.
pub const HDR_OPERATION_SUCCEEDED: &str = "_AMQ_OperationSucceeded";
/// `MessageUtil.CORRELATIONID_HEADER_NAME`.
pub const CORRELATION_ID: &str = "JMSCorrelationID";
/// `MessageUtil.REPLYTO_HEADER_NAME`.
pub const REPLY_TO: &str = "JMSReplyTo";
/// `ManagementServiceImpl`'s fallback correlation: the AMQP message ID.
const NATIVE_MESSAGE_ID: &str = "NATIVE_MESSAGE_ID";

/// What an operation or attribute read gives: its result, or the text of
/// the exception it threw.
pub type Outcome = Result<Json, String>;

/// The management side of the broker's configuration.
#[derive(Debug, Clone)]
pub struct ManagementConfig {
    /// `management-address`.
    pub address: String,
    /// `management-notification-address`.
    pub notification_address: String,
    /// `suppress-session-notifications`.
    pub suppress_session_notifications: bool,
    /// The broker's `name`.
    pub name: String,
    /// The parsed `broker.xml`, for the attributes that report it.
    pub file: Option<Arc<saranga_config::BrokerConfig>>,
    /// The `broker.xml` it came from, for `reloadConfigurationFile`.
    pub file_path: Option<std::path::PathBuf>,
}

impl Default for ManagementConfig {
    fn default() -> Self {
        Self {
            address: "activemq.management".to_owned(),
            notification_address: "activemq.notifications".to_owned(),
            suppress_session_notifications: false,
            name: "localhost".to_owned(),
            file: None,
            file_path: None,
        }
    }
}

/// The broker's management service.
pub struct Management {
    config: ManagementConfig,
    /// When the broker started, in epoch milliseconds.
    started_at: i64,
}

/// A resource, by its management name.
enum Resource {
    Broker,
    Address(String),
    Queue(crate::queue::Queue),
    Divert(Arc<crate::postoffice::Divert>),
}

impl Management {
    pub fn new(config: ManagementConfig) -> Self {
        Self {
            config,
            started_at: crate::message::now_millis(),
        }
    }

    pub fn config(&self) -> &ManagementConfig {
        &self.config
    }

    /// The management address (`getManagementAddress`).
    pub fn address(&self) -> &str {
        &self.config.address
    }

    pub fn started_at(&self) -> i64 {
        self.started_at
    }

    fn resource(&self, broker: &Broker, name: &str) -> Option<Resource> {
        let po = broker.post_office();
        if name == "broker" {
            return Some(Resource::Broker);
        }
        if let Some(a) = name.strip_prefix("address.") {
            return po.address_info(a).map(|_| Resource::Address(a.to_owned()));
        }
        if let Some(q) = name.strip_prefix("queue.") {
            return po.queue(q).map(Resource::Queue);
        }
        if let Some(d) = name.strip_prefix("divert.") {
            return po.divert(d).map(Resource::Divert);
        }
        None
    }

    /// `getAttribute`: the value of a resource's attribute, read through
    /// its `get` or `is` method.
    pub async fn attribute(
        &self,
        broker: &Arc<Broker>,
        resource: &str,
        attribute: &str,
    ) -> Outcome {
        let problem = || format!("Problem while retrieving attribute {attribute}");
        let Some(r) = self.resource(broker, resource) else {
            return Err(problem());
        };
        let mut chars = attribute.chars();
        let upper: String = match chars.next() {
            Some(c) => c.to_uppercase().chain(chars).collect(),
            None => return Err(problem()),
        };
        let found = match &r {
            Resource::Broker => server::attribute(self, broker, &upper).await,
            Resource::Address(a) => address::attribute(broker, a, &upper).await,
            Resource::Queue(q) => queue::attribute(broker, q, &upper).await,
            Resource::Divert(d) => Ok(divert_attribute(d, &upper)),
        };
        match found {
            Ok(Some(v)) => Ok(v),
            Ok(None) | Err(_) => Err(problem()),
        }
    }

    /// `invokeOperation`.
    pub async fn invoke(
        &self,
        broker: &Arc<Broker>,
        resource: &str,
        operation: &str,
        params: &[Json],
    ) -> Outcome {
        let Some(r) = self.resource(broker, resource) else {
            return Err(format!(
                "AMQ229067: Cannot find resource with name {resource}"
            ));
        };
        let call = args::Call {
            op: operation,
            params,
        };
        let found = match &r {
            Resource::Broker => server::invoke(self, broker, &call).await,
            Resource::Address(a) => address::invoke(broker, a, &call).await,
            Resource::Queue(q) => queue::invoke(broker, q, &call).await,
            Resource::Divert(_) => None,
        };
        if let Some(outcome) = found {
            return outcome;
        }
        // A getter is a method too.
        if params.is_empty() {
            for prefix in ["get", "is"] {
                if let Some(attr) = operation.strip_prefix(prefix)
                    && attr.starts_with(|c: char| c.is_ascii_uppercase())
                {
                    let value = match &r {
                        Resource::Broker => server::attribute(self, broker, attr).await,
                        Resource::Address(a) => address::attribute(broker, a, attr).await,
                        Resource::Queue(q) => queue::attribute(broker, q, attr).await,
                        Resource::Divert(d) => Ok(divert_attribute(d, attr)),
                    };
                    match value {
                        Ok(Some(v)) => return Ok(v),
                        Err(e) => return Err(e),
                        Ok(None) => {}
                    }
                }
            }
        }
        Err(format!(
            "AMQ229069: no operation {operation}/{}",
            params.len()
        ))
    }

    /// `handleMessage`: runs the request a message to the management
    /// address carries and returns the reply, to go to its reply-to
    /// address.
    pub async fn handle_message(&self, broker: &Arc<Broker>, request: &CoreMessage) -> CoreMessage {
        let mut reply = CoreMessage {
            message_id: broker.post_office().ids().next(),
            message_type: message_type::TEXT,
            ..Default::default()
        };
        let prop = |name: &str| crate::message::string_property(request, name);
        if let Some(reply_to) = request.properties.get(&SimpleString::from(REPLY_TO)) {
            reply.properties.insert(REPLY_TO.into(), reply_to.clone());
        }
        // getCorrelationIdentity: the correlation ID, else the message ID.
        let correlation = request
            .properties
            .get(&SimpleString::from(CORRELATION_ID))
            .cloned()
            .unwrap_or_else(|| {
                let id = prop(NATIVE_MESSAGE_ID).unwrap_or_else(|| match request.user_id {
                    Some(u) => uuid::Uuid::from_bytes(u).to_string(),
                    None => "null".to_owned(),
                });
                PropertyValue::String(SimpleString::from(id.as_str()))
            });
        reply.properties.insert(CORRELATION_ID.into(), correlation);
        let resource = prop(HDR_RESOURCE_NAME).unwrap_or_default();
        let outcome = if let Some(operation) = prop(HDR_OPERATION_NAME) {
            match operation_parameters(request) {
                Ok(params) => {
                    let o = self.invoke(broker, &resource, &operation, &params).await;
                    if let Err(e) = &o {
                        tracing::warn!("AMQ222140: Error invoking {operation} on {resource}: {e}");
                    }
                    Some(o)
                }
                Err(e) => Some(Err(e)),
            }
        } else if let Some(attribute) = prop(HDR_ATTRIBUTE) {
            let o = self.attribute(broker, &resource, &attribute).await;
            if let Err(e) = &o {
                tracing::warn!("AMQ222141: Error getting {attribute} on {resource}: {e}");
            }
            Some(o)
        } else {
            None
        };
        if let Some(outcome) = outcome {
            let (succeeded, result) = match outcome {
                Ok(v) => (true, v),
                Err(e) => (false, Json::String(e)),
            };
            reply.properties.insert(
                HDR_OPERATION_SUCCEEDED.into(),
                PropertyValue::Bool(succeeded),
            );
            store_result(&mut reply, result);
        }
        reply
    }
}

/// `ManagementHelper.retrieveOperationParameters`: the body's nullable
/// string, a JSON array.
fn operation_parameters(message: &CoreMessage) -> Result<Vec<Json>, String> {
    let text = Reader::new(&message.body)
        .nullable_simple_string()
        .map_err(|e| e.to_string())?;
    match text {
        None => Ok(Vec::new()),
        Some(t) => match Json::parse(&t.to_string_lossy())? {
            Json::Array(items) => Ok(items),
            _ => Err("The operation parameters are not a JSON array".to_owned()),
        },
    }
}

/// `ManagementHelper.storeResult`: the result as a one-element JSON array,
/// or null.
fn store_result(reply: &mut CoreMessage, result: Json) {
    let text = (!result.is_null()).then(|| Json::Array(vec![result]).to_string());
    let mut w = Writer::new();
    w.nullable_simple_string(text.map(|t| SimpleString::from(t.as_str())).as_ref());
    reply.body = w.into_vec();
}

/// `DivertControl`'s attributes.
fn divert_attribute(d: &crate::postoffice::Divert, name: &str) -> Option<Json> {
    use crate::postoffice::DivertRoutingType as T;
    Some(match name {
        "UniqueName" | "Name" => Json::from(&*d.name),
        "Address" => Json::from(&*d.address),
        "ForwardingAddress" => Json::from(d.forwarding.join(",")),
        "RoutingName" => Json::from(&*d.routing_name),
        "Exclusive" => Json::from(d.exclusive),
        "Filter" => Json::from(d.filter.as_ref().map(|f| f.text().to_owned())),
        "RoutingType" => Json::from(match d.routing_type {
            T::Multicast => "MULTICAST",
            T::Anycast => "ANYCAST",
            T::Strip => "STRIP",
            T::Pass => "PASS",
        }),
        "TransformerClassName" => Json::from(
            d.deployment
                .transformer
                .as_ref()
                .map(|(class, _)| class.clone()),
        ),
        _ => return None,
    })
}
