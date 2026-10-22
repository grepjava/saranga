//! Management requests over Core, as a client makes them
//! (`ManagementHelper.doManagement`, `ClientRequestor`): a message to the
//! management address naming the resource and the operation or attribute,
//! answered on a temporary queue.

use std::time::Duration;

use saranga_client::message::{JMS_REPLY_TO, new_message};
use saranga_client::{
    Consumer, CoreMessage, MessageExt, PropertyValue, QueueConfiguration, Session, message_type,
};

use super::json::Json;
use super::{
    CORRELATION_ID, HDR_ATTRIBUTE, HDR_OPERATION_NAME, HDR_OPERATION_SUCCEEDED, HDR_RESOURCE_NAME,
};

/// The management address a client sends to: the default
/// `management-address`.
pub const MANAGEMENT_ADDRESS: &str = "activemq.management";

/// A management reply: whether it succeeded, and its result.
#[derive(Debug)]
pub struct Reply {
    pub succeeded: bool,
    pub result: Option<Json>,
}

impl Reply {
    /// `ManagementHelper.getResult(reply, String.class)`.
    pub fn string(&self) -> String {
        match &self.result {
            None | Some(Json::Null) => "null".to_owned(),
            Some(Json::String(s)) => s.clone(),
            Some(other) => other.to_string(),
        }
    }
}

/// Why a request had no reply.
#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    #[error(transparent)]
    Client(#[from] saranga_client::Error),
    #[error("no management reply in {0:?}")]
    TimedOut(Duration),
    #[error("management reply: {0}")]
    Reply(String),
}

/// A session with a requestor on it.
pub struct Requestor {
    session: Session,
    consumer: Consumer,
    reply_queue: String,
    /// Each request carries a correlation ID, and a reply that does not
    /// carry it (one that came after its request gave up) is dropped.
    correlate: bool,
}

impl Requestor {
    /// `ClientRequestor` on a session of its own: its temporary reply
    /// queue, on the management address.
    pub async fn on(session: Session) -> saranga_client::Result<Self> {
        Self::open(session, false).await
    }

    /// A requestor whose requests carry correlation IDs.
    pub async fn correlated(session: Session) -> saranga_client::Result<Self> {
        Self::open(session, true).await
    }

    async fn open(session: Session, correlate: bool) -> saranga_client::Result<Self> {
        session.start().await?;
        let reply_queue = format!(
            "{MANAGEMENT_ADDRESS}.{}",
            saranga_client::uuid::to_string(&saranga_client::uuid::time_based())
        );
        session
            .create_queue(
                QueueConfiguration::new(&reply_queue)
                    .durable(false)
                    .temporary(true),
            )
            .await?;
        let consumer = session.create_consumer(&reply_queue, None, false).await?;
        Ok(Self {
            session,
            consumer,
            reply_queue,
            correlate,
        })
    }

    /// `putOperationInvocation`: the parameters, a JSON array, as the body.
    pub async fn invoke(
        &self,
        resource: &str,
        operation: &str,
        params: Vec<Json>,
        timeout: Duration,
    ) -> Result<Reply, RequestError> {
        self.invoke_raw(resource, operation, Some(params), timeout)
            .await
    }

    /// An operation whose parameters may be `null` (no body text at all).
    pub async fn invoke_raw(
        &self,
        resource: &str,
        operation: &str,
        params: Option<Vec<Json>>,
        timeout: Duration,
    ) -> Result<Reply, RequestError> {
        let mut m = new_message(message_type::DEFAULT, false);
        m.put_string_property(HDR_RESOURCE_NAME, resource)
            .put_string_property(HDR_OPERATION_NAME, operation)
            .set_text(params.map(|p| Json::Array(p).to_string()).as_deref());
        self.request(m, timeout).await
    }

    /// `putAttribute`.
    pub async fn attribute(
        &self,
        resource: &str,
        attribute: &str,
        timeout: Duration,
    ) -> Result<Reply, RequestError> {
        let mut m = new_message(message_type::DEFAULT, false);
        m.put_string_property(HDR_RESOURCE_NAME, resource)
            .put_string_property(HDR_ATTRIBUTE, attribute)
            .set_text(None);
        self.request(m, timeout).await
    }

    async fn request(&self, mut m: CoreMessage, timeout: Duration) -> Result<Reply, RequestError> {
        m.put_string_property(JMS_REPLY_TO, &self.reply_queue);
        let correlation = self
            .correlate
            .then(|| saranga_client::uuid::to_string(&saranga_client::uuid::time_based()));
        if let Some(id) = &correlation {
            m.put_string_property(CORRELATION_ID, id);
        }
        let producer = self
            .session
            .create_producer(Some(MANAGEMENT_ADDRESS))
            .await?;
        producer.send(m).await?;
        producer.close();
        let deadline = tokio::time::Instant::now() + timeout;
        let reply = loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return Err(RequestError::TimedOut(timeout));
            }
            let Some(d) = self.consumer.receive(left).await? else {
                continue;
            };
            let matches = match &correlation {
                None => true,
                Some(id) => d.message.string_property(CORRELATION_ID).as_ref() == Some(id),
            };
            if matches {
                break d;
            }
        };
        // `ClientRequestor.request` leaves the reply unacknowledged; it
        // goes with the reply queue.
        let message = &reply.message;
        let succeeded = matches!(
            message.property(HDR_OPERATION_SUCCEEDED),
            Some(PropertyValue::Bool(true))
        );
        // `getResults`: a JSON array, of which the result is the first.
        let text = message
            .text()
            .map_err(|e| RequestError::Reply(e.to_string()))?;
        let result = match text {
            Some(text) => match Json::parse(&text) {
                Ok(Json::Array(mut items)) if !items.is_empty() => Some(items.swap_remove(0)),
                Ok(_) => None,
                Err(e) => return Err(RequestError::Reply(e.to_string())),
            },
            None => None,
        };
        Ok(Reply { succeeded, result })
    }

    /// `ClientRequestor.close`: the reply queue is deleted.
    pub async fn close(self) {
        self.consumer.close().await;
        let _ = self.session.delete_queue(&self.reply_queue).await;
        self.session.close().await;
    }
}
