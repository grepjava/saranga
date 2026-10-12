//! Management requests over Core (`ManagementHelper.doManagement`,
//! `ClientRequestor`): a message to `activemq.management` naming the
//! resource and the operation or attribute, answered on a temporary queue.

use std::time::Duration;

use anyhow::Context;
use saranga_client::message::{JMS_REPLY_TO, new_message};
use saranga_client::{
    Consumer, MessageExt, PropertyValue, QueueConfiguration, Session, SessionOptions, message_type,
};
use saranga_server::management::json::Json;

use super::jms::JmsConnection;

/// `ManagementHelper.HDR_RESOURCE_NAME`.
const HDR_RESOURCE_NAME: &str = "_AMQ_ResourceName";
/// `ManagementHelper.HDR_OPERATION_NAME`.
const HDR_OPERATION_NAME: &str = "_AMQ_OperationName";
/// `ManagementHelper.HDR_ATTRIBUTE`.
const HDR_ATTRIBUTE: &str = "_AMQ_Attribute";
/// `ManagementHelper.HDR_OPERATION_SUCCEEDED`.
const HDR_OPERATION_SUCCEEDED: &str = "_AMQ_OperationSucceeded";
/// The management address the CLI uses.
const MANAGEMENT_ADDRESS: &str = "activemq.management";

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

/// A session with a requestor on it.
pub struct Requestor {
    session: Session,
    consumer: Consumer,
    reply_queue: String,
}

impl Requestor {
    /// `doManagement`'s session (non-transacted, auto-acked) and
    /// `ClientRequestor`'s temporary reply queue.
    pub async fn open(conn: &JmsConnection) -> anyhow::Result<Self> {
        let session = conn.conn.create_session(conn_session_options(conn)).await?;
        Self::on(session).await
    }

    /// `ClientRequestor` on a session of its own.
    pub async fn on(session: Session) -> anyhow::Result<Self> {
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
        })
    }

    /// `putOperationInvocation`: the parameters, a JSON array, as the body.
    pub async fn invoke(
        &self,
        resource: &str,
        operation: &str,
        params: Vec<Json>,
    ) -> anyhow::Result<Reply> {
        self.invoke_raw(resource, operation, Some(params)).await
    }

    /// An operation whose parameters may be `null` (no body text at all).
    pub async fn invoke_raw(
        &self,
        resource: &str,
        operation: &str,
        params: Option<Vec<Json>>,
    ) -> anyhow::Result<Reply> {
        let mut m = new_message(message_type::DEFAULT, false);
        m.put_string_property(HDR_RESOURCE_NAME, resource)
            .put_string_property(HDR_OPERATION_NAME, operation)
            .set_text(params.map(|p| Json::Array(p).to_string()).as_deref());
        self.request(m).await
    }

    /// `putAttribute`.
    pub async fn attribute(&self, resource: &str, attribute: &str) -> anyhow::Result<Reply> {
        let mut m = new_message(message_type::DEFAULT, false);
        m.put_string_property(HDR_RESOURCE_NAME, resource)
            .put_string_property(HDR_ATTRIBUTE, attribute)
            .set_text(None);
        self.request(m).await
    }

    async fn request(&self, mut m: saranga_client::CoreMessage) -> anyhow::Result<Reply> {
        m.put_string_property(JMS_REPLY_TO, &self.reply_queue);
        let producer = self
            .session
            .create_producer(Some(MANAGEMENT_ADDRESS))
            .await?;
        producer.send(m).await?;
        producer.close();
        let reply = loop {
            if let Some(d) = self.consumer.receive(Duration::from_secs(3600)).await? {
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
        let result = match message.text().context("management reply body")? {
            Some(text) => match Json::parse(&text) {
                Ok(Json::Array(mut items)) if !items.is_empty() => Some(items.swap_remove(0)),
                Ok(_) => None,
                Err(e) => anyhow::bail!("management reply: {e}"),
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

fn conn_session_options(conn: &JmsConnection) -> SessionOptions {
    let (user, password) = conn.credentials();
    SessionOptions {
        username: user,
        password,
        ..SessionOptions::default()
    }
}

/// One management request on a new connection, as `performCoreManagement`
/// makes it: the reply, after `ok` or `failed` has seen it.
pub async fn perform(
    args: &super::ConnectionArgs,
    request: impl AsyncFnOnce(&Requestor) -> anyhow::Result<Reply>,
) -> anyhow::Result<Reply> {
    let (conn, session) = args.core_session().await?;
    let requestor = match Requestor::on(session).await {
        Ok(r) => r,
        Err(e) => {
            conn.close().await;
            return Err(e);
        }
    };
    let reply = request(&requestor).await;
    requestor.close().await;
    conn.close().await;
    reply
}
