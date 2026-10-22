//! Management requests over Core (`ManagementHelper.doManagement`,
//! `ClientRequestor`): a message to `activemq.management` naming the
//! resource and the operation or attribute, answered on a temporary queue.

use std::time::Duration;

use saranga_client::SessionOptions;
use saranga_server::management::json::Json;
use saranga_server::management::requestor;
pub use saranga_server::management::requestor::Reply;

use super::jms::JmsConnection;

/// `ClientRequestor.request` waits for its reply as long as it takes.
const FOREVER: Duration = Duration::from_secs(10 * 365 * 24 * 3600);

/// A session with a requestor on it.
pub struct Requestor(requestor::Requestor);

impl Requestor {
    /// `doManagement`'s session (non-transacted, auto-acked) and
    /// `ClientRequestor`'s temporary reply queue.
    pub async fn open(conn: &JmsConnection) -> anyhow::Result<Self> {
        let session = conn.conn.create_session(conn_session_options(conn)).await?;
        Self::on(session).await
    }

    /// `ClientRequestor` on a session of its own.
    pub async fn on(session: saranga_client::Session) -> anyhow::Result<Self> {
        Ok(Self(requestor::Requestor::on(session).await?))
    }

    /// `putOperationInvocation`: the parameters, a JSON array, as the body.
    pub async fn invoke(
        &self,
        resource: &str,
        operation: &str,
        params: Vec<Json>,
    ) -> anyhow::Result<Reply> {
        Ok(self.0.invoke(resource, operation, params, FOREVER).await?)
    }

    /// An operation whose parameters may be `null` (no body text at all).
    pub async fn invoke_raw(
        &self,
        resource: &str,
        operation: &str,
        params: Option<Vec<Json>>,
    ) -> anyhow::Result<Reply> {
        Ok(self
            .0
            .invoke_raw(resource, operation, params, FOREVER)
            .await?)
    }

    /// `putAttribute`.
    pub async fn attribute(&self, resource: &str, attribute: &str) -> anyhow::Result<Reply> {
        Ok(self.0.attribute(resource, attribute, FOREVER).await?)
    }

    /// `ClientRequestor.close`: the reply queue is deleted.
    pub async fn close(self) {
        self.0.close().await;
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
    let session = args.core_session().await?;
    request_on(session, request).await
}

/// One management request as `SimpleManagement` makes it: a session
/// without the connection factory's `tryConnect`.
pub async fn perform_simple(
    args: &super::ConnectionArgs,
    request: impl AsyncFnOnce(&Requestor) -> anyhow::Result<Reply>,
) -> anyhow::Result<Reply> {
    let session = args.simple_session().await?;
    request_on(session, request).await
}

async fn request_on(
    (conn, session): (saranga_client::Connection, saranga_client::Session),
    request: impl AsyncFnOnce(&Requestor) -> anyhow::Result<Reply>,
) -> anyhow::Result<Reply> {
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

/// The exception `SimpleManagement` throws for an operation that failed:
/// `Failed <result>`.
#[derive(Debug)]
pub struct SimpleFailure(pub String);

impl std::fmt::Display for SimpleFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed {}", self.0)
    }
}

impl std::error::Error for SimpleFailure {}

/// `SimpleManagement.simpleManagement`: the operation's result, or, if it
/// failed, the warning it logs and a [`SimpleFailure`].
pub async fn simple_call(
    args: &super::ConnectionArgs,
    resource: &str,
    operation: &str,
    params: Vec<Json>,
) -> anyhow::Result<String> {
    let reply = perform_simple(args, async |r| r.invoke(resource, operation, params).await).await?;
    if reply.succeeded {
        return Ok(reply.string());
    }
    let result = reply.string();
    tracing::warn!(
        target: "org.apache.activemq.artemis.api.core.management.SimpleManagement",
        "simple management operation failed:: {result}"
    );
    Err(SimpleFailure(result).into())
}

/// An exception a command does not catch, as Java's `main` reports it.
pub fn uncaught(e: anyhow::Error) -> anyhow::Error {
    match e.downcast_ref::<SimpleFailure>() {
        Some(f) => super::CliError(format!(
            "Exception in thread \"main\" java.lang.Exception: {f}"
        ))
        .into(),
        None => e,
    }
}
