//! The console's other nodes: the brokers in this broker's cluster topology,
//! reached with management messages over Core as the logged-in user
//! (`docs/console-design.md`, decision 1). Each node checks the user's
//! credentials and permissions itself, Artemis nodes as Saranga ones.
//!
//! A user's link to a node is one connection and one session with a
//! requestor, made at the first request and closed once unused for
//! [`IDLE`], or when its connection fails. Requests on a link go one at a
//! time, each with a correlation ID, so a reply that comes after its request
//! gave up is not taken for the next one's.
//!
//! A queue's messages are browsed with a browser on the link's session, as
//! a client browses them: management messages have no `browse` whose answer
//! is not Java-serialized (`CompositeData`), so this is how a page of them,
//! with their bodies, comes from an Artemis node too. The user then needs
//! the `browse` permission on that queue there.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use saranga_client::{Connection, ConnectionOptions, Connector, SessionOptions};
use saranga_core_protocol::ErrorType;
use saranga_core_protocol::transport::TransportConfiguration;
use saranga_server::Broker;
use saranga_server::management::export::message_json;
use saranga_server::management::json::Json;
use saranga_server::management::requestor::{RequestError, Requestor};
use saranga_server::management::{Composed, composite};
use sha2::{Digest, Sha256};

use crate::api::Problem;

/// How long a request waits for its reply.
pub(crate) const TIMEOUT: Duration = Duration::from_secs(30);
/// How long an unused link stays open.
const IDLE: Duration = Duration::from_secs(120);
/// How often unused links are looked for.
const SWEEP: Duration = Duration::from_secs(30);

/// The node a request is for: this broker, or another by its node ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Scope {
    Local,
    Node(String),
    /// Every node: only the listings span them.
    All,
}

impl Scope {
    /// The URL's name for a node: `local`, `all`, or a node ID. This
    /// broker's own node ID is `local` too.
    pub fn parse(node: &str, local_id: &str) -> Self {
        match node {
            "local" => Self::Local,
            "all" => Self::All,
            id if id == local_id => Self::Local,
            id => Self::Node(id.to_owned()),
        }
    }
}

/// A broker of the topology.
#[derive(Clone, Debug)]
pub(crate) struct Member {
    pub node_id: String,
    /// The connector its primary announced, to reach it by.
    pub primary: Option<TransportConfiguration>,
    pub backup: Option<TransportConfiguration>,
    pub backup_group: Option<String>,
    /// The cluster connections that know of it.
    pub clusters: Vec<String>,
    pub local: bool,
}

/// The brokers this one knows of, itself first, from every cluster
/// connection's topology.
pub(crate) fn members(broker: &Broker) -> Vec<Member> {
    let local = broker.node_id().to_owned();
    let mut out: Vec<Member> = vec![Member {
        node_id: local.clone(),
        primary: None,
        backup: None,
        backup_group: None,
        clusters: Vec::new(),
        local: true,
    }];
    for c in broker.clusters() {
        for m in c.members() {
            let i = match out.iter().position(|o| o.node_id == m.node_id) {
                Some(i) => i,
                None => {
                    out.push(Member {
                        node_id: m.node_id.clone(),
                        primary: None,
                        backup: None,
                        backup_group: None,
                        clusters: Vec::new(),
                        local: false,
                    });
                    out.len() - 1
                }
            };
            let o = &mut out[i];
            if o.primary.is_none() {
                o.primary = m.primary.clone();
            }
            if o.backup.is_none() {
                o.backup = m.backup.clone();
            }
            if o.backup_group.is_none() {
                o.backup_group = m.backup_group_name.clone();
            }
            o.clusters.push(c.config().name.clone());
        }
    }
    out[1..].sort_by(|a, b| a.node_id.cmp(&b.node_id));
    out
}

/// `host:port` of a connector, as `listNetworkTopology` writes it.
pub(crate) fn endpoint(tc: &TransportConfiguration) -> Option<String> {
    let c = Connector::from_transport(tc)?;
    Some(format!("{}:{}", c.host, c.port))
}

/// A link's key: the node, the user, and a digest of the password.
type Key = (String, String, [u8; 32]);

/// The credentials a link is made with: the user's own.
#[derive(Clone)]
pub(crate) struct Credentials {
    pub user: String,
    pub password: Option<String>,
}

impl Credentials {
    /// A link's key: never the password itself.
    fn key(&self, node: &str) -> Key {
        let digest = Sha256::digest(self.password.as_deref().unwrap_or("").as_bytes());
        (node.to_owned(), self.user.clone(), digest.into())
    }
}

struct Link {
    conn: Connection,
    requestor: tokio::sync::Mutex<Option<Requestor>>,
    used: Mutex<Instant>,
}

impl Link {
    async fn close(&self) {
        if let Some(r) = self.requestor.lock().await.take() {
            r.close().await;
        }
        self.conn.close().await;
    }
}

/// The links of every user to every node.
#[derive(Default)]
pub(crate) struct Peers {
    links: Mutex<HashMap<Key, Arc<Link>>>,
    sweeping: std::sync::atomic::AtomicBool,
}

/// A management request: an operation with its arguments, or an attribute.
pub(crate) enum Request<'a> {
    Invoke {
        resource: &'a str,
        operation: &'a str,
        arguments: Vec<Json>,
    },
    Attribute {
        resource: &'a str,
        name: &'a str,
    },
    /// The messages of a queue after the first `skip`, at most `take`, as
    /// `QueueControl.browse` writes them.
    Browse {
        queue: &'a str,
        filter: Option<&'a str>,
        skip: usize,
        take: usize,
    },
    /// At most `max` of a queue's messages, whole, as the console's export
    /// writes them.
    Export {
        queue: &'a str,
        filter: Option<&'a str>,
        max: usize,
    },
    /// Messages sent to an address, in order: the console's import.
    Import {
        address: &'a str,
        messages: Vec<saranga_message::CoreMessage>,
    },
}

/// How long a browser waits for a message more before it takes the queue
/// to have no more.
const BROWSE_WAIT: Duration = Duration::from_secs(1);

impl Peers {
    /// One request to a node, as the user: its result, or why there is
    /// none. A node the topology does not have is 404, one that cannot be
    /// reached 502, and a refusal 403.
    pub async fn request(
        self: &Arc<Self>,
        broker: &Broker,
        node: &str,
        who: &Credentials,
        request: Request<'_>,
    ) -> Result<Json, Problem> {
        let link = self.link(broker, node, who).await?;
        *link.used.lock().unwrap() = Instant::now();
        let request_queue = match &request {
            Request::Browse { queue, .. } | Request::Export { queue, .. } => {
                Some((*queue).to_owned())
            }
            _ => None,
        };
        let guard = link.requestor.lock().await;
        let Some(r) = guard.as_ref() else {
            return Err(unreachable(node, "the link was closed"));
        };
        let reply = match request {
            Request::Invoke {
                resource,
                operation,
                arguments,
            } => r.invoke(resource, operation, arguments, TIMEOUT).await,
            Request::Attribute { resource, name } => r.attribute(resource, name, TIMEOUT).await,
            Request::Browse { .. } | Request::Export { .. } | Request::Import { .. } => {
                let done = match request {
                    Request::Browse {
                        queue,
                        filter,
                        skip,
                        take,
                    } => browse(r.session(), queue, filter, skip, take, shown).await,
                    Request::Export { queue, filter, max } => {
                        browse(r.session(), queue, filter, 0, max, whole).await
                    }
                    Request::Import { address, messages } => {
                        import(r.session(), address, messages).await
                    }
                    _ => unreachable!(),
                };
                let queue = match &request_queue {
                    Some(q) => q.as_str(),
                    None => "",
                };
                drop(guard);
                *link.used.lock().unwrap() = Instant::now();
                return done.map_err(|e| match e.kind() {
                    Some(ErrorType::SecurityException) => client_problem(node, &e, false),
                    Some(ErrorType::QueueDoesNotExist) => Problem::new(
                        StatusCode::NOT_FOUND,
                        format!("no such resource: queue.{queue}"),
                    ),
                    Some(ErrorType::AddressDoesNotExist) => {
                        Problem::new(StatusCode::NOT_FOUND, e.to_string())
                    }
                    Some(_) => Problem::new(StatusCode::BAD_REQUEST, e.to_string()),
                    None => unreachable(node, e),
                });
            }
        };
        drop(guard);
        *link.used.lock().unwrap() = Instant::now();
        match reply {
            Ok(reply) if reply.succeeded => Ok(reply.result.unwrap_or(Json::Null)),
            Ok(reply) => Err(refused(reply.string())),
            Err(RequestError::TimedOut(t)) => Err(Problem::of_node(
                StatusCode::GATEWAY_TIMEOUT,
                format!("node {node} did not answer in {} s", t.as_secs()),
            )),
            Err(RequestError::Client(e)) => {
                // A send the node refused leaves the link usable; anything
                // else and it is made again next time.
                if e.kind() != Some(ErrorType::SecurityException) {
                    self.drop_link(node, who).await;
                }
                Err(client_problem(node, &e, false))
            }
            Err(RequestError::Reply(e)) => Err(Problem::new(StatusCode::BAD_GATEWAY, e)),
        }
    }

    /// The user's link to a node, made if there is none.
    async fn link(
        self: &Arc<Self>,
        broker: &Broker,
        node: &str,
        who: &Credentials,
    ) -> Result<Arc<Link>, Problem> {
        let key = who.key(node);
        if let Some(l) = self.links.lock().unwrap().get(&key) {
            return Ok(l.clone());
        }
        let member = members(broker)
            .into_iter()
            .find(|m| !m.local && m.node_id == node)
            .ok_or_else(|| Problem::of_node(StatusCode::NOT_FOUND, format!("no node {node}")))?;
        let connector = member
            .primary
            .as_ref()
            .and_then(Connector::from_transport)
            .ok_or_else(|| unreachable(node, "it has no connector the console can use"))?;
        let options = ConnectionOptions {
            reconnect_attempts: 0,
            initial_connect_attempts: 1,
            ..ConnectionOptions::default()
        };
        let conn = Connection::connect_to(vec![connector], options)
            .await
            .map_err(|e| client_problem(node, &e, true))?;
        let session = match conn
            .create_session(SessionOptions {
                username: Some(who.user.clone()),
                password: who.password.clone(),
                // A send the node refuses is answered with why, rather
                // than left to time out.
                block_on_non_durable_send: true,
                ..SessionOptions::default()
            })
            .await
        {
            Ok(s) => s,
            Err(e) => {
                conn.close().await;
                return Err(client_problem(node, &e, true));
            }
        };
        let requestor = match Requestor::correlated(session).await {
            Ok(r) => r,
            Err(e) => {
                conn.close().await;
                return Err(client_problem(node, &e, true));
            }
        };
        let link = Arc::new(Link {
            conn,
            requestor: tokio::sync::Mutex::new(Some(requestor)),
            used: Mutex::new(Instant::now()),
        });
        let existing = {
            let mut links = self.links.lock().unwrap();
            match links.get(&key) {
                // Another request made one meanwhile.
                Some(l) => Some(l.clone()),
                None => {
                    links.insert(key, link.clone());
                    None
                }
            }
        };
        if let Some(l) = existing {
            link.close().await;
            return Ok(l);
        }
        self.sweep();
        Ok(link)
    }

    async fn drop_link(&self, node: &str, who: &Credentials) {
        let link = self.links.lock().unwrap().remove(&who.key(node));
        if let Some(l) = link {
            l.close().await;
        }
    }

    /// The user's links, closed as they log out.
    pub async fn forget(&self, user: &str) {
        let gone: Vec<Arc<Link>> = {
            let mut links = self.links.lock().unwrap();
            let keys: Vec<_> = links.keys().filter(|k| k.1 == user).cloned().collect();
            keys.iter().filter_map(|k| links.remove(k)).collect()
        };
        for l in gone {
            l.close().await;
        }
    }

    /// Closes the links unused for [`IDLE`], from now on.
    fn sweep(self: &Arc<Self>) {
        if self
            .sweeping
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(SWEEP).await;
                let Some(peers) = weak.upgrade() else { return };
                let idle: Vec<Arc<Link>> = {
                    let mut links = peers.links.lock().unwrap();
                    let keys: Vec<_> = links
                        .iter()
                        .filter(|(_, l)| l.used.lock().unwrap().elapsed() > IDLE)
                        .map(|(k, _)| k.clone())
                        .collect();
                    keys.iter().filter_map(|k| links.remove(k)).collect()
                };
                for l in idle {
                    l.close().await;
                }
            }
        });
    }
}

/// A browsed message as the queue's `browse` writes it.
fn shown(d: &saranga_client::Delivery) -> Json {
    composite(
        &d.message,
        &Composed {
            protocol: None,
            redelivered: d.delivery_count > 1,
            large: d.is_large(),
            persistent_size: i64::from(d.encode_size()),
        },
        MESSAGE_ATTRIBUTE_LIMIT,
    )
}

/// A browsed message whole, as the console's export writes it: a large
/// message's body is read with it.
fn whole(d: &saranga_client::Delivery) -> Json {
    message_json(&d.message)
}

/// Messages sent to an address in order, as many as can be: how many were.
async fn import(
    session: &saranga_client::Session,
    address: &str,
    messages: Vec<saranga_message::CoreMessage>,
) -> saranga_client::Result<Json> {
    let producer = session.create_producer(Some(address)).await?;
    let mut sent = 0i64;
    let mut failed = None;
    for m in messages {
        if let Err(e) = producer.send(m).await {
            failed = Some(e);
            break;
        }
        sent += 1;
    }
    producer.close();
    match failed {
        // None sent: the node's refusal is the answer.
        Some(e) if sent == 0 => Err(e),
        Some(e) => Ok(Json::object()
            .with("sent", sent)
            .with("error", e.to_string())),
        None => Ok(Json::object().with("sent", sent)),
    }
}

/// A page of a queue's messages, as a client browses them, each written by
/// `write`.
async fn browse(
    session: &saranga_client::Session,
    queue: &str,
    filter: Option<&str>,
    skip: usize,
    take: usize,
    write: fn(&saranga_client::Delivery) -> Json,
) -> saranga_client::Result<Json> {
    let browser = session.create_consumer(queue, filter, true).await?;
    let mut out = Vec::new();
    let mut seen = 0;
    let read = async {
        while out.len() < take {
            let Some(d) = browser.receive(BROWSE_WAIT).await? else {
                break;
            };
            seen += 1;
            if seen <= skip {
                continue;
            }
            out.push(write(&d));
        }
        saranga_client::Result::Ok(())
    }
    .await;
    browser.close().await;
    read?;
    Ok(Json::Array(out))
}

/// `management-message-attribute-size-limit`'s default: how much of a body
/// or a property a browsed message shows.
const MESSAGE_ATTRIBUTE_LIMIT: i32 = 256;

fn unreachable(node: &str, why: impl std::fmt::Display) -> Problem {
    Problem::of_node(
        StatusCode::BAD_GATEWAY,
        format!("node {node} cannot be reached: {why}"),
    )
}

/// What the node answered when it refused or failed an operation: the
/// broker's own text.
fn refused(message: String) -> Problem {
    let status = if message.contains("AMQ229213")
        || message.contains("AMQ229032")
        || message.contains("does not have permission")
    {
        StatusCode::FORBIDDEN
    } else {
        StatusCode::BAD_REQUEST
    };
    Problem::new(status, message)
}

/// A client error: a refusal is 403, and the rest that the node cannot be
/// reached. Refused while the link was made, the node refuses the user.
fn client_problem(node: &str, e: &saranga_client::Error, linking: bool) -> Problem {
    match e.kind() {
        Some(ErrorType::SecurityException) => {
            let message = match e {
                saranga_client::Error::ActiveMq { message, .. } => message.clone(),
                _ => e.to_string(),
            };
            if linking {
                Problem::of_node(
                    StatusCode::FORBIDDEN,
                    format!("node {node} refused the user: {message}"),
                )
            } else {
                Problem::new(StatusCode::FORBIDDEN, message)
            }
        }
        _ => unreachable(node, e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes() {
        assert_eq!(Scope::parse("local", "a"), Scope::Local);
        assert_eq!(Scope::parse("a", "a"), Scope::Local);
        assert_eq!(Scope::parse("all", "a"), Scope::All);
        assert_eq!(Scope::parse("b", "a"), Scope::Node("b".into()));
    }

    #[test]
    fn refusals_are_forbidden() {
        let p = refused(
            "AMQ229213: User: guest does not have permission='MANAGE' for queue activemq.management on address activemq.management".into(),
        );
        assert_eq!(p.status, StatusCode::FORBIDDEN);
        assert_eq!(
            refused("AMQ229017: Queue x does not exist".into()).status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn keys_hide_the_password() {
        let c = Credentials {
            user: "admin".into(),
            password: Some("secret".into()),
        };
        let (node, user, digest) = c.key("n1");
        assert_eq!((node.as_str(), user.as_str()), ("n1", "admin"));
        assert_ne!(&digest[..6], b"secret");
    }
}
