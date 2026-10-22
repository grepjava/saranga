//! The protocol-neutral server session (`ServerSessionImpl`): sends,
//! consumers, acknowledgement, queue management and local and XA
//! transactions. Every protocol drives one of these per client session;
//! what goes back on the wire is the protocol's business.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use saranga_buffer::SimpleString;
use saranga_config::CheckType;
use saranga_core_protocol::ErrorType;
use saranga_core_protocol::packets::xa::Xid;
use saranga_message::CoreMessage;
use saranga_security::{Authenticated, ConnectionInfo, SecurityAuth};

use crate::large::LargeBody;
use crate::message::{MessageRef, NativeMessage, ServerMessage, routing_type};
use crate::notifications::{NotificationType, hdr, string};
use crate::postoffice::{
    BindingQueryResult, Creator, QueueQueryResult, QueueSpec, RoutingTypes, queue_name_of,
};
use crate::queue::{
    AckResult, ConsumerId, ConsumerMetrics, ConsumerRegistration, DeliverySink, Queue,
};
use crate::registry::{ConsumerEntry, SessionEntry};
use crate::storage::{Pending, Storage};
use crate::transaction::{self, Outcome, Transaction, TxError, TxRef, TxState, XaError, xa_code};
use crate::{Broker, BrokerError, Name, Result, RoutingType};

/// What a connection is carried over, as management shows it.
pub struct Transport {
    /// `getProtocolName`: `CORE`, `AMQP`, `STOMP`, `MQTT`, `OPENWIRE`.
    pub protocol: &'static str,
    /// The class Artemis implements the connection with
    /// (`connection.getClass().getSimpleName()`).
    pub implementation: &'static str,
    /// `getRemoteAddress`: `127.0.0.1:51234`.
    pub remote_address: String,
    /// `getTransportLocalAddress`: `127.0.0.1:61616`, `[::1]:61616`.
    pub local_address: String,
    /// `isSupportsFlowControl`: the client can be held back by credits.
    /// Sends from one that cannot fail the connection while the disk is
    /// full.
    pub flow_control: bool,
}

impl Transport {
    /// A connection over a socket, its addresses as Netty's
    /// `SocketAddressUtil` prints them.
    pub fn socket(
        protocol: &'static str,
        implementation: &'static str,
        peer: Option<std::net::SocketAddr>,
        local: Option<std::net::SocketAddr>,
    ) -> Self {
        let text = |a: Option<std::net::SocketAddr>, enclose: bool| {
            a.map(|a| {
                let java = crate::core_protocol::java_socket_address(a);
                let bare = java.trim_start_matches('/');
                match a {
                    // IPV6Util.encloseHost
                    std::net::SocketAddr::V6(v6) if enclose => {
                        let host = &bare[..bare.len() - v6.port().to_string().len() - 1];
                        format!("[{host}]:{}", v6.port())
                    }
                    _ => bare.to_owned(),
                }
            })
            .unwrap_or_default()
        };
        Self {
            protocol,
            implementation,
            remote_address: text(peer, false),
            local_address: text(local, true),
            // STOMP's and MQTT's connections keep AbstractRemotingConnection's
            // false; Core, AMQP and OpenWire override it.
            flow_control: !matches!(protocol, "STOMP" | "MQTT"),
        }
    }

    /// A connection inside the broker.
    pub fn internal(name: &str) -> Self {
        Self {
            protocol: "CORE",
            implementation: "RemotingConnectionImpl",
            remote_address: name.to_owned(),
            local_address: name.to_owned(),
            flow_control: true,
        }
    }

    /// The connection of management's own sends
    /// (`ManagementRemotingConnection`), which has no flow control.
    pub fn management() -> Self {
        Self {
            flow_control: false,
            ..Self::internal("management")
        }
    }
}

/// What every protocol's connection shares with its sessions
/// (`RemotingConnection` as the server session sees it).
pub struct ClientConnection {
    /// `getID`: eight hex digits, as Netty's `ChannelId.asShortText`.
    id: String,
    transport: Transport,
    created: i64,
    client_id: Mutex<Option<String>>,
    /// Temporary queues, deleted when the connection goes away.
    temp_queues: Mutex<Vec<Name>>,
    /// What the security store knows of this connection.
    security_info: Arc<ConnectionInfo>,
    /// The subject its sessions authenticated as (`getSubject`), for the
    /// audit log.
    subject: Mutex<Option<Arc<saranga_security::jaas::Subject>>>,
    /// The acceptor's address prefixes.
    prefixes: Vec<(String, RoutingType)>,
    /// The name of the acceptor the connection came in on.
    acceptor: Option<String>,
    sessions: Mutex<Vec<Arc<SessionEntry>>>,
    /// Management closed the connection (`fail`).
    failed: AtomicBool,
    fail_notify: tokio::sync::Notify,
    /// The exception the broker failed the connection with
    /// (`fail(ActiveMQException)`), when the broker did.
    cause: Mutex<Option<BrokerError>>,
    /// The slow-consumer policy killed the connection.
    killed: AtomicBool,
    /// The broker is stopping (`RemotingServiceImpl.freeze`).
    disconnecting: AtomicBool,
}

impl ClientConnection {
    pub fn new(
        transport: Transport,
        security_info: Arc<ConnectionInfo>,
        prefixes: Vec<(String, RoutingType)>,
    ) -> Self {
        let id = uuid::Uuid::new_v4().as_bytes()[..4]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        // The audit log names the connection a login was on.
        let security_info = Arc::new(ConnectionInfo {
            connection_id: Some(id.clone()),
            ..(*security_info).clone()
        });
        Self {
            id,
            transport,
            created: crate::message::now_millis(),
            client_id: Mutex::default(),
            temp_queues: Mutex::default(),
            security_info,
            subject: Mutex::default(),
            prefixes,
            acceptor: None,
            sessions: Mutex::default(),
            failed: AtomicBool::new(false),
            cause: Mutex::default(),
            killed: AtomicBool::new(false),
            disconnecting: AtomicBool::new(false),
            fail_notify: tokio::sync::Notify::new(),
        }
    }

    /// Names the acceptor the connection came in on.
    pub fn with_acceptor(mut self, name: &str) -> Self {
        self.acceptor = (!name.is_empty()).then(|| name.to_owned());
        self
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// The name of the acceptor the connection came in on, if any.
    pub fn acceptor(&self) -> Option<&str> {
        self.acceptor.as_deref()
    }

    pub fn protocol(&self) -> &'static str {
        self.transport.protocol
    }

    pub fn implementation(&self) -> &'static str {
        self.transport.implementation
    }

    pub fn local_address(&self) -> &str {
        &self.transport.local_address
    }

    /// `getCreationTime`, in milliseconds since the epoch.
    pub fn created(&self) -> i64 {
        self.created
    }

    /// The sessions open on the connection (`getSessions(connectionID)`).
    pub fn sessions(&self) -> Vec<Arc<SessionEntry>> {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn add_session(&self, session: Arc<SessionEntry>) {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(session);
    }

    fn remove_session(&self, session: &Arc<SessionEntry>) {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|s| !Arc::ptr_eq(s, session));
    }

    /// `RemotingConnection.fail`, from management: the protocol closes the
    /// connection as if the client had gone.
    pub fn fail(&self) {
        self.failed.store(true, Ordering::SeqCst);
        self.fail_notify.notify_waiters();
    }

    /// `RemotingConnection.fail(e)` from the broker: the connection fails
    /// as [`fail`](Self::fail) does, its protocol telling the client `e`
    /// where it can. The first cause stays.
    pub fn fail_with(&self, e: BrokerError) {
        self.cause
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert(e);
        self.fail();
    }

    /// What the broker failed the connection with, if it did.
    pub fn cause(&self) -> Option<BrokerError> {
        self.cause.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// `isSupportsFlowControl`.
    pub fn supports_flow_control(&self) -> bool {
        self.transport.flow_control
    }

    /// The slow-consumer policy's `KILL`: the client is told, where its
    /// protocol can tell it (`killMessage`), and the connection fails.
    pub fn kill(&self) {
        self.killed.store(true, Ordering::SeqCst);
        self.fail();
    }

    /// `RemotingConnection.disconnect`, as the broker stops: the client is
    /// told, where its protocol can tell it, and the connection fails.
    pub fn disconnect(&self) {
        self.disconnecting.store(true, Ordering::SeqCst);
        self.fail();
    }

    pub fn is_disconnecting(&self) -> bool {
        self.disconnecting.load(Ordering::SeqCst)
    }

    pub fn is_killed(&self) -> bool {
        self.killed.load(Ordering::SeqCst)
    }

    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    /// Completes once management has failed the connection; the protocol's
    /// read loop waits on it with the socket.
    pub async fn failed(&self) {
        loop {
            let notified = self.fail_notify.notified();
            if self.is_failed() {
                return;
            }
            notified.await;
        }
    }

    pub fn client_id(&self) -> Option<String> {
        self.client_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn set_client_id(&self, client_id: Option<String>) {
        *self.client_id.lock().unwrap_or_else(|e| e.into_inner()) = client_id;
    }

    pub fn add_temp_queue(&self, name: Name) {
        self.temp_queues
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(name);
    }

    pub fn remove_temp_queue(&self, name: &str) {
        self.temp_queues
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|q| &**q != name);
    }

    pub fn remote_address(&self) -> &str {
        &self.transport.remote_address
    }

    pub fn security_info(&self) -> Arc<ConnectionInfo> {
        self.security_info.clone()
    }

    /// `setSubject`: the subject a session of this connection logged in as.
    pub fn set_subject(&self, subject: Option<Arc<saranga_security::jaas::Subject>>) {
        if subject.is_some() {
            *self.subject.lock().unwrap_or_else(|e| e.into_inner()) = subject;
        }
    }

    pub fn subject(&self) -> Option<Arc<saranga_security::jaas::Subject>> {
        self.subject
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn prefixes(&self) -> &[(String, RoutingType)] {
        &self.prefixes
    }

    /// `PrefixUtil.getAddressAndRoutingTypes` over the acceptor's prefixes:
    /// the name without the first prefix it starts with, and that prefix's
    /// routing type.
    pub fn strip_prefix<'a>(&self, name: &'a str) -> (&'a str, Option<RoutingType>) {
        for (prefix, t) in &self.prefixes {
            if let Some(rest) = name.strip_prefix(prefix.as_str()) {
                return (rest, Some(*t));
            }
        }
        (name, None)
    }

    /// The connection is gone: removes its temporary queues
    /// (`TempResourceCleanerUpper`). Its sessions must be closed first.
    pub fn remove_temp_queues(&self, broker: &Broker) {
        let temp = std::mem::take(&mut *self.temp_queues.lock().unwrap_or_else(|e| e.into_inner()));
        let post_office = broker.post_office();
        for name in temp {
            let auto_delete_address = post_office
                .queue(&name)
                .map(|q| {
                    post_office
                        .settings(&q.config().address)
                        .auto_delete_addresses
                })
                .unwrap_or(false);
            let _ = post_office.delete_queue(&name, false, auto_delete_address);
        }
    }
}

/// How a session is opened (`ActiveMQServer.createSession`).
pub struct SessionOptions {
    pub name: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub xa: bool,
    pub auto_commit_sends: bool,
    pub auto_commit_acks: bool,
    pub pre_acknowledge: bool,
    pub default_address: Option<String>,
}

struct ServerConsumer {
    id: ConsumerId,
    queue: Queue,
    browse_only: bool,
    metrics: Arc<ConsumerMetrics>,
    filter: Option<String>,
}

/// `b` after `a`, taking `b` whole when `a` is empty (no allocation).
fn append<T>(a: &mut Vec<T>, b: Vec<T>) {
    if a.is_empty() {
        *a = b;
    } else {
        a.extend(b);
    }
}

/// The journal writes a request lined up, and the queue updates that must
/// wait for them (`OperationContext`).
#[derive(Default)]
pub struct Completion {
    waits: Vec<Pending>,
    /// Queue updates that must follow the journal writes
    /// (`afterCompleteOperations`): routed references, committed outcomes.
    actions: Vec<Box<dyn FnOnce() + Send>>,
    /// What `sync` mirrors' peers complete once they have what the queue
    /// updates sent them (`replicationLineUp`), waited for after those.
    replicated: Vec<Replicated>,
}

/// Mirror waits a request lines up, some of them as its queue updates are
/// made.
pub type Replicated = Arc<Mutex<Vec<Pending>>>;

impl Completion {
    pub fn is_empty(&self) -> bool {
        self.waits.is_empty()
            && self.actions.is_empty()
            && self
                .replicated
                .iter()
                .all(|r| lock_replicated(r).is_empty())
    }

    /// Adds a journal write to wait for.
    pub fn push(&mut self, wait: Pending) {
        self.waits.push(wait);
    }

    /// With no journal write to wait for, makes the queue updates now, as
    /// `OperationContext.executeOnCompletion` runs a task at once when no
    /// store is pending. What is left (the mirrors' waits) is still to be
    /// waited for.
    pub fn apply_if_written(&mut self) {
        if self.waits.is_empty() {
            for action in std::mem::take(&mut self.actions) {
                action();
            }
        }
    }

    /// Adds another request's writes and updates to these.
    pub fn merge(&mut self, other: Completion) {
        append(&mut self.waits, other.waits);
        append(&mut self.actions, other.actions);
        append(&mut self.replicated, other.replicated);
    }

    /// Waits for the journal writes, then makes the queue updates; after a
    /// failed write, the error and no updates (`OperationContext.onError`).
    pub async fn wait(self) -> std::result::Result<(), saranga_journal::JournalError> {
        let mut failure = None;
        for w in self.waits {
            if let Err(e) = w.wait().await {
                failure.get_or_insert(e);
            }
        }
        match failure {
            Some(e) => Err(e),
            None => {
                for action in self.actions {
                    action();
                }
                for r in self.replicated {
                    let waits = std::mem::take(&mut *lock_replicated(&r));
                    for w in waits {
                        // A peer that never answers keeps the client
                        // waiting, as Artemis does; one gone is no error.
                        let _ = w.wait().await;
                    }
                }
                Ok(())
            }
        }
    }
}

fn lock_replicated(r: &Replicated) -> std::sync::MutexGuard<'_, Vec<Pending>> {
    r.lock().unwrap_or_else(|e| e.into_inner())
}

/// Polls `f` once: its output if it is ready, `None` if it is not (it can
/// still be awaited).
pub async fn poll_once<F: Future>(f: std::pin::Pin<&mut F>) -> Option<F::Output> {
    let mut f = f;
    std::future::poll_fn(|cx| {
        std::task::Poll::Ready(match f.as_mut().poll(cx) {
            std::task::Poll::Ready(v) => Some(v),
            std::task::Poll::Pending => None,
        })
    })
    .await
}

/// An acknowledgement on its way to its queue
/// ([`ServerSession::acknowledge_then`]).
pub struct AckPending {
    consumer_id: i64,
    message_id: i64,
    queue: Queue,
}

impl AckPending {
    /// The rest of the acknowledgement when its session has gone: the
    /// references are acknowledged in storage, or go back.
    pub fn finish_alone(self, result: AckResult) {
        match result {
            Ok(mut refs) => {
                for r in &mut refs {
                    if let Err(e) = self.queue.store_ack(r) {
                        tracing::warn!("acknowledging message {}: {e}", self.message_id);
                    }
                    let _ = self.queue.post_acknowledge(r);
                }
            }
            Err(failed) => self.queue.reinsert(failed.refs),
        }
    }
}

pub struct ServerSession {
    broker: Arc<Broker>,
    conn: Arc<ClientConnection>,
    name: String,
    /// Who the session acts as, for permission checks.
    auth: SecurityAuth,
    /// The user the security manager validated (`getValidatedUser`).
    validated_user: Option<String>,
    /// XA sessions work in the transaction of the last `xaStart`, or in
    /// none.
    xa: bool,
    auto_commit_sends: bool,
    auto_commit_acks: bool,
    pre_acknowledge: bool,
    default_address: Option<Name>,
    started: bool,
    closed: bool,
    consumers: BTreeMap<i64, ServerConsumer>,
    /// The current transaction: always one for a local session.
    tx: Option<TxRef>,
    /// An XA branch ended but not yet prepared, committed or rolled back
    /// (`pendingTX`), rolled back if the client fails.
    pending_tx: Option<TxRef>,
    timeout_seconds: i32,
    /// Journal writes lined up by the request being handled.
    ops: Vec<Pending>,
    /// Queue updates to make once those writes are done.
    actions: Vec<Box<dyn FnOnce() + Send>>,
    /// Permission checks are off (a send the broker makes).
    unchecked: bool,
    /// Routing a message the broker took from elsewhere
    /// ([`ServerSession::route_direct`]): no auto-creation permission or
    /// validated-user checks either.
    direct: bool,
    /// A broker connection's session (`createInternalSession`): nothing it
    /// does is checked.
    internal: bool,
    /// What management sees of the session.
    entry: Arc<SessionEntry>,
    /// The producer the next sends are counted against.
    sender: Option<Arc<str>>,
    /// The address of the last message sent and its name, which the next one
    /// to the same address takes without converting it again.
    last_address: Option<(SimpleString, Name)>,
    /// How its routes are mirrored: by a mirror target, as from its peer.
    mirroring: crate::mirror::Mirroring,
    /// The `sync` mirror waits the request so far lined up.
    replicated: Replicated,
    /// Its grants waiting for an address to have room
    /// (`blockedRunnables`), cancelled when it closes.
    blocked: Arc<crate::paging::BlockedRunnables>,
}

/// What [`ServerSession::send_message`] sends besides the message.
enum Outgoing {
    /// A Core message, with the body of a large one.
    Core(Option<Arc<LargeBody>>),
    /// A native message, its view before the send changed it, the routing
    /// type the protocol chose for it, and a large message's file.
    Native(
        Arc<dyn NativeMessage>,
        CoreMessage,
        Option<RoutingType>,
        Option<Arc<LargeBody>>,
    ),
}

impl ServerSession {
    pub fn new(
        broker: Arc<Broker>,
        conn: Arc<ClientConnection>,
        options: SessionOptions,
        authenticated: Authenticated,
    ) -> Self {
        // The connection is who its sessions authenticated as.
        conn.set_subject(authenticated.subject.clone());
        if crate::audit::base_enabled() {
            let subject = conn.subject();
            crate::audit::session_created(
                &broker,
                crate::audit::Who {
                    subject: subject.as_deref(),
                    remote: Some(conn.remote_address()),
                },
                &options.name,
                options.username.as_deref(),
                &conn,
                options.auto_commit_sends,
                options.auto_commit_acks,
                options.pre_acknowledge,
                options.xa,
                options.default_address.as_deref(),
            );
        }
        let tx =
            (!options.xa).then(|| Transaction::new_ref(broker.post_office().ids().next(), None));
        let security_info = conn.security_info();
        let entry = Arc::new(SessionEntry::new(
            options.name.clone(),
            options.username.clone(),
            authenticated.validated_user.clone(),
            &conn,
            options.default_address.clone(),
        ));
        conn.add_session(entry.clone());
        broker.registry().session_created();
        session_notification(&broker, &conn, &entry, NotificationType::SessionCreated);
        broker
            .post_office()
            .plugins()
            .each(|p| p.after_create_session(&entry));
        Self {
            broker,
            conn,
            entry,
            sender: None,
            last_address: None,
            blocked: Arc::default(),
            name: options.name,
            auth: SecurityAuth {
                username: options.username,
                password: options.password,
                connection: security_info,
                subject: authenticated.subject,
                management: false,
            },
            validated_user: authenticated.validated_user,
            xa: options.xa,
            auto_commit_sends: options.auto_commit_sends,
            auto_commit_acks: options.auto_commit_acks,
            pre_acknowledge: options.pre_acknowledge,
            default_address: options.default_address.map(Into::into),
            started: false,
            closed: false,
            consumers: BTreeMap::new(),
            tx,
            pending_tx: None,
            timeout_seconds: -1,
            ops: Vec::new(),
            actions: Vec::new(),
            unchecked: false,
            direct: false,
            internal: false,
            mirroring: crate::mirror::Mirroring::Enabled,
            replicated: Replicated::default(),
        }
    }

    /// A session through which the broker routes messages it took from
    /// elsewhere (`PostOfficeImpl.route(message, direct)`, as a federated
    /// consumer does): no client, so it is neither listed nor told of, and
    /// its sends ([`ServerSession::route_direct`]) are not checked.
    pub fn internal(broker: Arc<Broker>, name: &str) -> Self {
        let info = Arc::new(saranga_security::ConnectionInfo {
            remote_address: name.to_owned(),
            certificates: None,
            security_domain: None,
            connection_id: None,
        });
        let conn = Arc::new(ClientConnection::new(
            Transport::internal(name),
            info.clone(),
            Vec::new(),
        ));
        let entry = Arc::new(SessionEntry::new(name.to_owned(), None, None, &conn, None));
        let tx = Transaction::new_ref(broker.post_office().ids().next(), None);
        Self {
            broker,
            conn,
            entry,
            sender: None,
            last_address: None,
            blocked: Arc::default(),
            name: name.to_owned(),
            auth: SecurityAuth {
                username: None,
                password: None,
                connection: info,
                subject: None,
                management: false,
            },
            validated_user: None,
            xa: false,
            auto_commit_sends: true,
            auto_commit_acks: true,
            pre_acknowledge: false,
            default_address: None,
            started: false,
            closed: true,
            consumers: BTreeMap::new(),
            tx: Some(tx),
            pending_tx: None,
            timeout_seconds: -1,
            ops: Vec::new(),
            actions: Vec::new(),
            unchecked: false,
            direct: false,
            internal: false,
            mirroring: crate::mirror::Mirroring::Enabled,
            replicated: Replicated::default(),
        }
    }

    /// `PostOfficeImpl.route(message, true)` for an internal session: the
    /// message, with the ID it has (and its large body, if any), is routed
    /// with duplicate detection, paged and journaled as a send is, without
    /// permission checks or send hooks.
    pub async fn route_direct(
        &mut self,
        message: CoreMessage,
        large: Option<Arc<LargeBody>>,
    ) -> Result<()> {
        self.unchecked = true;
        self.direct = true;
        let r = self
            .send_message(message, Outgoing::Core(large), None)
            .await;
        self.unchecked = false;
        self.direct = false;
        r
    }

    /// `AMQPMirrorControllerTarget.sendMessage`: a message the peer `origin`
    /// mirrored, kept as it came (its ID there as `x-opt-amq-mr-id`, its
    /// broker as `x-opt-amq-bkr-id`), routed to `address` without
    /// auto-creation, diverts or remote bindings (or to `target_queues`
    /// only), in a transaction of its own (`MirrorTransaction`) with its
    /// duplicate ID `duplicate` added to the mirror's cache. Returns false
    /// for one the cache already has (not routed again).
    #[allow(clippy::too_many_arguments)] // the parts of a mirrored message
    pub fn send_mirrored(
        &mut self,
        native: Arc<dyn NativeMessage>,
        body: Option<Arc<LargeBody>>,
        address: Option<&str>,
        target_queues: Option<&[String]>,
        origin: crate::mirror::Origin,
        cache: &crate::duplicate::CacheRef,
        duplicate: Vec<u8>,
    ) -> Result<bool> {
        let po = self.broker.post_office().clone();
        if crate::duplicate::lock(cache).contains(&duplicate) {
            return Ok(false);
        }
        let view = native.view();
        let mut message = view.clone();
        if message.message_id == 0 {
            message.message_id = po.ids().next();
        }
        if let Some(a) = address
            && message.address.as_ref().is_none_or(|m| !m.eq_str(a))
        {
            message.address = Some(SimpleString::from(a));
        }
        let Some(address) = message.address.as_ref().map(|a| a.to_string_lossy()) else {
            return Err(BrokerError::no_address());
        };
        let settings = po.settings(crate::postoffice::split_fqqn(&address).0);
        crate::message::apply_expiry_delay(&mut message, &settings, crate::message::now_millis());
        let scheduled_time = crate::message::scheduled_delivery_time(&message);
        let n = native.updated(&view, &message);
        let mut r = MessageRef::new(match body {
            Some(b) => ServerMessage::new_native_large(n, b),
            None => ServerMessage::new_native(n),
        });
        r.scheduled_time = scheduled_time;
        let tx = self.new_tx(None);
        {
            let record = po.ids().next();
            let mut t = transaction::lock(&tx);
            let c = crate::duplicate::lock(cache);
            c.store_tx(t.id, &duplicate, record).map_err(io_error)?;
            t.persistent |= c.is_persistent();
            t.duplicates.push(crate::duplicate::PendingId {
                cache: cache.clone(),
                id: duplicate,
                record,
            });
        }
        let routed = po.route_mirrored(&r, &address, target_queues)?;
        self.mirroring = crate::mirror::Mirroring::From(origin);
        self.unchecked = true;
        let sent = self
            .page_routed(routed, Some(&tx))
            .and_then(|routed| self.deliver_routed(routed, Some(tx.clone()), None));
        self.mirroring = crate::mirror::Mirroring::Enabled;
        self.unchecked = false;
        if let Err(e) = sent {
            transaction::lock(&tx).mark_rollback_only(&e);
        }
        self.commit_tx(&tx)?;
        Ok(true)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Who this session is, for the audit log: its connection's subject
    /// and address.
    pub fn who(&self) -> crate::audit::Who<'_> {
        crate::audit::Who {
            subject: self.auth.subject.as_deref(),
            remote: Some(self.conn.remote_address()),
        }
    }

    fn audit(&self) -> crate::audit::SessionAudit<'_> {
        crate::audit::SessionAudit {
            who: self.who(),
            session: &self.name,
            connection: &self.conn,
            node_id: self.broker.node_id(),
            tx_id: self.tx.as_ref().map_or(-1, |t| transaction::lock(t).id),
            consumers: self.consumers.len(),
        }
    }

    /// What management sees of the session.
    pub fn entry(&self) -> &Arc<SessionEntry> {
        &self.entry
    }

    /// Its grants waiting for an address to have room (`blockedRunnables`).
    pub fn blocked_runnables(&self) -> &Arc<crate::paging::BlockedRunnables> {
        &self.blocked
    }

    /// `addProducer`: a producer the protocol names, to `address` or to
    /// any.
    pub fn add_producer(&self, name: &str, protocol: &str, address: Option<&str>) {
        let id = self.broker.registry().next_producer_id();
        self.entry.add_producer(id, name, protocol, address);
    }

    /// `removeProducer`.
    pub fn remove_producer(&self, name: &str) {
        self.entry.remove_producer(name);
    }

    /// The producer the following sends are counted against
    /// (`send(..., senderName)`).
    pub fn set_sender(&mut self, name: Option<Arc<str>>) {
        self.sender = name;
    }

    pub fn broker(&self) -> &Arc<Broker> {
        &self.broker
    }

    pub fn connection(&self) -> &Arc<ClientConnection> {
        &self.conn
    }

    pub fn validated_user(&self) -> Option<&str> {
        self.validated_user.as_deref()
    }

    pub fn username(&self) -> Option<&str> {
        self.auth.username.as_deref()
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn is_started(&self) -> bool {
        self.started
    }

    /// Whether sends and acks are committed one by one.
    pub fn auto_commit_sends(&self) -> bool {
        self.auto_commit_sends
    }

    pub fn auto_commit_acks(&self) -> bool {
        self.auto_commit_acks
    }

    /// The journal writes and queue updates the requests so far lined up;
    /// the protocol answers its client once they are done.
    pub fn take_completion(&mut self) -> Completion {
        Completion {
            waits: std::mem::take(&mut self.ops),
            actions: std::mem::take(&mut self.actions),
            replicated: vec![std::mem::take(&mut self.replicated)],
        }
    }

    /// Makes this a broker connection's session (`createInternalSession`):
    /// its permissions are not checked.
    pub fn set_internal(&mut self) {
        self.internal = true;
    }

    /// Routing on behalf of this session: its security checks and user.
    pub fn creator(&self) -> Creator<'_> {
        Creator {
            security: (!self.direct && !self.internal)
                .then(|| self.broker.security())
                .flatten()
                .map(|s| (&**s, &self.auth)),
            user: self.validated_user.as_deref(),
        }
    }

    /// `ServerSessionImpl.securityCheck`.
    pub fn security_check(
        &self,
        address: &str,
        queue: Option<&str>,
        check: CheckType,
    ) -> Result<()> {
        if self.unchecked || self.internal {
            return Ok(());
        }
        self.creator().check(address, queue, check)
    }

    /// A send the broker makes itself (an advisory), which no permission
    /// stops.
    pub async fn send_unchecked(&mut self, message: CoreMessage) -> Result<()> {
        self.unchecked = true;
        let r = self.send_in(message, None, None).await;
        self.unchecked = false;
        r
    }

    /// `checkAutoCreate(QueueConfiguration)` for `address` without its
    /// acceptor prefix.
    pub fn check_auto_create_with(
        &self,
        address: &str,
        routing_type: Option<RoutingType>,
        options: &crate::postoffice::AutoCreateOptions,
    ) -> Result<crate::postoffice::AutoCreate> {
        let address = self.remove_prefix(address);
        self.broker.post_office().check_auto_create_with(
            address,
            routing_type,
            self.creator(),
            options,
        )
    }

    /// Starts an XA transaction apart from the session's own
    /// (`ResourceManager.putTransaction` of a new `TransactionImpl`).
    pub fn begin_xa(&self, xid: Xid) -> Result<TxRef, TxError> {
        let tx = self.new_tx(Some(xid.clone()));
        transaction::lock(&tx).timeout_seconds = self.timeout_seconds;
        if !self.broker.resource_manager().put(xid.clone(), tx.clone()) {
            return Err(XaError::new(
                xa_code::XAER_DUPID,
                format!(
                    "Cannot start, there is already a xid {}",
                    transaction::xid_string(&xid)
                ),
            )
            .into());
        }
        Ok(tx)
    }

    fn storage(&self) -> Option<Arc<Storage>> {
        self.broker.post_office().ids().storage().cloned()
    }

    /// `newTransaction`, with its journal ID.
    fn new_tx(&self, xid: Option<Xid>) -> TxRef {
        Transaction::new_ref(self.broker.post_office().ids().next(), xid)
    }

    /// Journals a finished transaction's record, then applies it to the
    /// queues: once the record is written when `defer` (commits), at once
    /// otherwise (rollbacks).
    fn finish(&mut self, mut outcome: Outcome, defer: bool) -> Result<()> {
        for c in self.consumers.values() {
            c.metrics.transaction_ended();
        }
        let storage = self.storage();
        if let Some(p) = outcome.write(storage.as_deref())? {
            self.ops.push(p);
        }
        if defer && storage.is_some() {
            let replicated = self.replicated.clone();
            self.actions.push(Box::new(move || {
                let waits = outcome.apply();
                lock_replicated(&replicated).extend(waits);
            }));
        } else {
            let waits = outcome.apply();
            lock_replicated(&self.replicated).extend(waits);
        }
        Ok(())
    }

    /// `PostOfficeImpl.processRoute` with `PagingStore.page`: the queues of
    /// an address that pages get the message in its pages instead of a
    /// reference; a full address under DROP drops it, under FAIL refuses
    /// the send.
    fn page_routed(
        &mut self,
        routed: crate::postoffice::Routed,
        tx: Option<&TxRef>,
    ) -> Result<crate::postoffice::Routed> {
        let paged = self
            .broker
            .post_office()
            .page_routed_mirrored(routed, tx, &self.mirroring)?;
        self.ops.extend(paged.pending);
        // A transaction's messages reach the mirror once it commits.
        match tx {
            Some(tx) => transaction::lock(tx).replicated.extend(paged.replicated),
            None => lock_replicated(&self.replicated).extend(paged.replicated),
        }
        Ok(paged.routed)
    }

    /// `LargeServerMessageImpl.checkLargeMessage`: a message too big for a
    /// journal record becomes a large message.
    fn check_large_message(&self, message: &mut CoreMessage) -> Result<Option<Arc<LargeBody>>> {
        let Some(max) = self.broker.max_record_size() else {
            return Ok(None);
        };
        // The encoded size, without copying the body: its length, the body,
        // then the headers and properties.
        if message.encode_size() + 512 <= max {
            return Ok(None);
        }
        let id = self.broker.post_office().ids().next();
        let io = |e: std::io::Error| BrokerError::new(ErrorType::IoError, e.to_string());
        let mut writer = self
            .broker
            .large_messages()
            .create(id, message.durable)
            .map_err(io)?;
        for chunk in message.body.chunks(crate::large::CHUNK_SIZE) {
            writer.append(chunk).map_err(io)?;
        }
        let size = message.body.len() as i64;
        message.body = Vec::new().into();
        message.message_id = id;
        let key = SimpleString::from(crate::message::HDR_LARGE_BODY_SIZE);
        if message.properties.get(&key).is_none() {
            message
                .properties
                .insert(key, saranga_buffer::PropertyValue::Long(size));
        }
        Ok(Some(writer.finish().map_err(io)?))
    }

    /// `ServerSessionImpl.send`: assigns the ID, routes, and either adds to
    /// the queues or holds the message in the transaction. `large` is the
    /// body of a large message the client streamed; the message's ID is
    /// then already set.
    pub async fn send(
        &mut self,
        message: CoreMessage,
        large: Option<Arc<LargeBody>>,
    ) -> Result<()> {
        // doSend: outside a transaction when auto-committing or when an XA
        // session has none.
        let tx = if self.auto_commit_sends {
            None
        } else {
            self.tx.clone()
        };
        self.send_in(message, large, tx).await
    }

    /// `ServerSessionImpl.send(tx, message, ...)`: as [`ServerSession::send`]
    /// in the given transaction, or none.
    pub async fn send_in(
        &mut self,
        mut message: CoreMessage,
        large: Option<Arc<LargeBody>>,
        tx: Option<TxRef>,
    ) -> Result<()> {
        let large = match large {
            Some(body) => Some(body),
            None => {
                let body = self.check_large_message(&mut message)?;
                if body.is_none() {
                    message.message_id = self.broker.post_office().ids().next();
                }
                body
            }
        };
        self.send_message(message, Outgoing::Core(large), tx).await
    }

    /// As [`ServerSession::send`], keeping the message's ID (a message the
    /// broker made, as management's `sendMessage` does).
    pub async fn send_with_id(&mut self, message: CoreMessage) -> Result<()> {
        let tx = if self.auto_commit_sends {
            None
        } else {
            self.tx.clone()
        };
        self.send_message(message, Outgoing::Core(None), tx).await
    }

    /// Sends a message kept in its own protocol's encoding, which has no ID
    /// yet, in the session's transaction or none as for
    /// [`ServerSession::send`].
    pub async fn send_native(&mut self, native: Arc<dyn NativeMessage>) -> Result<()> {
        let tx = if self.auto_commit_sends {
            None
        } else {
            self.tx.clone()
        };
        self.send_native_in(native, tx).await
    }

    /// As [`ServerSession::send_native`] for a large message whose file is
    /// `body`.
    pub async fn send_native_large(
        &mut self,
        native: Arc<dyn NativeMessage>,
        body: Arc<LargeBody>,
    ) -> Result<()> {
        let tx = if self.auto_commit_sends {
            None
        } else {
            self.tx.clone()
        };
        self.send_native_large_to(native, Some(body), tx, None, None)
            .await
    }

    /// As [`ServerSession::send_native`] in the given transaction, or none.
    pub async fn send_native_in(
        &mut self,
        native: Arc<dyn NativeMessage>,
        tx: Option<TxRef>,
    ) -> Result<()> {
        self.send_native_to(native, tx, None, None).await
    }

    /// As [`ServerSession::send_native_in`], to `address` rather than the
    /// message's own and with the routing type the protocol chose, if
    /// given (`ServerSessionImpl.send(tx, address, routingType, ...)`).
    pub async fn send_native_to(
        &mut self,
        native: Arc<dyn NativeMessage>,
        tx: Option<TxRef>,
        address: Option<&str>,
        routing_type: Option<RoutingType>,
    ) -> Result<()> {
        self.send_native_large_to(native, None, tx, address, routing_type)
            .await
    }

    /// As [`ServerSession::send_native_to`] for a large message whose file
    /// is `body`; its ID is the file's.
    pub async fn send_native_large_to(
        &mut self,
        native: Arc<dyn NativeMessage>,
        body: Option<Arc<LargeBody>>,
        tx: Option<TxRef>,
        address: Option<&str>,
        routing_type: Option<RoutingType>,
    ) -> Result<()> {
        let view = native.view();
        let mut message = view.clone();
        // A large message has its ID already: its file is named after it.
        if message.message_id == 0 {
            message.message_id = self.broker.post_office().ids().next();
        }
        if let Some(a) = address {
            message.address = Some(SimpleString::from(a));
        }
        self.send_message(
            message,
            Outgoing::Native(native, view, routing_type, body),
            tx,
        )
        .await
    }

    /// The rest of `send`: `message` is the message, or the view of a
    /// native one, with its ID.
    async fn send_message(
        &mut self,
        mut message: CoreMessage,
        outgoing: Outgoing,
        tx: Option<TxRef>,
    ) -> Result<()> {
        let entry = self.entry.clone();
        let _critical = entry.critical.enter();
        let address: Name = match &message.address {
            Some(a) => {
                let a: Name = match &self.last_address {
                    Some((s, n)) if s == a => n.clone(),
                    _ => {
                        let n: Name = a.to_string_lossy().into();
                        self.last_address = Some((a.clone(), n.clone()));
                        n
                    }
                };
                if self.default_address.is_none() {
                    self.entry.set_default_address(&a);
                }
                self.default_address.get_or_insert_with(|| a.clone());
                a
            }
            None => {
                let a = self
                    .default_address
                    .clone()
                    .ok_or_else(BrokerError::no_address)?;
                message.address = Some(SimpleString::from(&*a));
                a
            }
        };
        // doSend: an address with one of the acceptor's prefixes loses it
        // and is routed with the prefix's routing type.
        let (address, prefixed): (Name, Option<RoutingType>) = match self.strip_prefix(&address) {
            (bare, Some(t)) => {
                let bare: Name = bare.into();
                message.address = Some(SimpleString::from(&*bare));
                (bare, Some(t))
            }
            (_, None) => (address, None),
        };
        // beforeSend: a session's own sends, not the replies the broker
        // sends for it.
        let plugins = (!self.unchecked)
            .then(|| self.broker.post_office().plugins().clone())
            .filter(|p| !p.is_empty());
        if let Some(plugins) = &plugins
            && let Err(e) = plugins.check(|p| p.before_send(&entry, &message, tx.is_some()))
        {
            plugins.each(|p| p.on_send_exception(&entry, &message, &e));
            return Err(e);
        }
        // A client that credits cannot hold back is failed while the disk
        // is full: there is nothing else to do with it.
        if !self.conn.supports_flow_control() {
            let paging = self.broker.post_office().paging();
            if paging.is_disk_full() {
                let (usable, total) = paging.disk_space();
                let e = BrokerError::new(
                    ErrorType::IoError,
                    format!(
                        "AMQ229119: Free storage space is at {} of {} total. Usage rate is {:.1}% which is beyond the configured <max-disk-usage>. System will start blocking producers.",
                        crate::disk::human_bytes(usable),
                        crate::disk::human_bytes(total),
                        crate::disk::usage_of(usable, total) * 100.0
                    ),
                );
                self.conn.fail_with(e.clone());
                if let Some(plugins) = &plugins {
                    plugins.each(|p| p.on_send_exception(&entry, &message, &e));
                }
                return Err(e);
            }
        }
        // handleManagementMessage: a message to the management address is
        // a request, answered on its reply-to address.
        if !self.unchecked && *address == *self.broker.management().address() {
            let request = match &outgoing {
                Outgoing::Native(native, ..) => native.to_core(),
                Outgoing::Core(_) => message,
            };
            let Some(plugins) = plugins else {
                return Box::pin(self.handle_management_message(request, tx)).await;
            };
            let sent = ServerMessage::new(request.clone());
            let handled = Box::pin(self.handle_management_message(request, tx.clone())).await;
            match &handled {
                Ok(()) => {
                    let t = tx.as_ref().map(transaction::lock);
                    plugins.each(|p| {
                        p.after_send(
                            &entry,
                            &sent,
                            crate::plugin::RoutingStatus::Ok,
                            t.as_deref(),
                        )
                    });
                }
                Err(e) => plugins.each(|p| p.on_send_exception(&entry, sent.core(), e)),
            }
            return handled;
        }
        // The user must be allowed to send to the address (and to the queue
        // a fully qualified name picks).
        let (bare_address, fqqn_queue) = crate::postoffice::split_fqqn(&address);
        if let Err(e) = self.security_check(bare_address, fqqn_queue, CheckType::Send) {
            if let Some(tx) = &tx {
                transaction::lock(tx).mark_rollback_only(&e);
            }
            return Err(e);
        }
        if self.broker.populate_validated_user()
            && let Some(user) = &self.validated_user
        {
            message.properties.insert(
                crate::message::HDR_VALIDATED_USER.into(),
                saranga_buffer::PropertyValue::String(user.as_str().into()),
            );
        }
        if !self.direct
            && !self.internal
            && self.broker.reject_empty_validated_user()
            && message
                .properties
                .get_str(crate::message::HDR_VALIDATED_USER)
                .is_none()
        {
            return Err(BrokerError::reject_empty_validated_user());
        }
        let chosen = match &outgoing {
            Outgoing::Native(_, _, t, _) => *t,
            Outgoing::Core(_) => None,
        };
        let routing_type = prefixed.or(chosen).or_else(|| routing_type(&message));
        // PostOfficeImpl.route: the address's expiry settings apply first.
        let settings = self
            .broker
            .post_office()
            .settings(crate::postoffice::split_fqqn(&address).0);
        crate::message::apply_expiry_delay(&mut message, &settings, crate::message::now_millis());
        let scheduled_time = crate::message::scheduled_delivery_time(&message);
        // checkDuplicateID: a bridge's duplicate ID comes off the message and
        // is checked, in place of the message's own, in the bridge cache of
        // the address (BRIDGE.<address>).
        let bridge_dup = match message
            .properties
            .remove(&SimpleString::from(crate::message::HDR_BRIDGE_DUPLICATE_ID))
        {
            Some(saranga_buffer::PropertyValue::Bytes(b)) => Some(b),
            _ => None,
        };
        let duplicate_id = match bridge_dup {
            Some(b) => Some((b, true)),
            None => crate::duplicate::duplicate_id_bytes(&message).map(|d| (d, false)),
        };
        let mut r = MessageRef::new(match outgoing {
            Outgoing::Core(Some(body)) => ServerMessage::new_large(message, body),
            Outgoing::Core(None) => ServerMessage::new(message),
            // Unchanged by the broker, as a message whose protocol set its ID
            // and address is: the native message and its view as they are.
            Outgoing::Native(native, before, _, body) if before == message => {
                ServerMessage::with_view(native, before, body)
            }
            Outgoing::Native(native, before, _, body) => {
                let n = native.updated(&before, &message);
                match body {
                    Some(b) => ServerMessage::new_native_large(n, b),
                    None => ServerMessage::new_native(n),
                }
            }
        });
        r.scheduled_time = scheduled_time;
        // updateProducerMetrics, with the size before routing.
        if !self.unchecked {
            let size = match r.message.large() {
                Some(body) => body.size() as i64,
                None => r.message.encode_size() as i64,
            };
            let user_id = r
                .message
                .core()
                .user_id
                .map(|u| uuid::Uuid::from_bytes(u).to_string());
            self.entry.producer_sent(
                self.broker.registry(),
                self.sender.as_deref(),
                &address,
                user_id,
                size,
            );
        }
        let storage = self.storage();
        // checkDuplicateID: a send outside a transaction gets one of its own
        // so the ID is journaled with the message.
        let mut own_tx: Option<(i64, bool)> = None;
        if let Some((id, bridge)) = duplicate_id {
            let po = self.broker.post_office();
            let name = crate::postoffice::split_fqqn(&address).0;
            let cache = if bridge {
                po.duplicate_cache(&format!("BRIDGE.{name}"))
            } else {
                po.duplicate_cache(name)
            };
            let mut c = crate::duplicate::lock(&cache);
            if c.contains(&id) {
                if bridge {
                    tracing::trace!("Message {} hit a bridge duplicate", r.message.message_id());
                } else {
                    tracing::warn!(
                        "AMQ222059: Duplicate message detected - message will not be routed. Message information:\n{}",
                        r.message.message_id()
                    );
                }
                if !bridge && let Some(tx) = &tx {
                    transaction::lock(tx).mark_rollback_only(&BrokerError::new(
                        ErrorType::DuplicateIdRejected,
                        format!(
                            "Duplicate message detected - message will not be routed. Message information:{}",
                            r.message.message_id()
                        ),
                    ));
                }
                if let Some(plugins) = &plugins {
                    let t = tx.as_ref().map(transaction::lock);
                    plugins.each(|p| {
                        p.after_send(
                            &entry,
                            &r.message,
                            crate::plugin::RoutingStatus::DuplicatedId,
                            t.as_deref(),
                        )
                    });
                }
                return Ok(());
            }
            let record = po.ids().next();
            match &tx {
                None => {
                    let jtx = po.ids().next();
                    c.store_tx(jtx, &id, record).map_err(io_error)?;
                    own_tx = Some((jtx, c.is_persistent()));
                    c.add_in_memory(id, record);
                }
                Some(tx) => {
                    let mut t = transaction::lock(tx);
                    c.store_tx(t.id, &id, record).map_err(io_error)?;
                    t.persistent |= c.is_persistent();
                    t.duplicates.push(crate::duplicate::PendingId {
                        cache: cache.clone(),
                        id,
                        record,
                    });
                }
            }
        }
        let routed = self
            .broker
            .post_office()
            .route(&r, &address, routing_type, self.creator());
        let routed = match routed.and_then(|routed| self.page_routed(routed, tx.as_ref())) {
            Ok(routed) => routed,
            Err(e) => {
                if let Some(tx) = &tx {
                    transaction::lock(tx).mark_rollback_only(&e);
                }
                if let (Some((jtx, true)), Some(s)) = (own_tx, &storage) {
                    let _ = s.rollback(jtx);
                }
                if let Some(plugins) = &plugins {
                    plugins.each(|p| p.on_send_exception(&entry, r.message.core(), &e));
                }
                return Err(e);
            }
        };
        let Some(plugins) = plugins else {
            return self.deliver_routed(routed, tx, own_tx);
        };
        let status = crate::plugin::RoutingStatus::of(&routed);
        let sent = self.deliver_routed(routed, tx.clone(), own_tx);
        match &sent {
            Ok(()) => {
                let t = tx.as_ref().map(transaction::lock);
                plugins.each(|p| p.after_send(&entry, &r.message, status, t.as_deref()));
            }
            Err(e) => plugins.each(|p| p.on_send_exception(&entry, r.message.core(), e)),
        }
        sent
    }

    /// `ServerSessionImpl.handleManagementMessage`: with the `manage`
    /// permission, the request is run and the reply sent to its reply-to
    /// address, in the send's transaction.
    async fn handle_management_message(
        &mut self,
        request: CoreMessage,
        tx: Option<TxRef>,
    ) -> Result<()> {
        if crate::audit::base_enabled() {
            // The session's transaction, whether the send is in it or not.
            let audit = self.audit();
            let session_tx = self.tx.as_ref().map(transaction::lock);
            audit.management_message(
                session_tx.as_deref(),
                &crate::message::ServerMessage::new(request.clone()),
            );
        }
        let address = self.broker.management().address().to_owned();
        if let Err(e) = self.security_check(&address, None, CheckType::Manage) {
            if let Some(tx) = &tx {
                transaction::lock(tx).mark_rollback_only(&e);
            }
            return Err(e);
        }
        let broker = self.broker.clone();
        let caller = crate::management::Caller {
            security: broker.security().map(|s| (&**s, &self.auth)),
            who: Some(self.who()),
        };
        let mut reply = broker
            .management()
            .handle_message(&broker, &request, caller)
            .await;
        let reply_to = crate::message::string_property(&request, crate::management::REPLY_TO).map(
            |reply_to| {
                ["queue://", "topic://", "temp-queue://", "temp-topic://"]
                    .iter()
                    .find_map(|p| reply_to.strip_prefix(p))
                    .unwrap_or(&reply_to)
                    .to_owned()
            },
        );
        if let Some(reply_to) = &reply_to {
            reply.address = Some(SimpleString::from(reply_to.as_str()));
            self.unchecked = true;
            let sent = self
                .send_message(reply, Outgoing::Core(None), tx.clone())
                .await;
            self.unchecked = false;
            sent?;
        }
        // The request is logged as sent once handled, as any other message;
        // its routing context is the reply's (the session reuses one).
        if crate::audit::message_enabled() {
            let message = crate::message::ServerMessage::new(request);
            match &tx {
                Some(tx) if !self.auto_commit_sends => {
                    let tx = crate::audit::transaction_string(&transaction::lock(tx));
                    crate::audit::send_in_transaction(self.who(), &message, &tx);
                }
                _ => {
                    let context = match &reply_to {
                        Some(address) => {
                            let queues: Vec<Queue> = self
                                .broker
                                .post_office()
                                .queues()
                                .into_iter()
                                .filter(|q| *q.config().address == **address)
                                .collect();
                            crate::audit::routing_context(address, &queues)
                        }
                        None => crate::audit::routing_context("null", &[]),
                    };
                    crate::audit::sent(self.who(), &message, &context, None);
                }
            }
        }
        Ok(())
    }

    /// `MQTTUtil.sendMessageDirectlyToQueue`: the message goes to this one
    /// queue, whatever is bound to its address, in the transaction given
    /// or none. Its ID must be set.
    pub fn send_to_queue(
        &mut self,
        message: CoreMessage,
        large: Option<Arc<LargeBody>>,
        queue: &Queue,
        tx: Option<TxRef>,
    ) -> Result<()> {
        let scheduled_time = crate::message::scheduled_delivery_time(&message);
        let mut r = MessageRef::new(match large {
            Some(body) => ServerMessage::new_large(message, body),
            None => ServerMessage::new(message),
        });
        r.scheduled_time = scheduled_time;
        let routed = self.page_routed(vec![(r, vec![queue.clone()])], tx.as_ref())?;
        self.deliver_routed(routed, tx, None)
    }

    /// Journals routed messages and hands them to their queues: at once
    /// (once journaled) outside a transaction, at commit inside one.
    /// `own_tx` is the journal transaction a send outside a transaction
    /// took for its duplicate ID.
    fn deliver_routed(
        &mut self,
        routed: crate::postoffice::Routed,
        tx: Option<TxRef>,
        mut own_tx: Option<(i64, bool)>,
    ) -> Result<()> {
        // The broker's own sends (a management reply, an advisory) are not
        // the session's.
        if crate::audit::message_enabled()
            && !self.unchecked
            && let Some((r, _)) = routed.first()
        {
            // The message as sent; its divert copies are not logged.
            match &tx {
                Some(tx) if !self.auto_commit_sends => {
                    let tx = crate::audit::transaction_string(&transaction::lock(tx));
                    crate::audit::send_in_transaction(self.who(), &r.message, &tx);
                }
                _ => {
                    let address = r.message.core().address.as_ref().map(ToString::to_string);
                    let queues: Vec<Queue> = routed.iter().flat_map(|(_, q)| q.clone()).collect();
                    let context = crate::audit::routing_context(
                        address.as_deref().unwrap_or("null"),
                        &queues,
                    );
                    crate::audit::sent(self.who(), &r.message, &context, None);
                }
            }
        }
        let storage = self.storage();
        // processRouteToDurableQueues, for the message and each divert
        // copy: the message record with its first durable reference, then
        // one reference per durable queue.
        match tx {
            None => {
                if let Some(s) = &storage {
                    for (r, queues) in &routed {
                        let message = &r.message;
                        let scheduled_time = r.scheduled_time;
                        let durable: Vec<&Queue> =
                            queues.iter().filter(|q| q.is_durable_ref(r)).collect();
                        let last = durable.len().saturating_sub(1);
                        for (i, q) in durable.into_iter().enumerate() {
                            let mid = message.message_id();
                            if let Some((jtx, persistent)) = &mut own_tx {
                                if message.durable_up() == 1 {
                                    s.store_message_tx(*jtx, message).map_err(io_error)?;
                                }
                                s.store_reference_tx(*jtx, q.id(), mid).map_err(io_error)?;
                                if scheduled_time > 0 {
                                    s.update_scheduled_delivery_time_tx(
                                        *jtx,
                                        q.id(),
                                        mid,
                                        scheduled_time,
                                    )
                                    .map_err(io_error)?;
                                }
                                *persistent = true;
                                continue;
                            }
                            if message.durable_up() == 1 {
                                s.store_message(message).map_err(io_error)?;
                            }
                            self.ops.push(
                                s.store_reference(q.id(), mid, i == last)
                                    .map_err(io_error)?,
                            );
                            if scheduled_time > 0
                                && let Some(p) = s
                                    .update_scheduled_delivery_time(q.id(), mid, scheduled_time, 0)
                                    .map_err(io_error)?
                            {
                                self.ops.push(p);
                            }
                        }
                    }
                    if let Some((jtx, true)) = own_tx {
                        self.ops.push(s.commit(jtx).map_err(io_error)?);
                    }
                    // The references count against their addresses now,
                    // and join their queues once journaled.
                    for (r, queues) in &routed {
                        for q in queues {
                            q.ref_up(r);
                        }
                    }
                    self.actions.push(Box::new(move || {
                        for (r, queues) in routed {
                            for q in queues {
                                q.add_counted(r.clone());
                            }
                        }
                    }));
                } else {
                    for (r, queues) in routed {
                        for q in queues {
                            q.add(r.clone());
                        }
                    }
                }
            }
            Some(tx) => {
                let mut t = transaction::lock(&tx);
                for (r, queues) in routed {
                    let message = &r.message;
                    for q in queues.iter().filter(|q| q.is_durable_ref(&r)) {
                        let Some(s) = &storage else { break };
                        if message.durable_up() == 1 {
                            s.store_message_tx(t.id, message).map_err(io_error)?;
                        }
                        s.store_reference_tx(t.id, q.id(), message.message_id())
                            .map_err(io_error)?;
                        if r.scheduled_time > 0 {
                            s.update_scheduled_delivery_time_tx(
                                t.id,
                                q.id(),
                                message.message_id(),
                                r.scheduled_time,
                            )
                            .map_err(io_error)?;
                        }
                        t.persistent = true;
                    }
                    // Counted now; they join their queues at the commit.
                    for q in &queues {
                        q.ref_up(&r);
                    }
                    t.sends.extend(queues.into_iter().map(|q| (q, r.clone())));
                }
            }
        }
        Ok(())
    }

    /// `ServerSessionImpl.acknowledge` and `individualAcknowledge`: the
    /// consumer's message, and with `!individual` every message delivered
    /// before it. Returns the IDs of the messages acknowledged.
    pub async fn acknowledge(
        &mut self,
        consumer_id: i64,
        message_id: i64,
        individual: bool,
    ) -> Result<Vec<i64>> {
        let tx = if self.auto_commit_acks {
            None
        } else {
            self.tx.clone()
        };
        if !self.consumers.contains_key(&consumer_id)
            && let Some(tx) = &self.tx
        {
            transaction::lock(tx)
                .mark_rollback_only(&BrokerError::consumer_doesnt_exist(consumer_id));
        }
        self.acknowledge_in(consumer_id, message_id, individual, tx)
            .await
    }

    /// As [`ServerSession::acknowledge`], in the given transaction or none.
    pub async fn acknowledge_in(
        &mut self,
        consumer_id: i64,
        message_id: i64,
        individual: bool,
        tx: Option<TxRef>,
    ) -> Result<Vec<i64>> {
        let entry = self.entry.clone();
        let _critical = entry.critical.enter();
        let Some(c) = self.consumers.get(&consumer_id) else {
            let e = BrokerError::consumer_doesnt_exist(consumer_id);
            if let Some(tx) = &tx {
                transaction::lock(tx).mark_rollback_only(&e);
            }
            return Err(e);
        };
        let queue = c.queue.clone();
        let result = queue.ack(c.id, message_id, individual, tx.is_some()).await;
        let pending = AckPending {
            consumer_id,
            message_id,
            queue,
        };
        self.acknowledged_in(pending, tx, result)
    }

    /// [`acknowledge_in`](Self::acknowledge_in) outside a transaction,
    /// without waiting for the queue: `then` has its answer, on the
    /// queue's task, and gives it to
    /// [`acknowledged`](Self::acknowledged). A connection whose client
    /// acknowledges message after message so has them all on their way at
    /// once, rather than each waiting for the one before.
    pub fn acknowledge_then(
        &mut self,
        consumer_id: i64,
        message_id: i64,
        individual: bool,
        then: impl FnOnce(AckPending, AckResult) + Send + 'static,
    ) -> Result<()> {
        let Some(c) = self.consumers.get(&consumer_id) else {
            return Err(BrokerError::consumer_doesnt_exist(consumer_id));
        };
        let pending = AckPending {
            consumer_id,
            message_id,
            queue: c.queue.clone(),
        };
        c.queue
            .ack_then(c.id, message_id, individual, false, move |result| {
                then(pending, result)
            });
        Ok(())
    }

    /// The rest of an [`acknowledge_then`](Self::acknowledge_then), once the
    /// queue answered.
    pub fn acknowledged(&mut self, pending: AckPending, result: AckResult) -> Result<Vec<i64>> {
        let entry = self.entry.clone();
        let _critical = entry.critical.enter();
        self.acknowledged_in(pending, None, result)
    }

    /// [`acknowledge_in`](Self::acknowledge_in) of one message as part of
    /// `tx`, without waiting for the queue: `then` has its answer, on the
    /// queue's task, and gives it to
    /// [`acknowledged_tx`](Self::acknowledged_tx) before `tx` commits.
    pub fn acknowledge_in_then(
        &mut self,
        consumer_id: i64,
        message_id: i64,
        individual: bool,
        tx: &TxRef,
        then: impl FnOnce(AckPending, AckResult) + Send + 'static,
    ) -> Result<()> {
        let Some(c) = self.consumers.get(&consumer_id) else {
            let e = BrokerError::consumer_doesnt_exist(consumer_id);
            transaction::lock(tx).mark_rollback_only(&e);
            return Err(e);
        };
        let pending = AckPending {
            consumer_id,
            message_id,
            queue: c.queue.clone(),
        };
        c.queue
            .ack_then(c.id, message_id, individual, true, move |result| {
                then(pending, result)
            });
        Ok(())
    }

    /// The rest of an [`acknowledge_in_then`](Self::acknowledge_in_then),
    /// once the queue answered: the references join `tx`.
    pub fn acknowledged_tx(
        &mut self,
        pending: AckPending,
        tx: &TxRef,
        result: AckResult,
    ) -> Result<Vec<i64>> {
        let entry = self.entry.clone();
        let _critical = entry.critical.enter();
        self.acknowledged_in(pending, Some(tx.clone()), result)
    }

    fn acknowledged_in(
        &mut self,
        pending: AckPending,
        tx: Option<TxRef>,
        result: AckResult,
    ) -> Result<Vec<i64>> {
        let AckPending {
            consumer_id,
            message_id,
            queue,
        } = pending;
        match result {
            Ok(mut refs) => {
                let ids = refs.iter().map(MessageRef::message_id).collect();
                if crate::audit::message_enabled() {
                    let name = queue.name().to_string();
                    match &tx {
                        Some(tx) => {
                            let tx = crate::audit::transaction_string(&transaction::lock(tx));
                            for r in &refs {
                                crate::audit::ack_in_transaction(
                                    self.who(),
                                    &name,
                                    &r.message,
                                    &tx,
                                );
                            }
                        }
                        None => {
                            // ServerConsumerImpl.acknowledge: in a
                            // transaction of its own, committed at once.
                            let t =
                                Transaction::new_ref(self.broker.post_office().ids().next(), None);
                            let mut t = transaction::lock(&t);
                            let active = crate::audit::transaction_string(&t);
                            for r in &refs {
                                crate::audit::ack_in_transaction(
                                    self.who(),
                                    &name,
                                    &r.message,
                                    &active,
                                );
                            }
                            t.state = TxState::Committed;
                            let committed = crate::audit::transaction_string(&t);
                            for r in &refs {
                                crate::audit::acknowledged(
                                    self.who(),
                                    &name,
                                    &r.message,
                                    Some(&committed),
                                );
                            }
                        }
                    }
                }
                // QueueImpl.acknowledge
                match tx {
                    Some(tx) => {
                        let mut t = transaction::lock(&tx);
                        let id = t.id;
                        for r in &mut refs {
                            if queue.store_ack_tx(id, r, &mut t.paging).map_err(io_error)? {
                                t.persistent = true;
                            }
                        }
                        t.acks.extend(refs.into_iter().map(|r| (queue.clone(), r)));
                    }
                    None => {
                        for r in &mut refs {
                            if let Some(p) = queue.store_ack(r).map_err(io_error)? {
                                self.ops.push(p);
                            }
                            if let Some(p) = queue.post_acknowledge(r) {
                                lock_replicated(&self.replicated).push(p);
                            }
                        }
                    }
                }
                Ok(ids)
            }
            Err(failed) => {
                let e = BrokerError::consumer_no_reference(consumer_id, message_id, queue.name());
                match tx {
                    Some(tx) => {
                        let mut t = transaction::lock(&tx);
                        t.acks
                            .extend(failed.refs.into_iter().map(|r| (queue.clone(), r)));
                        t.mark_rollback_only(&e);
                    }
                    None => queue.reinsert(failed.refs),
                }
                Err(e)
            }
        }
    }

    /// `ServerSessionImpl.expire`: the client found the message expired.
    pub fn expire(&mut self, consumer_id: i64, message_id: i64) -> Result<()> {
        match self.consumers.get(&consumer_id) {
            Some(c) => {
                c.queue.expire(c.id, message_id);
                Ok(())
            }
            None => Err(BrokerError::consumer_doesnt_exist(consumer_id)),
        }
    }

    /// `ServerSessionImpl.receiveConsumerCredits`: -1 for no flow control.
    pub fn consumer_credits(&self, consumer_id: i64, credits: i32) {
        if let Some(c) = self.consumers.get(&consumer_id) {
            c.queue.credits(c.id, credits);
        }
    }

    /// `ServerSessionImpl.forceConsumerDelivery`.
    pub fn force_delivery(&self, consumer_id: i64, sequence: i64) {
        if let Some(c) = self.consumers.get(&consumer_id) {
            c.queue.force_delivery(c.id, sequence);
        }
    }

    /// `ServerConsumerImpl.individualCancel`: a message the consumer is
    /// delivering goes back to its queue.
    pub fn cancel_one(&self, consumer_id: i64, message_id: i64, failed: bool) {
        if let Some(c) = self.consumers.get(&consumer_id) {
            c.queue.cancel_one(c.id, message_id, failed);
        }
    }

    /// `individualCancel` after `rejectConsumer`: the message goes back,
    /// but not to this consumer.
    pub fn cancel_undeliverable(&self, consumer_id: i64, message_id: i64, failed: bool) {
        if let Some(c) = self.consumers.get(&consumer_id) {
            c.queue.cancel_undeliverable(c.id, message_id, failed);
        }
    }

    /// `ServerConsumerImpl.reject`: the message goes to the dead-letter
    /// address. A browser takes nothing, so rejects nothing.
    pub fn reject(&self, consumer_id: i64, message_id: i64) {
        if let Some(c) = self.consumers.get(&consumer_id)
            && !c.browse_only
        {
            c.queue.reject(c.id, message_id);
        }
    }

    /// [`ServerSession::reject`], the dead-letter copy getting
    /// `properties`.
    pub fn reject_with(
        &self,
        consumer_id: i64,
        message_id: i64,
        properties: Vec<(String, saranga_buffer::PropertyValue)>,
    ) {
        if let Some(c) = self.consumers.get(&consumer_id)
            && !c.browse_only
        {
            c.queue.reject_with(c.id, message_id, properties);
        }
    }

    /// Whether consumers created from now on are acknowledged as their
    /// messages go out (`ServerConsumer.setPreAcknowledge`).
    pub fn set_pre_acknowledge(&mut self, pre_acknowledge: bool) {
        self.pre_acknowledge = pre_acknowledge;
    }

    /// `ServerConsumerImpl.promptDelivery`.
    pub fn prompt_delivery(&self, consumer_id: i64) {
        if let Some(c) = self.consumers.get(&consumer_id) {
            c.queue.prompt_delivery();
        }
    }

    /// Whether the session has this consumer (`locateConsumer`).
    pub fn has_consumer(&self, consumer_id: i64) -> bool {
        self.consumers.contains_key(&consumer_id)
    }

    /// The queue a consumer reads from.
    pub fn consumer_queue(&self, consumer_id: i64) -> Option<&Queue> {
        self.consumers.get(&consumer_id).map(|c| &c.queue)
    }

    fn current_xid_is(&self, xid: &Xid) -> bool {
        self.tx
            .as_ref()
            .is_some_and(|t| transaction::lock(t).xid.as_ref() == Some(xid))
    }

    /// `xaStart`: a transaction still open on the session is rolled back
    /// (unless prepared) and replaced.
    pub fn xa_start(&mut self, xid: Xid) -> Result<(), TxError> {
        if let Some(old) = self.tx.take() {
            let mut t = transaction::lock(&old);
            tracing::warn!(
                "AMQ222210: Session already had a transaction {} when xaStart {} was called",
                t.xid
                    .as_ref()
                    .map(transaction::xid_string)
                    .unwrap_or_default(),
                transaction::xid_string(&xid)
            );
            if t.state != TxState::Prepared {
                if let Some(x) = &t.xid {
                    self.broker.resource_manager().remove(x);
                }
                if let Ok(outcome) = t.rollback() {
                    drop(t);
                    self.finish(outcome, false)?;
                }
            }
        }
        let tx = self.new_tx(Some(xid.clone()));
        transaction::lock(&tx).timeout_seconds = self.timeout_seconds;
        self.tx = Some(tx.clone());
        if !self.broker.resource_manager().put(xid.clone(), tx) {
            return Err(XaError::new(
                xa_code::XAER_DUPID,
                format!(
                    "Cannot start, there is already a xid {}",
                    transaction::xid_string(&xid)
                ),
            )
            .into());
        }
        Ok(())
    }

    /// `xaEnd`.
    pub fn xa_end(&mut self, xid: Xid) -> Result<(), TxError> {
        if self.current_xid_is(&xid) {
            let tx = self.tx.clone().expect("current transaction");
            let state = transaction::lock(&tx).state;
            match state {
                TxState::Suspended => Err(XaError::new(
                    xa_code::XAER_PROTO,
                    "Cannot end, transaction is suspended",
                )
                .into()),
                TxState::RolledBack => {
                    self.tx = None;
                    Err(XaError::new(
                        xa_code::XAER_PROTO,
                        "Cannot end, transaction is rolled back",
                    )
                    .into())
                }
                _ => {
                    self.pending_tx = self.tx.take();
                    Ok(())
                }
            }
        } else {
            // Ending a suspended branch (JTA 1.1, 3.4.4).
            let Some(tx) = self.broker.resource_manager().get(&xid) else {
                return Err(XaError::new(
                    xa_code::XAER_NOTA,
                    format!(
                        "Cannot find suspended transaction to end {}",
                        transaction::xid_string(&xid)
                    ),
                )
                .into());
            };
            let mut t = transaction::lock(&tx);
            match t.state {
                TxState::Active => Ok(()),
                TxState::Suspended => {
                    t.state = TxState::Active;
                    Ok(())
                }
                _ => Err(XaError::new(
                    xa_code::XAER_PROTO,
                    format!(
                        "Transaction is not suspended {}",
                        transaction::xid_string(&xid)
                    ),
                )
                .into()),
            }
        }
    }

    fn not_found(xid: &Xid) -> TxError {
        XaError::new(
            xa_code::XAER_NOTA,
            format!(
                "Cannot find xid in resource manager: {}",
                transaction::xid_string(xid)
            ),
        )
        .into()
    }

    /// `xaPrepare`.
    pub fn xa_prepare(&mut self, xid: Xid) -> Result<(), TxError> {
        if self.current_xid_is(&xid) {
            let current = self
                .tx
                .as_ref()
                .and_then(|t| transaction::lock(t).xid.clone());
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot commit, session is currently doing work in a transaction {}",
                    current
                        .as_ref()
                        .map(transaction::xid_string)
                        .unwrap_or_default()
                ),
            )
            .into());
        }
        let Some(tx) = self.broker.resource_manager().get(&xid) else {
            return Err(Self::not_found(&xid));
        };
        let mut t = transaction::lock(&tx);
        match t.state {
            TxState::Suspended => Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot prepare transaction, it is suspended {}",
                    transaction::xid_string(&xid)
                ),
            )
            .into()),
            TxState::Prepared => {
                tracing::warn!(
                    "AMQ222098: Ignoring prepare on xid as already called : {}",
                    transaction::xid_string(&xid)
                );
                Ok(())
            }
            _ => match t.prepare() {
                Ok(()) => {
                    let (id, xid) = (t.id, t.xid.clone().expect("XA transaction"));
                    if let Some(s) = self.storage() {
                        t.paging.before_commit(&s, id).map_err(io_error)?;
                        drop(t);
                        self.ops.push(s.prepare(id, &xid).map_err(io_error)?);
                    }
                    Ok(())
                }
                Err(failed) => {
                    let (e, outcome) = *failed;
                    drop(t);
                    self.finish(outcome, false)?;
                    Err(e.into())
                }
            },
        }
    }

    /// `xaCommit`.
    pub fn xa_commit(&mut self, xid: Xid, one_phase: bool) -> Result<(), TxError> {
        self.pending_tx = None;
        if self.current_xid_is(&xid) {
            let current = self
                .tx
                .as_ref()
                .and_then(|t| transaction::lock(t).xid.clone());
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot commit, session is currently doing work in transaction {}",
                    current
                        .as_ref()
                        .map(transaction::xid_string)
                        .unwrap_or_default()
                ),
            )
            .into());
        }
        let rm = self.broker.resource_manager();
        let Some(tx) = rm.remove(&xid) else {
            return Err(match rm.heuristic(&xid) {
                Some(true) => XaError::new(
                    xa_code::XA_HEURCOM,
                    format!(
                        "transaction has been heuristically committed: {}",
                        transaction::xid_string(&xid)
                    ),
                )
                .into(),
                Some(false) => XaError::new(
                    xa_code::XA_HEURRB,
                    format!(
                        "transaction has been heuristically rolled back: {}",
                        transaction::xid_string(&xid)
                    ),
                )
                .into(),
                None => Self::not_found(&xid),
            });
        };
        let mut t = transaction::lock(&tx);
        if t.state == TxState::Suspended {
            drop(t);
            rm.put(xid.clone(), tx);
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot commit transaction, it is suspended {}",
                    transaction::xid_string(&xid)
                ),
            )
            .into());
        }
        let result = t.commit(one_phase);
        drop(t);
        match result {
            Ok(outcome) => {
                self.finish(outcome, true)?;
                Ok(())
            }
            Err(failed) => {
                let (e, outcome) = *failed;
                self.finish(outcome, false)?;
                Err(e.into())
            }
        }
    }

    /// `xaRollback`: rolls back the branch, and like Artemis also takes
    /// back what this session's consumers are delivering.
    pub async fn xa_rollback(&mut self, xid: Xid) -> Result<(), TxError> {
        self.pending_tx = None;
        if self.current_xid_is(&xid) {
            let current = self
                .tx
                .as_ref()
                .and_then(|t| transaction::lock(t).xid.clone());
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot roll back, session is currently doing work in a transaction {}",
                    current
                        .as_ref()
                        .map(transaction::xid_string)
                        .unwrap_or_default()
                ),
            )
            .into());
        }
        let rm = self.broker.resource_manager();
        let Some(tx) = rm.remove(&xid) else {
            // Artemis spells it "ben" here.
            return Err(match rm.heuristic(&xid) {
                Some(true) => XaError::new(
                    xa_code::XA_HEURCOM,
                    format!(
                        "transaction has ben heuristically committed: {}",
                        transaction::xid_string(&xid)
                    ),
                )
                .into(),
                Some(false) => XaError::new(
                    xa_code::XA_HEURRB,
                    format!(
                        "transaction has ben heuristically rolled back: {}",
                        transaction::xid_string(&xid)
                    ),
                )
                .into(),
                None => Self::not_found(&xid),
            });
        };
        if transaction::lock(&tx).state == TxState::Suspended {
            rm.put(xid.clone(), tx);
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot rollback transaction, it is suspended {}",
                    transaction::xid_string(&xid)
                ),
            )
            .into());
        }
        self.do_rollback(false, false, &tx).await?;
        Ok(())
    }

    /// `xaForget`: a branch management completed is forgotten and its
    /// record deleted.
    pub fn xa_forget(&mut self, xid: Xid) -> Result<(), TxError> {
        let Some(record) = self.broker.resource_manager().remove_heuristic(&xid) else {
            return Err(XaError {
                code: xa_code::XAER_NOTA,
                message: None,
            }
            .into());
        };
        if let Some(s) = self.storage() {
            match s.delete_heuristic_completion(record) {
                Ok(p) => self.ops.push(p),
                Err(e) => {
                    tracing::warn!(
                        "AMQ222182: Unable to delete heuristic completion from storage manager: {e}"
                    );
                    return Err(XaError {
                        code: xa_code::XAER_RMFAIL,
                        message: None,
                    }
                    .into());
                }
            }
        }
        Ok(())
    }

    /// `xaJoin`.
    pub fn xa_join(&mut self, xid: Xid) -> Result<(), TxError> {
        let Some(tx) = self.broker.resource_manager().get(&xid) else {
            return Err(Self::not_found(&xid));
        };
        if transaction::lock(&tx).state == TxState::Suspended {
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot join tx, it is suspended {}",
                    transaction::xid_string(&xid)
                ),
            )
            .into());
        }
        self.tx = Some(tx);
        Ok(())
    }

    /// `xaResume`.
    pub fn xa_resume(&mut self, xid: Xid) -> Result<(), TxError> {
        if let Some(current) = &self.tx {
            let x = transaction::lock(current).xid.clone();
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                format!(
                    "Cannot resume, session is currently doing work in a transaction {}",
                    x.as_ref().map(transaction::xid_string).unwrap_or_default()
                ),
            )
            .into());
        }
        let Some(tx) = self.broker.resource_manager().get(&xid) else {
            return Err(Self::not_found(&xid));
        };
        {
            let mut t = transaction::lock(&tx);
            if t.state != TxState::Suspended {
                return Err(XaError::new(
                    xa_code::XAER_PROTO,
                    format!(
                        "Cannot resume transaction, it is not suspended {}",
                        transaction::xid_string(&xid)
                    ),
                )
                .into());
            }
            t.state = TxState::Active;
        }
        self.tx = Some(tx);
        Ok(())
    }

    /// `xaSuspend`.
    pub fn xa_suspend(&mut self) -> Result<(), TxError> {
        let Some(tx) = &self.tx else {
            return Err(XaError::new(
                xa_code::XAER_PROTO,
                "Cannot suspend, session is not doing work in a transaction ",
            )
            .into());
        };
        let mut t = transaction::lock(tx);
        match t.state {
            TxState::Suspended => {
                let x = t
                    .xid
                    .as_ref()
                    .map(transaction::xid_string)
                    .unwrap_or_default();
                Err(XaError::new(
                    xa_code::XAER_PROTO,
                    format!("Cannot suspend, transaction is already suspended {x}"),
                )
                .into())
            }
            TxState::Active => {
                t.state = TxState::Suspended;
                drop(t);
                self.tx = None;
                Ok(())
            }
            _ => Err(BrokerError::internal("Can only suspend active transaction").into()),
        }
    }

    /// `xaFailed`: after a failover the branch can only roll back.
    pub fn xa_failed(&mut self, xid: Xid) {
        let rm = self.broker.resource_manager();
        let tx = rm.get(&xid).unwrap_or_else(|| {
            let tx =
                Transaction::new_ref(self.broker.post_office().ids().next(), Some(xid.clone()));
            rm.put(xid, tx.clone());
            tx
        });
        let effective = transaction::lock(&tx).is_effective();
        if effective {
            self.tx = None;
        } else {
            transaction::lock(&tx).mark_rollback_only(&BrokerError::new(
                ErrorType::GenericException,
                "Can't commit as a Failover happened during the operation",
            ));
            self.tx = Some(tx);
        }
    }

    /// `xaSetTimeout`.
    pub fn xa_set_timeout(&mut self, timeout_seconds: i32) {
        self.timeout_seconds = timeout_seconds;
        if let Some(tx) = &self.tx {
            transaction::lock(tx).timeout_seconds = timeout_seconds;
        }
    }

    /// `ServerSessionImpl.createConsumer`. `sink` makes the consumer's
    /// delivery sink once its queue is known.
    pub async fn create_consumer(
        &mut self,
        consumer_id: i64,
        queue_name: &str,
        filter: Option<String>,
        priority: i32,
        browse_only: bool,
        sink: impl FnOnce(&Queue) -> Arc<dyn DeliverySink>,
    ) -> Result<()> {
        self.audit().consumer(
            consumer_id,
            queue_name,
            filter.as_deref(),
            priority,
            browse_only,
            None,
        );
        let post_office = self.broker.post_office();
        post_office.plugins().check(|p| {
            p.before_create_consumer(consumer_id, queue_name, filter.as_deref(), browse_only)
        })?;
        let name = queue_name_of(queue_name);
        let queue = post_office
            .queue(name)
            .ok_or_else(|| BrokerError::no_such_queue(name))?;
        let check = if browse_only {
            CheckType::Browse
        } else {
            CheckType::Consume
        };
        let address = queue.config().address.clone();
        if let Err(e) = self.security_check(&address, Some(name), check) {
            // The pre-FQQN syntax (ARTEMIS-592): a security setting matching
            // exactly `address.queue` still applies. Artemis appends the name
            // as the client gave it, so an FQQN never matches.
            let exact = format!("{address}.{queue_name}");
            match self.broker.security() {
                Some(s) if s.contains_exact_match(&exact) => {
                    self.security_check(&exact, Some(name), check)?
                }
                _ => return Err(e),
            }
        }
        let filter_string = filter.clone().filter(|f| !f.trim().is_empty());
        let filter = post_office.filters().create(filter.as_deref())?;
        // Management closed the session (`closeSessionWithID`) while its
        // protocol still holds it.
        if self.closed {
            return Err(BrokerError::consumer_on_closed_session(queue_name));
        }
        let c = queue.config();
        if !browse_only && c.max_consumers != -1 && queue.consumer_count() >= c.max_consumers {
            return Err(BrokerError::max_consumer_limit_reached(&c.address, &c.name));
        }
        let id = self.broker.next_consumer_id();
        // Its deliveries go through the audit log's message logger, which
        // can be turned on while it consumes.
        let audited = Arc::new(crate::audit::AuditedSink {
            inner: sink(&queue),
            subject: self.auth.subject.clone(),
            remote: self.conn.remote_address().to_owned(),
            queue: queue.name().to_string(),
            plugins: post_office.plugins().clone(),
            consumer: Default::default(),
        });
        let sink: Arc<dyn DeliverySink> = audited.clone();
        let metrics = Arc::new(ConsumerMetrics::default());
        let consumer = Arc::new(ConsumerEntry {
            id: consumer_id,
            sequential_id: id,
            session: self.name.clone(),
            queue: queue.clone(),
            filter: filter_string.clone(),
            browse_only,
            priority,
            created: crate::message::now_millis(),
            metrics: metrics.clone(),
            sink: sink.clone(),
        });
        let _ = audited.consumer.set(Arc::downgrade(&consumer));
        queue.add_consumer(ConsumerRegistration {
            id,
            client_id: consumer_id,
            sink: sink.clone(),
            filter,
            priority,
            browse_only,
            pre_acknowledge: self.pre_acknowledge,
            started: self.started,
            metrics: metrics.clone(),
        });
        self.entry.add_consumer(consumer.clone());
        self.broker.registry().consumer_created();
        post_office
            .plugins()
            .each(|p| p.after_create_consumer(&consumer));
        if !browse_only {
            // CONSUMER_CREATED: the address, or the FQQN the client named.
            let c = queue.config();
            let address = if queue_name.contains("::") {
                queue_name.to_owned()
            } else {
                c.address.to_string()
            };
            let props = self.consumer_props(consumer_id, &queue, filter_string.as_deref());
            let validated = self.entry.validated_user.clone();
            let cert = self.conn.security_info().cert_subject_dn().to_owned();
            self.broker
                .post_office()
                .notify(NotificationType::ConsumerCreated, move |n| {
                    let mut p = vec![(hdr::ADDRESS, string(Some(&address)))];
                    p.extend(props.head(n));
                    p.push((hdr::USER, props.user.clone()));
                    p.push((hdr::VALIDATED_USER, string(validated.as_deref())));
                    p.push((hdr::CERT_SUBJECT_DN, string(Some(&cert))));
                    p.push((hdr::REMOTE_ADDRESS, string(Some(&props.remote))));
                    p.push((hdr::SESSION_NAME, string(Some(&props.session))));
                    if let Some(f) = &props.filter {
                        p.push((hdr::FILTERSTRING, string(Some(f))));
                    }
                    p.extend(props.tail());
                    p
                });
        }
        self.consumers.insert(
            consumer_id,
            ServerConsumer {
                id,
                queue,
                browse_only,
                metrics,
                filter: filter_string,
            },
        );
        Ok(())
    }

    /// `ServerConsumerImpl.close`: what it was delivering goes back to the
    /// queue.
    pub async fn close_consumer(&mut self, consumer_id: i64, failed: bool) {
        let Some(c) = self.consumers.remove(&consumer_id) else {
            tracing::debug!(consumer_id, "closing an unknown consumer");
            return;
        };
        let plugins = self.broker.post_office().plugins().clone();
        let entry = self.entry.remove_consumer(consumer_id);
        if let Some(e) = &entry {
            plugins.each(|p| p.before_close_consumer(e, failed));
        }
        c.queue.remove_consumer(c.id, failed).await;
        if let Some(e) = &entry {
            plugins.each(|p| p.after_close_consumer(e, failed));
        }
        if !c.browse_only {
            let address = c.queue.config().address.to_string();
            let props = self.consumer_props(consumer_id, &c.queue, c.filter.as_deref());
            self.broker
                .post_office()
                .notify(NotificationType::ConsumerClosed, move |n| {
                    let head = props.head(n);
                    let mut p = vec![(hdr::ADDRESS, string(Some(&address)))];
                    p.extend(head.iter().take(2).cloned());
                    p.push((hdr::FILTERSTRING, string(props.filter.as_deref())));
                    p.extend(head.into_iter().skip(2));
                    p.push((hdr::USER, props.user.clone()));
                    p.push((hdr::REMOTE_ADDRESS, string(Some(&props.remote))));
                    p.push((hdr::SESSION_NAME, string(Some(&props.session))));
                    p.extend(props.tail());
                    p
                });
        }
    }

    /// `PrefixUtil.getPrefix`: the acceptor prefix `name` starts with.
    pub fn prefix_of(&self, name: &str) -> Option<&str> {
        self.conn
            .prefixes()
            .iter()
            .find(|(p, _)| name.starts_with(p.as_str()))
            .map(|(p, _)| p.as_str())
    }

    /// `checkAutoCreate` for `address` (which may be a fully qualified
    /// queue name) without its acceptor prefix. `None` for the routing
    /// type means the caller does not know it, which counts as existing.
    pub fn check_auto_create(
        &self,
        address: &str,
        routing_type: Option<RoutingType>,
    ) -> Result<crate::postoffice::AutoCreate> {
        let address = self.remove_prefix(address);
        self.broker
            .post_office()
            .check_auto_create(address, routing_type, self.creator())
    }

    /// Grants a consumer credits later, from outside the session: `-1` for
    /// no flow control.
    pub fn credit_granter(&self, consumer_id: i64) -> Option<impl FnOnce(i32) + Send + use<>> {
        let c = self.consumers.get(&consumer_id)?;
        let (queue, id) = (c.queue.clone(), c.id);
        Some(move |credits| queue.credits(id, credits))
    }

    /// See [`ClientConnection::strip_prefix`].
    pub fn strip_prefix<'a>(&self, name: &'a str) -> (&'a str, Option<RoutingType>) {
        self.conn.strip_prefix(name)
    }

    /// `ServerSessionImpl.removePrefix`.
    pub fn remove_prefix<'a>(&self, name: &'a str) -> &'a str {
        self.strip_prefix(name).0
    }

    /// `createQueue`'s prefix handling: the address's prefix picks the
    /// routing type; address and name lose their prefixes.
    pub fn unprefix_queue(&self, spec: &mut QueueSpec) {
        let (address, t) = self.strip_prefix(&spec.address);
        if let Some(t) = t {
            spec.routing_type = Some(t);
        }
        spec.address = address.into();
        spec.name = self.remove_prefix(&spec.name).into();
    }

    /// `ServerSessionImpl.createAddress`. A prefix on the address names
    /// the routing type (`getAddressAndRoutingType`).
    pub fn create_address(
        &mut self,
        address: &str,
        mut types: RoutingTypes,
        auto_created: bool,
    ) -> Result<()> {
        if crate::audit::base_enabled() {
            // EnumSet order.
            let names: Vec<&str> = [RoutingType::Multicast, RoutingType::Anycast]
                .into_iter()
                .filter(|t| types.contains(*t))
                .map(|t| t.name())
                .collect();
            self.audit()
                .address(&self.broker, address, &names.join(","), auto_created);
        }
        let (address, prefixed) = self.strip_prefix(address);
        if let Some(t) = prefixed {
            types = RoutingTypes::of(t);
        }
        self.security_check(address, None, CheckType::CreateAddress)?;
        self.broker
            .post_office()
            .add_or_update_address(address, types, auto_created, false)
            .map(|_| ())
    }

    /// `ServerSessionImpl.createQueue`.
    pub fn create_queue(&mut self, mut spec: QueueSpec) -> Result<Queue> {
        self.audit().queue(&spec, false);
        // The user must be allowed to create the queue, and the address too
        // if the queue's creation would create or extend it.
        self.security_check(&spec.address, Some(&spec.name), create_check(spec.durable))?;
        let post_office = self.broker.post_office();
        let settings = post_office.settings(&spec.address);
        let routing_type = spec
            .routing_type
            .unwrap_or(settings.default_queue_routing_type);
        if settings.auto_create_addresses
            && post_office
                .address_info(&spec.address)
                .is_none_or(|i| !i.routing_types.contains(routing_type))
        {
            self.security_check(&spec.address, Some(&spec.name), CheckType::CreateAddress)?;
        }
        spec.user = self.validated_user.clone();
        let temporary = spec.temporary;
        let queue = self.broker.post_office().create_queue(spec, false)?;
        if temporary {
            self.conn.add_temp_queue(queue.name().clone());
        }
        Ok(queue)
    }

    /// `createSharedQueue`: creates the queue unless one with the same
    /// address and filter exists. Only the address loses its prefix.
    pub fn create_shared_queue(&mut self, mut spec: QueueSpec) -> Result<()> {
        self.audit().queue(&spec, true);
        spec.address = self.remove_prefix(&spec.address).into();
        self.security_check(&spec.address, Some(&spec.name), create_check(spec.durable))?;
        spec.user = self.validated_user.clone();
        let existing = self.broker.post_office().queue_query(&spec.name);
        if existing.exists
            && existing.address == spec.address
            && existing.filter_string == spec.filter_string
        {
            return Ok(());
        }
        self.broker
            .post_office()
            .create_queue(spec, false)
            .map(|_| ())
    }

    /// `ServerSessionImpl.deleteQueue`.
    pub async fn delete_queue(&mut self, name: &str) -> Result<()> {
        self.delete_queue_with(name, true).await
    }

    /// `deleteQueue(name, checkSecurity)`: without the check, the session's
    /// user need not be allowed to delete the queue or its address.
    pub async fn delete_queue_with(&mut self, name: &str, check_security: bool) -> Result<()> {
        self.audit().delete_queue(name);
        let post_office = self.broker.post_office();
        let name = queue_name_of(self.remove_prefix(name));
        let queue = post_office
            .queue(name)
            .ok_or_else(|| BrokerError::no_such_queue(name))?;
        let address = queue.config().address.clone();
        let check = if queue.is_durable() {
            CheckType::DeleteDurableQueue
        } else {
            CheckType::DeleteNonDurableQueue
        };
        if check_security {
            self.security_check(&address, Some(name), check)?;
        }
        let durable = post_office.delete_queue_reporting(name, true).await?;
        self.conn.remove_temp_queue(name);
        // `destroyQueue(forceAutoDeleteAddress)`: a client's delete takes an
        // auto-created address along whatever `auto-delete-addresses` says,
        // if the user may delete it.
        if post_office.address_auto_deletable(&address, durable) {
            if check_security {
                self.security_check(&address, None, CheckType::DeleteAddress)?;
            }
            post_office.remove_address(&address);
        }
        Ok(())
    }

    /// `ServerSessionImpl.executeQueueQuery`, without the acceptor's
    /// prefix. The management address is a queue that exists, with no
    /// consumer or message count (-1), as `ActiveMQServerImpl.queueQuery`
    /// has it: the JMS client sending to it then creates nothing.
    pub fn queue_query(&self, name: &str) -> QueueQueryResult {
        let name = self.remove_prefix(name);
        let mut r = self.broker.post_office().queue_query(name);
        if !r.exists && crate::postoffice::queue_name_of(name) == self.broker.management().address()
        {
            r.exists = true;
            r.consumer_count = -1;
            r.message_count = -1;
        }
        r
    }

    /// `ServerSessionImpl.executeBindingQuery`, without the acceptor's
    /// prefix.
    pub fn binding_query(&self, address: &str, new_fqqn: bool) -> BindingQueryResult {
        self.broker
            .post_office()
            .binding_query(self.remove_prefix(address), new_fqqn)
    }

    /// `newTransaction`: a transaction of the caller's own, apart from the
    /// session's.
    pub fn new_transaction(&self) -> TxRef {
        self.new_tx(None)
    }

    /// Commits a transaction of the caller's own; a rollback-only one rolls
    /// back and fails instead.
    pub fn commit_tx(&mut self, tx: &TxRef) -> Result<()> {
        let ending = crate::audit::ending(&transaction::lock(tx));
        let result = transaction::lock(tx).commit(true);
        self.audit_ended(ending, tx, result.is_ok());
        match result {
            Ok(outcome) => self.finish(outcome, true),
            Err(failed) => {
                let (e, outcome) = *failed;
                self.finish(outcome, false)?;
                Err(e)
            }
        }
    }

    /// The audit log's messages for a transaction that has ended.
    fn audit_ended(&self, ending: Option<crate::audit::Ending>, tx: &TxRef, committed: bool) {
        if let Some(ending) = ending {
            crate::audit::ended(self.who(), ending, &transaction::lock(tx), committed);
        }
    }

    /// Rolls back a transaction of the caller's own, except that the
    /// acknowledged messages `keep` names (consumer ID, message ID) stay
    /// with their consumers as delivered, counted once more
    /// (`ProtonTransactionRefsOperation`).
    pub fn rollback_tx_keeping(&mut self, tx: &TxRef, keep: &[(i64, i64)]) -> Result<()> {
        let kept: Vec<(ConsumerId, Queue, MessageRef)> = {
            let mut t = transaction::lock(tx);
            let mut kept = Vec::new();
            let mut i = 0;
            while i < t.acks.len() {
                let (q, r) = &t.acks[i];
                let owner = keep.iter().find_map(|(consumer, message)| {
                    let c = self.consumers.get(consumer)?;
                    (*message == r.message_id() && c.queue.id() == q.id()).then_some(c.id)
                });
                match owner {
                    Some(id) => {
                        let (q, mut r) = t.acks.remove(i);
                        r.delivery_count += 1;
                        kept.push((id, q, r));
                    }
                    None => i += 1,
                }
            }
            kept
        };
        for (id, q, r) in kept {
            q.back_to_delivering(id, r);
        }
        self.rollback_tx(tx)
    }

    /// Rolls back a transaction of the caller's own.
    pub fn rollback_tx(&mut self, tx: &TxRef) -> Result<()> {
        let ending = crate::audit::ending(&transaction::lock(tx));
        let outcome = transaction::lock(tx).rollback()?;
        self.audit_ended(ending, tx, false);
        self.finish(outcome, false)
    }

    /// Deletes a journal record as part of `tx` (`deleteMapRecordTx`).
    pub fn delete_record_in(&self, tx: &TxRef, record: i64) -> Result<()> {
        let Some(s) = self.storage() else {
            return Ok(());
        };
        let mut t = transaction::lock(tx);
        s.delete_record_tx(t.id, record).map_err(io_error)?;
        t.persistent = true;
        Ok(())
    }

    /// Adds `id` to a duplicate-ID cache as part of `tx`
    /// (`DuplicateIDCache.addToCache(id, tx)`).
    pub fn add_duplicate_in(
        &self,
        tx: &TxRef,
        cache: &crate::duplicate::CacheRef,
        id: Vec<u8>,
    ) -> Result<()> {
        let record = self.broker.post_office().ids().next();
        let mut t = transaction::lock(tx);
        let c = crate::duplicate::lock(cache);
        c.store_tx(t.id, &id, record).map_err(io_error)?;
        t.persistent |= c.is_persistent();
        drop(c);
        t.duplicates.push(crate::duplicate::PendingId {
            cache: cache.clone(),
            id,
            record,
        });
        Ok(())
    }

    /// `ServerSessionImpl.commit`: commits the current transaction (a
    /// rollback-only one rolls back and fails instead) and starts the next.
    pub fn commit(&mut self) -> Result<()> {
        let tx = self.tx.take();
        if !self.xa {
            self.tx = Some(self.new_tx(None));
        }
        let Some(tx) = tx else {
            return Ok(());
        };
        let ending = crate::audit::ending(&transaction::lock(&tx));
        let result = transaction::lock(&tx).commit(true);
        self.audit_ended(ending, &tx, result.is_ok());
        match result {
            Ok(outcome) => self.finish(outcome, true),
            Err(failed) => {
                let (e, outcome) = *failed;
                self.finish(outcome, false)?;
                Err(e)
            }
        }
    }

    /// `ServerSessionImpl.rollback`.
    pub async fn rollback(&mut self, last_as_delivered: bool, failed: bool) {
        let tx = match self.tx.take() {
            Some(tx) => tx,
            None => self.new_tx(None),
        };
        if let Err(e) = self.do_rollback(failed, last_as_delivered, &tx).await {
            tracing::warn!(session = %self.name, "rollback: {e}");
        }
        if !self.xa {
            self.tx = Some(self.new_tx(None));
        }
    }

    /// `doRollback`: takes back what this session's consumers are
    /// delivering and rolls the transaction back, returning both to the
    /// queues.
    async fn do_rollback(
        &mut self,
        failed: bool,
        last_as_delivered: bool,
        tx: &TxRef,
    ) -> Result<(), BrokerError> {
        let was_started = self.started;
        let mut cancelled = Vec::new();
        for c in self.consumers.values() {
            if c.browse_only {
                continue;
            }
            let refs = c
                .queue
                .cancel_delivering(c.id, last_as_delivered, failed)
                .await;
            cancelled.extend(refs.into_iter().map(|r| (c.queue.clone(), r)));
        }
        let ending = crate::audit::ending(&transaction::lock(tx));
        let result = {
            let mut t = transaction::lock(tx);
            // What was delivered after a transaction already rolled back
            // (timed out) goes back on its own.
            if t.state == TxState::RolledBack {
                Ok(Outcome::default())
            } else {
                t.rollback()
            }
        };
        if result.is_ok() {
            self.audit_ended(ending, tx, false);
        }
        let (mut outcome, result) = match result {
            Ok(o) => (o, Ok(())),
            Err(e) => (Outcome::default(), Err(e)),
        };
        outcome.cancel.extend(cancelled);
        let finished = self.finish(outcome, false);
        if was_started && !failed {
            for c in self.consumers.values() {
                c.queue.set_started(c.id, true).await;
            }
        }
        result.and(finished)
    }

    /// `ServerSessionImpl.start` and `stop`.
    pub async fn set_started(&mut self, started: bool) {
        self.started = started;
        for c in self.consumers.values() {
            c.queue.set_started(c.id, started).await;
        }
    }

    /// `ServerSessionImpl.doClose`. A failed client's transactions roll back
    /// unless prepared; a clean close rolls back only a local transaction,
    /// leaving XA branches to the transaction manager. Then the consumers
    /// close.
    pub async fn close(&mut self, failed: bool) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.blocked.cancel();
        let plugins = self.broker.post_office().plugins().clone();
        plugins.each(|p| p.before_close_session(&self.entry, failed));
        if failed {
            for tx in [self.tx.take(), self.pending_tx.take()]
                .into_iter()
                .flatten()
            {
                let (xid, outcome) = {
                    let mut t = transaction::lock(&tx);
                    (t.xid.clone(), t.try_rollback())
                };
                if let Some(outcome) = outcome {
                    if let Err(e) = self.finish(outcome, false) {
                        tracing::warn!(session = %self.name, "AMQ222207: Unable to rollback on close: {e}");
                    }
                    if let Some(xid) = xid {
                        self.broker.resource_manager().remove(&xid);
                    }
                }
            }
        } else if self
            .tx
            .as_ref()
            .is_some_and(|t| transaction::lock(t).xid.is_none())
        {
            self.rollback(false, false).await;
        }
        let ids: Vec<i64> = self.consumers.keys().copied().collect();
        for id in ids {
            self.close_consumer(id, failed).await;
        }
        self.broker.remove_session_metadata(&self.name);
        self.entry.clear_producers();
        self.conn.remove_session(&self.entry);
        session_notification(
            &self.broker,
            &self.conn,
            &self.entry,
            NotificationType::SessionClosed,
        );
        plugins.each(|p| p.after_close_session(&self.entry, failed));
    }

    /// What `CONSUMER_CREATED` and `CONSUMER_CLOSED` say of a consumer.
    fn consumer_props(
        &self,
        consumer_id: i64,
        queue: &Queue,
        filter: Option<&str>,
    ) -> ConsumerProps {
        ConsumerProps {
            queue: queue.name().to_string(),
            routing_name: crate::postoffice::routing_name(queue).to_string(),
            consumer_count: queue.consumer_count(),
            user: string(self.username()),
            remote: self.conn.remote_address().to_owned(),
            session: self.name.clone(),
            filter: filter.map(str::to_owned),
            client_id: self.conn.client_id(),
            consumer_id,
        }
    }
}

/// The parts of a consumer notification.
struct ConsumerProps {
    queue: String,
    /// `binding.getRoutingName()`.
    routing_name: String,
    consumer_count: i32,
    user: saranga_buffer::PropertyValue,
    remote: String,
    session: String,
    filter: Option<String>,
    client_id: Option<String>,
    consumer_id: i64,
}

impl ConsumerProps {
    /// Cluster name, routing name, distance and consumer count.
    fn head(
        &self,
        n: &crate::notifications::Notifier,
    ) -> Vec<(&'static str, saranga_buffer::PropertyValue)> {
        use saranga_buffer::PropertyValue as V;
        vec![
            (
                hdr::CLUSTER_NAME,
                string(Some(&n.cluster_name(&self.queue))),
            ),
            (hdr::ROUTING_NAME, string(Some(&self.routing_name))),
            (hdr::DISTANCE, V::Int(0)),
            (hdr::CONSUMER_COUNT, V::Int(self.consumer_count)),
        ]
    }

    /// The client ID, if any, and the consumer's ID.
    fn tail(&self) -> Vec<(&'static str, saranga_buffer::PropertyValue)> {
        let mut p = Vec::new();
        if let Some(c) = &self.client_id {
            p.push((hdr::CLIENT_ID, string(Some(c))));
        }
        p.push((
            hdr::CONSUMER_NAME,
            saranga_buffer::PropertyValue::Long(self.consumer_id),
        ));
        p
    }
}

/// `sendSessionNotification`, unless `suppress-session-notifications`.
fn session_notification(
    broker: &Broker,
    conn: &ClientConnection,
    entry: &SessionEntry,
    kind: NotificationType,
) {
    if broker.management().config().suppress_session_notifications {
        return;
    }
    let (connection, user, session, client_id, protocol) = (
        conn.id().to_owned(),
        entry.username.clone(),
        entry.name.clone(),
        conn.client_id(),
        conn.protocol(),
    );
    broker.post_office().notify(kind, move |n| {
        vec![
            (hdr::CONNECTION_NAME, string(Some(&connection))),
            (hdr::USER, string(user.as_deref())),
            (hdr::SESSION_NAME, string(Some(&session))),
            (hdr::CLIENT_ID, string(client_id.as_deref())),
            (hdr::PROTOCOL_NAME, string(Some(protocol))),
            (hdr::ADDRESS, string(Some(n.address()))),
            (hdr::DISTANCE, saranga_buffer::PropertyValue::Int(0)),
        ]
    });
}

impl ServerSession {}

/// The check that creating a queue of this durability needs.
fn create_check(durable: bool) -> CheckType {
    if durable {
        CheckType::CreateDurableQueue
    } else {
        CheckType::CreateNonDurableQueue
    }
}

/// A journal failure as the client sees it (`ActiveMQIOErrorException`).
pub(crate) fn io_error(e: saranga_journal::JournalError) -> BrokerError {
    BrokerError::new(ErrorType::IoError, e.to_string())
}
