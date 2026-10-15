//! Management notifications (`ManagementServiceImpl.sendNotification`):
//! durable messages to the notification address (`activemq.notifications`)
//! telling of bindings, consumers, sessions and security violations. They
//! are built where things happen and routed in order by one task.

use std::sync::{Arc, Weak};

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use tokio::sync::{mpsc, oneshot};

use crate::message::{MessageRef, ServerMessage, now_millis};
use crate::postoffice::PostOffice;

/// `CoreNotificationType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationType {
    BindingAdded,
    BindingRemoved,
    ConsumerCreated,
    ConsumerClosed,
    SecurityAuthenticationViolation,
    SecurityPermissionViolation,
    AcceptorStarted,
    AcceptorStopped,
    ConsumerSlow,
    SessionCreated,
    SessionClosed,
    BindingUpdated,
    ConnectionCreated,
    ConnectionDestroyed,
    AddressAdded,
    AddressRemoved,
    MessageDelivered,
    MessageExpired,
    BroadcastGroupStarted,
    BroadcastGroupStopped,
    BridgeStarted,
    BridgeStopped,
    ClusterConnectionStarted,
    ClusterConnectionStopped,
}

impl NotificationType {
    pub fn name(self) -> &'static str {
        match self {
            Self::BindingAdded => "BINDING_ADDED",
            Self::BindingRemoved => "BINDING_REMOVED",
            Self::ConsumerCreated => "CONSUMER_CREATED",
            Self::ConsumerClosed => "CONSUMER_CLOSED",
            Self::SecurityAuthenticationViolation => "SECURITY_AUTHENTICATION_VIOLATION",
            Self::SecurityPermissionViolation => "SECURITY_PERMISSION_VIOLATION",
            Self::AcceptorStarted => "ACCEPTOR_STARTED",
            Self::AcceptorStopped => "ACCEPTOR_STOPPED",
            Self::ConsumerSlow => "CONSUMER_SLOW",
            Self::SessionCreated => "SESSION_CREATED",
            Self::SessionClosed => "SESSION_CLOSED",
            Self::BindingUpdated => "BINDING_UPDATED",
            Self::ConnectionCreated => "CONNECTION_CREATED",
            Self::ConnectionDestroyed => "CONNECTION_DESTROYED",
            Self::AddressAdded => "ADDRESS_ADDED",
            Self::AddressRemoved => "ADDRESS_REMOVED",
            Self::MessageDelivered => "MESSAGE_DELIVERED",
            Self::MessageExpired => "MESSAGE_EXPIRED",
            Self::BroadcastGroupStarted => "BROADCAST_GROUP_STARTED",
            Self::BroadcastGroupStopped => "BROADCAST_GROUP_STOPPED",
            Self::BridgeStarted => "BRIDGE_STARTED",
            Self::BridgeStopped => "BRIDGE_STOPPED",
            Self::ClusterConnectionStarted => "CLUSTER_CONNECTION_STARTED",
            Self::ClusterConnectionStopped => "CLUSTER_CONNECTION_STOPPED",
        }
    }
}

/// `ManagementHelper`'s notification headers.
pub mod hdr {
    pub const NOTIFICATION_TYPE: &str = "_AMQ_NotifType";
    pub const NOTIFICATION_TIMESTAMP: &str = "_AMQ_NotifTimestamp";
    pub const ROUTING_NAME: &str = "_AMQ_RoutingName";
    pub const CLUSTER_NAME: &str = "_AMQ_ClusterName";
    pub const ADDRESS: &str = "_AMQ_Address";
    pub const BINDING_ID: &str = "_AMQ_Binding_ID";
    pub const BINDING_TYPE: &str = "_AMQ_Binding_Type";
    pub const FILTERSTRING: &str = "_AMQ_FilterString";
    pub const DISTANCE: &str = "_AMQ_Distance";
    pub const CONSUMER_COUNT: &str = "_AMQ_ConsumerCount";
    pub const USER: &str = "_AMQ_User";
    pub const VALIDATED_USER: &str = "_AMQ_ValidatedUser";
    pub const CERT_SUBJECT_DN: &str = "_AMQ_CertSubjectDN";
    pub const CHECK_TYPE: &str = "_AMQ_CheckType";
    pub const SESSION_NAME: &str = "_AMQ_SessionName";
    pub const REMOTE_ADDRESS: &str = "_AMQ_RemoteAddress";
    pub const CONSUMER_NAME: &str = "_AMQ_ConsumerName";
    pub const CONNECTION_NAME: &str = "_AMQ_ConnectionName";
    pub const PROTOCOL_NAME: &str = "_AMQ_Protocol_Name";
    pub const CLIENT_ID: &str = "_AMQ_Client_ID";
    pub const ROUTING_TYPE: &str = "_AMQ_Routing_Type";
    pub const MESSAGE_ID: &str = "_AMQ_Message_ID";
}

/// A string property, or null (`SimpleString.of(null)`).
pub fn string(v: Option<&str>) -> PropertyValue {
    match v {
        Some(s) => PropertyValue::String(SimpleString::from(s)),
        None => PropertyValue::Null,
    }
}

/// What the notification task does, in order.
enum Job {
    Notify(CoreMessage),
    /// `sendQueueInfoToQueue`.
    QueueInfo {
        queue: String,
        address: String,
        done: oneshot::Sender<Result<(), String>>,
    },
}

/// Where notifications go once the broker is active.
pub struct Notifier {
    address: String,
    node_id: String,
    tx: mpsc::UnboundedSender<Job>,
}

impl Notifier {
    /// Starts the task that routes notifications to `address`. It keeps
    /// what they told of the queues of this broker and of the cluster
    /// (`PostOfficeImpl.queueInfos`), so that what `sendQueueInfoToQueue`
    /// sends and the notifications after it follow on without a gap.
    pub fn start(post_office: &Arc<PostOffice>, address: String, node_id: String) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
        let po: Weak<PostOffice> = Arc::downgrade(post_office);
        // The queues bound before the broker was active: the post office
        // heard of them as a notification listener then.
        let mut infos = QueueInfos::default();
        for q in post_office.queues() {
            let c = q.config();
            infos.0.push(QueueInfo {
                routing_name: Some(SimpleString::from(&*crate::postoffice::routing_name(&q))),
                cluster_name: SimpleString::from(format!("{}{node_id}", q.name()).as_str()),
                address: Some(SimpleString::from(&*c.address)),
                filter_string: q.filter_string().map(|f| SimpleString::from(f.as_str())),
                id: q.id(),
                distance: 0,
                consumers: 0,
                filter_strings: None,
            });
        }
        tokio::spawn(async move {
            let mut infos = infos;
            while let Some(job) = rx.recv().await {
                let Some(po) = po.upgrade() else { break };
                match job {
                    Job::Notify(m) => {
                        infos.on_notification(&m);
                        po.send_internal(m).await;
                    }
                    Job::QueueInfo {
                        queue,
                        address,
                        done,
                    } => {
                        let _ = done.send(infos.send_to_queue(&po, &queue, &address));
                    }
                }
            }
        });
        Self {
            address,
            node_id,
            tx,
        }
    }

    /// `PostOfficeImpl.sendQueueInfoToQueue`: what is known of the queues
    /// goes to `queue`, between a reset and its completion, as
    /// `BINDING_ADDED` and `CONSUMER_CREATED` messages its filter accepts.
    /// The receiver hears when it is done.
    fn send_queue_info(&self, queue: &str, address: &str) -> oneshot::Receiver<Result<(), String>> {
        let (done, wait) = oneshot::channel();
        let _ = self.tx.send(Job::QueueInfo {
            queue: queue.to_owned(),
            address: address.to_owned(),
            done,
        });
        wait
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    /// `LocalQueueBinding.clusterName`: the queue's name and the node ID.
    pub fn cluster_name(&self, queue: &str) -> String {
        format!("{queue}{}", self.node_id)
    }

    /// Builds and queues one notification: its properties, then the type
    /// and timestamp. Durable, with the default priority.
    pub fn send(&self, kind: NotificationType, props: Vec<(&str, PropertyValue)>, id: i64) {
        let now = now_millis();
        let mut m = CoreMessage {
            message_id: id,
            address: Some(SimpleString::from(self.address.as_str())),
            durable: true,
            timestamp: now,
            priority: 4,
            ..Default::default()
        };
        for (k, v) in props {
            m.properties.insert(SimpleString::from(k), v);
        }
        m.properties.insert(
            SimpleString::from(hdr::NOTIFICATION_TYPE),
            PropertyValue::String(SimpleString::from(kind.name())),
        );
        m.properties.insert(
            SimpleString::from(hdr::NOTIFICATION_TIMESTAMP),
            PropertyValue::Long(now),
        );
        let _ = self.tx.send(Job::Notify(m));
    }
}

/// `PostOfficeImpl.HDR_RESET_QUEUE_DATA`.
pub const HDR_RESET_QUEUE_DATA: &str = "_AMQ_RESET_QUEUE_DATA";

/// `PostOfficeImpl.HDR_RESET_QUEUE_DATA_COMPLETE`.
pub const HDR_RESET_QUEUE_DATA_COMPLETE: &str = "_AMQ_RESET_QUEUE_DATA_COMPLETE";

/// `QueueInfo`: a queue bound here or in the cluster, as notifications
/// told of it.
#[derive(Debug, Clone)]
struct QueueInfo {
    routing_name: Option<SimpleString>,
    cluster_name: SimpleString,
    address: Option<SimpleString>,
    filter_string: Option<SimpleString>,
    id: i64,
    distance: i32,
    consumers: i32,
    filter_strings: Option<Vec<SimpleString>>,
}

/// `PostOfficeImpl.queueInfos`, keyed by cluster name in insertion order.
#[derive(Default)]
struct QueueInfos(Vec<QueueInfo>);

fn prop<'a>(m: &'a CoreMessage, name: &str) -> Option<&'a PropertyValue> {
    m.properties.get(&SimpleString::from(name))
}

fn prop_string(m: &CoreMessage, name: &str) -> Option<SimpleString> {
    match prop(m, name)? {
        PropertyValue::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn prop_int(m: &CoreMessage, name: &str) -> Option<i32> {
    match prop(m, name)? {
        PropertyValue::Int(i) => Some(*i),
        _ => None,
    }
}

impl QueueInfos {
    fn get_mut(&mut self, cluster_name: &SimpleString) -> Option<&mut QueueInfo> {
        self.0.iter_mut().find(|i| i.cluster_name == *cluster_name)
    }

    /// `PostOfficeImpl.onNotification`, as far as the queue infos go.
    fn on_notification(&mut self, m: &CoreMessage) {
        let Some(kind) = prop_string(m, hdr::NOTIFICATION_TYPE) else {
            return;
        };
        match kind.to_string_lossy().as_str() {
            "BINDING_ADDED" => {
                // Diverts are not propagated.
                if prop_int(m, hdr::BINDING_TYPE) == Some(2) {
                    return;
                }
                let (Some(cluster_name), Some(PropertyValue::Long(id)), Some(distance)) = (
                    prop_string(m, hdr::CLUSTER_NAME),
                    prop(m, hdr::BINDING_ID),
                    prop_int(m, hdr::DISTANCE),
                ) else {
                    return;
                };
                let info = QueueInfo {
                    routing_name: prop_string(m, hdr::ROUTING_NAME),
                    address: prop_string(m, hdr::ADDRESS),
                    filter_string: prop_string(m, hdr::FILTERSTRING),
                    id: *id,
                    distance,
                    consumers: 0,
                    filter_strings: None,
                    cluster_name: cluster_name.clone(),
                };
                // `queueInfos.put`: a key put again keeps its place.
                match self.get_mut(&cluster_name) {
                    Some(slot) => *slot = info,
                    None => self.0.push(info),
                }
            }
            "BINDING_REMOVED" => {
                if let Some(c) = prop_string(m, hdr::CLUSTER_NAME) {
                    self.0.retain(|i| i.cluster_name != c);
                }
            }
            "CONSUMER_CREATED" => {
                let Some(c) = prop_string(m, hdr::CLUSTER_NAME) else {
                    return;
                };
                let filter = prop_string(m, hdr::FILTERSTRING);
                if let Some(info) = self.get_mut(&c) {
                    info.consumers += 1;
                    if let Some(f) = filter {
                        info.filter_strings.get_or_insert_with(Vec::new).push(f);
                    }
                }
            }
            "CONSUMER_CLOSED" => {
                let Some(c) = prop_string(m, hdr::CLUSTER_NAME) else {
                    return;
                };
                let filter = prop_string(m, hdr::FILTERSTRING);
                if let Some(info) = self.get_mut(&c) {
                    info.consumers -= 1;
                    if let (Some(f), Some(fs)) = (filter, info.filter_strings.as_mut())
                        && let Some(i) = fs.iter().position(|x| *x == f)
                    {
                        fs.remove(i);
                    }
                }
            }
            _ => {}
        }
    }

    /// `sendQueueInfoToQueue`, in the order of the `ConcurrentHashMap`
    /// holding the infos.
    fn send_to_queue(
        &self,
        po: &PostOffice,
        queue_name: &str,
        _address: &str,
    ) -> Result<(), String> {
        let Some(queue) = po.queue(queue_name) else {
            return Err(format!("Cannot find queue {queue_name}"));
        };
        let route = |m: CoreMessage, filtered: bool| {
            let m = ServerMessage::new(m);
            if filtered && queue.filter().is_some_and(|f| !f.matches(&*m)) {
                return;
            }
            queue.add(MessageRef::new(m));
        };
        let reset = |key: &str| {
            let mut m = CoreMessage {
                message_id: po.ids().next(),
                address: Some(SimpleString::from(queue_name)),
                ..Default::default()
            };
            m.properties
                .insert(SimpleString::from(key), PropertyValue::Bool(true));
            m
        };
        let info_message = |kind: &str| {
            let now = now_millis();
            let mut m = CoreMessage {
                message_id: po.ids().next(),
                address: Some(SimpleString::from(queue_name)),
                timestamp: now,
                ..Default::default()
            };
            m.properties.insert(
                SimpleString::from(hdr::NOTIFICATION_TYPE),
                PropertyValue::String(SimpleString::from(kind)),
            );
            m.properties.insert(
                SimpleString::from(hdr::NOTIFICATION_TIMESTAMP),
                PropertyValue::Long(now),
            );
            m
        };
        let opt = |s: &Option<SimpleString>| match s {
            Some(s) => PropertyValue::String(s.clone()),
            None => PropertyValue::Null,
        };
        route(reset(HDR_RESET_QUEUE_DATA), false);
        let hashes: Vec<i32> = self
            .0
            .iter()
            .map(|i| saranga_buffer::java_hash::string_hash(&i.cluster_name.to_string_lossy()))
            .collect();
        // `QueueInfo.matchesAddress` compares the address with its own
        // parts: any address matches.
        for i in saranga_buffer::java_hash::hash_map_order(&hashes, None) {
            let info = &self.0[i];
            let common = |m: &mut CoreMessage| {
                let p = &mut m.properties;
                p.insert(SimpleString::from(hdr::ADDRESS), opt(&info.address));
                p.insert(
                    SimpleString::from(hdr::CLUSTER_NAME),
                    PropertyValue::String(info.cluster_name.clone()),
                );
                p.insert(
                    SimpleString::from(hdr::ROUTING_NAME),
                    opt(&info.routing_name),
                );
            };
            let mut m = info_message("BINDING_ADDED");
            common(&mut m);
            m.properties.insert(
                SimpleString::from(hdr::BINDING_ID),
                PropertyValue::Long(info.id),
            );
            m.properties.insert(
                SimpleString::from(hdr::FILTERSTRING),
                opt(&info.filter_string),
            );
            m.properties.insert(
                SimpleString::from(hdr::DISTANCE),
                PropertyValue::Int(info.distance),
            );
            route(m, true);
            let with_filters = info.filter_strings.as_ref().map_or(0, Vec::len) as i32;
            for _ in 0..info.consumers - with_filters {
                let mut m = info_message("CONSUMER_CREATED");
                common(&mut m);
                m.properties.insert(
                    SimpleString::from(hdr::DISTANCE),
                    PropertyValue::Int(info.distance),
                );
                route(m, true);
            }
            for f in info.filter_strings.iter().flatten() {
                let mut m = info_message("CONSUMER_CREATED");
                common(&mut m);
                m.properties.insert(
                    SimpleString::from(hdr::FILTERSTRING),
                    PropertyValue::String(f.clone()),
                );
                m.properties.insert(
                    SimpleString::from(hdr::DISTANCE),
                    PropertyValue::Int(info.distance),
                );
                route(m, true);
            }
        }
        route(reset(HDR_RESET_QUEUE_DATA_COMPLETE), false);
        Ok(())
    }
}

/// `SECURITY_AUTHENTICATION_VIOLATION` and `SECURITY_PERMISSION_VIOLATION`.
pub fn security_violation(po: &PostOffice, v: saranga_security::Violation) {
    use saranga_security::Violation as V;
    match v {
        V::Authentication {
            user,
            cert_subject_dn,
            remote_address,
        } => po.notify(
            NotificationType::SecurityAuthenticationViolation,
            move |_| {
                vec![
                    (hdr::USER, string(user.as_deref())),
                    (hdr::CERT_SUBJECT_DN, string(Some(&cert_subject_dn))),
                    (hdr::REMOTE_ADDRESS, string(Some(&remote_address))),
                ]
            },
        ),
        V::Permission {
            address,
            check,
            user,
        } => po.notify(NotificationType::SecurityPermissionViolation, move |_| {
            vec![
                (hdr::ADDRESS, string(Some(&address))),
                (hdr::CHECK_TYPE, string(Some(check.name()))),
                (hdr::USER, string(Some(&user))),
            ]
        }),
    }
}

impl PostOffice {
    /// `PostOfficeImpl.route` of a message the broker sends on its own (a
    /// notification): no auto-creation and no permission checks; durable
    /// references are journaled, and join their queues once written.
    pub async fn send_internal(&self, mut message: CoreMessage) {
        let Some(address) = message.address.as_ref().map(|a| a.to_string_lossy()) else {
            return;
        };
        crate::message::apply_expiry_delay(&mut message, &self.settings(&address), now_millis());
        self.deliver_internal(ServerMessage::new(message), &address)
            .await;
    }

    /// Routes a message the broker sends on its own to `address`, journals
    /// its durable references and adds it to its queues once written.
    pub async fn deliver_internal(&self, message: Arc<ServerMessage>, address: &str) {
        let r = MessageRef::new(message);
        let routed = self.route_internal(&r, address);
        let routed = match self.page_routed(routed.clone(), None) {
            Ok((routed, _)) => routed,
            Err(e) => {
                tracing::debug!("notification to {address}: {e}");
                routed
            }
        };
        let mut waits = Vec::new();
        if let Some(s) = self.ids().storage() {
            for (m, queues) in &routed {
                let durable: Vec<_> = queues.iter().filter(|q| q.is_durable_ref(m)).collect();
                let last = durable.len().saturating_sub(1);
                for (i, q) in durable.into_iter().enumerate() {
                    let stored = (|| {
                        if m.message.durable_up() == 1 {
                            s.store_message(&m.message)?;
                        }
                        s.store_reference(q.id(), m.message_id(), i == last)
                    })();
                    match stored {
                        Ok(p) => waits.push(p),
                        Err(e) => tracing::warn!("notification to {address}: {e}"),
                    }
                }
            }
        }
        for w in waits {
            let _ = w.wait().await;
        }
        for (m, queues) in routed {
            for q in queues {
                q.add(m.clone());
            }
        }
    }
}

impl PostOffice {
    /// `ActiveMQServerControl.sendQueueInfoToQueue`.
    pub async fn send_queue_info_to_queue(&self, queue: &str, address: &str) -> Result<(), String> {
        let wait = {
            let guard = self.notifier_guard();
            match guard.as_ref() {
                Some(n) => n.send_queue_info(queue, address),
                None => return Err("the broker is not active".to_owned()),
            }
        };
        wait.await
            .unwrap_or_else(|_| Err("the broker is stopping".to_owned()))
    }
}
