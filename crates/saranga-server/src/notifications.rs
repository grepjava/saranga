//! Management notifications (`ManagementServiceImpl.sendNotification`):
//! durable messages to the notification address (`activemq.notifications`)
//! telling of bindings, consumers, sessions and security violations. They
//! are built where things happen and routed in order by one task.

use std::sync::{Arc, Weak};

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_message::CoreMessage;
use tokio::sync::mpsc;

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
}

/// A string property, or null (`SimpleString.of(null)`).
pub fn string(v: Option<&str>) -> PropertyValue {
    match v {
        Some(s) => PropertyValue::String(SimpleString::from(s)),
        None => PropertyValue::Null,
    }
}

/// Where notifications go once the broker is active.
pub struct Notifier {
    address: String,
    node_id: String,
    tx: mpsc::UnboundedSender<CoreMessage>,
}

impl Notifier {
    /// Starts the task that routes notifications to `address`.
    pub fn start(post_office: &Arc<PostOffice>, address: String, node_id: String) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<CoreMessage>();
        let po: Weak<PostOffice> = Arc::downgrade(post_office);
        tokio::spawn(async move {
            while let Some(m) = rx.recv().await {
                let Some(po) = po.upgrade() else { break };
                po.send_internal(m).await;
            }
        });
        Self {
            address,
            node_id,
            tx,
        }
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
        let _ = self.tx.send(m);
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
        let r = MessageRef::new(ServerMessage::new(message));
        let routed = self.route_internal(&r, &address);
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
