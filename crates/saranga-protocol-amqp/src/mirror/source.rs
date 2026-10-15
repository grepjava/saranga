//! A broker connection's `<mirror>` (`AMQPMirrorControllerSource`): its
//! store-and-forward queue takes a reference to every message the broker
//! routes (or a copy, when it pages) and a control message for every
//! acknowledgement, address and queue added or removed, and the mirror link
//! sends them on to the peer.

use std::sync::{Arc, Mutex, Weak};

use saranga_config::AmqpMirrorElement;
use saranga_server::Broker;
use saranga_server::message::{MessageRef, ServerMessage};
use saranga_server::mirror::{MirrorController, Origin, SyncWaits};
use saranga_server::postoffice::{AddressInfo, QueueSpec, RoutingTypes};
use saranga_server::queue::{AckReason, Queue};
use saranga_server::settings::AddressFullPolicy;
use saranga_server::storage::Pending;
use saranga_server::{BrokerError, RoutingType};

use crate::native::AmqpNative;
use crate::types::Value;

use super::{
    ADD_ADDRESS, CREATE_QUEUE, DELETE_ADDRESS, DELETE_QUEUE, POST_ACK, address_json,
    control_message,
};

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// `MirrorAddressFilter`: `address-filter`'s comma-separated prefixes, those
/// starting with `!` excluded.
#[derive(Debug, Clone, Default)]
pub struct AddressFilter {
    allow: Vec<String>,
    deny: Vec<String>,
}

impl AddressFilter {
    pub fn new(filter: Option<&str>) -> Self {
        let mut f = Self::default();
        for part in filter.unwrap_or_default().split(',') {
            if part.is_empty() || part == "!" {
                continue;
            }
            let list = match part.strip_prefix('!') {
                Some(p) => (&mut f.deny, p),
                None => (&mut f.allow, part),
            };
            if !list.0.iter().any(|e| e == list.1) {
                list.0.push(list.1.to_owned());
            }
        }
        f
    }

    /// `match`.
    pub fn matches(&self, address: &str) -> bool {
        if self.deny.iter().any(|p| address.starts_with(p.as_str())) {
            return false;
        }
        self.allow.is_empty() || self.allow.iter().any(|p| address.starts_with(p.as_str()))
    }
}

/// `AMQPMirrorControllerSource`.
pub struct MirrorSource {
    broker: Weak<Broker>,
    snf: Queue,
    snf_address: String,
    acks: bool,
    add_queues: bool,
    delete_queues: bool,
    sync: bool,
    filter: AddressFilter,
    management_address: String,
    /// The peer's node ID, once its mirror link said it
    /// (`getRemoteMirrorId`).
    remote_id: Mutex<Option<String>>,
    sync_waits: SyncWaits,
}

impl MirrorSource {
    /// `installMirrorController`: the store-and-forward queue
    /// `$ACTIVEMQ_ARTEMIS_MIRROR_<connection>` (internal, anycast, durable
    /// as configured) is made if missing, paging (or with `sync`, blocking)
    /// whatever its address settings say; every address and queue there is
    /// goes to the peer (`scanAddresses`), and the mirror is installed.
    pub fn install(
        broker: &Arc<Broker>,
        connection: &str,
        element: &AmqpMirrorElement,
    ) -> Result<Arc<Self>, BrokerError> {
        let po = broker.post_office();
        let snf_address = saranga_server::mirror::mirror_address(connection);
        let info = match po.address_info(&snf_address) {
            Some(info) => info,
            None => {
                po.add_address_info(AddressInfo {
                    id: 0,
                    name: snf_address.as_str().into(),
                    routing_types: RoutingTypes::of(RoutingType::Anycast),
                    auto_created: false,
                    temporary: !element.durable,
                    internal: true,
                })?
                .0
            }
        };
        if !info.routing_types.contains(RoutingType::Anycast) {
            return Err(BrokerError::internal(format!(
                "{snf_address} has {} instead of ANYCAST",
                info.routing_types.describe()
            )));
        }
        let snf = match po.queue(&snf_address) {
            Some(q) => q,
            None => {
                let mut spec = QueueSpec::new(snf_address.as_str(), snf_address.as_str());
                spec.routing_type = Some(RoutingType::Anycast);
                spec.durable = element.durable;
                spec.internal = true;
                po.create_queue(spec, true)?
            }
        };
        if *snf.config().address != *snf_address {
            tracing::warn!(
                "Queue {} belong to a different address ({}), while we expected it to be {}",
                snf.name(),
                snf.config().address,
                snf_address
            );
            return Err(BrokerError::internal("Cannot start replica"));
        }
        po.paging_store(&snf_address)
            .enforce_policy(match element.sync {
                true => AddressFullPolicy::Block,
                false => AddressFullPolicy::Page,
            });
        let source = Arc::new(Self {
            broker: Arc::downgrade(broker),
            snf,
            snf_address,
            acks: element.message_acknowledgements,
            add_queues: element.queue_creation,
            delete_queues: element.queue_removal,
            sync: element.sync,
            filter: AddressFilter::new(element.address_filter.as_deref()),
            management_address: broker.management().address().to_owned(),
            remote_id: Mutex::new(None),
            sync_waits: SyncWaits::default(),
        });
        // scanAddresses
        let queues = po.queues();
        for info in po.addresses() {
            source.add_address(&info);
            for q in queues
                .iter()
                .filter(|q| q.config().address == info.name && !q.config().internal)
            {
                source.create_queue(q);
            }
        }
        po.mirrors().install(source.clone());
        Ok(source)
    }

    /// The store-and-forward queue.
    pub fn snf(&self) -> &Queue {
        &self.snf
    }

    pub fn snf_address(&self) -> &str {
        &self.snf_address
    }

    /// The peer's mirror link answered with its node ID.
    pub fn set_remote_id(&self, id: Option<String>) {
        *lock(&self.remote_id) = id;
    }

    /// `ignoreAddress`: the management address, and what the filter
    /// leaves out.
    fn ignore_address(&self, address: &str) -> bool {
        address.starts_with(self.management_address.as_str()) || !self.filter.matches(address)
    }

    /// `invalidTarget(controller)`: the target in use is this mirror's own
    /// peer, which is not sent back what it sent.
    fn invalid_target(&self, origin: Option<&Origin>) -> bool {
        let Some(origin) = origin else {
            return false;
        };
        same_node(
            self.remote_mirror_id().as_deref(),
            origin.remote_id.as_deref(),
        )
    }

    /// `routeMirrorCommand`: a control message, its ID given, goes to the
    /// store-and-forward queue. Returns its ID.
    fn route_command(
        &self,
        address: Option<&str>,
        queue: Option<&str>,
        event: &str,
        broker_id: Option<&str>,
        body: Option<Value>,
        reason: AckReason,
    ) -> Option<i64> {
        let broker = self.broker.upgrade()?;
        let po = broker.post_office();
        let mut m = control_message(
            &self.snf_address,
            address,
            queue,
            event,
            broker_id,
            body,
            reason,
        );
        let id = po.ids().next();
        m.set_message_id(id);
        let r = MessageRef::new(ServerMessage::new_native(AmqpNative::arc(m)));
        po.route_to_queue(r, &self.snf);
        Some(id)
    }
}

fn same_node(remote: Option<&str>, source: Option<&str>) -> bool {
    matches!((remote, source), (Some(a), Some(b)) if a == b)
}

impl MirrorController for MirrorSource {
    fn add_address(&self, info: &AddressInfo) {
        // A target in use adds what its peer sent, not sent on.
        if saranga_server::mirror::origin().is_some() && !info.internal {
            return;
        }
        if info.internal || info.temporary || self.ignore_address(&info.name) {
            return;
        }
        if self.add_queues {
            self.route_command(
                Some(&info.name),
                None,
                ADD_ADDRESS,
                None,
                Some(Value::string(address_json(info))),
                AckReason::Normal,
            );
        }
    }

    fn delete_address(&self, info: &AddressInfo) {
        let origin = saranga_server::mirror::origin();
        if self.invalid_target(origin.as_ref()) || info.internal || info.temporary {
            return;
        }
        if self.ignore_address(&info.name) {
            return;
        }
        if self.delete_queues {
            self.route_command(
                Some(&info.name),
                None,
                DELETE_ADDRESS,
                None,
                Some(Value::string(address_json(info))),
                AckReason::Normal,
            );
        }
    }

    fn create_queue(&self, queue: &Queue) {
        let origin = saranga_server::mirror::origin();
        let c = queue.config();
        if self.invalid_target(origin.as_ref()) || c.internal || c.temporary {
            return;
        }
        if self.ignore_address(&c.address) {
            return;
        }
        if self.add_queues {
            let json = saranga_server::management::queue_config_json(queue).to_string();
            self.route_command(
                Some(&c.address),
                Some(&c.name),
                CREATE_QUEUE,
                None,
                Some(Value::string(json)),
                AckReason::Normal,
            );
        }
    }

    fn delete_queue(&self, queue: &Queue) {
        let origin = saranga_server::mirror::origin();
        let c = queue.config();
        if self.invalid_target(origin.as_ref()) || self.ignore_address(&c.address) {
            return;
        }
        if c.temporary || c.internal {
            return;
        }
        if self.delete_queues {
            self.route_command(
                Some(&c.address),
                Some(&c.name),
                DELETE_QUEUE,
                None,
                Some(Value::string(c.name.to_string())),
                AckReason::Normal,
            );
        }
    }

    fn send_message(&self, r: &MessageRef, origin: Option<&Origin>) -> Option<Queue> {
        let message = &r.message;
        // invalidTarget(context.getMirrorSource(), message): before this
        // end's link is up, the peer's ID is the message's.
        if let Some(origin) = origin {
            let remote = self
                .remote_mirror_id()
                .or_else(|| annotation_broker_id(message));
            if same_node(remote.as_deref(), origin.remote_id.as_deref()) {
                return None;
            }
        }
        let address = message
            .core()
            .address
            .as_ref()
            .map(|a| a.to_string_lossy())
            .unwrap_or_default();
        if self.ignore_address(&address) {
            return None;
        }
        let node_id = saranga_server::mirror::server_id(message);
        let remote = self
            .remote_mirror_id()
            .or_else(|| origin.and_then(|o| o.remote_id.clone()));
        if node_id.is_some() && node_id == remote {
            // It came from the peer: it is not sent back.
            return None;
        }
        Some(self.snf.clone())
    }

    fn sync_wait(&self, message_id: i64) -> Option<Pending> {
        self.sync.then(|| self.sync_waits.wait(message_id))
    }

    fn acknowledged(
        &self,
        queue: &Queue,
        r: &MessageRef,
        reason: AckReason,
        origin: Option<&Origin>,
    ) -> Option<Pending> {
        // postAcknowledge: the peer took a message from the
        // store-and-forward queue (`postACKInternalMessage`).
        if queue == &self.snf {
            if self.sync {
                self.sync_waits.done(r.message_id());
            }
            return None;
        }
        // preAcknowledge: a retried acknowledgement was the peer's.
        if origin.is_some_and(|o| o.retry_ack) {
            return None;
        }
        if !self.acks || self.invalid_target(origin) {
            return None;
        }
        let c = queue.config();
        if c.internal || c.temporary || self.ignore_address(&c.address) {
            return None;
        }
        let node_id = saranga_server::mirror::server_id(&r.message);
        let internal_id = saranga_server::mirror::internal_id(&r.message);
        let id = self.route_command(
            Some(&c.address),
            Some(&c.name),
            POST_ACK,
            node_id.as_deref(),
            Some(Value::Long(internal_id)),
            reason,
        )?;
        self.sync_wait(id)
    }

    fn remote_mirror_id(&self) -> Option<String> {
        lock(&self.remote_id).clone()
    }
}

impl std::fmt::Debug for MirrorSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MirrorSource({})", self.snf_address)
    }
}

/// `message.getAnnotation(x-opt-amq-bkr-id)`.
fn annotation_broker_id(message: &ServerMessage) -> Option<String> {
    let n = message.native()?.as_any().downcast_ref::<AmqpNative>()?;
    n.message
        .annotation(super::BROKER_ID)
        .and_then(Value::as_str)
        .map(str::to_owned)
}
