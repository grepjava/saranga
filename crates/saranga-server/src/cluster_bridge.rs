//! A cluster connection's bridges to the other primaries: for each node
//! the topology brings, a store-and-forward queue
//! (`$.artemis.internal.sf.<cluster>.<node ID>`) and a bridge consuming it
//! that sends to the other node. Over the bridge's connection, a consumer
//! of the other node's notifications learns its queues and consumers, which
//! become remote queue bindings here (`MessageFlowRecordImpl`).
//!
//! Ported from `ClusterConnectionImpl` (`nodeUP`, `createNewRecord`,
//! `removeRecord`, `disconnectRecord`, `MessageFlowRecordImpl`,
//! `TopologyScanner`, the cluster's own `ServerLocator`) and
//! `ClusterConnectionBridge`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use saranga_buffer::PropertyValue;
use saranga_client::{
    Connection, ConnectionOptions, Connector, CoreMessage, Error as ClientError, MessageExt,
    Session, TopologyEvent, TopologyHook,
};
use saranga_config::BridgeConfig;
use saranga_core_protocol::Flagged;
use saranga_core_protocol::packets::cluster::NodeAnnounceMessage;
use saranga_core_protocol::transport::TransportConfiguration;

use crate::bridge::Bridge;
use crate::cluster::{ClusterConnection, Member};
use crate::notifications::{self, NotificationType, hdr};
use crate::postoffice::QueueSpec;
use crate::queue::Queue;
use crate::remote_binding::RemoteBinding;
use crate::{Broker, RoutingType};

/// `ClusterConnectionImpl.SN_PREFIX`.
const SF_PREFIX: &str = "sf.";

/// `BindingType.DIVERT_INDEX`.
const BINDING_TYPE_DIVERT: i32 = 2;

/// `AdvisorySupport.ADVISORY_TOPIC_PREFIX`, which OpenWire keeps out of
/// clusters (`addProtocolIgnoredAddress`).
const ADVISORY_TOPIC_PREFIX: &str = "ActiveMQ.Advisory.";

/// What makes a bridge a cluster connection's (`ClusterConnectionBridge`).
pub struct ClusterLink {
    cluster: Weak<ClusterConnection>,
    record: Weak<Record>,
    target_node_id: String,
    /// The connector the node was found at.
    pub(crate) connector: TransportConfiguration,
    /// `disconnectedAndDown`: the node was taken out of the topology when
    /// the connection failed, and goes back in when it is made again.
    down: AtomicBool,
}

impl ClusterLink {
    /// The cluster's topology hears what this bridge's connection is told.
    pub(crate) fn topology_hook(&self) -> Option<TopologyHook> {
        self.cluster.upgrade().map(|c| c.topology_hook())
    }

    /// `serverLocator.createSessionFactory(targetNodeID)`: the node's
    /// primary as the topology has it, else the original connector
    /// (`reconnectOnOriginalNode`).
    pub(crate) fn target_connector(&self) -> TransportConfiguration {
        self.cluster
            .upgrade()
            .and_then(|c| c.member(&self.target_node_id))
            .and_then(|m| m.primary)
            .unwrap_or_else(|| self.connector.clone())
    }

    /// `ClusterConnectionImpl.onConnection` and the start of `afterConnect`.
    pub(crate) async fn on_connection(&self, conn: &Connection) {
        let Some(cluster) = self.cluster.upgrade() else {
            return;
        };
        cluster.announce_over(conn).await;
        if self.down.swap(false, Ordering::SeqCst)
            && let Some(m) = cluster.member_or_known(&self.target_node_id, &self.connector)
        {
            // notifyNodeUp, as the bridge connected again.
            cluster.update_from_locator(crate::message::now_millis(), m);
        }
        cluster.start_topology_scanner();
    }

    /// `setupNotificationConsumer`: a temporary queue on the other node's
    /// notification address, filtered to the notifications of bindings,
    /// consumers, sessions and groupings within `max-hops`; its messages go
    /// to the flow record. Then the other node is asked for what it has
    /// (`sendQueueInfoToQueue`).
    pub(crate) async fn setup_notification_consumer(
        &self,
        session: &Session,
    ) -> Result<tokio::task::JoinHandle<()>, ClientError> {
        let (Some(cluster), Some(record)) = (self.cluster.upgrade(), self.record.upgrade()) else {
            return Err(ClientError::InvalidArgument(
                "the cluster connection is gone".to_owned(),
            ));
        };
        let broker = cluster
            .broker()
            .ok_or_else(|| ClientError::InvalidArgument("the broker is gone".to_owned()))?;
        record.reset();
        let notification_address = broker.management().config().notification_address.clone();
        let management_address = broker.management().address().to_owned();
        let queue_name = format!(
            "notif.{}.{}",
            saranga_client::uuid::to_string(&saranga_client::uuid::time_based()),
            server_name(&broker).replace("::", "_")
        );
        let mut ignored = vec![
            cluster.store_and_forward_prefix(&broker),
            management_address.clone(),
        ];
        if broker.has_protocol("OPENWIRE") {
            ignored.push(ADVISORY_TOPIC_PREFIX.to_owned());
        }
        let filter = notification_filter(
            cluster.max_hops(),
            &append_ignores(cluster.address(), &ignored),
            &notification_address,
        );
        session
            .create_queue(
                saranga_client::QueueConfiguration::new(queue_name.as_str())
                    .address(notification_address.as_str())
                    .filter(filter)
                    .durable(false)
                    .temporary(true)
                    .routing_type(saranga_client::RoutingType::Multicast),
            )
            .await?;
        let consumer = session.create_consumer(&queue_name, None, false).await?;
        session.start().await?;
        let task = tokio::spawn(async move {
            loop {
                match consumer.receive(Duration::ZERO).await {
                    Ok(Some(d)) => record.on_message(&d.message),
                    Ok(None) | Err(_) => return,
                }
            }
        });
        // ManagementHelper.putOperationInvocation, without a reply-to.
        let mut request =
            saranga_client::message::new_message(saranga_client::message_type::DEFAULT, false);
        let params = saranga_buffer::json::Json::Array(vec![
            saranga_buffer::json::Json::from(queue_name.as_str()),
            saranga_buffer::json::Json::from(cluster.address()),
        ]);
        request
            .put_string_property(crate::management::HDR_RESOURCE_NAME, "broker")
            .put_string_property(
                crate::management::HDR_OPERATION_NAME,
                "sendQueueInfoToQueue",
            )
            .set_text(Some(&params.to_string()));
        let sent = async {
            let producer = session.create_producer(Some(&management_address)).await?;
            let r = producer.send(request).await;
            producer.close();
            r
        };
        if let Err(e) = sent.await {
            task.abort();
            return Err(e);
        }
        tracing::debug!(
            "Cluster connection bridge on {} requesting information on queues",
            cluster.config().name
        );
        Ok(task)
    }

    /// `ClusterConnectionBridge.fail`: the node leaves the topology; if it
    /// said it was going, its record goes too and the bridge stops trying
    /// (true), otherwise its bindings wait, disconnected, for the bridge to
    /// connect again.
    pub(crate) fn fail(&self, permanently: bool) -> bool {
        let Some(cluster) = self.cluster.upgrade() else {
            return permanently;
        };
        self.down.store(true, Ordering::SeqCst);
        cluster.remove_member(crate::message::now_millis(), &self.target_node_id, false);
        if permanently {
            tracing::debug!(
                "cluster node for bridge {} is permanently down",
                self.target_node_id
            );
            let node = self.target_node_id.clone();
            tokio::spawn(async move { cluster.remove_record(&node).await });
        } else {
            cluster.disconnect_record(&self.target_node_id);
        }
        permanently
    }
}

/// `ActiveMQServerImpl.toString`.
fn server_name(broker: &Broker) -> String {
    match broker.management().config().file.as_ref().map(|f| &f.name) {
        Some(name) if !name.is_empty() => format!("ActiveMQServerImpl::name={name}"),
        _ => format!("ActiveMQServerImpl::serverUUID={}", broker.node_id()),
    }
}

/// `appendIgnoresToFilter`: the cluster's address, then the addresses
/// clusters leave alone, each negated.
fn append_ignores(address: &str, ignored: &[String]) -> String {
    let mut s = address.to_owned();
    if !s.is_empty() {
        s.push(',');
    }
    let negated: Vec<String> = ignored.iter().map(|a| format!("!{a}")).collect();
    s.push_str(&negated.join(","));
    s
}

/// `createSelectorFromAddress` and `buildSelectorFromArray`.
pub fn selector_from_address(address: &str) -> String {
    let a = hdr::ADDRESS;
    if !address.contains(',') {
        return match address.strip_prefix('!') {
            Some(rest) => format!("{a} NOT LIKE '{rest}%'"),
            None => format!("{a} LIKE '{address}%'"),
        };
    }
    let (excludes, includes): (Vec<&str>, Vec<&str>) =
        address.split(',').partition(|s| s.starts_with('!'));
    let excludes: Vec<&str> = excludes.iter().map(|s| &s[1..]).collect();
    let mut b = String::from("(");
    if !includes.is_empty() {
        if !excludes.is_empty() {
            b.push('(');
        }
        let parts: Vec<String> = includes
            .iter()
            .map(|i| format!("({a} LIKE '{i}%')"))
            .collect();
        b.push_str(&parts.join(" OR "));
        if !excludes.is_empty() {
            b.push(')');
        }
    }
    if !excludes.is_empty() {
        if !includes.is_empty() {
            b.push_str(" AND (");
        }
        let parts: Vec<String> = excludes
            .iter()
            .map(|e| format!("({a} NOT LIKE '{e}%')"))
            .collect();
        b.push_str(&parts.join(" AND "));
        if !includes.is_empty() {
            b.push(')');
        }
    }
    b.push(')');
    b
}

/// The notification queue's filter (`setupNotificationConsumer`).
fn notification_filter(max_hops: i32, addresses: &str, notification_address: &str) -> String {
    let types = [
        "SESSION_CREATED",
        "BINDING_ADDED",
        "BINDING_REMOVED",
        "BINDING_UPDATED",
        "CONSUMER_CREATED",
        "CONSUMER_CLOSED",
        "PROPOSAL",
        "PROPOSAL_RESPONSE",
        "UNPROPOSAL",
    ];
    format!(
        "({bt} <> {BINDING_TYPE_DIVERT} OR {bt} IS NULL) AND {nt} IN ('{types}') AND {d} < {max_hops} AND ({sel}) AND ({nt} = 'SESSION_CREATED' OR ({a} NOT LIKE '{notification_address}%'))",
        bt = hdr::BINDING_TYPE,
        nt = hdr::NOTIFICATION_TYPE,
        d = hdr::DISTANCE,
        a = hdr::ADDRESS,
        types = types.join("', '"),
        sel = selector_from_address(addresses),
    )
}

/// What a flow record knows of the other node's queues.
#[derive(Default)]
struct RecordState {
    /// Its remote bindings by cluster name, in the order they came.
    bindings: Vec<Arc<RemoteBinding>>,
    /// The other node's queue data is being sent again: notifications are
    /// heard once it has begun.
    reset: bool,
}

/// `MessageFlowRecordImpl`: a node of the cluster, its store-and-forward
/// queue, the bridge consuming it, and the remote bindings its
/// notifications made.
pub struct Record {
    cluster: Weak<ClusterConnection>,
    target_node_id: String,
    queue: Queue,
    bridge: OnceLock<Arc<Bridge>>,
    st: Mutex<RecordState>,
    closed: AtomicBool,
}

impl Record {
    pub fn target_node_id(&self) -> &str {
        &self.target_node_id
    }

    pub fn bridge(&self) -> Option<&Arc<Bridge>> {
        self.bridge.get()
    }

    pub fn queue(&self) -> &Queue {
        &self.queue
    }

    fn st(&self) -> std::sync::MutexGuard<'_, RecordState> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn broker(&self) -> Option<Arc<Broker>> {
        self.cluster.upgrade()?.broker()
    }

    /// The remote bindings it holds, for management and tests.
    pub fn bindings(&self) -> Vec<Arc<RemoteBinding>> {
        self.st().bindings.clone()
    }

    /// `reset`: the consumers of every binding are forgotten until the
    /// other node tells of them again.
    fn reset(&self) {
        for b in &self.st().bindings {
            b.reset();
        }
    }

    /// `disconnectBindings`: the bridge lost its connection.
    fn disconnect_bindings(&self) {
        let mut st = self.st();
        st.reset = false;
        for b in &st.bindings {
            b.disconnect();
        }
    }

    /// `close`: the bindings go and the bridge stops.
    async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.clear_bindings();
        if let Some(b) = self.bridge.get() {
            let _ = b.stop().await;
        }
    }

    fn clear_bindings(&self) {
        let names: Vec<String> = self
            .st()
            .bindings
            .iter()
            .map(|b| b.unique_name().to_string())
            .collect();
        for n in names {
            self.remove_binding(&n);
        }
    }

    /// `removeBinding`.
    fn remove_binding(&self, cluster_name: &str) {
        let removed = {
            let mut st = self.st();
            st.bindings
                .iter()
                .position(|b| &**b.unique_name() == cluster_name)
                .map(|i| st.bindings.remove(i))
        };
        if removed.is_none() {
            tracing::warn!(
                "Cannot remove binding, because cannot find binding for queue {cluster_name}"
            );
            return;
        }
        if let Some(b) = self.broker() {
            b.post_office().remove_remote_binding(cluster_name);
        }
    }

    /// `onMessage`: a notification from the other node.
    fn on_message(&self, m: &CoreMessage) {
        let has = |k: &str| m.properties.get_str(k).is_some();
        if has(notifications::HDR_RESET_QUEUE_DATA) {
            self.st().reset = true;
            return;
        }
        if has(notifications::HDR_RESET_QUEUE_DATA_COMPLETE) {
            // clearDisconnectedBindings.
            let gone: Vec<String> = self
                .st()
                .bindings
                .iter()
                .filter(|b| !b.is_connected())
                .map(|b| b.unique_name().to_string())
                .collect();
            for n in gone {
                self.remove_binding(&n);
            }
            return;
        }
        if !self.st().reset {
            tracing::debug!("Notification being ignored since first reset wasn't received yet");
            return;
        }
        if let Err(e) = self.handle_notification(m) {
            tracing::error!("AMQ224037: cluster connection Failed to handle message: {e}");
        }
    }

    fn handle_notification(&self, m: &CoreMessage) -> Result<(), String> {
        let kind = string_prop(m, hdr::NOTIFICATION_TYPE).unwrap_or_default();
        match kind.as_str() {
            "BINDING_ADDED" => self.binding_added(m),
            "BINDING_REMOVED" => {
                let name = required(string_prop(m, hdr::CLUSTER_NAME), "clusterName")?;
                self.remove_binding(&name);
                Ok(())
            }
            "BINDING_UPDATED" => {
                let name = required(string_prop(m, hdr::CLUSTER_NAME), "clusterName")?;
                let broker = self.broker().ok_or("the broker is gone")?;
                let po = broker.post_office();
                if let Some(b) = po.remote_binding(&name) {
                    let filter = po
                        .filters()
                        .create(string_prop(m, hdr::FILTERSTRING).as_deref())
                        .map_err(|e| e.to_string())?;
                    b.set_filter(filter);
                }
                Ok(())
            }
            "CONSUMER_CREATED" => self.consumer_changed(m, true),
            "CONSUMER_CLOSED" => self.consumer_changed(m, false),
            "SESSION_CREATED" => {
                let broker = self.broker().ok_or("the broker is gone")?;
                let distance = int_prop(m, hdr::DISTANCE).unwrap_or(0) + 1;
                let s = |k: &str| notifications::string(string_prop(m, k).as_deref());
                let props = vec![
                    (hdr::CONNECTION_NAME, s(hdr::CONNECTION_NAME)),
                    (hdr::REMOTE_ADDRESS, s(hdr::REMOTE_ADDRESS)),
                    (hdr::CLIENT_ID, s(hdr::CLIENT_ID)),
                    (hdr::PROTOCOL_NAME, s(hdr::PROTOCOL_NAME)),
                    (hdr::DISTANCE, PropertyValue::Int(distance)),
                ];
                broker
                    .post_office()
                    .notify(NotificationType::SessionCreated, move |_| props);
                Ok(())
            }
            "PROPOSAL" | "PROPOSAL_RESPONSE" | "UNPROPOSAL" => self.proposal(&kind, m),
            other => Err(format!("AMQ229045: Invalid type: {other}")),
        }
    }

    /// `doProposalReceived`, `doProposalResponseReceived` and
    /// `doUnProposalReceived`: the grouping handler hears of them.
    fn proposal(&self, kind: &str, m: &CoreMessage) -> Result<(), String> {
        let group_id = string_prop(m, hdr::PROPOSAL_GROUP_ID).ok_or("proposal type is null")?;
        let value = string_prop(m, hdr::PROPOSAL_VALUE).unwrap_or_default();
        let hops = int_prop(m, hdr::DISTANCE).unwrap_or(0);
        let broker = self.broker().ok_or("the broker is gone")?;
        let handler = broker
            .post_office()
            .grouping()
            .cloned()
            .ok_or("grouping handler is null")?;
        match kind {
            "PROPOSAL" => {
                if let Some(r) = handler.receive(&group_id, &value, hops + 1) {
                    handler.send_proposal_response(&r, 0);
                }
            }
            "UNPROPOSAL" => handler.remove_at(&group_id, &value, hops + 1),
            _ => {
                let alt = string_prop(m, hdr::PROPOSAL_ALT_VALUE);
                let r = crate::grouping::Response::new(&group_id, &value, alt.as_deref());
                handler.proposed(r.clone());
                handler.send_proposal_response(&r, hops + 1);
            }
        }
        Ok(())
    }

    /// `doBindingAdded`.
    fn binding_added(&self, m: &CoreMessage) -> Result<(), String> {
        let distance = required(int_prop(m, hdr::DISTANCE), "distance")?;
        let address = required(string_prop(m, hdr::ADDRESS), "queueAddress")?;
        let cluster_name = required(string_prop(m, hdr::CLUSTER_NAME), "clusterName")?;
        let routing_name = required(string_prop(m, hdr::ROUTING_NAME), "routingName")?;
        let queue_id = match m.properties.get_str(hdr::BINDING_ID) {
            Some(PropertyValue::Long(id)) => *id,
            _ => return Err("queueID is null".to_owned()),
        };
        let filter_string = string_prop(m, hdr::FILTERSTRING);
        let cluster = self
            .cluster
            .upgrade()
            .ok_or("the cluster connection is gone")?;
        let broker = cluster.broker().ok_or("the broker is gone")?;
        let po = broker.post_office();
        if let Some(existing) = po.remote_binding(&cluster_name) {
            if existing.remote_queue_id() == queue_id {
                if !existing.is_connected() {
                    existing.connect();
                    return Ok(());
                }
                tracing::warn!(
                    "AMQ222139: {}::Remote queue binding {cluster_name} has already been bound in the post office. Most likely cause for this is you have a loop in your cluster due to cluster max-hops being too large or you have multiple cluster connections to the same nodes using overlapping addresses",
                    self.describe()
                );
                return Ok(());
            }
            // The queue was made again with another ID.
            self.remove_binding(&cluster_name);
        }
        let filter = po
            .filters()
            .create(filter_string.as_deref())
            .map_err(|e| e.to_string())?;
        let bridge_name = self
            .bridge
            .get()
            .map_or_else(String::new, |b| b.name().to_owned());
        let binding = Arc::new(RemoteBinding::new(
            po.ids().next(),
            &address,
            &cluster_name,
            &routing_name,
            queue_id,
            filter,
            self.queue.clone(),
            &bridge_name,
            distance + 1,
            cluster.load_balancing(),
        ));
        self.st().bindings.push(binding.clone());
        po.add_remote_binding(binding);
        Ok(())
    }

    /// `doConsumerCreated` and `doConsumerClosed`: the binding's consumers
    /// change, and the change is told on, one hop further.
    fn consumer_changed(&self, m: &CoreMessage, created: bool) -> Result<(), String> {
        let distance = required(int_prop(m, hdr::DISTANCE), "distance")?;
        let cluster_name = required(string_prop(m, hdr::CLUSTER_NAME), "clusterName")?;
        let filter_string = string_prop(m, hdr::FILTERSTRING);
        let binding = self
            .st()
            .bindings
            .iter()
            .find(|b| **b.unique_name() == cluster_name)
            .cloned()
            .ok_or_else(|| format!("Cannot find binding for {cluster_name}"))?;
        let broker = self.broker().ok_or("the broker is gone")?;
        let po = broker.post_office();
        if created {
            let filter = match filter_string.as_deref().filter(|f| !f.is_empty()) {
                Some(f) => po
                    .filters()
                    .create(Some(f))
                    .map_err(|e| e.to_string())?
                    .map(|compiled| (f.to_owned(), compiled)),
                None => None,
            };
            binding.add_consumer(filter);
        } else {
            binding.remove_consumer(filter_string.as_deref());
        }
        let address = match string_prop(m, hdr::ADDRESS) {
            Some(a) if created && a.contains(crate::postoffice::FQQN_SEPARATOR) => a,
            _ => binding.address().to_string(),
        };
        let mut props = vec![
            (hdr::ADDRESS, notifications::string(Some(&address))),
            (
                hdr::CLUSTER_NAME,
                notifications::string(Some(&cluster_name)),
            ),
            (
                hdr::ROUTING_NAME,
                notifications::string(Some(binding.routing_name())),
            ),
            (hdr::DISTANCE, PropertyValue::Int(distance + 1)),
            (
                hdr::CONSUMER_COUNT,
                PropertyValue::Int(self.queue.consumer_count()),
            ),
        ];
        if let Some(f) = &filter_string {
            props.push((hdr::FILTERSTRING, notifications::string(Some(f))));
        }
        let kind = if created {
            NotificationType::ConsumerCreated
        } else {
            NotificationType::ConsumerClosed
        };
        po.notify(kind, move |_| props);
        Ok(())
    }

    /// `MessageFlowRecordImpl.toString`.
    fn describe(&self) -> String {
        format!(
            "MessageFlowRecordImpl [nodeID={}, queueName={}, isClosed={}, reset={}]",
            self.target_node_id,
            self.queue.name(),
            self.closed.load(Ordering::SeqCst),
            self.st().reset
        )
    }
}

fn required<T>(v: Option<T>, what: &str) -> Result<T, String> {
    v.ok_or_else(|| format!("{what} is null"))
}

fn string_prop(m: &CoreMessage, k: &str) -> Option<String> {
    match m.properties.get_str(k)? {
        PropertyValue::String(s) => Some(s.to_string_lossy()),
        _ => None,
    }
}

fn int_prop(m: &CoreMessage, k: &str) -> Option<i32> {
    match m.properties.get_str(k)? {
        PropertyValue::Int(i) => Some(*i),
        _ => None,
    }
}

impl ClusterConnection {
    /// `getSfQueueName`.
    pub fn sf_queue_name(&self, broker: &Broker, node_id: &str) -> String {
        format!(
            "{}{}.{node_id}",
            self.store_and_forward_prefix(broker),
            self.config().name
        )
    }

    /// `storeAndForwardPrefix`: the internal naming prefix and `sf.`.
    fn store_and_forward_prefix(&self, broker: &Broker) -> String {
        format!("{}{SF_PREFIX}", crate::management::internal_prefix(broker))
    }

    /// The cluster connection's `address`: what its bridges forward.
    pub fn address(&self) -> &str {
        &self.config().settings.address
    }

    pub fn max_hops(&self) -> i32 {
        self.config().settings.max_hops
    }

    pub fn load_balancing(&self) -> saranga_config::MessageLoadBalancing {
        self.config().settings.message_load_balancing
    }

    /// Whether it makes bridges at all: a cluster connection with neither
    /// static connectors nor a discovery group has no `serverLocator`.
    fn connects(&self) -> bool {
        !self.config().static_connectors.is_empty() || self.config().discovery.is_some()
    }

    /// The topology changes a locator of this cluster connection is told,
    /// as they reach the shared `Topology` (`ServerLocatorImpl.notifyNodeUp`
    /// and `notifyNodeDown`).
    pub fn topology_hook(self: &Arc<Self>) -> TopologyHook {
        let weak = Arc::downgrade(self);
        TopologyHook(Arc::new(move |event| {
            let Some(c) = weak.upgrade() else { return };
            // `stop` removes the cluster connection from its locator's
            // listeners first.
            if c.is_stopping() {
                return;
            }
            match event {
                TopologyEvent::Up {
                    node_id,
                    unique_event_id,
                    primary,
                    backup,
                    backup_group_name,
                    scale_down_group_name,
                    ..
                } => c.update_from_locator(
                    unique_event_id,
                    Member {
                        node_id,
                        backup_group_name,
                        scale_down_group_name,
                        primary,
                        backup,
                        unique_event_id,
                    },
                ),
                TopologyEvent::Down {
                    node_id,
                    unique_event_id,
                    disconnect,
                } => {
                    c.remove_member(unique_event_id, &node_id, disconnect);
                }
            }
        }))
    }

    /// The locator settings of the cluster's connections.
    fn locator_options(self: &Arc<Self>) -> ConnectionOptions {
        let s = &self.config().settings;
        let millis = |v: i64| (v >= 0).then(|| Duration::from_millis(v as u64));
        ConnectionOptions {
            client_failure_check_period: millis(s.check_period),
            connection_ttl: millis(s.connection_ttl),
            call_timeout: Duration::from_millis(s.call_timeout.max(0) as u64),
            call_failover_timeout: millis(s.call_failover_timeout),
            user: Some(self.config().cluster_user.clone()),
            password: Some(self.config().cluster_password.clone()),
            cluster_connection: true,
            ha: true,
            reconnect_attempts: 0,
            initial_connect_attempts: 0,
            retry_interval: Duration::from_millis(s.retry_interval.max(0) as u64),
            topology_listener: Some(self.topology_hook()),
            ..ConnectionOptions::default()
        }
    }

    /// `ClusterControl.authorize` and `sendNodeAnnounce` of this node, as
    /// `onConnection` does over each connection the cluster makes.
    pub(crate) async fn announce_over(&self, conn: &Connection) {
        let Some(local) = self.member(self.node_id()) else {
            tracing::warn!(
                "AMQ222138: Local Member is not set at on ClusterConnection {}",
                self.config().name
            );
            return;
        };
        let (user, password) = (&self.config().cluster_user, &self.config().cluster_password);
        match conn.cluster_connect(user, password).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    "AMQ222186: unable to authorise cluster control: AMQ219016: Connection failure detected. Unblocking a blocking call that will never get a response"
                );
                return;
            }
            Err(e) => {
                tracing::warn!("AMQ222186: unable to authorise cluster control: {e}");
                return;
            }
        }
        let announced = conn.node_announce(NodeAnnounceMessage {
            node_id: local.node_id.as_str().into(),
            backup_group_name: None,
            backup: false,
            current_event_id: local.unique_event_id,
            connector: Flagged(local.primary.clone()),
            backup_connector: Flagged(local.backup.clone()),
            scale_down_group_name: None,
        });
        if let Err(e) = announced {
            tracing::warn!("AMQ222186: unable to authorise cluster control: {e}");
        }
    }

    /// `activate`'s part after the topology: the cluster's own locator
    /// connects to the static connectors or the discovery group, and every
    /// primary the topology brings is bridged to.
    pub fn start_bridging(self: &Arc<Self>, broker: &Arc<Broker>) {
        self.set_broker(broker);
        broker
            .post_office()
            .notify(NotificationType::ClusterConnectionStarted, {
                let name = self.config().name.clone();
                move |_| vec![("name", notifications::string(Some(&name)))]
            });
        // addClusterTopologyListener(this)
        let listener = Arc::new(NodeUpListener(Arc::downgrade(self)));
        let id = self.add_listener(listener);
        self.set_bridging_listener(id);
        if !self.connects() {
            return;
        }
        let this = self.clone();
        let task = tokio::spawn(async move { this.locate().await });
        self.set_locator_task(task);
    }

    /// The cluster's `ServerLocator`: connects (without limit) to the
    /// static connectors in turn, or through the discovery group, announces
    /// this node and hears the topology over that connection while it lasts.
    async fn locate(self: Arc<Self>) {
        let retry = Duration::from_millis(self.config().settings.retry_interval.max(0) as u64);
        let mut next = 0usize;
        let conn = loop {
            if self.is_stopping() {
                return;
            }
            let attempt = match &self.config().discovery {
                Some(d) => Connection::connect_discovery(d.clone(), self.locator_options()).await,
                None => {
                    let targets: Vec<Connector> = self
                        .config()
                        .static_connectors
                        .iter()
                        .filter(|tc| !crate::cluster::same_connector(tc, self.connector()))
                        .filter_map(Connector::from_transport)
                        .collect();
                    if targets.is_empty() {
                        return;
                    }
                    let target = targets[next % targets.len()].clone();
                    next += 1;
                    Connection::connect_to(vec![target], self.locator_options()).await
                }
            };
            match attempt {
                Ok(c) => break c,
                Err(e) => tracing::debug!("cluster {}: {e}", self.config().name),
            }
            tokio::time::sleep(retry).await;
        };
        self.announce_over(&conn).await;
        self.start_topology_scanner();
        let mut stopping = self.stopping_rx();
        tokio::select! {
            _ = conn.failed() => {}
            _ = stopping.wait_for(|s| *s) => {}
        }
        conn.close().await;
    }

    /// `nodeUP`: a primary other than this node gets a flow record, its
    /// store-and-forward queue made if it is not there.
    fn node_up(self: &Arc<Self>, member: &Member) {
        if self.is_stopping() || member.node_id == self.node_id() || !self.connects() {
            return;
        }
        let Some(primary) = member.primary.clone() else {
            return;
        };
        if self.config().settings.allow_direct_connections_only
            && !self
                .config()
                .static_connectors
                .iter()
                .any(|tc| same_endpoint(tc, &primary))
        {
            return;
        }
        let Some(broker) = self.broker() else { return };
        let mut records = self.records();
        if records.contains_key(&member.node_id) {
            tracing::trace!(
                "ignored nodeUp record for {} as the record already existed",
                member.node_id
            );
            return;
        }
        match self.create_record(&broker, &member.node_id, primary) {
            Ok(r) => {
                records.insert(member.node_id.clone(), r);
            }
            Err(e) => {
                tracing::error!("AMQ224036: Failed to update cluster connection topology: {e}")
            }
        }
    }

    /// `createNewRecord`: the queue, the record and its bridge, started.
    fn create_record(
        self: &Arc<Self>,
        broker: &Arc<Broker>,
        node_id: &str,
        connector: TransportConfiguration,
    ) -> Result<Arc<Record>, crate::BrokerError> {
        let queue_name = self.sf_queue_name(broker, node_id);
        let po = broker.post_office();
        let queue = match po.queue(&queue_name) {
            Some(q) => q,
            None => {
                let mut spec = QueueSpec::new(queue_name.as_str(), queue_name.as_str());
                spec.routing_type = Some(RoutingType::Multicast);
                spec.auto_create_address = Some(true);
                spec.max_consumers = Some(-1);
                spec.purge_on_no_consumers = Some(false);
                spec.internal = true;
                po.create_queue(spec, true)?
            }
        };
        let record = Arc::new(Record {
            cluster: Arc::downgrade(self),
            target_node_id: node_id.to_owned(),
            queue: queue.clone(),
            bridge: OnceLock::new(),
            st: Mutex::default(),
            closed: AtomicBool::new(false),
        });
        let link = ClusterLink {
            cluster: Arc::downgrade(self),
            record: Arc::downgrade(&record),
            target_node_id: node_id.to_owned(),
            connector,
            down: AtomicBool::new(false),
        };
        let bridge = Bridge::new_cluster(broker, self.bridge_config(&queue_name), queue, link)?;
        let _ = record.bridge.set(bridge.clone());
        bridge.start()?;
        Ok(record)
    }

    /// The `BridgeConfiguration` `ClusterConnectionBridge` makes.
    fn bridge_config(&self, name: &str) -> BridgeConfig {
        let s = &self.config().settings;
        let mut c = BridgeConfig::new(name);
        c.queue_name = Some(name.to_owned());
        c.forwarding_address = None;
        c.initial_connect_attempts = s.initial_connect_attempts;
        c.reconnect_attempts = s.reconnect_attempts;
        c.reconnect_attempts_on_same_node = 0;
        c.retry_interval = s.retry_interval;
        c.retry_interval_multiplier = s.retry_interval_multiplier;
        c.max_retry_interval = s.max_retry_interval;
        c.use_duplicate_detection = s.use_duplicate_detection;
        c.user = Some(self.config().cluster_user.clone());
        c.password = Some(self.config().cluster_password.clone());
        c.routing_type = saranga_config::ComponentRoutingType::Pass;
        c.client_id = s.client_id.clone();
        c.confirmation_window_size = s.confirmation_window_size;
        c.producer_window_size = s.producer_window_size;
        c.min_large_message_size = s.min_large_message_size;
        c.call_timeout = s.call_timeout;
        c.client_failure_check_period = s.check_period;
        c.connection_ttl = s.connection_ttl;
        c.ha = true;
        c
    }

    /// `removeRecord`: the record of a node gone for good is closed; if the
    /// node is back in the topology meanwhile, it gets a new one.
    pub(crate) async fn remove_record(self: &Arc<Self>, node_id: &str) {
        let record = self.records().remove(node_id);
        if let Some(r) = record {
            r.close().await;
        }
        if self.is_stopping() {
            return;
        }
        if let Some(m) = self.member(node_id)
            && m.primary.is_some()
        {
            tracing::debug!(
                "Node {node_id} is already back in the topology after record removal; re-creating bridge"
            );
            self.node_up(&m);
        }
    }

    /// `disconnectRecord`.
    pub(crate) fn disconnect_record(&self, node_id: &str) {
        if let Some(r) = self.records().get(node_id) {
            r.disconnect_bindings();
        }
    }

    /// The flow records, by node ID.
    pub fn flow_records(&self) -> Vec<Arc<Record>> {
        self.records().values().cloned().collect()
    }

    /// `getNodes`: each node whose bridge is connected, with the remote
    /// address of its connection.
    pub fn nodes(&self) -> Vec<(String, String)> {
        self.records()
            .iter()
            .filter_map(|(id, r)| Some((id.clone(), r.bridge()?.remote_address()?)))
            .collect()
    }

    /// `getMetrics`: the messages pending acknowledgement and acknowledged,
    /// over every bridge.
    pub fn metrics(&self) -> (i64, i64) {
        self.records()
            .values()
            .filter_map(|r| r.bridge())
            .fold((0, 0), |(p, a), b| {
                (
                    p + b.metrics().messages_pending_acknowledgement(),
                    a + b.metrics().messages_acknowledged(),
                )
            })
    }

    /// `getBridgeMetrics`: the bridge to a node's.
    pub fn bridge_metrics(&self, node_id: &str) -> Option<(i64, i64)> {
        let records = self.records();
        let b = records.get(node_id)?.bridge()?;
        Some((
            b.metrics().messages_pending_acknowledgement(),
            b.metrics().messages_acknowledged(),
        ))
    }

    /// `stop`'s part for bridging: every record is closed, and
    /// `CLUSTER_CONNECTION_STOPPED` told.
    pub(crate) async fn stop_bridging(&self) {
        let records: Vec<Arc<Record>> = self.records().drain().map(|(_, r)| r).collect();
        for r in records {
            r.close().await;
        }
        if let Some(b) = self.broker() {
            let name = self.config().name.clone();
            b.post_office()
                .notify(NotificationType::ClusterConnectionStopped, move |_| {
                    vec![("name", notifications::string(Some(&name)))]
                });
        }
    }

    /// `TopologyScanner`: connects for a moment to each static connector
    /// the topology does not have yet, so its node tells its topology; every
    /// `retry-interval`, `topology-scanner-attempts` times, until a round
    /// connects to them all.
    pub(crate) fn start_topology_scanner(self: &Arc<Self>) {
        let attempts = self.config().settings.topology_scanner_attempts;
        if attempts == 0 || self.is_stopping() || self.config().static_connectors.is_empty() {
            return;
        }
        let this = self.clone();
        let task = tokio::spawn(async move {
            let retry = Duration::from_millis(this.config().settings.retry_interval.max(0) as u64);
            let mut count = 0;
            loop {
                tokio::time::sleep(retry).await;
                if this.is_stopping() {
                    return;
                }
                let members = this.members();
                let missing: Vec<TransportConfiguration> = this
                    .config()
                    .static_connectors
                    .iter()
                    .filter(|tc| {
                        !same_endpoint(this.connector(), tc)
                            && !members.iter().any(|m| {
                                m.primary.as_ref().is_some_and(|p| same_endpoint(p, tc))
                                    || m.backup.as_ref().is_some_and(|b| same_endpoint(b, tc))
                            })
                    })
                    .cloned()
                    .collect();
                let mut updated = true;
                for tc in missing {
                    let Some(c) = Connector::from_transport(&tc) else {
                        continue;
                    };
                    match Connection::connect_to(vec![c], this.locator_options()).await {
                        Ok(conn) => conn.close().await,
                        Err(e) => {
                            tracing::debug!("Cluster topology scanner failed to connect: {e}");
                            updated = false;
                        }
                    }
                }
                count += 1;
                if updated || this.is_stopping() {
                    return;
                }
                if attempts != -1 && count >= attempts {
                    tracing::warn!(
                        "AMQ224144: The topology of the cluster connection {} doesn't include all the expected members. Check the discovery group or the static connectors of the cluster connection if the topology is correct",
                        this.config().name
                    );
                    return;
                }
            }
        });
        self.set_scanner_task(task);
    }

    fn records(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Record>>> {
        self.records_mutex()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
}

/// `TopologyScanner.compareTCs`: the same port, and the same host or one
/// resolving to the same address.
fn same_endpoint(a: &TransportConfiguration, b: &TransportConfiguration) -> bool {
    match (Connector::from_transport(a), Connector::from_transport(b)) {
        (Some(a), Some(b)) => a.is_equivalent(&b),
        _ => false,
    }
}

/// The cluster connection as a listener of its own topology.
struct NodeUpListener(Weak<ClusterConnection>);

impl crate::cluster::TopologyListener for NodeUpListener {
    fn node_up(&self, member: &Member, _last: bool) {
        if let Some(c) = self.0.upgrade() {
            // Topology listeners run on the cluster's executor.
            let member = member.clone();
            tokio::spawn(async move { c.node_up(&member) });
        }
    }

    /// Bridges decide for themselves when their node is gone.
    fn node_down(&self, _unique_event_id: i64, _node_id: &str) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_notification_filter_is_artemis_s() {
        let ignores = append_ignores(
            "",
            &[
                "$.artemis.internal.sf.".to_owned(),
                "activemq.management".to_owned(),
            ],
        );
        assert_eq!(
            notification_filter(1, &ignores, "activemq.notifications"),
            "(_AMQ_Binding_Type <> 2 OR _AMQ_Binding_Type IS NULL) AND _AMQ_NotifType IN ('SESSION_CREATED', 'BINDING_ADDED', 'BINDING_REMOVED', 'BINDING_UPDATED', 'CONSUMER_CREATED', 'CONSUMER_CLOSED', 'PROPOSAL', 'PROPOSAL_RESPONSE', 'UNPROPOSAL') AND _AMQ_Distance < 1 AND (((_AMQ_Address NOT LIKE '$.artemis.internal.sf.%') AND (_AMQ_Address NOT LIKE 'activemq.management%'))) AND (_AMQ_NotifType = 'SESSION_CREATED' OR (_AMQ_Address NOT LIKE 'activemq.notifications%'))"
        );
    }

    #[test]
    fn selectors_include_and_exclude() {
        assert_eq!(selector_from_address("jms"), "_AMQ_Address LIKE 'jms%'");
        assert_eq!(
            selector_from_address("a,b,!c"),
            "(((_AMQ_Address LIKE 'a%') OR (_AMQ_Address LIKE 'b%')) AND ((_AMQ_Address NOT LIKE 'c%')))"
        );
    }
}
