//! A peer's mirror link (`AMQPMirrorControllerTarget`): its control
//! messages add and remove addresses and queues and acknowledge messages
//! here, the latter through the broker's `AckManager`; its other messages
//! are routed here as they were there, keeping their broker and ID.

use std::sync::{Arc, Mutex};

use saranga_core_protocol::ErrorType;
use saranga_server::Broker;
use saranga_server::BrokerError;
use saranga_server::duplicate::CacheRef;
use saranga_server::mirror::{MIRROR_ADDRESS, Origin, with_origin};
use saranga_server::session::ServerSession;

use crate::message::AmqpMessage;
use crate::native::AmqpNative;
use crate::types::Value;

use super::{
    ACK_REASON, ADD_ADDRESS, BROKER_ID, CREATE_QUEUE, DELETE_ADDRESS, DELETE_QUEUE, EVENT_TYPE,
    INTERNAL_DESTINATION, INTERNAL_ID, POST_ACK, QUEUE, TARGET_QUEUES, address_from_json,
    annotation_map, set_mirror_properties,
};

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// `AMQPMirrorControllerTarget`.
pub struct MirrorTarget {
    broker: Arc<Broker>,
    /// The peer's node ID, from its link's properties.
    remote_id: Option<String>,
    /// The duplicate cache of the broker messages last came from
    /// (`lruDuplicateIDKey`, `lruduplicateIDCache`).
    cache: Mutex<Option<(String, CacheRef)>>,
    /// The link's credit, which sizes the duplicate caches.
    credits: i32,
}

impl MirrorTarget {
    pub fn new(broker: Arc<Broker>, remote_id: Option<String>, credits: i32) -> Self {
        Self {
            broker,
            remote_id,
            cache: Mutex::new(None),
            credits,
        }
    }

    /// What the peer is as the target in use.
    fn origin(&self) -> Origin {
        Origin {
            remote_id: self.remote_id.clone(),
            retry_ack: false,
        }
    }

    /// `actualDelivery`: an event is applied, a message routed.
    pub async fn receive(
        &self,
        server: &mut ServerSession,
        format: u32,
        payload: Vec<u8>,
        now: i64,
        large_at: usize,
    ) -> Result<(), BrokerError> {
        let large = large_at > 0 && payload.len() >= large_at;
        let file = large.then(|| payload.clone());
        let mut m = AmqpMessage::new(i64::from(format), payload, now)
            .map_err(|e| BrokerError::internal(e.to_string()))?;
        let event = m
            .annotation(EVENT_TYPE)
            .as_ref()
            .and_then(Value::as_symbol)
            .map(|s| s.as_str().to_owned());
        if let Some(event) = event {
            self.event(&event, &m).await;
            return Ok(());
        }
        // sendMessage
        let da = annotation_map(m.delivery_annotations());
        let get = |k: &str| da.get(&crate::Symbol::from(k));
        let mirror_id = get(BROKER_ID)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| self.remote_id.clone());
        let internal_id = get(INTERNAL_ID).and_then(Value::as_i64).unwrap_or(0);
        let internal_address = get(INTERNAL_DESTINATION)
            .and_then(Value::as_str)
            .map(str::to_owned);
        let target_queues: Option<Vec<String>> = match get(TARGET_QUEUES) {
            Some(Value::List(l)) => Some(
                l.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect(),
            ),
            Some(Value::Array(a)) => Some(
                a.items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect(),
            ),
            _ => None,
        };
        let cache = self.cache(mirror_id.as_deref().unwrap_or("null"));
        match &mirror_id {
            Some(id) => set_mirror_properties(&mut m, internal_id, id),
            None => m.set_broker_property(
                INTERNAL_ID,
                saranga_buffer::PropertyValue::Long(internal_id),
            ),
        }
        let body = match file {
            Some(bytes) => {
                let id = self.broker.post_office().ids().next();
                m.set_message_id(id);
                m.make_large();
                let io = |e: std::io::Error| BrokerError::new(ErrorType::IoError, e.to_string());
                let mut w = self
                    .broker
                    .large_messages()
                    .create(id, m.is_durable())
                    .map_err(io)?;
                w.append(&bytes).map_err(io)?;
                Some(w.finish().map_err(io)?)
            }
            None => None,
        };
        let routed = server.send_mirrored(
            AmqpNative::arc(m),
            body,
            internal_address.as_deref(),
            target_queues.as_deref(),
            self.origin(),
            &cache,
            internal_id.to_be_bytes().to_vec(),
        );
        if let Err(e) = &routed {
            tracing::warn!("{}", e.message);
        }
        routed.map(|_| ())
    }

    /// The duplicate cache of messages from `mirror_id`
    /// (`$ACTIVEMQ_ARTEMIS_MIRROR_<id>`, of the link's credit).
    fn cache(&self, mirror_id: &str) -> CacheRef {
        let mut lru = lock(&self.cache);
        if let Some((key, c)) = &*lru
            && key == mirror_id
        {
            return c.clone();
        }
        let c = self
            .broker
            .post_office()
            .named_duplicate_cache(&format!("{MIRROR_ADDRESS}_{mirror_id}"), self.credits);
        *lru = Some((mirror_id.to_owned(), c.clone()));
        c
    }

    /// The control messages.
    async fn event(&self, event: &str, m: &AmqpMessage) {
        let po = self.broker.post_office().clone();
        let body = || match m.body() {
            Some((_, Value::String(s))) => Some(s),
            _ => None,
        };
        let annotation = |k: &str| {
            m.annotation(k)
                .as_ref()
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        match event {
            ADD_ADDRESS => match body().map(|b| address_from_json(&b)) {
                Some(Ok(info)) => {
                    tracing::debug!("adding address {}", info.name);
                    if let Err(e) = with_origin(Some(self.origin()), || po.add_address_info(info)) {
                        tracing::debug!("{}", e.message);
                    }
                }
                Some(Err(e)) => tracing::warn!("{e}"),
                None => {}
            },
            DELETE_ADDRESS => match body().map(|b| address_from_json(&b)) {
                Some(Ok(info)) => {
                    // removeAddressInfo(address, null, true): its queues
                    // go too.
                    for q in po.queues() {
                        if q.config().address != info.name {
                            continue;
                        }
                        self.broker.disconnect_consumers(&q).await;
                        let _ = with_origin(Some(self.origin()), || {
                            po.delete_queue(q.name(), false, false)
                        });
                    }
                    with_origin(Some(self.origin()), || po.remove_address(&info.name));
                }
                Some(Err(e)) => tracing::warn!("{e}"),
                None => {}
            },
            CREATE_QUEUE => {
                let Some(json) = body() else { return };
                match saranga_server::management::queue_spec_from_json(&json) {
                    Ok(spec) => {
                        if let Err(e) =
                            with_origin(Some(self.origin()), || po.create_queue(spec, true))
                        {
                            tracing::debug!(
                                "Queue could not be created, already existed: {}",
                                e.message
                            );
                        }
                    }
                    Err(e) => tracing::warn!("{e}"),
                }
            }
            DELETE_QUEUE => {
                let Some(name) = annotation(QUEUE) else {
                    return;
                };
                if let Some(q) = po.queue(&name) {
                    self.broker.disconnect_consumers(&q).await;
                }
                if let Err(e) =
                    with_origin(Some(self.origin()), || po.delete_queue(&name, false, false))
                {
                    tracing::debug!("queue {name} was previously removed: {}", e.message);
                }
            }
            POST_ACK => {
                // Without one, the message was the peer's own.
                let node_id = annotation(BROKER_ID).or_else(|| self.remote_id.clone());
                let reason = saranga_server::mirror::reason_from(
                    m.annotation(ACK_REASON)
                        .as_ref()
                        .and_then(Value::as_i64)
                        .unwrap_or(0),
                );
                let Some(queue_name) = annotation(QUEUE) else {
                    return;
                };
                let Some(Value::Long(message_id)) = m.body().map(|(_, v)| v) else {
                    return;
                };
                let Some(queue) = po.queue(&queue_name) else {
                    tracing::warn!(
                        "Queue {queue_name} not found on mirror target, ignoring ack for queue={queue_name}, messageID={message_id}, nodeID={}",
                        node_id.as_deref().unwrap_or("null")
                    );
                    return;
                };
                self.broker
                    .ack_manager()
                    .ack(
                        node_id.as_deref(),
                        &queue,
                        message_id,
                        reason,
                        self.origin(),
                    )
                    .await;
            }
            other => tracing::debug!("unknown mirror event {other}"),
        }
    }
}
