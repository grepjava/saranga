//! `AMQConsumer`: one OpenWire consumer on one destination, the prefetch
//! window that paces its deliveries, and the delivery sink the queue
//! hands its messages to (`AMQSession.sendMessage`, `handleDeliver`).

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use saranga_buffer::SimpleString;
use saranga_server::message::ServerMessage;
use saranga_server::queue::{DeliverySink, FORCED_DELIVERY_MESSAGE, Queue};

use crate::commands::{
    ConsumerId, DataStructure, Destination, Message, MessageDispatch, MessageId,
};
use crate::connection::Outbound;
use crate::convert::{self, DispatchTarget};
use crate::{WireFormat, lock};

/// `MessageUtil.CONNECTION_ID_PROPERTY_NAME`.
pub(crate) const CONNECTION_ID_PROPERTY: &str = "__AMQ_CID";

/// `RemoveInfo.LAST_DELIVERED_UNKNOWN`.
pub(crate) const LAST_DELIVERED_UNKNOWN: i64 = -2;

/// A message dispatched and not yet acknowledged: the delivering reference
/// as the consumer's acknowledgements name it.
#[derive(Debug, Clone)]
pub(crate) struct Delivered {
    pub id: MessageId,
    /// The Core message ID the queue knows it by.
    pub core: i64,
    /// The `brokerSequenceId` it went out with.
    pub seq: i64,
}

impl Delivered {
    /// `MessageId.equals`: the producer and its sequence.
    fn is(&self, id: &MessageId) -> bool {
        self.id.producer_id == id.producer_id
            && self.id.producer_sequence_id == id.producer_sequence_id
    }
}

/// `MessagePullHandler`.
struct Pull {
    next: i64,
    timeout: i64,
    /// `messagePullSequence`.
    sequence: i64,
    timer: Option<tokio::task::JoinHandle<()>>,
}

struct State {
    prefetch: i32,
    /// `currentWindow`.
    window: i32,
    /// `deliveredAcksCreditExtension`.
    extension: i32,
    pull: Option<Pull>,
    delivered: VecDeque<Delivered>,
    /// `rolledbackMessageRefs`, by Core message ID.
    rolled_back: HashSet<i64>,
    /// `deliveredSequenceId`.
    delivery_sequence: i64,
    last_delivered_sequence: i64,
    closed: bool,
}

pub(crate) struct AmqConsumer {
    /// The client's ID for it; the members of a composite consumer share
    /// it.
    pub id: ConsumerId,
    /// `openwireDestination`: the member, after virtual topic mapping.
    pub destination: Destination,
    /// The ID its core consumer has in the session.
    pub core_id: i64,
    pub browser: bool,
    /// Acknowledged as its messages go out (no-local advisory consumers).
    pub pre_ack: bool,
    /// `__AMQ_CID` comes off what it is sent: an advisory consumer of the
    /// connection's own session.
    strip_connection_id: bool,
    queue: OnceLock<Queue>,
    state: Mutex<State>,
    out: Arc<Outbound>,
    /// The connection made a no-local consumer (`noLocal`, sticky).
    no_local: Arc<AtomicBool>,
    node_id: String,
    /// The session's unnegotiated wire format, for conversion.
    wf: Mutex<WireFormat>,
    client: Arc<saranga_server::session::ClientConnection>,
}

pub(crate) struct NewConsumer {
    pub id: ConsumerId,
    pub destination: Destination,
    pub core_id: i64,
    pub prefetch: i32,
    pub browser: bool,
    pub pre_ack: bool,
    pub strip_connection_id: bool,
    pub out: Arc<Outbound>,
    pub no_local: Arc<AtomicBool>,
    pub node_id: String,
    pub client: Arc<saranga_server::session::ClientConnection>,
}

impl AmqConsumer {
    pub fn new(n: NewConsumer) -> Arc<Self> {
        Arc::new(Self {
            id: n.id,
            destination: n.destination,
            core_id: n.core_id,
            browser: n.browser,
            pre_ack: n.pre_ack,
            strip_connection_id: n.strip_connection_id,
            queue: OnceLock::new(),
            state: Mutex::new(State {
                prefetch: n.prefetch,
                window: n.prefetch,
                extension: 0,
                pull: (n.prefetch == 0).then(|| Pull {
                    next: -1,
                    timeout: 0,
                    sequence: 0,
                    timer: None,
                }),
                delivered: VecDeque::new(),
                rolled_back: HashSet::new(),
                delivery_sequence: 0,
                last_delivered_sequence: LAST_DELIVERED_UNKNOWN,
                closed: false,
            }),
            out: n.out,
            no_local: n.no_local,
            node_id: n.node_id,
            wf: Mutex::new(WireFormat::new(crate::codec::DEFAULT_WIRE_VERSION)),
            client: n.client,
        })
    }

    pub fn sink(self: &Arc<Self>) -> Arc<dyn DeliverySink> {
        Arc::new(Sink(self.clone()))
    }

    pub fn set_queue(&self, queue: Queue) {
        let _ = self.queue.set(queue);
    }

    pub fn queue(&self) -> Option<&Queue> {
        self.queue.get()
    }

    /// Whether its queue is temporary: an advisory subscription's.
    pub fn temporary_queue(&self) -> bool {
        self.queue.get().is_some_and(|q| q.config().temporary)
    }

    pub fn non_destructive(&self) -> bool {
        self.queue.get().is_some_and(|q| q.config().non_destructive)
    }

    pub fn set_last_delivered_sequence(&self, id: i64) {
        lock(&self.state).last_delivered_sequence = id;
    }

    /// `handleDeliverNullDispatch`: nothing more for a pull.
    fn null_dispatch(&self) {
        self.out
            .send_command(&DataStructure::MessageDispatch(MessageDispatch {
                consumer_id: Some(self.id.clone()),
                destination: Some(self.destination.clone()),
                ..Default::default()
            }));
    }

    /// `browseFinished`: the end of a browse.
    pub fn browse_finished(&self) {
        self.out
            .send_command(&DataStructure::MessageDispatch(MessageDispatch {
                consumer_id: Some(self.id.clone()),
                ..Default::default()
            }));
    }

    /// `replenishCredit`.
    pub fn replenish(&self, n: i32, delivered: bool) {
        let mut st = lock(&self.state);
        if st.pull.is_some() {
            return;
        }
        let mut n = n;
        if delivered {
            st.extension += n;
        } else if st.extension > 0 {
            if st.extension < n {
                n -= st.extension;
                st.extension = 0;
            } else {
                st.extension -= n;
                return;
            }
        }
        let old = st.window;
        st.window = st.window.saturating_add(n);
        let prompt = old < st.prefetch;
        drop(st);
        if prompt && let Some(q) = self.queue.get() {
            q.prompt_delivery();
        }
    }

    /// `processMessagePull`: one more credit, and for a prefetch-0
    /// consumer the sequence of the forced delivery that tells it the
    /// queue is empty.
    pub fn pull(&self, timeout: i64) -> Option<i64> {
        let mut st = lock(&self.state);
        st.window = st.window.saturating_add(1);
        let p = st.pull.as_mut()?;
        p.next = p.sequence;
        p.sequence += 1;
        p.timeout = timeout;
        Some(p.sequence)
    }

    /// `setPrefetchSize`: the window starts over.
    pub fn set_prefetch(&self, p: i32) {
        let mut st = lock(&self.state);
        st.prefetch = p;
        st.window = p;
        if p == 0 {
            st.pull.get_or_insert(Pull {
                next: -1,
                timeout: 0,
                sequence: 0,
                timer: None,
            });
        } else {
            st.pull = None;
        }
        drop(st);
        if p > 0
            && let Some(q) = self.queue.get()
        {
            q.prompt_delivery();
        }
    }

    /// `scanDeliveringReferences`: from the first message to the last,
    /// inclusive, in dispatch order; removed with `remove`.
    pub fn take_range(&self, first: &MessageId, last: &MessageId, remove: bool) -> Vec<Delivered> {
        let mut st = lock(&self.state);
        let Some(start) = st.delivered.iter().position(|d| d.is(first)) else {
            return Vec::new();
        };
        let end = st
            .delivered
            .iter()
            .skip(start)
            .position(|d| d.is(last))
            .map(|n| start + n + 1)
            .unwrap_or(st.delivered.len());
        if remove {
            let taken: Vec<Delivered> = st.delivered.drain(start..end).collect();
            for d in &taken {
                st.rolled_back.remove(&d.core);
            }
            taken
        } else {
            st.delivered.range(start..end).cloned().collect()
        }
    }

    /// `returnReferences`: acknowledged messages of a rolled back
    /// transaction are the consumer's again, first in line.
    pub fn back_to_delivering(&self, refs: Vec<Delivered>) {
        let mut st = lock(&self.state);
        for d in refs.into_iter().rev() {
            st.rolled_back.insert(d.core);
            st.delivered.push_front(d);
        }
    }

    /// Closes the consumer to deliveries and returns what it was
    /// delivering, last first, each with whether its delivery counts
    /// (`updateDeliveryCountAfterCancel`).
    pub fn close(&self) -> Vec<(i64, bool)> {
        let mut st = lock(&self.state);
        st.closed = true;
        if let Some(t) = st.pull.as_mut().and_then(|p| p.timer.take()) {
            t.abort();
        }
        let last = st.last_delivered_sequence;
        let delivered = std::mem::take(&mut st.delivered);
        delivered
            .into_iter()
            .rev()
            .map(|d| {
                let counts = last == LAST_DELIVERED_UNKNOWN
                    || (d.seq <= last && !st.rolled_back.contains(&d.core));
                (d.core, counts)
            })
            .collect()
    }

    /// `handleDeliver`.
    fn deliver(self: &Arc<Self>, message: &ServerMessage, delivery_count: i32) -> i64 {
        let core = message.delivery_core();
        let forced = core.properties.get_str(FORCED_DELIVERY_MESSAGE).is_some();
        let mut st = lock(&self.state);
        if st.closed {
            return 0;
        }
        // checkForcedConsumer
        if let Some(p) = st.pull.as_mut() {
            if forced {
                if p.next >= 0 {
                    if p.timeout <= 0 {
                        st.window = (st.window - 1).max(0);
                        drop(st);
                        self.null_dispatch();
                    } else {
                        let me = self.clone();
                        let timeout = Duration::from_millis(p.timeout as u64);
                        if let Some(t) = p.timer.take() {
                            t.abort();
                        }
                        p.timer = Some(tokio::spawn(async move {
                            tokio::time::sleep(timeout).await;
                            let mut st = lock(&me.state);
                            if !st.closed && st.pull.as_ref().is_some_and(|p| p.next >= 0) {
                                st.window = (st.window - 1).max(0);
                                drop(st);
                                me.null_dispatch();
                            }
                        }));
                    }
                }
                return 0;
            }
            p.next = -1;
            if let Some(t) = p.timer.take() {
                t.abort();
            }
        } else if forced {
            return 0;
        }
        st.rolled_back.remove(&message.message_id());
        let seq = st.delivery_sequence;
        st.delivery_sequence += 1;
        let stripped;
        let core = if self.no_local.load(Ordering::SeqCst) || self.strip_connection_id {
            let mut c = core.clone();
            c.properties
                .remove(&SimpleString::from(CONNECTION_ID_PROPERTY));
            stripped = c;
            &stripped
        } else {
            core
        };
        let target = DispatchTarget {
            consumer_id: &self.id,
            destination: &self.destination,
            server_node_id: &self.node_id,
            decoded_properties: false,
        };
        let dispatch =
            convert::message_dispatch(core, delivery_count, &target, &mut lock(&self.wf), seq);
        let mut dispatch = match dispatch {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!("Error during message dispatch: {e}");
                return 0;
            }
        };
        let Some(m) = dispatch.message.as_deref_mut() else {
            return 0;
        };
        m.broker_out_time = saranga_server::message::now_millis();
        let size = message_size(m);
        if !self.pre_ack
            && !self.browser
            && let Some(id) = m.message_id.clone()
        {
            st.delivered.push_back(Delivered {
                id,
                core: message.message_id(),
                seq,
            });
        }
        st.window = (st.window - 1).max(0);
        drop(st);
        self.out
            .send_command(&DataStructure::MessageDispatch(dispatch));
        size
    }

    /// A browser's message, from a snapshot of its queue.
    pub fn deliver_browsed(&self, message: &ServerMessage, delivery_count: i32) {
        let seq = {
            let mut st = lock(&self.state);
            st.delivery_sequence += 1;
            st.delivery_sequence - 1
        };
        let target = DispatchTarget {
            consumer_id: &self.id,
            destination: &self.destination,
            server_node_id: &self.node_id,
            decoded_properties: false,
        };
        match convert::message_dispatch(
            message.delivery_core(),
            delivery_count,
            &target,
            &mut lock(&self.wf),
            seq,
        ) {
            Ok(d) => {
                self.out.send_command(&DataStructure::MessageDispatch(d));
            }
            Err(e) => tracing::warn!("Error during message dispatch: {e}"),
        }
    }
}

/// `Message.getSize()` of a message the broker made.
pub(crate) fn message_size(m: &Message) -> i64 {
    1024 + m.marshalled_properties.as_ref().map_or(0, Vec::len) as i64
        + m.content.as_ref().map_or(0, Vec::len) as i64
}

struct Sink(Arc<AmqConsumer>);

impl DeliverySink for Sink {
    fn deliver(&self, _consumer_id: i64, message: &Arc<ServerMessage>, delivery_count: i32) -> i64 {
        self.0.deliver(message, delivery_count)
    }

    fn deliver_large(
        &self,
        _consumer_id: i64,
        _message: &ServerMessage,
        _body_size: i64,
        _delivery_count: i32,
    ) -> i64 {
        0
    }

    fn deliver_continuation(&self, _consumer_id: i64, _chunk: Vec<u8>, _continues: bool) -> i64 {
        0
    }

    /// `AMQSession.disconnect`: fatal for an OpenWire consumer, whose
    /// binding would be in limbo; the client is told and the connection
    /// fails, so it can fail over and subscribe again.
    fn disconnect(&self, _consumer_id: i64, reason: &str) {
        self.0
            .out
            .send(&crate::connection::io_connection_error(reason));
        self.0.client.fail();
    }

    fn chunk_size(&self) -> usize {
        usize::MAX
    }

    fn supports_large(&self) -> bool {
        false
    }

    /// `AMQConsumer.hasCredits`.
    fn has_credits(&self, _message: &ServerMessage) -> bool {
        let st = lock(&self.0.state);
        !st.closed && st.window > 0
    }
}
