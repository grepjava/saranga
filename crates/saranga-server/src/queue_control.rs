//! What management does to a queue (`QueueControlImpl` over `QueueImpl`):
//! statistics, listing messages, and acting on the messages a filter or an
//! ID selects (`iterQueue`). Each operation runs in the queue's actor.

use super::*;
use crate::paging::PageIterator;

/// The statistics a queue works out from its lists (`QueueImpl`'s
/// `pendingMetrics`, `deliveringMetrics` and scheduled handler). Sizes
/// are persistent sizes (`getPersistentSize`).
#[derive(Debug, Clone, Default)]
pub struct QueueStats {
    pub delivering_count: i32,
    pub delivering_size: i64,
    pub durable_delivering_count: i32,
    pub durable_delivering_size: i64,
    pub scheduled_count: i64,
    pub scheduled_size: i64,
    pub durable_scheduled_count: i64,
    pub durable_scheduled_size: i64,
    /// Queued (neither delivering nor scheduled), paged ones aside.
    pub pending_size: i64,
    pub durable_pending_count: i64,
    pub durable_pending_size: i64,
    pub group_count: i32,
}

/// A consumer as the queue knows it, for `listConsumersAsJSON`.
#[derive(Debug, Clone)]
pub struct ConsumerInfo {
    pub id: ConsumerId,
    pub client_id: i64,
    pub browse_only: bool,
    pub priority: i32,
    pub delivering: usize,
    pub delivering_size: i64,
    pub filter: Option<String>,
}

/// Which messages an operation works on (`QueueIterateAction`): those a
/// filter accepts, or one message by ID, up to a number of hits.
#[derive(Debug, Clone, Default)]
pub struct Selection {
    pub filter: Option<FilterRef>,
    pub message_id: Option<i64>,
    pub max: Option<usize>,
}

impl Selection {
    pub fn filter(filter: Option<FilterRef>) -> Self {
        Self {
            filter,
            ..Self::default()
        }
    }

    pub fn id(message_id: i64) -> Self {
        Self {
            message_id: Some(message_id),
            max: Some(1),
            ..Self::default()
        }
    }

    fn matches(&self, r: &MessageRef) -> bool {
        match self.message_id {
            Some(id) => r.message_id() == id,
            None => self.filter.as_ref().is_none_or(|f| f.matches(&*r.message)),
        }
    }

    fn reached(&self, hits: usize) -> bool {
        self.max.is_some_and(|m| hits >= m)
    }
}

/// What an operation does with each selected message.
#[derive(Debug, Clone)]
pub enum SelectionAction {
    /// Acknowledged (`deleteReference` with `NORMAL`,
    /// `deleteMatchingReferences` with `KILLED`).
    Remove(AckReason),
    /// `expireReferences`.
    Expire,
    /// `sendToDeadLetterAddress`.
    SendToDeadLetter,
    /// `moveReferences`: a copy goes to `address` (only to `target` when
    /// given) and the message is acknowledged. With `reject_duplicates` a
    /// message whose duplicate ID the address has seen is only
    /// acknowledged.
    Move {
        address: String,
        target: Option<Queue>,
        reject_duplicates: bool,
    },
    /// `copyReference`: a copy goes to `target`; the message stays.
    Copy { address: String, target: Queue },
    /// `retryMessages`: a message from a dead-letter or expiry queue goes
    /// back to its original address and queue.
    Retry,
    /// `changeReferencesPriority`.
    ChangePriority(u8),
    /// `deliverScheduledMessages`: scheduled messages are delivered now.
    DeliverScheduled,
}

impl MessageList {
    /// Takes out, in delivery order, the references `sel` selects that
    /// `take` wants, counting every selected one in `hits`.
    fn take_selected(
        &mut self,
        sel: &Selection,
        hits: &mut usize,
        mut take: impl FnMut(&MessageRef) -> bool,
    ) -> Vec<MessageRef> {
        let mut out = Vec::new();
        for level in self.levels.iter_mut().rev() {
            if sel.reached(*hits) {
                break;
            }
            let mut keep = VecDeque::with_capacity(level.len());
            for r in level.drain(..) {
                if !sel.reached(*hits) && sel.matches(&r) {
                    *hits += 1;
                    if take(&r) {
                        out.push(r);
                        continue;
                    }
                }
                keep.push_back(r);
            }
            *level = keep;
        }
        out
    }
}

impl Queue {
    async fn manage<T: Send + 'static>(
        &self,
        op: impl FnOnce(&mut Actor) -> T + Send + 'static,
    ) -> Option<T> {
        let (reply, wait) = oneshot::channel();
        self.send(Cmd::Manage(Box::new(move |actor| {
            let _ = reply.send(op(actor));
        })));
        wait.await.ok()
    }

    pub fn messages_added(&self) -> i64 {
        self.0.messages_added.load(Ordering::SeqCst)
    }

    pub fn messages_acknowledged(&self) -> i64 {
        self.0.messages_acknowledged.load(Ordering::SeqCst)
    }

    pub fn messages_expired(&self) -> i64 {
        self.0.messages_expired.load(Ordering::SeqCst)
    }

    pub fn messages_killed(&self) -> i64 {
        self.0.messages_killed.load(Ordering::SeqCst)
    }

    pub fn messages_replaced(&self) -> i64 {
        self.0.messages_replaced.load(Ordering::SeqCst)
    }

    /// A message was routed to the queue's pages: it counts as added.
    pub fn paged_added(&self) {
        self.0.messages_added.fetch_add(1, Ordering::SeqCst);
    }

    pub fn reset_messages_added(&self) {
        self.0.messages_added.store(0, Ordering::SeqCst);
    }

    pub fn reset_messages_acknowledged(&self) {
        self.0.messages_acknowledged.store(0, Ordering::SeqCst);
    }

    pub fn reset_messages_expired(&self) {
        self.0.messages_expired.store(0, Ordering::SeqCst);
    }

    pub fn reset_messages_killed(&self) {
        self.0.messages_killed.store(0, Ordering::SeqCst);
    }

    /// `isPaused`: the queue or its address is paused.
    pub fn is_paused(&self) -> bool {
        self.0.paused.load(Ordering::SeqCst) || self.0.address_paused.load(Ordering::SeqCst)
    }

    /// `isPersistedPause`.
    pub fn is_persisted_pause(&self) -> bool {
        self.0.pause_record.load(Ordering::SeqCst) >= 0
    }

    /// `pause(persist)`: delivery stops. A durable queue paused with
    /// `persist` stays paused over a restart (`storeQueueStatus`).
    pub fn pause(&self, persist: bool) {
        if persist
            && self.is_durable()
            && let Some(s) = self.0.ids.storage()
        {
            let old = self.0.pause_record.load(Ordering::SeqCst);
            if old >= 0
                && let Err(e) = s.delete_binding_record(old)
            {
                tracing::warn!("AMQ222251: Unable to pause a queue: {e}");
            }
            match s.store_queue_status(self.0.id) {
                Ok(record) => self.0.pause_record.store(record, Ordering::SeqCst),
                Err(e) => tracing::warn!("AMQ222251: Unable to pause a queue: {e}"),
            }
        }
        self.0.paused.store(true, Ordering::SeqCst);
    }

    /// A pause recovered from its `QUEUE_STATUS_RECORD`.
    pub fn reload_pause(&self, record: i64) {
        self.0.pause_record.store(record, Ordering::SeqCst);
        self.0.paused.store(true, Ordering::SeqCst);
    }

    /// `resume`: delivery starts again, and a persisted pause is forgotten.
    pub fn resume(&self) {
        self.0.paused.store(false, Ordering::SeqCst);
        let old = self.0.pause_record.swap(-1, Ordering::SeqCst);
        if old >= 0
            && let Some(s) = self.0.ids.storage()
            && let Err(e) = s.delete_binding_record(old)
        {
            tracing::warn!("AMQ222252: Unable to resume a queue: {e}");
        }
        self.prompt_delivery();
    }

    /// Its address was paused or resumed.
    pub fn set_address_paused(&self, paused: bool) {
        self.0.address_paused.store(paused, Ordering::SeqCst);
        if !paused {
            self.prompt_delivery();
        }
    }

    /// The statistics the queue works out from its lists.
    pub async fn stats(&self) -> QueueStats {
        self.manage(|a| a.stats()).await.unwrap_or_default()
    }

    /// `peekFirstMessage`: the next message to deliver.
    pub async fn peek_first(&self) -> Option<MessageRef> {
        self.manage(|a| a.messages.iter().next().cloned())
            .await
            .flatten()
    }

    /// `peekFirstScheduledMessage`.
    pub async fn peek_first_scheduled(&self) -> Option<MessageRef> {
        self.manage(|a| a.scheduled.values().next().cloned())
            .await
            .flatten()
    }

    /// `getScheduledMessages`.
    pub async fn scheduled_messages(&self) -> Vec<MessageRef> {
        self.manage(|a| a.scheduled.values().cloned().collect())
            .await
            .unwrap_or_default()
    }

    /// `getDeliveringMessages`: each consumer's delivering messages.
    pub async fn delivering_messages(&self) -> Vec<(ConsumerInfo, Vec<MessageRef>)> {
        self.manage(|a| {
            a.consumers
                .iter()
                .filter(|c| !c.delivering.is_empty())
                .map(|c| (consumer_info(c), c.delivering.iter().cloned().collect()))
                .collect()
        })
        .await
        .unwrap_or_default()
    }

    /// The queue's consumers, browsers last.
    pub async fn consumers(&self) -> Vec<ConsumerInfo> {
        self.manage(|a| {
            a.consumers
                .iter()
                .chain(a.browsers.iter())
                .map(consumer_info)
                .collect()
        })
        .await
        .unwrap_or_default()
    }

    /// Tells each consumer's client its consumer is gone
    /// (`ServerConsumer.disconnect`).
    pub async fn disconnect_consumers(&self) {
        let sinks = self
            .manage(|a| {
                a.consumers
                    .iter()
                    .chain(a.browsers.iter())
                    .map(|c| (c.reg.sink.clone(), c.reg.client_id))
                    .collect::<Vec<_>>()
            })
            .await
            .unwrap_or_default();
        let reason = format!("Queue deleted: {}", self.name());
        for (sink, id) in sinks {
            sink.disconnect(id, &reason);
        }
    }

    /// `getGroups`: each group and the consumer it is pinned to.
    pub async fn groups(&self) -> Vec<(String, ConsumerId)> {
        self.manage(|a| {
            a.groups
                .iter()
                .map(|(k, c)| {
                    let k = match k {
                        GroupKey::Id(id) => String::from_utf8_lossy(id).into_owned(),
                        GroupKey::Bucket(b) => b.to_string(),
                    };
                    (k, *c)
                })
                .collect()
        })
        .await
        .unwrap_or_default()
    }

    /// `resetAllGroups`, or `resetGroup` for one.
    pub async fn reset_groups(&self, group: Option<String>) {
        self.manage(move |a| match group {
            None => a.groups.clear(),
            Some(g) => {
                a.groups.remove(&GroupKey::Id(g.into_bytes()));
            }
        })
        .await;
        self.prompt_delivery();
    }

    /// Acts on the messages `sel` selects: the queued ones, the scheduled
    /// ones and those still in pages (`iterQueue`). Returns how many were
    /// selected.
    pub async fn act(&self, sel: Selection, action: SelectionAction) -> usize {
        let n = self.manage(move |a| a.act(&sel, &action)).await;
        self.prompt_delivery();
        n.unwrap_or(0)
    }
}

fn consumer_info(c: &Consumer) -> ConsumerInfo {
    ConsumerInfo {
        id: c.reg.id,
        client_id: c.reg.client_id,
        browse_only: c.reg.browse_only,
        priority: c.reg.priority,
        delivering: c.delivering.len(),
        delivering_size: c
            .delivering
            .iter()
            .map(|r| crate::paging::persistent_size(&r.message))
            .sum(),
        filter: c.reg.filter.as_ref().map(|f| f.text().to_owned()),
    }
}

impl Actor {
    pub(super) fn is_paused(&self) -> bool {
        self.queue().is_paused()
    }

    fn stats(&self) -> QueueStats {
        let queue = self.queue();
        let size = |r: &MessageRef| crate::paging::persistent_size(&r.message);
        let durable = |r: &MessageRef| queue.is_durable() && r.message.is_durable();
        let mut s = QueueStats::default();
        for r in self.consumers.iter().flat_map(|c| c.delivering.iter()) {
            s.delivering_count += 1;
            s.delivering_size += size(r);
            if durable(r) {
                s.durable_delivering_count += 1;
                s.durable_delivering_size += size(r);
            }
        }
        for r in self.scheduled.values() {
            s.scheduled_count += 1;
            s.scheduled_size += size(r);
            if durable(r) {
                s.durable_scheduled_count += 1;
                s.durable_scheduled_size += size(r);
            }
        }
        for r in self.messages.iter().filter(|r| r.paged.is_none()) {
            s.pending_size += size(r);
            if durable(r) {
                s.durable_pending_count += 1;
                s.durable_pending_size += size(r);
            }
        }
        s.group_count = self.groups.len() as i32;
        s
    }

    fn act(&mut self, sel: &Selection, action: &SelectionAction) -> usize {
        if !matches!(action, SelectionAction::Remove(_)) || !crate::audit::message_enabled() {
            return self.act_unaudited(sel, action);
        }
        // `iterQueue`: the removals' acknowledgements, in a transaction.
        self.removing = Some(Vec::new());
        let hits = self.act_unaudited(sel, action);
        let removed = self.removing.take().unwrap_or_default();
        if !removed.is_empty() {
            let messages: Vec<&ServerMessage> = removed.iter().map(|m| &**m).collect();
            let tx_id = self.shared.ids.next();
            crate::audit::removed(&self.shared.name, &messages, tx_id);
        }
        hits
    }

    fn act_unaudited(&mut self, sel: &Selection, action: &SelectionAction) -> usize {
        // `isExpiryDisabled`: a queue on its own expiry address keeps its
        // messages.
        if matches!(action, SelectionAction::Expire) {
            let s = self.queue().settings();
            if s.expiry_address.as_deref() == Some(&*self.shared.config().address) {
                return 0;
            }
        }
        // `changeReferencesPriority`: the message changes in place and keeps
        // its position.
        if let SelectionAction::ChangePriority(p) = action {
            let mut hits = 0;
            for level in self.messages.levels.iter_mut().rev() {
                for r in level.iter_mut() {
                    if sel.reached(hits) {
                        return hits;
                    }
                    if sel.matches(r) {
                        hits += 1;
                        let mut core = r.message.core().clone();
                        core.priority = *p;
                        r.message = r.message.with_core(core);
                    }
                }
            }
            return hits;
        }
        let mut hits = 0;
        let mut copies = Vec::new();
        let po = self.shared.post_office.upgrade();
        let retryable = |r: &MessageRef| retry_target(po.as_deref(), r).is_some();
        let taken = match action {
            SelectionAction::DeliverScheduled => Vec::new(),
            SelectionAction::Copy { .. } => self.messages.take_selected(sel, &mut hits, |r| {
                copies.push(r.clone());
                false
            }),
            SelectionAction::Retry => self.messages.take_selected(sel, &mut hits, retryable),
            _ => self.messages.take_selected(sel, &mut hits, |_| true),
        };
        for r in taken {
            self.act_on(r, action);
        }
        for r in copies {
            self.act_on(r, action);
        }
        // Scheduled messages (`scheduledDeliveryHandler.cancel`).
        let scheduled_too = !matches!(
            action,
            SelectionAction::Expire
                | SelectionAction::ChangePriority(_)
                | SelectionAction::Copy { .. }
        );
        if scheduled_too && !sel.reached(hits) {
            let keys: Vec<_> = self
                .scheduled
                .iter()
                .filter(|(_, r)| sel.matches(r))
                .map(|(k, r)| (*k, retryable(r)))
                .collect();
            for (k, can_retry) in keys {
                if sel.reached(hits) {
                    break;
                }
                hits += 1;
                if matches!(action, SelectionAction::Retry) && !can_retry {
                    continue;
                }
                let Some(mut r) = self.scheduled.remove(&k) else {
                    continue;
                };
                if matches!(action, SelectionAction::DeliverScheduled) {
                    r.scheduled_time = 0;
                    self.messages.push_front(r);
                } else {
                    r.scheduled_time = 0;
                    self.act_on(r, action);
                }
            }
        }
        if matches!(action, SelectionAction::DeliverScheduled) {
            return hits;
        }
        // Messages still in pages, through an iterator of the operation's
        // own; those already read into memory were seen above.
        if let Some(sub) = self.shared.paging.clone()
            && let Some(store) = sub.store()
            && !sel.reached(hits)
        {
            let in_memory: HashSet<(i64, i32)> = self
                .messages
                .iter()
                .chain(self.scheduled.values())
                .chain(self.consumers.iter().flat_map(|c| c.delivering.iter()))
                .filter_map(|r| r.paged.as_ref().map(|p| (p.page, p.message_nr)))
                .collect();
            let mut it = PageIterator::default();
            let mut found = Vec::new();
            while !sel.reached(hits) {
                let Some(p) = store.scan(&sub, &mut it) else {
                    break;
                };
                if in_memory.contains(&(p.position.page, p.position.message_nr)) {
                    continue;
                }
                let mut r = MessageRef::new(p.message);
                r.paged = Some(p.position);
                if !sel.matches(&r) {
                    continue;
                }
                hits += 1;
                match action {
                    SelectionAction::Retry if !retryable(&r) => continue,
                    SelectionAction::Copy { .. } => {
                        self.act_on(r, action);
                        continue;
                    }
                    _ => {}
                }
                found.push(r);
            }
            for r in found {
                // Counted as read in, so its acknowledgement balances.
                if let Some(p) = &r.paged {
                    sub.read_in(p.size);
                }
                self.act_on(r, action);
            }
        }
        hits
    }

    fn reprioritised(&self, mut r: MessageRef, action: &SelectionAction) -> MessageRef {
        if let SelectionAction::ChangePriority(p) = action {
            let mut core = r.message.core().clone();
            core.priority = *p;
            r.message = r.message.with_core(core);
        }
        r
    }

    /// Does `action` to one reference taken off the queue (or, for a copy,
    /// still on it).
    fn act_on(&mut self, r: MessageRef, action: &SelectionAction) {
        match action {
            SelectionAction::Remove(reason) => {
                if let Some(removed) = &mut self.removing {
                    removed.push(r.message.clone());
                }
                self.kill_for(r, *reason)
            }
            SelectionAction::Expire => self.expire(r),
            SelectionAction::SendToDeadLetter => {
                let s = self.queue().settings();
                self.send_to_dead_letter_address(r, &s);
            }
            SelectionAction::Move {
                address,
                target,
                reject_duplicates,
            } => {
                if *reject_duplicates
                    && let Some(id) = crate::duplicate::duplicate_id_bytes(r.message.core())
                    && let Some(po) = self.shared.post_office.upgrade()
                {
                    let cache = po.duplicate_cache(address);
                    let mut c = crate::duplicate::lock(&cache);
                    if c.contains(&id) {
                        drop(c);
                        tracing::warn!(
                            "AMQ222059: Duplicate message detected - message will not be routed. Message information:\n{}",
                            r.message_id()
                        );
                        self.kill_for(r, AckReason::Normal);
                        return;
                    }
                    let record = po.ids().next();
                    c.add_in_memory(id, record);
                }
                self.move_to(address, r, AckReason::Normal, target.clone());
            }
            SelectionAction::Copy { address, target } => self.copy_to(address, &r, target),
            SelectionAction::Retry => {
                let po = self.shared.post_office.upgrade();
                match retry_target(po.as_deref(), &r) {
                    Some((address, target)) => {
                        self.move_to(&address, r, AckReason::Normal, target);
                    }
                    None => self.add_sorted(r),
                }
            }
            SelectionAction::ChangePriority(_) => {
                let r = self.reprioritised(r, action);
                self.messages.insert_sorted(r);
            }
            SelectionAction::DeliverScheduled => self.messages.push_front(r),
        }
    }

    /// `copyReference`: a copy of the message goes to `target`.
    fn copy_to(&mut self, address: &str, r: &MessageRef, target: &Queue) {
        let now = now_millis();
        let copy = message::make_copy(
            r.message.core(),
            self.shared.ids.next(),
            address,
            &self.shared.name,
            false,
            now,
        );
        let copy = MessageRef::new(r.message.copy(copy));
        if target.is_durable_ref(&copy)
            && let Some(s) = self.shared.ids.storage()
        {
            copy.message.durable_up();
            let tx = self.shared.ids.next();
            let written = s
                .store_message_tx(tx, &copy.message)
                .and_then(|()| s.store_reference_tx(tx, target.id(), copy.message_id()))
                .and_then(|()| s.commit(tx).map(|_| ()));
            if let Err(e) = written {
                tracing::warn!("copying message {} to {address}: {e}", r.message_id());
            }
        }
        target.add(copy);
    }
}

/// Where `retryMessages` sends a message: its original address, and its
/// original queue when that is not named as the address. `None` when the
/// message has no original queue that still exists.
fn retry_target(po: Option<&PostOffice>, r: &MessageRef) -> Option<(String, Option<Queue>)> {
    let po = po?;
    let core = r.message.core();
    let address = message::string_property(core, message::HDR_ORIGINAL_ADDRESS)?;
    let queue_name = message::string_property(core, message::HDR_ORIGINAL_QUEUE)?;
    let queue = po.queue(&queue_name)?;
    let target = (queue_name != address).then_some(queue);
    Some((address, target))
}
