//! A queue's scheduled and delivering references, kept with running counts
//! and sizes as Artemis's `QueuePendingMessageMetrics` keeps them, so the
//! queue's statistics are read without walking its messages. The lists are
//! private here: every reference that joins or leaves one goes through a
//! method that counts it.

use std::collections::{BTreeMap, VecDeque};

use crate::message::MessageRef;

/// The count and persistent size of a set of references, with the durable
/// messages and those never paged counted apart.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Tally {
    pub count: i64,
    pub size: i64,
    pub durable_count: i64,
    pub durable_size: i64,
    /// Those never paged (a depaged one is still the page subscription's).
    pub non_paged_count: i64,
    pub non_paged_size: i64,
    pub non_paged_durable_count: i64,
    pub non_paged_durable_size: i64,
}

impl Tally {
    /// Counted by walking `refs`: what the running tally must equal.
    #[cfg(test)]
    pub fn of<'a>(refs: impl IntoIterator<Item = &'a MessageRef>) -> Self {
        let mut t = Self::default();
        refs.into_iter().for_each(|r| t.add(r));
        t
    }

    pub fn add(&mut self, r: &MessageRef) {
        self.apply(r, 1);
    }

    pub fn sub(&mut self, r: &MessageRef) {
        self.apply(r, -1);
    }

    fn apply(&mut self, r: &MessageRef, sign: i64) {
        let size = crate::paging::persistent_size(&r.message) * sign;
        let durable = r.message.is_durable();
        self.count += sign;
        self.size += size;
        if durable {
            self.durable_count += sign;
            self.durable_size += size;
        }
        if r.paged.is_none() {
            self.non_paged_count += sign;
            self.non_paged_size += size;
            if durable {
                self.non_paged_durable_count += sign;
                self.non_paged_durable_size += size;
            }
        }
    }

    pub fn plus(mut self, o: &Self) -> Self {
        self.count += o.count;
        self.size += o.size;
        self.durable_count += o.durable_count;
        self.durable_size += o.durable_size;
        self.non_paged_count += o.non_paged_count;
        self.non_paged_size += o.non_paged_size;
        self.non_paged_durable_count += o.non_paged_durable_count;
        self.non_paged_durable_size += o.non_paged_durable_size;
        self
    }
}

/// A consumer's references delivered and not yet acknowledged, oldest
/// first.
#[derive(Default)]
pub(super) struct Delivering {
    refs: VecDeque<MessageRef>,
    tally: Tally,
}

impl Delivering {
    pub fn push_back(&mut self, r: MessageRef) {
        self.tally.add(&r);
        self.refs.push_back(r);
    }

    pub fn push_front(&mut self, r: MessageRef) {
        self.tally.add(&r);
        self.refs.push_front(r);
    }

    pub fn remove(&mut self, i: usize) -> Option<MessageRef> {
        let r = self.refs.remove(i)?;
        self.tally.sub(&r);
        Some(r)
    }

    pub fn pop_front(&mut self) -> Option<MessageRef> {
        let r = self.refs.pop_front()?;
        self.tally.sub(&r);
        Some(r)
    }

    /// Takes every reference out, oldest first.
    pub fn take_all(&mut self) -> VecDeque<MessageRef> {
        self.tally = Tally::default();
        std::mem::take(&mut self.refs)
    }

    pub fn into_refs(self) -> VecDeque<MessageRef> {
        self.refs
    }

    pub fn iter(&self) -> std::collections::vec_deque::Iter<'_, MessageRef> {
        self.refs.iter()
    }

    pub fn len(&self) -> usize {
        self.refs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.refs.is_empty()
    }

    pub fn tally(&self) -> &Tally {
        &self.tally
    }
}

/// Where a scheduled reference waits: its delivery time, whether it is a
/// new message (after redeliveries due at the same time), and the order it
/// was scheduled in.
pub(super) type ScheduledKey = (i64, bool, u64);

/// The references held until their delivery time, earliest first.
#[derive(Default)]
pub(super) struct Scheduled {
    map: BTreeMap<ScheduledKey, MessageRef>,
    tally: Tally,
}

impl Scheduled {
    pub fn insert(&mut self, k: ScheduledKey, r: MessageRef) {
        self.tally.add(&r);
        if let Some(old) = self.map.insert(k, r) {
            self.tally.sub(&old);
        }
    }

    pub fn remove(&mut self, k: &ScheduledKey) -> Option<MessageRef> {
        let r = self.map.remove(k)?;
        self.tally.sub(&r);
        Some(r)
    }

    /// The earliest reference, if it is due at `now`.
    pub fn pop_due(&mut self, now: i64) -> Option<MessageRef> {
        let entry = self.map.first_entry()?;
        if entry.key().0 > now {
            return None;
        }
        let r = entry.remove();
        self.tally.sub(&r);
        Some(r)
    }

    /// Takes every reference out, earliest first.
    pub fn take_all(&mut self) -> Vec<MessageRef> {
        self.tally = Tally::default();
        std::mem::take(&mut self.map).into_values().collect()
    }

    pub fn get(&self, k: &ScheduledKey) -> Option<&MessageRef> {
        self.map.get(k)
    }

    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, ScheduledKey, MessageRef> {
        self.map.iter()
    }

    pub fn values(&self) -> std::collections::btree_map::Values<'_, ScheduledKey, MessageRef> {
        self.map.values()
    }

    pub fn keys(&self) -> std::collections::btree_map::Keys<'_, ScheduledKey, MessageRef> {
        self.map.keys()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn tally(&self) -> &Tally {
        &self.tally
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(id: i64, durable: bool, paged: bool) -> MessageRef {
        let m = crate::message::ServerMessage::new(saranga_message::CoreMessage {
            message_id: id,
            durable,
            ..Default::default()
        });
        let mut r = MessageRef::new(m);
        if paged {
            r.paged = Some(crate::message::PagePosition {
                page: 1,
                message_nr: id as i32,
                tx_id: -1,
                size: 0,
                ack_record: -1,
            });
        }
        r
    }

    #[test]
    fn delivering_counts_what_it_holds() {
        let mut d = Delivering::default();
        d.push_back(r(1, true, false));
        d.push_front(r(2, false, true));
        d.push_back(r(3, true, true));
        assert_eq!(*d.tally(), Tally::of(d.iter()));
        assert_eq!(d.tally().count, 3);
        assert_eq!(d.tally().durable_count, 2);
        assert_eq!(d.tally().non_paged_count, 1);
        d.remove(1);
        d.pop_front();
        assert_eq!(*d.tally(), Tally::of(d.iter()));
        assert_eq!(d.tally().count, 1);
        d.take_all();
        assert_eq!(*d.tally(), Tally::default());
    }

    #[test]
    fn scheduled_counts_what_it_holds() {
        let mut s = Scheduled::default();
        s.insert((10, false, 1), r(1, true, false));
        s.insert((20, true, 2), r(2, true, true));
        s.insert((30, true, 3), r(3, false, false));
        assert_eq!(*s.tally(), Tally::of(s.values()));
        assert!(s.pop_due(5).is_none());
        assert_eq!(s.pop_due(25).unwrap().message_id(), 1);
        s.remove(&(30, true, 3));
        assert_eq!(*s.tally(), Tally::of(s.values()));
        assert_eq!(s.tally().count, 1);
        // A key taken twice counts the reference it holds now.
        s.insert((20, true, 2), r(4, false, false));
        assert_eq!(*s.tally(), Tally::of(s.values()));
        s.take_all();
        assert_eq!(*s.tally(), Tally::default());
    }
}
