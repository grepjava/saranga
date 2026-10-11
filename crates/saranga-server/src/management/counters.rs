//! Message counters (`MessageCounter`, `MessageCounterManagerImpl` and
//! `MessageCounterHelper`): each queue's message count sampled every
//! sample period while `message-counter-enabled`, with an hourly history
//! of the messages added, kept for a number of days.
//!
//! Artemis makes a queue's counter when the queue is registered; Saranga
//! makes it the first time the sampler or management looks for it, which
//! counts the same, as a counter starts from the queue's first message
//! either way.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use super::Json;
use super::transactions::{short_date, short_medium};
use crate::Broker;
use crate::message::now_millis;
use crate::queue::Queue;

/// `MessageCounterManagerImpl.MIN_SAMPLE_PERIOD`.
pub const MIN_SAMPLE_PERIOD: i64 = 1000;

const MILLIS_PER_DAY: i64 = 86_400_000;
const HOURS: usize = 24;

/// The message counter manager.
pub struct MessageCounters {
    enabled: AtomicBool,
    sample_period: AtomicI64,
    max_day_count: AtomicI32,
    counters: Mutex<HashMap<i64, Arc<Mutex<MessageCounter>>>>,
    sampler: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl MessageCounters {
    pub fn new(enabled: bool, sample_period: i64, max_day_count: i32) -> Self {
        Self {
            enabled: AtomicBool::new(enabled),
            sample_period: AtomicI64::new(sample_period),
            max_day_count: AtomicI32::new(max_day_count),
            counters: Mutex::new(HashMap::new()),
            sampler: Mutex::new(None),
        }
    }

    /// `isMessageCounterEnabled`.
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn sample_period(&self) -> i64 {
        self.sample_period.load(Ordering::Relaxed)
    }

    pub fn max_day_count(&self) -> i32 {
        self.max_day_count.load(Ordering::Relaxed)
    }

    /// `setMaxDayCount`: for counters made from now on.
    pub fn set_max_day_count(&self, count: i32) {
        self.max_day_count.store(count, Ordering::Relaxed);
    }

    /// `setMessageCounterEnabled`: starting the sampler, or stopping it and
    /// resetting every counter and its history.
    pub fn set_enabled(&self, broker: &Arc<Broker>, enable: bool) {
        let was = self.enabled.swap(enable, Ordering::Relaxed);
        if was && !enable {
            self.stop();
            for c in self.all(broker) {
                let mut c = lock(&c);
                c.reset_counter();
                c.reset_history();
            }
        } else if !was && enable {
            self.start(broker);
        }
    }

    /// `MessageCountersPinger.start`: a sample every period, the first a
    /// period from now.
    pub fn start(&self, broker: &Arc<Broker>) {
        let period = Duration::from_millis(self.sample_period().max(1) as u64);
        let weak: Weak<Broker> = Arc::downgrade(broker);
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(broker) = weak.upgrade() else { break };
                let counters = broker.management().counters();
                for c in counters.all(&broker) {
                    lock(&c).on_timer();
                }
            }
        });
        if let Some(old) = lock(&self.sampler).replace(task) {
            old.abort();
        }
    }

    pub fn stop(&self) {
        if let Some(task) = lock(&self.sampler).take() {
            task.abort();
        }
    }

    /// `reschedule`: the new period, from now if the sampler runs.
    pub fn reschedule(&self, broker: &Arc<Broker>, period: i64) {
        self.sample_period.store(period, Ordering::Relaxed);
        if lock(&self.sampler).is_some() {
            self.start(broker);
        }
    }

    /// Runs `f` on a queue's counter.
    pub fn with<R>(&self, q: &Queue, f: impl FnOnce(&mut MessageCounter) -> R) -> R {
        let c = self.get(q);
        f(&mut lock(&c))
    }

    /// A queue's counter.
    fn get(&self, q: &Queue) -> Arc<Mutex<MessageCounter>> {
        let max = self.max_day_count();
        lock(&self.counters)
            .entry(q.id())
            .or_insert_with(|| Arc::new(Mutex::new(MessageCounter::new(q.clone(), max))))
            .clone()
    }

    /// Every queue's counter; those of deleted queues are dropped.
    pub fn all(&self, broker: &Broker) -> Vec<Arc<Mutex<MessageCounter>>> {
        let queues = broker.post_office().queues();
        let max = self.max_day_count();
        let mut counters = lock(&self.counters);
        let live: HashMap<i64, &Queue> = queues.iter().map(|q| (q.id(), q)).collect();
        counters.retain(|id, _| live.contains_key(id));
        live.iter()
            .map(|(id, q)| {
                counters
                    .entry(*id)
                    .or_insert_with(|| Arc::new(Mutex::new(MessageCounter::new((*q).clone(), max))))
                    .clone()
            })
            .collect()
    }

    /// `resetAllCounters`.
    pub fn reset_all_counters(&self, broker: &Broker) {
        for c in self.all(broker) {
            lock(&c).reset_counter();
        }
    }

    /// `resetAllCounterHistories`.
    pub fn reset_all_histories(&self, broker: &Broker) {
        for c in self.all(broker) {
            lock(&c).reset_history();
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// `MessageCounter`, of a queue (never a topic subscription).
pub struct MessageCounter {
    queue: Queue,
    count_total: i64,
    count_total_last: i64,
    depth_last: i64,
    time_last_update: i64,
    time_last_add: i64,
    time_last_ack: i64,
    day_counter_max: i32,
    day_counters: Vec<DayCounter>,
    last_messages_added: i64,
    last_messages_acked: i64,
}

impl MessageCounter {
    fn new(queue: Queue, day_counter_max: i32) -> Self {
        let mut c = Self {
            queue,
            count_total: 0,
            count_total_last: 0,
            depth_last: 0,
            time_last_update: 0,
            time_last_add: 0,
            time_last_ack: 0,
            day_counter_max: 0,
            day_counters: Vec::new(),
            last_messages_added: 0,
            last_messages_acked: 0,
        };
        c.set_history_limit(day_counter_max);
        c
    }

    /// `onTimer`: the messages added and acknowledged since the last sample.
    fn on_timer(&mut self) {
        let added = self.queue.messages_added();
        let acked = self.queue.messages_acknowledged();
        let new_added = added - self.last_messages_added;
        let new_acked = acked - self.last_messages_acked;
        self.count_total += new_added;
        self.last_messages_added = added;
        self.last_messages_acked = acked;
        let now = now_millis();
        if new_added > 0 {
            self.time_last_add = now;
        }
        if new_acked > 0 {
            self.time_last_ack = now;
        }
        self.time_last_update = now;
        self.update_history(new_added);
    }

    /// `getCountDelta`: the count since the last time it was asked.
    fn count_delta(&mut self) -> i64 {
        let delta = self.count_total - self.count_total_last;
        self.count_total_last = self.count_total;
        delta
    }

    /// `getMessageCountDelta`: the depth's change since the last time it
    /// was asked, as an `int`.
    fn message_count_delta(&mut self) -> i64 {
        let current = self.queue.message_count();
        let delta = (current - self.depth_last) as i32;
        self.depth_last = current;
        i64::from(delta)
    }

    /// `resetCounter`.
    pub fn reset_counter(&mut self) {
        self.count_total = 0;
        self.count_total_last = 0;
        self.depth_last = 0;
        self.time_last_update = 0;
        self.time_last_add = 0;
        self.time_last_ack = 0;
    }

    /// `setHistoryLimit`.
    fn set_history_limit(&mut self, max: i32) {
        self.day_counter_max = max;
        let initialize = if max > 0 {
            let excess = self.day_counters.len().saturating_sub(max as usize);
            self.day_counters.drain(..excess);
            self.day_counters.is_empty()
        } else if max == 0 {
            self.day_counters.clear();
            false
        } else {
            self.day_counters.is_empty()
        };
        if initialize {
            let now = now_millis();
            self.day_counters.push(DayCounter::new(
                now.div_euclid(MILLIS_PER_DAY),
                hour_of(now),
                true,
            ));
        }
    }

    /// `resetHistory`: one day counter, from now.
    pub fn reset_history(&mut self) {
        let max = self.day_counter_max;
        self.set_history_limit(0);
        self.set_history_limit(max);
    }

    /// `updateHistory`: a counter for each day since the last, then the new
    /// messages in the current hour.
    fn update_history(&mut self, new_messages: i64) {
        if self.day_counters.is_empty() {
            return;
        }
        let now = now_millis();
        let today = now.div_euclid(MILLIS_PER_DAY);
        let last_day = self.day_counters.last().map_or(today, |c| c.day);
        let delta = today - last_day;
        if delta > 0 {
            if let Some(last) = self.day_counters.last_mut() {
                last.finalize();
            }
            for i in 1..delta {
                let mut c = DayCounter::new(last_day + i, 0, false);
                c.finalize();
                self.day_counters.push(c);
            }
            // The calendar has been cleared to midnight.
            self.day_counters.push(DayCounter::new(today, 0, false));
            self.set_history_limit(self.day_counter_max);
        }
        if let Some(last) = self.day_counters.last_mut() {
            last.update(hour_of(now), new_messages);
        }
    }

    fn history(&mut self) -> Vec<DayCounter> {
        self.update_history(0);
        self.day_counters.clone()
    }

    /// `toJSon`, which takes the deltas (`getCountDelta` and
    /// `getMessageCountDelta` reset them).
    pub fn take_json(&mut self) -> Json {
        let (count_delta, depth_delta) = (self.count_delta(), self.message_count_delta());
        Json::object()
            .with("destinationName", self.queue.name().to_string())
            .with("destinationSubscription", Json::Null)
            .with("destinationDurable", self.queue.is_durable())
            .with("count", self.count_total)
            .with("countDelta", count_delta)
            .with("messageCount", self.queue.message_count())
            .with("messageCountDelta", depth_delta)
            .with("lastAddTimestamp", short_medium(self.time_last_add))
            .with("lastAckTimestamp", short_medium(self.time_last_ack))
            .with("updateTimestamp", short_medium(self.time_last_update))
    }

    /// `MessageCounterHelper.listMessageCounterHistory`.
    pub fn history_json(&mut self) -> Json {
        let days = self
            .history()
            .into_iter()
            .map(|d| {
                Json::object()
                    .with("date", short_date(d.day * MILLIS_PER_DAY))
                    .with(
                        "counters",
                        Json::Array(d.counters.iter().map(|&c| Json::from(c)).collect()),
                    )
            })
            .collect();
        Json::object().with("dayCounters", Json::Array(days))
    }

    /// `MessageCounterHelper.listMessageCounterAsHTML` of this counter.
    pub fn take_html(&mut self) -> String {
        let prettify = |v: i64| {
            if v == 0 {
                "-".to_owned()
            } else {
                v.to_string()
            }
        };
        let as_date = |t: i64| {
            if t > 0 {
                short_medium(t)
            } else {
                "-".to_owned()
            }
        };
        let mut s = String::from(
            "<table class=\"activemq-message-counter\">\n<tr><th>Type</th><th>Name</th><th>Subscription</th><th>Durable</th><th>Count</th><th>CountDelta</th><th>Depth</th><th>DepthDelta</th><th>Last Add</th><th>Last Ack</th><th>Last Update</th></tr>\n",
        );
        s.push_str("<tr bgcolor=\"#FFFFFF\">");
        s.push_str("<td>Queue</td>");
        s.push_str(&format!("<td>{}</td>", self.queue.name()));
        s.push_str("<td>-</td>");
        s.push_str("<td>-</td>");
        s.push_str(&format!("<td>{}</td>", self.count_total));
        let count_delta = self.count_delta();
        s.push_str(&format!("<td>{}</td>", prettify(count_delta)));
        s.push_str(&format!(
            "<td>{}</td>",
            prettify(self.queue.message_count())
        ));
        let depth_delta = self.message_count_delta();
        s.push_str(&format!("<td>{}</td>", prettify(depth_delta)));
        s.push_str(&format!("<td>{}</td>", as_date(self.time_last_add)));
        s.push_str(&format!("<td>{}</td>", as_date(self.time_last_ack)));
        s.push_str(&format!("<td>{}</td>", as_date(self.time_last_update)));
        s.push_str("</tr>\n</table>\n");
        s
    }

    /// `MessageCounterHelper.listMessageCounterHistoryAsHTML` of this
    /// counter.
    pub fn history_html(&mut self) -> String {
        let mut s = String::from("<ul>\n<li>\n  <ul>\n    <li>");
        s.push_str(&format!("Queue '{}'", self.queue.name()));
        s.push_str(
            "</li>\n    <li><table class=\"activemq-message-counter-history\">\n<tr><th>Date</th>",
        );
        for j in 0..HOURS {
            s.push_str(&format!("<th>{j}</th>"));
        }
        s.push_str("<th>Total</th></tr>\n");
        for (j, day) in self.history().iter().enumerate() {
            let bg = if j % 2 == 0 { "FFFFFF" } else { "F0F0F0" };
            s.push_str(&format!(
                "<tr bgcolor=\"#{bg}\"><td>{}</td>",
                short_date(day.day * MILLIS_PER_DAY)
            ));
            let mut total = 0i32;
            for &v in &day.counters {
                let v = v as i32;
                if v == -1 {
                    s.push_str("<td></td>");
                } else {
                    s.push_str(&format!("<td>{v}</td>"));
                    total = total.wrapping_add(v);
                }
            }
            s.push_str(&format!("<td>{total}</td></tr>\n"));
        }
        s.push_str("</table></li>\n  </ul>\n</li>\n</ul>\n");
        s
    }
}

fn hour_of(millis: i64) -> usize {
    (millis.rem_euclid(MILLIS_PER_DAY) / 3_600_000) as usize
}

/// `MessageCounter.DayCounter`: a day's messages, hour by hour; -1 for an
/// hour not counted.
#[derive(Clone)]
struct DayCounter {
    /// Days since the epoch.
    day: i64,
    counters: [i64; HOURS],
}

impl DayCounter {
    /// The hours before `hour` are -1 on the first day, else 0; `hour` is
    /// 0 and the rest -1.
    fn new(day: i64, hour: usize, start_day: bool) -> Self {
        let mut counters = [-1; HOURS];
        if !start_day {
            counters[..hour].fill(0);
        }
        counters[hour] = 0;
        Self { day, counters }
    }

    /// `updateDayCounter`: the hours from the first counted up to now are
    /// counted, and the new messages added to this hour.
    fn update(&mut self, hour: usize, new_messages: i64) {
        let mut counting = false;
        for c in &mut self.counters[..=hour] {
            counting |= *c > -1;
            if counting && *c == -1 {
                *c = 0;
            }
        }
        self.counters[hour] += new_messages;
    }

    /// `finalizeDayCounter`: the day is over; every hour from the first
    /// counted is counted.
    fn finalize(&mut self) {
        let mut counting = false;
        for c in &mut self.counters {
            counting |= *c > -1;
            if counting && *c == -1 {
                *c = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn day_counters() {
        let mut d = DayCounter::new(0, 5, true);
        assert_eq!(&d.counters[..7], &[-1, -1, -1, -1, -1, 0, -1]);
        d.update(8, 3);
        assert_eq!(&d.counters[4..10], &[-1, 0, 0, 0, 3, -1]);
        d.finalize();
        assert_eq!(&d.counters[..6], &[-1, -1, -1, -1, -1, 0]);
        assert!(d.counters[6..].iter().all(|&c| c >= 0));
        let d = DayCounter::new(1, 3, false);
        assert_eq!(&d.counters[..5], &[0, 0, 0, 0, -1]);
    }
}
