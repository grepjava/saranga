//! The slow-consumer policy (`QueueImpl.SlowConsumerReaperRunnable`): a
//! queue whose address settings give a `slow-consumer-threshold` is checked
//! every `slow-consumer-check-period` seconds, and each of its consumers
//! acknowledging fewer messages a second than the threshold is reported
//! with a `CONSUMER_SLOW` notification (`NOTIFY`) or has its connection
//! killed (`KILL`).
//!
//! Artemis schedules a task for each queue; Saranga has one task that
//! checks each queue when its period is up.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

use saranga_config::{SlowConsumerPolicy, SlowConsumerThresholdMeasurementUnit};
use tokio::time::Instant;

use crate::Broker;
use crate::message::now_millis;
use crate::notifications::{NotificationType, hdr, string};
use crate::queue::Queue;
use crate::registry::ConsumerEntry;

/// How often the task looks at the queues.
const TICK: Duration = Duration::from_millis(200);

/// A queue's reaper (`SlowConsumerReaperRunnable`) and the queue's rate
/// snapshot (`queueRateCheckTime`, `messagesAddedSnapshot`).
struct Reaper {
    /// Check period, threshold, its unit and the policy.
    settings: (
        i64,
        i64,
        SlowConsumerThresholdMeasurementUnit,
        SlowConsumerPolicy,
    ),
    next: Instant,
    rate: Rate,
}

impl Reaper {
    /// `thresholdInMsgPerSecond`.
    fn threshold(&self) -> f32 {
        self.settings.1 as f32 / unit_seconds(self.settings.2)
    }
}

/// `getRate`'s state: when it was last asked, and the count then.
struct Rate {
    checked: i64,
    snapshot: i64,
}

impl Rate {
    fn new(since: i64) -> Self {
        Self {
            checked: since,
            snapshot: 0,
        }
    }

    /// `getRate`: the count's growth a second since the last call, rounded
    /// up to two decimals.
    fn take(&mut self, count: i64) -> f32 {
        let now = now_millis();
        let slice = (now - std::mem::replace(&mut self.checked, now)) as f32 / 1000.0;
        let delta = count - std::mem::replace(&mut self.snapshot, count);
        if slice == 0.0 {
            return 0.0;
        }
        round_up_2(f64::from(delta as f32 / slice))
    }
}

/// `BigDecimal.valueOf(d).setScale(2, ROUND_UP).floatValue()`.
fn round_up_2(d: f64) -> f32 {
    // valueOf takes the shortest decimal that reads back as `d`.
    let text = format!("{}", d.abs());
    let (int, frac) = text.split_once('.').unwrap_or((&text, ""));
    let mut cents: f64 = format!("{int}.{:0<2}", &frac[..frac.len().min(2)])
        .parse()
        .unwrap_or(0.0);
    if frac.len() > 2 && frac[2..].bytes().any(|b| b != b'0') {
        cents += 0.01;
    }
    (cents.copysign(d)) as f32
}

/// `Float.toString` of an ordinary rate: `10.0`, `0.35`.
fn java_float(f: f32) -> String {
    if f.is_finite() && f.fract() == 0.0 {
        format!("{f:.1}")
    } else {
        format!("{f}")
    }
}

/// `SlowConsumerThresholdMeasurementUnit.getValue`: seconds.
fn unit_seconds(unit: SlowConsumerThresholdMeasurementUnit) -> f32 {
    use SlowConsumerThresholdMeasurementUnit as U;
    match unit {
        U::MessagesPerSecond => 1.0,
        U::MessagesPerMinute => 60.0,
        U::MessagesPerHour => 3600.0,
        U::MessagesPerDay => 86_400.0,
    }
}

/// Starts the task, which ends with the broker.
pub(crate) fn spawn(broker: &Arc<Broker>) {
    let weak: Weak<Broker> = Arc::downgrade(broker);
    tokio::spawn(async move {
        let mut reapers: HashMap<i64, Reaper> = HashMap::new();
        let mut consumers: HashMap<(String, i64), Rate> = HashMap::new();
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let Some(broker) = weak.upgrade() else { break };
            check(&broker, &mut reapers, &mut consumers);
        }
    });
}

fn check(
    broker: &Arc<Broker>,
    reapers: &mut HashMap<i64, Reaper>,
    consumers: &mut HashMap<(String, i64), Rate>,
) {
    let now = Instant::now();
    let queues = broker.post_office().queues();
    let mut live = HashSet::new();
    let mut due = Vec::new();
    for q in queues {
        let s = q.settings();
        if s.slow_consumer_threshold
            == saranga_config::AddressSettings::DEFAULT_SLOW_CONSUMER_THRESHOLD
        {
            continue;
        }
        live.insert(q.id());
        let key = (
            s.slow_consumer_check_period,
            s.slow_consumer_threshold,
            s.slow_consumer_threshold_measurement_unit,
            s.slow_consumer_policy,
        );
        let period = Duration::from_secs(key.0.max(0) as u64);
        let reaper = reapers.entry(q.id()).or_insert_with(|| Reaper {
            settings: key,
            next: now + period,
            rate: Rate::new(now_millis()),
        });
        // configureSlowConsumerReaper: changed settings reschedule it.
        if reaper.settings != key {
            reaper.settings = key;
            reaper.next = now + period;
        }
        if now >= reaper.next {
            reaper.next = now + period;
            due.push(q);
        }
    }
    reapers.retain(|id, _| live.contains(id));
    if due.is_empty() {
        return;
    }
    let entries: Vec<Arc<ConsumerEntry>> = broker
        .registry()
        .sessions()
        .iter()
        .flat_map(|s| s.consumers())
        .filter(|c| !c.browse_only)
        .collect();
    for q in due {
        let Some(reaper) = reapers.get_mut(&q.id()) else {
            continue;
        };
        reap(broker, &q, reaper, &entries, consumers);
    }
    let seen: HashSet<(String, i64)> = entries.iter().map(|c| (c.session.clone(), c.id)).collect();
    consumers.retain(|k, _| seen.contains(k));
}

/// `SlowConsumerReaperRunnable.run`.
fn reap(
    broker: &Arc<Broker>,
    q: &Queue,
    reaper: &mut Reaper,
    entries: &[Arc<ConsumerEntry>],
    rates: &mut HashMap<(String, i64), Rate>,
) {
    let queue_rate = reaper.rate.take(q.messages_added());
    let queue_messages = q.message_count();
    let consumer_count = q.consumer_count();
    tracing::debug!(
        "{}:{} has {queue_messages} message(s) and {consumer_count} consumer(s) and is receiving messages at a rate of {queue_rate} msgs/second.",
        q.config().address,
        q.name()
    );
    if consumer_count == 0 {
        return;
    }
    let threshold = reaper.threshold();
    let queue_threshold = threshold * consumer_count as f32;
    if queue_rate < queue_threshold && (queue_messages as f32) < queue_threshold {
        return;
    }
    for c in entries.iter().filter(|c| c.queue.id() == q.id()) {
        let rate = rates
            .entry((c.session.clone(), c.id))
            .or_insert_with(|| Rate::new(c.created))
            .take(c.metrics.acknowledged());
        if !(rate < threshold || (rate == 0.0 && threshold == 0.0)) {
            continue;
        }
        let Some(connection) = broker
            .registry()
            .session(&c.session)
            .and_then(|s| s.connection())
        else {
            continue;
        };
        tracing::info!(
            "AMQ221048: Consumer {}:{} attached to queue '{}' from {} identified as 'slow.' Expected consumption rate: {} msgs/second; actual consumption rate: {} msgs/second.",
            c.session,
            c.id,
            q.name(),
            connection.remote_address(),
            java_float(threshold),
            java_float(rate)
        );
        match reaper.settings.3 {
            SlowConsumerPolicy::Kill => connection.kill(),
            SlowConsumerPolicy::Notify => {
                let (address, remote, connection_id, session, id) = (
                    q.config().address.to_string(),
                    connection.remote_address().to_owned(),
                    connection.id().to_owned(),
                    c.session.clone(),
                    c.id,
                );
                broker
                    .post_office()
                    .notify(NotificationType::ConsumerSlow, move |_| {
                        vec![
                            (
                                hdr::CONSUMER_COUNT,
                                saranga_buffer::PropertyValue::Int(consumer_count),
                            ),
                            (hdr::ADDRESS, string(Some(&address))),
                            (hdr::REMOTE_ADDRESS, string(Some(&remote))),
                            (hdr::CONNECTION_NAME, string(Some(&connection_id))),
                            (hdr::CONSUMER_NAME, saranga_buffer::PropertyValue::Long(id)),
                            (hdr::SESSION_NAME, string(Some(&session))),
                        ]
                    });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::round_up_2;

    #[test]
    fn rates_round_up() {
        assert_eq!(round_up_2(2.0), 2.0);
        assert_eq!(round_up_2(0.07), 0.07);
        assert_eq!(round_up_2(0.071), 0.08);
        assert_eq!(round_up_2(f64::from(0.3f32)), 0.31);
        assert_eq!(round_up_2(0.0), 0.0);
        assert_eq!(round_up_2(12.345), 12.35);
    }
}
