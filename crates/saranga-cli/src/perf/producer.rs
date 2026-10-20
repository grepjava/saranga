//! `ProducerBenchmark`, its load generators (`ProducerMaxLoadGenerator`,
//! `ProducerTargetRateLoadGenerator`) and `AsyncJms2ProducerFacade`: a task
//! per producer sending `BytesMessage`s with a `time` property.
//!
//! The JMS producer sends with a `CompletionListener`. Artemis's Core
//! client, with the default confirmation window of -1, completes a send
//! once it is written; Qpid completes it when the broker's outcome comes.
//! At most `--max-pending` sends wait for their completion (at least the
//! commit interval when transacted), and a transaction is committed once
//! as many sends as the interval have completed.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::bail;
use saranga_buffer::PropertyValue;
use tokio::task::JoinHandle;

use super::Clock;
use super::stats::ProducerStats;
use crate::client::jms::Destination;
use crate::client::jmsapi::{Body, Completion, Conn, ConnFactory, Msg, Prod, Protocol, Sess};

/// The producer side of `perf producer` and `perf client`.
#[derive(Clone, Debug)]
pub struct ProducerConfig {
    pub persistent: bool,
    pub message_size: usize,
    pub rate: Option<i64>,
    pub ttl: i64,
    pub group: Option<String>,
    pub shared_connections: bool,
    pub producers: usize,
    pub threads: usize,
    pub commit_interval: i64,
    pub message_count: i64,
    pub enable_message_id: bool,
    pub enable_timestamp: bool,
    /// `--max-pending`: sends not yet completed, 0 or less for no limit.
    pub max_pending: i64,
}

struct Generator {
    stats: Arc<ProducerStats>,
    /// The generator stopped: no more sends.
    done: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

pub struct ProducerBenchmark {
    generators: Vec<Generator>,
    /// The messages sent to each destination, by its producers.
    per_destination: Vec<(Destination, Vec<Arc<ProducerStats>>)>,
    connections: Vec<Arc<Conn>>,
    stop: Arc<AtomicBool>,
    closed: bool,
}

impl ProducerBenchmark {
    /// `start`: the connections (one per producer, or one per worker
    /// thread when shared), a session and producer each, and the load
    /// generators running.
    pub async fn start(
        factory: &ConnFactory,
        destinations: &[Destination],
        config: &ProducerConfig,
        clock: Clock,
    ) -> anyhow::Result<Self> {
        let total = destinations.len() * config.producers;
        let connection_count = if config.shared_connections {
            config.threads.max(1)
        } else {
            total
        };
        let mut connections = Vec::with_capacity(connection_count);
        for _ in 0..connection_count {
            connections.push(Arc::new(factory.connect(None).await?));
        }
        let content = Arc::new(vec![1u8; config.message_size]);
        let stop = Arc::new(AtomicBool::new(false));
        let total_i = total as i64;
        let count_per_producer = if total_i == 0 {
            0
        } else {
            config.message_count / total_i
        };
        let mut remaining_count = config.message_count;
        let rate_per_producer = config.rate.map(|r| r / total_i.max(1));
        let mut remaining_rate = config.rate;
        let mut generators = Vec::with_capacity(total);
        let mut per_destination = Vec::with_capacity(destinations.len());
        let mut sequence = 0usize;
        for dest in destinations {
            let mut producers = Vec::with_capacity(config.producers);
            for _ in 0..config.producers {
                // eventLoopGroup.next(): round robin over the worker threads.
                let connection = if config.shared_connections {
                    &connections[sequence % connection_count]
                } else {
                    &connections[sequence]
                };
                let session = connection
                    .async_send_session(config.commit_interval > 0)
                    .await?;
                let producer = session.producer(dest).await?;
                let count = if config.message_count == 0 {
                    None
                } else {
                    let c = count_per_producer.min(remaining_count);
                    remaining_count -= c;
                    Some(c)
                };
                let period_ns = match (rate_per_producer, remaining_rate) {
                    (Some(per), Some(remaining)) => {
                        let rate = per.min(remaining);
                        if rate <= 0 {
                            bail!("/ by zero");
                        }
                        remaining_rate = Some(remaining - rate);
                        Some(1_000_000_000 / rate)
                    }
                    _ => None,
                };
                let stats = Arc::new(ProducerStats {
                    wait_latencies: period_ns.map(|_| Default::default()),
                    ..ProducerStats::default()
                });
                producers.push(stats.clone());
                let done = Arc::new(AtomicBool::new(false));
                let load = Load {
                    session,
                    producer,
                    protocol: factory.protocol(),
                    config: config.clone(),
                    stats: stats.clone(),
                    count,
                    period_ns,
                    clock: clock.clone(),
                    stop: stop.clone(),
                    content: content.clone(),
                };
                let task = tokio::spawn(load.run(done.clone()));
                generators.push(Generator { stats, done, task });
                sequence += 1;
            }
            per_destination.push((dest.clone(), producers));
        }
        Ok(Self {
            generators,
            per_destination,
            connections,
            stop,
            closed: false,
        })
    }

    pub fn stats(&self) -> Vec<Arc<ProducerStats>> {
        self.generators.iter().map(|g| g.stats.clone()).collect()
    }

    /// `anyError`.
    pub fn any_error(&self) -> bool {
        !self.closed
            && self
                .generators
                .iter()
                .any(|g| g.stats.fatal.load(Ordering::Relaxed))
    }

    /// `isRunning`: a generator is still sending, and none failed.
    pub fn is_running(&self) -> bool {
        if self.closed {
            return false;
        }
        let mut running = false;
        for g in &self.generators {
            if !g.done.load(Ordering::Acquire) {
                running = true;
            } else if g.stats.fatal.load(Ordering::Relaxed) {
                return false;
            }
        }
        running
    }

    /// `expectedTotalMessageCountToReceive`: what was sent to each queue,
    /// and to each topic times its subscribers.
    pub fn expected_total_to_receive(&self, shared: usize, consumers: usize) -> i64 {
        self.per_destination
            .iter()
            .map(|(dest, producers)| {
                let sent: i64 = producers
                    .iter()
                    .map(|p| p.sent.load(Ordering::Relaxed))
                    .sum();
                if dest.is_queue() {
                    sent
                } else {
                    let subscribers = if shared > 0 { shared } else { consumers };
                    subscribers as i64 * sent
                }
            })
            .sum()
    }

    /// `asyncClose`: no more sends; once the generators are done (their
    /// pending sends completed and their transactions committed), the
    /// connections are closed.
    pub async fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.stop.store(true, Ordering::Release);
        for g in &mut self.generators {
            let _ = (&mut g.task).await;
        }
        for c in self.connections.drain(..) {
            c.shutdown().await;
        }
    }
}

/// A load generator and the producer it drives.
struct Load {
    session: Sess,
    producer: Prod,
    protocol: Protocol,
    config: ProducerConfig,
    stats: Arc<ProducerStats>,
    /// The messages to send, none for no limit.
    count: Option<i64>,
    /// The time between sends at the target rate.
    period_ns: Option<i64>,
    clock: Clock,
    stop: Arc<AtomicBool>,
    content: Arc<Vec<u8>>,
}

/// `AsyncJms2ProducerFacade`'s count of sends: those not yet completed, in
/// order, with the time each message carries.
struct Window {
    pending: VecDeque<(i64, Completion)>,
    max_pending: i64,
    transaction_capacity: i64,
    pending_in_transaction: i64,
    completed_in_transaction: i64,
    completed: i64,
}

impl Window {
    /// `isAvailable`.
    fn is_available(&self) -> bool {
        if self.max_pending > 0 && self.pending.len() as i64 >= self.max_pending {
            return false;
        }
        self.transaction_capacity == 0 || self.pending_in_transaction != self.transaction_capacity
    }
}

impl Load {
    async fn run(self, done: Arc<AtomicBool>) {
        if let Err(e) = self.generate().await {
            tracing::debug!("perf producer failed: {e:#}");
            self.stats.fatal.store(true, Ordering::Relaxed);
        }
        done.store(true, Ordering::Release);
    }

    /// `run` of the generator, until the count is sent or the benchmark
    /// closes, then the sends still pending completed (`requestClose`).
    async fn generate(&self) -> anyhow::Result<()> {
        let capacity = self.config.commit_interval.max(0);
        let mut window = Window {
            pending: VecDeque::new(),
            max_pending: if capacity > 0 && self.config.max_pending > 0 {
                self.config.max_pending.max(capacity)
            } else {
                self.config.max_pending
            },
            transaction_capacity: capacity,
            pending_in_transaction: 0,
            completed_in_transaction: 0,
            completed: 0,
        };
        let us_period = self.period_ns.map(|ns| ns as f64 / 1000.0);
        let mut start = 0;
        let mut fire_time = 0;
        let mut fire_count = 0i64;
        let mut sent = 0i64;
        while !self.stop.load(Ordering::Acquire) {
            let now = self.clock.now();
            if sent == 0 {
                start = now;
                fire_time = now;
            }
            let expected = if us_period.is_some() { fire_time } else { now };
            // `NotAvailable`: the send waits for a completion, and is tried
            // again at the time it then is.
            if !window.is_available() {
                self.stats.not_available.fetch_add(1, Ordering::Relaxed);
                while !window.is_available() {
                    self.complete_next(&mut window, false).await?;
                }
            }
            let time = self.clock.now().max(now);
            let completion = self.send(time).await?;
            window.pending.push_back((time, completion));
            window.pending_in_transaction += 1;
            sent += 1;
            self.stats.sent.store(sent, Ordering::Relaxed);
            if let Some(w) = &self.stats.wait_latencies {
                w.record(time - expected);
            }
            self.complete_ready(&mut window).await?;
            if self.count.is_some_and(|c| sent >= c) {
                break;
            }
            if let Some(us) = us_period {
                fire_count += 1;
                fire_time = start + (fire_count as f64 * us) as i64;
                self.wait_until(fire_time, &mut window).await?;
            }
        }
        while !window.pending.is_empty() {
            self.complete_next(&mut window, true).await?;
        }
        if window.transaction_capacity > 0 && window.completed_in_transaction > 0 {
            self.session.commit().await?;
        }
        Ok(())
    }

    /// The completions that have come, in order.
    async fn complete_ready(&self, window: &mut Window) -> anyhow::Result<()> {
        loop {
            let ready = match window.pending.front_mut() {
                Some((_, c)) => poll_ready(c),
                None => None,
            };
            let Some(r) = ready else {
                return Ok(());
            };
            let (time, _) = window.pending.pop_front().expect("front");
            self.completed(window, time, r, false).await?;
        }
    }

    /// The next completion, waited for.
    async fn complete_next(&self, window: &mut Window, closing: bool) -> anyhow::Result<()> {
        if let Some((time, c)) = window.pending.pop_front() {
            let r = c.await;
            self.completed(window, time, r, closing).await?;
        }
        Ok(())
    }

    /// `onCompletion`, then `onSendCompleted`: the latency since the
    /// message's time, and the commit once the transaction is complete.
    async fn completed(
        &self,
        window: &mut Window,
        time: i64,
        result: anyhow::Result<()>,
        closing: bool,
    ) -> anyhow::Result<()> {
        result?;
        self.stats
            .send_completed_latencies
            .record(self.clock.now() - time);
        window.completed += 1;
        self.stats
            .completed
            .store(window.completed, Ordering::Relaxed);
        window.completed_in_transaction += 1;
        if window.transaction_capacity > 0
            && (window.completed_in_transaction == window.transaction_capacity
                || (closing && window.pending.is_empty()))
        {
            window.completed_in_transaction = 0;
            window.pending_in_transaction = 0;
            self.session.commit().await?;
        }
        Ok(())
    }

    /// The microsecond the next send fires at, as the event loop schedules
    /// it: a sleep to within the timer's resolution, then yields, the
    /// completions handled meanwhile.
    async fn wait_until(&self, fire_time: i64, window: &mut Window) -> anyhow::Result<()> {
        const TIMER_RESOLUTION_US: i64 = 16_000;
        let delay = fire_time - self.clock.now();
        if delay > TIMER_RESOLUTION_US {
            tokio::time::sleep(Duration::from_micros((delay - TIMER_RESOLUTION_US) as u64)).await;
        }
        while self.clock.now() < fire_time && !self.stop.load(Ordering::Acquire) {
            self.complete_ready(window).await?;
            tokio::task::yield_now().await;
        }
        self.complete_ready(window).await
    }

    /// `trySend`: a new `BytesMessage` of the content, its `time`, and the
    /// group as `JMSXGroupID`.
    async fn send(&self, time: i64) -> anyhow::Result<Completion> {
        let mut message = Msg::create(self.protocol, Body::Bytes(self.content.to_vec()));
        message.set_property("time", &PropertyValue::Long(time))?;
        if let Some(g) = &self.config.group {
            message.set_property("JMSXGroupID", &PropertyValue::String(g.as_str().into()))?;
        }
        self.session
            .send_completion(
                &self.producer,
                message,
                self.config.persistent,
                self.config.ttl,
                self.config.enable_message_id,
                self.config.enable_timestamp,
            )
            .await
    }
}

/// The completion's result if it has come, without waiting.
fn poll_ready(c: &mut Completion) -> Option<anyhow::Result<()>> {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match c.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(r) => Some(r),
        std::task::Poll::Pending => None,
    }
}
