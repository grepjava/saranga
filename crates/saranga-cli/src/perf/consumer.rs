//! `MessageListenerBenchmark` and `RecordingMessageListener`: consumers
//! on each destination (or shared subscriptions of each topic) over a
//! number of connections, each counting what it receives and recording
//! the time since the message's `time` property.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use saranga_buffer::PropertyValue;
use saranga_client::{Consumer, MessageExt};
use tokio::task::JoinHandle;

use super::Clock;
use super::stats::ListenerStats;
use crate::client::jms::{Destination, JmsConnection, JmsSession};

/// The consumer side of `perf consumer` and `perf client`.
#[derive(Clone, Debug)]
pub struct ConsumerConfig {
    pub transacted: bool,
    pub shared: usize,
    pub durable: bool,
    pub connections: usize,
    pub consumers: usize,
    pub message_count: i64,
    /// `canDelayMessageCount`: the count can be set once started.
    pub can_delay_message_count: bool,
}

/// `MessageCountLimiter`.
#[derive(Debug)]
struct Limiter {
    limit: AtomicI64,
    received: AtomicI64,
}

impl Limiter {
    fn is_limit_reached(&self) -> bool {
        self.received.load(Ordering::Relaxed) >= self.limit.load(Ordering::Relaxed)
    }
}

pub struct ListenerBenchmark {
    listeners: Vec<Arc<ListenerStats>>,
    tasks: Vec<JoinHandle<()>>,
    connections: Vec<Arc<JmsConnection>>,
    /// The sessions of shared durable subscriptions and their names.
    unsubscribe: Vec<(JmsSession, String)>,
    limiter: Option<Arc<Limiter>>,
    fatal: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    closed: bool,
}

impl ListenerBenchmark {
    /// `start`: the connections (with the client ID, numbered when more
    /// than one), then a session and consumer per listener.
    pub async fn start(
        args: &crate::client::ConnectionArgs,
        destinations: &[Destination],
        config: &ConsumerConfig,
        clock: Clock,
    ) -> anyhow::Result<Self> {
        let fatal = Arc::new(AtomicBool::new(false));
        let mut connections = Vec::with_capacity(config.connections);
        for i in 0..config.connections {
            let mut a = args.clone();
            if let Some(id) = &args.client_id
                && config.connections > 1
            {
                a.client_id = Some(format!("{id}{i}"));
            }
            connections.push(Arc::new(a.connect().await?));
        }
        let limiter = if config.message_count > 0 {
            Some(config.message_count)
        } else if config.can_delay_message_count {
            Some(i64::MAX)
        } else {
            None
        }
        .map(|limit| {
            Arc::new(Limiter {
                limit: AtomicI64::new(limit),
                received: AtomicI64::new(0),
            })
        });
        let stop = Arc::new(AtomicBool::new(false));
        let mut b = Self {
            listeners: Vec::new(),
            tasks: Vec::new(),
            connections,
            unsubscribe: Vec::new(),
            limiter,
            fatal,
            stop,
            closed: false,
        };
        let mut sequence = 0usize;
        let count = b.connections.len().max(1);
        for dest in destinations {
            if config.shared == 0 {
                for index in 0..config.consumers {
                    let connection = b.connections[sequence % count].clone();
                    sequence += 1;
                    let session = connection.session(config.transacted).await?;
                    let consumer = if config.durable {
                        let Destination::Topic(topic) = dest else {
                            anyhow::bail!("{dest} cannot be cast to a topic");
                        };
                        let name = format!("{topic}{index}");
                        session
                            .consumer(
                                dest,
                                None,
                                Some((name.as_str(), connection.client_id.as_deref())),
                            )
                            .await?
                    } else {
                        session.consumer(dest, None, None).await?
                    };
                    b.listen(session, consumer, &clock).await?;
                }
            } else {
                let Destination::Topic(topic) = dest else {
                    anyhow::bail!("{dest} cannot be cast to a topic");
                };
                for subscription in 0..config.shared {
                    let name = format!("{topic}{subscription}");
                    let mut connection = None;
                    if args.client_id.is_some() {
                        connection = Some(b.connections[sequence % count].clone());
                        sequence += 1;
                    }
                    for _ in 0..config.consumers {
                        if args.client_id.is_none() {
                            connection = Some(b.connections[sequence % count].clone());
                            sequence += 1;
                        }
                        let connection = connection.clone().expect("connection");
                        let session = connection.session(config.transacted).await?;
                        let consumer = session
                            .shared_consumer(topic, &name, config.durable, None)
                            .await?;
                        let unsubscribe = config.durable.then(|| session.clone());
                        b.listen(session, consumer, &clock).await?;
                        if let Some(s) = unsubscribe {
                            b.unsubscribe.push((s, name.clone()));
                        }
                    }
                }
            }
        }
        Ok(b)
    }

    /// A `RecordingMessageListener` on the consumer, its session started
    /// as the started connection starts the sessions made on it.
    async fn listen(
        &mut self,
        session: JmsSession,
        consumer: Consumer,
        clock: &Clock,
    ) -> anyhow::Result<()> {
        session.start().await?;
        let stats = Arc::new(ListenerStats {
            received: AtomicI64::new(0),
            latencies: Default::default(),
            fatal: self.fatal.clone(),
        });
        self.listeners.push(stats.clone());
        let listener = Listener {
            _session: session,
            consumer,
            stats,
            limiter: self.limiter.clone(),
            clock: clock.clone(),
            stop: self.stop.clone(),
        };
        self.tasks.push(tokio::spawn(listener.run()));
        Ok(())
    }

    pub fn listeners(&self) -> Vec<Arc<ListenerStats>> {
        self.listeners.clone()
    }

    /// `anyError`: a connection broke.
    pub fn any_error(&self) -> bool {
        self.check_connections();
        self.fatal.load(Ordering::Relaxed)
    }

    /// The connections' exception listener.
    fn check_connections(&self) {
        if self.connections.iter().any(|c| c.conn.failure().is_some()) {
            self.fatal.store(true, Ordering::Relaxed);
        }
    }

    /// `isRunning`: no connection broke, and the count is not reached.
    pub fn is_running(&self) -> bool {
        if self.closed || self.any_error() {
            return false;
        }
        self.limiter.as_ref().is_none_or(|l| !l.is_limit_reached())
    }

    /// `setMessageCount`.
    pub fn set_message_count(&self, count: i64) {
        if let Some(l) = &self.limiter {
            l.limit.store(count, Ordering::Relaxed);
        }
    }

    /// `close`: the shared durable subscriptions are unsubscribed, then
    /// the connections closed.
    pub async fn close(&mut self) -> anyhow::Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let mut result = Ok(());
        for (session, name) in std::mem::take(&mut self.unsubscribe) {
            if let Err(e) = session.unsubscribe(&name).await {
                // A RuntimeException from close: the message and the usage.
                result = Err(crate::client::UsageError(format!("{e:#}")).into());
                break;
            }
        }
        self.stop.store(true, Ordering::Release);
        for t in &mut self.tasks {
            let _ = t.await;
        }
        for c in self.connections.drain(..) {
            c.shutdown().await;
        }
        result
    }
}

/// A consumer and its `RecordingMessageListener`.
struct Listener {
    /// Kept open while the listener runs.
    _session: JmsSession,
    consumer: Consumer,
    stats: Arc<ListenerStats>,
    limiter: Option<Arc<Limiter>>,
    clock: Clock,
    stop: Arc<AtomicBool>,
}

impl Listener {
    /// Each message: counted, its latency recorded, then acknowledged (in
    /// the session's transaction when transacted, which is never
    /// committed).
    async fn run(self) {
        while !self.stop.load(Ordering::Acquire) {
            let delivery = match self.consumer.receive(Duration::from_millis(100)).await {
                Ok(Some(d)) => d,
                Ok(None) => continue,
                Err(_) => break,
            };
            if let Some(l) = &self.limiter {
                l.received.fetch_add(1, Ordering::Relaxed);
            }
            self.stats.received.fetch_add(1, Ordering::Relaxed);
            match delivery.message.property("time") {
                Some(PropertyValue::Long(start)) => {
                    self.stats.latencies.record(self.clock.now() - start)
                }
                _ => self.stats.fatal.store(true, Ordering::Relaxed),
            }
            if self.consumer.acknowledge(&delivery).await.is_err() {
                self.stats.fatal.store(true, Ordering::Relaxed);
            }
        }
    }
}
