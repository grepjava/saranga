//! `saranga perf` (`PerfGroup`): `client`, `producer` and `consumer`, the
//! Artemis load generator and its live report, over Core; and
//! `perf-journal` (`PerfJournal`).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::bail;

use crate::client::ConnectionArgs;
use crate::client::jms::Destination;

mod consumer;
mod hdr;
pub mod journal;
mod producer;
mod stats;

use consumer::{ConsumerConfig, ListenerBenchmark};
use producer::{ProducerBenchmark, ProducerConfig};
use stats::LiveStatistics;

#[derive(clap::Subcommand)]
pub enum PerfCommand {
    /// Produce messages to and consume messages from a broker instance.
    Client(ClientArgs),
    /// Send messages to a broker.
    Producer(ProducerArgs),
    /// Consume messages from a queue.
    Consumer(ConsumerArgs),
}

/// The options of every `perf` command (`PerfCommand`).
#[derive(clap::Args, Clone, Debug)]
pub struct PerfArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    /// Show latencies at interval on output. Default: disabled.
    #[arg(long = "show-latency")]
    show_latency: bool,
    /// Report file name. Default: none.
    #[arg(long = "json")]
    report_file: Option<String>,
    /// HDR Histogram Report file name. Default: none.
    #[arg(long = "hdr")]
    hdr_file: Option<String>,
    /// Test duration (in seconds). Default: 0.
    #[arg(long, default_value_t = 0)]
    duration: i64,
    /// Warmup time (in seconds). Default: 0.
    #[arg(long, default_value_t = 0)]
    warmup: i64,
    /// Total number of messages. Default: 0.
    #[arg(long = "message-count", default_value_t = 0)]
    message_count: i64,
    /// If present, generate --num-destinations for each destination name,
    /// using it as a prefix and adding a number [0,--num-destinations) as
    /// suffix. Default: none.
    #[arg(long = "num-destinations", default_value_t = 1)]
    num_destinations: usize,
    /// Transaction size.
    #[arg(long = "tx-size", hide = true, default_value_t = 0)]
    tx_size: i64,
    /// Transaction size.
    #[arg(long = "commit-interval", default_value_t = 0)]
    commit_interval: i64,
    /// List of destination names. Each name can be prefixed with queue://
    /// or topic:// and can be an FQQN in the form of <address>::<queue>.
    /// Default: queue://TEST.
    destinations: Vec<String>,
}

/// The producer options of `perf producer` and `perf client`.
#[derive(clap::Args, Clone, Debug)]
pub struct SendArgs {
    /// Send messages persistently. Default: non persistent.
    #[arg(long)]
    persistent: bool,
    /// Size of each bytesMessage. Default: 1024.
    #[arg(long = "message-size", default_value_t = 1024)]
    message_size: usize,
    /// Expected total message rate. Default: unbounded.
    #[arg(long)]
    rate: Option<i64>,
    /// TTL for each message.
    #[arg(long, default_value_t = 0)]
    ttl: i64,
    /// Message Group to be used.
    #[arg(long = "group")]
    group: Option<String>,
    /// Create --threads shared connections among producers. Default: not
    /// shared.
    #[arg(long = "shared-connections")]
    shared_connections: bool,
    /// Number of producers to use for each generated destination. Default:
    /// 1.
    #[arg(long, default_value_t = 1)]
    producers: usize,
    /// Number of worker threads to schedule producer load tasks. Default:
    /// 1.
    #[arg(long, default_value_t = 1)]
    threads: usize,
    /// How many not yet completed messages can exists. Default is 1.
    // Each send completes once written, so none are ever pending.
    #[allow(dead_code)]
    #[arg(long = "max-pending", default_value_t = 1)]
    max_pending: i64,
    /// Enable setting JMS messageID per-message. Default: disabled.
    #[arg(long = "enable-msg-id")]
    enable_message_id: bool,
    /// Enable setting JMS timestamp per-message. Default: disabled.
    #[arg(long = "enable-timestamp")]
    enable_timestamp: bool,
}

/// The consumer options of `perf consumer` and `perf client`.
#[derive(clap::Args, Clone, Debug)]
pub struct ReceiveArgs {
    /// Individually acknowledge each message received. Default: disabled.
    #[arg(long = "tx")]
    transaction: bool,
    /// Create shared subscription. Default: 0.
    #[arg(long = "shared", default_value_t = 0)]
    shared: usize,
    /// Enabled durable subscription. Default: disabled.
    #[arg(long = "durable")]
    durable: bool,
    /// Number of consumer to use for each generated destination. Default:
    /// 1.
    #[arg(long = "consumers", default_value_t = 1)]
    consumers: usize,
}

#[derive(clap::Args, Clone, Debug)]
pub struct ClientArgs {
    #[command(flatten)]
    perf: PerfArgs,
    #[command(flatten)]
    send: SendArgs,
    #[command(flatten)]
    receive: ReceiveArgs,
    /// Number of consumer connections to be used. Default: same as the
    /// total number of consumers
    #[arg(long = "consumer-connections", default_value_t = 0)]
    consumer_connections: usize,
    /// The url used for MessageListener(s) connections. Default: same as
    /// --url.
    #[arg(long = "consumer-url")]
    consumer_url: Option<String>,
    /// The protocol used for MessageListener(s) connections. Default: same
    /// as --protocol. Valid values are CORE.
    #[arg(long = "consumer-protocol")]
    consumer_protocol: Option<String>,
}

#[derive(clap::Args, Clone, Debug)]
pub struct ProducerArgs {
    #[command(flatten)]
    perf: PerfArgs,
    #[command(flatten)]
    send: SendArgs,
}

#[derive(clap::Args, Clone, Debug)]
pub struct ConsumerArgs {
    #[command(flatten)]
    perf: PerfArgs,
    #[command(flatten)]
    receive: ReceiveArgs,
    /// Number of connections to be used. Default: same as the total number
    /// of consumers.
    #[arg(long = "num-connections", default_value_t = 0)]
    connections: usize,
}

/// The `MicrosTimeProvider`: wall-clock microseconds when the producer and
/// consumer are different processes, else `System.nanoTime` ones.
#[derive(Clone, Debug)]
pub enum Clock {
    Wall,
    Monotonic(Instant),
}

impl Clock {
    pub fn now(&self) -> i64 {
        match self {
            Self::Wall => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as i64,
            Self::Monotonic(base) => base.elapsed().as_micros() as i64,
        }
    }
}

pub async fn run(command: PerfCommand) -> anyhow::Result<()> {
    match command {
        PerfCommand::Client(a) => client(a).await,
        PerfCommand::Producer(a) => producer(a).await,
        PerfCommand::Consumer(a) => consumer(a).await,
    }
}

impl PerfArgs {
    /// `PerfCommand.execute`: the URL, the deprecated `--tx-size`.
    fn prepare(&mut self) -> anyhow::Result<()> {
        self.connection.resolve(false)?;
        if self.tx_size > 0 {
            println!("--tx-size is deprecated, please use --commit-interval");
            self.commit_interval = self.tx_size;
        }
        Ok(())
    }

    /// `lookupDestinations`: each name, or each with the numbers
    /// `0..--num-destinations` appended; `queue://TEST` by default.
    async fn destinations(&self, connection: &ConnectionArgs) -> anyhow::Result<Vec<Destination>> {
        let names = if self.destinations.is_empty() {
            vec!["queue://TEST".to_owned()]
        } else {
            self.destinations.clone()
        };
        // A connection (from a factory without the client ID) and session
        // are made to create the destinations.
        let mut connection = connection.clone();
        connection.client_id = None;
        let conn = connection.connect().await?;
        let session = conn.session(false).await;
        if let Ok(s) = &session {
            s.close().await;
        }
        conn.close().await;
        session?;
        let mut out = Vec::new();
        for name in names {
            if self.num_destinations == 1 {
                out.push(Destination::parse(&name));
            } else {
                for suffix in 0..self.num_destinations {
                    out.push(Destination::parse(&format!("{name}{suffix}")));
                }
            }
        }
        Ok(out)
    }

    fn silent(&self) -> bool {
        self.connection.common.silent
    }
}

impl SendArgs {
    fn config(&self, perf: &PerfArgs) -> ProducerConfig {
        ProducerConfig {
            persistent: self.persistent,
            message_size: self.message_size,
            rate: self.rate,
            ttl: self.ttl,
            group: self.group.clone(),
            shared_connections: self.shared_connections,
            producers: self.producers,
            threads: self.threads,
            commit_interval: perf.commit_interval,
            message_count: perf.message_count,
            enable_message_id: self.enable_message_id,
            enable_timestamp: self.enable_timestamp,
        }
    }

    /// No more worker threads than producers.
    fn adjust_threads(&mut self, destinations: usize) {
        let total = self.producers * destinations;
        if self.threads > total {
            eprintln!(
                "Doesn't make sense to set workers > producers: auto-adjusting it to be the same as the producer count"
            );
            self.threads = total;
        }
    }
}

/// The default number of consumer connections: one per consumer, or per
/// shared subscription when the client ID is set.
fn consumer_connections(
    connections: usize,
    receive: &ReceiveArgs,
    client_id: bool,
    destinations: usize,
) -> usize {
    if connections != 0 {
        connections
    } else if receive.shared > 0 {
        if client_id {
            receive.shared * destinations
        } else {
            receive.shared * receive.consumers * destinations
        }
    } else {
        receive.consumers * destinations
    }
}

/// Something `collectAndReportStatisticsWhileRunning` watches.
trait Benchmark {
    fn is_running(&self) -> bool;
}

impl Benchmark for ProducerBenchmark {
    fn is_running(&self) -> bool {
        ProducerBenchmark::is_running(self)
    }
}

impl Benchmark for ListenerBenchmark {
    fn is_running(&self) -> bool {
        ListenerBenchmark::is_running(self)
    }
}

/// The run's timing: when the warmup ends and when the run does.
struct Timing {
    warming_up: bool,
    end_warmup: i64,
    end: i64,
}

impl Timing {
    fn new(perf: &PerfArgs) -> Self {
        let now = stats::now_ms();
        Self {
            warming_up: perf.warmup != 0,
            end_warmup: if perf.warmup > 0 {
                now + perf.warmup * 1000
            } else {
                0
            },
            end: if perf.duration > 0 {
                now + perf.duration * 1000
            } else {
                0
            },
        }
    }
}

/// `collectAndReportStatisticsWhileRunning`: a sample and its report each
/// second while the benchmark runs, until the end time, if any. Ctrl-C
/// stops the run as the shutdown hook does.
async fn collect(
    perf: &PerfArgs,
    statistics: &mut LiveStatistics,
    timing: &mut Timing,
    end: i64,
    benchmark: &dyn Benchmark,
) -> anyhow::Result<bool> {
    while benchmark.is_running() {
        let tick = stats::now_ms();
        if end != 0 && tick - end >= 0 {
            break;
        }
        if timing.end_warmup != 0 && timing.warming_up && tick - timing.end_warmup >= 0 {
            timing.warming_up = false;
        }
        statistics.sample_metrics(timing.warming_up);
        let mut out = String::new();
        statistics.out_at_interval(timing.warming_up, &mut out, perf.show_latency)?;
        if !perf.silent() {
            println!("{out}");
        }
        if interrupted(Duration::from_secs(1)).await {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Waits, true when Ctrl-C came first.
async fn interrupted(d: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(d) => false,
        _ = tokio::signal::ctrl_c() => true,
    }
}

/// The last sample, taken with the benchmark complete, and the summary.
fn summary(
    perf: &PerfArgs,
    mut statistics: LiveStatistics,
    warming_up: bool,
) -> anyhow::Result<()> {
    statistics.sample_metrics(warming_up);
    let mut out = String::new();
    statistics.out_summary(&mut out)?;
    if !perf.silent() {
        println!("{out}");
    }
    statistics.close()
}

/// `PerfProducerCommand`.
async fn producer(mut a: ProducerArgs) -> anyhow::Result<()> {
    a.perf.prepare()?;
    let destinations = a.perf.destinations(&a.perf.connection).await?;
    if a.perf.connection.client_id.is_some() {
        eprintln!("ClientID configuration is not supported");
    }
    a.send.adjust_threads(destinations.len());
    let config = a.send.config(&a.perf);
    let mut benchmark =
        ProducerBenchmark::start(&a.perf.connection, &destinations, &config, Clock::Wall).await?;
    let mut timing = Timing::new(&a.perf);
    let mut statistics = LiveStatistics::new(
        a.perf.report_file.as_deref(),
        a.perf.hdr_file.as_deref(),
        benchmark.stats(),
        None,
    )?;
    let result = async {
        if !interrupted(Duration::from_secs(1)).await {
            let end = timing.end;
            collect(&a.perf, &mut statistics, &mut timing, end, &benchmark).await?;
        }
        anyhow::Ok(())
    }
    .await;
    benchmark.close().await;
    result?;
    summary(&a.perf, statistics, timing.warming_up)
}

/// `PerfConsumerCommand`.
async fn consumer(mut a: ConsumerArgs) -> anyhow::Result<()> {
    a.perf.prepare()?;
    let destinations = a.perf.destinations(&a.perf.connection).await?;
    let config = ConsumerConfig {
        transacted: a.receive.transaction,
        shared: a.receive.shared,
        durable: a.receive.durable,
        connections: consumer_connections(
            a.connections,
            &a.receive,
            a.perf.connection.client_id.is_some(),
            destinations.len(),
        ),
        consumers: a.receive.consumers,
        message_count: a.perf.message_count,
        can_delay_message_count: false,
    };
    let mut benchmark =
        ListenerBenchmark::start(&a.perf.connection, &destinations, &config, Clock::Wall).await?;
    let mut timing = Timing::new(&a.perf);
    let mut statistics = LiveStatistics::new(
        a.perf.report_file.as_deref(),
        a.perf.hdr_file.as_deref(),
        Vec::new(),
        Some(benchmark.listeners()),
    )?;
    let result = async {
        if !interrupted(Duration::from_secs(1)).await {
            let end = timing.end;
            collect(&a.perf, &mut statistics, &mut timing, end, &benchmark).await?;
        }
        anyhow::Ok(())
    }
    .await;
    let closed = benchmark.close().await;
    result?;
    closed?;
    summary(&a.perf, statistics, timing.warming_up)
}

/// `PerfClientCommand`: producers and listeners in one process; once the
/// producers are done, the listeners run until they have what was sent.
async fn client(mut a: ClientArgs) -> anyhow::Result<()> {
    if a.receive.durable && a.perf.destinations.is_empty() {
        // Durable subscriptions need a topic.
        a.perf.destinations = vec!["topic://TEST".to_owned()];
    }
    if a.receive.durable && a.perf.connection.client_id.is_none() {
        bail!(crate::client::UsageError(
            "The clientID must be set on durable subscriptions".to_owned()
        ));
    }
    a.perf.prepare()?;
    if let Some(p) = &a.consumer_protocol
        && !p.eq_ignore_ascii_case("core")
    {
        bail!(crate::client::UsageError(format!(
            "protocol {} not supported",
            p.to_uppercase()
        )));
    }
    let destinations = a.perf.destinations(&a.perf.connection).await?;
    let mut listener_args = a.perf.connection.clone();
    if let Some(url) = &a.consumer_url {
        listener_args.url = Some(url.clone());
    }
    let listener_destinations = if a.consumer_protocol.is_some() {
        a.perf.destinations(&listener_args).await?
    } else {
        destinations.clone()
    };
    let consumer_config = ConsumerConfig {
        transacted: a.receive.transaction,
        shared: a.receive.shared,
        durable: a.receive.durable,
        connections: consumer_connections(
            a.consumer_connections,
            &a.receive,
            a.perf.connection.client_id.is_some(),
            destinations.len(),
        ),
        consumers: a.receive.consumers,
        message_count: a.perf.message_count,
        can_delay_message_count: true,
    };
    a.send.adjust_threads(destinations.len());
    let clock = Clock::Monotonic(Instant::now());
    let mut listeners = ListenerBenchmark::start(
        &listener_args,
        &listener_destinations,
        &consumer_config,
        clock.clone(),
    )
    .await?;
    let config = a.send.config(&a.perf);
    let mut producers =
        match ProducerBenchmark::start(&a.perf.connection, &destinations, &config, clock).await {
            Ok(p) => p,
            Err(e) => {
                let _ = listeners.close().await;
                return Err(e);
            }
        };
    let mut timing = Timing::new(&a.perf);
    let mut statistics = LiveStatistics::new(
        a.perf.report_file.as_deref(),
        a.perf.hdr_file.as_deref(),
        producers.stats(),
        Some(listeners.listeners()),
    )?;
    let result = async {
        if interrupted(Duration::from_secs(1)).await {
            return anyhow::Ok(());
        }
        let end = timing.end;
        if collect(&a.perf, &mut statistics, &mut timing, end, &producers).await? {
            return Ok(());
        }
        let fatal = producers.any_error();
        producers.close().await;
        if !fatal {
            listeners.set_message_count(
                producers.expected_total_to_receive(a.receive.shared, a.receive.consumers),
            );
            // Only the expected count matters now, not the duration.
            collect(&a.perf, &mut statistics, &mut timing, 0, &listeners).await?;
        }
        Ok(())
    }
    .await;
    producers.close().await;
    if let Err(e) = result {
        let _ = listeners.close().await;
        return Err(e);
    }
    // The summary comes before the listeners close, which can fail.
    summary(&a.perf, statistics, timing.warming_up)?;
    listeners.close().await
}
