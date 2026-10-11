//! The critical analyzer (`CriticalAnalyzerImpl`): every check period it
//! looks for a component that entered a critical path and has not left it
//! within `critical-analyzer-timeout`, and acts as
//! `critical-analyzer-policy` says: `LOG` logs, `SHUTDOWN` stops the
//! broker, `HALT` ends the process with status 70.
//!
//! The components are Artemis's: the journal's writer (`TimedBuffer`),
//! each queue (its actor handling a command) and each session (a send or
//! an acknowledgement). A path that expires is reset, so it is reported
//! once.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use saranga_config::CriticalAnalyzerPolicy;

use crate::Broker;

const LOGGER: &str = "org.apache.activemq.artemis.utils.critical";

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

fn now() -> u64 {
    epoch().elapsed().as_nanos() as u64 + 1
}

/// The analyzer's settings.
#[derive(Debug, Clone, Copy)]
pub struct CriticalConfig {
    pub enabled: bool,
    pub timeout: Duration,
    pub check_period: Duration,
    pub policy: CriticalAnalyzerPolicy,
}

impl Default for CriticalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            timeout: Duration::from_millis(120_000),
            check_period: Duration::from_millis(60_000),
            policy: CriticalAnalyzerPolicy::Log,
        }
    }
}

impl CriticalConfig {
    /// From `broker.xml`: a check period of 0 is half the timeout.
    pub fn from_file(f: &saranga_config::BrokerConfig) -> Self {
        let timeout = f.critical_analyzer_timeout.max(0) as u64;
        let period = match f.critical_analyzer_check_period {
            p if p > 0 => p as u64,
            _ => timeout / 2,
        };
        Self {
            enabled: f.critical_analyzer,
            timeout: Duration::from_millis(timeout),
            check_period: Duration::from_millis(period.max(1)),
            policy: f.critical_analyzer_policy,
        }
    }
}

/// A critical path (`CriticalMeasure`): when it was entered, while it is.
#[derive(Debug, Default)]
pub struct Measure {
    entered: AtomicU64,
}

impl Measure {
    /// Enters the path until the guard is dropped.
    pub fn enter(&self) -> Leave<'_> {
        self.entered.store(now(), Ordering::Release);
        Leave(self)
    }

    /// `checkExpiration(timeout, reset)`: whether the path has been in
    /// for longer than `timeout`; an expired path is reset.
    fn expired(&self, timeout: Duration) -> bool {
        let entered = self.entered.load(Ordering::Acquire);
        if entered == 0 {
            return false;
        }
        let expired = now().saturating_sub(entered) > timeout.as_nanos() as u64;
        if expired {
            let _ = self
                .entered
                .compare_exchange(entered, 0, Ordering::AcqRel, Ordering::Relaxed);
        }
        expired
    }
}

/// Leaves a critical path when dropped (`leaveCritical`).
pub struct Leave<'a>(&'a Measure);

impl Drop for Leave<'_> {
    fn drop(&mut self) {
        self.0.entered.store(0, Ordering::Release);
    }
}

/// Starts the analyzer for a broker, if it is enabled.
pub(crate) fn start(broker: &Arc<Broker>, config: CriticalConfig) {
    if !config.enabled {
        return;
    }
    let weak = Arc::downgrade(broker);
    std::thread::Builder::new()
        .name("critical-analyzer".to_owned())
        .spawn(move || {
            let mut journal_fired = 0;
            loop {
                std::thread::sleep(config.check_period);
                let Some(broker) = weak.upgrade() else { return };
                if broker.is_stopped() {
                    return;
                }
                if let Some(component) = check(&broker, config.timeout, &mut journal_fired) {
                    fire(&broker, &config, &component);
                }
            }
        })
        .ok();
}

/// `CriticalAnalyzerImpl.check`: the first expired component, by its
/// `toString`.
fn check(broker: &Broker, timeout: Duration, journal_fired: &mut u64) -> Option<String> {
    let expired = |component: &str, path: usize| {
        tracing::warn!(target: "org.apache.activemq.artemis.utils.critical", "Component {component} is expired on path {path}");
    };
    if let Some(storage) = broker.storage()
        && let Some((took, id)) = storage.writing()
        && took > timeout
        && *journal_fired != id
    {
        *journal_fired = id;
        let name = "org.apache.activemq.artemis.core.io.buffer.TimedBuffer";
        expired(name, 0);
        return Some(format!("{name}@{:x}", id as u32 & 0x7fff_ffff));
    }
    for q in broker.post_office().queues() {
        if q.critical().expired(timeout) {
            expired("org.apache.activemq.artemis.core.server.impl.QueueImpl", 0);
            return Some(crate::audit::queue_display(broker, &q));
        }
    }
    for s in broker.registry().sessions() {
        if s.critical.expired(timeout) {
            expired(
                "org.apache.activemq.artemis.core.server.impl.ServerSessionImpl",
                0,
            );
            return Some(format!("ServerSessionImpl(name={})", s.name));
        }
    }
    None
}

/// The policy's action (`initializeCriticalAnalyzer`).
fn fire(broker: &Arc<Broker>, config: &CriticalConfig, component: &str) {
    if !tracing::enabled!(target: "org.apache.activemq.artemis.utils.critical", tracing::Level::TRACE)
    {
        tracing::info!(
            "AMQ224107: The Critical Analyzer detected slow paths on the broker.  It is recommended that you enable trace logs on {LOGGER} while you troubleshoot this issue. You should disable the trace logs when you have finished troubleshooting."
        );
    }
    match config.policy {
        CriticalAnalyzerPolicy::Halt => tracing::error!(
            "AMQ224079: The process for the virtual machine will be killed, as component {component} is not responsive"
        ),
        CriticalAnalyzerPolicy::Shutdown => tracing::error!(
            "AMQ224080: The server process will now be stopped, as component {component} is not responsive"
        ),
        CriticalAnalyzerPolicy::Log => {
            tracing::error!("AMQ224081: The component {component} is not responsive")
        }
    }
    tracing::warn!(
        "AMQ222199: Thread dump: {}",
        thread_dump(broker, config.timeout)
    );
    match config.policy {
        // Runtime.halt(70): no shutdown, no hooks.
        CriticalAnalyzerPolicy::Halt => std::process::exit(70),
        CriticalAnalyzerPolicy::Shutdown => broker.request_shutdown(),
        CriticalAnalyzerPolicy::Log => {}
    }
}

/// `threadDump`: Saranga cannot dump its threads, so it lists what is in a
/// critical path and for how long.
fn thread_dump(broker: &Broker, timeout: Duration) -> String {
    let mut s = String::from(
        "\n*******************************************************************************\nThe components in a critical path\n",
    );
    let line = |s: &mut String, what: &str, entered: u64| {
        if entered != 0 {
            let ms = now().saturating_sub(entered) / 1_000_000;
            s.push_str(&format!("{what}: {ms} ms\n"));
        }
    };
    if let Some((took, _)) = broker.storage().and_then(|s| s.writing()) {
        s.push_str(&format!("journal write: {} ms\n", took.as_millis()));
    }
    for q in broker.post_office().queues() {
        line(
            &mut s,
            &format!("queue {}", q.name()),
            q.critical().entered.load(Ordering::Acquire),
        );
    }
    for e in broker.registry().sessions() {
        line(
            &mut s,
            &format!("session {}", e.name),
            e.critical.entered.load(Ordering::Acquire),
        );
    }
    s.push_str(&format!(
        "(critical-analyzer-timeout {} ms)\n*******************************************************************************",
        timeout.as_millis()
    ));
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measures_expire_once() {
        let m = Measure::default();
        assert!(!m.expired(Duration::ZERO));
        let guard = m.enter();
        std::thread::sleep(Duration::from_millis(5));
        assert!(!m.expired(Duration::from_secs(10)));
        assert!(m.expired(Duration::from_millis(1)));
        // Reset: reported once.
        assert!(!m.expired(Duration::from_millis(1)));
        drop(guard);
        assert!(!m.expired(Duration::ZERO));
    }
}
