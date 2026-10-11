//! The critical analyzer: a queue stuck in its critical path past the
//! timeout makes the SHUTDOWN policy ask for the broker to stop.

use std::sync::Arc;
use std::time::Duration;

use saranga_config::CriticalAnalyzerPolicy;
use saranga_server::critical::CriticalConfig;
use saranga_server::postoffice::{QueueSpec, SelectorFilters};
use saranga_server::{Broker, BrokerConfig};

async fn start(policy: CriticalAnalyzerPolicy, enabled: bool) -> Arc<Broker> {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.critical = CriticalConfig {
        enabled,
        timeout: Duration::from_millis(200),
        check_period: Duration::from_millis(50),
        policy,
    };
    Broker::start(config).await.unwrap()
}

#[tokio::test]
async fn a_stuck_queue_stops_the_broker() {
    let broker = start(CriticalAnalyzerPolicy::Shutdown, true).await;
    let queue = broker
        .post_office()
        .create_queue(QueueSpec::new("stuck", "stuck"), false)
        .unwrap();
    let _stuck = queue.critical().enter();
    tokio::time::timeout(Duration::from_secs(5), broker.shutdown_requested())
        .await
        .expect("the analyzer asks for a shutdown");
}

#[tokio::test]
async fn a_path_left_in_time_is_not_reported() {
    let broker = start(CriticalAnalyzerPolicy::Shutdown, true).await;
    let queue = broker
        .post_office()
        .create_queue(QueueSpec::new("busy", "busy"), false)
        .unwrap();
    for _ in 0..10 {
        let _working = queue.critical().enter();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(500), broker.shutdown_requested())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_disabled_analyzer_does_nothing() {
    let broker = start(CriticalAnalyzerPolicy::Shutdown, false).await;
    let queue = broker
        .post_office()
        .create_queue(QueueSpec::new("stuck", "stuck"), false)
        .unwrap();
    let _stuck = queue.critical().enter();
    assert!(
        tokio::time::timeout(Duration::from_millis(600), broker.shutdown_requested())
            .await
            .is_err()
    );
}
