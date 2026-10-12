//! The disk monitor: past `min-disk-free`, addresses refuse or drop what is
//! sent as their `disk-full-policy` says.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use saranga_config as cfg;
use saranga_server::disk::{self, Limit, MonitorConfig};
use saranga_server::management::Json;
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Broker, BrokerConfig};

async fn start(dir: &Path, min_disk_free: &str) -> Arc<Broker> {
    let file = cfg::BrokerConfig::parse(&format!(
        r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
             <persistence-enabled>true</persistence-enabled>
             <min-disk-free>{min_disk_free}</min-disk-free>
             <disk-scan-period>50</disk-scan-period>
             <address-settings>
               <address-setting match="failing"><disk-full-policy>FAIL</disk-full-policy></address-setting>
               <address-setting match="dropping"><disk-full-policy>DROP</disk-full-policy></address-setting>
             </address-settings>
           </core></configuration>"#
    ))
    .unwrap();
    let config = BrokerConfig::from_file_config(&file, Arc::new(SelectorFilters), dir).unwrap();
    Broker::start(config).await.unwrap()
}

async fn op(
    broker: &Arc<Broker>,
    resource: &str,
    operation: &str,
    params: Vec<Json>,
) -> Result<Json, String> {
    broker
        .management()
        .invoke(broker, resource, operation, &params, Default::default())
        .await
        .map_err(|e| e.to_string())
}

fn s(v: &str) -> Json {
    Json::from(v)
}

async fn queue(broker: &Arc<Broker>, name: &str) {
    op(
        broker,
        "broker",
        "createQueue",
        vec![
            s(name),
            s("ANYCAST"),
            s(name),
            Json::Null,
            Json::Bool(true),
            Json::Long(-1),
            Json::Bool(false),
            Json::Bool(true),
        ],
    )
    .await
    .unwrap();
}

async fn send(broker: &Arc<Broker>, address: &str) -> Result<Json, String> {
    op(
        broker,
        &format!("address.{address}"),
        "sendMessage",
        vec![
            Json::Object(vec![]),
            Json::Long(3),
            s("body"),
            Json::Bool(true),
            Json::Null,
            Json::Null,
        ],
    )
    .await
}

async fn count(broker: &Arc<Broker>, queue: &str) -> Json {
    op(broker, &format!("queue.{queue}"), "countMessages", vec![])
        .await
        .unwrap()
}

async fn wait_disk_full(broker: &Arc<Broker>, full: bool) {
    for _ in 0..100 {
        if broker.post_office().paging().is_disk_full() == full {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "the disk never became {}",
        if full { "full" } else { "free" }
    );
}

#[tokio::test]
async fn full_disk_fails_or_drops_by_policy() {
    let dir = tempfile::tempdir().unwrap();
    // More free space than any disk has.
    let broker = start(dir.path(), "1000000000000000000").await;
    for q in ["failing", "dropping"] {
        queue(&broker, q).await;
    }
    wait_disk_full(&broker, true).await;

    let e = send(&broker, "failing").await.unwrap_err();
    assert!(e.contains("AMQ229102"), "{e}");
    send(&broker, "dropping").await.unwrap();
    assert_eq!(count(&broker, "failing").await, Json::Long(0));
    assert_eq!(count(&broker, "dropping").await, Json::Long(0));
    broker.stop().unwrap();
}

#[tokio::test]
async fn disk_with_room_takes_messages() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start(dir.path(), "1").await;
    queue(&broker, "failing").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    wait_disk_full(&broker, false).await;
    send(&broker, "failing").await.unwrap();
    assert_eq!(count(&broker, "failing").await, Json::Long(1));
    broker.stop().unwrap();
}

#[test]
fn monitor_config_as_deploy_file_store_monitor() {
    assert_eq!(
        MonitorConfig::new(90, -1, 5000),
        Some(MonitorConfig {
            limit: Limit::MaxUsage(0.9),
            scan_period: 5000
        })
    );
    assert_eq!(
        MonitorConfig::new(90, 1024, 5000).map(|m| m.limit),
        Some(Limit::MinFree(1024))
    );
    assert_eq!(MonitorConfig::new(-1, -1, 5000), None);
    assert_eq!(MonitorConfig::new(90, -1, -1), None);
}

#[test]
fn human_bytes_as_byte_util() {
    assert_eq!(disk::human_bytes(0), "0B");
    assert_eq!(disk::human_bytes(999), "999.0B");
    assert_eq!(disk::human_bytes(1000), "1.0KB");
    assert_eq!(disk::human_bytes(31_400_000_000), "31.4GB");
    assert_eq!(disk::human_bytes(935_000_000_000), "935.0GB");
}
