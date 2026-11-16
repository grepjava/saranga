//! Connection router settings Artemis refuses are refused: a negative
//! cache timeout (Caffeine's `expireAfterAccess`) fails the deployment,
//! where it used to expire every entry at once. The failed start stops
//! what it had started.

use std::sync::Arc;

use saranga_config::configuration::{CacheConfiguration, ConnectionRouterConfiguration};
use saranga_server::postoffice::SelectorFilters;
use saranga_server::{Broker, BrokerConfig};

#[tokio::test]
async fn a_negative_cache_timeout_is_refused() {
    let mut config = BrokerConfig::new(Arc::new(SelectorFilters));
    config.connection_routers = vec![ConnectionRouterConfiguration {
        name: "r".into(),
        key_type: "CLIENT_ID".into(),
        key_filter: None,
        local_target_filter: None,
        cache_configuration: Some(CacheConfiguration {
            persisted: false,
            timeout: -1,
        }),
        pool_configuration: None,
        policy_configuration: None,
    }];
    let e = Broker::start(config).await.err().expect("refused");
    assert!(
        e.to_string().contains("duration cannot be negative: -1"),
        "{e}"
    );
}
