//! The security store's caches: a grant adds to the login's set in place
//! (Artemis's `ConcurrentHashSet.add`), so many addresses cost linear time,
//! and a login a permission check has to make again does not hold up the
//! runtime worker it runs on.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use saranga_config::{BrokerConfig, CheckType, HierarchicalRepository, Role, SecuritySetting};
use saranga_security::manager::{NoCache, authorize_roles};
use saranga_security::{
    ConnectionInfo, Principal, SecurityAuth, SecurityManager, SecurityStore, SecurityStoreConfig,
    Subject,
};

struct Slow {
    logins: AtomicUsize,
    delay: Duration,
}

impl SecurityManager for Slow {
    fn authenticate(
        &self,
        user: Option<&str>,
        _password: Option<&str>,
        _: &ConnectionInfo,
    ) -> Result<Option<Subject>, NoCache> {
        self.logins.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(self.delay);
        let mut s = Subject::default();
        s.add(Principal::User(user.unwrap_or("x").into()));
        s.add(Principal::Role("a".into()));
        Ok(Some(s))
    }

    fn authorize(&self, subject: &Subject, roles: &[Role], check: CheckType, _: &str) -> bool {
        authorize_roles(subject, roles, check)
    }
}

fn store(ttl: Duration, delay: Duration) -> (SecurityStore, Arc<Slow>) {
    let mut config = SecurityStoreConfig::from_broker(&BrokerConfig::default());
    config.enabled = true;
    config.invalidation_interval = ttl;
    let mut repo = HierarchicalRepository::new(Default::default());
    repo.set_default(Some(SecuritySetting::default()));
    repo.add_match(
        "#",
        SecuritySetting {
            roles: vec![Role::new("a", [CheckType::Send])],
        },
    )
    .unwrap();
    let m = Arc::new(Slow {
        logins: AtomicUsize::new(0),
        delay,
    });
    (SecurityStore::new(config, repo, m.clone()), m)
}

fn auth() -> SecurityAuth {
    SecurityAuth {
        username: Some("u".into()),
        password: Some("pw".into()),
        connection: Arc::new(ConnectionInfo::default()),
        subject: None,
        management: false,
        credential: Default::default(),
    }
}

fn grants(store: &SecurityStore, n: usize) -> Duration {
    store.invalidate_authorization_cache();
    let a = auth();
    let t = Instant::now();
    for i in 0..n {
        assert!(store.has_permission(&format!("addr.{i}"), None, CheckType::Send, &a));
    }
    t.elapsed()
}

#[test]
fn grants_to_many_addresses_take_linear_time() {
    let (store, _) = store(Duration::from_secs(600), Duration::ZERO);
    grants(&store, 1000);
    let small = grants(&store, 1000);
    let large = grants(&store, 8000);
    // Eight times the addresses: about eight times the time, not 64.
    let ratio = large.as_secs_f64() / small.as_secs_f64().max(1e-6);
    assert!(ratio < 24.0, "1000 grants {small:?}, 8000 grants {large:?}");
}

/// A permission check after the authentication cache expired logs in again,
/// and the worker's other tasks go on meanwhile.
#[test]
fn a_login_again_does_not_hold_up_the_worker() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_time()
        .build()
        .unwrap();
    let (store, m) = store(Duration::from_millis(100), Duration::from_millis(1000));
    let store = Arc::new(store);
    let a = auth();
    store
        .authenticate(Some("u"), Some("pw"), &ConnectionInfo::default())
        .unwrap();
    std::thread::sleep(Duration::from_millis(200));
    let ticks = Arc::new(AtomicUsize::new(0));
    runtime.block_on(async {
        let t = ticks.clone();
        let ticker = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                t.fetch_add(1, Ordering::SeqCst);
            }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let s = store.clone();
        let check = tokio::spawn(async move {
            let before = Instant::now();
            assert!(s.check("q", None, CheckType::Send, &a).is_ok());
            before.elapsed()
        });
        let started = ticks.load(Ordering::SeqCst);
        let took = check.await.unwrap();
        let during = ticks.load(Ordering::SeqCst) - started;
        ticker.abort();
        assert_eq!(m.logins.load(Ordering::SeqCst), 2, "logged in again");
        assert!(took >= Duration::from_millis(1000));
        assert!(
            during > 20,
            "only {during} ticks while the login took {took:?}"
        );
    });
}
