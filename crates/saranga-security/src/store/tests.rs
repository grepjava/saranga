use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use saranga_config::{BrokerConfig, CheckType, HierarchicalRepository, Role, SecuritySetting};

use super::*;
use crate::jaas::{Principal, Subject};
use crate::manager::{ConnectionInfo, NoCache, SecurityManager, authorize_roles};

/// Users `u1` (roles a) and `u2` (roles a, b), password `pw`; counts
/// logins.
struct Fixed {
    logins: AtomicUsize,
}

impl SecurityManager for Fixed {
    fn authenticate(
        &self,
        user: Option<&str>,
        password: Option<&str>,
        _: &ConnectionInfo,
    ) -> Result<Option<Subject>, NoCache> {
        self.logins.fetch_add(1, Ordering::SeqCst);
        if password != Some("pw") {
            return Ok(None);
        }
        let roles: &[&str] = match user {
            Some("u1") => &["a"],
            Some("u2") => &["a", "b"],
            Some("nouser") => {
                let mut s = Subject::default();
                s.add(Principal::Role("a".into()));
                return Ok(Some(s));
            }
            _ => return Ok(None),
        };
        let mut s = Subject::default();
        s.add(Principal::User(user.unwrap().into()));
        for r in roles {
            s.add(Principal::Role((*r).into()));
        }
        Ok(Some(s))
    }

    fn authorize(&self, subject: &Subject, roles: &[Role], check: CheckType, _: &str) -> bool {
        authorize_roles(subject, roles, check)
    }
}

fn store(enabled: bool) -> (SecurityStore, Arc<Fixed>) {
    let mut config = SecurityStoreConfig::from_broker(&BrokerConfig::default());
    config.enabled = enabled;
    config.cluster_user = "cluster".into();
    config.cluster_password = "cpw".into();
    let mut repo = HierarchicalRepository::new(Default::default());
    repo.set_default(Some(SecuritySetting::default()));
    let all = |name: &str| Role::new(name, CheckType::ALL);
    repo.add_match(
        "#",
        SecuritySetting {
            roles: vec![Role::new("a", [CheckType::Send, CheckType::Consume])],
        },
    )
    .unwrap();
    repo.add_match(
        "b.#",
        SecuritySetting {
            roles: vec![all("b")],
        },
    )
    .unwrap();
    repo.add_match(
        "q.addr::special",
        SecuritySetting {
            roles: vec![Role::new("b", [CheckType::Consume])],
        },
    )
    .unwrap();
    let manager = Arc::new(Fixed {
        logins: AtomicUsize::new(0),
    });
    (SecurityStore::new(config, repo, manager.clone()), manager)
}

fn auth(user: &str) -> SecurityAuth {
    SecurityAuth {
        username: Some(user.into()),
        password: Some("pw".into()),
        connection: Arc::new(ConnectionInfo {
            remote_address: "/127.0.0.1:5000".into(),
            ..Default::default()
        }),
        subject: None,
    }
}

#[test]
fn authenticate_and_cache() {
    let (store, manager) = store(true);
    let conn = ConnectionInfo {
        remote_address: "/127.0.0.1:5000".into(),
        ..Default::default()
    };
    let ok = store.authenticate(Some("u1"), Some("pw"), &conn).unwrap();
    assert_eq!(ok.validated_user.as_deref(), Some("u1"));
    store.authenticate(Some("u1"), Some("pw"), &conn).unwrap();
    assert_eq!(manager.logins.load(Ordering::SeqCst), 1, "cached");

    let e = store
        .authenticate(Some("u1"), Some("bad"), &conn)
        .unwrap_err();
    assert_eq!(
        e.0,
        "AMQ229031: Unable to validate user from /127.0.0.1:5000. Username: u1; SSL certificate subject DN: unavailable"
    );
    // A subject without a user principal is refused.
    assert!(
        store
            .authenticate(Some("nouser"), Some("pw"), &conn)
            .is_err()
    );
    assert_eq!(
        store
            .counters
            .authentication_failure
            .load(Ordering::Relaxed),
        2
    );
}

#[test]
fn cluster_credentials() {
    let (store, _) = store(true);
    let conn = ConnectionInfo::default();
    let ok = store
        .authenticate(Some("cluster"), Some("cpw"), &conn)
        .unwrap();
    assert_eq!(ok.validated_user.as_deref(), Some("cluster"));
    let e = store
        .authenticate(Some("cluster"), Some("x"), &conn)
        .unwrap_err();
    assert!(
        e.0.contains("from null. Username: cluster; SSL certificate subject DN: null"),
        "{e}"
    );
    assert!(
        store
            .authenticate(
                Some(BrokerConfig::DEFAULT_CLUSTER_USER),
                Some(BrokerConfig::DEFAULT_CLUSTER_PASSWORD),
                &conn
            )
            .is_err()
    );
}

#[test]
fn disabled_allows_everything() {
    let (store, manager) = store(false);
    let conn = ConnectionInfo::default();
    assert!(
        store
            .authenticate(None, None, &conn)
            .unwrap()
            .validated_user
            .is_none()
    );
    store
        .check("x", None, CheckType::Manage, &auth("nobody"))
        .unwrap();
    assert_eq!(manager.logins.load(Ordering::SeqCst), 0);
}

#[test]
fn permissions_follow_the_most_specific_match() {
    let (store, _) = store(true);
    store
        .check("orders", None, CheckType::Send, &auth("u1"))
        .unwrap();
    let e = store
        .check("orders", None, CheckType::CreateDurableQueue, &auth("u1"))
        .unwrap_err();
    assert_eq!(
        e.0,
        "AMQ229032: User: u1 does not have permission='CREATE_DURABLE_QUEUE' on address orders"
    );
    // `b.#` is more specific than `#`, and only grants role b.
    let e = store
        .check("b.x", None, CheckType::Send, &auth("u1"))
        .unwrap_err();
    assert!(e.0.contains("permission='SEND' on address b.x"));
    store
        .check("b.x", None, CheckType::Manage, &auth("u2"))
        .unwrap();
}

#[test]
fn fully_qualified_queue_settings() {
    let (store, _) = store(true);
    // An exact FQQN match replaces the address's settings for that queue.
    let e = store
        .check("q.addr", Some("special"), CheckType::Consume, &auth("u1"))
        .unwrap_err();
    assert_eq!(
        e.0,
        "AMQ229213: User: u1 does not have permission='CONSUME' for queue special on address q.addr"
    );
    store
        .check("q.addr", Some("special"), CheckType::Consume, &auth("u2"))
        .unwrap();
    store
        .check("q.addr", Some("other"), CheckType::Consume, &auth("u1"))
        .unwrap();
}

#[test]
fn caller_falls_back_to_the_subject() {
    let (store, _) = store(true);
    let mut subject = Subject::default();
    subject.add(Principal::User("guest".into()));
    let a = SecurityAuth {
        username: None,
        password: None,
        connection: Arc::default(),
        subject: Some(Arc::new(subject)),
    };
    let e = store.check("x", None, CheckType::Send, &a).unwrap_err();
    assert!(e.0.starts_with("AMQ229032: User: guest does"), "{e}");
}

#[test]
fn granted_checks_are_cached_and_reload_clears_them() {
    let (store, _) = store(true);
    store
        .check("orders", None, CheckType::Send, &auth("u1"))
        .unwrap();
    // Take `#`'s permissions away; the cached grant still holds until the
    // settings change through the store.
    store.set_repository({
        let mut repo = HierarchicalRepository::new(Default::default());
        repo.set_default(Some(SecuritySetting::default()));
        repo
    });
    assert!(
        store
            .check("orders", None, CheckType::Send, &auth("u1"))
            .is_err()
    );
}

#[test]
fn ttl_cache_expires_and_evicts() {
    let cache: TtlCache<u32, u32> = TtlCache::new(2, Duration::from_millis(50));
    cache.put(1, 1);
    cache.put(2, 2);
    cache.put(3, 3);
    assert_eq!(cache.get(&1), None, "oldest evicted");
    assert_eq!(cache.get(&3), Some(3));
    std::thread::sleep(Duration::from_millis(60));
    assert_eq!(cache.get(&3), None, "expired");
}
