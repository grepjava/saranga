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
        management: false,
        credential: Default::default(),
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
        "AMQ229031: Unable to validate user from 127.0.0.1:5000. Username: u1; SSL certificate subject DN: unavailable"
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

/// Like `GuestLoginModule` with `credentialsInvalidate=true` and nothing
/// after it: only a client that sends no credentials at all is the guest.
struct GuestOnlyWithoutCredentials;

impl SecurityManager for GuestOnlyWithoutCredentials {
    fn authenticate(
        &self,
        user: Option<&str>,
        password: Option<&str>,
        _: &ConnectionInfo,
    ) -> Result<Option<Subject>, NoCache> {
        if user.is_some() || password.is_some() {
            return Ok(None);
        }
        let mut s = Subject::default();
        s.add(Principal::User("guest".into()));
        s.add(Principal::Role("a".into()));
        Ok(Some(s))
    }

    fn authorize(&self, subject: &Subject, roles: &[Role], check: CheckType, _: &str) -> bool {
        authorize_roles(subject, roles, check)
    }
}

#[test]
fn a_cached_anonymous_login_does_not_admit_empty_credentials() {
    let mut config = SecurityStoreConfig::from_broker(&BrokerConfig::default());
    config.enabled = true;
    let store = SecurityStore::new(
        config,
        HierarchicalRepository::new(Default::default()),
        Arc::new(GuestOnlyWithoutCredentials),
    );
    let conn = ConnectionInfo::default();
    let guest = store.authenticate(None, None, &conn).unwrap();
    assert_eq!(guest.validated_user.as_deref(), Some("guest"));
    // Refused when asked first, so refused after the anonymous login too.
    for (user, password) in [(None, Some("")), (Some(""), None), (Some(""), Some(""))] {
        assert!(
            store.authenticate(user, password, &conn).is_err(),
            "{user:?}/{password:?} must not reuse the anonymous login"
        );
    }
    // Fields whose bytes run together differently do not share a key.
    assert_ne!(
        authentication_cache_key(Some("a\0"), Some(""), &conn),
        authentication_cache_key(Some("a"), Some("\0"), &conn)
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
        management: false,
        credential: Default::default(),
    };
    let e = store.check("x", None, CheckType::Send, &a).unwrap_err();
    assert!(e.0.starts_with("AMQ229032: User: guest does"), "{e}");
}

#[test]
fn a_management_caller_is_authorized_as_its_subject() {
    let (store, _) = store(true);
    let mut subject = Subject::default();
    subject.add(Principal::User("web".into()));
    subject.add(Principal::Role("a".into()));
    let mut a = SecurityAuth {
        subject: Some(Arc::new(subject)),
        ..SecurityAuth::default()
    };
    // Otherwise the store logs in again, without credentials.
    assert!(store.check("x", None, CheckType::Send, &a).is_err());
    a.management = true;
    store.check("x", None, CheckType::Send, &a).unwrap();
    let e = store.check("x", None, CheckType::Manage, &a).unwrap_err();
    assert!(e.0.starts_with("AMQ229032: User: web does"), "{e}");
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

/// The same user name holds role admins in realm admin, guests in
/// restricted, and cannot log in to any other.
struct Realms;

impl SecurityManager for Realms {
    fn authenticate(
        &self,
        user: Option<&str>,
        _: Option<&str>,
        connection: &ConnectionInfo,
    ) -> Result<Option<Subject>, NoCache> {
        let role = match connection.security_domain.as_deref() {
            Some("admin") => "admins",
            Some("restricted") => "guests",
            _ => return Ok(None),
        };
        let mut s = Subject::default();
        s.add(Principal::User(user.unwrap_or("guest").into()));
        s.add(Principal::Role(role.into()));
        Ok(Some(s))
    }

    fn authorize(&self, subject: &Subject, roles: &[Role], check: CheckType, _: &str) -> bool {
        authorize_roles(subject, roles, check)
    }
}

fn realm_store(authentication_cache: usize, authorization_cache: usize) -> SecurityStore {
    let mut cfg = SecurityStoreConfig::from_broker(&BrokerConfig::default());
    cfg.enabled = true;
    cfg.authentication_cache_size = authentication_cache;
    cfg.authorization_cache_size = authorization_cache;
    let mut repo = HierarchicalRepository::new(Default::default());
    repo.add_match(
        "secret",
        SecuritySetting {
            roles: vec![Role::new("admins", [CheckType::Send])],
        },
    )
    .unwrap();
    SecurityStore::new(cfg, repo, Arc::new(Realms))
}

fn in_realm(realm: &str) -> SecurityAuth {
    SecurityAuth {
        username: Some("same".into()),
        connection: Arc::new(ConnectionInfo {
            security_domain: Some(realm.into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// A login module can bind a login to the client certificate,
/// so a cached login holds only for the same certificate, not
/// for another one with the same subject.
#[test]
fn a_cached_login_holds_only_for_its_certificate() {
    let (store, manager) = store(true);
    let with = |der: u8| ConnectionInfo {
        certificates: Some(Arc::from(vec![crate::jaas::PeerCertificate {
            der: vec![der; 8],
            subject_dn: "CN=client".into(),
            subject_dn_legacy: "CN=client".into(),
            san_uris: Vec::new(),
        }])),
        ..Default::default()
    };
    store
        .authenticate(Some("u1"), Some("pw"), &with(1))
        .unwrap();
    store
        .authenticate(Some("u1"), Some("pw"), &with(1))
        .unwrap();
    assert_eq!(manager.logins.load(Ordering::SeqCst), 1, "same certificate");
    store
        .authenticate(Some("u1"), Some("pw"), &with(2))
        .unwrap();
    assert_eq!(
        manager.logins.load(Ordering::SeqCst),
        2,
        "same subject, another certificate"
    );
}

#[test]
fn a_cached_login_holds_only_in_its_realm() {
    let store = realm_store(100, 0);
    let send = |realm| store.has_permission("secret", None, CheckType::Send, &in_realm(realm));
    assert!(send("admin"));
    assert!(!send("restricted"));
    assert!(!send("missing"));
    assert!(send("admin"));
}

#[test]
fn a_cached_grant_holds_only_in_its_realm() {
    let store = realm_store(0, 100);
    let send = |realm| store.has_permission("secret", None, CheckType::Send, &in_realm(realm));
    assert!(!send("restricted"));
    assert!(send("admin"));
    assert!(!send("restricted"));
    assert_eq!(store.authorization_cache_size(), 1);
}

#[test]
fn a_cached_grant_holds_only_for_its_sasl_identity() {
    let store = realm_store(0, 100);
    let sasl = |name: &str| SecurityAuth {
        connection: Arc::new(ConnectionInfo {
            security_domain: Some("admin".into()),
            sasl_principals: Some(Arc::from([Principal::User(name.into())])),
            ..Default::default()
        }),
        ..in_realm("admin")
    };
    assert!(store.has_permission("secret", None, CheckType::Send, &sasl("a")));
    assert!(store.has_permission("secret", None, CheckType::Send, &sasl("b")));
    assert_eq!(store.authorization_cache_size(), 2);
}

/// A token login: any user name, the token decides the roles.
struct Tokens;

impl SecurityManager for Tokens {
    fn authenticate(
        &self,
        _: Option<&str>,
        password: Option<&str>,
        _: &ConnectionInfo,
    ) -> Result<Option<Subject>, NoCache> {
        let role = match password {
            Some("admin-token") => "a",
            Some("viewer-token") => "viewer",
            _ => return Ok(None),
        };
        let mut s = Subject::default();
        s.add(Principal::User("from-token".into()));
        s.add(Principal::Role(role.into()));
        Ok(Some(s))
    }

    fn authorize(&self, subject: &Subject, roles: &[Role], check: CheckType, _: &str) -> bool {
        authorize_roles(subject, roles, check)
    }
}

/// A grant is cached for the credentials it was made for, not the user name
/// a client gives: two tokens under one name keep their own roles.
#[test]
fn a_cached_grant_holds_only_for_its_credentials() {
    let mut config = SecurityStoreConfig::from_broker(&BrokerConfig::default());
    config.enabled = true;
    let mut repo = HierarchicalRepository::new(Default::default());
    repo.add_match(
        "#",
        SecuritySetting {
            roles: vec![Role::new("a", [CheckType::Send])],
        },
    )
    .unwrap();
    let store = SecurityStore::new(config, repo, Arc::new(Tokens));
    let auth = |token: &str| SecurityAuth {
        username: Some("same-name".into()),
        password: Some(token.into()),
        ..Default::default()
    };
    let (admin, viewer) = (auth("admin-token"), auth("viewer-token"));
    assert!(store.has_permission("q", None, CheckType::Send, &admin));
    assert!(
        !store.has_permission("q", None, CheckType::Send, &viewer),
        "the viewer used the admin's cached grant"
    );
    assert!(
        store.has_permission("q", None, CheckType::Send, &admin),
        "still cached"
    );
}

/// [`Fixed`], taking every permission away the first time it authorizes:
/// the settings change while that check is in flight.
struct Revoking {
    inner: Fixed,
    store: std::sync::OnceLock<std::sync::Weak<SecurityStore>>,
    fired: std::sync::atomic::AtomicBool,
}

impl SecurityManager for Revoking {
    fn authenticate(
        &self,
        user: Option<&str>,
        password: Option<&str>,
        conn: &ConnectionInfo,
    ) -> Result<Option<Subject>, NoCache> {
        self.inner.authenticate(user, password, conn)
    }

    fn authorize(&self, subject: &Subject, roles: &[Role], check: CheckType, _: &str) -> bool {
        let granted = authorize_roles(subject, roles, check);
        if !self.fired.swap(true, Ordering::SeqCst)
            && let Some(store) = self.store.get().and_then(std::sync::Weak::upgrade)
        {
            store.set_repository({
                let mut repo = HierarchicalRepository::new(Default::default());
                repo.set_default(Some(SecuritySetting::default()));
                repo
            });
        }
        granted
    }
}

/// A grant read from settings that changed before it is cached is not
/// cached: the change's revocation holds at once.
#[test]
fn a_grant_from_settings_changed_meanwhile_is_not_cached() {
    let mut config = SecurityStoreConfig::from_broker(&BrokerConfig::default());
    config.enabled = true;
    let mut repo = HierarchicalRepository::new(Default::default());
    repo.set_default(Some(SecuritySetting::default()));
    repo.add_match(
        "#",
        SecuritySetting {
            roles: vec![Role::new("a", [CheckType::Send])],
        },
    )
    .unwrap();
    let manager = Arc::new(Revoking {
        inner: Fixed {
            logins: AtomicUsize::new(0),
        },
        store: Default::default(),
        fired: Default::default(),
    });
    let store = Arc::new(SecurityStore::new(config, repo, manager.clone()));
    manager.store.set(Arc::downgrade(&store)).unwrap();
    // Granted by the settings it read; they change before it is cached.
    store
        .check("orders", None, CheckType::Send, &auth("u1"))
        .unwrap();
    assert!(
        store
            .check("orders", None, CheckType::Send, &auth("u1"))
            .is_err(),
        "the revoked permission is not served from the cache"
    );
}
