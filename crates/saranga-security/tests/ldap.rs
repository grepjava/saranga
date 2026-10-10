//! `LDAPLoginModule` against a real directory: the OpenLDAP container of
//! scripts/ldap-interop.sh, loaded with tests/interop/ldap/users.ldif.
//! Skipped unless `LDAP_URL` is set, e.g. `LDAP_URL=ldap://127.0.0.1:3890`.

use saranga_security::jaas::{Callbacks, LoginConfig, ModuleFactory, login};

fn config(url: &str, extra: &str) -> Vec<saranga_security::jaas::ModuleEntry> {
    let text = format!(
        r#"activemq {{
   org.apache.activemq.artemis.spi.core.security.jaas.LDAPLoginModule required
       initialContextFactory=com.sun.jndi.ldap.LdapCtxFactory
       connectionURL="{url}"
       connectionUsername="cn=admin,dc=example,dc=com"
       connectionPassword=secret
       connectionProtocol=s
       authentication=simple
       userBase="ou=People,dc=example,dc=com"
       userSearchMatching="(uid={{0}})"
       userSearchSubtree=false
       roleBase="ou=Groups,dc=example,dc=com"
       roleName=cn
       roleSearchMatching="(member={{0}})"
       roleSearchSubtree=false
       {extra};
}};"#
    );
    LoginConfig::parse(&text, |_| None)
        .unwrap()
        .entry("activemq")
        .unwrap()
        .to_vec()
}

fn url() -> Option<String> {
    std::env::var("LDAP_URL").ok()
}

fn creds<'a>(user: &'a str, password: &'a str) -> Callbacks<'a> {
    Callbacks {
        user: Some(user),
        password: Some(password),
        ..Callbacks::default()
    }
}

fn roles(s: &saranga_security::Subject) -> Vec<&str> {
    let mut r: Vec<&str> = s.roles().collect();
    r.sort();
    r
}

#[test]
fn users_and_roles() {
    let Some(url) = url() else { return };
    let entries = config(&url, "");
    let factory = ModuleFactory::new(None);
    let s = login(&entries, &factory, &creds("jdoe", "jdoe")).unwrap();
    assert_eq!(s.user(), Some("jdoe"));
    assert_eq!(roles(&s), vec!["amq"]);
    let s = login(&entries, &factory, &creds("ann", "ann")).unwrap();
    assert_eq!(roles(&s), vec!["staff"]);
    // Filter characters in the user name are escaped.
    let s = login(&entries, &factory, &creds("x(y)*", "odd")).unwrap();
    assert_eq!(roles(&s), vec!["staff"]);

    let e = login(&entries, &factory, &creds("jdoe", "wrong")).unwrap_err();
    assert_eq!(e.message, "Password does not match for user: jdoe");
    let e = login(&entries, &factory, &creds("nobody", "x")).unwrap_err();
    assert_eq!(e.message, "User nobody not found in LDAP.");
    let e = login(&entries, &factory, &creds("jdoe", "")).unwrap_err();
    assert_eq!(e.message, "Password cannot be null or empty");
}

#[test]
fn nested_groups() {
    let Some(url) = url() else { return };
    let entries = config(
        &url,
        "expandRoles=true expandRolesMatching=\"(member={0})\"",
    );
    let factory = ModuleFactory::new(None);
    let s = login(&entries, &factory, &creds("ann", "ann")).unwrap();
    assert_eq!(roles(&s), vec!["senders", "staff"]);
}

#[test]
fn unreachable_directory() {
    if url().is_none() {
        return;
    }
    let factory = ModuleFactory::new(None);
    let entries = config("ldap://127.0.0.1:1", "");
    let e = login(&entries, &factory, &creds("jdoe", "jdoe")).unwrap_err();
    assert_eq!(e.message, "Error opening LDAP connection");
    assert!(!e.no_cache);
    let entries = config(
        "ldap://127.0.0.1:1",
        "noCacheExceptions=\"java.net.ConnectException\"",
    );
    let e = login(&entries, &factory, &creds("jdoe", "jdoe")).unwrap_err();
    assert!(e.no_cache, "{e:?}");
    assert!(
        e.message.starts_with("java.net.ConnectException"),
        "{}",
        e.message
    );
    // Wrong service account password.
    let entries = config(&url().unwrap(), "").into_iter().map(|mut m| {
        m.options.retain(|(k, _)| k != "connectionPassword");
        m.options.push(("connectionPassword".into(), "nope".into()));
        m
    });
    let entries: Vec<_> = entries.collect();
    let e = login(&entries, &factory, &creds("jdoe", "jdoe")).unwrap_err();
    assert_eq!(e.message, "Error opening LDAP connection");
}
