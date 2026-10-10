use std::path::Path;

use super::*;

const PROPS: &str = "org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule";
const GUEST: &str = "org.apache.activemq.artemis.spi.core.security.jaas.GuestLoginModule";

/// `artemis create --user admin --password secret`.
const USERS: &str = "admin = ENC(1024:C64C51B47FE4EF732634D1434FD3D8C6091321464F97D4D03C031C85C051BBBF:9418F3F3A278B5532F8A87AD4E578A7D0B142AFA919DD900FD220917221E9B48A78D724BE8712D4E66B5E983E3EC4DABA5991BD85468EF214241B76B4108A5C9)\nplain = pw\n";
const ROLES: &str = "amq = admin\nops = plain,admin\n";

fn etc(dir: &Path) {
    std::fs::write(dir.join("artemis-users.properties"), USERS).unwrap();
    std::fs::write(dir.join("artemis-roles.properties"), ROLES).unwrap();
}

fn config(text: &str) -> Vec<ModuleEntry> {
    LoginConfig::parse(text, |_| None)
        .unwrap()
        .entry("activemq")
        .unwrap()
        .to_vec()
}

fn props(flag: &str) -> String {
    format!(
        "{PROPS} {flag} reload=true org.apache.activemq.jaas.properties.user=\"artemis-users.properties\" org.apache.activemq.jaas.properties.role=\"artemis-roles.properties\";"
    )
}

fn creds<'a>(user: Option<&'a str>, password: Option<&'a str>) -> Callbacks<'a> {
    Callbacks {
        user,
        password,
        ..Callbacks::default()
    }
}

#[test]
fn properties_required() {
    let dir = tempfile::tempdir().unwrap();
    etc(dir.path());
    let entries = config(&format!("activemq {{ {} }};", props("required")));
    let factory = ModuleFactory::new(Some(dir.path().to_path_buf()));

    let subject = login(&entries, &factory, &creds(Some("admin"), Some("secret"))).unwrap();
    assert_eq!(subject.user(), Some("admin"));
    assert_eq!(subject.roles().collect::<Vec<_>>(), vec!["amq", "ops"]);

    let subject = login(&entries, &factory, &creds(Some("plain"), Some("pw"))).unwrap();
    assert_eq!(subject.roles().collect::<Vec<_>>(), vec!["ops"]);

    let e = login(&entries, &factory, &creds(Some("admin"), Some("nope"))).unwrap_err();
    assert_eq!(e.message, "Password does not match for user: admin");
    let e = login(&entries, &factory, &creds(Some("who"), Some("x"))).unwrap_err();
    assert_eq!(e.message, "User does not exist: who");
    let e = login(&entries, &factory, &creds(None, None)).unwrap_err();
    assert_eq!(e.message, "User is null");
}

#[test]
fn anonymous_falls_back_to_guest() {
    let dir = tempfile::tempdir().unwrap();
    etc(dir.path());
    let entries = config(&format!(
        "activemq {{ {} {GUEST} sufficient org.apache.activemq.jaas.guest.user=\"admin\" org.apache.activemq.jaas.guest.role=\"amq\"; }};",
        props("sufficient")
    ));
    let factory = ModuleFactory::new(Some(dir.path().to_path_buf()));

    // Valid credentials: the properties module, then stop.
    let subject = login(&entries, &factory, &creds(Some("plain"), Some("pw"))).unwrap();
    assert_eq!(subject.user(), Some("plain"));
    assert_eq!(subject.roles().collect::<Vec<_>>(), vec!["ops"]);

    // No credentials, or wrong ones: the guest.
    for (u, p) in [(None, None), (Some("plain"), Some("bad"))] {
        let subject = login(&entries, &factory, &creds(u, p)).unwrap();
        assert_eq!(subject.user(), Some("admin"));
        assert_eq!(subject.roles().collect::<Vec<_>>(), vec!["amq"]);
    }
}

#[test]
fn guest_credentials_invalidate() {
    let entries = config(&format!(
        "activemq {{ {GUEST} required credentialsInvalidate=true; }};"
    ));
    let factory = ModuleFactory::new(None);
    let subject = login(&entries, &factory, &creds(None, None)).unwrap();
    assert_eq!(subject.user(), Some("guest"));
    assert_eq!(subject.roles().collect::<Vec<_>>(), vec!["guests"]);
    let e = login(&entries, &factory, &creds(Some("u"), Some("p"))).unwrap_err();
    assert_eq!(e.message, "Login Failure: all modules ignored");
}

#[test]
fn control_flags() {
    let dir = tempfile::tempdir().unwrap();
    etc(dir.path());
    let factory = ModuleFactory::new(Some(dir.path().to_path_buf()));
    // A failed requisite module stops the login even before a guest.
    let entries = config(&format!(
        "activemq {{ {} {GUEST} required; }};",
        props("requisite")
    ));
    assert!(login(&entries, &factory, &creds(None, None)).is_err());
    // An optional failure does not matter when something else succeeds.
    let entries = config(&format!(
        "activemq {{ {} {GUEST} required; }};",
        props("optional")
    ));
    assert_eq!(
        login(&entries, &factory, &creds(None, None))
            .unwrap()
            .user(),
        Some("guest")
    );
    // A required failure fails the login even if a later module succeeds.
    let entries = config(&format!(
        "activemq {{ {} {GUEST} sufficient; }};",
        props("required")
    ));
    assert!(login(&entries, &factory, &creds(None, None)).is_err());
    // An unknown module class fails as its flag says.
    let entries = config(&format!(
        "activemq {{ com.example.Nope optional; {GUEST} required; }};"
    ));
    assert!(login(&entries, &factory, &creds(None, None)).is_ok());
    let entries = config("activemq { com.example.Nope required; };");
    assert!(login(&entries, &factory, &creds(None, None)).is_err());
}

#[test]
fn users_file_reloads() {
    let dir = tempfile::tempdir().unwrap();
    etc(dir.path());
    let entries = config(&format!("activemq {{ {} }};", props("required")));
    let factory = ModuleFactory::new(Some(dir.path().to_path_buf()));
    assert!(login(&entries, &factory, &creds(Some("new"), Some("x"))).is_err());
    std::fs::write(
        dir.path().join("artemis-users.properties"),
        format!("{USERS}new = x\n"),
    )
    .unwrap();
    let subject = login(&entries, &factory, &creds(Some("new"), Some("x"))).unwrap();
    assert_eq!(subject.user(), Some("new"));
}

#[test]
fn missing_files_mean_no_users() {
    let dir = tempfile::tempdir().unwrap();
    let entries = config(&format!("activemq {{ {} }};", props("required")));
    let factory = ModuleFactory::new(Some(dir.path().to_path_buf()));
    let e = login(&entries, &factory, &creds(Some("admin"), Some("secret"))).unwrap_err();
    assert_eq!(e.message, "User does not exist: admin");
}
