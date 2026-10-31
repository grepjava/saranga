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

fn cert(dn: &str, san_uris: &[&str]) -> PeerCertificate {
    PeerCertificate {
        der: Vec::new(),
        subject_dn: dn.replace(", ", ","),
        subject_dn_legacy: dn.to_owned(),
        san_uris: san_uris.iter().map(|s| s.to_string()).collect(),
    }
}

#[test]
fn text_file_certificate() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("cert-users.properties"),
        "client = CN=client, OU=Test, O=Saranga\nanyone = /CN=.*, OU=Ops, O=Saranga/\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("cert-roles.properties"),
        "amq = client\nops = anyone\n",
    )
    .unwrap();
    let entries = config(
        "activemq { org.apache.activemq.artemis.spi.core.security.jaas.TextFileCertificateLoginModule required org.apache.activemq.jaas.textfiledn.user=\"cert-users.properties\" org.apache.activemq.jaas.textfiledn.role=\"cert-roles.properties\"; };",
    );
    let factory = ModuleFactory::new(Some(dir.path().to_path_buf()));
    let login_with = |c: Option<&[PeerCertificate]>| {
        login(
            &entries,
            &factory,
            &Callbacks {
                certificates: c,
                ..Callbacks::default()
            },
        )
    };
    let subject = login_with(Some(&[cert("CN=client, OU=Test, O=Saranga", &[])])).unwrap();
    assert_eq!(subject.user(), Some("client"));
    assert_eq!(subject.roles().collect::<Vec<_>>(), vec!["amq"]);
    let subject = login_with(Some(&[cert("CN=bob, OU=Ops, O=Saranga", &[])])).unwrap();
    assert_eq!(subject.user(), Some("anyone"));
    let e = login_with(Some(&[cert("CN=eve, O=Elsewhere", &[])])).unwrap_err();
    assert_eq!(
        e.message,
        "No user for client certificate: CN=eve, O=Elsewhere"
    );
    let e = login_with(None).unwrap_err();
    assert_eq!(
        e.message,
        "Client certificates not found. Cannot authenticate."
    );
}

#[test]
fn external_certificate() {
    let entries = config(
        "activemq { org.apache.activemq.artemis.spi.core.security.jaas.ExternalCertificateLoginModule required sanUriRolePrefix=\"urn:role:\"; };",
    );
    let factory = ModuleFactory::new(None);
    let certs = [cert(
        "CN=svc, O=Saranga",
        &["urn:role:admin", "urn:other:x", "urn:role:ops"],
    )];
    let subject = login(
        &entries,
        &factory,
        &Callbacks {
            certificates: Some(&certs),
            ..Callbacks::default()
        },
    )
    .unwrap();
    assert_eq!(subject.user(), Some("CN=svc, O=Saranga"));
    assert_eq!(subject.roles().collect::<Vec<_>>(), vec!["admin", "ops"]);
    // No certificate: the module is ignored, so nothing logs in.
    assert!(login(&entries, &factory, &Callbacks::default()).is_err());
}

const SCRAM_PROPS: &str =
    "org.apache.activemq.artemis.spi.core.security.jaas.SCRAMPropertiesLoginModule";
const SCRAM: &str = "org.apache.activemq.artemis.spi.core.security.jaas.SCRAMLoginModule";

/// SCRAMPropertiesLoginModule: a user's encoded data for the mechanism, a
/// password in clear made into data, data made up for a user that does
/// not exist, and roles; only SCRAM's handler serves it.
#[test]
fn scram_properties() {
    let dir = tempfile::tempdir().unwrap();
    let data = crate::scram::new_password(crate::scram::Scram::Sha256, "pw", b"0123456789", 4096)
        .unwrap()
        .user_data();
    let encoded = format!(
        r"ENC({}\:{}\:{}\:{})",
        data.salt, data.iterations, data.server_key, data.stored_key
    )
    .replace('=', r"\=");
    std::fs::write(
        dir.path().join("users.properties"),
        format!("alice|SHA256 = {encoded}\nbob = clear\n"),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("roles.properties"),
        "admins = alice\nops = alice,bob\n",
    )
    .unwrap();
    let entries = config(&format!(
        "activemq {{ {SCRAM_PROPS} required org.apache.activemq.jaas.properties.user=\"users.properties\" org.apache.activemq.jaas.properties.role=\"roles.properties\"; }};"
    ));
    let factory = ModuleFactory::new(Some(dir.path().to_path_buf()));
    let scram = |user, mechanism| Callbacks {
        user: Some(user),
        scram: Some(mechanism),
        ..Callbacks::default()
    };
    use crate::scram::Scram;
    let subject = login(&entries, &factory, &scram("alice", Scram::Sha256)).unwrap();
    assert_eq!(subject.scram_user_data(), Some(&data));
    assert_eq!(subject.user(), Some("alice"));
    assert_eq!(subject.roles().collect::<Vec<_>>(), vec!["admins", "ops"]);
    // SHA512 has no entry of its own: `alice` is looked up, and there is
    // none, so the data is made up.
    let other = login(&entries, &factory, &scram("alice", Scram::Sha512)).unwrap();
    assert_ne!(other.scram_user_data(), Some(&data));
    let bob = login(&entries, &factory, &scram("bob", Scram::Sha256)).unwrap();
    let d = bob.scram_user_data().unwrap();
    let salt = crate::scram::base64_decode(&d.salt).unwrap();
    let expected = crate::scram::new_password(Scram::Sha256, "clear", &salt, 4096).unwrap();
    assert_eq!(d, &expected.user_data());
    assert!(login(&entries, &factory, &scram("nobody", Scram::Sha256)).is_ok());
    // The usual handler has no SCRAMMechanismCallback.
    assert!(login(&entries, &factory, &creds(Some("alice"), Some("pw"))).is_err());
}

/// SCRAMLoginModule: the principals of the peer's identity and a user
/// principal named like the first; PropertiesLoginModule after it maps
/// that user to roles. SCRAM's handler has no PrincipalsCallback, nor a
/// password for PropertiesLoginModule.
#[test]
fn scram_login_module_takes_the_peer_principals() {
    let dir = tempfile::tempdir().unwrap();
    etc(dir.path());
    let entries = config(&format!(
        "activemq {{ {SCRAM} required; {} }};",
        props("optional")
    ));
    let factory = ModuleFactory::new(Some(dir.path().to_path_buf()));
    let peer = [
        Principal::User("plain".into()),
        Principal::Role("scrammers".into()),
    ];
    let subject = login(
        &entries,
        &factory,
        &Callbacks {
            user: Some("plain"),
            peer_principals: Some(&peer),
            ..Callbacks::default()
        },
    )
    .unwrap();
    assert_eq!(subject.user(), Some("plain"));
    assert_eq!(
        subject.roles().collect::<Vec<_>>(),
        vec!["scrammers", "ops"]
    );
    // No identity: the module is ignored, and the password is wrong.
    assert!(login(&entries, &factory, &creds(Some("plain"), None)).is_err());
    let e = login(
        &config(&format!("activemq {{ {SCRAM} required; }};")),
        &factory,
        &Callbacks {
            peer_principals: Some(&peer),
            scram: Some(crate::scram::Scram::Sha256),
            ..Callbacks::default()
        },
    )
    .unwrap_err();
    assert!(e.message.contains("PrincipalsCallback"), "{e}");
    let e = login(
        &config(&format!("activemq {{ {} }};", props("required"))),
        &factory,
        &Callbacks {
            user: Some("plain"),
            scram: Some(crate::scram::Scram::Sha256),
            ..Callbacks::default()
        },
    )
    .unwrap_err();
    assert!(e.message.contains("PasswordCallback"), "{e}");
}
