//! `bootstrap.xml` and `management.xml` that Artemis's `XmlUtil.decode`
//! refuses, reported as Artemis 2.57.0's CLI reports them (recorded with
//! its stack frames left out).

use std::path::{Path, PathBuf};
use std::process::Command;

const BOOTSTRAP: &str = "<broker xmlns=\"http://activemq.apache.org/schema\">\n   <jaas-security domain=\"a\" bogus=\"1\"/>\n   <server configuration=\"file:x\"/>\n</broker>\n";
const MANAGEMENT: &str = "<management-context xmlns=\"http://activemq.apache.org/schema\">\n   <connector connector-port=\"x\"/>\n</management-context>\n";

const MANAGEMENT_ERROR: &str = "javax.xml.bind.UnmarshalException
 - with linked exception:
[org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 35; cvc-datatype-valid.1.2.1: 'x' is not a valid value for 'integer'.]
Caused by: org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 35; cvc-datatype-valid.1.2.1: 'x' is not a valid value for 'integer'.
";
const BOOTSTRAP_ERROR: &str = "javax.xml.bind.UnmarshalException
 - with linked exception:
[org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 41; cvc-complex-type.3.2.2: Attribute 'bogus' is not allowed to appear in element 'jaas-security'.]
Caused by: org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 41; cvc-complex-type.3.2.2: Attribute 'bogus' is not allowed to appear in element 'jaas-security'.
";

/// An instance directory with these files in its `etc`.
fn instance(name: &str, files: &[(&str, &str)]) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("saranga-dto-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("etc")).unwrap();
    for (file, text) in files {
        std::fs::write(dir.join("etc").join(file), text).unwrap();
    }
    dir
}

fn saranga(instance: &Path, args: &[&str]) -> (String, Option<i32>) {
    let out = Command::new(env!("CARGO_BIN_EXE_saranga"))
        .args(args)
        .env("ARTEMIS_INSTANCE", instance)
        .env("ARTEMIS_INSTANCE_ETC", instance.join("etc"))
        .env_remove("JAVA_ARGS")
        .output()
        .expect("saranga runs");
    let _ = std::fs::remove_dir_all(instance);
    (
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.code(),
    )
}

#[test]
fn management_xml_is_reported_before_any_command() {
    let dir = instance("version", &[("management.xml", MANAGEMENT)]);
    let (err, status) = saranga(&dir, &["version"]);
    assert_eq!(err, MANAGEMENT_ERROR);
    assert_eq!(status, Some(0));
}

#[test]
fn a_tool_ends_on_bootstrap_xml_as_an_uncaught_exception() {
    let dir = instance(
        "stop",
        &[("bootstrap.xml", BOOTSTRAP), ("management.xml", MANAGEMENT)],
    );
    let (err, status) = saranga(&dir, &["stop"]);
    assert_eq!(
        err,
        format!("{MANAGEMENT_ERROR}Exception in thread \"main\" {BOOTSTRAP_ERROR}")
    );
    assert_eq!(status, Some(1));
}

#[test]
fn run_prints_what_it_cannot_decode_and_ends() {
    let dir = instance(
        "run",
        &[("bootstrap.xml", BOOTSTRAP), ("management.xml", MANAGEMENT)],
    );
    let (err, status) = saranga(&dir, &["run"]);
    assert_eq!(err, format!("{MANAGEMENT_ERROR}{BOOTSTRAP_ERROR}"));
    assert_eq!(status, Some(1));
    // A valid bootstrap.xml: management.xml, again where `Run` reads it.
    let dir = instance(
        "run2",
        &[("management.xml", MANAGEMENT), ("broker.xml", "")],
    );
    let broker = dir.join("etc").join("broker.xml");
    let bootstrap = format!(
        "<broker xmlns=\"http://activemq.apache.org/schema\"><jaas-security domain=\"a\"/><server configuration=\"file:{}\"/></broker>",
        broker.display().to_string().replace('\\', "/")
    );
    std::fs::write(dir.join("etc").join("bootstrap.xml"), bootstrap).unwrap();
    let (err, status) = saranga(&dir, &["run"]);
    assert_eq!(err, format!("{MANAGEMENT_ERROR}{MANAGEMENT_ERROR}"));
    assert_eq!(status, Some(1));
}
