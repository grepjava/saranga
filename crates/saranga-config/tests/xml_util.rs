//! Ported from the `replaceSystemPropsInString` cases of `XMLUtilTest`, plus
//! the `${name:default}` form.

use saranga_config::{BrokerConfig, replace_system_props};

fn props(name: &str) -> Option<String> {
    match name {
        "sysprop1" => Some("test1".into()),
        "sysprop2" => Some("content4".into()),
        _ => None,
    }
}

fn lines(tests: &[(&str, &str)]) -> String {
    let mut s = String::from("<configuration>\n");
    for (name, content) in tests {
        s.push_str(&format!("   <test name=\"{name}\">{content}</test>\n"));
    }
    s.push_str("</configuration>");
    s
}

#[test]
fn replace_system_properties() {
    let before = lines(&[
        ("${sysprop1}", "content1"),
        ("test2", "content2"),
        ("test3", "content3"),
        ("test4", "${sysprop2}"),
        ("test5", "content5"),
        ("test6", "content6"),
    ]);
    let after = lines(&[
        ("test1", "content1"),
        ("test2", "content2"),
        ("test3", "content3"),
        ("test4", "content4"),
        ("test5", "content5"),
        ("test6", "content6"),
    ]);
    assert_eq!(replace_system_props(&before, props), after);
}

#[test]
fn replace_system_properties_with_unclosed_property_reference() {
    let before = lines(&[
        ("${sysprop1}", "content1"),
        ("test2", "content2"),
        ("test3", "content3"),
        ("test4", "${sysprop2"),
        ("test5", "content5"),
        ("test6", "content6"),
    ]);
    let after = lines(&[
        ("test1", "content1"),
        ("test2", "content2"),
        ("test3", "content3"),
        ("test4", "${sysprop2"),
        ("test5", "content5"),
        ("test6", "content6"),
    ]);
    assert_eq!(replace_system_props(&before, props), after);
}

#[test]
fn replace_system_properties_with_misc_curly_braces() {
    let before = lines(&[
        ("${sysprop1}", "content1{ }"),
        ("test2", "content2 {"),
        ("test3", "content3 }"),
        ("test4", "${sysprop2}"),
        ("test5", "content5{ }"),
        ("test6", "content6 }"),
    ]);
    let after = lines(&[
        ("test1", "content1{ }"),
        ("test2", "content2 {"),
        ("test3", "content3 }"),
        ("test4", "content4"),
        ("test5", "content5{ }"),
        ("test6", "content6 }"),
    ]);
    assert_eq!(replace_system_props(&before, props), after);
}

#[test]
fn defaults_and_trimming() {
    assert_eq!(
        replace_system_props("${missing:fallback}", props),
        "fallback"
    );
    assert_eq!(
        replace_system_props("${ sysprop1 : fallback }", props),
        "test1"
    );
    assert_eq!(replace_system_props("${missing: a:b }", props), "a:b");
    // Unresolved without a default: the empty string.
    assert_eq!(replace_system_props("x${missing}y", props), "xy");
    // Every occurrence of the same placeholder is replaced at once.
    assert_eq!(
        replace_system_props("${sysprop1}-${sysprop1}", props),
        "test1-test1"
    );
}

#[test]
fn recursive_expansion_and_self_reference() {
    let lookup = |name: &str| match name {
        "outer" => Some("${inner}".to_owned()),
        "inner" => Some("value".to_owned()),
        "loop" => Some("${loop}".to_owned()),
        _ => None,
    };
    assert_eq!(replace_system_props("${outer}", lookup), "value");
    // Java would spin forever here.
    assert_eq!(replace_system_props("${loop}", lookup), "${loop}");
}

#[test]
fn substitution_applies_to_broker_xml() {
    let xml = r#"<configuration xmlns="urn:activemq"><core xmlns="urn:activemq:core">
        <name>${BROKER_NAME:fallback}</name>
        <journal-directory>${DATA}/journal</journal-directory>
        <acceptors><acceptor name="a">tcp://${HOST:0.0.0.0}:${PORT}</acceptor></acceptors>
    </core></configuration>"#;
    let lookup = |name: &str| match name {
        "DATA" => Some("/var/lib/saranga".to_owned()),
        "PORT" => Some("61617".to_owned()),
        _ => None,
    };
    let config = BrokerConfig::parse_with(xml, lookup).unwrap();
    assert_eq!(config.name, "fallback");
    assert_eq!(config.journal_directory, "/var/lib/saranga/journal");
    assert_eq!(config.acceptors[0].host.as_deref(), Some("0.0.0.0"));
    assert_eq!(config.acceptors[0].port, Some(61617));
}
