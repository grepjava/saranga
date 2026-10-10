//! Ported from artemis-server's `FilterTest`, run against `CoreMessage`.

use saranga_buffer::{PropertyValue, SimpleString, Writer};
use saranga_filter::Filter;
use saranga_message::CoreMessage;

fn create_filter(text: &str) -> Filter {
    Filter::create(Some(text))
        .unwrap_or_else(|e| panic!("{text}: {e}"))
        .expect("not blank")
}

fn message() -> CoreMessage {
    CoreMessage {
        message_id: 1,
        ..Default::default()
    }
}

fn put_string(message: &mut CoreMessage, key: &str, value: &str) {
    message
        .properties
        .insert(key.into(), PropertyValue::String(SimpleString::from(value)));
}

fn put_bool(message: &mut CoreMessage, key: &str, value: bool) {
    message
        .properties
        .insert(key.into(), PropertyValue::Bool(value));
}

#[test]
fn newline_match() {
    let filter = create_filter("fooprop LIKE '%1234%'");
    let mut m = message();
    put_string(&mut m, "fooprop", "hello1234\n");
    assert!(filter.matches(&m));
}

#[test]
fn filter_forgets() {
    let filter = create_filter("color = 'RED'");
    let mut m = message();
    put_string(&mut m, "color", "RED");
    assert!(filter.matches(&m));
    let m = CoreMessage::default();
    assert!(!filter.matches(&m));
}

#[test]
fn invalid_string() {
    assert!(Filter::create(Some("color = 'red")).is_err());
    assert!(Filter::create(Some("3")).is_err());
}

#[test]
fn null_filter() {
    assert!(Filter::create(None).unwrap().is_none());
    assert!(Filter::create(Some("")).unwrap().is_none());
}

#[test]
fn amq_durable() {
    let filter = create_filter("AMQDurable='DURABLE'");
    let mut m = message();
    m.durable = true;
    assert!(filter.matches(&m));
    m.durable = false;
    assert!(!filter.matches(&m));

    let filter = create_filter("AMQDurable='NON_DURABLE'");
    let mut m = CoreMessage {
        durable: true,
        ..Default::default()
    };
    assert!(!filter.matches(&m));
    m.durable = false;
    assert!(filter.matches(&m));
}

#[test]
fn amq_size() {
    let mut m = message();
    m.address = Some("4f2b6a8e-0c1d-4e5f-9a7b-3c2d1e0f9a8b".into());

    let mut w = Writer::new();
    m.encode(&mut w);
    let encode_size = w.len() as i64;

    let more_than_small = create_filter(&format!("AMQSize > {}", encode_size - 1));
    let less_than_large = create_filter(&format!("AMQSize < {}", encode_size + 1));

    let less_than_small = create_filter(&format!("AMQSize < {encode_size}"));
    let more_than_large = create_filter(&format!("AMQSize > {encode_size}"));

    assert!(more_than_small.matches(&m));
    assert!(less_than_large.matches(&m));

    assert!(!less_than_small.matches(&m));
    assert!(!more_than_large.matches(&m));
}

#[test]
fn amq_priority() {
    let filter = create_filter("AMQPriority=3");
    let mut m = message();
    for i in 0..10 {
        m.priority = i;
        assert_eq!(filter.matches(&m), i == 3, "priority {i}");
    }
}

#[test]
fn amq_timestamp() {
    let filter = create_filter("AMQTimestamp=12345678");
    let mut m = message();
    m.timestamp = 87654321;
    assert!(!filter.matches(&m));
    m.timestamp = 12345678;
    assert!(filter.matches(&m));
}

/// AMQUserID is the user-provided message ID (the JMS message ID).
#[test]
fn amq_user_id() {
    let mut m = message();
    let uuid = [
        0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30,
        0xc8,
    ];

    let filter = create_filter("AMQUserID='ID:6ba7b810-9dad-11d1-80b4-00c04fd430c8'");
    let null_filter = create_filter("AMQUserID IS NULL");
    let not_null_filter = create_filter("AMQUserID IS NOT NULL");

    assert!(!filter.matches(&m));
    assert!(null_filter.matches(&m));
    assert!(!not_null_filter.matches(&m));

    m.user_id = Some(uuid);

    assert!(filter.matches(&m));
    assert!(!null_filter.matches(&m));
    assert!(not_null_filter.matches(&m));
}

fn test_boolean(filter: &Filter, name: &str, flag: bool) {
    let mut m = message();
    put_bool(&mut m, name, flag);
    assert!(filter.matches(&m));
    put_bool(&mut m, name, !flag);
    assert!(!filter.matches(&m));
}

#[test]
fn boolean_true() {
    test_boolean(&create_filter("MyBoolean=true"), "MyBoolean", true);
}

#[test]
fn identifier() {
    test_boolean(&create_filter("MyBoolean"), "MyBoolean", true);
}

#[test]
fn boolean_false() {
    test_boolean(&create_filter("MyBoolean=false"), "MyBoolean", false);
}

#[test]
fn different_null_string() {
    let mut m = message();
    assert!(!create_filter("prop <> 'foo'").matches(&m));
    assert!(create_filter("prop <> 'foo' OR prop IS NULL").matches(&m));
    assert!(!create_filter("NOT (prop = 'foo')").matches(&m));
    assert!(create_filter("NOT (prop = 'foo') OR prop IS NULL").matches(&m));

    put_string(&mut m, "prop", "bar");
    assert!(create_filter("prop <> 'foo'").matches(&m));

    put_string(&mut m, "prop", "foo");
    assert!(!create_filter("prop <> 'foo'").matches(&m));
}

#[test]
fn string_equals() {
    let mut m = message();
    // First, simple test of string equality and inequality
    let filter = create_filter("MyString='astring'");

    put_string(&mut m, "MyString", "astring");
    assert!(filter.matches(&m));

    put_string(&mut m, "MyString", "NOTastring");
    assert!(!filter.matches(&m));

    // test empty string
    let filter = create_filter("MyString=''");

    put_string(&mut m, "MyString", "");
    assert!(filter.matches(&m), "test 1");

    put_string(&mut m, "MyString", "NOTastring");
    assert!(!filter.matches(&m), "test 2");

    // test literal apostrophes (which are escaped using two apostrophes in
    // selectors)
    let filter = create_filter("MyString='test JBoss''s filter'");

    // note: apostrophes are not escaped in string properties
    put_string(&mut m, "MyString", "test JBoss's filter");
    // Commented out in the Java test ("bug 530120"), but it holds here and
    // in Artemis today.
    assert!(filter.matches(&m), "test 3");

    put_string(&mut m, "MyString", "NOTastring");
    assert!(!filter.matches(&m), "test 4");
}

#[test]
fn not_in_with_null_property() {
    let filter = create_filter("myNullProp NOT IN ('foo','jms','test')");
    let mut m = message();
    assert!(!filter.matches(&m));
    put_string(&mut m, "myNullProp", "JMS");
    assert!(filter.matches(&m));
}

#[test]
fn not_like_with_null_property() {
    let filter = create_filter("myNullProp NOT LIKE '1_3'");
    let mut m = message();
    assert!(!filter.matches(&m));
    put_string(&mut m, "myNullProp", "JMS");
    assert!(filter.matches(&m));
}

#[test]
fn is_not_null_with_null_property() {
    let filter = create_filter("myNullProp IS NOT NULL");
    let mut m = message();
    assert!(!filter.matches(&m));
    put_string(&mut m, "myNullProp", "JMS");
    assert!(filter.matches(&m));
}

/// Applies `filter` to a message whose `MyString` is each of `values`.
fn check(filter: &str, unset: Option<bool>, values: &[(&str, bool)]) {
    let filter = create_filter(filter);
    let mut m = message();
    if let Some(expected) = unset {
        assert_eq!(
            filter.matches(&m),
            expected,
            "{} with MyString unset",
            filter.text()
        );
    }
    for (value, expected) in values {
        put_string(&mut m, "MyString", value);
        assert_eq!(
            filter.matches(&m),
            *expected,
            "{} with MyString={value:?}",
            filter.text()
        );
    }
}

#[test]
fn string_like() {
    // test LIKE operator with no wildcards
    check("MyString LIKE 'astring'", Some(false), &[("astring", true)]);
    // test one character string
    check("MyString LIKE 'a'", None, &[("a", true)]);
    // test empty string
    check("MyString LIKE ''", None, &[("", true)]);
    // tests where operand does not match
    check(
        "MyString LIKE 'astring'",
        None,
        &[
            ("NOTastring", false),
            ("astringNOT", false),
            ("astNOTring", false),
            ("totally different", false),
            ("ASTRING", false),
            ("", false),
        ],
    );
}

#[test]
fn string_like_underbar_wildcard() {
    check(
        "MyString LIKE '_'",
        Some(false),
        &[("a", true), ("aaaaa", false), ("", false)],
    );
    check(
        "MyString LIKE '_bcdf'",
        None,
        &[
            ("abcdf", true),
            ("aXcdf", false),
            ("abXdf", false),
            ("abcdX", false),
            ("", false),
            ("XXXabcdf", false),
            ("abcdfXXX", false),
            ("bcdf", false),
        ],
    );
    check(
        "MyString LIKE 'abcd_'",
        None,
        &[
            ("abcdf", true),
            ("abcXf", false),
            ("abXdf", false),
            ("Xbcdf", false),
            ("", false),
            ("XXXabcdf", false),
            ("abcdfXXX", false),
            ("abcd", false),
        ],
    );
    check(
        "MyString LIKE 'ab_df'",
        None,
        &[
            ("abcdf", true),
            ("aXcdf", false),
            ("abcXf", false),
            ("abcdX", false),
            ("", false),
            ("XXXabcdf", false),
            ("abcdfXXX", false),
            ("abdf", false),
        ],
    );
}

#[test]
fn not_like_expression() {
    // Should evaluate to false when the property MyString is not set
    check("NOT (MyString LIKE '%')", Some(false), &[]);
}

#[test]
fn string_like_percent_wildcard() {
    check(
        "MyString LIKE '%'",
        Some(false),
        &[("a", true), ("aaaaa", true), ("abcdf", true), ("", true)],
    );
    check(
        "MyString LIKE '%bcdf'",
        None,
        &[
            ("Xbcdf", true),
            ("XXbcdf", true),
            ("abXdf", false),
            ("abcdX", false),
            ("", false),
            ("abcdfXXX", false),
            ("bcdf", true),
        ],
    );
    check(
        "MyString LIKE 'abcd%'",
        None,
        &[
            ("abcdf", true),
            ("abcdfgh", true),
            ("abcXf", false),
            ("abXdf", false),
            ("Xbcdf", false),
            ("", false),
            ("XXXabcdf", false),
            ("abcd", true),
        ],
    );
    check(
        "MyString LIKE 'ab%df'",
        None,
        &[
            ("abXdf", true),
            ("abXXXdf", true),
            ("aXcdf", false),
            ("abcXf", false),
            ("abcdX", false),
            ("", false),
            ("XXXabcdf", false),
            ("abcdfXXX", false),
            ("abdf", true),
        ],
    );
}

#[test]
fn string_like_punctuation() {
    check("MyString LIKE 'a^$b'", Some(false), &[("a^$b", true)]);
    check("MyString LIKE 'a\\dc'", None, &[("a\\dc", true)]);
    check("MyString LIKE 'a.c'", None, &[("abc", false)]);
    for pattern in [
        "[abc]",
        "[^abc]",
        "[a-c]",
        "[:alpha]",
        "(abc)",
        "a|bc",
        "(abc)?",
        "(abc)*",
        "(abc)+",
        "(abc){3}",
        "(abc){3,5}",
        "(abc){3,}",
        "(?=abc)",
        "(?!abc)",
    ] {
        check(
            &format!("MyString LIKE '{pattern}'"),
            None,
            &[(pattern, true)],
        );
    }
}
