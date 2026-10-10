//! Ported from `artemis-selector`'s `SelectorTest` and
//! `UnknownHandlingSelectorTest`, run against a mock message.

use std::collections::HashMap;

use saranga_filter::{Filter, Filterable, Value};

/// `MockMessage`.
#[derive(Default)]
struct MockMessage {
    properties: HashMap<String, Option<Value>>,
    jms_type: Option<String>,
    message_id: Option<String>,
}

impl MockMessage {
    fn set(&mut self, key: &str, value: Value) {
        self.properties.insert(key.into(), Some(value));
    }

    fn set_string(&mut self, key: &str, value: &str) {
        self.set(key, Value::String(value.into()));
    }
}

impl Filterable for MockMessage {
    fn get(&self, identifier: &str) -> Option<Value> {
        match identifier {
            "JMSType" => self.jms_type.clone().map(Value::String),
            "JMSMessageID" => self.message_id.clone().map(Value::String),
            _ => self.properties.get(identifier).cloned().flatten(),
        }
    }
}

fn assert_selector(message: &MockMessage, text: &str, expected: bool) {
    let filter = Filter::parse(text).unwrap_or_else(|e| panic!("{text}: {e}"));
    assert_eq!(filter.matches(message), expected, "Selector for: {text}");
}

fn assert_invalid_selector(text: &str) {
    assert!(
        Filter::parse(text).is_err(),
        "Created a valid selector: {text}"
    );
}

/// `SelectorTest.createMessage`.
fn create_message() -> MockMessage {
    let mut m = MockMessage {
        jms_type: Some("selector-test".into()),
        message_id: Some("connection:1:1:1:1".into()),
        ..Default::default()
    };
    m.set_string("name", "James");
    m.set_string("location", "London");
    m.set("byteProp", Value::Byte(123));
    m.set("byteProp2", Value::Byte(33));
    m.set("shortProp", Value::Short(123));
    m.set("intProp", Value::Int(123));
    m.set("longProp", Value::Long(123));
    m.set("floatProp", Value::Float(123.0));
    m.set("doubleProp", Value::Double(123.0));
    m.set("rank", Value::Int(123));
    m.set("version", Value::Int(2));
    m.set_string("quote", "'In God We Trust'");
    m.set_string("foo", "_foo");
    m.set_string("punctuation", "!#$&()*+,-./:;<=>?@[\\]^`{|}~");
    m.set_string("endingUnderScore", "XD7xlJIQn_");
    m.set("trueProp", Value::Bool(true));
    m.set("falseProp", Value::Bool(false));
    m
}

mod selector_test {
    use super::*;

    #[test]
    fn boolean_selector() {
        let m = create_message();
        assert_selector(&m, "(trueProp OR falseProp) AND trueProp", true);
        assert_selector(&m, "(trueProp OR falseProp) AND falseProp", false);
        assert_selector(
            &m,
            "(falseProp OR falseProp OR falseProp OR falseProp OR falseProp OR falseProp OR trueProp)",
            true,
        );
        assert_selector(
            &m,
            "(falseProp OR falseProp OR falseProp OR falseProp OR falseProp OR falseProp OR falseProp)",
            false,
        );
        assert_selector(
            &m,
            "(trueProp AND trueProp AND trueProp AND trueProp AND trueProp AND trueProp AND falseProp)",
            false,
        );
        assert_selector(
            &m,
            "(trueProp AND trueProp AND trueProp AND trueProp AND trueProp AND trueProp AND trueProp)",
            true,
        );
    }

    /// The selectors all parse; matching needs XPath evaluation against the
    /// message body, which is not implemented (XPATH is always FALSE).
    #[test]
    #[ignore = "XPath evaluation is not implemented"]
    fn xpath_selectors() {
        let m = MockMessage {
            jms_type: Some("xml".into()),
            ..Default::default()
        };
        for (text, expected) in [
            ("XPATH 'root/a'", true),
            ("XPATH '//root/b'", true),
            ("XPATH 'root/c'", false),
            ("XPATH '//root/b/text()=\"b\"'", true),
            ("XPATH '//root/b=\"b\"'", true),
            ("XPATH '//root/b=\"c\"'", false),
            ("XPATH '//root/b!=\"c\"'", true),
            ("XPATH '//root/*[@key=''second'']'", true),
            ("XPATH '//root/*[@key=''third'']'", false),
            ("XPATH '//root/a[@key=''first'']'", true),
            ("XPATH '//root/a[@num=1]'", true),
            ("XPATH '//root/a[@key=''second'']'", false),
            ("XPATH '/root/*[@key=''first'' or @key=''third'']'", true),
            ("XPATH '//root/*[@key=''third'' or @key=''forth'']'", false),
            ("XPATH '/root/b=''b'' and /root/b[@key=''second'']'", true),
            ("XPATH '/root/b=''b'' and /root/b[@key=''first'']'", false),
            ("XPATH 'not(//root/a)'", false),
            ("XPATH 'not(//root/c)'", true),
            ("XPATH '//root/a[not(@key=''first'')]'", false),
            ("XPATH '//root/a[not(not(@key=''first''))]'", true),
            ("XPATH 'string(//root/b)'", true),
            ("XPATH 'string(//root/a)'", false),
            ("XPATH 'sum(//@num) < 10'", true),
            ("XPATH 'sum(//@num) > 10'", false),
            ("XPATH '//root/a[@num > 1]'", false),
            ("XPATH '//root/b[@num > 1]'", true),
        ] {
            assert_selector(&m, text, expected);
        }
    }

    #[test]
    fn jms_property_selectors() {
        let mut m = create_message();
        m.jms_type = Some("selector-test".into());
        m.message_id = Some("id:test:1:1:1:1".into());

        assert_selector(&m, "JMSType = 'selector-test'", true);
        assert_selector(&m, "JMSType = 'crap'", false);

        assert_selector(&m, "JMSMessageID = 'id:test:1:1:1:1'", true);
        assert_selector(&m, "JMSMessageID = 'id:not-test:1:1:1:1'", false);

        let mut m = create_message();
        m.jms_type = Some("1001".into());

        assert_selector(&m, "JMSType='1001'", true);
        assert_selector(&m, "JMSType='1001' OR JMSType='1002'", true);
        assert_selector(&m, "JMSType = 'crap'", false);
    }

    fn quoted_property(c: char) {
        let key = format!("a{c}test");
        let mut m = create_message();
        m.set_string(&key, "value");
        assert_selector(&m, &format!("\"{key}\" = 'value'"), true);
    }

    #[test]
    fn dotted_property() {
        quoted_property('.');
    }

    #[test]
    fn hyphenated_property() {
        quoted_property('-');
    }

    #[test]
    fn basic_selectors() {
        let m = create_message();
        assert_selector(&m, "name = 'James'", true);
        assert_selector(&m, "rank > 100", true);
        assert_selector(&m, "rank >= 123", true);
        assert_selector(&m, "rank >= 124", false);
    }

    #[test]
    fn property_types() {
        let m = create_message();
        assert_selector(&m, "byteProp = 123", true);
        assert_selector(&m, "byteProp = 10", false);
        assert_selector(&m, "byteProp2 = 33", true);
        assert_selector(&m, "byteProp2 = 10", false);

        assert_selector(&m, "shortProp = 123", true);
        assert_selector(&m, "shortProp = 10", false);

        assert_selector(&m, "shortProp = 123", true);
        assert_selector(&m, "shortProp = 10", false);

        assert_selector(&m, "intProp = 123", true);
        assert_selector(&m, "intProp = 10", false);

        assert_selector(&m, "longProp = 123", true);
        assert_selector(&m, "longProp = 10", false);

        assert_selector(&m, "floatProp = 123", true);
        assert_selector(&m, "floatProp = 10", false);

        assert_selector(&m, "doubleProp = 123", true);
        assert_selector(&m, "doubleProp = 10", false);
    }

    #[test]
    fn and_selectors() {
        let m = create_message();
        assert_selector(&m, "name = 'James' and rank < 200", true);
        assert_selector(&m, "name = 'James' and rank > 200", false);
        assert_selector(&m, "name = 'Foo' and rank < 200", false);
        assert_selector(&m, "unknown = 'Foo' and anotherUnknown < 200", false);
    }

    #[test]
    fn or_selectors() {
        let m = create_message();
        assert_selector(&m, "name = 'James' or rank < 200", true);
        assert_selector(&m, "name = 'James' or rank > 200", true);
        assert_selector(&m, "name = 'Foo' or rank < 200", true);
        assert_selector(&m, "name = 'Foo' or rank > 200", false);
        assert_selector(&m, "unknown = 'Foo' or anotherUnknown < 200", false);
    }

    #[test]
    fn plus() {
        let m = create_message();
        assert_selector(&m, "rank + 2 = 125", true);
        assert_selector(&m, "(rank + 2) = 125", true);
        assert_selector(&m, "125 = (rank + 2)", true);
        assert_selector(&m, "rank + version = 125", true);
        assert_selector(&m, "rank + 2 < 124", false);
        assert_selector(&m, "name + '!' = 'James!'", true);
    }

    #[test]
    fn minus() {
        let m = create_message();
        assert_selector(&m, "rank - 2 = 121", true);
        assert_selector(&m, "rank - version = 121", true);
        assert_selector(&m, "rank - 2 > 122", false);
    }

    #[test]
    fn multiply() {
        let m = create_message();
        assert_selector(&m, "rank * 2 = 246", true);
        assert_selector(&m, "rank * version = 246", true);
        assert_selector(&m, "rank * 2 < 130", false);
    }

    #[test]
    fn divide() {
        let m = create_message();
        assert_selector(&m, "rank / version = 61.5", true);
        assert_selector(&m, "rank / 3 > 100.0", false);
        assert_selector(&m, "rank / 3 > 100", false);
        assert_selector(&m, "version / 2 = 1", true);
    }

    #[test]
    fn between() {
        let m = create_message();
        assert_selector(&m, "rank between 100 and 150", true);
        assert_selector(&m, "rank between 10 and 120", false);
    }

    #[test]
    fn in_list() {
        let m = create_message();
        assert_selector(&m, "name in ('James', 'Bob', 'Gromit')", true);
        assert_selector(&m, "name in ('Bob', 'James', 'Gromit')", true);
        assert_selector(&m, "name in ('Gromit', 'Bob', 'James')", true);

        assert_selector(&m, "name in ('Gromit', 'Bob', 'Cheddar')", false);
        assert_selector(&m, "name not in ('Gromit', 'Bob', 'Cheddar')", true);
    }

    #[test]
    fn is_null() {
        let m = create_message();
        assert_selector(&m, "dummy is null", true);
        assert_selector(&m, "dummy is not null", false);
        assert_selector(&m, "name is not null", true);
        assert_selector(&m, "name is null", false);
    }

    #[test]
    fn like() {
        let selector = "modelClassId LIKE 'com.whatever.something.%' AND modelInstanceId = '170' AND (modelRequestError IS NULL OR modelCorrelatedClientId = 'whatever')";
        let mut m = create_message();
        m.set_string("modelClassId", "com.whatever.something.foo.bar");
        m.set_string("modelInstanceId", "170");
        m.set_string("modelRequestError", "abc");
        m.set_string("modelCorrelatedClientId", "whatever");

        assert_selector(&m, selector, true);

        m.set_string("modelCorrelatedClientId", "shouldFailNow");

        assert_selector(&m, selector, false);

        let mut m = create_message();
        m.set_string("modelClassId", "com.whatever.something.foo.bar");
        m.set_string("modelInstanceId", "170");
        m.set_string("modelCorrelatedClientId", "shouldNotMatch");

        assert_selector(&m, selector, true);
    }

    #[test]
    fn mats_henricson_use_cases() {
        let mut m = create_message();
        assert_selector(&m, "SessionserverId=1870414179", false);

        m.set("SessionserverId", Value::Long(1870414179));
        assert_selector(&m, "SessionserverId=1870414179", true);

        m.set("SessionserverId", Value::Long(1234));
        assert_selector(&m, "SessionserverId=1870414179", false);

        let not_in = "Command NOT IN ('MirrorLobbyRequest', 'MirrorLobbyReply')";
        assert_selector(&m, not_in, false);

        m.set_string("Command", "Cheese");
        assert_selector(&m, not_in, true);

        m.set_string("Command", "MirrorLobbyRequest");
        assert_selector(&m, not_in, false);
        m.set_string("Command", "MirrorLobbyReply");
        assert_selector(&m, not_in, false);
    }

    #[test]
    fn float_comparisons() {
        let m = create_message();
        for text in [
            "1.0 < 1.1",
            "-1.1 < 1.0",
            "1.0E1 < 1.1E1",
            "-1.1E1 < 1.0E1",
            "1. < 1.1",
            "-1.1 < 1.",
            "1.E1 < 1.1E1",
            "-1.1E1 < 1.E1",
            ".1 < .5",
            "-.5 < .1",
            ".1E1 < .5E1",
            "-.5E1 < .1E1",
            "4E10 < 5E10",
            "5E8 < 5E10",
            "-4E10 < 2E10",
            "-5E8 < 2E2",
            "4E+10 < 5E+10",
            "4E-10 < 5E-10",
        ] {
            assert_selector(&m, text, true);
        }
    }

    #[test]
    fn string_quote_parsing() {
        let m = create_message();
        assert_selector(&m, "quote = '''In God We Trust'''", true);
    }

    #[test]
    fn like_comparisons() {
        let m = create_message();

        assert_selector(&m, "quote LIKE '''In G_d We Trust'''", true);
        assert_selector(&m, "quote LIKE '''In Gd_ We Trust'''", false);
        assert_selector(&m, "quote NOT LIKE '''In G_d We Trust'''", false);
        assert_selector(&m, "quote NOT LIKE '''In Gd_ We Trust'''", true);

        assert_selector(&m, "foo LIKE '%oo'", true);
        assert_selector(&m, "foo LIKE '%ar'", false);
        assert_selector(&m, "foo NOT LIKE '%oo'", false);
        assert_selector(&m, "foo NOT LIKE '%ar'", true);

        assert_selector(&m, "foo LIKE '!_%' ESCAPE '!'", true);
        assert_selector(&m, "quote LIKE '!_%' ESCAPE '!'", false);
        assert_selector(&m, "foo NOT LIKE '!_%' ESCAPE '!'", false);
        assert_selector(&m, "quote NOT LIKE '!_%' ESCAPE '!'", true);

        assert_selector(&m, "punctuation LIKE '!#$&()*+,-./:;<=>?@[\\]^`{|}~'", true);
    }

    #[test]
    fn special_escape_literal() {
        let m = create_message();
        assert_selector(&m, "foo LIKE '%_%' ESCAPE '%'", true);
        assert_selector(&m, "endingUnderScore LIKE '_D7xlJIQn$_' ESCAPE '$'", true);
        assert_selector(&m, "endingUnderScore LIKE '_D7xlJIQn__' ESCAPE '_'", true);
        assert_selector(&m, "endingUnderScore LIKE '%D7xlJIQn%_' ESCAPE '%'", true);
        assert_selector(&m, "endingUnderScore LIKE '%D7xlJIQn%'  ESCAPE '%'", true);

        // literal '%' at the end, no match
        assert_selector(&m, "endingUnderScore LIKE '%D7xlJIQn%%'  ESCAPE '%'", false);

        assert_selector(&m, "endingUnderScore LIKE '_D7xlJIQn\\_' ESCAPE '\\'", true);
        assert_selector(&m, "endingUnderScore LIKE '%D7xlJIQn\\_' ESCAPE '\\'", true);
    }

    #[test]
    fn like_with_consecutive_wildcards_are_merged() {
        let mut m = create_message();
        m.set_string("prop", "helloZ");
        assert_selector(&m, "prop LIKE '%%%%%%%%%%%%Z'", true);

        m.set_string("prop", "anything");
        assert_selector(&m, "prop LIKE '%%'", true);

        m.set_string("prop", "aXb");
        assert_selector(&m, "prop LIKE 'a%%b'", true);
        assert_selector(&m, "prop LIKE 'a%%c'", false);
    }

    #[test]
    fn like_with_single_wildcards_still_works() {
        let mut m = create_message();

        m.set_string("prop", "helloZ");
        assert_selector(&m, "prop LIKE '%Z'", true);

        m.set_string("prop", "Z");
        assert_selector(&m, "prop LIKE '%Z'", true);

        m.set_string("prop", "abcZdef");
        assert_selector(&m, "prop LIKE '%Z%'", true);

        m.set_string("prop", "nothing");
        assert_selector(&m, "prop LIKE '%Z%'", false);
    }

    #[test]
    fn like_with_too_many_wildcards_is_rejected() {
        assert_invalid_selector("prop LIKE '%_%_%_%_%_%_%_%_%_%Z'");
        assert_invalid_selector("prop LIKE '%a%b%c%d%e%f'");
    }

    #[test]
    fn like_with_max_allowed_wildcards() {
        let mut m = create_message();
        m.set_string("prop", "aXbYcZd");
        assert_selector(&m, "prop LIKE '%X%Y%Z%'", true);
        assert_selector(&m, "prop LIKE '%X%Q%Z%'", false);
    }

    #[test]
    fn like_escaped_wildcards_do_not_count_toward_limit() {
        let mut m = create_message();

        // 6 '%' characters but all escaped to literals: zero actual wildcards
        m.set_string("prop", "%%%%%%%%");
        assert_selector(&m, "prop LIKE '!%!%!%!%!%!%!%%' ESCAPE '!'", true);

        // mix: 4 real wildcards + 3 escaped '%' literals: should be allowed
        m.set_string("prop", "a%b%c%end");
        assert_selector(&m, "prop LIKE '%a!%b!%c!%end%'  ESCAPE '!'", true);
        assert_selector(&m, "prop LIKE '%a!%b!%c!%XXX%' ESCAPE '!'", false);
    }

    #[test]
    fn invalid_selector() {
        assert_invalid_selector("3+5");
        assert_invalid_selector("True AND 3+5");
        assert_invalid_selector("=TEST 'test'");
        assert_invalid_selector("prop1 = prop2 foo AND string = 'Test'");
        assert_invalid_selector("a = 1 AMD  b = 2");
    }
}

mod unknown_handling_selector_test {
    use super::*;

    fn message() -> MockMessage {
        let mut m = MockMessage {
            jms_type: Some("selector-test".into()),
            message_id: Some("connection:1:1:1:1".into()),
            ..Default::default()
        };
        m.set("trueProp", Value::Bool(true));
        m.set("falseProp", Value::Bool(false));
        m.properties.insert("nullProp".into(), None);
        m
    }

    fn not(selector: &str) -> String {
        format!("not({selector})")
    }

    fn evaluates_to_unknown(selector: &str) {
        let m = message();
        assert_selector(&m, selector, false);
        assert_selector(&m, &not(selector), false);
    }

    fn evaluates_to_true(selector: &str) {
        let m = message();
        assert_selector(&m, selector, true);
        assert_selector(&m, &not(selector), false);
    }

    fn evaluates_to_false(selector: &str) {
        let m = message();
        assert_selector(&m, selector, false);
        assert_selector(&m, &not(selector), true);
    }

    #[test]
    fn not_evaluation() {
        let m = message();
        assert_selector(&m, "not(trueProp)", false);
        assert_selector(&m, "not(falseProp)", true);
        assert_selector(&m, "not(unknownProp)", false);
    }

    #[test]
    fn and_evaluation() {
        evaluates_to_true("trueProp AND trueProp");
        evaluates_to_false("trueProp AND falseProp");
        evaluates_to_false("falseProp AND trueProp");
        evaluates_to_false("falseProp AND falseProp");
        evaluates_to_false("falseProp AND unknownProp");
        evaluates_to_false("unknownProp AND falseProp");
        evaluates_to_unknown("trueProp AND unknownProp");
        evaluates_to_unknown("unknownProp AND trueProp");
        evaluates_to_unknown("unknownProp AND unknownProp");
    }

    #[test]
    fn or_evaluation() {
        evaluates_to_true("trueProp OR trueProp");
        evaluates_to_true("trueProp OR falseProp");
        evaluates_to_true("falseProp OR trueProp");
        evaluates_to_true("trueProp OR unknownProp");
        evaluates_to_true("unknownProp OR trueProp");
        evaluates_to_false("falseProp OR falseProp");
        evaluates_to_unknown("falseProp OR unknownProp");
        evaluates_to_unknown("unknownProp OR falseProp");
        evaluates_to_unknown("unknownProp OR unknownProp");
    }

    fn comparisons_are_unknown(prop: &str) {
        for op in [
            "= 0",
            "> 0",
            ">= 0",
            "< 0",
            "<= 0",
            "<> 0",
            "LIKE 'zero'",
            "NOT LIKE 'zero'",
            "IN ('zero')",
            "NOT IN ('zero')",
            "BETWEEN 1 AND 2",
            "NOT BETWEEN 1 AND 2",
        ] {
            evaluates_to_unknown(&format!("{prop} {op}"));
        }
    }

    #[test]
    fn comparison_with_unknown_should_evaluate_to_unknown() {
        comparisons_are_unknown("unknownProp");
    }

    #[test]
    fn comparison_with_null_prop_should_evaluate_to_unknown() {
        comparisons_are_unknown("nullProp");
    }

    #[test]
    fn is_null_is_not_null() {
        evaluates_to_true("unknownProp IS NULL");
        evaluates_to_true("nullProp IS NULL");
        evaluates_to_false("trueProp IS NULL");
        evaluates_to_false("unknownProp IS NOT NULL");
        evaluates_to_false("nullProp IS NOT NULL");
        evaluates_to_true("trueProp IS NOT NULL");
    }

    #[test]
    fn arithmetic_with_null() {
        evaluates_to_unknown("-unknownProp = 0");
        evaluates_to_unknown("+unknownProp = 0");
        evaluates_to_unknown("unknownProp * 2 = 0");
        evaluates_to_unknown("unknownProp / 2 = 0");
        evaluates_to_unknown("unknownProp + 2 = 0");
        evaluates_to_unknown("unknownProp - 2 = 0");
    }
}
