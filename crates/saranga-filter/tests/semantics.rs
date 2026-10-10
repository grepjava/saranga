//! Artemis behaviours that are easy to get wrong, each checked against what
//! the Java classes do (`ComparisonExpression.compare`,
//! `ArithmeticExpression`, `UnaryExpression`, `FilterImpl`).

use std::collections::HashMap;

use saranga_buffer::{PropertyValue, SimpleString};
use saranga_filter::{Filter, Filterable, Value};
use saranga_message::CoreMessage;

struct Props(HashMap<&'static str, Value>);

impl Filterable for Props {
    fn get(&self, identifier: &str) -> Option<Value> {
        self.0.get(identifier).cloned()
    }
}

fn props() -> Props {
    Props(HashMap::from([
        ("byteProp", Value::Byte(123)),
        ("shortProp", Value::Short(123)),
        ("intProp", Value::Int(123)),
        ("longProp", Value::Long(123)),
        ("floatProp", Value::Float(123.0)),
        ("doubleProp", Value::Double(123.0)),
        ("intMax", Value::Int(i32::MAX)),
        ("longMin", Value::Long(i64::MIN)),
        ("nan", Value::Double(f64::NAN)),
        ("negZero", Value::Double(-0.0)),
        ("zero", Value::Double(0.0)),
        ("str5", Value::String("5".into())),
        ("str15", Value::String("1.5".into())),
        ("strTrue", Value::String("TRUE".into())),
        ("strX", Value::String("x".into())),
        ("strA", Value::String("a".into())),
        ("strB", Value::String("b".into())),
        ("charA", Value::Char(u16::from(b'a'))),
        ("bytes", Value::Bytes(vec![1, 2, 3])),
        ("trueProp", Value::Bool(true)),
        ("falseProp", Value::Bool(false)),
        ("name", Value::String("James".into())),
    ]))
}

fn check(cases: &[(&str, bool)]) {
    let p = props();
    for (text, expected) in cases {
        let filter = Filter::parse(text).unwrap_or_else(|e| panic!("{text}: {e}"));
        assert_eq!(filter.matches(&p), *expected, "{text}");
    }
}

#[test]
fn numeric_promotion_is_one_sided() {
    check(&[
        ("shortProp = 123", true),
        ("byteProp = 123", true),
        // An Integer on the left is never widened against a Short or Byte.
        ("123 = shortProp", false),
        ("123 = byteProp", false),
        ("intProp = shortProp", false),
        ("shortProp = intProp", true),
        ("longProp = byteProp", false),
        ("doubleProp = shortProp", false),
        ("doubleProp = floatProp", true),
        ("floatProp = doubleProp", true),
        ("longProp = intProp", true),
        ("intProp = longProp", true),
        // A failed promotion is FALSE for every operator, so <> is TRUE.
        ("123 <> shortProp", true),
        ("intProp > shortProp - 1", true),
    ]);
}

#[test]
fn strings_and_numbers_do_not_mix_by_default() {
    check(&[
        ("str5 = 5", false),
        ("5 = str5", false),
        ("str5 <> 5", true),
        ("charA = 'a'", false),
        ("trueProp = 'true'", false),
        ("strA < strB", true),
        ("strB > strA", true),
    ]);
}

#[test]
fn convert_string_expressions() {
    check(&[
        ("convert_string_expressions:str5 = 5", true),
        ("convert_string_expressions:5 = str5", true),
        ("convert_string_expressions:str5 > 4", true),
        ("convert_string_expressions:str15 = 1.5", true),
        ("convert_string_expressions:strTrue = TRUE", true),
        ("convert_string_expressions:trueProp = 'TRUE'", true),
        // NumberFormatException is FALSE.
        ("convert_string_expressions:strX = 5", false),
        // `+` concatenates whenever the left side is a string.
        ("convert_string_expressions:str5 + 1 = 51", true),
        ("convert_string_expressions:1 + str5 = 6", true),
        ("convert_string_expressions:str15 * 2 = 3", true),
        // Arithmetic on a non-number throws, so the filter does not match.
        ("convert_string_expressions:strX - 1 = 6", false),
        ("convert_string_expressions:NOT (strX - 1 = 6)", false),
        ("convert_string_expressions:NOT (strX + 1 = 6)", true),
        ("no_convert_string_expressions:str5 = 5", false),
        (
            "convert_string_expressions:no_convert_string_expressions:str5 = 5",
            false,
        ),
    ]);
}

#[test]
fn hyphenated_props() {
    let p = Props(HashMap::from([("foo-bar", Value::Int(1))]));
    assert!(
        Filter::parse("hyphenated_props:foo-bar = 1")
            .unwrap()
            .matches(&p)
    );
    assert!(
        !Filter::parse("hyphenated_props:foo-bar-1 = 0")
            .unwrap()
            .matches(&p)
    );
    // Without the prefix, it is foo minus bar.
    assert!(!Filter::parse("foo-bar = 1").unwrap().matches(&p));
    assert!(Filter::parse("\"foo-bar\" = 1").unwrap().matches(&p));
    // Quoted identifiers do not exist in the hyphenated grammar.
    assert!(Filter::parse("hyphenated_props:\"foo-bar\" = 1").is_err());
    assert!(
        !Filter::parse("hyphenated_props:no_hyphenated_props:foo-bar = 1")
            .unwrap()
            .matches(&p)
    );
}

#[test]
fn nulls() {
    check(&[
        // Two missing values are equal.
        ("missing1 = missing2", true),
        ("missing1 <> missing2", false),
        // A missing right-hand side is FALSE, not unknown.
        ("name = missing", false),
        ("name <> missing", true),
        ("missing = name", false),
        ("missing <> name", false),
        ("NULL IS NULL", true),
    ]);
}

#[test]
fn arithmetic() {
    check(&[
        ("intMax + 1 < 0", true),
        ("intMax + 1 = -2147483648", true),
        ("longProp + intMax > 2147483647", true),
        ("-2147483648 = intMax + 1", true),
        ("-9223372036854775808 = longMin", true),
        ("9223372036854775808 > 0", false),
        ("7 / 2 = 3.5", true),
        ("7 % 2 = 1", true),
        ("1 / 0 > 1000000", true),
        ("name + 1.5 = 'James1.5'", true),
        ("name + 1.0E10 = 'James1.0E10'", true),
        ("name + trueProp = 'Jamestrue'", true),
        ("1 + name = 'x'", false),
        ("-byteProp = -123", false),
        ("NOT (-byteProp = -123)", false),
        ("-name IS NULL", true),
    ]);
}

#[test]
fn floating_point_order_is_javas() {
    check(&[
        ("nan > 1", true),
        ("nan = nan", true),
        ("nan > 1.0E308", true),
        ("negZero < zero", true),
        ("negZero = zero", false),
        ("negZero = 0", false),
    ]);
}

#[test]
fn booleans_and_bytes() {
    check(&[
        ("trueProp > falseProp", true),
        ("trueProp = 1", false),
        ("bytes = bytes", false),
        ("bytes IS NULL", false),
        ("bytes > 1", false),
        ("NOT (bytes > 1)", false),
        // The comparison throws before OR can look at TRUE...
        ("bytes > 1 OR trueProp", false),
        // ...but OR short-circuits on a match first.
        ("trueProp OR bytes > 1", true),
        ("bytes LIKE '%'", false),
        ("NOT (bytes LIKE '%')", true),
        ("bytes IN ('a')", false),
        ("NOT (bytes IN ('a'))", false),
        ("intProp", false),
        ("NOT intProp", true),
    ]);
}

#[test]
fn xpath_and_xquery_parse_but_never_match() {
    check(&[
        ("XPATH '//a'", false),
        ("NOT XPATH '//a'", true),
        ("XQUERY 'x'", false),
    ]);
}

#[test]
fn keywords_are_case_insensitive() {
    check(&[
        ("name LiKe 'J%' aNd intProp BeTwEeN 1 AnD 200", true),
        ("name nOt In ('x') Or FaLsE", true),
        ("missing iS nUlL", true),
    ]);
}

#[test]
fn create_trims_and_keeps_prefixes() {
    assert!(Filter::create(Some(" \t\n ")).unwrap().is_none());
    let f = Filter::create(Some("  a = 1 \n")).unwrap().unwrap();
    assert_eq!(f.text(), "a = 1");
    let f = Filter::parse("convert_string_expressions:a = 1").unwrap();
    assert_eq!(f.text(), "convert_string_expressions:a = 1");
    assert_eq!(
        f,
        Filter::parse("convert_string_expressions:a = 1").unwrap()
    );
}

#[test]
fn string_map() {
    let map = HashMap::from([("color".to_string(), "RED".to_string())]);
    assert!(Filter::parse("color = 'RED'").unwrap().matches(&map));
    assert!(!Filter::parse("color > 'A'").is_ok());
}

fn core_message() -> CoreMessage {
    CoreMessage {
        message_id: 7,
        address: Some("orders".into()),
        priority: 4,
        timestamp: 1000,
        expiration: 2000,
        ..Default::default()
    }
}

fn put(m: &mut CoreMessage, key: &str, value: PropertyValue) {
    m.properties.insert(SimpleString::from(key), value);
}

#[test]
fn core_message_headers() {
    let mut m = core_message();
    put(
        &mut m,
        "_AMQ_GROUP_ID",
        PropertyValue::String("group-1".into()),
    );
    put(&mut m, "AMQFoo", PropertyValue::Int(9));
    put(&mut m, "AMQAddress", PropertyValue::String("prop".into()));
    for (text, expected) in [
        ("AMQAddress = 'orders'", true),
        ("AMQPriority = 4", true),
        ("AMQTimestamp = 1000", true),
        ("AMQExpiration = 2000", true),
        ("AMQGroupID = 'group-1'", true),
        ("AMQFoo = 9", true),
        ("AMQDurable = 'NON_DURABLE'", true),
        ("AMQUserID IS NULL", true),
    ] {
        assert_eq!(Filter::parse(text).unwrap().matches(&m), expected, "{text}");
    }
    // A null header falls back to the property of the same name.
    m.address = None;
    assert!(Filter::parse("AMQAddress = 'prop'").unwrap().matches(&m));
    // The group ID header converts non-string properties with toString.
    put(&mut m, "_AMQ_GROUP_ID", PropertyValue::Long(5));
    assert!(Filter::parse("AMQGroupID = '5'").unwrap().matches(&m));
    put(&mut m, "_AMQ_GROUP_ID", PropertyValue::Bytes(vec![1]));
    assert!(!Filter::parse("AMQGroupID IS NOT NULL").unwrap().matches(&m));
    assert!(!Filter::parse("AMQGroupID IS NULL").unwrap().matches(&m));
}

#[test]
fn core_message_user_id_falls_back_to_native_message_id() {
    let mut m = core_message();
    put(
        &mut m,
        "NATIVE_MESSAGE_ID",
        PropertyValue::String("amqp-id".into()),
    );
    assert!(Filter::parse("AMQUserID = 'amqp-id'").unwrap().matches(&m));
    m.user_id = Some([0; 16]);
    assert!(
        Filter::parse("AMQUserID = 'ID:00000000-0000-0000-0000-000000000000'")
            .unwrap()
            .matches(&m)
    );
}

#[test]
fn core_message_hops_and_property_types() {
    let mut m = core_message();
    put(&mut m, "_AMQ_Hops", PropertyValue::Bytes(vec![0, 0, 0, 2]));
    put(&mut m, "s", PropertyValue::String("v".into()));
    put(&mut m, "c", PropertyValue::Char(u16::from(b'v')));
    put(&mut m, "n", PropertyValue::Null);
    for (text, expected) in [
        ("_AMQ_Hops = 2", true),
        ("_AMQ_HopsAnything = 2", true),
        ("s = 'v'", true),
        ("c = 'v'", false),
        ("n IS NULL", true),
    ] {
        assert_eq!(Filter::parse(text).unwrap().matches(&m), expected, "{text}");
    }
    // A hops property that is not a 4-byte array makes the lookup throw.
    put(&mut m, "_AMQ_Hops", PropertyValue::Bytes(vec![2]));
    assert!(!Filter::parse("_AMQ_Hops IS NULL").unwrap().matches(&m));
    assert!(!Filter::parse("_AMQ_Hops IS NOT NULL").unwrap().matches(&m));
}
