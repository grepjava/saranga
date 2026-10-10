use proptest::prelude::*;
use saranga_buffer::{JString, PropertyValue, Reader, SimpleString, TypedProperties, Writer};

fn jstring() -> impl Strategy<Value = JString> {
    // Cover all three writeString encodings: <9 chars, <0xfff chars, longer.
    prop_oneof![
        prop::collection::vec(any::<u16>(), 0..9),
        prop::collection::vec(any::<u16>(), 9..300),
        prop::collection::vec(any::<u16>(), 0xfff..0x1100),
    ]
    .prop_map(JString::from_code_units)
}

fn property_value() -> impl Strategy<Value = PropertyValue> {
    prop_oneof![
        Just(PropertyValue::Null),
        any::<bool>().prop_map(PropertyValue::Bool),
        any::<i8>().prop_map(PropertyValue::Byte),
        prop::collection::vec(any::<u8>(), 0..32).prop_map(PropertyValue::Bytes),
        any::<i16>().prop_map(PropertyValue::Short),
        any::<i32>().prop_map(PropertyValue::Int),
        any::<i64>().prop_map(PropertyValue::Long),
        any::<u32>().prop_map(|b| PropertyValue::Float(f32::from_bits(b))),
        any::<u64>().prop_map(|b| PropertyValue::Double(f64::from_bits(b))),
        any::<u16>().prop_map(PropertyValue::Char),
        ".{0,20}".prop_map(|s: String| PropertyValue::String(s.as_str().into())),
    ]
}

proptest! {
    #[test]
    fn string_roundtrip(s in jstring()) {
        let mut w = Writer::new();
        w.string(&s);
        let mut r = Reader::new(w.as_slice());
        prop_assert_eq!(r.string().unwrap(), s);
        prop_assert!(r.is_empty());
    }

    #[test]
    fn nullable_string_roundtrip(s in prop::option::of(jstring())) {
        let mut w = Writer::new();
        w.nullable_string(s.as_ref());
        prop_assert_eq!(Reader::new(w.as_slice()).nullable_string().unwrap(), s);
    }

    #[test]
    fn properties_roundtrip(entries in prop::option::of(
        prop::collection::vec(("[a-zA-Z_]{1,12}", property_value()), 0..12))) {
        let mut props = TypedProperties::new();
        let mut empty_but_present = false;
        if let Some(entries) = &entries {
            empty_but_present = entries.is_empty();
            for (k, v) in entries {
                props.insert(k.as_str().into(), v.clone());
            }
        }
        // Encode, decode, re-encode: the bytes must be identical.
        let mut w = Writer::new();
        props.encode(&mut w);
        let decoded = TypedProperties::decode(&mut Reader::new(w.as_slice())).unwrap();
        let mut w2 = Writer::new();
        decoded.encode(&mut w2);
        prop_assert_eq!(w.as_slice(), w2.as_slice());
        prop_assert_eq!(decoded, props);
        let _ = empty_but_present;
    }
}

#[test]
fn utf_known_vectors() {
    // Java DataOutput-style modified UTF-8, but NUL is a single 0 byte (UTF8Util).
    let mut w = Writer::new();
    w.utf(&"aü€\0".into());
    assert_eq!(w.as_slice(), &[0, 7, b'a', 0xc3, 0xbc, 0xe2, 0x82, 0xac, 0]);
}

#[test]
fn short_string_is_raw_chars() {
    let mut w = Writer::new();
    w.string(&"hi".into());
    assert_eq!(w.as_slice(), &[0, 0, 0, 2, 0, b'h', 0, b'i']);
}

#[test]
fn lone_surrogate_survives() {
    let s = JString::from_code_units(vec![0xd800; 20]);
    let mut w = Writer::new();
    w.string(&s);
    assert_eq!(Reader::new(w.as_slice()).string().unwrap(), s);
}

#[test]
fn simple_string_is_utf16le() {
    let s: SimpleString = "ab".into();
    assert_eq!(s.as_bytes(), &[b'a', 0, b'b', 0]);
    let mut w = Writer::new();
    w.simple_string(&s);
    assert_eq!(w.as_slice(), &[0, 0, 0, 4, b'a', 0, b'b', 0]);
}

#[test]
fn empty_but_present_properties_stay_present() {
    let bytes = [1u8, 0, 0, 0, 0];
    let props = TypedProperties::decode(&mut Reader::new(&bytes)).unwrap();
    let mut w = Writer::new();
    props.encode(&mut w);
    assert_eq!(w.as_slice(), &bytes);
}

#[test]
fn truncated_input_is_an_error() {
    assert!(Reader::new(&[0, 0, 0, 5, 1]).simple_string().is_err());
    assert!(
        Reader::new(&[0xff, 0xff, 0xff, 0xff])
            .simple_string()
            .is_err()
    );
}
