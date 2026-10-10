//! Ported from `ByteUtilTest.testTextBytesToLongBytes` and
//! `testTextBytesToLongBytesNegative`.

use saranga_config::convert_text_bytes;

#[test]
fn text_bytes_to_long_bytes() {
    let factors = [1i64, 5, 10];
    let types = ["", "b", "k", "m", "g"];
    let sizes = [1i64, 1, 1024, 1024 * 1024, 1024 * 1024 * 1024];

    for f in factors {
        for (t, size) in types.iter().zip(sizes) {
            let expected = f * size;
            let upper = t.to_uppercase();
            assert_eq!(convert_text_bytes(&format!("{f}{t}")).unwrap(), expected);
            assert_eq!(convert_text_bytes(&format!("{f} {t}")).unwrap(), expected);
            assert_eq!(
                convert_text_bytes(&format!("{f}{upper}")).unwrap(),
                expected
            );
            assert_eq!(
                convert_text_bytes(&format!("{f} {upper}")).unwrap(),
                expected
            );
            if size >= 1024 {
                assert_eq!(convert_text_bytes(&format!("{f}{t}b")).unwrap(), expected);
                assert_eq!(convert_text_bytes(&format!("{f} {t}b")).unwrap(), expected);
                assert_eq!(
                    convert_text_bytes(&format!("{f}{upper}B")).unwrap(),
                    expected
                );
                assert_eq!(
                    convert_text_bytes(&format!("{f} {upper}B")).unwrap(),
                    expected
                );
            }
        }
    }
}

#[test]
fn text_bytes_to_long_bytes_negative() {
    assert!(convert_text_bytes("x").is_err());
}

#[test]
fn binary_prefixes_and_padding() {
    assert_eq!(convert_text_bytes("1GiB").unwrap(), 1 << 30);
    assert_eq!(convert_text_bytes("20Mb").unwrap(), 20 << 20);
    assert_eq!(convert_text_bytes("100MiB").unwrap(), 100 << 20);
    assert_eq!(convert_text_bytes("  10M  ").unwrap(), 10 << 20);
    assert_eq!(convert_text_bytes("4ki").unwrap(), 4096);
}

#[test]
fn plain_long_fallback() {
    assert_eq!(convert_text_bytes("-1").unwrap(), -1);
    assert_eq!(convert_text_bytes("+7").unwrap(), 7);
    // The fallback is `Long.parseLong`, which does not trim.
    assert!(convert_text_bytes(" -1").is_err());
    assert!(convert_text_bytes("-1M").is_err());
    assert!(convert_text_bytes("1T").is_err());
    assert!(convert_text_bytes("5ib").is_err());
    assert!(convert_text_bytes("").is_err());
}

#[test]
fn overflow_wraps_like_java() {
    // 2^63 / 2^30 = 2^33: the product wraps to i64::MIN in Java.
    assert_eq!(convert_text_bytes("8589934592G").unwrap(), i64::MIN);
    assert!(convert_text_bytes("99999999999999999999").is_err());
}
