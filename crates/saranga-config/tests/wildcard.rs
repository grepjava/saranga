//! Ported from `WildcardConfigurationTest`.

use saranga_config::WildcardConfiguration;

fn wc(delimiter: char, any_words: char, single_word: char) -> WildcardConfiguration {
    WildcardConfiguration {
        routing_enabled: true,
        delimiter,
        any_words,
        single_word,
    }
}

const DEFAULT: WildcardConfiguration = WildcardConfiguration {
    routing_enabled: true,
    delimiter: '.',
    any_words: '#',
    single_word: '*',
};

#[test]
fn default_wildcard() {
    assert_eq!(WildcardConfiguration::default(), DEFAULT);
}

#[test]
fn to_from_core_mqtt() {
    let mqtt = wc('/', '#', '+');
    for (core, topic) in [
        ("foo.foo", "foo/foo"),
        ("foo.*.foo", "foo/+/foo"),
        ("foo.#", "foo/#"),
        ("foo.*.foo.#", "foo/+/foo/#"),
        ("foo\\.foo.foo", "foo.foo/foo"),
    ] {
        assert_eq!(mqtt.convert(topic, &DEFAULT), core);
        assert_eq!(DEFAULT.convert(core, &mqtt), topic);
    }
}

#[test]
fn equality() {
    let a = wc('a', 'b', 'c');
    let b = wc('a', 'b', 'c');
    assert_eq!(a, b);
    assert_eq!(a.convert("x.y", &b), "x.y");

    for other in [
        wc('x', 'y', 'z'),
        wc('a', 'y', 'z'),
        wc('x', 'b', 'z'),
        wc('x', 'y', 'c'),
        wc('x', 'b', 'c'),
        wc('a', 'y', 'c'),
        wc('a', 'b', 'z'),
    ] {
        assert_ne!(a, other);
    }
}

#[test]
fn is_wild() {
    for s in ["", "a", "a.b", "a\\.\\#", "a\\.\\*", "\\*", "\\#"] {
        assert!(!DEFAULT.is_wild(s), "{s}");
    }
    for s in [
        "*", "#", "a.*", "*.b", "a.*.c", "a.#", "a.b.#", "a.*.#", "a.*.\\#", "a.\\*.#",
    ] {
        assert!(DEFAULT.is_wild(s), "{s}");
    }
}

#[test]
fn contains() {
    assert!(!DEFAULT.contains('x'));
    assert!(!DEFAULT.contains('!'));
    assert!(DEFAULT.contains('#'));
    assert!(DEFAULT.contains('*'));
    assert!(DEFAULT.contains('.'));
    assert!(wc('.', '!', '*').contains('!'));
    assert!(wc('!', '#', '*').contains('!'));
    assert!(wc('.', '#', '!').contains('!'));
}
