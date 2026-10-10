//! Ported from `MatchTest` and `RepositoryTest` (the Java
//! `HierarchicalObjectRepository` tests).

use std::collections::BTreeMap;

use saranga_config::{
    AddressSettings, HierarchicalRepository, Match, Mergeable, WildcardConfiguration,
};

/// A value that is not `Mergeable` in Java: the most specific match wins.
#[derive(Debug, Clone, PartialEq)]
struct Plain(&'static str);

impl Mergeable for Plain {
    fn merge_copy(&self, _other: &Self) -> Self {
        self.clone()
    }
}

/// `RepositoryTest.DummyMergeable`.
#[derive(Debug, Clone)]
struct Dummy {
    id: i32,
    settings: BTreeMap<String, String>,
    merged: Vec<Dummy>,
}

impl Dummy {
    fn new(id: i32) -> Self {
        Self::with(id, &[])
    }

    fn with(id: i32, settings: &[(&str, &str)]) -> Self {
        Self {
            id,
            settings: settings
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            merged: Vec::new(),
        }
    }

    fn merged_ids(&self) -> Vec<i32> {
        self.merged.iter().map(|d| d.id).collect()
    }
}

impl Mergeable for Dummy {
    fn merge_copy(&self, other: &Self) -> Self {
        let mut target = self.clone();
        for (k, v) in &other.settings {
            target
                .settings
                .entry(k.clone())
                .or_insert_with(|| v.clone());
        }
        target.merged.push(other.clone());
        target
    }
}

fn repo<T>() -> HierarchicalRepository<T> {
    HierarchicalRepository::default()
}

fn get(r: &HierarchicalRepository<Plain>, address: &str) -> &'static str {
    r.get_match(address).expect("a match").0
}

// ---- MatchTest ----

fn predicate_test_any_child(any_words: char) {
    let wc = WildcardConfiguration {
        any_words,
        ..WildcardConfiguration::default()
    };
    let m = Match::new(&format!("test.{any_words}"), &wc);
    assert!(m.matches("test"));
    assert!(m.matches("test.A"));
    assert!(m.matches("test.A.B"));
    assert!(!m.matches("testing.A"));
}

#[test]
fn predicate_test_any_child_default() {
    predicate_test_any_child('#');
}

#[test]
fn predicate_test_any_child_dollar() {
    predicate_test_any_child('$');
}

#[test]
fn predicate_test_any_word() {
    let m = Match::new("test.*", &WildcardConfiguration::default());
    assert!(m.matches("test.A"));
    assert!(!m.matches("testing.A"));
    assert!(!m.matches("test"));
    assert!(!m.matches("test.A.B"));
}

#[test]
fn dollar_matching() {
    let m = Match::new("$test.#", &WildcardConfiguration::default());
    assert!(m.matches("$test"));
    assert!(m.matches("$test.A"));
    assert!(m.matches("$test.A.B"));
    assert!(!m.matches("$testing.A"));
}

#[test]
fn any_words_alone_matches_everything_on_one_line() {
    let m = Match::new("#", &WildcardConfiguration::default());
    assert!(m.matches(""));
    assert!(m.matches("a.b.c"));
    // Java's `.*` does not cross line terminators.
    assert!(!m.matches("a\nb"));
}

#[test]
fn single_word_inside_a_word() {
    let m = Match::new("a*b.c", &WildcardConfiguration::default());
    assert!(m.matches("axyzb.c"));
    assert!(!m.matches("ab.c"));
    assert!(!m.matches("ax.yb.c"));
}

#[test]
fn regex_metacharacters_are_literal() {
    let wc = WildcardConfiguration::default();
    assert!(Match::new("a+b", &wc).matches("a+b"));
    assert!(!Match::new("a.b", &wc).matches("axb"));
}

// ---- RepositoryTest ----

#[test]
fn default_value() {
    let mut r: HierarchicalRepository<Plain> = repo();
    assert_eq!(r.get_match("queues.something"), None);
    r.set_default(Some(Plain("default")));
    assert_eq!(get(&r, "queues.something"), "default");
}

#[test]
fn matching_docs() {
    let mut r = repo();
    r.add_match("a.b.#", Plain("ab#")).unwrap();
    r.add_match("a.b.d.#", Plain("abd#")).unwrap();
    r.add_match("#", Plain("root")).unwrap();

    assert_eq!(get(&r, "a.b"), "ab#");
    assert_eq!(get(&r, "a.b.c"), "ab#");
    assert_eq!(get(&r, "a.b.d.lll"), "abd#");
    assert_eq!(get(&r, "z.z.z.z.z"), "root");
    assert_eq!(get(&r, "a.babc"), "root");
    assert_eq!(get(&r, "a.b.dabc"), "ab#");
    assert_eq!(get(&r, "a.b.d"), "abd#");
}

#[test]
fn literal() {
    let mut r: HierarchicalRepository<Dummy> = repo().with_literal_markers('(', ')');
    r.add_match("#", Dummy::new(0)).unwrap();
    r.add_match("(a.#)", Dummy::new(1)).unwrap();
    r.add_match("a.#", Dummy::new(2)).unwrap();
    r.add_match("a.b", Dummy::new(3)).unwrap();
    r.add_match("a.*", Dummy::new(4)).unwrap();

    let ab = r.get_match("a.b").unwrap();
    assert_eq!(ab.id, 3);
    assert_eq!(ab.merged_ids(), [4, 2, 0]);

    let a = r.get_match("a.#").unwrap();
    assert_eq!(a.id, 1);
    assert_eq!(a.merged_ids(), [4, 2, 0]);
}

#[test]
fn cache_with_wildcards() {
    let mut r = repo();
    r.add_match("#", Plain("root")).unwrap();
    assert_eq!(get(&r, "b"), "root");
    r.add_match("b", Plain("leaf")).unwrap();
    assert_eq!(get(&r, "b"), "leaf");
}

#[test]
fn multiple_matches_has_right_order() {
    let mut r = repo();
    for m in [
        "a.b.c.d.e.f",
        "a.b.c.d.e.*",
        "a.*.*.*.*.*",
        "*.b.c.d.*.f",
        "*.b.*.d.*.f",
        "a.b.c.d.e.#",
    ] {
        r.add_match(m, Plain(m)).unwrap();
    }
    assert_eq!(get(&r, "a.b.c.d.e.f"), "a.b.c.d.e.f");
    assert_eq!(get(&r, "a.b.c.d.e.x"), "a.b.c.d.e.*");
    assert_eq!(get(&r, "a.b.x.d.x.f"), "a.*.*.*.*.*");
    assert_eq!(get(&r, "x.b.c.d.e.f"), "*.b.c.d.*.f");
    assert_eq!(get(&r, "x.b.x.d.e.f"), "*.b.*.d.*.f");
    assert_eq!(get(&r, "a.b.c.d.e.f.g"), "a.b.c.d.e.#");
}

fn custom_delimiter_docs(d: char) {
    let wc = WildcardConfiguration {
        delimiter: d,
        ..WildcardConfiguration::default()
    };
    let s = |p: &str| p.replace('.', &d.to_string());
    let mut r = HierarchicalRepository::new(wc);
    r.add_match(&s("a.b.#"), Plain("ab#")).unwrap();
    r.add_match(&s("a.b.d.#"), Plain("abd#")).unwrap();
    r.add_match("#", Plain("root")).unwrap();

    assert_eq!(get(&r, &s("a.b")), "ab#");
    assert_eq!(get(&r, &s("a.b.c")), "ab#");
    assert_eq!(get(&r, &s("a.b.d.lll")), "abd#");
    assert_eq!(get(&r, &s("z.z.z.z.z")), "root");
    assert_eq!(get(&r, &s("a.babc")), "root");
    assert_eq!(get(&r, &s("a.b.dabc")), "ab#");
    assert_eq!(get(&r, &s("a.b.d")), "abd#");
}

#[test]
fn matching_docs_custom_underscore_delimiter() {
    custom_delimiter_docs('_');
}

#[test]
fn matching_docs_custom_forward_slash_delimiter() {
    custom_delimiter_docs('/');
}

#[test]
fn single_match() {
    let mut r = repo();
    r.add_match("queues.*", Plain("q")).unwrap();
    assert_eq!(get(&r, "queues.something"), "q");
}

#[test]
fn single_two() {
    let mut r = repo();
    r.add_match("queues.another.aq.*", Plain("1")).unwrap();
    r.add_match("queues.aq", Plain("2")).unwrap();
    r.add_match("queues.another.andanother", Plain("3"))
        .unwrap();
    assert_eq!(get(&r, "queues.another.andanother"), "3");
}

#[test]
fn without_wildcard() {
    let mut r = repo();
    r.add_match("queues.1.*", Plain("1")).unwrap();
    r.add_match("queues.2.aq", Plain("2")).unwrap();
    assert_eq!(get(&r, "queues.2.aq"), "2");
}

#[test]
fn multiple_wildcards() {
    let mut r = repo();
    for m in [
        "#", "a", "a.#", "a.*", "a.b.c", "a.*.c", "a.d.c", "a.b.#", "a.b", "a.b.c.#", "a.b.c.d",
        "a.*.*.d", "a.*.d.#",
    ] {
        r.add_match(m, Plain(m)).unwrap();
    }
    assert_eq!(get(&r, "a"), "a");
    assert_eq!(get(&r, "a.b"), "a.b");
    assert_eq!(get(&r, "a.x"), "a.*");
    assert_eq!(get(&r, "a.b.x"), "a.b.#");
    assert_eq!(get(&r, "a.b.c"), "a.b.c");
    assert_eq!(get(&r, "a.d.c"), "a.d.c");
    assert_eq!(get(&r, "a.x.c"), "a.*.c");
    assert_eq!(get(&r, "a.b.c.d"), "a.b.c.d");
    assert_eq!(get(&r, "a.x.c.d"), "a.*.*.d");
    assert_eq!(get(&r, "a.b.x.d"), "a.*.*.d");
    assert_eq!(get(&r, "a.d.x.d"), "a.*.*.d");
    assert_eq!(get(&r, "a.d.d.g"), "a.*.d.#");
    assert_eq!(get(&r, "zzzz.z.z.z.d.r.g.f.sd.s.fsdfd.fsdfs"), "#");
}

#[test]
fn repository_merge() {
    let mut r = repo();
    for (m, id) in [
        ("#", 1),
        ("a.#", 2),
        ("b.#", 3),
        ("a.b.#", 4),
        ("b.c.#", 5),
        ("a.b.c.#", 6),
        ("a.b.*.d", 7),
        ("a.b.c.*", 8),
    ] {
        r.add_match(m, Dummy::new(id)).unwrap();
    }
    let abcd = r.get_match("a.b.c.d").unwrap();
    assert_eq!(abcd.id, 8);
    assert_eq!(abcd.merged_ids(), [7, 6, 4, 2, 1]);

    let abc = r.get_match("a.b.c").unwrap();
    assert_eq!(abc.id, 6);
    assert_eq!(abc.merged_ids(), [4, 2, 1]);

    let z = r.get_match("z").unwrap();
    assert_eq!(z.id, 1);
    assert!(z.merged.is_empty());
}

#[test]
fn illegal_matches() {
    let mut r = repo();
    assert!(r.add_match("hjhjhjhjh.#.hhh", Plain("test")).is_err());
    assert!(r.add_match("##", Plain("test")).is_err());
}

#[test]
fn remove_match() {
    let mut r = repo();
    r.add_match("#", Plain("1")).unwrap();
    r.add_match("B", Plain("2")).unwrap();
    assert_eq!(get(&r, "B"), "2");
    r.remove_match("B");
    assert_eq!(get(&r, "B"), "1");
    r.remove_match("#");
    assert_eq!(r.get_match("B"), None);
}

#[test]
fn match_merge_idempotence() {
    let mut r = repo();
    r.add_match("foo.*", Dummy::with(0, &[("s0", "x")]))
        .unwrap();
    r.add_match("foo.0.#", Dummy::with(1, &[("so", "a"), ("s1", "a")]))
        .unwrap();
    r.add_match("foo.1.#", Dummy::with(2, &[("so", "b"), ("s1", "b")]))
        .unwrap();

    for _ in 0..2 {
        let foox = r.get_match("foo.x").unwrap();
        assert_eq!(foox.id, 0);
        assert!(foox.merged.is_empty());
        assert_eq!(foox.settings.get("s0").unwrap(), "x");
        assert_eq!(foox.settings.get("s1"), None);

        let foo0 = r.get_match("foo.0").unwrap();
        assert_eq!(foo0.id, 0);
        assert_eq!(foo0.merged.len(), 1);
        assert_eq!(foo0.settings.get("s0").unwrap(), "x");
        assert_eq!(foo0.settings.get("s1").unwrap(), "a");

        let foo1 = r.get_match("foo.1").unwrap();
        assert_eq!(foo1.id, 0);
        assert_eq!(foo1.merged.len(), 1);
        assert_eq!(foo1.settings.get("s0").unwrap(), "x");
        assert_eq!(foo1.settings.get("s1").unwrap(), "b");
    }
}

#[test]
fn address_settings_merge_idempotence() {
    let foox = AddressSettings {
        max_redelivery_delay: Some(10000),
        ..Default::default()
    };
    let foo0 = AddressSettings {
        max_redelivery_delay: Some(20000),
        max_expiry_delay: Some(20000),
        ..Default::default()
    };
    let foo1 = AddressSettings {
        max_redelivery_delay: Some(30000),
        max_expiry_delay: Some(30000),
        ..Default::default()
    };
    let mut r = repo();
    r.add_match("foo.*", foox).unwrap();
    r.add_match("foo.0.#", foo0).unwrap();
    r.add_match("foo.1.#", foo1).unwrap();

    for _ in 0..2 {
        let x = r.get_match("foo.x").unwrap();
        assert_eq!(x.max_redelivery_delay(), 10000);
        assert_eq!(
            x.max_expiry_delay(),
            AddressSettings::DEFAULT_MAX_EXPIRY_DELAY
        );

        let zero = r.get_match("foo.0").unwrap();
        assert_eq!(zero.max_redelivery_delay(), 10000);
        assert_eq!(zero.max_expiry_delay(), 20000);

        let one = r.get_match("foo.1").unwrap();
        assert_eq!(one.max_redelivery_delay(), 10000);
        assert_eq!(one.max_expiry_delay(), 30000);
    }
}

#[test]
fn fully_qualified_wildcard_is_an_exact_match() {
    let mut r = repo();
    r.add_match("a.*::q", Plain("fqqn")).unwrap();
    r.add_match("#", Plain("root")).unwrap();
    assert!(r.contains_exact_match("a.*::q"));
    assert_eq!(get(&r, "a.b::q"), "root");
    assert_eq!(get(&r, "a.*::q"), "fqqn");
}

#[test]
fn specificity_comparator() {
    use std::cmp::Ordering::*;
    let wc = WildcardConfiguration::default();
    let cmp = |a, b| Match::compare_specificity(a, b, &wc);
    assert_eq!(cmp("a.b", "a.#"), Less);
    assert_eq!(cmp("a.#", "a.b"), Greater);
    assert_eq!(cmp("a.b.#", "a.#"), Less);
    assert_eq!(cmp("a.*", "a.#"), Less);
    assert_eq!(cmp("a.b", "a.*"), Less);
    assert_eq!(cmp("a.*.c", "*.b.c"), Less);
    assert_eq!(cmp("a.b", "a.b"), Equal);
}
