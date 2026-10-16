//! The history checker against small histories, valid and not.

use saranga_chaos::history::{check, parse};

fn verdict(text: &str) -> String {
    check(&parse(text).unwrap())
        .to_string()
        .lines()
        .last()
        .unwrap()
        .to_string()
}

#[test]
fn every_enqueue_dequeued_once_is_valid() {
    let report = check(
        &parse(
            "# two producers, a consumer
             0 ok enqueue 1 0 10
             1 ok enqueue 2 5 12
             2 ok dequeue 2 13 20
             2 ok dequeue 1 21 30",
        )
        .unwrap(),
    );
    assert!(report.valid(), "{report}");
    assert!(
        report
            .to_string()
            .starts_with("enqueue: 2 ok 0 fail 0 info\ndequeue: 2 ok 0 fail 0 info\n")
    );
}

#[test]
fn a_lost_enqueue_is_invalid() {
    assert_eq!(
        verdict("0 ok enqueue 1 0 10\n0 ok enqueue 2 11 20\n1 ok dequeue 1 21 30"),
        "invalid: 1 lost"
    );
}

#[test]
fn an_unknown_enqueue_may_be_dequeued_or_not() {
    let report = check(
        &parse("0 info enqueue 1 0 10\n1 info enqueue 2 0 10\n2 ok dequeue 2 11 20").unwrap(),
    );
    assert!(report.valid(), "{report}");
    assert_eq!(report.recovered, 1);
}

#[test]
fn a_failed_enqueue_must_not_be_dequeued() {
    assert_eq!(
        verdict("0 fail enqueue 1 0 10\n1 ok dequeue 1 11 20"),
        "invalid: 1 unexpected"
    );
    assert_eq!(verdict("1 ok dequeue 7 11 20"), "invalid: 1 unexpected");
}

#[test]
fn two_ok_dequeues_are_a_duplicate_but_an_unknown_one_is_allowed() {
    assert_eq!(
        verdict("0 ok enqueue 1 0 10\n1 ok dequeue 1 11 20\n2 ok dequeue 1 21 30"),
        "invalid: 1 duplicated"
    );
    let report =
        check(&parse("0 ok enqueue 1 0 10\n1 info dequeue 1 11 20\n2 ok dequeue 1 21 30").unwrap());
    assert!(report.valid(), "{report}");
    assert_eq!(report.unknown_twice, 1);
}

#[test]
fn a_failed_dequeue_does_not_count() {
    assert_eq!(
        verdict("0 ok enqueue 1 0 10\n1 fail dequeue 1 11 20"),
        "invalid: 1 lost"
    );
    assert_eq!(
        verdict("0 ok enqueue 1 0 10\n1 fail dequeue 1 11 20\n1 ok dequeue 1 21 30"),
        "valid"
    );
}

#[test]
fn a_dequeue_before_its_enqueue_began_is_invalid() {
    assert_eq!(
        verdict("1 ok dequeue 1 0 5\n0 ok enqueue 1 6 10"),
        "invalid: 1 early"
    );
    // Overlapping is fine: the send took effect before its commit returned.
    assert_eq!(verdict("0 ok enqueue 1 0 10\n1 ok dequeue 1 5 8"), "valid");
}

#[test]
fn processes_must_be_well_formed() {
    assert_eq!(
        verdict(
            "0 ok enqueue 1 0 10\n0 ok enqueue 2 5 12\n1 ok dequeue 1 20 21\n1 ok dequeue 2 22 23"
        ),
        "invalid: 1 malformed"
    );
    assert_eq!(
        verdict("0 info enqueue 1 0 10\n0 ok enqueue 2 11 12\n1 ok dequeue 2 22 23"),
        "invalid: 1 malformed"
    );
    assert_eq!(
        verdict("0 ok enqueue 1 0 10\n2 ok enqueue 1 11 12\n1 ok dequeue 1 22 23"),
        "invalid: 1 malformed"
    );
}

#[test]
fn bad_lines_are_refused() {
    assert!(parse("0 ok enqueue 1 0").is_err());
    assert!(parse("0 maybe enqueue 1 0 1").is_err());
    assert!(parse("0 ok peek 1 0 1").is_err());
    assert!(parse("x ok enqueue 1 0 1").is_err());
}
