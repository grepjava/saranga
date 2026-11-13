//! What `XPathFactory`'s compiler refuses before `sxd_xpath` sees an
//! expression: one past the JDK's limits on its groups and operators, and a
//! call to a function it does not know or with a count of arguments the
//! function does not take.

use super::functions::TABLE;

/// `jdk.xml.xpathExprGrpLimit`'s default: the most parenthesised groups.
pub const MAX_GROUPS: usize = 10;

/// `jdk.xml.xpathExprOpLimit`'s default: the most operators.
pub const MAX_OPERATORS: usize = 100;

/// `Lexer.tokenize`'s counting of groups and operators, as the JDK counts
/// them, refusing an expression past [`MAX_GROUPS`] or [`MAX_OPERATORS`].
/// The limits also bound how deeply `sxd_xpath`, which recurses on every
/// group, call, predicate, operator and unary minus, nests its work.
pub(super) fn within_jdk_limits(pattern: &str) -> Result<(), String> {
    let chars: Vec<char> = pattern.chars().collect();
    let next = |i: usize| chars.get(i + 1).copied();
    let (mut operators, mut groups) = (0usize, 0usize);
    let (mut is_literal, mut is_num, mut is_axis) = (false, false, false);
    // The start of the token being read, and of its prefix's `:`.
    let mut start: Option<usize> = None;
    let mut ns_separator: Option<usize> = None;
    let count = |operators: &mut usize, is_literal: &mut bool| {
        *operators += 1;
        *is_literal = false;
    };
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '"' | '\'' => {
                if start.is_some() {
                    is_num = false;
                    ns_separator = None;
                }
                i += 1;
                while i < chars.len() && chars[i] != c {
                    i += 1;
                }
                if i == chars.len() {
                    return Err(format!("misquoted literal: expected {c}"));
                }
                start = None;
            }
            '\n' | '\r' | ' ' | '\t' => {
                if let Some(s) = start {
                    is_num = false;
                    if ns_separator.is_some() {
                        ns_separator = None;
                    } else {
                        let word: String = chars[s..i].iter().collect();
                        if [
                            "or",
                            "and",
                            "div",
                            "mod",
                            "quo",
                            "..",
                            "::",
                            "attribute",
                            "child",
                        ]
                        .contains(&word.as_str())
                        {
                            count(&mut operators, &mut is_literal);
                        }
                    }
                    start = None;
                }
            }
            '@' | '-' | '(' | '[' | ')' | ']' | '|' | '/' | '*' | '+' | '=' | ','
            | '\\' | '^' | '!' | '$' | '<' | '>' => 'delimiter: {
                if c == '-' {
                    // A `-` in a name, not after a number, is part of it.
                    if !(is_num || start.is_none()) {
                        break 'delimiter;
                    }
                    is_num = false;
                }
                if start.is_some() {
                    is_num = false;
                    ns_separator = None;
                    start = None;
                }
                match c {
                    '[' => {
                        count(&mut operators, &mut is_literal);
                        is_axis = false;
                    }
                    '(' => {
                        if is_literal {
                            if !is_axis {
                                count(&mut operators, &mut is_literal);
                            }
                        } else {
                            groups += 1;
                            count(&mut operators, &mut is_literal);
                        }
                        is_axis = false;
                    }
                    _ => {}
                }
                if matches!(c, '>' | '<' | '=' | '!') {
                    if next(i) != Some('=') {
                        count(&mut operators, &mut is_literal);
                    }
                } else if c == '/' {
                    is_axis = false;
                    if next(i) != Some('/') {
                        count(&mut operators, &mut is_literal);
                    }
                } else if !matches!(c, '(' | '[' | ')' | ']' | ',') {
                    if c != '*' || !is_axis {
                        count(&mut operators, &mut is_literal);
                    }
                    is_axis = false;
                }
            }
            _ => 'other: {
                if c == ':' && i > 0 {
                    if ns_separator == Some(i - 1) {
                        // `::`, after an axis name.
                        is_num = false;
                        start = None;
                        ns_separator = None;
                        operators += 1;
                        is_axis = true;
                        break 'other;
                    }
                    ns_separator = Some(i);
                }
                is_literal = true;
                if !is_num && c == '.' && next(i) != Some('.') {
                    count(&mut operators, &mut is_literal);
                }
                match start {
                    None => {
                        start = Some(i);
                        is_num = c.is_numeric();
                    }
                    Some(_) if is_num => is_num = c.is_numeric(),
                    Some(_) => {}
                }
            }
        }
        if groups > MAX_GROUPS {
            return Err(format!(
                "the expression has {groups} groups, more than the {MAX_GROUPS} allowed"
            ));
        }
        if operators > MAX_OPERATORS {
            return Err(format!(
                "the expression has {operators} operators, more than the {MAX_OPERATORS} allowed"
            ));
        }
        i += 1;
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq)]
enum Previous {
    /// Nothing, `@`, `::`, `(`, `[`, `,` or an operator: what follows a
    /// name here is a name, not an operator.
    Opening,
    /// Anything else: a name here is an operator (`and`, `div`...) and `*`
    /// is multiplication.
    Operand,
}

/// A call's name, and the arguments seen so far.
struct Frame {
    function: Option<String>,
    commas: usize,
    content: bool,
}

fn name_start(c: char) -> bool {
    c.is_alphabetic() || c == '_' || !c.is_ascii()
}

fn name_char(c: char) -> bool {
    name_start(c) || c.is_ascii_digit() || matches!(c, '-' | '.')
}

/// Refuses a call `XPathParser` refuses: an unprefixed name not in the
/// table, an argument count its function does not take, a node test
/// (`node()`, `text()`...) given arguments it does not take, and a call
/// where only a node test can be (after `@` or an axis, or `*`). A
/// prefixed name is left for evaluation to fail, as Java leaves it.
pub(super) fn check_calls(expression: &str) -> Result<(), String> {
    let chars: Vec<char> = expression.chars().collect();
    let skip_space = |mut i: usize| {
        while chars.get(i).is_some_and(|c| c.is_whitespace()) {
            i += 1;
        }
        i
    };
    let mut previous = Previous::Opening;
    let mut after_axis = false;
    let mut call: Option<String> = None;
    let mut frames: Vec<Frame> = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if !matches!(c, ')' | ']' | ',')
            && let Some(top) = frames.last_mut()
        {
            top.content = true;
        }
        let was_after_axis = std::mem::take(&mut after_axis);
        match c {
            '(' | '[' => {
                frames.push(Frame {
                    function: if c == '(' { call.take() } else { None },
                    commas: 0,
                    content: false,
                });
                previous = Previous::Opening;
                i += 1;
            }
            ')' | ']' => {
                if let Some(Frame {
                    function: Some(name),
                    commas,
                    content,
                }) = frames.pop()
                {
                    let given = if content || commas > 0 { commas + 1 } else { 0 };
                    arity(&name, given)?;
                }
                previous = Previous::Operand;
                i += 1;
            }
            ',' => {
                if let Some(top) = frames.last_mut() {
                    top.commas += 1;
                }
                previous = Previous::Opening;
                i += 1;
            }
            '\'' | '"' => {
                i += 1;
                while i < chars.len() && chars[i] != c {
                    i += 1;
                }
                i += 1;
                previous = Previous::Operand;
            }
            '@' => {
                after_axis = true;
                previous = Previous::Opening;
                i += 1;
            }
            ':' if chars.get(i + 1) == Some(&':') => {
                after_axis = true;
                previous = Previous::Opening;
                i += 2;
            }
            '*' if previous == Previous::Operand => {
                previous = Previous::Opening;
                i += 1;
            }
            '*' => {
                if chars.get(skip_space(i + 1)) == Some(&'(') {
                    return Err("`*` is not a function".to_owned());
                }
                previous = Previous::Operand;
                i += 1;
            }
            '$' => {
                i += 1;
                while chars.get(i).is_some_and(|&c| name_char(c) || c == ':') {
                    i += 1;
                }
                previous = Previous::Operand;
            }
            c if c.is_ascii_digit()
                || (c == '.' && chars.get(i + 1).is_some_and(char::is_ascii_digit)) =>
            {
                while chars
                    .get(i)
                    .is_some_and(|&c| c.is_ascii_digit() || c == '.')
                {
                    i += 1;
                }
                previous = Previous::Operand;
            }
            '.' => {
                i += if chars.get(i + 1) == Some(&'.') { 2 } else { 1 };
                previous = Previous::Operand;
            }
            c if name_start(c) => {
                let begin = i;
                while chars.get(i).is_some_and(|&c| name_char(c)) {
                    i += 1;
                }
                let mut prefixed = false;
                if chars.get(i) == Some(&':') && chars.get(i + 1) != Some(&':') {
                    prefixed = true;
                    i += 1;
                    if chars.get(i) == Some(&'*') {
                        i += 1;
                    } else {
                        while chars.get(i).is_some_and(|&c| name_char(c)) {
                            i += 1;
                        }
                    }
                }
                let name: String = chars[begin..i].iter().collect();
                if previous == Previous::Operand {
                    // `and`, `or`, `div`, `mod`.
                    previous = Previous::Opening;
                    continue;
                }
                previous = Previous::Operand;
                let after = skip_space(i);
                if chars.get(after) == Some(&'(') {
                    let node_test = !prefixed
                        && matches!(
                            name.as_str(),
                            "node" | "text" | "comment" | "processing-instruction"
                        );
                    if was_after_axis && !node_test {
                        return Err(format!("{name} is not a node type"));
                    }
                    if node_test {
                        call = Some(format!("{name}()"));
                    } else if prefixed {
                        call = None;
                    } else {
                        call = Some(name);
                    }
                }
            }
            _ => {
                // An operator: `/`, `|`, `+`, `-`, `=`, `!=`, `<`...
                previous = Previous::Opening;
                i += 1;
            }
        }
    }
    Ok(())
}

/// Whether the function `name` (a node test as `name()`) takes `given`
/// arguments.
fn arity(name: &str, given: usize) -> Result<(), String> {
    let (least, most) = match name {
        "node()" | "text()" | "comment()" => (0, Some(0)),
        "processing-instruction()" => (0, Some(1)),
        _ => TABLE
            .iter()
            .find(|(n, ..)| *n == name)
            .map(|(_, least, most)| (*least, *most))
            .ok_or_else(|| format!("could not find function: {name}"))?,
    };
    if given < least || most.is_some_and(|m| given > m) {
        return Err(format!("{name} does not take {given} arguments"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested(open: &str, close: &str, n: usize) -> String {
        open.repeat(n) + "1" + &close.repeat(n)
    }

    #[test]
    fn the_jdk_limits_are_counted_as_the_jdk_counts_them() {
        // Each with what `XPathFactory.newInstance().newXPath().compile`
        // (JDK 25) made of it.
        let cases = [
            (nested("(", ")", 10), true),
            (nested("(", ")", 11), false),
            ("(1)".to_owned() + &"+(1)".repeat(9), true),
            ("(1)".to_owned() + &"+(1)".repeat(10), false),
            (nested("not(", ")", 100), true),
            (nested("not(", ")", 101), false),
            ("1".to_owned() + &"+1".repeat(100), true),
            ("1".to_owned() + &"+1".repeat(101), false),
            ("/a".to_owned() + &"[b".repeat(99) + &"]".repeat(99), true),
            (
                "/a".to_owned() + &"[b".repeat(100) + &"]".repeat(100),
                false,
            ),
            ("-".repeat(101) + "1", false),
            ("'((((((((((((('".to_owned(), true),
            ("order-id-and-more".to_owned(), true),
            ("'unterminated".to_owned(), false),
            ("a and ".repeat(51) + "a", true),
            ("child::a/".repeat(34) + "a", true),
            ("@a | ".repeat(50) + "@a", false),
            ("a != ".repeat(100) + "a", true),
            ("a != ".repeat(101) + "a", false),
            ("x:y = ".repeat(100) + "1", true),
            ("x:y = ".repeat(101) + "1", false),
            ("1.5 + ".repeat(50) + "1", true),
            (". + ".repeat(50) + "1", true),
            (". + ".repeat(51) + "1", false),
            ("a-1 + ".repeat(100) + "1", true),
            ("a -1 + ".repeat(50) + "1", true),
            ("a -1 + ".repeat(51) + "1", false),
            ("//a".repeat(100), true),
            ("//a".repeat(101), false),
            ("concat(1,".repeat(51) + "1" + &")".repeat(51), true),
        ];
        for (expression, compiles) in cases {
            assert_eq!(
                within_jdk_limits(&expression).is_ok(),
                compiles,
                "{expression}"
            );
        }
    }

    #[test]
    fn calls_are_checked_as_the_jdk_checks_them() {
        for ok in [
            "concat('a','b','c','d')",
            "count (//a)",
            "child::text()",
            "a and(1)",
            "/a/b[position() = 1]",
            "//*[local-name()='x']",
            "a/text ()",
            "processing-instruction()",
            "processing-instruction('a')",
            "-count(1)",
            "1 div count(/a)",
            "foo:bar(1)",
            "1*count(/)",
            "count(/a)[1]",
            "concat('a', ('b'))",
            "substring('abc', (1), 2)",
            "concat('a,', 'b')",
            "concat('a', \"(,\")",
            "unknown",
            "last ()",
            "string-length()",
            "id('a')",
            "lang('en')",
            "current()",
            "generate-id(/)",
            "function-available('x')",
            "document-location()",
            "$x",
        ] {
            assert!(check_calls(ok).is_ok(), "{ok}");
        }
        for refused in [
            "unknown()",
            "contains('a')",
            "concat('a')",
            "substring('a',1,2,3)",
            "last(1)",
            "count()",
            "translate('a','b')",
            "processing-instruction('a', 'b')",
            "node(1)",
            "text(1)",
            "*(1)",
            "@count(1)",
            "child::count(1)",
            "Count(/a)",
            "not(true(), false())",
            "a-b(1)",
            "count(a[1], 2)",
            "key('a','b')",
            "format-number(1,'#')",
            "document('x')",
        ] {
            assert!(check_calls(refused).is_err(), "{refused}");
        }
    }
}
