//! A recursive-descent port of the `StrictParser.jj` / `HyphenatedParser.jj`
//! grammar, including the checks the `ComparisonExpression.create*`
//! factories make while the tree is built.
//!
//! Precedence, loosest first: `OR`; `AND`; `=`, `<>`, `IS [NOT] NULL`;
//! `>`, `>=`, `<`, `<=`, `[NOT] LIKE`, `[NOT] BETWEEN`, `[NOT] IN`; `+`, `-`;
//! `*`, `/`, `%`; unary `+`, `-`, `NOT`, `XPATH`, `XQUERY`. Note that `NOT`
//! binds tighter than comparisons, so `NOT a = 1` is `(NOT a) = 1`.

use crate::expr::{ArithOp, CmpOp, Constant, Expr};
use crate::java::BigInt;
use crate::lexer::{Keyword, Tok, Token, tokenize};
use crate::like::LikePattern;

/// `JmsSelector()`: parses a whole selector into a boolean expression.
pub(crate) fn parse(text: &str, hyphenated: bool) -> Result<Expr, String> {
    let mut p = Parser {
        tokens: tokenize(text, hyphenated)?,
        pos: 0,
    };
    let e = p.or_expression()?;
    if p.peek() != &Tok::Eof {
        return Err(p.unexpected());
    }
    as_boolean(e)
}

/// `asBooleanExpression`: properties become boolean casts; anything else
/// that is not boolean is rejected.
fn as_boolean(e: Expr) -> Result<Expr, String> {
    match e {
        Expr::Property(_) => Ok(Expr::BooleanCast(Box::new(e))),
        e if e.is_boolean() => Ok(e),
        e => Err(format!(
            "Expression will not result in a boolean value: {e}"
        )),
    }
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &Tok {
        &self.tokens[self.pos].tok
    }

    fn peek2(&self) -> &Tok {
        &self.tokens[(self.pos + 1).min(self.tokens.len() - 1)].tok
    }

    fn bump(&mut self) -> Tok {
        let t = self.tokens[self.pos].tok.clone();
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn eat(&mut self, tok: &Tok) -> bool {
        if self.peek() == tok {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, tok: &Tok) -> Result<(), String> {
        if self.eat(tok) {
            Ok(())
        } else {
            Err(self.unexpected())
        }
    }

    fn unexpected(&self) -> String {
        let t = &self.tokens[self.pos];
        match &t.tok {
            Tok::Eof => format!("Encountered end of input at column {}", t.column),
            tok => format!("Encountered {tok:?} at column {}", t.column),
        }
    }

    fn or_expression(&mut self) -> Result<Expr, String> {
        let mut left = self.and_expression()?;
        while self.eat(&Tok::Kw(Keyword::Or)) {
            let right = self.and_expression()?;
            left = create_or(as_boolean(left)?, as_boolean(right)?);
        }
        Ok(left)
    }

    fn and_expression(&mut self) -> Result<Expr, String> {
        let mut left = self.equality_expression()?;
        while self.eat(&Tok::Kw(Keyword::And)) {
            let right = self.equality_expression()?;
            left = create_and(as_boolean(left)?, as_boolean(right)?);
        }
        Ok(left)
    }

    fn equality_expression(&mut self) -> Result<Expr, String> {
        let mut left = self.comparison_expression()?;
        loop {
            match self.peek() {
                Tok::Eq => {
                    self.bump();
                    let right = self.comparison_expression()?;
                    left = create_equal(left, right)?;
                }
                Tok::Ne => {
                    self.bump();
                    let right = self.comparison_expression()?;
                    left = Expr::Not(Box::new(create_equal(left, right)?));
                }
                Tok::Kw(Keyword::Is) => {
                    self.bump();
                    let not = self.eat(&Tok::Kw(Keyword::Not));
                    self.expect(&Tok::Kw(Keyword::Null))?;
                    let is_null = compare(CmpOp::Eq, left, Expr::BoolConst(None));
                    left = if not {
                        Expr::Not(Box::new(is_null))
                    } else {
                        is_null
                    };
                }
                _ => return Ok(left),
            }
        }
    }

    fn comparison_expression(&mut self) -> Result<Expr, String> {
        let mut left = self.add_expression()?;
        loop {
            let op = match self.peek() {
                Tok::Gt => Some(CmpOp::Gt),
                Tok::Ge => Some(CmpOp::Ge),
                Tok::Lt => Some(CmpOp::Lt),
                Tok::Le => Some(CmpOp::Le),
                _ => None,
            };
            if let Some(op) = op {
                self.bump();
                let right = self.add_expression()?;
                left = create_ordering(op, left, right)?;
                continue;
            }
            let not = match (self.peek(), self.peek2()) {
                (
                    Tok::Kw(Keyword::Not),
                    Tok::Kw(Keyword::Like | Keyword::Between | Keyword::In),
                ) => {
                    self.bump();
                    true
                }
                _ => false,
            };
            match self.peek() {
                Tok::Kw(Keyword::Like) => {
                    self.bump();
                    let pattern = self.string_literal()?;
                    let escape = if self.eat(&Tok::Kw(Keyword::Escape)) {
                        Some(self.string_literal()?)
                    } else {
                        None
                    };
                    let like = Expr::Like(
                        Box::new(left),
                        LikePattern::new(&pattern, escape.as_deref())?,
                    );
                    left = if not { Expr::Not(Box::new(like)) } else { like };
                }
                Tok::Kw(Keyword::Between) => {
                    self.bump();
                    let low = self.add_expression()?;
                    self.expect(&Tok::Kw(Keyword::And))?;
                    let high = self.add_expression()?;
                    left = if not {
                        create_or(
                            create_ordering(CmpOp::Lt, left.clone(), low)?,
                            create_ordering(CmpOp::Gt, left, high)?,
                        )
                    } else {
                        create_and(
                            create_ordering(CmpOp::Ge, left.clone(), low)?,
                            create_ordering(CmpOp::Le, left, high)?,
                        )
                    };
                }
                Tok::Kw(Keyword::In) => {
                    self.bump();
                    self.expect(&Tok::LParen)?;
                    let mut values = vec![self.string_literal()?];
                    while self.eat(&Tok::Comma) {
                        values.push(self.string_literal()?);
                    }
                    self.expect(&Tok::RParen)?;
                    let Expr::Property(property) = left else {
                        return Err(format!(
                            "Expected a property for In expression, got: {left}"
                        ));
                    };
                    left = Expr::In {
                        property,
                        values,
                        not,
                    };
                }
                _ => return Ok(left),
            }
        }
    }

    fn add_expression(&mut self) -> Result<Expr, String> {
        let mut left = self.mult_expr()?;
        loop {
            let op = match self.peek() {
                Tok::Plus => ArithOp::Plus,
                Tok::Minus => ArithOp::Minus,
                _ => return Ok(left),
            };
            self.bump();
            let right = self.mult_expr()?;
            left = Expr::Arith(op, Box::new(left), Box::new(right));
        }
    }

    fn mult_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.unary_expr()?;
        loop {
            let op = match self.peek() {
                Tok::Star => ArithOp::Multiply,
                Tok::Slash => ArithOp::Divide,
                Tok::Percent => ArithOp::Mod,
                _ => return Ok(left),
            };
            self.bump();
            let right = self.unary_expr()?;
            left = Expr::Arith(op, Box::new(left), Box::new(right));
        }
    }

    fn unary_expr(&mut self) -> Result<Expr, String> {
        match self.peek() {
            Tok::Plus => {
                self.bump();
                self.unary_expr()
            }
            Tok::Minus => {
                self.bump();
                Ok(Expr::Negate(Box::new(self.unary_expr()?)))
            }
            Tok::Kw(Keyword::Not) => {
                self.bump();
                Ok(Expr::Not(Box::new(as_boolean(self.unary_expr()?)?)))
            }
            Tok::Kw(Keyword::XPath) => {
                self.bump();
                Ok(Expr::XPath(self.string_literal()?))
            }
            Tok::Kw(Keyword::XQuery) => {
                self.bump();
                Ok(Expr::XQuery(self.string_literal()?))
            }
            _ => self.primary_expr(),
        }
    }

    fn primary_expr(&mut self) -> Result<Expr, String> {
        let e = match self.peek().clone() {
            Tok::LParen => {
                self.bump();
                let e = self.or_expression()?;
                self.expect(&Tok::RParen)?;
                return Ok(e);
            }
            Tok::Str(s) => Expr::Const(Constant::Str(s)),
            Tok::Decimal(t) => Expr::Const(from_decimal(&t)),
            Tok::Hex(t) => Expr::Const(narrow(
                i64::from_str_radix(&t[2..], 16).map_err(|e| format!("{t}: {e}"))?,
            )),
            Tok::Octal(t) => Expr::Const(narrow(
                i64::from_str_radix(&t, 8).map_err(|e| format!("{t}: {e}"))?,
            )),
            Tok::Float(t) => Expr::Const(Constant::Double(
                crate::java::parse_double(&t).ok_or_else(|| format!("bad float {t}"))?,
            )),
            Tok::Kw(Keyword::True) => Expr::BoolConst(Some(true)),
            Tok::Kw(Keyword::False) => Expr::BoolConst(Some(false)),
            Tok::Kw(Keyword::Null) => Expr::BoolConst(None),
            Tok::Id(name) => Expr::Property(name),
            _ => return Err(self.unexpected()),
        };
        self.bump();
        Ok(e)
    }

    fn string_literal(&mut self) -> Result<String, String> {
        match self.peek().clone() {
            Tok::Str(s) => {
                self.bump();
                Ok(s)
            }
            _ => Err(self.unexpected()),
        }
    }
}

/// `ConstantExpression.createFromDecimal`.
fn from_decimal(text: &str) -> Constant {
    let digits = text.trim_end_matches(['l', 'L']);
    match digits.parse::<i64>() {
        Ok(v) => narrow(v),
        Err(_) => {
            let big = BigInt::from_digits(digits);
            // Java narrows on the wrapped `longValue()`.
            let wrapped = big.long_value();
            if i32::try_from(wrapped).is_ok() {
                Constant::Int(big.int_value())
            } else {
                Constant::Big(big)
            }
        }
    }
}

/// Integer literals that fit in an `int` are `Integer`s.
fn narrow(v: i64) -> Constant {
    match i32::try_from(v) {
        Ok(i) => Constant::Int(i),
        Err(_) => Constant::Long(v),
    }
}

fn compare(op: CmpOp, left: Expr, right: Expr) -> Expr {
    Expr::Compare(op, Box::new(left), Box::new(right))
}

/// `LogicExpression.createOR`: extends a left-hand OR in place.
fn create_or(left: Expr, right: Expr) -> Expr {
    match left {
        Expr::Or(mut list) => {
            list.push(right);
            Expr::Or(list)
        }
        left => Expr::Or(vec![left, right]),
    }
}

fn create_and(left: Expr, right: Expr) -> Expr {
    match left {
        Expr::And(mut list) => {
            list.push(right);
            Expr::And(list)
        }
        left => Expr::And(vec![left, right]),
    }
}

/// `ComparisonExpression.createEqual`, with `checkEqualOperand` and
/// `checkEqualOperandCompatibility`.
fn create_equal(left: Expr, right: Expr) -> Result<Expr, String> {
    for e in [&left, &right] {
        if *e == Expr::BoolConst(None) {
            return Err(format!("'{e}' cannot be compared."));
        }
    }
    if matches!(left, Expr::BoolConst(_)) && matches!(right, Expr::Const(_)) {
        return Err(format!("'{left}' cannot be compared with '{right}'"));
    }
    Ok(compare(CmpOp::Eq, left, right))
}

/// `createGreaterThan` and friends, with `checkLessThanOperand`: constants
/// must be numbers, and boolean expressions are rejected.
fn create_ordering(op: CmpOp, left: Expr, right: Expr) -> Result<Expr, String> {
    for e in [&left, &right] {
        let ok = match e {
            Expr::Const(Constant::Str(_)) => false,
            Expr::Const(_) => true,
            e => !e.is_boolean(),
        };
        if !ok {
            return Err(format!("Value '{e}' cannot be compared."));
        }
    }
    Ok(compare(op, left, right))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(text: &str) -> Expr {
        parse(text, false).unwrap_or_else(|e| panic!("{text}: {e}"))
    }

    // SelectorParserTest.testParseXPath
    #[test]
    fn parse_xpath() {
        assert_eq!(
            p("XPATH '//title[@lang=''eng'']'"),
            Expr::XPath("//title[@lang='eng']".into())
        );
    }

    // SelectorParserTest.testParseWithParensAround
    #[test]
    fn parse_with_parens_around() {
        for value in [
            "x = 1 and y = 2",
            "(x = 1) and (y = 2)",
            "((x = 1) and (y = 2))",
        ] {
            let Expr::And(list) = p(value) else {
                panic!("{value}: not a LogicExpression");
            };
            let [left, right] = &list[..] else {
                panic!("{value}: not binary");
            };
            let (Expr::Compare(_, l, _), Expr::Compare(_, r, _)) = (left, right) else {
                panic!("{value}: operands are not comparisons");
            };
            assert_eq!(**l, Expr::Property("x".into()));
            assert_eq!(**r, Expr::Property("y".into()));
        }
    }

    // UnaryExpressionTest.testEquals
    #[test]
    fn unary_equals() {
        let e1 = Expr::Not(Box::new(p("x = 1")));
        let e2 = Expr::Not(Box::new(p("x = 1")));
        assert_eq!(e1, e2);
    }

    // UnaryExpressionTest.testInExpressionToString
    #[test]
    fn in_expression_to_string() {
        let e = Expr::In {
            property: "foo".into(),
            values: vec!["bar".into()],
            not: false,
        };
        assert_eq!(e.to_string(), "foo IN ( bar )");
        let e = Expr::In {
            property: "foo".into(),
            values: vec![],
            not: false,
        };
        assert_eq!(e.to_string(), "foo IN (  )");
    }

    #[test]
    fn to_string_matches_java() {
        assert_eq!(
            p("a = 1 AND b > 2.5 OR c LIKE 'x' OR NOT d").to_string(),
            "(( (a = 1) AND (b > 2.5) ) OR (LIKE c) OR (NOT d))"
        );
        assert_eq!(p("a <> 'it''s'").to_string(), "(NOT (a = 'it''s'))");
        assert_eq!(p("-a + 1 > 0").to_string(), "(((- a) + 1) > 0)");
    }

    #[test]
    fn literals() {
        let constant = |text: &str| match p(&format!("a = {text}")) {
            Expr::Compare(_, _, r) => match *r {
                Expr::Const(c) => c,
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        };
        assert_eq!(constant("1"), Constant::Int(1));
        assert_eq!(constant("1L"), Constant::Int(1));
        assert_eq!(constant("2147483648"), Constant::Long(2_147_483_648));
        assert_eq!(constant("0x7FFFFFFFFFFFFFFF"), Constant::Long(i64::MAX));
        assert_eq!(constant("0X1f"), Constant::Int(31));
        assert_eq!(constant("010"), Constant::Int(8));
        assert_eq!(constant("0"), Constant::Int(0));
        assert_eq!(constant("1.5e1"), Constant::Double(15.0));
        assert_eq!(
            constant("9223372036854775808"),
            Constant::Big(BigInt::from_digits("9223372036854775808"))
        );
        // 2^64 wraps to a long of 0, which Java then narrows to Integer 0.
        assert_eq!(constant("18446744073709551616"), Constant::Int(0));
        assert!(parse("a = 0x8000000000000000", false).is_err());
        assert!(parse("a = 08", false).is_err());
    }

    #[test]
    fn not_binds_tighter_than_comparison() {
        assert_eq!(
            p("NOT a = TRUE"),
            Expr::Compare(
                CmpOp::Eq,
                Box::new(Expr::Not(Box::new(Expr::BooleanCast(Box::new(
                    Expr::Property("a".into())
                ))))),
                Box::new(Expr::BoolConst(Some(true)))
            )
        );
    }

    #[test]
    fn rejected_by_factories() {
        for text in [
            "a = NULL",
            "NULL = a",
            "TRUE = 1",
            "TRUE = 'x'",
            "a > 'x'",
            "a > TRUE",
            "a > NULL",
            "(a = 1) > 1",
            "a BETWEEN 'a' AND 'z'",
            "1 IN ('a')",
            "(a) + 1 IN ('a')",
            "a LIKE 'x' ESCAPE 'ab'",
            "a LIKE '%a%b%c%d%e%f'",
            "1",
            "a + 1",
            "-a",
            "NOT 1",
            "'x'",
            "a IN ()",
            "a IN (1)",
            "a = 1 b",
            "a != 1",
            "a IS NOT",
            "a NOT = 1",
        ] {
            assert!(parse(text, false).is_err(), "{text} should be rejected");
        }
        for text in [
            "1 = TRUE",
            "TRUE = FALSE",
            "NULL IS NULL",
            "a > b",
            "(a) IN ('x')",
            "a IS NULL",
            "a",
            "(a)",
            "XQUERY 'x'",
            "a = 1 -- trailing comment\n",
            "a = 1 /* block */",
        ] {
            assert!(parse(text, false).is_ok(), "{text} should be accepted");
        }
        // Without a newline "--" is two minus signs: a = 1 - (-comment).
        assert_eq!(p("a = 1 -- comment").to_string(), "(a = (1 - (- comment)))");
        assert!(parse("a = 1 -- not a comment", false).is_err());
    }
}
