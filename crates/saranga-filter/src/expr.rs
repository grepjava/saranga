//! The selector expression tree and its evaluation. Ported from the
//! `org.apache.activemq.artemis.selector.filter` package:
//! `ComparisonExpression`, `LogicExpression`, `ArithmeticExpression`,
//! `UnaryExpression`, `ConstantExpression`, `PropertyExpression`,
//! `XPathExpression` and `XQueryExpression`.
//!
//! Evaluation yields `Ok(None)` for SQL's unknown (Java's `null`), and `Err`
//! where Java throws; `FilterImpl.match` turns an exception into "no match".

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;

use crate::java::{self, BigInt};
use crate::like::LikePattern;
use crate::{Filterable, PropertyError, Value};

/// An exception thrown while evaluating a selector.
#[derive(Debug)]
pub(crate) struct EvalError(#[allow(dead_code)] pub String);

impl From<PropertyError> for EvalError {
    fn from(e: PropertyError) -> Self {
        Self(e.to_string())
    }
}

type Eval<'a> = Result<Option<Val<'a>>, EvalError>;

/// A runtime value, one variant per Java class the selector distinguishes.
#[derive(Debug, Clone)]
pub(crate) enum Val<'a> {
    Bool(bool),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Char(u16),
    Str(Cow<'a, str>),
    /// `byte[]`: its contents never matter to the selector.
    Bytes,
    /// `BigDecimal`, only ever produced by integer literals beyond `long`.
    Big(BigInt),
}

impl From<Value> for Val<'_> {
    fn from(v: Value) -> Self {
        match v {
            Value::Bool(b) => Val::Bool(b),
            Value::Byte(b) => Val::Byte(b),
            Value::Short(s) => Val::Short(s),
            Value::Int(i) => Val::Int(i),
            Value::Long(l) => Val::Long(l),
            Value::Float(f) => Val::Float(f),
            Value::Double(d) => Val::Double(d),
            Value::Char(c) => Val::Char(c),
            Value::String(s) => Val::Str(Cow::Owned(s)),
            Value::Bytes(_) => Val::Bytes,
        }
    }
}

impl Val<'_> {
    /// Java's `toString()`, as used by string concatenation.
    fn java_string(&self) -> Cow<'_, str> {
        match self {
            Val::Bool(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
            Val::Byte(v) => v.to_string().into(),
            Val::Short(v) => v.to_string().into(),
            Val::Int(v) => v.to_string().into(),
            Val::Long(v) => v.to_string().into(),
            Val::Float(v) => java::float_to_string(*v).into(),
            Val::Double(v) => java::double_to_string(*v).into(),
            Val::Char(c) => String::from_utf16_lossy(&[*c]).into(),
            Val::Str(s) => Cow::Borrowed(s),
            // Java prints `[B@` and an identity hash code.
            Val::Bytes => Cow::Borrowed("[B@0"),
            Val::Big(b) => b.to_string().into(),
        }
    }

    fn is_number(&self) -> bool {
        matches!(
            self,
            Val::Byte(_)
                | Val::Short(_)
                | Val::Int(_)
                | Val::Long(_)
                | Val::Float(_)
                | Val::Double(_)
                | Val::Big(_)
        )
    }

    fn int_value(&self) -> i32 {
        match self {
            Val::Byte(v) => i32::from(*v),
            Val::Short(v) => i32::from(*v),
            Val::Int(v) => *v,
            Val::Long(v) => *v as i32,
            Val::Float(v) => *v as i32,
            Val::Double(v) => *v as i32,
            Val::Big(b) => b.int_value(),
            _ => unreachable!("not a number"),
        }
    }

    fn long_value(&self) -> i64 {
        match self {
            Val::Long(v) => *v,
            Val::Float(v) => *v as i64,
            Val::Double(v) => *v as i64,
            Val::Big(b) => b.long_value(),
            other => i64::from(other.int_value()),
        }
    }

    fn double_value(&self) -> f64 {
        match self {
            Val::Float(v) => f64::from(*v),
            Val::Double(v) => *v,
            Val::Big(b) => b.double_value(),
            Val::Long(v) => *v as f64,
            other => f64::from(other.int_value()),
        }
    }
}

/// A literal that is not a boolean (`ConstantExpression`).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Constant {
    Str(String),
    Int(i32),
    Long(i64),
    Double(f64),
    Big(BigInt),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArithOp {
    Plus,
    Minus,
    Multiply,
    Divide,
    Mod,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CmpOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Expr {
    Const(Constant),
    /// `ConstantExpression.TRUE`, `FALSE` and `NULL` (`None`), which are
    /// boolean expressions in Artemis.
    BoolConst(Option<bool>),
    Property(String),
    Arith(ArithOp, Box<Expr>, Box<Expr>),
    Negate(Box<Expr>),
    Compare(CmpOp, Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Like(Box<Expr>, LikePattern),
    In {
        property: String,
        values: Vec<String>,
        not: bool,
    },
    /// `UnaryExpression.createBooleanCast`: a property used as a boolean.
    BooleanCast(Box<Expr>),
    XPath(String),
    XQuery(String),
}

/// Evaluation context: the message and the `convert_string_expressions:`
/// flag, which Artemis stores in every comparison and arithmetic node of
/// the filter.
pub(crate) struct Ctx<'t, T: Filterable + ?Sized> {
    pub target: &'t T,
    pub convert_strings: bool,
}

impl Expr {
    /// `instanceof BooleanExpression`.
    pub(crate) fn is_boolean(&self) -> bool {
        !matches!(
            self,
            Expr::Const(_) | Expr::Property(_) | Expr::Arith(..) | Expr::Negate(_)
        )
    }

    /// `BooleanExpression.matches`. Only `AND` and `OR` differ from
    /// "evaluates to TRUE": they short-circuit on their operands' `matches`,
    /// so an operand that would throw may never run.
    pub(crate) fn matches<T: Filterable + ?Sized>(
        &self,
        ctx: &Ctx<'_, T>,
    ) -> Result<bool, EvalError> {
        match self {
            Expr::And(list) => {
                for e in list {
                    if !e.matches(ctx)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Expr::Or(list) => {
                for e in list {
                    if e.matches(ctx)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            _ => Ok(matches!(self.evaluate(ctx)?, Some(Val::Bool(true)))),
        }
    }

    fn evaluate_bool<T: Filterable + ?Sized>(
        &self,
        ctx: &Ctx<'_, T>,
    ) -> Result<Option<bool>, EvalError> {
        match self.evaluate(ctx)? {
            None => Ok(None),
            Some(Val::Bool(b)) => Ok(Some(b)),
            Some(other) => Err(EvalError(format!("{other:?} cannot be cast to Boolean"))),
        }
    }

    pub(crate) fn evaluate<'a, T: Filterable + ?Sized>(&'a self, ctx: &Ctx<'_, T>) -> Eval<'a> {
        match self {
            Expr::Const(c) => Ok(Some(match c {
                Constant::Str(s) => Val::Str(Cow::Borrowed(s)),
                Constant::Int(v) => Val::Int(*v),
                Constant::Long(v) => Val::Long(*v),
                Constant::Double(v) => Val::Double(*v),
                Constant::Big(v) => Val::Big(v.clone()),
            })),
            Expr::BoolConst(b) => Ok(b.map(Val::Bool)),
            Expr::Property(name) => Ok(ctx.target.try_get(name)?.map(Val::from)),
            Expr::Arith(op, left, right) => {
                let Some(l) = left.evaluate(ctx)? else {
                    return Ok(None);
                };
                let Some(r) = right.evaluate(ctx)? else {
                    return Ok(None);
                };
                arithmetic(*op, l, r, ctx.convert_strings).map(Some)
            }
            Expr::Negate(e) => match e.evaluate(ctx)? {
                Some(Val::Int(v)) => Ok(Some(Val::Int(v.wrapping_neg()))),
                Some(Val::Long(v)) => Ok(Some(Val::Long(v.wrapping_neg()))),
                Some(Val::Float(v)) => Ok(Some(Val::Float(-v))),
                Some(Val::Double(v)) => Ok(Some(Val::Double(-v))),
                Some(Val::Big(b)) => {
                    let negated = b.negate();
                    Ok(Some(if negated.is_long_min() {
                        Val::Long(i64::MIN)
                    } else {
                        Val::Big(negated)
                    }))
                }
                Some(v @ (Val::Byte(_) | Val::Short(_))) => Err(EvalError(format!(
                    "Don't know how to negate: {}",
                    v.java_string()
                ))),
                _ => Ok(None),
            },
            Expr::Compare(CmpOp::Eq, left, right) => {
                let l = left.evaluate(ctx)?;
                let r = right.evaluate(ctx)?;
                Ok(Some(Val::Bool(match (l, r) {
                    (None, None) => true,
                    (None, Some(_)) => return Ok(None),
                    (Some(_), None) => false,
                    // `byte[]` is not Comparable, and its `equals` is identity.
                    (Some(Val::Bytes), Some(_)) | (Some(_), Some(Val::Bytes)) => false,
                    (Some(l), Some(r)) => compare(CmpOp::Eq, l, r, ctx.convert_strings),
                })))
            }
            Expr::Compare(op, left, right) => {
                let l = match left.evaluate(ctx)? {
                    None => return Ok(None),
                    Some(Val::Bytes) => return Err(not_comparable()),
                    Some(v) => v,
                };
                let r = match right.evaluate(ctx)? {
                    None => return Ok(None),
                    Some(Val::Bytes) => return Err(not_comparable()),
                    Some(v) => v,
                };
                Ok(Some(Val::Bool(compare(*op, l, r, ctx.convert_strings))))
            }
            Expr::Not(e) => Ok(e.evaluate_bool(ctx)?.map(|b| Val::Bool(!b))),
            Expr::And(list) => {
                let mut some_nulls = false;
                for e in list {
                    match e.evaluate_bool(ctx)? {
                        Some(false) => return Ok(Some(Val::Bool(false))),
                        None => some_nulls = true,
                        Some(true) => {}
                    }
                }
                Ok((!some_nulls).then_some(Val::Bool(true)))
            }
            Expr::Or(list) => {
                let mut some_nulls = false;
                for e in list {
                    match e.evaluate_bool(ctx)? {
                        Some(true) => return Ok(Some(Val::Bool(true))),
                        None => some_nulls = true,
                        Some(false) => {}
                    }
                }
                Ok((!some_nulls).then_some(Val::Bool(false)))
            }
            Expr::Like(e, pattern) => Ok(match e.evaluate(ctx)? {
                None => None,
                Some(Val::Str(s)) => Some(Val::Bool(pattern.matches(&s))),
                Some(_) => Some(Val::Bool(false)),
            }),
            Expr::In {
                property,
                values,
                not,
            } => Ok(match ctx.target.try_get(property)? {
                Some(Value::String(s)) => Some(Val::Bool(values.contains(&s) ^ not)),
                _ => None,
            }),
            Expr::BooleanCast(e) => Ok(match e.evaluate(ctx)? {
                None => None,
                Some(Val::Bool(b)) => Some(Val::Bool(b)),
                Some(_) => Some(Val::Bool(false)),
            }),
            // XPath evaluation against the message body is not implemented;
            // XQuery is always false in Artemis too.
            Expr::XPath(_) | Expr::XQuery(_) => Ok(Some(Val::Bool(false))),
        }
    }
}

fn not_comparable() -> EvalError {
    EvalError("[B cannot be cast to java.lang.Comparable".into())
}

/// `ArithmeticExpression.asNumber`.
fn as_number<'a>(v: Val<'a>, convert: bool) -> Result<Val<'a>, EvalError> {
    if v.is_number() {
        return Ok(v);
    }
    if convert && let Val::Str(s) = &v {
        let n = if s.contains('.') {
            java::parse_double(s).map(Val::Double)
        } else {
            java::parse_int::<i64>(s).map(Val::Long)
        };
        if let Some(n) = n {
            return Ok(n);
        }
    }
    Err(EvalError(format!(
        "Cannot convert value: {} into a number",
        v.java_string()
    )))
}

fn arithmetic<'a>(
    op: ArithOp,
    l: Val<'a>,
    r: Val<'a>,
    convert: bool,
) -> Result<Val<'a>, EvalError> {
    if op == ArithOp::Plus
        && let Val::Str(text) = &l
    {
        return Ok(Val::Str(Cow::Owned(format!("{text}{}", r.java_string()))));
    }
    let l = as_number(l, convert)?;
    let r = as_number(r, convert)?;
    let is_double = |v: &Val<'_>| matches!(v, Val::Float(_) | Val::Double(_));
    let kind = if is_double(&l) || is_double(&r) {
        2
    } else if matches!(l, Val::Long(_)) || matches!(r, Val::Long(_)) {
        1
    } else {
        0
    };
    Ok(match op {
        ArithOp::Divide => Val::Double(l.double_value() / r.double_value()),
        ArithOp::Mod => Val::Double(l.double_value() % r.double_value()),
        _ => match kind {
            0 => {
                let (a, b) = (l.int_value(), r.int_value());
                Val::Int(match op {
                    ArithOp::Plus => a.wrapping_add(b),
                    ArithOp::Minus => a.wrapping_sub(b),
                    _ => a.wrapping_mul(b),
                })
            }
            1 => {
                let (a, b) = (l.long_value(), r.long_value());
                Val::Long(match op {
                    ArithOp::Plus => a.wrapping_add(b),
                    ArithOp::Minus => a.wrapping_sub(b),
                    _ => a.wrapping_mul(b),
                })
            }
            _ => {
                let (a, b) = (l.double_value(), r.double_value());
                Val::Double(match op {
                    ArithOp::Plus => a + b,
                    ArithOp::Minus => a - b,
                    _ => a * b,
                })
            }
        },
    })
}

/// `ComparisonExpression.compare`, followed by the operator's `asBoolean`.
///
/// Values of different classes are only promoted in the directions Artemis
/// promotes them; every other mix (including `int` against `short`, `byte`
/// or `BigDecimal`) compares as FALSE whatever the operator.
fn compare(op: CmpOp, l: Val<'_>, r: Val<'_>, convert: bool) -> bool {
    let Some(ordering) = promote(l, r, convert).map(|(l, r)| same_class_cmp(&l, &r)) else {
        return false;
    };
    match op {
        CmpOp::Eq => ordering == Ordering::Equal,
        CmpOp::Gt => ordering == Ordering::Greater,
        CmpOp::Ge => ordering != Ordering::Less,
        CmpOp::Lt => ordering == Ordering::Less,
        CmpOp::Le => ordering != Ordering::Greater,
    }
}

/// Brings two values to one class, or `None` where Artemis gives up
/// (including a `NumberFormatException` from a string conversion).
fn promote<'a>(l: Val<'a>, r: Val<'a>, convert: bool) -> Option<(Val<'a>, Val<'a>)> {
    use Val::*;
    if std::mem::discriminant(&l) == std::mem::discriminant(&r) {
        return Some((l, r));
    }
    let parse_rhs = |s: &str, like: &Val<'_>| -> Option<Val<'static>> {
        Some(match like {
            Bool(_) => Bool(java::parse_boolean(s)),
            Byte(_) => Byte(java::parse_int(s)?),
            Short(_) => Short(java::parse_int(s)?),
            Int(_) => Int(java::parse_int(s)?),
            Long(_) => Long(java::parse_int(s)?),
            Float(_) => Float(java::parse_float(s)?),
            Double(_) => Double(java::parse_double(s)?),
            _ => return None,
        })
    };
    match (&l, &r) {
        (Byte(a), Short(_)) => Some((Short(i16::from(*a)), r)),
        (Byte(_) | Short(_), Int(_)) => Some((Int(l.int_value()), r)),
        (Byte(_) | Short(_) | Int(_), Long(_)) => Some((Long(l.long_value()), r)),
        (Byte(_) | Short(_) | Int(_) | Long(_), Float(_)) => Some((Float(to_float(&l)), r)),
        (Byte(_) | Short(_) | Int(_) | Long(_) | Float(_), Double(_)) => {
            Some((Double(l.double_value()), r))
        }
        (Long(_), Int(b)) => Some((l, Long(i64::from(*b)))),
        (Float(_), Int(_) | Long(_)) => {
            let rv = Float(to_float(&r));
            Some((l, rv))
        }
        (Double(_), Int(_) | Long(_) | Float(_)) => {
            let rv = Double(r.double_value());
            Some((l, rv))
        }
        (Bool(_) | Byte(_) | Short(_) | Int(_) | Long(_) | Float(_) | Double(_), Str(s))
            if convert =>
        {
            let rv = parse_rhs(s, &l)?;
            Some((l, rv))
        }
        (Str(s), Bool(_) | Byte(_) | Short(_) | Int(_) | Long(_) | Float(_) | Double(_))
            if convert =>
        {
            let lv = parse_rhs(s, &r)?;
            Some((lv, r))
        }
        _ => None,
    }
}

/// Java's widening conversion to `float`.
fn to_float(v: &Val<'_>) -> f32 {
    match v {
        Val::Long(x) => *x as f32,
        Val::Float(x) => *x,
        other => other.int_value() as f32,
    }
}

/// `compareTo` between two values of the same class.
fn same_class_cmp(l: &Val<'_>, r: &Val<'_>) -> Ordering {
    match (l, r) {
        (Val::Bool(a), Val::Bool(b)) => a.cmp(b),
        (Val::Byte(a), Val::Byte(b)) => a.cmp(b),
        (Val::Short(a), Val::Short(b)) => a.cmp(b),
        (Val::Int(a), Val::Int(b)) => a.cmp(b),
        (Val::Long(a), Val::Long(b)) => a.cmp(b),
        (Val::Float(a), Val::Float(b)) => java::compare_f32(*a, *b),
        (Val::Double(a), Val::Double(b)) => java::compare_f64(*a, *b),
        (Val::Char(a), Val::Char(b)) => a.cmp(b),
        // `String.compareTo` orders by UTF-16 code unit.
        (Val::Str(a), Val::Str(b)) => a.encode_utf16().cmp(b.encode_utf16()),
        (Val::Big(a), Val::Big(b)) => a.compare(b),
        _ => unreachable!("values of different classes"),
    }
}

/// `ConstantExpression.encodeString`.
fn encode_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Java's `toString()` of each expression class.
impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let logic = |f: &mut fmt::Formatter<'_>, list: &[Expr], symbol: &str| {
            if let [a, b] = list {
                return write!(f, "( {a} {symbol} {b} )");
            }
            f.write_str("(")?;
            for (i, e) in list.iter().enumerate() {
                if i > 0 {
                    write!(f, " {symbol} ")?;
                }
                write!(f, "{e}")?;
            }
            f.write_str(")")
        };
        match self {
            Expr::Const(Constant::Str(s)) => f.write_str(&encode_string(s)),
            Expr::Const(Constant::Int(v)) => write!(f, "{v}"),
            Expr::Const(Constant::Long(v)) => write!(f, "{v}"),
            Expr::Const(Constant::Double(v)) => f.write_str(&java::double_to_string(*v)),
            Expr::Const(Constant::Big(v)) => write!(f, "{v}"),
            Expr::BoolConst(None) => f.write_str("NULL"),
            Expr::BoolConst(Some(true)) => f.write_str("TRUE"),
            Expr::BoolConst(Some(false)) => f.write_str("FALSE"),
            Expr::Property(name) => f.write_str(name),
            Expr::Arith(op, l, r) => {
                let symbol = match op {
                    ArithOp::Plus => "+",
                    ArithOp::Minus => "-",
                    ArithOp::Multiply => "*",
                    ArithOp::Divide => "/",
                    ArithOp::Mod => "%",
                };
                write!(f, "({l} {symbol} {r})")
            }
            Expr::Compare(op, l, r) => {
                let symbol = match op {
                    CmpOp::Eq => "=",
                    CmpOp::Gt => ">",
                    CmpOp::Ge => ">=",
                    CmpOp::Lt => "<",
                    CmpOp::Le => "<=",
                };
                write!(f, "({l} {symbol} {r})")
            }
            Expr::Negate(e) => write!(f, "(- {e})"),
            Expr::Not(e) => write!(f, "(NOT {e})"),
            Expr::Like(e, _) => write!(f, "(LIKE {e})"),
            Expr::And(list) => logic(f, list, "AND"),
            Expr::Or(list) => logic(f, list, "OR"),
            Expr::In {
                property,
                values,
                not,
            } => {
                let symbol = if *not { "NOT IN" } else { "IN" };
                write!(f, "{property} {symbol} ( {} )", values.join(", "))
            }
            Expr::BooleanCast(e) => write!(f, "{e}"),
            Expr::XPath(x) => write!(f, "XPATH {}", encode_string(x)),
            Expr::XQuery(x) => write!(f, "XQUERY {}", encode_string(x)),
        }
    }
}
