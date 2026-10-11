//! Operation parameters, matched to an operation's signature as
//! `ManagementServiceImpl.invokeOperation` matches them to a Java method:
//! by name and count, then each parameter by type after
//! `JsonUtil.convertJsonValue`. A null fits any parameter, any number fits
//! any numeric one, and nothing else converts.

use super::json::Json;

/// A Java parameter type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ty {
    /// `String`.
    S,
    /// `int` or `Integer`.
    I,
    /// `long` or `Long`.
    L,
    /// `double`.
    D,
    /// `boolean` or `Boolean`.
    B,
    /// `Map`.
    M,
    /// `Object[]`.
    A,
}

/// The parameters of a matched call, converted to the operation's types.
#[derive(Debug, Clone)]
pub struct Args(Vec<Json>);

impl Args {
    /// A `String` parameter; `None` when null.
    pub fn s(&self, i: usize) -> Option<String> {
        match &self.0[i] {
            Json::String(s) => Some(s.clone()),
            _ => None,
        }
    }

    /// A `String` parameter, null as empty.
    pub fn s_or_empty(&self, i: usize) -> String {
        self.s(i).unwrap_or_default()
    }

    /// An `int`; a null boxed `Integer` is `None`.
    pub fn oi(&self, i: usize) -> Option<i32> {
        self.0[i].as_i64().map(|n| n as i32)
    }

    pub fn i(&self, i: usize) -> i32 {
        self.oi(i).unwrap_or(0)
    }

    pub fn ol(&self, i: usize) -> Option<i64> {
        self.0[i].as_i64()
    }

    pub fn l(&self, i: usize) -> i64 {
        self.ol(i).unwrap_or(0)
    }

    pub fn d(&self, i: usize) -> f64 {
        match &self.0[i] {
            Json::Long(n) => *n as f64,
            Json::Double(d) => *d,
            _ => 0.0,
        }
    }

    pub fn ob(&self, i: usize) -> Option<bool> {
        self.0[i].as_bool()
    }

    pub fn b(&self, i: usize) -> bool {
        self.ob(i).unwrap_or(false)
    }

    /// A `Map<String, String>` parameter, its values as strings.
    pub fn string_map(&self, i: usize) -> Option<Vec<(String, String)>> {
        match &self.0[i] {
            Json::Object(members) => Some(
                members
                    .iter()
                    .map(|(k, v)| {
                        let v = match v {
                            Json::String(s) => s.clone(),
                            Json::Null => String::new(),
                            other => other.to_string(),
                        };
                        (k.clone(), v)
                    })
                    .collect(),
            ),
            _ => None,
        }
    }

    pub fn array(&self, i: usize) -> Option<&[Json]> {
        match &self.0[i] {
            Json::Array(items) => Some(items),
            _ => None,
        }
    }

    pub fn is_null(&self, i: usize) -> bool {
        self.0[i].is_null()
    }
}

/// An operation call: the name and the parameters as the request gave them.
pub struct Call<'a> {
    pub op: &'a str,
    pub params: &'a [Json],
}

impl Call<'_> {
    /// The parameters, if the call names `op` with parameters of these
    /// types.
    pub fn is(&self, op: &str, types: &[Ty]) -> Option<Args> {
        if self.op != op || self.params.len() != types.len() {
            return None;
        }
        let mut out = Vec::with_capacity(types.len());
        for (p, t) in self.params.iter().zip(types) {
            let fits = matches!(
                (p, t),
                (Json::Null, _)
                    | (Json::Long(_) | Json::Double(_), Ty::I | Ty::L | Ty::D)
                    | (Json::String(_), Ty::S)
                    | (Json::Bool(_), Ty::B)
                    | (Json::Object(_), Ty::M)
                    | (Json::Array(_), Ty::A)
            );
            if !fits {
                return None;
            }
            out.push(match (p, t) {
                (Json::Double(d), Ty::I | Ty::L) => Json::Long(*d as i64),
                _ => p.clone(),
            });
        }
        Some(Args(out))
    }
}
