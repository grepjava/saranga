//! JSON, as `JsonUtil` in `artemis-commons` reads and writes it: the
//! management messages' parameters and results, the JSON strings the list
//! operations return, and JSON broker properties. Written as Johnzon, which
//! Artemis uses, writes it: objects keep their insertion order, numbers
//! are printed as Java prints a `long` or a `double`, and strings escape
//! only quotes, backslashes and control characters.

use std::fmt::Write;

/// A JSON value. Integral numbers are kept apart from the others, as
/// `JsonNumber.isIntegral` tells them apart.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Long(i64),
    Double(f64),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    pub fn object() -> Self {
        Self::Object(Vec::new())
    }

    /// Adds a member to an object, replacing one of the same name
    /// (`JsonObjectBuilder.add`).
    pub fn with(mut self, key: &str, value: impl Into<Json>) -> Self {
        self.put(key, value);
        self
    }

    pub fn put(&mut self, key: &str, value: impl Into<Json>) {
        if let Self::Object(members) = self {
            let value = value.into();
            match members.iter_mut().find(|(k, _)| k == key) {
                Some((_, v)) => *v = value,
                None => members.push((key.to_owned(), value)),
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Self::Object(members) => members.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Long(n) => Some(*n),
            Self::Double(d) => Some(*d as i64),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Parses JSON text (`JsonUtil.readJsonArray` / `readJsonObject`).
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut p = Parser {
            s: text.as_bytes(),
            i: 0,
        };
        p.ws();
        let v = p.value()?;
        p.ws();
        if p.i != p.s.len() {
            return Err(format!("Unexpected character at offset {}", p.i));
        }
        Ok(v)
    }
}

impl Json {
    /// `JsonUtil.mergeAndUpdate`: `self`'s members, in order, with
    /// `update`'s merged in: objects are merged member by member, array
    /// elements index by index, anything else is replaced, and new members
    /// go last.
    pub fn merge_and_update(&self, update: &Json) -> Json {
        let (Json::Object(source), Json::Object(updates)) = (self, update) else {
            return update.clone();
        };
        let mut out = source.clone();
        for (key, value) in updates {
            match out.iter_mut().find(|(k, _)| k == key) {
                None => out.push((key.clone(), value.clone())),
                Some((_, old)) => {
                    *old = match (&*old, value) {
                        (Json::Object(_), Json::Object(_)) => old.merge_and_update(value),
                        (Json::Array(olds), Json::Array(news)) => Json::Array(
                            news.iter()
                                .enumerate()
                                .map(|(i, n)| match (olds.get(i), n) {
                                    (Some(o @ Json::Object(_)), Json::Object(_)) => {
                                        o.merge_and_update(n)
                                    }
                                    _ => n.clone(),
                                })
                                .collect(),
                        ),
                        _ => value.clone(),
                    }
                }
            }
        }
        Json::Object(out)
    }

    /// `JsonUtil.objectBuilderWithValueAtPath`: `value` nested in objects
    /// along a `/`-separated path.
    pub fn at_path(path: &str, value: Json) -> Json {
        path.rsplit('/')
            .fold(value, |v, key| Json::object().with(key, v))
    }
}

impl std::fmt::Display for Json {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = String::new();
        write_json(&mut out, self);
        f.write_str(&out)
    }
}

impl From<bool> for Json {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<i32> for Json {
    fn from(n: i32) -> Self {
        Self::Long(n.into())
    }
}

impl From<i64> for Json {
    fn from(n: i64) -> Self {
        Self::Long(n)
    }
}

impl From<usize> for Json {
    fn from(n: usize) -> Self {
        Self::Long(n as i64)
    }
}

impl From<f64> for Json {
    fn from(d: f64) -> Self {
        Self::Double(d)
    }
}

impl From<&str> for Json {
    fn from(s: &str) -> Self {
        Self::String(s.to_owned())
    }
}

impl From<String> for Json {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl<T: Into<Json>> From<Option<T>> for Json {
    fn from(v: Option<T>) -> Self {
        v.map_or(Self::Null, Into::into)
    }
}

impl<T: Into<Json>> From<Vec<T>> for Json {
    fn from(v: Vec<T>) -> Self {
        Self::Array(v.into_iter().map(Into::into).collect())
    }
}

fn write_json(out: &mut String, v: &Json) {
    match v {
        Json::Null => out.push_str("null"),
        Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Json::Long(n) => {
            let _ = write!(out, "{n}");
        }
        Json::Double(d) => out.push_str(&java_double(*d)),
        Json::String(s) => write_string(out, s),
        Json::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(out, item);
            }
            out.push(']');
        }
        Json::Object(members) => {
            out.push('{');
            for (i, (k, item)) in members.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(out, k);
                out.push(':');
                write_json(out, item);
            }
            out.push('}');
        }
    }
}

/// Johnzon's `Strings.escape`.
fn write_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// `Double.toString`: plain between 10^-3 and 10^7, otherwise in
/// computerized scientific notation, always with a fraction digit.
pub fn java_double(d: f64) -> String {
    if d.is_nan() {
        return "NaN".to_owned();
    }
    if d.is_infinite() {
        return if d > 0.0 { "Infinity" } else { "-Infinity" }.to_owned();
    }
    if d == 0.0 {
        return if d.is_sign_negative() { "-0.0" } else { "0.0" }.to_owned();
    }
    let a = d.abs();
    if (1e-3..1e7).contains(&a) {
        let s = format!("{d}");
        if s.contains('.') { s } else { format!("{s}.0") }
    } else {
        // Rust's shortest round-trip digits, laid out as Java lays them.
        let s = format!("{d:e}");
        let (mantissa, exp) = s.split_once('e').unwrap_or((&s, "0"));
        let mantissa = if mantissa.contains('.') {
            mantissa.to_owned()
        } else {
            format!("{mantissa}.0")
        };
        format!("{mantissa}E{exp}")
    }
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn err<T>(&self, what: &str) -> Result<T, String> {
        Err(format!("{what} at offset {}", self.i))
    }

    fn eat(&mut self, lit: &str) -> bool {
        if self.s[self.i..].starts_with(lit.as_bytes()) {
            self.i += lit.len();
            true
        } else {
            false
        }
    }

    fn value(&mut self) -> Result<Json, String> {
        match self.s.get(self.i) {
            None => self.err("Unexpected end of input"),
            Some(b'{') => {
                self.i += 1;
                let mut members = Vec::new();
                self.ws();
                if self.eat("}") {
                    return Ok(Json::Object(members));
                }
                loop {
                    self.ws();
                    if self.s.get(self.i) != Some(&b'"') {
                        return self.err("Expected a name");
                    }
                    let k = self.string()?;
                    self.ws();
                    if !self.eat(":") {
                        return self.err("Expected ':'");
                    }
                    self.ws();
                    let v = self.value()?;
                    members.push((k, v));
                    self.ws();
                    if self.eat(",") {
                        continue;
                    }
                    if self.eat("}") {
                        return Ok(Json::Object(members));
                    }
                    return self.err("Expected ',' or '}'");
                }
            }
            Some(b'[') => {
                self.i += 1;
                let mut items = Vec::new();
                self.ws();
                if self.eat("]") {
                    return Ok(Json::Array(items));
                }
                loop {
                    self.ws();
                    items.push(self.value()?);
                    self.ws();
                    if self.eat(",") {
                        continue;
                    }
                    if self.eat("]") {
                        return Ok(Json::Array(items));
                    }
                    return self.err("Expected ',' or ']'");
                }
            }
            Some(b'"') => self.string().map(Json::String),
            Some(b't') if self.eat("true") => Ok(Json::Bool(true)),
            Some(b'f') if self.eat("false") => Ok(Json::Bool(false)),
            Some(b'n') if self.eat("null") => Ok(Json::Null),
            Some(c) if *c == b'-' || c.is_ascii_digit() => self.number(),
            Some(_) => self.err("Unexpected character"),
        }
    }

    fn number(&mut self) -> Result<Json, String> {
        let start = self.i;
        let mut integral = true;
        while let Some(&c) = self.s.get(self.i) {
            match c {
                b'0'..=b'9' | b'-' | b'+' => {}
                b'.' | b'e' | b'E' => integral = false,
                _ => break,
            }
            self.i += 1;
        }
        let text = std::str::from_utf8(&self.s[start..self.i]).unwrap_or("");
        if integral && let Ok(n) = text.parse::<i64>() {
            return Ok(Json::Long(n));
        }
        // `isIntegral` holds for a value like 1E3 or 2.0 too, which
        // `longValue` reads as a long.
        match text.parse::<f64>() {
            Ok(d) if d.fract() == 0.0 && d.abs() < 9.2e18 && !text.contains('.') => {
                Ok(Json::Long(d as i64))
            }
            // Johnzon keeps a number past a double's range as a BigDecimal;
            // as a double it would become an infinity, which JSON cannot
            // write, so it is refused.
            Ok(d) if d.is_finite() => Ok(Json::Double(d)),
            _ => self.err("Invalid number"),
        }
    }

    fn string(&mut self) -> Result<String, String> {
        self.i += 1;
        let mut out = Vec::new();
        loop {
            let Some(&c) = self.s.get(self.i) else {
                return self.err("Unterminated string");
            };
            self.i += 1;
            match c {
                b'"' => break,
                b'\\' => {
                    let Some(&e) = self.s.get(self.i) else {
                        return self.err("Unterminated string");
                    };
                    self.i += 1;
                    let ch = match e {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{8}',
                        b'f' => '\u{c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => {
                            let mut code = self.hex4()?;
                            if (0xd800..0xdc00).contains(&code) && self.eat("\\u") {
                                let low = self.hex4()?;
                                code = 0x10000 + ((code - 0xd800) << 10) + (low - 0xdc00);
                            }
                            char::from_u32(code).unwrap_or('\u{fffd}')
                        }
                        _ => return self.err("Invalid escape"),
                    };
                    let mut buf = [0; 4];
                    out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                }
                c => out.push(c),
            }
        }
        String::from_utf8(out).map_err(|_| "Invalid UTF-8 in a string".to_owned())
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let Some(h) = self.s.get(self.i..self.i + 4) else {
            return self.err("Invalid escape");
        };
        let code = std::str::from_utf8(h)
            .ok()
            .and_then(|h| u32::from_str_radix(h, 16).ok());
        self.i += 4;
        code.ok_or_else(|| format!("Invalid escape at offset {}", self.i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let text = r#"[1,"a\"b\\c\n",true,null,{"x":1.5,"y":[]},-7,1.0E10]"#;
        let v = Json::parse(text).unwrap();
        assert_eq!(v.to_string(), text);
    }

    #[test]
    fn java_doubles() {
        assert_eq!(java_double(1.5), "1.5");
        assert_eq!(java_double(2.0), "2.0");
        assert_eq!(java_double(1e7), "1.0E7");
        assert_eq!(java_double(0.0001), "1.0E-4");
        assert_eq!(java_double(123.456), "123.456");
    }
}
