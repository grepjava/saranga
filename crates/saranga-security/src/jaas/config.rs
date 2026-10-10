//! JAAS configuration files (`login.config`). Ported from the JDK's
//! `sun.security.provider.ConfigFile`:
//!
//! ```text
//! activemq {
//!    org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule required
//!        debug=false
//!        org.apache.activemq.jaas.properties.user="artemis-users.properties";
//! };
//! ```
//!
//! Comments are `//` and `/* */`. Option values may be quoted, and
//! `${name}` in a value is expanded from the lookup the caller gives (the
//! JDK expands system properties); an unknown name is an error, as it is in
//! the JDK.

use std::fmt;

/// `AppConfigurationEntry.LoginModuleControlFlag`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlFlag {
    Required,
    Requisite,
    Sufficient,
    Optional,
}

impl ControlFlag {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "REQUIRED" => Some(Self::Required),
            "REQUISITE" => Some(Self::Requisite),
            "SUFFICIENT" => Some(Self::Sufficient),
            "OPTIONAL" => Some(Self::Optional),
            _ => None,
        }
    }
}

/// One login module line of an entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleEntry {
    pub class_name: String,
    pub flag: ControlFlag,
    /// In file order; a repeated key keeps its last value.
    pub options: Vec<(String, String)>,
}

impl ModuleEntry {
    pub fn option(&self, key: &str) -> Option<&str> {
        self.options
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// `Boolean.parseBoolean(options.get(key))`.
    pub fn bool_option(&self, key: &str) -> bool {
        self.option(key)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }
}

/// A parsed configuration file: named entries (domains) of login modules.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoginConfig {
    pub entries: Vec<(String, Vec<ModuleEntry>)>,
}

impl LoginConfig {
    pub fn entry(&self, name: &str) -> Option<&[ModuleEntry]> {
        self.entries
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, m)| m.as_slice())
    }

    pub fn parse(text: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ParseError> {
        Parser {
            tokens: tokenize(text)?,
            pos: 0,
            lookup: &lookup,
        }
        .parse()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("login configuration, line {line}: {message}")]
pub struct ParseError {
    pub line: usize,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Word(String),
    Quoted(String),
    Char(char),
}

impl fmt::Display for Tok {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Tok::Word(w) => f.write_str(w),
            Tok::Quoted(q) => write!(f, "\"{q}\""),
            Tok::Char(c) => write!(f, "{c}"),
        }
    }
}

/// `StreamTokenizer` as `ConfigFile` sets it up: letters, digits and
/// `. $ _ - *` make words; `"` and `'` quote; `//` and `/* */` are comments.
fn tokenize(text: &str) -> Result<Vec<(Tok, usize)>, ParseError> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    let mut line = 1;
    let is_word = |c: char| c.is_alphanumeric() || "._$-*".contains(c) || c as u32 >= 160;
    while i < chars.len() {
        let c = chars[i];
        if c == '\n' {
            line += 1;
            i += 1;
        } else if c.is_whitespace() || (c as u32) < 32 {
            i += 1;
        } else if c == '/' && chars.get(i + 1) == Some(&'/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                if chars[i] == '\n' {
                    line += 1;
                }
                i += 1;
            }
            i += 2;
        } else if c == '"' || c == '\'' {
            let start_line = line;
            let mut s = String::new();
            i += 1;
            loop {
                let Some(&d) = chars.get(i) else {
                    return Err(ParseError {
                        line: start_line,
                        message: "unterminated quoted string".into(),
                    });
                };
                i += 1;
                if d == c || d == '\n' {
                    if d == '\n' {
                        line += 1;
                    }
                    break;
                }
                if d == '\\' {
                    let Some(&e) = chars.get(i) else { break };
                    i += 1;
                    s.push(match e {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        'b' => '\u{8}',
                        'f' => '\u{c}',
                        'a' => '\u{7}',
                        'v' => '\u{b}',
                        other => other,
                    });
                } else {
                    s.push(d);
                }
            }
            out.push((Tok::Quoted(s), start_line));
        } else if is_word(c) {
            let start = i;
            while i < chars.len() && is_word(chars[i]) {
                i += 1;
            }
            out.push((Tok::Word(chars[start..i].iter().collect()), line));
        } else {
            out.push((Tok::Char(c), line));
            i += 1;
        }
    }
    Ok(out)
}

struct Parser<'a> {
    tokens: Vec<(Tok, usize)>,
    pos: usize,
    lookup: &'a dyn Fn(&str) -> Option<String>,
}

impl Parser<'_> {
    fn line(&self) -> usize {
        self.tokens
            .get(self.pos)
            .or(self.tokens.last())
            .map(|(_, l)| *l)
            .unwrap_or(1)
    }

    fn error(&self, message: impl Into<String>) -> ParseError {
        ParseError {
            line: self.line(),
            message: message.into(),
        }
    }

    fn peek_char(&self, c: char) -> bool {
        matches!(self.tokens.get(self.pos), Some((Tok::Char(d), _)) if *d == c)
    }

    fn expect_char(&mut self, c: char) -> Result<(), ParseError> {
        if self.peek_char(c) {
            self.pos += 1;
            Ok(())
        } else {
            Err(self.error(format!("expected [{c}], found [{}]", self.found())))
        }
    }

    fn found(&self) -> String {
        match self.tokens.get(self.pos) {
            Some((t, _)) => t.to_string(),
            None => "end of file".into(),
        }
    }

    /// A word or a quoted string.
    fn expect_value(&mut self, what: &str) -> Result<String, ParseError> {
        match self.tokens.get(self.pos) {
            Some((Tok::Word(w), _)) | Some((Tok::Quoted(w), _)) => {
                let w = w.clone();
                self.pos += 1;
                Ok(w)
            }
            _ => Err(self.error(format!("expected [{what}], found [{}]", self.found()))),
        }
    }

    fn parse(mut self) -> Result<LoginConfig, ParseError> {
        let mut config = LoginConfig::default();
        while self.pos < self.tokens.len() {
            let name = self.expect_value("application name")?;
            self.expect_char('{')?;
            let mut modules = Vec::new();
            while !self.peek_char('}') {
                let class_name = self.expect_value("module class name")?;
                let flag_text = self.expect_value("controlFlag")?;
                let flag = ControlFlag::parse(&flag_text)
                    .ok_or_else(|| self.error(format!("invalid control flag: {flag_text}")))?;
                let mut options: Vec<(String, String)> = Vec::new();
                while !self.peek_char(';') {
                    let key = self.expect_value("option key")?;
                    self.expect_char('=')?;
                    let raw = self.expect_value("option value")?;
                    let value = expand(&raw, self.lookup)
                        .map_err(|name| self.error(format!("unable to expand property {name}")))?;
                    match options.iter_mut().find(|(k, _)| *k == key) {
                        Some(entry) => entry.1 = value,
                        None => options.push((key, value)),
                    }
                }
                self.pos += 1;
                modules.push(ModuleEntry {
                    class_name,
                    flag,
                    options,
                });
            }
            self.expect_char('}')?;
            self.expect_char(';')?;
            if config.entry(&name).is_some() {
                return Err(self.error(format!("Can not specify multiple entries for {name}")));
            }
            config.entries.push((name, modules));
        }
        Ok(config)
    }
}

/// `PropertyExpander.expand`: `${name}` from the lookup, `${/}` as the file
/// separator. Returns the unknown name on failure.
fn expand(value: &str, lookup: &dyn Fn(&str) -> Option<String>) -> Result<String, String> {
    let mut out = String::new();
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            out.push_str(&rest[start..]);
            return Ok(out);
        };
        let name = &after[..end];
        if name == "/" {
            out.push(std::path::MAIN_SEPARATOR);
        } else {
            out.push_str(&lookup(name).ok_or_else(|| name.to_owned())?);
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ANONYMOUS: &str = r#"/*
 * Licensed to the Apache Software Foundation (ASF)
 */
activemq {
   org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule sufficient
       debug=false
       reload=true
       org.apache.activemq.jaas.properties.user="artemis-users.properties"
       org.apache.activemq.jaas.properties.role="artemis-roles.properties";

   org.apache.activemq.artemis.spi.core.security.jaas.GuestLoginModule sufficient
       debug=false
       org.apache.activemq.jaas.guest.user="admin"
       org.apache.activemq.jaas.guest.role="amq";
};
"#;

    #[test]
    fn artemis_create_anonymous() {
        let config = LoginConfig::parse(ANONYMOUS, |_| None).unwrap();
        let modules = config.entry("activemq").unwrap();
        assert_eq!(modules.len(), 2);
        assert_eq!(
            modules[0].class_name,
            "org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule"
        );
        assert_eq!(modules[0].flag, ControlFlag::Sufficient);
        assert_eq!(
            modules[0].option("org.apache.activemq.jaas.properties.user"),
            Some("artemis-users.properties")
        );
        assert!(modules[0].bool_option("reload"));
        assert!(!modules[0].bool_option("debug"));
        assert_eq!(
            modules[1].option("org.apache.activemq.jaas.guest.role"),
            Some("amq")
        );
    }

    #[test]
    fn several_entries_and_expansion() {
        let text =
            "// c\na { x.Y Required k=\"${dir}/f\" j=v; };\nb { x.Z OPTIONAL; x.W requisite; };";
        let config = LoginConfig::parse(text, |n| (n == "dir").then(|| "/etc".to_owned())).unwrap();
        assert_eq!(config.entry("a").unwrap()[0].flag, ControlFlag::Required);
        assert_eq!(config.entry("a").unwrap()[0].option("k"), Some("/etc/f"));
        assert_eq!(config.entry("b").unwrap().len(), 2);
        assert_eq!(config.entry("b").unwrap()[1].flag, ControlFlag::Requisite);
        assert!(config.entry("c").is_none());
    }

    #[test]
    fn errors() {
        assert!(LoginConfig::parse("a { x.Y maybe; };", |_| None).is_err());
        assert!(LoginConfig::parse("a { x.Y required k=\"${nope}\"; };", |_| None).is_err());
        assert!(LoginConfig::parse("a { x.Y required; };\na { };", |_| None).is_err());
        let e = LoginConfig::parse("a {\n x.Y required k v; };", |_| None).unwrap_err();
        assert_eq!(e.line, 2);
    }
}
