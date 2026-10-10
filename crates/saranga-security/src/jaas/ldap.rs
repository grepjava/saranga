//! `LDAPLoginModule`: users and roles from an LDAP directory.
//!
//! The module connects as `connectionUsername`, finds the user's entry with
//! `userSearchMatching` under `userBase` (or takes the user name as the DN
//! when that is unset), checks the password by binding as that DN, then
//! collects roles: values of `userRoleName` on the user's entry, and the
//! entries `roleSearchMatching` finds under `roleBase` (their `roleName`
//! attribute, or their DN), optionally following nested groups with
//! `expandRoles` / `expandRolesMatching`.
//!
//! LDAP calls block, so each login runs its directory work on a thread of
//! its own (the module may be called from inside the broker's runtime).
//! GSSAPI binds are not supported.

use std::sync::Arc;
use std::time::Duration;

use ldap3::{LdapConn, LdapConnSettings, LdapError, Scope, SearchEntry};

use super::{Callbacks, LoginError, LoginModule, ModuleEntry, Principal, Subject};

/// The option keys the module itself uses (`LDAPLoginModule.ConfigKey`).
const CONFIG_KEYS: &[&str] = &[
    "debug",
    "initialContextFactory",
    "connectionURL",
    "connectionUsername",
    "connectionPassword",
    "connectionProtocol",
    "authentication",
    "userBase",
    "userSearchMatching",
    "userSearchSubtree",
    "roleBase",
    "roleName",
    "roleSearchMatching",
    "roleSearchSubtree",
    "userRoleName",
    "expandRoles",
    "expandRolesMatching",
    "saslLoginConfigScope",
    "authenticateUser",
    "referral",
    "ignorePartialResultException",
    "passwordCodec",
    "connectionTimeout",
    "readTimeout",
    "noCacheExceptions",
];

/// A failure, named after the Java exception that would be its root cause,
/// which `noCacheExceptions` lists.
struct DirError {
    java_class: &'static str,
    message: String,
}

impl DirError {
    fn from_ldap(e: LdapError) -> Self {
        let java_class = match &e {
            LdapError::Io { source } => match source.kind() {
                std::io::ErrorKind::ConnectionRefused => "java.net.ConnectException",
                std::io::ErrorKind::TimedOut => "java.net.SocketTimeoutException",
                std::io::ErrorKind::NotFound => "java.net.UnknownHostException",
                _ => "javax.naming.CommunicationException",
            },
            LdapError::Timeout { .. } => "java.net.SocketTimeoutException",
            LdapError::LdapResult { result } if result.rc == 49 => {
                "javax.naming.AuthenticationException"
            }
            _ => "javax.naming.NamingException",
        };
        Self {
            java_class,
            message: e.to_string(),
        }
    }

    fn naming(message: impl Into<String>) -> Self {
        Self {
            java_class: "javax.naming.NamingException",
            message: message.into(),
        }
    }
}

/// What a login failed with: a plain `FailedLoginException`, or one wrapping
/// a directory error.
enum Failure {
    Login(String),
    Dir(String, DirError),
}

impl From<DirError> for Failure {
    fn from(e: DirError) -> Self {
        Failure::Dir("Error contacting LDAP".into(), e)
    }
}

#[derive(Clone)]
struct Options {
    options: Vec<(String, String)>,
}

impl Options {
    fn get(&self, key: &str) -> Option<&str> {
        self.options
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// `isLoginPropertySet`: present and not empty.
    fn set(&self, key: &str) -> Option<&str> {
        self.get(key).filter(|v| !v.is_empty())
    }

    fn flag(&self, key: &str) -> bool {
        self.get(key)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }

    fn millis(&self, key: &str) -> Option<Duration> {
        self.set(key)
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&ms| ms > 0)
            .map(Duration::from_millis)
    }
}

pub(crate) struct LdapLoginModule {
    options: Options,
    authenticate_user: bool,
    no_cache: Vec<String>,
    user: Option<String>,
    authenticated: bool,
    roles: Vec<String>,
}

impl LdapLoginModule {
    pub(crate) fn new(entry: &ModuleEntry) -> Self {
        let options = Options {
            options: entry.options.clone(),
        };
        let authenticate_user = options
            .set("authenticateUser")
            .is_none_or(|v| v.eq_ignore_ascii_case("true"));
        let no_cache = options
            .set("noCacheExceptions")
            .map(|v| v.split(',').map(|s| s.trim().to_owned()).collect())
            .unwrap_or_default();
        for (k, _) in &entry.options {
            if !CONFIG_KEYS.contains(&k.as_str()) && !k.starts_with("java.naming.") {
                tracing::debug!(
                    "LDAPLoginModule: option {k} is passed to JNDI only and was ignored"
                );
            }
        }
        Self {
            options,
            authenticate_user,
            no_cache,
            user: None,
            authenticated: false,
            roles: Vec::new(),
        }
    }

    /// `handleException`: a failure whose root cause `noCacheExceptions`
    /// names (exactly or as a regular expression) is not cached.
    fn error(&self, failure: Failure) -> LoginError {
        match failure {
            Failure::Login(m) => LoginError::new(m),
            Failure::Dir(m, cause) => {
                let class = cause.java_class;
                let listed = self.no_cache.iter().any(|n| {
                    n == class
                        || regex::Regex::new(&format!("^(?:{n})$")).is_ok_and(|r| r.is_match(class))
                });
                tracing::debug!("LDAP: {m}: {}: {}", class, cause.message);
                if listed {
                    LoginError::no_cache(format!("{class}: {}", cause.message))
                } else {
                    LoginError::new(m)
                }
            }
        }
    }

    /// Runs directory work on its own thread.
    fn on_thread<T: Send + 'static>(
        &self,
        work: impl FnOnce(Options) -> Result<T, Failure> + Send + 'static,
    ) -> Result<T, LoginError> {
        let options = self.options.clone();
        let result = std::thread::spawn(move || work(options))
            .join()
            .unwrap_or_else(|_| Err(Failure::Login("LDAP login thread failed".into())));
        result.map_err(|f| self.error(f))
    }
}

impl LoginModule for LdapLoginModule {
    fn login(&mut self, callbacks: &Callbacks<'_>) -> Result<bool, LoginError> {
        if !self.authenticate_user {
            return Ok(false);
        }
        let Some(user) = callbacks.user else {
            return Ok(false);
        };
        let Some(password) = callbacks.password.filter(|p| !p.is_empty()) else {
            return Err(LoginError::new("Password cannot be null or empty"));
        };
        let (user, password) = (user.to_owned(), password.to_owned());
        let u = user.clone();
        let roles = self.on_thread(move |o| {
            let mut conn = open(&o)?;
            let (dn, mut roles) = resolve_dn(&mut conn, &o, &u)?;
            if !bind_user(&mut conn, &o, &dn, &password)? {
                return Err(Failure::Login(format!(
                    "Password does not match for user: {u}"
                )));
            }
            add_roles(&mut conn, &o, &dn, &u, &mut roles)?;
            let _ = conn.unbind();
            Ok(roles)
        })?;
        self.user = Some(user);
        self.roles = roles;
        self.authenticated = true;
        Ok(true)
    }

    fn commit(&mut self, subject: &mut Subject) -> Result<bool, LoginError> {
        let result = self.authenticated;
        if result && let Some(user) = &self.user {
            subject.add(Principal::User(user.clone()));
        }
        // Roles for the users other modules authenticated too.
        let others: Vec<String> = subject
            .users()
            .filter(|u| Some(*u) != self.user.as_deref() || !result)
            .map(str::to_owned)
            .collect();
        if !others.is_empty() {
            let roles = self.on_thread(move |o| {
                let mut conn = open(&o)?;
                let mut roles = Vec::new();
                for u in &others {
                    let (dn, mut found) = resolve_dn(&mut conn, &o, u)?;
                    add_roles(&mut conn, &o, &dn, u, &mut found)?;
                    roles.extend(found);
                }
                let _ = conn.unbind();
                Ok(roles)
            })?;
            self.roles.extend(roles);
        }
        for role in self.roles.drain(..) {
            subject.add(Principal::Role(role));
        }
        self.user = None;
        self.authenticated = false;
        Ok(result)
    }
}

/// `openContext`: connects and binds as the configured connection user.
fn open(o: &Options) -> Result<LdapConn, Failure> {
    let opening = |e: DirError| Failure::Dir("Error opening LDAP connection".into(), e);
    if o.get("authentication")
        .is_some_and(|a| a.eq_ignore_ascii_case("GSSAPI"))
    {
        return Err(opening(DirError::naming("GSSAPI is not supported")));
    }
    let url = o
        .set("connectionURL")
        .ok_or_else(|| opening(DirError::naming("connectionURL is not set")))?;
    // JNDI accepts several space-separated URLs and uses the first that
    // answers.
    let ssl = o
        .get("connectionProtocol")
        .is_some_and(|p| p.eq_ignore_ascii_case("ssl"));
    let mut last = None;
    for url in url.split_whitespace() {
        let url = if ssl && let Some(rest) = url.strip_prefix("ldap://") {
            format!("ldaps://{rest}")
        } else {
            url.to_owned()
        };
        let mut settings = LdapConnSettings::new();
        if let Some(t) = o.millis("connectionTimeout") {
            settings = settings.set_conn_timeout(t);
        }
        if url.starts_with("ldaps://") {
            settings = tls_settings(settings, o).map_err(|e| opening(DirError::naming(e)))?;
        }
        match LdapConn::with_settings(settings, &url) {
            Ok(conn) => {
                let mut conn = conn;
                bind_service(&mut conn, o).map_err(opening)?;
                return Ok(conn);
            }
            Err(e) => last = Some(DirError::from_ldap(e)),
        }
    }
    Err(opening(
        last.unwrap_or_else(|| DirError::naming("no LDAP URL")),
    ))
}

fn timeout<'a>(conn: &'a mut LdapConn, o: &Options) -> &'a mut LdapConn {
    match o.millis("readTimeout") {
        Some(t) => conn.with_timeout(t),
        None => conn,
    }
}

/// The connection user's bind, required as in `openContext`.
fn bind_service(conn: &mut LdapConn, o: &Options) -> Result<(), DirError> {
    let user = o
        .set("connectionUsername")
        .ok_or_else(|| DirError::naming("Empty username is not allowed"))?;
    let password = o
        .set("connectionPassword")
        .ok_or_else(|| DirError::naming("Empty password is not allowed"))?;
    let password = saranga_config::mask::resolve_mask(None, password, o.get("passwordCodec"))
        .map_err(|e| DirError::naming(format!("Failed to decode password: {e}")))?;
    timeout(conn, o)
        .simple_bind(user, &password)
        .and_then(|r| r.success())
        .map(|_| ())
        .map_err(DirError::from_ldap)
}

/// `LDAPLoginSSLSocketFactory`: the module's truststore, or the system's.
fn tls_settings(settings: LdapConnSettings, o: &Options) -> Result<LdapConnSettings, String> {
    if o.flag("trustAll") {
        return Ok(settings.set_no_tls_verify(true));
    }
    let Some(path) = o.set("truststorePath") else {
        return Ok(settings);
    };
    let store = crate::keystore::KeyStore::load(
        o.get("truststoreType").unwrap_or("JKS"),
        Some(path),
        o.get("truststorePassword"),
        None,
    )
    .map_err(|e| format!("truststore: {e}"))?;
    let mut roots = rustls::RootCertStore::empty();
    for c in store.trusted_certificates() {
        roots.add(c).map_err(|e| format!("truststore: {e}"))?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(settings.set_config(Arc::new(config)))
}

/// `resolveDN`: the user's DN, and the roles its `userRoleName` attribute
/// names.
fn resolve_dn(
    conn: &mut LdapConn,
    o: &Options,
    user: &str,
) -> Result<(String, Vec<String>), Failure> {
    let mut roles = Vec::new();
    let Some(matching) = o.set("userSearchMatching") else {
        return Ok((user.to_owned(), roles));
    };
    let filter = message_format(matching, &[&rfc2254(user)]);
    let scope = if o.flag("userSearchSubtree") {
        Scope::Subtree
    } else {
        Scope::OneLevel
    };
    let user_role_name = o.set("userRoleName");
    let attrs: Vec<&str> = match user_role_name {
        Some(a) => vec![a],
        None => vec!["1.1"],
    };
    let base = o.get("userBase").unwrap_or_default();
    let (entries, _) = timeout(conn, o)
        .search(base, scope, &filter, attrs)
        .and_then(|r| r.success())
        .map_err(|e| {
            Failure::Dir(
                "Error executing search query to resolve DN".into(),
                DirError::from_ldap(e),
            )
        })?;
    let Some(first) = entries.into_iter().next() else {
        return Err(Failure::Login(format!("User {user} not found in LDAP.")));
    };
    let entry = SearchEntry::construct(first);
    if let Some(attr) = user_role_name {
        let role_name = o.set("roleName");
        for value in attribute(&entry, attr) {
            match role_name {
                Some(rn) => roles.extend(
                    parse_dn(value)
                        .into_iter()
                        .filter(|(t, _)| t == rn)
                        .map(|(_, v)| v),
                ),
                None => roles.push(value.clone()),
            }
        }
    }
    Ok((entry.dn, roles))
}

/// `Attributes.get` ignores the attribute name's case.
fn attribute<'a>(entry: &'a SearchEntry, name: &str) -> &'a [String] {
    entry
        .attrs
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_slice())
        .unwrap_or_default()
}

/// `bindUser`: binds as the user to check the password, then as the
/// connection user again. A refused bind is a wrong password.
fn bind_user(conn: &mut LdapConn, o: &Options, dn: &str, password: &str) -> Result<bool, Failure> {
    let valid = match timeout(conn, o).simple_bind(dn, password) {
        Ok(r) => match r.success() {
            Ok(_) => true,
            Err(LdapError::LdapResult { result }) if result.rc == 49 => false,
            Err(e) => return Err(DirError::from_ldap(e).into()),
        },
        Err(e) => return Err(DirError::from_ldap(e).into()),
    };
    bind_service(conn, o)?;
    Ok(valid)
}

/// `addRoles`: the roles `roleSearchMatching` finds, with nested groups
/// when `expandRoles`.
fn add_roles(
    conn: &mut LdapConn,
    o: &Options,
    dn: &str,
    user: &str,
    roles: &mut Vec<String>,
) -> Result<(), Failure> {
    let Some(matching) = o.set("roleSearchMatching") else {
        return Ok(());
    };
    let filter = message_format(matching, &[&rfc2254(dn), &rfc2254(user)]);
    let scope = if o.flag("roleSearchSubtree") {
        Scope::Subtree
    } else {
        Scope::OneLevel
    };
    let base = o.get("roleBase").unwrap_or_default().to_owned();
    let role_name = o.set("roleName").map(str::to_owned);
    let expand = o.flag("expandRoles");
    let search = |conn: &mut LdapConn, filter: &str| -> Result<Vec<SearchEntry>, Failure> {
        let attrs = match &role_name {
            Some(r) => vec![r.clone()],
            None => vec!["1.1".to_owned()],
        };
        let (entries, _) = timeout(conn, o)
            .search(&base, scope, filter, attrs)
            .and_then(|r| r.success())
            .map_err(|e| {
                Failure::Dir(
                    "Error executing search query to resolve roles".into(),
                    DirError::from_ldap(e),
                )
            })?;
        Ok(entries.into_iter().map(SearchEntry::construct).collect())
    };
    let add = |entry: &SearchEntry, roles: &mut Vec<String>| match &role_name {
        Some(r) => {
            if let Some(v) = attribute(entry, r).first() {
                roles.push(v.clone());
            }
        }
        None => roles.push(entry.dn.clone()),
    };
    let mut seen = std::collections::HashSet::new();
    let mut pending = std::collections::VecDeque::new();
    for entry in search(conn, &filter)? {
        if expand {
            seen.insert(entry.dn.clone());
            pending.push_back(entry.dn.clone());
        }
        add(&entry, roles);
    }
    if expand {
        let expand_matching = o.get("expandRolesMatching").unwrap_or_default().to_owned();
        while let Some(name) = pending.pop_front() {
            let filter = message_format(&expand_matching, &[&name]);
            for entry in search(conn, &filter)? {
                if seen.insert(entry.dn.clone()) {
                    add(&entry, roles);
                    pending.push_back(entry.dn.clone());
                }
            }
        }
    }
    Ok(())
}

/// `doRFC2254Encoding`.
pub(crate) fn rfc2254(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\5c"),
            '*' => out.push_str("\\2a"),
            '(' => out.push_str("\\28"),
            ')' => out.push_str("\\29"),
            '\0' => out.push_str("\\00"),
            c => out.push(c),
        }
    }
    out
}

/// `java.text.MessageFormat.format` for `{n}` arguments: `'...'` quotes
/// text and `''` is a quote.
pub(crate) fn message_format(pattern: &str, args: &[&str]) -> String {
    let mut out = String::new();
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;
    let mut quoted = false;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' {
            if chars.get(i + 1) == Some(&'\'') {
                out.push('\'');
                i += 2;
            } else {
                quoted = !quoted;
                i += 1;
            }
        } else if c == '{' && !quoted {
            let end = chars[i..].iter().position(|&c| c == '}').map(|p| i + p);
            match end {
                Some(end) => {
                    let inner: String = chars[i + 1..end].iter().collect();
                    let index = inner.split(',').next().unwrap_or_default().trim();
                    match index.parse::<usize>().ok().and_then(|n| args.get(n)) {
                        Some(arg) => out.push_str(arg),
                        None => {
                            out.push('{');
                            out.push_str(&inner);
                            out.push('}');
                        }
                    }
                    i = end + 1;
                }
                None => {
                    out.extend(&chars[i..]);
                    break;
                }
            }
        } else {
            out.push(c);
            i += 1;
        }
    }
    out
}

/// The attribute types and values of a DN string, all RDNs, in order
/// (`LdapName`/`Rdn`), with escapes resolved.
pub(crate) fn parse_dn(dn: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut part = String::new();
    let mut parts = Vec::new();
    let mut escaped = false;
    for c in dn.chars() {
        if escaped {
            part.push('\\');
            part.push(c);
            escaped = false;
        } else if c == '\\' {
            escaped = true;
        } else if c == ',' || c == ';' || c == '+' {
            parts.push(std::mem::take(&mut part));
        } else {
            part.push(c);
        }
    }
    parts.push(part);
    for p in parts {
        if let Some((t, v)) = p.split_once('=') {
            out.push((t.trim().to_owned(), unescape(v.trim())));
        }
    }
    out
}

fn unescape(v: &str) -> String {
    let v = v
        .strip_prefix('"')
        .and_then(|x| x.strip_suffix('"'))
        .unwrap_or(v);
    let mut out = String::new();
    let mut chars = v.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(&n) = chars.peek() {
                chars.next();
                // `\XX` hex pairs are bytes; anything else is itself.
                if n.is_ascii_hexdigit()
                    && let Some(&m) = chars.peek()
                    && m.is_ascii_hexdigit()
                {
                    chars.next();
                    let b = u8::from_str_radix(&format!("{n}{m}"), 16).unwrap_or(b'?');
                    out.push(b as char);
                } else {
                    out.push(n);
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters() {
        assert_eq!(rfc2254("a*(b)\\"), "a\\2a\\28b\\29\\5c");
        assert_eq!(message_format("(uid={0})", &["jdoe"]), "(uid=jdoe)");
        assert_eq!(
            message_format("(&(member={0})(cn={1}))", &["uid=a,dc=x", "a"]),
            "(&(member=uid=a,dc=x)(cn=a))"
        );
        assert_eq!(message_format("'{0}' is ''{0}''", &["x"]), "{0} is 'x'");
    }

    #[test]
    fn dn_parts() {
        assert_eq!(
            parse_dn("cn=admins,ou=Groups,dc=example,dc=com"),
            vec![
                ("cn".into(), "admins".into()),
                ("ou".into(), "Groups".into()),
                ("dc".into(), "example".into()),
                ("dc".into(), "com".into())
            ]
        );
        assert_eq!(parse_dn("cn=a\\,b,dc=x")[0].1, "a,b");
    }
}
