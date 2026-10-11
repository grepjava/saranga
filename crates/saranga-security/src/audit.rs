//! The audit log, as Artemis's `AuditLogger` writes it: three loggers,
//! each off unless the logging configuration turns it on.
//!
//! - `org.apache.activemq.audit.base`: management calls, sessions,
//!   consumers, queues and addresses made by clients, security failures;
//! - `org.apache.activemq.audit.resource`: authentication, connections,
//!   and what management did to addresses and queues;
//! - `org.apache.activemq.audit.message`: messages sent, consumed and
//!   acknowledged.
//!
//! Each message names its caller as [`caller`] formats it.

use std::fmt::Display;

use crate::jaas::{Principal, Subject};

pub const BASE: &str = "org.apache.activemq.audit.base";
pub const RESOURCE: &str = "org.apache.activemq.audit.resource";
pub const MESSAGE: &str = "org.apache.activemq.audit.message";

/// `isBaseLoggingEnabled`.
pub fn base_enabled() -> bool {
    tracing::enabled!(target: "org.apache.activemq.audit.base", tracing::Level::INFO)
}

/// `isResourceLoggingEnabled`.
pub fn resource_enabled() -> bool {
    tracing::enabled!(target: "org.apache.activemq.audit.resource", tracing::Level::INFO)
}

/// `isMessageLoggingEnabled`.
pub fn message_enabled() -> bool {
    tracing::enabled!(target: "org.apache.activemq.audit.message", tracing::Level::INFO)
}

/// Logs to the base audit logger.
pub fn base(message: impl Display) {
    tracing::info!(target: "org.apache.activemq.audit.base", "{message}");
}

/// Logs to the resource audit logger.
pub fn resource(message: impl Display) {
    tracing::info!(target: "org.apache.activemq.audit.resource", "{message}");
}

/// Logs to the message audit logger.
pub fn message(message: impl Display) {
    tracing::info!(target: "org.apache.activemq.audit.message", "{message}");
}

/// `AuditLogger.getCaller(subject, remoteAddress)`: `user(role,...)@address`,
/// with the subject's last user principal (`anonymous` without one), its
/// roles sorted, and the remote address without its leading `/`
/// (`@unknown` when there is none).
pub fn caller(subject: Option<&Subject>, remote: Option<&str>) -> String {
    let mut user = "anonymous";
    let mut roles: Vec<&str> = Vec::new();
    for p in subject.map(Subject::principals).unwrap_or_default() {
        match p {
            Principal::User(u) => user = u,
            Principal::Role(r) => roles.push(r),
            Principal::Other { class, name } if class.ends_with("UserPrincipal") => user = name,
            Principal::Other { class, name } if class.ends_with("RolePrincipal") => {
                roles.push(name)
            }
            Principal::Other { .. } => {}
        }
    }
    roles.sort_unstable_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
    let mut s = user.to_owned();
    if !roles.is_empty() {
        s.push('(');
        s.push_str(&roles.join(","));
        s.push(')');
    }
    match remote {
        Some(r) => s.push_str(&format_remote(r)),
        None => s.push_str("@unknown"),
    }
    s
}

/// `formatRemoteAddress`: `@` and the address without its leading `/`.
pub fn format_remote(remote: &str) -> String {
    format!("@{}", remote.strip_prefix('/').unwrap_or(remote))
}

/// `MessageFormatter`: the template with each `{}` replaced by the next
/// argument, in order; extra `{}` stay as they are.
pub fn fill(template: &str, args: &[&str]) -> String {
    let mut out = String::with_capacity(template.len() + 32);
    let mut rest = template;
    let mut args = args.iter();
    while let Some(i) = rest.find("{}") {
        out.push_str(&rest[..i]);
        match args.next() {
            Some(a) => out.push_str(a),
            None => out.push_str("{}"),
        }
        rest = &rest[i + 2..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callers() {
        let mut s = Subject::default();
        s.add(Principal::User("admin".to_owned()));
        s.add(Principal::Role("b".to_owned()));
        s.add(Principal::Role("amq".to_owned()));
        assert_eq!(
            caller(Some(&s), Some("/127.0.0.1:5000")),
            "admin(amq,b)@127.0.0.1:5000"
        );
        assert_eq!(caller(None, Some("invm:0")), "anonymous@invm:0");
        assert_eq!(caller(None, None), "anonymous@unknown");
    }

    #[test]
    fn fills() {
        assert_eq!(fill("User {} is {}: {}", &["a", "b"]), "User a is b: {}");
        assert_eq!(
            fill("{} connection {} created", &["CORE", "1"]),
            "CORE connection 1 created"
        );
    }
}
