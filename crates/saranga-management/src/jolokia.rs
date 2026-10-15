//! The Jolokia protocol (agent 2.4.3, protocol 8.1, as Artemis 2.57.0's web
//! console serves it at `/console/jolokia`) over Saranga's management:
//! `read`, `write`, `exec`, `search`, `list` and `version`, by GET path or
//! POSTed JSON, with the request echoes, values and errors the Java agent
//! gives.
//!
//! Errors are written as Jolokia writes them: `error_type` is the Java
//! exception's class and `error` is `<class> : <message>`, with the
//! response's own `status`; the HTTP status stays 200.

use std::sync::Arc;

use saranga_server::Current;
use saranga_server::management::json::Json;

use crate::mbean::{Bean, Control, ObjectName, Op, registered};
use crate::rbac::Guard;

/// Who is asking, and what they may do.
pub struct Caller {
    pub roles: Vec<String>,
    /// The subject the login gave, for the audit log.
    pub subject: Option<Arc<saranga_security::jaas::Subject>>,
    /// The client's `host:port`, as Artemis's `JolokiaFilter` gives the
    /// audit log the request's remote host and port.
    pub remote: Option<String>,
}

impl Caller {
    /// `getMBeanInfo` on a bean's control, as Artemis's guard calls it
    /// before each attribute and Jolokia before each operation.
    fn mbean_info(&self, bean: &Bean) {
        let (class, key) = match bean {
            Bean::Broker => ("ActiveMQServerControlImpl", ""),
            Bean::Address(n) => ("AddressControlImpl", n.as_str()),
            Bean::Queue(n) => ("QueueControlImpl", n.as_str()),
            Bean::Divert(n) => ("DivertControlImpl", n.as_str()),
            Bean::Acceptor(n, _) => ("AcceptorControlImpl", n.as_str()),
            Bean::BroadcastGroup(n) => ("BroadcastGroupControlImpl", n.as_str()),
            Bean::Bridge(n) => ("BridgeControlImpl", n.as_str()),
            Bean::BrokerConnection(n) => ("BrokerConnectionControlImpl", n.as_str()),
        };
        saranga_server::audit::mbean_info(
            class,
            key,
            saranga_server::audit::Who {
                subject: self.subject.as_deref(),
                remote: self.remote.as_deref(),
            },
        );
    }

    /// For the audit log.
    fn management(&self) -> saranga_server::management::Caller<'_> {
        saranga_server::management::Caller {
            security: None,
            who: Some(saranga_server::audit::Who {
                subject: self.subject.as_deref(),
                remote: self.remote.as_deref(),
            }),
        }
    }
}

/// The agent: the broker's MBeans and their guard.
pub struct Agent {
    pub broker: Arc<Current>,
    pub domain: String,
    pub broker_name: String,
    pub access: Guard,
    pub agent_id: String,
    /// The acceptors, by name, with their parameters.
    pub acceptors: Vec<(String, Vec<(String, String)>)>,
}

/// A Jolokia error.
#[derive(Debug)]
pub struct Failure {
    pub error_type: &'static str,
    pub message: String,
    pub status: u16,
}

impl Failure {
    fn new(error_type: &'static str, message: impl Into<String>, status: u16) -> Self {
        Self {
            error_type,
            message: message.into(),
            status,
        }
    }

    fn illegal(message: impl Into<String>) -> Self {
        Self::new("java.lang.IllegalArgumentException", message, 400)
    }

    fn json(&self) -> Json {
        Json::object()
            .with("error_type", self.error_type)
            .with("error", format!("{} : {}", self.error_type, self.message))
            .with("status", i64::from(self.status))
    }
}

/// Which attributes a read or write names.
#[derive(Debug, Clone)]
enum Attrs {
    All,
    One(String),
    Many(Vec<String>),
}

/// A parsed request.
#[derive(Debug, Clone)]
pub struct Request {
    kind: &'static str,
    mbean: Option<ObjectName>,
    attrs: Attrs,
    path: Option<String>,
    value: Option<Json>,
    operation: Option<String>,
    arguments: Vec<Json>,
}

impl Request {
    /// The request as the response echoes it, keys in the agent's order.
    fn echo(&self) -> Json {
        let mut e = Json::object();
        if let Some(p) = &self.path {
            e.put("path", p.clone());
        }
        if self.kind == "list" {
            e.put("type", "list");
            return e;
        }
        if let Some(m) = &self.mbean {
            e.put("mbean", m.canonical());
        }
        match self.kind {
            "read" | "write" => match &self.attrs {
                Attrs::All => {}
                Attrs::One(a) => e.put("attribute", a.clone()),
                Attrs::Many(v) => e.put(
                    "attribute",
                    Json::Array(v.iter().map(|a| Json::from(a.clone())).collect()),
                ),
            },
            // As JolokiaExecRequest writes it: no arguments, no member.
            "exec" if !self.arguments.is_empty() => {
                e.put("arguments", Json::Array(self.arguments.clone()))
            }
            _ => {}
        }
        e.put("type", self.kind);
        if self.kind == "write" {
            e.put("value", self.value.clone().unwrap_or(Json::Null));
        }
        if let Some(op) = &self.operation {
            e.put("operation", op.clone());
        }
        e
    }
}

/// A response to one request: the request, then the value or the error.
fn respond(request: Option<&Request>, outcome: Result<Json, Failure>) -> Json {
    let mut r = Json::object();
    if let Some(req) = request {
        r.put("request", req.echo());
    }
    match outcome {
        Ok(v) => {
            r.put("value", v);
            r.put("status", 200i64);
        }
        Err(f) => {
            if let Json::Object(members) = f.json() {
                for (k, v) in members {
                    r.put(&k, v);
                }
            }
        }
    }
    r
}

/// An HTTP answer: JSON (always status 200), or a plain-text error.
pub enum Answer {
    Json(Json),
    BadRequest(String),
}

const TYPES: [&str; 6] = ["read", "write", "exec", "list", "search", "version"];

/// Splits a GET path on unescaped `/`: `!/` is a slash, `!!` an `!`.
fn split_path(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = path.chars();
    while let Some(c) = chars.next() {
        match c {
            '!' => match chars.next() {
                Some(n) => cur.push(n),
                None => cur.push('!'),
            },
            '/' => out.push(std::mem::take(&mut cur)),
            c => cur.push(c),
        }
    }
    out.push(cur);
    out
}

/// A GET argument: `[null]` is null, `""` the empty string.
fn get_arg(s: &str) -> Json {
    match s {
        "[null]" => Json::Null,
        "\"\"" => Json::from(""),
        s => Json::from(s),
    }
}

fn parse_name(s: &str) -> Result<ObjectName, Failure> {
    ObjectName::parse(s).map_err(|e| Failure::illegal(format!("Invalid object name. {e}")))
}

fn attrs_of(s: &str) -> Attrs {
    if s.contains(',') {
        Attrs::Many(s.split(',').map(str::to_owned).collect())
    } else {
        Attrs::One(s.to_owned())
    }
}

/// The number of parameters an operation names with its signature.
fn signature_arity(op: &str) -> Option<usize> {
    let inner = op.split_once('(')?.1.strip_suffix(')')?;
    Some(if inner.trim().is_empty() {
        0
    } else {
        inner.split(',').count()
    })
}

impl Agent {
    /// A GET request: `<type>/<arguments>` after the agent's path.
    pub async fn get(&self, caller: &Caller, path_info: &str, url: &str) -> Answer {
        let trimmed = path_info.trim_start_matches('/');
        if trimmed.is_empty() {
            let req = Request {
                kind: "version",
                mbean: None,
                attrs: Attrs::All,
                path: None,
                value: None,
                operation: None,
                arguments: Vec::new(),
            };
            return Answer::Json(respond(Some(&req), Ok(self.version(url))));
        }
        let mut parts = split_path(trimmed);
        while parts.last().is_some_and(String::is_empty) && parts.len() > 1 {
            parts.pop();
        }
        let kind = parts.remove(0);
        let Some(&kind) = TYPES.iter().find(|t| **t == kind) else {
            return Answer::BadRequest(format!("No type with name '{kind}' exists"));
        };
        let invalid = || {
            Answer::Json(respond(
                None,
                Err(Failure::illegal(format!(
                    "Invalid arguments in pathinfo \"{path_info}\" for command {}",
                    kind.to_uppercase()
                ))),
            ))
        };
        let rest_path = |from: usize, parts: &[String]| -> Option<String> {
            (parts.len() > from).then(|| {
                parts[from..]
                    .iter()
                    .map(|p| p.replace('!', "!!").replace('/', "!/"))
                    .collect::<Vec<_>>()
                    .join("/")
            })
        };
        let mut req = Request {
            kind,
            mbean: None,
            attrs: Attrs::All,
            path: None,
            value: None,
            operation: None,
            arguments: Vec::new(),
        };
        match kind {
            "version" => {}
            "list" => {
                let segs: Vec<&String> = parts.iter().filter(|p| !p.is_empty()).collect();
                if !segs.is_empty() {
                    req.path = Some(
                        segs.iter()
                            .map(|p| p.replace('!', "!!").replace('/', "!/"))
                            .collect::<Vec<_>>()
                            .join("/"),
                    );
                }
            }
            "search" => {
                let Some(p) = parts.first().filter(|p| !p.is_empty()) else {
                    return invalid();
                };
                match parse_name(p) {
                    Ok(n) => req.mbean = Some(n),
                    Err(f) => return Answer::Json(respond(None, Err(f))),
                }
            }
            "read" | "write" | "exec" => {
                let Some(m) = parts.first().filter(|p| !p.is_empty()) else {
                    return invalid();
                };
                match parse_name(m) {
                    Ok(n) => req.mbean = Some(n),
                    Err(f) => return Answer::Json(respond(None, Err(f))),
                }
                match kind {
                    "read" => {
                        if let Some(a) = parts.get(1).filter(|a| !a.is_empty()) {
                            req.attrs = attrs_of(a);
                        }
                        req.path = rest_path(2, &parts);
                    }
                    "write" => {
                        let (Some(a), Some(v)) = (parts.get(1), parts.get(2)) else {
                            return invalid();
                        };
                        req.attrs = Attrs::One(a.clone());
                        req.value = Some(get_arg(v));
                        req.path = rest_path(3, &parts);
                    }
                    _ => {
                        let Some(op) = parts.get(1).filter(|o| !o.is_empty()) else {
                            return invalid();
                        };
                        let args = &parts[2..];
                        let n = signature_arity(op).unwrap_or(args.len()).min(args.len());
                        req.operation = Some(op.clone());
                        req.arguments = args[..n].iter().map(|a| get_arg(a)).collect();
                        req.path = rest_path(2 + n, &parts);
                    }
                }
            }
            _ => unreachable!(),
        }
        let outcome = self.execute(caller, &req, url).await;
        Answer::Json(respond(Some(&req), outcome))
    }

    /// A POST request: one JSON object, or an array of them answered in
    /// order.
    pub async fn post(&self, caller: &Caller, body: &str, url: &str) -> Answer {
        let Ok(json) = Json::parse(body) else {
            return Answer::BadRequest("Invalid JSON request".to_owned());
        };
        match json {
            Json::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    out.push(self.post_one(caller, &item, url).await);
                }
                Answer::Json(Json::Array(out))
            }
            Json::Object(_) => Answer::Json(self.post_one(caller, &json, url).await),
            _ => Answer::BadRequest("Invalid JSON request".to_owned()),
        }
    }

    async fn post_one(&self, caller: &Caller, item: &Json, url: &str) -> Json {
        let text = |k: &str| item.get(k).and_then(Json::as_str).map(str::to_owned);
        let Some(kind) = text("type").and_then(|t| TYPES.iter().find(|x| **x == t).copied()) else {
            return respond(
                None,
                Err(Failure::illegal(format!(
                    "No type with name '{}' exists",
                    text("type").unwrap_or_default()
                ))),
            );
        };
        let mbean = match text("mbean") {
            Some(m) => match parse_name(&m) {
                Ok(n) => Some(n),
                Err(f) => return respond(None, Err(f)),
            },
            // A read of nothing in particular reads everything.
            None if kind == "read" => ObjectName::parse("*:*").ok(),
            None => None,
        };
        let attrs = match item.get("attribute") {
            Some(Json::String(a)) => Attrs::One(a.clone()),
            Some(Json::Array(v)) => Attrs::Many(
                v.iter()
                    .filter_map(|a| a.as_str().map(str::to_owned))
                    .collect(),
            ),
            _ => Attrs::All,
        };
        let req = Request {
            kind,
            mbean,
            attrs,
            path: text("path"),
            value: item.get("value").cloned(),
            operation: text("operation"),
            arguments: match item.get("arguments") {
                Some(Json::Array(a)) => a.clone(),
                Some(Json::Null) | None => Vec::new(),
                Some(other) => vec![other.clone()],
            },
        };
        let outcome = self.execute(caller, &req, url).await;
        respond(Some(&req), outcome)
    }

    async fn execute(&self, caller: &Caller, req: &Request, url: &str) -> Result<Json, Failure> {
        match req.kind {
            "version" => Ok(self.version(url)),
            "search" => self.search(caller, req.mbean.as_ref()),
            "list" => self.list(caller, req.path.as_deref()),
            "read" => {
                let v = self.read(caller, req).await?;
                navigate(v, req.path.as_deref())
            }
            "write" => self.write(caller, req).await,
            "exec" => {
                let v = self.exec(caller, req).await?;
                navigate(v, req.path.as_deref())
            }
            _ => Err(Failure::illegal("unknown type")),
        }
    }

    /// The `version` value, as the Artemis agent gives it.
    pub fn version(&self, url: &str) -> Json {
        let id = self.agent_id.clone();
        Json::object()
            .with("agent", "2.4.3")
            .with("protocol", "8.1")
            .with(
                "decorators",
                Json::object().with(
                    "artemis",
                    Json::object().with("annotations", Json::Array(vec![Json::from("rbac")])),
                ),
            )
            .with(
                "details",
                Json::object()
                    .with("agent_version", "2.4.3")
                    .with("agent_id", id.clone())
                    .with("server_product", "Artemis")
                    .with("server_vendor", "Apache")
                    .with("server_version", "2.57.0")
                    .with("secured", false)
                    .with("url", url),
            )
            .with("id", id.clone())
            .with(
                "config",
                Json::object()
                    .with("agentId", id)
                    .with("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX")
                    .with("policyLocation", "classpath:jolokia-access.xml")
                    .with("allowErrorDetails", "false")
                    .with("includeStackTrace", "false")
                    .with("mbeanQualifier", "qualifier=hawtio"),
            )
            .with(
                "info",
                Json::object()
                    .with("proxy", Json::object())
                    .with("jmx", Json::object()),
            )
    }

    fn beans(&self) -> Vec<(ObjectName, Bean)> {
        registered(
            &self.broker.get(),
            &self.domain,
            &self.broker_name,
            &self.acceptors,
        )
    }

    fn search(&self, caller: &Caller, pattern: Option<&ObjectName>) -> Result<Json, Failure> {
        let names: Vec<Json> = self
            .query(caller)?
            .into_iter()
            .filter(|(n, _)| pattern.is_none_or(|p| p.matches(n)))
            .map(|(n, _)| Json::from(n.canonical()))
            .collect();
        Ok(Json::Array(names))
    }

    /// `queryNames`: the MBeans, those the RBAC guard lets the caller view.
    fn query(&self, caller: &Caller) -> Result<Vec<(ObjectName, Bean)>, Failure> {
        let mut beans = self.beans();
        if let Guard::Rbac(r) = &self.access {
            let broker = self.broker.get();
            r.check_query(&broker, caller.subject.clone())
                .map_err(Self::refused)?;
            beans.retain(|(n, _)| r.visible(&broker, caller.subject.clone(), n));
        }
        Ok(beans)
    }

    /// A call the RBAC guard refuses: the security store's message.
    fn refused(message: String) -> Failure {
        Failure::new("java.lang.Exception", message, 403)
    }

    /// The guard's check of a method of an MBean (`get<Attribute>` for a
    /// read); `denied` is how management.xml's guard refuses it.
    fn check(
        &self,
        caller: &Caller,
        name: &ObjectName,
        method: &str,
        denied: impl FnOnce() -> Failure,
    ) -> Result<(), Failure> {
        match &self.access {
            Guard::Acl(l) if l.can_invoke(&caller.roles, name, method) => Ok(()),
            Guard::Acl(_) => Err(denied()),
            Guard::Rbac(r) => r
                .check(&self.broker.get(), caller.subject.clone(), name, method)
                .map_err(Self::refused),
        }
    }

    /// `canInvoke`, for the MBean info.
    fn can_invoke(&self, caller: &Caller, name: &ObjectName, method: &str) -> bool {
        match &self.access {
            Guard::Acl(l) => l.can_invoke(&caller.roles, name, method),
            Guard::Rbac(r) => {
                r.can_invoke(&self.broker.get(), caller.subject.clone(), name, method)
            }
        }
    }

    /// The one MBean a non-pattern name names; not found, it is named as
    /// the request gave it (`ObjectName.toString`).
    fn bean(&self, name: &ObjectName) -> Result<(ObjectName, Bean), Failure> {
        self.beans()
            .into_iter()
            .find(|(n, _)| name.matches(n))
            .ok_or_else(|| {
                Failure::new(
                    "javax.management.InstanceNotFoundException",
                    name.serialized(),
                    404,
                )
            })
    }

    fn denied(what: &str, name: &str) -> Failure {
        Failure::new(
            "java.lang.SecurityException",
            format!("User not authorized to access {what}: {name}"),
            403,
        )
    }

    /// One attribute of one MBean, with the guard's check of its getter.
    async fn attribute(
        &self,
        caller: &Caller,
        name: &ObjectName,
        bean: &Bean,
        attr: &str,
    ) -> Result<Json, Failure> {
        caller.mbean_info(bean);
        let control = bean.control();
        let Some(info) = control.attr(attr).filter(|a| a.readable) else {
            return Err(Failure::new(
                "javax.management.AttributeNotFoundException",
                format!("No such attribute: {attr}"),
                404,
            ));
        };
        let getter = format!("{}{attr}", if info.is { "is" } else { "get" });
        self.check(caller, name, &getter, || Self::denied("attribute", attr))?;
        if let Bean::Acceptor(n, params) = bean {
            let started = self.broker.get().acceptor(n).is_some_and(|(_, s)| s);
            return Ok(acceptor_attribute(n, params, started, attr));
        }
        self.broker
            .get()
            .management()
            .attribute(
                &self.broker.get(),
                &bean.resource(),
                attr,
                caller.management(),
            )
            .await
            .map_err(|e| Failure::new("java.lang.UnsupportedOperationException", e, 500))
    }

    async fn read(&self, caller: &Caller, req: &Request) -> Result<Json, Failure> {
        let Some(name) = &req.mbean else {
            return Err(Failure::illegal("No MBean name given"));
        };
        if name.is_pattern() {
            // Every matching MBean that has the attributes.
            let mut out = Json::object();
            for (n, bean) in self
                .query(caller)?
                .into_iter()
                .filter(|(n, _)| name.matches(n))
            {
                if let Ok(v) = self.read_bean(caller, &n, &bean, &req.attrs, true).await {
                    out.put(&n.canonical(), v);
                }
            }
            if matches!(&out, Json::Object(m) if m.is_empty()) {
                return Err(Failure::new(
                    "javax.management.InstanceNotFoundException",
                    format!("No matching MBean found for {}", name.serialized()),
                    404,
                ));
            }
            return Ok(out);
        }
        let (n, bean) = self.bean(name)?;
        match &req.attrs {
            Attrs::One(a) => self.attribute(caller, &n, &bean, a).await,
            attrs => self.read_bean(caller, &n, &bean, attrs, false).await,
        }
    }

    /// The attributes of one MBean as a map; for a pattern read, a missing
    /// attribute leaves the MBean out.
    async fn read_bean(
        &self,
        caller: &Caller,
        name: &ObjectName,
        bean: &Bean,
        attrs: &Attrs,
        pattern: bool,
    ) -> Result<Json, Failure> {
        let control = bean.control();
        if matches!(attrs, Attrs::All) {
            // Jolokia lists the attributes from the MBean info.
            caller.mbean_info(bean);
        }
        let names: Vec<String> = match attrs {
            Attrs::All => control
                .attrs
                .iter()
                .filter(|a| a.readable)
                .map(|a| a.name.to_owned())
                .collect(),
            Attrs::One(a) => vec![a.clone()],
            Attrs::Many(v) => v.clone(),
        };
        let mut out = Json::object();
        for a in names {
            match self.attribute(caller, name, bean, &a).await {
                Ok(v) => out.put(&a, v),
                // `getAttributes` leaves out what it cannot read.
                Err(f) if matches!(attrs, Attrs::All) && f.status == 500 => {}
                Err(f) if pattern && f.status == 404 => return Err(f),
                Err(f) => return Err(f),
            }
        }
        Ok(out)
    }

    async fn write(&self, caller: &Caller, req: &Request) -> Result<Json, Failure> {
        let Some(name) = &req.mbean else {
            return Err(Failure::illegal("No MBean name given"));
        };
        let (n, bean) = self.bean(name)?;
        let Attrs::One(attr) = &req.attrs else {
            return Err(Failure::illegal("No attribute given"));
        };
        let Some(info) = bean.control().attr(attr).filter(|a| a.writable) else {
            return Err(Failure::new(
                "javax.management.AttributeNotFoundException",
                format!("No such attribute: {attr}"),
                404,
            ));
        };
        let setter = format!("set{attr}");
        self.check(caller, &n, &setter, || Self::denied("attribute", attr))?;
        let old = if info.readable {
            self.attribute(caller, &n, &bean, attr)
                .await
                .unwrap_or(Json::Null)
        } else {
            Json::Null
        };
        let value = convert(req.value.as_ref().unwrap_or(&Json::Null), info.type_name)?;
        self.broker
            .get()
            .management()
            .invoke(
                &self.broker.get(),
                &bean.resource(),
                &setter,
                &[value],
                caller.management(),
            )
            .await
            .map_err(|e| Failure::new(error_type(&setter, &e), e, 500))?;
        Ok(old)
    }

    /// The operation a request names: by signature, or the only one of
    /// its name.
    fn resolve(
        &self,
        name: &ObjectName,
        control: &Control,
        op: &str,
    ) -> Result<&'static Op, Failure> {
        let base = op.split('(').next().unwrap_or(op);
        let candidates = control.ops_named(base);
        if candidates.is_empty() {
            return Err(Failure::illegal(format!(
                "No operation {op} found on MBean {}",
                name.canonical()
            )));
        }
        let signatures = || {
            candidates
                .iter()
                .map(|o| o.signature())
                .collect::<Vec<_>>()
                .join(",")
        };
        if let Some((_, sig)) = op.split_once('(') {
            let wanted = format!("({sig}");
            let wanted: String = wanted.chars().filter(|c| !c.is_whitespace()).collect();
            return candidates
                .iter()
                .find(|o| o.signature() == wanted)
                .copied()
                .ok_or_else(|| {
                    Failure::illegal(format!(
                        "No operation {op} on MBean {} exists. Known signatures: {}",
                        name.canonical(),
                        signatures()
                    ))
                });
        }
        if candidates.len() > 1 {
            return Err(Failure::illegal(format!(
                "Operation {op} on MBean {} is overloaded. Signatures found: {}. Use a signature when specifying the operation.",
                name.canonical(),
                signatures()
            )));
        }
        Ok(candidates[0])
    }

    async fn exec(&self, caller: &Caller, req: &Request) -> Result<Json, Failure> {
        let Some(name) = &req.mbean else {
            return Err(Failure::illegal("No MBean name given"));
        };
        let (n, bean) = self.bean(name)?;
        caller.mbean_info(&bean);
        let op_name = req.operation.as_deref().unwrap_or("");
        let op = self.resolve(&n, bean.control(), op_name)?;
        if req.arguments.len() != op.params.len() {
            return Err(Failure::illegal(format!(
                "Invalid number of operation arguments. Operation {op_name} on {} requires {} parameters, not {} as given",
                n.canonical(),
                op.params.len(),
                req.arguments.len()
            )));
        }
        let mut params = Vec::with_capacity(op.params.len());
        for (arg, p) in req.arguments.iter().zip(op.params) {
            params.push(convert(arg, p.type_name)?);
        }
        self.check(caller, &n, op.name, || {
            Failure::new(
                "java.lang.SecurityException",
                format!("User not authorized to access operation: {}", op.name),
                403,
            )
        })?;
        let broker = self.broker.get();
        let management = broker.management();
        let result = match (&bean, op.name) {
            // Composite data, which management messages cannot carry.
            (Bean::Queue(q), "browse") => {
                let s = |j: &Json| match j {
                    Json::String(s) => Some(s.clone()),
                    _ => None,
                };
                let i = |j: &Json| match j {
                    Json::Long(n) => *n as i32,
                    _ => 0,
                };
                let (page, filter) = match params.as_slice() {
                    [] => (None, None),
                    [f] => (None, s(f)),
                    [p, size] => (Some((i(p), i(size))), None),
                    [p, size, f, ..] => (Some((i(p), i(size))), s(f)),
                };
                management
                    .browse(&broker, q, page, filter, caller.management())
                    .await
            }
            _ => {
                management
                    .invoke(
                        &broker,
                        &bean.resource(),
                        op.name,
                        &params,
                        caller.management(),
                    )
                    .await
            }
        }
        .map_err(|e| Failure::new(error_type(op.name, &e), e, 500))?;
        Ok(if op.ret == "void" { Json::Null } else { result })
    }

    /// `list`: the MBeans' metadata, as the agent with the Artemis RBAC
    /// decorator gives it, under `domain/properties/part`.
    fn list(&self, caller: &Caller, path: Option<&str>) -> Result<Json, Failure> {
        let segments: Vec<String> = path.map(split_path).unwrap_or_default();
        let segments: Vec<String> = segments.into_iter().filter(|s| !s.is_empty()).collect();
        if segments.len() > 3 {
            return Err(Failure::illegal(format!(
                "List operation supports only 3 path segments for MBean domain,canonical list of ObjectName properties and updater to use. Remaining path: {}",
                segments[3..].join("/")
            )));
        }
        let mut domains = Json::object();
        for (n, bean) in self.query(caller)? {
            let info = self.bean_info(caller, &n, &bean);
            let props = n.canonical_props();
            let mut d = domains.get(&n.domain).cloned().unwrap_or_else(Json::object);
            d.put(&props, info);
            domains.put(&n.domain, d);
        }
        let not_found = || {
            Failure::illegal(format!(
                "No MBean '{}' found",
                segments
                    .iter()
                    .take(2)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(":")
            ))
        };
        let mut value = domains;
        if let Some(domain) = segments.first() {
            value = value.get(domain).cloned().ok_or_else(not_found)?;
        }
        if let Some(props) = segments.get(1) {
            // The properties may be given in any order.
            let key = ObjectName::parse(&format!("x:{props}"))
                .map(|n| n.canonical_props())
                .unwrap_or_else(|_| props.clone());
            value = value.get(&key).cloned().ok_or_else(not_found)?;
        }
        if let Some(part) = segments.get(2) {
            value = value.get(part).cloned().ok_or_else(not_found)?;
        }
        Ok(value)
    }

    fn bean_info(&self, caller: &Caller, name: &ObjectName, bean: &Bean) -> Json {
        let control = bean.control();
        let can = |m: &str| self.can_invoke(caller, name, m);
        let op_json = |o: &Op| {
            Json::object()
                .with(
                    "args",
                    Json::Array(
                        o.params
                            .iter()
                            .map(|p| {
                                Json::object()
                                    .with("name", p.name)
                                    .with("type", p.type_name)
                                    .with("desc", p.desc)
                            })
                            .collect(),
                    ),
                )
                .with("ret", o.ret)
                .with("canInvoke", can(o.name))
                .with("desc", o.desc)
        };
        let mut ops = Json::object();
        let mut by_string = Json::object();
        for o in control.ops {
            if ops.get(o.name).is_some() {
                continue;
            }
            let same = control.ops_named(o.name);
            if same.len() == 1 {
                ops.put(o.name, op_json(o));
            } else {
                ops.put(
                    o.name,
                    Json::Array(same.iter().map(|o| op_json(o)).collect()),
                );
            }
            for s in same {
                by_string.put(
                    &format!("{}{}", s.name, s.signature()),
                    Json::object().with("canInvoke", can(s.name)),
                );
            }
        }
        let mut attrs = Json::object();
        for a in control.attrs {
            let getter = format!("{}{}", if a.is { "is" } else { "get" }, a.name);
            let mut j = Json::object()
                .with("r", a.readable)
                .with("rw", a.writable)
                .with("canRead", can(&getter))
                .with("w", a.writable)
                .with("is", a.is);
            if a.writable {
                j.put("canWrite", can(&format!("set{}", a.name)));
            }
            j.put("type", a.type_name);
            j.put("desc", a.desc);
            attrs.put(a.name, j);
        }
        Json::object()
            .with("op", ops)
            .with("canInvoke", true)
            .with("opByString", by_string)
            .with("attr", attrs)
            .with("class", bean.class_name())
            .with(
                "desc",
                "Information on the management interface of the MBean",
            )
    }
}

/// `AcceptorControl`'s attributes.
fn acceptor_attribute(name: &str, params: &[(String, String)], started: bool, attr: &str) -> Json {
    match attr {
        "Name" => Json::from(name),
        "FactoryClassName" => Json::from(saranga_server::acceptors::FACTORY),
        "Parameters" => {
            let mut m = Json::object();
            for (k, v) in params {
                m.put(k, v.clone());
            }
            m
        }
        "Started" => Json::from(started),
        _ => Json::Null,
    }
}

/// A value converted to a Java parameter type, as Jolokia's
/// `StringToObjectConverter` and its JSON conversions do.
fn convert(v: &Json, type_name: &str) -> Result<Json, Failure> {
    let number = |s: &str| -> Result<Json, Failure> {
        s.trim().parse::<i64>().map(Json::from).map_err(|_| {
            Failure::new(
                "java.lang.NumberFormatException",
                format!("For input string: \"{s}\""),
                400,
            )
        })
    };
    let primitive = matches!(
        type_name,
        "int" | "long" | "short" | "byte" | "boolean" | "double" | "float"
    );
    match (type_name, v) {
        (_, Json::Null) if primitive => Err(Failure::illegal(format!(
            "Cannot convert null to primitive type {type_name}"
        ))),
        (_, Json::Null) => Ok(Json::Null),
        ("java.lang.String", Json::String(_)) => Ok(v.clone()),
        ("java.lang.String", other) => Ok(Json::from(match other {
            Json::String(s) => s.clone(),
            o => o.to_string(),
        })),
        (
            "int" | "long" | "short" | "byte" | "java.lang.Integer" | "java.lang.Long"
            | "java.lang.Short" | "java.lang.Byte",
            Json::String(s),
        ) => number(s),
        (
            "int" | "long" | "short" | "byte" | "java.lang.Integer" | "java.lang.Long",
            Json::Double(d),
        ) => Ok(Json::from(*d as i64)),
        ("boolean" | "java.lang.Boolean", Json::String(s)) => {
            Ok(Json::from(s.eq_ignore_ascii_case("true")))
        }
        ("double" | "float" | "java.lang.Double" | "java.lang.Float", Json::String(s)) => {
            s.trim().parse::<f64>().map(Json::from).map_err(|_| {
                Failure::new(
                    "java.lang.NumberFormatException",
                    format!("For input string: \"{s}\""),
                    400,
                )
            })
        }
        ("double" | "float" | "java.lang.Double" | "java.lang.Float", Json::Long(n)) => {
            Ok(Json::from(*n as f64))
        }
        (t, Json::String(s)) if t.starts_with('[') => {
            // A JSON array, or comma-separated values.
            if let Ok(Json::Array(a)) = Json::parse(s) {
                return Ok(Json::Array(a));
            }
            Ok(Json::Array(
                s.split(',').map(|x| Json::from(x.to_owned())).collect(),
            ))
        }
        ("java.util.Map", Json::String(s)) => Json::parse(s)
            .map_err(|e| Failure::illegal(format!("Cannot convert {s} to a Map: {e}"))),
        _ => Ok(v.clone()),
    }
}

/// A value's part a path names: map keys and list indexes, `!`-escaped.
fn navigate(v: Json, path: Option<&str>) -> Result<Json, Failure> {
    let Some(path) = path else {
        return Ok(v);
    };
    let mut cur = v;
    for seg in split_path(path).into_iter().filter(|s| !s.is_empty()) {
        cur = match &cur {
            Json::Object(_) => cur.get(&seg).cloned().unwrap_or(Json::Null),
            Json::Array(items) => {
                let i: usize = seg
                    .parse()
                    .map_err(|_| Failure::illegal(format!("Index '{seg}' is not a number")))?;
                items
                    .get(i)
                    .cloned()
                    .ok_or_else(|| Failure::illegal(format!("Index {i} out of range")))?
            }
            _ => {
                return Err(Failure::illegal(format!(
                    "Cannot navigate further via the path {path}"
                )));
            }
        };
    }
    Ok(cur)
}

/// The class of the exception an operation's error was thrown as, which
/// Jolokia reports as `error_type`: the class Artemis's message bundle
/// declares for the error's AMQ code. The `ActiveMQServerControl` methods
/// that catch an `ActiveMQException` and throw an `IllegalStateException`
/// with its message report that.
fn error_type(operation: &str, message: &str) -> &'static str {
    const RETHROWN: &[&str] = &[
        "deleteAddress",
        "createQueue",
        "addAddressSettings",
        "createBridge",
    ];
    let class = message
        .strip_prefix("AMQ")
        .and_then(|m| m.get(..6))
        .and_then(|c| c.parse::<u32>().ok())
        .and_then(|code| {
            crate::exceptions::EXCEPTIONS
                .binary_search_by_key(&code, |(c, _)| *c)
                .ok()
                .map(|i| crate::exceptions::EXCEPTIONS[i].1)
        });
    match class {
        Some(c)
            if c.starts_with("org.apache.activemq.artemis.api.core.ActiveMQ")
                && RETHROWN.contains(&operation) =>
        {
            "java.lang.IllegalStateException"
        }
        Some(c) => c,
        None => "java.lang.Exception",
    }
}

#[cfg(test)]
mod tests {
    use super::{error_type, signature_arity, split_path};

    #[test]
    fn paths() {
        assert_eq!(split_path("a!/b/c!!"), vec!["a/b", "c!"]);
        assert_eq!(signature_arity("op(java.lang.String,int)"), Some(2));
        assert_eq!(signature_arity("op()"), Some(0));
        assert_eq!(signature_arity("op"), None);
    }

    #[test]
    fn errors_have_their_java_classes() {
        assert_eq!(
            error_type("destroyQueue", "AMQ229017: Queue q does not exist"),
            "org.apache.activemq.artemis.api.core.ActiveMQNonExistentQueueException"
        );
        // deleteAddress rethrows as IllegalStateException.
        assert_eq!(
            error_type("deleteAddress", "AMQ229203: Address Does Not Exist: a"),
            "java.lang.IllegalStateException"
        );
        assert_eq!(
            error_type("removeMessages", "no code"),
            "java.lang.Exception"
        );
    }
}
