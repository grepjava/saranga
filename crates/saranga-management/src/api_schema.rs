//! The console API ([`crate::api`]) as an OpenAPI 3.1 document, from which
//! the console's TypeScript client is generated. The listings' fields come
//! from the broker's views, so the document cannot drift from them.

use saranga_server::management::VERSION;
use saranga_server::management::json::Json;
use saranga_server::management::view_fields;

use crate::api::{API, LISTINGS};

fn obj() -> Json {
    Json::object()
}

fn schema_ref(name: &str) -> Json {
    obj().with("$ref", format!("#/components/schemas/{name}"))
}

fn string() -> Json {
    obj().with("type", "string")
}

fn integer() -> Json {
    obj().with("type", "integer")
}

fn boolean() -> Json {
    obj().with("type", "boolean")
}

fn array(items: Json) -> Json {
    obj().with("type", "array").with("items", items)
}

/// An object schema: its properties, each required.
fn object(props: &[(&str, Json)]) -> Json {
    let mut p = obj();
    for (k, v) in props {
        p.put(k, v.clone());
    }
    let required = props.iter().map(|(k, _)| Json::from(*k)).collect();
    obj()
        .with("type", "object")
        .with("properties", p)
        .with("required", Json::Array(required))
}

/// An object schema with some optional properties.
fn object_with_optional(props: &[(&str, Json)], optional: &[(&str, Json)]) -> Json {
    let mut o = object(props);
    if let Some(Json::Object(p)) = o.get("properties").cloned() {
        let mut p = Json::Object(p);
        for (k, v) in optional {
            p.put(k, v.clone());
        }
        o.put("properties", p);
    }
    o
}

/// Any JSON value.
fn any() -> Json {
    obj()
}

fn enumeration(values: &[&str]) -> Json {
    obj().with("type", "string").with(
        "enum",
        Json::Array(values.iter().map(|v| Json::from(*v)).collect()),
    )
}

fn map_of(values: Json) -> Json {
    obj()
        .with("type", "object")
        .with("additionalProperties", values)
}

fn json_content(schema: Json) -> Json {
    obj().with("application/json", obj().with("schema", schema))
}

/// The answers of an operation: `ok` with its schema (none for 204), and
/// the errors it may give.
fn responses(ok: Option<(u16, Json)>, errors: &[u16]) -> Json {
    let mut r = obj();
    match ok {
        Some((code, schema)) => r.put(
            &code.to_string(),
            obj()
                .with("description", "OK")
                .with("content", json_content(schema)),
        ),
        None => r.put("204", obj().with("description", "Done")),
    }
    for code in errors {
        let description = match code {
            400 => "Bad parameters, or the operation failed",
            401 => "Not logged in",
            403 => "Not permitted, or the X-Saranga-Console header is missing",
            404 => "Not found",
            429 => "Too many failed logins",
            _ => "Error",
        };
        r.put(
            &code.to_string(),
            obj()
                .with("description", description)
                .with("content", json_content(schema_ref("Error"))),
        );
    }
    r
}

fn param(name: &str, location: &str, schema: Json, required: bool, description: &str) -> Json {
    obj()
        .with("name", name)
        .with("in", location)
        .with("required", required)
        .with("schema", schema)
        .with("description", description)
}

fn path_param(name: &str) -> Json {
    param(name, "path", string(), true, "")
}

fn mark() -> Json {
    param(
        "X-Saranga-Console",
        "header",
        obj().with("type", "string").with("const", "1"),
        true,
        "Marks a request the console sent",
    )
}

fn operation(id: &str, summary: &str, parameters: Vec<Json>, responses: Json) -> Json {
    obj()
        .with("operationId", id)
        .with("summary", summary)
        .with("parameters", Json::Array(parameters))
        .with("responses", responses)
}

fn with_body(mut op: Json, schema: Json) -> Json {
    op.put(
        "requestBody",
        obj()
            .with("required", true)
            .with("content", json_content(schema)),
    );
    op
}

/// How a listing writes a member of its rows.
#[derive(Clone, Copy)]
enum T {
    /// A string, as most are: Artemis's views write `toString`.
    S,
    I,
}

use T::{I, S};

/// The members of each listing's rows and their types, as the views'
/// `toJson` writes them (`views.rs`). The console suite checks live rows
/// against them.
const ROWS: &[(&str, &[(&str, T)])] = &[
    (
        "connections",
        &[
            ("connectionID", S),
            ("remoteAddress", S),
            ("users", S),
            ("creationTime", S),
            ("implementation", S),
            ("protocol", S),
            ("clientID", S),
            ("localAddress", S),
            ("sessionCount", I),
            ("proxyAddress", S),
            ("proxyProtocolVersion", S),
        ],
    ),
    (
        "sessions",
        &[
            ("id", S),
            ("user", S),
            ("validatedUser", S),
            ("creationTime", S),
            ("consumerCount", I),
            ("producerCount", I),
            ("connectionID", S),
            ("clientID", S),
        ],
    ),
    (
        "consumers",
        &[
            ("id", S),
            ("session", S),
            ("clientID", S),
            ("user", S),
            ("validatedUser", S),
            ("protocol", S),
            ("queue", S),
            ("queueType", S),
            ("filter", S),
            ("address", S),
            ("localAddress", S),
            ("remoteAddress", S),
            ("creationTime", S),
            ("messagesInTransit", S),
            ("messagesInTransitSize", S),
            ("messagesDelivered", S),
            ("messagesDeliveredSize", S),
            ("messagesAcknowledged", S),
            ("messagesAcknowledgedAwaitingCommit", S),
            ("lastDeliveredTime", I),
            ("lastAcknowledgedTime", I),
            ("status", S),
        ],
    ),
    (
        "producers",
        &[
            ("id", S),
            ("name", S),
            ("session", S),
            ("clientID", S),
            ("user", S),
            ("validatedUser", S),
            ("protocol", S),
            ("address", S),
            ("localAddress", S),
            ("remoteAddress", S),
            ("creationTime", S),
            ("msgSent", I),
            ("msgSizeSent", I),
            ("lastProducedMessageID", S),
        ],
    ),
    (
        "addresses",
        &[
            ("id", S),
            ("name", S),
            ("routingTypes", S),
            ("queueCount", S),
            ("internal", S),
            ("temporary", S),
            ("autoCreated", S),
            ("paused", S),
            ("currentDuplicateIdCacheSize", S),
            ("retroactiveResource", S),
            ("unroutedMessageCount", S),
            ("routedMessageCount", S),
            ("messageCount", S),
            ("addressLimitPercent", S),
            ("numberOfPages", S),
            ("addressSize", S),
            ("maxPageReadBytes", S),
            ("maxPageReadMessages", S),
            ("prefetchPageBytes", S),
            ("prefetchPageMessages", S),
            ("numberOfBytesPerPage", S),
            ("paging", S),
        ],
    ),
    (
        "queues",
        &[
            ("id", S),
            ("name", S),
            ("address", S),
            ("filter", S),
            ("durable", S),
            ("paused", S),
            ("persistedPause", S),
            ("temporary", S),
            ("purgeOnNoConsumers", S),
            ("consumerCount", S),
            ("maxConsumers", S),
            ("autoCreated", S),
            ("user", S),
            ("routingType", S),
            ("messagesAdded", S),
            ("messageCount", S),
            ("messagesAcked", S),
            ("messagesExpired", S),
            ("deliveringCount", S),
            ("messagesKilled", S),
            ("directDeliver", S),
            ("exclusive", S),
            ("lastValue", S),
            ("lastValueKey", S),
            ("scheduledCount", S),
            ("groupRebalance", S),
            ("groupRebalancePauseDispatch", S),
            ("groupBuckets", S),
            ("groupFirstKey", S),
            ("enabled", S),
            ("ringSize", S),
            ("consumersBeforeDispatch", S),
            ("delayBeforeDispatch", S),
            ("autoDelete", S),
            ("internalQueue", S),
        ],
    ),
];

/// A listing's row: exactly its members.
fn row(kind: &str) -> Json {
    let members = ROWS
        .iter()
        .find(|(k, _)| *k == kind)
        .map_or(&[][..], |(_, m)| *m);
    let props: Vec<(&str, Json)> = members
        .iter()
        .map(|(n, t)| {
            (
                *n,
                match t {
                    S => string(),
                    I => integer(),
                },
            )
        })
        .collect();
    object(&props).with("additionalProperties", false)
}

fn listing_name(kind: &str) -> String {
    let mut c = kind.chars();
    let upper: String = c
        .next()
        .map(|f| f.to_uppercase().chain(c).collect())
        .unwrap_or_default();
    format!("{upper}Page")
}

fn schemas() -> Json {
    let mut s = obj()
        .with(
            "Error",
            object(&[("error", string()), ("status", integer())]),
        )
        .with(
            "Login",
            object(&[("user", string()), ("password", string())]),
        )
        .with(
            "Me",
            object(&[
                ("user", string()),
                ("roles", array(string())),
                (
                    "guard",
                    obj().with("type", "string").with(
                        "enum",
                        Json::Array(vec!["management.xml".into(), "rbac".into()]),
                    ),
                ),
                ("broker", map_of(boolean())),
            ]),
        )
        .with(
            "Checks",
            object(&[(
                "checks",
                array(object(&[("resource", string()), ("method", string())])),
            )]),
        )
        .with("CheckResults", object(&[("results", array(boolean()))]))
        .with(
            "Broker",
            object(&[("attributes", map_of(obj())), ("denied", array(string()))]),
        );
    let mut views = Vec::new();
    for kind in LISTINGS {
        let Some((fields, default_sort)) = view_fields(kind) else {
            continue;
        };
        let names: Vec<Json> = fields.iter().map(|(_, n)| Json::from(*n)).collect();
        let field = obj()
            .with("type", "string")
            .with("enum", Json::Array(names));
        s.put(
            &format!("{}Field", listing_name(kind).trim_end_matches("Page")),
            field,
        );
        let row_name = format!("{}Row", listing_name(kind).trim_end_matches("Page"));
        s.put(&row_name, row(kind));
        s.put(
            &listing_name(kind),
            object(&[
                ("data", array(schema_ref(&row_name))),
                ("count", integer()),
                ("page", integer()),
                ("size", integer()),
            ]),
        );
        views.push((
            kind,
            object(&[
                (
                    "fields",
                    array(object(&[("name", string()), ("constant", string())])),
                ),
                (
                    "defaultSort",
                    obj().with("type", "string").with("const", default_sort),
                ),
            ]),
        ));
    }
    s.put("Views", object(&views));
    let summary = [
        ("resource", string()),
        ("type", enumeration(RESOURCE_TYPES)),
        ("objectName", string()),
        ("className", string()),
    ];
    let value = [("value", any()), ("error", string()), ("denied", boolean())];
    s.put("ResourceSummary", object(&summary));
    s.put(
        "Resources",
        object(&[("data", array(schema_ref("ResourceSummary")))]),
    );
    s.put("AttributeValue", object_with_optional(&[], &value));
    s.put(
        "Attribute",
        object_with_optional(
            &[
                ("name", string()),
                ("type", string()),
                ("description", string()),
                ("readable", boolean()),
                ("writable", boolean()),
            ],
            &value,
        ),
    );
    s.put(
        "Operation",
        object(&[
            ("name", string()),
            ("signature", string()),
            ("description", string()),
            ("returns", string()),
            (
                "impact",
                enumeration(&["INFO", "ACTION", "ACTION_INFO", "UNKNOWN"]),
            ),
            (
                "params",
                array(object(&[
                    ("name", string()),
                    ("type", string()),
                    ("description", string()),
                ])),
            ),
            ("allowed", boolean()),
        ]),
    );
    let mut resource = summary.to_vec();
    resource.push(("attributes", array(schema_ref("Attribute"))));
    resource.push(("operations", array(schema_ref("Operation"))));
    s.put("Resource", object(&resource));
    s.put(
        "Attributes",
        object(&[("attributes", map_of(schema_ref("AttributeValue")))]),
    );
    s.put("Write", object(&[("value", any())]));
    s.put("Written", object(&[("previous", any())]));
    s.put(
        "Invoke",
        object_with_optional(&[("operation", string())], &[("arguments", array(any()))]),
    );
    s.put("Result", object(&[("result", any())]));
    s
}

/// The kinds of resource, as `GET /resources` names them.
const RESOURCE_TYPES: &[&str] = &[
    "broker",
    "address",
    "queue",
    "divert",
    "acceptor",
    "broadcastgroup",
    "bridge",
    "brokerconnection",
    "clusterconnection",
    "hawtio",
    "platform",
];

fn paths() -> Json {
    let p = |path: &str| format!("{API}{path}");
    let mut paths = obj()
        .with(
            &p("/session"),
            obj()
                .with(
                    "post",
                    with_body(
                        operation(
                            "login",
                            "Log in: the broker's JAAS domain, with a console role",
                            vec![mark()],
                            responses(Some((200, schema_ref("Me"))), &[401, 403, 429]),
                        ),
                        schema_ref("Login"),
                    ),
                )
                .with(
                    "delete",
                    operation("logout", "Log out", vec![mark()], responses(None, &[403])),
                ),
        )
        .with(
            &p("/me"),
            obj().with(
                "get",
                operation(
                    "me",
                    "The user, their roles, and the broker operations they may call",
                    vec![],
                    responses(Some((200, schema_ref("Me"))), &[401]),
                ),
            ),
        )
        .with(
            &p("/me/can"),
            obj().with(
                "post",
                with_body(
                    operation(
                        "can",
                        "Whether the user may call each method on each resource",
                        vec![],
                        responses(Some((200, schema_ref("CheckResults"))), &[400, 401]),
                    ),
                    schema_ref("Checks"),
                ),
            ),
        )
        .with(
            &p("/views"),
            obj().with(
                "get",
                operation(
                    "views",
                    "The fields each listing filters and sorts by",
                    vec![],
                    responses(Some((200, schema_ref("Views"))), &[401]),
                ),
            ),
        )
        .with(
            &p("/broker"),
            obj().with(
                "get",
                operation(
                    "broker",
                    "The broker's status attributes the user may read",
                    vec![],
                    responses(Some((200, schema_ref("Broker"))), &[401]),
                ),
            ),
        );
    for kind in LISTINGS {
        let field = schema_ref(&format!(
            "{}Field",
            listing_name(kind).trim_end_matches("Page")
        ));
        paths.put(
            &p(&format!("/{kind}")),
            obj().with(
                "get",
                operation(
                    &format!("list_{kind}"),
                    &format!("One page of the {kind}"),
                    vec![
                        obj()
                            .with("name", "filter")
                            .with("in", "query")
                            .with("required", false)
                            .with("schema", array(string()))
                            .with("style", "form")
                            .with("explode", true)
                            .with(
                                "description",
                                "field:OPERATION:value, OPERATION one of EQUALS, NOT_EQUALS, CONTAINS, NOT_CONTAINS, GREATER_THAN, LESS_THAN",
                            ),
                        param("sort", "query", field, false, "The field to sort by"),
                        param(
                            "order",
                            "query",
                            obj()
                                .with("type", "string")
                                .with("enum", Json::Array(vec!["asc".into(), "desc".into()])),
                            false,
                            "",
                        ),
                        param("page", "query", integer(), false, "From 1"),
                        param("size", "query", integer(), false, "1 to 10000; 100 by default"),
                    ],
                    responses(Some((200, schema_ref(&listing_name(kind)))), &[400, 401, 403]),
                ),
            ),
        );
    }
    let close = |id: &str, summary: &str, params: &[&str]| {
        let mut parameters: Vec<Json> = params.iter().map(|n| path_param(n)).collect();
        parameters.push(mark());
        obj().with(
            "delete",
            operation(id, summary, parameters, responses(None, &[401, 403, 404])),
        )
    };
    paths.put(
        &p("/connections/{id}"),
        close("closeConnection", "Close a connection", &["id"]),
    );
    paths.put(
        &p("/sessions/{connection}/{id}"),
        close("closeSession", "Close a session", &["connection", "id"]),
    );
    paths.put(
        &p("/consumers/{session}/{id}"),
        close("closeConsumer", "Close a consumer", &["session", "id"]),
    );
    paths.put(
        &p("/resources"),
        obj().with(
            "get",
            operation(
                "resources",
                "The management resources (MBeans) the user may see",
                vec![],
                responses(Some((200, schema_ref("Resources"))), &[401, 403]),
            ),
        ),
    );
    paths.put(
        &p("/resources/{resource}"),
        obj().with(
            "get",
            operation(
                "resource",
                "A resource: its attributes' values and its operations",
                vec![path_param("resource")],
                responses(Some((200, schema_ref("Resource"))), &[401, 403, 404]),
            ),
        ),
    );
    paths.put(
        &p("/resources/{resource}/attributes"),
        obj().with(
            "get",
            operation(
                "attributes",
                "Some attributes' values",
                vec![
                    path_param("resource"),
                    obj()
                        .with("name", "name")
                        .with("in", "query")
                        .with("required", false)
                        .with("schema", array(string()))
                        .with("style", "form")
                        .with("explode", true)
                        .with("description", "An attribute's name"),
                ],
                responses(Some((200, schema_ref("Attributes"))), &[400, 401, 403, 404]),
            ),
        ),
    );
    paths.put(
        &p("/resources/{resource}/attributes/{name}"),
        obj().with(
            "put",
            with_body(
                operation(
                    "writeAttribute",
                    "Write an attribute; the answer is its previous value",
                    vec![path_param("resource"), path_param("name"), mark()],
                    responses(Some((200, schema_ref("Written"))), &[400, 401, 403, 404]),
                ),
                schema_ref("Write"),
            ),
        ),
    );
    paths.put(
        &p("/resources/{resource}/operations"),
        obj().with(
            "post",
            with_body(
                operation(
                    "invoke",
                    "Call an operation, by its signature when it is overloaded",
                    vec![path_param("resource"), mark()],
                    responses(Some((200, schema_ref("Result"))), &[400, 401, 403, 404]),
                ),
                schema_ref("Invoke"),
            ),
        ),
    );
    paths
}

/// The document.
pub fn document() -> Json {
    obj()
        .with("openapi", "3.1.0")
        .with(
            "info",
            obj()
                .with("title", "Saranga console API")
                .with("version", VERSION),
        )
        .with("paths", paths())
        .with(
            "components",
            obj().with("schemas", schemas()).with(
                "securitySchemes",
                obj()
                    .with(
                        "session",
                        obj()
                            .with("type", "apiKey")
                            .with("in", "cookie")
                            .with("name", "SARANGA_SESSION")
                            .with(
                                "description",
                                "SARANGA_SESSION_<binding>_<port>, which POST /session sets",
                            ),
                    )
                    .with("basic", obj().with("type", "http").with("scheme", "basic")),
            ),
        )
        .with(
            "security",
            Json::Array(vec![
                obj().with("session", Json::Array(vec![])),
                obj().with("basic", Json::Array(vec![])),
            ]),
        )
}

#[cfg(test)]
mod tests {
    #[test]
    fn every_path_is_routed_and_named_once() {
        let doc = super::document();
        let Some(saranga_server::management::json::Json::Object(paths)) = doc.get("paths") else {
            panic!("no paths");
        };
        let mut ids = std::collections::BTreeSet::new();
        for (_, item) in paths {
            let saranga_server::management::json::Json::Object(ops) = item else {
                panic!("not an object");
            };
            for (_, op) in ops {
                let id = op.get("operationId").and_then(|i| i.as_str()).unwrap();
                assert!(ids.insert(id.to_owned()), "{id} twice");
            }
        }
        // Six of the session and the user, six listings, three closes, five
        // of the resources.
        assert_eq!(ids.len(), 20);
    }

    #[test]
    fn every_listing_has_rows() {
        for kind in crate::api::LISTINGS {
            assert!(
                super::ROWS.iter().any(|(k, m)| *k == kind && !m.is_empty()),
                "no rows for {kind}"
            );
        }
    }

    /// The JSON indented as `JSON.stringify(v, null, '\t')` writes it, so
    /// the committed copy reads well in a diff.
    fn pretty(v: &Json, depth: usize, out: &mut String) {
        let indent = |out: &mut String, d: usize| {
            out.push('\n');
            out.extend(std::iter::repeat_n('\t', d));
        };
        match v {
            Json::Array(items) if !items.is_empty() => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    indent(out, depth + 1);
                    pretty(item, depth + 1, out);
                }
                indent(out, depth);
                out.push(']');
            }
            Json::Object(members) if !members.is_empty() => {
                out.push('{');
                for (i, (k, item)) in members.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    indent(out, depth + 1);
                    out.push_str(&Json::from(k.as_str()).to_string());
                    out.push_str(": ");
                    pretty(item, depth + 1, out);
                }
                indent(out, depth);
                out.push('}');
            }
            other => out.push_str(&other.to_string()),
        }
    }

    use saranga_server::management::json::Json;

    /// The console generates its client from `console/src/lib/api/openapi.json`,
    /// so it must be this document. `SARANGA_UPDATE_OPENAPI=1` rewrites it.
    #[test]
    fn the_console_has_this_document() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../console/src/lib/api/openapi.json");
        let mut text = String::new();
        pretty(&super::document(), 0, &mut text);
        text.push('\n');
        if std::env::var_os("SARANGA_UPDATE_OPENAPI").is_some() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &text).unwrap();
            return;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            committed.replace("\r\n", "\n") == text,
            "{} is not the API's document: run SARANGA_UPDATE_OPENAPI=1 cargo test -p saranga-management the_console_has_this_document, then npm run api in console/",
            path.display()
        );
    }
}
