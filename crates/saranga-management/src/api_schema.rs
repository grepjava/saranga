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
        s.put(
            &listing_name(kind),
            object(&[
                ("data", array(map_of(obj()))),
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
    s
}

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
        // Six of the session and the user, six listings, three closes.
        assert_eq!(ids.len(), 15);
    }
}
