//! The Saranga console's pages at `/saranga/`: the single-page app
//! `console/` builds, embedded by `build.rs`. Its routes are in the URL's
//! fragment, so every page is `index.html`; its scripts and styles are
//! under `_app/immutable/`, named by their content, and cached for good.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;

use crate::api::BASE;

/// An embedded file.
pub(crate) struct Page {
    pub(crate) path: &'static str,
    pub(crate) content_type: &'static str,
    pub(crate) etag: &'static str,
    pub(crate) gzip: &'static [u8],
}

include!(concat!(env!("OUT_DIR"), "/pages.rs"));

const IMMUTABLE: &str = "public, max-age=31536000, immutable";

pub(crate) struct PagesState {
    strict_transport_security: Option<HeaderValue>,
    csp: HeaderValue,
}

impl PagesState {
    pub(crate) fn new(strict_transport_security: Option<HeaderValue>) -> Self {
        // Svelte's transitions set inline styles; the scripts are only
        // the app's own and SvelteKit's inline start-up script.
        let csp = format!(
            "default-src 'self'; script-src 'self'{SCRIPT_HASHES}; \
             style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; \
             connect-src 'self'; worker-src 'self' blob:; frame-ancestors 'none'; \
             form-action 'self'; base-uri 'self'; object-src 'none'"
        );
        Self {
            strict_transport_security,
            csp: HeaderValue::from_str(&csp).expect("a header value"),
        }
    }
}

pub(crate) fn router(state: Arc<PagesState>) -> Router {
    Router::new()
        .route(
            BASE,
            get(|| async {
                (
                    StatusCode::MOVED_PERMANENTLY,
                    [(header::LOCATION, format!("{BASE}/"))],
                )
                    .into_response()
            }),
        )
        .route(
            &format!("{BASE}/"),
            get(|State(s): State<Arc<PagesState>>, headers: HeaderMap| async move {
                serve(&s, "index.html", &headers)
            }),
        )
        .route(
            &format!("{BASE}/{{*path}}"),
            get(
                |State(s): State<Arc<PagesState>>, Path(path): Path<String>, headers: HeaderMap| async move {
                    serve(&s, &path, &headers)
                },
            ),
        )
        .with_state(state)
}

fn serve(state: &PagesState, path: &str, headers: &HeaderMap) -> Response {
    let Some(page) = PAGES.iter().find(|p| p.path == path) else {
        return crate::secure(
            (
                StatusCode::NOT_FOUND,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                "Not found",
            )
                .into_response(),
        );
    };
    let cache = if page.path.starts_with("_app/immutable/") {
        IMMUTABLE
    } else {
        crate::console::NO_CACHE
    };
    let fresh = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .any(|t| t.trim() == page.etag || t.trim() == "*")
        });
    let mut r = if fresh {
        StatusCode::NOT_MODIFIED.into_response()
    } else if accepts_gzip(headers) {
        let mut r = ([(header::CONTENT_TYPE, page.content_type)], page.gzip).into_response();
        r.headers_mut()
            .insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        r
    } else {
        (
            [(header::CONTENT_TYPE, page.content_type)],
            gunzip(page.gzip),
        )
            .into_response()
    };
    let h = r.headers_mut();
    h.insert(header::ETAG, HeaderValue::from_static(page.etag));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
    h.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    h.insert("Content-Security-Policy", state.csp.clone());
    if let Some(v) = &state.strict_transport_security {
        h.insert("Strict-Transport-Security", v.clone());
    }
    crate::secure(r)
}

fn accepts_gzip(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|e| {
            let mut parts = e.split(';');
            let coding = parts.next().unwrap_or_default().trim();
            let refused = parts.any(|p| {
                p.trim()
                    .strip_prefix("q=")
                    .is_some_and(|q| q.trim().parse::<f32>().is_ok_and(|q| q == 0.0))
            });
            (coding.eq_ignore_ascii_case("gzip") || coding == "*") && !refused
        })
}

fn gunzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Read;
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_end(&mut out)
        .expect("the build gzipped it");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn there_is_an_index() {
        assert!(PAGES.iter().any(|p| p.path == "index.html"));
        assert!(PAGES.iter().all(|p| !gunzip(p.gzip).is_empty()));
    }

    #[test]
    fn gzip_is_accepted_unless_refused() {
        let h = |v: &str| {
            let mut h = HeaderMap::new();
            h.insert(header::ACCEPT_ENCODING, HeaderValue::from_str(v).unwrap());
            h
        };
        assert!(accepts_gzip(&h("gzip, deflate, br")));
        assert!(accepts_gzip(&h("br;q=1.0, gzip;q=0.8")));
        assert!(accepts_gzip(&h("*")));
        assert!(!accepts_gzip(&h("gzip;q=0")));
        assert!(!accepts_gzip(&h("br")));
        assert!(!accepts_gzip(&HeaderMap::new()));
    }
}
