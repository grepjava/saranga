//! Embeds the console's built pages (`console/build`, which `npm run build`
//! writes) in the binary, each gzipped, with its type and ETag, and the
//! hashes of `index.html`'s inline scripts for its Content-Security-Policy.
//!
//! Without the pages (a build with no Node.js) a stub page says how to
//! build them, and the build warns; `SARANGA_CONSOLE_REQUIRED=1`, which the
//! release builds set, makes it fail instead.

use std::io::Write;
use std::path::{Path, PathBuf};

use flate2::Compression;
use flate2::write::GzEncoder;
use sha2::{Digest, Sha256};

const STUB: &str = "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\
<title>Saranga console</title></head><body>\
<h1>The Saranga console is not built</h1>\
<p>This binary was built without the console's pages. Build them with \
<code>npm ci &amp;&amp; npm run build</code> in <code>console/</code>, \
then build Saranga again.</p></body></html>\n";

fn main() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../console/build");
    println!("cargo:rerun-if-changed={}", dir.display());
    println!("cargo:rerun-if-env-changed=SARANGA_CONSOLE_REQUIRED");
    let out = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

    let mut files = Vec::new();
    if dir.join("index.html").is_file() {
        walk(&dir, &dir, &mut files);
        files.sort();
    } else if std::env::var_os("SARANGA_CONSOLE_REQUIRED").is_some_and(|v| v == "1") {
        panic!(
            "{} has no index.html: run npm ci && npm run build in console/",
            dir.display()
        );
    } else {
        println!(
            "cargo:warning=the Saranga console is not built ({} has no index.html): the binary serves a stub page at /saranga",
            dir.display()
        );
    }

    let mut code = String::from("pub(crate) static PAGES: &[Page] = &[\n");
    let mut scripts = Vec::new();
    let mut add = |name: &str, bytes: &[u8], index: usize| {
        let gz = out.join(format!("page{index}.gz"));
        let mut e = GzEncoder::new(Vec::new(), Compression::best());
        e.write_all(bytes).unwrap();
        std::fs::write(&gz, e.finish().unwrap()).unwrap();
        let etag = hex(&Sha256::digest(bytes)[..12]);
        code.push_str(&format!(
            "    Page {{ path: {name:?}, content_type: {:?}, etag: \"\\\"{etag}\\\"\", gzip: include_bytes!({:?}) }},\n",
            content_type(name),
            gz.display().to_string(),
        ));
    };
    if files.is_empty() {
        add("index.html", STUB.as_bytes(), 0);
    }
    for (i, name) in files.iter().enumerate() {
        let bytes = std::fs::read(dir.join(name)).unwrap();
        if name == "index.html" {
            scripts = inline_scripts(&String::from_utf8_lossy(&bytes));
        }
        add(name, &bytes, i);
    }
    code.push_str("];\n\n/// The hashes of `index.html`'s inline scripts, as CSP sources.\n");
    code.push_str("pub(crate) const SCRIPT_HASHES: &str = \"");
    for s in &scripts {
        code.push_str(&format!(
            " 'sha256-{}'",
            base64(&Sha256::digest(s.as_bytes()))
        ));
    }
    code.push_str("\";\n");
    std::fs::write(out.join("pages.rs"), code).unwrap();
}

/// Every file under `dir`, as a `/`-separated path relative to `root`.
fn walk(root: &Path, dir: &Path, files: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            walk(root, &path, files);
        } else {
            let rel = path.strip_prefix(root).unwrap();
            let parts: Vec<_> = rel.iter().map(|p| p.to_string_lossy()).collect();
            files.push(parts.join("/"));
        }
    }
}

fn content_type(name: &str) -> &'static str {
    match name.rsplit('.').next().unwrap_or_default() {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "txt" => "text/plain; charset=utf-8",
        "webmanifest" => "application/manifest+json",
        _ => "application/octet-stream",
    }
}

/// The bodies of the `<script>` elements without a `src`.
fn inline_scripts(html: &str) -> Vec<String> {
    let mut scripts = Vec::new();
    let mut rest = html;
    while let Some(start) = rest.find("<script") {
        rest = &rest[start..];
        let Some(open_end) = rest.find('>') else {
            break;
        };
        let open = &rest[..open_end];
        let body_start = open_end + 1;
        let Some(close) = rest[body_start..].find("</script>") else {
            break;
        };
        if !open.contains(" src=") {
            scripts.push(rest[body_start..body_start + close].to_owned());
        }
        rest = &rest[body_start + close..];
    }
    scripts
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn base64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut s = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                s.push(A[(n >> (18 - 6 * i)) as usize & 63] as char);
            } else {
                s.push('=');
            }
        }
    }
    s
}
