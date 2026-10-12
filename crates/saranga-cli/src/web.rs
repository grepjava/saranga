//! The embedded web server of an instance (`bootstrap.xml`'s `<web>`), as
//! `artemis run` starts it: each binding, with TLS from its key and trust
//! stores, serving the management API and metrics.

use std::sync::Arc;

use anyhow::Context;
use saranga_config::{BrokerConfig, WebBinding, WebConfig};
use saranga_transport::{TlsAcceptor, TlsSettings};

use crate::instance::Instance;

/// The console roles in the instance's profile: `HAWTIO_ROLES` in
/// `etc/artemis.profile`, `-Dhawtio.roles` in `artemis.profile.cmd`.
fn profile_roles(instance: &Instance) -> Option<Vec<String>> {
    for name in ["artemis.profile", "artemis.profile.cmd"] {
        let Ok(text) = std::fs::read_to_string(instance.etc.join(name)) else {
            continue;
        };
        // `-Dhawtio.roles=<roles>` in the Windows profile's JAVA_ARGS.
        if let Some(i) = text.find("-Dhawtio.roles=") {
            let rest = &text[i + "-Dhawtio.roles=".len()..];
            let v = rest.split_whitespace().next().unwrap_or("");
            return Some(v.split(',').map(|r| r.trim().to_owned()).collect());
        }
        for line in text.lines() {
            let line = line.trim();
            let line = line.strip_prefix("set ").unwrap_or(line);
            let line = line.strip_prefix("export ").unwrap_or(line);
            if let Some(v) = line.strip_prefix("HAWTIO_ROLES=") {
                let v = v.trim().trim_matches(['\'', '"']);
                return Some(v.split(',').map(|r| r.trim().to_owned()).collect());
            }
        }
    }
    None
}

/// The console roles: `-Dhawtio.roles` (or `hawtio.role`), the profile's
/// `HAWTIO_ROLES`, else the role `artemis create` makes, `amq`.
fn console_roles(instance: &Instance) -> Vec<String> {
    instance
        .lookup("hawtio.roles")
        .or_else(|| instance.lookup("hawtio.role"))
        .map(|v| v.split(',').map(|r| r.trim().to_owned()).collect())
        .or_else(|| profile_roles(instance))
        .unwrap_or_else(|| vec!["amq".to_owned()])
}

fn binding(b: &WebBinding) -> anyhow::Result<saranga_management::Binding> {
    let (scheme, rest) = b
        .uri
        .split_once("://")
        .with_context(|| format!("web binding URI {}", b.uri))?;
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (
            h.to_owned(),
            p.parse().with_context(|| format!("port in {}", b.uri))?,
        ),
        None => (
            authority.to_owned(),
            if scheme == "https" { 443 } else { 80 },
        ),
    };
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let tls = if scheme == "https" {
        let mut params: Vec<(&str, String)> = vec![("sslEnabled", "true".to_owned())];
        let mut put = |k: &'static str, v: &Option<String>| {
            if let Some(v) = v {
                params.push((k, v.clone()));
            }
        };
        put("keyStorePath", &b.key_store_path);
        put("keyStoreType", &b.key_store_type);
        put("keyStorePassword", &b.key_store_password);
        put("trustStorePath", &b.trust_store_path);
        put("trustStoreType", &b.trust_store_type);
        put("trustStorePassword", &b.trust_store_password);
        put("enabledProtocols", &b.included_tls_protocols);
        put("enabledCipherSuites", &b.included_cipher_suites);
        if b.client_auth {
            params.push(("needClientAuth", "true".to_owned()));
        }
        let codec = b.password_codec.clone();
        let settings = TlsSettings::from_params(
            |k| params.iter().find(|(n, _)| *n == k).map(|(_, v)| v.clone()),
            |v| {
                saranga_config::mask::resolve_mask(None, v, codec.as_deref())
                    .map_err(|e| e.to_string())
            },
        )
        .with_context(|| format!("web binding {}", b.uri))?;
        settings
            .map(TlsAcceptor::new)
            .transpose()
            .with_context(|| format!("web binding {}", b.uri))?
    } else {
        None
    };
    Ok(saranga_management::Binding {
        name: b.name.clone().unwrap_or_default(),
        uri: b.uri.clone(),
        host,
        port,
        tls,
        apps: b
            .apps
            .iter()
            .map(|a| (a.url.clone(), a.war.clone()))
            .collect(),
    })
}

/// Starts the web server of `<web>`.
pub async fn start(
    instance: &Instance,
    file: &BrokerConfig,
    web: &WebConfig,
    current: &Arc<saranga_server::Current>,
) -> anyhow::Result<()> {
    tracing::info!("AMQ241003: Starting embedded web server");
    let bindings = web
        .bindings
        .iter()
        .map(binding)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let acceptors = file
        .acceptors
        .iter()
        .map(|a| {
            let mut params: Vec<(String, String)> = a
                .params
                .iter()
                .map(|(k, v)| (k.clone(), v.clone().unwrap_or_default()))
                .collect();
            params.push((
                "host".to_owned(),
                a.host.clone().unwrap_or_else(|| "localhost".to_owned()),
            ));
            params.push(("port".to_owned(), a.port.unwrap_or(61616).to_string()));
            (a.name.clone().unwrap_or_default(), params)
        })
        .collect();
    let options = saranga_management::Options {
        domain: file.jmx_domain.clone(),
        broker_name: current.get().management().config().name.clone(),
        access: saranga_management::access_list(&instance.etc),
        console_roles: console_roles(instance),
        cors: saranga_management::cors(&instance.etc),
        root_redirect: web.root_redirect_location.clone(),
        acceptors,
    };
    saranga_management::start(current.clone(), bindings, options)
        .await
        .context("starting the web server")
}
