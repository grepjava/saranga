//! `bootstrap.xml`, the file `artemis run` starts from. Ported from
//! `BrokerDTO`, `JaasSecurityDTO`, `SecurityManagerDTO` and `ServerDTO`:
//!
//! ```xml
//! <broker xmlns="http://activemq.apache.org/schema">
//!    <jaas-security domain="activemq"/>
//!    <server configuration="file:/var/lib/artemis-instance/etc//broker.xml"/>
//!    <web path="web">...</web>
//! </broker>
//! ```
//!
//! `${...}` placeholders are replaced before parsing, as `XmlUtil.decode`
//! does with system properties.

use std::path::PathBuf;

use roxmltree::{Document, ParsingOptions};

use crate::broker::Warning;
use crate::{ConfigError, Result};

/// How clients are authenticated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapSecurity {
    /// `<jaas-security domain certificate-domain>`.
    Jaas {
        domain: String,
        certificate_domain: Option<String>,
    },
    /// `<security-manager class-name>` with its `<property key value>`s.
    Manager {
        class_name: String,
        properties: Vec<(String, String)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapConfig {
    pub security: Option<BootstrapSecurity>,
    /// `<server configuration>`: a URI, normally `file:` and absolute.
    pub server_configuration: Option<String>,
    /// `<web>`: the embedded web server (`WebServerDTO`).
    pub web: Option<WebConfig>,
    pub warnings: Vec<Warning>,
}

impl BootstrapConfig {
    pub fn parse(xml: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let text = crate::substitute::replace_system_props(xml, lookup);
        let options = ParsingOptions {
            allow_dtd: true,
            ..ParsingOptions::default()
        };
        let doc = Document::parse_with_options(&text, options)?;
        let root = doc.root_element();
        let mut config = Self {
            security: None,
            server_configuration: None,
            web: None,
            warnings: Vec::new(),
        };
        for child in root.children().filter(|c| c.is_element()) {
            let name = child.tag_name().name();
            let attribute = |a: &str| {
                child
                    .attribute(a)
                    .map(str::to_owned)
                    .ok_or_else(|| ConfigError::MissingAttribute {
                        element: name.to_owned(),
                        attribute: a.to_owned(),
                    })
            };
            match name {
                "jaas-security" => {
                    config.security = Some(BootstrapSecurity::Jaas {
                        domain: attribute("domain")?,
                        certificate_domain: child
                            .attribute("certificate-domain")
                            .map(str::to_owned),
                    })
                }
                "security-manager" => {
                    let properties = child
                        .children()
                        .filter(|p| p.is_element() && p.tag_name().name() == "property")
                        .map(|p| {
                            (
                                p.attribute("key").unwrap_or_default().to_owned(),
                                p.attribute("value").unwrap_or_default().to_owned(),
                            )
                        })
                        .collect();
                    config.security = Some(BootstrapSecurity::Manager {
                        class_name: attribute("class-name")?,
                        properties,
                    });
                }
                "server" => config.server_configuration = Some(attribute("configuration")?),
                "web" => config.web = Some(WebConfig::parse(child)),
                // Management (`management.xml` holds the JMX side) and plugin
                // components.
                "authorization" | "naming" => config.warnings.push(Warning::Unsupported {
                    name: name.to_owned(),
                    reason: "JMX is not supported",
                }),
                _ => config.warnings.push(Warning::UnknownElement {
                    parent: "broker".to_owned(),
                    name: name.to_owned(),
                }),
            }
        }
        Ok(config)
    }

    /// The `<server configuration>` URI as a file path, when it is a
    /// `file:` URI.
    pub fn configuration_path(&self) -> Option<PathBuf> {
        self.server_configuration.as_deref().and_then(file_uri_path)
    }
}

/// `<web>` (`WebServerDTO`): the embedded web server and its bindings.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WebConfig {
    /// `path`: the directory of the web applications.
    pub path: String,
    pub root_redirect_location: Option<String>,
    /// `<binding>`s, or the one the legacy `bind` attribute and the
    /// `<web>`-level TLS attributes and `<app>`s make.
    pub bindings: Vec<WebBinding>,
    pub warnings: Vec<Warning>,
}

/// `<binding>` (`BindingDTO`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WebBinding {
    pub name: Option<String>,
    /// `http://host:port` or `https://host:port`.
    pub uri: String,
    pub client_auth: bool,
    pub password_codec: Option<String>,
    pub key_store_path: Option<String>,
    pub key_store_type: Option<String>,
    pub key_store_password: Option<String>,
    pub trust_store_path: Option<String>,
    pub trust_store_type: Option<String>,
    pub trust_store_password: Option<String>,
    pub included_tls_protocols: Option<String>,
    pub excluded_tls_protocols: Option<String>,
    pub included_cipher_suites: Option<String>,
    pub excluded_cipher_suites: Option<String>,
    pub ssl_auto_reload: bool,
    pub apps: Vec<WebApp>,
}

/// `<app>` (`AppDTO`): a web application and the URL path it is at.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WebApp {
    pub name: Option<String>,
    pub url: String,
    pub war: String,
}

impl WebConfig {
    fn parse(node: roxmltree::Node) -> Self {
        let attr = |n: roxmltree::Node, a: &str| n.attribute(a).map(str::to_owned);
        let flag = |n: roxmltree::Node, a: &str| {
            n.attribute(a)
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
        };
        let apps = |n: roxmltree::Node| -> Vec<WebApp> {
            n.children()
                .filter(|c| c.is_element() && c.tag_name().name() == "app")
                .map(|c| WebApp {
                    name: attr(c, "name"),
                    url: attr(c, "url").unwrap_or_default(),
                    war: attr(c, "war").unwrap_or_default(),
                })
                .collect()
        };
        let binding = |n: roxmltree::Node, uri: String, apps: Vec<WebApp>| WebBinding {
            name: attr(n, "name"),
            uri,
            client_auth: flag(n, "clientAuth"),
            password_codec: attr(n, "passwordCodec"),
            key_store_path: attr(n, "keyStorePath"),
            key_store_type: attr(n, "keyStoreType"),
            key_store_password: attr(n, "keyStorePassword"),
            trust_store_path: attr(n, "trustStorePath"),
            trust_store_type: attr(n, "trustStoreType"),
            trust_store_password: attr(n, "trustStorePassword"),
            included_tls_protocols: attr(n, "includedTLSProtocols"),
            excluded_tls_protocols: attr(n, "excludedTLSProtocols"),
            included_cipher_suites: attr(n, "includedCipherSuites"),
            excluded_cipher_suites: attr(n, "excludedCipherSuites"),
            ssl_auto_reload: flag(n, "sslAutoReload"),
            apps,
        };
        let mut config = Self {
            path: attr(node, "path").unwrap_or_default(),
            root_redirect_location: attr(node, "rootRedirectLocation"),
            ..Self::default()
        };
        for child in node.children().filter(|c| c.is_element()) {
            match child.tag_name().name() {
                "binding" => {
                    let uri = attr(child, "uri").unwrap_or_default();
                    config.bindings.push(binding(child, uri, apps(child)));
                }
                "app" | "request-log" => {}
                other => config.warnings.push(Warning::UnknownElement {
                    parent: "web".to_owned(),
                    name: other.to_owned(),
                }),
            }
        }
        // `WebServerDTO.getAllBindings`: the legacy `bind` is one more
        // binding, with the `<web>`-level settings and apps.
        if let Some(bind) = attr(node, "bind") {
            config.bindings.insert(0, binding(node, bind, apps(node)));
        }
        config
    }
}

/// `new File(new URI(uri))` for a `file:` URI: `file:/a/b`, `file:///a/b`,
/// `file:/C:/a`, with `%XX` escapes decoded.
pub fn file_uri_path(uri: &str) -> Option<PathBuf> {
    let rest = uri.strip_prefix("file:")?;
    let rest = rest.strip_prefix("//").map_or(rest, |r| {
        // `file://host/path`: only an empty host is a local file.
        if r.starts_with('/') { r } else { "" }
    });
    if rest.is_empty() {
        return None;
    }
    let decoded = percent_decode(rest)?;
    // `/C:/x` names a Windows drive.
    let bytes = decoded.as_bytes();
    let path = if bytes.len() >= 3
        && bytes[0] == b'/'
        && bytes[1].is_ascii_alphabetic()
        && bytes[2] == b':'
    {
        &decoded[1..]
    } else {
        &decoded
    };
    Some(PathBuf::from(path))
}

fn percent_decode(s: &str) -> Option<String> {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = s.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artemis_create_bootstrap() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<broker xmlns="http://activemq.apache.org/schema">
   <jaas-security domain="activemq"/>
   <server configuration="file:${artemis.instance}/etc//broker.xml"/>
   <web path="web" rootRedirectLocation="console">
       <binding name="artemis" uri="http://localhost:8161"/>
   </web>
</broker>"#;
        let config = BootstrapConfig::parse(xml, |n| {
            (n == "artemis.instance").then(|| "/var/lib/i".to_owned())
        })
        .unwrap();
        assert_eq!(
            config.security,
            Some(BootstrapSecurity::Jaas {
                domain: "activemq".into(),
                certificate_domain: None
            })
        );
        assert_eq!(
            config.configuration_path(),
            Some(PathBuf::from("/var/lib/i/etc//broker.xml"))
        );
        let web = config.web.unwrap();
        assert_eq!(web.path, "web");
        assert_eq!(web.root_redirect_location.as_deref(), Some("console"));
        assert_eq!(web.bindings.len(), 1);
        assert_eq!(web.bindings[0].uri, "http://localhost:8161");
        assert!(config.warnings.is_empty());
    }

    #[test]
    fn basic_security_manager() {
        let xml = r#"<broker xmlns="http://activemq.apache.org/schema">
   <security-manager class-name="org.apache.activemq.artemis.spi.core.security.ActiveMQBasicSecurityManager">
      <property key="bootstrapUser" value="admin"/>
      <property key="bootstrapRole" value="amq"/>
   </security-manager>
   <server configuration="file:/tmp/basic/etc//broker.xml"/>
</broker>"#;
        let config = BootstrapConfig::parse(xml, |_| None).unwrap();
        let Some(BootstrapSecurity::Manager {
            class_name,
            properties,
        }) = config.security
        else {
            panic!()
        };
        assert!(class_name.ends_with("ActiveMQBasicSecurityManager"));
        assert_eq!(properties[1], ("bootstrapRole".into(), "amq".into()));
    }

    #[test]
    fn file_uris() {
        assert_eq!(file_uri_path("file:/a/b"), Some(PathBuf::from("/a/b")));
        assert_eq!(file_uri_path("file:///a/b"), Some(PathBuf::from("/a/b")));
        assert_eq!(file_uri_path("file:/C:/x/y"), Some(PathBuf::from("C:/x/y")));
        assert_eq!(file_uri_path("file:/a%20b"), Some(PathBuf::from("/a b")));
        assert_eq!(file_uri_path("http://x/y"), None);
        assert_eq!(file_uri_path("file://host/y"), None);
    }
}
