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
//! The file is decoded as `XmlUtil.decode` decodes it ([`crate::dto`]):
//! validated against `activemq.xsd`, `${...}` replaced in attribute values
//! only.

use std::path::PathBuf;

use roxmltree::{Document, Node, ParsingOptions};

use crate::Result;
use crate::broker::Warning;

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
    /// `bootstrap.xml` decoded as `XmlUtil.decode` decodes a `BrokerDTO`,
    /// `lookup` giving the properties of `${...}` in attribute values.
    pub fn parse(xml: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        crate::dto::decode(crate::dto::Dto::Broker, xml, &lookup)?;
        let options = ParsingOptions {
            allow_dtd: true,
            ..ParsingOptions::default()
        };
        let doc = Document::parse_with_options(xml, options)?;
        let root = doc.root_element();
        let value = |n: Node, a: &str| n.attribute(a).map(|v| crate::dto::filter(v, &lookup));
        let mut config = Self {
            security: None,
            server_configuration: None,
            web: None,
            warnings: Vec::new(),
        };
        // `activemq.xsd` has the children in this order: the security, the
        // server, then the web servers and components.
        for child in root.children().filter(|c| c.is_element()) {
            let name = child.tag_name().name();
            match name {
                "jaas-security" => {
                    config.security = Some(BootstrapSecurity::Jaas {
                        domain: value(child, "domain").unwrap_or_default(),
                        certificate_domain: value(child, "certificate-domain"),
                    })
                }
                "security-manager" => {
                    let properties = child
                        .children()
                        .filter(|p| p.is_element())
                        .map(|p| {
                            (
                                value(p, "key").unwrap_or_default(),
                                value(p, "value").unwrap_or_default(),
                            )
                        })
                        .collect();
                    config.security = Some(BootstrapSecurity::Manager {
                        class_name: value(child, "class-name").unwrap_or_default(),
                        properties,
                    });
                }
                "server" => {
                    // `ServerDTO.getConfigurationURI`: the `configurationURI`
                    // element, else the `configuration` attribute.
                    let uri = child
                        .children()
                        .find(|c| c.is_element() && c.tag_name().name() == "configurationURI")
                        .map(|c| c.text().unwrap_or_default().to_owned());
                    config.server_configuration = uri.or_else(|| value(child, "configuration"));
                }
                "web" if config.web.is_none() => {
                    config.web = Some(WebConfig::parse(child, &|v| crate::dto::filter(v, &lookup)))
                }
                // Further web servers and components (`ExternalComponent`
                // classes) are started by Artemis.
                _ => config.warnings.push(Warning::Unsupported {
                    name: name.to_owned(),
                    reason: "Saranga runs one web server and no Java components",
                }),
            }
        }
        Ok(config)
    }

    /// The `securityconfig.` system properties applied over the security
    /// manager, as `BrokerFactory.populateSecurityWithSystemProperties`
    /// does: `jaas.domain` makes it JAAS, else `manager.className` a
    /// security manager, unless it is one already; then `jaas.domain` and
    /// `jaas.certificateDomain`, or `manager.className` and each
    /// `manager.properties.<key>`, replace or add to what bootstrap.xml says.
    pub fn apply_system_security(&mut self, system: &[(String, String)]) {
        let get = |name: &str| {
            system
                .iter()
                .rev()
                .find(|(k, _)| k.strip_prefix(SYSTEM_SECURITY_PROPERTY_PREFIX) == Some(name))
                .map(|(_, v)| v.clone())
        };
        let domain = get("jaas.domain");
        let certificate = get("jaas.certificateDomain");
        let class = get("manager.className");
        if domain.is_some() {
            if !matches!(self.security, Some(BootstrapSecurity::Jaas { .. })) {
                self.security = Some(BootstrapSecurity::Jaas {
                    domain: String::new(),
                    certificate_domain: None,
                });
            }
        } else if class.is_some()
            && !matches!(self.security, Some(BootstrapSecurity::Manager { .. }))
        {
            self.security = Some(BootstrapSecurity::Manager {
                class_name: String::new(),
                properties: Vec::new(),
            });
        }
        match &mut self.security {
            Some(BootstrapSecurity::Jaas {
                domain: d,
                certificate_domain: c,
            }) => {
                if let Some(domain) = domain {
                    *d = domain;
                }
                if certificate.is_some() {
                    *c = certificate;
                }
            }
            Some(BootstrapSecurity::Manager {
                class_name,
                properties,
            }) => {
                if let Some(class) = class {
                    *class_name = class;
                }
                let prefix = format!("{SYSTEM_SECURITY_PROPERTY_PREFIX}manager.properties.");
                properties.extend(system.iter().filter_map(|(k, v)| {
                    Some((k.strip_prefix(prefix.as_str())?.to_owned(), v.clone()))
                }));
            }
            None => {}
        }
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
    /// `<web>`, `filter` giving the values of its attributes.
    fn parse(node: Node, filter: &dyn Fn(&str) -> String) -> Self {
        let attr = |n: Node, a: &str| n.attribute(a).map(filter);
        // JAXB's `xs:boolean`: `true` or `1`, the white space collapsed.
        let flag = |n: Node, a: &str| {
            attr(n, a).is_some_and(|v| {
                matches!(
                    v.trim_matches(|c| matches!(c, ' ' | '\t' | '\n' | '\r')),
                    "true" | "1"
                )
            })
        };
        let apps = |n: Node| -> Vec<WebApp> {
            n.children()
                .filter(|c| c.is_element() && c.tag_name().name() == "app")
                .map(|c| WebApp {
                    name: attr(c, "name"),
                    url: attr(c, "url").unwrap_or_default(),
                    war: attr(c, "war").unwrap_or_default(),
                })
                .collect()
        };
        let binding = |n: Node, uri: String, apps: Vec<WebApp>| WebBinding {
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
        // `<app>`s are the legacy `bind`'s; the schema allows nothing else
        // but `<request-log>`.
        for child in node
            .children()
            .filter(|c| c.is_element() && c.tag_name().name() == "binding")
        {
            let uri = attr(child, "uri").unwrap_or_default();
            config.bindings.push(binding(child, uri, apps(child)));
        }
        // `WebServerDTO.getAllBindings`: the legacy `bind` is one more
        // binding, with the `<web>`-level settings and apps.
        if let Some(bind) = attr(node, "bind") {
            config.bindings.insert(0, binding(node, bind, apps(node)));
        }
        config
    }

    /// The `webconfig.` system properties applied over the configuration,
    /// as `Run` does with `parsePrefixedProperties`: `<attribute>` of
    /// `<web>`, `bindings.<binding>.<attribute>` and
    /// `bindings.<binding>.apps.<app>.<attribute>`, a segment in double
    /// quotes kept whole. A binding or an app is the one of that name, else
    /// of that uri or url (`getName`), else a new one of that name. What
    /// cannot be applied is returned, as Artemis records it in its status.
    pub fn apply_system(&mut self, system: &[(String, String)]) -> Vec<Warning> {
        let mut warnings = Vec::new();
        for (key, value) in system {
            let Some(key) = key.strip_prefix(SYSTEM_WEB_PROPERTY_PREFIX) else {
                continue;
            };
            let path = crate::broker_properties::split_key(key, "\"");
            let p: Vec<&str> = path.iter().map(String::as_str).collect();
            if let Err(reason) = self.apply_one(&p, value) {
                warnings.push(Warning::BrokerProperty {
                    source: format!("system-{SYSTEM_WEB_PROPERTY_PREFIX}"),
                    key: key.to_owned(),
                    reason,
                });
            }
        }
        warnings
    }

    fn apply_one(&mut self, path: &[&str], value: &str) -> std::result::Result<(), String> {
        let flag = |v: &str| match v.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(format!("{v:?} is not a boolean")),
        };
        let some = || Some(value.to_owned());
        match path {
            ["path"] => self.path = value.to_owned(),
            ["rootRedirectLocation"] => self.root_redirect_location = some(),
            ["bindings", name, rest @ ..] if !rest.is_empty() => {
                let i = match self
                    .bindings
                    .iter()
                    .position(|b| b.name.as_deref().unwrap_or(&b.uri) == *name)
                {
                    Some(i) => i,
                    None => {
                        self.bindings.push(WebBinding {
                            name: Some((*name).to_owned()),
                            ..WebBinding::default()
                        });
                        self.bindings.len() - 1
                    }
                };
                let b = &mut self.bindings[i];
                match rest {
                    ["name"] => b.name = some(),
                    ["uri"] => b.uri = value.to_owned(),
                    ["clientAuth"] => b.client_auth = flag(value)?,
                    ["passwordCodec"] => b.password_codec = some(),
                    ["keyStorePath"] => b.key_store_path = some(),
                    ["keyStoreType"] => b.key_store_type = some(),
                    ["keyStorePassword"] => b.key_store_password = some(),
                    ["trustStorePath"] => b.trust_store_path = some(),
                    ["trustStoreType"] => b.trust_store_type = some(),
                    ["trustStorePassword"] => b.trust_store_password = some(),
                    ["includedTLSProtocols"] => b.included_tls_protocols = some(),
                    ["excludedTLSProtocols"] => b.excluded_tls_protocols = some(),
                    ["includedCipherSuites"] => b.included_cipher_suites = some(),
                    ["excludedCipherSuites"] => b.excluded_cipher_suites = some(),
                    ["sslAutoReload"] => b.ssl_auto_reload = flag(value)?,
                    ["apps", app, attribute] => {
                        let i = match b
                            .apps
                            .iter()
                            .position(|a| a.name.as_deref().unwrap_or(&a.url) == *app)
                        {
                            Some(i) => i,
                            None => {
                                b.apps.push(WebApp {
                                    name: Some((*app).to_owned()),
                                    ..WebApp::default()
                                });
                                b.apps.len() - 1
                            }
                        };
                        let a = &mut b.apps[i];
                        match *attribute {
                            "name" => a.name = some(),
                            "url" => a.url = value.to_owned(),
                            "war" => a.war = value.to_owned(),
                            _ => return Err(format!("no app attribute {attribute}")),
                        }
                    }
                    _ => return Err(format!("no binding attribute {}", rest.join("."))),
                }
            }
            _ => return Err(format!("no web attribute {}", path.join("."))),
        }
        Ok(())
    }
}

/// `ActiveMQDefaultConfiguration.DEFAULT_SYSTEM_WEB_PROPERTY_PREFIX`.
pub const SYSTEM_WEB_PROPERTY_PREFIX: &str = "webconfig.";

/// `ActiveMQDefaultConfiguration.DEFAULT_SYSTEM_SECURITY_PROPERTY_PREFIX`.
pub const SYSTEM_SECURITY_PROPERTY_PREFIX: &str = "securityconfig.";

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
    fn decoded_as_xml_util_decodes() {
        let lookup = |n: &str| (n == "i").then(|| "/var/lib/i".to_owned());
        // `${...}` only in attribute values; `configurationURI` over
        // `configuration`; a boolean of 1.
        let xml = r#"<broker xmlns="http://activemq.apache.org/schema">
   <jaas-security domain="${i}"/>
   <server configuration="file:/x/broker.xml"><configurationURI>file:${i}/b.xml</configurationURI></server>
   <web path="web"><binding uri="https://h:1" clientAuth=" 1 "/></web>
   <component componentClassName="c"/>
</broker>"#;
        let config = BootstrapConfig::parse(xml, lookup).unwrap();
        assert_eq!(
            config.security,
            Some(BootstrapSecurity::Jaas {
                domain: "/var/lib/i".into(),
                certificate_domain: None
            })
        );
        assert_eq!(
            config.server_configuration.as_deref(),
            Some("file:${i}/b.xml")
        );
        assert!(config.web.unwrap().bindings[0].client_auth);
        assert_eq!(config.warnings.len(), 1);
        // What the schema refuses fails the file.
        let invalid = r#"<broker xmlns="http://activemq.apache.org/schema"><jaas-security domain="a" x="1"/><server/></broker>"#;
        let e = BootstrapConfig::parse(invalid, lookup).unwrap_err();
        assert!(
            e.to_string().contains(
                "lineNumber: 1; columnNumber: 84; cvc-complex-type.3.2.2: Attribute 'x' is not allowed to appear in element 'jaas-security'."
            ),
            "{e}"
        );
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

    #[test]
    fn securityconfig_system_properties() {
        let xml = r#"<broker xmlns="http://activemq.apache.org/schema">
   <jaas-security domain="activemq"/>
   <server configuration="file:broker.xml"/>
</broker>"#;
        let props = |p: &[(&str, &str)]| -> Vec<(String, String)> {
            p.iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect()
        };
        let mut config = BootstrapConfig::parse(xml, |_| None).unwrap();
        config.apply_system_security(&props(&[("securityconfig.jaas.certificateDomain", "Cert")]));
        assert_eq!(
            config.security,
            Some(BootstrapSecurity::Jaas {
                domain: "activemq".into(),
                certificate_domain: Some("Cert".into())
            })
        );
        let mut config = BootstrapConfig::parse(xml, |_| None).unwrap();
        config.apply_system_security(&props(&[
            ("securityconfig.manager.className", "x.Basic"),
            ("securityconfig.manager.properties.bootstrapUser", "admin"),
            ("securityconfig.manager.properties.bootstrapRole", "amq"),
        ]));
        assert_eq!(
            config.security,
            Some(BootstrapSecurity::Manager {
                class_name: "x.Basic".into(),
                properties: vec![
                    ("bootstrapUser".into(), "admin".into()),
                    ("bootstrapRole".into(), "amq".into())
                ]
            })
        );
        // jaas.domain wins over manager.className.
        config.apply_system_security(&props(&[
            ("securityconfig.manager.className", "y"),
            ("securityconfig.jaas.domain", "Other"),
        ]));
        assert_eq!(
            config.security,
            Some(BootstrapSecurity::Jaas {
                domain: "Other".into(),
                certificate_domain: None
            })
        );
    }

    #[test]
    fn webconfig_system_properties() {
        let xml = r#"<broker xmlns="http://activemq.apache.org/schema">
   <jaas-security domain="activemq"/>
   <server configuration="file:broker.xml"/>
   <web path="web">
       <binding name="artemis" uri="http://localhost:8161"/>
       <binding uri="http://localhost:8163"><app url="console" war="console.war"/></binding>
   </web>
</broker>"#;
        let mut web = BootstrapConfig::parse(xml, |_| None).unwrap().web.unwrap();
        let system: Vec<(String, String)> = [
            ("webconfig.bindings.artemis.clientAuth", "true"),
            ("webconfig.bindings.my-binding.uri", "http://localhost:8162"),
            ("webconfig.bindings.my-binding.apps.my-app.url", "my-app"),
            (
                "webconfig.bindings.my-binding.apps.my-app.war",
                "my-app.war",
            ),
            (
                "webconfig.bindings.\"http://localhost:8163\".apps.\"console\".war",
                "my-console.war",
            ),
            ("webconfig.bindings.artemis.nosuch", "x"),
            ("brokerconfig.name", "other"),
        ]
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
        let warnings = web.apply_system(&system);
        assert!(web.bindings[0].client_auth);
        assert_eq!(web.bindings[1].apps[0].war, "my-console.war");
        let b = &web.bindings[2];
        assert_eq!(b.name.as_deref(), Some("my-binding"));
        assert_eq!(b.uri, "http://localhost:8162");
        assert_eq!(
            (b.apps[0].url.as_str(), b.apps[0].war.as_str()),
            ("my-app", "my-app.war")
        );
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].to_string().contains("bindings.artemis.nosuch"));
    }
}
