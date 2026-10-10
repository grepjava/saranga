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
    /// Whether `<web>` is present: the embedded web console, which Saranga
    /// does not have.
    pub web: bool,
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
            web: false,
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
                "web" => config.web = true,
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
        assert!(config.web);
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
