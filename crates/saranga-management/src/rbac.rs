//! What guards the MBeans over the web: `management.xml`'s `<authorisation>`
//! (`ArtemisMBeanServerGuard`), or, with
//! `-Djavax.management.builder.initial=` [`RBAC_BUILDER`] and no
//! `<authorisation>`, the broker's security-settings on `mops.` addresses
//! (`ArtemisRbacInvocationHandler`).

use std::sync::Arc;

use saranga_config::CheckType;
use saranga_security::jaas::Subject;
use saranga_security::{SecurityAuth, SecurityStore};
use saranga_server::Broker;

use crate::access::AccessList;
use crate::mbean::ObjectName;

/// `ArtemisRbacMBeanServerBuilder`, as `javax.management.builder.initial`
/// names it.
pub const RBAC_BUILDER: &str =
    "org.apache.activemq.artemis.core.server.management.ArtemisRbacMBeanServerBuilder";

/// The domains the RBAC guard does not check (`uncheckedDomains`).
const UNCHECKED_DOMAINS: [&str; 1] = ["hawtio"];

#[derive(Debug, Clone)]
pub enum Guard {
    Acl(AccessList),
    Rbac(Rbac),
}

/// `ArtemisRbacInvocationHandler`'s settings, from broker.xml.
#[derive(Debug, Clone)]
pub struct Rbac {
    /// `management-rbac-prefix`.
    pub prefix: String,
    /// The address delimiter of `wildcard-addresses`.
    pub delimiter: char,
    /// `jmx-domain`: the broker's MBeans, whose addresses leave it out.
    pub domain: String,
}

impl Rbac {
    /// `addressFrom`: `<prefix>[.<domain>.<type>][.<component>][.<name>][.<method>]`.
    pub fn address(&self, name: &ObjectName, method: Option<&str>) -> String {
        let unquote = |v: &str| {
            if v.ends_with('"') {
                v.replace('"', "")
            } else {
                v.to_owned()
            }
        };
        let key = |k: &str| name.get(k).map(unquote);
        let mut parts = vec![self.prefix.clone()];
        let mut component = key("component");
        let mut name_part = key("name");
        if name.domain == self.domain {
            match component.as_deref() {
                Some("addresses") => {
                    let c = match key("subcomponent").as_deref() {
                        Some("diverts") => "divert",
                        Some("queues") => "queue",
                        _ => "address",
                    };
                    name_part = key(c);
                    component = Some(c.to_owned());
                }
                Some(_) => {}
                None => {
                    // The server control: a broker key and no component.
                    if key("broker").is_some() {
                        component = Some("broker".to_owned());
                    }
                }
            }
        } else {
            parts.push(name.domain.clone());
            parts.extend(key("type"));
        }
        parts.extend(component);
        parts.extend(name_part);
        parts.extend(method.map(str::to_owned));
        parts.join(&self.delimiter.to_string())
    }

    /// `permissionFrom`: VIEW for a method the broker's
    /// `view-permission-method-match-pattern` matches, else EDIT.
    pub fn permission(broker: &Broker, method: &str) -> CheckType {
        let view = broker
            .management()
            .config()
            .view_permission
            .as_ref()
            .is_some_and(|p| p.is_match(method));
        if view {
            CheckType::View
        } else {
            CheckType::Edit
        }
    }

    fn auth(subject: Option<Arc<Subject>>) -> SecurityAuth {
        SecurityAuth {
            subject,
            management: true,
            ..SecurityAuth::default()
        }
    }

    fn unchecked(name: &ObjectName) -> bool {
        UNCHECKED_DOMAINS.contains(&name.domain.as_str())
    }

    /// `handleInvoke`: the permission the method needs on its address,
    /// or the security store's refusal (`AMQ229032`).
    pub fn check(
        &self,
        broker: &Broker,
        subject: Option<Arc<Subject>>,
        name: &ObjectName,
        method: &str,
    ) -> Result<(), String> {
        if Self::unchecked(name) {
            return Ok(());
        }
        self.check_address(
            broker,
            subject,
            &self.address(name, Some(method)),
            Self::permission(broker, method),
        )
    }

    /// The check of a query on the MBean server
    /// (`<prefix>.mbeanserver.queryNames`).
    pub fn check_query(
        &self,
        broker: &Broker,
        subject: Option<Arc<Subject>>,
    ) -> Result<(), String> {
        let d = self.delimiter;
        let address = format!("{}{d}mbeanserver{d}queryNames", self.prefix);
        self.check_address(
            broker,
            subject,
            &address,
            Self::permission(broker, "queryNames"),
        )
    }

    fn check_address(
        &self,
        broker: &Broker,
        subject: Option<Arc<Subject>>,
        address: &str,
        check: CheckType,
    ) -> Result<(), String> {
        let Some(store) = broker.security_store() else {
            return Ok(());
        };
        store
            .check(address, None, check, &Self::auth(subject))
            .map_err(|e| e.0)
    }

    /// `canInvoke`, for the MBean info's `canInvoke`, `canRead` and
    /// `canWrite`.
    pub fn can_invoke(
        &self,
        broker: &Broker,
        subject: Option<Arc<Subject>>,
        name: &ObjectName,
        method: &str,
    ) -> bool {
        Self::unchecked(name)
            || has_permission(
                broker.security_store(),
                &self.address(name, Some(method)),
                Self::permission(broker, method),
                subject,
            )
    }

    /// Whether a query's result keeps the MBean: the VIEW permission on
    /// its address (`viewPermissionCheckFails`).
    pub fn visible(
        &self,
        broker: &Broker,
        subject: Option<Arc<Subject>>,
        name: &ObjectName,
    ) -> bool {
        Self::unchecked(name)
            || has_permission(
                broker.security_store(),
                &self.address(name, None),
                CheckType::View,
                subject,
            )
    }
}

fn has_permission(
    store: Option<&Arc<SecurityStore>>,
    address: &str,
    check: CheckType,
    subject: Option<Arc<Subject>>,
) -> bool {
    store.is_none_or(|s| s.has_permission(address, None, check, &Rbac::auth(subject)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rbac() -> Rbac {
        Rbac {
            prefix: "mops".to_owned(),
            delimiter: '.',
            domain: "org.apache.activemq.artemis".to_owned(),
        }
    }

    fn address(name: &str, method: Option<&str>) -> String {
        rbac().address(&ObjectName::parse(name).unwrap(), method)
    }

    #[test]
    fn addresses_of_the_broker_mbeans() {
        let d = "org.apache.activemq.artemis";
        assert_eq!(
            address(&format!("{d}:broker=\"0.0.0.0\""), Some("forceFailover")),
            "mops.broker.forceFailover"
        );
        assert_eq!(
            address(
                &format!(
                    "{d}:broker=\"0.0.0.0\",component=addresses,address=\"activemq.management\""
                ),
                Some("getMessageCount")
            ),
            "mops.address.activemq.management.getMessageCount"
        );
        assert_eq!(
            address(
                &format!(
                    "{d}:address=\"DLQ\",broker=\"0.0.0.0\",component=addresses,queue=\"DLQ\",routing-type=\"anycast\",subcomponent=queues"
                ),
                None
            ),
            "mops.queue.DLQ"
        );
        assert_eq!(
            address(
                &format!(
                    "{d}:address=\"a\",broker=\"0.0.0.0\",component=addresses,divert=\"x\",subcomponent=diverts"
                ),
                Some("getFilter")
            ),
            "mops.divert.x.getFilter"
        );
        assert_eq!(
            address(
                &format!("{d}:broker=\"0.0.0.0\",component=acceptors,name=\"artemis\""),
                Some("reload")
            ),
            "mops.acceptors.artemis.reload"
        );
        assert_eq!(
            address("java.lang:type=Memory", Some("gc")),
            "mops.java.lang.Memory.gc"
        );
    }
}
