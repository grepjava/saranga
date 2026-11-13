//! Who may call what over the web: `management.xml`'s `<authorisation>`, as
//! `JMXAccessControlList` and `ArtemisMBeanServerGuard` apply it.

use std::collections::HashMap;

use crate::mbean::ObjectName;

/// `JMXAccessControlList.Access`: the roles for methods of matching MBeans.
#[derive(Debug, Clone, Default)]
struct Access {
    key: String,
    /// `keyPattern`: the key as a regular expression, `*` standing for
    /// `.*`, matched against a whole `name=value`.
    pattern: Option<Box<fancy_regex::Regex>>,
    catch_all: Vec<String>,
    methods: HashMap<String, Vec<String>>,
    /// Method prefixes, in the order they were added.
    prefixes: Vec<(String, Vec<String>)>,
}

impl Access {
    /// `Access(id, key)`: a key that is not a valid regular expression is
    /// refused, as `Pattern.compile` refuses it. The key is in Java's
    /// syntax (see [`crate::java_regex`]).
    fn new(key: &str) -> Result<Self, String> {
        let invalid = |e: String| format!("key {key:?} is not a valid pattern: {e}");
        let java = crate::java_regex::translate(&key.replace('*', ".*")).map_err(invalid)?;
        let pattern = fancy_regex::Regex::new(&format!(r"\A(?:{java})\z"))
            .map_err(|e| invalid(e.to_string()))?;
        Ok(Self {
            key: key.to_owned(),
            pattern: Some(Box::new(pattern)),
            ..Self::default()
        })
    }

    /// `getKeyPattern().matcher(key).matches()`.
    fn key_matches(&self, key: &str) -> bool {
        self.pattern
            .as_ref()
            .is_some_and(|p| p.is_match(key).unwrap_or(false))
    }

    fn add(&mut self, method: &str, roles: &[String]) {
        if method == "*" {
            self.catch_all.extend_from_slice(roles);
        } else if method.ends_with('*') {
            let prefix = method.replace('*', "");
            match self.prefixes.iter_mut().find(|(p, _)| *p == prefix) {
                Some((_, r)) => r.extend_from_slice(roles),
                None => self.prefixes.push((prefix, roles.to_vec())),
            }
        } else {
            self.methods
                .entry(method.to_owned())
                .or_default()
                .extend_from_slice(roles);
        }
    }

    /// `getMatchingRolesForMethod`.
    fn roles(&self, method: &str) -> &[String] {
        if let Some(r) = self.methods.get(method) {
            return r;
        }
        for (prefix, roles) in &self.prefixes {
            if method.starts_with(prefix.as_str()) {
                return roles;
            }
        }
        &self.catch_all
    }
}

/// Whether `management.xml` has an `<authorisation>`.
pub fn has_authorisation(xml: &str) -> bool {
    roxmltree::Document::parse(xml).is_ok_and(|doc| {
        doc.root_element()
            .children()
            .any(|c| c.is_element() && c.tag_name().name() == "authorisation")
    })
}

/// `JMXAccessControlList`.
#[derive(Debug, Clone)]
pub struct AccessList {
    default_access: Access,
    /// Per domain, the accesses in `keyComparator` order.
    domain_access: HashMap<String, Vec<Access>>,
    allow_list: HashMap<String, Vec<Access>>,
}

/// `normalizeKey`: quotes dropped from a key ending in one.
fn normalize(key: &str) -> String {
    if key.ends_with('"') {
        key.replace('"', "")
    } else {
        key.to_owned()
    }
}

/// `keyComparator`: keys without wildcards first, longer first, then by
/// text.
fn key_order(a: &str, b: &str) -> std::cmp::Ordering {
    let (aw, bw) = (a.contains('*'), b.contains('*'));
    if aw && !bw {
        std::cmp::Ordering::Greater
    } else if !aw && bw {
        std::cmp::Ordering::Less
    } else if a.len() == b.len() {
        a.cmp(b)
    } else {
        b.len().cmp(&a.len())
    }
}

fn accesses<'a, 'i>(n: roxmltree::Node<'a, 'i>) -> Vec<roxmltree::Node<'a, 'i>> {
    n.children()
        .filter(|c| c.is_element() && c.tag_name().name() == "access")
        .collect()
}

impl AccessList {
    fn empty() -> Self {
        Self {
            default_access: Access::default(),
            domain_access: HashMap::new(),
            allow_list: HashMap::new(),
        }
    }

    fn add_allow(&mut self, domain: &str, key: Option<&str>) -> Result<(), String> {
        let key = normalize(key.unwrap_or(""));
        let list = self.allow_list.entry(domain.to_owned()).or_default();
        if !list.iter().any(|a| a.key == key) {
            list.push(Access::new(&key)?);
            list.sort_by(|a, b| key_order(&a.key, &b.key));
        }
        Ok(())
    }

    fn add_role_access(
        &mut self,
        domain: &str,
        key: Option<&str>,
        method: &str,
        roles: &[String],
    ) -> Result<(), String> {
        let key = normalize(key.unwrap_or(""));
        let list = self.domain_access.entry(domain.to_owned()).or_default();
        if !list.iter().any(|a| a.key == key) {
            list.push(Access::new(&key)?);
            list.sort_by(|a, b| key_order(&a.key, &b.key));
        }
        if let Some(a) = list.iter_mut().find(|a| a.key == key) {
            a.add(method, roles);
        }
        Ok(())
    }

    /// `createDefaultList`: what applies without `management.xml`.
    pub fn default_list() -> Self {
        let mut l = Self::empty();
        let r = |roles: &[&str]| roles.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let fixed = "the default list's keys are valid patterns";
        l.add_allow("hawtio", Some("type=*")).expect(fixed);
        let d = "org.apache.activemq.artemis";
        for (method, roles) in [
            ("list*", &["view", "update", "amq"][..]),
            ("get*", &["view", "update", "amq"]),
            ("is*", &["view", "update", "amq"]),
            ("set*", &["update", "amq"]),
            ("*", &["amq"]),
        ] {
            l.add_role_access(d, None, method, &r(roles)).expect(fixed);
        }
        l.default_access
            .add("list*", &r(&["view", "update", "amq"]));
        l.default_access.add("get*", &r(&["view", "update", "amq"]));
        l.default_access.add("is*", &r(&["view", "update", "amq"]));
        l.default_access.add("set*", &r(&["update", "amq"]));
        l.default_access.add("*", &r(&["amq"]));
        l
    }

    /// `management.xml`'s `<authorisation>` (`ManagementContextDTO`), or
    /// the default list without one.
    pub fn parse(xml: &str) -> Result<Self, String> {
        Self::parse_with(xml, &|v| v.to_owned())
    }

    /// [`parse`](Self::parse), `filter` giving the attributes' values, as
    /// `XmlUtil.decode`'s reader does with `${...}`.
    pub fn parse_with(xml: &str, filter: &dyn Fn(&str) -> String) -> Result<Self, String> {
        let doc = roxmltree::Document::parse(xml).map_err(|e| e.to_string())?;
        let attribute = |n: roxmltree::Node, a: &str| n.attribute(a).map(filter);
        let Some(auth) = doc
            .root_element()
            .children()
            .find(|c| c.is_element() && c.tag_name().name() == "authorisation")
        else {
            return Ok(Self::default_list());
        };
        let mut l = Self::empty();
        let roles = |n: roxmltree::Node| -> Vec<String> {
            attribute(n, "roles")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
                .collect()
        };
        for section in auth.children().filter(|c| c.is_element()) {
            match section.tag_name().name() {
                "allowlist" | "whitelist" => {
                    for e in section
                        .children()
                        .filter(|c| c.is_element() && c.tag_name().name() == "entry")
                    {
                        l.add_allow(
                            &attribute(e, "domain").unwrap_or_default(),
                            attribute(e, "key").as_deref(),
                        )?;
                    }
                }
                "default-access" => {
                    for a in accesses(section) {
                        l.default_access
                            .add(&attribute(a, "method").unwrap_or_default(), &roles(a));
                    }
                }
                "role-access" => {
                    for m in section
                        .children()
                        .filter(|c| c.is_element() && c.tag_name().name() == "match")
                    {
                        let domain = attribute(m, "domain").unwrap_or_default();
                        let key = attribute(m, "key");
                        for a in accesses(m) {
                            l.add_role_access(
                                &domain,
                                key.as_deref(),
                                &attribute(a, "method").unwrap_or_default(),
                                &roles(a),
                            )?;
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(l)
    }

    /// `getRolesForObject`.
    pub fn roles(&self, name: &ObjectName, method: &str) -> &[String] {
        if let Some(list) = self.domain_access.get(&name.domain) {
            for (k, v) in &name.props {
                let key = normalize(&format!("{k}={v}"));
                if let Some(a) = list
                    .iter()
                    .find(|a| !a.key.is_empty() && a.key_matches(&key))
                {
                    return a.roles(method);
                }
            }
            if let Some(a) = list.iter().find(|a| a.key.is_empty()) {
                return a.roles(method);
            }
        }
        self.default_access.roles(method)
    }

    /// `isInAllowList`.
    pub fn allowed(&self, name: &ObjectName) -> bool {
        let list = self
            .allow_list
            .get(&name.domain)
            .or_else(|| self.allow_list.get("*"));
        let Some(list) = list else {
            return false;
        };
        if list.iter().any(|a| a.key.is_empty()) {
            return true;
        }
        name.props.iter().any(|(k, v)| {
            let key = normalize(&format!("{k}={v}"));
            list.iter().any(|a| a.key_matches(&key))
        })
    }

    /// `canInvoke` / `handleInvoke`: may a user with these roles call the
    /// method (`get<Attribute>` for a read)?
    pub fn can_invoke(&self, user_roles: &[String], name: &ObjectName, method: &str) -> bool {
        if self.allowed(name) {
            return true;
        }
        let method = method.split('(').next().unwrap_or(method);
        self.roles(name, method)
            .iter()
            .any(|r| user_roles.iter().any(|u| u == r))
    }
}

#[cfg(test)]
mod tests {
    use super::AccessList;
    use crate::mbean::ObjectName;

    const MANAGEMENT_XML: &str = r#"<management-context xmlns="http://activemq.apache.org/schema">
   <authorisation>
      <allowlist><entry domain="hawtio"/></allowlist>
      <default-access>
         <access method="list*" roles="amq"/>
         <access method="get*" roles="amq"/>
      </default-access>
      <role-access>
         <match domain="org.apache.activemq.artemis">
            <access method="list*" roles="amq"/>
            <access method="get*" roles="amq"/>
            <access method="*" roles="amq"/>
         </match>
         <match domain="org.apache.activemq.artemis" key="subcomponent=queues">
            <access method="list*" roles="view,update,amq"/>
            <access method="get*" roles="view,update,amq"/>
            <access method="set*" roles="update,amq"/>
            <access method="*" roles="amq"/>
         </match>
      </role-access>
   </authorisation>
</management-context>"#;

    #[test]
    fn artemis_create_management_xml() {
        let l = AccessList::parse(MANAGEMENT_XML).unwrap();
        let roles = |r: &[&str]| r.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let broker = ObjectName::parse("org.apache.activemq.artemis:broker=\"b\"").unwrap();
        let queue = ObjectName::parse(
            "org.apache.activemq.artemis:broker=\"b\",component=addresses,address=\"a\",subcomponent=queues,routing-type=\"anycast\",queue=\"q\"",
        )
        .unwrap();
        assert!(l.can_invoke(&roles(&["amq"]), &broker, "createQueue"));
        assert!(!l.can_invoke(&roles(&["view"]), &broker, "getVersion"));
        assert!(l.can_invoke(&roles(&["view"]), &queue, "getMessageCount"));
        assert!(!l.can_invoke(&roles(&["view"]), &queue, "removeAllMessages"));
        assert!(l.can_invoke(&roles(&["update"]), &queue, "setX"));
        let hawtio = ObjectName::parse("hawtio:type=x").unwrap();
        assert!(l.can_invoke(&[], &hawtio, "anything"));
    }

    /// A key is a regular expression (`*` standing for `.*`), as
    /// `JMXAccessControlList` compiles it: a restrictive rule written with a
    /// character class applies, rather than letting the broader rule grant
    /// the call.
    #[test]
    fn a_key_is_a_regular_expression() {
        let xml = r#"<management-context xmlns="http://activemq.apache.org/schema">
   <authorisation>
      <role-access>
         <match domain="org.apache.activemq.artemis">
            <access method="*" roles="view,amq"/>
         </match>
         <match domain="org.apache.activemq.artemis" key="queue=secret[0-9]">
            <access method="get*" roles="view,amq"/>
            <access method="*" roles="amq"/>
         </match>
      </role-access>
   </authorisation>
</management-context>"#;
        let l = AccessList::parse(xml).unwrap();
        let view = ["view".to_owned()];
        let queue = |name: &str| {
            ObjectName::parse(&format!(
                "org.apache.activemq.artemis:broker=\"b\",component=addresses,address=\"a\",subcomponent=queues,routing-type=\"anycast\",queue=\"{name}\""
            ))
            .unwrap()
        };
        assert!(l.can_invoke(&view, &queue("secret1"), "getMessageCount"));
        assert!(!l.can_invoke(&view, &queue("secret1"), "removeAllMessages"));
        // Not matched by the class: the broader rule applies.
        assert!(l.can_invoke(&view, &queue("secretX"), "removeAllMessages"));
        // A key that is not a valid pattern is refused, as Pattern.compile
        // refuses it.
        assert!(AccessList::parse(&xml.replace("secret[0-9]", "secret[0-9")).is_err());
    }

    /// A key in Java's own syntax restricts what it names, and one this
    /// broker cannot give Java's meaning is refused rather than skipped.
    #[test]
    fn a_key_is_read_as_java_reads_it() {
        let xml = |key: &str| {
            format!(
                r#"<management-context xmlns="http://activemq.apache.org/schema">
   <authorisation>
      <role-access>
         <match domain="org.apache.activemq.artemis">
            <access method="*" roles="view,amq"/>
         </match>
         <match domain="org.apache.activemq.artemis" key="{key}">
            <access method="get*" roles="view,amq"/>
            <access method="*" roles="amq"/>
         </match>
      </role-access>
   </authorisation>
</management-context>"#
            )
        };
        let queue = |name: &str| {
            ObjectName::parse(&format!(
                "org.apache.activemq.artemis:broker=\"b\",component=addresses,address=\"a\",subcomponent=queues,routing-type=\"anycast\",queue=\"{name}\""
            ))
            .unwrap()
        };
        let view = ["view".to_owned()];
        let l = AccessList::parse(&xml(r"queue=\Qsecret.1\E")).unwrap();
        assert!(l.can_invoke(&view, &queue("secret.1"), "getMessageCount"));
        assert!(!l.can_invoke(&view, &queue("secret.1"), "removeAllMessages"));
        assert!(l.can_invoke(&view, &queue("secretX1"), "removeAllMessages"));
        // Java's `\h` is horizontal whitespace, not a hexadecimal digit.
        let l = AccessList::parse(&xml(r"queue=secret\h1")).unwrap();
        assert!(!l.can_invoke(&view, &queue("secret 1"), "removeAllMessages"));
        assert!(l.can_invoke(&view, &queue("secreta1"), "removeAllMessages"));
        assert!(AccessList::parse(&xml(r"queue=\p{InBasicLatin}")).is_err());
    }
}

#[cfg(test)]
mod template_tests {
    use super::AccessList;
    use crate::mbean::ObjectName;

    /// The management.xml `artemis create` writes: its queue rule is an
    /// example in a comment, so `view` reads nothing until it is enabled.
    #[test]
    fn created_instance_grants_amq_only() {
        let xml = include_str!("../../saranga-cli/templates/etc/management.xml")
            .replace("${role}", "amq");
        let l = AccessList::parse(&xml).unwrap();
        let queue = ObjectName::parse(
            "org.apache.activemq.artemis:broker=\"0.0.0.0\",component=addresses,address=\"a\",subcomponent=queues,routing-type=\"anycast\",queue=\"q\"",
        )
        .unwrap();
        assert!(!l.can_invoke(&["view".to_owned()], &queue, "getMessageCount"));
        assert!(l.can_invoke(&["amq".to_owned()], &queue, "getMessageCount"));
        assert!(l.can_invoke(&["amq".to_owned()], &queue, "removeAllMessages"));
        let uncommented = xml.replace(
            "<!--
         <match domain=\"org.apache.activemq.artemis\" key=\"subcomponent=queues\">",
            "<match domain=\"org.apache.activemq.artemis\" key=\"subcomponent=queues\">",
        );
        let uncommented = uncommented.replace(
            "</match>
         -->",
            "</match>",
        );
        let l = AccessList::parse(&uncommented).unwrap();
        assert!(l.can_invoke(&["view".to_owned()], &queue, "getMessageCount"));
        assert!(!l.can_invoke(&["view".to_owned()], &queue, "removeAllMessages"));
    }
}
