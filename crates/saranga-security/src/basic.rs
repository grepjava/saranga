//! `ActiveMQBasicSecurityManager`: users and their roles kept by the broker
//! itself (in the bindings journal), with no JAAS configuration.

use std::sync::Arc;

use saranga_config::{CheckType, Role, mask};

use crate::jaas::{Principal, Subject};
use crate::manager::{ConnectionInfo, NoCache, SecurityManager, authorize_roles};

/// Where the basic security manager finds users: the broker's persisted
/// `USER_RECORD`s and `ROLE_RECORD`s.
pub trait UserDirectory: Send + Sync {
    /// The stored password (plain or `ENC(...)` hashed) of `user`, and its
    /// roles. `None` for an unknown user. A user without a role record has
    /// `None` roles: Artemis fails such a login.
    fn user(&self, user: &str) -> Option<(String, Option<Vec<String>>)>;
}

pub struct BasicSecurityManager {
    users: Arc<dyn UserDirectory>,
}

impl BasicSecurityManager {
    /// `bootstrapUser`, `bootstrapPassword`, `bootstrapRole`,
    /// `bootstrapUserFile` and `bootstrapRoleFile`.
    pub const BOOTSTRAP_PROPERTIES: [&str; 5] = [
        "bootstrapUser",
        "bootstrapPassword",
        "bootstrapRole",
        "bootstrapUserFile",
        "bootstrapRoleFile",
    ];

    pub fn new(users: Arc<dyn UserDirectory>) -> Self {
        Self { users }
    }
}

impl SecurityManager for BasicSecurityManager {
    /// `authenticate`: the user's stored password must match
    /// (`User.isValid`); the subject holds the user and its roles.
    fn authenticate(
        &self,
        user: Option<&str>,
        password: Option<&str>,
        _connection: &ConnectionInfo,
    ) -> Result<Option<Subject>, NoCache> {
        let (Some(user), Some(password)) = (user, password) else {
            return Ok(None);
        };
        let Some((stored, Some(roles))) = self.users.user(user) else {
            return Ok(None);
        };
        if !mask::password_matches(password, &stored, None) {
            return Ok(None);
        }
        let mut subject = Subject::default();
        subject.add(Principal::User(user.to_owned()));
        for role in roles {
            subject.add(Principal::Role(role));
        }
        Ok(Some(subject))
    }

    fn authorize(
        &self,
        subject: &Subject,
        roles: &[Role],
        check: CheckType,
        _address: &str,
    ) -> bool {
        authorize_roles(subject, roles, check)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct Users(HashMap<String, (String, Option<Vec<String>>)>);

    impl UserDirectory for Users {
        fn user(&self, user: &str) -> Option<(String, Option<Vec<String>>)> {
            self.0.get(user).cloned()
        }
    }

    #[test]
    fn logins() {
        let mut users = HashMap::new();
        users.insert(
            "a".to_owned(),
            ("pw".to_owned(), Some(vec!["amq".to_owned()])),
        );
        users.insert("norole".to_owned(), ("pw".to_owned(), None));
        let m = BasicSecurityManager::new(Arc::new(Users(users)));
        let conn = ConnectionInfo::default();
        let s = m
            .authenticate(Some("a"), Some("pw"), &conn)
            .unwrap()
            .unwrap();
        assert_eq!(s.user(), Some("a"));
        assert!(m.authorize(
            &s,
            &[Role::new("amq", [CheckType::Send])],
            CheckType::Send,
            "x"
        ));
        assert!(
            m.authenticate(Some("a"), Some("bad"), &conn)
                .unwrap()
                .is_none()
        );
        assert!(m.authenticate(Some("a"), None, &conn).unwrap().is_none());
        assert!(
            m.authenticate(Some("b"), Some("pw"), &conn)
                .unwrap()
                .is_none()
        );
        assert!(
            m.authenticate(Some("norole"), Some("pw"), &conn)
                .unwrap()
                .is_none()
        );
    }
}
