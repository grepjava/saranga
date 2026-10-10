//! `PropertiesLoginModule`: users and passwords from one properties file,
//! roles from another (`role = user1,user2`).

use std::collections::HashMap;
use std::sync::Arc;

use saranga_config::mask::{self, Codec};

use super::{Callbacks, LoadedProperties, LoginError, LoginModule, ModuleEntry, ModuleFactory};
use super::{Principal, Subject};

pub const USER_FILE_PROP_NAME: &str = "org.apache.activemq.jaas.properties.user";
pub const ROLE_FILE_PROP_NAME: &str = "org.apache.activemq.jaas.properties.role";
pub const PASSWORD_CODEC_PROP_NAME: &str = "org.apache.activemq.jaas.properties.password.codec";

pub(crate) struct PropertiesLoginModule {
    users: Arc<LoadedProperties>,
    roles: Arc<LoadedProperties>,
    /// The `password.codec` option, made into a codec on first use.
    password_codec: Option<String>,
    user: Option<String>,
    login_succeeded: bool,
}

impl PropertiesLoginModule {
    pub(crate) fn new(factory: &ModuleFactory, entry: &ModuleEntry) -> Self {
        let reload = entry.bool_option("reload");
        let file = |prop: &str, fallback: &str| {
            factory.file(entry, entry.option(prop).unwrap_or(fallback))
        };
        Self {
            users: factory.properties(&file(USER_FILE_PROP_NAME, "user"), reload),
            roles: factory.properties(&file(ROLE_FILE_PROP_NAME, "role"), reload),
            password_codec: entry.option(PASSWORD_CODEC_PROP_NAME).map(str::to_owned),
            user: None,
            login_succeeded: false,
        }
    }
}

impl LoginModule for PropertiesLoginModule {
    fn login(&mut self, callbacks: &Callbacks<'_>) -> Result<bool, LoginError> {
        let Some(user) = callbacks.user else {
            return Err(LoginError::new("User is null"));
        };
        let Some(stored) = self.users.get(user) else {
            return Err(LoginError::new(format!("User does not exist: {user}")));
        };
        let codec = match &self.password_codec {
            Some(desc) if mask::is_enc_masked(stored) => Some(
                Codec::from_desc(Some(desc))
                    .map_err(|_| LoginError::new("Failed to get hash processor"))?,
            ),
            _ => None,
        };
        let password = callbacks.password.unwrap_or_default();
        if !mask::password_matches(password, stored, codec.as_ref()) {
            return Err(LoginError::new(format!(
                "Password does not match for user: {user}"
            )));
        }
        self.user = Some(user.to_owned());
        self.login_succeeded = true;
        Ok(true)
    }

    fn commit(&mut self, subject: &mut Subject) -> Result<bool, LoginError> {
        let result = self.login_succeeded;
        if result && let Some(user) = &self.user {
            subject.add(Principal::User(user.clone()));
        }
        // Roles for every user principal, including those earlier modules
        // added.
        let by_user: HashMap<&str, Vec<&str>> = self.roles.inverted_values();
        let users: Vec<String> = subject.users().map(str::to_owned).collect();
        for user in users {
            if let Some(roles) = by_user.get(user.as_str()) {
                for role in roles {
                    subject.add(Principal::Role((*role).to_owned()));
                }
            }
        }
        self.user = None;
        self.login_succeeded = false;
        Ok(result)
    }
}
