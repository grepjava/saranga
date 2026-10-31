//! `SCRAMPropertiesLoginModule`: the SCRAM data of a user from a users
//! properties file, for the SCRAM SASL mechanisms to challenge the client
//! with, and the user's roles from a roles file.
//!
//! A user's entry is `user|SHA256` (or `user|SHA512`) or just `user`,
//! holding `ENC(salt:iterations:serverKey:storedKey)` or a password in
//! clear, whose data is made at each login. A user that does not exist
//! gets data made from a random password, so that a client cannot tell it
//! from a wrong password.

use std::collections::HashMap;
use std::sync::Arc;

use saranga_config::mask;

use super::properties_module::{ROLE_FILE_PROP_NAME, USER_FILE_PROP_NAME};
use super::{Callbacks, Credential, LoadedProperties, LoginError, LoginModule, ModuleEntry};
use super::{ModuleFactory, Principal, Subject};
use crate::scram::{self, Scram, UserData};

const SEPARATOR_MECHANISM: &str = "|";
const SEPARATOR_PARAMETER: char = ':';
/// The iterations of data made at login.
const ITERATIONS: i32 = 4096;

pub(crate) struct ScramPropertiesLoginModule {
    users: Arc<LoadedProperties>,
    roles: Arc<LoadedProperties>,
    user: Option<String>,
    user_data: Option<UserData>,
}

impl ScramPropertiesLoginModule {
    pub(crate) fn new(factory: &ModuleFactory, entry: &ModuleEntry) -> Self {
        let reload = entry.bool_option("reload");
        let file = |prop: &str, fallback: &str| {
            factory.file(entry, entry.option(prop).unwrap_or(fallback))
        };
        Self {
            users: factory.properties(&file(USER_FILE_PROP_NAME, "user"), reload),
            roles: factory.properties(&file(ROLE_FILE_PROP_NAME, "role"), reload),
            user: None,
            user_data: None,
        }
    }
}

/// `generateUserData`: the data of a password in clear, or of a random one.
fn generate_user_data(scram: Scram, password: Option<&str>) -> Result<UserData, LoginError> {
    let random;
    let password = match password {
        Some(p) => p,
        None => {
            random = String::from_utf8_lossy(&scram::random_bytes(256)).into_owned();
            &random
        }
    };
    let salt = scram::random_bytes(32);
    scram::new_password(scram, password, &salt, ITERATIONS)
        .map(|p| p.user_data())
        .map_err(|_| LoginError::new(""))
}

/// The `ENC(...)` form `main` writes: salt, iterations, server key and
/// stored key.
fn masked_user_data(masked: &str) -> Result<UserData, LoginError> {
    let bare = mask::unwrap(masked);
    let parts: Vec<&str> = bare.split(SEPARATOR_PARAMETER).collect();
    let bad = || LoginError::new(format!("invalid SCRAM data: {masked}"));
    if parts.len() < 4 {
        return Err(bad());
    }
    Ok(UserData {
        salt: parts[0].to_owned(),
        iterations: parts[1].parse().map_err(|_| bad())?,
        server_key: parts[2].to_owned(),
        stored_key: parts[3].to_owned(),
    })
}

impl LoginModule for ScramPropertiesLoginModule {
    fn login(&mut self, callbacks: &Callbacks<'_>) -> Result<bool, LoginError> {
        self.user = callbacks.user.map(str::to_owned);
        // SCRAMMechanismCallback: only SCRAM's handler has it.
        let Some(scram) = callbacks.scram else {
            return Err(LoginError::new(""));
        };
        self.user_data = Some(match callbacks.user {
            None => generate_user_data(scram, None)?,
            Some(user) => {
                let key = format!("{user}{SEPARATOR_MECHANISM}{}", scram.constant());
                let password = self.users.get(&key).or_else(|| self.users.get(user));
                match password {
                    Some(p) if mask::is_enc_masked(p) => masked_user_data(p)?,
                    p => generate_user_data(scram, p)?,
                }
            }
        });
        Ok(true)
    }

    fn commit(&mut self, subject: &mut Subject) -> Result<bool, LoginError> {
        let Some(data) = self.user_data.take() else {
            return Err(LoginError::new(""));
        };
        subject.add_credential(Credential::Scram(data));
        // `new UserPrincipal(null)` throws.
        let Some(user) = self.user.take() else {
            return Err(LoginError::new("name cannot be null"));
        };
        let mut principals = vec![Principal::User(user)];
        let mut users: Vec<String> = subject.users().map(str::to_owned).collect();
        users.push(principals[0].name().to_owned());
        let by_user: HashMap<&str, Vec<&str>> = self.roles.inverted_values();
        for user in &users {
            if let Some(roles) = by_user.get(user.as_str()) {
                for role in roles {
                    let role = Principal::Role((*role).to_owned());
                    if !principals.contains(&role) {
                        principals.push(role);
                    }
                }
            }
        }
        for p in principals {
            subject.add(p);
        }
        Ok(true)
    }
}
