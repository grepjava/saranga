//! The users and roles `ActiveMQBasicSecurityManager` keeps in the bindings
//! journal (`USER_RECORD`, `ROLE_RECORD`; `mapPersistedUsers` and
//! `mapPersistedRoles` in `AbstractJournalStorageManager`).
//!
//! Without persistence there is no store at all, as with Artemis's
//! `NullStorageManager`: no user can log in.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use saranga_storage::RecordCodec;
use saranga_storage::codec::{PersistedRole, PersistedUser};

use crate::storage::{Storage, StoreResult, ids};

#[derive(Default)]
struct State {
    storage: Option<Arc<Storage>>,
    /// Usernames to their record ID and stored password.
    users: HashMap<String, (i64, String)>,
    /// Usernames to their record ID and roles.
    roles: HashMap<String, (i64, Vec<String>)>,
}

/// Shared by the broker, which loads and stores it, and the basic security
/// manager, which reads it.
#[derive(Default)]
pub struct Users {
    state: Mutex<State>,
}

impl Users {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A `USER_RECORD` or `ROLE_RECORD` found loading the bindings journal;
    /// a later record for the same user replaces the earlier one.
    pub(crate) fn load(&self, id: i64, user_type: u8, data: &[u8]) -> Result<(), String> {
        let mut state = self.lock();
        match user_type {
            ids::USER_RECORD => {
                let u = PersistedUser::decode(data).map_err(|e| e.to_string())?;
                state
                    .users
                    .insert(u.username.to_string(), (id, u.password.to_string()));
            }
            ids::ROLE_RECORD => {
                let r = PersistedRole::decode(data).map_err(|e| e.to_string())?;
                let roles = r.roles.iter().map(ToString::to_string).collect();
                state.roles.insert(r.username.to_string(), (id, roles));
            }
            _ => {}
        }
        Ok(())
    }

    /// The broker's storage, once the journals are loaded.
    pub(crate) fn attach(&self, storage: Arc<Storage>) {
        self.lock().storage = Some(storage);
    }

    /// `ActiveMQBasicSecurityManager.addOrUpdateUser`, as for the bootstrap
    /// credentials: a new user is stored with its password and roles; an
    /// existing one gets the new password and, if any are given, roles.
    /// Does nothing without persistence.
    pub fn add_or_update(&self, user: &str, password: &str, roles: &[String]) -> StoreResult<()> {
        let mut state = self.lock();
        let Some(storage) = state.storage.clone() else {
            return Ok(());
        };
        let exists = state.users.contains_key(user);
        // updateUser: deleteUser then storeUser.
        if let Some((old, _)) = state.users.remove(user) {
            storage.delete_binding(old)?;
        }
        let id = storage.generate_id();
        let record = PersistedUser {
            username: user.into(),
            password: password.into(),
        };
        storage.add_binding(id, ids::USER_RECORD, record.encode())?;
        state
            .users
            .insert(user.to_owned(), (id, password.to_owned()));
        if !exists || !roles.is_empty() {
            if let Some((old, _)) = state.roles.remove(user) {
                storage.delete_binding(old)?;
            }
            let id = storage.generate_id();
            let record = PersistedRole {
                username: user.into(),
                roles: roles.iter().map(|r| r.as_str().into()).collect(),
            };
            storage.add_binding(id, ids::ROLE_RECORD, record.encode())?;
            state.roles.insert(user.to_owned(), (id, roles.to_vec()));
        }
        Ok(())
    }
}

impl Users {
    fn store_user(
        state: &mut State,
        storage: &Storage,
        user: &str,
        password: &str,
    ) -> StoreResult<()> {
        let id = storage.generate_id();
        let record = PersistedUser {
            username: user.into(),
            password: password.into(),
        };
        storage.add_binding(id, ids::USER_RECORD, record.encode())?;
        state
            .users
            .insert(user.to_owned(), (id, password.to_owned()));
        Ok(())
    }

    fn store_roles(
        state: &mut State,
        storage: &Storage,
        user: &str,
        roles: &[String],
    ) -> StoreResult<()> {
        let id = storage.generate_id();
        let record = PersistedRole {
            username: user.into(),
            roles: roles.iter().map(|r| r.as_str().into()).collect(),
        };
        storage.add_binding(id, ids::ROLE_RECORD, record.encode())?;
        state.roles.insert(user.to_owned(), (id, roles.to_vec()));
        Ok(())
    }

    /// `ActiveMQBasicSecurityManager.addNewUser`.
    pub fn add_new(&self, user: &str, password: &str, roles: &[String]) -> Result<(), String> {
        let mut state = self.lock();
        let Some(storage) = state.storage.clone() else {
            return Ok(());
        };
        if state.users.contains_key(user) {
            return Err(format!("AMQ229223: User {user} already exists"));
        }
        Self::store_user(&mut state, &storage, user, password).map_err(|e| e.to_string())?;
        Self::store_roles(&mut state, &storage, user, roles).map_err(|e| e.to_string())
    }

    /// `ActiveMQBasicSecurityManager.removeUser`.
    pub fn remove(&self, user: &str) -> Result<(), String> {
        let mut state = self.lock();
        let storage = state.storage.clone();
        let Some((id, _)) = state.users.remove(user) else {
            return Err(format!("AMQ229224: User {user} does not exist"));
        };
        if let Some(s) = &storage {
            s.delete_binding(id).map_err(|e| e.to_string())?;
            if let Some((id, _)) = state.roles.remove(user) {
                s.delete_binding(id).map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }

    /// `ActiveMQBasicSecurityManager.updateUser`: the password if given,
    /// the roles if any are given.
    pub fn update(
        &self,
        user: &str,
        password: Option<&str>,
        roles: Option<&[String]>,
    ) -> Result<(), String> {
        let mut state = self.lock();
        let Some(storage) = state.storage.clone() else {
            return Err(format!("AMQ229224: User {user} does not exist"));
        };
        if !state.users.contains_key(user) {
            return Err(format!("AMQ229224: User {user} does not exist"));
        }
        let io = |e: saranga_journal::JournalError| e.to_string();
        if let Some(password) = password {
            if let Some((old, _)) = state.users.remove(user) {
                storage.delete_binding(old).map_err(io)?;
            }
            Self::store_user(&mut state, &storage, user, password).map_err(io)?;
        }
        if let Some(roles) = roles.filter(|r| !r.is_empty()) {
            if let Some((old, _)) = state.roles.remove(user) {
                storage.delete_binding(old).map_err(io)?;
            }
            Self::store_roles(&mut state, &storage, user, roles).map_err(io)?;
        }
        Ok(())
    }

    /// `ActiveMQBasicSecurityManager.listUser`: one user and its roles, or
    /// every user when none is named.
    pub fn list(&self, user: Option<&str>) -> Result<Vec<(String, Vec<String>)>, String> {
        let state = self.lock();
        let roles = |u: &str| {
            let mut r = state
                .roles
                .get(u)
                .map(|(_, r)| r.clone())
                .unwrap_or_default();
            r.sort();
            r.dedup();
            r
        };
        match user.filter(|u| !u.is_empty()) {
            Some(u) => {
                if !state.users.contains_key(u) {
                    return Err(format!("AMQ229224: User {u} does not exist"));
                }
                Ok(vec![(u.to_owned(), roles(u))])
            }
            None => {
                let mut all: Vec<_> = state.users.keys().map(|u| (u.clone(), roles(u))).collect();
                all.sort();
                Ok(all)
            }
        }
    }
}

impl saranga_security::UserDirectory for Users {
    fn user(&self, user: &str) -> Option<(String, Option<Vec<String>>)> {
        let state = self.lock();
        state.storage.as_ref()?;
        let (_, password) = state.users.get(user)?;
        let roles = state.roles.get(user).map(|(_, r)| r.clone());
        Some((password.clone(), roles))
    }
}
