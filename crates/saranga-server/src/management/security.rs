//! `ActiveMQServerControl`'s users and security settings: the users of the
//! basic security manager or of the JAAS `PropertiesLoginModule`'s files,
//! security settings added and removed at runtime and journaled, and the
//! security caches.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use saranga_config::properties::Document;
use saranga_config::{CheckType, Role, SecuritySetting};
use saranga_storage::RecordCodec;
use saranga_storage::codec::PersistedSecuritySetting;

use super::args::{Args, Call, Ty::*};
use super::json::Json;
use super::{Management, Outcome};
use crate::Broker;
use crate::storage::ids;

/// The permissions in the order of `CheckType.asObjectArray` and
/// `SecurityFormatter`, with their JSON names.
const PERMISSIONS: [CheckType; 12] = [
    CheckType::Send,
    CheckType::Consume,
    CheckType::CreateDurableQueue,
    CheckType::DeleteDurableQueue,
    CheckType::CreateNonDurableQueue,
    CheckType::DeleteNonDurableQueue,
    CheckType::Manage,
    CheckType::Browse,
    CheckType::CreateAddress,
    CheckType::DeleteAddress,
    CheckType::View,
    CheckType::Edit,
];

/// `SecurityFormatter.toListOfRoles`: split on commas and blanks.
fn role_list(text: &str) -> Vec<String> {
    text.split([',', ' '])
        .filter(|r| !r.is_empty())
        .map(str::to_owned)
        .collect()
}

/// `SecurityFormatter.fromJSON`: a role for every name listed under any
/// permission, granted those that list it.
fn from_json(o: &Json) -> SecuritySetting {
    let lists: Vec<Vec<String>> = PERMISSIONS
        .iter()
        .map(|c| match o.get(c.permission_name()) {
            Some(Json::Array(items)) => items
                .iter()
                .filter_map(Json::as_str)
                .map(str::to_owned)
                .collect(),
            _ => Vec::new(),
        })
        .collect();
    let mut names: Vec<&String> = lists.iter().flatten().collect();
    names.sort();
    names.dedup();
    SecuritySetting {
        roles: names
            .into_iter()
            .map(|n| {
                let grants = PERMISSIONS
                    .iter()
                    .zip(&lists)
                    .filter(|(_, l)| l.contains(n))
                    .map(|(c, _)| *c);
                Role::new(n.clone(), grants)
            })
            .collect(),
    }
}

/// `JsonUtil.arrayToString`: the names joined with `", "`.
fn joined(o: &Json, check: CheckType) -> String {
    match o.get(check.permission_name()) {
        Some(Json::Array(items)) => items
            .iter()
            .filter_map(Json::as_str)
            .collect::<Vec<_>>()
            .join(", "),
        _ => String::new(),
    }
}

/// `new PersistedSecuritySetting(addressMatch, json)`.
fn record(address_match: &str, o: &Json) -> PersistedSecuritySetting {
    let s = |c| Some(joined(o, c).as_str().into());
    PersistedSecuritySetting {
        address_match: address_match.into(),
        send_roles: s(CheckType::Send),
        consume_roles: s(CheckType::Consume),
        create_durable_queue_roles: s(CheckType::CreateDurableQueue),
        delete_durable_queue_roles: s(CheckType::DeleteDurableQueue),
        create_non_durable_queue_roles: s(CheckType::CreateNonDurableQueue),
        delete_non_durable_queue_roles: s(CheckType::DeleteNonDurableQueue),
        manage_roles: s(CheckType::Manage),
        browse_roles: s(CheckType::Browse),
        create_address_roles: s(CheckType::CreateAddress),
        delete_address_roles: s(CheckType::DeleteAddress),
        view_roles: Some(s(CheckType::View)),
        edit_roles: Some(s(CheckType::Edit)),
    }
}

/// `recoverStoredSecuritySettings` with `SecurityFormatter.createSecurity`:
/// the roles a journaled setting gives. As in Artemis, the view and edit
/// roles are not restored, and a role is granted manage, create-address
/// and delete-address when its name appears anywhere in those lists'
/// text (`String.contains`).
pub(crate) fn recovered(p: &PersistedSecuritySetting) -> SecuritySetting {
    let text = |v: &Option<saranga_buffer::SimpleString>| {
        v.as_ref().map(|s| s.to_string_lossy()).unwrap_or_default()
    };
    let lists = [
        (
            CheckType::CreateDurableQueue,
            text(&p.create_durable_queue_roles),
        ),
        (
            CheckType::DeleteDurableQueue,
            text(&p.delete_durable_queue_roles),
        ),
        (
            CheckType::CreateNonDurableQueue,
            text(&p.create_non_durable_queue_roles),
        ),
        (
            CheckType::DeleteNonDurableQueue,
            text(&p.delete_non_durable_queue_roles),
        ),
        (CheckType::Send, text(&p.send_roles)),
        (CheckType::Consume, text(&p.consume_roles)),
        (CheckType::Manage, text(&p.manage_roles)),
        (CheckType::Browse, text(&p.browse_roles)),
        (CheckType::CreateAddress, text(&p.create_address_roles)),
        (CheckType::DeleteAddress, text(&p.delete_address_roles)),
    ];
    let mut names: Vec<String> = lists.iter().flat_map(|(_, t)| role_list(t)).collect();
    names.sort();
    names.dedup();
    SecuritySetting {
        roles: names
            .into_iter()
            .map(|n| {
                let grants = lists.iter().filter(|(c, t)| match c {
                    CheckType::Manage | CheckType::CreateAddress | CheckType::DeleteAddress => {
                        t.contains(n.as_str())
                    }
                    _ => role_list(t).contains(&n),
                });
                Role::new(n.clone(), grants.map(|(c, _)| *c))
            })
            .collect(),
    }
}

/// `Role.toJson`.
fn role_json(r: &Role) -> Json {
    let mut o = Json::object().with("name", r.name.clone());
    for c in PERMISSIONS {
        o.put(c.permission_name(), r.allows(c));
    }
    o
}

/// `addSecuritySettings(addressMatch, json)`.
fn add_security_settings(broker: &Broker, address_match: &str, text: &str) -> Outcome {
    let o = Json::parse(text)?;
    if !matches!(o, Json::Object(_)) {
        return Err("Not a JSON object".to_owned());
    }
    if let Some(store) = broker.security_store() {
        store.add_match(address_match, from_json(&o))?;
    }
    if let Some(storage) = broker.storage() {
        let mut records = broker.security_records();
        let old = records.get(address_match).copied();
        let id = storage
            .store_configuration(
                ids::SECURITY_SETTING_RECORD,
                record(address_match, &o).encode(),
                old,
            )
            .map_err(|e| e.to_string())?;
        records.insert(address_match.to_owned(), id);
    }
    Ok(Json::Null)
}

/// `SecurityFormatter.toJSON` of comma-separated role lists, in
/// `PERMISSIONS` order; missing lists are empty.
fn lists_json(lists: &[Option<String>]) -> Json {
    let mut o = Json::object();
    for (i, c) in PERMISSIONS.iter().enumerate() {
        let names = lists
            .get(i)
            .cloned()
            .flatten()
            .map(|l| role_list(&l))
            .unwrap_or_default();
        o.put(c.permission_name(), Json::from(names));
    }
    o
}

/// Where users are kept.
enum UserStore {
    /// `ActiveMQBasicSecurityManager`: in the bindings journal.
    Basic(Arc<crate::users::Users>),
    /// `PropertiesLoginModuleConfigurator`: the login module's users and
    /// roles files.
    Files { users: PathBuf, roles: PathBuf },
}

const PROPERTIES_LOGIN_MODULE: &str =
    "org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule";

/// The license header Artemis gives a users or roles file it writes from
/// empty.
const LICENSE_HEADER: &str =
    "## ---------------------------------------------------------------------------
## Licensed to the Apache Software Foundation (ASF) under one or more
## contributor license agreements.  See the NOTICE file distributed with
## this work for additional information regarding copyright ownership.
## The ASF licenses this file to You under the Apache License, Version 2.0
## (the \"License\"); you may not use this file except in compliance with
## the License.  You may obtain a copy of the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an \"AS IS\" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
## ---------------------------------------------------------------------------";

fn user_store(m: &Management, broker: &Broker) -> Result<UserStore, String> {
    if let Some(users) = broker.users() {
        return Ok(UserStore::Basic(users.clone()));
    }
    let etc: PathBuf = m
        .config
        .file_path
        .as_deref()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or("AMQ229218: Failed to locate broker configuration URL")?;
    let jaas = broker.security_store().and_then(|s| s.manager().as_jaas());
    let parsed;
    let (config, domain) = match jaas {
        Some(j) => (j.config(), j.domain()),
        None => {
            let text = std::fs::read_to_string(etc.join("login.config"))
                .map_err(|_| "AMQ229219: Failed to load security configuration".to_owned())?;
            parsed = saranga_security::LoginConfig::parse(&text, |_| None)
                .map_err(|_| "AMQ229219: Failed to load security configuration".to_owned())?;
            (
                &parsed,
                saranga_security::JaasSecurityManager::DEFAULT_DOMAIN,
            )
        }
    };
    let domain = if domain.is_empty() {
        "activemq"
    } else {
        domain
    };
    let entries = config
        .entry(domain)
        .filter(|e| !e.is_empty())
        .ok_or("AMQ229219: Failed to load security configuration")?;
    let entry = entries
        .iter()
        .find(|e| e.class_name == PROPERTIES_LOGIN_MODULE)
        .ok_or_else(|| {
            format!("AMQ229222: Failed to find login module entry {domain} from JAAS configuration")
        })?;
    let file = |prop: &str| entry.option(prop).unwrap_or("null").to_owned();
    let (user_name, role_name) = (
        file("org.apache.activemq.jaas.properties.user"),
        file("org.apache.activemq.jaas.properties.role"),
    );
    let (users, roles) = (etc.join(&user_name), etc.join(&role_name));
    // brokerEtc + name, without a separator, as Artemis prints it.
    let etc_text = etc.display().to_string();
    if !users.exists() {
        return Err(format!(
            "AMQ229220: Failed to load user file: {etc_text}{user_name}"
        ));
    }
    if !roles.exists() {
        return Err(format!(
            "AMQ229221: Failed to load role file: {etc_text}{role_name}"
        ));
    }
    Ok(UserStore::Files { users, roles })
}

/// The users and roles files, read for editing; an empty file without a
/// header gets the license header.
fn read_files(users: &Path, roles: &Path) -> Result<(Document, Document), String> {
    let read = |p: &Path| -> Result<Document, String> {
        let bytes = std::fs::read(p).map_err(|e| format!("{}: {e}", p.display()))?;
        let mut d = Document::parse(&bytes);
        if d.is_empty() && !bytes.starts_with(b"#") && !bytes.starts_with(b"!") {
            d.set_header(LICENSE_HEADER);
        }
        Ok(d)
    };
    Ok((read(users)?, read(roles)?))
}

fn write_files(users: (&Path, &Document), roles: (&Path, &Document)) -> Result<(), String> {
    for (p, d) in [users, roles] {
        std::fs::write(p, d.to_bytes()).map_err(|e| format!("{}: {e}", p.display()))?;
    }
    Ok(())
}

/// `findRoles`: the roles listing `user`.
fn find_roles(roles: &Document, user: &str) -> Vec<String> {
    let mut out: Vec<String> = roles
        .keys()
        .into_iter()
        .filter(|role| {
            roles
                .values(role)
                .iter()
                .any(|list| list.split(',').any(|item| item.trim() == user))
        })
        .collect();
    out.sort();
    out
}

/// `addRoles`.
fn add_roles(roles: &mut Document, user: &str, names: &[String]) {
    for role in names {
        let role = role.trim();
        let mut users = roles.values(role);
        users.push(user.to_owned());
        roles.set(role, &users.join(","));
    }
}

/// `removeRoles`: `user` comes off every role listing it; a role whose
/// list changes goes to the end of the file, or away if now empty.
fn remove_roles(roles: &mut Document, user: &str) {
    let mut updates = Vec::new();
    for role in roles.keys() {
        let mut changed = false;
        let mut new_list = Vec::new();
        for value in roles.values(&role) {
            let items: Vec<&str> = value.split(',').collect();
            let kept: Vec<&str> = items.iter().copied().filter(|i| *i != user).collect();
            if kept.len() != items.len() {
                changed = true;
                if !kept.is_empty() {
                    new_list.push(kept.join(","));
                }
            }
        }
        if changed {
            updates.push((role, new_list));
        }
    }
    for (role, list) in updates {
        roles.clear(&role);
        if !list.is_empty() {
            roles.add(&role, &list.join(","));
        }
    }
}

/// `PasswordMaskingUtil.getHashProcessor().hash`: `ENC(...)` around the
/// one-way codec's hash.
fn hash(password: &str) -> Result<String, String> {
    saranga_config::mask::Codec::one_way()
        .encode(password)
        .map(|h| format!("ENC({h})"))
        .map_err(|e| e.to_string())
}

/// `roles.split(",")`.
fn split_roles(roles: &str) -> Vec<String> {
    let mut parts: Vec<String> = roles.split(',').map(str::to_owned).collect();
    while parts.len() > 1 && parts.last().is_some_and(String::is_empty) {
        parts.pop();
    }
    parts
}

fn add_user(m: &Management, broker: &Broker, a: &Args) -> Outcome {
    let user = a.s(0);
    let password = a.s(1);
    let roles = split_roles(&a.s_or_empty(2));
    let password = match password {
        Some(p) if !a.b(3) => Some(hash(&p)?),
        other => other,
    };
    match user_store(m, broker)? {
        UserStore::Basic(users) => {
            let user = user.ok_or("AMQ229072: User cannot be null")?;
            let password = password.ok_or("AMQ229073: Password cannot be null")?;
            users.add_new(&user, &password, &roles)?;
        }
        UserStore::Files {
            users: user_file,
            roles: role_file,
        } => {
            let user = user.unwrap_or_else(|| "null".to_owned());
            let (mut users, mut role_doc) = read_files(&user_file, &role_file)?;
            if users.get(&user).is_some() {
                return Err(format!("AMQ229223: User {user} already exists"));
            }
            users.add(&user, &password.unwrap_or_else(|| "null".to_owned()));
            add_roles(&mut role_doc, &user, &roles);
            write_files((&user_file, &users), (&role_file, &role_doc))?;
        }
    }
    Ok(Json::Null)
}

fn reset_user(m: &Management, broker: &Broker, a: &Args, plaintext: bool) -> Outcome {
    let user = a.s_or_empty(0);
    let password = match a.s(1) {
        Some(p) if !p.is_empty() && !plaintext => Some(hash(&p)?),
        other => other,
    };
    let roles = a.s(2).map(|r| split_roles(&r));
    match user_store(m, broker)? {
        UserStore::Basic(users) => users.update(&user, password.as_deref(), roles.as_deref())?,
        UserStore::Files {
            users: user_file,
            roles: role_file,
        } => {
            let (mut users, mut role_doc) = read_files(&user_file, &role_file)?;
            if users.get(&user).is_none() {
                return Err(format!("AMQ229224: User {user} does not exist"));
            }
            if let Some(p) = &password {
                users.set(&user, p);
            }
            if let Some(roles) = roles.filter(|r| !r.is_empty()) {
                remove_roles(&mut role_doc, &user);
                add_roles(&mut role_doc, &user, &roles);
            }
            write_files((&user_file, &users), (&role_file, &role_doc))?;
        }
    }
    Ok(Json::Null)
}

fn remove_user(m: &Management, broker: &Broker, user: &str) -> Outcome {
    match user_store(m, broker)? {
        UserStore::Basic(users) => users.remove(user)?,
        UserStore::Files {
            users: user_file,
            roles: role_file,
        } => {
            let (mut users, mut role_doc) = read_files(&user_file, &role_file)?;
            if users.get(user).is_none() {
                return Err(format!("AMQ229224: User {user} does not exist"));
            }
            users.clear(user);
            remove_roles(&mut role_doc, user);
            write_files((&user_file, &users), (&role_file, &role_doc))?;
        }
    }
    Ok(Json::Null)
}

/// `listUser`, as `buildJsonUserList` writes it.
fn list_user(m: &Management, broker: &Broker, user: Option<&str>) -> Outcome {
    let list = match user_store(m, broker)? {
        UserStore::Basic(users) => users.list(user)?,
        UserStore::Files {
            users: user_file,
            roles: role_file,
        } => {
            let (users, roles) = read_files(&user_file, &role_file)?;
            match user.filter(|u| !u.is_empty()) {
                Some(u) => vec![(u.to_owned(), find_roles(&roles, u))],
                None => users
                    .keys()
                    .into_iter()
                    .map(|u| {
                        let r = find_roles(&roles, &u);
                        (u, r)
                    })
                    .collect(),
            }
        }
    };
    Ok(Json::from(
        Json::Array(
            list.into_iter()
                .map(|(u, roles)| {
                    Json::object()
                        .with("username", u)
                        .with("roles", Json::from(roles))
                })
                .collect(),
        )
        .to_string(),
    ))
}

/// The broker attributes these give.
pub(super) fn attribute(broker: &Broker, name: &str) -> Option<Json> {
    let store = broker.security_store();
    let counter =
        |f: fn(&saranga_security::store::SecurityCounters) -> &std::sync::atomic::AtomicU64| {
            Json::from(store.map_or(0, |s| {
                f(&s.counters).load(std::sync::atomic::Ordering::Relaxed) as i64
            }))
        };
    Some(match name {
        "AuthenticationCacheSize" => {
            Json::from(store.map_or(0, |s| s.authentication_cache_size() as i64))
        }
        "AuthorizationCacheSize" => {
            Json::from(store.map_or(0, |s| s.authorization_cache_size() as i64))
        }
        "AuthenticationSuccessCount" => counter(|c| &c.authentication_success),
        "AuthenticationFailureCount" => counter(|c| &c.authentication_failure),
        "AuthorizationSuccessCount" => counter(|c| &c.authorization_success),
        "AuthorizationFailureCount" => counter(|c| &c.authorization_failure),
        _ => return None,
    })
}

pub(super) async fn invoke(
    m: &Management,
    broker: &Arc<Broker>,
    call: &Call<'_>,
) -> Option<Outcome> {
    // Users.
    if let Some(a) = call.is("addUser", &[S, S, S, B]) {
        return Some(add_user(m, broker, &a));
    }
    if let Some(a) = call.is("resetUser", &[S, S, S, B]) {
        return Some(reset_user(m, broker, &a, a.b(3)));
    }
    if let Some(a) = call.is("resetUser", &[S, S, S]) {
        return Some(reset_user(m, broker, &a, true));
    }
    if let Some(a) = call.is("removeUser", &[S]) {
        return Some(remove_user(m, broker, &a.s_or_empty(0)));
    }
    if let Some(a) = call.is("listUser", &[S]) {
        return Some(list_user(m, broker, a.s(0).as_deref()));
    }
    // Security settings.
    if let Some(a) = call.is("addSecuritySettings", &[S, S]) {
        return Some(add_security_settings(
            broker,
            &a.s_or_empty(0),
            &a.s_or_empty(1),
        ));
    }
    for count in [7, 8, 10, 12] {
        let types = vec![S; count + 1];
        if let Some(a) = call.is("addSecuritySettings", &types) {
            let lists: Vec<Option<String>> = (1..=count).map(|i| a.s(i)).collect();
            let json = lists_json(&lists).to_string();
            return Some(add_security_settings(broker, &a.s_or_empty(0), &json));
        }
    }
    if let Some(a) = call.is("removeSecuritySettings", &[S]) {
        let address_match = a.s_or_empty(0);
        if let Some(store) = broker.security_store() {
            store.remove_match(&address_match);
        }
        if let (Some(storage), Some(id)) = (
            broker.storage(),
            broker.security_records().remove(&address_match),
        ) && let Err(e) = storage.delete_binding_record(id)
        {
            return Some(Err(e.to_string()));
        }
        return Some(Ok(Json::Null));
    }
    if let Some(a) = call.is("getRoles", &[S]) {
        let roles = broker
            .security_store()
            .map(|s| s.roles(&a.s_or_empty(0)))
            .unwrap_or_default();
        return Some(Ok(Json::Array(
            roles
                .iter()
                .map(|r| {
                    let mut row = vec![Json::from(r.name.clone())];
                    row.extend(PERMISSIONS.iter().map(|c| Json::from(r.allows(*c))));
                    Json::Array(row)
                })
                .collect(),
        )));
    }
    if let Some(a) = call.is("getRolesAsJSON", &[S]) {
        let roles = broker
            .security_store()
            .map(|s| s.roles(&a.s_or_empty(0)))
            .unwrap_or_default();
        return Some(Ok(Json::from(
            Json::Array(roles.iter().map(role_json).collect()).to_string(),
        )));
    }
    // Caches.
    if call.is("clearAuthenticationCache", &[]).is_some() {
        if let Some(s) = broker.security_store() {
            s.invalidate_authentication_cache();
        }
        return Some(Ok(Json::Null));
    }
    if call.is("clearAuthorizationCache", &[]).is_some() {
        if let Some(s) = broker.security_store() {
            s.invalidate_authorization_cache();
        }
        return Some(Ok(Json::Null));
    }
    None
}
