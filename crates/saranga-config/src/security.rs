//! Security settings: roles and the permissions they grant on a match.
//! Ported from `Role`, `CheckType` and `FileConfigurationParser
//! .parseSecurity`.

use std::fmt;

/// `CheckType`: what a user is trying to do. [`Display`](fmt::Display)
/// gives the Java enum name, which Artemis puts in its error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CheckType {
    Send,
    Consume,
    CreateAddress,
    DeleteAddress,
    CreateDurableQueue,
    DeleteDurableQueue,
    CreateNonDurableQueue,
    DeleteNonDurableQueue,
    Manage,
    Browse,
    View,
    Edit,
}

impl CheckType {
    pub const ALL: [CheckType; 12] = [
        CheckType::Send,
        CheckType::Consume,
        CheckType::CreateAddress,
        CheckType::DeleteAddress,
        CheckType::CreateDurableQueue,
        CheckType::DeleteDurableQueue,
        CheckType::CreateNonDurableQueue,
        CheckType::DeleteNonDurableQueue,
        CheckType::Manage,
        CheckType::Browse,
        CheckType::View,
        CheckType::Edit,
    ];

    /// The Java enum constant name.
    pub fn name(self) -> &'static str {
        match self {
            CheckType::Send => "SEND",
            CheckType::Consume => "CONSUME",
            CheckType::CreateAddress => "CREATE_ADDRESS",
            CheckType::DeleteAddress => "DELETE_ADDRESS",
            CheckType::CreateDurableQueue => "CREATE_DURABLE_QUEUE",
            CheckType::DeleteDurableQueue => "DELETE_DURABLE_QUEUE",
            CheckType::CreateNonDurableQueue => "CREATE_NON_DURABLE_QUEUE",
            CheckType::DeleteNonDurableQueue => "DELETE_NON_DURABLE_QUEUE",
            CheckType::Manage => "MANAGE",
            CheckType::Browse => "BROWSE",
            CheckType::View => "VIEW",
            CheckType::Edit => "EDIT",
        }
    }

    /// The `type` of a `<permission>` that grants this, as written in
    /// `broker.xml`.
    pub fn permission_name(self) -> &'static str {
        match self {
            CheckType::Send => "send",
            CheckType::Consume => "consume",
            CheckType::CreateAddress => "createAddress",
            CheckType::DeleteAddress => "deleteAddress",
            CheckType::CreateDurableQueue => "createDurableQueue",
            CheckType::DeleteDurableQueue => "deleteDurableQueue",
            CheckType::CreateNonDurableQueue => "createNonDurableQueue",
            CheckType::DeleteNonDurableQueue => "deleteNonDurableQueue",
            CheckType::Manage => "manage",
            CheckType::Browse => "browse",
            CheckType::View => "view",
            CheckType::Edit => "edit",
        }
    }

    /// The check a `<permission type>` grants. `createTempQueue` and
    /// `deleteTempQueue` are the old names of the non-durable permissions.
    pub fn from_permission(name: &str) -> Option<Self> {
        match name {
            "createTempQueue" => Some(CheckType::CreateNonDurableQueue),
            "deleteTempQueue" => Some(CheckType::DeleteNonDurableQueue),
            _ => Self::ALL.into_iter().find(|c| c.permission_name() == name),
        }
    }

    /// `CheckType.hasRole`.
    pub fn has_role(self, role: &Role) -> bool {
        role.permissions & self.bit() != 0
    }

    fn bit(self) -> u16 {
        1 << (self as u16)
    }
}

impl fmt::Display for CheckType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// `Role`: a role name and the checks it passes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Role {
    pub name: String,
    permissions: u16,
}

impl Role {
    pub fn new(name: impl Into<String>, grants: impl IntoIterator<Item = CheckType>) -> Self {
        let mut role = Self {
            name: name.into(),
            permissions: 0,
        };
        for check in grants {
            role.grant(check);
        }
        role
    }

    pub fn grant(&mut self, check: CheckType) {
        self.permissions |= check.bit();
    }

    pub fn allows(&self, check: CheckType) -> bool {
        check.has_role(self)
    }
}

/// One `<security-setting match>`: the roles that apply to the addresses it
/// matches. Unlike address settings, security settings are not merged: the
/// most specific match alone decides.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SecuritySetting {
    pub roles: Vec<Role>,
}

/// One `<security-setting-plugin>`, kept so its absence of support can be
/// reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecuritySettingPluginConfig {
    pub class_name: String,
    /// `<setting name value>` pairs, in document order, masks resolved.
    pub settings: Vec<(String, String)>,
}
