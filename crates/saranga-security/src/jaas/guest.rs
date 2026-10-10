//! `GuestLoginModule`: logs everyone in as one guest user and role. With
//! `credentialsInvalidate=true`, a client that sends a password is not a
//! guest and the module is ignored.

use super::{Callbacks, LoginError, LoginModule, ModuleEntry, Principal, Subject};

pub const GUEST_USER: &str = "org.apache.activemq.jaas.guest.user";
pub const GUEST_ROLE: &str = "org.apache.activemq.jaas.guest.role";

pub(crate) struct GuestLoginModule {
    user: String,
    role: String,
    credentials_invalidate: bool,
    login_succeeded: bool,
}

impl GuestLoginModule {
    pub(crate) fn new(entry: &ModuleEntry) -> Self {
        Self {
            user: entry.option(GUEST_USER).unwrap_or("guest").to_owned(),
            role: entry.option(GUEST_ROLE).unwrap_or("guests").to_owned(),
            credentials_invalidate: entry.bool_option("credentialsInvalidate"),
            login_succeeded: false,
        }
    }
}

impl LoginModule for GuestLoginModule {
    fn login(&mut self, callbacks: &Callbacks<'_>) -> Result<bool, LoginError> {
        self.login_succeeded = !(self.credentials_invalidate && callbacks.password.is_some());
        Ok(self.login_succeeded)
    }

    fn commit(&mut self, subject: &mut Subject) -> Result<bool, LoginError> {
        if self.login_succeeded {
            subject.add(Principal::User(self.user.clone()));
            subject.add(Principal::Role(self.role.clone()));
        }
        Ok(self.login_succeeded)
    }
}
