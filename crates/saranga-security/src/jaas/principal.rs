//! `AbstractPrincipalLoginModule`, as `SCRAMLoginModule` and Artemis's `Krb5LoginModule`: the principals
//! of the identity the peer authenticated as (`PrincipalsCallback`), with a
//! user principal named like the first.

use super::{Callbacks, LoginError, LoginModule, Principal, Subject};

#[derive(Default)]
pub(crate) struct PrincipalLoginModule {
    authenticated: Vec<Principal>,
    login_succeeded: bool,
}

impl LoginModule for PrincipalLoginModule {
    fn login(&mut self, callbacks: &Callbacks<'_>) -> Result<bool, LoginError> {
        callbacks.jaas("PrincipalsCallback")?;
        if let Some(principals) = callbacks.peer_principals {
            self.authenticated.extend(principals.iter().cloned());
        }
        if !self.authenticated.is_empty() {
            self.login_succeeded = true;
        }
        tracing::debug!("login {:?}", self.authenticated);
        Ok(self.login_succeeded)
    }

    fn commit(&mut self, subject: &mut Subject) -> Result<bool, LoginError> {
        let result = self.login_succeeded;
        if result {
            let user = Principal::User(self.authenticated[0].name().to_owned());
            self.authenticated.push(user);
            for p in &self.authenticated {
                subject.add(p.clone());
            }
        }
        self.login_succeeded = false;
        tracing::debug!("commit, result: {result}");
        Ok(result)
    }
}
