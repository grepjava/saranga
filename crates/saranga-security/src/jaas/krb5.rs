//! The JDK's `com.sun.security.auth.module.Krb5LoginModule`, through the
//! system's Kerberos library (see [`crate::kerberos`]): a TGT from a
//! ticket cache, a keytab or a password, and the keytab an acceptor
//! accepts with (`storeKey`), as the JDK's module obtains them, with its
//! options and its checks of them. GSSAPI's acceptor realm
//! (`saslLoginConfigScope`) and LDAP's GSSAPI identity log in with it.
//!
//! The subject gets a `KerberosPrincipal` (unless the principal is `*`)
//! and the credentials, kept with it. JAAS's shared state is not kept
//! between modules in Saranga, so `useFirstPass` and `tryFirstPass` find
//! no name in it, as when no module before stored one.

use crate::kerberos::{self, Acceptor, Initiator};

use super::{Callbacks, Credential, LoginError, LoginModule, ModuleEntry, Principal, Subject};

pub const KRB5_LOGIN_MODULE: &str = "com.sun.security.auth.module.Krb5LoginModule";
pub const KERBEROS_PRINCIPAL: &str = "javax.security.auth.kerberos.KerberosPrincipal";

fn flag(entry: &ModuleEntry, key: &str) -> bool {
    entry.bool_option(key)
}

pub(crate) struct Krb5LoginModule {
    debug: bool,
    store_key: bool,
    do_not_prompt: bool,
    use_ticket_cache: bool,
    use_key_tab: bool,
    ticket_cache_name: Option<String>,
    key_tab_name: Option<String>,
    princ_name: Option<String>,
    renew_tgt: bool,
    is_initiator: bool,
    try_first_pass: bool,
    use_first_pass: bool,
    /// What `login` obtained, for `commit`.
    outcome: Option<Outcome>,
}

struct Outcome {
    principal: String,
    unbound: bool,
    initiator: Option<Initiator>,
    /// The keytab (`ktab`).
    keytab: Option<Acceptor>,
    /// Keys made from the password (`encKeys`), when `storeKey`.
    password_keys: bool,
}

impl Krb5LoginModule {
    pub(crate) fn new(entry: &ModuleEntry) -> Self {
        let m = Self {
            debug: flag(entry, "debug"),
            store_key: flag(entry, "storeKey"),
            do_not_prompt: flag(entry, "doNotPrompt"),
            use_ticket_cache: flag(entry, "useTicketCache"),
            use_key_tab: flag(entry, "useKeyTab"),
            ticket_cache_name: entry.option("ticketCache").map(str::to_owned),
            key_tab_name: entry.option("keyTab").map(str::to_owned),
            princ_name: entry.option("principal").map(str::to_owned),
            renew_tgt: flag(entry, "renewTGT"),
            // True unless given.
            is_initiator: entry
                .option("isInitiator")
                .is_none_or(|v| v.eq_ignore_ascii_case("true")),
            try_first_pass: flag(entry, "tryFirstPass"),
            use_first_pass: flag(entry, "useFirstPass"),
            outcome: None,
        };
        if m.debug {
            tracing::debug!(
                "Debug is true storeKey {} useTicketCache {} useKeyTab {} doNotPrompt {} ticketCache is {:?} isInitiator {} KeyTab is {:?} principal is {:?} tryFirstPass is {} useFirstPass is {}",
                m.store_key,
                m.use_ticket_cache,
                m.use_key_tab,
                m.do_not_prompt,
                m.ticket_cache_name,
                m.is_initiator,
                m.key_tab_name,
                m.princ_name,
                m.try_first_pass,
                m.use_first_pass
            );
        }
        m
    }

    fn validate(&self, princ_name: Option<&str>) -> Result<(), LoginError> {
        let err = |m: &str| Err(LoginError::new(m));
        if self.do_not_prompt
            && !self.use_ticket_cache
            && !self.use_key_tab
            && !self.try_first_pass
            && !self.use_first_pass
        {
            return err(
                "Configuration Error - either doNotPrompt should be  false or at least one of useTicketCache,  useKeyTab, tryFirstPass and useFirstPass should be true",
            );
        }
        if let Some(c) = &self.ticket_cache_name
            && !self.use_ticket_cache
        {
            return err(&format!(
                "Configuration Error  - useTicketCache should be set to true to use the ticket cache{c}"
            ));
        }
        if let Some(k) = &self.key_tab_name
            && !self.use_key_tab
        {
            return err(&format!(
                "Configuration Error - useKeyTab should be set to true to use the keytab{k}"
            ));
        }
        if self.store_key
            && self.do_not_prompt
            && !self.use_key_tab
            && !self.try_first_pass
            && !self.use_first_pass
        {
            return err(
                "Configuration Error - either doNotPrompt should be set to  false or at least one of tryFirstPass, useFirstPass or useKeyTab must be set to true for storeKey option",
            );
        }
        if self.renew_tgt && !self.use_ticket_cache {
            return err(
                "Configuration Error - either useTicketCache should be  true or renewTGT should be false",
            );
        }
        if princ_name == Some("*") && self.is_initiator {
            return err("Configuration Error - principal cannot be * when isInitiator is true");
        }
        Ok(())
    }

    /// `attemptAuthentication`.
    fn attempt(
        &self,
        princ_name: Option<&str>,
        callbacks: &Callbacks<'_>,
        from_shared_state: bool,
    ) -> Result<Outcome, LoginError> {
        let lib = || kerberos::backend().map_err(|e| LoginError::new(e.0));
        let k = |e: kerberos::Error| LoginError::new(e.0);
        let unbound = princ_name == Some("*");
        let mut principal = princ_name
            .map(|n| lib()?.canonical_name(n).map_err(k))
            .transpose()?;
        let mut initiator = None;
        if self.use_ticket_cache {
            if self.debug {
                tracing::debug!("Acquire TGT from Cache");
            }
            match lib()?
                .ticket_cache(
                    principal.as_deref(),
                    self.ticket_cache_name.as_deref(),
                    self.renew_tgt,
                )
                .map_err(k)?
            {
                Some((client, cred)) => {
                    principal.get_or_insert(client);
                    initiator = Some(cred);
                }
                None if self.debug => tracing::debug!("Principal is null"),
                None => {}
            }
        }
        let mut keytab = None;
        let mut password_keys = false;
        if initiator.is_none() {
            let principal = match principal.take() {
                Some(p) => p,
                None => {
                    let name = self.prompt_for_name(callbacks, from_shared_state)?;
                    lib()?.canonical_name(&name).map_err(k)?
                }
            };
            if self.use_key_tab {
                let tab = Acceptor::Keytab {
                    keytab: self.key_tab_name.clone(),
                    principal: (!unbound).then(|| principal.clone()),
                };
                keytab = Some(tab);
                if self.is_initiator
                    && !lib()?.keytab_has_keys(self.key_tab_name.as_deref(), &principal)
                {
                    keytab = None;
                    if self.debug {
                        tracing::debug!(
                            "Key for the principal {principal} not available in {}",
                            self.key_tab_name.as_deref().unwrap_or("default key tab")
                        );
                    }
                }
            }
            if keytab.is_none() {
                let password = self.prompt_for_pass(callbacks, from_shared_state)?;
                if self.is_initiator {
                    initiator = Some(
                        lib()?
                            .initiate_with_password(&principal, &password)
                            .map_err(k)?,
                    );
                }
                password_keys = self.store_key;
            } else if self.is_initiator {
                initiator = Some(
                    lib()?
                        .initiate_with_keytab(self.key_tab_name.as_deref(), &principal)
                        .map_err(k)?,
                );
            }
            if self.is_initiator && initiator.is_none() {
                return Err(LoginError::new("TGT Can not be obtained from the KDC "));
            }
            return Ok(Outcome {
                principal,
                unbound,
                initiator,
                keytab,
                password_keys,
            });
        }
        Ok(Outcome {
            principal: principal.unwrap_or_default(),
            unbound,
            initiator,
            keytab,
            password_keys,
        })
    }

    fn prompt_for_name(
        &self,
        callbacks: &Callbacks<'_>,
        from_shared_state: bool,
    ) -> Result<String, LoginError> {
        if from_shared_state {
            return Err(LoginError::new(
                "Username can not be obtained from sharedstate ",
            ));
        }
        if self.do_not_prompt {
            return Err(LoginError::new(
                "Unable to obtain Principal Name for authentication ",
            ));
        }
        if callbacks.no_handler {
            return Err(LoginError::new(super::NO_HANDLER));
        }
        // `JaasCallbackHandler`'s name, else `user.name`.
        Ok(match callbacks.user.filter(|u| !u.is_empty()) {
            Some(u) => u.to_owned(),
            None => saranga_config::system::property("user.name")
                .or_else(|| std::env::var("USER").ok())
                .or_else(|| std::env::var("USERNAME").ok())
                .unwrap_or_default(),
        })
    }

    fn prompt_for_pass(
        &self,
        callbacks: &Callbacks<'_>,
        from_shared_state: bool,
    ) -> Result<String, LoginError> {
        if from_shared_state {
            return Err(LoginError::new(
                "Password can not be obtained from sharedstate ",
            ));
        }
        if self.do_not_prompt {
            return Err(LoginError::new("Unable to obtain password from user\n"));
        }
        if callbacks.no_handler {
            return Err(LoginError::new(super::NO_HANDLER));
        }
        if callbacks.scram.is_some() {
            return Err(LoginError::new(
                "Unrecognized Callback PasswordCallback not available to garner  authentication information  from the user",
            ));
        }
        callbacks
            .password
            .map(str::to_owned)
            .ok_or_else(|| LoginError::new("No password provided"))
    }
}

impl LoginModule for Krb5LoginModule {
    fn login(&mut self, callbacks: &Callbacks<'_>) -> Result<bool, LoginError> {
        let princ_name = saranga_config::system::property("sun.security.krb5.principal")
            .or_else(|| self.princ_name.clone());
        self.validate(princ_name.as_deref())?;
        if self.try_first_pass {
            match self.attempt(princ_name.as_deref(), callbacks, true) {
                Ok(o) => {
                    self.outcome = Some(o);
                    return Ok(true);
                }
                Err(e) if self.debug => {
                    tracing::debug!(
                        "\t\t[Krb5LoginModule] tryFirstPass failed with:{}",
                        e.message
                    )
                }
                Err(_) => {}
            }
        } else if self.use_first_pass {
            let o = self.attempt(princ_name.as_deref(), callbacks, true)?;
            self.outcome = Some(o);
            return Ok(true);
        }
        match self.attempt(princ_name.as_deref(), callbacks, false) {
            Ok(o) => {
                self.outcome = Some(o);
                Ok(true)
            }
            Err(e) => {
                if self.debug {
                    tracing::debug!(
                        "\t\t[Krb5LoginModule] authentication failed \n{}",
                        e.message
                    );
                }
                Err(e)
            }
        }
    }

    fn commit(&mut self, subject: &mut Subject) -> Result<bool, LoginError> {
        let Some(o) = self.outcome.take() else {
            return Ok(false);
        };
        if self.is_initiator && o.initiator.is_none() {
            return Err(LoginError::new("Null Client Credential"));
        }
        let acceptor = if self.store_key {
            if o.password_keys {
                Some(Acceptor::PasswordKeys {
                    principal: o.principal.clone(),
                })
            } else if o.keytab.is_some() {
                o.keytab
            } else {
                return Err(LoginError::new("No key to store"));
            }
        } else {
            None
        };
        if !o.unbound {
            subject.add(Principal::Other {
                class: KERBEROS_PRINCIPAL.to_owned(),
                name: o.principal.clone(),
            });
        }
        if o.initiator.is_some() || acceptor.is_some() {
            subject.add_credential(Credential::Kerberos {
                initiator: o.initiator,
                acceptor,
            });
        }
        if self.debug {
            tracing::debug!("Commit Succeeded \n");
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn module(options: &str) -> Krb5LoginModule {
        let text = format!("activemq {{ {KRB5_LOGIN_MODULE} required {options}; }};");
        let config = super::super::LoginConfig::parse(&text, |_| None).unwrap();
        Krb5LoginModule::new(&config.entry("activemq").unwrap()[0])
    }

    fn refused(options: &str) -> String {
        module(options)
            .login(&Callbacks::default())
            .unwrap_err()
            .message
    }

    #[test]
    fn options_are_checked_as_the_jdk_checks_them() {
        assert!(
            refused("doNotPrompt=true").starts_with("Configuration Error - either doNotPrompt")
        );
        assert!(refused("ticketCache=\"/tmp/x\"").contains("useTicketCache should be set"));
        assert!(
            refused("keyTab=\"/tmp/x\"")
                .contains("useKeyTab should be set to true to use the keytab/tmp/x")
        );
        assert!(refused("renewTGT=true").contains("renewTGT should be false"));
        assert!(
            refused("principal=\"*\"").contains("principal cannot be * when isInitiator is true")
        );
        assert!(refused("useFirstPass=true").contains("can not be obtained from sharedstate"));
        let m = module("isInitiator=false storeKey=true useKeyTab=true principal=\"amqp/h\"");
        assert!(!m.is_initiator && m.store_key && m.use_key_tab);
        assert!(module("").is_initiator);
    }
}
