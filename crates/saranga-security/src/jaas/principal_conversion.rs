//! `PrincipalConversionLoginModule`: when no module has given the subject
//! a user, the first principal of each class `principalClassList` names
//! becomes one (a Kerberos principal, say, or an X.500 one).

use super::{Callbacks, LoginError, LoginModule, ModuleEntry, Principal, Subject};

pub const PRINCIPAL_CLASS_LIST: &str = "principalClassList";

pub(crate) struct PrincipalConversionLoginModule {
    classes: Option<String>,
}

impl PrincipalConversionLoginModule {
    pub(crate) fn new(entry: &ModuleEntry) -> Self {
        Self {
            classes: entry.option(PRINCIPAL_CLASS_LIST).map(str::to_owned),
        }
    }
}

impl LoginModule for PrincipalConversionLoginModule {
    fn login(&mut self, _callbacks: &Callbacks<'_>) -> Result<bool, LoginError> {
        tracing::debug!("login ok!");
        Ok(true)
    }

    fn commit(&mut self, subject: &mut Subject) -> Result<bool, LoginError> {
        if subject.user().is_some() {
            return Ok(false);
        }
        let mut converted = false;
        if let Some(classes) = &self.classes {
            for class in classes.split(',') {
                let class = class.trim();
                let found = subject
                    .principals()
                    .iter()
                    .find(|p| p.class_name() == class)
                    .map(|p| p.name().to_owned());
                if let Some(name) = found {
                    tracing::debug!("converting: {class} {name}");
                    subject.add(Principal::User(name));
                    converted = true;
                }
            }
        }
        tracing::debug!("commit, result: {converted}");
        Ok(converted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn other(class: &str, name: &str) -> Principal {
        Principal::Other {
            class: class.into(),
            name: name.into(),
        }
    }

    fn module(classes: Option<&str>) -> PrincipalConversionLoginModule {
        PrincipalConversionLoginModule {
            classes: classes.map(str::to_owned),
        }
    }

    #[test]
    fn the_first_principal_of_each_class_becomes_a_user() {
        let mut subject = Subject::default();
        subject.add(other("a.K", "k1"));
        subject.add(other("a.K", "k2"));
        subject.add(other("a.X", "x1"));
        subject.add(Principal::Role("r".into()));
        let mut m = module(Some(" a.X , a.K,"));
        assert!(m.login(&Callbacks::default()).unwrap());
        assert!(m.commit(&mut subject).unwrap());
        assert_eq!(subject.users().collect::<Vec<_>>(), ["x1", "k1"]);
        // A subject with a user already is left alone.
        assert!(!module(Some("a.K")).commit(&mut subject).unwrap());
        let mut subject = Subject::default();
        subject.add(Principal::Role("r".into()));
        assert!(!module(None).commit(&mut subject).unwrap());
        assert!(
            module(Some(Principal::Role(String::new()).class_name()))
                .commit(&mut subject)
                .unwrap()
        );
        assert_eq!(subject.user(), Some("r"));
    }
}
