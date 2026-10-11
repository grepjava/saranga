//! Security: who clients are and what they may do.
//!
//! - [`jaas`]: JAAS login configurations (`login.config`) and Artemis's
//!   login modules.
//! - [`manager`]: security managers, which turn credentials into a subject
//!   and decide whether its roles pass a check.
//! - [`store`]: the security store the broker asks, with its caches.
//! - [`properties`]: the Java properties format of users and roles files.
//!
//! Ported from `org.apache.activemq.artemis.core.security` and
//! `org.apache.activemq.artemis.spi.core.security`.

pub mod audit;
pub mod basic;
mod der;
pub mod dn;
pub mod jaas;
pub mod keystore;
pub mod manager;
pub use saranga_config::properties;
pub mod store;

pub use basic::{BasicSecurityManager, UserDirectory};
pub use jaas::{LoginConfig, PeerCertificate, Principal, Subject};
pub use manager::{ConnectionInfo, JaasSecurityManager, SecurityManager};
pub use store::{
    Authenticated, SecurityAuth, SecurityError, SecurityStore, SecurityStoreConfig, Violation,
    ViolationListener,
};
