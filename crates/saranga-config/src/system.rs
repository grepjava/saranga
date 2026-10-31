//! The JVM's system properties, as the broker or the command line was
//! given them (`-D` in `JAVA_ARGS` or on `run`), for what reads
//! `System.getProperty`: connectors and login modules.

use std::sync::RwLock;

static SYSTEM_PROPERTIES: RwLock<Vec<(String, String)>> = RwLock::new(Vec::new());

/// Sets the system properties. The last of a name wins.
pub fn set_properties(properties: Vec<(String, String)>) {
    *SYSTEM_PROPERTIES.write().unwrap_or_else(|e| e.into_inner()) = properties;
}

/// `System.getProperty`.
pub fn property(name: &str) -> Option<String> {
    SYSTEM_PROPERTIES
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .rev()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.clone())
}
