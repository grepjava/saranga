//! What Artemis's ZooKeeper lock manager does through Curator, with the
//! same znodes, so that Java and Saranga brokers coordinate on one
//! ensemble: the namespace, the retry policy (`RetryNTimes`,
//! `RetryForever`), `creatingParentContainersIfNeeded`, protected
//! sequential nodes (`withProtection`), `InterProcessSemaphoreV2` with one
//! lease and its `InterProcessMutex`, and `DistributedAtomicLong`.

use std::time::Duration;

use crate::zookeeper::{CreateMode, Error, Result, ZooKeeper, proto::code};

/// `InterProcessSemaphoreV2.LOCK_PARENT`, `LEASE_PARENT`, `LEASE_BASE_NAME`.
const LOCK_PARENT: &str = "locks";
const LEASE_PARENT: &str = "leases";
const LEASE_BASE_NAME: &str = "lease-";
/// `InterProcessMutex.LOCK_NAME`.
const LOCK_NAME: &str = "lock-";
/// `ProtectedUtils.PROTECTED_PREFIX`.
const PROTECTED_PREFIX: &str = "_c_";

/// A `CuratorFramework`: the session, its namespace and retry policy.
#[derive(Clone)]
pub struct Curator {
    pub zk: ZooKeeper,
    namespace: Option<String>,
    /// `RetryNTimes`'s count; negative retries for ever (`RetryForever`).
    retries: i32,
    retry_wait: Duration,
}

/// `ZKPaths.makePath`.
pub fn make_path(parent: &str, child: &str) -> String {
    let parent = parent.trim_end_matches('/');
    let child = child.trim_start_matches('/');
    if child.is_empty() {
        if parent.is_empty() {
            "/".to_owned()
        } else {
            parent.to_owned()
        }
    } else {
        format!("{parent}/{child}")
    }
}

/// `ZKPaths.getNodeFromPath`.
pub fn node_from_path(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn is(e: &Error, c: i32) -> bool {
    matches!(e, Error::Api { code, .. } if *code == c)
}

impl Curator {
    pub fn new(
        zk: ZooKeeper,
        namespace: Option<String>,
        retries: i32,
        retry_wait: Duration,
    ) -> Self {
        Self {
            zk,
            namespace: namespace.filter(|n| !n.is_empty()),
            retries,
            retry_wait,
        }
    }

    /// `NamespaceImpl.fixForNamespace`.
    fn fix(&self, path: &str) -> String {
        match &self.namespace {
            Some(ns) => make_path(&format!("/{ns}"), path),
            None => path.to_owned(),
        }
    }

    fn unfix(&self, path: String) -> String {
        match &self.namespace {
            Some(ns) => {
                let prefix = format!("/{ns}");
                match path.strip_prefix(&prefix) {
                    Some("") => "/".to_owned(),
                    Some(p) => p.to_owned(),
                    None => path,
                }
            }
            None => path,
        }
    }

    /// `RetryLoop.callWithRetry`.
    async fn retry<T, F, Fut>(&self, mut f: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut count = 0;
        loop {
            match f().await {
                Err(e) if e.is_retryable() && (self.retries < 0 || count < self.retries) => {
                    count += 1;
                    tokio::time::sleep(self.retry_wait).await;
                }
                r => return r,
            }
        }
    }

    /// `ZKPaths.mkdirs` with containers: every ancestor of `path` (and
    /// `path` itself when `include_last`).
    async fn mkdirs(&self, path: &str, include_last: bool) -> Result<()> {
        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        let n = if include_last {
            parts.len()
        } else {
            parts.len().saturating_sub(1)
        };
        let mut at = String::new();
        for p in &parts[..n] {
            at.push('/');
            at.push_str(p);
            if self.zk.exists(&at).await?.is_some() {
                continue;
            }
            match self.zk.create(&at, &[], CreateMode::Container).await {
                Ok(_) => {}
                Err(e) if is(&e, code::NODE_EXISTS) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// `create().creatingParentContainersIfNeeded()`, optionally
    /// `withProtection()`: the path created, without the namespace.
    pub async fn create(
        &self,
        path: &str,
        data: &[u8],
        mode: CreateMode,
        protect: bool,
    ) -> Result<String> {
        let full = self.fix(path);
        let full = if protect {
            let id = uuid::Uuid::new_v4().to_string();
            let (parent, name) = full.rsplit_once('/').unwrap_or(("", &full));
            format!("{parent}/{PROTECTED_PREFIX}{id}-{name}")
        } else {
            full
        };
        let mut first = true;
        let created = self
            .retry(|| {
                let full = full.clone();
                let retrying = !std::mem::replace(&mut first, false);
                async move {
                    if protect && retrying {
                        // `findProtectedNodeInForeground`: the create may
                        // have reached the server before the connection went.
                        if let Some(found) = self.find_protected(&full).await? {
                            return Ok(found);
                        }
                    }
                    match self.zk.create(&full, data, mode).await {
                        Err(e) if is(&e, code::NO_NODE) => {
                            self.mkdirs(&full, false).await?;
                            self.zk.create(&full, data, mode).await
                        }
                        r => r,
                    }
                }
            })
            .await?;
        Ok(self.unfix(created))
    }

    async fn find_protected(&self, full: &str) -> Result<Option<String>> {
        let (parent, name) = full.rsplit_once('/').unwrap_or(("", full));
        let prefix = &name[..PROTECTED_PREFIX.len() + 36 + 1];
        let children = match self
            .zk
            .get_children(if parent.is_empty() { "/" } else { parent })
            .await
        {
            Ok(c) => c,
            Err(e) if is(&e, code::NO_NODE) => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(children
            .into_iter()
            .find(|c| c.starts_with(prefix))
            .map(|c| format!("{parent}/{c}")))
    }

    pub async fn get_data(&self, path: &str) -> Result<Vec<u8>> {
        let full = self.fix(path);
        self.retry(|| async { self.zk.get_data(&full).await.map(|(d, _)| d) })
            .await
    }

    pub async fn set_data(&self, path: &str, data: &[u8]) -> Result<()> {
        let full = self.fix(path);
        self.retry(|| async { self.zk.set_data(&full, data, -1).await.map(|_| ()) })
            .await
    }

    pub async fn get_children(&self, path: &str) -> Result<Vec<String>> {
        let full = self.fix(path);
        self.retry(|| async { self.zk.get_children(&full).await })
            .await
    }

    /// `delete().guaranteed()`: tried again until it is gone or the
    /// session is.
    pub async fn delete(&self, path: &str) -> Result<()> {
        let full = self.fix(path);
        let r = self
            .retry(|| async {
                match self.zk.delete(&full, -1).await {
                    Err(e) if is(&e, code::NO_NODE) => Ok(()),
                    r => r,
                }
            })
            .await;
        if r.is_err() {
            // Curator's guaranteed delete goes on in the background while
            // the session lives.
            let zk = self.zk.clone();
            let full = full.clone();
            tokio::spawn(async move {
                for _ in 0..30 {
                    match zk.delete(&full, -1).await {
                        Err(e) if e.is_retryable() => {
                            tokio::time::sleep(Duration::from_secs(1)).await
                        }
                        _ => return,
                    }
                }
            });
        }
        r
    }

    /// `InterProcessSemaphoreV2(client, path, 1).acquire(0, ...)` with
    /// `setNodeData(data)`: the lease's path, or `None` when another
    /// holds it.
    pub async fn try_acquire_lease(&self, path: &str, data: &[u8]) -> Result<Option<String>> {
        // The semaphore's InterProcessMutex, tried once.
        let lock_path = make_path(path, LOCK_PARENT);
        let ours = self
            .create(
                &make_path(&lock_path, LOCK_NAME),
                &local_address(),
                CreateMode::EphemeralSequential,
                true,
            )
            .await?;
        let held = async {
            let mut children = self.get_children(&lock_path).await?;
            children
                .sort_by(|a, b| fix_for_sorting(a, LOCK_NAME).cmp(fix_for_sorting(b, LOCK_NAME)));
            let name = node_from_path(&ours);
            if !children.iter().any(|c| c == name) {
                return Err(Error::Api {
                    code: code::NO_NODE,
                    path: ours.clone(),
                });
            }
            Ok(children.first().is_some_and(|c| c == name))
        }
        .await;
        let result = match held {
            Ok(true) => self.lease(path, data).await,
            Ok(false) => Ok(None),
            Err(e) => Err(e),
        };
        let _ = self.delete(&ours).await;
        result
    }

    /// `internalAcquire1Lease` holding the mutex.
    async fn lease(&self, path: &str, data: &[u8]) -> Result<Option<String>> {
        let leases = make_path(path, LEASE_PARENT);
        let lease = self
            .create(
                &make_path(&leases, LEASE_BASE_NAME),
                data,
                CreateMode::EphemeralSequential,
                true,
            )
            .await?;
        let children = match self.get_children(&leases).await {
            Ok(c) => c,
            Err(e) => {
                let _ = self.delete(&lease).await;
                return Err(e);
            }
        };
        let name = node_from_path(&lease);
        if !children.iter().any(|c| c == name) {
            // "Sequential path not found - possible session loss".
            return Err(Error::Api {
                code: code::NO_NODE,
                path: lease,
            });
        }
        if children.len() <= 1 {
            Ok(Some(lease))
        } else {
            let _ = self.delete(&lease).await;
            Ok(None)
        }
    }

    /// `DistributedAtomicLong.get`: 0 while the node does not exist.
    pub async fn get_long(&self, path: &str) -> Result<i64> {
        match self.get_data(path).await {
            Ok(d) => Ok(bytes_to_long(&d)),
            Err(e) if is(&e, code::NO_NODE) => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// `DistributedAtomicLong.forceSet`.
    pub async fn force_set_long(&self, path: &str, value: i64) -> Result<()> {
        let bytes = value.to_be_bytes();
        match self.set_data(path, &bytes).await {
            Err(e) if is(&e, code::NO_NODE) => {
                match self
                    .create(path, &bytes, CreateMode::Persistent, false)
                    .await
                {
                    Err(e) if is(&e, code::NODE_EXISTS) => self.set_data(path, &bytes).await,
                    r => r.map(|_| ()),
                }
            }
            r => r,
        }
    }
}

/// `StandardLockInternalsDriver.fixForSorting`.
fn fix_for_sorting<'a>(s: &'a str, lock_name: &str) -> &'a str {
    match s.rfind(lock_name) {
        Some(i) => &s[i + lock_name.len()..],
        None => s,
    }
}

/// `DistributedAtomicLong.bytesToValue`.
fn bytes_to_long(d: &[u8]) -> i64 {
    match d.get(..8) {
        Some(b) => i64::from_be_bytes(b.try_into().unwrap()),
        None => 0,
    }
}

/// `CuratorFrameworkFactory.LOCAL_ADDRESS`: the data Curator gives a node
/// created without any.
fn local_address() -> Vec<u8> {
    std::net::UdpSocket::bind("0.0.0.0:0")
        .and_then(|s| {
            s.connect("8.8.8.8:53")?;
            s.local_addr()
        })
        .map(|a| a.ip().to_string())
        .unwrap_or_else(|_| "127.0.0.1".to_owned())
        .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths() {
        assert_eq!(make_path("/a", "locks"), "/a/locks");
        assert_eq!(make_path("/a/", "/b"), "/a/b");
        assert_eq!(
            node_from_path("/a/b/_c_x-lock-0000000001"),
            "_c_x-lock-0000000001"
        );
        assert_eq!(
            fix_for_sorting("_c_0b0a-lock-0000000007", LOCK_NAME),
            "0000000007"
        );
        assert_eq!(bytes_to_long(&5i64.to_be_bytes()), 5);
        assert_eq!(bytes_to_long(&[]), 0);
    }
}
