//! The event loops connections run on, as Netty's: each connection an
//! acceptor takes runs on one of its loops, a single-threaded runtime,
//! chosen in turn (`NettyAcceptor`'s event loop group of `remotingThreads`
//! threads), with what it spawns. On the work-stealing runtime a delivery
//! moved between threads at each task it woke (the producer's connection,
//! the queue, the consumer's writer), which cost more than the work.
//!
//! The loops are the process's and are never stopped: what a connection
//! spawned (a queue it created, a session's tasks) stays on its loop after
//! the connection, and its acceptor, are gone. An acceptor of `n` threads
//! uses the first `n`, each started when a connection first needs it, as a
//! Netty event loop's thread starts with its first task.

use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

static LOOPS: Mutex<Vec<tokio::runtime::Handle>> = Mutex::new(Vec::new());

/// An acceptor's share of the loops.
pub(crate) struct EventLoops {
    n: usize,
    next: AtomicUsize,
}

/// `NettyAcceptor`'s `remotingThreads`: `nioRemotingThreads`, then
/// `remotingThreads`; 0 is Netty's default of two a processor, and a
/// negative count the IllegalArgumentException
/// `MultithreadEventExecutorGroup` throws. Unless given it is one a
/// processor, where Artemis has three: Saranga's loops run the queues
/// their connections create as well, and more loops than processors only
/// had them wake each other more often (with 100 queues, a third more CPU
/// a message and a higher p50).
pub(crate) fn remoting_threads(
    params: &std::collections::HashMap<String, String>,
) -> std::io::Result<usize> {
    let cpus = std::thread::available_parallelism().map_or(1, |n| n.get()) as i64;
    // ConfigurationHelper.getIntProperty: Integer.parseInt, the default
    // (with a warning) when the value is not a number.
    let int = |key: &str, default: i64| match params.get(key) {
        None => default,
        Some(v) => v.parse::<i32>().map_or_else(
            |_| {
                tracing::warn!(
                    "AMQ212043: Property {key} must be an Integer, it is java.lang.String"
                );
                default
            },
            i64::from,
        ),
    };
    let n = int("remotingThreads", int("nioRemotingThreads", cpus));
    match n {
        0 => Ok((cpus * 2).max(1) as usize),
        n if n < 0 => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("nThreads: {n} (expected: >= 0)"),
        )),
        n => Ok(n as usize),
    }
}

impl EventLoops {
    /// The first `n` loops (at least one).
    pub(crate) fn new(n: usize) -> Self {
        Self {
            n: n.max(1),
            next: AtomicUsize::new(0),
        }
    }

    /// Loop `i`, started if it is not yet.
    fn get(i: usize) -> tokio::runtime::Handle {
        let mut all = LOOPS.lock().unwrap_or_else(|e| e.into_inner());
        while all.len() <= i {
            let next = start(all.len());
            all.push(next);
        }
        all[i].clone()
    }

    /// Runs a connection's task, given its socket, on the next loop.
    pub(crate) fn spawn<F, Fut>(&self, stream: tokio::net::TcpStream, f: F)
    where
        F: FnOnce(tokio::net::TcpStream) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        // A socket is registered with one runtime's reactor: taken out of
        // this one's to be registered with the loop's.
        let std = match stream.into_std() {
            Ok(s) => s,
            Err(e) => {
                tracing::debug!("connection not moved to its event loop: {e}");
                return;
            }
        };
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.n;
        Self::get(i).spawn(async move {
            match tokio::net::TcpStream::from_std(std) {
                Ok(stream) => f(stream).await,
                Err(e) => tracing::debug!("connection not moved to its event loop: {e}"),
            }
        });
    }
}

fn start(i: usize) -> tokio::runtime::Handle {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name(format!("saranga-remoting-{i}"))
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("an event loop's runtime");
            tx.send(rt.handle().clone()).ok();
            rt.block_on(std::future::pending::<()>());
        })
        .expect("an event loop's thread");
    rx.recv().expect("an event loop's handle")
}

#[cfg(test)]
mod tests {
    use super::remoting_threads;
    use std::collections::HashMap;

    fn threads(params: &[(&str, &str)]) -> std::io::Result<usize> {
        let p: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        remoting_threads(&p)
    }

    #[test]
    fn remoting_threads_as_netty_acceptor_reads_them() {
        let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
        assert_eq!(threads(&[]).unwrap(), cpus);
        assert_eq!(threads(&[("nioRemotingThreads", "5")]).unwrap(), 5);
        // remotingThreads wins over nioRemotingThreads.
        assert_eq!(
            threads(&[("nioRemotingThreads", "5"), ("remotingThreads", "2")]).unwrap(),
            2
        );
        // Not a number: the default.
        assert_eq!(threads(&[("remotingThreads", "many")]).unwrap(), cpus);
        // Netty's own default for 0.
        assert_eq!(threads(&[("remotingThreads", "0")]).unwrap(), cpus * 2);
        let e = threads(&[("remotingThreads", "-1")]).unwrap_err();
        assert_eq!(e.to_string(), "nThreads: -1 (expected: >= 0)");
    }
}
