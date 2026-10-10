//! Persistence: the bindings and message journals and the operations the
//! broker stores in them. Ported from `AbstractJournalStorageManager`,
//! `JournalStorageManager` and `OperationContextImpl`.

use saranga_journal::{Completion, JournalError};
use tokio::sync::oneshot;

/// A journal write the caller may wait for before answering the client
/// (an `OperationContext` line-up).
#[derive(Debug)]
pub struct Pending(oneshot::Receiver<Result<(), JournalError>>);

impl Pending {
    /// A journal completion and the handle that waits for it.
    pub fn new() -> (Completion, Pending) {
        let (tx, rx) = oneshot::channel();
        (
            Box::new(move |r| {
                let _ = tx.send(r);
            }),
            Pending(rx),
        )
    }

    pub async fn wait(self) -> Result<(), JournalError> {
        self.0
            .await
            .unwrap_or_else(|_| Err(JournalError::IllegalState("journal stopped".into())))
    }
}
