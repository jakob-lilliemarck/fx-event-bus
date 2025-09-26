use std::sync::Arc;

use crate::listener::{ListenerError, listener::Listener};
use tokio::{
    sync::{RwLock, mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

pub async fn run_counter(
    count: Arc<RwLock<usize>>,
    mut rx: mpsc::Receiver<()>,
) {
    while let Some(_) = rx.recv().await {
        let mut lock = count.write().await;
        *lock += 1;
    }
}
pub struct Runner {
    cancel: CancellationToken,
    set: JoinSet<Result<(), ListenerError>>,
    count: Arc<RwLock<usize>>,
}

impl Runner {
    pub fn new() -> Self {
        Self {
            cancel: CancellationToken::new(),
            set: JoinSet::new(),
            count: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn count(&self) -> usize {
        *self.count.read().await
    }

    pub async fn wait_until(
        &self,
        until: usize,
    ) {
        loop {
            let current_count = *self.count.read().await;
            if until <= current_count {
                break;
            }

            // Small delay to prevent busy waiting and allow other tasks to run
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    pub async fn run(
        &mut self,
        mut listener: Listener,
    ) {
        let count = self.count.clone();
        let (tx, rx) = mpsc::channel(100);
        let (ready_tx, ready_rx) = oneshot::channel();

        let cancel = self.cancel.clone();
        self.set.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => {
                    Ok(())
                }
                _ = run_counter(count, rx) => {
                    Ok(())
                }
            }
        });

        let cancel = self.cancel.clone();
        self.set.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => {
                    Ok(())
                }
                result = listener.listen(Some(tx), Some(ready_tx)) => {
                    result
                }
            }
        });

        let _ = ready_rx.await; // Wait for listener to signal it's ready
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub async fn join(
        mut self
    ) -> Result<Vec<Result<(), ListenerError>>, tokio::task::JoinError> {
        let mut results = Vec::new();
        while let Some(result) = self.set.join_next().await {
            match result {
                Ok(listener_result) => results.push(listener_result),
                Err(join_err) => return Err(join_err), // Propagate task infrastructure errors
            }
        }
        Ok(results)
    }
}
