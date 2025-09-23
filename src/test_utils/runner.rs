use std::sync::Arc;

use crate::listener::{ListenerError, listener::Listener};
use tokio::{
    sync::{RwLock, mpsc},
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
            if until <= *self.count.read().await {
                break;
            }
        }
    }

    pub fn run(
        &mut self,              // Need mut to spawn on JoinSet
        mut listener: Listener, // Need mut for listener.listen()
    ) {
        let count = self.count.clone();
        let (tx, rx) = mpsc::channel(100);

        let cancel = self.cancel.clone();
        self.set.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => {
                    Ok(())
                }
                _ = run_counter(count, rx) => {
                    tracing::info!("Counter finished");
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
                result = listener.listen(Some(tx)) => {
                    result
                }
            }
        });
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
