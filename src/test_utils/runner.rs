use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::listener::{ListenerError, listener::Listener};

pub struct Runner {
    cancel: CancellationToken,
    set: JoinSet<Result<(), ListenerError>>,
}

impl Runner {
    pub fn new() -> Self {
        Self {
            cancel: CancellationToken::new(),
            set: JoinSet::new(),
        }
    }

    pub fn run(
        &mut self,              // Need mut to spawn on JoinSet
        mut listener: Listener, // Need mut for listener.listen()
    ) {
        let cancel = self.cancel.clone();
        self.set.spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => {
                    Ok(())
                }
                result = listener.listen() => {
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
