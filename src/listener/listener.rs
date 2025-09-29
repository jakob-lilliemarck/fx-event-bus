use super::poll_control::PollControlStream;
use crate::EventHandlerRegistry;
use chrono::Utc;
use futures::StreamExt;
use sqlx::{PgPool, postgres::PgListener};
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

const EVENTS_CHANNEL: &str = "fx_event_bus";

pub struct Listener {
    pub(super) pool: PgPool,
    pub(super) registry: EventHandlerRegistry,
    pub(super) max_attempts: i32,
    pub(super) retry_duration: Duration,
    /// The number of events processed by this listener since start
    count: usize,
}

pub struct Handled {
    /// The ID of the event that was handled, if any
    pub id: Option<Uuid>,
    /// The number of events processed by this listener since start
    pub count: usize,
}

impl Listener {
    pub fn new(
        pool: PgPool,
        registry: EventHandlerRegistry,
    ) -> Self {
        Listener {
            pool,
            registry,
            max_attempts: 3,
            retry_duration: Duration::from_millis(15_000),
            count: 0,
        }
    }

    pub fn with_max_attempts(
        mut self,
        max_attempts: u16,
    ) -> Self {
        self.max_attempts = max_attempts as i32;
        self
    }

    pub fn with_retry_duration(
        mut self,
        retry_duration: Duration,
    ) -> Self {
        self.retry_duration = retry_duration;
        self
    }

    #[tracing::instrument(
        skip(self, tx),
        fields(
            max_attempts = self.max_attempts,
            retry_duration_ms = self.retry_duration.as_millis(),
            has_channel = tx.is_some()
        ),
        err
    )]
    pub async fn listen(
        &mut self,
        tx: Option<mpsc::Sender<Handled>>,
    ) -> Result<(), super::ListenerError> {
        let mut control = PollControlStream::new(
            Duration::from_millis(500),
            Duration::from_millis(2_500),
        );

        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(EVENTS_CHANNEL).await?;
        let pg_stream = listener.into_stream();

        control.with_pg_stream(pg_stream);

        while let Some(result) = control.next().await {
            if let Err(err) = result {
                tracing::warn!(message="The control stream returned an error", error=?err)
            }
            match self.poll(Utc::now()).await {
                Ok(handled) => {
                    // Reset failed polling attempts on success
                    control.reset_failed_attempts();

                    // If an event was handled, override the next wait
                    if let Some(_) = handled {
                        self.count += 1;
                        control.set_poll();
                    }

                    // If a channel is provided, inform of the result of the poll
                    if let Some(tx) = &tx {
                        if let Err(_) = tx
                            .send(Handled {
                                id: handled,
                                count: self.count,
                            })
                            .await
                        {
                            tracing::warn!(
                                "Listener failed to broadcast the poll result"
                            );
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        message = "Polling for events returned an error",
                        error = ?err
                    );
                    control.increment_failed_attempts();
                }
            }
        }

        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::Event;
//     use crate::test_utils::{
//         HandlerAlpha, HandlerBeta, HandlerGamma, Runner, SharedHandlerState,
//         TestEvent, get_event_failed,
//     };
//     use sqlx::PgTransaction;
//     use std::sync::Arc;
//     use std::sync::Once;
//     use tokio::sync::Mutex;
//     use tracing::Level;
//     use tracing_subscriber;
//
//     static INIT: Once = Once::new();
//
//     fn init_tracing() {
//         INIT.call_once(|| {
//             tracing_subscriber::fmt()
//                 .with_test_writer()
//                 .with_max_level(Level::DEBUG)
//                 .init();
//         });
//     }
//
//     #[sqlx::test(migrations = "./migrations")]
//     async fn it_handles_a_single_event(
//         pool: sqlx::PgPool
//     ) -> anyhow::Result<()> {
//         init_tracing();
//
//         // Pass none to not fail
//         let state = Arc::new(Mutex::new(SharedHandlerState::new(None)));
//
//         let handler_alpha = HandlerAlpha::new(&state);
//
//         let mut registry = EventHandlerRegistry::new();
//         registry.with_handler(handler_alpha);
//
//         let listener = Listener::new(pool.clone(), registry);
//
//         let mut runner = Runner::new();
//         runner.run(listener).await;
//
//         let tx = pool.begin().await?;
//
//         let mut publisher = crate::Publisher::new(tx);
//
//         publisher
//             .publish(TestEvent {
//                 a_string: "a_string".to_string(),
//                 a_number: 42,
//                 a_bool: true,
//             })
//             .await?;
//
//         let tx: PgTransaction<'_> = publisher.into();
//
//         tx.commit().await?;
//
//         runner.wait_until(1).await;
//         runner.cancel();
//         runner.join().await?;
//
//         let lock = state.lock().await;
//         assert_eq!(lock.count(), 1);
//         assert_eq!(
//             lock.seen,
//             &[(TestEvent::HASH, TestEvent::NAME.to_string(), true)]
//         );
//
//         Ok(())
//     }
//
//     #[sqlx::test(migrations = "./migrations")]
//     async fn it_does_not_acknowledge_on_failure(
//         pool: sqlx::PgPool
//     ) -> anyhow::Result<()> {
//         init_tracing();
//
//         // Pass none to not fail
//         let state = Arc::new(Mutex::new(SharedHandlerState::new(Some(
//             TestEvent::HASH,
//         ))));
//
//         let handler_alpha = HandlerAlpha::new(&state);
//
//         let mut registry = EventHandlerRegistry::new();
//         registry.with_handler(handler_alpha);
//
//         let listener = Listener::new(pool.clone(), registry);
//
//         let mut runner = Runner::new();
//         runner.run(listener).await;
//
//         let tx = pool.begin().await?;
//         let mut publisher = crate::Publisher::new(tx);
//         let published = publisher
//             .publish(TestEvent {
//                 a_string: "a_string".to_string(),
//                 a_number: 42,
//                 a_bool: true,
//             })
//             .await?;
//         let tx: PgTransaction<'_> = publisher.into();
//         tx.commit().await?;
//
//         runner.wait_until(1).await;
//         runner.cancel();
//         runner.join().await?;
//
//         let lock = state.lock().await;
//         assert_eq!(lock.count(), 1);
//         assert_eq!(
//             lock.seen,
//             &[(TestEvent::HASH, TestEvent::NAME.to_string(), false)]
//         );
//
//         // Assert that the event was acknowledged and moved to failed
//         let event = get_event_failed(&pool, published.id).await?;
//         assert!(event.len() == 1);
//         assert_eq!(event[0].id, published.id);
//         assert_eq!(event[0].name, published.name);
//         assert_eq!(event[0].hash, published.hash);
//         assert_eq!(event[0].payload, published.payload);
//         Ok(())
//     }
//
//     #[sqlx::test(migrations = "./migrations")]
//     async fn it_handles_an_event_with_multiple_handlers(
//         pool: sqlx::PgPool
//     ) -> anyhow::Result<()> {
//         init_tracing();
//
//         // Pass none to not fail
//         let state = Arc::new(Mutex::new(SharedHandlerState::new(Some(
//             TestEvent::HASH,
//         ))));
//
//         let handler_alpha = HandlerAlpha::new(&state);
//         let handler_beta = HandlerBeta::new(&state);
//
//         let mut registry = EventHandlerRegistry::new();
//         registry.with_handler(handler_alpha);
//         registry.with_handler(handler_beta);
//
//         let listener = Listener::new(pool.clone(), registry);
//
//         let mut runner = Runner::new();
//         runner.run(listener).await;
//
//         let tx = pool.begin().await?;
//         let mut publisher = crate::Publisher::new(tx);
//         let published = publisher
//             .publish(TestEvent {
//                 a_string: "a_string".to_string(),
//                 a_number: 42,
//                 a_bool: true,
//             })
//             .await?;
//         let tx: PgTransaction<'_> = publisher.into();
//         tx.commit().await?;
//
//         runner.wait_until(1).await;
//         runner.cancel();
//         runner.join().await?;
//
//         let lock = state.lock().await;
//         assert_eq!(lock.count(), 1);
//         assert_eq!(
//             lock.seen,
//             &[(TestEvent::HASH, TestEvent::NAME.to_string(), false)]
//         );
//
//         // Assert that the event was acknowledged and moved to failed
//         let event = get_event_failed(&pool, published.id).await?;
//         assert!(event.len() == 1);
//         assert_eq!(event[0].id, published.id);
//         assert_eq!(event[0].name, published.name);
//         assert_eq!(event[0].hash, published.hash);
//         assert_eq!(event[0].payload, published.payload);
//         Ok(())
//     }
//
//     #[sqlx::test(migrations = "./migrations")]
//     async fn it_skips_locked_rows_during_poll(
//         pool: sqlx::PgPool
//     ) -> anyhow::Result<()> {
//         let tx = pool.begin().await?;
//         let mut publisher = crate::Publisher::new(tx);
//         publisher
//             .publish(TestEvent {
//                 a_string: "a_string".to_string(),
//                 a_number: 42,
//                 a_bool: true,
//             })
//             .await?;
//         let tx: PgTransaction<'_> = publisher.into();
//         tx.commit().await?;
//
//         let handler_gamma = HandlerGamma::new(pool.clone());
//         let mut registry = EventHandlerRegistry::new();
//         registry.with_handler(handler_gamma);
//
//         let listener = Listener::new(pool.clone(), registry);
//         listener.poll().await?;
//
//         Ok(())
//     }
// }
