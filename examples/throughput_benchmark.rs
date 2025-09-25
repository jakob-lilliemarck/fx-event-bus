use std::time::{Duration, Instant};

use fx_event_bus::{
    Event, EventHandler, EventHandlerRegistry, Publisher,
    listener::listener::Listener,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, PgTransaction};

#[derive(Clone, Serialize, Deserialize)]
struct ThroughputBenchmarkEvent {
    index: usize,
}

impl Event for ThroughputBenchmarkEvent {
    const NAME: &'static str = "ThroughputBenchmarkEvent";
}

struct ThroughputBenchmarkHandler;

impl EventHandler<ThroughputBenchmarkEvent> for ThroughputBenchmarkHandler {
    fn handle<'a>(
        &'a self,
        _: ThroughputBenchmarkEvent,
        tx: PgTransaction<'a>,
    ) -> futures::future::BoxFuture<
        'a,
        (
            PgTransaction<'a>,
            Result<(), fx_event_bus::EventHandlingError>,
        ),
    > {
        Box::pin(async move { (tx, Ok(())) })
    }
}

struct Bencher {
    listener: Listener,
    measurements: Vec<Duration>,
    iterations: usize,
    pool: PgPool,
}

impl Bencher {
    fn new(
        listener: Listener,
        iterations: usize,
        pool: PgPool,
    ) -> Self {
        Bencher {
            listener,
            measurements: Vec::with_capacity(iterations),
            iterations,
            pool,
        }
    }

    async fn arrange(&self) -> anyhow::Result<()> {
        let tx = self.pool.begin().await?;
        let mut publisher = Publisher::new(tx);
        publisher
            .publish_many(
                &(0..1000)
                    .map(|i| ThroughputBenchmarkEvent { index: i })
                    .collect::<Vec<ThroughputBenchmarkEvent>>(),
            )
            .await?;
        let tx: PgTransaction<'_> = publisher.into();
        tx.commit().await?;
        Ok(())
    }

    async fn measure(&mut self) -> anyhow::Result<()> {
        let now = Instant::now();
        for _ in 0..1000 {
            self.listener.poll().await?;
        }
        let elapsed = now.elapsed();
        self.measurements.push(elapsed);
        Ok(())
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        for _ in 0..self.iterations {
            self.arrange().await?;
            self.measure().await?;
        }
        Ok(())
    }

    fn report(&self) {
        let total_time = self.measurements.iter().sum::<Duration>();
        let average_time = total_time / self.measurements.len() as u32;
        let max_time = self.measurements.iter().max().unwrap();
        let min_time = self.measurements.iter().min().unwrap();
        println!(
            "Total time: {:?}, {:?} messages/second ",
            total_time,
            1000.0 / total_time.as_secs_f64()
        );
        println!(
            "Average time: {:?}, {:?} messages/second",
            average_time,
            1000.0 / average_time.as_secs_f64()
        );
        println!(
            "Min time: {:?}, {:?} messages/second",
            min_time,
            1000.0 / min_time.as_secs_f64()
        );
        println!(
            "Max time: {:?}, {:?} messages/second",
            max_time,
            1000.0 / max_time.as_secs_f64()
        );
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;

    let mut registry = EventHandlerRegistry::new();
    registry.with_handler(ThroughputBenchmarkHandler);

    let listener = Listener::new(pool.clone(), registry);
    let mut bencher = Bencher::new(listener, 100, pool);

    bencher.run().await?;
    bencher.report();

    Ok(())
}
