use chrono::{DateTime, Utc};
use fx_event_bus::{
    Event, EventHandler, EventHandlerRegistry, Publisher,
    listener::listener::Listener,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, PgTransaction};
use std::time::{Duration, Instant};
use tabled::{Style, Table, Tabled};
use textplots::{Chart, Plot, Shape};

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
        _input: ThroughputBenchmarkEvent,
        _polled_at: DateTime<Utc>,
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

#[derive(Tabled)]
struct ReportRow {
    #[tabled(rename = "AVG ms")]
    avg_ms: u128,
    #[tabled(rename = "MIN ms")]
    min_ms: u128,
    #[tabled(rename = "MAX ms")]
    max_ms: u128,
}

pub struct Statistics {
    measurements: Vec<Duration>,
    min: Duration,
    max: Duration,
    sum: Duration,
}

impl Statistics {
    pub fn new(iterations: usize) -> Self {
        Self {
            measurements: Vec::with_capacity(iterations),
            min: Duration::MAX,
            max: Duration::new(0, 0),
            sum: Duration::new(0, 0),
        }
    }

    pub fn push(
        &mut self,
        measurement: Duration,
    ) {
        self.measurements.push(measurement);
        self.min = self.min.min(measurement);
        self.max = self.max.max(measurement);
        self.sum += measurement;
    }

    pub fn plot(&self) {
        let plot = self
            .measurements
            .iter()
            .enumerate()
            .map(|(i, m)| (i as f32, m.as_millis() as f32))
            .collect::<Vec<(f32, f32)>>();

        println!("\n--- Performance Graph (Iteration vs Duration) ---");
        Chart::new(120, 60, 1.0, self.measurements.len() as f32)
            .lineplot(&Shape::Lines(&plot))
            .display();
        println!("--- End Graph ---\n");
    }

    pub fn report(&self) {
        let avg_ms = self.sum.as_millis() / self.measurements.len() as u128;
        let row = ReportRow {
            avg_ms: avg_ms,
            min_ms: self.min.as_millis(),
            max_ms: self.max.as_millis(),
        };
        let table = Table::new(&[row]).with(Style::psql()).to_string();
        println!("{}", table);
    }
}

struct Bencher {
    listener: Listener,
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

    async fn measure(&mut self) -> anyhow::Result<Duration> {
        let now = Instant::now();
        for _ in 0..1000 {
            self.listener.poll().await?;
        }
        Ok(now.elapsed())
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut statistics = Statistics::new(self.iterations);

        for _ in 0..self.iterations {
            self.arrange().await?;
            let measurement = self.measure().await?;
            statistics.push(measurement);
        }

        statistics.plot();
        statistics.report();
        Ok(())
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

    Ok(())
}
