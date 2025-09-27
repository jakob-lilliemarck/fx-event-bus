use futures::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

type PgStream = Pin<
    Box<
        dyn Stream<Item = Result<sqlx::postgres::PgNotification, sqlx::Error>>
            + Send,
    >,
>;

pub struct PollControlStream {
    pg_stream: Option<PgStream>,
    failed_attempts: u32,
    polled_at: Instant,
    duration: Duration,
    duration_max: Duration,
    poll: bool,
}

impl PollControlStream {
    pub fn new(
        duration: Duration,
        duration_max: Duration,
    ) -> Self {
        Self {
            pg_stream: None,
            duration,
            duration_max,
            failed_attempts: 0,
            polled_at: Instant::now(),
            poll: true, // The initial duration is overridden
        }
    }

    pub fn with_pg_stream(
        &mut self,
        pg_stream: impl Stream<
            Item = Result<sqlx::postgres::PgNotification, sqlx::Error>,
        > + Unpin
        + Send
        + 'static,
    ) {
        self.pg_stream = Some(Box::pin(pg_stream))
    }

    pub fn increment_failed_attempts(&mut self) {
        self.failed_attempts += 1;
    }

    pub fn reset_failed_attempts(&mut self) {
        self.failed_attempts = 0;
    }

    pub fn set_poll(&mut self) {
        self.poll = true
    }

    fn wake_in(
        cx: &mut Context<'_>,
        duration: Duration,
    ) {
        let waker = cx.waker().clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            waker.wake();
        });
    }

    fn backoff(&self) -> Duration {
        let backoff = self.duration * 2_u32.pow(self.failed_attempts);
        backoff.min(self.duration_max)
    }
}

impl Stream for PollControlStream {
    type Item = Result<bool, sqlx::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let slf = self.get_mut();

        // check if there were failed attempts
        if slf.failed_attempts > 0 {
            if slf.polled_at.elapsed() > slf.backoff() {
                slf.polled_at = Instant::now();
                return Poll::Ready(Some(Ok(true)));
            } else {
                // Schedule wakeup after backoff period
                let remaining = slf.backoff() - slf.polled_at.elapsed();
                Self::wake_in(cx, remaining);
                return Poll::Pending;
            }
        }

        // check the poll flag
        if slf.poll {
            // set it back to false
            slf.poll = false;
            slf.polled_at = Instant::now();
            return Poll::Ready(Some(Ok(true)));
        }

        // if there is a notification stream, check for notifications
        if let Some(ref mut pg_stream) = slf.pg_stream {
            match pg_stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(_))) => {
                    // received a Pg notification
                    slf.polled_at = Instant::now();
                    return Poll::Ready(Some(Ok(true)));
                }
                Poll::Ready(Some(Err(err))) => {
                    // forward any database errors
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) => {
                    // ignore ended stream
                }
                Poll::Pending => {
                    // ignore pending state
                }
            }
        }

        // check if enough time elapsed
        if slf.polled_at.elapsed() > slf.duration {
            slf.polled_at = Instant::now();
            return Poll::Ready(Some(Ok(true)));
        } else {
            // Schedule wakeup for duration
            let remaining = slf.duration - slf.polled_at.elapsed();
            Self::wake_in(cx, remaining);
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_backoff() {
        let duration = Duration::from_millis(5);

        let mut stream =
            PollControlStream::new(duration, Duration::from_millis(20));

        let iterations = 3;
        // 0 waits 0ms,  0ms
        // 1 waits 10ms, 10ms
        // 2 waits 20ms, 30ms
        let mut n = 0;
        let now = Instant::now();
        while let Some(_) = stream.next().await {
            // increment the failed count on each iteration
            stream.increment_failed_attempts();
            if n == iterations - 1 {
                break;
            }
            n += 1;
        }

        let elapsed = now.elapsed();
        let duration = duration * 2 + duration * 4;
        assert!(
            elapsed >= duration,
            "Expected elapsed to exceed duration {:?}ms, got {:?}ms",
            duration.as_millis(),
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_poll_duration_override() {
        let duration = Duration::from_millis(5);

        let mut stream =
            PollControlStream::new(duration, Duration::from_millis(20));

        stream.set_poll();

        let now = Instant::now();

        stream.next().await;

        let elapsed = now.elapsed();
        assert!(
            elapsed < duration,
            "Expected elapsed to be smaller than duration"
        );
    }
}
