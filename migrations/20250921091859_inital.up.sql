CREATE SCHEMA IF NOT EXISTS fx_event_bus;

-- Unacknowledged events table: Small queue, fast operations, always ~1000 rows
CREATE TABLE fx_event_bus.events_unacknowledged (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    hash INTEGER NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ NOT NULL
);

-- Tune autovacuum for high-churn events_unacknowledged table
ALTER TABLE fx_event_bus.events_unacknowledged SET (
    autovacuum_vacuum_scale_factor = 0.05,   -- Vacuum at 5% dead tuples (vs 20% default)
    autovacuum_analyze_scale_factor = 0.02,  -- Analyze at 2% changes (vs 10% default)
    autovacuum_vacuum_cost_delay = 5,        -- Faster vacuuming
    autovacuum_vacuum_cost_limit = 400       -- Higher work rate
);

-- Acknowledged events table: Large archive, write-only during processing
CREATE TABLE fx_event_bus.events_acknowledged (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    hash INTEGER NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    acknowledged_at TIMESTAMPTZ NOT NULL
);

-- Index for unacknowledged queue processing
CREATE INDEX idx_events_unacknowledged_queue
ON fx_event_bus.events_unacknowledged (published_at ASC, id ASC);

-- Create a function to notify on event insert
CREATE OR REPLACE FUNCTION fx_event_bus.notify_event_inserted()
RETURNS TRIGGER AS $$
BEGIN
    -- Notify for new work items (trigger only fires on unacknowledged table)
    PERFORM pg_notify(
        'fx_event_bus',
        json_build_object(
            'id', NEW.id::text,
            'name', NEW.name,
            'hash', NEW.hash,
            'published_at', extract(epoch from NEW.published_at)::bigint
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger only on unacknowledged table (new work items)
CREATE TRIGGER event_inserted_trigger
    AFTER INSERT ON fx_event_bus.events_unacknowledged
    FOR EACH ROW
    EXECUTE FUNCTION fx_event_bus.notify_event_inserted();

-- Successful handling results
CREATE TABLE fx_event_bus.attempts_succeeded (
    event_id UUID NOT NULL PRIMARY KEY,
    attempted_at TIMESTAMPTZ NOT NULL
);

-- Failed handling results, handling may be retried from here.
CREATE TABLE fx_event_bus.attempts_failed (
    id UUID NOT NULL PRIMARY KEY,
    event_id UUID NOT NULL,
    try_earliest TIMESTAMPTZ NOT NULL,
    attempted INTEGER NOT NULL,
    attempted_at TIMESTAMPTZ, -- update when retrying
    error TEXT NOT NULL
);

-- Index for retry queue processing (using latest retry time)
CREATE INDEX idx_attempts_failed_queue
ON fx_event_bus.attempts_failed (try_earliest ASC, id ASC)
WHERE attempted_at IS NULL;

-- Index for cleanup (using event_id)
CREATE INDEX idx_attempts_failed_cleanup
ON fx_event_bus.attempts_failed (event_id)
WHERE attempted_at IS NOT NULL;

-- Handling attempts that exhausted retries. The "dead letter queue"
CREATE TABLE fx_event_bus.attempts_dead (
    event_id UUID NOT NULL PRIMARY KEY,
    dead_at TIMESTAMPTZ NOT NULL,
    attempted_at TIMESTAMPTZ[] NOT NULL,
    errors TEXT[] NOT NULL
);
