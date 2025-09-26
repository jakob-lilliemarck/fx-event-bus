CREATE SCHEMA IF NOT EXISTS fx_event_bus;

-- Unacknowledged events table: Small queue, fast operations, always ~1000 rows
CREATE TABLE fx_event_bus.events_unacknowledged (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    hash INTEGER NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ NOT NULL
);

-- Tune autovacuum for high-churn unacknowledged table
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

-- Tune autovacuum for insert-heavy acknowledged table
ALTER TABLE fx_event_bus.events_acknowledged SET (
    autovacuum_vacuum_scale_factor = 0.1,    -- Less frequent vacuum (mostly INSERTs)
    autovacuum_analyze_scale_factor = 0.05,  -- More frequent analyze for query planning
    autovacuum_vacuum_cost_delay = 10,       -- Less aggressive vacuum
    fillfactor = 90                          -- Leave 10% free space for updates
);

-- Index for unacknowledged queue processing
CREATE INDEX idx_events_unacknowledged_queue
ON fx_event_bus.events_unacknowledged (published_at ASC, id ASC);

-- Indexes for acknowledged events (analytics)
CREATE INDEX idx_events_acknowledged_acked_at
ON fx_event_bus.events_acknowledged (acknowledged_at DESC);

CREATE INDEX idx_events_acknowledged_published_at
ON fx_event_bus.events_acknowledged (published_at DESC);

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

-- Results table to track event processing outcomes
CREATE TYPE fx_event_bus.event_result AS ENUM ('succeeded', 'failed');

CREATE TABLE fx_event_bus.results (
    id UUID NOT NULL,
    event_id UUID NOT NULL,
    status fx_event_bus.event_result NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    error_message TEXT,
    PRIMARY KEY (id, status)
    -- Note: No foreign key constraint since event could be in either table
) PARTITION BY LIST (status);

-- Create partitions for results
CREATE TABLE fx_event_bus.results_failed PARTITION OF fx_event_bus.results
FOR VALUES IN ('failed');

CREATE TABLE fx_event_bus.results_succeeded PARTITION OF fx_event_bus.results
FOR VALUES IN ('succeeded');
