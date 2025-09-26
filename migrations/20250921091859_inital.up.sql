CREATE SCHEMA IF NOT EXISTS fx_event_bus;

CREATE TABLE fx_event_bus.events (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    hash INTEGER NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    acknowledged BOOLEAN NOT NULL,
    acknowledged_at TIMESTAMPTZ
);

-- Key indexes
CREATE INDEX idx_events_unacknowledged_queue
ON fx_event_bus.events (published_at ASC, id ASC)
WHERE acknowledged = FALSE;  -- Partial index!

CREATE INDEX idx_events_acknowledged
ON fx_event_bus.events (acknowledged_at DESC)
WHERE acknowledged = TRUE;   -- For analytics

-- Results table to track event processing outcomes
CREATE TYPE fx_event_bus.event_result AS ENUM ('succeeded', 'failed');

CREATE TABLE fx_event_bus.results (
    id UUID NOT NULL,
    event_id UUID NOT NULL,
    status fx_event_bus.event_result NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    error_message TEXT,
    PRIMARY KEY (id, status),
    FOREIGN KEY (event_id) REFERENCES fx_event_bus.events(id)
) PARTITION BY LIST (status);

-- Create partitions for results
CREATE TABLE fx_event_bus.results_failed PARTITION OF fx_event_bus.results
FOR VALUES IN ('failed');

CREATE TABLE fx_event_bus.results_succeeded PARTITION OF fx_event_bus.results
FOR VALUES IN ('succeeded');

-- Create a function to notify on event insert
CREATE OR REPLACE FUNCTION fx_event_bus.notify_event_inserted()
RETURNS TRIGGER AS $$
BEGIN
    -- Only notify for unacknowledged events (new work items)
    IF NEW.acknowledged = FALSE THEN
        PERFORM pg_notify(
            'fx_event_bus',
            json_build_object(
                'id', NEW.id::text,
                'name', NEW.name,
                'hash', NEW.hash,
                'published_at', extract(epoch from NEW.published_at)::bigint
            )::text
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger on events table
CREATE TRIGGER event_inserted_trigger
    AFTER INSERT ON fx_event_bus.events
    FOR EACH ROW
    EXECUTE FUNCTION fx_event_bus.notify_event_inserted();
