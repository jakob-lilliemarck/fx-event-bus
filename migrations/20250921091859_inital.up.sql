CREATE SCHEMA IF NOT EXISTS fx_event_bus;

CREATE TYPE fx_event_bus.event_status AS ENUM ('unacknowledged', 'acknowledged', 'failed');

CREATE TABLE fx_event_bus.events (
    id UUID NOT NULL,
    name TEXT NOT NULL,
    hash INTEGER NOT NULL,
    status fx_event_bus.event_status NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    acknowledged_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    PRIMARY KEY (id, status)
) PARTITION BY LIST (status);

-- Create partitions
CREATE TABLE fx_event_bus.events_unacknowledged PARTITION OF fx_event_bus.events
FOR VALUES IN ('unacknowledged');

CREATE TABLE fx_event_bus.events_acknowledged PARTITION OF fx_event_bus.events
FOR VALUES IN ('acknowledged');

CREATE TABLE fx_event_bus.events_failed PARTITION OF fx_event_bus.events
FOR VALUES IN ('failed');

-- Create a function to notify on event insert
CREATE OR REPLACE FUNCTION fx_event_bus.notify_event_inserted()
RETURNS TRIGGER AS $$
BEGIN
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

-- Create trigger on events table
CREATE TRIGGER event_inserted_trigger
    AFTER INSERT ON fx_event_bus.events
    FOR EACH ROW
    EXECUTE FUNCTION fx_event_bus.notify_event_inserted();
