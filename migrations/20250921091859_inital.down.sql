-- Drop trigger first (depends on function and table)
DROP TRIGGER IF EXISTS event_inserted_trigger ON fx_event_bus.events_unacknowledged;

-- Drop function
DROP FUNCTION IF EXISTS fx_event_bus.notify_event_inserted();

-- Drop results partitions (must be dropped before parent table)
DROP TABLE IF EXISTS fx_event_bus.results_succeeded;
DROP TABLE IF EXISTS fx_event_bus.results_failed;

-- Drop results parent table
DROP TABLE IF EXISTS fx_event_bus.results;

-- Drop indexes (will be dropped with tables, but explicit is clearer)
DROP INDEX IF EXISTS fx_event_bus.idx_events_acknowledged_published_at;
DROP INDEX IF EXISTS fx_event_bus.idx_events_acknowledged_acked_at;
DROP INDEX IF EXISTS fx_event_bus.idx_events_unacknowledged_queue;

-- Drop separate event tables
DROP TABLE IF EXISTS fx_event_bus.events_acknowledged;
DROP TABLE IF EXISTS fx_event_bus.events_unacknowledged;

-- Drop custom types
DROP TYPE IF EXISTS fx_event_bus.event_result;

-- Drop schema (will only succeed if empty)
DROP SCHEMA IF EXISTS fx_event_bus;
