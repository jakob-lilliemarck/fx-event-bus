-- Drop trigger first (depends on function and table)
DROP TRIGGER IF EXISTS event_inserted_trigger ON fx_event_bus.events;

-- Drop function
DROP FUNCTION IF EXISTS fx_event_bus.notify_event_inserted();

-- Drop results partitions (must be dropped before parent table)
DROP TABLE IF EXISTS fx_event_bus.results_succeeded;
DROP TABLE IF EXISTS fx_event_bus.results_failed;

-- Drop results parent table
DROP TABLE IF EXISTS fx_event_bus.results;

-- Drop events partitions (must be dropped before parent table)
DROP TABLE IF EXISTS fx_event_bus.events_acknowledged;
DROP TABLE IF EXISTS fx_event_bus.events_unacknowledged;

-- Drop events parent table
DROP TABLE IF EXISTS fx_event_bus.events;

-- Drop custom types
DROP TYPE IF EXISTS fx_event_bus.event_result;
DROP TYPE IF EXISTS fx_event_bus.event_status;

-- Drop schema (will only succeed if empty)
DROP SCHEMA IF EXISTS fx_event_bus;
