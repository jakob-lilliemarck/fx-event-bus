-- Drop trigger first (depends on function and table)
DROP TRIGGER IF EXISTS event_inserted_trigger ON fx_event_bus.events;

-- Drop function
DROP FUNCTION IF EXISTS fx_event_bus.notify_event_inserted();

-- Drop partitions (must be dropped before parent table)
DROP TABLE IF EXISTS fx_event_bus.events_failed;
DROP TABLE IF EXISTS fx_event_bus.events_acknowledged;
DROP TABLE IF EXISTS fx_event_bus.events_unacknowledged;

-- Drop parent table
DROP TABLE IF EXISTS fx_event_bus.events;

-- Drop custom type
DROP TYPE IF EXISTS fx_event_bus.event_status;

-- Drop schema (will only succeed if empty)
DROP SCHEMA IF EXISTS fx_event_bus;
