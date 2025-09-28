-- Drop result tables
DROP TABLE IF EXISTS fx_event_bus.attempts_dead;
DROP TABLE IF EXISTS fx_event_bus.attempts_failed;
DROP TABLE IF EXISTS fx_event_bus.attempts_succeeded;

-- Drop event tables (this will drop the trigger automatically)
DROP TABLE IF EXISTS fx_event_bus.events_acknowledged;
DROP TABLE IF EXISTS fx_event_bus.events_unacknowledged;

-- Drop function (now safe since trigger is gone with the table)
DROP FUNCTION IF EXISTS fx_event_bus.notify_event_inserted();

-- Drop schema (will only succeed if empty)
DROP SCHEMA IF EXISTS fx_event_bus;
