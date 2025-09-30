# fx-event-bus
A simple event bus for monoliths where every node can handle any event. Designed to support loose coupling between independent parts of an application domain.

## What it is
 - Bus-based architecture for monolithic apps.
 - Uses Postgres FOR UPDATE SKIP LOCKED for concurrent polling and exactly-once delivery.
 - Provides event handlers with typed, deserialized input.
 - *Fully safe and statically type-checked*: no unsafe code, no interior mutability.
 - Handles ~2k events/sec on a single DB connection; scale horizontally by adding more servers.

## What it is not
 - Not a casual pub/sub library. Nodes must fully handle an event once acknowledged, or risk losing it.
 - Not designed for microservices where nodes handle events differently.
