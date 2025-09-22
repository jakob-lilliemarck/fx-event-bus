# Listener Test Plan

## Overview
This document outlines the test strategy for the `listener.rs` module in the fx-event-bus crate. Tests focus on runtime behaviors that cannot be verified by the type system, particularly around database transactions, concurrency control, and error handling.

## Test Categories

### Core Integration Tests

#### 1. Event Processing Happy Path ✅ (Implemented)
- **Test**: `it_publishes_evens` 
- **Purpose**: Verify end-to-end event processing works correctly
- **Flow**: Publish event → PgNotify → acknowledge → handle → commit
- **Assertions**: Event appears in handler state after processing
- **Status**: Complete - covers basic integration

### Critical Runtime Behavior Tests

#### 2. Handler Failure Causes Event to be Marked Failed 🎯 Priority 1
- **Test**: `test_handler_failure_marks_event_failed`
- **Purpose**: Verify error handling marks events as failed instead of reprocessing
- **Setup**: Create handler that always returns `EventHandlingError`
- **Flow**: Publish event → handler fails → event marked as 'failed' status → transaction commits
- **Assertions**: 
  - Event status becomes 'failed'
  - Event has `failed_at` timestamp
  - Event doesn't get reprocessed
- **Why Critical**: Prevents infinite retry loops and lost events

#### 3. Concurrent Listener Isolation 🎯 Priority 2  
- **Test**: `test_concurrent_listener_isolation`
- **Purpose**: Verify `FOR UPDATE SKIP LOCKED` prevents duplicate processing
- **Setup**: Two listeners running simultaneously
- **Flow**: Publish one event → both listeners poll → only one processes it
- **Assertions**:
  - Only one handler receives the event
  - Other listener's poll returns `Ok(())` (no events found)
- **Why Critical**: Prevents duplicate processing in multi-instance deployments

#### 4. Mixed Handler Success/Failure Rollback 🎯 Priority 3
- **Test**: `test_mixed_handler_rollback`
- **Purpose**: Verify transaction atomicity with multiple handlers per event
- **Setup**: Event type with handler group containing both success/fail handlers
- **Flow**: Event → handler1 succeeds, handler2 fails → entire transaction rolls back
- **Assertions**:
  - Event remains 'unacknowledged' (transaction rolled back)
  - No side effects from successful handler are persisted
  - Event available for reprocessing
- **Why Critical**: Ensures atomicity - partial success doesn't corrupt state

### Edge Case Tests

#### 5. Empty Queue Handling 🔄 Priority 4
- **Test**: `test_empty_queue_poll`
- **Purpose**: Verify graceful handling when no events are available
- **Setup**: Clean database with no events
- **Flow**: Call `poll()` directly → should return immediately
- **Assertions**: Returns `Ok(())` without blocking or errors
- **Why Important**: Ensures listener doesn't hang in empty queue scenarios

#### 6. Near-FIFO Ordering Documentation 📝 Priority 5
- **Test**: `test_near_fifo_ordering`
- **Purpose**: Document expected ordering behavior under normal conditions
- **Setup**: Publish multiple events with small delays
- **Flow**: Process events → record processing order
- **Assertions**: Events are *usually* processed in chronological order
- **Note**: This is documentation, not strict requirement - test should allow some reordering
- **Why Valuable**: Documents expected behavior for users

## Test Utilities Needed

Based on the current test structure, we need these utilities:

### Event Creation Utilities
```rust
// Helper for creating test events with different characteristics
fn create_test_event(message: &str, value: i32) -> TestEvent
fn create_failing_test_event() -> FailingTestEvent  // Always fails in handler
```

### Handler Utilities  
```rust
// Handler that always fails
struct FailingHandler<E> { ... }

// Handler that tracks call count/timing
struct TrackingHandler<E> {
    calls: Arc<Mutex<Vec<(TestEvent, Instant)>>>,
    ...
}

// Handler that can be configured to fail after N successes
struct ConditionalFailHandler<E> {
    fail_after: usize,
    call_count: Arc<AtomicUsize>,
    ...
}
```

### Test Setup Utilities
```rust
// Creates listener with cancellation token setup
async fn setup_listener_with_cancellation(
    pool: PgPool, 
    registry: EventHandlerRegistry
) -> (Listener, CancellationToken, JoinHandle<Result<(), ListenerError>>)

// Publishes event and commits transaction in one call
async fn publish_and_commit(
    pool: &PgPool, 
    event: impl Event
) -> anyhow::Result<()>

// Waits for condition with timeout
async fn wait_for_condition<F>(
    condition: F, 
    timeout: Duration
) -> anyhow::Result<()>
where F: Fn() -> Pin<Box<dyn Future<Output = bool> + Send>>
```

### Database State Utilities
```rust
// Check event status in database
async fn get_event_status(pool: &PgPool, event_id: Uuid) -> Option<EventStatus>

// Count events by status  
async fn count_events_by_status(pool: &PgPool) -> HashMap<EventStatus, i64>

// Clean up test events
async fn cleanup_test_events(pool: &PgPool) -> anyhow::Result<()>
```

## Test Implementation Strategy

1. **Start with Priority 1-3** - These test critical failure modes
2. **Extract common utilities** as we implement each test
3. **Use existing `init_tracing()`** for consistent logging
4. **Follow existing pattern** with `sqlx::test(migrations = "./migrations")`
5. **Keep tests isolated** - each test should clean up after itself

## Notes

- **Concurrency Testing**: Use `tokio::spawn` for concurrent listener testing
- **Timing Sensitivity**: Use reasonable timeouts and avoid flaky timing assertions  
- **Database State**: Always verify database state changes, not just in-memory state
- **Error Propagation**: Test both error detection and proper error type propagation
- **Transaction Boundaries**: Carefully test transaction commit/rollback boundaries

## Success Criteria

Tests should:
- ✅ Pass consistently (not flaky)
- ✅ Cover runtime-only behaviors  
- ✅ Support safe refactoring
- ✅ Document expected behaviors
- ✅ Fail clearly when behavior breaks
- ✅ Run quickly (< 1 second each)