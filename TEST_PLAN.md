# Test Plan for fx-event-bus

This document outlines the comprehensive test strategy for the event bus system, organized by method-level unit tests and integration tests.

## Overview

The test plan focuses on:
- **Unit Tests**: Testing individual methods in isolation within their respective files
- **Integration Tests**: Testing complete workflows and system behavior
- **Method Coverage**: Each public/internal method has dedicated unit tests

## Unit Tests by Method

### 1. `retry` Method
**File**: `src/listener/methods/retry.rs`  
**Function**: `pub async fn retry(tx, now) -> Result<Option<RawEvent>, ListenerError>`  
**Priority**: High

#### Test Cases:
- `test_retry_claims_ready_event` - Claims events where `try_earliest <= now` and `attempted_at IS NULL`
- `test_retry_skips_future_events` - Skips events where `try_earliest > now`
- `test_retry_skips_claimed_events` - Skips events where `attempted_at IS NOT NULL`
- `test_retry_skip_locked_behavior` - FOR UPDATE SKIP LOCKED prevents duplicate claims
- `test_retry_returns_correct_data` - Returned event contains correct id, name, hash, payload, attempted
- `test_retry_updates_attempted_at` - Sets attempted_at to provided timestamp
- `test_retry_fifo_ordering` - Claims earliest try_earliest first, then by id
- `test_retry_empty_queue` - Returns None when no events available

### 2. `acknowledge` Method
**File**: `src/listener/methods/acknowledge.rs`  
**Function**: `pub(super) async fn acknowledge(tx, now) -> Result<Option<RawEvent>, ListenerError>`  
**Priority**: High

#### Test Cases:
- `test_acknowledge_moves_event` - Moves event from unacknowledged to acknowledged table
- `test_acknowledge_fifo_ordering` - Processes events in published_at ASC, id ASC order
- `test_acknowledge_skip_locked_behavior` - FOR UPDATE SKIP LOCKED prevents conflicts
- `test_acknowledge_sets_acknowledged_at` - Sets acknowledged_at to provided timestamp
- `test_acknowledge_sets_attempted_zero` - Always sets attempted field to 0
- `test_acknowledge_empty_queue` - Returns None when no unacknowledged events
- `test_acknowledge_returns_correct_data` - Returned event has correct structure

### 3. `report_failure` Method
**File**: `src/listener/methods/report_failure.rs`  
**Function**: `pub(super) async fn report_failure(tx, event_id, attempted, attempted_at, error) -> Result<(), ListenerError>`  
**Priority**: High

#### Test Cases:
- `test_report_failure_creates_retry` - Creates retry when `attempted < max_attempts`
- `test_report_failure_moves_to_dlq` - Moves to DLQ when `attempted >= max_attempts`
- `test_report_failure_sets_try_earliest` - Correctly calculates next retry time
- `test_report_failure_preserves_error` - Stores error message correctly
- `test_report_failure_dlq_aggregates_errors` - Aggregates all attempt errors in DLQ
- `test_report_failure_dlq_aggregates_timestamps` - Aggregates all attempt timestamps in DLQ
- `test_report_failure_cleans_failed_attempts` - Removes failed attempts when moving to DLQ
- `test_report_failure_respects_max_attempts` - Uses configured max_attempts value

### 4. `try_earliest` Method
**File**: `src/listener/methods/report_failure.rs`  
**Function**: `fn try_earliest(attempted_at, attempted) -> DateTime<Utc>`  
**Priority**: Medium

#### Test Cases:
- `test_try_earliest_exponential_backoff` - Formula: `attempted_at + base_duration * 2^(attempted-1)`
- `test_try_earliest_first_attempt` - Attempt 1: base_duration * 1
- `test_try_earliest_second_attempt` - Attempt 2: base_duration * 2
- `test_try_earliest_third_attempt` - Attempt 3: base_duration * 4
- `test_try_earliest_precision` - Maintains millisecond precision
- `test_try_earliest_large_attempts` - Handles high attempt counts
- `test_try_earliest_custom_base_duration` - Works with different base durations

### 5. `report_success` Method
**File**: `src/listener/methods/report_success.rs`  
**Function**: `pub(super) async fn report_success(tx, event_id, attempted_at) -> Result<(), ListenerError>`  
**Priority**: Medium

#### Test Cases:
- `test_report_success_creates_record` - Inserts record into attempts_succeeded table
- `test_report_success_stores_timestamp` - Correctly stores attempted_at timestamp
- `test_report_success_stores_event_id` - Correctly stores event_id
- `test_report_success_database_constraints` - Respects database constraints

### 6. `poll` Method
**File**: `src/listener/methods/poll.rs`  
**Function**: `pub async fn poll() -> Result<bool, ListenerError>`  
**Priority**: High

#### Test Cases:
- `test_poll_prioritizes_acknowledge` - Tries acknowledge before retry
- `test_poll_falls_back_to_retry` - Uses retry when no unacknowledged events
- `test_poll_returns_false_no_work` - Returns false when no events available
- `test_poll_returns_true_work_done` - Returns true when event processed
- `test_poll_handles_success` - Calls report_success on successful handling
- `test_poll_handles_failure` - Calls report_failure on failed handling
- `test_poll_commits_transaction` - Commits transaction after processing
- `test_poll_preserves_event_data` - Passes correct event data to handler
- `test_poll_increments_attempted` - Increments attempted count on failure
- `test_poll_uses_current_timestamp` - Uses consistent timestamps throughout

## Integration Tests

**File**: `tests/integration_tests.rs`  
**Priority**: Medium

### Core Workflows
- `test_full_event_lifecycle` - Publish → Acknowledge → Handle → Success
- `test_retry_cycle_with_backoff` - Publish → Acknowledge → Fail → Retry → Success
- `test_dlq_movement_workflow` - Multiple failures → DLQ with history
- `test_mixed_event_processing` - Both new events and retries processed correctly

### Concurrency Scenarios
- `test_concurrent_acknowledge` - Multiple workers acknowledging different events
- `test_concurrent_retry` - Multiple workers claiming different retries
- `test_skip_locked_effectiveness` - No duplicate processing under concurrency
- `test_dlq_movement_concurrency` - Safe DLQ operations under load

### Error Recovery
- `test_transaction_rollback` - System consistency after transaction failures
- `test_handler_timeout` - Timeout handling in event processing
- `test_database_reconnection` - Recovery after connection loss

### System Behavior
- `test_high_volume_processing` - Performance under load
- `test_mixed_success_failure_rates` - System behavior with mixed outcomes
- `test_backpressure_handling` - Behavior when queues fill up

## Test Infrastructure

### Unit Test Helpers (per method file)
- Database transaction setup/cleanup
- Test event creation utilities
- Timestamp verification helpers
- Error simulation tools

### Integration Test Helpers
- Full database setup with migrations
- Event publishing utilities
- Handler registry setup
- Concurrency test frameworks
- Performance measurement tools

### Common Test Data
- `TestEvent` structure for consistent testing
- Predefined error scenarios
- Timing test patterns
- Load test data sets

## Implementation Strategy

### Phase 1: Core Method Unit Tests
1. `retry` method tests (highest priority)
2. `acknowledge` method tests
3. `report_failure` method tests
4. `poll` method tests

### Phase 2: Supporting Method Tests
1. `try_earliest` calculation tests
2. `report_success` method tests
3. Error handling edge cases

### Phase 3: Integration Tests
1. Core workflow tests
2. Concurrency tests
3. Error recovery tests
4. Performance tests

## Success Criteria

### Unit Tests
- Each method has 95%+ code coverage
- Tests run in <1 second each
- All edge cases covered
- Mock/stub external dependencies

### Integration Tests
- End-to-end workflows validated
- Concurrency safety verified
- Real database interactions tested
- Performance benchmarks established

## Notes

- Unit tests are co-located with method implementations in `#[cfg(test)]` modules
- Integration tests use full PostgreSQL database with migrations
- All tests should be deterministic and repeatable
- Timing-sensitive tests should have appropriate tolerance margins
- Database state should be properly isolated between tests