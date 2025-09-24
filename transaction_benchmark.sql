-- Transaction overhead benchmark
-- Run with: psql -f transaction_benchmark.sql your_database

-- Setup
CREATE TEMP TABLE bench_test (id SERIAL, data TEXT);

-- Test 1: Autocommit (1000 individual inserts)
\timing on
\echo '=== Test 1: Autocommit Mode ==='
DO $$
BEGIN
  FOR i IN 1..1000 LOOP
    INSERT INTO bench_test (data) VALUES ('autocommit_' || i);
  END LOOP;
END $$;

-- Clear data
TRUNCATE bench_test;

-- Test 2: Single explicit transaction (1000 inserts)
\echo '=== Test 2: Single Transaction ==='
BEGIN;
DO $$
BEGIN
  FOR i IN 1..1000 LOOP
    INSERT INTO bench_test (data) VALUES ('transaction_' || i);
  END LOOP;
END $$;
COMMIT;

-- Clear data
TRUNCATE bench_test;

-- Test 3: Many small transactions (1000 individual transactions)
\echo '=== Test 3: Many Small Transactions ==='
DO $$
BEGIN
  FOR i IN 1..1000 LOOP
    BEGIN;
    INSERT INTO bench_test (data) VALUES ('small_tx_' || i);
    COMMIT;
  END LOOP;
END $$;

\timing off
DROP TABLE bench_test;