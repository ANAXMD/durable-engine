-- Benchmark script: Measures claim query performance with 300,000 sleeping workflows.
--
-- Inserts 300,000 dummy workflows with status 'SLEEPING' and run_at in the future,
-- plus a batch of claimable workflows. Then runs EXPLAIN ANALYZE on the SKIP LOCKED
-- claim query to measure local index scan latency.

BEGIN;

\echo 'Generating 300,000 sleeping workflows in the future...'
INSERT INTO workflows (id, workflow_type, status, run_at, wake_at, created_at)
SELECT
    gen_random_uuid(),
    'BenchmarkWorkflow',
    'SLEEPING',
    NOW() + INTERVAL '1 hour' + (g * INTERVAL '1 millisecond'),
    NOW() + INTERVAL '1 hour' + (g * INTERVAL '1 millisecond'),
    NOW()
FROM generate_series(1, 300000) AS g;

\echo 'Inserting 5 claimable workflows...'
INSERT INTO workflows (id, workflow_type, status, run_at, created_at)
SELECT
    gen_random_uuid(),
    'BenchmarkWorkflow',
    'PENDING',
    NOW() - INTERVAL '10 seconds',
    NOW() - INTERVAL '10 seconds'
FROM generate_series(1, 5);

\echo 'Analyzing table statistics...'
ANALYZE workflows;

\echo 'Running EXPLAIN ANALYZE on SKIP LOCKED claim query...'
EXPLAIN ANALYZE
UPDATE workflows
SET
    status           = 'RUNNING',
    locked_by        = 'bench-worker',
    run_at           = NOW() + INTERVAL '1 minute',
    lease_expires_at = NOW() + INTERVAL '1 minute'
WHERE id IN (
    SELECT id FROM workflows
    WHERE status IN ('PENDING', 'RUNNING', 'SLEEPING')
      AND run_at <= NOW()
    ORDER BY run_at ASC, id ASC
    LIMIT 5
    FOR UPDATE SKIP LOCKED
)
RETURNING id, workflow_type;

\echo 'Rolling back benchmark data to leave database clean...'
ROLLBACK;
