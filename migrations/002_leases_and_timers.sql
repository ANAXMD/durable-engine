-- Migration 002: Distributed Leases and Durable Timers
--
-- This migration upgrades the workflows table from a simple status machine to
-- a lease-based distributed queue. The key problems it solves:
--
-- 1. ZOMBIE WORKERS: Without leases, if a worker crashes while running a
--    workflow, the row stays in 'RUNNING' forever. The new lease_expires_at
--    column lets any worker reclaim orphaned jobs after a timeout.
--
-- 2. BOOT-TIME RESET HACK: We previously reset all 'RUNNING' rows to 'PENDING'
--    on startup. This is wrong in a multi-worker deployment — another worker
--    may still be actively processing those rows. Leases eliminate the need
--    for this boot-time sweep entirely.
--
-- 3. DURABLE SLEEP: The wake_at column is the engine's durable timer. When a
--    workflow calls Sleep(), the worker sets wake_at instead of re-queueing to
--    PENDING immediately. The poll query picks up SLEEPING rows whose wake_at
--    has elapsed.
--
-- 4. DUPLICATE EVENT PREVENTION: The unique constraint on (workflow_id,
--    sequence_number) prevents SaveEvent from writing the same step twice, even
--    if a worker crashes after saving but before returning, and then re-runs.
--    Postgres will reject the duplicate INSERT and we surface it as an error.

ALTER TABLE workflows
    ADD COLUMN locked_by        VARCHAR(255),
    ADD COLUMN lease_expires_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN wake_at          TIMESTAMP WITH TIME ZONE;

-- Prevent duplicate event writes. CRITICAL for idempotent replay safety.
ALTER TABLE workflow_events
    ADD CONSTRAINT unique_workflow_seq UNIQUE (workflow_id, sequence_number);
