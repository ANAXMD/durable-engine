-- Migration 006: Unified run_at Scheduling Column
--
-- Problem: The polling query previously checked three separate columns
-- (created_at, wake_at, lease_expires_at) with complex OR conditions, which
-- required multiple index lookups and was difficult to optimize with a single
-- covering index.
--
-- Solution: A single `run_at` column encodes the canonical "earliest time this
-- workflow can be picked up" for all statuses:
--   PENDING  → run_at = created_at  (ready immediately)
--   SLEEPING → run_at = wake_at     (ready when the timer fires)
--   RUNNING  → run_at = lease_expires_at (reclaimable if the worker crashes)
--
-- The new polling query simply becomes: WHERE status IN (...) AND run_at <= NOW()
-- ORDER BY run_at ASC, id ASC, which matches the new index perfectly.

-- 1. Add the unified scheduling column.
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS run_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();

-- 2. Backfill existing rows from their respective source columns.
UPDATE workflows SET run_at = created_at       WHERE status = 'PENDING';
UPDATE workflows SET run_at = wake_at          WHERE status = 'SLEEPING';
UPDATE workflows SET run_at = lease_expires_at WHERE status = 'RUNNING';

-- 3. Drop the old, now-redundant indexes.
DROP INDEX IF EXISTS idx_workflows_claimable;
DROP INDEX IF EXISTS idx_workflows_claimable_ordered;

-- 4. Create the new, unified partial index.
--    Covers only claimable rows; terminal rows (COMPLETED, FAILED) are excluded.
--    Ordering by (run_at, id) exactly matches the new ORDER BY clause so the
--    planner can satisfy the query with an index scan.
CREATE INDEX idx_workflows_run_at ON workflows (run_at, id) WHERE status IN ('PENDING', 'RUNNING', 'SLEEPING');
