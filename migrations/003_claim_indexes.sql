-- Migration 003: Partial index for workflow claiming query
--
-- In production, the workflows table accumulates historical records in terminal states
-- ('COMPLETED' or 'FAILED'). Polling every 2 seconds without an index results in sequential scans.
--
-- A partial index indexing only claimable states ('PENDING', 'RUNNING', 'SLEEPING')
-- keeps the B-Tree index extremely small, cache-resident, and makes lease acquisition instantaneous.

CREATE INDEX idx_workflows_claimable 
ON workflows (status, lease_expires_at, wake_at, id)
WHERE status IN ('PENDING', 'RUNNING', 'SLEEPING');
