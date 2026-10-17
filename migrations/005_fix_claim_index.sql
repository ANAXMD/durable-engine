-- Migration 005: Fix Claim Index for Ordered Workflow Claiming
--
-- The claim query orders claimable workflows by (created_at ASC, id ASC) to prevent starvation.
-- Adding this partial index matches the ORDER BY clause for pending/claimable workflows.

CREATE INDEX idx_workflows_claimable_ordered ON workflows (created_at, id) WHERE status IN ('PENDING', 'RUNNING', 'SLEEPING');
