-- Migration 007: NOT NULL constraint on run_at for non-terminal workflows
--
-- This constraint enforces the invariant established by migration 006:
-- every workflow that the scheduler can pick up (PENDING, RUNNING, SLEEPING)
-- MUST have a run_at value. Terminal workflows (COMPLETED, FAILED) have
-- run_at = NULL to keep them out of the partial index entirely.
--
-- Without this constraint a bug that NULLs out run_at for a PENDING workflow
-- would silently starve it: it would pass the WHERE status IN (...) filter
-- but never satisfy run_at <= NOW(), making it invisible to all workers.
-- The CHECK catches such bugs immediately at write time.

ALTER TABLE workflows
    ADD CONSTRAINT chk_run_at
    CHECK (status IN ('COMPLETED', 'FAILED') OR run_at IS NOT NULL);
