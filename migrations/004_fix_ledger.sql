-- Migration 004: Fix Ledger Immutability & Schema
--
-- 1. Drop the single-event-per-sequence constraint.
--    A sequence number may legitimately record multiple lifecycle events
--    for a single step (e.g. ACTIVITY_STARTED intent, followed by ACTIVITY_COMPLETED).
ALTER TABLE workflow_events DROP CONSTRAINT IF EXISTS unique_workflow_seq;

-- 2. Add the correct compound constraint allowing multiple distinct event types
--    per sequence number while keeping each event type strictly unique and idempotent.
ALTER TABLE workflow_events ADD CONSTRAINT unique_workflow_seq_type UNIQUE (workflow_id, sequence_number, event_type);

-- 3. Ensure created_at exists on the workflows table for queue ordering.
ALTER TABLE workflows ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
