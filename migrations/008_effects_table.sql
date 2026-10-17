-- Migration 008: Create effects table for verifying side-effect deduplication
--
-- The effects table records external side-effects performed by workflow activities.
-- Using the idempotency_key as the PRIMARY KEY ensures that even under repeated
-- crashes and at-least-once re-executions, exactly one effect record exists per step.
CREATE TABLE IF NOT EXISTS effects (
    idempotency_key TEXT PRIMARY KEY,
    activity_name TEXT NOT NULL,
    data TEXT,
    execution_count INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
