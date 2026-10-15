CREATE TABLE workflows (
    id UUID PRIMARY KEY,
    workflow_type TEXT NOT NULL,
    status TEXT NOT NULL, -- 'RUNNING', 'COMPLETED', 'FAILED'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE workflow_events (
    id SERIAL PRIMARY KEY,
    workflow_id UUID REFERENCES workflows(id),
    event_type TEXT NOT NULL, -- 'ACTIVITY_SCHEDULED', 'ACTIVITY_COMPLETED', etc.
    event_name TEXT NOT NULL, -- e.g., "ChargeCreditCard"
    input_data JSONB,
    output_data JSONB,
    sequence_number INT NOT NULL, -- CRITICAL: Used for deterministic replay
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_workflow_events_workflow_id ON workflow_events(workflow_id);