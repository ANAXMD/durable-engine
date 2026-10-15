package persistence

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/lib/pq"
)

// DB wraps our sql connection so we can attach methods to it.
type DB struct {
	sqlDB *sql.DB
}

// ConnectDB opens the database connection and verifies it is alive.
func ConnectDB(dsn string) (*DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return &DB{sqlDB: db}, nil
}

// Event exactly matches the columns in our migrations/001_init.sql schema.
type Event struct {
	ID             int
	WorkflowID     string
	EventType      string
	EventName      string
	InputData      json.RawMessage
	OutputData     json.RawMessage
	SequenceNumber int
}

// SaveEvent appends a new state change to the ledger.
// Notice there is absolutely no UPDATE statement here — events are immutable.
func (d *DB) SaveEvent(workflowID, eventType, eventName string, input, output []byte, seqNum int) error {
	const query = `
		INSERT INTO workflow_events (workflow_id, event_type, event_name, input_data, output_data, sequence_number)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := d.sqlDB.Exec(query, workflowID, eventType, eventName, input, output, seqNum)
	if err != nil {
		return fmt.Errorf("failed to save event: %w", err)
	}
	return nil
}

// GetEvent fetches a specific event by its sequence number for the Replay Core.
func (d *DB) GetEvent(workflowID string, seqNum int) (*Event, error) {
	const query = `
		SELECT id, workflow_id, event_type, event_name, input_data, output_data, sequence_number
		FROM workflow_events 
		WHERE workflow_id = $1 AND sequence_number = $2
	`
	row := d.sqlDB.QueryRow(query, workflowID, seqNum)

	var e Event
	var input, output []byte

	err := row.Scan(&e.ID, &e.WorkflowID, &e.EventType, &e.EventName, &input, &output, &e.SequenceNumber)
	if err != nil {
		// CRITICAL: If the event doesn't exist, it means the workflow hasn't run this step yet.
		// We return nil, nil so the engine knows to execute the code.
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get event: %w", err)
	}

	e.InputData = input
	e.OutputData = output

	return &e, nil
}

// Close releases all connections held by the underlying pool. It should be
// called exactly once, after all queries are done (e.g. via defer in main).
func (d *DB) Close() error {
	return d.sqlDB.Close()
}

// CreateWorkflow initializes a new workflow record to satisfy foreign key constraints.
func (d *DB) CreateWorkflow(workflowID, wfType string) error {
	const query = `
		INSERT INTO workflows (id, workflow_type, status) 
		VALUES ($1, $2, 'PENDING') 
		ON CONFLICT (id) DO NOTHING
	`
	_, err := d.sqlDB.Exec(query, workflowID, wfType)
	if err != nil {
		return fmt.Errorf("failed to create workflow: %w", err)
	}
	return nil
}

// GetPendingWorkflows atomically claims up to `limit` PENDING workflows by
// transitioning them to RUNNING in a single statement.
//
// Using FOR UPDATE SKIP LOCKED is the correct pattern for building a
// distributed work queue on top of Postgres. Without it, two worker processes
// running concurrently would race and both claim the same row, causing a
// workflow to be executed twice — a catastrophic bug in a payment system.
//
// SKIP LOCKED means: "if another transaction has already locked this row,
// don't wait — just skip it and return the next available row." This gives us
// a safe, high-throughput queue with zero blocking.
func (d *DB) GetPendingWorkflows(limit int) ([]string, error) {
	// The entire SELECT + UPDATE + RETURNING runs atomically in one round-trip.
	// No explicit transaction wrapper is needed because a single SQL statement
	// in Postgres is always implicitly atomic.
	const query = `
		UPDATE workflows
		SET status = 'RUNNING'
		WHERE id IN (
			SELECT id FROM workflows
			WHERE status = 'PENDING'
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id
	`
	rows, err := d.sqlDB.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to claim pending workflows: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan workflow id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// UpdateWorkflowStatus transitions the aggregate state of a workflow.
// This is the only UPDATE allowed by our event-sourcing rules — it updates
// the aggregate view (workflows table), not the event ledger (workflow_events).
func (d *DB) UpdateWorkflowStatus(workflowID, status string) error {
	const query = `UPDATE workflows SET status = $1 WHERE id = $2`
	_, err := d.sqlDB.Exec(query, status, workflowID)
	if err != nil {
		return fmt.Errorf("failed to update workflow %s to %s: %w", workflowID, status, err)
	}
	return nil
}
