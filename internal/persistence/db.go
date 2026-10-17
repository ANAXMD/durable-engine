package persistence

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

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

// Close releases all connections held by the underlying pool. It should be
// called exactly once, after all queries are done (e.g. via defer in main).
func (d *DB) Close() error {
	return d.sqlDB.Close()
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
// The UNIQUE constraint on (workflow_id, sequence_number, event_type) added in migration 004
// ensures append-only immutability. If an event insert is re-attempted (e.g. after a crash),
// ON CONFLICT DO NOTHING makes the insert safely idempotent without updating existing records.
// Notice there is absolutely no UPDATE statement here — events are immutable.
func (d *DB) SaveEvent(workflowID, eventType, eventName string, input, output []byte, seqNum int) error {
	const query = `
		INSERT INTO workflow_events (workflow_id, event_type, event_name, input_data, output_data, sequence_number)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT DO NOTHING
	`
	_, err := d.sqlDB.Exec(query, workflowID, eventType, eventName, input, output, seqNum)
	if err != nil {
		return fmt.Errorf("failed to save event: %w", err)
	}
	return nil
}

// GetEvent fetches a specific event by its sequence number for the Replay Core.
// When multiple events share a sequence number (e.g. ACTIVITY_STARTED and ACTIVITY_COMPLETED),
// ORDER BY id DESC ensures the latest lifecycle event is returned.
func (d *DB) GetEvent(workflowID string, seqNum int) (*Event, error) {
	const query = `
		SELECT id, workflow_id, event_type, event_name, input_data, output_data, sequence_number
		FROM workflow_events 
		WHERE workflow_id = $1 AND sequence_number = $2
		ORDER BY id DESC
		LIMIT 1
	`
	row := d.sqlDB.QueryRow(query, workflowID, seqNum)

	var e Event
	var input, output []byte

	err := row.Scan(&e.ID, &e.WorkflowID, &e.EventType, &e.EventName, &input, &output, &e.SequenceNumber)
	if err != nil {
		// CRITICAL: If the event doesn't exist, it means the workflow hasn't run
		// this step yet. Return nil, nil so the engine knows to execute the code.
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get event: %w", err)
	}

	e.InputData = input
	e.OutputData = output

	return &e, nil
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

// WorkflowRecord represents a claimed workflow header from the database.
type WorkflowRecord struct {
	ID   string
	Type string
}

// GetPendingWorkflows atomically claims up to `limit` workable jobs and stamps
// them with a lease owned by workerID. A job is claimable if it is:
//
//   - PENDING                               — ready to run for the first time
//   - RUNNING with an expired lease         — the previous worker crashed
//   - SLEEPING with wake_at <= NOW()        — the durable timer has elapsed
//
// Claiming sets status = 'RUNNING', locked_by = workerID, and
// lease_expires_at = NOW() + 1 minute. ORDER BY created_at ASC, id ASC prevents starvation
// (oldest jobs are always claimed first).
//
// FOR UPDATE SKIP LOCKED ensures concurrent workers never claim the same row.
// Without SKIP LOCKED, a second worker would block on the locked row, then
// claim it after the first transaction commits — executing the workflow twice.
func (d *DB) GetPendingWorkflows(limit int, workerID string) ([]WorkflowRecord, error) {
	const query = `
		UPDATE workflows
		SET
			status           = 'RUNNING',
			locked_by        = $2,
			lease_expires_at = NOW() + INTERVAL '1 minute'
		WHERE id IN (
			SELECT id FROM workflows
			WHERE
				status = 'PENDING'
				OR (status = 'RUNNING'  AND lease_expires_at < NOW())
				OR (status = 'SLEEPING' AND wake_at <= NOW())
			ORDER BY created_at ASC, id ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, workflow_type
	`
	rows, err := d.sqlDB.Query(query, limit, workerID)
	if err != nil {
		return nil, fmt.Errorf("failed to claim workflows: %w", err)
	}
	defer rows.Close()

	var records []WorkflowRecord
	for rows.Next() {
		var rec WorkflowRecord
		if err := rows.Scan(&rec.ID, &rec.Type); err != nil {
			return nil, fmt.Errorf("failed to scan workflow record: %w", err)
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

// UpdateWorkflowStatus transitions a workflow's aggregate status and clears the
// lease. The workerID guard prevents a stale worker whose lease expired (and
// whose row was reclaimed by another worker) from overwriting the new owner's
// state — a classic distributed systems ABA problem.
//
// If the UPDATE affects 0 rows, the lease was lost; we return an error so the
// caller knows the completion was not recorded.
func (d *DB) UpdateWorkflowStatus(workflowID, status, workerID string) error {
	const query = `
		UPDATE workflows
		SET
			status           = $1,
			locked_by        = NULL,
			lease_expires_at = NULL
		WHERE id = $2
		  AND locked_by = $3
	`
	result, err := d.sqlDB.Exec(query, status, workflowID, workerID)
	if err != nil {
		return fmt.Errorf("failed to update workflow %s to %s: %w", workflowID, status, err)
	}

	// Check that we actually owned the lease when we tried to finalize.
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("lease lost for workflow %s: another worker reclaimed it", workflowID)
	}

	return nil
}

// SleepWorkflow transitions a workflow to SLEEPING state and records the
// absolute time at which the engine should wake it (wake_at). It also clears
// the lease so no worker holds an active claim on a sleeping workflow.
//
// The workerID guard here prevents a zombie worker from accidentally sleeping
// a workflow it no longer owns.
func (d *DB) SleepWorkflow(workflowID, workerID string, wakeAt time.Time) error {
	const query = `
		UPDATE workflows
		SET
			status           = 'SLEEPING',
			wake_at          = $1,
			locked_by        = NULL,
			lease_expires_at = NULL
		WHERE id       = $2
		  AND locked_by = $3
	`
	result, err := d.sqlDB.Exec(query, wakeAt, workflowID, workerID)
	if err != nil {
		return fmt.Errorf("failed to sleep workflow %s: %w", workflowID, err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected for sleep: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("lease lost for workflow %s before sleep could be recorded", workflowID)
	}

	return nil
}

// ExpireLeaseForTest is a test-only helper that artificially sets a workflow's
// lease_expires_at to the past, allowing the next poll to reclaim it without
// waiting the full 1-minute TTL. Never call this in production code.
func (d *DB) ExpireLeaseForTest(workflowID string) error {
	const query = `
		UPDATE workflows
		SET lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE id = $1
	`
	_, err := d.sqlDB.Exec(query, workflowID)
	if err != nil {
		return fmt.Errorf("failed to expire lease for test: %w", err)
	}
	return nil
}

// ExtendLease extends the lease duration for an actively running workflow owned by workerID.
func (d *DB) ExtendLease(workflowID, workerID string, additional time.Duration) error {
	intervalStr := fmt.Sprintf("%d seconds", int(additional.Seconds()))
	const query = `
		UPDATE workflows
		SET lease_expires_at = NOW() + $3::interval
		WHERE id = $1 AND locked_by = $2
	`
	_, err := d.sqlDB.Exec(query, workflowID, workerID, intervalStr)
	if err != nil {
		return fmt.Errorf("failed to extend lease: %w", err)
	}
	return nil
}
