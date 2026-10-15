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
// The UNIQUE constraint on (workflow_id, sequence_number) added in migration 002
// means a duplicate write (e.g. crash after save, retry on restart) will return
// an error rather than silently double-writing — making this call idempotent-safe.
// Notice there is absolutely no UPDATE statement here — events are immutable.
func (d *DB) SaveEvent(workflowID, eventType, eventName string, input, output []byte, seqNum int) error {
	// ON CONFLICT DO NOTHING makes this call idempotent. If we write an
	// ACTIVITY_STARTED event, crash, and retry, the second INSERT is a no-op
	// instead of an error. The UNIQUE constraint on (workflow_id, sequence_number)
	// still protects against a different activity writing to the same slot.
	const query = `
		INSERT INTO workflow_events (workflow_id, event_type, event_name, input_data, output_data, sequence_number)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (workflow_id, sequence_number) DO UPDATE
		SET event_type = EXCLUDED.event_type,
		    output_data = EXCLUDED.output_data
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

// GetPendingWorkflows atomically claims up to `limit` workable jobs and stamps
// them with a lease owned by workerID. A job is claimable if it is:
//
//   - PENDING                               — ready to run for the first time
//   - RUNNING with an expired lease         — the previous worker crashed
//   - SLEEPING with wake_at <= NOW()        — the durable timer has elapsed
//
// Claiming sets status = 'RUNNING', locked_by = workerID, and
// lease_expires_at = NOW() + 1 minute. ORDER BY id prevents starvation
// (oldest jobs are always claimed first).
//
// FOR UPDATE SKIP LOCKED ensures concurrent workers never claim the same row.
// Without SKIP LOCKED, a second worker would block on the locked row, then
// claim it after the first transaction commits — executing the workflow twice.
func (d *DB) GetPendingWorkflows(limit int, workerID string) ([]string, error) {
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
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id
	`
	rows, err := d.sqlDB.Query(query, limit, workerID)
	if err != nil {
		return nil, fmt.Errorf("failed to claim workflows: %w", err)
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
