package engine

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"durable-engine/internal/persistence"
	"durable-engine/internal/registry"
)

func newTestUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func setupTestDB(t *testing.T) *persistence.DB {
	t.Helper()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://engine_user:engine_password@127.0.0.1:5432/durable_engine?sslmode=disable"
	}

	db, err := persistence.ConnectDB(dsn)
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("database connection failed in CI: %v", err)
		}
		t.Skipf("skipping test: database not available: %v", err)
	}

	return db
}

// 1. Recovery from a STARTED step
// A workflow has an ACTIVITY_STARTED event but no completion. Assert that when the engine replays,
// it re-executes the activity and successfully completes it.
func TestRecoveryFromStartedStep(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	wfID := newTestUUID()
	t.Cleanup(func() {
		_, _ = db.RawDB().Exec("DELETE FROM workflow_events WHERE workflow_id = $1", wfID)
		_, _ = db.RawDB().Exec("DELETE FROM workflows WHERE id = $1", wfID)
	})

	var executionCount int32
	reg := registry.NewRegistry()
	reg.RegisterActivity("RecoverableActivity", func(ctx context.Context, idempKey string, input any) (any, error) {
		atomic.AddInt32(&executionCount, 1)
		return "step-success", nil
	})

	var capturedResult string
	reg.RegisterWorkflow("RecoverableWorkflow", func(ctx WorkflowContext) error {
		return ctx.ExecuteActivity("RecoverableActivity", "step-input", &capturedResult)
	})

	// Pre-seed database: workflow created and has ACTIVITY_STARTED at sequence 1, but no ACTIVITY_COMPLETED.
	if err := db.CreateWorkflow(wfID, "RecoverableWorkflow"); err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	inputBytes, _ := json.Marshal("step-input")
	inserted, err := db.SaveEvent(wfID, "ACTIVITY_STARTED", "RecoverableActivity", inputBytes, []byte("{}"), 1)
	if err != nil || !inserted {
		t.Fatalf("failed to insert initial ACTIVITY_STARTED: inserted=%v, err=%v", inserted, err)
	}

	// Verify only ACTIVITY_STARTED exists
	initialEv, err := db.GetEvent(wfID, 1)
	if err != nil || initialEv == nil {
		t.Fatalf("expected initial event, got: %v (err: %v)", initialEv, err)
	}
	if initialEv.EventType != "ACTIVITY_STARTED" {
		t.Fatalf("expected initial event type ACTIVITY_STARTED, got: %s", initialEv.EventType)
	}

	// Run engine replay
	err = Run(wfID, "RecoverableWorkflow", reg, db, "worker-1")
	if err != nil {
		t.Fatalf("engine.Run failed on replay: %v", err)
	}

	// Assert the activity was re-executed (executionCount == 1) and result captured
	if count := atomic.LoadInt32(&executionCount); count != 1 {
		t.Fatalf("expected activity to re-execute exactly once, ran %d times", count)
	}
	if capturedResult != "step-success" {
		t.Fatalf("expected captured result 'step-success', got '%s'", capturedResult)
	}

	// Assert that database ledger now has ACTIVITY_COMPLETED
	finalEv, err := db.GetEvent(wfID, 1)
	if err != nil || finalEv == nil {
		t.Fatalf("failed to get final event: %v", err)
	}
	if finalEv.EventType != "ACTIVITY_COMPLETED" {
		t.Fatalf("expected event type ACTIVITY_COMPLETED after replay, got: %s", finalEv.EventType)
	}

	var storedOutput string
	if err := json.Unmarshal(finalEv.OutputData, &storedOutput); err != nil || storedOutput != "step-success" {
		t.Fatalf("expected stored output 'step-success', got '%s' (err: %v)", storedOutput, err)
	}
}

// 2. COMPLETED beating a stale FAILED
// Assert that if a workflow is already marked COMPLETED in the database, a delayed/stale worker
// attempting to save an ACTIVITY_FAILED event is rejected and does not corrupt the state.
func TestCompletedBeatingStaleFailed(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	wfID := newTestUUID()
	t.Cleanup(func() {
		_, _ = db.RawDB().Exec("DELETE FROM workflow_events WHERE workflow_id = $1", wfID)
		_, _ = db.RawDB().Exec("DELETE FROM workflows WHERE id = $1", wfID)
	})

	if err := db.CreateWorkflow(wfID, "TestWorkflow"); err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	// Worker 1 claims and completes the workflow
	workerWinner := "worker-winner"
	_, err := db.RawDB().Exec(
		"UPDATE workflows SET status = 'RUNNING', locked_by = $1, lease_expires_at = NOW() + INTERVAL '1 minute' WHERE id = $2",
		workerWinner, wfID,
	)
	if err != nil {
		t.Fatalf("failed to lock workflow for winner: %v", err)
	}

	inputBytes, _ := json.Marshal("input")
	outputBytes, _ := json.Marshal("success-data")
	_, _ = db.SaveEvent(wfID, "ACTIVITY_STARTED", "Step1", inputBytes, []byte("{}"), 1)
	_, _ = db.SaveEvent(wfID, "ACTIVITY_COMPLETED", "Step1", inputBytes, outputBytes, 1)

	if err := db.UpdateWorkflowStatus(wfID, "COMPLETED", workerWinner); err != nil {
		t.Fatalf("failed to mark workflow COMPLETED: %v", err)
	}

	status, err := db.GetWorkflowStatus(wfID)
	if err != nil || status != "COMPLETED" {
		t.Fatalf("expected status COMPLETED, got '%s' (err: %v)", status, err)
	}

	// A delayed/stale worker attempts to finalize the workflow as FAILED
	workerStale := "worker-stale"
	staleStatusErr := db.UpdateWorkflowStatus(wfID, "FAILED", workerStale)
	if staleStatusErr == nil {
		t.Fatalf("expected stale worker status update to fail, but it succeeded")
	}

	// The stale worker attempts to record an ACTIVITY_FAILED event
	errBytes, _ := json.Marshal(map[string]string{"error": "stale execution failed"})
	_, _ = db.SaveEvent(wfID, "ACTIVITY_FAILED", "Step1", inputBytes, errBytes, 1)

	// Assert state integrity:
	// 1. GetEvent must still return ACTIVITY_COMPLETED due to prioritization
	ev, err := db.GetEvent(wfID, 1)
	if err != nil || ev == nil {
		t.Fatalf("GetEvent failed: %v", err)
	}
	if ev.EventType != "ACTIVITY_COMPLETED" {
		t.Fatalf("expected GetEvent to prioritize ACTIVITY_COMPLETED over stale ACTIVITY_FAILED, got: %s", ev.EventType)
	}

	var output string
	_ = json.Unmarshal(ev.OutputData, &output)
	if output != "success-data" {
		t.Fatalf("expected output 'success-data', got: '%s'", output)
	}

	// 2. Workflow status must remain COMPLETED
	status, err = db.GetWorkflowStatus(wfID)
	if err != nil || status != "COMPLETED" {
		t.Fatalf("expected workflow status to remain COMPLETED, got: '%s' (err: %v)", status, err)
	}
}

// 3. Race adoption (inserted = false)
// Assert that when two workers try to save ACTIVITY_COMPLETED simultaneously and SaveEvent
// returns inserted = false for the loser, the losing worker catches it, fetches the winner's
// output payload, and successfully adopts the winner's state.
func TestRaceAdoptionInsertedFalse(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	wfID := newTestUUID()
	t.Cleanup(func() {
		_, _ = db.RawDB().Exec("DELETE FROM workflow_events WHERE workflow_id = $1", wfID)
		_, _ = db.RawDB().Exec("DELETE FROM workflows WHERE id = $1", wfID)
	})

	if err := db.CreateWorkflow(wfID, "RaceWorkflow"); err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	inputBytes, _ := json.Marshal("step-input")
	winnerOutputBytes, _ := json.Marshal("winner-payload-12345")

	// Worker 2 registers activity that produces "loser-payload-67890".
	// During execution of this activity (before Worker 2 saves its completion),
	// Worker 1 simulates finishing first by writing ACTIVITY_COMPLETED to the database.
	reg := registry.NewRegistry()
	reg.RegisterActivity("RaceStep", func(ctx context.Context, idempKey string, input any) (any, error) {
		// Worker 1 wins the race right before Worker 2 tries to save
		ins, err := db.SaveEvent(wfID, "ACTIVITY_COMPLETED", "RaceStep", inputBytes, winnerOutputBytes, 1)
		if err != nil || !ins {
			return nil, fmt.Errorf("failed to insert winner event: ins=%v, err=%v", ins, err)
		}
		return "loser-payload-67890", nil
	})

	var adoptedResult string
	reg.RegisterWorkflow("RaceWorkflow", func(ctx WorkflowContext) error {
		return ctx.ExecuteActivity("RaceStep", "step-input", &adoptedResult)
	})

	// Run Worker 2
	err := Run(wfID, "RaceWorkflow", reg, db, "worker-loser")
	if err != nil {
		t.Fatalf("expected Worker 2 to succeed via race adoption, got error: %v", err)
	}

	// Worker 2's SaveEvent triggered ON CONFLICT DO NOTHING (inserted == false).
	// It must have fetched the winner's event and adopted "winner-payload-12345".
	if adoptedResult != "winner-payload-12345" {
		t.Fatalf("expected loser to adopt winner's state 'winner-payload-12345', got '%s'", adoptedResult)
	}
}

// 4. Heartbeat Lease Loss
// Assert that if ExtendLease returns persistence.ErrLeaseLost during a long-running activity,
// the worker's context is canceled and it aborts silently without writing an ACTIVITY_FAILED event.
func TestHeartbeatLeaseLoss(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Shorten heartbeat interval to 20ms for fast test execution
	origInterval := heartbeatInterval
	heartbeatInterval = 20 * time.Millisecond
	defer func() { heartbeatInterval = origInterval }()

	wfID := newTestUUID()
	t.Cleanup(func() {
		_, _ = db.RawDB().Exec("DELETE FROM workflow_events WHERE workflow_id = $1", wfID)
		_, _ = db.RawDB().Exec("DELETE FROM workflows WHERE id = $1", wfID)
	})

	if err := db.CreateWorkflow(wfID, "LeaseLossWorkflow"); err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	// Simulate that the workflow is locked by another worker ("worker-other"),
	// so any ExtendLease attempt by "worker-victim" will return ErrLeaseLost.
	_, err := db.RawDB().Exec(
		"UPDATE workflows SET status = 'RUNNING', locked_by = 'worker-other', lease_expires_at = NOW() + INTERVAL '1 minute' WHERE id = $1",
		wfID,
	)
	if err != nil {
		t.Fatalf("failed to set up workflow lease: %v", err)
	}

	reg := registry.NewRegistry()
	reg.RegisterActivity("LongRunningStep", func(ctx context.Context, idempKey string, input any) (any, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return "done", nil
		}
	})

	reg.RegisterWorkflow("LeaseLossWorkflow", func(ctx WorkflowContext) error {
		var res string
		return ctx.ExecuteActivity("LongRunningStep", nil, &res)
	})

	errChan := make(chan error, 1)
	go func() {
		errChan <- Run(wfID, "LeaseLossWorkflow", reg, db, "worker-victim")
	}()

	select {
	case runErr := <-errChan:
		if runErr == nil {
			t.Fatalf("expected run to fail due to lost lease, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for activity to cancel on lease loss")
	}

	// Assert that no ACTIVITY_FAILED event was saved to the ledger
	failedCount, err := db.CountEvents(wfID, "ACTIVITY_FAILED")
	if err != nil {
		t.Fatalf("failed to count failed events: %v", err)
	}
	if failedCount != 0 {
		t.Fatalf("expected 0 ACTIVITY_FAILED events on heartbeat lease loss, got %d", failedCount)
	}

	// Verify ACTIVITY_STARTED was saved
	startedCount, err := db.CountEvents(wfID, "ACTIVITY_STARTED")
	if err != nil {
		t.Fatalf("failed to count started events: %v", err)
	}
	if startedCount != 1 {
		t.Fatalf("expected 1 ACTIVITY_STARTED event, got %d", startedCount)
	}
}
