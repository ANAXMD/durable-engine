// cmd/chaos/main.go
//
// Chaos Test Harness for the Durable Engine.
//
// This script proves that the engine survives a catastrophic mid-execution crash
// and correctly resumes a workflow from the exact point it stopped — without
// re-running steps that already completed.
//
// Test Scenario:
//
//  1. Start a 3-step workflow:  FastStep1 → Sleep(3s) → FastStep2
//  2. Let it yield on Sleep (FastStep1 is COMPLETED in the ledger)
//  3. Simulate a crash: close the DB connection (worker 1 is dead)
//  4. Spin up a new worker (worker 2) with a fresh DB connection
//  5. Artificially expire the DB lease for the workflow (bypass 1-min TTL)
//  6. Worker 2 polls, finds the workflow, tries to run:
//     — FastStep1 is REPLAYED (NOT re-executed)
//     — Sleep is not yet elapsed → re-yields
//  7. Wait for sleep to elapse, poll again:
//     — FastStep1 is REPLAYED
//     — Sleep is elapsed → continue
//     — FastStep2 executes
//     — Workflow reaches COMPLETED
//
// Assert: FastStep1 executed exactly once. FastStep2 executed exactly once.
//
// Run: go run ./cmd/chaos
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"durable-engine/internal/engine"
	"durable-engine/internal/persistence"
	"durable-engine/internal/registry"
)

// ─── Chaos Test Workflow ───────────────────────────────────────────────────────

// chaosWorkflow orchestrates 3 steps: fast → sleep → fast.
// Designed so the crash happens during the sleep.
func chaosWorkflow(ctx engine.WorkflowContext) error {
	var step1Result string
	if err := ctx.ExecuteActivity("FastStep1", "input-A", &step1Result); err != nil {
		return fmt.Errorf("FastStep1: %w", err)
	}
	log.Printf("   ✅ FastStep1 result: %s", step1Result)

	// 3-second durable sleep. The crash will happen 1.5s in.
	if err := ctx.Sleep("chaos-sleep", 3*time.Second); err != nil {
		return fmt.Errorf("sleep: %w", err)
	}

	var step2Result string
	if err := ctx.ExecuteActivity("FastStep2", "input-B", &step2Result); err != nil {
		return fmt.Errorf("FastStep2: %w", err)
	}
	log.Printf("   ✅ FastStep2 result: %s", step2Result)

	return nil
}

// ─── Execution Tracker ────────────────────────────────────────────────────────

// Counts how many times each activity's real code ran (vs. was replayed).
// Replays do NOT increment this — they return early from the DB check.
var step1Count, step2Count int

func fastStep1(_ context.Context, idempotencyKey string, _ any) (any, error) {
	step1Count++
	log.Printf("   ⚡ [EXECUTE] FastStep1 (key=%s, total executions=%d)", idempotencyKey, step1Count)
	return "step1-done", nil
}

func fastStep2(_ context.Context, idempotencyKey string, _ any) (any, error) {
	step2Count++
	log.Printf("   ⚡ [EXECUTE] FastStep2 (key=%s, total executions=%d)", idempotencyKey, step2Count)
	return "step2-done", nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func buildReg() *registry.Registry {
	reg := registry.NewRegistry()
	reg.RegisterWorkflow("ChaosWorkflow", chaosWorkflow)
	reg.RegisterActivity("FastStep1", fastStep1)
	reg.RegisterActivity("FastStep2", fastStep2)
	return reg
}

// runEngine runs the workflow once and handles the yield/terminal state machine.
// Returns (false, nil) if the workflow yielded, (true, nil) if COMPLETED,
// or (false, err) on failure.
func runEngine(db *persistence.DB, reg *registry.Registry, wfID, workerID string) (completed bool, err error) {
	runErr := engine.Run(wfID, "ChaosWorkflow", reg, db)

	var yieldErr engine.YieldError
	switch {
	case runErr == nil:
		if dbErr := db.UpdateWorkflowStatus(wfID, "COMPLETED", workerID); dbErr != nil {
			return false, fmt.Errorf("marking COMPLETED: %w", dbErr)
		}
		return true, nil

	case errors.As(runErr, &yieldErr):
		log.Printf("   ⏸️  Yielded — sleeping workflow until %s", yieldErr.WakeAt.Format(time.RFC3339))
		if dbErr := db.SleepWorkflow(wfID, workerID, yieldErr.WakeAt); dbErr != nil {
			return false, fmt.Errorf("persisting sleep: %w", dbErr)
		}
		return false, nil

	default:
		_ = db.UpdateWorkflowStatus(wfID, "FAILED", workerID)
		return false, fmt.Errorf("workflow failed: %w", runErr)
	}
}

func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func claimOrFatal(db *persistence.DB, workerID string) string {
	records, err := db.GetPendingWorkflows(1, workerID)
	if err != nil || len(records) == 0 {
		log.Fatalf("FATAL: could not claim workflow: err=%v records=%v", err, records)
	}
	return records[0].ID
}

func assert(label string, pass bool) bool {
	if pass {
		log.Printf("   PASS ✅  %s", label)
	} else {
		log.Printf("   FAIL ❌  %s", label)
	}
	return pass
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	_ = context.Background() // satisfy import
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://engine_user:engine_password@localhost:5432/durable_engine?sslmode=disable"
	}
	allPassed := true

	fmt.Println("\n╔══════════════════════════════════════════════════╗")
	fmt.Println("║      DURABLE ENGINE — CHAOS TEST HARNESS         ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")

	// ─── Phase 1: Setup ───────────────────────────────────────────────────────
	fmt.Println("\n━━━ Phase 1: Setup ━━━")
	db1, err := persistence.ConnectDB(dsn)
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}

	wfID, err := newUUID()
	if err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	workerID1 := fmt.Sprintf("chaos-w1-%d", os.Getpid())

	if err := db1.CreateWorkflow(wfID, "ChaosWorkflow"); err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	// Atomically claim the new workflow so we hold the lease.
	claimOrFatal(db1, workerID1)
	log.Printf("✅ Created and claimed workflow: %s", wfID)

	// ─── Phase 2: First run — yields on Sleep ────────────────────────────────
	fmt.Println("\n━━━ Phase 2: First run (worker 1) ━━━")
	reg1 := buildReg()

	// We call engine.Run directly instead of the runEngine() helper.
	// We intentionally DO NOT call db.SleepWorkflow() after it yields to simulate
	// the worker dying the exact millisecond after appending to the event ledger.
	runErr := engine.Run(wfID, "ChaosWorkflow", reg1, db1)
	var yieldErr engine.YieldError
	if !errors.As(runErr, &yieldErr) {
		log.Fatalf("FAIL: expected a yield on first run, got err=%v", runErr)
	}
	log.Printf("✅ Worker 1 yielded correctly (FastStep1 done, SLEEP_START in ledger)")

	// ─── Phase 3: Simulate catastrophic crash ────────────────────────────────
	fmt.Println("\n━━━ Phase 3: Simulating catastrophic crash ━━━")
	log.Printf("   💥 CRASH: Worker 1 dies before calling db.SleepWorkflow()!")
	log.Printf("   💥 Workflow is orphaned as 'RUNNING' with an active lease.")

	// Wait 1.5s to simulate time passing during the outage.
	time.Sleep(1500 * time.Millisecond)
	db1.Close() // Force-close the DB connection

	// ─── Phase 4: New worker boots ───────────────────────────────────────────
	fmt.Println("\n━━━ Phase 4: New worker boots (worker 2) ━━━")
	db2, err := persistence.ConnectDB(dsn)
	if err != nil {
		log.Fatalf("FATAL: worker 2 could not connect: %v", err)
	}
	defer db2.Close()
	workerID2 := fmt.Sprintf("chaos-w2-%d", os.Getpid())
	reg2 := buildReg()
	log.Printf("✅ Worker 2 connected (ID: %s)", workerID2)

	// ─── Phase 5: Expire the lease so the poll can reclaim ───────────────────
	fmt.Println("\n━━━ Phase 5: Expiring lease (bypass 1-min TTL) ━━━")
	if err := db2.ExpireLeaseForTest(wfID); err != nil {
		log.Fatalf("FATAL: %v", err)
	}
	log.Printf("✅ Lease expired artificially")

	// ─── Phase 6: Worker 2 polls — sleep not yet elapsed ────────────────────
	fmt.Println("\n━━━ Phase 6: Worker 2 polls — sleep not yet elapsed ━━━")
	claimOrFatal(db2, workerID2)

	completed, err := runEngine(db2, reg2, wfID, workerID2)
	if err != nil || completed {
		log.Fatalf("FAIL: expected a re-yield (sleep not elapsed), got completed=%v err=%v", completed, err)
	}

	allPassed = assert(
		fmt.Sprintf("FastStep1 was NOT re-executed on first retry (executed=%d, want=1)", step1Count),
		step1Count == 1,
	) && allPassed

	log.Printf("✅ Worker 2 correctly re-yielded (sleep still has ~1.5s remaining)")

	// ─── Phase 7: Wait for sleep to elapse, poll again ───────────────────────
	fmt.Println("\n━━━ Phase 7: Waiting for sleep to elapse ━━━")

	// The sleep was scheduled from Phase 2 for 3s; 1.5s has elapsed in Phase 3.
	// Worker 2 must wait the remaining ~1.5s.
	// We over-wait slightly to account for clock jitter.
	time.Sleep(2 * time.Second)
	log.Printf("✅ Sleep should now be elapsed")

	// Expire lease again to simulate the scheduler re-queuing the workflow.
	if err := db2.ExpireLeaseForTest(wfID); err != nil {
		log.Fatalf("FATAL: second lease expire: %v", err)
	}

	// ─── Phase 8: Final run — should complete ────────────────────────────────
	fmt.Println("\n━━━ Phase 8: Final run — expecting COMPLETED ━━━")
	claimOrFatal(db2, workerID2)

	completed, err = runEngine(db2, reg2, wfID, workerID2)
	if err != nil {
		log.Fatalf("FAIL: final run returned error: %v", err)
	}
	if !completed {
		log.Fatalf("FAIL: final run did not complete the workflow (still yielding?)")
	}

	// ─── Phase 9: Assertions ─────────────────────────────────────────────────
	fmt.Println("\n━━━ Phase 9: Final Assertions ━━━")

	allPassed = assert(
		fmt.Sprintf("FastStep1 executed exactly 1 time (got=%d)", step1Count),
		step1Count == 1,
	) && allPassed

	allPassed = assert(
		fmt.Sprintf("FastStep2 executed exactly 1 time (got=%d)", step2Count),
		step2Count == 1,
	) && allPassed

	allPassed = assert(
		"workflow reached COMPLETED status",
		completed,
	) && allPassed

	fmt.Println()
	if allPassed {
		fmt.Println("╔══════════════════════════════════════════════════╗")
		fmt.Println("║       ALL CHAOS TESTS PASSED ✅                   ║")
		fmt.Println("╚══════════════════════════════════════════════════╝")
		os.Exit(0)
	} else {
		fmt.Println("╔══════════════════════════════════════════════════╗")
		fmt.Println("║       CHAOS TEST FAILED ❌                        ║")
		fmt.Println("╚══════════════════════════════════════════════════╝")
		os.Exit(1)
	}
}
