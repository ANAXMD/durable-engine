package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"durable-engine/internal/engine"
	"durable-engine/internal/persistence"
	"durable-engine/internal/registry"
)

// --- 1. THE ACTUAL WORKFLOW AND ACTIVITIES ---

// MyOrderWorkflow is the orchestrator. It now returns an error so that
// failures propagate cleanly back to the worker loop instead of being
// silently dropped via a bare `return`.
func MyOrderWorkflow(ctx engine.WorkflowContext) error {
	// Deterministic time: saved to the ledger on first run, replayed on restart.
	startedAt, err := ctx.Now()
	if err != nil {
		return fmt.Errorf("now: %w", err)
	}
	fmt.Printf("   ⏰ Workflow started at: %s\n", startedAt.Format(time.Kitchen))

	// Type-safe result pointer: same value on first run and replay.
	var txID string
	if err := ctx.ExecuteActivity("ChargeCreditCard", 100, &txID); err != nil {
		return fmt.Errorf("ChargeCreditCard: %w", err)
	}
	fmt.Printf("   💰 Transaction ID: %s\n", txID)

	// Durable sleep: yields on first run; resumes transparently after timer fires.
	if err := ctx.Sleep("wait-before-email", 5*time.Second); err != nil {
		return fmt.Errorf("sleep: %w", err)
	}

	var emailResult string
	if err := ctx.ExecuteActivity("SendWelcomeEmail", "user@example.com", &emailResult); err != nil {
		return fmt.Errorf("SendWelcomeEmail: %w", err)
	}
	fmt.Printf("   📬 Email result: %s\n", emailResult)

	return nil
}

func ChargeActivity(ctx context.Context, idempotencyKey string, input any) (any, error) {
	// In production: pass idempotencyKey to Stripe as the Idempotency-Key header
	// so a crash-then-retry never charges the customer twice.
	fmt.Printf("   💳 [STRIPE API] Charging (key: %s)...\n", idempotencyKey)
	time.Sleep(2 * time.Second) // Simulate network latency
	return "tx_12345", nil
}

func EmailActivity(ctx context.Context, idempotencyKey string, input any) (any, error) {
	// In production: pass idempotencyKey to SendGrid to prevent duplicate emails.
	fmt.Printf("   📧 [SENDGRID API] Sending email (key: %s)...\n", idempotencyKey)
	time.Sleep(2 * time.Second) // Simulate network latency
	return "email_sent", nil
}

// --- 2. MICROSERVICE SETUP ---

func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func main() {
	fmt.Println("⚙️  Booting Durable Engine Worker...")

	dsn := "postgres://engine_user:engine_password@localhost:5432/durable_engine?sslmode=disable"
	db, err := persistence.ConnectDB(dsn)
	if err != nil {
		log.Fatalf("Fatal: Could not connect to database: %v", err)
	}
	defer db.Close()

	// Initialize the Registry and link strings to our real functions.
	reg := registry.NewRegistry()
	reg.RegisterWorkflow("MyOrderWorkflow", MyOrderWorkflow)
	reg.RegisterActivity("ChargeCreditCard", ChargeActivity)
	reg.RegisterActivity("SendWelcomeEmail", EmailActivity)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	shutdown := make(chan struct{})
	done := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("POST /workflows", makeCreateWorkflowHandler(db))
	server := &http.Server{Addr: ":8080", Handler: mux}

	go func() {
		fmt.Println("🌐 HTTP API listening on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Fatal: HTTP server error: %v", err)
		}
	}()

	// Pass the registry into the worker loop so it can trigger engine.Run().
	// NOTE: ResetStuckWorkflows() has been intentionally removed. Lease expiry
	// handles zombie recovery automatically — a boot-time reset would wrongly
	// re-queue workflows actively held by other workers in a multi-process setup.
	go runWorkerLoop(shutdown, done, db, reg)

	<-sigCh
	fmt.Println("\n🛑 Initiating graceful shutdown...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(shutdownCtx)

	close(shutdown)
	<-done
	fmt.Println("👋 All services stopped cleanly. Exiting.")
}

func makeCreateWorkflowHandler(db *persistence.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := newUUID()
		if err != nil {
			http.Error(w, `{"error":"failed to generate id"}`, http.StatusInternalServerError)
			return
		}
		if err := db.CreateWorkflow(id, "MyOrderWorkflow"); err != nil {
			log.Printf("Error creating workflow: %v", err)
			http.Error(w, `{"error":"failed to create workflow"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"workflow_id": id})
	}
}

func runWorkerLoop(shutdown <-chan struct{}, done chan<- struct{}, db *persistence.DB, reg *registry.Registry) {
	defer close(done)

	// workerID uniquely identifies this process instance. Using PID is a simple
	// approach that works well for single-host deployments. In Kubernetes, you
	// would use the pod name or a UUID generated at startup.
	workerID := fmt.Sprintf("worker-%d", os.Getpid())
	fmt.Printf("🔄 Worker loop running (ID: %s)...\n", workerID)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-shutdown:
			fmt.Println("🔄 Worker loop: shutdown signal received. Stopping.")
			return

		case <-ticker.C:
			// GetPendingWorkflows claims rows atomically with FOR UPDATE SKIP LOCKED.
			// The workerID is stamped onto the lease so only we can finalize the result.
			workflows, err := db.GetPendingWorkflows(5, workerID)
			if err != nil {
				log.Printf("❌ Error polling workflows: %v", err)
				continue
			}
			if len(workflows) == 0 {
				fmt.Println("🔍 Polling for workflows...")
				continue
			}

			for _, wfID := range workflows {
				fmt.Printf("\n🚀 Claimed Workflow: %s\n", wfID)

				runErr := engine.Run(wfID, "MyOrderWorkflow", reg, db)

				var yieldErr engine.YieldError
				switch {
				case runErr == nil:
					// All steps completed. Only succeeds if we still own the lease.
					if err := db.UpdateWorkflowStatus(wfID, "COMPLETED", workerID); err != nil {
						log.Printf("⚠️  Failed to mark %s COMPLETED: %v", wfID, err)
					} else {
						fmt.Printf("✅ Workflow %s marked COMPLETED.\n", wfID)
					}

				case errors.As(runErr, &yieldErr):
					// The workflow called Sleep(). Transition to SLEEPING state and
					// record the absolute wake time. The next poll that finds
					// wake_at <= NOW() will reclaim and resume this workflow.
					fmt.Printf("⏸️  Workflow %s sleeping until %s.\n", wfID, yieldErr.WakeAt.Format(time.RFC3339))
					if err := db.SleepWorkflow(wfID, workerID, yieldErr.WakeAt); err != nil {
						log.Printf("⚠️  Failed to sleep workflow %s: %v", wfID, err)
					}

				default:
					// Real error (panic or activity failure) — mark FAILED so the
					// workflow doesn't get retried indefinitely.
					log.Printf("❌ Workflow %s failed: %v", wfID, runErr)
					if err := db.UpdateWorkflowStatus(wfID, "FAILED", workerID); err != nil {
						log.Printf("⚠️  Failed to mark %s FAILED: %v", wfID, err)
					}
				}
			}
		}
	}
}
