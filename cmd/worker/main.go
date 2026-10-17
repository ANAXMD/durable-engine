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
	"sync"
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

// ChaosWorkflow is a 3-step test workflow (Activity -> Sleep -> Activity) designed
// for multi-process chaos testing. It writes actual side-effects to the effects table
// keyed by the step's idempotency key.
func ChaosWorkflow(ctx engine.WorkflowContext) error {
	var step1Result string
	if err := ctx.ExecuteActivity("ChaosStep1", "input-step1", &step1Result); err != nil {
		return fmt.Errorf("ChaosStep1: %w", err)
	}

	if err := ctx.Sleep("chaos-sleep", 2*time.Second); err != nil {
		return fmt.Errorf("chaos-sleep: %w", err)
	}

	var step2Result string
	if err := ctx.ExecuteActivity("ChaosStep2", "input-step2", &step2Result); err != nil {
		return fmt.Errorf("ChaosStep2: %w", err)
	}

	return nil
}

func makeChaosActivity(db *persistence.DB, activityName string, latency time.Duration) func(ctx context.Context, idempKey string, input any) (any, error) {
	return func(ctx context.Context, idempKey string, input any) (any, error) {
		// Record the side-effect to the effects table using idempotencyKey as the primary key
		count, err := db.RecordEffect(idempKey, activityName, fmt.Sprintf("%v", input))
		if err != nil {
			return nil, fmt.Errorf("RecordEffect: %w", err)
		}

		if count > 1 {
			fmt.Printf("   ⚠️  [DUPLICATE EXECUTION] %s (idempotency_key: %s, execution #%d)\n", activityName, idempKey, count)
		} else {
			fmt.Printf("   ⚡ [SIDE-EFFECT RECORDED] %s (idempotency_key: %s)\n", activityName, idempKey)
		}

		// Simulate external latency during which SIGKILL can strike
		select {
		case <-time.After(latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		return fmt.Sprintf("result-%s", activityName), nil
	}
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

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://engine_user:engine_password@127.0.0.1:5432/durable_engine?sslmode=disable"
	}

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

	// Register ChaosWorkflow and its side-effect recording activities
	reg.RegisterWorkflow("ChaosWorkflow", ChaosWorkflow)
	reg.RegisterActivity("ChaosStep1", makeChaosActivity(db, "ChaosStep1", 2500*time.Millisecond))
	reg.RegisterActivity("ChaosStep2", makeChaosActivity(db, "ChaosStep2", 1000*time.Millisecond))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	shutdown := make(chan struct{})
	done := make(chan struct{})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /workflows", makeCreateWorkflowHandler(db, reg))
	server := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		fmt.Printf("🌐 HTTP API listening on :%s\n", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Fatal: HTTP server error: %v", err)
		}
	}()

	// Launch worker loop with goroutine pool for concurrent workflow execution
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

func makeCreateWorkflowHandler(db *persistence.DB, reg *registry.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := newUUID()
		if err != nil {
			http.Error(w, `{"error":"failed to generate id"}`, http.StatusInternalServerError)
			return
		}

		var req struct {
			WorkflowType string `json:"workflow_type"`
			Type         string `json:"type"`
		}
		// Allow caller to optionally specify workflow_type (or type), defaulting to MyOrderWorkflow
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&req)
		}
		wfType := req.WorkflowType
		if wfType == "" {
			wfType = req.Type
		}
		if wfType == "" {
			wfType = "MyOrderWorkflow"
		}

		// Validate workflow_type exists in the registry
		if _, err := reg.GetWorkflow(wfType); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"unknown workflow type: %s"}`, wfType), http.StatusBadRequest)
			return
		}

		if err := db.CreateWorkflow(id, wfType); err != nil {
			log.Printf("Error creating workflow: %v", err)
			http.Error(w, `{"error":"failed to create workflow"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{
			"workflow_id":   id,
			"workflow_type": wfType,
		})
	}
}

const maxConcurrentWorkflows = 5

func runWorkerLoop(shutdown <-chan struct{}, done chan<- struct{}, db *persistence.DB, reg *registry.Registry) {
	defer close(done)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "worker"
	}
	u, err := newUUID()
	if err != nil {
		log.Fatalf("Fatal: failed to generate worker UUID: %v", err)
	}
	workerID := fmt.Sprintf("%s-%s", hostname, u[:8])
	fmt.Printf("🔄 Worker loop running (ID: %s)...\n", workerID)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Goroutine pool management: bounded concurrency semaphore and in-flight waitgroup.
	sem := make(chan struct{}, maxConcurrentWorkflows)
	var activeWg sync.WaitGroup

	for {
		select {
		case <-shutdown:
			fmt.Println("🔄 Worker loop: shutdown signal received. Waiting for in-flight workflows to complete...")
			activeWg.Wait()
			return

		case <-ticker.C:
			// Claim only up to our remaining worker capacity so claimed jobs don't starve on the local queue.
			availableCapacity := maxConcurrentWorkflows - len(sem)
			if availableCapacity <= 0 {
				continue
			}

			workflows, err := db.GetPendingWorkflows(availableCapacity, workerID)
			if err != nil {
				log.Printf("❌ Error polling workflows: %v", err)
				continue
			}
			if len(workflows) == 0 {
				fmt.Println("🔍 Polling for workflows...")
				continue
			}

			for _, wf := range workflows {
				sem <- struct{}{}
				activeWg.Add(1)

				go func(wfRec persistence.WorkflowRecord) {
					defer func() {
						<-sem
						activeWg.Done()
					}()

					processWorkflow(db, reg, wfRec, workerID)
				}(wf)
			}
		}
	}
}

func processWorkflow(db *persistence.DB, reg *registry.Registry, wf persistence.WorkflowRecord, workerID string) {
	fmt.Printf("\n🚀 Claimed Workflow: %s (Type: %s)\n", wf.ID, wf.Type)

	runErr := engine.Run(wf.ID, wf.Type, reg, db, workerID)

	var yieldErr engine.YieldError
	switch {
	case runErr == nil:
		// All steps completed. Only succeeds if we still own the lease.
		if err := db.UpdateWorkflowStatus(wf.ID, "COMPLETED", workerID); err != nil {
			log.Printf("⚠️  Failed to mark %s COMPLETED: %v", wf.ID, err)
		} else {
			fmt.Printf("✅ Workflow %s marked COMPLETED.\n", wf.ID)
		}

	case errors.As(runErr, &yieldErr):
		// The workflow called Sleep(). Transition to SLEEPING state and
		// record the absolute wake time. The next poll that finds
		// wake_at <= NOW() will reclaim and resume this workflow.
		fmt.Printf("⏸️  Workflow %s sleeping until %s.\n", wf.ID, yieldErr.WakeAt.Format(time.RFC3339))
		if err := db.SleepWorkflow(wf.ID, workerID, yieldErr.WakeAt); err != nil {
			log.Printf("⚠️  Failed to sleep workflow %s: %v", wf.ID, err)
		}

	default:
		// Real error (panic or activity failure) — mark FAILED so the
		// workflow doesn't get retried indefinitely.
		log.Printf("❌ Workflow %s failed: %v", wf.ID, runErr)
		if err := db.UpdateWorkflowStatus(wf.ID, "FAILED", workerID); err != nil {
			log.Printf("⚠️  Failed to mark %s FAILED: %v", wf.ID, err)
		}
	}
}
