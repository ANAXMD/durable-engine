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

// MyOrderWorkflow is the orchestrator. It demonstrates two activities and a
// durable sleep. The sleep will pause the workflow on first run and resume
// it on the next worker poll (simulating a real timer / delay).
func MyOrderWorkflow(ctx engine.WorkflowContext) {
	// Use typed result pointers for strict type safety across replay boundaries.
	var txID string
	if err := ctx.ExecuteActivity("ChargeCreditCard", 100, &txID); err != nil {
		log.Printf("❌ ChargeCreditCard failed: %v", err)
		return
	}
	fmt.Printf("   💰 Transaction ID: %s\n", txID)

	// Durable sleep: the workflow will yield here on first run and resume
	// on re-execution after the sleep event is already in the ledger.
	if err := ctx.Sleep("wait-before-email", 5*time.Second); err != nil {
		log.Printf("❌ Sleep failed: %v", err)
		return
	}

	var emailResult string
	if err := ctx.ExecuteActivity("SendWelcomeEmail", "user@example.com", &emailResult); err != nil {
		log.Printf("❌ SendWelcomeEmail failed: %v", err)
		return
	}
	fmt.Printf("   📬 Email result: %s\n", emailResult)
}

func ChargeActivity(ctx context.Context, input any) (any, error) {
	// Simulate talking to Stripe
	fmt.Printf("   💳 [STRIPE API] Charging credit card...\n")
	time.Sleep(2 * time.Second) // Simulate network delay
	return "tx_12345", nil
}

func EmailActivity(ctx context.Context, input any) (any, error) {
	// Simulate talking to SendGrid
	fmt.Printf("   📧 [SENDGRID API] Sending welcome email...\n")
	time.Sleep(2 * time.Second) // Simulate network delay
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
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	fmt.Println("🔄 Worker loop running...")

	for {
		select {
		case <-shutdown:
			fmt.Println("🔄 Worker loop: shutdown signal received. Stopping.")
			return

		case <-ticker.C:
			// GetPendingWorkflows now atomically claims rows with FOR UPDATE SKIP LOCKED,
			// so concurrent workers will never pick up the same workflow twice.
			workflows, err := db.GetPendingWorkflows(5)
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

				switch {
				case runErr == nil:
					// Workflow completed all steps cleanly.
					if err := db.UpdateWorkflowStatus(wfID, "COMPLETED"); err != nil {
						log.Printf("⚠️  Failed to mark %s COMPLETED: %v", wfID, err)
					} else {
						fmt.Printf("✅ Workflow %s marked COMPLETED.\n", wfID)
					}

				case errors.Is(runErr, engine.ErrYield):
					fmt.Printf("⏸️  Workflow %s is sleeping — leaving as RUNNING.\n", wfID)

					// --- SCHEDULER HACK: Simulate a Timer Queue waking it up ---
					go func(id string) {
						time.Sleep(5 * time.Second)
						db.UpdateWorkflowStatus(id, "PENDING")
					}(wfID)

				default:
					// Unexpected error — mark as FAILED so it doesn't re-run.
					log.Printf("❌ Workflow %s failed: %v", wfID, runErr)
					if err := db.UpdateWorkflowStatus(wfID, "FAILED"); err != nil {
						log.Printf("⚠️  Failed to mark %s FAILED: %v", wfID, err)
					}
				}
			}
		}
	}
}
