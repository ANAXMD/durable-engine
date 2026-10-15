package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
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

func MyOrderWorkflow(ctx engine.WorkflowContext) {
	ctx.ExecuteActivity("ChargeCreditCard", 100)
	ctx.ExecuteActivity("SendWelcomeEmail", "user@example.com")
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

	// Initialize the Registry and link the strings to our real functions
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

	// Pass the registry into the worker loop
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
		id, _ := newUUID()
		// Create the workflow targeting our real MyOrderWorkflow
		db.CreateWorkflow(id, "MyOrderWorkflow")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"workflow_id": id})
	}
}

func runWorkerLoop(shutdown <-chan struct{}, done chan<- struct{}, db *persistence.DB, reg *registry.Registry) {
	defer close(done)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-shutdown:
			return
		case <-ticker.C:
			workflows, err := db.GetPendingWorkflows(5)
			if err != nil || len(workflows) == 0 {
				continue
			}

			for _, wfID := range workflows {
				fmt.Printf("\n🚀 Claimed Pending Workflow: %s\n", wfID)

				// Hand the execution over to the Brain
				engine.Run(wfID, "MyOrderWorkflow", reg, db)

				db.UpdateWorkflowStatus(wfID, "COMPLETED")
				fmt.Printf("✅ Workflow %s marked COMPLETED.\n", wfID)
			}
		}
	}
}
