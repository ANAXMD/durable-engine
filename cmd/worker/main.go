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
)

// newUUID generates a random UUID (v4) using crypto/rand.
// We avoid an external dependency by building the canonical 8-4-4-4-12 hex
// form ourselves — the only requirement for Postgres uuid columns.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("failed to read random bytes: %w", err)
	}
	// Set version 4 (random) and variant bits per RFC 4122.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf(
		"%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:],
	), nil
}

func main() {
	fmt.Println("⚙️  Booting Durable Engine Worker...")

	// Connect to the Event Store. The DB handle is closed last, after both the
	// HTTP server and the worker loop have fully stopped, to prevent any
	// use-after-close races on the shared connection pool.
	dsn := "postgres://engine_user:engine_password@localhost:5432/durable_engine?sslmode=disable"
	db, err := persistence.ConnectDB(dsn)
	if err != nil {
		log.Fatalf("Fatal: Could not connect to database: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("Warning: error closing database connection: %v", closeErr)
		} else {
			fmt.Println("🗄️  Database connection closed.")
		}
	}()

	// --- Graceful Shutdown Setup ---
	//
	// sigCh receives OS-level termination signals. The buffer of 1 ensures the
	// signal runtime never blocks if we are not yet in the receive statement.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// shutdown is closed (not sent on) to broadcast the stop signal to every
	// goroutine at once. A single close() unblocks all receivers simultaneously,
	// which is more correct than sending N individual values.
	shutdown := make(chan struct{})

	// done lets the worker goroutine confirm it has exited before we tear down
	// the database. We must wait for it before db.Close() is called.
	done := make(chan struct{})

	// --- HTTP API Server ---
	//
	// The server is built before the goroutine that runs it so we hold a
	// reference for the controlled Shutdown() call during teardown.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /workflows", makeCreateWorkflowHandler(db))

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
		// Conservative timeouts to guard against slow clients / Slowloris attacks.
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Run the HTTP server in its own goroutine. ListenAndServe always returns a
	// non-nil error; ErrServerClosed is the expected value after Shutdown().
	go func() {
		fmt.Println("🌐 HTTP API listening on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Fatal: HTTP server error: %v", err)
		}
	}()

	// --- Worker Goroutine ---
	go runWorkerLoop(shutdown, done, db)

	fmt.Println("✅ Services started. Press Ctrl-C or send SIGTERM to stop.")

	// Block main until an OS signal arrives.
	sig := <-sigCh
	fmt.Printf("\n🛑 Received signal: %v. Initiating graceful shutdown...\n", sig)

	// --- Ordered Teardown ---
	//
	// Step 1: Stop accepting new HTTP connections and wait up to 5 seconds for
	// in-flight requests to complete. A deadline context ensures we never block
	// indefinitely if a client is holding a connection open.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Println("🌐 Draining HTTP connections (5s timeout)...")
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Warning: HTTP server shutdown error: %v", err)
	} else {
		fmt.Println("🌐 HTTP server stopped cleanly.")
	}

	// Step 2: Signal the worker loop to stop processing new tasks.
	close(shutdown)

	// Step 3: Wait for the worker to confirm it has finished its current task
	// and exited. Only after this is it safe to close the database pool.
	fmt.Println("🔄 Waiting for worker loop to finish...")
	<-done

	fmt.Println("👋 All services stopped cleanly. Exiting.")
	// The deferred db.Close() runs here, after both goroutines have exited.
}

// makeCreateWorkflowHandler returns an http.HandlerFunc that creates a new
// PENDING workflow and responds with its UUID. Using a closure to inject db
// keeps the handler signature compatible with http.HandleFunc while still
// following the dependency-injection principle from RULES.md.
func makeCreateWorkflowHandler(db *persistence.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Generate a cryptographically random UUID for the new workflow.
		id, err := newUUID()
		if err != nil {
			log.Printf("Error generating UUID: %v", err)
			http.Error(w, `{"error":"failed to generate workflow ID"}`, http.StatusInternalServerError)
			return
		}

		// Persist the new workflow in PENDING state so the worker loop will
		// pick it up on the next poll tick.
		if err := db.CreateWorkflow(id, "DynamicWorkflow"); err != nil {
			log.Printf("Error creating workflow %s: %v", id, err)
			http.Error(w, `{"error":"failed to create workflow"}`, http.StatusInternalServerError)
			return
		}

		fmt.Printf("📥 Created workflow via API: %s\n", id)

		// Respond with 201 Created and the new UUID in JSON.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"workflow_id": id})
	}
}

// runWorkerLoop is the long-running background goroutine that polls for pending
// workflow tasks. It uses a time.Ticker to perform work at a regular interval
// and respects the shutdown channel to exit gracefully.
//
// It signals completion via the done channel so main can sequence teardown
// (e.g., closing the DB) only after all in-flight work is finished.
func runWorkerLoop(shutdown <-chan struct{}, done chan<- struct{}, db *persistence.DB) {
	// Signal main that we have exited, regardless of which code path leads here.
	defer close(done)

	// A 2-second ticker drives the polling cadence. Stop() is deferred to
	// release the underlying runtime timer resources when the loop exits.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	fmt.Println("🔄 Worker loop running...")

	for {
		select {
		// The shutdown channel is closed by main after the HTTP server has
		// drained. A closed channel is always immediately readable, so this
		// case fires as soon as close(shutdown) is called.
		case <-shutdown:
			fmt.Println("🔄 Worker loop: shutdown signal received. Stopping.")
			return

		// On each tick, poll for pending workflows and execute them.
		case <-ticker.C:
			// 1. Ask the database for up to 5 tasks to process this cycle.
			workflows, err := db.GetPendingWorkflows(5)
			if err != nil {
				log.Printf("❌ Database error polling workflows: %v", err)
				continue
			}

			if len(workflows) == 0 {
				fmt.Println("🔍 Polling for workflows...")
				continue
			}

			// 2. Execute each claimed workflow through the replay engine.
			for _, wfID := range workflows {
				fmt.Printf("\n🚀 Claimed Pending Workflow: %s\n", wfID)

				ctx := engine.NewEngineContext(wfID, db)

				_, err1 := ctx.ExecuteActivity("ChargeCreditCard", map[string]interface{}{"amount": 100})
				_, err2 := ctx.ExecuteActivity("SendReceipt", map[string]interface{}{"email": "user@example.com"})

				// 3. Mark as COMPLETED only when both activities succeeded, so
				// the engine can replay and retry on a future tick if needed.
				if err1 == nil && err2 == nil {
					db.UpdateWorkflowStatus(wfID, "COMPLETED")
					fmt.Printf("✅ Workflow %s marked COMPLETED.\n", wfID)
				}
			}
		}
	}
}
