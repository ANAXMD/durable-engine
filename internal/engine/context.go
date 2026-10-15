package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	"durable-engine/internal/persistence"
	"durable-engine/internal/registry"
)

type WorkflowContext interface {
	ExecuteActivity(name string, input any) (any, error)
}

type engineContext struct {
	workflowID     string
	db             *persistence.DB
	registry       *registry.Registry
	sequenceNumber int
}

// Run is the entry point that dynamically invokes the workflow function using reflection.
func Run(wfID string, wfType string, reg *registry.Registry, db *persistence.DB) {
	ctx := &engineContext{
		workflowID:     wfID,
		db:             db,
		registry:       reg,
		sequenceNumber: 0,
	}

	fn, err := reg.GetWorkflow(wfType)
	if err != nil {
		log.Printf("❌ Failed to find workflow %s: %v", wfType, err)
		return
	}

	// Anti-Crash mechanism: catch panics to keep worker stable
	defer func() {
		if r := recover(); r != nil {
			log.Printf("⚠️ Workflow %s panicked: %v", wfID, r)
		}
	}()

	log.Printf("▶️ Starting workflow %s", wfID)

	// Dynamically pass the context into the workflow function via reflection
	fn.Call([]reflect.Value{reflect.ValueOf(ctx)})
}

func (c *engineContext) ExecuteActivity(name string, input any) (any, error) {
	c.sequenceNumber++

	// 1. Replay Phase: Check if this step already executed in a previous run
	event, err := c.db.GetEvent(c.workflowID, c.sequenceNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to check event history: %w", err)
	}

	if event != nil {
		log.Printf("⏪ [REPLAY] Skipping %s (Seq: %d). Already completed.", name, c.sequenceNumber)
		return event.OutputData, nil
	}

	// 2. Execution Phase: If it doesn't exist, run the registered activity code
	log.Printf("⚙️ [EXECUTE] Running NEW activity: %s (Seq: %d)...", name, c.sequenceNumber)

	actFn, err := c.registry.GetActivity(name)
	if err != nil {
		return nil, fmt.Errorf("activity %s not registered: %w", name, err)
	}

	result, err := actFn(context.Background(), input)
	if err != nil {
		return nil, fmt.Errorf("activity %s failed: %w", name, err)
	}

	// 3. Event Sourcing Phase: Save the result to the immutable ledger
	inputBytes, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal input for %s: %w", name, err)
	}

	outputBytes, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output for %s: %w", name, err)
	}

	err = c.db.SaveEvent(c.workflowID, "ACTIVITY_COMPLETED", name, inputBytes, outputBytes, c.sequenceNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to save event to ledger: %w", err)
	}

	return result, nil
}
