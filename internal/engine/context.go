package engine

import (
	"encoding/json"
	"fmt"

	"durable-engine/internal/persistence"
)

type WorkflowContext interface {
	ExecuteActivity(name string, input any) ([]byte, error)
}

type engineContext struct {
	workflowID     string
	sequenceNumber int
	db             *persistence.DB // We inject the database here
}

// NewEngineContext initializes a new workflow run with a fresh sequence number and database
func NewEngineContext(workflowID string, db *persistence.DB) *engineContext {
	return &engineContext{
		workflowID:     workflowID,
		sequenceNumber: 0,
		db:             db,
	}
}

func (c *engineContext) ExecuteActivity(name string, input any) ([]byte, error) {
	c.sequenceNumber++

	// 1. Check if this step already happened in a previous partial run
	event, err := c.db.GetEvent(c.workflowID, c.sequenceNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to check event history: %w", err)
	}

	// 2. THE REPLAY: If the event exists, skip execution and return the saved output
	if event != nil {
		fmt.Printf("[REPLAY] Skipping %s (Seq: %d). Already completed.\n", name, c.sequenceNumber)
		return event.OutputData, nil
	}

	// 3. THE EXECUTION: If it doesn't exist, run the actual code.
	fmt.Printf("[EXECUTE] Running %s (Seq: %d)...\n", name, c.sequenceNumber)

	inputBytes, _ := json.Marshal(input)

	// Simulate the activity doing some heavy work (like calling a payment API)
	outputBytes := []byte(`{"status": "success", "message": "Task finished"}`)

	// 4. THE EVENT SOURCING: Save the result to the immutable ledger
	err = c.db.SaveEvent(c.workflowID, "ACTIVITY_COMPLETED", name, inputBytes, outputBytes, c.sequenceNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to save event to ledger: %w", err)
	}

	return outputBytes, nil
}
