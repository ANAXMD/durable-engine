package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	"durable-engine/internal/persistence"
	"durable-engine/internal/registry"
)

// ErrYield is returned by Run when a workflow pauses itself via Sleep().
// The worker must keep the workflow status as RUNNING and revisit it on the
// next poll tick, at which point the sleep event will be replayed and the
// workflow will resume from where it paused.
var ErrYield = errors.New("workflow yielded: sleep not yet elapsed")

// yieldPanic is the sentinel value thrown via panic() inside Sleep() and
// caught by the defer/recover in Run(). We use a typed string constant rather
// than a plain string to avoid accidental collisions with other panics.
const yieldPanic = "YIELD_SLEEP"

// WorkflowContext is the interface injected into every workflow function.
// Using resultPtr any (instead of a generic) keeps the interface compatible
// with reflection-based dispatch while still enabling type-safe result binding.
type WorkflowContext interface {
	// ExecuteActivity runs a named activity with input and deserializes the
	// result into resultPtr. On replay, it unmarshals the stored JSON. On
	// first execution, it runs the real code, saves it, then unmarshals.
	ExecuteActivity(name string, input any, resultPtr any) error

	// Sleep pauses the workflow durably. On the first encounter it saves a
	// SLEEP_START event and panics with yieldPanic to unwind the goroutine.
	// On subsequent encounters (replay after re-schedule), it returns nil
	// immediately, allowing execution to continue past the sleep point.
	Sleep(sleepName string, d time.Duration) error
	Now() (time.Time, error)
}

type engineContext struct {
	workflowID     string
	db             *persistence.DB
	registry       *registry.Registry
	sequenceNumber int
}

// Run is the entry point that dynamically invokes the workflow function using
// reflection. It returns ErrYield if the workflow paused itself via Sleep(),
// or nil on clean completion. Any other panic is logged and swallowed to keep
// the worker goroutine alive.
func Run(wfID string, wfType string, reg *registry.Registry, db *persistence.DB) (err error) {
	ctx := &engineContext{
		workflowID:     wfID,
		db:             db,
		registry:       reg,
		sequenceNumber: 0,
	}

	fn, lookupErr := reg.GetWorkflow(wfType)
	if lookupErr != nil {
		return fmt.Errorf("❌ workflow %s not registered: %w", wfType, lookupErr)
	}

	// Intercept panics thrown by Sleep() (yieldPanic) separately from real
	// crashes. Named return `err` lets us assign inside the deferred closure.
	defer func() {
		r := recover()
		if r == nil {
			// Normal completion — no panic, err stays nil.
			return
		}
		if r == yieldPanic {
			// The workflow called Sleep() for the first time.
			// Signal the caller to leave the status as RUNNING.
			log.Printf("⏸️  Workflow %s yielded (sleep not yet elapsed).", wfID)
			err = ErrYield
			return
		}
		// Any other panic is a real bug — log it without crashing the worker.
		log.Printf("⚠️  Workflow %s panicked unexpectedly: %v", wfID, r)
	}()

	log.Printf("▶️  Starting workflow %s (type: %s)", wfID, wfType)

	// Dynamically pass the WorkflowContext into the workflow function.
	fn.Call([]reflect.Value{reflect.ValueOf(ctx)})
	return nil
}

// ExecuteActivity implements the Replay → Execute → Persist cycle.
func (c *engineContext) ExecuteActivity(name string, input any, resultPtr any) error {
	c.sequenceNumber++

	// 1. Replay Phase: look up this sequence position in the event ledger.
	event, err := c.db.GetEvent(c.workflowID, c.sequenceNumber)
	if err != nil {
		return fmt.Errorf("failed to check event history: %w", err)
	}

	if event != nil {
		log.Printf("⏪ [REPLAY]   %s (Seq %d) — deserializing saved output.", name, c.sequenceNumber)
		// Unmarshal the stored JSON directly into the caller's result pointer.
		// This guarantees the workflow receives the same typed value on replay
		// as it did during the original execution.
		if err := json.Unmarshal(event.OutputData, resultPtr); err != nil {
			return fmt.Errorf("failed to unmarshal replayed output for %s: %w", name, err)
		}
		return nil
	}

	// 2. Execution Phase: activity has never run — call the real code.
	log.Printf("⚙️  [EXECUTE]  %s (Seq %d) — running activity...", name, c.sequenceNumber)

	actFn, err := c.registry.GetActivity(name)
	if err != nil {
		return fmt.Errorf("activity %s not registered: %w", name, err)
	}

	rawResult, err := actFn(context.Background(), input)
	if err != nil {
		return fmt.Errorf("activity %s failed: %w", name, err)
	}

	// 3. Type-Safety Round-Trip: marshal then unmarshal into resultPtr.
	// This ensures that what the workflow sees is the exact same type it
	// would see on a replay — preventing subtle bugs where a live result
	// has a concrete Go type (e.g. string) but a replayed one would be
	// json.RawMessage if we skipped this step.
	outputBytes, err := json.Marshal(rawResult)
	if err != nil {
		return fmt.Errorf("failed to marshal output for %s: %w", name, err)
	}
	if err := json.Unmarshal(outputBytes, resultPtr); err != nil {
		return fmt.Errorf("failed to unmarshal output into resultPtr for %s: %w", name, err)
	}

	// 4. Event Sourcing Phase: persist to the immutable ledger.
	inputBytes, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("failed to marshal input for %s: %w", name, err)
	}
	if err := c.db.SaveEvent(c.workflowID, "ACTIVITY_COMPLETED", name, inputBytes, outputBytes, c.sequenceNumber); err != nil {
		return fmt.Errorf("failed to save event for %s: %w", name, err)
	}

	return nil
}

// Sleep implements a durable pause.
//
// First encounter: saves a SLEEP_START event to the ledger to record when the
// sleep was requested, then panics with yieldPanic to unwind the goroutine.
// The deferred recover() in Run() catches this and returns ErrYield.
//
// Subsequent encounters (workflow re-runs after re-scheduling): the SLEEP_START
// event already exists in the ledger so we return nil immediately, letting the
// workflow code continue past this point. The caller is responsible for
// re-queuing the workflow after the duration has elapsed (a future scheduler).
func (c *engineContext) Sleep(sleepName string, d time.Duration) error {
	c.sequenceNumber++

	// Check if this sleep was already recorded in the ledger.
	event, err := c.db.GetEvent(c.workflowID, c.sequenceNumber)
	if err != nil {
		return fmt.Errorf("failed to check sleep event: %w", err)
	}

	if event != nil {
		// Sleep was already recorded — the workflow is resuming past this point.
		log.Printf("⏩ [RESUME]   Sleep '%s' (Seq %d) — already elapsed, continuing.", sleepName, c.sequenceNumber)
		return nil
	}

	// First time we hit this sleep: record the start time in the ledger so
	// that on the next run we know when the sleep was requested.
	durationBytes, _ := json.Marshal(d.String())
	if err := c.db.SaveEvent(c.workflowID, "SLEEP_START", sleepName, durationBytes, durationBytes, c.sequenceNumber); err != nil {
		return fmt.Errorf("failed to save sleep event: %w", err)
	}

	log.Printf("💤 [SLEEP]    '%s' (Seq %d) — yielding for %s.", sleepName, c.sequenceNumber, d)

	// Panic with the sentinel value. Run()'s defer/recover will catch this,
	// distinguish it from a real crash, and return ErrYield to the worker.
	panic(yieldPanic)
}

// Now safely returns the current time in a deterministic way.
// On the first run, it generates time.Now() and saves it to the ledger.
// On replays, it fetches the exact same timestamp from the ledger so
// the workflow logic never changes between crashes.
func (c *engineContext) Now() (time.Time, error) {
	c.sequenceNumber++

	// 1. Replay Phase: Did we already check the time in a previous run?
	event, err := c.db.GetEvent(c.workflowID, c.sequenceNumber)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to check event history for Now(): %w", err)
	}

	if event != nil {
		log.Printf("⏪ [REPLAY]   Now() (Seq %d) — returning saved timestamp.", c.sequenceNumber)
		var savedTime time.Time
		if err := json.Unmarshal(event.OutputData, &savedTime); err != nil {
			return time.Time{}, fmt.Errorf("failed to unmarshal time: %w", err)
		}
		return savedTime, nil
	}

	// 2. Execution Phase: Generate the real time and save it.
	log.Printf("⏱️  [SIDE EFFECT] Now() (Seq %d) — generating new timestamp.", c.sequenceNumber)
	currentTime := time.Now()

	timeBytes, _ := json.Marshal(currentTime)

	// Save it as a "SIDE_EFFECT" event. Input is empty because it takes no arguments.
	if err := c.db.SaveEvent(c.workflowID, "SIDE_EFFECT", "TimeNow", []byte("{}"), timeBytes, c.sequenceNumber); err != nil {
		return time.Time{}, fmt.Errorf("failed to save time to ledger: %w", err)
	}

	return currentTime, nil
}
