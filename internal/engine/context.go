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

// heartbeatInterval controls how often in-flight activities extend their lease.
// It defaults to 20 seconds, but can be adjusted in tests for rapid verification.
var heartbeatInterval = 20 * time.Second

// WorkflowContext is the interface injected into every workflow function.
// Using resultPtr any (instead of a generic) keeps the interface compatible
// with reflection-based dispatch while still enabling type-safe result binding.
type WorkflowContext interface {
	// ExecuteActivity runs a named activity with input and deserializes the
	// result into resultPtr. On replay, it unmarshals the stored JSON. On
	// first execution, it runs the real code, saves it, then unmarshals.
	ExecuteActivity(name string, input any, resultPtr any) error

	// Sleep pauses the workflow durably. On first encounter it saves the
	// absolute wake-up time and panics with a YieldError. On re-execution,
	// if the wake time has not elapsed it panics again; if it has, it returns
	// nil and execution continues.
	Sleep(sleepName string, d time.Duration) error

	// Now returns the current time deterministically. The first call saves the
	// timestamp to the ledger; all replays return the exact same saved value.
	Now() (time.Time, error)
}

// YieldError is panicked by Sleep() and caught by Run(). It carries the
// absolute time the workflow wants to be woken at so the worker can persist
// it to the database via SleepWorkflow().
//
// It is a struct (not a plain error value) so errors.As() can extract WakeAt
// from within an arbitrary error wrapping chain.
type YieldError struct {
	WakeAt time.Time
}

func (y YieldError) Error() string {
	return fmt.Sprintf("workflow yielded: sleeping until %s", y.WakeAt.Format(time.RFC3339))
}

type engineContext struct {
	workflowID     string
	workerID       string
	db             *persistence.DB
	registry       *registry.Registry
	sequenceNumber int
}

// Run is the entry point that dynamically invokes the workflow function using
// reflection. Return values:
//
//   - nil            — workflow completed all steps cleanly
//   - YieldError     — workflow called Sleep() and is not yet due to resume
//   - any other error — workflow panicked or a step returned an error
//
// The workflow function is called via reflect so it can be registered by name
// without the engine having a compile-time dependency on user code.
func Run(wfID string, wfType string, reg *registry.Registry, db *persistence.DB, workerID string) (err error) {
	ctx := &engineContext{
		workflowID:     wfID,
		workerID:       workerID,
		db:             db,
		registry:       reg,
		sequenceNumber: 0,
	}

	fn, lookupErr := reg.GetWorkflow(wfType)
	if lookupErr != nil {
		return fmt.Errorf("workflow %s not registered: %w", wfType, lookupErr)
	}

	// Named return `err` lets the deferred closure assign into it.
	defer func() {
		r := recover()
		if r == nil {
			// fn.Call returned normally — err is whatever was set before defer.
			return
		}

		// YieldError: the workflow called Sleep() and needs to be re-scheduled.
		if ye, ok := r.(YieldError); ok {
			log.Printf("⏸️  Workflow %s yielded until %s.", wfID, ye.WakeAt.Format(time.RFC3339))
			err = ye // surface to caller for SleepWorkflow()
			return
		}

		// Any other panic is a real bug. Wrap it as an error so the worker can
		// mark the workflow FAILED rather than silently dropping it.
		err = fmt.Errorf("workflow %s panicked: %v", wfID, r)
		log.Printf("⚠️  %v", err)
	}()

	log.Printf("▶️  Starting workflow %s (type: %s)", wfID, wfType)

	// Reflect-based dispatch: call fn(ctx WorkflowContext) error
	// We check the return value so workflow functions can propagate errors.
	results := fn.Call([]reflect.Value{reflect.ValueOf(ctx)})

	// Workflows that return an error value signal failure via the return channel
	// rather than panicking — check for a non-nil error in the first return value.
	if len(results) > 0 && !results[0].IsNil() {
		return results[0].Interface().(error)
	}

	return nil
}

// ExecuteActivity implements the Replay → Execute → Persist cycle with
// idempotency keys and non-determinism detection.
//
// Crash safety guarantee:
//
//	ACTIVITY_STARTED is written BEFORE the activity runs. If the worker crashes
//	mid-activity, on the next run GetEvent returns the STARTED event, the engine
//	detects it has no output (type == ACTIVITY_STARTED), and re-executes the
//	activity with the same idempotency key — the external API can use that key
//	to deduplicate the request without charging twice.
func (c *engineContext) ExecuteActivity(name string, input any, resultPtr any) error {
	c.sequenceNumber++

	// Generate a stable idempotency key from the workflow ID and sequence number.
	// This key uniquely identifies "this specific step in this specific workflow run"
	// and is safe to pass to external APIs (Stripe, SendGrid, etc.) as a
	// deduplication token.
	idempotencyKey := fmt.Sprintf("%s-%d", c.workflowID, c.sequenceNumber)

	// 1. Replay Phase: look up this sequence position in the event ledger.
	event, err := c.db.GetEvent(c.workflowID, c.sequenceNumber)
	if err != nil {
		return fmt.Errorf("failed to check event history: %w", err)
	}

	if event != nil {
		// Non-Determinism Detection: verify the history matches the code.
		// If the workflow was changed (e.g., a new activity inserted before this
		// one), the event at this sequence position will have a different name.
		// Continuing would corrupt history by replaying the wrong activity's
		// output into the new step — a silent, catastrophic data bug.
		if event.EventName != name {
			return fmt.Errorf(
				"non-deterministic execution detected at Seq %d: expected activity '%s', found '%s' in history — do not reorder or insert workflow steps",
				c.sequenceNumber, name, event.EventName,
			)
		}

		// If the saved event is ACTIVITY_STARTED (crash between start and complete),
		// fall through to re-execute with the same idempotencyKey.
		// External APIs that received the key will deduplicate; local state is safe.
		if event.EventType == "ACTIVITY_COMPLETED" {
			log.Printf("⏪ [REPLAY]   %s (Seq %d) — deserializing saved output.", name, c.sequenceNumber)
			if err := json.Unmarshal(event.OutputData, resultPtr); err != nil {
				return fmt.Errorf("failed to unmarshal replayed output for %s: %w", name, err)
			}
			return nil
		}

		log.Printf("🔁 [RETRY]    %s (Seq %d) — found STARTED but no COMPLETED, re-executing with same idempotency key.", name, c.sequenceNumber)
	}

	// 2. Intent Recording: save ACTIVITY_STARTED BEFORE calling the real code.
	// This closes the crash window: if we crash between STARTED and COMPLETED,
	// the next run will find this event and retry with the same idempotencyKey.
	// inserted == false means a previous attempt already wrote this row; safe to continue.
	inputBytes, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("failed to marshal input for %s: %w", name, err)
	}
	if _, err := c.db.SaveEvent(c.workflowID, "ACTIVITY_STARTED", name, inputBytes, []byte("{}"), c.sequenceNumber); err != nil {
		return fmt.Errorf("failed to save ACTIVITY_STARTED for %s: %w", name, err)
	}

	// 3. Execution Phase: call the real activity code with the idempotency key.
	log.Printf("⚙️  [EXECUTE]  %s (Seq %d, key: %s) — running activity...", name, c.sequenceNumber, idempotencyKey)

	actFn, err := c.registry.GetActivity(name)
	if err != nil {
		return fmt.Errorf("activity %s not registered: %w", name, err)
	}

	actCtx, actCancel := context.WithCancel(context.Background())
	defer actCancel()

	// Heartbeats: start a background goroutine to extend lease every 30s while executing.
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)

	go func() {
		// Tick at 20s — well within the 60s lease — to provide two heartbeat
		// opportunities before the lease would naturally expire.
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				if c.workerID == "" {
					continue
				}
				if hbErr := c.db.ExtendLease(c.workflowID, c.workerID, 1*time.Minute); hbErr != nil {
					if errors.Is(hbErr, persistence.ErrLeaseLost) {
						// Definitive: another worker now owns the lease.
						// Cancel the activity context immediately so the
						// activity stops running without a lease.
						log.Printf("🔴 [HEARTBEAT] Lease permanently lost for %s — cancelling activity.", c.workflowID)
						actCancel()
						return
					}
					// Transient error (e.g. network blip). Log and let the
					// next tick retry — do NOT cancel the activity context.
					log.Printf("⚠️  [HEARTBEAT] Transient error extending lease for %s: %v", c.workflowID, hbErr)
				} else {
					log.Printf("💓 [HEARTBEAT] Extended lease for workflow %s by 1m", c.workflowID)
				}
			}
		}
	}()

	// Transient Activity Retries: exponential backoff retrying up to 3 times (wait 2s, 4s, 8s).
	backoffs := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}
	var rawResult any
	var actErr error

	const defaultActivityTimeout = 30 * time.Second

	for attempt := 0; attempt <= len(backoffs); attempt++ {
		if attempt > 0 {
			if actCtx.Err() != nil {
				break
			}
			waitTime := backoffs[attempt-1]
			log.Printf("⚠️  [RETRY] Activity %s failed (attempt %d/%d): %v. Retrying in %v...", name, attempt, len(backoffs), actErr, waitTime)
			select {
			case <-time.After(waitTime):
			case <-actCtx.Done():
				break
			}
		}
		if actCtx.Err() != nil {
			break
		}

		attemptCtx, attemptCancel := context.WithTimeout(actCtx, defaultActivityTimeout)
		rawResult, actErr = actFn(attemptCtx, idempotencyKey, input)
		attemptCancel()

		if actErr == nil {
			break
		}
	}

	if actCtx.Err() != nil && actErr == nil {
		actErr = actCtx.Err()
	}

	if actErr != nil {
		// If the activity context was cancelled by the heartbeat (lease lost), do NOT
		// write an ACTIVITY_FAILED event. The lease is gone, so we almost certainly no
		// longer own the workflow row. Writing a failure event here could corrupt a
		// concurrently-running worker's state. Instead, return silently like a crash:
		// the other worker (or the next reclaim cycle) will handle it cleanly.
		if errors.Is(actCtx.Err(), context.Canceled) {
			log.Printf("🔴 [FAILED] Activity %s abandoned — lease was lost mid-execution.", name)
			return fmt.Errorf("activity %s abandoned: lease lost mid-execution: %w", name, actErr)
		}
		log.Printf("❌ [FAILED] Activity %s failed after %d retries: %v", name, len(backoffs), actErr)
		errBytes, _ := json.Marshal(map[string]string{"error": actErr.Error()})
		// inserted == false means ACTIVITY_FAILED was already recorded (idempotent); safe to ignore.
		if _, saveErr := c.db.SaveEvent(c.workflowID, "ACTIVITY_FAILED", name, inputBytes, errBytes, c.sequenceNumber); saveErr != nil {
			log.Printf("⚠️  Failed to save ACTIVITY_FAILED for %s: %v", name, saveErr)
		}
		return fmt.Errorf("activity %s failed after retries: %w", name, actErr)
	}

	// 4. Type-Safety Round-Trip: serialize output and persist event.
	outputBytes, err := json.Marshal(rawResult)
	if err != nil {
		return fmt.Errorf("failed to marshal output for %s: %w", name, err)
	}

	// 5. Event Sourcing Phase: persist ACTIVITY_COMPLETED to the immutable ledger.
	// inserted == false means another worker won the race to write this event.
	// Adopt the winner's stored output rather than using our locally-computed value,
	// so both workers continue with identical in-memory state.
	inserted, saveErr := c.db.SaveEvent(c.workflowID, "ACTIVITY_COMPLETED", name, inputBytes, outputBytes, c.sequenceNumber)
	if saveErr != nil {
		return fmt.Errorf("failed to save ACTIVITY_COMPLETED for %s: %w", name, saveErr)
	}
	if !inserted {
		log.Printf("⚠️  [RACE LOST] Activity %s (Seq %d) was completed by another worker. Fetching winner's output...", name, c.sequenceNumber)
		winnerEvent, getErr := c.db.GetEvent(c.workflowID, c.sequenceNumber)
		if getErr != nil {
			return fmt.Errorf("failed to fetch winner event after race lost for %s: %w", name, getErr)
		}
		if winnerEvent == nil {
			return fmt.Errorf("winner event not found after race lost for %s (seq %d)", name, c.sequenceNumber)
		}
		if err := json.Unmarshal(winnerEvent.OutputData, resultPtr); err != nil {
			return fmt.Errorf("failed to unmarshal winner output for %s: %w", name, err)
		}
		return nil
	}

	if err := json.Unmarshal(outputBytes, resultPtr); err != nil {
		return fmt.Errorf("failed to unmarshal output into resultPtr for %s: %w", name, err)
	}

	return nil
}

// Sleep implements a durable pause.
//
// First encounter: calculate wakeAt = now + d, save a SLEEP_START event
// containing wakeAt, then panic(YieldError{WakeAt: wakeAt}). The deferred
// recover() in Run() catches YieldError and returns it to the worker, which
// calls db.SleepWorkflow(wakeAt) and exits the tick.
//
// Re-execution after the timer fires: the SLEEP_START event is in the ledger.
// Unmarshal wakeAt from it. If time.Now().Before(wakeAt), the timer hasn't
// elapsed — panic again so the worker re-sleeps without advancing state. If
// it has elapsed, return nil and let the workflow continue.
//
// This design means Sleep() is both durable (survives crashes) and correct
// under replay — the workflow never "forgets" how long it was supposed to sleep.
func (c *engineContext) Sleep(sleepName string, d time.Duration) error {
	c.sequenceNumber++

	event, err := c.db.GetEvent(c.workflowID, c.sequenceNumber)
	if err != nil {
		return fmt.Errorf("failed to check sleep event: %w", err)
	}

	if event != nil {
		// Non-Determinism Detection: verify the history matches the code.
		if event.EventType != "SLEEP_START" || event.EventName != sleepName {
			return fmt.Errorf(
				"non-deterministic execution detected at Seq %d: expected Sleep '%s', found '%s' ('%s')",
				c.sequenceNumber, sleepName, event.EventName, event.EventType,
			)
		}

		// The sleep was already saved — unmarshal the target wake time.
		var wakeAt time.Time
		if err := json.Unmarshal(event.OutputData, &wakeAt); err != nil {
			return fmt.Errorf("failed to unmarshal wakeAt for sleep '%s': %w", sleepName, err)
		}

		if time.Now().Before(wakeAt) {
			// Timer hasn't elapsed. Re-yield so the worker re-sleeps.
			log.Printf("💤 [SLEEP]    '%s' (Seq %d) — not yet elapsed (wakes at %s).", sleepName, c.sequenceNumber, wakeAt.Format(time.RFC3339))
			panic(YieldError{WakeAt: wakeAt})
		}

		// Timer has elapsed — the workflow resumes normally.
		log.Printf("⏩ [RESUME]   Sleep '%s' (Seq %d) — elapsed, continuing.", sleepName, c.sequenceNumber)
		return nil
	}

	// First encounter: calculate and save the absolute wake time.
	wakeAt := time.Now().Add(d)
	wakeBytes, err := json.Marshal(wakeAt)
	if err != nil {
		return fmt.Errorf("failed to marshal wakeAt for sleep '%s': %w", sleepName, err)
	}

	durationBytes, _ := json.Marshal(d.String())
	// inserted == false means another worker already wrote SLEEP_START for this slot.
	// Adopt the winner's wake time so both workers yield to the same absolute timestamp.
	inserted, saveErr := c.db.SaveEvent(c.workflowID, "SLEEP_START", sleepName, durationBytes, wakeBytes, c.sequenceNumber)
	if saveErr != nil {
		return fmt.Errorf("failed to save sleep event: %w", saveErr)
	}
	if !inserted {
		log.Printf("⚠️  [RACE LOST] Sleep '%s' (Seq %d) already recorded by another worker. Adopting winner's wakeAt.", sleepName, c.sequenceNumber)
		winnerEvent, getErr := c.db.GetEvent(c.workflowID, c.sequenceNumber)
		if getErr != nil {
			return fmt.Errorf("failed to fetch winner sleep event for '%s': %w", sleepName, getErr)
		}
		if winnerEvent == nil {
			return fmt.Errorf("winner sleep event not found for '%s' (seq %d)", sleepName, c.sequenceNumber)
		}
		var winnerWakeAt time.Time
		if unmarshalErr := json.Unmarshal(winnerEvent.OutputData, &winnerWakeAt); unmarshalErr != nil {
			return fmt.Errorf("failed to unmarshal winner wakeAt for '%s': %w", sleepName, unmarshalErr)
		}
		log.Printf("💤 [SLEEP]    '%s' (Seq %d) — yielding until winner's time %s.", sleepName, c.sequenceNumber, winnerWakeAt.Format(time.RFC3339))
		panic(YieldError{WakeAt: winnerWakeAt})
	}

	log.Printf("💤 [SLEEP]    '%s' (Seq %d) — yielding until %s.", sleepName, c.sequenceNumber, wakeAt.Format(time.RFC3339))
	panic(YieldError{WakeAt: wakeAt})
}

// Now safely returns the current time in a deterministic way.
// On the first run, it generates time.Now() and saves it to the ledger.
// On replays, it fetches the exact same timestamp from the ledger so
// the workflow logic never changes between crashes.
func (c *engineContext) Now() (time.Time, error) {
	c.sequenceNumber++

	event, err := c.db.GetEvent(c.workflowID, c.sequenceNumber)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to check event history for Now(): %w", err)
	}

	if event != nil {
		// Non-Determinism Detection: verify the history matches the code.
		if event.EventType != "SIDE_EFFECT" || event.EventName != "TimeNow" {
			return time.Time{}, fmt.Errorf(
				"non-deterministic execution detected at Seq %d: expected side effect 'TimeNow', found '%s' ('%s')",
				c.sequenceNumber, event.EventName, event.EventType,
			)
		}

		log.Printf("⏪ [REPLAY]   Now() (Seq %d) — returning saved timestamp.", c.sequenceNumber)
		var savedTime time.Time
		if err := json.Unmarshal(event.OutputData, &savedTime); err != nil {
			return time.Time{}, fmt.Errorf("failed to unmarshal time: %w", err)
		}
		return savedTime, nil
	}

	log.Printf("⏱️  [SIDE EFFECT] Now() (Seq %d) — generating new timestamp.", c.sequenceNumber)
	currentTime := time.Now()

	timeBytes, _ := json.Marshal(currentTime)
	// inserted == false means another worker already wrote the timestamp for this slot.
	// Adopt the winner's value so both workers return the same deterministic time.
	inserted, saveErr := c.db.SaveEvent(c.workflowID, "SIDE_EFFECT", "TimeNow", []byte("{}"), timeBytes, c.sequenceNumber)
	if saveErr != nil {
		return time.Time{}, fmt.Errorf("failed to save time to ledger: %w", saveErr)
	}
	if !inserted {
		log.Printf("⚠️  [RACE LOST] Now() (Seq %d) already recorded by another worker. Adopting winner's timestamp.", c.sequenceNumber)
		winnerEvent, getErr := c.db.GetEvent(c.workflowID, c.sequenceNumber)
		if getErr != nil {
			return time.Time{}, fmt.Errorf("failed to fetch winner Now() event: %w", getErr)
		}
		if winnerEvent == nil {
			return time.Time{}, fmt.Errorf("winner Now() event not found (seq %d)", c.sequenceNumber)
		}
		var savedTime time.Time
		if unmarshalErr := json.Unmarshal(winnerEvent.OutputData, &savedTime); unmarshalErr != nil {
			return time.Time{}, fmt.Errorf("failed to unmarshal winner Now() timestamp: %w", unmarshalErr)
		}
		return savedTime, nil
	}

	return currentTime, nil
}
