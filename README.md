# Durable Execution Engine

A lightweight, event-sourced durable workflow orchestrator written in Go and backed by PostgreSQL. The engine guarantees reliable, resumable workflow execution across worker crashes, network partitions, and process restarts.

---

## Architectural Guarantees & Correctness Invariants

### 1. Execution Semantics
* **At-least-once activity execution, exactly-once effect with idempotent activities.**
* **Two-Phase Activity Intent**: Before invoking an external activity function, the engine appends an `ACTIVITY_STARTED` event to PostgreSQL. If the process terminates mid-execution, a resuming worker detects the unfinished step and re-executes it with the exact same idempotency key.
* **Deterministic Idempotency Key**: Generated as `${workflow_id}-${sequence_number}`. Downstream integrations (payment gateways, emails, webhooks) must consume this key to ensure deduplication.
* **Ledger Immutability**: On activity completion, an `ACTIVITY_COMPLETED` record with serialized output payload is appended to the `workflow_events` table. Completed steps are never re-executed; their results are replayed from the database.

### 2. Deterministic Replay & Guardrails
* **Sequence-Based Event Sourcing**: During workflow execution, a sequence counter monotonically increments. For each step, if an event exists for the current sequence number, the stored output is deserialized directly into the target pointer without executing user code.
* **Non-Determinism Detection**: During replay, the engine compares the registered event name in history against the activity currently invoked by code. If workflow definitions are modified out-of-order, the engine halts immediately with a non-determinism violation rather than corrupting workflow state.
* **Activity Timeouts**: Activities are executed under a deadline-enforced context (`context.WithTimeout`) rather than unbounded background contexts, preventing downstream service hangs from starving worker threads.

### 3. Distributed Concurrency & Zombie Recovery
* **Atomic Claiming with Row-Level Locks**: Workers poll for workable jobs using PostgreSQL's pessimistic locking pattern:
  ```sql
  UPDATE workflows
  SET status = 'RUNNING', locked_by = $2, lease_expires_at = NOW() + INTERVAL '1 minute'
  WHERE id IN (
      SELECT id FROM workflows
      WHERE status = 'PENDING'
         OR (status = 'RUNNING'  AND lease_expires_at < NOW())
         OR (status = 'SLEEPING' AND wake_at <= NOW())
      ORDER BY id
      LIMIT $1
      FOR UPDATE SKIP LOCKED
  )
  RETURNING id, workflow_type;
  ```
* **Zombie Recovery without Startup Scans**: Orphaned workflows left behind by crashed workers are naturally reclaimed when their 1-minute `lease_expires_at` window lapses.
* **Stale Worker Guard**: All terminal and state transitions require `WHERE id = $1 AND locked_by = $2`. Stale workers whose leases expired are prevented from overwriting subsequent owners' state (mitigating distributed ABA problems).
* **Bounded Goroutine Pool**: Workers process claimed workflows concurrently using a semaphore-bounded pool (`sync.WaitGroup`), preventing worker starvation where one slow workflow blocks the entire batch until leases expire.

### 4. Durable Timers (Sleep)
* Workflows can yield execution for arbitrary durations via `ctx.Sleep(name, duration)`.
* On invocation, a `SLEEP_START` event is recorded with the absolute `wake_at` timestamp, and execution yields via structured control flow (`YieldError`).
* The worker transitions the workflow row to `status = 'SLEEPING'` and releases the thread. No thread, socket, or in-memory timer is held. Polling workers automatically re-claim the workflow once `wake_at <= NOW()`.

---

## Database Schema & Migrations

The engine operates on two core tables in PostgreSQL:

1. `workflows`: Tracks workflow instances, aggregate status (`PENDING`, `RUNNING`, `SLEEPING`, `COMPLETED`, `FAILED`), distributed lease locks (`locked_by`, `lease_expires_at`), and durable timers (`wake_at`).
2. `workflow_events`: Append-only event ledger tracking execution events (`ACTIVITY_STARTED`, `ACTIVITY_COMPLETED`, `SLEEP_START`) with a unique constraint on `(workflow_id, sequence_number)`.

### Migrations
* `001_init.sql`: Base tables and workflow reference foreign keys.
* `002_leases_and_timers.sql`: Distributed lease columns (`locked_by`, `lease_expires_at`, `wake_at`) and sequence uniqueness constraint.
* `003_claim_indexes.sql`: Partial index on claimable states (`PENDING`, `RUNNING`, `SLEEPING`) to prevent full table scans as historical rows grow:
  ```sql
  CREATE INDEX idx_workflows_claimable 
  ON workflows (status, lease_expires_at, wake_at, id)
  WHERE status IN ('PENDING', 'RUNNING', 'SLEEPING');
  ```

---

## Configuration

The engine respects standard environment variables:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `DATABASE_URL` | PostgreSQL connection string | `postgres://engine_user:engine_password@localhost:5432/durable_engine?sslmode=disable` |
| `PORT` | Worker HTTP trigger server port | `8080` |

---

## Quickstart

### 1. Start PostgreSQL
```bash
docker compose up -d
```

### 2. Apply Migrations
Apply migrations in sequence using `psql` or your preferred migration tool:
```bash
psql "$DATABASE_URL" -f migrations/001_init.sql
psql "$DATABASE_URL" -f migrations/002_leases_and_timers.sql
psql "$DATABASE_URL" -f migrations/003_claim_indexes.sql
```

### 3. Run the Worker
```bash
go run ./cmd/worker
```

Trigger a new workflow via the HTTP API:
```bash
curl -X POST http://localhost:8080/workflows \
  -H "Content-Type: application/json" \
  -d '{"workflow_type": "MyOrderWorkflow"}'
```

### 4. Verify Resilience via Chaos Harness
The project includes a chaos test harness that demonstrates crash survival mid-workflow (during durable sleep) and verifies that completed activities are replayed rather than re-executed:
```bash
go run ./cmd/chaos
```
