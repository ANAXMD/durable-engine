# Durable Execution Engine

[![CI](https://github.com/ANAXMD/durable-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/ANAXMD/durable-engine/actions/workflows/ci.yml)

A fault-tolerant, highly concurrent durable execution engine built in Go and PostgreSQL. This engine guarantees that workflows run to completion despite network partitions, database timeouts, or forceful worker crashes.

It handles distributed race conditions, memory divergence, and starvation natively, utilizing an append-only event sourcing ledger.

---

## 🚀 Chaos Testing & Demo

The engine is built to survive process failures. You can verify this locally using the multi-process chaos harness.

Run `./scripts/chaos.sh` to boot 3 independent Go workers, inject 20 concurrent workflows, and forcefully terminate (`kill -9`) the workers mid-execution. The surviving workers will gracefully respect leases, wait for expiration, and adopt the orphaned workflows to ensure at-least-once execution (expecting duplicate executions handled by idempotency keys).

![Chaos Test Demo](docs/chaos-demo.gif)

---

## 🛠️ Quick Start

**Prerequisites:** Go 1.26.8+ and Docker.

```bash
# 1. Start the PostgreSQL database
sudo docker compose up -d

# 2. Run the database migrations
for file in migrations/*.sql; do PGPASSWORD=engine_password psql -h 127.0.0.1 -U engine_user -d durable_engine -f "$file"; done

# 3. Start the worker (Defaults to PORT 8080)
go run ./cmd/worker

# 4. In a new terminal, trigger a workflow
curl -X POST http://localhost:8080/workflows -d '{"workflow_type": "MyOrderWorkflow"}'
```

### In-Process Crash Simulation Harness

You can also run the in-process execution simulation that demonstrates crash survival mid-workflow (during durable sleep) and verifies activity replay vs real executions:

```bash
go run ./cmd/simulation
```

---

## 🧪 Testing

The engine includes a full test suite validating crash recovery, lease loss, and distributed race conditions:

```bash
# Set DATABASE_URL (defaults to localhost:5432 if unset)
export DATABASE_URL="postgres://engine_user:engine_password@127.0.0.1:5432/durable_engine?sslmode=disable"

# Ensure all migrations are applied
for file in migrations/*.sql; do PGPASSWORD=engine_password psql -h 127.0.0.1 -U engine_user -d durable_engine -f "$file"; done

# Run the test suite with race detector enabled
go test -v -race ./...
```

The test suite requires an accessible PostgreSQL database configured via `DATABASE_URL` with all migrations applied. See [TestRecoveryFromStartedStep](file:///home/ana/durable-engine/internal/engine/engine_test.go#L44) for an example of validating crash recovery and race condition handling during event replay.

---

## ⚠️ Known Limitations (v1.0)

To keep the initial architecture lean, several intentional tradeoffs were made:

- **At-least-once execution:** The engine guarantees a step runs _at least_ once. Activities must honor the provided idempotency key (e.g., Stripe API idempotency).
- **Fencing Tokens:** Event writes are not yet fully fenced on lease ownership. The engine currently relies on `COMPLETED` priority logic to ignore stale appends.
- **In-Memory Retries:** Retries are currently in-memory and not durable. Every error (including permanent failures like a declined card) is aggressively retried.
- **Replay Performance:** Replay re-reads the entire workflow history. Extremely long workflows will degrade in replay speed. Future iterations will implement `continue-as-new` or state snapshots.
- **No I/O:** Workflows currently take no input, and there are no signals, queries, or versioning.
- **Security:** The HTTP API lacks authentication.

---

## 🏗️ Architecture Decision Records (ADRs)

### 1. Why Postgres and `SKIP LOCKED`?

Instead of adding a heavy distributed queue (like Kafka or RabbitMQ) and trying to synchronize it with database state, Postgres handles both. By using `FOR UPDATE SKIP LOCKED`, multiple Go workers can poll the `workflows` table concurrently without deadlocking or blocking each other. This turns Postgres into a highly concurrent message broker. This is sufficient for our current scale.

### 2. The Append-Only Ledger (Event Sourcing)

The ledger was always event-sourced, but the initial upsert logic mistakenly overwrote completions. Replaying the ledger ensures the in-memory state always matches reality. We enforce at-least-once execution via database constraints and a programmatic adoption pattern.

### 3. Leases over Boot Sweeps

Traditional background jobs use a "boot sweep" (a cron job that periodically checks for crashed tasks). This scales poorly. Instead, we use time-based leases. When a worker claims a job, it sets a lease expiration in the future. If a worker dies mid-charge, the lease naturally expires, and another worker claims it. Surviving workers extend their leases via background heartbeats. This is sufficient for our current scale.

### 4. The `run_at` Optimization

Polling originally checked three columns: `created_at` (for new jobs), `wake_at` (for sleeping jobs), and `lease_expires_at` (for crashed jobs). With 300,000 sleeping workflows, this forced Postgres to evaluate complex partial indexes, causing claim queries to take ~37ms.
**Fix:** Unified all scheduling into a single `run_at` column. The claim query is now a simple `WHERE status IN (...) AND run_at <= NOW()`. With a `(run_at, id)` index, claim latency dropped to **0.375ms** (benchmarkable via `scripts/benchmark.sql` with 300,000 sleeping workflows).

---

## 🐛 Bug Post-Mortem: The Crash Recovery Regression

**The Problem:**
To prevent duplicate execution events, we updated `SaveEvent` to return `ErrRaceLost` if an insert affected 0 rows (meaning the event already existed). However, this broke crash recovery.

If Worker 1 wrote `ACTIVITY_STARTED` and crashed, Worker 2 would claim the workflow, replay the history, and re-execute. When Worker 2 tried to save `ACTIVITY_STARTED`, Postgres returned 0 rows. Worker 2 returned an error (ErrRaceLost) and permanently failed the workflow.

**The Fix:**
Changed `SaveEvent` to return `(inserted bool, err error)` and shifted the responsibility to the caller:

- For `ACTIVITY_STARTED` and `ACTIVITY_FAILED`, `inserted == false` is treated as a harmless no-op (the step simply picks up where it left off).
- For `ACTIVITY_COMPLETED` and `SLEEP_START`, `inserted == false` indicates a true race condition. The worker catches this, fetches the winner's data from the database, and explicitly adopts the winner's value into its local memory to prevent state divergence.

---

## 🏛️ Architectural Guarantees & Correctness Invariants

### 1. Execution Semantics

- **At-least-once activity execution, exactly-once effect with idempotent activities.**
- **Two-Phase Activity Intent**: Before invoking an external activity function, the engine appends an `ACTIVITY_STARTED` event to PostgreSQL. If the process terminates mid-execution, a resuming worker detects the unfinished step and re-executes it with the exact same idempotency key.
- **Deterministic Idempotency Key**: Generated as `${workflow_id}-${sequence_number}`. Downstream integrations (payment gateways, emails, webhooks) must consume this key to ensure deduplication.
- **Ledger Immutability**: On activity completion, an `ACTIVITY_COMPLETED` record with serialized output payload is appended to the `workflow_events` table. Completed steps are never re-executed; their results are replayed from the database.

### 2. Deterministic Replay & Guardrails

- **Sequence-Based Event Sourcing**: During workflow execution, a sequence counter monotonically increments. For each step, if an event exists for the current sequence number, the stored output is deserialized directly into the target pointer without executing user code.
- **Non-Determinism Detection**: During replay, the engine compares the registered event name and event type in history against the code currently invoked. If workflow definitions are modified out-of-order, the engine halts immediately with a non-determinism violation rather than corrupting workflow state.
- **Activity Timeouts & Heartbeats**: Activities are executed under a deadline-enforced context (`context.WithTimeout`). While an activity is actively running, a background goroutine issues heartbeats every 20 seconds via `ExtendLease`, extending the workflow's lease by 1 minute to prevent lease expiration during long-running tasks.
- **Transient Activity Retries**: Activities automatically retry with exponential backoff on transient errors (waiting 2s, 4s, 8s across up to 3 retries). If all attempts are exhausted, an `ACTIVITY_FAILED` audit record is persisted before the workflow fails.

### 3. Distributed Concurrency & Zombie Recovery

- **Atomic Claiming with Row-Level Locks & Single `run_at` Schedule**: Workers poll for workable jobs using PostgreSQL's pessimistic locking pattern:
  ```sql
  UPDATE workflows
  SET
      status           = 'RUNNING',
      locked_by        = $2,
      run_at           = NOW() + INTERVAL '1 minute',
      lease_expires_at = NOW() + INTERVAL '1 minute'
  WHERE id IN (
      SELECT id FROM workflows
      WHERE status IN ('PENDING', 'RUNNING', 'SLEEPING')
        AND run_at <= NOW()
      ORDER BY run_at ASC, id ASC
      LIMIT $1
      FOR UPDATE SKIP LOCKED
  )
  RETURNING id, workflow_type;
  ```
- **Zombie Recovery without Startup Scans**: Orphaned workflows left behind by crashed workers are naturally reclaimed when their 1-minute `lease_expires_at` window lapses.
- **Stale Worker Guard**: All terminal and state transitions require `WHERE id = $1 AND locked_by = $2`. Stale workers whose leases expired are prevented from overwriting subsequent owners' state (mitigating distributed ABA problems).
- **Bounded Goroutine Pool**: Workers process claimed workflows concurrently using a semaphore-bounded pool (`sync.WaitGroup`), preventing worker starvation where one slow workflow blocks the entire batch until leases expire.

### 4. Durable Timers (Sleep)

- Workflows can yield execution for arbitrary durations via `ctx.Sleep(name, duration)`.
- On invocation, a `SLEEP_START` event is recorded with the absolute `wake_at` timestamp, and execution yields via structured control flow (`YieldError`).
- The worker transitions the workflow row to `status = 'SLEEPING'`, sets `run_at = wake_at`, and releases the thread. No thread, socket, or in-memory timer is held. Polling workers automatically re-claim the workflow once `run_at <= NOW()`.

---

## 🗄️ Database Schema & Migrations

The engine operates on two core tables in PostgreSQL:

1. `workflows`: Tracks workflow instances, aggregate status (`PENDING`, `RUNNING`, `SLEEPING`, `COMPLETED`, `FAILED`), distributed lease locks (`locked_by`, `lease_expires_at`), durable timers (`wake_at`), and unified scheduling (`run_at`).
2. `workflow_events`: Append-only event ledger tracking execution events (`ACTIVITY_STARTED`, `ACTIVITY_COMPLETED`, `SLEEP_START`, `ACTIVITY_FAILED`) with a unique constraint on `(workflow_id, sequence_number, event_type)`.

### Migrations

- `001_init.sql`: Base tables and workflow reference foreign keys.
- `002_leases_and_timers.sql`: Distributed lease columns (`locked_by`, `lease_expires_at`, `wake_at`).
- `003_claim_indexes.sql`: Partial index on claimable states (`PENDING`, `RUNNING`, `SLEEPING`).
- `004_fix_ledger.sql`: Upgrades event ledger constraint to `UNIQUE (workflow_id, sequence_number, event_type)` for immutability.
- `005_fix_claim_index.sql`: Indexes claimable workflows with deterministic `(created_at, id)` ordering.
- `006_run_at_optimization.sql`: Unifies scheduling columns (`created_at`, `wake_at`, `lease_expires_at`) into a single `run_at` column with `(run_at, id)` partial index.
- `007_run_at_constraint.sql`: Enforces `CHECK (status IN ('COMPLETED', 'FAILED') OR run_at IS NOT NULL)` constraint for non-terminal workflows.

---

## ⚙️ Configuration

The engine respects standard environment variables:

| Variable       | Description                     | Default                                                                                |
| :------------- | :------------------------------ | :------------------------------------------------------------------------------------- |
| `DATABASE_URL` | PostgreSQL connection string    | `postgres://engine_user:engine_password@localhost:5432/durable_engine?sslmode=disable` |
| `PORT`         | Worker HTTP trigger server port | `8080`                                                                                 |
