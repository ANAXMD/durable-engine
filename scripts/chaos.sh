#!/bin/bash
set -e

if [ -z "$DATABASE_URL" ]; then
  DATABASE_URL="postgres://engine_user:engine_password@127.0.0.1:5432/durable_engine?sslmode=disable"
fi
export DATABASE_URL

echo "=== DURABLE ENGINE: MULTI-PROCESS CHAOS TEST ==="
echo "Building worker..."
go build -o bin/worker ./cmd/worker

echo "Wiping database..."
psql "$DATABASE_URL" -c "TRUNCATE workflows, workflow_events, effects;"

echo "Booting 3 independent workers..."
PORT=8081 ./bin/worker &
W1=$!
PORT=8082 ./bin/worker &
W2=$!
PORT=8083 ./bin/worker &
W3=$!

sleep 2

echo "Injecting 20 concurrent Chaos Workflows..."
for i in {1..20}; do
  curl -s -X POST http://localhost:8081/workflows \
    -H "Content-Type: application/json" \
    -d '{"workflow_type": "ChaosWorkflow"}' > /dev/null
done

echo "Beginning SIGKILL Chaos..."
for i in {1..3}; do
  sleep 3
  echo "🔥 CRASH: SIGKILLing Worker 1 (PID $W1)..."
  kill -9 $W1 || true
  echo "♻️  RESTART: Booting replacement worker..."
  PORT=8081 ./bin/worker &
  W1=$!
done

sleep 3
echo "💥 SIMULTANEOUS OUTAGE: SIGKILLing ALL workers (PIDs $W1 $W2 $W3)..."
kill -9 $W1 $W2 $W3 || true
sleep 2
echo "♻️  RESTART: Rebooting all 3 workers after total outage..."
PORT=8081 ./bin/worker &
W1=$!
PORT=8082 ./bin/worker &
W2=$!
PORT=8083 ./bin/worker &
W3=$!

echo "Waiting for surviving workers to process the queue..."
for i in {1..90}; do
  completed=$(psql "$DATABASE_URL" -t -A -c "SELECT COUNT(*) FROM workflows WHERE status = 'COMPLETED';" 2>/dev/null || echo "0")
  if [ "$completed" -ge 20 ]; then
    echo "All 20 workflows completed in ~${i}s."
    break
  fi
  sleep 1
done

echo "Shutting down all workers..."
kill $W1 $W2 $W3 2>/dev/null || true
sleep 1

echo "=== FINAL DATABASE VERIFICATION ==="
psql "$DATABASE_URL" -c "SELECT status, COUNT(*) FROM workflows GROUP BY status;"

completed_count=$(psql "$DATABASE_URL" -t -A -c "SELECT COUNT(*) FROM workflows WHERE status = 'COMPLETED';")
failed_count=$(psql "$DATABASE_URL" -t -A -c "SELECT COUNT(*) FROM workflows WHERE status = 'FAILED';")
completed_events=$(psql "$DATABASE_URL" -t -A -c "SELECT COUNT(*) FROM workflow_events WHERE event_type = 'ACTIVITY_COMPLETED';")
failed_events=$(psql "$DATABASE_URL" -t -A -c "SELECT COUNT(*) FROM workflow_events WHERE event_type = 'ACTIVITY_FAILED';")
total_effects=$(psql "$DATABASE_URL" -t -A -c "SELECT COUNT(*) FROM effects;")
unique_effects=$(psql "$DATABASE_URL" -t -A -c "SELECT COUNT(DISTINCT idempotency_key) FROM effects;")
duplicate_executions=$(psql "$DATABASE_URL" -t -A -c "SELECT COALESCE(SUM(execution_count - 1), 0) FROM effects;")
total_executions=$(psql "$DATABASE_URL" -t -A -c "SELECT COALESCE(SUM(execution_count), 0) FROM effects;")

echo "=== VERIFICATION METRICS ==="
echo "Workflows COMPLETED:       $completed_count (Expected: 20)"
echo "Workflows FAILED:          $failed_count (Expected: 0)"
echo "Ledger ACTIVITY_COMPLETED: $completed_events (Expected: 40)"
echo "Ledger ACTIVITY_FAILED:    $failed_events (Expected: 0)"
echo "Total effects rows:        $total_effects (Expected: 40)"
echo "Unique idempotency keys:   $unique_effects (Expected: 40)"
echo "Total activity executions: $total_executions"
echo "Duplicate executions:      $duplicate_executions (Cap: 50)"

FAILED=0

if [ "$completed_count" -ne 20 ]; then
  echo "❌ ASSERTION FAILED: Expected exactly 20 COMPLETED workflows, got $completed_count"
  FAILED=1
else
  echo "✅ ASSERTION PASSED: Exactly 20 COMPLETED workflows"
fi

if [ "$failed_count" -ne 0 ]; then
  echo "❌ ASSERTION FAILED: Expected 0 FAILED workflows, got $failed_count"
  FAILED=1
else
  echo "✅ ASSERTION PASSED: Exactly 0 FAILED workflows"
fi

if [ "$completed_events" -ne 40 ]; then
  echo "❌ ASSERTION FAILED: Expected exactly 40 ACTIVITY_COMPLETED events in ledger, got $completed_events"
  FAILED=1
else
  echo "✅ ASSERTION PASSED: Exactly 40 ACTIVITY_COMPLETED events in ledger"
fi

if [ "$failed_events" -ne 0 ]; then
  echo "❌ ASSERTION FAILED: Expected 0 ACTIVITY_FAILED events in ledger, got $failed_events"
  FAILED=1
else
  echo "✅ ASSERTION PASSED: Exactly 0 ACTIVITY_FAILED events in ledger"
fi

if [ "$total_effects" -ne "$unique_effects" ] || [ "$total_effects" -ne 40 ]; then
  echo "❌ ASSERTION FAILED: Expected exactly one row per idempotency key (40 rows), got total=$total_effects, unique=$unique_effects"
  FAILED=1
else
  echo "✅ ASSERTION PASSED: Exactly one row per idempotency key in effects table ($total_effects rows)"
fi

if [ "$duplicate_executions" -gt 50 ]; then
  echo "❌ ASSERTION FAILED: Duplicate executions exceeded bound of 50, got $duplicate_executions"
  FAILED=1
else
  echo "✅ ASSERTION PASSED: Duplicate executions within cap ($duplicate_executions <= 50)"
fi

echo "=== DUPLICATE EXECUTION SUMMARY ==="
echo "ℹ️  Duplicate executions detected and safely deduplicated: $duplicate_executions"

if [ "$FAILED" -ne 0 ]; then
  echo "❌ CHAOS TEST FAILED"
  exit 1
fi

echo "🎉 ALL ASSERTIONS PASSED! Chaos test successfully verified."
exit 0
