#!/bin/bash
set -e

echo "=== DURABLE ENGINE: MULTI-PROCESS CHAOS TEST ==="
echo "Building worker..."
go build -o bin/worker ./cmd/worker

echo "Wiping database..."
sudo docker exec -i durable-engine-postgres-db-1 psql -U engine_user -d durable_engine -c "TRUNCATE workflows, workflow_events;"

echo "Booting 3 independent workers..."
PORT=8081 ./bin/worker > /dev/null 2>&1 &
W1=$!
PORT=8082 ./bin/worker > /dev/null 2>&1 &
W2=$!
PORT=8083 ./bin/worker > /dev/null 2>&1 &
W3=$!

sleep 2

echo "Injecting 20 concurrent Chaos Workflows..."
for i in {1..20}; do
  curl -s -X POST http://localhost:8081/workflows -d '{"type": "ChaosWorkflow"}' > /dev/null
done

echo "Beginning SIGKILL Chaos for 15 seconds..."
for i in {1..4}; do
  sleep 3
  echo "🔥 CRASH: SIGKILLing Worker 1 (PID $W1)..."
  kill -9 $W1
  echo "♻️  RESTART: Booting replacement worker..."
  PORT=8081 ./bin/worker > /dev/null 2>&1 &
  W1=$!
done

echo "Waiting 70 seconds for surviving workers to process the queue..."
sleep 70

echo "Shutting down all workers..."
kill $W1 $W2 $W3 || true

echo "=== FINAL DATABASE VERIFICATION ==="
echo "Expected: 20 rows with status 'COMPLETED'"
sudo docker exec -i durable-engine-postgres-db-1 psql -U engine_user -d durable_engine -c "SELECT status, COUNT(*) FROM workflows GROUP BY status;"
