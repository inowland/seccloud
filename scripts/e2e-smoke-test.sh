#!/bin/bash
set -euo pipefail

# E2E Smoke Test: Rust ingestion → Rust local worker → Projected runtime state
#
# This script:
# 1. Clears the workspace
# 2. Runs Rust collectors (fixture mode) to ingest data as Parquet
# 3. Starts local Postgres
# 4. Runs the Rust local worker once to normalize + build features + detect + project
# 5. Verifies runtime status and projections are materialized

WORKSPACE=".seccloud"
FIXTURE_DIR="examples/poc/vendor-fixtures/fixed-source-pack"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT_DIR"

echo "=== E2E Smoke Test ==="
echo ""

# Step 0: Clean workspace state (keep postgres if it exists)
echo "--- Clearing workspace state ---"
rm -rf "$WORKSPACE/intake" "$WORKSPACE/lake" "$WORKSPACE/raw" "$WORKSPACE/normalized"
rm -rf "$WORKSPACE/derived" "$WORKSPACE/detections" "$WORKSPACE/manifests"
rm -rf "$WORKSPACE/cases" "$WORKSPACE/dead_letters" "$WORKSPACE/worker_state.json"
echo "  done"
echo ""

# Step 1: Build Rust binaries
echo "--- Building Rust binaries ---"
cd rust && cargo build --bin seccloud-collector --bin seccloud-service-once 2>&1 | tail -1
cd "$ROOT_DIR"
echo ""

# Step 2: Start local Postgres
echo "--- Starting local Postgres ---"
uv run seccloud start-postgres --runtime-root . >/dev/null
echo "  started"
echo ""

# Step 3: Run collectors (fixture mode)
echo "--- Running Rust collectors ---"
for source in okta github gworkspace snowflake; do
    echo -n "  $source: "
    RUST_LOG=warn \
        SECCLOUD_WORKSPACE="$WORKSPACE" \
        SECCLOUD_COLLECTOR_SOURCE="$source" \
        SECCLOUD_FIXTURE_DIR="$FIXTURE_DIR" \
        SECCLOUD_COLLECTOR_MODE=once \
        ./rust/target/debug/seccloud-collector 2>/dev/null && echo "ok" || echo "FAILED"
done
echo ""

# Step 4: Verify Rust output
echo "--- Verifying Rust output ---"
PARQUET_COUNT=$(find "$WORKSPACE/lake/raw" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
QUEUE_COUNT=$(find "$WORKSPACE/intake/pending" -name "*.json" 2>/dev/null | wc -l | tr -d ' ')
echo "  Parquet files: $PARQUET_COUNT"
echo "  Queue entries: $QUEUE_COUNT"

if [ "$QUEUE_COUNT" -eq 0 ]; then
    echo "  ERROR: No queue entries found!"
    exit 1
fi
echo ""

# Step 5: Run Rust local worker and capture structured output
echo "--- Running Rust local worker once ---"
RESULT=$(uv run seccloud run-worker-service-once --workspace "$WORKSPACE" --runtime-root . 2>&1)

# Parse and display results
WORKER_SUMMARY=$(echo "$RESULT" | python3 -c "
import sys, json
data = json.load(sys.stdin)
result = data.get('result', {})
norm = result.get('normalization', {})
ingest = norm.get('ingest', {})
features = result.get('features', {})
detect_ctx = result.get('detection_context', {})
projection = result.get('projection', {})
status = data.get('status', '')
added = ingest.get('added_normalized_events', 0)
landed = norm.get('landed_record_count', 0)
dead = ingest.get('dead_letter_count', 0)
batches = norm.get('processed_batch_count', 0)
action_features = features.get('action_feature_row_count', 0)
static_features = features.get('static_feature_row_count', 0)
detect_context_events = detect_ctx.get('event_count', 0)
projected_events = projection.get('event_count', 0)
print(f'ADDED={added}')
print(f'LANDED={landed}')
print(f'DEAD={dead}')
print(f'BATCHES={batches}')
print(f'STATUS={status}')
print(f'ACTION_FEATURES={action_features}')
print(f'STATIC_FEATURES={static_features}')
print(f'DETECTION_CONTEXT_EVENTS={detect_context_events}')
print(f'PROJECTED_EVENTS={projected_events}')
" 2>&1)

eval "$WORKER_SUMMARY"

echo "  Worker status:      $STATUS"
echo "  Batches processed:  $BATCHES"
echo "  Records landed:     $LANDED"
echo "  Events normalized:  $ADDED"
echo "  Dead letters:       $DEAD"
echo "  Action features:    $ACTION_FEATURES"
echo "  Static features:    $STATIC_FEATURES"
echo "  Detection context:  $DETECTION_CONTEXT_EVENTS"
echo "  Projected events:   $PROJECTED_EVENTS"
echo ""

# Step 6: Check sources
echo "--- Source coverage ---"
echo "$RESULT" | python3 -c "
import sys, json
data = json.load(sys.stdin)
ops = data.get('result', data).get('ops_metadata', {})
for src, count in sorted(ops.get('event_counts_by_source', {}).items()):
    print(f'  {src}: {count} events')
" 2>&1
echo ""

# Step 7: Inspect runtime status
echo "--- Runtime status ---"
STATUS_JSON=$(uv run seccloud show-runtime-status --workspace "$WORKSPACE" --runtime-root . 2>&1)
RUNTIME_SUMMARY=$(echo "$STATUS_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
projection = data.get('projection', {})
feature_tables = data.get('feature_tables', {})
event_index = data.get('event_index', {})
detection_context = data.get('detection_context', {})
identity = data.get('identity_profiles', {})
print(f'PROJECTION_AVAILABLE={str(projection.get(\"available\", False)).lower()}')
print(f'INDEXED_EVENTS={event_index.get(\"event_count\", 0)}')
print(f'INDEXED_PRINCIPALS={event_index.get(\"principal_key_count\", 0)}')
print(f'ACTION_TABLE_ROWS={feature_tables.get(\"action_row_count\", 0)}')
print(f'STATIC_TABLE_ROWS={feature_tables.get(\"static_row_count\", 0)}')
print(f'PEER_GROUP_ROWS={feature_tables.get(\"peer_group_row_count\", 0)}')
print(f'DETECTION_CONTEXT_STATUS_EVENTS={detection_context.get(\"event_count\", 0)}')
print(f'IDENTITY_PRINCIPALS={identity.get(\"principal_count\", 0)}')
" 2>&1)

eval "$RUNTIME_SUMMARY"

echo "  Projection available: $PROJECTION_AVAILABLE"
echo "  Indexed events:       $INDEXED_EVENTS"
echo "  Indexed principals:   $INDEXED_PRINCIPALS"
echo "  Action table rows:    $ACTION_TABLE_ROWS"
echo "  Static table rows:    $STATIC_TABLE_ROWS"
echo "  Peer group rows:      $PEER_GROUP_ROWS"
echo "  Detection ctx events: $DETECTION_CONTEXT_STATUS_EVENTS"
echo "  Identity principals:  $IDENTITY_PRINCIPALS"
echo ""

# Step 8: Verdict
if [ "$STATUS" = "processed" ] &&
   [ "$ADDED" -gt 0 ] &&
   [ "$BATCHES" -eq 4 ] &&
   [ "$DEAD" -eq 0 ] &&
   [ "$ACTION_FEATURES" -gt 0 ] &&
   [ "$STATIC_FEATURES" -gt 0 ] &&
   [ "$PROJECTED_EVENTS" -gt 0 ] &&
   [ "$PROJECTION_AVAILABLE" = "true" ] &&
   [ "$INDEXED_EVENTS" -gt 0 ] &&
   [ "$INDEXED_PRINCIPALS" -gt 0 ] &&
   [ "$ACTION_TABLE_ROWS" -gt 0 ] &&
   [ "$STATIC_TABLE_ROWS" -gt 0 ] &&
   [ "$DETECTION_CONTEXT_STATUS_EVENTS" -eq "$INDEXED_EVENTS" ]; then
    echo "=== SMOKE TEST PASSED ==="
    echo "  4 Rust collectors → Rust local worker → projected runtime substrate is healthy"
else
    echo "=== SMOKE TEST FAILED ==="
    echo "  Expected processed Rust worker output plus non-zero runtime substrate coverage"
    echo "  Got: status=$STATUS batches=$BATCHES normalized=$ADDED dead=$DEAD projected=$PROJECTED_EVENTS indexed=$INDEXED_EVENTS"
    exit 1
fi
