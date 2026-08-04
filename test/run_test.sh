#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Configuration
PROJECT_ID="test-project"
TOPIC="fling-test"
SUBSCRIPTION="fling-test-sub"

# Track PIDs for cleanup
EMULATOR_PID=""
FLING_PID=""
TMPDIR_PATH=""

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    if [[ -n "$FLING_PID" ]] && kill -0 "$FLING_PID" 2>/dev/null; then
        echo "Killing fling (PID $FLING_PID)"
        kill "$FLING_PID" 2>/dev/null || true
        wait "$FLING_PID" 2>/dev/null || true
    fi
    if [[ -n "$EMULATOR_PID" ]] && kill -0 "$EMULATOR_PID" 2>/dev/null; then
        echo "Killing emulator (PID $EMULATOR_PID)"
        kill "$EMULATOR_PID" 2>/dev/null || true
        wait "$EMULATOR_PID" 2>/dev/null || true
    fi
    if [[ -n "$TMPDIR_PATH" ]] && [[ -d "$TMPDIR_PATH" ]]; then
        echo "Removing temp dir: $TMPDIR_PATH"
        rm -rf "$TMPDIR_PATH"
    fi
    if [[ -f "${PROJECT_DIR}/fling_test_bin" ]]; then
        rm -f "${PROJECT_DIR}/fling_test_bin"
    fi
}
trap cleanup EXIT

# --- Prerequisites ---
echo "=== Checking prerequisites ==="

if ! command -v gcloud &>/dev/null; then
    echo "FAIL: gcloud CLI not found. Install the Google Cloud SDK."
    exit 1
fi

EMULATOR_STATE="$(gcloud components list --filter="id:pubsub-emulator" --format="value(state.name)" 2>/dev/null)"
if [[ "$EMULATOR_STATE" == "Not Installed" ]] || [[ -z "$EMULATOR_STATE" ]]; then
    echo "FAIL: Pub/Sub emulator not installed. Run: gcloud components install pubsub-emulator"
    exit 1
fi

# Prefer the emulator's own launcher over "gcloud beta emulators pubsub", which
# needs the beta component. Homebrew installs of the SDK often ship the emulator
# without beta, and the launcher takes the same host/port.
EMULATOR_LAUNCHER="$(gcloud info --format='value(installation.sdk_root)' 2>/dev/null)/platform/pubsub-emulator/bin/cloud-pubsub-emulator"
if [[ ! -x "$EMULATOR_LAUNCHER" ]]; then
    EMULATOR_LAUNCHER=""

    BETA_STATE="$(gcloud components list --filter="id:beta" --format="value(state.name)" 2>/dev/null)"
    if [[ "$BETA_STATE" == "Not Installed" ]] || [[ -z "$BETA_STATE" ]]; then
        echo "FAIL: need either the emulator launcher or the gcloud beta component."
        echo "      Run: gcloud components install beta"
        exit 1
    fi
fi

if ! command -v java &>/dev/null; then
    echo "FAIL: java not found. The Pub/Sub emulator requires a JRE."
    exit 1
fi

echo "Prerequisites OK"

# --- Create temp directory and pre-populate files ---
echo "=== Setting up test files ==="

TMPDIR_PATH="$(mktemp -d)"
echo "Temp directory: $TMPDIR_PATH"

# Scenario 1: static.log - pre-populated, read_from_beginning=false
cat > "${TMPDIR_PATH}/static.log" <<'EOF'
static-prepop-1
static-prepop-2
static-prepop-3
EOF

# Scenario 2: static_beginning.log - pre-populated, read_from_beginning=true
cat > "${TMPDIR_PATH}/static_beginning.log" <<'EOF'
beginning-prepop-1
beginning-prepop-2
beginning-prepop-3
EOF

# Scenario 3: glob dir - no pre-populated files (created after fling starts)
mkdir -p "${TMPDIR_PATH}/glob"

# Scenario 4: glob_beginning dir - pre-populated, read_from_beginning=true
mkdir -p "${TMPDIR_PATH}/glob_beginning"
cat > "${TMPDIR_PATH}/glob_beginning/app.log" <<'EOF'
glob-beginning-prepop-1
glob-beginning-prepop-2
EOF

# Scenario 5: json.log - pre-populated JSON, read_from_beginning=true
cat > "${TMPDIR_PATH}/json.log" <<EOF
{"message":"json-prepop-1","level":"info","service":"test"}
{"message":"json-prepop-2","level":"warn","service":"test"}
EOF

# Scenario 6: mysql_error.log - a crash backtrace (one record over 5 lines)
# followed by a separate timestamped line. Expect exactly 2 events.
cat > "${TMPDIR_PATH}/mysql_error.log" <<'EOF'
2026-07-28T10:23:45.123456Z 0 [ERROR] [MY-013183] [InnoDB] Assertion failure: trx0trx.cc:1234
InnoDB: We intentionally generate a memory trap.
Thread pointer: 0x7f8b40000f00
Attempting backtrace. You can use the following information to find out
/usr/sbin/mysqld(my_print_stacktrace+0x41) [0x21f0b41]
2026-07-28T10:23:46.000000Z 0 [System] [MY-010910] [Server] Shutdown complete
EOF

# Scenario 10: mysql_slow.log - three queries. The second omits "# Time:" because
# it landed in the same second as the first, which is why the pattern anchors on
# "# User@Host:". Expect 4 events: the leading "# Time:" alone, then one per query.
cat > "${TMPDIR_PATH}/mysql_slow.log" <<'EOF'
# Time: 2026-07-28T10:23:45.123456Z
# User@Host: app[app] @ 10.0.0.5 [10.0.0.5]  Id: 12345
# Query_time: 2.345678  Lock_time: 0.000123 Rows_sent: 1  Rows_examined: 1000000
SET timestamp=1785316225;
SELECT * FROM orders WHERE customer_id = 42
  AND created_at > '2026-01-01';
# User@Host: app[app] @ 10.0.0.6 [10.0.0.6]  Id: 12346
# Query_time: 1.111  Lock_time: 0.000 Rows_sent: 5  Rows_examined: 900
SELECT COUNT(*) FROM line_items;
# Time: 2026-07-28T10:23:47.000000Z
# User@Host: app[app] @ 10.0.0.7 [10.0.0.7]  Id: 12347
# Query_time: 3.5  Lock_time: 0.0 Rows_sent: 1  Rows_examined: 10
SELECT SLEEP(3);
EOF

# Scenario 7: nginx_error.log - an entry with app stderr passthrough beneath it,
# then a second entry. Expect exactly 2 events.
cat > "${TMPDIR_PATH}/nginx_error.log" <<'EOF'
2026/07/28 10:23:45 [error] 1234#0: *567 Passenger: app error, client: 10.0.0.1
App 4242 stderr: ActiveRecord::StatementInvalid (Mysql2::Error: Lost connection)
App 4242 stderr:   from /webapp/app/models/order.rb:88
2026/07/28 10:23:47 [warn] 1234#0: *568 upstream server temporarily disabled
EOF

# Scenario 8: timeout.log - created empty; a lone record is appended later with
# nothing following it, so only the idle timeout can flush it.
touch "${TMPDIR_PATH}/timeout.log"

# Scenario 9: truncate.log - max_lines is set to 3 in the config below, so the
# record is cut short at 3 lines and the leftover line arrives in a follow-on
# chunk that is flagged too. Expect 2 events, both marked multiline_truncated.
cat > "${TMPDIR_PATH}/truncate.log" <<'EOF'
2026-07-28T10:23:45.000000Z 0 [ERROR] runaway record begins
runaway line 1
runaway line 2
runaway line 3
EOF

# --- Generate config.json ---
echo "=== Generating test config ==="

cat > "${SCRIPT_DIR}/config.json" <<EOF
{
  "input": {
    "files": [
      {
        "path": "${TMPDIR_PATH}/static.log",
        "is_json": false,
        "is_glob": false,
        "read_from_beginning": false,
        "outputs": ["test-output"],
        "injections": [
          {"field": "generator", "value": "scenario-1-static"}
        ]
      },
      {
        "path": "${TMPDIR_PATH}/static_beginning.log",
        "is_json": false,
        "is_glob": false,
        "read_from_beginning": true,
        "outputs": ["test-output"],
        "injections": [
          {"field": "generator", "value": "scenario-2-static-beginning"}
        ]
      },
      {
        "path": "${TMPDIR_PATH}/glob/*.log",
        "is_json": false,
        "is_glob": true,
        "glob_interval": 2,
        "read_from_beginning": false,
        "outputs": ["test-output"],
        "injections": [
          {"field": "generator", "value": "scenario-3-glob"}
        ]
      },
      {
        "path": "${TMPDIR_PATH}/glob_beginning/*.log",
        "is_json": false,
        "is_glob": true,
        "glob_interval": 2,
        "read_from_beginning": true,
        "outputs": ["test-output"],
        "injections": [
          {"field": "generator", "value": "scenario-4-glob-beginning"}
        ]
      },
      {
        "path": "${TMPDIR_PATH}/json.log",
        "is_json": true,
        "is_glob": false,
        "read_from_beginning": true,
        "outputs": ["test-output"],
        "injections": [
          {"field": "generator", "value": "scenario-5-json"}
        ]
      },
      {
        "path": "${TMPDIR_PATH}/mysql_error.log",
        "is_json": false,
        "is_glob": false,
        "read_from_beginning": true,
        "outputs": ["test-output"],
        "multiline": {
          "start_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}",
          "mode": "halt_before",
          "timeout_ms": 500
        },
        "injections": [
          {"field": "generator", "value": "scenario-6-multiline-mysql-error"}
        ]
      },
      {
        "path": "${TMPDIR_PATH}/mysql_slow.log",
        "is_json": false,
        "is_glob": false,
        "read_from_beginning": true,
        "outputs": ["test-output"],
        "multiline": {
          "start_pattern": "^# User@Host:",
          "mode": "halt_before",
          "timeout_ms": 500
        },
        "injections": [
          {"field": "generator", "value": "scenario-10-multiline-mysql-slow"}
        ]
      },
      {
        "path": "${TMPDIR_PATH}/nginx_error.log",
        "is_json": false,
        "is_glob": false,
        "read_from_beginning": true,
        "outputs": ["test-output"],
        "multiline": {
          "start_pattern": "^[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} ",
          "mode": "halt_before",
          "timeout_ms": 500
        },
        "injections": [
          {"field": "generator", "value": "scenario-7-multiline-nginx-error"}
        ]
      },
      {
        "path": "${TMPDIR_PATH}/timeout.log",
        "is_json": false,
        "is_glob": false,
        "read_from_beginning": true,
        "outputs": ["test-output"],
        "multiline": {
          "start_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}",
          "mode": "halt_before",
          "timeout_ms": 500
        },
        "injections": [
          {"field": "generator", "value": "scenario-8-multiline-timeout"}
        ]
      },
      {
        "path": "${TMPDIR_PATH}/truncate.log",
        "is_json": false,
        "is_glob": false,
        "read_from_beginning": true,
        "outputs": ["test-output"],
        "multiline": {
          "start_pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}",
          "mode": "halt_before",
          "timeout_ms": 500,
          "max_lines": 3
        },
        "injections": [
          {"field": "generator", "value": "scenario-9-multiline-truncation"}
        ]
      }
    ]
  },
  "output": {
    "pubsub": [
      {
        "name": "test-output",
        "project": "${PROJECT_ID}",
        "topic": "${TOPIC}",
        "auth_file": ""
      }
    ]
  }
}
EOF

echo "Config written to ${SCRIPT_DIR}/config.json"

# --- Build fling ---
echo "=== Building fling ==="
cd "$PROJECT_DIR"
go build -o fling_test_bin .
echo "Built fling_test_bin"

# --- Start Pub/Sub emulator ---
echo "=== Starting Pub/Sub emulator ==="

# Find a free port
EMULATOR_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')
EMULATOR_HOST="localhost:${EMULATOR_PORT}"
echo "Emulator will listen on ${EMULATOR_HOST}"

if [[ -n "$EMULATOR_LAUNCHER" ]]; then
    "$EMULATOR_LAUNCHER" --host=localhost --port="${EMULATOR_PORT}" &>"${TMPDIR_PATH}/emulator.log" &
else
    gcloud beta emulators pubsub start --project="${PROJECT_ID}" --host-port="${EMULATOR_HOST}" &>"${TMPDIR_PATH}/emulator.log" &
fi
EMULATOR_PID=$!
echo "Emulator PID: $EMULATOR_PID"

# Wait for emulator to be ready
echo "Waiting for emulator to be ready..."
MAX_WAIT=30
for i in $(seq 1 $MAX_WAIT); do
    if curl -s "http://${EMULATOR_HOST}" &>/dev/null; then
        echo "Emulator ready after ${i}s"
        break
    fi
    if ! kill -0 "$EMULATOR_PID" 2>/dev/null; then
        echo "FAIL: Emulator process died"
        cat "${TMPDIR_PATH}/emulator.log"
        exit 1
    fi
    if [[ $i -eq $MAX_WAIT ]]; then
        echo "FAIL: Emulator did not become ready within ${MAX_WAIT}s"
        cat "${TMPDIR_PATH}/emulator.log"
        exit 1
    fi
    sleep 1
done

# --- Create topic and subscription via REST API ---
echo "=== Creating topic and subscription ==="

curl -s -X PUT "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/topics/${TOPIC}" > /dev/null
echo "Created topic: ${TOPIC}"

curl -s -X PUT "http://${EMULATOR_HOST}/v1/projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION}" \
    -H "Content-Type: application/json" \
    -d "{\"topic\": \"projects/${PROJECT_ID}/topics/${TOPIC}\"}" > /dev/null
echo "Created subscription: ${SUBSCRIPTION}"

# --- Start fling ---
echo "=== Starting fling ==="
export PUBSUB_EMULATOR_HOST="${EMULATOR_HOST}"

"${PROJECT_DIR}/fling_test_bin" -c "${SCRIPT_DIR}/config.json" -d &>"${TMPDIR_PATH}/fling.log" &
FLING_PID=$!
echo "Fling PID: $FLING_PID"

# Wait for fling to initialize and process pre-existing files
echo "Waiting for fling to initialize (8s)..."
sleep 8

if ! kill -0 "$FLING_PID" 2>/dev/null; then
    echo "FAIL: Fling process died during initialization"
    cat "${TMPDIR_PATH}/fling.log"
    exit 1
fi

# --- Append lines for each scenario ---
echo "=== Appending test lines ==="

# Scenario 1: append 2 lines to static.log
echo "static-appended-1" >> "${TMPDIR_PATH}/static.log"
echo "static-appended-2" >> "${TMPDIR_PATH}/static.log"
echo "  Appended 2 lines to static.log"

# Scenario 2: append 2 lines to static_beginning.log
echo "beginning-appended-1" >> "${TMPDIR_PATH}/static_beginning.log"
echo "beginning-appended-2" >> "${TMPDIR_PATH}/static_beginning.log"
echo "  Appended 2 lines to static_beginning.log"

# Scenario 3: create empty glob/app.log first so fling discovers and tails it,
# then append lines after a delay so they arrive after the seek-to-end.
touch "${TMPDIR_PATH}/glob/app.log"
echo "  Created empty glob/app.log, waiting for fling to discover it..."
sleep 5
echo "glob-appended-1" >> "${TMPDIR_PATH}/glob/app.log"
echo "glob-appended-2" >> "${TMPDIR_PATH}/glob/app.log"
echo "  Appended 2 lines to glob/app.log"

# Scenario 4: append 1 line to glob_beginning/app.log
echo "glob-beginning-appended-1" >> "${TMPDIR_PATH}/glob_beginning/app.log"
echo "  Appended 1 line to glob_beginning/app.log"

# Scenario 5: append 1 JSON line to json.log
echo '{"message":"json-appended-1","level":"error","service":"test"}' >> "${TMPDIR_PATH}/json.log"
echo "  Appended 1 JSON line to json.log"

# Scenario 8: append a single record start with nothing after it. No later line
# can close the record, so it only ships if the idle timeout flushes it.
echo "2026-07-28T10:23:48.000000Z 0 [Note] lone record awaiting timeout flush" >> "${TMPDIR_PATH}/timeout.log"
echo "  Appended 1 unterminated record to timeout.log"

# Wait for fling to process and publish
echo "Waiting for fling to process messages (10s)..."
sleep 10

# --- Run verifier ---
echo "=== Running verification ==="

cd "${SCRIPT_DIR}/verify"
VERIFY_EXIT=0
go run main.go \
    --project="${PROJECT_ID}" \
    --subscription="${SUBSCRIPTION}" \
    --emulator-host="${EMULATOR_HOST}" || VERIFY_EXIT=$?

echo ""
if [[ $VERIFY_EXIT -eq 0 ]]; then
    echo "=== TEST SUITE PASSED ==="
else
    echo "=== TEST SUITE FAILED ==="
    echo ""
    echo "Fling logs (last 50 lines):"
    tail -50 "${TMPDIR_PATH}/fling.log" 2>/dev/null || true
fi

exit $VERIFY_EXIT
