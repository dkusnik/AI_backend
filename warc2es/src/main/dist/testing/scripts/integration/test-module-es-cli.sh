#!/bin/bash
# test-module-es-cli.sh - Test suite for Project ELM es-cli orchestrator
# Category: INTEGRATION (runs via target/dist/bin)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}" .sh)"

# === Paths ===
# Locate project root by walking up to pom.xml (works from src/main/dist and dist trees)
FIND_ROOT="$SCRIPT_DIR"
while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
    FIND_ROOT="$(dirname "$FIND_ROOT")"
done
if [[ ! -f "$FIND_ROOT/pom.xml" ]]; then
    echo "[FAIL] Could not locate project root (pom.xml) from: $SCRIPT_DIR"
    exit 1
fi
PROJECT_ROOT="$FIND_ROOT"
BIN_DIR="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/bin"
TMP_DIR="$PROJECT_ROOT/target/testing/tmp"
REPORTS_DIR="$PROJECT_ROOT/target/testing/tmp/reports"

# === Binary ===
ES_CLI="$BIN_DIR/es-cli"
if [[ ! -x "$ES_CLI" ]]; then
    echo "[FAIL] es-cli not found or not executable: $ES_CLI"
    exit 1
fi

# === Output ===
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
REPORT_FILE="$REPORTS_DIR/${TIMESTAMP}_${SCRIPT_NAME}.report.md"

mkdir -p "$TMP_DIR" "$REPORTS_DIR"

# Configuration
export ES_URL="${ES_URL:-http://localhost:9200}"
TEST_GEN="test-v1"
TEST_STREAM="nac-data-$TEST_GEN"
TEST_DOC_ID="doc-test-999"

if ! curl -s --connect-timeout 2 --max-time 5 "$ES_URL/_cluster/health" > /dev/null; then
    echo "=========================================================="
    echo "Project ELM: es-cli Module Test Suite"
    echo "=========================================================="
    echo "SKIP: Elasticsearch not reachable at $ES_URL"
    exit 0
fi

echo "=========================================================="
echo "Project ELM: es-cli Module Test Suite"
echo "=========================================================="

function assert_contains() {
    local haystack="$1"
    local needle="$2"
    local msg="$3"
    if echo "$haystack" | grep -q "$needle"; then
        echo "  [PASS] $msg"
    else
        echo "  [FAIL] $msg (Expected '$needle')"
        exit 1
    fi
}

function es_raw_call() {
    curl -s -X GET "$ES_URL$1"
}

# --- TEST 1: check-health ---
echo "[1] Testing: check-health"
HEALTH_OUT=$("$ES_CLI" check-health)
assert_contains "$HEALTH_OUT" "status" "Health output contains status"

# --- TEST 2: init ---
echo "[2] Testing: init"
# Note: es-cli receives the exact stream name.
"$ES_CLI" init "$TEST_STREAM"

# Verify ILM Policy
ILM_OUT=$(es_raw_call "/_ilm/policy/nac-1tb-policy")
assert_contains "$ILM_OUT" "nac-1tb-policy" "ILM policy created"

# Verify Template
TEMPLATE_OUT=$(es_raw_call "/_index_template/nac-data-template")
assert_contains "$TEMPLATE_OUT" "nac-data-template" "Index template created"

# Verify Data Stream
STREAM_OUT=$(es_raw_call "/_data_stream/$TEST_STREAM")
assert_contains "$STREAM_OUT" "$TEST_STREAM" "Data stream initialized"

# --- TEST 3: put-doc ---
echo "[3] Testing: put-doc"
PUT_OUT=$("$ES_CLI" put-doc "$TEST_STREAM" "$TEST_DOC_ID" '{"@timestamp":"2026-01-07T13:15:00Z","content":"Project ELM mechanical testing", "warc-date":"2026-01-07T13:15:00Z"}')
assert_contains "$PUT_OUT" '"result":"created"' "Document created"
# Extract the backing index for get-doc
BACKING_INDEX=$(echo "$PUT_OUT" | grep -oP '"_index":"\K[^"]+')
sleep 1

# --- TEST 4: get-doc (using backing index) ---
echo "[4] Testing: get-doc"
GET_OUT=$("$ES_CLI" get-doc "$BACKING_INDEX" "$TEST_DOC_ID")
assert_contains "$GET_OUT" '"found"' "Document found"

# --- TEST 5: search ---
echo "[5] Testing: search"
curl -s -X POST "$ES_URL/$TEST_STREAM/_refresh" > /dev/null
SEARCH_OUT=$("$ES_CLI" search "mechanical")
assert_contains "$SEARCH_OUT" "mechanical" "Search hit contains term"

# --- TEST 6: list-indices / list-shards ---
echo "[6] Testing: Diagnostics (list-indices, list-shards)"
"$ES_CLI" list-indices | grep -q "nac-data" && echo "  [PASS] list-indices works"
"$ES_CLI" list-shards | grep -q "nac-data" && echo "  [PASS] list-shards works"

# --- TEST 7: atomic-swap ---
# We'll create another stream and swap an alias
echo "[7] Testing: atomic-swap"
TEST_ALIAS="production-test-alias"
"$ES_CLI" init "nac-data-test-v2"
"$ES_CLI" atomic-swap "$TEST_STREAM" "nac-data-test-v2" "$TEST_ALIAS"
ALIAS_OUT=$(es_raw_call "/_alias/$TEST_ALIAS")
assert_contains "$ALIAS_OUT" "nac-data-test-v2" "Alias swapped to v2"

# --- TEST 8: stream-rollover ---
echo "[8] Testing: stream-rollover"
ROLL_OUT=$("$ES_CLI" stream-rollover "nac-data-test-v2")
assert_contains "$ROLL_OUT" "rolled_over" "Stream rolled over"

# --- TEST 9: delete-stream ---
echo "[9] Testing: delete-stream"
"$ES_CLI" delete-stream "nac-data-test-v2"
# Verify it's gone
es_raw_call "/_data_stream/nac-data-test-v2" | grep -q "404\|not_found" && echo "  [PASS] Stream deleted"

# --- CLEANUP ---
echo "Cleaning up test environment..."
"$ES_CLI" delete-stream "$TEST_STREAM" > /dev/null 2>&1 || true
curl -s -X DELETE "$ES_URL/.project-elm-audit" > /dev/null 2>&1 || true

echo "=========================================================="
echo "ALL TESTS PASSED SUCCESSFULLY"
echo "=========================================================="
