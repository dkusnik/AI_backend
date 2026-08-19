#!/bin/bash
# tc-es-009-backpressure-stress.sh
# T-101: Load verify_base.doet.gz with a very small batch size and high concurrency.
# Asserts no document loss (count matches expected) and no deadlock (completes).
# Uses pre-merged verify_base.doet.gz (398 records, TC-01 validated).
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"

EXPECTED_DOCS=398   # Known count from TC-01 verify_base.doet.gz

test_backpressure_stress() {
    if ! "$ES_CLI" check-health &>/dev/null; then
        log_warn "Elasticsearch not available — skipping"
        return 0
    fi

    ensure_test_data "verify_base.doet.gz" || { log_warn "Fixture verify_base.doet.gz missing — skipping"; return 0; }

    local stream="test-backpressure-$$"

    log_info "Loading with batch-size=2, max-concurrent-batches=10 into $stream..."
    "$ES_CLI" load-stream "$TEST_DATA_DIR/verify_base.doet.gz" "$stream" \
        --batch-size=2 --concurrent-batches=10
    local load_exit=$?

    if [[ "$load_exit" -ne 0 ]]; then
        log_fail "Load exited non-zero ($load_exit) — possible deadlock or crash"
        "$ES_CLI" delete-stream "$stream" &>/dev/null || true
        echo "TESTCASE|backpressure-stress|FAIL|exit=$load_exit"
        return 1
    fi

    sleep 2

    local actual
    actual=$(curl -s "$ES_URL/$stream/_count" -H "Content-Type: application/json" \
        -d '{"query":{"match_all":{}}}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
    log_info "Indexed: $actual (expected: $EXPECTED_DOCS)"

    "$ES_CLI" delete-stream "$stream" &>/dev/null || true

    if [[ "$actual" -ne "$EXPECTED_DOCS" ]]; then
        log_fail "Document loss under stress: expected $EXPECTED_DOCS, got $actual"
        echo "TESTCASE|backpressure-stress|FAIL|expected=$EXPECTED_DOCS,actual=$actual"
        return 1
    fi

    log_info "No document loss: $actual = $EXPECTED_DOCS ✓"
    echo "TESTCASE|backpressure-stress|PASS|docs=$actual,batch=2,concurrent=10"
}

run_test test_backpressure_stress
