#!/bin/bash
# tc-es-008-full-chain-consistency.sh
# T-098: Ingesting the same merged DOET 3 times into 3 independent fresh streams
# must produce identical document counts each time (deterministic ingest).
# Uses pre-merged verify_base.doet.gz (398 records, TC-01 validated).
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"

test_full_chain_consistency() {
    if ! "$ES_CLI" check-health &>/dev/null; then
        log_warn "Elasticsearch not available — skipping"
        return 0
    fi

    ensure_test_data "verify_base.doet.gz" || { log_warn "Fixture verify_base.doet.gz missing — skipping"; return 0; }

    local base_stream="test-chain-consistency-$$"
    local counts=()

    for run in 1 2 3; do
        local stream="${base_stream}-r${run}"
        log_info "Run $run/3: loading verify_base.doet.gz into $stream..."
        "$ES_CLI" load-stream "$TEST_DATA_DIR/verify_base.doet.gz" "$stream"
        sleep 2

        local count
        count=$(curl -s "$ES_URL/$stream/_count" -H "Content-Type: application/json" \
            -d '{"query":{"match_all":{}}}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
        log_info "Run $run count: $count"
        counts+=("$count")
        "$ES_CLI" delete-stream "$stream" &>/dev/null || true
    done

    # All three counts must be non-zero and equal
    local c0="${counts[0]}" c1="${counts[1]}" c2="${counts[2]}"

    if [[ "$c0" -eq 0 ]]; then
        log_fail "Run 1 produced 0 docs"
        echo "TESTCASE|full-chain-consistency|FAIL|counts=$c0,$c1,$c2"
        return 1
    fi

    if [[ "$c0" -ne "$c1" ]] || [[ "$c1" -ne "$c2" ]]; then
        log_fail "Inconsistent counts across 3 runs: $c0, $c1, $c2"
        echo "TESTCASE|full-chain-consistency|FAIL|counts=$c0,$c1,$c2"
        return 1
    fi

    log_info "Consistent across 3 runs: $c0 = $c1 = $c2 ✓"
    echo "TESTCASE|full-chain-consistency|PASS|count=$c0,runs=3"
}

run_test test_full_chain_consistency
