#!/bin/bash
# tc-es-010-no-cdx-sidecar-chain.sh
# T-103: Extract with --no-cdx-sidecar, verify no .cdxj is created, then
# load the resulting DOET to ES and assert stable semantic counts (docs > 0).
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"

test_no_cdx_sidecar_chain() {
    if ! "$ES_CLI" check-health &>/dev/null; then
        log_warn "Elasticsearch not available — skipping"
        return 0
    fi

    ensure_test_data "example.com.warc.gz" || return 1

    local wet="$TEST_OUTPUT_DIR/no-cdx.wet.gz"
    local doet="$TEST_OUTPUT_DIR/no-cdx.doet.gz"
    local wet_base="${wet%.gz}"           # without .gz for cdxj check
    local cdxj_candidate="${wet%.wet.gz}.cdxj"
    local stream="test-no-cdx-sidecar-$$"

    # Step 1: extract with --no-cdx-sidecar
    log_info "Extracting with --no-cdx-sidecar..."
    "$WARC_CLI" extract-text "$TEST_DATA_DIR/example.com.warc.gz" "$wet" --no-cdx-sidecar

    # Step 2: assert no .cdxj sidecar was created alongside wet
    if [[ -f "$cdxj_candidate" ]]; then
        log_fail ".cdxj sidecar was created despite --no-cdx-sidecar: $cdxj_candidate"
        "$ES_CLI" delete-stream "$stream" &>/dev/null || true
        echo "TESTCASE|no-cdx-sidecar-chain|FAIL|unexpected-cdxj=$cdxj_candidate"
        return 1
    fi
    log_info "No .cdxj sidecar created ✓"

    # Step 3: dedupe
    log_info "Deduplicating..."
    "$WARC_CLI" dedupe "$wet" "$doet"

    # Step 4: load to ES and assert docs > 0
    log_info "Loading into stream $stream..."
    "$ES_CLI" load-stream "$doet" "$stream"
    sleep 2

    local count
    count=$(curl -s "$ES_URL/$stream/_count" -H "Content-Type: application/json" \
        -d '{"query":{"match_all":{}}}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
    log_info "Indexed: $count"

    "$ES_CLI" delete-stream "$stream" &>/dev/null || true

    if [[ "$count" -eq 0 ]]; then
        log_fail "No documents indexed after no-cdx-sidecar chain"
        echo "TESTCASE|no-cdx-sidecar-chain|FAIL|count=0"
        return 1
    fi

    log_info "Chain completed: count=$count, no sidecar ✓"
    echo "TESTCASE|no-cdx-sidecar-chain|PASS|count=$count"
}

run_test test_no_cdx_sidecar_chain
