#!/bin/bash
# tc-e2e-001-full-pipeline.sh
set -euo pipefail
source "$(dirname "$0")/../../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"

assert_warc_output_exists() {
    local path="$1"
    if [[ -f "$path" ]]; then
        return 0
    fi
    if [[ -d "$path" ]] && find "$path" -maxdepth 1 -type f -name '*.gz' | grep -q .; then
        return 0
    fi
    log_fail "WARC output not found: $path"
    return 1
}

json_total_hits() {
    python3 -c "import json,sys; print(int(json.load(sys.stdin).get('hits',{}).get('total',{}).get('value',0)))" 2>/dev/null || echo "0"
}

cleanup_index() {
    local index="$1"
    curl -sS -o /dev/null -X DELETE "$ES_URL/$index" || true
}

test_full_pipeline() {
    if ! "$ES_CLI" check-health &> /dev/null; then
        log_warn "Elasticsearch not available. Skipping E2E test."
        echo "TESTCASE|e2e-es-unavailable|SKIP|reason=no-es"
        return 0
    fi

    ensure_test_data "example.com.warc.gz" || return 1
    local input="$TEST_DATA_DIR/example.com.warc.gz"

    local wet="$TEST_OUTPUT_DIR/e2e-001.wet.gz"
    local doet="$TEST_OUTPUT_DIR/e2e-001.doet.gz"
    local stream="test-stream-e2e-$(date +%s)"

    # 1. Extract
    log_info "Step 1: Extract Text"
    "$WARC_CLI" extract-text "$input" "$wet" || return 1
    assert_warc_output_exists "$wet" || return 1

    # 2. Dedupe
    log_info "Step 2: Dedupe"
    "$WARC_CLI" dedupe "$wet" "$doet" || return 1
    assert_warc_output_exists "$doet" || return 1

    # 3. Load
    log_info "Step 3: Load to ES stream $stream"
    "$ES_CLI" load-stream "$doet" "$stream" || return 1

    "$ES_CLI" refresh "$stream" > /dev/null || return 1

    # 4. Verification
    log_info "Step 4: Verify Search"
    # example.com fixture text contains "documentation examples".
    local output total
    output=$("$ES_CLI" search "documentation" --stream="$stream") || {
        cleanup_index "$stream"
        return 1
    }
    total=$(echo "$output" | json_total_hits)

    if [[ "$total" -gt 0 ]]; then
         log_success "Pipeline E2E successful. Found $total hits."
    else
         log_fail "Search returned 0 hits for 'documentation'"
         cleanup_index "$stream"
         return 1
    fi

    # Cleanup
    cleanup_index "$stream"
}

run_test test_full_pipeline
