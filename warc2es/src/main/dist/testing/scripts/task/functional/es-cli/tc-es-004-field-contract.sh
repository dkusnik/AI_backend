#!/bin/bash
# tc-es-004-field-contract.sh
# T-087: Every document indexed after merge must have all 6 required NAC fields.
# Required fields: warc-uri, warc-digest, nac-merge-result, nac-deduplicated,
#                  nac-first-seen, nac-last-seen
# Uses pre-merged verify_base.doet.gz (398 records, TC-01 validated).
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"

test_field_contract() {
    if ! "$ES_CLI" check-health &>/dev/null; then
        log_warn "Elasticsearch not available — skipping"
        return 0
    fi

    ensure_test_data "verify_base.doet.gz" || { log_warn "Fixture verify_base.doet.gz missing in $TEST_DATA_DIR — skipping"; return 0; }

    local stream="test-field-contract-$$"
    log_info "Loading verify_base.doet.gz into stream $stream..."
    "$ES_CLI" load-stream "$TEST_DATA_DIR/verify_base.doet.gz" "$stream"
    sleep 2

    local total
    total=$(curl -s "$ES_URL/$stream/_count" -H "Content-Type: application/json" \
        -d '{"query":{"match_all":{}}}' | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
    log_info "Total docs indexed: $total"

    if [[ "$total" -eq 0 ]]; then
        log_fail "No documents indexed — cannot verify field contract"
        "$ES_CLI" delete-stream "$stream" &>/dev/null || true
        echo "TESTCASE|field-contract|FAIL|no-docs"
        return 1
    fi

    # For each required field: count docs where the field is MISSING → must be 0
    local REQUIRED_FIELDS=("warc-uri" "warc-digest" "nac-merge-result" "nac-deduplicated" "nac-first-seen" "nac-last-seen")
    local failed=0
    for field in "${REQUIRED_FIELDS[@]}"; do
        local missing
        missing=$(curl -s "$ES_URL/$stream/_count" -H "Content-Type: application/json" -d \
            "{\"query\":{\"bool\":{\"must_not\":{\"exists\":{\"field\":\"$field\"}}}}}" | \
            python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
        if [[ "$missing" -gt 0 ]]; then
            log_fail "Field '$field' missing in $missing / $total documents"
            failed=$((failed + 1))
        else
            log_info "Field '$field': present in all $total docs ✓"
        fi
    done

    "$ES_CLI" delete-stream "$stream" &>/dev/null || true

    if [[ $failed -gt 0 ]]; then
        echo "TESTCASE|field-contract|FAIL|missing-fields=$failed"
        return 1
    fi
    echo "TESTCASE|field-contract|PASS|docs=$total"
}

run_test test_field_contract
