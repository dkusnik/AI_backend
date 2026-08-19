#!/bin/bash
# tc-es-007-mapping-compat.sh
# T-096: Ingesting a merged DOET must not create dynamic mapping conflicts —
# all 6 required NAC fields must appear in the index mapping with non-null types.
# Uses pre-merged verify_base.doet.gz (398 records, TC-01 validated).
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

ES_URL="${ES_URL:-http://localhost:9200}"

test_mapping_compat() {
    if ! "$ES_CLI" check-health &>/dev/null; then
        log_warn "Elasticsearch not available — skipping"
        return 0
    fi

    ensure_test_data "verify_base.doet.gz" || { log_warn "Fixture verify_base.doet.gz missing in $TEST_DATA_DIR — skipping"; return 0; }

    local stream="test-mapping-compat-$$"

    log_info "Loading verify_base.doet.gz into fresh stream $stream..."
    "$ES_CLI" load-stream "$TEST_DATA_DIR/verify_base.doet.gz" "$stream"
    sleep 2

    # Check that required fields appear in the mapping (not absent = properly typed, not dynamic gaps)
    local mapping
    mapping=$(curl -s "$ES_URL/$stream/_mapping")

    local REQUIRED_FIELDS=("warc-uri" "warc-digest" "nac-merge-result" "nac-deduplicated" "nac-first-seen" "nac-last-seen")
    local missing_in_mapping=0
    for field in "${REQUIRED_FIELDS[@]}"; do
        if echo "$mapping" | python3 -c "
import sys, json
m = json.load(sys.stdin)
for idx_data in m.values():
    if '$field' in idx_data.get('mappings',{}).get('properties',{}):
        exit(0)
exit(1)
" 2>/dev/null; then
            log_info "Field '$field': mapped ✓"
        else
            log_warn "Field '$field': not found in mapping (dynamic or not indexed)"
            missing_in_mapping=$((missing_in_mapping + 1))
        fi
    done

    # Check indexing failures (mapping conflict indicator)
    local rejected
    rejected=$(curl -s "$ES_URL/$stream/_stats/indexing" | \
        python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('_all',{}).get('total',{}).get('indexing',{}).get('index_failed',0))
" 2>/dev/null || echo "0")

    "$ES_CLI" delete-stream "$stream" &>/dev/null || true

    if [[ "$rejected" -gt 0 ]]; then
        log_fail "Mapping conflict detected: $rejected indexing failures"
        echo "TESTCASE|mapping-compat|FAIL|rejected=$rejected"
        return 1
    fi

    if [[ "$missing_in_mapping" -gt 0 ]]; then
        log_fail "$missing_in_mapping required fields absent from mapping"
        echo "TESTCASE|mapping-compat|FAIL|missing-in-mapping=$missing_in_mapping"
        return 1
    fi

    log_info "No mapping conflicts: rejected=$rejected ✓"
    echo "TESTCASE|mapping-compat|PASS|rejected=0,mapped=${#REQUIRED_FIELDS[@]}"
}

run_test test_mapping_compat
