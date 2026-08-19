#!/bin/bash
# tc-merge-007-header-completeness.sh
# T-094: Every record in a merged DOET must carry nac-merge-result and
# nac-deduplicated headers. Uses verify_base.doet.gz (TC-01 validated).
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_merge_header_completeness() {
    ensure_test_data "verify_base.doet.gz" || { log_warn "Fixture missing — skipping"; return 0; }

    log_info "Scanning verify_base.doet.gz for header completeness..."

    local total merge_result_count dedup_count
    total=$(zcat "$TEST_DATA_DIR/verify_base.doet.gz" | grep -c "^WARC/1.0" || echo 0)

    merge_result_count=$(zcat "$TEST_DATA_DIR/verify_base.doet.gz" | \
        grep -c "^nac-merge-result:" || echo 0)
    dedup_count=$(zcat "$TEST_DATA_DIR/verify_base.doet.gz" | \
        grep -c "^nac-deduplicated:" || echo 0)

    log_info "Total records: $total"
    log_info "nac-merge-result headers: $merge_result_count"
    log_info "nac-deduplicated headers: $dedup_count"

    local failed=0

    # Warcinfo header (record 1) doesn't carry these; subtract 1 for warcinfo
    local expected=$((total - 1))

    if [[ "$merge_result_count" -lt "$expected" ]]; then
        log_fail "nac-merge-result: $merge_result_count records have it, expected at least $expected"
        failed=$((failed+1))
    else
        log_info "nac-merge-result present in $merge_result_count/$expected eligible records ✓"
    fi

    if [[ "$dedup_count" -lt "$expected" ]]; then
        log_fail "nac-deduplicated: $dedup_count records have it, expected at least $expected"
        failed=$((failed+1))
    else
        log_info "nac-deduplicated present in $dedup_count/$expected eligible records ✓"
    fi

    if [[ $failed -gt 0 ]]; then
        echo "TESTCASE|merge-header-completeness|FAIL|total=$total,merge-result=$merge_result_count,dedup=$dedup_count"
        return 1
    fi

    echo "TESTCASE|merge-header-completeness|PASS|total=$total,merge-result=$merge_result_count,dedup=$dedup_count"
}

run_test test_merge_header_completeness
