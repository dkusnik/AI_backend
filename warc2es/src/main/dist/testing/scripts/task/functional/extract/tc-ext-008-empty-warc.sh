#!/bin/bash
# tc-ext-008-empty-warc.sh
# T-072 (partial): Empty WARC input — each core command must exit 0 and
# produce valid (possibly empty) output without crashing.
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_empty_warc_input() {
    ensure_test_data "empty.warc.gz" || { log_warn "empty.warc.gz missing — skipping"; return 0; }

    local input="$TEST_DATA_DIR/empty.warc.gz"
    local failed=0

    # 1. extract-text
    local wet_out="$TEST_OUTPUT_DIR/empty-extract.wet.gz"
    local extract_exit=0
    "$WARC_CLI" extract-text "$input" "$wet_out" --no-cdx-sidecar 2>/dev/null || extract_exit=$?
    if [[ "$extract_exit" -ne 0 ]]; then
        log_fail "extract-text on empty WARC exited $extract_exit (expected 0)"
        failed=$((failed+1))
    else
        log_info "extract-text empty input: exit=0 ✓"
    fi

    # 2. grep — filter on empty WARC
    local grep_out="$TEST_OUTPUT_DIR/empty-grep.warc.gz"
    local grep_exit=0
    "$WARC_CLI" grep "$input" "$grep_out" \
        --processor.grep.allow-warc-types=response 2>/dev/null || grep_exit=$?
    if [[ "$grep_exit" -ne 0 ]]; then
        log_fail "grep on empty WARC exited $grep_exit (expected 0)"
        failed=$((failed+1))
    else
        log_info "grep empty input: exit=0 ✓"
    fi

    # 3. info — should succeed even on empty WARC
    local info_exit=0
    "$WARC_CLI" info "$input" 2>/dev/null || info_exit=$?
    if [[ "$info_exit" -ne 0 ]]; then
        log_fail "info on empty WARC exited $info_exit (expected 0)"
        failed=$((failed+1))
    else
        log_info "info empty input: exit=0 ✓"
    fi

    if [[ $failed -gt 0 ]]; then
        echo "TESTCASE|empty-warc-input|FAIL|failures=$failed"
        return 1
    fi

    echo "TESTCASE|empty-warc-input|PASS|commands=extract-text,grep,info"
}

run_test test_empty_warc_input
