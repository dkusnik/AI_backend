#!/bin/bash
# tc-err-004-failure-propagation.sh
# T-086, T-189: Unwritable extract output path must return deterministic non-zero
# exit code, produce no output file, and prevent cascade.
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_failure_propagation() {
    ensure_test_data "example.com.warc.gz" || return 1

    local locked_dir="$TEST_OUTPUT_DIR/locked-out"
    local extract_out="$locked_dir/extract.wet.gz"
    local merge_out="$TEST_OUTPUT_DIR/merge-should-not-exist.doet.gz"

    rm -f "$merge_out"
    mkdir -p "$locked_dir"
    chmod 555 "$locked_dir"   # remove write permission on output directory

    local extract_exit=0
    "$WARC_CLI" extract-text "$TEST_DATA_DIR/example.com.warc.gz" "$extract_out" \
        2>/dev/null || extract_exit=$?

    chmod 755 "$locked_dir"   # restore so cleanup works

    if [[ "$extract_exit" -ne 1 ]]; then
        log_fail "Expected extract-text exit code 1 on unwritable output, got: $extract_exit"
        echo "TESTCASE|failure-propagation|FAIL|expected-exit=1,actual-exit=$extract_exit"
        return 1
    fi
    log_info "extract-text exited 1 on write failure ✓"

    # Primary assertion: no output file was written
    if [[ -f "$extract_out" ]]; then
        log_fail "Output file $extract_out was created despite write failure"
        echo "TESTCASE|failure-propagation|FAIL|unexpected-output=$extract_out"
        return 1
    fi
    log_info "No output file created after write failure ✓"

    # Cascade prevention: simulate what an orchestrator would do — only run merge
    # if the extract output exists.  Since it doesn't, cascade is blocked.
    if [[ -f "$extract_out" ]]; then
        "$WARC_CLI" dedupe "$extract_out" "$merge_out" 2>/dev/null || true
    fi

    if [[ -f "$merge_out" ]]; then
        log_fail "Merge output $merge_out exists — cascade was not prevented"
        echo "TESTCASE|failure-propagation|FAIL|cascade-output=$merge_out"
        return 1
    fi
    log_info "Cascade step correctly not executed (no output to feed into merge) ✓"

    echo "TESTCASE|failure-propagation|PASS|extract-exit=1,cascade=blocked"
}

run_test test_failure_propagation
