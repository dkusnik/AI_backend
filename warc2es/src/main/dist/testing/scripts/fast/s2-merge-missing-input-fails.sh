#!/bin/bash
# s2-merge-missing-input-fails.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_merge_missing_input_fails() {
    ensure_test_data "example.com.warc.gz" || return 1
    local in1="$TEST_DATA_DIR/example.com.warc.gz"
    local missing="$TEST_OUTPUT_DIR/merge-missing-input.doet.gz"
    local merged="$TEST_OUTPUT_DIR/m4-merged.doet.gz"
    local diff="$TEST_OUTPUT_DIR/m4-diff.doet.gz"

    local cmd_output
    local rc
    set +e
    cmd_output=$("$WARC_CLI" merge --output-base="$merged" --output-diff="$diff" "$in1" "$missing" --silent --progress-none --final-report-summary 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "merge with missing input should fail" || return 1

    if echo "$cmd_output" | grep -qiE "No such file|not found|Merge failed"; then
        return 0
    fi
    log_fail "Expected missing input diagnostics from merge"
    return 1
}

run_test test_merge_missing_input_fails
