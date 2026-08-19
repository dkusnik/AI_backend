#!/bin/bash
# s2-extract-missing-input-fails.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_extract_missing_input_fails() {
    local input="$TEST_OUTPUT_DIR/not-found-input.warc.gz"
    local output="$TEST_OUTPUT_DIR/extract-missing-output.doet.gz"

    local cmd_output
    local rc
    set +e
    cmd_output=$("$WARC_CLI" extract-text "$input" "$output" --silent --progress-none --final-report-summary 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "extract-text with missing input should fail" || return 1

    if echo "$cmd_output" | grep -qiE "No such file|not found|merge failed|Sequential bypass failed"; then
        return 0
    fi
    log_fail "Expected missing input diagnostics from extract-text"
    return 1
}

run_test test_extract_missing_input_fails
