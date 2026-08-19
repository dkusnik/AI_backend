#!/bin/bash
# Ensure warc2wet reports missing input directory without raw shell cd noise.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_warc2wet_missing_input_clean_error() {
    local script="$PROJECT_ROOT/src/main/dist/warc2wet.sh"
    assert_file_exists "$script" || return 1

    local missing="$TEST_OUTPUT_DIR/missing-input-dir"
    local output rc
    set +e
    output=$("$script" --url-id=test --crawl-id=test "$missing" 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "warc2wet should fail for missing input directory" || return 1
    echo "$output" | grep -q "Error: input not found: $missing" || {
        log_fail "Missing input error message is not explicit"
        return 1
    }
    if echo "$output" | grep -q "cd: "; then
        log_fail "Unexpected raw shell cd error leaked to user output"
        return 1
    fi
}

run_test test_warc2wet_missing_input_clean_error
