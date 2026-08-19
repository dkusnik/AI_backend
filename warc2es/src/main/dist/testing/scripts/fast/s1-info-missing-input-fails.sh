#!/bin/bash
# s1-info-missing-input-fails.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_info_missing_input_fails() {
    local missing="$PROJECT_ROOT/target/testing/tmp/warc-missing-input-$$.warc.gz"
    log_info "Checking info command fails for missing file..."

    local output
    local rc
    set +e
    output=$("$WARC_CLI" info "$missing" 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "info with missing input should fail" || return 1

    if echo "$output" | grep -qiE "not found|missing|no such file"; then
        return 0
    fi
    log_fail "Expected missing file diagnostics, got: $output"
    return 1
}

run_test test_info_missing_input_fails
