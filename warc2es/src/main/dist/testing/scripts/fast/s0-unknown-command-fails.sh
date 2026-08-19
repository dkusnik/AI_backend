#!/bin/bash
# s0-unknown-command-fails.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_unknown_command_fails() {
    log_info "Checking unknown command fails..."
    local output
    set +e
    output=$("$WARC_CLI" not-a-real-command 2>&1)
    local code=$?
    set -e

    assert_command_failure "$code" "unknown command should fail" || return 1
    if echo "$output" | grep -qiE "unknown|usage|pipeline"; then
        return 0
    fi
    log_fail "Unexpected error output for unknown command: $output"
    return 1
}

run_test test_unknown_command_fails
