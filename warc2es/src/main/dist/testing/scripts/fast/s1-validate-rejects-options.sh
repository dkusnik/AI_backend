#!/bin/bash
# s1-validate-rejects-options.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_validate_rejects_options() {
    local output
    local rc
    set +e
    output=$("$WARC_CLI" validate --silent 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "validate should reject options" || return 1
    echo "$output" | grep -q "validate does not accept options" || {
        log_fail "Expected explicit option rejection message"
        return 1
    }
}

run_test test_validate_rejects_options
