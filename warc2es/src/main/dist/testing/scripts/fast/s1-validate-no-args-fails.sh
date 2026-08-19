#!/bin/bash
# s1-validate-no-args-fails.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_validate_no_args_fails() {
    local output
    local rc
    set +e
    output=$("$WARC_CLI" validate 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "validate without args should fail" || return 1
    echo "$output" | grep -q "Usage: warc-cli validate" || {
        log_fail "Expected usage text for validate without args"
        return 1
    }
}

run_test test_validate_no_args_fails
