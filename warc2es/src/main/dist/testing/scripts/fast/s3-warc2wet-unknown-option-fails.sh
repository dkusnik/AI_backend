#!/bin/bash
# Ensure warc2wet rejects unknown options with a clear error.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_warc2wet_unknown_option_fails() {
    local script="$PROJECT_ROOT/src/main/dist/warc2wet.sh"
    assert_file_exists "$script" || return 1

    local output rc
    set +e
    output=$("$script" --this-option-does-not-exist 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "warc2wet should fail on unknown option" || return 1
    echo "$output" | grep -q "Error: unknown option: --this-option-does-not-exist" || {
        log_fail "Expected explicit unknown option diagnostics from warc2wet"
        return 1
    }
}

run_test test_warc2wet_unknown_option_fails
