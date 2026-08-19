#!/bin/bash
# s0-unknown-option-fails.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_unknown_option_fails() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"

    log_info "Checking unknown option handling via warc-cli..."
    local output
    local rc
    set +e
    output=$("$WARC_CLI" info "$input" --this-option-does-not-exist 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "unknown option should fail" || return 1

    echo "$output" | grep -q -- "--this-option-does-not-exist" || {
        log_fail "Expected unknown option token in output";
        return 1;
    }
}

run_test test_unknown_option_fails
