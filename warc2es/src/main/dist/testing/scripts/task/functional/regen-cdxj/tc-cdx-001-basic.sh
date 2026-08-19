#!/bin/bash
# Validate regen-cdxj creates output with SURT keys and is idempotent.
# Task-ID: T-195
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_regen_cdxj_basic() {
    ensure_test_data "example.com.warc.gz" || return 1

    local input="$TEST_OUTPUT_DIR/cdx-input.warc.gz"
    cp "$TEST_DATA_DIR/example.com.warc.gz" "$input"

    local output="${input%.warc.gz}.cdxj"

    "$WARC_CLI" regen-cdxj "$input" >/dev/null 2>&1
    assert_command_success $? "regen-cdxj first run failed" || return 1
    assert_file_exists "$output" || return 1

    if ! grep -q ")" "$output"; then
        log_fail "Expected SURT-like key marker ')' in CDX output"
        return 1
    fi

    local lines1 lines2
    lines1=$(wc -l < "$output")

    "$WARC_CLI" regen-cdxj "$input" >/dev/null 2>&1
    assert_command_success $? "regen-cdxj second run failed" || return 1
    lines2=$(wc -l < "$output")

    if [[ "$lines1" -ne "$lines2" ]]; then
        log_fail "regen-cdxj line count should remain stable across reruns"
        return 1
    fi
}

run_test test_regen_cdxj_basic
