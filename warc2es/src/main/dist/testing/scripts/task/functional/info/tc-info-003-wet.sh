#!/bin/bash
# Validate info command processes WET input and emits record metrics.
# Task-ID: T-197
source "$(dirname "$0")/../../../../lib/test-lib.sh"

extract_records_metric() {
    echo "$1" | sed -nE 's/.*records(In|Out)[=:][[:space:]]*([0-9]+).*/\2/p; s/^[[:space:]]*([0-9]+)[[:space:]]+records(In|Out).*/\1/p' | head -n1
}

test_info_wet_input() {
    ensure_test_data "tiny.wet.gz" || return 1

    local input="$TEST_DATA_DIR/tiny.wet.gz"
    local output
    output=$("$WARC_CLI" info "$input" 2>&1)
    assert_command_success $? "info failed on WET input" || return 1

    local records
    records=$(extract_records_metric "$output")
    if [[ -z "$records" || "$records" -lt 1 ]]; then
        log_fail "Expected at least one record metric for WET input"
        return 1
    fi
}

run_test test_info_wet_input
