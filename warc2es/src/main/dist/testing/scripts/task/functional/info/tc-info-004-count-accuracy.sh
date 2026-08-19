#!/bin/bash
# Validate info record metric is stable across repeated runs.
# Task-ID: T-197
source "$(dirname "$0")/../../../../lib/test-lib.sh"

extract_records_metric() {
    echo "$1" | sed -nE 's/.*records(In|Out)[=:][[:space:]]*([0-9]+).*/\2/p; s/^[[:space:]]*([0-9]+)[[:space:]]+records(In|Out).*/\1/p' | head -n1
}

test_info_count_accuracy() {
    ensure_test_data "example.com.warc.gz" || return 1

    local input="$TEST_DATA_DIR/example.com.warc.gz"

    local output1 output2
    output1=$("$WARC_CLI" info "$input" 2>&1)
    assert_command_success $? "first info run failed" || return 1

    output2=$("$WARC_CLI" info "$input" 2>&1)
    assert_command_success $? "second info run failed" || return 1

    local run1 run2
    run1=$(extract_records_metric "$output1")
    run2=$(extract_records_metric "$output2")
    if [[ -z "$run1" || -z "$run2" ]]; then
        log_fail "Could not parse records metric from repeated info runs"
        return 1
    fi

    if [[ "$run1" -ne "$run2" ]]; then
        log_fail "Record metric should be stable across runs: run1=$run1 run2=$run2"
        return 1
    fi
}

run_test test_info_count_accuracy
