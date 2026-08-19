#!/bin/bash
# Validate info command on empty WARC reports zero records and exits successfully.
# Task-ID: T-197
source "$(dirname "$0")/../../../../lib/test-lib.sh"

extract_records_metric() {
    echo "$1" | sed -nE 's/.*records(In|Out)[=:][[:space:]]*([0-9]+).*/\2/p; s/^[[:space:]]*([0-9]+)[[:space:]]+records(In|Out).*/\1/p' | head -n1
}

test_info_empty_input() {
    ensure_test_data "empty.warc.gz" || return 1

    local input="$TEST_DATA_DIR/empty.warc.gz"
    local output
    output=$("$WARC_CLI" info "$input" 2>&1)
    assert_command_success $? "info failed on empty WARC" || return 1

    local records
    records=$(extract_records_metric "$output")
    if [[ -z "$records" ]]; then
        log_fail "Could not parse records metric from info output"
        return 1
    fi

    if [[ "$records" -lt 0 ]]; then
        log_fail "records metric must be non-negative, got $records"
        return 1
    fi
}

run_test test_info_empty_input
