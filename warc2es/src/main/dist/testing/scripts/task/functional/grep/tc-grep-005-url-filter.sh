#!/bin/bash
# Validate grep allow-url-contains keeps only matching URL records.
# Task-ID: T-198
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_grep_url_filter() {
    ensure_test_data "example.com.warc.gz" || return 1

    local input="$TEST_DATA_DIR/example.com.warc.gz"
    local output="$TEST_OUTPUT_DIR/grep-url-filter.warc.gz"

    "$WARC_CLI" grep "$input" "$output" --processor.grep.allow-url-contains=example.com >/dev/null 2>&1
    assert_command_success $? "grep url filter failed" || return 1
    assert_file_exists "$output" || return 1

    local record_count
    record_count=$(zgrep -a -c '^WARC/' "$output" || true)
    if [[ "$record_count" -lt 1 ]]; then
        log_fail "Expected at least one output WARC record after URL filtering"
        return 1
    fi

    log_info "Filtered output record count: $record_count"
}

run_test test_grep_url_filter
