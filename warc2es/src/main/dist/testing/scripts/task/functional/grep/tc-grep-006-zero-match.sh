#!/bin/bash
# Validate grep with zero URL matches writes valid output with no response records.
# Task-ID: T-198
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_grep_zero_match() {
    ensure_test_data "example.com.warc.gz" || return 1

    local input="$TEST_DATA_DIR/example.com.warc.gz"
    local output="$TEST_OUTPUT_DIR/grep-zero-match.warc.gz"

    "$WARC_CLI" grep "$input" "$output" --processor.grep.allow-url-contains=definitely-no-match-domain.invalid >/dev/null 2>&1
    assert_command_success $? "grep zero-match run failed" || return 1
    assert_file_exists "$output" || return 1

    gzip -t "$output" || {
        log_fail "Zero-match output is not valid gzip"
        return 1
    }

    local record_count
    record_count=$(zgrep -a -c '^WARC/' "$output" || true)
    if [[ "$record_count" -ne 0 ]]; then
        log_fail "Expected zero output records for zero-match filter, got $record_count"
        return 1
    fi

    log_info "Zero-match output is valid gzip with zero records"
}

run_test test_grep_zero_match
