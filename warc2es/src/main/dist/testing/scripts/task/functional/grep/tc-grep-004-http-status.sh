#!/bin/bash
# tc-grep-004-http-status.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_http_status() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"
    local output="$TEST_OUTPUT_DIR/grep-004.warc.gz"

    # Allow 200
    log_info "Running grep allow 200..."
    "$WARC_CLI" grep "$input" "$output" \
        --processor.grep.allow-http-codes=200

    assert_command_success $?

    # Check if we got anything
    local count
    count=$(zgrep "HTTP/1.[01] 200" "$output" | wc -l)

    if [ "$count" -gt 0 ]; then
        log_success "Found 200 OK records ($count)"
    else
        log_warn "No 200 OK records found (maybe input has none?)"
    fi

    # Negative check: Allow 404 (likely 0)
    local output_404="$TEST_OUTPUT_DIR/grep-004-404.warc.gz"
    "$WARC_CLI" grep "$input" "$output_404" \
        --processor.grep.allow-http-codes=404

    local count_404
    count_404=$(zgrep "HTTP/1.[01] 200" "$output_404" | wc -l) # Check for leaked 200s

    if [ "$count_404" -gt 0 ]; then
        log_fail "Found 200 OK records despite allow=404 filter"
        return 1
    else
        log_success "Filtered out non-404 records"
    fi
}

run_test test_http_status
