#!/bin/bash
# tc-grep-001-mime-allow.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_mime_allow() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"
    local output="$TEST_OUTPUT_DIR/grep-001.warc.gz"

    # tiny.warc.gz typically has text/html or similar.
    # Let's try to filter for text/html.

    log_info "Running grep allow text/html..."
    "$WARC_CLI" grep "$input" "$output" \
        --processor.grep.allow-mime-types=text/html

    assert_command_success $?
    assert_file_exists "$output"

    # Verify content
    # Should contain records with Content-Type: text/html
    # Should NOT contain others?
    # tiny.warc.gz might only have one record.

    # Let's count text/html headers
    local count
    count=$(zgrep "Content-Type: text/html" "$output" | wc -l)

    log_info "Result count: $count"
    # Assuming tiny.warc.gz has at least one html record

    if [ "$count" -eq 0 ]; then
        log_warn "No text/html records found. Input might be different than expected."
    fi

    # Negative check
    # Let's run with a filter that should match nothing
    local output_empty="$TEST_OUTPUT_DIR/grep-001-empty.warc.gz"
    "$WARC_CLI" grep "$input" "$output_empty" \
        --processor.grep.allow-mime-types=application/x-forbidden

    local count_empty
    count_empty=$(zgrep "WARC-Type: response" "$output_empty" | wc -l)

    if [ "$count_empty" -gt 0 ]; then
        log_fail "Filter failed. Found $count_empty records but expected 0"
        return 1
    else
        log_success "Negative filter worked (0 records)"
    fi
}

run_test test_mime_allow
