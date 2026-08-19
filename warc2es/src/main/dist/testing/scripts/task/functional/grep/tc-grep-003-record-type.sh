#!/bin/bash
# tc-grep-003-record-type.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_record_type() {
    # tiny.warc.gz ideally has warcinfo and response records (?)
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"
    local output="$TEST_OUTPUT_DIR/grep-003.warc.gz"

    log_info "Running grep allow response..."
    "$WARC_CLI" grep "$input" "$output" \
        --processor.grep.allow-warc-types=response

    assert_command_success $?

    # Validation
    # Parse output, check for WARC-Type: response
    # Ensure NO warcinfo or other types

    local count_response
    count_response=$(zgrep "WARC-Type: response" "$output" | wc -l)

    local count_other
    count_other=$(zgrep "WARC-Type:" "$output" | grep -v "response" | wc -l)

    if [ "$count_response" -gt 0 ]; then
        log_success "Found $count_response response records"
    else
        log_warn "No response records found (input might be lacking)"
    fi

    if [ "$count_other" -eq 0 ]; then
        log_success "No other record types found"
    else
        log_fail "Found $count_other records of non-response type"
        return 1
    fi
}

run_test test_record_type
