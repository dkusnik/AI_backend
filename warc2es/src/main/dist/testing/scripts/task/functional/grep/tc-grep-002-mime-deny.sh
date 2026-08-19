#!/bin/bash
# tc-grep-002-mime-deny.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_mime_deny() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"
    local output="$TEST_OUTPUT_DIR/grep-002.warc.gz"

    # Deny text/html -> Should be empty if input is all html
    log_info "Running grep deny text/html..."
    "$WARC_CLI" grep "$input" "$output" \
        --processor.grep.deny-mime-types=text/html

    assert_command_success $?

    local count
    count=$(zgrep "Content-Type: text/html" "$output" | wc -l)

    if [ "$count" -gt 0 ]; then
        log_fail "Found text/html records despite deny filter"
        return 1
    else
        log_success "Filtered out text/html correctly"
    fi
}

run_test test_mime_deny
