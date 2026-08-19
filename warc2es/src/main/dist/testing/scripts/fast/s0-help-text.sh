#!/bin/bash
# s0-help-text.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_help_text() {
    log_info "Checking --help output..."

    local output
    output=$("$WARC_CLI" --help)
    assert_command_success $? "warc-cli --help"

    # Check for some expected keywords
    if echo "$output" | grep -qi "usage:"; then
        return 0
    else
        log_fail "Help text does not contain 'Usage:'"
        return 1
    fi
}

run_test test_help_text
