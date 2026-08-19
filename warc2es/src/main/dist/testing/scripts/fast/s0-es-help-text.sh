#!/bin/bash
# s0-es-help-text.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_es_help_text() {
    log_info "Checking es-cli help output..."
    local output
    output=$("$ES_CLI" --help 2>&1)
    assert_command_success $? "es-cli --help"

    if echo "$output" | grep -qi "Usage:"; then
        return 0
    fi
    log_fail "es-cli help output missing Usage"
    return 1
}

run_test test_es_help_text
