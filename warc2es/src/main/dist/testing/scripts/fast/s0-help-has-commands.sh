#!/bin/bash
# s0-help-has-commands.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_help_has_core_commands() {
    log_info "Checking warc-cli help includes core commands..."
    local output
    output=$("$WARC_CLI" --help 2>&1)
    assert_command_success $? "warc-cli --help"

    echo "$output" | grep -q "extract-text" || { log_fail "Missing extract-text in help"; return 1; }
    echo "$output" | grep -q "dedupe" || { log_fail "Missing dedupe in help"; return 1; }
    echo "$output" | grep -q "info" || { log_fail "Missing info in help"; return 1; }
}

run_test test_help_has_core_commands
