#!/bin/bash
# s0-help-command-order.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_help_command_order_snapshot() {
    local output
    output=$("$WARC_CLI" --help 2>&1)
    assert_command_success $? "warc-cli --help failed" || return 1

    local commands=(
        "extract-text"
        "dedupe"
        "grep"
        "convert"
        "merge"
        "info"
        "validate"
        "regen-cdxj"
        "regen-digests"
        "regen-zip"
    )

    local prev_line=0
    local cmd line
    for cmd in "${commands[@]}"; do
        line=$(echo "$output" | grep -nE "^[[:space:]]+$cmd[[:space:]]+" | head -n1 | cut -d: -f1)
        if [[ -z "$line" ]]; then
            log_fail "Command '$cmd' missing in help output"
            return 1
        fi
        if [[ "$line" -le "$prev_line" ]]; then
            log_fail "Command order mismatch at '$cmd' (line $line <= previous $prev_line)"
            return 1
        fi
        prev_line="$line"
    done
}

run_test test_help_command_order_snapshot
