#!/bin/bash
# s1-command-usage-nonempty.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_each_command_usage_text_nonempty() {
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

    local cmd output code
    for cmd in "${commands[@]}"; do
        set +e
        output=$("$WARC_CLI" "$cmd" 2>&1)
        code=$?
        set -e

        if [[ $code -eq 0 ]]; then
            log_fail "Command '$cmd' unexpectedly succeeded without arguments"
            return 1
        fi

        if [[ -z "$output" ]]; then
            log_fail "Command '$cmd' returned empty help/usage output"
            return 1
        fi

        if ! echo "$output" | grep -qiE "usage:|input|output"; then
            log_fail "Command '$cmd' missing usage-like diagnostics"
            return 1
        fi
    done
}

run_test test_each_command_usage_text_nonempty
