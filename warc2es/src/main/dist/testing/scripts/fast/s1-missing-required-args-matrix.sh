#!/bin/bash
# s1-missing-required-args-matrix.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_missing_required_positional_args_matrix() {
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
            log_fail "Command '$cmd' should fail when required positional args are missing"
            return 1
        fi

        if ! echo "$output" | grep -qiE "usage:|requires|missing|input"; then
            log_fail "Command '$cmd' did not report missing args clearly"
            return 1
        fi
    done
}

run_test test_missing_required_positional_args_matrix
