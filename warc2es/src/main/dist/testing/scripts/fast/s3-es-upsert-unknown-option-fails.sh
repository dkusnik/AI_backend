#!/bin/bash
# Ensure es-upsert rejects unknown options with a clear error.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_es_upsert_unknown_option_fails() {
    local script="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/es-upsert.sh"
    assert_file_exists "$script" || return 1

    local output rc
    set +e
    output=$(bash "$script" --this-option-does-not-exist 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "es-upsert should fail on unknown option" || return 1
    echo "$output" | grep -q "Error: unknown option: --this-option-does-not-exist" || {
        log_fail "Expected explicit unknown option diagnostics from es-upsert"
        return 1
    }
}

run_test test_es_upsert_unknown_option_fails
