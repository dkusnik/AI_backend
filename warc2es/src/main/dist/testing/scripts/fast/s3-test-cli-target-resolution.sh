#!/bin/bash
# Verify test-cli target resolution uses exact id/scope matching (no loose substring).
# Task-ID: T-190
source "$(dirname "$0")/../../lib/test-lib.sh"

test_cli_target_resolution() {
    local cli="$BIN_DIR/test-cli"

    local out code

    out=$("$cli" --preset=flash s0-version-check 2>&1)
    code=$?
    assert_command_success "$code" "test-cli should run exact id target" || return 1
    echo "$out" | grep -Eq "SCRIPTS TOTAL: 1([[:space:]]|$)" || {
        log_fail "Exact id target should resolve to exactly one script"
        return 1
    }

    set +e
    out=$("$cli" --preset=flash required 2>&1)
    code=$?
    set -e
    assert_command_failure "$code" "substring target 'required' must not resolve" || return 1
    echo "$out" | grep -q "No tests found for targets: required" || {
        log_fail "Expected not-found message for unmatched target"
        return 1
    }

    set +e
    out=$("$cli" --preset=flash red 2>&1)
    code=$?
    set -e
    assert_command_failure "$code" "red suite is expected to fail" || return 1
    local expected_red
    expected_red=$(awk '$1 == "scope:" && $2 == "red" { count++ } END { print count + 0 }' \
        "$PROJECT_ROOT/src/main/dist/testing/registry.yaml")
    echo "$out" | grep -Eq "SCRIPTS TOTAL: ${expected_red}([[:space:]]|$)" || {
        log_fail "red target should resolve exactly $expected_red red-scope scripts"
        return 1
    }
}

run_test test_cli_target_resolution
