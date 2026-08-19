#!/bin/bash
# Verify test-cli --print-timeout flag works and respects presets/caps.
# Task-ID: T-print-timeout
# @timeout: 10s
source "$(dirname "$0")/../../lib/test-lib.sh"

test_cli_print_timeout() {
    local cli="$BIN_DIR/test-cli"
    local out code

    log_info "Testing test-cli --print-timeout with s0-version-check..."
    out=$("$cli" --print-timeout s0-version-check 2>&1)
    code=$?
    assert_command_success "$code" "test-cli --print-timeout should execute successfully" || return 1
    echo "$out" | grep -q "fast__s0-version-check: 60s" || {
        log_fail "Expected default timeout of 60s, got: $out"
        return 1
    }

    log_info "Testing --preset=flash --print-timeout with s0-version-check..."
    out=$("$cli" --preset=flash --print-timeout s0-version-check 2>&1)
    code=$?
    assert_command_success "$code" "test-cli --preset=flash --print-timeout should execute successfully" || return 1
    echo "$out" | grep -q "fast__s0-version-check: 20s" || {
        log_fail "Expected capped timeout of 20s under flash preset, got: $out"
        return 1
    }

    log_info "Print-timeout checks completed successfully"
}

run_test test_cli_print_timeout
