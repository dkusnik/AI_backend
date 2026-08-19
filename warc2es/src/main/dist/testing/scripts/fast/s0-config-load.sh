#!/bin/bash
# s0-config-load.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_config_load() {
    # Use a safe command path and require successful execution plus no fatal text.
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"

    log_info "Verifying config loads correctly (via dummy info call)..."
    local output
    set +e
    output=$("$WARC_CLI" info "$input" 2>&1)
    local code=$?
    set -e

    assert_command_success "$code" "Config load check" || return 1

    # Guard against silent regressions that still emit fatal diagnostics.
    if echo "$output" | grep -qiE "(Exception|Traceback|FATAL|SEVERE)"; then
        log_fail "Config load emitted fatal diagnostics"
        return 1
    fi

    if ! echo "$output" | grep -q "WARC File Analysis"; then
        log_fail "Unexpected info output; header marker missing"
        return 1
    fi
}

run_test test_config_load
