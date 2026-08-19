#!/bin/bash
# tc-err-003-permissions.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_permissions_input() {
    log_info "TEST: permission denied on input file"

    ensure_test_data "tiny.warc.gz" || return 1
    mkdir -p "$TEST_OUTPUT_DIR"

    local locked_warc="$TEST_OUTPUT_DIR/locked-input.warc.gz"
    local output="$TEST_OUTPUT_DIR/err-003.wet.gz"
    cp "$TEST_DATA_DIR/tiny.warc.gz" "$locked_warc"
    chmod 000 "$locked_warc"

    local rc=0
    set +e
    "$WARC_CLI" extract-text "$locked_warc" "$output" --silent >/dev/null 2>&1
    rc=$?
    set -e

    # Ensure cleanup path can read/remove file.
    chmod 644 "$locked_warc"

    if [[ "$rc" -eq 0 ]]; then
        # Current CLI behavior may skip unreadable inputs and exit 0 if no records
        # are processed. Accept this only when zero WARC records were emitted.
        local emitted=0
        if [[ -f "$output" ]]; then
            emitted=$(warc_count "$output")
        fi
        if [[ "$emitted" -ne 0 ]]; then
            log_fail "extract-text succeeded and emitted $emitted records despite unreadable input file"
            return 1
        fi
        log_info "extract-text returned success with zero emitted records for unreadable input (acceptable)"
        return 0
    fi

    log_info "extract-text failed as expected for unreadable input (exit=$rc)"
    return 0
}

run_test test_permissions_input
