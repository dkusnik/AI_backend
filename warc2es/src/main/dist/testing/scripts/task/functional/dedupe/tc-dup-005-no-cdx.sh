#!/bin/bash
# tc-dup-005-no-cdx.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_dedupe_no_cdx() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input_warc="$TEST_DATA_DIR/tiny.warc.gz"
    local wet="$TEST_OUTPUT_DIR/dup-005-input.wet.gz"

    "$WARC_CLI" extract-text "$input_warc" "$wet"

    local output="$TEST_OUTPUT_DIR/dup-005-out.wet.gz"
    local cdx="$TEST_OUTPUT_DIR/dup-005-out.cdxj"

    log_info "Running dedupe --no-cdx-sidecar..."
    "$WARC_CLI" dedupe "$wet" "$output" --no-cdx-sidecar

    assert_command_success $?
    assert_file_exists "$output"

    if [ -f "$cdx" ]; then
        log_fail "CDX file present when it should not be"
        return 1
    fi
}

run_test test_dedupe_no_cdx
