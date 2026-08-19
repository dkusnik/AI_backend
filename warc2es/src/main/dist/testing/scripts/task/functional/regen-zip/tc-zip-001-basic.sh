#!/bin/bash
# Validate regen-zip round-trip and compression-level override behavior.
# Task-ID: T-196
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_regen_zip_basic() {
    ensure_test_data "tiny.warc.gz" || return 1

    local input="$TEST_DATA_DIR/tiny.warc.gz"
    local out_default="$TEST_OUTPUT_DIR/regen-default.warc.gz"
    local out_level1="$TEST_OUTPUT_DIR/regen-level1.warc.gz"

    "$WARC_CLI" regen-zip "$input" "$out_default" >/dev/null 2>&1
    assert_command_success $? "regen-zip basic run failed" || return 1
    assert_file_exists "$out_default" || return 1
    gzip -t "$out_default" || {
        log_fail "regen-zip output is not valid gzip"
        return 1
    }

    "$WARC_CLI" regen-zip "$input" "$out_level1" --output.compression-level=1 >/dev/null 2>&1
    assert_command_success $? "regen-zip compression-level override failed" || return 1
    assert_file_exists "$out_level1" || return 1
    gzip -t "$out_level1" || {
        log_fail "regen-zip level1 output is not valid gzip"
        return 1
    }

}

run_test test_regen_zip_basic
