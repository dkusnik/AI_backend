#!/bin/bash
# tc-ext-004-no-cdx.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_no_cdx() {
    ensure_test_data "example.com.warc.gz" || return 1
    local input="$TEST_DATA_DIR/example.com.warc.gz"
    local output="$TEST_OUTPUT_DIR/ext-004-no-cdx.wet.gz"
    local cdx="$TEST_OUTPUT_DIR/ext-004-no-cdx.cdxj"

    log_info "Running extract-text with --no-cdx-sidecar..."
    "$WARC_CLI" extract-text "$input" "$output" --no-cdx-sidecar

    assert_command_success $?

    assert_file_exists "$output"

    if [ -f "$cdx" ]; then
        log_fail "CDX file $cdx should NOT exist"
        return 1
    else
        log_success "CDX file was not created"
    fi
}

run_test test_no_cdx
