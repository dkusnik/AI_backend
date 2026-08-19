#!/bin/bash
# tc-info-001-basic.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_info() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"

    log_info "Running info on $input..."
    local output
    output=$("$WARC_CLI" info "$input")

    assert_command_success $?

    log_info "Output: $output"

    # Expected output contains record count info (metrics format: recordsOut or recordsIn)
    if echo "$output" | grep -qE "records(Out|In)"; then
        log_success "Found record count info"
    else
        log_fail "Info output missing record count metrics"
        return 1
    fi
}

run_test test_info
