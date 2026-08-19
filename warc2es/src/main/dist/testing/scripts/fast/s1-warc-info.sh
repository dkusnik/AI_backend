#!/bin/bash
# s1-warc-info.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_warc_info() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"

    log_info "Running info command on $input..."
    "$WARC_CLI" info "$input"
    assert_command_success $? "warc-cli info"
}

run_test test_warc_info
