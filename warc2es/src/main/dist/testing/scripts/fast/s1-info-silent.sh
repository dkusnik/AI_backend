#!/bin/bash
# s1-info-silent.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_info_silent_mode() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"

    log_info "Checking info command in silent mode..."
    "$WARC_CLI" info "$input" --silent > /dev/null 2>&1
    assert_command_success $? "warc-cli info --silent"
}

run_test test_info_silent_mode
