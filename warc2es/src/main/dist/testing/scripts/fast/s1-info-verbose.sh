#!/bin/bash
# s1-info-verbose.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_info_verbose_mode() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"

    log_info "Checking info command in verbose mode..."
    local output
    output=$("$WARC_CLI" info "$input" --verbose 2>&1)
    assert_command_success $? "warc-cli info --verbose"

    if echo "$output" | grep -qiE "info|warc|record|pipeline"; then
        return 0
    fi
    log_fail "Verbose info output unexpectedly empty/unknown"
    return 1
}

run_test test_info_verbose_mode
