#!/bin/bash
# s1-profile-light-info.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_profile_light_with_info() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"

    log_info "Checking --profile=light with info command..."
    "$WARC_CLI" --profile=light info "$input" > /dev/null 2>&1
    assert_command_success $? "warc-cli --profile=light info"
}

run_test test_profile_light_with_info
