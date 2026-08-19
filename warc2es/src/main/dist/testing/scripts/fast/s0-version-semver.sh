#!/bin/bash
# s0-version-semver.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_version_semver_format() {
    log_info "Checking version format..."
    local output
    output=$("$WARC_CLI" --version 2>&1)
    assert_command_success $? "warc-cli --version"

    if echo "$output" | grep -qE '[0-9]+\.[0-9]+\.[0-9]+'; then
        return 0
    fi
    log_fail "Version output does not look like semver: $output"
    return 1
}

run_test test_version_semver_format
