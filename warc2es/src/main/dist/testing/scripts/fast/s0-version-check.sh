#!/bin/bash
# s0-version-check.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_version_check() {
    log_info "Checking version output..."

    local output_warc output_es

    output_warc=$("$WARC_CLI" --version 2>&1)
    assert_command_success $? "warc-cli --version failed" || return 1

    output_es=$("$ES_CLI" --version 2>&1)
    assert_command_success $? "es-cli --version failed" || return 1

    if ! echo "$output_warc" | grep -qE '[0-9]+\.[0-9]+\.[0-9]+'; then
        log_fail "warc-cli version output does not contain semver token: $output_warc"
        return 1
    fi

    if ! echo "$output_es" | grep -qE '[0-9]+\.[0-9]+\.[0-9]+'; then
        log_fail "es-cli version output does not contain semver token: $output_es"
        return 1
    fi
}

run_test test_version_check
