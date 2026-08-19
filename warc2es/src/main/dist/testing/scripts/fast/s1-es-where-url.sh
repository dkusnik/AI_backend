#!/bin/bash
# s1-es-where-url.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_es_where_url_command() {
    log_info "Checking es-cli where-es-url command..."
    local output
    output=$("$ES_CLI" where-es-url 2>&1)
    assert_command_success $? "es-cli where-es-url"

    if echo "$output" | grep -qiE "http|localhost|127\.0\.0\.1"; then
        return 0
    fi
    log_fail "Unexpected where-es-url output: $output"
    return 1
}

run_test test_es_where_url_command
