#!/bin/bash
# s0-binary-check.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_binaries_exist() {
    log_info "Checking binaries in $BIN_DIR..."
    assert_file_exists "$WARC_CLI"
    assert_file_exists "$ES_CLI"

    # Check permissions (basic check if executable)
    if [ -x "$WARC_CLI" ]; then
        log_info "warc-cli is executable"
    else
        log_fail "warc-cli is NOT executable"
        return 1
    fi

    if [ -x "$ES_CLI" ]; then
         log_info "es-cli is executable"
    else
         log_fail "es-cli is NOT executable"
         return 1
    fi
}

run_test test_binaries_exist
