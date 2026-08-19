#!/bin/bash
# s1-validate-single-file-ok.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_validate_single_file_ok() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input="$TEST_DATA_DIR/tiny.warc.gz"
    local output
    local rc

    set +e
    output=$("$WARC_CLI" validate "$input" 2>&1)
    rc=$?
    set -e

    assert_command_success "$rc" "validate tiny.warc.gz should pass" || return 1
    echo "$output" | grep -q "$input: OK" || {
        log_fail "Expected per-file OK status"
        return 1
    }
    echo "$output" | grep -q "Validated: 1, Failed: 0" || {
        log_fail "Expected single-file summary counts"
        return 1
    }
}

run_test test_validate_single_file_ok
