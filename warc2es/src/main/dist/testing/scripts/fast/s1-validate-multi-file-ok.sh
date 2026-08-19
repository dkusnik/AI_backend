#!/bin/bash
# s1-validate-multi-file-ok.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_validate_multi_file_ok() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input1="$TEST_OUTPUT_DIR/validate-a.warc.gz"
    local input2="$TEST_OUTPUT_DIR/validate-b.warc.gz"
    cp "$TEST_DATA_DIR/tiny.warc.gz" "$input1"
    cp "$TEST_DATA_DIR/tiny.warc.gz" "$input2"
    local output
    local rc

    set +e
    output=$("$WARC_CLI" validate "$input1" "$input2" 2>&1)
    rc=$?
    set -e

    assert_command_success "$rc" "validate with two files should pass" || return 1
    echo "$output" | grep -q "$input1: OK" || {
        log_fail "Expected first file OK status"
        return 1
    }
    echo "$output" | grep -q "$input2: OK" || {
        log_fail "Expected second file OK status"
        return 1
    }
    echo "$output" | grep -q "Validated: 2, Failed: 0" || {
        log_fail "Expected two-file summary counts"
        return 1
    }
}

run_test test_validate_multi_file_ok
