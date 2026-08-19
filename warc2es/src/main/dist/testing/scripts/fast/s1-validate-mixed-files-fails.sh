#!/bin/bash
# s1-validate-mixed-files-fails.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_validate_mixed_files_fails() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input_ok="$TEST_DATA_DIR/tiny.warc.gz"
    local input_missing="$TEST_OUTPUT_DIR/does-not-exist.warc.gz"
    local output
    local rc

    set +e
    output=$("$WARC_CLI" validate "$input_ok" "$input_missing" 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "validate with missing file should fail" || return 1
    echo "$output" | grep -q "$input_ok: OK" || {
        log_fail "Expected valid input to be reported as OK"
        return 1
    }
    echo "$output" | grep -q "$input_missing: FAIL (file not found)" || {
        log_fail "Expected missing file message"
        return 1
    }
    echo "$output" | grep -q "Validated: 1, Failed: 1" || {
        log_fail "Expected mixed-file summary counts"
        return 1
    }
}

run_test test_validate_mixed_files_fails
