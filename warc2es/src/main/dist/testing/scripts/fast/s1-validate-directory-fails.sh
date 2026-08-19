#!/bin/bash
# s1-validate-directory-fails.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_validate_directory_fails() {
    ensure_test_data "tiny.warc.gz" || return 1
    local input_dir="$TEST_OUTPUT_DIR/validate-dir"
    mkdir -p "$input_dir"
    cp "$TEST_DATA_DIR/tiny.warc.gz" "$input_dir/a.warc.gz"
    cp "$TEST_DATA_DIR/tiny.warc.gz" "$input_dir/b.warc.gz"

    local output
    local rc
    set +e
    output=$("$WARC_CLI" validate "$input_dir" 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "validate with directory input should fail" || return 1
    echo "$output" | grep -qiE "file not found|fail" || {
        log_fail "Expected directory rejection diagnostics"
        return 1
    }
}

run_test test_validate_directory_fails
