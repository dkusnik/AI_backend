#!/bin/bash
# s2-merge-global-scope.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_merge_global_scope() {
    ensure_test_data "example.com.warc.gz" || return 1
    ensure_test_data "example_copy.warc.gz" || return 1
    local in1="$TEST_DATA_DIR/example.com.warc.gz"
    local in2="$TEST_DATA_DIR/example_copy.warc.gz"

    local base1="$TEST_OUTPUT_DIR/m3-base1.doet.gz"
    local base2="$TEST_OUTPUT_DIR/m3-base2.doet.gz"
    local merged="$TEST_OUTPUT_DIR/m3-merged.doet.gz"
    local diff="$TEST_OUTPUT_DIR/m3-diff.doet.gz"

    "$WARC_CLI" extract-text "$in1" "$base1" --silent --progress-none --final-report-summary > /dev/null 2>&1 || return 1
    "$WARC_CLI" extract-text "$in2" "$base2" --silent --progress-none --final-report-summary > /dev/null 2>&1 || return 1
    "$WARC_CLI" merge --output-base="$merged" --output-diff="$diff" --deduplicate-scope=global "$base1" "$base2" --silent --progress-none --final-report-summary > /dev/null 2>&1
    assert_command_success $? "merge with global scope" || return 1
    assert_file_exists "$merged" || return 1
    assert_file_exists "$diff" || return 1
}

run_test test_merge_global_scope
