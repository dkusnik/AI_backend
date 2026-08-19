#!/bin/bash
# s2-extract-min-length.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_extract_with_min_length() {
    ensure_test_data "example.com.warc.gz" || return 1
    local input="$TEST_DATA_DIR/example.com.warc.gz"
    local out_dir="$TEST_OUTPUT_DIR/extract-min-length-out"

    "$WARC_CLI" extract-text "$input" --output-dir="$out_dir" --output-prefix=extract-min \
        --processor.extract-text.extract-min-text-length=20 \
        --silent --progress-none --final-report-summary > /dev/null 2>&1
    assert_command_success $? "extract-text with min length" || return 1
    assert_directory_exists "$out_dir" || return 1

    local produced
    produced=$(find "$out_dir" -maxdepth 1 -type f -name '*.doet.gz' | wc -l)
    assert_greater_than "$produced" 0 || return 1
}

run_test test_extract_with_min_length
