#!/bin/bash
# s2-extract-basic.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_extract_basic() {
    ensure_test_data "example.com.warc.gz" || return 1
    local input="$TEST_DATA_DIR/example.com.warc.gz"
    local out_dir="$TEST_OUTPUT_DIR/extract-basic-out"

    "$WARC_CLI" extract-text "$input" --output-dir="$out_dir" --output-prefix=extract-basic --silent --progress-none --final-report-summary > /dev/null 2>&1
    assert_command_success $? "extract-text basic run" || return 1
    assert_directory_exists "$out_dir" || return 1

    local first_file
    first_file=$(find "$out_dir" -maxdepth 1 -type f -name '*.doet.gz' | head -n1)
    if [[ -z "$first_file" ]]; then
        log_fail "No output .doet.gz files found in $out_dir"
        return 1
    fi

    local size
    size=$(wc -c < "$first_file")
    assert_greater_than "$size" 0
}

run_test test_extract_basic
