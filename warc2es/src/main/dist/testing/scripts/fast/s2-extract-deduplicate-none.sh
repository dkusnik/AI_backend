#!/bin/bash
# --deduplicate-scope=none must retain records and publish only below the output directory.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_extract_deduplicate_none_output_containment() {
    ensure_test_data "example.com.warc.gz" || return 1

    local input_dir="$TEST_OUTPUT_DIR/none-input"
    local output_dir="$TEST_OUTPUT_DIR/none-output"
    local input="$input_dir/example.com.warc.gz"
    local output="$output_dir/example.com.doet.gz"
    mkdir -p "$input_dir"
    cp "$TEST_DATA_DIR/example.com.warc.gz" "$input"

    "$WARC_CLI" extract-text "$input" "$output_dir" \
        --deduplicate-scope=none --silent --progress-none --final-report-summary \
        > /dev/null 2>&1 || return 1

    assert_file_exists "$output" || return 1
    [[ ! -e "$input_dir/example.com.doet.gz" ]] || {
        log_fail "none scope escaped the requested output directory"
        return 1
    }

    local records
    records="$(warc_count "$output")"
    [[ "$records" -eq 1 ]] || {
        log_fail "none scope emitted $records records, expected 1"
        return 1
    }

    local marked
    marked="$(zgrep -ic '^nac-deduplicated: none' "$output" 2>/dev/null || true)"
    [[ "$marked" -eq "$records" ]] || {
        log_fail "none scope marked $marked/$records records"
        return 1
    }
}

run_test test_extract_deduplicate_none_output_containment
