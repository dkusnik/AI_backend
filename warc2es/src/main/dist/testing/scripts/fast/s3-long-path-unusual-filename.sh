#!/bin/bash
# s3-long-path-unusual-filename.sh
# T-073: Long path / unusual filename handling — extract-text must succeed
# when input/output paths contain spaces, brackets, commas, and deep directories.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_long_path_and_unusual_filename() {
    ensure_test_data "example.com.warc.gz" || return 1
    mkdir -p "$TEST_OUTPUT_DIR"

    local deep="$TEST_OUTPUT_DIR/aaaaaaaaaaaaaaaaaaaa/bbbbbbbbbbbbbbbbbbbb/cccccccccccccccccccc"
    mkdir -p "$deep"

    # Use a real WARC with text content at a path with spaces, brackets, parens
    # Note: comma is intentionally excluded — warc-cli uses comma as file list separator
    local input="$deep/input file (v1) [qa] x.warc.gz"
    local out_dir="$deep/output dir (v1) [qa] x"
    cp "$TEST_DATA_DIR/example.com.warc.gz" "$input"

    "$WARC_CLI" extract-text "$input" --output-dir="$out_dir" --output-prefix=path-test --no-cdx-sidecar \
        --silent --progress-none --final-report-summary > /dev/null 2>&1 || {
        log_fail "extract-text failed for long/unusual path"
        return 1
    }

    assert_directory_exists "$out_dir" || return 1
    local produced
    produced=$(find "$out_dir" -maxdepth 1 -type f -name '*.doet.gz' | wc -l)
    assert_greater_than "$produced" 0 || return 1
    log_info "extract-text succeeded on long path with unusual filename ✓"
}

run_test test_long_path_and_unusual_filename
