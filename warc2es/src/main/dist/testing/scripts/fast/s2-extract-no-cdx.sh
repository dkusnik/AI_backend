#!/bin/bash
# s2-extract-no-cdx.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_extract_no_cdx_sidecar() {
    ensure_test_data "example.com.warc.gz" || return 1
    local input="$TEST_DATA_DIR/example.com.warc.gz"
    local out_dir="$TEST_OUTPUT_DIR/extract-no-cdx-out"

    "$WARC_CLI" extract-text "$input" --output-dir="$out_dir" --output-prefix=extract-no-cdx --no-cdx-sidecar --silent --progress-none --final-report-summary > /dev/null 2>&1
    assert_command_success $? "extract-text --no-cdx-sidecar" || return 1
    assert_directory_exists "$out_dir" || return 1

    local produced
    produced=$(find "$out_dir" -maxdepth 1 -type f -name '*.doet.gz' | wc -l)
    assert_greater_than "$produced" 0 || return 1

    if find "$out_dir" -maxdepth 1 -type f -name '*.cdxj' | grep -q .; then
        log_fail "CDX sidecar files should not be created in $out_dir"
        return 1
    fi
}

run_test test_extract_no_cdx_sidecar
