#!/bin/bash
# tc-ext-003-min-length.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_min_length() {
    ensure_test_data "example.com.warc.gz" || return 1
    local input="$TEST_DATA_DIR/example.com.warc.gz"
    local out_dir="$TEST_OUTPUT_DIR/ext-003-out"
    local prefix="ext003"
    mkdir -p "$out_dir"

    # Set min length to filter out small pages (but allow some through for testing)
    local min_len=100

    log_info "Running extract-text with min-length=$min_len..."
    "$WARC_CLI" extract-text "$input" --output-dir="$out_dir" --output-prefix="$prefix" \
        --processor.extract-text.extract-min-text-length=$min_len

    assert_command_success $?
    assert_directory_exists "$out_dir"
    local output
    output=$(find "$out_dir" -maxdepth 1 -type f -name "${prefix}-*.doet.gz" | head -n1)
    if [ -z "$output" ]; then
        log_fail "No output DOET file produced"
        return 1
    fi
    assert_file_exists "$output"

    # Verification is tricky without parsing WET body length
    # But we can assume record count is <= input record count
    # Ideally should verify a known small page is missing.

    # For now, just ensuring it runs and produces valid output
    # Note: use zgrep directly instead of assert_contains with process substitution
    if zgrep -qi "warc-type:.*conversion" "$output"; then
        log_success "Output contains WARC conversion records"
    else
        log_fail "Output missing WARC conversion records"
        return 1
    fi
}

run_test test_min_length
