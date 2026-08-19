#!/bin/bash
# tc-ext-001-basic.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_basic_extract() {
    ensure_test_data "example.com.warc.gz" || return 1
    local input="$TEST_DATA_DIR/example.com.warc.gz"
    local out_dir="$TEST_OUTPUT_DIR/ext-001-out"
    local prefix="ext001"
    mkdir -p "$out_dir"

    log_info "Running extract-text on $input..."
    "$WARC_CLI" extract-text "$input" --output-dir="$out_dir" --output-prefix="$prefix"
    assert_command_success $? "extract-text"

    assert_directory_exists "$out_dir"
    local output
    output=$(find "$out_dir" -maxdepth 1 -type f -name "${prefix}-*.doet.gz" | head -n1)
    if [ -z "$output" ]; then
        log_fail "No output DOET file produced"
        return 1
    fi
    assert_file_exists "$output"

    # Validate content
    log_info "Validating output content..."

    if ! zgrep -qi "warc-type: conversion" "$output"; then
        log_fail "Output missing conversion records"
        return 1
    fi

    if ! zgrep -qi "content-type: text/plain" "$output"; then
        log_fail "Output missing text/plain content type"
        return 1
    fi
}

run_test test_basic_extract
