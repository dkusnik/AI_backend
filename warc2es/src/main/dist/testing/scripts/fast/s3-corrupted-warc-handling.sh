#!/bin/bash
# s3-corrupted-warc-handling.sh
# T-071/W1-7: corrupted WARC handling must fail loudly and must not produce
# content records from a completely invalid file.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_corrupted_warc_handling_fails_loudly() {
    local bad_file="$TEST_OUTPUT_DIR/corrupted.warc.gz"
    local out_file="$TEST_OUTPUT_DIR/corrupted-out.wet.gz"
    mkdir -p "$TEST_OUTPUT_DIR"

    # Deliberately invalid gzip/warc payload.
    printf 'not-a-valid-warc-or-gzip' > "$bad_file"

    local code=0
    "$WARC_CLI" extract-text "$bad_file" "$out_file" --no-cdx-sidecar 2>/dev/null || code=$?
    if [[ "$code" -eq 0 ]]; then
        log_fail "extract-text unexpectedly succeeded on corrupted input"
        return 1
    fi
    log_info "extract-text failed on corrupted input (exit=$code) ✓"

    # If output exists, it must contain 0 content records
    if [[ -f "$out_file" ]]; then
        local recs
        recs=$(zcat "$out_file" 2>/dev/null | grep -c "^WARC-Type: conversion" || echo 0)
        if [[ "$recs" -ne 0 ]]; then
            log_fail "Corrupted input produced $recs content records (expected 0)"
            return 1
        fi
        log_info "No content records from corrupted input ✓"
    else
        log_info "No output file produced from corrupted input ✓"
    fi
}

test_truncated_archive_fails_loudly() {
    local truncated_file="$TEST_OUTPUT_DIR/truncated.warc.gz"
    local out_file="$TEST_OUTPUT_DIR/truncated-out.wet.gz"
    local source_size
    source_size=$(wc -c < "$TEST_DATA_DIR/tiny.warc.gz") || return 1

    make_truncated_archive_fixture \
        "$TEST_DATA_DIR/tiny.warc.gz" \
        "$truncated_file" \
        "$((source_size - 1))" || return 1

    local code=0
    "$WARC_CLI" extract-text "$truncated_file" "$out_file" --no-cdx-sidecar \
        >/dev/null 2>&1 || code=$?
    if [[ "$code" -eq 0 ]]; then
        log_fail "extract-text unexpectedly succeeded on truncated gzip input"
        return 1
    fi
    log_info "extract-text failed on truncated gzip input (exit=$code) ✓"
}

run_test test_corrupted_warc_handling_fails_loudly
run_test test_truncated_archive_fails_loudly
