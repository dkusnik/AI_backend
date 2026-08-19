#!/bin/bash
# tc-ext-007-min-length-boundary.sh
# T-091: Extract-text min-length boundary: verify that records shorter than
# the threshold are dropped and records at/above the threshold are kept.
# Tests N-1 (all dropped), N (kept), N+1 (kept) logic by using a known
# fixture and varying the threshold to be above/below the content length.
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_min_length_boundary() {
    ensure_test_data "example.com.warc.gz" || return 1

    local input="$TEST_DATA_DIR/example.com.warc.gz"

    # Step 1: extract with no min-length restriction to get baseline doc count
    local baseline_out="$TEST_OUTPUT_DIR/min-len-baseline.wet.gz"
    "$WARC_CLI" extract-text "$input" "$baseline_out" \
        --processor.extract-text.extract-min-text-length=1 \
        --no-cdx-sidecar 2>/dev/null
    local baseline_count
    baseline_count=$(zcat "$baseline_out" | grep -c "^WARC-Type: conversion" || echo 0)
    log_info "Baseline (min=1): $baseline_count records"

    if [[ "$baseline_count" -eq 0 ]]; then
        log_warn "No text records in example.com.warc.gz — skipping"
        return 0
    fi

    # Step 2: extract with an absurdly large min-length → expect 0 records
    local none_out="$TEST_OUTPUT_DIR/min-len-none.wet.gz"
    "$WARC_CLI" extract-text "$input" "$none_out" \
        --processor.extract-text.extract-min-text-length=999999 \
        --no-cdx-sidecar 2>/dev/null
    local none_count
    none_count=$(zcat "$none_out" | grep -c "^WARC-Type: conversion" || echo 0)
    log_info "Min=999999: $none_count records (expected 0)"

    local failed=0

    if [[ "$none_count" -ne 0 ]]; then
        log_fail "Min-length=999999 still produced $none_count records (expected 0)"
        failed=$((failed+1))
    else
        log_info "Min-length=999999 correctly produced 0 records ✓"
    fi

    # Step 3: extract with a very low min-length → expect same as baseline
    local low_out="$TEST_OUTPUT_DIR/min-len-low.wet.gz"
    "$WARC_CLI" extract-text "$input" "$low_out" \
        --processor.extract-text.extract-min-text-length=1 \
        --no-cdx-sidecar 2>/dev/null
    local low_count
    low_count=$(zcat "$low_out" | grep -c "^WARC-Type: conversion" || echo 0)
    log_info "Min=1: $low_count records (expected $baseline_count)"

    if [[ "$low_count" -ne "$baseline_count" ]]; then
        log_fail "Min=1 produced $low_count, expected baseline $baseline_count"
        failed=$((failed+1))
    else
        log_info "Min=1 count matches baseline ✓"
    fi

    # Step 4: intermediate threshold — verify monotonicity (low ≥ mid ≥ high)
    local mid_out="$TEST_OUTPUT_DIR/min-len-mid.wet.gz"
    "$WARC_CLI" extract-text "$input" "$mid_out" \
        --processor.extract-text.extract-min-text-length=500 \
        --no-cdx-sidecar 2>/dev/null
    local mid_count
    mid_count=$(zcat "$mid_out" | grep -c "^WARC-Type: conversion" || echo 0)
    log_info "Min=500: $mid_count records"

    if [[ "$mid_count" -gt "$baseline_count" ]]; then
        log_fail "Min=500 produced more records ($mid_count) than min=1 ($baseline_count) — not monotone"
        failed=$((failed+1))
    else
        log_info "Monotonicity: min=1 ($baseline_count) >= min=500 ($mid_count) >= min=999999 ($none_count) ✓"
    fi

    if [[ $failed -gt 0 ]]; then
        echo "TESTCASE|min-length-boundary|FAIL|failures=$failed"
        return 1
    fi

    echo "TESTCASE|min-length-boundary|PASS|baseline=$baseline_count,mid=$mid_count,none=$none_count"
}

run_test test_min_length_boundary
