#!/bin/bash
# tc-perf-009-chain-jfr-smoke.sh
# T-100: JFR-enabled two-step chain (extract-text → dedupe) produces a .jfr
# recording for each step and completes successfully (no crash, non-empty output).
set -euo pipefail
source "$(dirname "$0")/../../lib/test-lib.sh"

test_chain_jfr_smoke() {
    ensure_test_data "example.com.warc.gz" || return 1

    local wet="$TEST_OUTPUT_DIR/chain-jfr.wet.gz"
    local doet="$TEST_OUTPUT_DIR/chain-jfr.doet.gz"
    local jfr_extract="$TEST_OUTPUT_DIR/chain-jfr-extract.jfr"
    local jfr_dedupe="$TEST_OUTPUT_DIR/chain-jfr-dedupe.jfr"

    mkdir -p "$TEST_OUTPUT_DIR"

    # Step 1: extract-text with JFR
    log_info "Step 1: extract-text with JFR recording..."
    local extract_exit=0
    WARC_JFR_ENABLED=true WARC_JFR_PATH="$jfr_extract" \
        "$WARC_CLI" extract-text "$TEST_DATA_DIR/example.com.warc.gz" "$wet" \
        2>/dev/null || extract_exit=$?

    if [[ "$extract_exit" -ne 0 ]]; then
        log_fail "extract-text (JFR) exited $extract_exit"
        echo "TESTCASE|chain-jfr-smoke|FAIL|step=extract,exit=$extract_exit"
        return 1
    fi
    assert_file_exists "$wet" || { echo "TESTCASE|chain-jfr-smoke|FAIL|step=extract,missing=wet"; return 1; }
    assert_file_exists "$jfr_extract" || { echo "TESTCASE|chain-jfr-smoke|FAIL|step=extract,missing=jfr"; return 1; }
    log_info "extract-text: output and JFR recording present ✓"

    # Step 2: dedupe with JFR
    log_info "Step 2: dedupe with JFR recording..."
    local dedupe_exit=0
    WARC_JFR_ENABLED=true WARC_JFR_PATH="$jfr_dedupe" \
        "$WARC_CLI" dedupe "$wet" "$doet" \
        2>/dev/null || dedupe_exit=$?

    if [[ "$dedupe_exit" -ne 0 ]]; then
        log_fail "dedupe (JFR) exited $dedupe_exit"
        echo "TESTCASE|chain-jfr-smoke|FAIL|step=dedupe,exit=$dedupe_exit"
        return 1
    fi
    assert_file_exists "$doet" || { echo "TESTCASE|chain-jfr-smoke|FAIL|step=dedupe,missing=doet"; return 1; }
    assert_file_exists "$jfr_dedupe" || { echo "TESTCASE|chain-jfr-smoke|FAIL|step=dedupe,missing=jfr"; return 1; }
    log_info "dedupe: output and JFR recording present ✓"

    # Sanity: JFR files must be non-empty
    local ext_size ded_size
    ext_size=$(stat -c%s "$jfr_extract")
    ded_size=$(stat -c%s "$jfr_dedupe")
    if [[ "$ext_size" -eq 0 ]] || [[ "$ded_size" -eq 0 ]]; then
        log_fail "One or both JFR recordings are empty (extract=$ext_size, dedupe=$ded_size)"
        echo "TESTCASE|chain-jfr-smoke|FAIL|jfr-empty=true"
        return 1
    fi
    log_info "JFR file sizes: extract=${ext_size}B, dedupe=${ded_size}B ✓"

    echo "TESTCASE|chain-jfr-smoke|PASS|jfr-extract=${ext_size}B,jfr-dedupe=${ded_size}B"
}

run_test test_chain_jfr_smoke
