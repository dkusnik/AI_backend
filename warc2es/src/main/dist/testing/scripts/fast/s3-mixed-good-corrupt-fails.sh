#!/bin/bash
# s3-mixed-good-corrupt-fails.sh
# W1-7: one good input plus one corrupt input must make the run non-zero.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_mixed_good_corrupt_fails() {
    ensure_test_data "tiny.warc.gz" || return 1

    local good="$TEST_DATA_DIR/tiny.warc.gz"
    local bad="$TEST_OUTPUT_DIR/corrupted.warc.gz"
    local out_dir="$TEST_OUTPUT_DIR/extract"
    mkdir -p "$TEST_OUTPUT_DIR"
    printf 'not-a-valid-warc-or-gzip' > "$bad"

    local code=0
    "$WARC_CLI" extract-text "$good" "$bad" --output-dir="$out_dir" --no-cdx-sidecar > "$TEST_OUTPUT_DIR/mixed.out" 2> "$TEST_OUTPUT_DIR/mixed.err" || code=$?
    if [[ "$code" -eq 0 ]]; then
        log_fail "mixed good+corrupt extraction unexpectedly succeeded"
        return 1
    fi
    log_info "mixed good+corrupt extraction failed (exit=$code) ✓"
}

run_test test_mixed_good_corrupt_fails
