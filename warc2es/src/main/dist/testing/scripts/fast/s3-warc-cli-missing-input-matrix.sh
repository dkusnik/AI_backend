#!/bin/bash
# s3-warc-cli-missing-input-matrix.sh
# W1-7: every file-processing warc-cli command must fail for a missing input.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_warc_cli_missing_input_matrix() {
    local missing="$TEST_OUTPUT_DIR/does-not-exist.warc.gz"
    local failures=0
    mkdir -p "$TEST_OUTPUT_DIR"

    run_must_fail() {
        local name="$1"
        shift
        local code=0
        "$@" > "$TEST_OUTPUT_DIR/$name.out" 2> "$TEST_OUTPUT_DIR/$name.err" || code=$?
        if [[ "$code" -eq 0 ]]; then
            log_fail "$name unexpectedly succeeded for missing input"
            failures=$((failures + 1))
        else
            log_info "$name failed for missing input (exit=$code) ✓"
        fi
    }

    run_must_fail extract-text "$WARC_CLI" extract-text "$missing" --output-dir="$TEST_OUTPUT_DIR/extract"
    run_must_fail dedupe "$WARC_CLI" dedupe "$missing" "$TEST_OUTPUT_DIR/deduped.wet.gz"
    run_must_fail grep "$WARC_CLI" grep "$missing" "$TEST_OUTPUT_DIR/filtered.warc.gz"
    run_must_fail convert "$WARC_CLI" convert "$missing" "$TEST_OUTPUT_DIR/converted.warc.gz"
    run_must_fail regen-cdxj "$WARC_CLI" regen-cdxj "$missing"
    run_must_fail regen-digests "$WARC_CLI" regen-digests "$missing" "$TEST_OUTPUT_DIR/digests.warc.gz"
    run_must_fail regen-zip "$WARC_CLI" regen-zip "$missing" "$TEST_OUTPUT_DIR/recompressed.warc.gz"
    run_must_fail merge "$WARC_CLI" merge --output-base="$TEST_OUTPUT_DIR/base.doet.gz" --output-diff="$TEST_OUTPUT_DIR/diff.doet.gz" "$missing"
    run_must_fail baseline "$WARC_CLI" baseline --output="$TEST_OUTPUT_DIR/baseline.doet.gz" "$missing"
    run_must_fail extract-merge-baseline "$WARC_CLI" extract-merge-baseline --output="$TEST_OUTPUT_DIR/extract-merge.doet.gz" "$missing"
    run_must_fail info "$WARC_CLI" info "$missing"
    run_must_fail validate "$WARC_CLI" validate "$missing"

    [[ "$failures" -eq 0 ]]
}

run_test test_warc_cli_missing_input_matrix
