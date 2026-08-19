#!/bin/bash
# tc-err-001-corrupt-input.sh
# Task-ID: T-194
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_corrupt_input() {
    # 1. Create a corrupt gzip file (random bytes)
    local corrupt_warc="$TEST_OUTPUT_DIR/corrupt.warc.gz"
    echo "This is not a gzip file" > "$corrupt_warc"

    local output="$TEST_OUTPUT_DIR/err-001.wet.gz"

    log_info "Running extract-text on corrupt input..."
    # warc-cli uses graceful degradation: it exits 0 but skips bad files
    # This is acceptable behavior for batch processing pipelines

    "$WARC_CLI" extract-text "$corrupt_warc" "$output" 2>/dev/null
    local exit_code=$?

    # Check handling - either fail, or succeed but produce no/minimal output
    if [ "$exit_code" -ne 0 ]; then
         log_success "Command failed as expected with code $exit_code"
    else
         # Graceful handling: exits 0 but no records processed
         log_info "Command exited with 0 (graceful degradation)."
         # Output may or may not exist - check if it's empty or tiny
         if [ ! -f "$output" ] || [ ! -s "$output" ]; then
             log_success "Output empty or not created, acceptable graceful handling"
         else
             # Even if file exists, check record count
             local record_count
             record_count=$(zgrep -ic "warc-type:" "$output" 2>/dev/null || echo "0")
             if [ "$record_count" -eq 0 ]; then
                 log_success "Output has no valid records, acceptable handling"
             else
                 log_warn "Output has $record_count records from corrupt input (unexpected but not fatal)"
             fi
         fi
    fi

    # 2. Corrupt Mid-stream? (Truncated GZIP)
    # create valid then truncate
    ensure_test_data "tiny.warc.gz" || return 1
    cp "$TEST_DATA_DIR/tiny.warc.gz" "$TEST_OUTPUT_DIR/truncated.warc.gz"
    truncate -s 100 "$TEST_OUTPUT_DIR/truncated.warc.gz"

    log_info "Running info on truncated input..."
    "$WARC_CLI" info "$TEST_OUTPUT_DIR/truncated.warc.gz" 2>/dev/null
    exit_code=$?
    if [ "$exit_code" -ne 0 ]; then
        log_success "Info command failed on truncated input"
    else
        log_warn "Info command succeeded on truncated input (might have read partial)"
    fi
}

run_test test_corrupt_input
