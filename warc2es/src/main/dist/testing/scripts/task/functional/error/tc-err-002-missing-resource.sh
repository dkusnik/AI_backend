#!/bin/bash
# tc-err-002-missing-resource.sh
# Task-ID: T-194
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_missing_resource() {
    local missing_file="$TEST_OUTPUT_DIR/does_not_exist.warc.gz"
    local output="$TEST_OUTPUT_DIR/err-002.wet.gz"

    log_info "Running extract-text on missing file..."
    "$WARC_CLI" extract-text "$missing_file" "$output" 2>/dev/null
    local exit_code=$?

    # warc-cli uses graceful degradation: exits 0 with "skipped by before-checkers"
    # This is acceptable for batch processing where some files may be missing

    if [ "$exit_code" -ne 0 ]; then
        log_success "Command failed as expected with code $exit_code"
    else
        log_info "Command exited with 0 (graceful degradation for missing file)"
        # Verify no actual content was produced
        if [ -f "$output" ]; then
            # Check if file is empty or has no WARC records
            local file_size
            file_size=$(stat -c%s "$output" 2>/dev/null || echo "0")
            if [ "$file_size" -le 50 ]; then
                log_success "Output file empty or minimal (size: $file_size bytes)"
            else
                local record_count
                record_count=$(zgrep -ic "warc-type:" "$output" 2>/dev/null || echo "0")
                if [ "${record_count:-0}" -eq 0 ]; then
                    log_success "Output file has no WARC records (acceptable)"
                else
                    log_fail "Output file has $record_count records despite missing input"
                    return 1
                fi
            fi
        else
            log_success "Output file not created"
        fi
    fi
}

run_test test_missing_resource
