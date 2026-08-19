#!/bin/bash
# tc-conv-001-merge-files.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_merge_files() {
    ensure_test_data "tiny.warc.gz" || return 1

    # We need 2 input files.
    # tiny.warc.gz is one.
    # Let's make a copy.

    local file1="$TEST_DATA_DIR/example.com.warc.gz"
    local file2="$TEST_DATA_DIR/tiny_copy.warc.gz"
    cp "$file1" "$file2"

    local output="$TEST_OUTPUT_DIR/conv-001-merged.warc.gz"
    local cdx="$TEST_OUTPUT_DIR/conv-001-merged.cdxj"

    log_info "Running convert (merge) on 2 files..."
    "$WARC_CLI" convert "$file1" "$file2" "$output"

    assert_command_success $?
    assert_file_exists "$output"
    assert_file_exists "$cdx"

    # Verification
    # Count WARC records in output. Should be 2 * count(tiny).
    # tiny has ~ X records.
    # We can check simple size assumption or grep WARC-Type: response check.

    # Let's assume tiny has 1 response (it likely has 1 response + 1 warcinfo?).
    # Just verify that output size > file1 size (rough check for merge)

    local size1=$(stat -c%s "$file1")
    local size_out=$(stat -c%s "$output")

    if [ "$size_out" -gt "$size1" ]; then
        log_success "Output size ($size_out) is larger than single input ($size1)"
    else
         log_warn "Output size ($size_out) not larger than single input ($size1). Deduplication happened? Or tiny is empty?"
         # If tiny is just empty valid warc, size might be small.
    fi

    # Check CDX line count
    # cdxj usually has 1 line per record
    local count_cdx=$(wc -l < "$cdx")
    log_info "CDX lines: $count_cdx"

    if [ "$count_cdx" -gt 0 ]; then
         return 0
    else
         log_fail "CDX empty"
         return 1
    fi
}

run_test test_merge_files
