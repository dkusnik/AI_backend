#!/bin/bash
# tc-merge-001-alias.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_merge_alias() {
    ensure_test_data "tiny.warc.gz" || return 1

    local file1="$TEST_DATA_DIR/tiny.warc.gz"
    local file2="$TEST_DATA_DIR/tiny.warc.gz" # Self-merge is fine for testing

    local output="$TEST_OUTPUT_DIR/merge-001.warc.gz"

    log_info "Running 'merge' command (should behave like convert/merge)..."
    "$WARC_CLI" merge "$file1" "$file2" "$output"

    assert_command_success $?
    assert_file_exists "$output"

    # Simple check
    local size1=$(stat -c%s "$file1")
    local size_out=$(stat -c%s "$output")

    if [ "$size_out" -gt "$size1" ]; then
        log_success "Merge produced larger file"
    else
        log_warn "Merge output size check inconclusive"
    fi
}

run_test test_merge_alias
