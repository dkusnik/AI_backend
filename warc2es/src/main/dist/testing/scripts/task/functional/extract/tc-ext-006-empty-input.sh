#!/bin/bash
# tc-ext-006-empty-input.sh
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_empty_input() {
    ensure_test_data "empty.warc.gz" || return 1
    local input="$TEST_DATA_DIR/empty.warc.gz"
    local out_dir="$TEST_OUTPUT_DIR/ext-006-out"
    local prefix="ext006"
    mkdir -p "$out_dir"

    log_info "Running extract-text on empty WARC..."
    "$WARC_CLI" extract-text "$input" --output-dir="$out_dir" --output-prefix="$prefix"

    # Should exit 0
    assert_command_success $?
    assert_directory_exists "$out_dir"

    # Empty input should not produce any dated output files.
    local out_count
    out_count=$(find "$out_dir" -maxdepth 1 -type f -name "${prefix}-*.doet.gz" | wc -l | tr -d '[:space:]')
    if [ "$out_count" -ne 0 ]; then
        log_fail "Expected 0 output files for empty input, got $out_count"
        return 1
    fi
}

run_test test_empty_input
