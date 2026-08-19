#!/bin/bash
# T-213: Directory input for extract-text: Java recursively discovers all WARC files.
# Creates a temp directory containing two small WARC files, runs extract-text with the
# directory as the sole input, and asserts the output record count matches
# explicit-file invocation output.
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

test_directory_input() {
    ensure_test_data "example.com.warc.gz"  || { log_warn "example.com fixture missing — skipping"; return 0; }
    ensure_test_data "large-test.warc.gz"   || { log_warn "large-test fixture missing — skipping"; return 0; }

    local src1="$TEST_DATA_DIR/example.com.warc.gz"
    local src2="$TEST_DATA_DIR/large-test.warc.gz"

    # Build a small directory tree with the two fixtures:
    #   $warc_dir/a/example.com.warc.gz
    #   $warc_dir/b/large-test.warc.gz
    local warc_dir="$TEST_OUTPUT_DIR/input-dir"
    mkdir -p "$warc_dir/a" "$warc_dir/b"
    cp "$src1" "$warc_dir/a/"
    cp "$src2" "$warc_dir/b/"

    local out_dir="$TEST_OUTPUT_DIR/ext-012-dir-out"
    local out_explicit="$TEST_OUTPUT_DIR/ext-012-explicit-out"
    local prefix_dir="ext012dir"
    local prefix_explicit="ext012explicit"

    count_conversions_in_dir() {
        local dir="$1"
        local pattern="$2"
        local total=0
        local f
        while IFS= read -r f; do
            total=$(( total + $(warc_count "$f") ))
        done < <(find "$dir" -maxdepth 1 -type f -name "${pattern}-*.doet.gz" | LC_ALL=C sort)
        echo "$total"
    }

    log_info "Extracting from directory (recursive discovery in Java)..."
    "$WARC_CLI" extract-text "$warc_dir" --output-dir="$out_dir" --output-prefix="$prefix_dir" --silent
    assert_command_success $? "extract from directory"
    assert_directory_exists "$out_dir"

    log_info "Extracting from explicit file list..."
    "$WARC_CLI" extract-text "$src1" "$src2" --output-dir="$out_explicit" --output-prefix="$prefix_explicit" --silent
    assert_command_success $? "extract from explicit files"
    assert_directory_exists "$out_explicit"

    local count_dir
    count_dir=$(count_conversions_in_dir "$out_dir" "$prefix_dir")
    local count_explicit
    count_explicit=$(count_conversions_in_dir "$out_explicit" "$prefix_explicit")

    log_info "directory input: $count_dir records | explicit files: $count_explicit records"

    # Both runs process the same records (same files, same pipeline) so counts must match.
    if [[ $count_dir -ne $count_explicit ]]; then
        log_fail "count mismatch: directory=$count_dir explicit=$count_explicit"
        return 1
    fi
    if [[ $count_dir -le 0 ]]; then
        log_fail "output is empty"
        return 1
    fi

    log_info "Directory and explicit-list outputs are equivalent ✓"
    return 0
}

run_test test_directory_input
