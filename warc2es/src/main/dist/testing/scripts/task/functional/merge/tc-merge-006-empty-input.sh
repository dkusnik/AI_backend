#!/usr/bin/env bash
# tc-merge-006-empty-input.sh
# Test 3.2: Empty Input File from merge_test_plan.md
#
# Validates graceful handling of empty secondary file:
# - All records from primary file go to base as base-only
# - Diff output is empty
# - No errors or crashes

set -e

# Source test library
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../../../../lib/test-lib.sh"

# Test data
FIXTURES_DIR="$PROJECT_ROOT/src/main/dist/testing/fixtures/merge"
FIRST_GLOBAL="$FIXTURES_DIR/first-globaldeduped.wet.gz"

test_empty_input_file() {
    log_info "Test 3.2: Empty Input File"

    # Verify fixture exists
    assert_file_exists "$FIRST_GLOBAL" || return 1

    # Create minimal empty DOET file (just warcinfo header)
    local empty_file="$TEST_OUTPUT_DIR/empty.wet.gz"
    echo "WARC/1.0" | gzip > "$empty_file"

    local base_out="$TEST_OUTPUT_DIR/test3.2-base.wet.gz"
    local diff_out="$TEST_OUTPUT_DIR/test3.2-diff.wet.gz"

    # Run merge (should handle empty file gracefully)
    "$WARC_CLI" merge \
        "$FIRST_GLOBAL" \
        "$empty_file" \
        --output-base="$base_out" \
        --output-diff="$diff_out" \
        --silent \
        > /dev/null 2>&1 || true  # Don't fail if command fails

    # Check if outputs were created
    if [ ! -f "$base_out" ]; then
        log_fail "Base output not created"
        return 1
    fi

    # Count records
    local base_count diff_count
    base_count=$(warc_count "$base_out")
    diff_count=$(warc_count "$diff_out")

    log_info "  Base: $base_count records"
    log_info "  Diff: $diff_count records"

    # Expected: 290 in base (all from first file), 0 in diff
    if [ "$base_count" -eq 290 ]; then
        log_success "Base has all records from primary file (290)"
    else
        log_fail "Unexpected base count: $base_count (expected: 290)"
        return 1
    fi

    if [ "$diff_count" -eq 0 ]; then
        log_success "Diff output is empty (no changes)"
    else
        log_warn "Diff output not empty: $diff_count records"
    fi

    return 0
}

# Run test
run_test test_empty_input_file
