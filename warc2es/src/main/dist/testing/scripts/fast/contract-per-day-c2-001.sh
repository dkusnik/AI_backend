#!/bin/bash
# OWNER: C2-001
# Record-level WARC-Date splitting is implemented and covered in Java.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_per_day_contract() {
    local fixture="$PROJECT_ROOT/src/test/resources/multi-day.warc.gz"
    local output_dir="$TEST_OUTPUT_DIR/per-day"
    local output rc

    assert_file_exists "$fixture" || return 1
    assert_file_exists \
        "$PROJECT_ROOT/src/test/java/pl/gov/nac/warc/integration/PerDaySplitTest.java" || return 1
    mkdir -p "$output_dir"

    set +e
    output=$("$WARC_CLI" extract-text "$fixture" \
        --output-dir="$output_dir" --per-day --deduplicate-scope=global \
        --no-cdx-sidecar --brief 2>&1)
    rc=$?
    set -e
    assert_command_success "$rc" "record-level per-day extraction failed: $output" || return 1

    assert_file_exists "$output_dir/20260101.doet.gz" || return 1
    assert_file_exists "$output_dir/20260102.doet.gz" || return 1
    [[ "$(find "$output_dir" -maxdepth 1 -type f -name '*.doet.gz' | wc -l)" -eq 2 ]] || {
        log_fail "per-day extraction did not produce exactly two date buckets"
        return 1
    }
    [[ "$(warc_count "$output_dir/20260101.doet.gz")" -eq 1 &&
       "$(warc_count "$output_dir/20260102.doet.gz")" -eq 1 ]] || {
        log_fail "record-level dates were not split one record per output day"
        return 1
    }
}

run_test test_per_day_contract
