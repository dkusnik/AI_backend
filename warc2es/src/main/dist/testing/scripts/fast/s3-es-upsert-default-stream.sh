#!/bin/bash
# Ensure es-upsert defaults to nac-data-default stream.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_es_upsert_default_stream_is_nac_data_default() {
    local script="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/es-upsert.sh"
    assert_file_exists "$script" || return 1

    local input="$TEST_OUTPUT_DIR/20260101-010101-ingest.wet.gz"
    : > "$input"

    local output rc
    set +e
    output=$(bash "$script" "$input" --url-id=u --crawl-id=c --dry-run 2>&1)
    rc=$?
    set -e

    assert_command_success "$rc" "es-upsert dry-run should succeed for existing file" || return 1
    echo "$output" | grep -q "nac-data-default" || {
        log_fail "Expected default stream nac-data-default in dry-run output"
        return 1
    }
}

run_test test_es_upsert_default_stream_is_nac_data_default
