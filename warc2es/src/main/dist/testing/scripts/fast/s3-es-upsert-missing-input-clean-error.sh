#!/bin/bash
# Ensure es-upsert reports missing input path without raw shell cd noise.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_es_upsert_missing_input_clean_error() {
    local script="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/es-upsert.sh"
    assert_file_exists "$script" || return 1

    local missing="$TEST_OUTPUT_DIR/missing-parent/file.wet.gz"
    local output rc
    set +e
    output=$(bash "$script" "$missing" --url-id=u --crawl-id=c --dry-run \
        --result-format=human 2>&1)
    rc=$?
    set -e

    assert_command_failure "$rc" "es-upsert should fail for missing input path" || return 1
    echo "$output" | grep -q "Error: input path not found: $missing" || {
        log_fail "Missing input path error message is not explicit"
        return 1
    }
    if echo "$output" | grep -q "cd: "; then
        log_fail "Unexpected raw shell cd error leaked to user output"
        return 1
    fi
}

run_test test_es_upsert_missing_input_clean_error
