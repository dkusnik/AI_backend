#!/bin/bash
# OWNER: C2-004
# Java emits exactly one stable processing result on every handled JSON-mode path.
source "$(dirname "$0")/../../lib/test-lib.sh"

assert_single_result() {
    local file="$1"
    local filter="$2"
    [[ "$(wc -l < "$file")" -eq 1 ]] || {
        log_fail "Expected exactly one stdout line in $file"
        return 1
    }
    jq -e "$filter" "$file" >/dev/null || {
        log_fail "Processing result failed schema assertion: $filter"
        cat "$file" >&2
        return 1
    }
}

test_processing_json_contract() {
    mkdir -p "$TEST_OUTPUT_DIR"
    local input="$TEST_DATA_DIR/tiny.warc.gz"
    local completed="$TEST_OUTPUT_DIR/completed.json"
    local success="$TEST_OUTPUT_DIR/success.json"
    local failure="$TEST_OUTPUT_DIR/failure.json"
    local processing_failure="$TEST_OUTPUT_DIR/processing-failure.json"
    local startup="$TEST_OUTPUT_DIR/startup.json"
    local rc

    "$WARC_CLI" info "$input" --result-format=json \
        >"$completed" 2>"$TEST_OUTPUT_DIR/completed.err"
    rc=$?
    assert_command_success "$rc" "JSON processing run should succeed" || return 1
    assert_single_result "$completed" '
        .schema == "warc2es.processing/v1" and
        .status == "ok" and .exit_code == 0 and .errors == 0 and
        (.records_in | type == "number" and . >= 0) and
        (.records_out | type == "number" and . >= 0) and
        (.records_skipped | type == "number" and . >= 0) and
        .records_indexed == null and .error == null and
        .metrics.schema == "warc2es.metrics/v1"
    ' || return 1

    "$WARC_CLI" info "$input" --dry-run --result-format=json \
        >"$success" 2>"$TEST_OUTPUT_DIR/success.err"
    rc=$?
    assert_command_success "$rc" "JSON dry run should succeed" || return 1
    assert_single_result "$success" '
        .schema == "warc2es.processing/v1" and
        .status == "dry_run" and .exit_code == 0 and .errors == 0 and
        .records_in == 0 and .records_out == 0 and .records_skipped == 0 and
        .records_indexed == null and .error == null and
        .metrics.schema == "warc2es.metrics/v1" and
        (.elapsed_ms | type == "number" and . >= 0)
    ' || return 1

    set +e
    "$WARC_CLI" info "$input" --result-format=json --definitely-unknown \
        >"$failure" 2>"$TEST_OUTPUT_DIR/failure.err"
    rc=$?
    set -e
    [[ "$rc" -eq 2 ]] || {
        log_fail "Invalid arguments should return 2, got $rc"
        return 1
    }
    assert_single_result "$failure" '
        .schema == "warc2es.processing/v1" and
        .status == "error" and .exit_code == 2 and .errors == 1 and
        .error.code == "invalid_arguments" and .metrics == null and
        .records_in == null and .records_out == null
    ' || return 1

    printf 'not-a-valid-warc-or-gzip' >"$TEST_OUTPUT_DIR/corrupt.warc.gz"
    mkdir -p "$TEST_OUTPUT_DIR/corrupt-output"
    set +e
    "$WARC_CLI" extract-text "$TEST_OUTPUT_DIR/corrupt.warc.gz" \
        --output-dir="$TEST_OUTPUT_DIR/corrupt-output" --no-cdx-sidecar --result-format=json \
        >"$processing_failure" 2>"$TEST_OUTPUT_DIR/processing-failure.err"
    rc=$?
    set -e
    [[ "$rc" -eq 1 ]] || {
        log_fail "Handled processing failure should return 1, got $rc"
        return 1
    }
    assert_single_result "$processing_failure" '
        .schema == "warc2es.processing/v1" and
        .status == "error" and .exit_code == 1 and .errors == 1 and
        .error.code == "processing_failed" and .metrics.schema == "warc2es.metrics/v1" and
        (.records_in | type == "number" and . >= 0) and
        (.records_out | type == "number" and . >= 0)
    ' || return 1

    set +e
    WARC_CONFIG_FILE="$TEST_OUTPUT_DIR/missing-config.yaml" \
        "$WARC_CLI" info "$input" --result-format=json \
        >"$startup" 2>"$TEST_OUTPUT_DIR/startup.err"
    rc=$?
    set -e
    [[ "$rc" -eq 12 ]] || {
        log_fail "Configuration failure should return 12, got $rc"
        return 1
    }
    assert_single_result "$startup" '
        .schema == "warc2es.processing/v1" and
        .status == "error" and .exit_code == 12 and .errors == 1 and
        .error.code == "configuration_error" and .metrics == null and
        .records_in == null and .records_out == null
    '
}

run_test test_processing_json_contract
