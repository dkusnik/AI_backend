#!/bin/bash
# Human console logs are JSON on stdout; JSON-result logs use stderr; files retain WARN+ only.
source "$(dirname "$0")/../../lib/test-lib.sh"

extract_json_lines() {
    awk '/^\{/{print}' "$1" > "$2"
}

test_json_result_logging() {
    mkdir -p "$TEST_OUTPUT_DIR"
    local input="$TEST_DATA_DIR/tiny.warc.gz"
    local human_out="$TEST_OUTPUT_DIR/human.out"
    local json_out="$TEST_OUTPUT_DIR/result.out"
    local json_logs="$TEST_OUTPUT_DIR/result-logs.jsonl"
    local human_logs="$TEST_OUTPUT_DIR/human-logs.jsonl"
    local log_file="$TEST_OUTPUT_DIR/pipeline.log"
    local rc

    "$WARC_CLI" info "$input" --dry-run --debug \
        >"$human_out" 2>"$TEST_OUTPUT_DIR/human.err"
    rc=$?
    assert_command_success "$rc" "Human debug dry run should succeed" || return 1
    extract_json_lines "$human_out" "$human_logs"
    jq -se 'length > 0 and all(.[]; .level != null and .message != null)' \
        "$human_logs" >/dev/null || {
        log_fail "Human-mode console log lines are not structured JSON on stdout"
        return 1
    }

    "$WARC_CLI" info "$input" --dry-run --debug --result-format=json \
        >"$json_out" 2>"$TEST_OUTPUT_DIR/result.err"
    rc=$?
    assert_command_success "$rc" "JSON debug dry run should succeed" || return 1
    [[ "$(wc -l < "$json_out")" -eq 1 ]] || {
        log_fail "JSON-result stdout contains more than the result object"
        return 1
    }
    jq -e '.schema == "warc2es.processing/v1" and .status == "dry_run"' \
        "$json_out" >/dev/null || return 1
    extract_json_lines "$TEST_OUTPUT_DIR/result.err" "$json_logs"
    jq -se 'length > 0 and all(.[]; .level != null and .message != null)' \
        "$json_logs" >/dev/null || {
        log_fail "JSON-result console logs were not routed to stderr"
        return 1
    }

    JAVA_OPTS="-Dwarc.logging.file=$log_file -Dwarc.logging.dir=$TEST_OUTPUT_DIR" \
        "$WARC_CLI" info "$input" --dry-run --debug --result-format=json \
        >"$TEST_OUTPUT_DIR/file-info.out" 2>"$TEST_OUTPUT_DIR/file-info.err"
    rc=$?
    assert_command_success "$rc" "INFO file-filter probe should succeed" || return 1
    if [[ -f "$log_file" ]] && grep -Fq '[INFO]' "$log_file"; then
        log_fail "INFO console logs leaked into the WARN+ rolling file"
        return 1
    fi

    set +e
    WARC_CONFIG_FILE="$TEST_OUTPUT_DIR/missing.yaml" \
        JAVA_OPTS="-Dwarc.logging.file=$log_file -Dwarc.logging.dir=$TEST_OUTPUT_DIR" \
        "$WARC_CLI" info "$input" --debug --result-format=json \
        >"$TEST_OUTPUT_DIR/file-error.out" 2>"$TEST_OUTPUT_DIR/file-error.err"
    rc=$?
    set -e
    [[ "$rc" -eq 12 ]] || {
        log_fail "File logging error probe should return 12, got $rc"
        return 1
    }
    grep -Fq '[ERROR]' "$log_file" || {
        log_fail "ERROR did not reach the WARN+ rolling file"
        return 1
    }
    grep -Fq 'pipeline-%d{yyyy-MM-dd}' "$PROJECT_ROOT/src/main/resources/log4j2.xml" || {
        log_fail "Daily rolling-file policy is missing"
        return 1
    }
}

run_test test_json_result_logging
