#!/bin/bash
# tc-perf-003-jfr-wrapper.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

test_jfr_wrapper_generates_recording_and_report() {
    local input="$TEST_DATA_DIR/tiny.warc.gz"
    local output="$TEST_OUTPUT_DIR/jfr-out.wet.gz"
    local jfr_file="$TEST_OUTPUT_DIR/jfr-recording.jfr"
    local jfr_report="$TEST_OUTPUT_DIR/jfr-summary.txt"

    mkdir -p "$TEST_OUTPUT_DIR"
    assert_file_exists "$input" || return 1

    local heap_opts="-Xms1g -Xmx1g -XX:+UseZGC -XX:ActiveProcessorCount=3"
    local original_java_opts="${JAVA_OPTS:-}"

    WARC_JFR_ENABLED=true \
    WARC_JFR_PATH="$jfr_file" \
    JAVA_OPTS="$heap_opts" \
    "$WARC_CLI" extract-text "$input" "$output" --silent >/dev/null 2>&1 || {
        log_fail "extract-text with JFR failed"
        return 1
    }

    assert_file_exists "$jfr_file" || return 1
    assert_file_exists "$output" || return 1

    if command -v jfr >/dev/null 2>&1; then
        jfr summary "$jfr_file" > "$jfr_report" 2>&1 || {
            log_fail "jfr summary failed"
            return 1
        }
        assert_file_exists "$jfr_report" || return 1
        assert_contains "Version:" "$jfr_report" || return 1
    else
        log_warn "jfr tool not available; skipped parser-report assertion"
    fi

    # Wrapper should not mutate caller shell heap options.
    if [[ "${JAVA_OPTS:-}" != "$original_java_opts" ]]; then
        log_fail "JAVA_OPTS changed in caller environment"
        return 1
    fi
}

run_test test_jfr_wrapper_generates_recording_and_report
