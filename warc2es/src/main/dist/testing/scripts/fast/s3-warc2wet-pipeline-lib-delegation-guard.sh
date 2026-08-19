#!/bin/bash
# Guard that warc2wet delegates execution to pipeline-lib/run_pipeline.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_warc2wet_pipeline_lib_delegation_guard() {
    local script="$PROJECT_ROOT/src/main/dist/warc2wet.sh"
    assert_file_exists "$script" || return 1

    grep -Fq 'source "$PIPELINE_LIB"' "$script" || {
        log_fail "warc2wet should source pipeline-lib"
        return 1
    }
    grep -Fq 'source "$RUNTIME_LIB"' "$script" || {
        log_fail "warc2wet should source runtime-lib"
        return 1
    }
    grep -Fq 'run_pipeline warc2wet' "$script" || {
        log_fail "warc2wet should invoke run_pipeline warc2wet"
        return 1
    }

    if grep -Fq 'java $JAVA_OPTS' "$script"; then
        log_fail "Found direct java invocation in warc2wet (expected pipeline-lib delegation)"
        return 1
    fi
    if grep -Fq 'CP=""' "$script"; then
        log_fail "Found legacy manual classpath bootstrap in warc2wet"
        return 1
    fi
}

run_test test_warc2wet_pipeline_lib_delegation_guard
