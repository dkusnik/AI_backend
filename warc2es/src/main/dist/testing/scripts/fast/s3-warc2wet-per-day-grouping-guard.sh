#!/bin/bash
# Guard that per-day grouping stays in Java and the shell supplies one call shape.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_warc2wet_per_day_grouping_guard() {
    local script="$PROJECT_ROOT/src/main/dist/warc2wet.sh"
    assert_file_exists "$script" || return 1

    grep -Fq -- '--consumer.codec.output-format=multi-warc' "$script" || {
        log_fail "Expected Java multi-output call shape for per-day extraction"
        return 1
    }
    grep -Fq -- '--consumer.codec.output-name-template=$output_stem-{source}.wet.gz' "$script" || {
        log_fail "Expected source-derived bucket output template for Java per-day grouping"
        return 1
    }

    if grep -Fq 'declare -A file_dates' "$script"; then
        log_fail "Found obsolete shell file-date grouping"
        return 1
    fi
    if grep -Fq 'mapfile -t sorted_dates' "$script"; then
        log_fail "Found obsolete per-day JVM loop"
        return 1
    fi
}

run_test test_warc2wet_per_day_grouping_guard
