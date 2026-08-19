#!/bin/bash
# Execute each top-level out/ wrapper through a safe, non-destructive path.
source "$(dirname "$0")/../../lib/test-lib.sh"

test_out_wrapper_behavioral_smoke() {
    local out_root="$PROJECT_ROOT/out"
    local tmp="$TEST_OUTPUT_DIR/out-wrapper-smoke"
    local entry output rc

    mkdir -p "$tmp/empty-wet" "$tmp/doet"

    set +e
    output=$("$out_root/warc2wet.sh" --url-id=test --crawl-id=test \
        "$tmp/missing.warc.gz" 2>&1)
    rc=$?
    set -e
    assert_command_failure "$rc" "out/warc2wet.sh should execute and fail on missing input" || return 1
    echo "$output" | grep -q "Error: input not found:" || {
        log_fail "out/warc2wet.sh did not emit the clean missing-input error"
        return 1
    }

    set +e
    output=$("$out_root/wet-merge.sh" "$tmp/empty-wet" "$tmp/doet" 2>&1)
    rc=$?
    set -e
    assert_command_success "$rc" "out/wet-merge.sh should execute on an empty WET dir" || return 1
    echo "$output" | grep -q "No dated WET files" || {
        log_fail "out/wet-merge.sh did not report the empty WET dir"
        return 1
    }

    set +e
    output=$("$out_root/es-upsert.sh" "$tmp/empty-wet" --url-id=u --crawl-id=c --dry-run 2>&1)
    rc=$?
    set -e
    assert_command_failure "$rc" "out/es-upsert.sh should reject an empty ingest dir" || return 1
    echo "$output" | grep -q "no .wet.gz files found" || {
        log_fail "out/es-upsert.sh did not report the empty ingest dir"
        return 1
    }

    set +e
    output=$("$out_root/es-delete.sh" --stream=test --all-documents --dry-run 2>&1)
    rc=$?
    set -e
    assert_command_success "$rc" "out/es-delete.sh should execute dry-run" || return 1
    echo "$output" | grep -q "match_all (--all-documents)" || {
        log_fail "out/es-delete.sh did not print the dry-run match_all scope"
        return 1
    }

    for entry in warc2wet wet-merge es-upsert es-delete es-reinit; do
        set +e
        output=$("$out_root/$entry.sh" --help 2>&1)
        rc=$?
        set -e
        assert_command_success "$rc" "out/$entry.sh --help should execute" || return 1
        echo "$output" | grep -Fq "Usage: $entry.sh" || {
            log_fail "out/$entry.sh --help did not print usage"
            return 1
        }
    done
}

run_test test_out_wrapper_behavioral_smoke
