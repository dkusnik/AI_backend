#!/bin/bash
# @timeout: 120
# T-221: merge with primary file + directory treats first arg as baseline.
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

create_doet_record() {
    local digest="$1"
    local url="$2"
    local date="$3"
    local content="$4"
    local len
    len=$(printf "%s" "$content" | wc -c | tr -d '[:space:]')

    printf "WARC/1.0\r\n"
    printf "WARC-Type: conversion\r\n"
    printf "Content-Type: text/plain; charset=utf-8\r\n"
    printf "WARC-Payload-Digest: %s\r\n" "$digest"
    printf "WARC-Target-URI: %s\r\n" "$url"
    printf "WARC-Date: %s\r\n" "$date"
    printf "Content-Length: %s\r\n" "$len"
    printf "\r\n"
    printf "%s\r\n" "$content"
    printf "\r\n"
}

count_records() {
    zgrep -ic '^WARC/1\.[01]' "$1" 2>/dev/null || true
}

test_primary_plus_directory() {
    local primary="$TEST_OUTPUT_DIR/m012-primary.doet.gz"
    local scan_dir="$TEST_OUTPUT_DIR/m012-scans"
    local out_base="$TEST_OUTPUT_DIR/m012-base.doet.gz"
    local out_diff="$TEST_OUTPUT_DIR/m012-diff.doet.gz"
    mkdir -p "$scan_dir/sub"

    {
        create_doet_record "sha256:m012-a" "http://m012/a" "2026-02-12T01:00:00Z" "same-content-a"
        create_doet_record "sha256:m012-b" "http://m012/b" "2026-02-12T01:01:00Z" "baseline-only-b"
    } | gzip > "$primary"

    {
        create_doet_record "sha256:m012-a" "http://m012/a" "2026-02-13T01:00:00Z" "same-content-a"
    } | gzip > "$scan_dir/scan1.doet.gz"
    {
        create_doet_record "sha256:m012-c" "http://m012/c" "2026-02-13T01:02:00Z" "new-content-c"
    } | gzip > "$scan_dir/sub/scan2.doet.gz"

    "$WARC_CLI" merge "$primary" "$scan_dir" --output-base="$out_base" --output-diff="$out_diff" --silent
    assert_command_success $? "merge primary + directory"

    local base_count diff_count
    base_count=$(count_records "$out_base")
    diff_count=$(count_records "$out_diff")
    [[ "$base_count" -eq 3 ]] || { log_fail "Expected 3 base records, got $base_count"; return 1; }
    [[ "$diff_count" -eq 2 ]] || { log_fail "Expected 2 diff records, got $diff_count"; return 1; }

    zgrep -qi "nac-merge-result: base-only" "$out_base" || { log_fail "Missing base-only provenance"; return 1; }
    zgrep -qi "nac-merge-result: merged" "$out_base" || { log_fail "Missing merged provenance"; return 1; }
    zgrep -qi "nac-merge-result: new" "$out_base" || { log_fail "Missing new provenance"; return 1; }
    return 0
}

run_test test_primary_plus_directory
