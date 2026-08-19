#!/bin/bash
# @timeout: 120
# T-220: merge with one directory argument is treated as initial merge (no primary).
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

test_single_input_directory_initial_merge() {
    local dir="$TEST_OUTPUT_DIR/m011-dir"
    local out_base="$TEST_OUTPUT_DIR/m011-base.doet.gz"
    local out_diff="$TEST_OUTPUT_DIR/m011-diff.doet.gz"
    mkdir -p "$dir/sub"

    {
        create_doet_record "sha256:m011-1" "http://m011/a" "2026-02-11T01:00:00Z" "m011 content a"
    } | gzip > "$dir/a.doet.gz"
    {
        create_doet_record "sha256:m011-2" "http://m011/b" "2026-02-11T01:01:00Z" "m011 content b"
    } | gzip > "$dir/sub/b.doet.gz"

    "$WARC_CLI" merge "$dir" --output-base="$out_base" --output-diff="$out_diff" --silent
    assert_command_success $? "merge single directory input"
    assert_file_exists "$out_base"
    assert_file_exists "$out_diff"

    local base_count diff_count
    base_count=$(count_records "$out_base")
    diff_count=$(count_records "$out_diff")
    [[ "$base_count" -eq 2 ]] || { log_fail "Expected 2 base records, got $base_count"; return 1; }
    [[ "$diff_count" -eq 2 ]] || { log_fail "Expected 2 diff records, got $diff_count"; return 1; }

    zgrep -qi "nac-merge-result: base-only" "$out_base" && {
        log_fail "Directory initial merge unexpectedly produced base-only records"
        return 1
    }
    return 0
}

run_test test_single_input_directory_initial_merge
