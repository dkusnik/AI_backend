#!/bin/bash
# @timeout: 120
# T-219: merge with a single input file behaves as initial merge (all records are "new").
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

test_single_input_file_initial_merge() {
    local in="$TEST_OUTPUT_DIR/m010-in.doet.gz"
    local out_base="$TEST_OUTPUT_DIR/m010-base.doet.gz"
    local out_diff="$TEST_OUTPUT_DIR/m010-diff.doet.gz"

    {
        create_doet_record "sha256:m010-1" "http://m010/a" "2026-02-10T01:00:00Z" "m010 content a"
        create_doet_record "sha256:m010-2" "http://m010/b" "2026-02-10T01:01:00Z" "m010 content b"
    } | gzip > "$in"

    "$WARC_CLI" merge "$in" --output-base="$out_base" --output-diff="$out_diff" --silent
    assert_command_success $? "merge single input file"
    assert_file_exists "$out_base"
    assert_file_exists "$out_diff"

    local base_count diff_count
    base_count=$(count_records "$out_base")
    diff_count=$(count_records "$out_diff")

    [[ "$base_count" -eq 2 ]] || { log_fail "Expected 2 base records, got $base_count"; return 1; }
    [[ "$diff_count" -eq 2 ]] || { log_fail "Expected 2 diff records, got $diff_count"; return 1; }

    zgrep -qi "nac-merge-result: new" "$out_base" || {
        log_fail "Expected 'new' provenance in base output"
        return 1
    }
    return 0
}

run_test test_single_input_file_initial_merge
