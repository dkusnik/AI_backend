#!/usr/bin/env bash
# test-merge-synthetic-s1-incremental.sh
# Scenario 1: Basic Incremental Crawl
# Tests: unchanged content, updated content, disappeared content, new content
set -e

# Source test library
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../../lib/test-lib.sh"

# Override TMP_DIR for synthetic tests
TMP_DIR="$TEST_OUTPUT_DIR/merge-synthetic-s1"
mkdir -p "$TMP_DIR"

# Helper: Create synthetic WET record
create_wet_record() {
    local digest="$1"
    local url="$2"
    local date="$3"
    local content="$4"
    local content_len=$(echo -n "$content" | wc -c)

    # WARC format requires CRLF (\r\n) line endings
    printf "WARC/1.0\r\n"
    printf "content-type: text/plain; charset=utf-8\r\n"
    printf "warc-payload-digest: %s\r\n" "${digest}"
    printf "warc-target-uri: %s\r\n" "${url}"
    printf "warc-date: %s\r\n" "${date}"
    printf "warc-type: conversion\r\n"
    printf "content-length: %s\r\n" "${content_len}"
    printf "\r\n"
    printf "%s\r\n" "${content}"
    printf "\r\n"
}

# Helper: Assert equals with logging
assert_eq() {
    local expected="$1"
    local actual="$2"
    local msg="$3"

    if [[ "$expected" == "$actual" ]]; then
        log_success "$msg: $actual"
        return 0
    else
        log_fail "$msg: got $actual, expected $expected"
        return 1
    fi
}

test_scenario1_basic_incremental() {
    log_info "Scenario 1: Basic Incremental Crawl"
    log_info "  Baseline: 10 records (5 stable, 3 will update, 2 will disappear)"
    log_info "  Scan:     10 records (5 stable, 3 updated, 2 new)"

    # Crawl 1 (baseline): 10 records
    {
        # 5 unchanged (A1-A5)
        create_wet_record "xxh128:0000000000000001" "http://example.com/page1" "2026-01-01T10:00:00Z" "Content A1 unchanged"
        create_wet_record "xxh128:0000000000000002" "http://example.com/page2" "2026-01-01T10:00:00Z" "Content A2 unchanged"
        create_wet_record "xxh128:0000000000000003" "http://example.com/page3" "2026-01-01T10:00:00Z" "Content A3 unchanged"
        create_wet_record "xxh128:0000000000000004" "http://example.com/page4" "2026-01-01T10:00:00Z" "Content A4 unchanged"
        create_wet_record "xxh128:0000000000000005" "http://example.com/page5" "2026-01-01T10:00:00Z" "Content A5 unchanged"
        # 3 will be updated (B1-B3 old versions)
        create_wet_record "xxh128:0000000000000011" "http://example.com/page11" "2026-01-01T10:00:00Z" "Content B1 old"
        create_wet_record "xxh128:0000000000000012" "http://example.com/page12" "2026-01-01T10:00:00Z" "Content B2 old"
        create_wet_record "xxh128:0000000000000013" "http://example.com/page13" "2026-01-01T10:00:00Z" "Content B3 old"
        # 2 will disappear (C1-C2)
        create_wet_record "xxh128:0000000000000021" "http://example.com/page21" "2026-01-01T10:00:00Z" "Content C1 will disappear"
        create_wet_record "xxh128:0000000000000022" "http://example.com/page22" "2026-01-01T10:00:00Z" "Content C2 will disappear"
    } | gzip > "$TMP_DIR/s1-crawl1.wet.gz"

    # Crawl 2 (scan): 10 records (MUST be sorted by digest for DOET format)
    {
        # 5 unchanged from crawl 1
        create_wet_record "xxh128:0000000000000001" "http://example.com/page1" "2026-02-01T10:00:00Z" "Content A1 unchanged"
        create_wet_record "xxh128:0000000000000002" "http://example.com/page2" "2026-02-01T10:00:00Z" "Content A2 unchanged"
        create_wet_record "xxh128:0000000000000003" "http://example.com/page3" "2026-02-01T10:00:00Z" "Content A3 unchanged"
        create_wet_record "xxh128:0000000000000004" "http://example.com/page4" "2026-02-01T10:00:00Z" "Content A4 unchanged"
        create_wet_record "xxh128:0000000000000005" "http://example.com/page5" "2026-02-01T10:00:00Z" "Content A5 unchanged"
        # 2 new (D1-D2) - using digests 31-32 to maintain sort order
        create_wet_record "xxh128:0000000000000031" "http://example.com/page31" "2026-02-01T10:00:00Z" "Content D1 new"
        create_wet_record "xxh128:0000000000000032" "http://example.com/page32" "2026-02-01T10:00:00Z" "Content D2 new"
        # 3 updated (B1-B3 new versions with different digests) - using 111-113 (sorted after 31-32)
        create_wet_record "xxh128:0000000000000111" "http://example.com/page11" "2026-02-01T10:00:00Z" "Content B1 NEW version"
        create_wet_record "xxh128:0000000000000112" "http://example.com/page12" "2026-02-01T10:00:00Z" "Content B2 NEW version"
        create_wet_record "xxh128:0000000000000113" "http://example.com/page13" "2026-02-01T10:00:00Z" "Content B3 NEW version"
    } | gzip > "$TMP_DIR/s1-crawl2.wet.gz"

    log_info "  Running merge..."
    "$WARC_CLI" merge \
        --output-base="$TMP_DIR/s1-base.wet.gz" \
        --output-diff="$TMP_DIR/s1-diff.wet.gz" \
        "$TMP_DIR/s1-crawl1.wet.gz" \
        "$TMP_DIR/s1-crawl2.wet.gz" 2>&1 | grep -E "Merge provenance" || true

    # Validate results
    local BASE_TOTAL=$(zcat "$TMP_DIR/s1-base.wet.gz" | grep -c "^WARC/1.0" || echo 0)
    local BASE_MERGED=$(zcat "$TMP_DIR/s1-base.wet.gz" | grep -ic "^nac-merge-result: merged" || echo 0)
    local BASE_BASE_ONLY=$(zcat "$TMP_DIR/s1-base.wet.gz" | grep -ic "^nac-merge-result: base-only" || echo 0)
    local BASE_NEW=$(zcat "$TMP_DIR/s1-base.wet.gz" | grep -ic "^nac-merge-result: new" || echo 0)

    local DIFF_TOTAL=$(zcat "$TMP_DIR/s1-diff.wet.gz" | grep -c "^WARC/1.0" || echo 0)
    local DIFF_MERGED=$(zcat "$TMP_DIR/s1-diff.wet.gz" | grep -ic "^nac-merge-result: merged" || echo 0)
    local DIFF_NEW=$(zcat "$TMP_DIR/s1-diff.wet.gz" | grep -ic "^nac-merge-result: new" || echo 0)

    log_info "  Expected (corrected behavior):"
    log_info "    Base: 15 records total (5 base-only + 5 merged + 5 new)"
    log_info "      - 5 base-only: old B1-B3 + disappeared C1-C2"
    log_info "      - 5 merged: A1-A5 unchanged"
    log_info "      - 5 new: new B1-B3 + D1-D2"
    log_info "    Diff: 10 records total (5 merged + 5 new)"
    log_info "      - 5 merged: A1-A5 with updated timestamps"
    log_info "      - 5 new: new B1-B3 + D1-D2"
    log_info "  Actual:"
    log_info "    Base: $BASE_TOTAL records ($BASE_BASE_ONLY base-only, $BASE_MERGED merged, $BASE_NEW new)"
    log_info "    Diff: $DIFF_TOTAL records ($DIFF_MERGED merged, $DIFF_NEW new)"

    # Base output: ALL records (base-only + merged + new)
    assert_eq "15" "$BASE_TOTAL" "Base total records" || return 1
    assert_eq "5" "$BASE_BASE_ONLY" "Base base-only records (old B1-B3, C1-C2)" || return 1
    assert_eq "5" "$BASE_MERGED" "Base merged records (A1-A5)" || return 1
    assert_eq "5" "$BASE_NEW" "Base new records (new B1-B3, D1-D2)" || return 1

    # Diff output: only changes (merged + new, NO base-only)
    assert_eq "10" "$DIFF_TOTAL" "Diff total records" || return 1
    assert_eq "5" "$DIFF_MERGED" "Diff merged records (A1-A5)" || return 1
    assert_eq "5" "$DIFF_NEW" "Diff new records (new B1-B3, D1-D2)" || return 1

    return 0
}

# Run test
run_test test_scenario1_basic_incremental
