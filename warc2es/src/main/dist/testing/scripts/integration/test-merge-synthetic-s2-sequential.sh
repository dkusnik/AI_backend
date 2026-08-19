#!/usr/bin/env bash
# test-merge-synthetic-s2-sequential.sh
# Scenario 2: Three Sequential Crawls (Cumulative Merging)
# Tests: merge of merge outputs, tracking changes across multiple crawls
set -e

# Source test library
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../../lib/test-lib.sh"

# Override TMP_DIR for synthetic tests
TMP_DIR="$TEST_OUTPUT_DIR/merge-synthetic-s2"
mkdir -p "$TMP_DIR"

# Helper: Create synthetic WET record (with CRLF line endings per WARC spec)
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

test_scenario2_sequential_crawls() {
    log_info "Scenario 2: Three Sequential Crawls"
    log_info "  Crawl 1: 5 records (A,B,C,D,E)"
    log_info "  Crawl 2: 5 records (A,B,D,E,F) - update A, delete C, add F"
    log_info "  Crawl 3: 5 records (A,B,D,F,G) - update B, delete E, add G"

    # Crawl 1
    {
        create_wet_record "xxh128:1000000000000001" "http://site.com/a" "2026-01-01T10:00:00Z" "Page A v1"
        create_wet_record "xxh128:1000000000000002" "http://site.com/b" "2026-01-01T10:00:00Z" "Page B v1"
        create_wet_record "xxh128:1000000000000003" "http://site.com/c" "2026-01-01T10:00:00Z" "Page C v1"
        create_wet_record "xxh128:1000000000000004" "http://site.com/d" "2026-01-01T10:00:00Z" "Page D v1"
        create_wet_record "xxh128:1000000000000005" "http://site.com/e" "2026-01-01T10:00:00Z" "Page E v1"
    } | gzip > "$TMP_DIR/s2-c1.wet.gz"

    # Crawl 2: Update A, keep B,D,E, delete C, add F (MUST be sorted by digest)
    {
        create_wet_record "xxh128:1000000000000002" "http://site.com/b" "2026-02-01T10:00:00Z" "Page B v1"
        create_wet_record "xxh128:1000000000000004" "http://site.com/d" "2026-02-01T10:00:00Z" "Page D v1"
        create_wet_record "xxh128:1000000000000005" "http://site.com/e" "2026-02-01T10:00:00Z" "Page E v1"
        create_wet_record "xxh128:1000000000000006" "http://site.com/f" "2026-02-01T10:00:00Z" "Page F v1"
        create_wet_record "xxh128:2000000000000001" "http://site.com/a" "2026-02-01T10:00:00Z" "Page A v2 UPDATED"
    } | gzip > "$TMP_DIR/s2-c2.wet.gz"

    # Crawl 3: Keep A(v2), update B, keep D,F, delete E, add G (MUST be sorted by digest)
    {
        create_wet_record "xxh128:1000000000000004" "http://site.com/d" "2026-03-01T10:00:00Z" "Page D v1"
        create_wet_record "xxh128:1000000000000006" "http://site.com/f" "2026-03-01T10:00:00Z" "Page F v1"
        create_wet_record "xxh128:1000000000000007" "http://site.com/g" "2026-03-01T10:00:00Z" "Page G v1"
        create_wet_record "xxh128:2000000000000001" "http://site.com/a" "2026-03-01T10:00:00Z" "Page A v2 UPDATED"
        create_wet_record "xxh128:3000000000000002" "http://site.com/b" "2026-03-01T10:00:00Z" "Page B v2 UPDATED"
    } | gzip > "$TMP_DIR/s2-c3.wet.gz"

    log_info "  Merge 1: crawl1 + crawl2..."
    "$WARC_CLI" merge \
        --threads=1 \
        --output-base="$TMP_DIR/s2-m1-base.wet.gz" \
        --output-diff="$TMP_DIR/s2-m1-diff.wet.gz" \
        "$TMP_DIR/s2-c1.wet.gz" \
        "$TMP_DIR/s2-c2.wet.gz" 2>&1 | grep -E "Merge provenance" || true

    local M1_BASE=$(zcat "$TMP_DIR/s2-m1-base.wet.gz" | grep -c "^WARC/1.0" || echo 0)
    local M1_DIFF=$(zcat "$TMP_DIR/s2-m1-diff.wet.gz" | grep -c "^WARC/1.0" || echo 0)

    log_info "  Merge 2: m1-base + crawl3..."
    "$WARC_CLI" merge \
        --threads=1 \
        --output-base="$TMP_DIR/s2-m2-base.wet.gz" \
        --output-diff="$TMP_DIR/s2-m2-diff.wet.gz" \
        "$TMP_DIR/s2-m1-base.wet.gz" \
        "$TMP_DIR/s2-c3.wet.gz" 2>&1 | grep -E "Merge provenance" || true

    local M2_BASE=$(zcat "$TMP_DIR/s2-m2-base.wet.gz" | grep -c "^WARC/1.0" || echo 0)
    local M2_DIFF=$(zcat "$TMP_DIR/s2-m2-diff.wet.gz" | grep -c "^WARC/1.0" || echo 0)
    local M2_BASE_ONLY=$(zcat "$TMP_DIR/s2-m2-base.wet.gz" | grep -ic "^nac-merge-result: base-only" || echo 0)
    local M2_MERGED=$(zcat "$TMP_DIR/s2-m2-base.wet.gz" | grep -ic "^nac-merge-result: merged" || echo 0)
    local M2_NEW=$(zcat "$TMP_DIR/s2-m2-base.wet.gz" | grep -ic "^nac-merge-result: new" || echo 0)

    log_info "  Expected (corrected behavior):"
    log_info "    Merge 1: base=7 (2 base-only + 3 merged + 2 new)"
    log_info "             - base-only: A v1, C"
    log_info "             - merged: B, D, E"
    log_info "             - new: A v2, F"
    log_info "             diff=5 (3 merged + 2 new)"
    log_info "    Merge 2: base=9 (4 base-only + 3 merged + 2 new)"
    log_info "             - base-only: A v1, B v1, C, E"
    log_info "             - merged: A v2, D, F"
    log_info "             - new: B v2, G"
    log_info "             diff=5 (3 merged + 2 new)"
    log_info "  Actual:"
    log_info "    Merge 1: base=$M1_BASE, diff=$M1_DIFF"
    log_info "    Merge 2: base=$M2_BASE, diff=$M2_DIFF ($M2_BASE_ONLY base-only, $M2_MERGED merged, $M2_NEW new)"

    # Merge 1: crawl1 (5) + crawl2 (5) → M1
    assert_eq "7" "$M1_BASE" "Merge 1: Base total (2 base-only + 3 merged + 2 new)" || return 1
    assert_eq "5" "$M1_DIFF" "Merge 1: Diff total (3 merged + 2 new)" || return 1

    # Merge 2: M1 (7) + crawl3 (5) → M2
    assert_eq "9" "$M2_BASE" "Merge 2: Base total (4 base-only + 3 merged + 2 new)" || return 1
    assert_eq "5" "$M2_DIFF" "Merge 2: Diff total (3 merged + 2 new)" || return 1
    assert_eq "4" "$M2_BASE_ONLY" "Merge 2: Base base-only (A v1, B v1, C, E)" || return 1
    assert_eq "3" "$M2_MERGED" "Merge 2: Base merged (A v2, D, F)" || return 1
    assert_eq "2" "$M2_NEW" "Merge 2: Base new (B v2, G)" || return 1
    assert_eq "4" "$M2_BASE_ONLY" "Merge 2: Base base-only (A v1, B v1, C, E)" || return 1
    assert_eq "3" "$M2_MERGED" "Merge 2: Base merged (A v2, D, F)" || return 1
    assert_eq "2" "$M2_NEW" "Merge 2: Base new (B v2, G)" || return 1

    return 0
}

# Run test
run_test test_scenario2_sequential_crawls
