#!/usr/bin/env bash
# test-merge-synthetic-s3-url-migration.sh
# Scenario 3: URL Migration (Global vs URL scope)
# Tests: deduplicate-scope behavior
set -e

# Source test library
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../../lib/test-lib.sh"

# Override TMP_DIR for synthetic tests
TMP_DIR="$TEST_OUTPUT_DIR/merge-synthetic-s3"
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

dump_merge_output() {
    local label="$1"
    local file="$2"

    log_info "  [$label] file: $file"
    if [[ ! -f "$file" ]]; then
        log_info "  [$label] missing file"
        return
    fi

    local records
    records=$(zcat "$file" | grep -c "^WARC/1.0" || echo 0)
    log_info "  [$label] records: $records"

    local base_only merged new uri_changed uri_reverted unknown
    base_only=$(zcat "$file" | grep -ic "nac-merge-result: base-only" || true); base_only=$(echo "$base_only" | head -n1); [[ -z "$base_only" ]] && base_only=0
    merged=$(zcat "$file" | grep -ic "nac-merge-result: merged" || true); merged=$(echo "$merged" | head -n1); [[ -z "$merged" ]] && merged=0
    new=$(zcat "$file" | grep -ic "nac-merge-result: new" || true); new=$(echo "$new" | head -n1); [[ -z "$new" ]] && new=0
    uri_changed=$(zcat "$file" | grep -ic "nac-merge-result: uri-changed" || true); uri_changed=$(echo "$uri_changed" | head -n1); [[ -z "$uri_changed" ]] && uri_changed=0
    uri_reverted=$(zcat "$file" | grep -ic "nac-merge-result: uri-reverted" || true); uri_reverted=$(echo "$uri_reverted" | head -n1); [[ -z "$uri_reverted" ]] && uri_reverted=0
    unknown=$(zcat "$file" | grep -ic "nac-merge-result:" || true); unknown=$(echo "$unknown" | head -n1); [[ -z "$unknown" ]] && unknown=0
    unknown=$((unknown - base_only - merged - new - uri_changed - uri_reverted))
    [[ "$unknown" -lt 0 ]] && unknown=0

    log_info "  [$label] provenance: base-only=$base_only merged=$merged new=$new uri-changed=$uri_changed uri-reverted=$uri_reverted other=$unknown"

    log_info "  [$label] digest+uri snapshot:"
    zcat "$file" | awk '
        BEGIN{d="";u="";p="";n=0}
        /^warc-payload-digest:/ {d=$2}
        /^warc-target-uri:/ {
            u=$2
            if (d!="") key=d " | " u
            else key="- | " u
            seen[key]++
        }
        /^nac-merge-result:/ {p=$2}
        /^WARC\/1\.0/ {n++}
        END{
            for (k in seen) print "    - " k "  x" seen[k]
        }
    ' | sort || true
}

test_scenario3_url_migration() {
    log_info "Scenario 3: URL Migration Detection"
    log_info "  Testing global scope (content-based) vs url scope (URL-based)"

    # Baseline: 2 records
    {
        create_wet_record "xxh128:9000000000000001" "http://old.com/page1" "2026-01-01T10:00:00Z" "Migrated content"
        create_wet_record "xxh128:9000000000000002" "http://old.com/page2" "2026-01-01T10:00:00Z" "Stable content"
    } | gzip > "$TMP_DIR/s3-c1.wet.gz"

    # Scan: Same content, one URL changed
    {
        create_wet_record "xxh128:9000000000000001" "http://new.com/page1" "2026-02-01T10:00:00Z" "Migrated content"
        create_wet_record "xxh128:9000000000000002" "http://old.com/page2" "2026-02-01T10:00:00Z" "Stable content"
    } | gzip > "$TMP_DIR/s3-c2.wet.gz"

    log_info "  Global scope (content-based dedup)..."
    "$WARC_CLI" merge \
        --output-base="$TMP_DIR/s3-global-base.wet.gz" \
        --output-diff="$TMP_DIR/s3-global-diff.wet.gz" \
        "$TMP_DIR/s3-c1.wet.gz" \
        "$TMP_DIR/s3-c2.wet.gz" 2>&1 | grep -E "Merge provenance" || true

    local GLOBAL_BASE=$(zcat "$TMP_DIR/s3-global-base.wet.gz" | grep -c "^WARC/1.0" || echo 0)
    local GLOBAL_DIFF=$(zcat "$TMP_DIR/s3-global-diff.wet.gz" | grep -c "^WARC/1.0" || echo 0)

    log_info "  URL scope (URL-based dedup)..."
    "$WARC_CLI" merge \
        --deduplicate-scope=url \
        --output-base="$TMP_DIR/s3-url-base.wet.gz" \
        --output-diff="$TMP_DIR/s3-url-diff.wet.gz" \
        "$TMP_DIR/s3-c1.wet.gz" \
        "$TMP_DIR/s3-c2.wet.gz" 2>&1 | grep -E "Merge provenance" || true

    local URL_BASE=$(zcat "$TMP_DIR/s3-url-base.wet.gz" | grep -c "^WARC/1.0" || echo 0)
    local URL_DIFF=$(zcat "$TMP_DIR/s3-url-diff.wet.gz" | grep -c "^WARC/1.0" || echo 0)

    log_info "  Expected (corrected behavior):"
    log_info "    Global: base=3 (URL-preserving; no cross-URL merge)"
    log_info "            - 9001+/old: base-only"
    log_info "            - 9002+/old: merged"
    log_info "            - 9001+/new: new"
    log_info "            diff=2 (1 merged + 1 new)"
    log_info "    URL:    base=3 (1 base-only + 1 merged + 1 new)"
    log_info "            - 9001+/old: base-only"
    log_info "            - 9002+/old: merged"
    log_info "            - 9001+/new: new"
    log_info "            diff=2 (1 merged + 1 new)"
    log_info "  Actual:"
    log_info "    Global: base=$GLOBAL_BASE, diff=$GLOBAL_DIFF"
    log_info "    URL:    base=$URL_BASE, diff=$URL_DIFF"
    dump_merge_output "global-base" "$TMP_DIR/s3-global-base.wet.gz"
    dump_merge_output "global-diff" "$TMP_DIR/s3-global-diff.wet.gz"
    dump_merge_output "url-base" "$TMP_DIR/s3-url-base.wet.gz"
    dump_merge_output "url-diff" "$TMP_DIR/s3-url-diff.wet.gz"

    # Global mode: URL-preserving merge (pywb-safe)
    assert_eq "3" "$GLOBAL_BASE" "Global: base total (1 base-only + 1 merged + 1 new)" || return 1
    assert_eq "2" "$GLOBAL_DIFF" "Global: diff total (1 merged + 1 new)" || return 1

    # URL mode: digest+URL deduplication
    assert_eq "3" "$URL_BASE" "URL: base total (1 base-only + 1 merged + 1 new)" || return 1
    assert_eq "2" "$URL_DIFF" "URL: diff total (1 merged + 1 new)" || return 1

    # Text extraction visibility: both modes preserve both old/new URL variants.
    local GLOBAL_MIGRATED_TEXT=$(zcat "$TMP_DIR/s3-global-base.wet.gz" | tr -d '\r' | grep -c "Migrated content" || echo 0)
    local URL_MIGRATED_TEXT=$(zcat "$TMP_DIR/s3-url-base.wet.gz" | tr -d '\r' | grep -c "Migrated content" || echo 0)
    assert_eq "2" "$GLOBAL_MIGRATED_TEXT" "Global: migrated text occurrences (old+new URL records)" || return 1
    assert_eq "2" "$URL_MIGRATED_TEXT" "URL: migrated text occurrences (old+new URL records)" || return 1

    # URL-scoped output should be grouped primarily by URL
    local URL_URI_ORDER_FILE="$TMP_DIR/url-base-uri-order.txt"
    zcat "$TMP_DIR/s3-url-base.wet.gz" \
        | tr -d '\r' \
        | awk -F': ' '/^WARC-Target-URI: /{print $2}' > "$URL_URI_ORDER_FILE"
    sort -c "$URL_URI_ORDER_FILE"
    assert_command_success $? "URL base records are not ordered by URL" || return 1

    return 0
}

# Run test
run_test test_scenario3_url_migration
