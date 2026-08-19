#!/bin/bash
# @timeout: 180
# T-217: extract-text dedup(global) groups contiguous days into one crawl output.
set -euo pipefail
source "$(dirname "$0")/../../../../lib/test-lib.sh"

REC_SEQ=0

create_response_record() {
    local digest="$1"
    local url="$2"
    local date="$3"
    local content="$4"
    REC_SEQ=$((REC_SEQ + 1))
    local rid
    rid=$(printf "%08d-0000-4000-8000-%012d" "$REC_SEQ" "$REC_SEQ")
    local body
    body="<html><body>${content}</body></html>"
    local http_head=$'HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n'
    local content_len
    content_len=$(( \
      $(printf "%s" "$http_head" | wc -c | tr -d '[:space:]') + \
      $(printf "%s" "$body" | wc -c | tr -d '[:space:]') + 2 \
    ))

    printf "WARC/1.0\r\n"
    printf "WARC-Type: response\r\n"
    printf "WARC-Record-ID: <urn:uuid:%s>\r\n" "$rid"
    printf "Content-Type: application/http; msgtype=response\r\n"
    printf "WARC-Payload-Digest: %s\r\n" "$digest"
    printf "WARC-Target-URI: %s\r\n" "$url"
    printf "WARC-Date: %s\r\n" "$date"
    printf "Content-Length: %s\r\n" "$content_len"
    printf "\r\n"
    printf "%s" "$http_head"
    printf "%s" "$body"
    printf "\r\n"
    printf "\r\n"
}

warc_type_count() {
    local file="$1"
    local type="$2"
    zgrep -ic "^WARC-Type: ${type}" "$file" 2>/dev/null || true
}

test_dedup_global_crawl_series() {
    local in_dir="$TEST_OUTPUT_DIR/ext-016-input"
    local out_dir="$TEST_OUTPUT_DIR/ext-016-out"
    local prefix="ext016"
    mkdir -p "$in_dir" "$out_dir"

    {
        create_response_record "sha256:1601" "http://example.org/very/long/path/a" "2026-02-01T01:00:00Z" "global dedup shared content 1601"
    } | gzip > "$in_dir/in-01.warc.gz"

    {
        create_response_record "sha256:1601" "http://a.co/x" "2026-02-02T01:00:00Z" "global dedup shared content 1601"
    } | gzip > "$in_dir/in-02a.warc.gz"

    {
        create_response_record "sha256:1602" "http://example.org/b" "2026-02-02T01:10:00Z" "global unique 1602 day2"
    } | gzip > "$in_dir/in-02b.warc.gz"

    {
        create_response_record "sha256:1601" "http://z.example/x" "2026-02-04T01:00:00Z" "global dedup content 1601 day4 gap new crawl"
    } | gzip > "$in_dir/in-03.warc.gz"

    "$WARC_CLI" extract-text \
        "$in_dir/in-01.warc.gz" "$in_dir/in-02a.warc.gz" "$in_dir/in-02b.warc.gz" "$in_dir/in-03.warc.gz" \
        --deduplicate-scope=global \
        --output-dir="$out_dir" --output-prefix="$prefix" --silent
    assert_command_success $? "extract-text dedup global"

    local out_a="$out_dir/${prefix}-20260201.doet.gz"
    local out_b="$out_dir/${prefix}-20260204.doet.gz"
    assert_file_exists "$out_a"
    assert_file_exists "$out_b"

    local out_count
    out_count=$(find "$out_dir" -maxdepth 1 -type f -name "${prefix}-*.doet.gz" | wc -l | tr -d '[:space:]')
    [[ "$out_count" -eq 2 ]] || { log_fail "Expected 2 crawl outputs, got $out_count"; return 1; }

    local c_a c_b
    c_a=$(warc_type_count "$out_a" "conversion")
    c_b=$(warc_type_count "$out_b" "conversion")
    [[ "$c_a" -eq 2 ]] || { log_fail "Expected 2 conversion records in crawl1, got $c_a"; return 1; }
    [[ "$c_b" -eq 1 ]] || { log_fail "Expected 1 conversion record in crawl2, got $c_b"; return 1; }

    zgrep -qi "warc-target-uri: http://a.co/x" "$out_a" || {
        log_fail "Global dedup did not keep shortest URI in first crawl"
        return 1
    }
    zgrep -qi "warc-target-uri: http://example.org/very/long/path/a" "$out_a" && {
        log_fail "Global dedup kept longer URI unexpectedly"
        return 1
    }

    zgrep -qi "x-nac-crawl-first-date: 2026-02-01" "$out_a" || {
        log_fail "Missing crawl first date in warcinfo for crawl1"
        return 1
    }
    zgrep -qi "x-nac-crawl-last-date: 2026-02-02" "$out_a" || {
        log_fail "Missing crawl last date in warcinfo for crawl1"
        return 1
    }
    zgrep -qi "x-nac-crawl-first-date: 2026-02-04" "$out_b" || {
        log_fail "Missing crawl first date in warcinfo for crawl2"
        return 1
    }
    zgrep -qi "x-nac-crawl-last-date: 2026-02-04" "$out_b" || {
        log_fail "Missing crawl last date in warcinfo for crawl2"
        return 1
    }

    return 0
}

run_test test_dedup_global_crawl_series
