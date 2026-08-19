#!/bin/bash
# @timeout: 180
# T-218: extract-text dedup(url) merges revisits per URL within contiguous-day crawl.
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

test_dedup_url_crawl_series() {
    local in_dir="$TEST_OUTPUT_DIR/ext-017-input"
    local out_dir="$TEST_OUTPUT_DIR/ext-017-out"
    local prefix="ext017"
    mkdir -p "$in_dir" "$out_dir"

    {
        create_response_record "sha256:1701" "http://example.org/same" "2026-03-01T01:00:00Z" "url dedup shared content 1701"
    } | gzip > "$in_dir/in-01.warc.gz"

    {
        create_response_record "sha256:1701" "http://example.org/same" "2026-03-02T01:00:00Z" "url dedup shared content 1701"
    } | gzip > "$in_dir/in-02a.warc.gz"

    {
        create_response_record "sha256:1701" "http://example.org/other" "2026-03-02T01:05:00Z" "url dedup shared content 1701"
    } | gzip > "$in_dir/in-02b.warc.gz"

    {
        create_response_record "sha256:1701" "http://example.org/other" "2026-03-03T01:00:00Z" "url dedup shared content 1701"
    } | gzip > "$in_dir/in-03.warc.gz"

    {
        create_response_record "sha256:1701" "http://example.org/same" "2026-03-05T01:00:00Z" "url dedup shared content 1701"
    } | gzip > "$in_dir/in-04.warc.gz"

    "$WARC_CLI" extract-text \
        "$in_dir/in-01.warc.gz" "$in_dir/in-02a.warc.gz" "$in_dir/in-02b.warc.gz" "$in_dir/in-03.warc.gz" "$in_dir/in-04.warc.gz" \
        --deduplicate-scope=url \
        --output-dir="$out_dir" --output-prefix="$prefix" --silent
    assert_command_success $? "extract-text dedup url"

    local out_a="$out_dir/${prefix}-20260301.doet.gz"
    local out_b="$out_dir/${prefix}-20260305.doet.gz"
    assert_file_exists "$out_a"
    assert_file_exists "$out_b"

    local c_a c_b
    c_a=$(warc_type_count "$out_a" "conversion")
    c_b=$(warc_type_count "$out_b" "conversion")
    [[ "$c_a" -eq 2 ]] || { log_fail "Expected 2 conversion records in crawl1, got $c_a"; return 1; }
    [[ "$c_b" -eq 1 ]] || { log_fail "Expected 1 conversion record in crawl2, got $c_b"; return 1; }

    local same_count
    same_count=$(zgrep -ic "warc-target-uri: http://example.org/same" "$out_a" || true)
    [[ "$same_count" -eq 1 ]] || { log_fail "URL dedup did not merge same-url revisit in crawl1"; return 1; }
    zgrep -qi "warc-target-uri: http://example.org/other" "$out_a" || {
        log_fail "URL dedup removed different URL unexpectedly in crawl1"
        return 1
    }

    zgrep -qi "x-nac-crawl-first-date: 2026-03-01" "$out_a" || {
        log_fail "Missing crawl first date in warcinfo for crawl1"
        return 1
    }
    zgrep -qi "x-nac-crawl-last-date: 2026-03-03" "$out_a" || {
        log_fail "Missing crawl last date in warcinfo for crawl1"
        return 1
    }

    return 0
}

run_test test_dedup_url_crawl_series
