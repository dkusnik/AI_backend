#!/bin/bash
# @timeout: 120
# T-216: Mixed input sources (file + directory) process all WARC files recursively (dedup-aware count bounds).
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

test_mixed_file_and_directory_recursive() {
    local in_dir="$TEST_OUTPUT_DIR/ext-015-input"
    local out_dir="$TEST_OUTPUT_DIR/ext-015-out"
    local prefix="ext015"
    mkdir -p "$in_dir/root/sub/deeper" "$out_dir"

    {
        create_response_record "sha256:1501" "http://t015/f1" "2026-02-03T03:00:00Z" "f1 synthetic text above min length"
        create_response_record "sha256:1502" "http://t015/f2" "2026-02-03T03:01:00Z" "f2 synthetic text above min length"
    } | gzip > "$in_dir/file-input.warc.gz"

    {
        create_response_record "sha256:1503" "http://t015/d1" "2026-02-03T03:02:00Z" "d1 synthetic text above min length"
    } | gzip > "$in_dir/root/dir-01.warc.gz"

    {
        create_response_record "sha256:1504" "http://t015/d2" "2026-02-03T03:03:00Z" "d2 synthetic text above min length"
    } | gzip > "$in_dir/root/sub/dir-02.warc.gz"

    {
        create_response_record "sha256:1505" "http://t015/d3" "2026-02-03T03:04:00Z" "d3 synthetic text above min length"
        create_response_record "sha256:1506" "http://t015/d4" "2026-02-03T03:05:00Z" "d4 synthetic text above min length"
    } | gzip > "$in_dir/root/sub/deeper/dir-03.warc.gz"

    local file_input="$in_dir/file-input.warc.gz"
    local dir_input="$in_dir/root"

    local input_total
    input_total=$(( \
      $(warc_type_count "$file_input" "response") + \
      $(warc_type_count "$in_dir/root/dir-01.warc.gz" "response") + \
      $(warc_type_count "$in_dir/root/sub/dir-02.warc.gz" "response") + \
      $(warc_type_count "$in_dir/root/sub/deeper/dir-03.warc.gz" "response") \
    ))
    if [[ "$input_total" -le 0 ]]; then
        log_fail "Synthetic input setup failed: expected response records > 0"
        return 1
    fi

    "$WARC_CLI" extract-text "$file_input" "$dir_input" --output-dir="$out_dir" --output-prefix="$prefix" --silent
    assert_command_success $? "extract-text with mixed file + directory inputs"

    local out="$out_dir/${prefix}-20260203.doet.gz"
    assert_file_exists "$out"

    local output_total
    output_total=$(warc_type_count "$out" "conversion")
    if [[ "$output_total" -le 0 || "$output_total" -gt "$input_total" ]]; then
        log_fail "Unexpected count for mixed input sources: input=$input_total output=$output_total"
        return 1
    fi

    log_info "Mixed file + recursive directory processed (dedup-aware): input=$input_total output=$output_total"
    return 0
}

run_test test_mixed_file_and_directory_recursive
