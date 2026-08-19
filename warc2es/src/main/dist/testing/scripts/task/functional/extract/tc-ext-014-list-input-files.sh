#!/bin/bash
# @timeout: 120
# T-215: Explicit list of input files is fully processed (dedup-aware count bounds).
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

test_list_of_input_files() {
    local in_dir="$TEST_OUTPUT_DIR/ext-014-input"
    local out_dir="$TEST_OUTPUT_DIR/ext-014-out"
    local prefix="ext014"
    mkdir -p "$in_dir" "$out_dir"

    {
        create_response_record "sha256:1401" "http://t014/a1" "2026-02-01T01:00:00Z" "a1 synthetic text above min length"
        create_response_record "sha256:1403" "http://t014/a3" "2026-02-01T01:02:00Z" "a3 synthetic text above min length"
    } | gzip > "$in_dir/list-01.warc.gz"

    {
        create_response_record "sha256:1402" "http://t014/a2" "2026-02-01T01:01:00Z" "a2 synthetic text above min length"
        create_response_record "sha256:1405" "http://t014/b2" "2026-02-02T02:01:00Z" "b2 synthetic text above min length"
    } | gzip > "$in_dir/list-02.warc.gz"

    {
        create_response_record "sha256:1404" "http://t014/b1" "2026-02-02T02:00:00Z" "b1 synthetic text above min length"
        create_response_record "sha256:1406" "http://t014/b3" "2026-02-02T02:02:00Z" "b3 synthetic text above min length"
    } | gzip > "$in_dir/list-03.warc.gz"

    local -a files=(
        "$in_dir/list-01.warc.gz"
        "$in_dir/list-02.warc.gz"
        "$in_dir/list-03.warc.gz"
    )

    local input_total=0
    local f
    for f in "${files[@]}"; do
        input_total=$(( input_total + $(warc_type_count "$f" "response") ))
    done
    if [[ "$input_total" -le 0 ]]; then
        log_fail "Synthetic input setup failed: expected response records > 0"
        return 1
    fi

    "$WARC_CLI" extract-text "${files[@]}" --output-dir="$out_dir" --output-prefix="$prefix" --silent
    assert_command_success $? "extract-text with explicit file list"

    local out_1="$out_dir/${prefix}-20260201.doet.gz"
    local out_2="$out_dir/${prefix}-20260202.doet.gz"
    assert_file_exists "$out_1"
    assert_file_exists "$out_2"

    local output_total
    output_total=$(( $(warc_type_count "$out_1" "conversion") + $(warc_type_count "$out_2" "conversion") ))
    if [[ "$output_total" -le 0 || "$output_total" -gt "$input_total" ]]; then
        log_fail "Unexpected count for file list: input=$input_total output=$output_total"
        return 1
    fi

    log_info "Explicit file list processed (dedup-aware): input=$input_total output=$output_total"
    return 0
}

run_test test_list_of_input_files
