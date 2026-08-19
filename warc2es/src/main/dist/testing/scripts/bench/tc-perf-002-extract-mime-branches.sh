#!/bin/bash
# tc-perf-002-extract-mime-branches.sh
source "$(dirname "$0")/../../lib/test-lib.sh"

write_warc_response() {
    local out="$1"
    local uri="$2"
    local mime="$3"
    local body="$4"

    local body_len
    body_len=$(printf "%s" "$body" | wc -c | tr -d ' ')

    {
        printf 'WARC/1.0\r\n'
        printf 'WARC-Type: response\r\n'
        printf 'WARC-Record-ID: <urn:uuid:11111111-1111-1111-1111-111111111111>\r\n'
        printf 'WARC-Target-URI: %s\r\n' "$uri"
        printf 'WARC-Date: 2026-01-01T00:00:00Z\r\n'
        printf 'Content-Type: application/http; msgtype=response\r\n'
        printf 'Content-Length: %s\r\n' "$((body_len + 47))"
        printf '\r\n'
        printf 'HTTP/1.1 200 OK\r\n'
        printf 'Content-Type: %s\r\n' "$mime"
        printf '\r\n'
        printf '%s' "$body"
        printf '\r\n\r\n'
    } > "$out"
}

run_branch_bench() {
    local name="$1"
    local input="$2"
    local output="$3"

    local size_bytes start end elapsed throughput
    size_bytes=$(stat -c %s "$input")
    start=$(date +%s.%N)
    "$WARC_CLI" extract-text "$input" "$output" --silent >/dev/null 2>&1 || return 1
    end=$(date +%s.%N)
    elapsed=$(awk -v s="$start" -v e="$end" 'BEGIN { d=e-s; if (d<=0) d=0.001; printf "%.6f", d }')
    throughput=$(awk -v b="$size_bytes" -v d="$elapsed" 'BEGIN { printf "%.3f", (b/1024/1024)/d }')

    echo "MODULE_BENCH|extract-${name}|${throughput}|MB/s"
}

test_extract_text_microbench_by_mime_branch() {
    mkdir -p "$TEST_OUTPUT_DIR"

    local html_in="$TEST_OUTPUT_DIR/mime-html.warc"
    local plain_in="$TEST_OUTPUT_DIR/mime-plain.warc"
    local pdf_in="$TEST_OUTPUT_DIR/mime-pdf.warc"
    local office_in="$TEST_OUTPUT_DIR/mime-office.warc"

    write_warc_response "$html_in" "http://example.com/html" "text/html" "<html><body><main><p>Hello HTML benchmark branch.</p></main></body></html>"
    write_warc_response "$plain_in" "http://example.com/plain" "text/plain" "Hello plain benchmark branch."
    write_warc_response "$pdf_in" "http://example.com/pdf" "application/pdf" "%PDF-1.4 INVALID PDF PAYLOAD"
    write_warc_response "$office_in" "http://example.com/doc" "application/msword" "FAKE-DOC-CONTENT"

    run_branch_bench "html" "$html_in" "$TEST_OUTPUT_DIR/html.wet.gz" || { log_fail "html branch failed"; return 1; }
    run_branch_bench "plain" "$plain_in" "$TEST_OUTPUT_DIR/plain.wet.gz" || { log_fail "plain branch failed"; return 1; }
    run_branch_bench "pdf" "$pdf_in" "$TEST_OUTPUT_DIR/pdf.wet.gz" || { log_fail "pdf branch failed"; return 1; }
    run_branch_bench "office" "$office_in" "$TEST_OUTPUT_DIR/office.wet.gz" || { log_fail "office branch failed"; return 1; }
}

run_test test_extract_text_microbench_by_mime_branch
